#!/usr/bin/env python3
"""One Region's ramp. Database progress/completion, not the Invoke socket, is authoritative."""
import argparse
import asyncio
import itertools
import json
import math
import signal
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from psycopg import errors

import demo_color
from lambda_diagnostics import Diagnostics
from loadgen_lambda import (
    COMPLETION_SECONDS,
    LEASE_SECONDS,
    MAX_DURATION,
    TRANSIENT,
    Database,
    Pacer,
    Session,
    handler,
    number,
    pause,
)
from pooled_latency import POOL_QUERY, region_pool

LATEST = """
SELECT DISTINCT ON (origin, gen)
       gen, workers, commits, p50_ms, p99_ms, conflicts, errors, ts,
       extract(epoch FROM clock_timestamp() - ts) AS age_s
FROM demo_stats
WHERE origin = %s AND gen LIKE %s
  AND ts > clock_timestamp() - interval '30 seconds'
ORDER BY origin, gen, ts DESC
"""
HEARTBEAT = (
    "UPDATE demo_lambda_runs SET lease_until = clock_timestamp() + interval '30 seconds' "
    "WHERE run_id = %s AND stop = false"
)
STOP = "UPDATE demo_lambda_runs SET stop = true, lease_until = clock_timestamp() WHERE run_id = %s"
RUN_EXISTS = "SELECT stop FROM demo_lambda_runs WHERE run_id = %s"
SCHEDULE_EXISTS = "SELECT phase, active FROM demo_lambda_schedule WHERE run_id = %s"
SCHEDULE = "UPDATE demo_lambda_schedule SET phase = %s, active = %s WHERE run_id = %s"
INVOCATIONS = (
    "SELECT slot, owner, status, phase, workers, first_done, summary, "
    "extract(epoch FROM clock_timestamp() - updated_at) FROM demo_lambda_invocations WHERE run_id = %s"
)
RETURN_WAIT_SECONDS = LEASE_SECONDS + COMPLETION_SECONDS + 6


class RunStopped(Exception):
    """Control work interrupted by the presenter, not a service failure."""

def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--function", required=True)
    p.add_argument("--region", required=True)
    p.add_argument("--host", required=True)
    p.add_argument("--origin", required=True)
    p.add_argument("--profile")
    p.add_argument("--ramp", default="10,100,1000,3000,9900")
    p.add_argument("--step-seconds", type=float, default=20)
    p.add_argument("--hold-seconds", type=float, default=0)
    p.add_argument("--conns-per-invocation", type=int, default=250)
    p.add_argument("--open-rate", type=float, default=90,
                   help="combined first-open plus reconnect budget for this cluster; maximum 90/s")
    p.add_argument("--think-ms", type=float, default=250)
    p.add_argument("--hot", action="store_true")
    p.add_argument("--include-first-txn", action="store_true", help="include first successes in displayed pooled latency")
    p.add_argument("--log-file", help="details file (default: logs/run-<id>.log beside this script)")
    p.add_argument("--hold-tolerance", type=float, default=.995)
    p.add_argument("--local-test", action="store_true",
                   help="call the handler locally; requires a loopback DSQL_DEMO_LOCAL_DSN; no AWS clients")
    return p.parse_args(argv)


def plan(cfg):
    steps = [int(x) for x in cfg.ramp.split(',')]
    if not steps or steps[0] < 1 or any(b <= a for a, b in itertools.pairwise(steps)) or steps[-1] > 9900:
        raise ValueError('ramp must strictly increase from at least 1 to at most 9900')
    number(cfg.conns_per_invocation, 'conns-per-invocation', 1, 500)
    number(cfg.open_rate, 'open-rate', 10, 90)
    number(cfg.step_seconds, 'step-seconds', 1, 600)
    number(cfg.hold_seconds, 'hold-seconds', 0, 600)
    number(cfg.think_ms, 'think-ms', 0, 60000)
    number(cfg.hold_tolerance, 'hold-tolerance', .99, 1)
    waves, previous = [], 0
    for target in steps:
        delta = target - previous
        counts = [min(cfg.conns_per_invocation, delta-i) for i in range(0, delta, cfg.conns_per_invocation)]
        waves.append({'target': target, 'counts': counts})
        previous = target
    count = sum(len(w['counts']) for w in waves)
    if count > 95:  # two driver connections, up to two watchers, one operational spare
        raise ValueError('at most 95 invocations within the 100 reserved database connections')
    budget = cfg.open_rate - 2  # driver's two independent sessions share 2 attempts/s
    for wave in waves:
        wave['rate'] = budget * .85 / len(wave['counts'])
        wave['retry_rate'] = budget * .10 / count
        wave['admin_rate'] = budget * .05 / count
        wave['recovery_budget'] = budget * .95
        wave['ramp_timeout'] = (max(wave['counts']) + 1) / wave['rate'] + 30
    recovery_slack = math.ceil(cfg.conns_per_invocation * count / (budget * .95)) + 30
    duration = math.ceil(sum(w['ramp_timeout'] + cfg.step_seconds + 3 for w in waves)
                         + cfg.hold_seconds + recovery_slack + 20)
    if duration > MAX_DURATION:
        raise ValueError(f'ramp and recovery reserve need {duration}s; shorten to fit {int(MAX_DURATION)}s')
    return waves, count, duration


def invoke(client, function, payload, local_test):
    if local_test:
        return handler(payload, None)
    response = client.invoke(FunctionName=function, InvocationType='RequestResponse', Payload=json.dumps(payload).encode())
    try:
        result = json.loads(response['Payload'].read())
    finally:
        response['Payload'].close()
    if response.get('FunctionError') or response.get('StatusCode') != 200:
        raise RuntimeError(f'Invoke response: {result}')
    return result


def aggregate(rows):
    live = [r for r in rows if r[-1] <= 3]
    return (sum(r[1] for r in live), sum(r[2] for r in live),
            max((r[3] for r in live if r[3] is not None), default=None),
            max((r[4] for r in live if r[4] is not None), default=None),
            sum(r[5] for r in live), sum(r[6] for r in live))


class ScreenSamples:
    """Only publish rates after every live generator has an unseen stats row."""
    def __init__(self):
        self.seen = {}

    def take(self, rows, histogram_rows=(), origin=None, include_first_txn=False):
        live = [r for r in rows if r[-1] <= 3]
        fresh = [r for r in live if self.seen.get(r[0]) != r[-2]]
        workers = sum(r[1] for r in live)
        pooled = region_pool(histogram_rows, origin, [r[0] for r in live], include_first_txn)
        p50, p99 = pooled['p50_ms'], pooled['p99_ms']
        if not live or len(fresh) != len(live):
            return (workers, None, p50, p99, None, None), len(fresh), len(live)
        self.seen.update((r[0], r[-2]) for r in live)
        _, commits, _, _, conflicts, errors = aggregate(live)
        return (workers, commits, p50, p99, conflicts, errors), len(fresh), len(live)


def rate(value, width):
    return f'{value:>{width}}' if value is not None else f'{"-":>{width}}'


def recovery_seconds(expected, records, rows, budget):
    fresh = {r[0].split(':')[1]: r[1] for r in rows if r[-1] <= 3}
    missing = max((n - fresh.get(slot, 0) for slot, n in expected.items()), default=0)
    # A stale progress record is conservatively treated as the whole slot missing.
    for slot, n in expected.items():
        if slot not in records or records[slot][-1] > LEASE_SECONDS:
            missing = max(missing, n)
    rate = budget / max(1, len(expected))
    return math.ceil(max(0, missing) / rate) + 30


async def heartbeat_loop(session, run_id, stop, fatal, log):
    try:
        while not stop.is_set():
            try:
                await session.attempt(HEARTBEAT, (run_id,), time.monotonic() + LEASE_SECONDS)
            except TRANSIENT as exc:
                log('heartbeat retry', str(exc))
            await pause(stop, 1)
        # No more renewals after stop. Even if all writes fail, the lease expires.
        deadline = time.monotonic() + LEASE_SECONDS
        while time.monotonic() < deadline:
            try:
                await session.attempt(STOP, (run_id,), deadline)
                break
            except TRANSIENT as exc:
                log('stop retry', str(exc))
                await asyncio.sleep(.2)
    except Exception as exc:  # noqa: BLE001 -- communicate fatal schema/permission errors
        fatal.append(exc)
        stop.set()
        log('heartbeat fatal', str(exc))
    finally:
        await session.close()


def run_log_path(cfg, run_id):
    path = Path(cfg.log_file) if cfg.log_file else Path(__file__).with_name('logs') / f'run-{run_id}.log'
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


async def drive(cfg, stop_event=None):
    waves, count, duration = plan(cfg)
    run_id = str(uuid.uuid4())
    log_path = run_log_path(cfg, run_id)
    detail = log_path.open('x')
    log_label = str(log_path) if cfg.log_file else f'logs/{log_path.name}'

    def log(event, data):
        detail.write(json.dumps({'event': event, 'data': data}, default=str) + '\n')
        detail.flush()

    diagnostics = Diagnostics(run_id, emit=lambda row: log("diagnostic", row))
    samples = ScreenSamples()
    stop = stop_event or asyncio.Event()
    loop = asyncio.get_running_loop()
    signals = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
            signals.append(sig)
        except (NotImplementedError, RuntimeError, ValueError):
            pass
    client = None
    pool = ThreadPoolExecutor(max_workers=count)
    futures, observed, expected = [], set(), {}
    heartbeat = None
    fatal = []
    start = time.monotonic()
    deadline = start + duration
    pacer = Pacer(2)
    monitor = pulse = None
    success, error = True, None
    mode = 'HOT ROW' if cfg.hot else 'random keys'
    print(demo_color.bold(f'\n  Aurora DSQL load generator | {cfg.origin} | {mode}'), flush=True)
    print(demo_color.bold(f'  max {waves[-1]["target"]} workers; {count} invocations; budget {cfg.open_rate:g}/s; details {log_label}'))
    first_mode = 'included' if cfg.include_first_txn else 'excluded'
    print(demo_color.bold(f'  Latency: pooled p50/p99 across all invocations; first txn per connection {first_mode}.\n'
          '  Reporting: closed UTC second two seconds behind database time; about 2-3s delay; incomplete = dashes.'))
    # Repaint this header every HEADER_EVERY data rows so the scrolling numbers
    # never drift far from their column labels (the output scrolls; it is not
    # redrawn in place like the watcher).
    header_line = demo_color.bold('  time    workers   target  commits/s  p50 ms    p99 ms   conflicts/s   errors/s')
    HEADER_EVERY = 20
    printed_rows = 0
    print(header_line, flush=True)

    async def retry_sql(sql, args):
        while not stop.is_set() and time.monotonic() < deadline:
            try:
                return await monitor.attempt(sql, args, deadline)
            except TRANSIENT as exc:
                log('monitor retry', str(exc))
                await pause(stop, .2)
        if stop.is_set():
            raise RunStopped()
        raise TimeoutError('control operation exceeded run deadline')

    async def ensure_startup(sql, verify, expected_row):
        try:
            await retry_sql(sql, (run_id,))
        except errors.UniqueViolation:
            # A prior attempt may have committed before its acknowledgement was
            # lost. Read back exactly the initial state; never reset a live run.
            row = await (await retry_sql(verify, (run_id,))).fetchone()
            if row is None or tuple(row) != expected_row:
                raise RuntimeError('startup key exists with unexpected state') from None

    async def records_now():
        return {r[0]: r for r in await (await retry_sql(INVOCATIONS, (run_id,))).fetchall()}

    def inspect(records, during_run=True):
        # HTTP outcomes are diagnostics only. Never retry an ambiguous Invoke.
        for future in futures:
            if future.done() and future not in observed:
                observed.add(future)
                try:
                    log('invoke response', future.result())
                except Exception as exc:  # noqa: BLE001 -- DB, not transport, determines failure
                    log('invoke transport/response error', f'{type(exc).__name__}: {exc}')
        for slot, row in records.items():
            if row[2] == 'done':
                summary = json.loads(row[6])
                if summary.get('error') or summary.get('open_connections_after_cleanup'):
                    raise RuntimeError(f'invocation {slot} failed: {summary}')
                if during_run and not stop.is_set() and slot in expected:
                    raise RuntimeError(f'invocation {slot} completed before the run stopped: {summary["reason"]}')

    try:
        if not cfg.local_test:
            import boto3
            from botocore.config import Config
            boto3.setup_default_session(profile_name=cfg.profile, region_name=cfg.region)
            client = boto3.client('lambda', config=Config(
                read_timeout=930, connect_timeout=5, max_pool_connections=count, tcp_keepalive=True,
                retries={'total_max_attempts': 1},
            ))
        db = Database(cfg.host, cfg.region, cfg.local_test, diagnostics)
        monitor, pulse = Session(db, pacer), Session(db, pacer, phase="heartbeat")
        await ensure_startup('INSERT INTO demo_lambda_runs (run_id, stop, lease_until) '
                             "VALUES (%s, false, clock_timestamp() + interval '30 seconds')", RUN_EXISTS, (False,))
        await ensure_startup('INSERT INTO demo_lambda_schedule (run_id, phase, active) VALUES (%s, 0, 0)',
                             SCHEDULE_EXISTS, (0, 0))
        await retry_sql(HEARTBEAT, (run_id,))
        heartbeat = asyncio.create_task(heartbeat_loop(pulse, run_id, stop, fatal, log))
        next_tick = start
        for step, wave in enumerate(waves):
            if stop.is_set():
                break
            active = len(expected) + len(wave['counts'])
            await retry_sql(SCHEDULE, (2 * step, active, run_id))
            # Existing invocations must acknowledge the smaller retry rate before
            # any next-wave first-open permits can be used. No stale-budget overlap.
            barrier_end = min(deadline, time.monotonic() + LEASE_SECONDS + 10)
            while expected and not stop.is_set():
                records = await records_now()
                inspect(records)
                if all(s in records and records[s][3] == 2 * step for s in expected):
                    break
                if time.monotonic() >= barrier_end:
                    raise TimeoutError('previous wave did not acknowledge ramp pacing')
                await pause(stop, .2)
            if stop.is_set():
                break
            for index, n in enumerate(wave['counts']):
                slot = f'{step}-{index}'
                expected[slot] = n
                payload = {'host': cfg.host, 'region': cfg.region, 'origin': cfg.origin,
                           'run_id': run_id, 'slot': slot, 'connections': n,
                           'open_rate': wave['rate'], 'retry_rate': wave['retry_rate'],
                           'admin_rate': wave['admin_rate'], 'recovery_budget': wave['recovery_budget'],
                           'duration': max(1, deadline - time.monotonic()), 'think_ms': cfg.think_ms,
                           'hot': cfg.hot, 'local_test': cfg.local_test}
                futures.append(pool.submit(invoke, client, cfg.function, payload, cfg.local_test))
            ramp_end = min(deadline, time.monotonic() + wave['ramp_timeout'])
            ready_at, recovery_end, borrowed = None, None, False
            recovery_allowance = 0
            recovering = False
            hold = cfg.step_seconds + (cfg.hold_seconds if step == len(waves)-1 else 0)
            floor = math.ceil(wave['target'] * cfg.hold_tolerance)
            while not stop.is_set():
                await pause(stop, next_tick - time.monotonic())
                if stop.is_set():
                    break
                tick = time.monotonic()
                next_tick = tick + 1
                try:
                    records = {r[0]: r for r in await (await monitor.attempt(INVOCATIONS, (run_id,), deadline)).fetchall()}
                    rows = await (await monitor.attempt(LATEST, (cfg.origin, run_id + ':%'), deadline)).fetchall()
                    histogram_rows = await (await monitor.attempt(POOL_QUERY, deadline=deadline)).fetchall()
                except TRANSIENT as exc:
                    log('stats retry', str(exc))
                    ready_at = None  # an unobserved interval never completes a hold
                    if time.monotonic() >= deadline:
                        raise TimeoutError('run deadline while reconnecting monitor') from exc
                    continue
                inspect(records)
                (workers, commits, p50, p99, conflicts, errs), fresh, live_count = samples.take(rows, histogram_rows, cfg.origin, cfg.include_first_txn)
                pooled = region_pool(histogram_rows, cfg.origin, [r[0] for r in rows if r[-1] <= 3], cfg.include_first_txn)
                sample = {'elapsed_s': round(tick-start, 3), 'workers': workers, 'target': wave['target'],
                          'commits_s': commits, 'p50_ms': p50, 'p99_ms': p99,
                          'worst_generator_p99_ms': aggregate(rows)[3], 'pooled': pooled,
                          'conflicts_s': conflicts, 'errors_s': errs,
                          'new_generators': fresh, 'live_generators': live_count,
                          'source_ts': {r[0]: str(r[-2]) for r in rows}}
                log('sample', sample)
                suffix = '' if commits is not None else f'  waiting for new stats ({fresh}/{live_count})'
                # Build each column to its fixed width FIRST, then wrap in color so the
                # zero-width escapes never shift alignment. Guard dashes (None) from red.
                commits_s = rate(commits, 9)
                p99_s = demo_color.p99(latency(p99), p99)
                conflicts_s = rate(conflicts, 11)
                if conflicts not in (None, 0):
                    conflicts_s = demo_color.red(conflicts_s)
                errs_s = rate(errs, 8)
                if errs not in (None, 0):
                    errs_s = demo_color.red(errs_s)
                suffix = demo_color.dim(suffix) if suffix else suffix
                if printed_rows and printed_rows % HEADER_EVERY == 0:
                    print(header_line, flush=True)
                print(f'  {int(tick-start):4d}s  {workers:>7}  {wave["target"]:>5}  {commits_s}  '
                      f'{latency(p50)}  {p99_s}  {conflicts_s}  {errs_s}{suffix}', flush=True)
                printed_rows += 1
                if workers > wave['target']:
                    raise RuntimeError('workers exceed planned target')
                if not borrowed and all(s in records and records[s][5] >= n for s, n in expected.items()):
                    await retry_sql(SCHEDULE, (2 * step + 1, active, run_id))
                    borrowed = True
                    recovery_allowance = recovery_seconds(expected, records, rows, wave['recovery_budget'])
                    recovery_end = tick + recovery_allowance
                if not borrowed and tick > ramp_end:
                    raise TimeoutError('first-open ramp did not finish')
                if borrowed:
                    if workers >= floor:
                        recovering = False
                        if ready_at is None:
                            ready_at = tick
                        if tick >= ready_at + hold:
                            break
                    else:
                        required = recovery_seconds(expected, records, rows, wave['recovery_budget'])
                        if not recovering or required > recovery_allowance:
                            recovery_allowance = required
                            recovery_end = tick + required
                        recovering = True
                        ready_at = None
                        if tick >= recovery_end:
                            raise TimeoutError('missing connections exceeded their recovery allowance')
                if tick >= deadline:
                    raise TimeoutError('run duration exhausted')
        if fatal:
            raise fatal[0]
    except RunStopped:
        pass
    except Exception as exc:  # noqa: BLE001 -- preserve details without flooding the recording
        success, error = False, str(exc)
        log('run exception', f'{type(exc).__name__}: {exc}')
    finally:
        stop.set()
        print(f'  stopping; details: {log_label}', flush=True)  # only post-Ctrl-C screen line
        stop_deadline = time.monotonic() + RETURN_WAIT_SECONDS
        complete = set()
        while monitor and time.monotonic() < stop_deadline and len(complete) < len(expected):
            try:
                records = {r[0]: r for r in await (await monitor.attempt(INVOCATIONS, (run_id,), stop_deadline)).fetchall()}
                inspect(records, during_run=False)
                complete = {s for s in expected if s in records and records[s][2] == 'done'}
                for s in complete:
                    log('database completion', json.loads(records[s][6]))
                if len(complete) == len(expected):
                    break
            except TRANSIENT as exc:
                log('completion poll retry', str(exc))
            except Exception as exc:  # noqa: BLE001 -- persist fatal completion errors
                success, error = False, str(exc)
                log('completion error', error)
                break
            await asyncio.sleep(.2)
        if len(complete) != len(expected):
            success, error = False, 'database completion not confirmed for all slots'
        if heartbeat:
            await heartbeat  # event-driven, never cancel an in-flight heartbeat query
        elif pulse:
            await pulse.close()
        if monitor:
            await monitor.close()
        if fatal:
            success, error = False, str(fatal[0])
        pool.shutdown(wait=False, cancel_futures=False)
        if client:
            client.close()
        for sig in signals:
            loop.remove_signal_handler(sig)
        result = {'run_id': run_id, 'invocations': len(expected), 'completed': len(complete),
                  'duration': time.monotonic()-start, 'success': success, 'error': error,
                  'log_file': str(log_path)}
        result['error_details'] = diagnostics.summary()['error_details']
        log('result', result)
        detail.close()
    return result


def latency(value):
    return f'{value:7.1f}' if value is not None else '      -'


def main():
    cfg = parse_args()
    try:
        plan(cfg)
    except ValueError as exc:
        print(f'Invalid ramp: {exc}')
        return 1
    return 0 if asyncio.run(drive(cfg))['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
