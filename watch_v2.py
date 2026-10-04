"""Both Regions' dashboard with pooled whole-transaction p99 from closed UTC seconds."""
import argparse
import asyncio
import signal
import time

import demo_color
from loadgen_lambda import TRANSIENT, Database, Pacer, Session, pause
from pooled_latency import POOL_QUERY, region_pool
from watch import STALE_AFTER_S

LATEST = """
SELECT DISTINCT ON (origin, gen)
       origin, gen, workers, commits, p99_ms, conflicts, last_order_id,
       extract(epoch FROM clock_timestamp() - ts) AS age_s
FROM demo_stats
WHERE ts > clock_timestamp() - interval '30 seconds'
ORDER BY origin, gen, ts DESC
"""


def summarize(rows, histogram_rows, include_first_txn=False):
    regions = {}
    for origin, gen, workers, commits, _worst, conflicts, last_id, age in rows:
        item = regions.setdefault(origin, {'live': 0, 'workers': 0, 'commits': 0, 'conflicts': 0,
                                           'last_ids': [], 'age': age, 'generators': []})
        item['age'] = min(item['age'], age)
        if age > STALE_AFTER_S:
            continue
        item['live'] += 1
        item['workers'] += workers
        item['commits'] += commits
        item['conflicts'] += conflicts
        if last_id is not None:
            item['last_ids'].append(last_id)
        item['generators'].append(gen)
    for origin, item in regions.items():
        item['pooled'] = region_pool(histogram_rows, origin, item['generators'], include_first_txn)
        item['p99'] = item['pooled']['p99_ms']
    return regions


NEWEST_WIDTH = 52


async def newest_order(session, candidates):
    """One PK lookup per origin; a missing fresh candidate takes precedence."""
    ids = tuple(dict.fromkeys(candidates))
    text = '-'
    if ids:
        placeholders = ', '.join('%s' for _ in ids)
        query = ('SELECT order_id, ordered_at, '
                 'extract(epoch FROM clock_timestamp() - ordered_at) AS age_s '
                 f'FROM demo_orders WHERE order_id IN ({placeholders})')
        rows = await (await session.attempt(query, ids)).fetchall()
        found = {str(row[0]) for row in rows}
        missing = sorted(str(key) for key in ids if str(key) not in found)
        if missing:
            text = f'{missing[0][:6]}... NOT FOUND'
            if len(missing) > 1:
                text += f' (+{len(missing)-1} more)'
        else:
            order_id, _, age = max(rows, key=lambda row: (row[1], str(row[0])))
            age_text = f'{age:.1f}s'
            if len(age_text) > 8:
                age_text = f'{float(age):.0e}s'
            text = f'{str(order_id)[:6]}... written {age_text:>8} ago visible here'
    # Pad to width FIRST so ANSI escapes (zero display width, non-zero len) never
    # shift alignment, THEN wrap the semantic payload: green when visible, red when missing.
    padded = f'{text:<{NEWEST_WIDTH}}'
    if text.endswith('visible here'):
        return demo_color.green(padded)
    if 'NOT FOUND' in text:
        return demo_color.red(padded)
    return padded


async def frame(session, cfg):
    rows = await (await session.attempt(LATEST)).fetchall()
    histograms = await (await session.attempt(POOL_QUERY)).fetchall()
    regions = summarize(rows, histograms, getattr(cfg, 'include_first_txn', False))
    lines, total = [], 0
    for origin in sorted(regions):
        item = regions[origin]
        if not item['live']:
            message = f'no traffic for {int(item["age"])}s'
            lines.append(f'  {origin:<12} {message:>44}')
            continue
        total += item['commits']
        seen = await newest_order(session, item['last_ids'])
        # Format each field to its fixed width FIRST, then wrap in color so the
        # zero-width escapes never disturb alignment.
        latency = f'{item["p99"]:7.1f}' if item['p99'] is not None else '      -'
        latency = demo_color.p99(latency, item['p99'])
        conflicts = f'{item["conflicts"]:>9}'
        if item['conflicts']:
            conflicts = demo_color.red(conflicts)
        lines.append(f'  {origin:<12} {item["commits"]:>9} {latency} {item["workers"]:>7} '
                     f'{conflicts}   {seen}')
    header = demo_color.bold(
        f'  {"written in":<12} {"orders/s":>9} {"p99 ms":>7} {"conns":>7} {"conflicts":>9}   newest order')
    endpoint_line = demo_color.bold(
        f'  Read through the {cfg.label} endpoint ({cfg.region})   {time.strftime("%H:%M:%S")}')
    return '\n'.join(['\033[H\033[J',
        endpoint_line, '',
        header,
        *(lines or ['  waiting for traffic...']), '', f'  {"both Regions":<12} {total:>9}'])


async def run(cfg):
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    signals = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
            signals.append(sig)
        except (NotImplementedError, RuntimeError, ValueError):
            pass
    session = Session(Database(cfg.host, cfg.region, cfg.local_test), Pacer(1))
    try:
        while not stop.is_set():
            started = time.monotonic()
            try:
                print(await frame(session, cfg), flush=True)
            except TRANSIENT as exc:
                print(f'  dashboard reconnecting: {type(exc).__name__}', flush=True)
            await pause(stop, max(.05, cfg.interval-(time.monotonic()-started)))
    finally:
        await session.close()
        for sig in signals:
            loop.remove_signal_handler(sig)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--label', required=True)
    parser.add_argument('--include-first-txn', action='store_true', help='include first successes in displayed pooled latency')
    parser.add_argument('--interval', type=float, default=1.)
    parser.add_argument('--local-test', action='store_true', help='loopback-only test DSN; no AWS clients')
    cfg = parser.parse_args(argv)
    if cfg.interval <= 0:
        parser.error('--interval must be positive')
    return cfg


def main():
    asyncio.run(run(parse_args()))


if __name__ == '__main__':
    main()
