"""One finite async load generator per Lambda invocation; no subprocesses.

Reuses the original token signing, measurement window and percentile code.
The transaction loop follows loadgen.worker, with bounded connection admission,
accurate live-connection counts, lease checks and cancellation-safe cleanup.
"""

import asyncio
import json
import math
import os
import socket
import time
import uuid
from contextlib import contextmanager
from types import SimpleNamespace

import psycopg
from psycopg import errors
from psycopg.conninfo import conninfo_to_dict

from lambda_diagnostics import Diagnostics
from latency_detail import order_transaction
from loadgen import Tokens, Window, pct
from pooled_latency import HIST_INSERT, HIST_READ

MAX_DURATION = 840.0
LEASE_SECONDS = 30
DB_TIMEOUT = 3.0
CLEANUP_SECONDS = 4.0
COMPLETION_SECONDS = 30.0
TRANSIENT = (psycopg.OperationalError, psycopg.InterfaceError, errors.SerializationFailure, TimeoutError, OSError)
STATS_INSERT = (
    "INSERT INTO demo_stats "
    "(origin, gen, workers, commits, p50_ms, p99_ms, conflicts, errors, last_order_id) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
)
LEASE_QUERY = (
    "SELECT r.stop, extract(epoch FROM r.lease_until - clock_timestamp()), s.phase, s.active "
    "FROM demo_lambda_runs r JOIN demo_lambda_schedule s USING (run_id) WHERE r.run_id = %s"
)
CLAIM = (
    "INSERT INTO demo_lambda_invocations (run_id, slot, owner, status, phase, workers, first_done, summary) "
    "VALUES (%s, %s, %s, 'running', -1, 0, 0, '{}')"
)
OWNER = "SELECT owner FROM demo_lambda_invocations WHERE run_id = %s AND slot = %s"
PROGRESS = (
    "UPDATE demo_lambda_invocations SET phase = %s, workers = %s, first_done = %s, "
    "updated_at = clock_timestamp() WHERE run_id = %s AND slot = %s AND owner = %s"
)
COMPLETE = (
    "UPDATE demo_lambda_invocations SET status = 'done', workers = 0, summary = %s, "
    "updated_at = clock_timestamp() WHERE run_id = %s AND slot = %s AND owner = %s"
)


class StatsWindow(Window):
    """Count all commits and retain a separate steady-transaction sample set."""
    def __init__(self):
        super().__init__()
        self.steady_latencies_ms = []

    def record(self, elapsed_ms, first_transaction):
        self.latencies_ms.append(elapsed_ms)
        if not first_transaction:
            self.steady_latencies_ms.append(elapsed_ms)


def window_metrics(win):
    latency = sorted(win.steady_latencies_ms)
    return len(win.latencies_ms), pct(latency, .50), pct(latency, .99)


def number(value, name, low, high):
    value = float(value)
    if not math.isfinite(value) or not low <= value <= high:
        raise ValueError(f"{name} must be between {low} and {high}")
    return value


def local_parameters():
    """An explicit local-only test switch; never usable in the Lambda runtime."""
    if os.environ.get("AWS_LAMBDA_FUNCTION_NAME"):
        raise ValueError("local_test is forbidden inside Lambda")
    dsn = os.environ.get("DSQL_DEMO_LOCAL_DSN")
    if not dsn:
        raise ValueError("local_test requires DSQL_DEMO_LOCAL_DSN")
    params = conninfo_to_dict(dsn)
    if params.get("host") not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("local test DSN must name a loopback host")
    if "hostaddr" in params or "service" in params or "servicefile" in params:
        raise ValueError("local test DSN cannot override its loopback host")
    # An explicit address also overrides inherited PGHOSTADDR/service defaults.
    params["hostaddr"] = "127.0.0.1" if params["host"] == "localhost" else params["host"]
    return params


class Database:
    def __init__(self, host, region, local_test=False, diagnostics=None):
        self.diagnostics = diagnostics
        self.local = local_parameters() if local_test else None
        self.host, self.region = host, region
        self.tokens = None if local_test else Tokens(host, region)
        self.addresses = []
        self.address_index = 0
        self.resolve_until = 0.0
        self.resolve_lock = asyncio.Lock()

    async def address(self):
        # libpq otherwise tries every resolved address inside ONE pacing permit.
        async with self.resolve_lock:
            if time.monotonic() >= self.resolve_until:
                # The demo's non-VPC Lambda functions require IPv4 egress.
                # Also filter returned rows so no alternate resolver can admit IPv6.
                try:
                    info = await asyncio.wait_for(
                        asyncio.get_running_loop().getaddrinfo(
                            self.host, 5432, family=socket.AF_INET, type=socket.SOCK_STREAM),
                        DB_TIMEOUT,
                    )
                except socket.gaierror as exc:
                    raise psycopg.OperationalError(f"IPv4 resolution failed for {self.host}: {exc}") from exc
                self.addresses = list(dict.fromkeys(row[4][0] for row in info if row[0] == socket.AF_INET))
                if not self.addresses:
                    raise psycopg.OperationalError(f"endpoint {self.host} resolved to no IPv4 addresses")
                self.resolve_until = time.monotonic() + 60
            address = self.addresses[self.address_index % len(self.addresses)]
            self.address_index += 1
            return address

    async def connect(self):
        started, params = time.perf_counter(), {}
        if self.diagnostics:
            self.diagnostics.connect_attempts += 1
        try:
            if self.local is not None:
                params = dict(self.local)
            else:
                params = {"host": self.host, "hostaddr": await self.address(), "port": 5432,
                          "user": "admin", "dbname": "postgres", "sslmode": "require"}
                signed_at = time.perf_counter()
                params["password"] = self.tokens.get()
                if self.diagnostics:
                    self.diagnostics.measure('token_sign', (time.perf_counter()-signed_at)*1000)
            params.update(autocommit=True, connect_timeout=3, target_session_attrs="any", gssencmode="disable")
            conn = await psycopg.AsyncConnection.connect(**params)
        except Exception as exc:
            elapsed = (time.perf_counter()-started)*1000
            exc.demo_context = {'hostaddr': params.get('hostaddr'), 'connect_ms': round(elapsed, 3)}
            if self.diagnostics:
                self.diagnostics.measure('connect_error', elapsed)
            raise
        if self.diagnostics:
            self.diagnostics.measure('connect', (time.perf_counter()-started)*1000)
        return conn


def abort_socket(conn):
    """PQfinish closes this socket; it never opens a CancelRequest connection."""
    conn.pgconn.finish()


@contextmanager
def socket_deadline(conn, seconds=DB_TIMEOUT):
    timer = asyncio.get_running_loop().call_later(seconds, abort_socket, conn)
    try:
        yield
    finally:
        timer.cancel()  # a timer handle, never an in-flight query task


async def execute(conn, sql, args=None):
    with socket_deadline(conn):
        return await conn.execute(sql, args)


async def pause(stop, seconds):
    """Only event waits are cancelled; psycopg awaits are never cancelled."""
    try:
        await asyncio.wait_for(stop.wait(), max(.001, seconds))
    except TimeoutError:
        pass


class Pacer:
    def __init__(self, rate):
        self.rate = rate
        self.last = -math.inf
        self.lock = asyncio.Lock()

    def set_rate(self, rate):
        self.rate = rate

    async def wait(self, stop=None, deadline=math.inf):
        # FIFO admission: queued workers sleep instead of polling the whole queue.
        # Administrative reconnects have their own pacer.
        async with self.lock:
            while time.monotonic() < deadline and not (stop and stop.is_set()):
                delay = self.last + 1 / self.rate - time.monotonic()
                if delay <= 0:
                    self.last = time.monotonic()
                    return True
                await asyncio.sleep(min(.05, max(.001, delay)))
        return False



class Session:
    """One administrative connection; failed attempts re-enter its pacer."""
    def __init__(self, db, pacer, initial=None, phase="stats"):
        self.db, self.pacer, self.initial = db, pacer, initial
        self.phase = phase
        self.conn = None

    async def attempt(self, sql, args=None, deadline=math.inf):
        try:
            if self.conn is None:
                pacer, self.initial = self.initial or self.pacer, None
                if not await pacer.wait(deadline=deadline):
                    raise TimeoutError("connection admission deadline")
                self.conn = await self.db.connect()
            return await execute(self.conn, sql, args)
        except Exception as exc:
            if self.db.diagnostics:
                phase = 'heartbeat' if sql == LEASE_QUERY else self.phase
                self.db.diagnostics.error(phase, exc, operation=sql.split()[0])
            if isinstance(exc, TRANSIENT):
                await self.close()
            raise

    async def close(self):
        if self.conn is not None:
            await self.conn.close()
            self.conn = None


async def persist_histograms(session, diagnostics, origin, gen, published, deadline):
    diagnostics.progress()  # seal only completed UTC seconds, including empty ones
    for second, row in diagnostics.closed_seconds.items():
        if second in published:
            continue
        if time.monotonic() >= deadline:
            raise TimeoutError('histogram publication deadline')
        text = json.dumps(row['bins_1ms'], separators=(',', ':'))
        first_text = json.dumps(row['first_bins_1ms'], separators=(',', ':'))
        args = (origin, gen, second, text, first_text)
        try:
            await session.attempt(HIST_INSERT, args, deadline)
        except errors.UniqueViolation:
            # Resolve an INSERT committed before its acknowledgement was lost.
            saved = await (await session.attempt(HIST_READ, args[:3], deadline)).fetchone()
            if not saved or tuple(saved) != (text, first_text):
                raise ValueError('conflicting immutable histogram row') from None
        published.add(second)


async def worker(cfg, db, win_ref, live, first_done, initial, retry, stop, worker_id=0):
    conn, first = None, True
    phase, t0, attempt, first_txn = "connect", None, 0, True
    worker_first_success = True
    try:
        while not stop.is_set():
            try:
                t0 = None
                if conn is None:
                    phase = "connect"
                    if not await (initial if first else retry).wait(stop):
                        break
                    was_first, first = first, False
                    try:
                        attempt += 1
                        conn = await db.connect()
                        first_txn = True
                        live.add(conn)
                    finally:
                        if was_first:
                            first_done[0] += 1
                if stop.is_set():
                    break
                phase = "first_txn" if first_txn else "txn"
                order_id = uuid.uuid4()
                t0 = time.perf_counter()
                # Covers BEGIN, statements and COMMIT, including transaction exit.
                with socket_deadline(conn):
                    await order_transaction(conn, order_id, cfg.origin, cfg.hot)
                elapsed = (time.perf_counter() - t0) * 1000
                if db.diagnostics:
                    db.diagnostics.measure(phase, elapsed)
                    db.diagnostics.transaction(elapsed, first_transaction=first_txn)
                    if worker_first_success:
                        db.diagnostics.measure('worker_first_txn', elapsed)
                worker_first_success = False
                win_ref[0].record(elapsed, first_txn)
                first_txn = False
                win_ref[0].last_order_id = order_id
            except errors.SerializationFailure as exc:
                win_ref[0].conflicts += 1
                if db.diagnostics:
                    db.diagnostics.error(phase, exc, worker=worker_id, connect_attempt=attempt)
                    if t0 is not None:
                        db.diagnostics.measure(phase + '_conflict', (time.perf_counter()-t0)*1000)
            except TRANSIENT as exc:
                win_ref[0].errors += 1
                if db.diagnostics:
                    db.diagnostics.error(phase, exc, worker=worker_id, connect_attempt=attempt)
                    if t0 is not None:
                        db.diagnostics.measure(phase + '_error', (time.perf_counter()-t0)*1000)
                if conn is not None:
                    live.discard(conn)
                    await conn.close()
                    conn = None
                await pause(stop, .2)
            except Exception as exc:
                if db.diagnostics:
                    db.diagnostics.error(phase, exc, worker=worker_id, connect_attempt=attempt)
                raise
            if cfg.think_ms:
                await pause(stop, cfg.think_ms / 1000)
    finally:
        if conn is not None:
            await conn.close()
            live.discard(conn)


async def run(event, context=None):
    entered = time.monotonic()
    n = number(event.get("connections", 250), "connections", 1, 500)
    if n != int(n):
        raise ValueError("connections must be an integer")
    connections = int(n)
    duration = number(event.get("duration", 300), "duration", 1, MAX_DURATION)
    if context is not None:
        duration = min(duration, context.get_remaining_time_in_millis() / 1000 - 45)
    run_id = str(uuid.UUID(event["run_id"]))
    slot = str(event.get("slot", f"{event.get('step', 0)}-{event.get('index', 0)}"))
    owner = getattr(context, "aws_request_id", None) or str(uuid.uuid4())
    gen = f"{run_id}:{slot}:{owner}"
    cfg = SimpleNamespace(origin=event["origin"], hot=bool(event.get("hot", False)),
                          think_ms=number(event.get("think_ms", 250), "think_ms", 0, 60000))
    first_rate = number(event.get("open_rate", 20), "open_rate", .001, 90)
    retry_rate = number(event.get("retry_rate", 2), "retry_rate", .001, 90)
    admin_rate = number(event.get("admin_rate", 1), "admin_rate", .001, 90)
    recovery_budget = number(event.get("recovery_budget", 20), "recovery_budget", .001, 90)
    result = {"gen": gen, "slot": slot, "run_id": run_id, "commits": 0, "conflicts": 0,
              "errors": 0, "reports": 0, "cold_start": event.get("_cold_start"), "reason": "duration", "open_connections_after_cleanup": 0}
    if duration <= 0:
        result["reason"] = "no time remaining"
        return result
    diagnostics = Diagnostics(gen)
    db = Database(event["host"], event["region"], event.get("local_test", False), diagnostics)
    initial, retry = Pacer(first_rate), Pacer(retry_rate)
    stats = Session(db, Pacer(admin_rate), initial)
    win_ref, live, first_done, tasks = [StatsWindow()], set(), [0], []
    stop = asyncio.Event()
    deadline = entered + duration  # includes setup; no laptop wall clock
    lease_deadline = [min(deadline, time.monotonic() + LEASE_SECONDS)]
    claimed, watchdog = False, None
    last_report = time.monotonic()
    published = set()
    lag_task = asyncio.create_task(diagnostics.ticker(stop))

    async def watch_lease():
        while not stop.is_set():
            if time.monotonic() >= min(deadline, lease_deadline[0]):
                result["reason"] = "duration" if time.monotonic() >= deadline else "lease expired"
                stop.set()  # finish the transaction; no Task.cancel or CancelRequest
                return
            await pause(stop, .05)

    try:
        # The logical-slot claim makes a duplicate delivery unable to start load.
        # An ambiguous INSERT acknowledgement is resolved by reading its owner.
        while time.monotonic() < lease_deadline[0] and not claimed:
            try:
                try:
                    await stats.attempt(CLAIM, (run_id, slot, owner), lease_deadline[0])
                    claimed = True
                except (errors.UniqueViolation, errors.SerializationFailure):
                    row = await (await stats.attempt(OWNER, (run_id, slot), lease_deadline[0])).fetchone()
                    if row and row[0] != owner:
                        result["reason"] = "duplicate delivery rejected"
                        return result
                    claimed = bool(row)
            except TRANSIENT:
                result["errors"] += 1
                await asyncio.sleep(.2)
        if not claimed:
            result["error"] = "could not claim invocation before startup deadline"
            return result
        watchdog = asyncio.create_task(watch_lease())
        while not stop.is_set():
            for task in tasks:
                if task.done():
                    task.result()  # fatal schema/permission errors are reported in completion
            try:
                query_started = time.monotonic()
                row = await (await stats.attempt(LEASE_QUERY, (run_id,), lease_deadline[0])).fetchone()
                if not row or row[0] or float(row[1]) <= 0:
                    result["reason"] = "stop flag or lease expired"
                    stop.set()
                    break
                lease_deadline[0] = min(deadline, query_started + float(row[1]))
                phase, active = int(row[2]), max(1, int(row[3]))
                retry.set_rate(recovery_budget / active if phase % 2 else retry_rate)
                if not tasks and not stop.is_set():
                    diagnostics.ready_after_ms = round((time.monotonic()-entered)*1000, 3)
                    tasks = [asyncio.create_task(worker(cfg, db, win_ref, live, first_done, initial, retry, stop, i))
                             for i in range(connections)]
                await stats.attempt(PROGRESS, (phase, len(live), first_done[0], run_id, slot, owner), lease_deadline[0])
                await persist_histograms(stats, diagnostics, cfg.origin, gen, published, lease_deadline[0])
                now = time.monotonic()
                if now - last_report >= .95:
                    elapsed = now - last_report
                    diagnostics.window(len(live))
                    win, win_ref[0] = win_ref[0], StatsWindow()
                    commits, p50, p99 = window_metrics(win)
                    result["commits"] += commits
                    result["conflicts"] += win.conflicts
                    result["errors"] += win.errors
                    last_report = now
                    await stats.attempt(STATS_INSERT, (
                        cfg.origin, gen, len(live), round(commits / elapsed),
                        p50, p99, round(win.conflicts / elapsed),
                        round(win.errors / elapsed), win.last_order_id,
                    ), lease_deadline[0])
                    result["reports"] += 1
            except TRANSIENT:
                result["errors"] += 1
            await pause(stop, .2)
    except Exception as exc:  # noqa: BLE001 -- persist fatal outcomes for the driver
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        stop.set()
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=CLEANUP_SECONDS)
            if pending:
                # Grace expired: close sockets, never cancel their waiting tasks.
                for conn in list(live):
                    abort_socket(conn)
            outcomes = await asyncio.gather(*tasks, return_exceptions=True)
            for outcome in outcomes:
                if isinstance(outcome, Exception):
                    result.setdefault("error", f"{type(outcome).__name__}: {outcome}")
        if watchdog:
            await watchdog
        await lag_task
        diagnostics.window(len(live))
        win = win_ref[0]
        result["commits"] += len(win.latencies_ms)
        result["conflicts"] += win.conflicts
        result["errors"] += win.errors
        result["open_connections_after_cleanup"] = len(live)
        if claimed:
            finish_by = time.monotonic() + COMPLETION_SECONDS
            # Close the last partial second before publishing it. No workers remain.
            await asyncio.sleep(max(0, math.floor(time.time()) + 1 - time.time()) + .002)
            histogram_deadline = min(finish_by - 5, time.monotonic() + 5)
            while time.monotonic() < histogram_deadline:
                try:
                    await persist_histograms(stats, diagnostics, cfg.origin, gen, published, histogram_deadline)
                    break
                except TRANSIENT:
                    result['errors'] += 1
                    await asyncio.sleep(.2)
                except (psycopg.Error, ValueError) as exc:
                    result.setdefault('error', f'{type(exc).__name__}: {exc}')
                    break
            result['histograms_published'] = len(published)
            result['histograms_pending'] = len(diagnostics.closed_seconds.keys() - published)
            # Final dashboard stats are best effort. They must never prevent the
            # separate completion record from reporting a fatal stats-path error.
            try:
                await stats.attempt(STATS_INSERT, (cfg.origin, gen, 0, 0, None, None, 0, 0, None), finish_by)
            except TRANSIENT:
                result['errors'] += 1
            except psycopg.Error as exc:
                result.setdefault('error', f'{type(exc).__name__}: {exc}')
            while time.monotonic() < finish_by:
                try:
                    result.update(diagnostics.summary())
                    await stats.attempt(COMPLETE, (json.dumps(result), run_id, slot, owner), finish_by)
                    result['completion_persisted'] = True
                    break
                except TRANSIENT:
                    await asyncio.sleep(.2)
                except psycopg.Error as exc:
                    result.setdefault('error', f'{type(exc).__name__}: {exc}')
                    break
        await stats.close()
        result.update(diagnostics.summary())
    return result


_cold = True


def handler(event, context):
    global _cold
    cold, _cold = _cold, False
    # Cold identifies the first handler in this interpreter; Init Duration remains
    # in Lambda's REPORT log and is outside transaction latency measurements.
    payload = dict(event, _cold_start=cold)
    return asyncio.run(run(payload, context))
