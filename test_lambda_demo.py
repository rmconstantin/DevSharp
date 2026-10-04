"""Credential-free behavioral tests using an in-memory SQL transport stub.

This is NOT PostgreSQL or DSQL verification. It exercises the actual handler,
driver, worker lifecycle and unchanged watcher query with deterministic storage.
Run: python -B test_lambda_demo.py
"""

import asyncio
import errno
import io
import itertools
import json
import math
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from contextlib import ExitStack, redirect_stdout
from pathlib import Path
from unittest.mock import AsyncMock, patch

import boto3
import psycopg

import drive
import loadgen_lambda as app
import watch
import watch_v2
from pooled_latency import HIST_INSERT, HIST_READ, POOL_QUERY


class Cursor:
    def __init__(self, rows=()):
        self.rows = list(rows)

    async def fetchone(self):
        return self.rows[0] if self.rows else None

    async def fetchall(self):
        return self.rows


class Transaction:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        self.conn.store.transactions_started += 1
        await self.conn.delay(self.conn.store.transaction_delay)

    async def __aexit__(self, *_):
        await self.conn.delay(.001)


class Store:
    def __init__(self):
        self.lock = threading.Lock()
        self.runs, self.schedules, self.invocations = {}, {}, {}
        self.stats, self.orders, self.connections, self.opens = [], {}, [], []
        self.max_open, self.hot_updates, self.connect_calls = 0, 0, 0
        self.fail_connect_at, self.fail_order = None, False
        self.driver_rows, self.connect_params = [], []
        self.inject_worker_dip, self.live_samples, self.delay_stats = False, 0, 0
        self.fail_next, self.heartbeat_times = {}, []
        self.transaction_delay, self.transactions_started, self.cancelled = .001, 0, 0
        self.delay_latest = 0
        self.latest_stalls = []
        self.lost_ack = set()
        self.fail_final_stats = False
        self.histograms = {}
        self.histogram_insert_calls = 0
        self.lose_histogram_ack = False
        self.recovery_scenario = False
        self.scenario_fault = False

    async def connect(self, **params):
        await asyncio.sleep(.001)
        with self.lock:
            self.connect_calls += 1
            self.connect_params.append(params)
            self.opens.append(time.monotonic())
            if self.connect_calls == self.fail_connect_at:
                raise psycopg.OperationalError('injected connection failure')
            conn = Connection(self)
            self.connections.append(conn)
            self.max_open = max(self.max_open, sum(not c.closed for c in self.connections))
            return conn

    def seed_run(self, run_id, lease=30, stopped=False):
        self.runs[run_id] = (stopped, time.monotonic() + lease)
        self.schedules.setdefault(run_id, (0, 1))


class Connection:
    def __init__(self, store):
        self.store, self.closed, self.role = store, False, None
        self.pgconn = self

    def finish(self):
        self.closed = True

    async def close(self):
        self.closed = True

    async def delay(self, seconds):
        until = time.monotonic() + seconds
        try:
            while time.monotonic() < until:
                if self.closed:
                    raise psycopg.OperationalError('socket closed')
                await asyncio.sleep(min(.01, max(0, until-time.monotonic())))
        except asyncio.CancelledError:
            self.store.cancelled += 1
            raise
        if self.closed:
            raise psycopg.OperationalError('socket closed')

    def transaction(self):
        self.role = 'worker'
        return Transaction(self)

    async def execute(self, sql, args=None):
        await self.delay(.001)
        s = self.store
        if sql == app.STATS_INSERT and args[2] == 0 and s.fail_final_stats:
            raise psycopg.errors.InsufficientPrivilege('injected final stats permission error')
        if sql == drive.LATEST and s.recovery_scenario and s.live_samples >= 3 and not s.scenario_fault:
            s.scenario_fault = True
            raise psycopg.OperationalError('monitor failed after a recovered dip')
        if s.fail_next.get(sql, 0):
            s.fail_next[sql] -= 1
            raise psycopg.OperationalError('injected transient administrative error')
        if sql == app.STATS_INSERT and s.delay_stats:
            delay, s.delay_stats = s.delay_stats, 0
            await self.delay(delay)
        if sql == drive.LATEST and s.delay_latest:
            delay, s.delay_latest = s.delay_latest, 0
            began = time.monotonic()
            try:
                await self.delay(delay)
            finally:
                s.latest_stalls.append((began, time.monotonic()))
        now = time.monotonic()
        with s.lock:
            if sql.startswith('INSERT INTO demo_lambda_runs'):
                if args[0] in s.runs:
                    raise psycopg.errors.UniqueViolation('run exists')
                s.runs[args[0]] = (False, now + app.LEASE_SECONDS)
                if 'runs' in s.lost_ack:
                    s.lost_ack.remove('runs')
                    raise psycopg.OperationalError('lost run INSERT acknowledgement')
            elif sql.startswith('INSERT INTO demo_lambda_schedule'):
                if args[0] in s.schedules:
                    raise psycopg.errors.UniqueViolation('schedule exists')
                s.schedules[args[0]] = (0, 0)
                if 'schedule' in s.lost_ack:
                    s.lost_ack.remove('schedule')
                    raise psycopg.OperationalError('lost schedule INSERT acknowledgement')
            elif sql == drive.RUN_EXISTS:
                return Cursor([(s.runs[args[0]][0],)] if args[0] in s.runs else [])
            elif sql == drive.SCHEDULE_EXISTS:
                return Cursor([s.schedules[args[0]]] if args[0] in s.schedules else [])
            elif sql == drive.SCHEDULE:
                s.schedules[args[2]] = (args[0], args[1])
            elif sql == drive.HEARTBEAT:
                stop, _ = s.runs[args[0]]
                s.heartbeat_times.append(now)
                if not stop:
                    s.runs[args[0]] = (False, now + app.LEASE_SECONDS)
            elif sql == drive.STOP:
                s.runs[args[0]] = (True, now)
            elif sql == app.LEASE_QUERY:
                row, schedule = s.runs.get(args[0]), s.schedules.get(args[0])
                return Cursor([(row[0], row[1]-now, *schedule)] if row and schedule else [])
            elif sql == app.CLAIM:
                run_id, slot, owner = args
                key = (run_id, slot)
                if key in s.invocations:
                    raise psycopg.errors.UniqueViolation('duplicate slot')
                s.invocations[key] = [slot, owner, 'running', -1, 0, 0, '{}', now]
            elif sql == app.OWNER:
                row = s.invocations.get(tuple(args))
                return Cursor([(row[1],)] if row else [])
            elif sql == app.PROGRESS:
                phase, workers, first_done, run_id, slot, owner = args
                row = s.invocations[(run_id, slot)]
                if row[1] == owner:
                    row[3:6], row[7] = [phase, workers, first_done], now
            elif sql == app.COMPLETE:
                summary, run_id, slot, owner = args
                row = s.invocations[(run_id, slot)]
                if row[1] == owner:
                    row[2], row[4], row[6], row[7] = 'done', 0, summary, now
            elif sql == drive.INVOCATIONS:
                return Cursor([(*row[:7], now-row[7]) for (run_id, _), row in s.invocations.items() if run_id == args[0]])
            elif sql == HIST_INSERT:
                s.histogram_insert_calls += 1
                key = tuple(args[:3])
                if key in s.histograms:
                    raise psycopg.errors.UniqueViolation('duplicate immutable histogram')
                s.histograms[key] = tuple(args[3:5])
                if s.lose_histogram_ack:
                    s.lose_histogram_ack = False
                    raise psycopg.OperationalError('lost histogram INSERT acknowledgement')
            elif sql == HIST_READ:
                return Cursor([s.histograms[tuple(args)]] if tuple(args) in s.histograms else [])
            elif sql == POOL_QUERY:
                second = int(time.time()) - 2
                return Cursor([(second, origin, gen, *texts) for (origin, gen, stamp), texts in s.histograms.items()
                               if stamp == second] or [(second, None, None, None, None)])
            elif sql == app.STATS_INSERT:
                s.stats.append((*args, now))
            elif sql.startswith('INSERT INTO demo_orders'):
                if s.fail_order:
                    raise psycopg.errors.UndefinedTable('injected missing table')
                s.orders[args[0]] = args
            elif sql.startswith('UPDATE demo_hot_counter'):
                s.hot_updates += 1
            elif sql in (drive.LATEST, watch.LATEST, watch_v2.LATEST):
                latest = {}
                for row in s.stats:
                    if row[-1] > now - 30:
                        latest[(row[0], row[1])] = row
                if sql == drive.LATEST:
                    rows = [(r[1], *r[2:8], r[-1], now-r[-1]) for r in latest.values()
                            if r[0] == args[0] and r[1].startswith(args[1][:-1])]
                    if any(row[1] > 0 for row in rows):
                        s.live_samples += 1
                        if s.inject_worker_dip and (s.live_samples == 2 or (s.recovery_scenario and s.live_samples == 4)):
                            rows = [(r[0], 0, *r[2:]) for r in rows]
                    s.driver_rows.append((now, drive.aggregate(rows)[0]))
                elif sql == watch_v2.LATEST:
                    rows = [(r[0], r[1], r[2], r[3], r[5], r[6], r[8], now-r[-1]) for r in latest.values()]
                else:
                    rows = [(r[0], r[2], r[3], r[5], r[6], r[8], now-r[-1]) for r in latest.values()]
                return Cursor(rows)
            elif sql.startswith('SELECT order_id, ordered_at,'):
                return Cursor([(key, now, .25) for key in args if key in s.orders])
            elif sql == watch.FOUND:
                return Cursor([(1,)] if args[0] in s.orders else [])
            else:
                raise AssertionError(f'SQL not implemented: {sql}')
        return Cursor()


class Tests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.s = Store()
        self.stack = ExitStack()
        self.stack.enter_context(patch.dict(os.environ, {"DSQL_DEMO_LOCAL_DSN": "host=127.0.0.1 dbname=postgres user=admin sslmode=disable"}))
        self.stack.enter_context(patch.object(boto3, "client", side_effect=AssertionError("AWS access forbidden in tests")))
        self.stack.enter_context(patch.object(boto3, "setup_default_session", side_effect=AssertionError("AWS credentials forbidden in tests")))
        self.stack.enter_context(patch.object(psycopg.AsyncConnection, "connect", side_effect=self.s.connect))
        self.addCleanup(self.stack.close)
        temp = self.stack.enter_context(tempfile.TemporaryDirectory(prefix='lambda-tests-'))
        self.logs = Path(temp)
        original_parse = drive.parse_args
        def parse(argv=None):
            cfg = original_parse(argv)
            cfg.log_file = str(self.logs / f'{uuid.uuid4()}.log')
            return cfg
        self.stack.enter_context(patch.object(drive, 'parse_args', side_effect=parse))

    def event(self, **kwargs):
        run_id = str(uuid.uuid4())
        self.s.seed_run(run_id)
        return dict(host="unused", region="local", origin="Test", run_id=run_id,
                    local_test=True, connections=3, open_rate=30, retry_rate=3,
                    think_ms=25, duration=10, **kwargs)

    async def test_handler_ten_seconds_and_unchanged_watcher(self):
        event = self.event()
        other = dict(event, index=1, connections=2)
        # Call the actual sync Lambda entry point in independent threads.
        jobs = [asyncio.create_task(asyncio.to_thread(app.handler, e, None)) for e in (event, other)]
        await asyncio.sleep(4)
        query_conn = Connection(self.s)
        rows = await (await query_conn.execute(watch.LATEST)).fetchall()
        live = [r for r in rows if r[-1] <= watch.STALE_AFTER_S]
        self.assertEqual(sum(r[1] for r in live), 5)
        self.assertEqual(len(live), 2)
        self.assertTrue(all(r[0] == "Test" and r[2] > 0 for r in live))
        for row in live:
            self.assertIsNotNone(await (await query_conn.execute(watch.FOUND, (row[5],))).fetchone())
        print(f"WATCH QUERY (unchanged): generators={len(live)}, workers={sum(r[1] for r in live)}, commits/s={sum(r[2] for r in live)}; both last_order_id lookups found")
        results = await asyncio.gather(*jobs)
        self.assertNotEqual(results[0]["gen"], results[1]["gen"])
        self.assertTrue(all(r["reports"] >= 8 and r["commits"] > 0 and r["open_connections_after_cleanup"] == 0 for r in results))
        self.assertTrue(all(c.closed for c in self.s.connections))
        print(f"HANDLER 10s: demo_stats rows={len(self.s.stats)}, orders={len(self.s.orders)}, summaries={results}")

    async def test_driver_ramp_keeps_early_workers(self):
        cfg = drive.parse_args(["--function", "local", "--region", "local", "--host", "unused", "--origin", "Test",
                                "--ramp", "2,4", "--step-seconds", "1", "--hold-seconds", "1",
                                "--conns-per-invocation", "2", "--think-ms", "25", "--local-test"])
        result = await drive.drive(cfg)
        self.assertEqual(result["invocations"], 2)
        self.assertEqual(self.s.max_open, 8)  # two driver connections + 2 stats + 4 workers
        self.assertTrue(result["success"])
        self.assertEqual(max(n for _, n in self.s.driver_rows), 4)
        self.assertTrue(all(b[0] - a[0] >= .9 for a, b in itertools.pairwise(self.s.driver_rows)))
        self.assertTrue(all(c.closed for c in self.s.connections))
        gens = {r[1] for r in self.s.stats}
        self.assertEqual(len(gens), 2)
        first, second = sorted(gens, key=lambda g: int(g.split(":")[1].split("-")[0]))
        second_start = min(r[-1] for r in self.s.stats if r[1] == second and r[2] > 0)
        self.assertTrue(any(r[1] == first and r[2] == 2 and r[-1] >= second_start for r in self.s.stats))
        print("RAMP: early invocation stayed connected during later step; target 4 reached; all connections closed")

    async def test_real_sigint_stops_driver_and_invocations(self):
        cfg = drive.parse_args(["--function", "local", "--region", "local", "--host", "unused", "--origin", "Test",
                                "--ramp", "3", "--step-seconds", "20", "--think-ms", "25", "--local-test"])
        timer = asyncio.get_running_loop().call_later(3, os.kill, os.getpid(), signal.SIGINT)
        t0 = time.monotonic()
        output = io.StringIO()
        cfg.hot = True
        try:
            with redirect_stdout(output):
                result = await drive.drive(cfg)
        finally:
            timer.cancel()
        self.assertTrue(result["success"])
        self.assertIn("HOT ROW", output.getvalue())
        self.assertIn("pooled p50/p99", output.getvalue())
        self.assertIn("first txn per connection excluded", output.getvalue())
        self.assertEqual(len(output.getvalue().split("  stopping;", 1)[1].splitlines()), 1)
        self.assertIn("database completion", Path(result["log_file"]).read_text())
        elapsed = time.monotonic() - t0
        self.assertLess(elapsed, 6)
        self.assertTrue(all(stop for stop, _ in self.s.runs.values()))
        self.assertTrue(all(c.closed for c in self.s.connections))
        print(f"CTRL-C: actual SIGINT at 3s; driver and workers returned at {elapsed:.2f}s")

    async def test_expired_lease_and_late_invocation(self):
        event = self.event()
        self.s.seed_run(event["run_id"], lease=1.5)
        t0 = time.monotonic()
        result = await asyncio.to_thread(app.handler, event, None)
        self.assertLess(time.monotonic()-t0, 4)
        self.assertIn(result["reason"], ("stop flag or lease expired", "lease expired"))
        count = len(self.s.orders)
        result = await asyncio.to_thread(app.handler, event, None)
        self.assertEqual(result["reason"], "duplicate delivery rejected")
        self.assertEqual(len(self.s.orders), count)
        self.assertTrue(all(c.closed for c in self.s.connections))
        print("LEASE: lost heartbeat stops work; a late invocation opens no load connections")

    async def test_stopping_one_run_leaves_other_origin_running(self):
        first = self.event()
        second = self.event()
        second["origin"] = "Other"
        first["duration"] = second["duration"] = 5
        jobs = [asyncio.create_task(asyncio.to_thread(app.handler, e, None)) for e in (first, second)]
        await asyncio.sleep(2)
        self.s.seed_run(first["run_id"], stopped=True)
        first_result = await jobs[0]
        self.assertIn(first_result["reason"], ("stop flag or lease expired", "lease expired"))
        self.assertFalse(jobs[1].done())
        with self.s.lock:
            before = sum(order[3] == "Other" for order in self.s.orders.values())
        await asyncio.sleep(1)
        with self.s.lock:
            after = sum(order[3] == "Other" for order in self.s.orders.values())
        self.assertGreater(after, before)
        await jobs[1]
        self.assertTrue(all(c.closed for c in self.s.connections))
        print("ISOLATION: stopping one run leaves the other origin writing")

    async def test_lambda_context_deadline_and_expired_payload(self):
        class Context:
            aws_request_id = "local-context-id"

            def get_remaining_time_in_millis(self):
                return 47000

        event = self.event()
        t0 = time.monotonic()
        result = await asyncio.to_thread(app.handler, event, Context())
        self.assertLess(time.monotonic() - t0, 4)
        self.assertIn("local-context-id", result["gen"])
        self.assertEqual(result['reason'], 'duration')
        self.assertNotIn('expires_at', event)
        print('RELATIVE DURATION: context bounds the run without a laptop timestamp')

    async def test_connection_failure_recovery_and_hot_mode(self):
        event = self.event(hot=True)
        event["duration"] = 3
        self.s.fail_connect_at = 3
        result = await asyncio.to_thread(app.handler, event, None)
        self.assertGreater(result["errors"], 0)
        self.assertGreater(self.s.hot_updates, 0)
        self.assertEqual(result["open_connections_after_cleanup"], 0)
        self.assertTrue(any(row[2] == 3 for row in self.s.stats))
        print("RECOVERY: injected open failure recovered; HOT branch executed; stats count successful connections")

    async def test_fatal_worker_error_closes_connections(self):
        event = self.event()
        self.s.fail_order = True
        result = await asyncio.to_thread(app.handler, event, None)
        self.assertIn("UndefinedTable", result["error"])
        self.assertTrue(result["completion_persisted"])
        self.assertTrue(all(c.closed for c in self.s.connections))

    async def test_pacer_does_not_catch_up(self):
        pacer = app.Pacer(20)
        await pacer.wait()
        await asyncio.sleep(.2)
        stamps = []
        for _ in range(3):
            await pacer.wait()
            stamps.append(time.monotonic())
        self.assertTrue(all(b-a >= .045 for a, b in itertools.pairwise(stamps)))

    async def test_hold_restarts_after_connection_dip(self):
        cfg = drive.parse_args(["--function", "local", "--region", "local", "--host", "unused", "--origin", "Test",
                                "--ramp", "2", "--step-seconds", "2", "--think-ms", "25", "--local-test"])
        self.s.inject_worker_dip = True
        result = await drive.drive(cfg)
        self.assertTrue(result["success"])
        samples = [n for _, n in self.s.driver_rows]
        first_live = samples.index(2)
        self.assertEqual(samples[first_live:first_live+3], [2, 0, 2])
        recovered_tick = self.s.driver_rows[first_live+2][0]
        self.assertGreaterEqual(self.s.driver_rows[-1][0] - recovered_tick, cfg.step_seconds - .03)
        self.assertTrue(all(c.closed for c in self.s.connections))
        print("HOLD REGRESSION: a sampled connection dip restarts the hold after recovery")

    async def test_dropped_invoke_socket_does_not_stop_take(self):
        threads = []
        def drop_response(client, function, payload, local):
            thread = threading.Thread(target=app.handler, args=(payload, None))
            threads.append(thread)
            thread.start()
            time.sleep(.5)
            raise ConnectionError('injected dropped Invoke socket while Lambda remains healthy')

        cfg = drive.parse_args(['--function', 'local', '--region', 'local', '--host', 'unused', '--origin', 'Test',
                                '--ramp', '2,4', '--conns-per-invocation', '2', '--step-seconds', '1', '--local-test'])
        with patch.object(drive, 'invoke', side_effect=drop_response):
            result = await drive.drive(cfg)
            for thread in threads:
                thread.join(2)
                self.assertFalse(thread.is_alive())
        self.assertTrue(result['success'], result)
        self.assertEqual(result['completed'], 2)
        self.assertEqual(max(n for _, n in self.s.driver_rows), 4)
        self.assertIn('dropped Invoke socket', Path(result['log_file']).read_text())
        print('H1: two dropped Invoke responses; ramp reached four workers; DB confirmed both completions')

    async def test_local_dsn_environment_and_duplicate_timeout(self):
        with patch.dict(os.environ, {
            "PGHOSTADDR": "192.0.2.1",
            "DSQL_DEMO_LOCAL_DSN": "host=localhost dbname=postgres connect_timeout=99",
        }):
            db = app.Database("unused", "local", local_test=True)
            conn = await db.connect()
            self.assertEqual(self.s.connect_params[-1]["hostaddr"], "127.0.0.1")
            self.assertEqual(self.s.connect_params[-1]["connect_timeout"], 3)
            await conn.close()
        print("DSN REGRESSION: loopback hostaddr overrides environment; timeout supplied exactly once")

    async def test_each_permit_uses_one_resolved_address(self):
        loop = asyncio.get_running_loop()
        addresses = [(2, 1, 6, "", ("192.0.2.10", 5432)), (2, 1, 6, "", ("192.0.2.11", 5432))]
        with patch.object(loop, "getaddrinfo", AsyncMock(return_value=addresses)) as resolver, patch.object(app, "Tokens"):
            db = app.Database("example.invalid", "local")
            first = await db.connect()
            second = await db.connect()
            self.assertEqual([p["hostaddr"] for p in self.s.connect_params], ["192.0.2.10", "192.0.2.11"])
            self.assertTrue(all(p["host"] == "example.invalid" for p in self.s.connect_params))
            self.assertEqual(resolver.await_count, 1)
            await first.close()
            await second.close()
        print("DNS REGRESSION: one address per connection permit; fallback needs a separately paced call")

    async def test_ipv4_only_resolution_rejects_unreachable_ipv6(self):
        loop = asyncio.get_running_loop()
        v4a = (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.0.2.10', 5432))
        v4b = (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.0.2.11', 5432))
        v6 = (socket.AF_INET6, socket.SOCK_STREAM, 6, '', ('2001:db8::10', 5432, 0, 0))
        attempts, failures, permits = [], [], []

        async def transport(**params):
            attempts.append(params['hostaddr'])
            if ':' in params['hostaddr']:
                try:
                    raise OSError(errno.EADDRNOTAVAIL, 'Cannot assign requested address')
                except OSError as exc:
                    raise psycopg.OperationalError(f"IPv6 {params['hostaddr']}: {exc}") from exc
            return await self.s.connect(**params)

        pacer = app.Pacer(100)
        with patch.object(loop, 'getaddrinfo', AsyncMock(return_value=[v4a, v6, v4b, v4a])) as resolver, \
                patch.object(app, 'Tokens'), patch.object(psycopg.AsyncConnection, 'connect', side_effect=transport):
            db = app.Database('example.invalid', 'local')
            for _ in range(4):
                self.assertTrue(await pacer.wait())
                permits.append(time.monotonic())
                try:
                    conn = await db.connect()
                    await conn.close()
                except psycopg.OperationalError as exc:
                    failures.append(str(exc))
            print(f'IPV4 REGRESSION: permits={len(permits)}, attempts={attempts}, failures={failures}', flush=True)
            self.assertEqual(failures, [], 'mixed DNS results must never cause an IPv6 dial')
            self.assertEqual(attempts, ['192.0.2.10', '192.0.2.11'] * 2)
            self.assertEqual(len(attempts), len(permits))
            self.assertTrue(all(b-a >= .009 for a, b in itertools.pairwise(permits)))
            self.assertTrue(all(p['host'] == 'example.invalid' for p in self.s.connect_params))
            resolver.assert_awaited_once_with('example.invalid', 5432, family=socket.AF_INET,
                                              type=socket.SOCK_STREAM)

    async def test_no_ipv4_address_fails_before_connect(self):
        loop = asyncio.get_running_loop()
        v6 = (socket.AF_INET6, socket.SOCK_STREAM, 6, '', ('2001:db8::10', 5432, 0, 0))
        for rows in ([], [v6]):
            with patch.object(loop, 'getaddrinfo', AsyncMock(return_value=rows)), patch.object(app, 'Tokens'), \
                    patch.object(psycopg.AsyncConnection, 'connect', AsyncMock()) as connect:
                db = app.Database('example.invalid', 'local')
                with self.assertRaisesRegex(psycopg.OperationalError, 'no IPv4 addresses'):
                    await db.connect()
                connect.assert_not_awaited()
        print('IPV4 NO ADDRESS: empty/IPv6-only answers fail clearly without a connection attempt')

    async def test_ipv4_dns_error_has_clear_context(self):
        loop = asyncio.get_running_loop()
        with patch.object(loop, 'getaddrinfo', AsyncMock(side_effect=socket.gaierror(socket.EAI_NONAME, 'no address'))), \
                patch.object(app, 'Tokens'), patch.object(psycopg.AsyncConnection, 'connect', AsyncMock()) as connect:
            db = app.Database('example.invalid', 'local')
            with self.assertRaisesRegex(psycopg.OperationalError, 'IPv4 resolution failed for example.invalid'):
                await db.connect()
            connect.assert_not_awaited()

    async def test_lease_timer_stops_workers_during_slow_stats(self):
        event = self.event()
        self.s.seed_run(event["run_id"], lease=1.5)
        self.s.delay_stats = 2
        job = asyncio.create_task(asyncio.to_thread(app.handler, event, None))
        await asyncio.sleep(2.2)
        workers = [c for c in self.s.connections if c.role == "worker"]
        self.assertEqual(len(workers), 3)
        self.assertTrue(all(c.closed for c in workers))
        self.assertFalse(job.done())  # stats I/O is still pending, but workers stopped
        result = await job
        self.assertIn(result["reason"], ("stop flag or lease expired", "lease expired"))
        self.assertTrue(all(c.closed for c in self.s.connections))
        print("LEASE TIMER REGRESSION: workers close at lease expiry while stats I/O remains blocked")

    async def test_stop_drains_current_transaction_without_task_cancel(self):
        event = self.event()
        event['duration'] = 1
        self.s.transaction_delay = .3
        result = await asyncio.to_thread(app.handler, event, None)
        self.assertTrue(result['completion_persisted'])
        self.assertEqual(self.s.cancelled, 0)
        self.assertTrue(all(c.closed for c in self.s.connections))
        print('H2: stop drained in-flight transactions; no task cancellation')

    async def test_transient_stats_and_completion_errors_reconnect(self):
        event = self.event()
        event['duration'] = 3
        self.s.fail_next[app.STATS_INSERT] = 1
        self.s.fail_next[app.COMPLETE] = 1
        result = await asyncio.to_thread(app.handler, event, None)
        self.assertTrue(result['completion_persisted'], result)
        self.assertNotIn('error', result)
        self.assertGreater(result['commits'], 0)
        self.assertGreater(self.s.connect_calls, 4)
        self.assertTrue(all(c.closed for c in self.s.connections))
        print('M2: transient stats and completion writes reconnected and completed')

    async def test_heartbeat_survives_monitor_stall_and_reconnect(self):
        self.s.delay_latest = 4
        self.s.fail_next[drive.HEARTBEAT] = 1
        cfg = drive.parse_args(['--function', 'local', '--region', 'local', '--host', 'unused', '--origin', 'Test',
                                '--ramp', '2', '--step-seconds', '1', '--local-test'])
        result = await drive.drive(cfg)
        self.assertTrue(result['success'], result)
        began, ended = self.s.latest_stalls[0]
        self.assertTrue(any(began < tick < ended for tick in self.s.heartbeat_times))
        self.assertEqual(self.s.cancelled, 0)
        print('H2/M2: separate heartbeat continued during a monitor socket timeout and transient heartbeat failure')

    async def test_duplicate_delivery_cannot_start_extra_workers(self):
        event = self.event()
        event['duration'] = 2
        first = asyncio.create_task(asyncio.to_thread(app.handler, event, None))
        await asyncio.sleep(.5)
        duplicate = await asyncio.to_thread(app.handler, event, None)
        self.assertEqual(duplicate['reason'], 'duplicate delivery rejected')
        result = await first
        self.assertTrue(result['completion_persisted'])
        self.assertEqual(len(self.s.invocations), 1)
        self.assertTrue(all(r[2] <= 3 for r in self.s.stats))
        print('H1: duplicate delivery rejected by logical-slot ownership')

    async def test_recovery_capacity_and_hold_tolerance(self):
        cfg = drive.parse_args(['--function', 'f', '--region', 'r', '--host', 'h', '--origin', 'o'])
        waves, count, _ = drive.plan(cfg)
        budget = waves[-1]['recovery_budget']
        expected = {f'{i}-0': 250 for i in range(count)}
        records = {slot: (slot, 'owner', 'running', 1, 0, 250, '{}', 0) for slot in expected}
        seconds = drive.recovery_seconds(expected, records, [], budget)
        self.assertEqual(seconds, math.ceil(250 / (budget/count)) + 30)
        self.assertGreater(budget/count, waves[-1]['retry_rate'] * 9)
        self.assertEqual(math.ceil(9900 * cfg.hold_tolerance), 9851)
        print(f'M1: recovery {budget:g}/s shared; 250 missing in one of {count} slots gets {seconds}s; tolerance 9851/9900')

    async def test_final_stats_permission_error_still_persists_completion(self):
        event = self.event()
        event['duration'] = 1
        self.s.fail_final_stats = True
        result = await asyncio.to_thread(app.handler, event, None)
        self.assertTrue(result['completion_persisted'])
        row = next(iter(self.s.invocations.values()))
        self.assertEqual(row[2], 'done')
        self.assertIn('InsufficientPrivilege', json.loads(row[6])['error'])
        print('COMPLETION: final stats permission failure is recorded independently in completion')

    async def test_startup_insert_acknowledgements_can_be_lost(self):
        self.s.lost_ack = {'runs', 'schedule'}
        cfg = drive.parse_args(['--function', 'local', '--region', 'local', '--host', 'unused', '--origin', 'Test',
                                '--ramp', '1', '--step-seconds', '1', '--local-test'])
        result = await drive.drive(cfg)
        self.assertTrue(result['success'], result)
        self.assertEqual(len(self.s.invocations), 1)
        self.assertEqual(self.s.lost_ack, set())
        print('STARTUP: lost INSERT acknowledgements resolved by reading initial state')

    async def test_new_dip_after_monitor_error_gets_fresh_recovery_window(self):
        self.s.inject_worker_dip = self.s.recovery_scenario = True
        cfg = drive.parse_args(['--function', 'local', '--region', 'local', '--host', 'unused', '--origin', 'Test',
                                '--ramp', '2', '--step-seconds', '2', '--think-ms', '25', '--local-test'])
        with patch.object(drive, 'recovery_seconds', return_value=1.2):
            result = await drive.drive(cfg)
        self.assertTrue(result['success'], result)
        self.assertTrue(self.s.scenario_fault)
        samples = [n for _, n in self.s.driver_rows]
        first = samples.index(2)
        self.assertEqual(samples[first:first+5], [2, 0, 2, 0, 2])
        print('RECOVERY: a new dip after a monitor failure does not reuse an expired recovery deadline')

    async def test_venv_hash_path_and_guide_commands(self):
        guide = Path('RECORDING_v2.md').read_text()
        self.assertIn('python3.12 -m venv --clear .venv && .venv/bin/python -m pip install -r lambda_requirements.txt', guide)
        self.assertNotRegex(guide, r'\.venv/bin/(?!python)[a-zA-Z]')
        for block in re.findall(r'```(?:sh)?\n(.*?)```', guide, re.DOTALL):
            if 'drive.py' in block or 'watch.py' in block:
                self.assertIn('export AWS_PROFILE=<sandbox-profile>', block)
        for name in ('build_lambda.sh', 'deploy_lambda.sh'):
            self.assertNotRegex(Path(name).read_text(), r'\.venv/bin/(?!python)[a-zA-Z]')
        with tempfile.TemporaryDirectory(prefix='lambda-Dev#-') as folder:
            env = Path(folder) / '.venv'
            await asyncio.to_thread(subprocess.run, [sys.executable, '-m', 'venv', str(env)], check=True, capture_output=True)
            result = await asyncio.to_thread(subprocess.run, [str(env/'bin/python'), '-m', 'pip', '--version'], check=True, capture_output=True, text=True)
            self.assertIn('python 3.12', result.stdout)
        print('ITEM 7: Python -m pip works in a fresh # path; existing project venv untouched')

    async def test_validation_and_peak_budget(self):
        cfg = drive.parse_args(["--function", "f", "--region", "r", "--host", "h", "--origin", "o"])
        waves, total, duration = drive.plan(cfg)
        self.assertEqual(waves[-1]["target"], 9900)
        self.assertEqual(total, 42)
        self.assertLessEqual(duration, app.MAX_DURATION)
        retry_total = total * waves[0]["retry_rate"]
        for wave in waves:
            initial_total = len(wave["counts"]) * wave["rate"]
            self.assertAlmostEqual(initial_total + retry_total + total * wave["admin_rate"] + 2, 90)
        for bad in ("10000", "3,2", "0", "", "2,2"):
            cfg.ramp = bad
            with self.assertRaises(ValueError):
                drive.plan(cfg)
        with patch.dict(os.environ, {"AWS_LAMBDA_FUNCTION_NAME": "test"}), self.assertRaises(ValueError):
            app.local_parameters()
        with patch.dict(os.environ, {"DSQL_DEMO_LOCAL_DSN": "host=example.org"}), self.assertRaises(ValueError):
            app.local_parameters()
        print(f"PLAN: 9,900 workers; {total} invocations; <=90 combined initial+retry attempts/s; deadline={duration}s")


if __name__ == "__main__":
    print("TEST TRANSPORT: in-memory SQL stub. No PostgreSQL server, DSQL, Lambda, IAM, or AWS credentials used.", flush=True)
    unittest.main(verbosity=2)
