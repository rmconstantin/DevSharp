"""Offline regression checks for diagnostics and screen sample freshness."""
import asyncio
import io
import json
import time
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import psycopg

import drive
import test_lambda_demo as fixtures
from lambda_diagnostics import Diagnostics
from test_lambda_demo import Transaction


class MeasurementTests(unittest.IsolatedAsyncioTestCase):
    def test_error_grouping_sqlstate_truncation_and_print_limit(self):
        emitted = []
        diag = Diagnostics('test', emit=emitted.append)
        for _ in range(3):
            diag.error('first_txn', psycopg.errors.SerializationFailure('x'*250), worker=7)
        for i in range(12):
            diag.error('connect', OSError(f'failure {i}'), worker=i)
        summary = diag.summary()
        self.assertEqual(len(emitted), 10)
        self.assertEqual(summary['error_details'][0]['count'], 3)
        self.assertEqual(summary['error_details'][0]['sqlstate'], '40001')
        self.assertEqual(len(summary['error_details'][0]['message']), 200)
        self.assertEqual(summary['workers_with_errors'], 12)

    def test_first_transaction_and_steady_latency_are_separate(self):
        diag = Diagnostics('test', emit=lambda _: None)
        diag.measure('first_txn', 90)
        diag.measure('first_txn_error', 3000)
        diag.measure('txn', 15)
        diag.measure('txn', 25)
        diag.observe_lag(1, 1.12)
        diag.window(1)
        out = diag.summary()
        self.assertAlmostEqual(out['loop_lag_max_ms'], 120)
        self.assertEqual(out['latency_totals_ms']['first_txn']['mean'], 90)
        self.assertEqual(out['latency_totals_ms']['txn']['mean'], 20)
        self.assertEqual(out['windows'][0]['latency_ms']['first_txn_error']['max'], 3000)

    def test_window_excludes_first_transaction_latency(self):
        import loadgen_lambda as app
        window = app.StatsWindow()
        window.record(150, first_transaction=True)
        window.record(10, first_transaction=False)
        window.record(10, first_transaction=False)
        commits, p50, p99 = app.window_metrics(window)
        print(f'STEADY REGRESSION: commits={commits}, p50={p50}, p99={p99}', flush=True)
        self.assertEqual(commits, 3, 'first transactions still count as commits')
        self.assertEqual((p50, p99), (10, 10))

    def test_first_only_window_reports_commits_without_latency(self):
        import loadgen_lambda as app
        window = app.StatsWindow()
        window.record(150, first_transaction=True)
        self.assertEqual(app.window_metrics(window), (1, None, None))

    async def test_real_event_loop_stall_is_observed(self):
        stop = asyncio.Event()
        diag = Diagnostics('test', emit=lambda _: None)
        ticker = asyncio.create_task(diag.ticker(stop))
        await asyncio.sleep(.02)
        time.sleep(.2)  # noqa: ASYNC251 -- reproduce a synchronous loop stall intentionally
        await asyncio.sleep(.02)
        stop.set()
        await ticker
        self.assertGreater(diag.summary()['loop_lag_max_ms'], 80)

    def test_same_database_window_is_not_printed_twice(self):
        display = drive.ScreenSamples()
        rows = [('run:0-0:owner', 10, 123, 10., 20., 0, 0, 100., .1)]
        self.assertEqual(display.take(rows)[0][1], 123)
        rows[0] = (*rows[0][:-1], 1.1)
        metrics, fresh, count = display.take(rows)
        self.assertEqual(metrics, (10, None, None, None, None, None))
        self.assertEqual((fresh, count), (0, 1))
        rows[0] = (*rows[0][:-2], 101., .1)
        self.assertEqual(display.take(rows)[0][1], 123)

    def test_partial_new_window_waits_for_other_generators(self):
        display = drive.ScreenSamples()
        a = ('run:0-0:owner', 10, 123, 10., 20., 0, 0, 100., .1)
        b = ('run:1-0:owner', 10, 100, 10., 20., 0, 0, 100., .1)
        self.assertEqual(display.take([a, b])[0][1], 223)
        newer_a = (*a[:-2], 101., .1)
        self.assertIsNone(display.take([newer_a, b])[0][1])
        self.assertEqual(display.take([newer_a, (*b[:-2], 101., .1)])[0][1], 223)


class HandlerDiagnosticsTests(unittest.IsolatedAsyncioTestCase):
    # Reuse only fixture helpers, not BaseTests' test cases.
    setUp = fixtures.Tests.setUp
    event = fixtures.Tests.event

    async def test_error_phases_survive_database_completion(self):
        import loadgen_lambda as app

        event = self.event()
        event.update(connections=1, duration=4)
        self.s.fail_connect_at = 2  # the first worker, after the stats connection
        original = Transaction.__aenter__
        entered = 0

        async def failing_enter(tx):
            nonlocal entered
            entered += 1
            if entered in (1, 3):
                raise psycopg.OperationalError('first txn' if entered == 1 else 'steady txn')
            return await original(tx)

        output = io.StringIO()
        with patch.object(Transaction, '__aenter__', failing_enter), redirect_stdout(output):
            result = await asyncio.to_thread(app.handler, event, None)
        self.assertTrue(result['completion_persisted'])
        details = result['error_details']
        self.assertEqual({x['phase'] for x in details}, {'connect', 'first_txn', 'txn'})
        self.assertEqual(sum(x['count'] for x in details), 3)
        self.assertEqual(result['workers_with_errors'], 1)
        self.assertIn('first_txn', result['latency_totals_ms'])
        self.assertIn('txn', result['latency_totals_ms'])
        self.assertEqual(result['latency_totals_ms']['worker_first_txn']['count'], 1)
        persisted = json.loads(next(iter(self.s.invocations.values()))[6])
        self.assertEqual(persisted['error_details'], details)
        self.assertTrue(persisted['windows'])
        self.assertEqual(len(output.getvalue().splitlines()), 3)

    async def test_replacement_first_transactions_excluded_from_stats(self):
        import loadgen_lambda as app
        event = self.event()
        event.update(connections=1, duration=2)
        original_record = app.StatsWindow.record
        original_execute = fixtures.Connection.execute
        flags, orders = [], 0

        def fixed_latency(window, elapsed_ms, first_transaction):
            flags.append(first_transaction)
            original_record(window, 150 if first_transaction else 10, first_transaction)

        async def break_connection(conn, sql, args=None):
            nonlocal orders
            if sql.startswith('INSERT INTO demo_orders'):
                orders += 1
                if orders == 5:
                    raise psycopg.OperationalError('injected connection replacement')
            return await original_execute(conn, sql, args)

        with patch.object(app.StatsWindow, 'record', fixed_latency), \
                patch.object(fixtures.Connection, 'execute', break_connection), redirect_stdout(io.StringIO()):
            result = await asyncio.to_thread(app.handler, event, None)
        self.assertTrue(result['completion_persisted'])
        self.assertEqual(sum(flags), 2, 'initial and replacement connections each have a first transaction')
        self.assertEqual(result['commits'], len(flags), 'commit totals include both first transactions')
        self.assertEqual(result['latency_totals_ms']['first_txn']['count'], 2)
        self.assertEqual(sum(n for w in result['second_histograms'] for _, n in w['first_bins_1ms']), 2)
        self.assertEqual(result['latency_totals_ms']['worker_first_txn']['count'], 1)
        self.assertTrue(any(row[5] == 10 for row in self.s.stats))
        self.assertTrue(all(row[4] in (None, 10) and row[5] in (None, 10) for row in self.s.stats))
        print('STEADY REPLACEMENT: both first transactions excluded from stats latency, retained in commits/diagnostics')

    async def test_handler_stats_and_lease_errors_are_named(self):
        import loadgen_lambda as app

        event = self.event()
        event.update(duration=3)
        self.s.fail_next[app.LEASE_QUERY] = 1
        self.s.fail_next[app.STATS_INSERT] = 1
        with redirect_stdout(io.StringIO()):
            result = await asyncio.to_thread(app.handler, event, None)
        self.assertTrue(result['completion_persisted'])
        self.assertEqual({x['phase'] for x in result['error_details']}, {'stats', 'heartbeat'})

    async def test_driver_heartbeat_details_are_in_run_log(self):
        self.s.fail_next[drive.HEARTBEAT] = 1  # startup monitor failure (stats)
        cfg = drive.parse_args(['--function', 'local', '--region', 'local', '--host', 'unused', '--origin', 'Test',
                                '--ramp', '1', '--step-seconds', '1', '--local-test'])
        with redirect_stdout(io.StringIO()):
            result = await drive.drive(cfg)
        self.assertTrue(result['success'])
        self.assertTrue(result['error_details'])
        # The dedicated heartbeat session supplies its phase even for connect errors.
        diag = Diagnostics('driver', emit=lambda _: None)
        import loadgen_lambda as app
        db = app.Database('unused', 'local', True, diag)
        self.s.fail_next[drive.HEARTBEAT] = 1
        session = app.Session(db, app.Pacer(100), phase='heartbeat')
        with self.assertRaises(psycopg.OperationalError):
            await session.attempt(drive.HEARTBEAT, (result['run_id'],))
        self.assertEqual(diag.summary()['error_details'][0]['phase'], 'heartbeat')
        await session.close()
