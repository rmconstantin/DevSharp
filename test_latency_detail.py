"""Offline whole-transaction measurement and publication tests."""
import asyncio
import io
import json
import unittest
import uuid
from collections import Counter
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import drive
import loadgen_lambda as app
import probe_latency
import test_lambda_demo as fixtures
from lambda_diagnostics import Diagnostics
from latency_detail import Histogram, order_transaction, percentile_bin


class DetailTests(unittest.IsolatedAsyncioTestCase):
    def test_bucket_boundaries_and_mode_fraction(self):
        histogram = Histogram()
        for ms in (0, 14.99, 15, 29.99, 30, 59.99, 60, 79.99, 80, 99.99, 100, 149.99, 150):
            histogram.add(ms)
        summary = histogram.summary(fine=True)
        self.assertEqual(list(summary['histogram'].values()), [2, 2, 2, 2, 2, 2, 1])
        self.assertEqual(summary['mode_80_100_fraction'], 2/13)
        self.assertEqual(sum(n for _, n in summary['bins_1ms']), 13)
        self.assertIsNone(Histogram().summary()['mode_80_100_fraction'])
        self.assertIsNone(percentile_bin(Counter(), .99))

    async def test_single_insert_workload_without_statement_measurements(self):
        calls = []

        class Connection:
            def transaction(self):
                return self

            async def __aenter__(self):
                calls.append('BEGIN')

            async def __aexit__(self, *args):
                calls.append('COMMIT')

            async def execute(self, sql, args=None):
                calls.append(sql.split()[0])

        self.assertIsNone(await order_transaction(Connection(), uuid.uuid4(), 'test'))
        self.assertEqual(calls, ['BEGIN', 'INSERT', 'COMMIT'])
        calls.clear()
        await order_transaction(Connection(), uuid.uuid4(), 'test', hot=True)
        self.assertEqual(calls, ['BEGIN', 'UPDATE', 'INSERT', 'COMMIT'])
        calls.clear()
        await order_transaction(Connection(), uuid.uuid4(), 'probe', autocommit_insert=True)
        self.assertEqual(calls, ['INSERT'])

    def test_closed_seconds_are_immutable_and_gaps_survive_completion(self):
        with patch('lambda_diagnostics.time.time', return_value=100.2):
            diag = Diagnostics('test')
            diag.transaction(88.)
            self.assertEqual(diag.progress()['diagnostic_progress']['seconds'], [])
        with patch('lambda_diagnostics.time.time', return_value=101.2):
            first = diag.progress()['diagnostic_progress']['seconds'][0]
            self.assertEqual(first, {'epoch_second': 100, 'bins_1ms': [(88, 1)], 'first_bins_1ms': []})
        with patch('lambda_diagnostics.time.time', return_value=110.2):
            self.assertEqual(len(diag.progress()['diagnostic_progress']['seconds']), 5)
            out = diag.summary()
            self.assertEqual(out['second_histograms'][0], first)
            self.assertEqual(len(out['second_histograms']), 11)
            self.assertEqual(out['steady_detail']['latency']['count'], 1)
        diag.window(0)
        self.assertEqual(diag.windows[0]['steady_detail']['latency']['count'], 1)
        diag.window(0)
        self.assertEqual(diag.windows[1]['steady_detail']['latency']['count'], 0)
        with patch('lambda_diagnostics.time.time', return_value=110.2):
            self.assertNotIn('statements', json.dumps(diag.summary()))
            self.assertNotIn('slow_groups', json.dumps(diag.summary()))


class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    setUp = fixtures.Tests.setUp
    event = fixtures.Tests.event

    async def test_handler_persists_whole_histograms_and_totals(self):
        event = self.event()
        event.update(duration=2, connections=1)
        self.s.transaction_delay = .075
        with redirect_stdout(io.StringIO()):
            result = await asyncio.to_thread(app.handler, event, None)
        persisted = json.loads(next(iter(self.s.invocations.values()))[6])
        count = result['commits']-1
        self.assertEqual(persisted['steady_detail']['latency']['count'], count)
        self.assertEqual(sum(w['steady_detail']['latency']['count'] for w in persisted['windows']), count)
        self.assertEqual(sum(n for row in persisted['second_histograms'] for _, n in row['bins_1ms']), count)
        self.assertEqual(sum(n for bins, _ in self.s.histograms.values() for _, n in json.loads(bins)), count)
        self.assertTrue(all(key[:2] == ('Test', result['gen']) for key in self.s.histograms))
        self.assertEqual(result['histograms_pending'], 0)
        self.assertEqual(sum(n for _, bins in self.s.histograms.values() for _, n in json.loads(bins)), 1)
        self.assertEqual(result['histograms_published'], len(self.s.histograms))
        self.assertNotIn('statements', persisted['steady_detail'])
        self.assertNotIn('slow_groups', json.dumps(persisted))
        self.assertTrue(result['completion_persisted'])

    async def test_probe_both_modes_share_connect_path_and_exclude_first(self):
        for extra in ([], ['--autocommit-insert']):
            cfg = probe_latency.parse_args(['--host', 'unused', '--region', 'local', '-n', '4', '--local-test', *extra])
            prior = self.s.transactions_started
            with redirect_stdout(io.StringIO()):
                result = await probe_latency.probe(cfg)
            self.assertTrue(result['success'])
            self.assertEqual(result['commits'], 4)
            self.assertEqual(result['steady_detail']['latency']['count'], 3)
            self.assertEqual(result['latency_totals_ms']['first_txn']['count'], 1)
            self.assertEqual(self.s.transactions_started-prior, 0 if extra else 4)
            self.assertTrue(all(row[-1] == 'probe' for row in self.s.orders.values()))
            self.assertTrue(all(conn.closed for conn in self.s.connections))
            self.assertNotIn('statements', json.dumps(result))
            self.assertNotIn('slow_groups', json.dumps(result))
        self.assertEqual(self.s.connect_calls, 2)

    async def test_driver_shows_pooled_values_and_logs_worst_only(self):
        cfg = drive.parse_args(['--function', 'local', '--region', 'local', '--host', 'unused', '--origin', 'Test',
                                '--ramp', '1', '--step-seconds', '2', '--local-test'])
        out = io.StringIO()
        with redirect_stdout(out):
            result = await drive.drive(cfg)
        self.assertTrue(result['success'])
        rows = [json.loads(line) for line in Path(result['log_file']).read_text().splitlines()]
        samples = [r['data'] for r in rows if r['event'] == 'sample']
        self.assertTrue(any(r['pooled']['complete'] and r['pooled']['transactions'] > 0 for r in samples))
        self.assertTrue(all(r['p99_ms'] == r['pooled']['p99_ms'] for r in samples))
        self.assertTrue(all('worst_generator_p99_ms' in r for r in samples))
        self.assertIn('pooled p50/p99', out.getvalue())
        self.assertNotIn('worst', out.getvalue().lower())
