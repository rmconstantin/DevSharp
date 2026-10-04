"""Histogram write recovery and dashboard visibility on the offline SQL transport."""
import io
import json
import time
import unittest
import uuid
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import patch

import psycopg

import loadgen_lambda as app
import test_lambda_demo as fixtures
import watch_v2
from lambda_diagnostics import Diagnostics


class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    setUp = fixtures.Tests.setUp

    async def test_lost_ack_retries_without_duplicate_histogram(self):
        with patch('lambda_diagnostics.time.time', return_value=100.2):
            diag = Diagnostics('test')
            diag.transaction(11.5)
        session = app.Session(app.Database('unused', 'local', True), app.Pacer(1000))
        published = set()
        self.s.lose_histogram_ack = True
        try:
            with patch('lambda_diagnostics.time.time', return_value=101.2):
                with self.assertRaises(psycopg.OperationalError):
                    await app.persist_histograms(session, diag, 'Test', 'gen', published, time.monotonic()+5)
                self.assertEqual(published, set())
                await app.persist_histograms(session, diag, 'Test', 'gen', published, time.monotonic()+5)
                await app.persist_histograms(session, diag, 'Test', 'gen', published, time.monotonic()+5)
            self.assertEqual(self.s.histogram_insert_calls, 2)
            self.assertEqual(self.s.histograms, {('Test', 'gen', 100): ('[[11,1]]', '[]')})
            self.assertEqual(published, {100})
        finally:
            await session.close()

    async def test_watcher_uses_local_endpoint_for_both_origins_and_order_visibility(self):
        found, missing = uuid.uuid4(), uuid.uuid4()
        self.s.orders[found] = (found, 1, 1, 'Frankfurt')
        now, second = time.monotonic(), int(time.time())-2
        self.s.stats = [('Frankfurt', 'a', 10, 50, 12., 88., 0, 0, found, now),
                        ('Frankfurt', 'b', 10, 950, 12., 12., 0, 0, found, now),
                        ('Paris', 'c', 10, 42, 50., 50., 3, 0, missing, now)]
        self.s.histograms = {('Frankfurt', 'a', second): ('[[11,48],[87,2]]', '[[149,30]]'),
                             ('Frankfurt', 'b', second): ('[[11,950]]', '[]'), ('Paris', 'c', second): ('[[49,42]]', '[]')}
        session = app.Session(app.Database('unused', 'local', True), app.Pacer(1000))
        try:
            with patch('test_lambda_demo.time.time', return_value=second+2.2), redirect_stdout(io.StringIO()):
                output = await watch_v2.frame(session, SimpleNamespace(label='Frankfurt', region='local'))
                included_output = await watch_v2.frame(session, SimpleNamespace(label='Frankfurt', region='local', include_first_txn=True))
            self.assertIn('both Regions', output)
            self.assertIn('1042', output)
            frankfurt = next(line for line in output.splitlines() if line.startswith('  Frankfurt'))
            paris = next(line for line in output.splitlines() if line.startswith('  Paris'))
            self.assertIn('12.0', frankfurt)
            self.assertIn('visible here', frankfurt)
            self.assertIn('50.0', paris)
            self.assertIn('NOT FOUND', paris)
            # include_first_txn still changes the pooled latency even though the
            # banner that used to name the mode has been removed from the frame.
            self.assertIn('150.0', next(line for line in included_output.splitlines() if line.startswith('  Frankfurt')))
            self.assertEqual(self.s.connect_calls, 1)
        finally:
            await session.close()

    async def test_histogram_rows_have_only_compact_integer_counts(self):
        with patch('lambda_diagnostics.time.time', return_value=200.1):
            diag = Diagnostics('test')
            for _ in range(1000):
                diag.transaction(11.2)
        session = app.Session(app.Database('unused', 'local', True), app.Pacer(1000))
        try:
            with patch('lambda_diagnostics.time.time', return_value=201.1):
                await app.persist_histograms(session, diag, 'Test', 'gen', set(), time.monotonic()+5)
            text, first_text = self.s.histograms[('Test', 'gen', 200)]
            self.assertEqual(first_text, '[]')
            self.assertEqual(json.loads(text), [[11, 1000]])
            self.assertLess(len(text), 20)
        finally:
            await session.close()
