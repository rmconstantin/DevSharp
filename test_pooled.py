"""Screen regression: pool transactions instead of selecting the worst generator."""
import json
import unittest

import drive


def samples():
    # 2/50 slow transactions make the small generator's p99 88 ms.
    # 2/1000 slow transactions leave the Region's p99 near 12 ms.
    stats = [('run:small:owner', 50, 50, 12., 88., 0, 0, 100., .1),
             ('run:large:owner', 250, 950, 12., 12., 0, 0, 100., .1)]
    hist = [(100, 'Frankfurt', 'run:small:owner', json.dumps([[11, 48], [87, 2]]), '[]'),
            (100, 'Frankfurt', 'run:large:owner', json.dumps([[11, 950]]), '[]')]
    return stats, hist


class PoolScreenTests(unittest.TestCase):
    def test_screen_uses_transaction_weighted_region_p99(self):
        stats, hist = samples()
        values, _, _ = drive.ScreenSamples().take(stats, hist, 'Frankfurt')
        print(f'POOLED SCREEN REGRESSION: worst generator={drive.aggregate(stats)[3]}, screen p99={values[3]}', flush=True)
        self.assertEqual(values[2:4], (12, 12))

    def test_incomplete_second_shows_dashes(self):
        stats, hist = samples()
        values, _, _ = drive.ScreenSamples().take(stats, hist[:1], 'Frankfurt')
        self.assertEqual(values[2:4], (None, None))
        self.assertEqual(drive.latency(values[3]).strip(), '-')


class PoolReaderTests(unittest.TestCase):
    def test_empty_histogram_is_present_but_missing_row_is_incomplete(self):
        from pooled_latency import region_pool
        rows = [(100, 'Frankfurt', 'a', '[]', '[]'), (100, 'Frankfurt', 'b', '[[11,100]]', '[]')]
        self.assertEqual(region_pool(rows, 'Frankfurt', ['a', 'b'])['p99_ms'], 12)
        rows[0] = (100, 'Frankfurt', 'a', None, None)
        self.assertIsNone(region_pool(rows, 'Frankfurt', ['a', 'b'])['p99_ms'])
        self.assertFalse(region_pool([(100, None, None, None, None)], 'Frankfurt', [])['complete'])

    def test_watch_merges_generators_and_keeps_origins_separate(self):
        import watch_v2
        stats, hist = samples()
        rows = [('Frankfurt', r[0], r[1], r[2], r[4], r[5], None, r[-1]) for r in stats]
        rows += [('Paris', 'paris', 10, 42, 50., 3, None, .1),
                 ('Frankfurt', 'old-run', 50, 9000, 999., 0, None, 5)]
        hist += [(100, 'Paris', 'paris', '[[49,42]]', '[]'), (100, 'Frankfurt', 'old-run', '[[998,9000]]', '[]')]
        regions = watch_v2.summarize(rows, hist)
        self.assertEqual((regions['Frankfurt']['p99'], regions['Frankfurt']['commits'], regions['Frankfurt']['workers']),
                         (12, 1000, 300))
        self.assertEqual((regions['Paris']['p99'], regions['Paris']['commits'], regions['Paris']['conflicts']), (50, 42, 3))
        incomplete = watch_v2.summarize(rows, hist[1:])
        self.assertIsNone(incomplete['Frankfurt']['p99'])
        self.assertEqual(incomplete['Paris']['p99'], 50)

    def test_fresh_zero_worker_invocation_still_requires_a_histogram(self):
        import watch_v2
        stats, hist = samples()
        stats[0] = (stats[0][0], 0, *stats[0][2:])
        self.assertIsNone(drive.ScreenSamples().take(stats, hist[1:], 'Frankfurt')[0][3])
        self.assertEqual(drive.ScreenSamples().take(stats, hist, 'Frankfurt')[0][3], 12)
        rows = [('Frankfurt', r[0], r[1], r[2], r[4], r[5], None, r[-1]) for r in stats]
        self.assertIsNone(watch_v2.summarize(rows, hist[1:])['Frankfurt']['p99'])

    def test_include_first_is_display_only_for_driver_and_watcher(self):
        import watch_v2
        stats, hist = samples()
        hist[0] = (*hist[0][:4], '[[149,30]]')
        default = drive.ScreenSamples().take(stats, hist, 'Frankfurt')[0][3]
        included = drive.ScreenSamples().take(stats, hist, 'Frankfurt', include_first_txn=True)[0][3]
        self.assertEqual((default, included), (12, 150))
        rows = [('Frankfurt', r[0], r[1], r[2], r[4], r[5], None, r[-1]) for r in stats]
        self.assertEqual(watch_v2.summarize(rows, hist)['Frankfurt']['p99'], 12)
        self.assertEqual(watch_v2.summarize(rows, hist, include_first_txn=True)['Frankfurt']['p99'], 150)
        print(f'FIRST DISPLAY FLAG: same rows default={default} ms; included={included} ms', flush=True)
        argv = ['--function', 'test', '--region', 'local', '--host', 'unused', '--origin', 'Frankfurt']
        self.assertFalse(drive.parse_args(argv).include_first_txn)
        self.assertTrue(drive.parse_args([*argv, '--include-first-txn']).include_first_txn)
        watcher_argv = ['--host', 'unused', '--region', 'local', '--label', 'Test']
        self.assertFalse(watch_v2.parse_args(watcher_argv).include_first_txn)
        self.assertTrue(watch_v2.parse_args([*watcher_argv, '--include-first-txn']).include_first_txn)

    def test_default_run_log_creates_logs_subfolder(self):
        import tempfile
        from pathlib import Path
        from types import SimpleNamespace
        from unittest.mock import patch
        with tempfile.TemporaryDirectory(prefix='recorded-logs-') as folder:
            script = Path(folder) / 'Dev#' / 'drive.py'
            with patch.object(drive, '__file__', str(script)):
                path = drive.run_log_path(SimpleNamespace(log_file=None), 'example')
            self.assertEqual(path, script.parent / 'logs' / 'run-example.log')
            self.assertTrue(path.parent.is_dir())
            self.assertFalse(path.exists())

    def test_each_ddl_is_one_statement_and_reset_keeps_controls(self):
        import re
        from pathlib import Path

        def expand(name):
            lines = []
            for line in Path(name).read_text().splitlines():
                if line.startswith('\\i'):
                    lines.append(expand(line.split()[1]))
                elif not line.lstrip().startswith('--'):
                    lines.append(line.split('--', 1)[0])
            return '\n'.join(lines)
        schema = expand('lambda_control.sql')
        self.assertNotRegex(schema, r'\[\]|\bARRAY\b|\bBEGIN\b|\bCOMMIT\b')
        statements = [s.strip() for s in schema.split(';') if s.strip()]
        self.assertEqual(len(statements), 4)
        self.assertTrue(all(s.startswith('CREATE TABLE IF NOT EXISTS') for s in statements))
        self.assertIn('steady_histogram text NOT NULL', schema)
        self.assertIn('first_histogram text NOT NULL', schema)
        self.assertIn('PRIMARY KEY (origin, gen, closed_second)', schema)
        reset = expand('reset_v2.sql')
        for stmt in filter(str.strip, reset.split(';')):
            self.assertEqual(len(re.findall(r'^\s*(?:CREATE|DROP|INSERT|ALTER|BEGIN|COMMIT)\b', stmt, re.MULTILINE)), 1)
        self.assertNotRegex(reset, r'\bTRUNCATE\b|\bDELETE\b|\bBEGIN\b|\bCOMMIT\b')
        self.assertIn('DROP TABLE IF EXISTS demo_latency_histograms', reset)
        self.assertNotIn('DROP TABLE IF EXISTS demo_lambda_', reset)
