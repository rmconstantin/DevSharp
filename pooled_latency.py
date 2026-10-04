"""Shared database-backed Region pooling for the driver and both dashboards."""
import json
from collections import Counter

from latency_detail import percentile_bin

# The same database-clock second is used through either Region's endpoint.
# One second of collection slack after closure; never fall back to older data.
POOL_QUERY = """
WITH tick AS (
    SELECT floor(extract(epoch FROM clock_timestamp()))::bigint - 2 AS closed_second
), live AS (
    SELECT DISTINCT ON (origin, gen) origin, gen, workers
    FROM demo_stats WHERE ts > clock_timestamp() - interval '3 seconds'
    ORDER BY origin, gen, ts DESC
)
SELECT tick.closed_second, live.origin, live.gen, h.steady_histogram, h.first_histogram
FROM tick LEFT JOIN live ON true
LEFT JOIN demo_latency_histograms h
    ON h.origin = live.origin AND h.gen = live.gen AND h.closed_second = tick.closed_second
"""
HIST_INSERT = (
    'INSERT INTO demo_latency_histograms (origin, gen, closed_second, steady_histogram, first_histogram) VALUES (%s, %s, %s, %s, %s)'
)
HIST_READ = 'SELECT steady_histogram, first_histogram FROM demo_latency_histograms WHERE origin = %s AND gen = %s AND closed_second = %s'


def region_pool(rows, origin, live_generators, include_first_txn=False):
    expected = set(live_generators)
    second = int(rows[0][0]) if rows else None
    observed = {}
    for stamp, label, gen, steady_text, first_text in rows:
        if stamp != second:
            raise ValueError('pool query mixed closed seconds')
        if label == origin and gen in expected and steady_text is not None and first_text is not None:
            steady, first = json.loads(steady_text), json.loads(first_text)
            if any(not isinstance(lo, int) or not isinstance(n, int) or lo < 0 or n <= 0 for lo, n in [*steady, *first]):
                raise ValueError('invalid latency histogram')
            observed[gen] = Counter(dict(steady))
            if include_first_txn:
                observed[gen].update(dict(first))
    missing = sorted(expected - observed.keys())
    complete = bool(expected) and second is not None and not missing
    merged = Counter()
    for bins in observed.values():
        merged.update(bins)
    count = sum(merged.values())
    return {'include_first_txn': include_first_txn, 'closed_second': second, 'complete': complete, 'missing_generators': missing,
            'reported_generators': len(observed), 'live_generators': len(expected),
            'transactions': count, 'resolution_ms': 1,
            'p50_ms': percentile_bin(merged, .5) if complete else None,
            'p99_ms': percentile_bin(merged, .99) if complete else None,
            'mode_80_100_fraction': sum(n for lo, n in merged.items() if 80 <= lo < 100)/count
                if count and complete else None,
            'bins_1ms': sorted(merged.items())}
