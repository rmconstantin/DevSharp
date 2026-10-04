"""Whole-transaction histograms and the shared single-INSERT workload."""
import math
import random
from collections import Counter

BUCKETS = ('<15', '15-30', '30-60', '60-80', '80-100', '100-150', '>=150')
EDGES = (15, 30, 60, 80, 100, 150)
INSERT = 'INSERT INTO demo_orders (order_id, item_id, user_id, origin) VALUES (%s, %s, %s, %s)'


class Histogram:
    def __init__(self):
        self.count, self.total, self.maximum = 0, 0.0, 0.0
        self.coarse, self.fine = Counter(), Counter()

    def add(self, ms):
        self.count += 1
        self.total += ms
        self.maximum = max(self.maximum, ms)
        bucket = next((i for i, edge in enumerate(EDGES) if ms < edge), len(EDGES))
        self.coarse[BUCKETS[bucket]] += 1
        self.fine[math.floor(ms)] += 1

    def summary(self, fine=False):
        row = {'count': self.count, 'mean_ms': self.total / self.count if self.count else None,
               'max_ms': self.maximum if self.count else None,
               'histogram': {key: self.coarse[key] for key in BUCKETS},
               'mode_80_100_fraction': self.coarse['80-100'] / self.count if self.count else None}
        if fine:
            row['bins_1ms'] = sorted(self.fine.items())
        return row


def percentile_bin(bins, fraction):
    """Upper edge of the bin containing a nearest-rank percentile; error <=1ms."""
    count = sum(bins.values())
    if not count:
        return None
    rank, cumulative = max(1, math.ceil(count * fraction)), 0
    for lower, n in sorted(bins.items()):
        cumulative += n
        if cumulative >= rank:
            return lower + 1
    raise AssertionError('histogram count mismatch')


async def order_transaction(conn, order_id, origin, hot=False, autocommit_insert=False):
    """Single INSERT workload; only the existing hot-row clip adds an UPDATE."""
    if autocommit_insert:
        await conn.execute(INSERT, (order_id, random.randint(1, 7), random.randint(1, 1_000_000), origin))
        return
    async with conn.transaction():
        if hot:
            await conn.execute('UPDATE demo_hot_counter SET n = n + 1 WHERE id = 1')
        await conn.execute(INSERT, (order_id, random.randint(1, 7), random.randint(1, 1_000_000), origin))
