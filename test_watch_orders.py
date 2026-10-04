"""Dashboard order selection and endpoint-clock age, without AWS or a database."""
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import watch_v2
from pooled_latency import POOL_QUERY
from test_lambda_demo import Cursor

NEW = uuid.UUID('abcdef00-0000-0000-0000-000000000001')
OLD = uuid.UUID('12345600-0000-0000-0000-000000000002')
MISSING = uuid.UUID('fedcba00-0000-0000-0000-000000000003')
STAMP = datetime(2026, 10, 3, tzinfo=timezone.utc)


def stats(origin, gen, order_id, age=.1):
    return (origin, gen, 10, 40, 12., 0, order_id, age)


class Endpoint:
    def __init__(self, rows, orders):
        self.rows, self.orders, self.lookups = rows, orders, []

    async def attempt(self, sql, args=None):
        if sql == watch_v2.LATEST:
            return Cursor(self.rows)
        if sql == POOL_QUERY:
            return Cursor([(100, r[0], r[1], '[[11,40]]', '[]') for r in self.rows])
        self.lookups.append((sql, args))
        if sql == 'SELECT 1 FROM demo_orders WHERE order_id = %s':
            return Cursor([(1,)] if args[0] in self.orders else [])
        return Cursor([(key, *self.orders[key]) for key in reversed(args) if key in self.orders])


class OrderColumnTests(unittest.IsolatedAsyncioTestCase):
    async def render(self, endpoint):
        return await watch_v2.frame(endpoint, SimpleNamespace(label='Paris', region='local'))

    def column(self, output, origin='Frankfurt'):
        line = next(line for line in output.splitlines() if line.startswith(f'  {origin:<12}'))
        # Prefix is shared with the existing numeric columns.
        return line[53:]

    async def test_newest_across_generators_and_endpoint_age(self):
        endpoint = Endpoint([stats('Frankfurt', 'a', NEW), stats('Frankfurt', 'z', OLD)],
                            {NEW: (STAMP+timedelta(seconds=10), Decimal('.34')),
                             OLD: (STAMP, Decimal('10.34'))})
        column = self.column(await self.render(endpoint))
        self.assertIn('abcdef...', column)
        self.assertRegex(column, r'written\s+0\.3s ago visible here')
        self.assertNotIn('123456...', column)
        self.assertEqual(len(endpoint.lookups), 1)
        sql, args = endpoint.lookups[0]
        self.assertIn('WHERE order_id IN (%s, %s)', sql)
        self.assertIn('clock_timestamp() - ordered_at', sql)
        self.assertIn('order_id, ordered_at,', sql)
        self.assertEqual(set(args), {NEW, OLD})
        self.assertNotIn(str(NEW), sql)
        print('NEWEST: abcdef... written 0.3s ago visible here; two generators, one IN lookup', flush=True)

    async def test_missing_fresh_candidate_takes_priority_over_visible_newest(self):
        endpoint = Endpoint([stats('Frankfurt', 'a', MISSING), stats('Frankfurt', 'z', NEW)],
                            {NEW: (STAMP, .2)})
        column = self.column(await self.render(endpoint))
        self.assertIn('fedcba... NOT FOUND', column)
        self.assertNotIn('visible here', column)
        self.assertEqual(len(endpoint.lookups), 1)

    async def test_stale_candidates_are_not_looked_up_and_ids_are_deduplicated(self):
        endpoint = Endpoint([stats('Frankfurt', 'a', MISSING, 4), stats('Frankfurt', 'b', NEW),
                             stats('Frankfurt', 'c', NEW)], {NEW: (STAMP, .2)})
        column = self.column(await self.render(endpoint))
        self.assertIn('visible here', column)
        self.assertNotIn('NOT FOUND', column)
        self.assertEqual(endpoint.lookups[0][1], (NEW,))

    async def test_one_query_per_origin_through_same_endpoint(self):
        endpoint = Endpoint([stats('Frankfurt', 'a', NEW), stats('Paris', 'a', OLD)],
                            {NEW: (STAMP, .2), OLD: (STAMP, .4)})
        output = await self.render(endpoint)
        self.assertIn('abcdef...', self.column(output))
        self.assertIn('123456...', self.column(output, 'Paris'))
        self.assertEqual(len(endpoint.lookups), 2)
        self.assertEqual([args for _, args in endpoint.lookups], [(NEW,), (OLD,)])

    async def test_column_width_is_stable_for_all_states_and_refresh_is_one_second(self):
        columns = []
        for candidates, orders in [([NEW], {NEW: (STAMP, .2)}),
                                   ([NEW], {NEW: (STAMP, 12345678.)}),
                                   ([NEW], {NEW: (STAMP, -.2)}),
                                   ([NEW], {}), ([NEW, OLD], {}), ([None], {})]:
            endpoint = Endpoint([stats('Frankfurt', str(i), key) for i, key in enumerate(candidates)], orders)
            columns.append(self.column(await self.render(endpoint)))
            self.assertEqual(len(endpoint.lookups), 0 if candidates == [None] else 1)
        self.assertEqual(len({len(c) for c in columns}), 1)
        self.assertTrue(all(c.isascii() for c in columns))
        self.assertIn('(+1 more)', columns[4])
        self.assertEqual(columns[-1].strip(), '-')
        cfg = watch_v2.parse_args(['--host', 'unused', '--region', 'local', '--label', 'Paris'])
        self.assertEqual(cfg.interval, 1.)
