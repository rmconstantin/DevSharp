"""Bounded per-second measurements and grouped errors; no database schema changes."""
import asyncio
import json
import re
import time

from latency_detail import Histogram
from loadgen import pct


class Diagnostics:
    def __init__(self, gen, emit=None):
        self.gen, self.started = gen, time.monotonic()
        self.started_at_epoch = time.time()
        self.emit = emit or (lambda row: print(json.dumps(row), flush=True))
        self.errors = {}
        self.samples, self.totals, self.windows = {}, {}, []
        self.lag_ms, self.max_lag_ms = 0.0, 0.0
        self.window_started = self.started
        self.connect_attempts = 0
        self.workers_with_errors = set()
        self.ready_after_ms = None
        self.steady_window, self.steady_total = Histogram(), Histogram()
        self.first_second = int(self.started_at_epoch)
        self.seconds = {}
        self.first_seconds = {}
        self.closed_seconds = {}
        self.next_second = self.first_second

    def error(self, phase, exc, **context):
        # Never print a DSN or token supplied by a library error message.
        message = re.sub(r'(?i)(password\s*[=:]\s*|X-Amz-[\w-]+=)[^\s&]+',
                         r'\1[redacted]', str(exc))[:200]
        key = (phase, type(exc).__name__, getattr(exc, 'sqlstate', None), message)
        context = {**getattr(exc, 'demo_context', {}), **context}
        if context.get('worker') is not None:
            self.workers_with_errors.add(context['worker'])
        elapsed = round(time.monotonic() - self.started, 3)
        if key not in self.errors:
            self.errors[key] = dict(zip(('phase', 'exception', 'sqlstate', 'message'), key),
                                    count=0, first_at_s=elapsed, first_context=context)
            if len(self.errors) <= 10:
                self.emit({'event': 'load_error', 'gen': self.gen, **self.errors[key], 'count': 1})
        row = self.errors[key]
        row['count'] += 1
        row['last_at_s'] = elapsed

    def measure(self, name, elapsed_ms):
        self.samples.setdefault(name, []).append(elapsed_ms)
        total = self.totals.setdefault(name, {'count': 0, 'total_ms': 0.0, 'max_ms': 0.0})
        total['count'] += 1
        total['total_ms'] += elapsed_ms
        total['max_ms'] = max(total['max_ms'], elapsed_ms)

    def transaction(self, elapsed_ms, first_transaction=False):
        second = int(time.time())
        steady = self.seconds.setdefault(second, Histogram())
        if first_transaction:
            self.first_seconds.setdefault(second, Histogram()).add(elapsed_ms)
        else:
            self.steady_window.add(elapsed_ms)
            self.steady_total.add(elapsed_ms)
            steady.add(elapsed_ms)

    def progress(self):
        # Only closed wall-clock seconds are published; never merge partial bins.
        now = int(time.time())
        for second in range(self.next_second, now):
            hist = self.seconds.get(second)
            self.closed_seconds[second] = {
                'epoch_second': second, 'bins_1ms': sorted(hist.fine.items()) if hist else [],
                'first_bins_1ms': sorted(self.first_seconds[second].fine.items()) if second in self.first_seconds else []}
        self.next_second = max(self.next_second, now)
        self.seconds = {sec: h for sec, h in self.seconds.items() if sec >= now}
        self.first_seconds = {sec: h for sec, h in self.first_seconds.items() if sec >= now}
        seconds = [row for sec, row in self.closed_seconds.items() if sec >= now-5]
        return {'diagnostic_progress': {'seconds': seconds, 'first_second': self.first_second}}

    def observe_lag(self, expected, actual):
        self.lag_ms = max(self.lag_ms, max(0.0, actual - expected) * 1000)
        self.max_lag_ms = max(self.max_lag_ms, self.lag_ms)

    async def ticker(self, stop):
        loop = asyncio.get_running_loop()
        while not stop.is_set():
            expected = loop.time() + .1
            await asyncio.sleep(.1)
            self.observe_lag(expected, loop.time())

    def window(self, workers):
        now = time.monotonic()
        row = {'start_s': round(self.window_started-self.started, 3),
               'end_s': round(now-self.started, 3), 'at_epoch': round(time.time(), 3), 'workers': workers,
               'loop_lag_max_ms': round(self.lag_ms, 3), 'latency_ms': {},
               'steady_detail': {'latency': self.steady_window.summary()}}
        for name, samples in self.samples.items():
            ordered = sorted(samples)
            row['latency_ms'][name] = {'count': len(samples), 'p50': round(pct(ordered, .5), 3),
                                      'p99': round(pct(ordered, .99), 3), 'max': round(ordered[-1], 3)}
        self.windows.append(row)
        self.steady_window = Histogram()
        self.samples, self.lag_ms, self.window_started = {}, 0.0, now

    def summary(self):
        self.progress()
        current = int(time.time())
        self.seconds.setdefault(current, Histogram())
        return {'second_histograms': [*self.closed_seconds.values(),
                    *({'epoch_second': sec, 'bins_1ms': sorted(h.fine.items()),
                       'first_bins_1ms': sorted(self.first_seconds[sec].fine.items()) if sec in self.first_seconds else []}
                      for sec, h in self.seconds.items() if sec not in self.closed_seconds)],
                'first_second': self.first_second, 'last_second': current,
                'steady_detail': {'latency': self.steady_total.summary()},
                'error_details': list(self.errors.values()), 'started_at_epoch': self.started_at_epoch,
                'workers_with_errors': len(self.workers_with_errors),
                'connect_attempts': self.connect_attempts,
                'ready_after_ms': self.ready_after_ms,
                'loop_lag_max_ms': round(self.max_lag_ms, 3),
                'latency_totals_ms': {name: {'count': x['count'],
                    'mean': round(x['total_ms']/x['count'], 3), 'max': round(x['max_ms'], 3)}
                    for name, x in self.totals.items()},
                'windows': self.windows}
