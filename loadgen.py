#!/usr/bin/env python3
"""Pizza-order load generator for the Dev# Gdansk recorded demos.

Each worker holds one connection and places orders in a loop: one INSERT with a
random UUID key per transaction. Workers ramp up in steps; once a second the
generator prints throughput, commit latency and conflicts, and writes the same
numbers to demo_stats so the watcher in the other Region can show them.

    # scaling clip: 8 -> 1024 connections, 20 s per step
    ./loadgen.py --host <endpoint> --region eu-central-1 --origin Frankfurt \
        --ramp 8,32,128,512,1024 --step-seconds 20

    # hot-row clip: every transaction also bumps one shared counter row
    ./loadgen.py --host <endpoint> --region eu-central-1 --origin Frankfurt \
        --ramp 64 --step-seconds 30 --hot

Run it from an EC2 host in the cluster's Region. Stop with Ctrl-C.
"""

import argparse
import asyncio
import multiprocessing as mp
import queue
import random
import signal
import socket
import sys
import time
import uuid

import boto3
import psycopg
from psycopg import errors

# Aurora DSQL quotas: burst of 1,000 new connections, refilled at 100/s. Stay
# well under the refill rate so a ramp never trips "rate exceeded" (53400).
DEFAULT_OPEN_RATE = 60
# A connection lasts at most 60 minutes; recycle well before that.
RECONNECT_AFTER_S = 50 * 60
# Children report often so each one-second line on screen is smooth.
REPORT_EVERY_S = 0.1


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", required=True, help="cluster endpoint, e.g. abc123.dsql.eu-central-1.on.aws")
    p.add_argument("--region", required=True)
    p.add_argument("--origin", required=True, help="label for this Region on screen, e.g. Frankfurt")
    p.add_argument("--ramp", default="8,32,128,512,1024", help="comma-separated total worker counts, one per step")
    p.add_argument("--step-seconds", type=float, default=20.0)
    p.add_argument("--hold-seconds", type=float, default=0.0, help="keep the last step running this much longer (0 = until Ctrl-C)")
    p.add_argument("--processes", type=int, default=mp.cpu_count())
    p.add_argument("--open-rate", type=float, default=DEFAULT_OPEN_RATE,
                   help="new connections per second from THIS host. The cluster-wide limit is 100/s, so with N hosts on one cluster use about 90/N each")
    p.add_argument("--gen", default=socket.gethostname().split(".")[0],
                   help="generator id; must differ between hosts that share an --origin (default: hostname)")
    p.add_argument("--hot", action="store_true", help="every transaction also updates one shared counter row")
    p.add_argument("--think-ms", type=float, default=0.0, help="pause between a worker's transactions")
    return p.parse_args()


class Tokens:
    """IAM auth tokens are signed locally, so one per connection is cheap."""

    def __init__(self, host, region):
        self.host, self.region = host, region
        self.client = boto3.client("dsql", region_name=region)

    def get(self):
        return self.client.generate_db_connect_admin_auth_token(self.host, self.region, 3600)


async def connect(cfg, tokens):
    return await psycopg.AsyncConnection.connect(
        host=cfg.host, user="admin", dbname="postgres", password=tokens.get(),
        sslmode="require", autocommit=True, connect_timeout=10,
    )


class Window:
    def __init__(self):
        self.latencies_ms = []
        self.conflicts = 0
        self.errors = 0
        self.last_order_id = None


async def worker(cfg, tokens, win_ref, stop):
    conn = None
    opened = 0.0
    while not stop.is_set():
        try:
            if conn is None or time.monotonic() - opened > RECONNECT_AFTER_S:
                if conn is not None:
                    await conn.close()
                conn = await connect(cfg, tokens)
                opened = time.monotonic()
            order_id = uuid.uuid4()
            t0 = time.perf_counter()
            async with conn.transaction():
                if cfg.hot:
                    await conn.execute("UPDATE demo_hot_counter SET n = n + 1 WHERE id = 1")
                await conn.execute(
                    "INSERT INTO demo_orders (order_id, item_id, user_id, origin) VALUES (%s, %s, %s, %s)",
                    (order_id, random.randint(1, 7), random.randint(1, 1_000_000), cfg.origin),
                )
            win = win_ref[0]
            win.latencies_ms.append((time.perf_counter() - t0) * 1000.0)
            win.last_order_id = order_id
        except errors.SerializationFailure:
            # 40001: OCC conflict (OC000). The loop simply tries a new order,
            # which is what a retrying application would do.
            win_ref[0].conflicts += 1
        except (psycopg.OperationalError, psycopg.InterfaceError):
            win_ref[0].errors += 1
            if conn is not None:
                try:
                    await conn.close()
                except Exception:
                    pass
            conn = None
            await asyncio.sleep(0.5)
        if cfg.think_ms:
            await asyncio.sleep(cfg.think_ms / 1000.0)
    if conn is not None:
        await conn.close()


async def child_main(idx, cfg, target, out_q, stop_evt, share):
    tokens = Tokens(cfg.host, cfg.region)
    stop = asyncio.Event()
    win_ref = [Window()]
    tasks = []
    open_interval = 1.0 / max(cfg.open_rate * share, 0.1)
    next_open = time.monotonic()
    next_report = time.monotonic() + REPORT_EVERY_S
    while not stop_evt.is_set():
        now = time.monotonic()
        if len(tasks) < target.value and now >= next_open:
            tasks.append(asyncio.create_task(worker(cfg, tokens, win_ref, stop)))
            next_open = now + open_interval
        if now >= next_report:
            win, win_ref[0] = win_ref[0], Window()
            out_q.put((idx, len(tasks), win.latencies_ms, win.conflicts, win.errors, win.last_order_id))
            next_report += REPORT_EVERY_S
        await asyncio.sleep(min(0.02, open_interval))
    stop.set()
    await asyncio.gather(*tasks, return_exceptions=True)


def child_entry(idx, cfg, target, out_q, stop_evt, share):
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # the parent owns Ctrl-C
    asyncio.run(child_main(idx, cfg, target, out_q, stop_evt, share))


def pct(sorted_vals, q):
    if not sorted_vals:
        return None
    return sorted_vals[min(len(sorted_vals) - 1, int(q * len(sorted_vals)))]


def main():
    cfg = parse_args()
    steps = [int(x) for x in cfg.ramp.split(",") if x.strip()]
    nproc = max(1, min(cfg.processes, steps[-1]))
    out_q = mp.Queue()
    stop_evt = mp.Event()
    targets = [mp.Value("i", 0) for _ in range(nproc)]
    procs = [mp.Process(target=child_entry, args=(i, cfg, targets[i], out_q, stop_evt, 1.0 / nproc), daemon=True)
             for i in range(nproc)]
    for p in procs:
        p.start()

    stats_tokens = Tokens(cfg.host, cfg.region)
    stats_conn = psycopg.connect(host=cfg.host, user="admin", dbname="postgres", password=stats_tokens.get(),
                                 sslmode="require", autocommit=True, connect_timeout=10)
    stats_opened = time.monotonic()

    def set_total(total):
        for i, t in enumerate(targets):
            t.value = total // nproc + (1 if i < total % nproc else 0)

    interrupted = False

    def on_sigint(*_):
        nonlocal interrupted
        interrupted = True

    signal.signal(signal.SIGINT, on_sigint)

    mode = "HOT ROW: every order also updates one shared counter" if cfg.hot else "random keys, no shared rows"
    print(f"\n  Aurora DSQL load generator  |  {cfg.origin} ({cfg.region}), host {cfg.gen}  |  {mode}\n")
    print(f"  {'time':>5}  {'workers':>7}  {'commits/s':>9}  {'p50 ms':>7}  {'p99 ms':>7}  {'conflicts/s':>11}  {'errors/s':>8}")

    open_now = [0] * nproc  # connections each child actually has open
    start = time.monotonic()
    step_idx = -1
    peak = 1
    end_at = None
    while not interrupted:
        elapsed = time.monotonic() - start
        want_step = min(int(elapsed // cfg.step_seconds), len(steps) - 1)
        if want_step != step_idx:
            step_idx = want_step
            set_total(steps[step_idx])
            if step_idx == len(steps) - 1 and cfg.hold_seconds > 0:
                end_at = time.monotonic() + cfg.step_seconds + cfg.hold_seconds
        if end_at and time.monotonic() >= end_at:
            break

        deadline = time.monotonic() + 1.0
        lat, conflicts, errs, last_id = [], 0, 0, None
        while time.monotonic() < deadline:
            try:
                i, n, l, c, e, lid = out_q.get(timeout=max(0.01, deadline - time.monotonic()))
            except queue.Empty:
                continue
            open_now[i] = n
            lat.extend(l)
            conflicts += c
            errs += e
            last_id = lid or last_id
        active = sum(open_now)
        lat.sort()
        p50, p99 = pct(lat, 0.50), pct(lat, 0.99)
        peak = max(peak, len(lat))
        bar = "#" * int(30 * len(lat) / peak)
        p50s = f"{p50:7.1f}" if p50 is not None else "      -"
        p99s = f"{p99:7.1f}" if p99 is not None else "      -"
        print(f"  {int(elapsed):>4}s  {active:>7}  {len(lat):>9}  {p50s}  {p99s}  {conflicts:>11}  {errs:>8}  {bar}", flush=True)

        try:
            if time.monotonic() - stats_opened > RECONNECT_AFTER_S:
                stats_conn.close()
                stats_conn = psycopg.connect(host=cfg.host, user="admin", dbname="postgres", password=stats_tokens.get(),
                                             sslmode="require", autocommit=True, connect_timeout=10)
                stats_opened = time.monotonic()
            stats_conn.execute(
                "INSERT INTO demo_stats (origin, gen, workers, commits, p50_ms, p99_ms, conflicts, errors, last_order_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (cfg.origin, cfg.gen, active, len(lat), p50, p99, conflicts, errs, last_id),
            )
        except psycopg.Error as e:
            print(f"  (stats write failed: {e.__class__.__name__})", file=sys.stderr)

    print("\n  stopping...", flush=True)
    stop_evt.set()
    for p in procs:
        p.join(timeout=15)
    stats_conn.close()


if __name__ == "__main__":
    main()
