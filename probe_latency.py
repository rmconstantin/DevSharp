"""One-connection diagnostic probe. Writes orders with origin 'probe'."""
import argparse
import asyncio
import json
import signal
import time
import uuid

from lambda_diagnostics import Diagnostics
from latency_detail import order_transaction
from loadgen_lambda import Database, socket_deadline


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--profile')
    parser.add_argument('--transactions', '-n', type=int, default=2000)
    parser.add_argument('--autocommit-insert', action='store_true')
    parser.add_argument('--local-test', action='store_true', help='loopback-only test DSN; no AWS clients')
    cfg = parser.parse_args(argv)
    if cfg.transactions < 1:
        parser.error('--transactions must be positive')
    return cfg


async def probe(cfg):
    if cfg.profile and not cfg.local_test:
        import boto3
        boto3.setup_default_session(profile_name=cfg.profile, region_name=cfg.region)
    diag = Diagnostics('probe')
    db = Database(cfg.host, cfg.region, cfg.local_test, diag)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    signals = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
            signals.append(sig)
        except (NotImplementedError, RuntimeError, ValueError):
            pass
    conn, committed, error = None, 0, None
    phase = 'connect'
    ticker = asyncio.create_task(diag.ticker(stop))
    reported_at = time.monotonic()
    try:
        conn = await db.connect()  # same IPv4, IAM signing, TLS and timeout as Lambda
        for index in range(cfg.transactions):
            if stop.is_set():
                break
            phase = 'first_txn' if index == 0 else 'txn'
            order_id = uuid.uuid4()
            began = time.perf_counter()
            with socket_deadline(conn):
                await order_transaction(conn, order_id, 'probe',
                                                        autocommit_insert=cfg.autocommit_insert)
            elapsed = (time.perf_counter()-began)*1000
            committed += 1
            diag.measure(phase, elapsed)
            diag.transaction(elapsed, first_transaction=index == 0)
            if time.monotonic()-reported_at >= 1:
                diag.window(1)
                diag.progress()
                reported_at = time.monotonic()
    except Exception as exc:  # noqa: BLE001 -- diagnostic command reports and stops, never retries writes
        diag.error(phase, exc)
        error = f'{type(exc).__name__}: {exc}'
    finally:
        stop.set()
        if conn is not None:
            await conn.close()
        await ticker
        for sig in signals:
            loop.remove_signal_handler(sig)
        diag.window(0)
    result = {'mode': 'autocommit-insert' if cfg.autocommit_insert else 'explicit-transaction',
              'requested': cfg.transactions, 'commits': committed, 'error': error,
              'success': error is None and committed == cfg.transactions, **diag.summary()}
    print(json.dumps(result), flush=True)
    return result


def main():
    return 0 if asyncio.run(probe(parse_args()))['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
