#!/usr/bin/env python3
"""Watch both Regions' traffic from ONE Region's endpoint.

Each load generator writes one demo_stats row per second. This watcher reads
the newest row per Region through its own local endpoint, then looks up that
Region's most recent order by primary key. Run one watcher per Region, side by
side: both show the same numbers, because it is one strongly consistent
database with two writable endpoints.

    ./watch.py --host <endpoint-in-this-region> --region eu-west-1 --label Ireland
"""

import argparse
import time

import boto3
import psycopg

STALE_AFTER_S = 3.0

# Newest row per load-generator host, then summed per Region below.
LATEST = """
SELECT DISTINCT ON (origin, gen)
       origin, workers, commits, p99_ms, conflicts, last_order_id,
       extract(epoch FROM clock_timestamp() - ts) AS age_s
FROM demo_stats
WHERE ts > clock_timestamp() - interval '30 seconds'
ORDER BY origin, gen, ts DESC
"""

FOUND = "SELECT 1 FROM demo_orders WHERE order_id = %s"


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", required=True)
    p.add_argument("--region", required=True)
    p.add_argument("--label", required=True, help="this Region's name on screen, e.g. Ireland")
    p.add_argument("--interval", type=float, default=1.0)
    cfg = p.parse_args()

    token = boto3.client("dsql", region_name=cfg.region).generate_db_connect_admin_auth_token(cfg.host, cfg.region, 3600)
    conn = psycopg.connect(host=cfg.host, user="admin", dbname="postgres", password=token,
                           sslmode="require", autocommit=True, connect_timeout=10)

    clear = "\033[H\033[J"
    while True:
        t0 = time.monotonic()
        rows = conn.execute(LATEST).fetchall()
        regions = {}
        for origin, workers, commits, p99, conflicts, last_id, age_s in rows:
            r = regions.setdefault(origin, {"live": 0, "workers": 0, "commits": 0, "p99": None,
                                            "conflicts": 0, "last_id": None, "age": age_s})
            r["age"] = min(r["age"], age_s)
            if age_s > STALE_AFTER_S:
                continue
            r["live"] += 1
            r["workers"] += workers
            r["commits"] += commits
            r["conflicts"] += conflicts
            if p99 is not None:
                r["p99"] = p99 if r["p99"] is None else max(r["p99"], p99)  # worst host
            r["last_id"] = last_id or r["last_id"]
        lines, total = [], 0
        for origin in sorted(regions):
            r = regions[origin]
            if not r["live"]:
                lines.append(f"  {origin:<12} {'no traffic for %ds' % r['age']:>44}")
                continue
            total += r["commits"]
            seen = "-"
            if r["last_id"] is not None:
                seen = "visible here" if conn.execute(FOUND, (r["last_id"],)).fetchone() else "NOT FOUND"
            p99s = f"{r['p99']:7.1f}" if r["p99"] is not None else "      -"
            lines.append(f"  {origin:<12} {r['commits']:>9} {p99s} {r['workers']:>7} {r['conflicts']:>9}   {seen}")
        out = [clear,
               f"  Read through the {cfg.label} endpoint ({cfg.region})   {time.strftime('%H:%M:%S')}", "",
               f"  {'written in':<12} {'orders/s':>9} {'p99 ms':>7} {'conns':>7} {'conflicts':>9}   newest order",
               *(lines or ["  waiting for traffic..."]), "",
               f"  {'both Regions':<12} {total:>9}"]
        print("\n".join(out), flush=True)
        time.sleep(max(0.0, cfg.interval - (time.monotonic() - t0)))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
