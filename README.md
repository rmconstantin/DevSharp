# DevSharp — Aurora DSQL latency demos

Reproduce the latency behaviors shown in the "Inside Amazon Aurora DSQL" talk (Dev#, Gdańsk, 7 Oct 2026) against your **own** Aurora DSQL cluster.

Two things to see for yourself:

1. **The pizza demo** — interactive `psql`: how `SELECT`/`UPDATE` stay in-Region and only `COMMIT` crosses Regions, and how a write conflict surfaces as `40001` at `COMMIT`.
2. **The load-driven dashboard** — a Lambda-backed load generator (`drive.py`) plus a live dashboard (`watch_v2.py`) showing pooled p99 at scale and, across two peered Regions, each Region's writes already visible through the other's endpoint.

> You bring your own DSQL cluster(s) and AWS account. Nothing here contains credentials or account-specific identifiers; every script reads what it needs from flags or environment variables.

## Prerequisites

- An Aurora DSQL cluster (for the multi-Region parts, a peered pair in two Regions).
- `psql` (PostgreSQL 15+ client) and the AWS CLI v2, with credentials for your account.
- Python 3.12 (`drive.py` uses `itertools.pairwise`, which needs 3.10+).
- `pip install -r requirements.txt` into a virtual environment.

The DSQL admin connection uses an IAM auth token as the password; `connect.sh` mints one for you.

## 1. The pizza demo (interactive psql)

Load the schema once, then connect:

```sh
./connect.sh A <your-cluster-endpoint> <your-region>
# in psql:
\i seed.sql
```

**Multi-Region reads and writes** — in a session on Region A:

```sql
SELECT qty FROM items WHERE item_id = 5;              -- a few ms, stays in-Region
BEGIN;
UPDATE items SET qty = qty - 1 WHERE item_id = 5;     -- a few ms, stays in-Region
COMMIT;                                               -- the only cross-Region step
```

Then read the same row through the **other** Region's endpoint — it's already there, no replica lag.

**Conflict and retry** — two sessions updating the same row; the second `COMMIT` gets `40001` (OC000). Retry the whole transaction. See the step-by-step tables in the talk.

Reset between runs with `\i reset.sql` (or `reset_v2.sql`, which also recreates the pooled-latency histogram table).

## 2. The load-driven dashboard

`drive.py` orchestrates a fleet of AWS Lambda invocations (`loadgen_lambda.py`) that open the connections and run the workload; the box running `drive.py` only coordinates and measures, so it stays light. `watch_v2.py` renders the live dashboard.

**Deploy the load-generator Lambda** (requires your account ID and cluster ARN as env vars — nothing is hardcoded):

```sh
export ACCOUNT_ID=<your-12-digit-account-id>
export FRANKFURT_CLUSTER_ARN=arn:aws:dsql:<region-a>:<account>:cluster/<id>
export PARIS_CLUSTER_ARN=arn:aws:dsql:<region-b>:<account>:cluster/<id>   # optional, for multi-Region
./deploy_lambda.sh
```

**Run a ramp** (one Region shown; run a second for multi-Region):

```sh
.venv/bin/python drive.py --function recorded-demo-loadgen \
  --host <region-a-endpoint> --region <region-a> --origin RegionA \
  --ramp 500,2000,9900 --step-seconds 30 --hold-seconds 180 --open-rate 90 --think-ms 250
```

**Watch it** (in another terminal):

```sh
.venv/bin/python watch_v2.py --host <endpoint> --region <region> --label RegionA
```

A note on the numbers: p50/p99 are **pooled** across all generators, and each connection's **first** transaction (which includes TLS + IAM session setup) is excluded by default — so the early ramp isn't skewed by one-time connection cost. Add `--include-first-txn` to see it included.

### A simpler latency probe

`probe_latency.py` uses a single connection over the same IPv4/IAM/TLS path and times whole transactions — handy for a quick in-Region latency number without the Lambda fleet:

```sh
.venv/bin/python probe_latency.py --host <endpoint> --region <region> --transactions 2000
```

## Running from inside the Region

Run the drivers and probe from a host **in the cluster's Region** (an EC2 instance), not a laptop over the public internet — otherwise your network's round trip is added to every measurement, and these demos are about the database's latency, not the network's.

## Tests

```sh
.venv/bin/python -m unittest discover -p 'test_*.py'
```

## Files

- `connect.sh` — open `psql` as admin with a fresh IAM token
- `seed.sql`, `reset.sql`, `reset_v2.sql`, `lambda_control.sql` — schema and resets
- `drive.py` — ramp controller (invokes the Lambda fleet, prints the per-second table)
- `watch_v2.py` / `watch.py` — live dashboards
- `loadgen.py`, `loadgen_lambda.py` — the load generator (local and Lambda handler)
- `pooled_latency.py`, `latency_detail.py`, `lambda_diagnostics.py`, `demo_color.py` — latency pooling, histograms, diagnostics, terminal color
- `probe_latency.py` — single-connection latency probe
- `build_lambda.sh`, `deploy_lambda.sh`, `lambda_requirements.txt` — build and deploy the Lambda
- `requirements.txt`, `test_*.py` — dependencies and tests

## Disclaimer

Sample code for demonstration, provided as-is. Review it before running anything against your own AWS account; it creates and invokes AWS resources (Lambda, IAM role) that may incur cost.
