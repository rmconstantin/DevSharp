-- Rerun this file to add the new tables. Keep all control tables between takes.
-- Each statement runs in its own transaction, like seed.sql.
CREATE TABLE IF NOT EXISTS demo_lambda_runs (
    run_id uuid PRIMARY KEY,
    stop boolean NOT NULL,
    lease_until timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS demo_lambda_schedule (
    run_id uuid PRIMARY KEY,
    phase integer NOT NULL,
    active integer NOT NULL
);
-- A logical slot has one owner. A duplicate Invoke cannot start more workers.
-- The handler writes progress/completion; the driver reads it, independent of HTTP.
CREATE TABLE IF NOT EXISTS demo_lambda_invocations (
    run_id uuid NOT NULL,
    slot text NOT NULL,
    owner text NOT NULL,
    status text NOT NULL,
    phase integer NOT NULL,
    workers integer NOT NULL,
    first_done integer NOT NULL,
    summary text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, slot)
);

-- Compact JSON text holds [lower_ms, count] pairs; no SQL arrays.
-- Closed UTC seconds are immutable; generators include a unique invocation ID.
CREATE TABLE IF NOT EXISTS demo_latency_histograms (
    origin text NOT NULL,
    gen text NOT NULL,
    closed_second bigint NOT NULL,
    steady_histogram text NOT NULL,
    first_histogram text NOT NULL,
    PRIMARY KEY (origin, gen, closed_second)
);
