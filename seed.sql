-- Schema for the recorded demos (separate from the live-demo pizza tables).
-- Run once through either Region's endpoint: psql -f seed.sql
-- Aurora DSQL runs one DDL statement per transaction; psql autocommits each one.

-- Orders get a client-generated random UUID key, so writes spread across the
-- key space and never contend (slide 46: "spread writes").
CREATE TABLE demo_orders (
  order_id   uuid PRIMARY KEY,
  ordered_at timestamptz NOT NULL DEFAULT now(),
  item_id    int  NOT NULL,
  user_id    int  NOT NULL,
  origin     text NOT NULL
);

-- One row that every transaction touches in --hot mode: the "global counter"
-- anti-pattern from slide 46.
CREATE TABLE demo_hot_counter (
  id int PRIMARY KEY,
  n  bigint NOT NULL
);

-- One row per load-generator host per second. The watcher in
-- the other Region reads these to show both Regions' traffic side by side.
CREATE TABLE demo_stats (
  origin        text        NOT NULL,
  gen           text        NOT NULL,  -- which load-generator host; several can share an origin
  ts            timestamptz NOT NULL DEFAULT now(),
  workers       int         NOT NULL,
  commits       int         NOT NULL,
  p50_ms        real,
  p99_ms        real,
  conflicts     int         NOT NULL,
  errors        int         NOT NULL,
  last_order_id uuid,
  PRIMARY KEY (origin, gen, ts)
);

INSERT INTO demo_hot_counter VALUES (1, 0);
