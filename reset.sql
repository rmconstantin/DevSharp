-- Start a fresh take. DELETE is capped at 3,000 rows per transaction and
-- TRUNCATE isn't supported, so drop and recreate instead.
DROP TABLE IF EXISTS demo_orders;
DROP TABLE IF EXISTS demo_hot_counter;
DROP TABLE IF EXISTS demo_stats;
\i seed.sql
