-- Stop both drivers and confirm completion before running this file.
-- Run psql in this folder, without --single-transaction or an enclosing BEGIN.
-- Original reset/seed stay unchanged; stop/schedule/completion rows are retained.
\ir reset.sql
DROP TABLE IF EXISTS demo_latency_histograms;
\ir lambda_control.sql
