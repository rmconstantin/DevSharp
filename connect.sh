#!/usr/bin/env bash
# Usage: ./connect.sh <label> <cluster-endpoint> <region>
#   ./connect.sh A abcdefgh1234.dsql.eu-central-1.on.aws eu-central-1
# Opens an interactive psql as admin with a fresh IAM token (valid 1 hour),
# a labelled prompt, \timing on, and verbose errors so SQLSTATE 40001 and the
# OC000 code show on screen.
# Run it from a host IN the Region (an EC2 instance or similar), not from the
# conference laptop: venue Wi-Fi adds its own round trip to every \timing number.
set -euo pipefail
label="$1"; host="$2"; region="$3"
rc="$(mktemp -t dsql-demo-psqlrc)"
cat > "$rc" <<RC
\set PROMPT1 '[$label] %R%# '
\set PROMPT2 '[$label] %R%# '
\set VERBOSITY verbose
\timing on
\echo 'connected to $region as $label'
RC
export PGPASSWORD="$(aws dsql generate-db-connect-admin-auth-token \
  --hostname "$host" --region "$region" --expires-in 14400)"
export PGSSLMODE=require
export PSQLRC="$rc"
exec psql -h "$host" -U admin -d postgres
