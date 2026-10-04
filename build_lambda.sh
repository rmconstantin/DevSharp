#!/usr/bin/env bash
# Packaging only. No AWS commands or credentials.
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
PYTHON=${PYTHON:-python3.12}
ZIP_PATH=${1:-"$ROOT/lambda-demo.zip"}
if [[ -e "$ZIP_PATH" ]]; then
    echo "Refusing to overwrite $ZIP_PATH; choose a new output path." >&2
    exit 1
fi
"$PYTHON" -c 'import sys; assert sys.version_info[:2] == (3, 12), "Build with Python 3.12"'
STAGING=$(mktemp -d "${TMPDIR:-/tmp}/lambda-demo-build.XXXXXX")
trap 'rm -rf -- "$STAGING"' EXIT
"$PYTHON" -m pip install --disable-pip-version-check --no-cache-dir \
    --platform manylinux2014_x86_64 --python-version 3.12 --implementation cp --abi cp312 \
    --only-binary=:all: --target "$STAGING/package" -r "$ROOT/lambda_requirements.txt"
cp -- "$ROOT/loadgen.py" "$ROOT/loadgen_lambda.py" "$ROOT/lambda_diagnostics.py" "$ROOT/latency_detail.py" "$ROOT/pooled_latency.py" "$STAGING/package/"
"$PYTHON" -m compileall -q -f --invalidation-mode checked-hash -d /var/task "$STAGING/package"
"$PYTHON" - "$STAGING/package" "$ZIP_PATH" <<'PY'
from pathlib import Path
import sys
import zipfile

root, out = map(Path, sys.argv[1:])
paths = sorted(p for p in root.rglob('*') if p.is_file())
unpacked = sum(p.stat().st_size for p in paths)
with zipfile.ZipFile(out, 'x', zipfile.ZIP_DEFLATED) as z:
    for p in paths:
        z.write(p, p.relative_to(root))
    names = z.namelist()
assert 'loadgen_lambda.py' in names and 'loadgen.py' in names
assert any('psycopg_binary' in n and n.endswith('.so') for n in names)
assert any('x86_64' in n and n.endswith('.so') for n in names)
assert unpacked < 250 * 1024 * 1024
assert out.stat().st_size < 50 * 1024 * 1024
print(f'ZIP: {out}\nCompressed bytes: {out.stat().st_size}\nUncompressed bytes: {unpacked}')
print('Runtime: python3.12; architecture: x86_64; platform: manylinux2014_x86_64')
print('Native contents (listed, NOT imported or executed on macOS):')
for n in names:
    if n.endswith(('.so', '/WHEEL')):
        print(n)
PY
