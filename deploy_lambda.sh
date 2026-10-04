#!/usr/bin/env bash
# Human-run deployment only. Requires Frankfurt ARN and an account; Paris is optional.
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
: "${ACCOUNT_ID:?Set the sandbox AWS account ID}"
: "${FRANKFURT_CLUSTER_ARN:?Set the Frankfurt cluster ARN}"
PARIS_CLUSTER_ARN=${PARIS_CLUSTER_ARN:-}
FUNCTION_NAME=${FUNCTION_NAME:-recorded-demo-loadgen}
ROLE_NAME=${ROLE_NAME:-recorded-demo-loadgen-role}
AWS_PROFILE=${AWS_PROFILE:-default}
PYTHON=${PYTHON:-python3.12}
[[ "$ACCOUNT_ID" =~ ^[0-9]{12}$ ]] || { echo 'Invalid account ID' >&2; exit 1; }
[[ "$FUNCTION_NAME" =~ ^[a-zA-Z0-9_-]{1,64}$ && "$ROLE_NAME" =~ ^[a-zA-Z0-9_-]{1,64}$ ]] || exit 1
[[ "$FRANKFURT_CLUSTER_ARN" == arn:aws:dsql:eu-central-1:"$ACCOUNT_ID":cluster/* ]] || exit 1
DEPLOY_REGIONS=(eu-central-1)
CLUSTER_ARNS=("$FRANKFURT_CLUSTER_ARN")
if [[ -n "$PARIS_CLUSTER_ARN" ]]; then
    [[ "$PARIS_CLUSTER_ARN" == arn:aws:dsql:eu-west-3:"$ACCOUNT_ID":cluster/* ]] || exit 1
    DEPLOY_REGIONS+=(eu-west-3)
    CLUSTER_ARNS+=("$PARIS_CLUSTER_ARN")
fi
TASK_TMP=$(mktemp -d "${TMPDIR:-/tmp}/lambda-demo-deploy.XXXXXX")
trap 'rm -rf -- "$TASK_TMP"' EXIT
aws_cli() { aws --profile "$AWS_PROFILE" --no-cli-pager "$@"; }
ACTUAL_ACCOUNT=$(aws_cli sts get-caller-identity --query Account --output text)
[[ "$ACTUAL_ACCOUNT" == "$ACCOUNT_ID" ]] || { echo 'Profile points at a different account' >&2; exit 1; }
# Build anew for each deployment. This helper itself makes no AWS calls.
PYTHON="$PYTHON" bash "$ROOT/build_lambda.sh" "$TASK_TMP/function.zip"
"$PYTHON" - "$TASK_TMP" "${CLUSTER_ARNS[@]}" <<'PY'
import json
from pathlib import Path
import sys

out = Path(sys.argv[1])
(out / 'trust.json').write_text(json.dumps({
    'Version': '2012-10-17', 'Statement': [{
        'Effect': 'Allow', 'Principal': {'Service': 'lambda.amazonaws.com'},
        'Action': 'sts:AssumeRole'}]}))
(out / 'database.json').write_text(json.dumps({
    'Version': '2012-10-17', 'Statement': [{
        'Effect': 'Allow', 'Action': 'dsql:DbConnectAdmin', 'Resource': sys.argv[2:]}]}))
PY
# Distinguish missing resources from access/network failures; never hide those failures.
if aws_cli iam get-role --role-name "$ROLE_NAME" > "$TASK_TMP/role.json" 2> "$TASK_TMP/error"; then
    aws_cli iam update-assume-role-policy --role-name "$ROLE_NAME" --policy-document "file://$TASK_TMP/trust.json"
elif grep -q 'NoSuchEntity' "$TASK_TMP/error"; then
    aws_cli iam create-role --role-name "$ROLE_NAME" --assume-role-policy-document "file://$TASK_TMP/trust.json"
else
    cat "$TASK_TMP/error" >&2
    exit 1
fi
aws_cli iam put-role-policy --role-name "$ROLE_NAME" --policy-name DemoDatabaseAccess --policy-document "file://$TASK_TMP/database.json"
aws_cli iam attach-role-policy --role-name "$ROLE_NAME" --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
aws_cli iam wait role-exists --role-name "$ROLE_NAME"
# Role propagation can lag role-exists. Retry only the specific AssumeRole propagation error for both create and update.
ROLE_ARN=$(aws_cli iam get-role --role-name "$ROLE_NAME" --query Role.Arn --output text)
with_role_retry() {
    for ATTEMPT in 1 2 3 4 5 6; do
        if aws_cli "$@" 2> "$TASK_TMP/error"; then
            return 0
        fi
        if ! grep -q 'role defined for the function cannot be assumed by Lambda' "$TASK_TMP/error"; then
            cat "$TASK_TMP/error" >&2
            return 1
        fi
        sleep 5
    done
    cat "$TASK_TMP/error" >&2
    return 1
}
for REGION in "${DEPLOY_REGIONS[@]}"; do
    if aws_cli lambda get-function --region "$REGION" --function-name "$FUNCTION_NAME" > "$TASK_TMP/function.json" 2> "$TASK_TMP/error"; then
        aws_cli lambda update-function-code --region "$REGION" --function-name "$FUNCTION_NAME" \
            --zip-file "fileb://$TASK_TMP/function.zip" --architectures x86_64
        aws_cli lambda wait function-updated-v2 --region "$REGION" --function-name "$FUNCTION_NAME"
        with_role_retry lambda update-function-configuration --region "$REGION" --function-name "$FUNCTION_NAME" \
            --runtime python3.12 --handler loadgen_lambda.handler --role "$ROLE_ARN" --memory-size 10240 --timeout 900
        aws_cli lambda wait function-updated-v2 --region "$REGION" --function-name "$FUNCTION_NAME"
    elif grep -q 'ResourceNotFoundException' "$TASK_TMP/error"; then
        with_role_retry lambda create-function --region "$REGION" --function-name "$FUNCTION_NAME" \
            --runtime python3.12 --handler loadgen_lambda.handler --role "$ROLE_ARN" \
            --memory-size 10240 --timeout 900 --architectures x86_64 \
            --zip-file "fileb://$TASK_TMP/function.zip"
        aws_cli lambda wait function-active-v2 --region "$REGION" --function-name "$FUNCTION_NAME"
    else
        cat "$TASK_TMP/error" >&2
        exit 1
    fi
    echo "Ready: $FUNCTION_NAME in $REGION (python3.12, x86_64, 10240 MB, 900s)"
done
