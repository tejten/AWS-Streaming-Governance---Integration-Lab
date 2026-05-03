#!/usr/bin/env bash
set -euo pipefail

FOUNDATION_STACK="${1:-streamgov-foundation-dev}"
DQ_TARGET="${DQ_TARGET:-latest}"

if [[ "$DQ_TARGET" == "history" ]]; then
  table_output_key="CuratedOrderStatusTableName"
  ruleset_output_key="CuratedOrderStatusRulesetName"
elif [[ "$DQ_TARGET" == "latest" ]]; then
  table_output_key="CuratedOrderStatusLatestTableName"
  ruleset_output_key="CuratedOrderStatusLatestRulesetName"
else
  echo "DQ_TARGET must be latest or history" >&2
  exit 2
fi

database="$(
  aws cloudformation describe-stacks \
    --stack-name "$FOUNDATION_STACK" \
    --query 'Stacks[0].Outputs[?OutputKey==`CuratedDatabaseName`].OutputValue' \
    --output text
)"
table="$(
  aws cloudformation describe-stacks \
    --stack-name "$FOUNDATION_STACK" \
    --query "Stacks[0].Outputs[?OutputKey==\`${table_output_key}\`].OutputValue" \
    --output text
)"
ruleset="$(
  aws cloudformation describe-stacks \
    --stack-name "$FOUNDATION_STACK" \
    --query "Stacks[0].Outputs[?OutputKey==\`${ruleset_output_key}\`].OutputValue" \
    --output text
)"
role_arn="$(
  aws cloudformation describe-stacks \
    --stack-name "$FOUNDATION_STACK" \
    --query 'Stacks[0].Outputs[?OutputKey==`GlueStreamingJobRoleArn`].OutputValue' \
    --output text
)"
bucket="$(
  aws cloudformation describe-stacks \
    --stack-name "$FOUNDATION_STACK" \
    --query 'Stacks[0].Outputs[?OutputKey==`DataLakeBucketName`].OutputValue' \
    --output text
)"

python3 src/glue/quality_gate.py \
  --database "$database" \
  --table "$table" \
  --ruleset "$ruleset" \
  --role-arn "$role_arn" \
  --results-s3-prefix "s3://${bucket}/governance/dq-results/" \
  --wait
