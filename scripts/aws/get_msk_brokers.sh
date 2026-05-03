#!/usr/bin/env bash
set -euo pipefail

STACK_NAME="${1:-streamgov-msk-dev}"

cluster_arn="$(
  aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs[?OutputKey==`MskClusterArn`].OutputValue' \
    --output text
)"

if [[ -z "$cluster_arn" || "$cluster_arn" == "None" ]]; then
  echo "Could not resolve MskClusterArn from stack $STACK_NAME" >&2
  exit 1
fi

aws kafka get-bootstrap-brokers --cluster-arn "$cluster_arn"
