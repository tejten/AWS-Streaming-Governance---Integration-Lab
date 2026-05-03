#!/usr/bin/env bash
set -euo pipefail

STACK_NAME="${1:-streamgov-foundation-dev}"

artifact_bucket="$(
  aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs[?OutputKey==`ArtifactBucketName`].OutputValue' \
    --output text
)"

if [[ -z "$artifact_bucket" || "$artifact_bucket" == "None" ]]; then
  echo "Could not resolve ArtifactBucketName from stack $STACK_NAME" >&2
  exit 1
fi

aws s3 cp src/glue/msk_to_lakehouse_stream.py "s3://${artifact_bucket}/jobs/msk_to_lakehouse_stream.py"
aws s3 cp src/glue/quality_gate.py "s3://${artifact_bucket}/jobs/quality_gate.py"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

mkdir -p "$tmp_dir/mongodb_sync" "$tmp_dir/lineage_emitter"
cp src/lambda/mongodb_sync/app.py "$tmp_dir/mongodb_sync/app.py"
cp src/lambda/lineage_emitter/app.py "$tmp_dir/lineage_emitter/app.py"

if [[ "${SKIP_PYMONGO_INSTALL:-0}" != "1" ]]; then
  python3 -m pip install --upgrade --target "$tmp_dir/mongodb_sync" pymongo >/dev/null
fi

(cd "$tmp_dir/mongodb_sync" && zip -q -r ../mongodb_sync.zip .)
(cd "$tmp_dir/lineage_emitter" && zip -q -r ../lineage_emitter.zip .)

aws s3 cp "$tmp_dir/mongodb_sync.zip" "s3://${artifact_bucket}/lambda/mongodb_sync.zip"
aws s3 cp "$tmp_dir/lineage_emitter.zip" "s3://${artifact_bucket}/lambda/lineage_emitter.zip"

echo "Uploaded artifacts to s3://${artifact_bucket}/"
