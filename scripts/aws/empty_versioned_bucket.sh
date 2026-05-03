#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/aws/empty_versioned_bucket.sh BUCKET_NAME [AWS_CLI_GLOBAL_ARGS...]

Deletes all current objects, previous versions, and delete markers from an S3
bucket. Extra arguments are passed to every aws command, for example:

  scripts/aws/empty_versioned_bucket.sh my-bucket --profile demo --region us-east-1
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 1 ]]; then
  usage >&2
  exit 2
fi

BUCKET_NAME="$1"
shift
AWS_ARGS=("$@")

log() {
  printf '[empty-bucket] %s\n' "$*"
}

if ! command -v aws >/dev/null 2>&1; then
  echo "aws CLI is required." >&2
  exit 127
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required." >&2
  exit 127
fi

if ! aws "${AWS_ARGS[@]}" s3api head-bucket --bucket "$BUCKET_NAME" >/dev/null 2>&1; then
  log "Bucket s3://${BUCKET_NAME} was not found or is not accessible; skipping."
  exit 0
fi

log "Removing current objects from s3://${BUCKET_NAME}"
aws "${AWS_ARGS[@]}" s3 rm "s3://${BUCKET_NAME}" --recursive >/dev/null || true

while true; do
  payload_file="$(mktemp)"
  object_count="$(
    aws "${AWS_ARGS[@]}" s3api list-object-versions --bucket "$BUCKET_NAME" --output json |
      python3 -c '
import json
import sys

payload_path = sys.argv[1]
data = json.load(sys.stdin)
objects = []

for collection_name in ("Versions", "DeleteMarkers"):
    for item in data.get(collection_name) or []:
        objects.append({"Key": item["Key"], "VersionId": item["VersionId"]})

objects = objects[:1000]
with open(payload_path, "w", encoding="utf-8") as handle:
    json.dump({"Objects": objects, "Quiet": True}, handle)

print(len(objects))
' "$payload_file"
  )"

  if [[ "$object_count" == "0" ]]; then
    rm -f "$payload_file"
    break
  fi

  log "Deleting ${object_count} object versions/delete markers from s3://${BUCKET_NAME}"
  aws "${AWS_ARGS[@]}" s3api delete-objects \
    --bucket "$BUCKET_NAME" \
    --delete "file://${payload_file}" >/dev/null
  rm -f "$payload_file"
done

log "Bucket s3://${BUCKET_NAME} is empty."
