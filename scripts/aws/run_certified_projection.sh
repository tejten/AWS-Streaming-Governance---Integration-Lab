#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<USAGE
Usage:
  $0 [foundation-stack] [ingest-date] [--replace] [--sync]

Runs the certified publish sequence:
  1. Compact append-history CDC rows into customer_order_status_latest.
  2. Run Glue Data Quality against customer_order_status_latest.
  3. Invoke MongoDB sync only if DQ completed and the rule score is 1.0.

By default, MongoDB sync runs as a dry-run. Pass --sync to perform real upserts.
USAGE
}

FOUNDATION_STACK="${1:-streamgov-foundation-dev}"
INGEST_DATE="${2:-$(date -u +%F)}"
shift $(( $# >= 1 ? 1 : 0 ))
shift $(( $# >= 1 ? 1 : 0 ))

REPLACE_ARGS=()
SYNC_PAYLOAD='{"dry_run":true}'
while [[ $# -gt 0 ]]; do
  case "$1" in
    --replace)
      REPLACE_ARGS+=(--replace)
      ;;
    --sync)
      SYNC_PAYLOAD='{}'
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      exit 2
      ;;
  esac
  shift
done

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

echo "[certified-projection] compacting latest-state table"
scripts/aws/compact_customer_order_status_latest.sh "$FOUNDATION_STACK" "$INGEST_DATE" "${REPLACE_ARGS[@]}"

echo "[certified-projection] running Glue Data Quality"
DQ_TARGET=latest scripts/aws/run_quality_gate.sh "$FOUNDATION_STACK" > "$tmp_dir/dq-run.json"
cat "$tmp_dir/dq-run.json"

dq_status="$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["Status"])' "$tmp_dir/dq-run.json")"
if [[ "$dq_status" != "SUCCEEDED" ]]; then
  echo "[certified-projection] DQ run did not succeed: ${dq_status}" >&2
  exit 1
fi

result_id="$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["ResultIds"][0])' "$tmp_dir/dq-run.json")"
dq_score="$(aws glue get-data-quality-result --result-id "$result_id" --query Score --output text)"
if ! python3 - "$dq_score" <<'PY'
import sys
sys.exit(0 if float(sys.argv[1]) >= 1.0 else 1)
PY
then
  echo "[certified-projection] DQ score is ${dq_score}; blocking MongoDB sync." >&2
  aws glue get-data-quality-result \
    --result-id "$result_id" \
    --query 'RuleResults[].{Rule:Description,Result:Result}' \
    --output table >&2
  exit 1
fi

echo "[certified-projection] DQ score ${dq_score}; invoking MongoDB sync"
aws lambda invoke \
  --cli-binary-format raw-in-base64-out \
  --function-name streamgov-dev-mongodb-sync \
  --payload "$SYNC_PAYLOAD" \
  "$tmp_dir/mongodb-sync-response.json" >/dev/null

cat "$tmp_dir/mongodb-sync-response.json"
