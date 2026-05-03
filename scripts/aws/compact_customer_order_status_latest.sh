#!/usr/bin/env bash
set -euo pipefail

FOUNDATION_STACK="${1:-streamgov-foundation-dev}"
INGEST_DATE="${2:-$(date -u +%F)}"
REPLACE_MODE="${3:-}"

if [[ "$REPLACE_MODE" != "" && "$REPLACE_MODE" != "--replace" ]]; then
  echo "Usage: $0 [foundation-stack] [ingest-date] [--replace]" >&2
  exit 2
fi

stack_output() {
  local output_key="$1"
  aws cloudformation describe-stacks \
    --stack-name "$FOUNDATION_STACK" \
    --query "Stacks[0].Outputs[?OutputKey==\`${output_key}\`].OutputValue" \
    --output text
}

wait_for_query() {
  local query_id="$1"
  local state
  while true; do
    state="$(
      aws athena get-query-execution \
        --query-execution-id "$query_id" \
        --query 'QueryExecution.Status.State' \
        --output text
    )"

    case "$state" in
      SUCCEEDED)
        return 0
        ;;
      FAILED|CANCELLED)
        aws athena get-query-execution \
          --query-execution-id "$query_id" \
          --query 'QueryExecution.Status.StateChangeReason' \
          --output text >&2
        return 1
        ;;
      *)
        sleep 5
        ;;
    esac
  done
}

start_query() {
  local sql="$1"
  aws athena start-query-execution \
    --query-string "$sql" \
    --query-execution-context "Database=${database},Catalog=AwsDataCatalog" \
    --result-configuration "OutputLocation=${athena_output}" \
    --query 'QueryExecutionId' \
    --output text
}

database="$(stack_output CuratedDatabaseName)"
history_table="$(stack_output CuratedOrderStatusTableName)"
latest_table="$(stack_output CuratedOrderStatusLatestTableName)"
artifact_bucket="$(stack_output ArtifactBucketName)"
lake_bucket="$(stack_output DataLakeBucketName)"
athena_output="s3://${artifact_bucket}/athena-results/"
latest_prefix="curated/customer_order_status_latest/"

existing_objects="$(
  aws s3api list-objects-v2 \
    --bucket "$lake_bucket" \
    --prefix "$latest_prefix" \
    --max-keys 1 \
    --query 'KeyCount' \
    --output text
)"

if [[ "$existing_objects" != "0" && "$existing_objects" != "None" ]]; then
  if [[ "$REPLACE_MODE" != "--replace" ]]; then
    echo "Latest-state prefix already has objects: s3://${lake_bucket}/${latest_prefix}" >&2
    echo "Rerun with --replace if you want to rebuild this derived table." >&2
    exit 1
  fi
  aws s3 rm "s3://${lake_bucket}/${latest_prefix}" --recursive
fi

compact_sql="$(cat <<SQL
INSERT INTO ${latest_table}
SELECT
  order_id,
  customer_id,
  order_ts,
  order_status,
  amount,
  currency,
  shipment_status,
  carrier,
  tracking_number,
  updated_at,
  cdc_operation,
  cdc_timestamp,
  source_transaction_id,
  shipment_event_id,
  shipment_event_ts,
  curation_ts,
  ingest_date AS source_ingest_date,
  current_timestamp AS snapshot_ts
FROM (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY order_id
      ORDER BY coalesce(updated_at, cdc_timestamp) DESC, cdc_timestamp DESC
    ) AS row_num
  FROM ${history_table}
  WHERE ingest_date = '${INGEST_DATE}'
    AND order_id IS NOT NULL
    AND cdc_operation <> 'delete'
) ranked
WHERE row_num = 1
SQL
)"

echo "Compacting ${database}.${history_table} into ${database}.${latest_table} for ingest_date=${INGEST_DATE}"
compact_query_id="$(start_query "$compact_sql")"
wait_for_query "$compact_query_id"
echo "Compaction query succeeded: ${compact_query_id}"

count_sql="SELECT order_status, count(*) AS records FROM ${latest_table} GROUP BY order_status ORDER BY order_status"
count_query_id="$(start_query "$count_sql")"
wait_for_query "$count_query_id"
aws athena get-query-results --query-execution-id "$count_query_id" --output table
