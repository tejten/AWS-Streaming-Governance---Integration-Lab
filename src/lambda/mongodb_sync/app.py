"""Lambda projection sync from curated lakehouse objects to MongoDB."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Iterable

import boto3

try:
    import pymongo
except ImportError:  # pragma: no cover - Lambda package/layer supplies pymongo.
    pymongo = None


s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager")
dynamodb = boto3.resource("dynamodb")
cloudwatch = boto3.client("cloudwatch")
athena = boto3.client("athena")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def get_mongodb_uri(secret_arn: str) -> str:
    response = secrets.get_secret_value(SecretId=secret_arn)
    payload = json.loads(response["SecretString"])
    return payload["uri"]


def get_watermark(table_name: str, pipeline_name: str) -> str:
    table = dynamodb.Table(table_name)
    response = table.get_item(Key={"pipeline_name": pipeline_name})
    return response.get("Item", {}).get("last_object_key", "")


def set_watermark(table_name: str, pipeline_name: str, object_key: str) -> None:
    table = dynamodb.Table(table_name)
    table.update_item(
        Key={"pipeline_name": pipeline_name},
        UpdateExpression="SET last_object_key = :key, updated_at = :updated_at",
        ExpressionAttributeValues={":key": object_key, ":updated_at": utc_now()},
    )


def append_filter(sql: str, predicate: str) -> str:
    if not predicate:
        return sql
    if " where " in sql.lower():
        return f"{sql} AND {predicate}"
    return f"{sql} WHERE {predicate}"


def list_new_objects(bucket: str, prefix: str, last_key: str) -> list[str]:
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if key > last_key and not key.endswith("/"):
                keys.append(key)
    return sorted(keys)


def read_records(bucket: str, key: str) -> Iterable[dict[str, Any]]:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    if key.endswith(".json"):
        payload = json.loads(body)
        if isinstance(payload, list):
            yield from payload
        else:
            yield payload
        return

    for line in body.splitlines():
        if line.strip():
            yield json.loads(line)


def coerce_athena_value(value: str | None, type_name: str) -> Any:
    if value is None:
        return None
    if type_name in {"double", "float", "real"}:
        return float(value)
    if type_name in {"integer", "int", "bigint", "smallint", "tinyint"}:
        return int(value)
    return value


def athena_rows(query_execution_id: str) -> list[dict[str, Any]]:
    paginator = athena.get_paginator("get_query_results")
    rows: list[dict[str, Any]] = []
    columns: list[dict[str, str]] = []
    saw_header = False

    for page in paginator.paginate(QueryExecutionId=query_execution_id):
        if not columns:
            columns = page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]

        for row in page["ResultSet"].get("Rows", []):
            values = row.get("Data", [])
            if not saw_header:
                saw_header = True
                continue
            record: dict[str, Any] = {}
            for index, column in enumerate(columns):
                cell = values[index] if index < len(values) else {}
                record[column["Name"]] = coerce_athena_value(cell.get("VarCharValue"), column["Type"])
            rows.append(record)

    return rows


def wait_for_athena_query(query_execution_id: str, poll_seconds: int, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = response["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            return
        if state in {"FAILED", "CANCELLED"}:
            raise RuntimeError(status.get("StateChangeReason", f"Athena query {state}"))
        time.sleep(poll_seconds)
    raise TimeoutError(f"Athena query did not finish within {timeout_seconds} seconds: {query_execution_id}")


def read_certified_rows_from_athena(last_snapshot_ts: str) -> tuple[list[dict[str, Any]], str]:
    database = os.environ.get("ATHENA_DATABASE", "streamgov_dev_curated")
    table = os.environ.get("ATHENA_TABLE", "customer_order_status_latest")
    output = os.environ["ATHENA_OUTPUT_LOCATION"]
    poll_seconds = int(os.environ.get("ATHENA_POLL_SECONDS", "5"))
    timeout_seconds = int(os.environ.get("ATHENA_TIMEOUT_SECONDS", "120"))

    columns = [
        "order_id",
        "customer_id",
        "order_ts",
        "order_status",
        "amount",
        "currency",
        "shipment_status",
        "carrier",
        "tracking_number",
        "updated_at",
        "cdc_operation",
        "cdc_timestamp",
        "source_transaction_id",
        "shipment_event_id",
        "shipment_event_ts",
        "curation_ts",
        "source_ingest_date",
        "snapshot_ts",
    ]
    query = f"SELECT {', '.join(columns)} FROM {table}"
    if last_snapshot_ts:
        query = append_filter(query, f"snapshot_ts > TIMESTAMP '{last_snapshot_ts}'")
    query = f"{query} ORDER BY order_id"

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Catalog": "AwsDataCatalog", "Database": database},
        ResultConfiguration={"OutputLocation": output},
    )
    query_execution_id = response["QueryExecutionId"]
    wait_for_athena_query(query_execution_id, poll_seconds, timeout_seconds)
    rows = athena_rows(query_execution_id)
    max_snapshot_ts = max((str(row["snapshot_ts"]) for row in rows if row.get("snapshot_ts")), default=last_snapshot_ts)
    return rows, max_snapshot_ts


def sync_records(
    records: Iterable[dict[str, Any]],
    collection: Any | None,
    *,
    dry_run: bool,
) -> tuple[int, int]:
    upserts = 0
    deletes = 0
    for record in records:
        order_id = record.get("order_id")
        if not order_id:
            continue
        if record.get("is_deleted") or record.get("cdc_operation") == "delete":
            if not dry_run and collection is not None:
                collection.delete_one({"_id": order_id})
            deletes += 1
        else:
            document = dict(record)
            document["_id"] = order_id
            document["synced_at"] = utc_now()
            if not dry_run and collection is not None:
                collection.replace_one({"_id": order_id}, document, upsert=True)
            upserts += 1
    return upserts, deletes


def publish_metrics(namespace: str, upserts: int, deletes: int, processed_objects: int) -> None:
    cloudwatch.put_metric_data(
        Namespace=namespace,
        MetricData=[
            {"MetricName": "MongoUpserts", "Value": upserts, "Unit": "Count"},
            {"MetricName": "MongoDeletes", "Value": deletes, "Unit": "Count"},
            {"MetricName": "ProcessedObjects", "Value": processed_objects, "Unit": "Count"},
        ],
    )


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    dry_run = bool(event.get("dry_run")) or os.environ.get("DRY_RUN", "false").lower() == "true"
    sync_mode = os.environ.get("SYNC_MODE", "s3").lower()
    if not dry_run and pymongo is None:
        raise RuntimeError("pymongo is required. Package it with the Lambda artifact or attach a layer.")

    bucket = os.environ["CURATED_BUCKET"]
    prefix = os.environ.get("CURATED_PREFIX", "curated/customer_order_status/")
    secret_arn = os.environ["MONGODB_SECRET_ARN"]
    watermark_table = os.environ["WATERMARK_TABLE"]
    pipeline_name = os.environ.get("PIPELINE_NAME", "customer_order_status")
    database_name = os.environ.get("MONGODB_DATABASE", "commerce")
    collection_name = os.environ.get("MONGODB_COLLECTION", "customer_order_status")
    metric_namespace = os.environ.get("METRIC_NAMESPACE", "StreamingGovernance")

    last_key = get_watermark(watermark_table, pipeline_name)
    if sync_mode == "athena":
        records, watermark = read_certified_rows_from_athena(last_key)
        collection = None
        if not dry_run:
            client = pymongo.MongoClient(get_mongodb_uri(secret_arn), retryWrites=True)
            collection = client[database_name][collection_name]
        upserts, deletes = sync_records(records, collection, dry_run=dry_run)
        if watermark and not dry_run:
            set_watermark(watermark_table, pipeline_name, watermark)
        if not dry_run:
            publish_metrics(metric_namespace, upserts, deletes, 1 if records else 0)
        return {
            "mode": sync_mode,
            "dry_run": dry_run,
            "records": len(records),
            "upserts": upserts,
            "deletes": deletes,
            "watermark": watermark,
        }

    keys = list_new_objects(bucket, prefix, last_key)
    if not keys:
        if not dry_run:
            publish_metrics(metric_namespace, 0, 0, 0)
        return {"mode": sync_mode, "processed_objects": 0, "upserts": 0, "deletes": 0, "watermark": last_key}

    collection = None
    if not dry_run:
        client = pymongo.MongoClient(get_mongodb_uri(secret_arn), retryWrites=True)
        collection = client[database_name][collection_name]

    upserts = 0
    deletes = 0
    highest_key = last_key
    for key in keys:
        object_upserts, object_deletes = sync_records(read_records(bucket, key), collection, dry_run=dry_run)
        upserts += object_upserts
        deletes += object_deletes
        highest_key = max(highest_key, key)

    if not dry_run:
        set_watermark(watermark_table, pipeline_name, highest_key)
        publish_metrics(metric_namespace, upserts, deletes, len(keys))
    return {
        "mode": sync_mode,
        "dry_run": dry_run,
        "processed_objects": len(keys),
        "upserts": upserts,
        "deletes": deletes,
        "watermark": highest_key,
    }
