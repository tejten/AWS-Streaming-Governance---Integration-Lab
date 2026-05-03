"""Lambda helper for writing pipeline lineage events to S3."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import boto3


s3 = boto3.client("s3")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    bucket = os.environ["LINEAGE_BUCKET"]
    prefix = os.environ.get("LINEAGE_PREFIX", "governance/lineage/")

    detail = event.get("detail", event)
    run_ts = detail.get("run_ts", utc_now())
    run_id = detail.get("run_id") or getattr(context, "aws_request_id", "manual")
    key = f"{prefix.rstrip('/')}/run_date={run_ts[:10]}/{run_id}.json"

    lineage = {
        "run_id": run_id,
        "run_ts": run_ts,
        "producer": detail.get("producer", "unknown"),
        "inputs": detail.get("inputs", []),
        "outputs": detail.get("outputs", []),
        "quality": json.dumps(detail.get("quality", {}), sort_keys=True),
        "metrics": json.dumps(detail.get("metrics", {}), sort_keys=True),
    }

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(lineage, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )
    return {"bucket": bucket, "key": key, "lineage": lineage}
