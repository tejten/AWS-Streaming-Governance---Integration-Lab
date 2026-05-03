"""Dependency-free local simulation of the AWS streaming governance pipeline."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


VALID_ORDER_STATUSES = {"PLACED", "CONFIRMED", "SHIPPED", "CANCELLED", "RETURNED"}
VALID_CURRENCIES = {"USD", "CAD", "EUR"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_number} is not valid JSON") from exc
    return records


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def apply_order_cdc(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse DMS-style change records into the latest order state."""
    state: dict[str, dict[str, Any]] = {}

    for event in events:
        metadata = event.get("metadata", {})
        data = event.get("data", {})
        order_id = data.get("order_id")
        if not order_id:
            continue

        operation = metadata.get("operation", "unknown")
        timestamp = metadata.get("timestamp") or data.get("updated_at")
        current = state.get(order_id, {"order_id": order_id})

        if operation == "delete":
            current.update(
                {
                    "is_deleted": True,
                    "last_operation": operation,
                    "deleted_at": timestamp,
                    "last_cdc_ts": timestamp,
                    "source_transaction_id": metadata.get("transaction-id"),
                }
            )
        else:
            current.update(data)
            current.update(
                {
                    "is_deleted": False,
                    "last_operation": operation,
                    "last_cdc_ts": timestamp,
                    "source_transaction_id": metadata.get("transaction-id"),
                }
            )

        state[order_id] = current

    return sorted(state.values(), key=lambda row: row["order_id"])


def latest_shipments(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for event in events:
        order_id = event.get("order_id")
        if not order_id:
            continue
        existing = latest.get(order_id)
        if existing is None or event.get("event_ts", "") >= existing.get("event_ts", ""):
            latest[order_id] = event
    return latest


def build_curated_orders(
    orders: list[dict[str, Any]], shipments_by_order: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    curated: list[dict[str, Any]] = []
    for order in orders:
        if order.get("is_deleted"):
            continue

        shipment = shipments_by_order.get(order["order_id"], {})
        curated.append(
            {
                "order_id": order.get("order_id"),
                "customer_id": order.get("customer_id"),
                "order_ts": order.get("order_ts"),
                "order_status": order.get("status"),
                "amount": float(order.get("amount", 0)),
                "currency": order.get("currency"),
                "shipment_status": shipment.get("shipment_status", "UNKNOWN"),
                "carrier": shipment.get("carrier"),
                "tracking_number": shipment.get("tracking_number"),
                "updated_at": max(
                    order.get("updated_at", ""),
                    shipment.get("event_ts", ""),
                ),
                "cdc_timestamp": order.get("last_cdc_ts"),
                "source_transaction_id": order.get("source_transaction_id"),
            }
        )

    return sorted(curated, key=lambda row: row["order_id"])


def validate_curated_orders(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    seen: set[str] = set()

    for index, row in enumerate(rows, start=1):
        order_id = row.get("order_id")
        if not order_id:
            errors.append({"row": index, "field": "order_id", "message": "missing order_id"})
        elif order_id in seen:
            errors.append({"row": index, "field": "order_id", "message": "duplicate order_id"})
        else:
            seen.add(order_id)

        if not row.get("customer_id"):
            errors.append({"row": index, "field": "customer_id", "message": "missing customer_id"})

        if row.get("amount", -1) < 0:
            errors.append({"row": index, "field": "amount", "message": "amount must be non-negative"})

        if row.get("currency") not in VALID_CURRENCIES:
            errors.append({"row": index, "field": "currency", "message": "unsupported currency"})

        if row.get("order_status") not in VALID_ORDER_STATUSES:
            errors.append({"row": index, "field": "order_status", "message": "unsupported status"})

    return errors


def upsert_mock_mongo(path: Path, rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    if path.exists():
        existing = json.loads(path.read_text(encoding="utf-8"))
    else:
        existing = {}

    synced_at = utc_now()
    for row in rows:
        document = dict(row)
        document["_id"] = row["order_id"]
        document["synced_at"] = synced_at
        existing[row["order_id"]] = document

    write_json(path, existing)
    return existing


def run_pipeline(order_path: Path, shipment_path: Path, output_dir: Path) -> dict[str, Any]:
    order_events = load_jsonl(order_path)
    shipment_events = load_jsonl(shipment_path)

    bronze_dir = output_dir / "lakehouse" / "bronze"
    silver_dir = output_dir / "lakehouse" / "silver"
    curated_dir = output_dir / "lakehouse" / "curated"
    governance_dir = output_dir / "governance"

    write_jsonl(bronze_dir / "orders_cdc.jsonl", order_events)
    write_jsonl(bronze_dir / "partner_shipments.jsonl", shipment_events)

    silver_orders = apply_order_cdc(order_events)
    write_jsonl(silver_dir / "orders.jsonl", silver_orders)

    curated_orders = build_curated_orders(silver_orders, latest_shipments(shipment_events))
    dq_errors = validate_curated_orders(curated_orders)
    write_jsonl(curated_dir / "customer_order_status.jsonl", curated_orders)
    write_json(governance_dir / "dq_results.json", {"passed": not dq_errors, "errors": dq_errors})

    mongo_projection = upsert_mock_mongo(output_dir / "mongo" / "customer_order_status.json", curated_orders)

    lineage_event = {
        "run_id": f"local-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
        "run_ts": utc_now(),
        "producer": "src.local_demo.run_demo",
        "inputs": [
            {"name": "orders_cdc", "path": str(order_path)},
            {"name": "partner_shipments", "path": str(shipment_path)},
        ],
        "outputs": [
            {"name": "silver_orders", "path": str(silver_dir / "orders.jsonl")},
            {"name": "customer_order_status", "path": str(curated_dir / "customer_order_status.jsonl")},
            {"name": "mongodb_projection", "path": str(output_dir / "mongo" / "customer_order_status.json")},
        ],
        "quality_passed": not dq_errors,
    }
    write_jsonl(governance_dir / "lineage_events.jsonl", [lineage_event])

    return {
        "order_events": len(order_events),
        "shipment_events": len(shipment_events),
        "silver_orders": len(silver_orders),
        "curated_orders": len(curated_orders),
        "mongo_documents": len(mongo_projection),
        "quality_passed": not dq_errors,
        "dq_errors": dq_errors,
        "output_dir": str(output_dir),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local streaming governance demo.")
    parser.add_argument("--orders", type=Path, required=True)
    parser.add_argument("--shipments", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=Path("build/demo"))
    args = parser.parse_args()

    summary = run_pipeline(args.orders, args.shipments, args.out)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
