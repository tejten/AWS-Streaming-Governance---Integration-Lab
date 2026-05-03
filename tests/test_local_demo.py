import tempfile
import unittest
from pathlib import Path

from src.local_demo.run_demo import (
    apply_order_cdc,
    build_curated_orders,
    latest_shipments,
    run_pipeline,
    validate_curated_orders,
)


class LocalDemoTests(unittest.TestCase):
    def test_cdc_collapse_marks_delete_and_keeps_latest_update(self):
        events = [
            {
                "metadata": {"operation": "insert", "timestamp": "2026-01-01T00:00:00Z"},
                "data": {"order_id": "O-1", "status": "PLACED", "amount": 1, "currency": "USD"},
            },
            {
                "metadata": {"operation": "update", "timestamp": "2026-01-01T00:01:00Z"},
                "data": {"order_id": "O-1", "status": "SHIPPED", "amount": 1, "currency": "USD"},
            },
            {
                "metadata": {"operation": "delete", "timestamp": "2026-01-01T00:02:00Z"},
                "data": {"order_id": "O-2"},
            },
        ]

        rows = apply_order_cdc(events)

        self.assertEqual(rows[0]["status"], "SHIPPED")
        self.assertTrue(rows[1]["is_deleted"])

    def test_quality_rejects_negative_amount(self):
        errors = validate_curated_orders(
            [
                {
                    "order_id": "O-1",
                    "customer_id": "C-1",
                    "amount": -1,
                    "currency": "USD",
                    "order_status": "PLACED",
                }
            ]
        )

        self.assertEqual(errors[0]["field"], "amount")

    def test_pipeline_writes_projection(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "demo"
            summary = run_pipeline(
                Path("sample-data/orders_cdc.jsonl"),
                Path("sample-data/partner_shipments.jsonl"),
                output_dir,
            )

            self.assertTrue(summary["quality_passed"])
            self.assertEqual(summary["curated_orders"], 2)
            self.assertTrue((output_dir / "mongo" / "customer_order_status.json").exists())

    def test_curated_join_uses_latest_shipment(self):
        orders = [
            {
                "order_id": "O-1",
                "customer_id": "C-1",
                "status": "SHIPPED",
                "amount": 1,
                "currency": "USD",
                "updated_at": "2026-01-01T00:01:00Z",
            }
        ]
        shipments = latest_shipments(
            [
                {"order_id": "O-1", "shipment_status": "PENDING", "event_ts": "2026-01-01T00:00:00Z"},
                {"order_id": "O-1", "shipment_status": "IN_TRANSIT", "event_ts": "2026-01-01T00:02:00Z"},
            ]
        )

        curated = build_curated_orders(orders, shipments)

        self.assertEqual(curated[0]["shipment_status"], "IN_TRANSIT")


if __name__ == "__main__":
    unittest.main()
