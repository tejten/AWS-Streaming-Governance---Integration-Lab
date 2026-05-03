# Example Use Cases

## 1. Real-time CDC Lakehouse Ingestion

**Problem:** Order analysts need current operational data without querying the transactional database.

**Flow:** PostgreSQL/Aurora `sales.orders` changes are captured by AWS DMS and published to `cdc.sales.orders` in MSK. Glue streaming ETL consumes CDC events, collapses updates/deletes into silver state, and writes curated Parquet data.

**You can discuss:** DMS task settings, Kafka partition key choice, full-load plus CDC cutover, replay strategy, idempotent upserts, and checkpointing.

## 2. External Partner Event Integration

**Problem:** Fulfillment status lives outside the core order database.

**Flow:** Partner shipment events land on `external.shipments`; Glue joins the latest shipment event with order state to build `customer_order_status`.

**You can discuss:** Schema contracts, late-arriving events, malformed partner messages, DLQs, and event-time ordering.

## 3. Governed Curated Data Product

**Problem:** Analysts need self-service access while customer data stays controlled.

**Flow:** Curated tables are registered in Glue Data Catalog. Lake Formation LF-tags label data by `domain=commerce` and `sensitivity=internal`. Analysts receive `SELECT` only on approved LF-tag policies; stewards can inspect DQ and metadata.

**You can discuss:** LF-tag governance, column-level tagging, Glue Catalog ownership, and Athena/Redshift Spectrum integration.

## 4. Quality Gate Before Operational Sync

**Problem:** Bad lakehouse records should not poison MongoDB read models.

**Flow:** Glue Data Quality evaluates `customer_order_status` using DQDL rules. If quality passes, the MongoDB sync Lambda upserts records. If quality fails, CloudWatch/SNS alerts fire and the sync can be paused.

**You can discuss:** DQ thresholds, failed-record handling, EventBridge schedules, quality trend metrics, and rollback.

## 5. Lakehouse-to-MongoDB Projection

**Problem:** Customer support APIs need fast document lookups by order ID.

**Flow:** Lambda reads newly published curated JSON/Parquet manifest partitions, uses DynamoDB watermarks for idempotency, and upserts MongoDB documents keyed by `order_id`.

**You can discuss:** Exactly-once vs effectively-once sync, watermarks, retry behavior, duplicate suppression, and source-of-truth boundaries.

## 6. End-to-End Operations

**Problem:** The team needs to keep pipelines healthy.

**Flow:** CloudWatch monitors DMS replication lag, MSK topic throughput/lag, Glue streaming failures, DQ pass rates, Lambda errors, and MongoDB sync counts. SNS sends alerts to operators.

**You can discuss:** Runbooks, SLOs, backpressure, topic retention, replay, consumer lag, and incident response.
