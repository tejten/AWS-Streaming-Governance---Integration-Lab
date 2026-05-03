# Interview Map

Use this project to answer role-specific questions with concrete examples.

## Kafka / MSK

- Explain topic design: `cdc.sales.orders`, `external.shipments`, `pipeline.dlq`.
- Discuss partition keys: use `order_id` to preserve per-order ordering.
- Explain auth split: SCRAM for DMS compatibility, IAM for AWS clients.
- Talk through replay: consumer group offset reset plus Glue checkpoint strategy.

## CDC

- DMS publishes JSON records with `metadata` and `data`.
- Full-load and CDC can share the same topic, so consumers need operation-aware logic.
- Deletes are represented as tombstone-like events and should be handled explicitly.
- Include transaction/control details when lineage and auditability matter.

## Glue / Lakehouse

- Bronze keeps raw events for replay.
- Silver collapses CDC into normalized latest state.
- Curated is contract-driven and quality-gated.
- Glue streaming jobs use checkpoints rather than bookmarks.

## Governance

- Glue Data Catalog is the metadata source of truth.
- Lake Formation LF-tags express domain and sensitivity.
- Analysts get tag-policy based access; stewards get quality and metadata access.
- Contracts define owners, primary keys, schema, quality rules, and compatibility.

## Data Quality

- DQDL verifies keys, nulls, valid values, positive amounts, and row count.
- Failed critical rules should block MongoDB sync.
- DQ results should be stored to S3 and emitted to CloudWatch.

## MongoDB Sync

- Treat MongoDB as an operational projection, not the lakehouse source of truth.
- Use idempotent upserts by `order_id`.
- Store watermarks in DynamoDB.
- Replays should be safe because documents are replaced by deterministic keys.

## Monitoring

- DMS: task status and CDC latency.
- MSK: broker health, throughput, under-replicated partitions, consumer lag.
- Glue: job failures, batch duration, checkpoint progress.
- DQ: failed rule count and score.
- Lambda: errors, duration, throttles, upsert count.
