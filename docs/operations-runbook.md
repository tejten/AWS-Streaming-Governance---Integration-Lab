# Operations Runbook

## Health Checks

| Component | Signal | What Good Looks Like |
| --- | --- | --- |
| DMS task | Replication task status and CDCLatency metrics | Running, low and stable latency |
| MSK | Broker health, bytes in/out, under-replicated partitions | Brokers healthy, no under-replication |
| Glue streaming | Job run status, batch duration, checkpoint progress | Running, batch time below trigger window |
| Data Quality | DQ evaluation status and failed rule count | Succeeded with zero critical failures |
| MongoDB sync | Lambda errors, throttles, upsert count, watermark age | No errors, watermark advances |
| Lineage | New lineage event per successful curated publish | Event written for every run |

## Common Incidents

### DMS CDC Lag Rising

1. Check source database replication slot/WAL retention.
2. Check MSK broker throughput and topic partition count.
3. Verify DMS replication instance CPU, memory, and free storage.
4. Scale DMS instance or increase Kafka partitions if lag is sustained.

### Glue Streaming Job Failed

1. Inspect CloudWatch logs for schema parse or Kafka auth errors.
2. Verify checkpoint path still exists and is writable.
3. Check MSK bootstrap brokers and security group ingress.
4. Restart from checkpoint. Delete checkpoint only when intentional replay is acceptable.

### Data Quality Failed

1. Open the Glue Data Quality run result.
2. Identify failed DQDL rule and affected partition.
3. Stop or pause MongoDB sync for the table if critical.
4. Backfill corrected records and rerun quality evaluation.

### MongoDB Projection Drift

1. Compare DynamoDB watermark with latest curated object/partition.
2. Check Lambda timeout, memory, and MongoDB connection errors.
3. Re-run sync for the affected prefix after resetting watermark to the last known good object.
4. Validate counts against Athena/Glue table row counts.

## Replay Strategy

- Kafka replay: reset consumer group offsets or start a new Glue checkpoint path.
- Lake replay: rewrite target partitions after staging validation.
- MongoDB replay: reset DynamoDB watermark and upsert curated records again.
- DMS replay: use a new task or CDC start position only after confirming source log availability.

## Cost Controls

- Keep MSK provisioned clusters stopped/deleted when not practicing streaming.
- Use small broker instances for labs and delete DMS replication instances after use.
- Prefer the local demo for interview practice when AWS resources are unnecessary.
- Put S3 lifecycle policies on bronze and governance logs for non-production environments.
