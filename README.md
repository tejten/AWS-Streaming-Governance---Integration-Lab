# AWS Streaming Governance & Integration Lab

This project demonstrates CDC ingestion into Amazon MSK, governed lakehouse curation with AWS Glue Data Catalog and AWS Lake Formation, quality/lineage standards, CloudWatch monitoring, and synchronization of curated analytical data into MongoDB for operational use.

## Scenario

A retail platform needs real-time order status across analytics and operations:

- PostgreSQL/Aurora order changes are captured by AWS DMS and published to MSK as CDC events.
- External shipment events arrive as partner messages on another Kafka topic.
- AWS Glue streaming ETL normalizes and curates the data into S3 lakehouse zones.
- Glue Data Catalog stores metadata; Lake Formation applies tag-based access controls.
- Glue Data Quality validates curated tables before publishing downstream.
- A Lambda projection syncs curated order status records to MongoDB or Amazon DocumentDB.
- CloudWatch, SNS, DQ results, and lineage manifests keep the pipeline observable.

## What This Demonstrates

- **Streaming:** MSK topics, partitions, SCRAM/IAM auth, CDC event design, dead-letter flow.
- **CDC ingestion:** DMS PostgreSQL source to Kafka/MSK target with transaction metadata.
- **Governance:** Glue databases/tables, LF-tags, Lake Formation grants, curated contracts.
- **Quality:** AWS Glue Data Quality DQDL rules and a quality gate runner.
- **Lineage:** Table-level lineage event schema plus an emitter Lambda.
- **Integration:** S3 lakehouse to MongoDB upsert projection with DynamoDB watermarks.
- **Monitoring:** CloudWatch dashboard/metrics hooks and SNS alert target.

## Project Layout

```text
.
├── contracts/                 # Data contract and AWS Glue DQDL rules
├── docs/                      # Architecture, runbook, use cases, interview map
├── infra/cloudformation/      # Deployable AWS-native infrastructure templates
├── infra/dms/                 # DMS table mappings and task settings examples
├── sample-data/               # DMS-like CDC and partner events
├── schemas/                   # JSON Schemas for event contracts
├── scripts/aws/               # Packaging and operational AWS CLI helpers
├── src/glue/                  # Glue streaming and data quality runner scripts
├── src/lambda/                # MongoDB sync and lineage emitter Lambdas
├── src/local_demo/            # Dependency-free local simulation
└── tests/                     # Unit tests for local pipeline behavior
```

## Quick Local Demo

The local demo uses only Python standard library and writes a mock lakehouse plus a mock MongoDB projection under `build/demo`.

```bash
make local-demo
make test
```

Expected outputs:

- `build/demo/lakehouse/bronze/orders_cdc.jsonl`
- `build/demo/lakehouse/silver/orders.jsonl`
- `build/demo/lakehouse/curated/customer_order_status.jsonl`
- `build/demo/mongo/customer_order_status.json`
- `build/demo/governance/lineage_events.jsonl`

## AWS Deployment Path

The AWS templates are intentionally split so you can control cost.

Before deploying the foundation stack, make sure the AWS CLI principal is a Lake Formation data lake administrator in the target region. The foundation stack creates LF-tags and grants Lake Formation permissions, which require Lake Formation admin rights in addition to IAM permissions.

Set your AWS profile and region once before running the examples:

```bash
export AWS_PROFILE=your-aws-profile
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION="$AWS_REGION"
```

1. Deploy the low-cost foundation stack:

   ```bash
   aws cloudformation deploy \
     --stack-name streamgov-foundation-dev \
     --template-file infra/cloudformation/foundation.yml \
     --capabilities CAPABILITY_NAMED_IAM \
     --parameter-overrides ProjectName=streamgov EnvironmentName=dev
   ```

2. Upload Glue and Lambda code artifacts:

   ```bash
   scripts/aws/upload_artifacts.sh streamgov-foundation-dev
   ```

3. Deploy the projection Lambdas:

   ```bash
   aws cloudformation deploy \
     --stack-name streamgov-projections-dev \
     --template-file infra/cloudformation/projections.yml \
     --capabilities CAPABILITY_NAMED_IAM \
     --parameter-overrides \
       ProjectName=streamgov \
       EnvironmentName=dev \
       ArtifactBucketName=$(aws cloudformation describe-stacks --stack-name streamgov-foundation-dev --query 'Stacks[0].Outputs[?OutputKey==`ArtifactBucketName`].OutputValue' --output text) \
       DataLakeBucketName=$(aws cloudformation describe-stacks --stack-name streamgov-foundation-dev --query 'Stacks[0].Outputs[?OutputKey==`DataLakeBucketName`].OutputValue' --output text) \
       MongoDbSecretArn=$(aws cloudformation describe-stacks --stack-name streamgov-foundation-dev --query 'Stacks[0].Outputs[?OutputKey==`MongoDbSecretArn`].OutputValue' --output text) \
       MongoSyncWatermarkTableName=$(aws cloudformation describe-stacks --stack-name streamgov-foundation-dev --query 'Stacks[0].Outputs[?OutputKey==`MongoSyncWatermarkTableName`].OutputValue' --output text) \
       MongoSyncLambdaRoleArn=$(aws cloudformation describe-stacks --stack-name streamgov-foundation-dev --query 'Stacks[0].Outputs[?OutputKey==`MongoSyncLambdaRoleArn`].OutputValue' --output text) \
       LineageEmitterLambdaRoleArn=$(aws cloudformation describe-stacks --stack-name streamgov-foundation-dev --query 'Stacks[0].Outputs[?OutputKey==`LineageEmitterLambdaRoleArn`].OutputValue' --output text)
   ```

4. Deploy MSK only when you are ready for the streaming lab. Provisioned MSK has meaningful hourly cost.

   ```bash
   aws cloudformation deploy \
     --stack-name streamgov-msk-dev \
     --template-file infra/cloudformation/msk-provisioned.yml \
     --capabilities CAPABILITY_NAMED_IAM \
     --parameter-overrides \
       ProjectName=streamgov \
       EnvironmentName=dev \
       VpcId=vpc-xxxxxxxx \
       PrivateSubnetIds=subnet-a,subnet-b,subnet-c \
       ClientCidr=10.0.0.0/16
   ```

5. Fetch MSK bootstrap brokers:

   ```bash
   scripts/aws/get_msk_brokers.sh streamgov-msk-dev
   ```

6. Deploy the optional demo PostgreSQL source database in the same VPC as MSK:

   ```bash
   aws cloudformation deploy \
     --stack-name streamgov-rds-source-dev \
     --template-file infra/cloudformation/rds-postgres-source.yml \
     --parameter-overrides \
       ProjectName=streamgov \
       EnvironmentName=dev \
       VpcId=vpc-xxxxxxxx \
       SubnetIds=subnet-a,subnet-b \
       AdminClientCidr="$(curl -fsS https://checkip.amazonaws.com)/32"
   ```

7. Load the demo `sales.orders` source table:

   ```bash
   scripts/aws/bootstrap_postgres_source.sh streamgov-rds-source-dev
   ```

8. Deploy the DMS lab after you have a PostgreSQL/Aurora source endpoint:

   ```bash
   aws cloudformation deploy \
     --stack-name streamgov-dms-dev \
     --template-file infra/cloudformation/dms-postgres-to-msk.yml \
     --capabilities CAPABILITY_NAMED_IAM \
     --parameter-overrides \
       ProjectName=streamgov \
       EnvironmentName=dev \
       VpcId=vpc-xxxxxxxx \
       SubnetIds=subnet-a,subnet-b,subnet-c \
       MskSecurityGroupId=sg-xxxxxxxx \
       MskBootstrapBrokersSaslScram='b-1...:9096,b-2...:9096,b-3...:9096' \
       SourceDbHost=orders.xxxxxx.us-east-1.rds.amazonaws.com \
       SourceDbName=orders \
       SourceDbUser=cdc_user \
       SourceDbPassword='replace-me' \
       SourceDbSecurityGroupId=sg-xxxxxxxx
   ```

9. Create or update the Glue streaming consumer that reads MSK and writes the governed S3 lakehouse:

   ```bash
   scripts/aws/create_glue_streaming_job.sh \
     streamgov-foundation-dev \
     streamgov-msk-dev \
     "$(aws cloudformation describe-stacks --stack-name streamgov-msk-dev --query 'Stacks[0].Outputs[?OutputKey==`ScramSecretArn`].OutputValue' --output text)" \
     "$(aws kafka get-bootstrap-brokers --cluster-arn "$(aws cloudformation describe-stacks --stack-name streamgov-msk-dev --query 'Stacks[0].Outputs[?OutputKey==`ClusterArn`].OutputValue' --output text)" --query BootstrapBrokerStringSaslScram --output text)"
   ```

10. Start the streaming job when you want CDC events to land in the lakehouse:

    ```bash
    aws glue start-job-run \
      --job-name streamgov-dev-msk-to-lakehouse
    ```

    The job writes:

    - `s3://<lake-bucket>/bronze/dms_cdc/`
    - `s3://<lake-bucket>/silver/orders/`
    - `s3://<lake-bucket>/curated/customer_order_status/`
    - `s3://<lake-bucket>/governance/lineage/`

    Stop the Glue run when you are done demoing the live stream to control cost.

11. Compact the append-style CDC output into the latest-state curated table:

    ```bash
    scripts/aws/compact_customer_order_status_latest.sh streamgov-foundation-dev 2026-05-03
    ```

12. Run Glue Data Quality against the latest-state table:

    ```bash
    scripts/aws/run_quality_gate.sh streamgov-foundation-dev
    ```

    To inspect the append-history table instead, set `DQ_TARGET=history`.

13. Dry-run the MongoDB projection from the certified latest-state table:

    ```bash
    aws lambda invoke \
      --cli-binary-format raw-in-base64-out \
      --function-name streamgov-dev-mongodb-sync \
      --payload '{"dry_run":true}' \
      /tmp/mongodb-sync-dry-run.json

    cat /tmp/mongodb-sync-dry-run.json
    ```

    The EventBridge schedule is disabled by default so the Lambda will not try to connect to MongoDB until you replace the placeholder MongoDB secret and intentionally enable the schedule.

14. Run the certified publish gate as one command:

    ```bash
    scripts/aws/run_certified_projection.sh streamgov-foundation-dev 2026-05-03 --replace
    ```

    The command compacts latest state, runs Glue Data Quality, and invokes MongoDB sync only when the DQ score is `1.0`. It dry-runs MongoDB by default; add `--sync` to perform real upserts.

## AWS Teardown

Delete demo resources in reverse dependency order when you are done. The teardown script removes the Glue streaming job created by `scripts/aws/create_glue_streaming_job.sh`, deletes the DMS/projection/MSK/foundation CloudFormation stacks, empties the demo S3 buckets, removes Lambda log groups for the projection functions, and purges demo Secrets Manager secrets if CloudFormation leaves them in a recovery window.

The teardown is VPC-preserving: the demo stacks reference an existing VPC by parameter, and the script refuses to delete any stack that owns VPC-level resources such as a VPC, subnets, route tables, NAT gateways, internet gateways, or VPC gateway attachments.

```bash
scripts/aws/teardown_demo.sh --project streamgov --env dev
```

For automation, add `--yes` to skip the confirmation prompt. You can also pass `--profile` and `--region` for a specific AWS account and region.

## Design Notes

- The DMS-to-MSK path uses **SASL/SCRAM over TLS** because AWS DMS Kafka target endpoints do not support MSK IAM access control.
- IAM auth is still included for Glue/app clients that can use MSK IAM.
- The local demo intentionally avoids Kafka/Mongo dependencies so you can rehearse the data flow anywhere.
- For a production lakehouse, convert the curated Parquet outputs to Iceberg tables and enforce Lake Formation grants by persona.

## Primary AWS References

- [AWS DMS Kafka target](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.Kafka.html)
- [Amazon MSK IAM access control](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html)
- [AWS Glue streaming ETL jobs](https://docs.aws.amazon.com/glue/latest/dg/add-job-streaming.html)
- [AWS Glue Data Quality](https://docs.aws.amazon.com/glue/latest/dg/glue-data-quality.html)
- [AWS Glue DQDL reference](https://docs.aws.amazon.com/glue/latest/dg/dqdl.html)
- [Lake Formation and Glue Data Catalog](https://docs.aws.amazon.com/en_us/lake-formation/latest/dg/populating-catalog.html)
- [AWS::MSK::Topic CloudFormation](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-msk-topic.html)
