#!/usr/bin/env bash
set -euo pipefail

FOUNDATION_STACK="${1:-streamgov-foundation-dev}"
MSK_STACK="${2:-streamgov-msk-dev}"
KAFKA_SECRET_ARN="${3:?Pass the MSK SCRAM secret ARN as the third argument}"
BOOTSTRAP_BROKERS="${4:?Pass SASL/SCRAM bootstrap brokers as the fourth argument}"

artifact_bucket="$(
  aws cloudformation describe-stacks \
    --stack-name "$FOUNDATION_STACK" \
    --query 'Stacks[0].Outputs[?OutputKey==`ArtifactBucketName`].OutputValue' \
    --output text
)"
lake_bucket="$(
  aws cloudformation describe-stacks \
    --stack-name "$FOUNDATION_STACK" \
    --query 'Stacks[0].Outputs[?OutputKey==`DataLakeBucketName`].OutputValue' \
    --output text
)"
role_arn="$(
  aws cloudformation describe-stacks \
    --stack-name "$FOUNDATION_STACK" \
    --query 'Stacks[0].Outputs[?OutputKey==`GlueStreamingJobRoleArn`].OutputValue' \
    --output text
)"
glue_connection="$(
  aws cloudformation describe-stacks \
    --stack-name "$MSK_STACK" \
    --query 'Stacks[0].Outputs[?OutputKey==`GlueConnectionName`].OutputValue' \
    --output text
)"

job_name="${FOUNDATION_STACK/-foundation-/-}-msk-to-lakehouse"

if [[ -z "$glue_connection" || "$glue_connection" == "None" ]]; then
  echo "Could not resolve GlueConnectionName from stack $MSK_STACK" >&2
  exit 1
fi

command_arg="Name=glueetl,ScriptLocation=s3://${artifact_bucket}/jobs/msk_to_lakehouse_stream.py,PythonVersion=3"
default_args="{
    \"--KAFKA_BOOTSTRAP_SERVERS\":\"${BOOTSTRAP_BROKERS}\",
    \"--KAFKA_TOPIC_PATTERN\":\"cdc.sales.orders|external.shipments\",
    \"--KAFKA_AUTH_MODE\":\"SCRAM\",
    \"--KAFKA_SECRET_ARN\":\"${KAFKA_SECRET_ARN}\",
    \"--STARTING_OFFSETS\":\"earliest\",
    \"--WAREHOUSE_PATH\":\"s3://${lake_bucket}\",
    \"--CHECKPOINT_PATH\":\"s3://${lake_bucket}/checkpoints/msk-to-lakehouse/\",
    \"--LINEAGE_PATH\":\"s3://${lake_bucket}/governance/lineage/\",
    \"--WINDOW_SIZE\":\"60 seconds\",
    \"--enable-continuous-cloudwatch-log\":\"true\",
    \"--enable-metrics\":\"true\"
  }"

if aws glue get-job --job-name "$job_name" >/dev/null 2>&1; then
  aws glue update-job \
    --job-name "$job_name" \
    --job-update "{
      \"Role\":\"${role_arn}\",
      \"GlueVersion\":\"4.0\",
      \"WorkerType\":\"G.1X\",
      \"NumberOfWorkers\":2,
      \"Command\":{\"Name\":\"glueetl\",\"ScriptLocation\":\"s3://${artifact_bucket}/jobs/msk_to_lakehouse_stream.py\",\"PythonVersion\":\"3\"},
      \"DefaultArguments\":${default_args},
      \"Connections\":{\"Connections\":[\"${glue_connection}\"]}
    }" >/dev/null
  echo "Updated Glue job: $job_name"
else
  aws glue create-job \
    --name "$job_name" \
    --role "$role_arn" \
    --glue-version "4.0" \
    --worker-type G.1X \
    --number-of-workers 2 \
    --connections "Connections=${glue_connection}" \
    --command "$command_arg" \
    --default-arguments "$default_args" >/dev/null
  echo "Created Glue job: $job_name"
fi

echo "Attached Glue connection: $glue_connection"
