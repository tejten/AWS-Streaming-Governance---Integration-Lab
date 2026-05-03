#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/aws/teardown_demo.sh [options]

Deletes AWS resources created by the Streaming Governance & Integration demo.

Options:
  --project NAME             Project name used in stack names. Default: streamgov
  --env NAME                 Environment name used in stack names. Default: dev
  --foundation-stack NAME    Override foundation stack name.
  --projections-stack NAME   Override projections stack name.
  --msk-stack NAME           Override MSK stack name.
  --dms-stack NAME           Override DMS stack name.
  --rds-source-stack NAME    Override demo RDS source stack name.
  --glue-job NAME            Extra Glue job name to delete. Can be repeated.
  --profile NAME             AWS CLI profile to use.
  --region NAME              AWS region to use.
  --yes, -y                  Do not prompt before deleting.
  --help, -h                 Show this help.

Default stack names:
  streamgov-foundation-dev
  streamgov-projections-dev
  streamgov-msk-dev
  streamgov-dms-dev
  streamgov-rds-source-dev

The script deletes resources in dependency order:
  Glue job -> DMS stack -> RDS source stack -> projection stack -> MSK stack -> S3 contents -> foundation stack
EOF
}

PROJECT_NAME="streamgov"
ENVIRONMENT_NAME="dev"
YES=0
FOUNDATION_STACK_OVERRIDE=""
PROJECTIONS_STACK_OVERRIDE=""
MSK_STACK_OVERRIDE=""
DMS_STACK_OVERRIDE=""
RDS_SOURCE_STACK_OVERRIDE=""
AWS_ARGS=()
EXTRA_GLUE_JOBS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project)
      PROJECT_NAME="${2:?Missing value for --project}"
      shift 2
      ;;
    --env|--environment)
      ENVIRONMENT_NAME="${2:?Missing value for --env}"
      shift 2
      ;;
    --foundation-stack)
      FOUNDATION_STACK_OVERRIDE="${2:?Missing value for --foundation-stack}"
      shift 2
      ;;
    --projections-stack)
      PROJECTIONS_STACK_OVERRIDE="${2:?Missing value for --projections-stack}"
      shift 2
      ;;
    --msk-stack)
      MSK_STACK_OVERRIDE="${2:?Missing value for --msk-stack}"
      shift 2
      ;;
    --dms-stack)
      DMS_STACK_OVERRIDE="${2:?Missing value for --dms-stack}"
      shift 2
      ;;
    --rds-source-stack)
      RDS_SOURCE_STACK_OVERRIDE="${2:?Missing value for --rds-source-stack}"
      shift 2
      ;;
    --glue-job)
      EXTRA_GLUE_JOBS+=("${2:?Missing value for --glue-job}")
      shift 2
      ;;
    --profile)
      AWS_ARGS+=(--profile "${2:?Missing value for --profile}")
      shift 2
      ;;
    --region)
      AWS_ARGS+=(--region "${2:?Missing value for --region}")
      shift 2
      ;;
    --yes|-y)
      YES=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

FOUNDATION_STACK="${FOUNDATION_STACK_OVERRIDE:-${PROJECT_NAME}-foundation-${ENVIRONMENT_NAME}}"
PROJECTIONS_STACK="${PROJECTIONS_STACK_OVERRIDE:-${PROJECT_NAME}-projections-${ENVIRONMENT_NAME}}"
MSK_STACK="${MSK_STACK_OVERRIDE:-${PROJECT_NAME}-msk-${ENVIRONMENT_NAME}}"
DMS_STACK="${DMS_STACK_OVERRIDE:-${PROJECT_NAME}-dms-${ENVIRONMENT_NAME}}"
RDS_SOURCE_STACK="${RDS_SOURCE_STACK_OVERRIDE:-${PROJECT_NAME}-rds-source-${ENVIRONMENT_NAME}}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EMPTY_BUCKET_SCRIPT="${SCRIPT_DIR}/empty_versioned_bucket.sh"

log() {
  printf '[teardown] %s\n' "$*"
}

warn() {
  printf '[teardown] WARNING: %s\n' "$*" >&2
}

aws_call() {
  aws "${AWS_ARGS[@]}" "$@"
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "$1 is required." >&2
    exit 127
  fi
}

stack_exists() {
  aws_call cloudformation describe-stacks --stack-name "$1" >/dev/null 2>&1
}

stack_output() {
  local stack_name="$1"
  local output_key="$2"
  local value

  value="$(
    aws_call cloudformation describe-stacks \
      --stack-name "$stack_name" \
      --query "Stacks[0].Outputs[?OutputKey=='${output_key}'].OutputValue | [0]" \
      --output text 2>/dev/null || true
  )"

  if [[ "$value" == "None" ]]; then
    value=""
  fi

  printf '%s' "$value"
}

delete_stack() {
  local stack_name="$1"

  if ! stack_exists "$stack_name"; then
    log "CloudFormation stack ${stack_name} not found; skipping."
    return
  fi

  log "Deleting CloudFormation stack: ${stack_name}"
  aws_call cloudformation delete-stack --stack-name "$stack_name"

  log "Waiting for ${stack_name} to delete. MSK and DMS stacks can take a while."
  if ! aws_call cloudformation wait stack-delete-complete --stack-name "$stack_name"; then
    warn "Stack deletion did not complete for ${stack_name}."
    aws_call cloudformation describe-stack-events \
      --stack-name "$stack_name" \
      --max-items 20 \
      --query 'StackEvents[?ResourceStatus==`DELETE_FAILED`].[LogicalResourceId,ResourceType,ResourceStatusReason]' \
      --output table || true
    exit 1
  fi
}

delete_glue_job() {
  local job_name="$1"
  local run_ids

  if ! aws_call glue get-job --job-name "$job_name" >/dev/null 2>&1; then
    log "Glue job ${job_name} not found; skipping."
    return
  fi

  run_ids="$(
    aws_call glue get-job-runs \
      --job-name "$job_name" \
      --query "JobRuns[?JobRunState=='RUNNING' || JobRunState=='STARTING' || JobRunState=='STOPPING'].Id" \
      --output text 2>/dev/null || true
  )"

  if [[ -n "$run_ids" && "$run_ids" != "None" ]]; then
    log "Stopping active Glue job runs for ${job_name}: ${run_ids}"
    # shellcheck disable=SC2086
    aws_call glue batch-stop-job-run --job-name "$job_name" --job-run-ids $run_ids >/dev/null || true
  fi

  log "Deleting Glue job: ${job_name}"
  aws_call glue delete-job --job-name "$job_name" >/dev/null
}

stop_dms_task_from_stack() {
  local stack_name="$1"
  local task_arn
  local status

  if ! stack_exists "$stack_name"; then
    return
  fi

  task_arn="$(
    aws_call cloudformation describe-stack-resource \
      --stack-name "$stack_name" \
      --logical-resource-id OrdersCdcTask \
      --query 'StackResourceDetail.PhysicalResourceId' \
      --output text 2>/dev/null || true
  )"

  if [[ -z "$task_arn" || "$task_arn" == "None" ]]; then
    return
  fi

  status="$(
    aws_call dms describe-replication-tasks \
      --filters "Name=replication-task-arn,Values=${task_arn}" \
      --query 'ReplicationTasks[0].Status' \
      --output text 2>/dev/null || true
  )"

  case "$status" in
    running|starting|modifying|moving)
      log "Stopping DMS replication task before stack deletion: ${task_arn}"
      aws_call dms stop-replication-task --replication-task-arn "$task_arn" >/dev/null || true

      for _ in {1..60}; do
        status="$(
          aws_call dms describe-replication-tasks \
            --filters "Name=replication-task-arn,Values=${task_arn}" \
            --query 'ReplicationTasks[0].Status' \
            --output text 2>/dev/null || true
        )"

        case "$status" in
          running|starting|stopping|modifying|moving)
            sleep 10
            ;;
          *)
            break
            ;;
        esac
      done
      ;;
  esac
}

empty_foundation_buckets() {
  local artifact_bucket
  local lake_bucket

  if ! stack_exists "$FOUNDATION_STACK"; then
    log "Foundation stack ${FOUNDATION_STACK} not found; skipping S3 cleanup."
    return
  fi

  artifact_bucket="$(stack_output "$FOUNDATION_STACK" ArtifactBucketName)"
  lake_bucket="$(stack_output "$FOUNDATION_STACK" DataLakeBucketName)"

  if [[ -n "$artifact_bucket" ]]; then
    "$EMPTY_BUCKET_SCRIPT" "$artifact_bucket" "${AWS_ARGS[@]}"
  fi

  if [[ -n "$lake_bucket" ]]; then
    "$EMPTY_BUCKET_SCRIPT" "$lake_bucket" "${AWS_ARGS[@]}"
  fi
}

delete_log_group_if_exists() {
  local log_group_name="$1"
  local matches

  matches="$(
    aws_call logs describe-log-groups \
      --log-group-name-prefix "$log_group_name" \
      --query 'logGroups[].logGroupName' \
      --output text 2>/dev/null || true
  )"

  if printf '%s\n' "$matches" | tr '\t' '\n' | grep -Fxq "$log_group_name"; then
    log "Deleting CloudWatch log group: ${log_group_name}"
    aws_call logs delete-log-group --log-group-name "$log_group_name" >/dev/null || true
  fi
}

force_delete_secret_if_exists() {
  local secret_id="$1"
  local deleted_date

  if ! aws_call secretsmanager describe-secret --secret-id "$secret_id" >/dev/null 2>&1; then
    return
  fi

  deleted_date="$(
    aws_call secretsmanager describe-secret \
      --secret-id "$secret_id" \
      --query 'DeletedDate' \
      --output text 2>/dev/null || true
  )"

  if [[ -n "$deleted_date" && "$deleted_date" != "None" ]]; then
    log "Restoring scheduled-delete secret so it can be purged: ${secret_id}"
    aws_call secretsmanager restore-secret --secret-id "$secret_id" >/dev/null || true
  fi

  log "Force deleting demo secret: ${secret_id}"
  aws_call secretsmanager delete-secret \
    --secret-id "$secret_id" \
    --force-delete-without-recovery >/dev/null || true
}

require_command aws
require_command python3

if [[ ! -x "$EMPTY_BUCKET_SCRIPT" ]]; then
  echo "Missing executable helper: ${EMPTY_BUCKET_SCRIPT}" >&2
  exit 1
fi

caller_arn="$(aws_call sts get-caller-identity --query Arn --output text 2>/dev/null || true)"

cat <<EOF
This will delete AWS resources for:
  Project:     ${PROJECT_NAME}
  Environment: ${ENVIRONMENT_NAME}
  Caller:      ${caller_arn:-unknown}

Stacks:
  ${DMS_STACK}
  ${RDS_SOURCE_STACK}
  ${PROJECTIONS_STACK}
  ${MSK_STACK}
  ${FOUNDATION_STACK}

Also deletes:
  Glue job candidates:
    ${PROJECT_NAME}-msk-to-lakehouse
    ${PROJECT_NAME}-${ENVIRONMENT_NAME}-msk-to-lakehouse
  Foundation S3 bucket contents before deleting the foundation stack
  Lambda log groups for the demo projection functions
  Demo Secrets Manager secrets if they remain in recovery
EOF

if [[ "$YES" != "1" ]]; then
  if [[ ! -t 0 ]]; then
    echo "Refusing to continue without an interactive terminal. Re-run with --yes to confirm." >&2
    exit 2
  fi

  read -r -p "Type delete to continue: " confirmation
  if [[ "$confirmation" != "delete" ]]; then
    echo "Canceled."
    exit 0
  fi
fi

delete_glue_job "${PROJECT_NAME}-msk-to-lakehouse"
delete_glue_job "${PROJECT_NAME}-${ENVIRONMENT_NAME}-msk-to-lakehouse"

for job_name in "${EXTRA_GLUE_JOBS[@]}"; do
  delete_glue_job "$job_name"
done

stop_dms_task_from_stack "$DMS_STACK"
delete_stack "$DMS_STACK"
delete_stack "$RDS_SOURCE_STACK"
delete_stack "$PROJECTIONS_STACK"

delete_log_group_if_exists "/aws/lambda/${PROJECT_NAME}-${ENVIRONMENT_NAME}-mongodb-sync"
delete_log_group_if_exists "/aws/lambda/${PROJECT_NAME}-${ENVIRONMENT_NAME}-lineage-emitter"

delete_stack "$MSK_STACK"
delete_log_group_if_exists "/aws/msk/${PROJECT_NAME}-${ENVIRONMENT_NAME}"
empty_foundation_buckets
delete_stack "$FOUNDATION_STACK"

force_delete_secret_if_exists "${PROJECT_NAME}/${ENVIRONMENT_NAME}/mongodb"
force_delete_secret_if_exists "${PROJECT_NAME}/${ENVIRONMENT_NAME}/rds/orders-source"
force_delete_secret_if_exists "AmazonMSK_${PROJECT_NAME}_${ENVIRONMENT_NAME}_dms"

log "AWS demo teardown complete."
