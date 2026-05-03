#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/aws/bootstrap_postgres_source.sh [STACK_NAME]

Loads the demo sales.orders table into the RDS PostgreSQL source stack.

Environment:
  AWS_PROFILE, AWS_REGION, and AWS_DEFAULT_REGION are honored by the AWS CLI.
  Defaults use stack name streamgov-rds-source-dev.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

STACK_NAME="${1:-streamgov-rds-source-dev}"
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
SQL_FILE="infra/rds/bootstrap_orders.sql"

if [[ ! -f "$SQL_FILE" ]]; then
  echo "Could not find ${SQL_FILE}. Run this from the repository root." >&2
  exit 2
fi

stack_output() {
  local key="$1"
  aws cloudformation describe-stacks \
    --region "$REGION" \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='${key}'].OutputValue | [0]" \
    --output text
}

host="$(stack_output SourceDbEndpoint)"
port="$(stack_output SourceDbPort)"
database="$(stack_output SourceDbName)"
username="$(stack_output SourceDbUser)"
secret_arn="$(stack_output SourceDbSecretArn)"

password="$(
  aws secretsmanager get-secret-value \
    --region "$REGION" \
    --secret-id "$secret_arn" \
    --query SecretString \
    --output text |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["password"])'
)"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

python3 -m pip install --quiet --target "$tmp_dir" pg8000

PYTHONPATH="$tmp_dir" python3 - "$host" "$port" "$database" "$username" "$password" "$SQL_FILE" <<'PY'
import sys
from pathlib import Path

import pg8000.native

host, port, database, username, password, sql_file = sys.argv[1:]

conn = pg8000.native.Connection(
    user=username,
    password=password,
    host=host,
    port=int(port),
    database=database,
    timeout=30,
    ssl_context=True,
)

try:
    sql = Path(sql_file).read_text(encoding="utf-8")
    results = conn.run(sql)
finally:
    conn.close()

print("Loaded demo orders into sales.orders")
if results:
    print(results)
PY
