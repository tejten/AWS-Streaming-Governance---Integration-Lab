"""Start and optionally wait for an AWS Glue Data Quality ruleset evaluation."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
import uuid

import boto3


TERMINAL_STATES = {"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"}


def start_quality_run(args: argparse.Namespace) -> str:
    glue = boto3.client("glue")
    if not hasattr(glue, "start_data_quality_ruleset_evaluation_run"):
        return start_quality_run_with_aws_cli(args)

    glue_table = {
        "DatabaseName": args.database,
        "TableName": args.table,
    }
    if args.pushdown_predicate:
        glue_table["AdditionalOptions"] = {"pushDownPredicate": args.pushdown_predicate}

    response = glue.start_data_quality_ruleset_evaluation_run(
        DataSource={"GlueTable": glue_table},
        Role=args.role_arn,
        NumberOfWorkers=args.workers,
        Timeout=args.timeout,
        ClientToken=str(uuid.uuid4()),
        AdditionalRunOptions={
            "CloudWatchMetricsEnabled": True,
            "ResultsS3Prefix": args.results_s3_prefix,
        },
        RulesetNames=[args.ruleset],
    )
    return response["RunId"]


def start_quality_run_with_aws_cli(args: argparse.Namespace) -> str:
    glue_table = {
        "DatabaseName": args.database,
        "TableName": args.table,
    }
    if args.pushdown_predicate:
        glue_table["AdditionalOptions"] = {"pushDownPredicate": args.pushdown_predicate}

    data_source = {"GlueTable": glue_table}
    additional_options = {
        "CloudWatchMetricsEnabled": True,
        "ResultsS3Prefix": args.results_s3_prefix,
    }
    command = [
        "aws",
        "glue",
        "start-data-quality-ruleset-evaluation-run",
        "--data-source",
        json.dumps(data_source),
        "--role",
        args.role_arn,
        "--number-of-workers",
        str(args.workers),
        "--timeout",
        str(args.timeout),
        "--client-token",
        str(uuid.uuid4()),
        "--additional-run-options",
        json.dumps(additional_options),
        "--ruleset-names",
        args.ruleset,
        "--query",
        "RunId",
        "--output",
        "text",
    ]
    return subprocess.check_output(command, text=True).strip()


def wait_for_run(run_id: str, poll_seconds: int) -> dict:
    glue = boto3.client("glue")
    if not hasattr(glue, "get_data_quality_ruleset_evaluation_run"):
        return wait_for_run_with_aws_cli(run_id, poll_seconds)

    while True:
        response = glue.get_data_quality_ruleset_evaluation_run(RunId=run_id)
        if response["Status"] in TERMINAL_STATES:
            return response
        time.sleep(poll_seconds)


def wait_for_run_with_aws_cli(run_id: str, poll_seconds: int) -> dict:
    while True:
        command = [
            "aws",
            "glue",
            "get-data-quality-ruleset-evaluation-run",
            "--run-id",
            run_id,
            "--output",
            "json",
        ]
        response = json.loads(subprocess.check_output(command, text=True))
        if response["Status"] in TERMINAL_STATES:
            return response
        time.sleep(poll_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a Glue Data Quality ruleset evaluation.")
    parser.add_argument("--database", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--ruleset", required=True)
    parser.add_argument("--role-arn", required=True)
    parser.add_argument("--results-s3-prefix", required=True)
    parser.add_argument("--pushdown-predicate")
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--wait", action="store_true")
    parser.add_argument("--poll-seconds", type=int, default=30)
    args = parser.parse_args()

    run_id = start_quality_run(args)
    if args.wait:
        print(json.dumps(wait_for_run(run_id, args.poll_seconds), default=str, indent=2))
    else:
        print(json.dumps({"RunId": run_id}, indent=2))


if __name__ == "__main__":
    main()
