"""AWS Glue streaming job: MSK CDC + partner events to governed S3 lakehouse zones.

This script is designed for AWS Glue Spark streaming. It supports MSK SASL/SCRAM
for the DMS path and MSK IAM for clients that have the IAM auth JAR available.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_date, from_json, lit, to_timestamp
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


ARGS = [
    "JOB_NAME",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC_PATTERN",
    "KAFKA_AUTH_MODE",
    "WAREHOUSE_PATH",
    "CHECKPOINT_PATH",
    "WINDOW_SIZE",
    "LINEAGE_PATH",
]


def read_secret(secret_arn: str) -> dict[str, str]:
    response = boto3.client("secretsmanager").get_secret_value(SecretId=secret_arn)
    return json.loads(response["SecretString"])


def kafka_options(args: dict[str, str]) -> dict[str, str]:
    options = {
        "kafka.bootstrap.servers": args["KAFKA_BOOTSTRAP_SERVERS"],
        "subscribePattern": args["KAFKA_TOPIC_PATTERN"],
        "startingOffsets": args.get("STARTING_OFFSETS", "latest"),
        "failOnDataLoss": "false",
    }

    auth_mode = args.get("KAFKA_AUTH_MODE", "SCRAM").upper()
    if auth_mode == "SCRAM":
        secret = read_secret(args["KAFKA_SECRET_ARN"])
        username = secret["username"]
        password = secret["password"]
        options.update(
            {
                "kafka.security.protocol": "SASL_SSL",
                "kafka.sasl.mechanism": "SCRAM-SHA-512",
                "kafka.sasl.jaas.config": (
                    "org.apache.kafka.common.security.scram.ScramLoginModule required "
                    f'username="{username}" password="{password}";'
                ),
            }
        )
    elif auth_mode == "IAM":
        options.update(
            {
                "kafka.security.protocol": "SASL_SSL",
                "kafka.sasl.mechanism": "AWS_MSK_IAM",
                "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
                "kafka.sasl.client.callback.handler.class": (
                    "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
                ),
            }
        )

    return options


DMS_SCHEMA = StructType(
    [
        StructField(
            "metadata",
            StructType(
                [
                    StructField("record-type", StringType()),
                    StructField("operation", StringType()),
                    StructField("schema-name", StringType()),
                    StructField("table-name", StringType()),
                    StructField("timestamp", StringType()),
                    StructField("transaction-id", StringType()),
                ]
            ),
        ),
        StructField(
            "data",
            StructType(
                [
                    StructField("order_id", StringType()),
                    StructField("customer_id", StringType()),
                    StructField("order_ts", StringType()),
                    StructField("status", StringType()),
                    StructField("amount", DoubleType()),
                    StructField("currency", StringType()),
                    StructField("updated_at", StringType()),
                ]
            ),
        ),
    ]
)

SHIPMENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("order_id", StringType()),
        StructField("shipment_status", StringType()),
        StructField("carrier", StringType()),
        StructField("tracking_number", StringType()),
        StructField("event_ts", StringType()),
    ]
)


def write_parquet(df: DataFrame, path: str) -> None:
    (
        df.withColumn("ingest_date", current_date())
        .write.mode("append")
        .format("parquet")
        .partitionBy("ingest_date")
        .save(path)
    )


def emit_lineage(glue_context: GlueContext, args: dict[str, str], batch_id: int, row_count: int) -> None:
    spark = glue_context.spark_session
    event = {
        "run_id": f"{args['JOB_NAME']}-{batch_id}",
        "run_ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "producer": args["JOB_NAME"],
        "inputs": args["KAFKA_TOPIC_PATTERN"].split("|"),
        "outputs": [
            f"{args['WAREHOUSE_PATH'].rstrip('/')}/bronze/dms_cdc/",
            f"{args['WAREHOUSE_PATH'].rstrip('/')}/silver/orders/",
            f"{args['WAREHOUSE_PATH'].rstrip('/')}/curated/customer_order_status/",
        ],
        "row_count": row_count,
    }
    spark.createDataFrame([json.dumps(event)], "string").write.mode("append").text(args["LINEAGE_PATH"])


def process_batch(glue_context: GlueContext, args: dict[str, str], batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    warehouse = args["WAREHOUSE_PATH"].rstrip("/")
    parsed = (
        batch_df.selectExpr("topic", "CAST(value AS STRING) AS raw_json", "timestamp AS kafka_timestamp")
        .withColumn("dms", from_json(col("raw_json"), DMS_SCHEMA))
        .withColumn("shipment", from_json(col("raw_json"), SHIPMENT_SCHEMA))
        .cache()
    )

    write_parquet(parsed.select("topic", "raw_json", "kafka_timestamp"), f"{warehouse}/bronze/dms_cdc/")

    metadata = col("dms.metadata")
    data = col("dms.data")

    order_events = parsed.filter(metadata.getField("table-name") == "orders").select(
        data.getField("order_id").alias("order_id"),
        data.getField("customer_id").alias("customer_id"),
        to_timestamp(data.getField("order_ts")).alias("order_ts"),
        data.getField("status").alias("order_status"),
        data.getField("amount").alias("amount"),
        data.getField("currency").alias("currency"),
        to_timestamp(data.getField("updated_at")).alias("updated_at"),
        metadata.getField("operation").alias("cdc_operation"),
        to_timestamp(metadata.getField("timestamp")).alias("cdc_timestamp"),
        metadata.getField("transaction-id").alias("source_transaction_id"),
    ).filter(col("order_id").isNotNull())

    shipments = parsed.filter(col("topic").contains("shipments")).select(
        col("shipment.event_id").alias("shipment_event_id"),
        col("shipment.order_id").alias("order_id"),
        col("shipment.shipment_status").alias("shipment_status"),
        col("shipment.carrier").alias("carrier"),
        col("shipment.tracking_number").alias("tracking_number"),
        to_timestamp(col("shipment.event_ts")).alias("shipment_event_ts"),
    )

    write_parquet(order_events, f"{warehouse}/silver/orders/")
    write_parquet(shipments, f"{warehouse}/silver/shipments/")

    curated = (
        order_events.filter(col("cdc_operation") != "delete")
        .join(shipments.dropDuplicates(["order_id"]), "order_id", "left")
        .withColumn("curation_ts", lit(datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")))
    )
    write_parquet(curated, f"{warehouse}/curated/customer_order_status/")

    emit_lineage(glue_context, args, batch_id, curated.count())
    parsed.unpersist()


def main() -> None:
    optional_args = ["KAFKA_SECRET_ARN", "STARTING_OFFSETS"]
    available_args = ARGS + [arg for arg in optional_args if f"--{arg}" in sys.argv]
    args = getResolvedOptions(sys.argv, available_args)

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    stream_df = spark.readStream.format("kafka").options(**kafka_options(args)).load()
    query = (
        stream_df.writeStream.foreachBatch(
            lambda batch_df, batch_id: process_batch(glue_context, args, batch_df, batch_id)
        )
        .option("checkpointLocation", args["CHECKPOINT_PATH"])
        .trigger(processingTime=args["WINDOW_SIZE"])
        .start()
    )
    query.awaitTermination()
    job.commit()


if __name__ == "__main__":
    main()
