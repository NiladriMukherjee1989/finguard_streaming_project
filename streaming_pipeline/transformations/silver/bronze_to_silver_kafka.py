from pyspark import pipelines as dp
from pyspark.sql.functions import from_json, col, current_timestamp


@dp.table(name="finguard.silver.transactions")
def transactions():
    json_schema = (
        "transaction_id STRING, customer_id STRING, card_number STRING, "
        "merchant_id STRING, merchant_name STRING, merchant_category STRING, "
        "amount DOUBLE, currency STRING, transaction_type STRING, "
        "payment_channel STRING, device_id STRING, city STRING, "
        "country STRING, transaction_timestamp STRING, "
        "is_international BOOLEAN, status STRING"
    )

    return (
        spark.readStream.table("finguard.bronze_kafka.transactions")
        .withColumn("parsed", from_json(col("event_value"), json_schema))
        .select(
            "event_key",
            "parsed.*",
            "topic",
            "partition",
            "offset",
            col("_ingested_at").alias("bronze_TS"),
        )
        .withColumn("silver_TS",current_timestamp())
    )