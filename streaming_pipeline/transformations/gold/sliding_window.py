from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    comment="Gold layer: Aggregation based on windows"
)
def transactions_count_by_sliding():
    # Streaming read from silver transactions
    # Cast string transaction_timestamp to proper timestamp for watermark
    transactions = (
        spark.readStream.table("finguard.silver.transactions")
        .withColumn("transaction_ts", F.to_timestamp("transaction_timestamp"))
    )

    # Watermark on the streaming side using the cast timestamp column
    transactions_watermark = transactions.withWatermark("transaction_ts", "5 minutes")

    transactions_count = (
        transactions_watermark.groupBy(
            F.window("transaction_ts", "5 minutes","1 minute")
        )
        .agg(F.count("*").alias("transactions_count"))
        .select(F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                 F.col("transactions_count"))
    )

    return transactions_count