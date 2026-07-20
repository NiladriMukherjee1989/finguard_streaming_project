from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    comment="Gold layer: streaming transactions matched against watchlist entities for fraud detection"
)
def fraud_watchlist_streaming():
    # Streaming read from silver transactions
    # Cast string transaction_timestamp to proper timestamp for watermark
    transactions = (
        spark.readStream.table("finguard.silver.transactions")
        .withColumn("transaction_ts", F.to_timestamp("transaction_timestamp"))
    )

    # Batch read from silver watchlist (static reference — no watermark needed)
    watchlist = spark.readStream.table("finguard.silver.watchlist_parsed")

    # Watermark on the streaming side using the cast timestamp column
    transactions_watermark = transactions.withWatermark("transaction_ts", "5 minutes")
    watchlist_watermark = watchlist.withWatermark("effective_from", "5 minutes")

    # Alias both dataframes to disambiguate overlapping columns: city, country, status
    txn = transactions_watermark.alias("transactions_watermark")
    wl = watchlist.alias("watchlist_watermark")

    # Inner join on card_number (transactions) == entity_id (watchlist)
    joined_df = txn.join(
        wl,
        txn["card_number"] == wl["entity_id"],
        "inner"
    ).select(
        F.col("transactions_watermark.transaction_id").alias("event_id"),
        F.col("transactions_watermark.transaction_ts").alias("event_ts"),
        F.col("transactions_watermark.amount").alias("event_value"),
        F.col("transactions_watermark.country").alias("entity_country"),
        F.col("transactions_watermark.city").alias("entity_type"),
        F.col("transactions_watermark.status").alias("entity_status")
            )

    return joined_df

