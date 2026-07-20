from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="finguard.silver.watchlist_parsed",
    comment="Silver layer watchlist data: ID columns uppercased, date strings cast to timestamp, audit column added"
)
def watchlist_parsed():
    return (
        spark.readStream
            .option("skipChangeCommits", "true")
            .table("finguard.bronze_blob.watchlist_raw")
            .select(
                # ID columns - uppercased
                F.upper(F.col("entity_id")).alias("entity_id"),
                F.upper(F.col("watchlist_id")).alias("watchlist_id"),
                # String date column cast to timestamp
                F.to_timestamp(F.col("effective_from"), "dd-MMM-yyyy HH:mm:ss").alias("effective_from"),
                # Remaining columns kept as-is
                F.col("action"),
                F.col("city"),
                F.col("country"),
                F.col("reason_code"),
                F.col("reason_description"),
                F.col("reported_by"),
                F.col("reported_source"),
                F.col("risk_level"),
                F.col("status"),
                F.col("watch_type"),
                F.col("_rescued_data"),
                F.col("Ingested_AT"),
                F.col("metadata_filepath"),
                # Audit column
                F.current_timestamp().alias("silver_ingested_TS")
            )
    )
