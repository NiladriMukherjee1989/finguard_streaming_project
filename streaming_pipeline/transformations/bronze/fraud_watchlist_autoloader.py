from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(name="finguard.bronze_blob.watchlist_raw")
def watchlist_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/finguard/bronze_blob/watchlist_data/watchlist/")
        .withColumn("Ingested_AT", F.current_timestamp())
        .withColumn("metadata_filepath", F.col("_metadata.file_path"))
    )
