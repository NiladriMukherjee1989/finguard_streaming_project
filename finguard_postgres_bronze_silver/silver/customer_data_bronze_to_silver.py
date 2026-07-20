from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="customers",
    comment="Silver layer customers table with data quality checks applied"
)
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_all({
    "valid_first_name": "first_name IS NOT NULL",
    "valid_last_name": "last_name IS NOT NULL",
    "valid_email": "email IS NOT NULL AND email LIKE '%@%'",
    "valid_age": "age IS NULL OR (age >= 0 AND age <= 120)",
    "valid_annual_income": "annual_income IS NULL OR annual_income >= 0",
    "valid_risk_score": "risk_score IS NULL OR (risk_score >= 0 AND risk_score <= 100)",
    "valid_transaction_limit": "transaction_limit IS NULL OR transaction_limit >= 0"
})
def customers():
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table("finguard.bronze_postgres.customers")
        .withColumn("account_open_date", F.to_date(F.col("account_open_date")))
        .withColumn("silver_ingest_TS", F.current_timestamp())
    )