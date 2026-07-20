from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

KAFKA_BOOTSTRAP_SERVERS = dbutils.secrets.get(scope="finguard secret vault", key="kafka_bootstrap_servers")
KAFKA_TOPIC             = dbutils.secrets.get(scope="finguard secret vault", key="kafka_topic")
API_KEY                 = dbutils.secrets.get(scope="finguard secret vault", key="api_key")
API_SECRET              = dbutils.secrets.get(scope="finguard secret vault", key="api_secret")

JAAS_CONFIG = (
    f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{API_KEY}" password="{API_SECRET}";'
)


@dp.table(name="transactions")
def transactions() -> DataFrame:
    # Defines a Lakeflow Declarative Pipelines table named "transactions" that ingests streaming data from a Kafka topic.
    # The function returns a DataFrame with selected and casted columns from the Kafka stream.
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", JAAS_CONFIG)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr(
            "CAST(key AS STRING) AS event_key",
            "CAST(value AS STRING) AS event_value",
            "topic",
            "partition",
            "offset",
            "timestamp AS kafka_timestamp",
            "current_timestamp() AS _ingested_at"
        )
    )