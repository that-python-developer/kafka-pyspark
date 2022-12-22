from pyspark.sql.functions import to_json, struct


def write_to_kafka_topic(kafka_servers, kafka_topic, pyspark_df):
    pyspark_df.select(to_json(struct("*")).alias("value")) \
        .selectExpr("CAST(value AS STRING)") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("failOnDataLoss", "false") \
        .option("topic", kafka_topic) \
        .save()
