"""
     Creating a consumer for the kafka prodcuer running inside a docker container
     exposed at localhost:9093.
     Setting spark.streaming.receiver.maxRate
     as could not use default timestamp value available in streams
     as it was difficult to aggregate them.
     Setting scheduler mode as 'FAIR' to run multiple aggregations in a fair mechanism
"""


def read_kafka_to_pyspark_df(spark, kafka_servers, kafka_topic):
    clean_df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("spark.streaming.backpressure.enabled", True) \
        .option("spark.streaming.receiver.maxRate", "2") \
        .option("spark.scheduler.mode", "FAIR") \
        .load()

    return clean_df
