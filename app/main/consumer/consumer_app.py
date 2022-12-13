from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from user_data_processor import *

from app.main.config import *


def get_spark_streaming_session():
    """
        :return: Returns a spark streaming object
    """
    scala_version = '2.12'
    spark_version = '3.3.0'
    # TODO: Ensure match above values match the correct versions
    packages = [
        f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
        'org.apache.kafka:kafka-clients:3.2.1'
    ]
    spark = SparkSession.builder \
        .master("local") \
        .appName("kafka-example") \
        .config("spark.jars.packages", ",".join(packages)) \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    return spark


def get_raw_df(spark, schema, alias_value):
    """
            :param spark: spark session for the streaming app_old
            :param alias_value: The alias value for the clean data frame
            :return: raw spark dataframe from the kafka consumer
    """
    """
         Creating a consumer for the kafka prodcuer running inside a docker container 
         exposed at localhost:9093.
         Setting spark.streaming.receiver.maxRate
         as could not use default timestamp value available in streams
         as it was difficult to aggregate them.
         Setting scheduler mode as 'FAIR' to run multiple aggregations in a fair mechanism
    """
    kafka_topic = KAFKA_TOPIC
    kafka_servers = KAFKA_SERVERS

    clean_df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("spark.streaming.backpressure.enabled", True) \
        .option("spark.streaming.receiver.maxRate", "2") \
        .option("spark.scheduler.mode", "FAIR") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema)) \
        .alias(alias_value)

    return clean_df


def start():
    spark = get_spark_streaming_session()

    df_clean = get_raw_df(spark, USER_SCHEMA, USER_ALIAS)

    UserDataProcessor.get_gender_by_counts(df_clean)
    UserDataProcessor.get_country_by_counts(df_clean)

    # spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    """
    Entry point of the spark streaming application
    """
    start()
