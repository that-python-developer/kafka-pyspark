from pyspark.sql.functions import *
from data_processor import MockDataUserDataProcessor

from app.main.config import *
from app.main.sessions.pyspark_session import get_spark_streaming_session
from app.main.utils.kafka_pyspark_read import read_kafka_to_pyspark_df


def get_raw_df(spark, schema, alias_value):
    """
        :param spark: spark session for the streaming app_old
        :param alias_value: The alias value for the clean data frame
        :return: raw spark dataframe from the kafka consumer
    """
    kafka_topic = DATA_STREAM_ANALYSIS
    kafka_servers = KAFKA_SERVERS

    clean_df = read_kafka_to_pyspark_df(spark, kafka_servers, kafka_topic)

    clean_df = clean_df \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias(alias_value))

    return clean_df


def start():
    spark = get_spark_streaming_session()

    df_clean = get_raw_df(
        spark,
        MockDataUserDataProcessor().MOCK_DATA_USER_SCHEMA,
        MockDataUserDataProcessor().MOCK_DATA_USER_ALIAS
    )

    MockDataUserDataProcessor().get_gender_by_counts(df_clean)
    MockDataUserDataProcessor().get_country_by_counts(df_clean)

    # spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    """
    Entry point of the spark streaming application
    """
    start()
