from pyspark.sql import SparkSession


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
