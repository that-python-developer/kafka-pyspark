from pyspark.sql.types import StringType, StructType, StructField, DateType, BooleanType


class MockDataUserDataProcessor:
    def __init__(self):
        self.MOCK_DATA_USER_SCHEMA = StructType(
            [
                StructField("id", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("ip_address", StringType(), True),
                StructField("date", StringType(), True),
                StructField("country", StringType(), True)
            ]
        )

        self.MOCK_DATA_USER_ALIAS = "clean_user_data"

    @staticmethod
    def get_country_by_counts(df_raw):
        """
            :param df_raw: The raw data frame from the kafka stream
            :return: The aggregated data frame for the counts by country for each streaming micro batch
        """
        result_df = df_raw \
            .groupBy("clean_user_data.country") \
            .count() \
            .alias("country_count") \
            .orderBy("country_count.count", ascending=False)

        pandas_df = result_df.toPandas()
        pandas_df.to_csv("D:\\kafka_workspaces\\kafka-pyspark\\app\\data\\output\\mock_data\\country_by_counts.csv")

    @staticmethod
    def get_gender_by_counts(df_raw):
        """
        :param df_raw: The raw data frame from the kafka stream
        :return:  The aggregated data frame for the counts by gender for each streaming micro batch
        """
        result_df = df_raw \
            .groupBy("clean_user_data.gender") \
            .count() \
            .alias("gender_count") \
            .orderBy("gender_count.count", ascending=False)

        pandas_df = result_df.toPandas()
        pandas_df.to_csv("D:\\kafka_workspaces\\kafka-pyspark\\app\\data\\output\\mock_data\\gender_by_counts.csv")
        # result_df.write.options(header='True', delimiter=',') \
        #     .csv("D:\\kafka_workspaces\\kafka-pyspark\\app\\data\\output")


class GithubDataProcessor:
    def __init__(self):
        self.GITHUB_DATA_USER_SCHEMA = StructType(
            [
                StructField("id", StringType(), True),
                StructField("type", StringType(), True),
                StructField("public", BooleanType(), True),
                StructField("created_at", DateType(), True)
            ]
        )

        self.GITHUB_DATA_USER_ALIAS = "clean_user_data"

    @staticmethod
    def get_type_by_counts(df_raw):
        """
            :param df_raw: The raw data frame from the kafka stream
            :return: The aggregated data frame for the counts by country for each streaming micro batch
        """
        result_df = df_raw \
            .groupBy("clean_user_data.type") \
            .count() \
            .alias("repo_count") \
            .orderBy("repo_count.count", ascending=False)

        pandas_df = result_df.toPandas()
        pandas_df.to_csv("D:\\kafka_workspaces\\kafka-pyspark\\app\\data\\output\\github\\type_by_counts.csv")
