from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def get_spark_session():
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("us-cities-demographics") \
        .getOrCreate()

    return spark


def read_csv_to_spark_df(spark):
    spark_df = spark \
        .read \
        .options(delimiter=';', inferSchema=True, header=True) \
        .csv("D:\\kafka_workspaces\\kafka-pyspark\\app\\data\\input\\us_cities_demographics.csv")

    return spark_df


def gender_wise_population_percentage(spark_df):
    spark_df = spark_df \
        .groupBy("state") \
        .sum("male_population", "female_population", "total_population") \
        .withColumnRenamed("sum(male_population)", "male_population") \
        .withColumnRenamed("sum(female_population)", "female_population") \
        .withColumnRenamed("sum(total_population)", "total_population") \
        .selectExpr(
            "state",
            "round((male_population / total_population * 100), 2) as male_percentage",
            "round((female_population / total_population * 100), 2) as female_percentage"
        ) \
        .orderBy("state")

    spark_df.toPandas().to_csv(
        "D:\\kafka_workspaces\\kafka-pyspark\\app\\data\\output\\us_demographics_pyspark\\us_state_gender_percentage.csv",
        index=False
    )


def spark_transpose(spark_df):
    spark_df = spark_df \
        .groupBy(
            'state', 'state_code', 'city', 'median_age', 'male_population', 'female_population',
            'total_population', 'number_of_veterans', 'foreign_born', 'average_household_size'
        ) \
        .pivot("race") \
        .sum('count') \
        .orderBy("state", "city")

    spark_df = spark_df.na.fill(
        value=0,
        subset=[
            'American Indian and Alaska Native', 'Asian', 'Black or African-American', 'Hispanic or Latino', 'White'
        ]
    ).orderBy("state", "city")
    spark_df.toPandas().to_csv(
        "D:\\kafka_workspaces\\kafka-pyspark\\app\\data\\output\\us_demographics_pyspark\\us_state_race_transpose.csv",
        index=False
    )


if __name__ == '__main__':
    spark = get_spark_session()
    spark_df = read_csv_to_spark_df(spark)

    gender_wise_population_percentage(spark_df)
    spark_transpose(spark_df)
