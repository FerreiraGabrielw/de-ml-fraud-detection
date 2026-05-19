from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import os
from dotenv import load_dotenv


# ============================================================
# LOADING ENVIRONMENT VARIABLES
# ============================================================

load_dotenv("/home/jovyan/work/.env")

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")


# ============================================================
# SPARK SESSION
# ============================================================

spark = SparkSession.builder \
    .appName("FraudDetectionBronzeToSilver") \
    .config(
        "spark.jars",
        "/home/jovyan/work/jars/hadoop-aws-3.3.4.jar,"
        "/home/jovyan/work/jars/aws-java-sdk-bundle-1.12.262.jar"
    ) \
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    ) \
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    ) \
    .getOrCreate()


# ============================================================
# AWS CONFIGURATION
# ============================================================

spark._jsc.hadoopConfiguration().set(
    "fs.s3a.access.key",
    AWS_ACCESS_KEY
)

spark._jsc.hadoopConfiguration().set(
    "fs.s3a.secret.key",
    AWS_SECRET_KEY
)

spark._jsc.hadoopConfiguration().set(
    "fs.s3a.endpoint",
    "s3.amazonaws.com"
)


# ============================================================
# S3 PATHS
# ============================================================

BRONZE_PATH = (
    f"s3a://{S3_BUCKET_NAME}/"
    "bronze/transactions/"
)

SILVER_PATH = (
    f"s3a://{S3_BUCKET_NAME}/"
    "silver/transactions/"
)


# ============================================================
# READING BRONZE LAYER
# ============================================================

print("Reading Bronze layer from S3...")

df_bronze = spark.read.parquet(BRONZE_PATH)

print(f"Bronze rows loaded: {df_bronze.count():,}")


# ============================================================
# DATA STANDARDIZATION
# ============================================================

print("Applying Silver transformations...")


df_silver = (

    df_bronze

    # Converting transaction timestamp
    .withColumn(
        "trans_date_trans_time",
        to_timestamp("trans_date_trans_time")
    )

    # Creating temporal features
    .withColumn(
        "transaction_year",
        year("trans_date_trans_time")
    )

    .withColumn(
        "transaction_month",
        month("trans_date_trans_time")
    )

    .withColumn(
        "transaction_day",
        dayofmonth("trans_date_trans_time")
    )

    .withColumn(
        "transaction_hour",
        hour("trans_date_trans_time")
    )

    # Weekend transaction flag
    .withColumn(
        "is_weekend",
        when(
            dayofweek("trans_date_trans_time").isin([1, 7]),
            1
        ).otherwise(0)
    )

    # Standardizing amount type
    .withColumn(
        "amt",
        col("amt").cast("double")
    )

)


print(
    f"Silver rows after transformation: "
    f"{df_silver.count():,}"
)


# ============================================================
# WRITING SILVER LAYER
# ============================================================

print("Writing Silver layer into S3...")


df_silver.write \
    .mode("overwrite") \
    .partitionBy(
        "transaction_year",
        "transaction_month"
    ) \
    .parquet(SILVER_PATH)


print("Silver layer successfully updated.")


# ============================================================
# STOPPING SPARK SESSION
# ============================================================

spark.stop()

print("Bronze to Silver pipeline completed.")