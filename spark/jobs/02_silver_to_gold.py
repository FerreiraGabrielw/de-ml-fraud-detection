from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import os
from dotenv import load_dotenv


# ============================================================
# LOADING ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")


# ============================================================
# SPARK SESSION
# ============================================================

spark = SparkSession.builder \
    .appName("FraudDetectionSilverToGold") \
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

SILVER_PATH = (
    f"s3a://{S3_BUCKET_NAME}/"
    "silver/transactions/"
)

GOLD_PATH = (
    f"s3a://{S3_BUCKET_NAME}/"
    "gold/fraud_features/"
)


# ============================================================
# READING SILVER LAYER
# ============================================================

print("Reading Silver layer from S3...")

df_silver = spark.read.parquet(SILVER_PATH)

print(f"Silver rows loaded: {df_silver.count():,}")


# ============================================================
# GOLD FEATURE ENGINEERING
# ============================================================

print("Applying Gold feature engineering...")


df_gold = (

    df_silver

    # Customer age
    .withColumn(
        "customer_age",
        floor(
            datediff(
                current_date(),
                col("dob")
            ) / 365
        )
    )

    # High transaction amount
    .withColumn(
        "high_amount_flag",
        when(col("amt") > 500, 1).otherwise(0)
    )

    # Night transaction flag
    .withColumn(
        "night_transaction_flag",
        when(
            col("transaction_hour").between(0, 5),
            1
        ).otherwise(0)
    )

    # Geographic distance approximation
    .withColumn(
        "customer_merchant_distance",
        sqrt(
            pow(col("lat") - col("merch_lat"), 2) +
            pow(col("long") - col("merch_long"), 2)
        )
    )

)


print(f"Gold rows generated: {df_gold.count():,}")


# ============================================================
# WRITING GOLD LAYER
# ============================================================

print("Writing Gold layer into S3...")


df_gold.write \
    .mode("overwrite") \
    .partitionBy(
        "transaction_year",
        "transaction_month"
    ) \
    .parquet(GOLD_PATH)


print("Gold layer successfully updated.")


# ============================================================
# STOPPING SPARK SESSION
# ============================================================

spark.stop()

print("Silver to Gold pipeline completed.")