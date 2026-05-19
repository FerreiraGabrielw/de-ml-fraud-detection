from pyspark.sql import SparkSession

import pandas as pd

from sqlalchemy import create_engine

import os
from dotenv import load_dotenv


# ============================================================
# LOADING ENVIRONMENT VARIABLES
# ============================================================

load_dotenv("/home/jovyan/work/.env")

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

RDS_HOST = os.getenv("RDS_HOST")
RDS_PORT = os.getenv("RDS_PORT")
RDS_DATABASE = os.getenv("RDS_DATABASE")
RDS_USER = os.getenv("RDS_USER")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")


# ============================================================
# SPARK SESSION
# ============================================================

spark = SparkSession.builder \
    .appName("WritePredictionsToRDS") \
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
    .config(
        "spark.executor.heartbeatInterval",
        "60s"
    ) \
    .config(
        "spark.network.timeout",
        "300s"
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
# S3 PATH
# ============================================================

CURRENT_BATCH_PATH = (
    f"s3a://{S3_BUCKET_NAME}/"
    "gold/current_predictions_batch/"
)


# ============================================================
# READING CURRENT BATCH
# ============================================================

print("Reading current prediction batch from S3...")

df_predictions = spark.read.parquet(
    CURRENT_BATCH_PATH
)

batch_rows = df_predictions.count()

print(
    f"Prediction batch rows loaded: "
    f"{batch_rows:,}"
)


# ============================================================
# EXIT IF NO DATA
# ============================================================

if batch_rows == 0:

    print(
        "No prediction batch found."
    )

    spark.stop()

    exit()


# ============================================================
# CONVERTING TO PANDAS
# ============================================================

print("Converting Spark DataFrame to Pandas...")

pdf_predictions = df_predictions.toPandas()

print("Conversion completed.")


# ============================================================
# RDS CONNECTION
# ============================================================

print("Connecting to Amazon RDS...")


engine = create_engine(

    f"postgresql+psycopg2://{RDS_USER}:"
    f"{RDS_PASSWORD}@"
    f"{RDS_HOST}:{RDS_PORT}/"
    f"{RDS_DATABASE}"

)

print("RDS connection established.")


# ============================================================
# WRITING BATCH INTO RDS
# ============================================================

print("Writing prediction batch into RDS...")


pdf_predictions.to_sql(
    name="fraud_predictions",
    con=engine,
    if_exists="append",
    index=False,
    method="multi",
    chunksize=5000
)


print(
    "Prediction batch successfully written into RDS."
)


# ============================================================
# STOPPING SPARK SESSION
# ============================================================

spark.stop()

print(
    "write_predictions_to_rds pipeline completed."
)