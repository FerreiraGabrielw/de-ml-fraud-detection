from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import pandas as pd
import joblib

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
    .appName("FraudDetectionInference") \
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
# PATHS
# ============================================================

GOLD_PATH = (
    f"s3a://{S3_BUCKET_NAME}/"
    "gold/fraud_features/"
)

CURRENT_BATCH_PATH = (
    f"s3a://{S3_BUCKET_NAME}/"
    "gold/current_predictions_batch/"
)

MODEL_PATH = (
    "/home/jovyan/work/models/"
    "fraud_detection_model.pkl"
)


# ============================================================
# READING GOLD DATASET
# ============================================================

print("Reading Gold layer from S3...")

df_gold = spark.read.parquet(GOLD_PATH)

print(
    f"Gold rows available: "
    f"{df_gold.count():,}"
)


# ============================================================
# CONNECTING TO RDS
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
# READING EXISTING PREDICTIONS FROM RDS
# ============================================================

print("Checking existing predictions in RDS...")

query = """
SELECT trans_num
FROM fraud_predictions
"""


try:

    existing_predictions = pd.read_sql(
        query,
        engine
    )

    existing_count = len(existing_predictions)

    print(
        f"Existing predictions found: "
        f"{existing_count:,}"
    )

    df_existing_predictions = spark.createDataFrame(
        existing_predictions
    )

except Exception:

    print(
        "No previous predictions found."
    )

    df_existing_predictions = None


# ============================================================
# FILTERING ONLY NEW TRANSACTIONS
# ============================================================

print(
    "Filtering transactions without predictions..."
)

if df_existing_predictions is not None:

    df_new_transactions = (

        df_gold.alias("gold")

        .join(

            df_existing_predictions.alias(
                "predictions"
            ),

            on="trans_num",

            how="left_anti"

        )

    )

else:

    df_new_transactions = df_gold


# ============================================================
# BATCH PROCESSING
# ============================================================

BATCH_SIZE = 5000


df_new_transactions = (

    df_new_transactions

    .orderBy(
        col("ingestion_timestamp").asc()
    )

    .limit(BATCH_SIZE)

)


new_rows = df_new_transactions.count()

print(
    f"New transactions selected: "
    f"{new_rows:,}"
)


# ============================================================
# EXIT IF NO NEW DATA
# ============================================================

if new_rows == 0:

    print(
        "No new transactions to infer."
    )

    spark.stop()

    exit()


# ============================================================
# SELECTING FEATURES
# ============================================================

selected_columns = [

    "trans_num",

    "amt",
    "city_pop",
    "lat",
    "long",
    "merch_lat",
    "merch_long",
    "unix_time",
    "transaction_hour",
    "is_weekend",
    "customer_age",
    "high_amount_flag",
    "night_transaction_flag",
    "customer_merchant_distance"

]


df_features = df_new_transactions.select(
    selected_columns
)

print("Feature selection completed.")


# ============================================================
# CONVERTING TO PANDAS
# ============================================================

print("Converting Spark DataFrame to Pandas...")

pdf_features = df_features.toPandas()

print("Conversion completed.")


# ============================================================
# MODEL INPUT DATASET
# ============================================================

model_features = pdf_features.drop(
    columns=["trans_num"]
)


# ============================================================
# LOADING TRAINED MODEL
# ============================================================

print("Loading trained model...")

model = joblib.load(MODEL_PATH)

print("Model successfully loaded.")


# ============================================================
# GENERATING PREDICTIONS
# ============================================================

print("Generating fraud predictions...")

predictions = model.predict(
    model_features
)

probabilities = model.predict_proba(
    model_features
)[:, 1]

print("Inference completed.")


# ============================================================
# BUILDING PREDICTION DATASET
# ============================================================

pdf_result = pdf_features.copy()

pdf_result["fraud_prediction"] = predictions

pdf_result["fraud_probability"] = probabilities

pdf_result["inference_timestamp"] = (
    pd.Timestamp.now()
)


# ============================================================
# CONVERTING BACK TO SPARK
# ============================================================

print(
    "Converting prediction dataset back to Spark..."
)

df_predictions = spark.createDataFrame(
    pdf_result
)

print(
    "Spark DataFrame successfully created."
)


# ============================================================
# WRITING CURRENT BATCH
# ============================================================

print("Writing current prediction batch into S3...")


df_predictions.write \
    .mode("overwrite") \
    .parquet(CURRENT_BATCH_PATH)


print(
    "Current prediction batch successfully saved."
)


# ============================================================
# STOPPING SPARK SESSION
# ============================================================

spark.stop()

print(
    "Fraud inference pipeline completed."
)