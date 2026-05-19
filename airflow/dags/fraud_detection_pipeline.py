from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator

from docker.types import Mount

from datetime import datetime


# ============================================================
# LOCAL SPARK PROJECT PATH
# ============================================================

SPARK_PROJECT_PATH = (
    "C:/Users/br_vicgab/Desktop/Gabriel/"
    "github/fraud-detection-platform/"
    "de-ml-fraud-detection/spark"
)


# ============================================================
# DOCKER IMAGE
# ============================================================

SPARK_IMAGE = "de-ml-fraud-detection-pyspark"


# ============================================================
# DAG CONFIGURATION
# ============================================================

default_args = {

    "owner": "Gabriel Ferreira",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1

}


# ============================================================
# DAG DEFINITION
# ============================================================

with DAG(

    dag_id="fraud_detection_pipeline",

    default_args=default_args,

    schedule=None,

    catchup=False,

    tags=["fraud", "spark", "ml", "aws"]

) as dag:


    # ========================================================
    # BRONZE TO SILVER
    # ========================================================

    bronze_to_silver = DockerOperator(

        task_id="bronze_to_silver",

        image=SPARK_IMAGE,

        api_version="auto",

        auto_remove=True,

        docker_url="unix://var/run/docker.sock",

        network_mode="de-ml-fraud-detection_fraud_network",

        command="""
        spark-submit
        --jars /home/jovyan/work/jars/hadoop-aws-3.3.4.jar,/home/jovyan/work/jars/aws-java-sdk-bundle-1.12.262.jar
        /home/jovyan/work/jobs/01_bronze_to_silver.py
        """,

        working_dir="/home/jovyan/work",

        mounts=[
            Mount(
                source=SPARK_PROJECT_PATH,
                target="/home/jovyan/work",
                type="bind"
            )
        ],

        mount_tmp_dir=False

    )


    # ========================================================
    # SILVER TO GOLD
    # ========================================================

    silver_to_gold = DockerOperator(

        task_id="silver_to_gold",

        image=SPARK_IMAGE,

        api_version="auto",

        auto_remove=True,

        docker_url="unix://var/run/docker.sock",

        network_mode="de-ml-fraud-detection_fraud_network",

        command="""
        spark-submit
        --jars /home/jovyan/work/jars/hadoop-aws-3.3.4.jar,/home/jovyan/work/jars/aws-java-sdk-bundle-1.12.262.jar
        /home/jovyan/work/jobs/02_silver_to_gold.py
        """,

        working_dir="/home/jovyan/work",

        mounts=[
            Mount(
                source=SPARK_PROJECT_PATH,
                target="/home/jovyan/work",
                type="bind"
            )
        ],

        mount_tmp_dir=False

    )


    # ========================================================
    # FRAUD INFERENCE
    # ========================================================

    fraud_inference = DockerOperator(

        task_id="fraud_inference",

        image=SPARK_IMAGE,

        api_version="auto",

        auto_remove=True,

        docker_url="unix://var/run/docker.sock",

        network_mode="de-ml-fraud-detection_fraud_network",

        command="""
        spark-submit
        --jars /home/jovyan/work/jars/hadoop-aws-3.3.4.jar,/home/jovyan/work/jars/aws-java-sdk-bundle-1.12.262.jar
        /home/jovyan/work/jobs/03_fraud_inference.py
        """,

        working_dir="/home/jovyan/work",

        mounts=[
            Mount(
                source=SPARK_PROJECT_PATH,
                target="/home/jovyan/work",
                type="bind"
            )
        ],

        mount_tmp_dir=False

    )


    # ========================================================
    # WRITE PREDICTIONS TO RDS
    # ========================================================

    write_predictions_to_rds = DockerOperator(

        task_id="write_predictions_to_rds",

        image=SPARK_IMAGE,

        api_version="auto",

        auto_remove=True,

        docker_url="unix://var/run/docker.sock",

        network_mode="de-ml-fraud-detection_fraud_network",

        command="""
        spark-submit
        --jars /home/jovyan/work/jars/hadoop-aws-3.3.4.jar,/home/jovyan/work/jars/aws-java-sdk-bundle-1.12.262.jar
        /home/jovyan/work/jobs/04_write_predictions_to_rds.py
        """,

        working_dir="/home/jovyan/work",

        mounts=[
            Mount(
                source=SPARK_PROJECT_PATH,
                target="/home/jovyan/work",
                type="bind"
            )
        ],

        mount_tmp_dir=False

    )


    # ========================================================
    # TRIGGER FRAUD ALERT LAMBDA
    # ========================================================

    trigger_fraud_alert = BashOperator(

        task_id="trigger_fraud_alert",

        bash_command="""
        aws lambda invoke \
        --function-name fraud-alert-function \
        --payload '{}' \
        /tmp/fraud_alert_response.json
        """

    )


    # ========================================================
    # TASK FLOW
    # ========================================================

    bronze_to_silver \
    >> silver_to_gold \
    >> fraud_inference \
    >> write_predictions_to_rds \
    >> trigger_fraud_alert