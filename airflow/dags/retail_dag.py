from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# Tell Python where your ETL scripts live
sys.path.insert(0, "/root/etl")

# Import your three ETL functions
from scripts.extract import extract
from scripts.transform import transform
from scripts.load import load
from airflow.models import Variable

# ---------------------------
# DEFAULT ARGUMENTS
# ---------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,

    # Read email from Airflow Variables
    "email": [Variable.get("ALERT_EMAIL")],
    "email_on_failure": True,
    "email_on_retry": False,

    # Auto-retry twice, 5 minutes apart
    "retries": 2,
    "retry_delay": timedelta(minutes=5),

    # Kill task if it hangs longer than 10 minutes
    "execution_timeout": timedelta(minutes=10),
}

# ---------------------------
# DEFINE THE DAG
# ---------------------------
with DAG(
    dag_id="retail_etl_pipeline",
    description="Retail API ETL pipeline loading to Snowflake star schema",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    
    # Run every day at 6am UTC
    schedule="0 6 * * *",

    catchup=False,
    tags=["retail", "etl", "snowflake"],

    # Auto-unpause when deployed — never forget again
    is_paused_upon_creation=False,

) as dag:

    # ---------------------------
    # DEFINE TASKS
    # ---------------------------
    task_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    task_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    task_load = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    # ---------------------------
    # SET TASK ORDER
    # ---------------------------
    task_extract >> task_transform >> task_load
