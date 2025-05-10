from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'recommendation_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    submit_job = SparkSubmitOperator(
        task_id='run_recommendation_job',
        application='/opt/airflow/dags/recommendation_job.py',  # Ensure this path is correct
        conn_id=None,  # Not required unless using a specific Spark connection in Airflow
        conf={'spark.master': 'spark://spark-master:7077'},  # Define the Spark master URL
        verbose=True,
    )
