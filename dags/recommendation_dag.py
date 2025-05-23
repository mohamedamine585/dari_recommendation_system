from airflow import DAG
from airflow.models import Connection
from airflow.settings import Session
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.utils.db import provide_session


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


@provide_session
def create_spark_connection(session=None):
    conn_id = 'spark_default'
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if existing_conn:
        print(f"Connection '{conn_id}' already exists")
        return

    new_conn = Connection(
        conn_id=conn_id,
        conn_type='spark',
        host='spark-master',
        port=7077,
        schema='spark'  # Optional, for compatibility
    )
    session.add(new_conn)
    session.commit()
    print(f"Created connection '{conn_id}'")


with DAG(
    dag_id='dari_recommendation_dag',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='* * * * *',
    catchup=False,
) as dag:

    # Create Spark connection at DAG init
    create_spark_connection()

    run_spark_job = SparkSubmitOperator(
        task_id='run_recommendation_model',
        application='recommendation_job.py',
        conn_id='spark_default',
        verbose=True,
        conf={
            'spark.driver.extraClassPath': 'mysql-connector-j-9.3.0.jar'
        },
        executor_memory='2g',
        total_executor_cores=2
    )
