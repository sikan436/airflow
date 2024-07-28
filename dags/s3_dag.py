from airflow import DAG
from datetime import datetime,timedelta
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args={
    'owner':'siku',
    'retry':5,
    'retry_delay':timedelta(minutes=2)
}

with DAG (
    dag_id='s3_dag_v2',
    start_date=datetime(2024,7,26),
    schedule_interval='@daily',
    default_args=default_args
) as dag:
    task1=S3KeySensor(
        task_id='s3_task',
        bucket_name='airflow',
        bucket_key='data.csv',
        aws_conn_id='minio_conn'

 )

    task1
