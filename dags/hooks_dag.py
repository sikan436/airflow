from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import logging
import csv
default_args={
    'owner':'siku',
    'retries':5,
    'retry_delay':timedelta(minutes=1)
}

def postgres_to_s3(ds_nodash):
    hook=PostgresHook(postgres_conn_id='airflow_conn')
    conn=hook.get_conn()
    cursor=conn.cursor()
    cursor.execute("select * from orders where date <='20220501' ")
    with open(f'dags/get_orders_{ds_nodash}.csv','w') as f:
        csv.writer=csv.writer(f)
        csv.writer.writerow([i[0] for i in cursor.description])
        csv.writer.writerows(cursor)
        cursor.close()
        conn.close()
        logging.info(f"saving the file get_orders_{ds_nodash}.csv from postgres")

    s3_hook=S3Hook(aws_conn_id='minio_conn')
    s3_hook.load_file(
        filename=f'dags/get_orders_{ds_nodash}.csv',
        key=f'orders/{ds_nodash}.csv',
        bucket_name='airflow',
        replace=True
    )

with DAG (
    dag_id='hooks_dag_v3',
    start_date=datetime(2024,7,24),
    schedule_interval='@daily',
    default_args=default_args

) as dag:
    task1=PythonOperator(
        task_id='hook_task',
        python_callable=postgres_to_s3
        )
    
    task1