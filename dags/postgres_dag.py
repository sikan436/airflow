from datetime import datetime,timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args={
    'owner':'siku',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG (
    dag_id='postgres_dag_v2',
    start_date=datetime(2024,7,24),
    schedule_interval='@daily',
    default_args=default_args

) as dag:
    task1= PostgresOperator(
        task_id='create_table',
        postgres_conn_id='airflow_conn',
        sql='''
        create table if not exists dag_runs(
        id int )
        '''
    )

    task2= PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id='airflow_conn',
        sql='''
        insert into dag_runs values(1)
        '''
    )
    task2>>task1