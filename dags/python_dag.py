from airflow import DAG
from datetime import timedelta,datetime
from airflow.operators.python import PythonOperator

default_args={
    'owner':'siku',
    'retries':5,
    'retry_delay':timedelta(minutes=2)

}

def greet(ti):
    first_name=ti.xcom_pull(task_ids='name',key='first_name')
    last_name=ti.xcom_pull(task_ids='name',key='last_name')
    age=ti.xcom_pull(task_ids='age',key='age')
    print (f"hello brother {first_name} {last_name} and your age {age}")

def get_name(ti):
    ti.xcom_push(key='first_name',value='xcoms_elon')
    ti.xcom_push(key='last_name',value='xcoms_kid')

def get_age(ti):
    ti.xcom_push(key='age',value=19)
    
with DAG (
    default_args=default_args,
    dag_id='python_dag_v6',
    description='python dag',
    start_date=datetime(2024,7,24),
    schedule_interval='@daily'

) as dag:
    task1=PythonOperator(
        task_id='greet',
        python_callable=greet
    )

    task2=PythonOperator(
        task_id='name',
        python_callable=get_name
    )
    task3=PythonOperator(
        task_id='age',
        python_callable=get_age
    )
    [task2,task3]>>task1