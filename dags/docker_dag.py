from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator

default_args={
    'owner':'siku',
    'retry':5,
    'retry_delay':timedelta(minutes=2)}

def func1():
  import sklearn
  print(f"sklearn with version: {sklearn.__version__} ")

    

with DAG(
    dag_id='docker_dag_v2',
    default_args=default_args,
    start_date=datetime(2024,7,26),
    schedule_interval='@daily'

) as dag:
    
    task1=PythonOperator(
        task_id='greet',
        python_callable=func1

    )
    task1