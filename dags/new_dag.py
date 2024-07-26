from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash_operator import BashOperator

default_args={
    'owner': 'siku',
    'retries': 5,
    'retry_delay':timedelta(minutes=2)

}


with DAG(
    dag_id='new_dag',
    description='This is first dag',
    default_args=default_args,
    start_date=datetime(2024,7,24),
    schedule_interval='@daily'
) as dag :
    task1= BashOperator(
        task_id='first_task',
        bash_command=" echo hello world"
    )
    task2= BashOperator(
        task_id='second_task',
        bash_command=" echo 2 hello world"
    )
    task1.set_downstream(task2)