from datetime import timedelta,datetime
from airflow.decorators import dag,task

default_args= {
    'owner':'siku',
    'retries':5,
    'nretry_delay':timedelta(minutes=1)
}

@dag(dag_id='api_dag_v1',start_date=datetime(2024,7,24),default_args=default_args,schedule_interval='@daily')
def hello_world_etl():
    @task()
    def get_name():
        return 'Jerry'
    @task
    def get_age():
        return 19
    @task
    def greet(name,age):
        print (f"the name is {name} and age is {age} ")

    name=get_name()
    age=get_age()
    greet(name,age)

mydag=hello_world_etl()
    


