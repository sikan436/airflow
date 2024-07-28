from datetime import timedelta,datetime
from airflow.decorators import dag,task

default_args= {
    'owner':'siku',
    'retries':5,
    'nretry_delay':timedelta(minutes=1)
}

@dag(dag_id='api_dag_v2',start_date=datetime(2024,7,24),default_args=default_args,schedule_interval='@daily')
def hello_world_etl():
    @task(multiple_outputs=True)
    def get_name():
       return {'first_name':'jerry', 'last_name':'simba'}
    @task
    def get_age():
        return 19
    @task
    def greet(first_name,last_name,age):
        print (f"the name is {first_name},{last_name} and age is {age} ")

    name_dict=get_name()
    age=get_age()
    greet(name_dict["first_name"],name_dict["last_name"],age)

mydag=hello_world_etl()
    


