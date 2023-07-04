from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args={
    'owner': 'surya',
    'retries':5,
    'retries_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='our_first_dag_v3',
    default_args=default_args,
    description='this is out first dat that we wrote',
    start_date=datetime(2023,7,10,2),
    schedule_interval='@daily'
    
    ) as dag:
    task1=BashOperator(
        task_id='first_task',
        bash_command='echo hello first task'
    )
    
    task2=BashOperator(
        task_id='2nd_task',
        bash_command='echo this is the 2nd task'
    
    )
    task3=BashOperator(
        task_id='3rd_task',
        bash_command='echo this is the 3rd task'
    
    )
    
    task1.set_downstream(task2)
    task1.set_downstream(task3)