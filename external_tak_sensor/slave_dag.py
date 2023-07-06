from airflow import DAG
from airflow.operators.dummy import DummyOperator 
from datetime import datetime,timedelta

default_args={
    'owner':'surya',
    'start_date': datetime(2023,7,5)
}

with DAG(
    dag_id='slave_dag_v05',
    default_args=default_args,
    schedule_interval=timedelta(hours=4),
    catchup=False
    ) as dag:
    
    t1=DummyOperator(
        task_id='t1'
    )
  