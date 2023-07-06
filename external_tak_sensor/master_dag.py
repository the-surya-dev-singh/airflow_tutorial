
from airflow import DAG
from airflow.operators.dummy import  DummyOperator 
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime,timedelta

default_args={
    'owner': 'surya',
    'start_date': datetime(2023,7,5)
}

with DAG(
    dag_id='master_dag_v05',
    default_args=default_args,
    schedule_interval='@daily'
    ) as dag:
    
    sensor_task= ExternalTaskSensor(
        task_id="sensor_task",
        external_dag_id='slave_dag_v05',
        external_task_id='t1'
       #execution_delta=timedelta(=3)
    )
    t2=DummyOperator(
        task_id="t2"
    )
    sensor_task>>t2
    