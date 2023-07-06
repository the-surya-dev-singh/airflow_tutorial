from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime,timedelta

default_args={
    'owner':'surya',
    'start_date':datetime(2023,7,5)
}
def test_print():
    print('test for task')
with DAG(
    dag_id='dag_a',
    default_args=default_args,
    schedule_interval='@daily',
    
    ) as dag:
    
    t1=PythonOperator(
        task_id='t1',
        python_callable=test_print
    )
    trigger_task=TriggerDagRunOperator(
        task_id='trigger_next_dag',
        trigger_dag_id='dag_b',
        wait_for_completion=True, # waits for the completion of the triggered dag otherwise will just trigger and run t10(downstream task)
        poke_interval=30 # checks if completed every 30 sec
        
    )
    t10=PythonOperator(
        task_id='t10',
        python_callable=test_print
    )
    t1>>trigger_task>>t10
   