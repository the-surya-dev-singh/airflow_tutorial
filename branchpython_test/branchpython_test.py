from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow import DAG
from datetime import datetime,timedelta

default_args={
    'owner':'surya',
    'start_date':datetime(2023,7,5)
}

def check_branch_funct():
    value=6
    if(value>5):
        return't1' #in return state the task id for which you want to run if the condition meets
    elif (value<=5):
        return't2'
        
with DAG(
    dag_id='branch_python_test_dag',
    default_args=default_args,
    schedule_interval='@daily'
    
    )as dag:
    
    check_branch=BranchPythonOperator(
        task_id="check_branch",
        python_callable=check_branch_funct
    )
    
    t1=DummyOperator(
        task_id='t1'
    )
    t2=DummyOperator(
        task_id="t2"
    )
    storing_task=DummyOperator(
        task_id="storing_task",
        trigger_rule='none_failed_or_skipped' #trigger this if at least one parent suceed 
    )
    check_branch>>[t1,t2]>>storing_task