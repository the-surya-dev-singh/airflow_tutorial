from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta

default_args={
    'owner':'surya',
    'start_date':datetime(2023,7,5)
}
def test_print():
    print('test for task')
with DAG(
    dag_id='dag_b',
    default_args=default_args,
    schedule_interval='@daily',
    
    ) as dag:
    
    t6=PythonOperator(
        task_id='t6',
        python_callable=test_print
    )
    t7=BashOperator(
        task_id='t7',
        bash_command='sleep 60'
    )
    t8=PythonOperator(
        task_id='t8',
    python_callable=test_print
    )
    t6>>t7>>t8