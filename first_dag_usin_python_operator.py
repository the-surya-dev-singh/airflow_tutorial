from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
default_args={
    'owner':'surya',
    'retries':2,
    'retries_delay':timedelta(minutes=2)
}
def greet(name,age,ti):
    ti.xcom_push(key='name',value=name)
    ti.xcom_push(key='age',value=age)

def greet2( ti):
    name=ti.xcom_pull(task_ids='python_task_1',key='name')
    age=ti.xcom_pull(task_ids='python_task_1',key='age')
    print(f'hello world. I am {name} and '
          f'I am {age} years old')
    
with DAG(
    dag_id='first_python_operator_dag_v10',
    description="this is the first python dag desc",
    default_args=default_args,
    start_date=datetime(2023,7,1),
    schedule_interval=timedelta(minutes=10)
    ) as dag:
    
    task1=PythonOperator(
        task_id='python_task_1',
        python_callable=greet,
        op_kwargs={'name':'tom',
                   'age':26
                   }
    )
    task2=PythonOperator(
        task_id='task_2',
        python_callable=greet2,
      
    )
    task1>>task2