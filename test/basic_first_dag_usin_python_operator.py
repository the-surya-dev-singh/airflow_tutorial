from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
default_args={
    'owner':'surya',
    'retries':2,
    'retries_delay':timedelta(minutes=2)
}
def greet(name,age):
      print(f'hello world. I am {name} and '
          f'I am {age} years old')

def greet2( name,age):
    print(f'hello world. I am {name} and '
          f'I am {age} years old')
    
with DAG(
    dag_id='basic_first_python_operator_dag_v10',
    description="this is the first python dag desc",
    default_args=default_args,
    start_date=datetime(2023,7,1),
    schedule_interval=timedelta(minutes=10)
    ) as dag:
    
    task1=PythonOperator(
        task_id='python_task_1',
        python_callable=greet, #if we give parameters in the funtion itself it won't work. We must use op_kwargs function. 
        op_kwargs={'name':'tom', # we should write [name : tom , age: 26], [tom,26] won't work
                   'age':26
                   }
    )
    task2=PythonOperator(
        task_id='task_2',
        python_callable=greet2,
        op_kwargs={'name':'ram','age':26}
        
      
    )
    task1>>task2