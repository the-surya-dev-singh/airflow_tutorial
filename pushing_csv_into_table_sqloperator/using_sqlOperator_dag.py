from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email import EmailOperator
import pandas as pd
from sqlalchemy import create_engine
default_args={
    'owner':'surya',
    'start_date':datetime(2023,7,4)
    
}
def process_data():
    data=pd.read_csv('~/airflow/dags/pushing_csv_into_table_sqloperator/ip_files/or1.csv')
    data['total_price'] = data['UnitPrice'] * data['Quantity']
    
    g_data = data.groupby(['StockCode','Description','Country'],as_index=False)['total_price'].sum()
    
    g_data.to_csv("~/airflow/dags/pushing_csv_into_table_sqloperator/op_files/fin.csv",index=False) #use absolute path
    
def pre_process():
    data=pd.read_csv("~/airflow/dags/pushing_csv_into_table_sqloperator/ip_files/or.csv")
    
    data['Description'] = data['Description'].str.replace('\W', ' ')
    
    if data.isnull().values.any():
        data=data.dropna(axis=0, how='any')

    data.to_csv("~/airflow/dags/pushing_csv_into_table_sqloperator/ip_files/or1.csv")
    
def insert_data():
    data=pd.read_csv("~/airflow/dags/pushing_csv_into_table_sqloperator/op_files/fin.csv")
    data=data.rename(columns={
        "StockCode":"stock_code",
        "Description":"descb",
        "Country":"country"
        
        
    })
    data.to_sql(name="aggre_res",con=create_engine("mysql://surya:Surya123#@localhost:3306/surya_sql_test"),schema="surya_sql_test",if_exists="replace") # create engine for db connection
with DAG(
    dag_id='sql_dag_v08',
    default_args=default_args,
    schedule_interval='@daily'
    )as dag:
 
    check_file_task=BashOperator(
        task_id='check_file',
        bash_command='shasum ~/airflow/dags/pushing_csv_into_table_sqloperator/ip_files/or.csv', # use absolute path
        retries=2,
        retry_delay=timedelta(seconds=10)
    )
    pre_process_csv=PythonOperator(
        task_id='pre_process_csv_file',
        python_callable=pre_process
    )
    aggregate=PythonOperator(
        task_id='aggregate',
        python_callable=process_data
    )
    create_table = MySqlOperator(
        task_id='create_table', 
        mysql_conn_id="my_db1", 
        sql="CREATE table IF NOT EXISTS aggre_res (stock_code varchar(100) NULL,descb varchar(100) NULL,country varchar(100) NULL,total_price varchar(100) NULL)"
    )
    insert=PythonOperator(
        task_id='insert',
        python_callable=insert_data
    )
check_file_task>>pre_process_csv>>aggregate>>create_table>>insert