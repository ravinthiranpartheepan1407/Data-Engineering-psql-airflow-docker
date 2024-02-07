import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from csv_psql import insert_data

start_date = datetime(2024,1,1,12,10)
data_args = {
    "owner": "Ravinthiran",
    "start_date": start_date,
    "retry": 1,
    "retry_delay": timedelta(seconds=10)
}

with DAG("CSV Stream to Airflow", default_args=data_args, schedule_interval="@daily", catchup=False) as dag:
    write_csv_postgres = PythonOperator(
        task_id = "write_csv_postgres",
        python_callable = insert_data,
        retry = 1,
        retry_delay = timedelta(seconds=15))