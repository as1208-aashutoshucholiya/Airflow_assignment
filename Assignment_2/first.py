from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def first_dag():
    print("hello airflow")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1),
}

with DAG(
    dag_id='first_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    task = PythonOperator(
        task_id='print_hello',
        python_callable=first_dag,
    )
