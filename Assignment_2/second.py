from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

def print_variable():
    # Fetch the value for the key "greeting"
    var_value = Variable.get("Greeting", default_var="No value found")
    print(f"Greeting Variable: {var_value}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1),
}

with DAG(
    dag_id='print_variable_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    task = PythonOperator(
        task_id='print_variable',
        python_callable=print_variable,
    )
