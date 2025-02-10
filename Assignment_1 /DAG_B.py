from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def read_fan_letter(**kwargs):

    luck_score_result = kwargs['ti'].xcom_pull(task_ids='TASK_A', key='luck_score_result')

    if luck_score_result == 'TASK_B':
        file_path = "/tmp/fan_letter_supe_a.txt"
    else:
        file_path = "/tmp/fan_letter_supe_b.txt"

    with open(file_path, "r") as f:
        letter_content = f.read()

    print(f"Fan letter content:\n{letter_content}")


with DAG(
        'DAG_B',
        description='DAG that reads fan letters from DAG_A and prints the content.',
        schedule_interval=None,
        start_date=datetime(2024, 12, 3),
        catchup=False,
) as dag_b:

    task = PythonOperator(
        task_id='read_fan_letter',
        python_callable=read_fan_letter,
        provide_context=True,
    )

    task
