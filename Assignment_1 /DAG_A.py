import random
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago



def generate_luck_score(**kwargs):
    luck_score = random.random()
    print(f"LUCK_SCORE generated: {luck_score}")


    if luck_score > 0.5:
        kwargs['ti'].xcom_push(key='luck_score_result', value='TASK_B')
    else:
        kwargs['ti'].xcom_push(key='luck_score_result', value='TASK_C')


def write_fan_letter_supe_a(**kwargs):
    letter_content = "Dear SUPE_A,\n\nYou are my most favorite superhero! Keep saving the world!\nSincerely, A Fan."
    with open("/tmp/fan_letter_supe_a.txt", "w") as f:
        f.write(letter_content)
    print("Fan letter to SUPE_A written.")


def write_fan_letter_supe_b(**kwargs):
    letter_content = "Dear SUPE_B,\n\nYou are my second favorite superhero! Keep up the great work!\nSincerely, A Fan."
    with open("/tmp/fan_letter_supe_b.txt", "w") as f:
        f.write(letter_content)
    print("Fan letter to SUPE_B written.")


with DAG(
        'DAG_A',
        schedule_interval='@daily',
        start_date=days_ago(1),
        catchup=False,
) as dag_a:

    task_a = PythonOperator(
        task_id='TASK_A',
        python_callable=generate_luck_score,
        provide_context=True,
    )

    task_b = PythonOperator(
        task_id='TASK_B',
        python_callable=write_fan_letter_supe_a,
    )


    task_c = PythonOperator(
        task_id='TASK_C',
        python_callable=write_fan_letter_supe_b,
    )


    task_d = TriggerDagRunOperator(
        task_id='trigger_dag_b',
        trigger_dag_id='DAG_B',
        conf={'message': 'Triggered by DAG_A'},
    )

    task_a >> [task_b, task_c]
    [task_b, task_c] >> task_d
