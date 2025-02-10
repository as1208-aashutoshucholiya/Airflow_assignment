from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from airflow.utils.email import send_email

def send_dag2_notification(status):
    send_email(
        to='your-email@example.com',
        subject=f"DAG2 Status: {status}",
        html_content=f"The status of DAG2 is: {status}. Please check the logs for more details."
    )


with DAG(
        'dag2_status_check',
        schedule_interval=None,
        start_date=datetime(2024, 12, 3),
        catchup=False,
        default_args={
            'owner': 'airflow',
            'email_on_failure': True,
            'email': ['your-email@example.com'],
        },
) as dag2:

    check_aus_status = ExternalTaskSensor(
        task_id='check_aus_status',
        external_dag_id='assignment_3_dag_a',
        external_task_id='fetch_aus_data',
        allowed_states=['success'],
        failed_states=['failed'],
        poke_interval=30,
        timeout=600,
        mode='poke',
        dag=dag2
    )

    check_belgium_status = ExternalTaskSensor(
        task_id='check_belgium_status',
        external_dag_id='assignment_3_dag_a',
        external_task_id='fetch_belgium_data',
        allowed_states=['success'],
        failed_states=['failed'],
        poke_interval=30,
        timeout=600,
        mode='poke',
        dag=dag2
    )

    check_brazil_status = ExternalTaskSensor(
        task_id='check_brazil_status',
        external_dag_id='assignment_3_dag_a',
        external_task_id='fetch_brazil_data',
        allowed_states=['success'],
        failed_states=['failed'],
        poke_interval=30,
        timeout=600,
        mode='poke',
        dag=dag2
    )


    def check_all_tasks_status(**kwargs):

        task_instance = kwargs['task_instance']
        aus_status = task_instance.xcom_pull(task_ids='check_aus_status')
        belgium_status = task_instance.xcom_pull(task_ids='check_belgium_status')
        brazil_status = task_instance.xcom_pull(task_ids='check_brazil_status')

        if aus_status == 'success' and belgium_status == 'success' and brazil_status == 'success':
            status = 'success'
        else:
            status = 'failed'


        task_instance.xcom_push(key='dag2_status', value=status)
        print(f"DAG2 status: {status}")
        return status

    check_status = PythonOperator(
        task_id='check_all_status',
        python_callable=check_all_tasks_status,
        provide_context=True,
        dag=dag2
    )


    def send_status_notification(**kwargs):
        status = kwargs['task_instance'].xcom_pull(task_ids='check_all_status', key='dag2_status')
        send_dag2_notification(status)

    send_email_notification = PythonOperator(
        task_id='send_email_notification',
        python_callable=send_status_notification,
        provide_context=True,
        dag=dag2
    )


    check_aus_status >> check_belgium_status >> check_brazil_status >> check_status >> send_email_notification
