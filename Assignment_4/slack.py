from airflow import DAG
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator 
from airflow.operators.python import PythonOperator



SLACK_WEBHOOK_URL = "https://lalalalal-hq.slack.com/archives/C0846A2CAJ1"

SLACK_CHANNEL = "#all-lalalalal"

# Define the DAG and its default arguments
dag = DAG(
    'slack_notification_example',
    default_args={
        'owner': 'airflow',
    },
    description='DAG with Slack notifications',
    schedule_interval='@daily',
    start_date=days_ago(1),
)

# Function to send Slack message for success/failure
def send_slack_message(context, task_instance, status):
    """
    Send Slack message based on the task's status.
    :param context: Airflow context
    :param task_instance: Task instance
    :param status: Task status (either 'success' or 'failure')
    """
    task_name = task_instance.task_id
    message = f"Task `{task_name}` {status.upper()} in DAG `{dag.dag_id}`"

    # Modify the message based on status
    if status == "success":
        message = f":white_check_mark: {message} :white_check_mark: \nEverything completed successfully!"
    else:
        message = f":x: {message} :x: \nSomething went wrong!"

    # Send the message to Slack
    slack_message = SlackWebhookOperator(
        task_id=f"slack_notify_{task_name}_{status}",
        webhook_token=SLACK_WEBHOOK_URL,
        message=message,
        channel=SLACK_CHANNEL,
        dag=dag
    )
    slack_message.execute(context=context)

# EmptyOperator as a no-op task
start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Task that might succeed or fail
def my_task():

    print("This is my task")

task_to_monitor = PythonOperator(
    task_id='task_to_monitor',
    python_callable=my_task,
    on_success_callback=lambda context, task_instance: send_slack_message(context, task_instance, "success"),
    on_failure_callback=lambda context, task_instance: send_slack_message(context, task_instance, "failure"),
    dag=dag,
)

# Set up task dependencies
start_task >> task_to_monitor
