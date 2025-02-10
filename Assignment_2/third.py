import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

# Function to send an alert to Google Chat
def send_alert():
    try:
        # Fetch variables from Airflow
        webhook_url = Variable.get("chat_webhook_url")
        message = Variable.get("greeting", default_var="Hello from Airflow!")
        
        # Google Chat payload
        payload = {"text": message}
        
        # Send message to Google Chat via webhook
        response = requests.post(webhook_url, json=payload)
        if response.status_code == 200:
            print(f"Message sent successfully: {message}")
        else:
            print(f"Failed to send message: {response.status_code}, {response.text}")
    except Exception as e:
        print(f"Error sending alert: {str(e)}")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1),
}

# Define the DAG
with DAG(
    dag_id='send_google_chat_alert_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    # Define a single task to send the alert
    task = PythonOperator(
        task_id='send_google_chat_alert',
        python_callable=send_alert,
    )
