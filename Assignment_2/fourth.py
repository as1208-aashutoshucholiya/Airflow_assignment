from airflow import models
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def log_bq_results(**kwargs):

    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='get_bigquery_data')


    if result:
        for row in result:
            print(f"Row: {row}")
    else:
        print("No data fetched from BigQuery.")

with models.DAG(
        'bigquery_random_data_dag',
        schedule_interval=None,
        start_date=days_ago(1),
        tags=['example', 'bigquery'],
) as dag:


    get_bigquery_data = BigQueryGetDataOperator(
        task_id='get_bigquery_data',
        gcp_conn_id='google_cloud_default',
        dataset_id='test_123',
        table_id='test_456',
        table_project_id='smooth-command-443818-t6',
        max_results=10
    )


    log_results_task = PythonOperator(
        task_id='log_results',
        python_callable=log_bq_results,
        provide_context=True
    )


    get_bigquery_data >> log_results_task
