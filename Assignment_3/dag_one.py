from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import csv

def fetch_and_write_to_csv(country, output_file, gcs_file_path,bucketname='assignment_bucket_123'):
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn_id')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()

    cursor.execute("USE practise;")
    query = f"SELECT * FROM retail_sales WHERE country = '{country}';"
    cursor.execute(query)
    records = cursor.fetchall()


    with open(output_file, mode='w', newline='') as file:
        csv_writer = csv.writer(file)


        column_names = [i[0] for i in cursor.description]
        csv_writer.writerow(column_names)


        for row in records:
            csv_writer.writerow(row)

    cursor.close()

    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default') # Your GCS connection ID
    gcs_hook.upload(bucket_name=bucketname, object_name=gcs_file_path, filename=output_file)


with DAG(
        'assignment_3_dag_a',
        schedule_interval=None,
        start_date=datetime(2024, 12, 3),
        catchup=False,
) as dag:

    fetch_Aus_data = PythonOperator(
        task_id='fetch_aus_data',
        python_callable=fetch_and_write_to_csv,
        op_args=['Australia', '/Users/archanaashture/airflow_snowflake/output/usa_output.csv','data/retail_sales/aus_output.csv'],
    )

    fetch_Belgium_data = PythonOperator(
        task_id='fetch_belgium_data',
        python_callable=fetch_and_write_to_csv,
        op_args=['Belgium', '/Users/archanaashture/airflow_snowflake/output/india_output.csv','data/retail_sales/belgium_output.csv'],
    )

    fetch_Brazil_data = PythonOperator(
        task_id='fetch_brazil_data',
        python_callable=fetch_and_write_to_csv,
        op_args=['Brazil', '/Users/archanaashture/airflow_snowflake/output/canada_output.csv','data/retail_sales/brazil_output.csv'],
    )

    [fetch_Brazil_data,fetch_Belgium_data,fetch_Aus_data]
