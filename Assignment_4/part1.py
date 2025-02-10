from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
# from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

# Define the DAG
dag = DAG(
    'ques4',
    default_args={
        'owner': 'airflow',
        'retries': 1,
    },
    description='A simple DAG to interact with MySQL',
    schedule_interval='@once',
    start_date=days_ago(1),
)

# Task to create a table
create_table = MySqlOperator(
    task_id='create_table',
    mysql_conn_id='mysql',  # Use the connection ID you set in the UI
    sql="""
    CREATE TABLE IF NOT EXISTS users (
        id INT PRIMARY KEY AUTO_INCREMENT,
        name VARCHAR(50),
        age INT
    );
    """,
    dag=dag,
)

# Task to insert values into the table
insert_values = MySqlOperator(
    task_id='insert_values',
    mysql_conn_id='mysql',
    sql="""
    INSERT INTO users (name, age)
    VALUES ('Alice', 30), 
           ('Bob', 25),
           ('Charlie', 35);
    """,
    dag=dag,
)

# Task to select and log values
def select_and_log():
    from airflow.providers.mysql.hooks.mysql import MySqlHook
    hook = MySqlHook(mysql_conn_id='mysql')
    sql = "SELECT * FROM users;"
    results = hook.get_records(sql)
    for row in results:
        print(f"User: {row[1]}, Age: {row[2]}")

select_values = PythonOperator(
    task_id='select_values',
    python_callable=select_and_log,
    dag=dag,
)

# Set task dependencies
create_table >> insert_values >> select_values
