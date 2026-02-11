"""
Simple Hello World DAG
Runs a series of simple Python tasks
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_hello():
    print("Hello from Airflow!")
    return "Hello World"

def print_date():
    print(f"Current date and time: {datetime.now()}")
    return datetime.now()

def print_context(**context):
    print(f"Execution date: {context['ds']}")
    print(f"DAG run: {context['dag_run']}")
    return "Context printed"

with DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'hello-world'],
) as dag:

    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=print_hello,
    )

    date_task = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
    )

    context_task = PythonOperator(
        task_id='print_context',
        python_callable=print_context,
    )

    bash_task = BashOperator(
        task_id='bash_hello',
        bash_command='echo "Hello from Bash!" && date',
    )

    # Define task dependencies
    hello_task >> date_task >> context_task >> bash_task
