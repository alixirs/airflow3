"""
Data Processing DAG with Branching
Demonstrates branching logic and task dependencies
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import random

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_data(**context):
    """Simulate data extraction"""
    data = {
        'records': random.randint(100, 1000),
        'timestamp': datetime.now().isoformat()
    }
    print(f"Extracted {data['records']} records at {data['timestamp']}")
    context['ti'].xcom_push(key='record_count', value=data['records'])
    return data

def choose_processing_path(**context):
    """Branch based on record count"""
    record_count = context['ti'].xcom_pull(key='record_count', task_ids='extract_data')
    
    if record_count > 500:
        print(f"Large dataset detected ({record_count} records) - using heavy processing")
        return 'heavy_processing'
    else:
        print(f"Small dataset detected ({record_count} records) - using light processing")
        return 'light_processing'

def light_processing(**context):
    """Light processing for small datasets"""
    record_count = context['ti'].xcom_pull(key='record_count', task_ids='extract_data')
    print(f"Running light processing on {record_count} records")
    return f"Processed {record_count} records (light)"

def heavy_processing(**context):
    """Heavy processing for large datasets"""
    record_count = context['ti'].xcom_pull(key='record_count', task_ids='extract_data')
    print(f"Running heavy processing on {record_count} records")
    return f"Processed {record_count} records (heavy)"

def load_data(**context):
    """Load processed data"""
    print("Loading data to destination")
    return "Data loaded successfully"

with DAG(
    'data_processing_dag',
    default_args=default_args,
    description='Data processing DAG with branching logic',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['data', 'processing', 'branching'],
) as dag:

    start = EmptyOperator(task_id='start')

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    branch = BranchPythonOperator(
        task_id='choose_processing_path',
        python_callable=choose_processing_path,
    )

    light = PythonOperator(
        task_id='light_processing',
        python_callable=light_processing,
    )

    heavy = PythonOperator(
        task_id='heavy_processing',
        python_callable=heavy_processing,
    )

    join = EmptyOperator(
        task_id='join',
        trigger_rule='none_failed_min_one_success',
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    end = EmptyOperator(task_id='end')

    # Define dependencies
    start >> extract >> branch >> [light, heavy] >> join >> load >> end
