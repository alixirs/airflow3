"""
ETL Pipeline DAG
Demonstrates a complete ETL workflow with task groups
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import random

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_from_source(source_name, **context):
    """Extract data from a source"""
    record_count = random.randint(50, 200)
    print(f"Extracting data from {source_name}")
    print(f"Extracted {record_count} records")
    context['ti'].xcom_push(key=f'{source_name}_count', value=record_count)
    return record_count

def transform_data(dataset_name, **context):
    """Transform data"""
    print(f"Transforming {dataset_name} dataset")
    print("- Cleaning null values")
    print("- Normalizing fields")
    print("- Applying business rules")
    print(f"✓ {dataset_name} transformation complete")
    return f"{dataset_name}_transformed"

def validate_data(dataset_name, **context):
    """Validate transformed data"""
    print(f"Validating {dataset_name} dataset")
    
    # Simulate validation
    is_valid = random.choice([True, True, True, False])  # 75% pass rate
    
    if is_valid:
        print(f"✓ {dataset_name} validation passed")
        return True
    else:
        print(f"⚠️  {dataset_name} validation failed - data quality issues detected")
        return False

def load_to_warehouse(**context):
    """Load data to warehouse"""
    print("Loading data to data warehouse")
    
    # Get counts from all sources
    api_count = context['ti'].xcom_pull(key='api_count', task_ids='extract.extract_api')
    db_count = context['ti'].xcom_pull(key='database_count', task_ids='extract.extract_database')
    file_count = context['ti'].xcom_pull(key='files_count', task_ids='extract.extract_files')
    
    total_records = (api_count or 0) + (db_count or 0) + (file_count or 0)
    
    print(f"Loading {total_records} total records")
    print("✓ Load complete")
    return total_records

def send_success_notification(**context):
    """Send success notification"""
    execution_date = context['ds']
    print(f"\n{'='*60}")
    print(f"ETL PIPELINE COMPLETED SUCCESSFULLY")
    print(f"{'='*60}")
    print(f"Execution Date: {execution_date}")
    print(f"Timestamp: {datetime.now()}")
    print(f"Status: SUCCESS")
    print(f"{'='*60}\n")
    return "notification_sent"

with DAG(
    'etl_pipeline_dag',
    default_args=default_args,
    description='ETL pipeline with task groups',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'pipeline', 'data-warehouse'],
) as dag:

    start = EmptyOperator(task_id='start')

    # Extract Task Group
    with TaskGroup('extract', tooltip='Extract data from sources') as extract_group:
        extract_api = PythonOperator(
            task_id='extract_api',
            python_callable=extract_from_source,
            op_kwargs={'source_name': 'api'},
        )

        extract_db = PythonOperator(
            task_id='extract_database',
            python_callable=extract_from_source,
            op_kwargs={'source_name': 'database'},
        )

        extract_files = PythonOperator(
            task_id='extract_files',
            python_callable=extract_from_source,
            op_kwargs={'source_name': 'files'},
        )

    # Transform Task Group
    with TaskGroup('transform', tooltip='Transform and validate data') as transform_group:
        transform_api = PythonOperator(
            task_id='transform_api',
            python_callable=transform_data,
            op_kwargs={'dataset_name': 'API'},
        )

        transform_db = PythonOperator(
            task_id='transform_database',
            python_callable=transform_data,
            op_kwargs={'dataset_name': 'Database'},
        )

        transform_files = PythonOperator(
            task_id='transform_files',
            python_callable=transform_data,
            op_kwargs={'dataset_name': 'Files'},
        )

        validate_api = PythonOperator(
            task_id='validate_api',
            python_callable=validate_data,
            op_kwargs={'dataset_name': 'API'},
        )

        validate_db = PythonOperator(
            task_id='validate_database',
            python_callable=validate_data,
            op_kwargs={'dataset_name': 'Database'},
        )

        validate_files = PythonOperator(
            task_id='validate_files',
            python_callable=validate_data,
            op_kwargs={'dataset_name': 'Files'},
        )

        # Transform -> Validate dependencies
        transform_api >> validate_api
        transform_db >> validate_db
        transform_files >> validate_files

    # Load Task Group
    with TaskGroup('load', tooltip='Load data to warehouse') as load_group:
        load_warehouse = PythonOperator(
            task_id='load_warehouse',
            python_callable=load_to_warehouse,
        )

        notify = PythonOperator(
            task_id='notify_success',
            python_callable=send_success_notification,
        )

        load_warehouse >> notify

    end = EmptyOperator(task_id='end')

    # Pipeline dependencies
    start >> extract_group >> transform_group >> load_group >> end
