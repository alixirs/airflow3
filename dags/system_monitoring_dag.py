"""
System Monitoring DAG
Runs frequent health checks and monitoring tasks
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import random

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def check_cpu_usage(**context):
    """Simulate CPU usage check"""
    cpu_usage = random.randint(10, 90)
    print(f"CPU Usage: {cpu_usage}%")
    
    if cpu_usage > 80:
        print("⚠️  WARNING: High CPU usage detected!")
    else:
        print("✓ CPU usage is normal")
    
    context['ti'].xcom_push(key='cpu_usage', value=cpu_usage)
    return cpu_usage

def check_memory_usage(**context):
    """Simulate memory usage check"""
    memory_usage = random.randint(20, 85)
    print(f"Memory Usage: {memory_usage}%")
    
    if memory_usage > 80:
        print("⚠️  WARNING: High memory usage detected!")
    else:
        print("✓ Memory usage is normal")
    
    context['ti'].xcom_push(key='memory_usage', value=memory_usage)
    return memory_usage

def check_disk_space(**context):
    """Simulate disk space check"""
    disk_usage = random.randint(30, 95)
    print(f"Disk Usage: {disk_usage}%")
    
    if disk_usage > 90:
        print("⚠️  WARNING: Low disk space!")
    else:
        print("✓ Disk space is adequate")
    
    context['ti'].xcom_push(key='disk_usage', value=disk_usage)
    return disk_usage

def generate_report(**context):
    """Generate monitoring report"""
    cpu = context['ti'].xcom_pull(key='cpu_usage', task_ids='check_cpu')
    memory = context['ti'].xcom_pull(key='memory_usage', task_ids='check_memory')
    disk = context['ti'].xcom_pull(key='disk_usage', task_ids='check_disk')
    
    print("\n" + "="*50)
    print("SYSTEM MONITORING REPORT")
    print("="*50)
    print(f"Timestamp: {datetime.now()}")
    print(f"CPU Usage: {cpu}%")
    print(f"Memory Usage: {memory}%")
    print(f"Disk Usage: {disk}%")
    print("="*50 + "\n")
    
    # Determine overall health
    issues = []
    if cpu > 80:
        issues.append("High CPU")
    if memory > 80:
        issues.append("High Memory")
    if disk > 90:
        issues.append("Low Disk Space")
    
    if issues:
        print(f"⚠️  Issues detected: {', '.join(issues)}")
        return "unhealthy"
    else:
        print("✓ All systems operational")
        return "healthy"

with DAG(
    'system_monitoring_dag',
    default_args=default_args,
    description='System health monitoring DAG',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['monitoring', 'health-check'],
) as dag:

    check_cpu = PythonOperator(
        task_id='check_cpu',
        python_callable=check_cpu_usage,
    )

    check_memory = PythonOperator(
        task_id='check_memory',
        python_callable=check_memory_usage,
    )

    check_disk = PythonOperator(
        task_id='check_disk',
        python_callable=check_disk_space,
    )

    ping_service = BashOperator(
        task_id='ping_service',
        bash_command='echo "Pinging services..." && sleep 1 && echo "All services responding"',
    )

    generate_report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
    )

    # Run checks in parallel, then generate report
    [check_cpu, check_memory, check_disk, ping_service] >> generate_report_task
