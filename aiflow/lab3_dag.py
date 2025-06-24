from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'L3-T04',
    'depends_on_past': False,
    'email': ['marvin.ernst@bse.eu'],
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='lab3_pipeline_orchestration',
    default_args=default_args,
    description='Run Lab 3 Data Pipelines Sequentially',
    schedule_interval=None,  # a manual trigger
    start_date=datetime(2025, 6, 24),
    catchup=False,
    tags=['lab3'],
) as dag:

    formatting = BashOperator(
        task_id='run_data_formatting',
        bash_command='python /opt/airflow/notebooks/scripts/01_data_formatting_pipeline.py',
    )

    exploitation = BashOperator(
        task_id='run_exploitation_pipeline',
        bash_command='python /opt/airflow/notebooks/scripts/02_exploitation_pipeline.py',
    )

    analysis = BashOperator(
        task_id='run_analysis_pipeline',
        bash_command='python /opt/airflow/notebooks/scripts/03_analysis_pipeline.py',
    )

    formatting >> exploitation >> analysis  # these are the task dependencies