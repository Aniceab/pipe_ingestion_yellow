from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.ingest.ingesta import download_file, upload_to_s3
from src.dq.pipeline_dq import process_s3_files
import subprocess

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "ingestion_pipeline",
    default_args=default_args,
    description="Pipeline de ingestão, testes e validação de qualidade de dados",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

def run_tests():
    result = subprocess.run(["pytest", "tests/test_ingesta.py"], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Testes falharam:\n{result.stdout}\n{result.stderr}")

def ingest_data():
    # Código de ingestão (chamar funções do módulo `ingesta.py`)
    pass

def validate_data():
    # Código de validação de DQ (chamar funções do módulo `pipeline_dq.py`)
    pass

ingest_task = PythonOperator(
    task_id="ingest_data",
    python_callable=ingest_data,
    dag=dag,
)

test_task = PythonOperator(
    task_id="run_tests",
    python_callable=run_tests,
    dag=dag,
)

validate_task = PythonOperator(
    task_id="validate_data",
    python_callable=validate_data,
    dag=dag,
)

ingest_task >> test_task >> validate_task