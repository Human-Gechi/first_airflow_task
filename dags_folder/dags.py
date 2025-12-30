from pendulum import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from pathlib import Path


def check_ext(folder_path):
    folder = Path(folder_path)

    gz_files = list(folder.glob("*.gz"))

    if gz_files:
        return "process_data"
    else:
        print("No .gz file found. Downloading file ........")
        return "download_file"

def read_file(filepath):
    with open(filepath, "r") as file:
        return file.read()

def process_func():
    print("Processing data...")

with DAG('file_check_download_dag', start_date=datetime(2025,12,30), schedule=None) as dag:

    branch_task = BranchPythonOperator(
        task_id="check_file_exists",
        python_callable=check_ext,
        op_kwargs={"folder_path": "/opt/airflow/dags/airflow_task/downloads"}
    )

    download_task = BashOperator(
        task_id="download_file",
        bash_command=read_file("/opt/airflow/dags/airflow_task/download/download.sh")
    )

    process_task = PythonOperator(
        task_id="process_data",
        python_callable=process_func,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )


    branch_task >> [download_task, process_task]
    download_task >> process_task
