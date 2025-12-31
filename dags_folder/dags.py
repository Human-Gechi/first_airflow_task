import os
import sys
from pendulum import datetime
from pathlib import Path
from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.trigger_rule import TriggerRule

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from airflow_task.scripts.db_conn import (
    get_setup_sql, get_copy_sql,
    get_production_insert_sql
)

@dag(
    dag_id='wikipedia_dag',
    start_date=datetime(2025, 12, 30),
    schedule=None,
    catchup=False,
    tags=['snowflake', 'optimized']
)
def wikipedia_pipeline():

    @task.branch(task_id="check_file_exists")
    def check_ext_task(folder_path):
        folder = Path(folder_path)
        return "setup_snowflake" if list(folder.glob("*.gz")) else "download_file"

    download_file = BashOperator(
        task_id="download_file",
        bash_command="bash /opt/airflow/dags/airflow_task/download/download.sh"
    )

    setup_snowflake = SQLExecuteQueryOperator(
        task_id="setup_snowflake",
        conn_id='snowflake_hook',
        sql=get_setup_sql(),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    @task(task_id="parse_file_to_csv")
    def parse_to_csv():
        from airflow_task.scripts.file_parser import dataframe_parser
        return dataframe_parser()

    @task(task_id="upload_to_stage")
    def upload_to_stage(local_csv_path):
        hook = SnowflakeHook(snowflake_conn_id='snowflake_hook')
        hook.run(f"PUT file://{local_csv_path} @WIKI_STAGING_STAGE OVERWRITE=TRUE")


    copy_to_staging = SQLExecuteQueryOperator(
        task_id="copy_into_staging",
        conn_id='snowflake_hook',
        sql=get_copy_sql()
    )


    insert_prod = SQLExecuteQueryOperator(
        task_id="insert_into_production",
        conn_id='snowflake_hook',
        sql=get_production_insert_sql()
    )


    analyze_data = SQLExecuteQueryOperator(
        task_id="companies_analysis",
        conn_id='snowflake_hook',
        sql="SELECT PAGE_TITLE, SUM(VIEW_COUNT) FROM WIKI_PAGES_VIEWS_FINAL GROUP BY 1;",
        do_xcom_push=True
    )

    path_decision = check_ext_task("/opt/airflow/dags/airflow_task")
    csv_path = parse_to_csv()

    path_decision >> [download_file, setup_snowflake]
    download_file >> setup_snowflake
    setup_snowflake >> csv_path >> upload_to_stage(csv_path) >> copy_to_staging >> insert_prod >> analyze_data

wikipedia_pipeline()
