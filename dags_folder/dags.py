#Importing necessary libraries
import os
import sys
from pendulum import datetime
from pathlib import Path
from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from airflow_task.scripts.file_parser import dataframe_parser
from airflow_task.scripts.db_conn import(
        get_setup_sql, get_copy_sql,
        get_production_insert_sql, select_companies_to_list
    )
#Utilizing the domain dag for wikipedia
@dag(
    #DAG definition using the dag decorator
    dag_id='wikipedia_dag',
    start_date=datetime(2025, 12, 30),
    schedule=None,
    catchup=False,
    tags=['snowflake', 'wiki_page_views']
)
def wikipedia_pipeline():
    """ Getting the using TaskFlow API pipeline running """
    #Acessing file path for .py files
    @task.branch(task_id="check_file_exists") #Using TaskFlow API alongside its decorator
    def check_ext_task(folder_path): #Checking file path
        folder = Path(folder_path) #Folder path
        return "setup_snowflake" if list(folder.glob("*.gz")) else "download_file"

    #BashOperator for file download
    download_file = BashOperator(
        task_id="download_file",
        bash_command="bash download.sh ",
        cwd="/opt/airflow/dags/airflow_task/dags_folder"
    )

    #Setting up Snowflake using the get_setup_sql func.
    setup_snowflake = SQLExecuteQueryOperator(
        task_id="setup_snowflake",
        conn_id='snowflake_hook',
        sql=get_setup_sql(),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    #Parsing file from .gz into temp. storage
    @task(task_id="parse_file_to_csv")
    def parse_to_csv():
        """ CSV parsinf for temporary storage"""
        #File importation inside function to prevent dag failures

        return dataframe_parser()

    #Uploading file to staging table
    @task(task_id="upload_to_stage")
    def upload_to_stage(local_csv_path):
        hook = SnowflakeHook(snowflake_conn_id='snowflake_hook')
        hook.run(f"PUT file://{local_csv_path} @WIKI_STAGING_STAGE OVERWRITE=TRUE")

    #COPYING data into staging
    copy_to_staging = SQLExecuteQueryOperator(
        task_id="copy_into_staging",
        conn_id='snowflake_hook',
        sql=get_copy_sql()
    )

    #Inserting into production table
    insert_prod = SQLExecuteQueryOperator(
        task_id="insert_into_production",
        conn_id='snowflake_hook',
        sql=get_production_insert_sql()
    )

    #Companies analysis
    analyze_data = SQLExecuteQueryOperator(
        task_id="companies_analysis",
        conn_id = "snowflake_hook",
        sql=select_companies_to_list(),
        do_xcom_push=True
    )
    #CHecking file path
    path_decision = check_ext_task("/opt/airflow/dags/airflow_task")
    csv_path = parse_to_csv() #CSV file path

    #Defining dependencies
    path_decision >> [download_file, setup_snowflake] #Checks existence of file and downloads then setup
    download_file >> setup_snowflake #File is available, setup Snowflake
    setup_snowflake >> csv_path >> upload_to_stage(csv_path) >> copy_to_staging >> insert_prod >> analyze_data  #Setup Snowflake,upload data

wikipedia_pipeline() #Function fot the decorator
