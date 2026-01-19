#Importing necessary libraries
import os
import sys
from pendulum import datetime
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.email import send_email

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from airflow_task.scripts.db_conn import(
        get_setup_sql, get_copy_sql,
        get_production_insert_sql, select_companies_to_list
    )
def read_file(filepath):
    with open(filepath, "r") as file:
        return file.read()

def on_failure_email_alert(context):
        """Send a custom email alert on failure."""
        dag_run = context.get('dag_run')
        subject = f"Airflow DAG Failure: {dag_run.dag_id}"
        html_content = read_file('/opt/airflow/dags/airflow_task/dags_folder/email.html')
        send_email(to=['okoliogechi74@gmail.com'], subject=subject, html_content=html_content)

default_args = {
    'owner' : "data_consult",
    'email' : "okoliogechi74@gmail.com",
    'email_on_failure' : True,
    'email_on_retry' : False,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=2)
}

#Utilizing the domain dag for wikipedia
@dag(
    #DAG definition using the dag decorator
    dag_id='wikipedia_page_views',
    default_args=default_args,
    start_date=datetime(2025, 12, 30),
    schedule=None,
    catchup=False,
    on_failure_callback=on_failure_email_alert,
    tags=['snowflake', 'wiki_page_views']
)
def wikipedia_pipeline():
    """ Getting the using TaskFlow API pipeline running """
    #BashOperator for file download
    download_file = BashOperator(
        task_id="download_file",
        bash_command="bash download.sh ",
        cwd="/opt/airflow/dags/airflow_task/dags_folder",
        execution_timeout = timedelta(minutes=10)
    )

    #Setting up Snowflake using the get_setup_sql func.
    setup_snowflake = SQLExecuteQueryOperator(
        task_id="setup_snowflake",
        conn_id='snowflake_hook',
        sql=get_setup_sql(),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

        #Parsing file from .gz into temp. storage
    def parse_file_to_csv_task(**kwargs):
        from airflow_task.scripts.file_parser import dataframe_parser
        # This function manages its own rows and indices internally
        output_directory = dataframe_parser()
        return output_directory

    #parsring_.gz files_to_Csv
    parse_task = PythonOperator(
        task_id='parse_file_to_csv',
        python_callable=parse_file_to_csv_task,
    )
    
    #Uploading chunked files to staging table
    @task(task_id="upload_to_stage")
    def upload_to_stage():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_hook')
        hook.run("PUT file:///tmp/wikipedia_pageviews_*.csv @WIKI_STAGING_STAGE;")

    #COPYING data into staging
    copy_to_staging = SQLExecuteQueryOperator(
        task_id="copy_into_staging",
        conn_id='snowflake_hook',
        sql=get_copy_sql()
    )
    # Delete the raw .gz and all chunked CSV files in /tmp directory
    cleanup_temp_files = BashOperator(
        task_id='cleanup_temp_files',
        bash_command='rm -f /tmp/wikipedia_pageviews.gz /tmp/wikipedia_pageviews_*.csv',
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

    download_file >> setup_snowflake #File is available, setup Snowflake
    setup_snowflake >> parse_task >> upload_to_stage() >> copy_to_staging >>cleanup_temp_files >>insert_prod >> analyze_data  #Setup Snowflake,upload data

wikipedia_pipeline() #Function for the decorator
