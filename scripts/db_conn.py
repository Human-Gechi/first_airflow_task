#Necessary Libraries for the task
import os
from dotenv import load_dotenv
from airflow_task.scripts.logs import logger
import snowflake.connector

load_dotenv()

def get_setup_sql():
    """SQL code for creating tables and stage"""
    return  """   -- Create staging and final tables
            CREATE TABLE IF NOT EXISTS WIKI_PAGES_VIEWS_STAGING (
                DOMAIN_CODE STRING,
                PAGE_TITLE STRING,
                VIEW_COUNT NUMBER,
                RESPONSE_IN_BYTES NUMBER
            );
            CREATE TABLE IF NOT EXISTS WIKI_PAGES_VIEWS_FINAL (
                DOMAIN_CODE STRING, 
                PAGE_TITLE STRING, 
                VIEW_COUNT NUMBER,
                RESPONSE_IN_BYTES NUMBER, 
                LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            );
            CREATE STAGE IF NOT EXISTS WIKI_STAGING_STAGE;
        """

def get_copy_sql():
    """SQL code for uploading instead of loading with_pandas to prevent signal_9 errors in Airflow"""
    return """
        COPY INTO WIKI_PAGES_VIEWS_STAGING
        FROM @WIKI_STAGING_STAGE
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_DELIMITER = '|'
            SKIP_HEADER = 0
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        )
        ON_ERROR = 'CONTINUE';
    """

def get_production_insert_sql():
    """SQL code for moving data to the final layer."""
    # Function to select just required companies e.g Amazon, Microsoft,Apple etc
    return """
        INSERT INTO WIKI_PAGES_VIEWS_FINAL (DOMAIN_CODE, PAGE_TITLE, VIEW_COUNT, RESPONSE_IN_BYTES)
        SELECT DOMAIN_CODE, PAGE_TITLE, VIEW_COUNT, RESPONSE_IN_BYTES
        FROM WIKI_PAGES_VIEWS_STAGING
        WHERE PAGE_TITLE IN ('Amazon', 'Microsoft', 'Apple', 'Facebook', 'Google');
    """

def select_companies_to_list():
    return """SELECT PAGE_TITLE, SUM(VIEW_COUNT) FROM WIKI_PAGES_VIEWS_FINAL GROUP BY 1 ORDER BY 2 DESC LIMIT 1;"""

