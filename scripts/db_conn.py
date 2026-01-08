#Necessary Libraries for the task
import os
from dotenv import load_dotenv
from airflow_task.scripts.logs import logger
import snowflake.connector

load_dotenv()

def get_snowflake_connection():
    """Function to make a Snowflake Connection"""
    try:
        #Making Snowflake connection.
        return snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE")
        )
    except Exception as e:
        #Error message
        logger.error(f"Could not connect to Snowflake: {e}")
        return None

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
    """SQL code for high-performance loading instead of loading with_pandas to prevent signal_9 errors in Airflow"""
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
    """SQL for selecting company analysis"""
    conn = get_snowflake_connection()
    if not conn:
        return [] #Return empty list of no connection was made
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT PAGE_TITLE, SUM(VIEW_COUNT) FROM WIKI_PAGES_VIEWS_FINAL GROUP BY 1;")
           #Output message for companies in the group
            return [{"company": r[0], "total_views": r[1]} for r in cur.fetchall()]
    finally:
        conn.close()
