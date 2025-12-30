import psycopg2
import os
from dotenv import load_dotenv
from logs import logger
load_dotenv()

def get_connection():
    try:
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            dbname=os.getenv("POSTGRES_NAME"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            sslmode="require"
        )
    except psycopg2.OperationalError as e:
        logger.info("Could not connect to server: Connection refused ; {e}")
    finally:
        logger.info("Database connection successful")

def create_tables():
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        logger.info("Database connection is successful")

        create_table_query = """
        CREATE TABLE IF NOT EXISTS wiki_pages_views (
            domain_code VARCHAR(100),
            page_title TEXT,
            view_count INT,
            response_in_bytes BIGINT 
            ON CONFLICT DO NOTHING)
        """

        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Table 'wiki_pages_views' has been created successfully")
    except Exception as e:
        logger.exception(f"Error creating table: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info("Database connection closed after table creation")

def insert_tables():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
                    INSERT INTO wiki_pages_views (domain_code, page_title ,view_count, response_in_bytes)
                    VALUES (%s, %s, %s, %s, %s)
                """)
        conn.commit()
    except Exception as e:
        logger.error(f"An error occured as {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info("Database connection closed after data insertion")
if __name__ == "__main__":
    create_tables()