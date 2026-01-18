# 🚀 Wikipedia Pageview Data Pipeline (Airflow)

This repository contains an automated data pipeline built with **Apache Airflow**. The project handles the end-to-end extraction, transformation, and loading (ETL) of Wikipedia pageview traffic data into a **Snowflake** data warehouse.

---

## 🎯 Project Overview/Statement
This repository contains the first version of the **LaunchSentiment** data pipeline, built as part of a data engineering capstone project. The project validates a market hypothesis: that fluctuations in a company’s Wikipedia pageviews can serve as a sentiment indicator for its stock performance.With emphasis placed on the Big namely;  Microsoft,Amazon,Apple,Google,Facebook

The pipeline manages the lifecycle of Wikipedia's traffic data. It automates the ingestion of raw, compressed files, applies cleaning logic via Python to ensure data quality, and stages the data for analytical processing.



---

## 🛠️ Tech Stack
* **Orchestration:** Apache Airflow
* **Data Warehouse:** Snowflake
* **Languages:** Python (Pandas), SQL
* **Infrastructure:** Docker

---

## 🏗️ Pipeline Architecture
The DAG (Directed Acyclic Graph) is structured into distinct functional units to ensure reliability and scalability:

1.  **Extraction & Parsing:**
    * Uses a custom `dataframe_parser` to efficiently read and process raw `.gz` files.
    * Standardizes raw traffic logs into a structured format for downstream use.
2.  **Transformation:**
    * Performs cleaning and deduplication using Python.
    * Exports processed data to a temporary storage path before cloud ingestion.
3.  **Snowflake Integration (`upload_to_stage`):**
    * Utilizes the **SnowflakeHook** to manage secure warehouse connections.
    * Executes a `PUT` command to move files into the `@WIKI_STAGING_STAGE` internal stage before uploading to the raw layer and on filtering for the five companies, data is inserted into staging.
    * Includes `OVERWRITE=TRUE` logic to ensure **idempotency**, allowing tasks to be safely re-run without creating duplicate data.


---
## 📂 Key Components
* **`dags.py`**: The core script containing the DAG definition and task logic.
* **`db_conn.py`**: Contains SQL logic: Tables creation, data insertion etc
* **Dockerized Environment**: The entire stack is containerized, ensuring that the Airflow scheduler, webserver, and worker run consistently across all environments.
* **`file_parser.py`** : Contains parsing pageviews file content for 1st December 2025 at 10:00 PM
* **Snowflake Data Warehouse**: Snowflake for holding the 8 million rows of raw data and the final production table ready to be used for sentiment analysis
---

## 🚀 Getting Started
To run this pipeline locally:

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/Human-Gechi/first_airflow_task.git
    cd first_airflow_task
    ```
2. **Ensure you have Airflow installed**
3.  **Initialize Docker:**
    ```bash
    docker-compose up -d
    ```
4.  **Configure Connections:**
    Set up your Snowflake credentials in the Airflow UI under `Admin > Connections` to allow the **SnowflakeHook** to authenticate.

---

## 👤 About the Author
**Ogechukwu Abimbola Okoli**

I help **scale data pipelines** by building the "digital plumbing" that moves and cleans information.

* **Focus:** Building reliable and scalable data pipelines
* **LinkedIn:**