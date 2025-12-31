import gzip
import csv
from pathlib import Path
from airflow_task.scripts.logs import logger

def get_gz_file(folder_path):
    folder = Path(folder_path)
    gz_files = list(folder.glob("*.gz"))
    return gz_files[0] if gz_files else None

def dataframe_parser():
    """Parses .gz and writes a clean CSV for Snowflake COPY INTO."""
    input_path = get_gz_file("/opt/airflow/dags/airflow_task")
    output_path = "/tmp/processed_wiki_data.csv"

    if not input_path:
        logger.error("No input file found.")
        return None

    logger.info(f"Parsing {input_path} into {output_path}")

    with gzip.open(input_path, "rt", encoding="utf-8", errors="replace") as f_in:
        with open(output_path, "w", newline="", encoding="utf-8") as f_out:
            writer = csv.writer(f_out, delimiter='|')

            for line in f_in:
                parts = line.split()
                if len(parts) < 4: continue
                try:
                    
                    domain = parts[0].replace('""', "")
                    title = " ".join(parts[1:-2])
                    views = int(parts[-2])
                    bytes_size = int(parts[-1])
                    writer.writerow([domain, title, views, bytes_size])
                except (ValueError, IndexError):
                    continue

    return output_path
