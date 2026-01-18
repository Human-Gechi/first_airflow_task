import gzip
import csv
from pathlib import Path
from airflow_task.scripts.logs import logger
import hashlib

def get_gz_file(folder_path):
    """ Gets .gz file path"""
    folder = Path(folder_path)
    gz_files = list(folder.glob("*.gz"))
    return gz_files[0] if gz_files else None

def dataframe_parser():
    """Parses .gz and writes a clean CSV for Snowflake COPY INTO."""
    input_path = get_gz_file("/opt/airflow/dags/airflow_task") #folder path for .gz file
    output_path = "/tmp/wikipedia.csv" #temporary storage of csv file

    if not input_path:
        logger.error("No input file found.") #Error message id no file found
        return None

    logger.info(f"Parsing {input_path} into {output_path}") #Info: Parsing .gz into the temp. storage

    with gzip.open(input_path, "rt", encoding="utf-8", errors="replace") as f_in: #Reading .gz file as f_in
        with open(output_path, "w", newline="", encoding="utf-8") as f_out: #Opening file in the output path as f_out to write as CSV
            writer = csv.writer(f_out, delimiter='|') #CSV writer

            for line in f_in: #Loop in input path/ file
                parts = line.split() #Splitting data on space
                if len(parts) < 4: #Lenght of splitted parts = 3
                    continue
                try: #Try, Except , Block
                    domain = parts[0].replace('""', "").strip() #First part before space
                    title = " ".join(parts[1:-2]).strip() #Accessing the title using negative and positive indexing to avoid errors
                    views =  int(parts[-2]) #Negative indexing accessing second to the last data item
                    bytes_size = int(parts[-1]) #Negative indexing accessing last item
                    hashkey = f"{title[1:4].lower()}|{views}|2025-12-01"
                    encode_hash = hashkey.encode('utf-8')
                    hash_object = hashlib.sha256(encode_hash).hexdigest()
                    writer.writerow([domain, title, views, bytes_size,hash_object]) #Write each row to output file in the output path
                except (ValueError, IndexError): #Do nothing if an error occured
                    continue

    return output_path #Return output path
