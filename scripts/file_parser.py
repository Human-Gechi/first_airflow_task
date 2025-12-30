#import gzip
from pathlib import Path
from logs import logger

def check_ext(folder_path):
    folder = Path(folder_path)

    gz_files = list(folder.glob("*.gz"))

    if not gz_files:
        print("No .gz files found")
        return
    if gz_files:
        file_path = gz_files[0]
        logger.info(f"File path : {file_path}")

check_ext(r"C:\Users\HP\airflow\dags\airflow_task")
#with gzip.open(file_path, 'rt') as f:
  #      for _ in range(50):
   #         line = f.readline()
    #        if not line:
     #           break
      #      print(line.strip())

