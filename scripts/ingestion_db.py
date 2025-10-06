import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# ------------------- Logging -------------------
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# ------------------- Database -------------------
engine = create_engine('sqlite:///inventory.db')

# ------------------- Functions -------------------
def ingest_db(df, table_name, engine, if_exists='replace'):
    """This function will ingest the dataframe into database table"""
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)

def load_raw_data(chunk_size=100000):
    """This function will load the CSVs as dataframe and ingest into db in chunks"""
    start = time.time()

    for file in os.listdir('data'):
        if file.endswith('.csv'):
            file_path = os.path.join('data', file)
            table_name = file[:-4]

            logging.info(f"Starting ingestion of {file}")
            first_chunk = True

            # Read CSV in chunks
            for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
                logging.info(f"Ingesting {file} - chunk {i+1}")
                if first_chunk:
                    ingest_db(chunk, table_name, engine, if_exists='replace')
                    first_chunk = False
                else:
                    ingest_db(chunk, table_name, engine, if_exists='append')

            logging.info(f"Completed ingestion of {file}")
            print(f"Completed ingestion of {file}")  # shows progress in Jupyter

    end = time.time()
    total_time = (end - start)/60
    logging.info("----------Injection Complete----------")
    logging.info(f"Total Time Taken: {total_time:.2f} minutes")
    print(f"Total Time Taken: {total_time:.2f} minutes")

# ------------------- Run -------------------
if __name__ == '__main__':
    load_raw_data(chunk_size=100000)  # adjust chunk_size if needed
