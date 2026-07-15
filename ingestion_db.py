import pandas as pd
import os
import time
import logging
from sqlalchemy import create_engine

# Set up logging -> writes to logs_ingestion.log instead of the screen
logging.basicConfig(
    filename="logs_ingestion.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
)

# Create the database connection (a file called inventory.db)
engine = create_engine("sqlite:///inventory.db")


def ingest_db(df, table_name, engine):
    """Write a dataframe into the database as a table."""
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)


def load_raw_data():
    """Read every CSV in this folder and load it into the database."""
    start = time.time()
    for file in os.listdir('.'):
        if file.endswith('.csv'):
            df = pd.read_csv(file)                 # reads the whole file into memory
            table_name = file[:-4]                 # strip ".csv" -> table name
            logging.info(f'Ingesting {file} ...')
            ingest_db(df, table_name, engine)
    end = time.time()
    total_time = (end - start) / 60
    logging.info('-------- Ingestion Complete --------')
    logging.info(f'Total Time Taken: {total_time:.2f} minutes')


if __name__ == '__main__':
    load_raw_data()
