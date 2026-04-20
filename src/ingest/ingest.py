import pandas as pd
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
RAW_DATA_PATH = os.path.join(BASE_DIR, 'data', 'online_retail_II.csv')
INTERMEDIATE_DATA_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'ingested_data.parquet')
def run_ingestion():
    #load raw data 
    df = pd.read_csv(RAW_DATA_PATH, encoding='ISO-8859-1')
    df.to_parquet(INTERMEDIATE_DATA_PATH, index=False)
    
    
if __name__ == "__main__":
    run_ingestion()