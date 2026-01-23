import pandas as pd
from src.db_connector import get_engine

def run_ingestion():
    df = pd.read_csv('data/online_retail_II.csv', encoding='ISO-8859-1')
    df.columns = [c.replace(' ','') for c in df.columns]
    engine = get_engine()
    df.to_sql('raw_data', engine, if_exists='replace', index=False, chunksize=10000)
    print("Data ingestion completed successfully.")

if __name__ == "__main__":
    run_ingestion()