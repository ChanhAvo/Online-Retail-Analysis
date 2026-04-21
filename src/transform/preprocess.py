import os 
import pandas as pd

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
INGESTED_DATA_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'ingested_data.parquet')
PROCESSED_DATA_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'preprocessed_data.parquet')

def preprocess():
    df = pd.read_parquet(INGESTED_DATA_PATH)
    df.columns = [c.replace(' ', '') for c in df.columns]
    #Handle duplicates 
    df = df.drop_duplicates()
    #Handle missing values 
    reference = (df.groupby('StockCode')['Description'].agg(lambda x: x.dropna().value_counts().idxmax() if x.dropna().shape[0] > 0 else 'Unknown'))
    df['Description'] = df['Description'].fillna(df['StockCode'].map(reference))
    df = df.dropna(subset=['CustomerID'])
    #Change data type 
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['CustomerID'] = df['CustomerID'].astype(int)
     # Drop cancelled invoices 
    cancelled = df['Invoice'].astype(str).str.startswith('C')
    df = df[~cancelled]
    # Drop invalid transactions
    df = df[(df['Quantity'] > 0) & (df['Price'] > 0)]
    # Clean stock code, supposed to be 5 digits code
    df = df[df['StockCode'].str.strip().str.match(r'^\d{5}[a-zA-Z]*$', na=False)]
    
    print("Current number of rows: ", df.shape[0])
    print("Dataset information: ", df.info())
    
    # Save the preprocessed data
    df.to_parquet(PROCESSED_DATA_PATH, index=False)
if __name__ == "__main__":
    preprocess()