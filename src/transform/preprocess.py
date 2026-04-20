import os 
import pandas as pd

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
INGESTED_DATA_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'ingested_data.parquet')

def preprocess():
    df = pd.read_parquet(INGESTED_DATA_PATH)
    df.columns = [c.replace(' ', '') for c in df.columns]
    #Handle duplicates 
    df = df.drop_duplicates()
    #Change data type 
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['CustomerID'] = df['CustomerID'].astype(int)
    #Handle missing values 
    df['CustomerID'] = df['CustomerID'].dropna()
    reference = (df.groupby('StockCode')['Description'].agg(lambda x: x.dropna().value_counts().idxmax() if x.dropna().shape[0] > 0 else 'Unknown'))
    df['Description'] = df['Description'].fillna(df['StockCode'].map(reference))
     # Drop cancelled invoices 
    cancelled = df['Invoice'].astype(str).str.startswith('C')
    df = df[~cancelled]
    # Drop invalid transactions
    df = df[(df['Quantity'] > 0) & (df['Price'] > 0)]
    # Clean stock code, supposed to be 5 ditigts code
    removed_stockcodes = ['POST', 'DOT', 'M', 'C2', 'BANK CHARGES', 'TEST001', 'gift_0001_80', 'gift_0001_20',
                     'TEST002', 'gift_0001_10', 'gift_0001_50', 'gift_0001_30',
                      'PADS', 'ADJUST', 'gift_0001_40', 'gift_0001_60', 'gift_0001_70', 'gift_0001_90', 'm',
                      'D', 'S', 'DCGSSBOY', 'DCGSSGIRL', 'ADJUST2', 'AMAZONFEE', 'B']
    mask = ~(df['StockCode'].isin(removed_stockcodes))
    df = df[mask]
    # Save the preprocessed data
    df
if __name__ == "__main__":
    preprocess()