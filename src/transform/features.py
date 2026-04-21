import os 
import pandas as pd 
import matplotlib.pyplot as plt 
import seaborn as sns

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
PROCESSED_DATA_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'preprocessed_data.parquet')
FEATURES_DATA_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'features_data.parquet')
recency_bins = [0, 30, 90, 180, 365, float('inf')]
recency_labels = ['Active', 'Recent', 'Lapsing', 'At Risk', 'Churned']

def dashboard_features():
    df = pd.read_parquet(PROCESSED_DATA_PATH)
    
    # Customer level features 
    rfm = df.groupby('CustomerID').agg(
        Frequency = ('InvoiceNo', 'nunique'),
        Monetary = ('Price', 'sum'),
        FirstPurchase = ('InvoiceDate', 'min'),
        LastPurchase = ('InvoiceDate', 'max'),
        ActiveMonths  = ('InvoiceDate', lambda x: x.dt.to_period('M').nunique()),
    )
    
    rfm['Recency'] = (df['InvoiceDate'].max() - rfm['LastPurchase']).dt.days
    
    # Derived features 
    rfm['AvgOrderValue'] = (rfm['Monetary'] / rfm['Frequency']).round(2)
    rfm['RecencyBucket'] = pd.cut( rfm['Recency'], bins = recency_bins, labels = recency_labels, right = True, include_lowest = True,).astype(str)
    
    # Visualize the distribution of features 

    print("Generating outlier visualizations...")
    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    
    # Recency Boxplot
    sns.boxplot(y=rfm['Recency'], ax=axes[0], color='skyblue')
    axes[0].set_title('Recency (Days)', fontsize=14)
    axes[0].set_ylabel('Days Since Last Purchase')
    
    # Frequency Boxplot
    sns.boxplot(y=rfm['Frequency'], ax=axes[1], color='lightgreen')
    axes[1].set_title('Frequency (Unique Invoices)', fontsize=14)
    axes[1].set_ylabel('Invoice Count')
    
    # Monetary Boxplot
    sns.boxplot(y=rfm['Monetary'], ax=axes[2], color='salmon')
    axes[2].set_title('Monetary (Total Spend)', fontsize=14)
    axes[2].set_ylabel('Total Revenue (£)')
    
    plt.suptitle('RFM Outlier Detection', fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.show()
    
    return rfm 

if __name__ == "__main__":
    dashboard_features() 
