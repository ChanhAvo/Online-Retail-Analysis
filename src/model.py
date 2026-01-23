import pandas as pd
import numpy as np 
from sqlalchemy import text
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from src.db_connector import get_engine

#Customer Segmentation 
def rfm_clustering():
    engine = get_engine()
    df_rfm = pd.read_sql("SELECT * FROM rfm_analysis", engine)
    
    scaler = StandardScaler()
    rfm_scaled = scaler.fit_transform(df_rfm[['Recency', 'Frequency', 'Monetary']])
    kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
    df_rfm['Cluster_ID'] = kmeans.fit_predict(rfm_scaled)
    cluster_avg = df_rfm.groupby('Cluster_ID')['Recency'].mean()
    churn_cluster_id = cluster_avg.idxmax() #churn risk
    monetary_avg = df_rfm.groupby('Cluster_ID')['Monetary'].mean() #vip
    vip_cluster_id = monetary_avg.idxmax()
    
    def assign_label(c_id):
        if c_id == churn_cluster_id:
            return "Churn Risk"
        elif c_id == vip_cluster_id:
            return "VIP"
        else:
            return "Regular"
    df_rfm['Segment_Name'] = df_rfm['Cluster_ID'].apply(assign_label)
    df_rfm.to_sql('customer_segments', engine, if_exists='replace', index=False)
    print("RFM Clustering completed and saved to 'customer_segments' table.") 
    
if __name__ == "__main__":
    rfm_clustering()
    