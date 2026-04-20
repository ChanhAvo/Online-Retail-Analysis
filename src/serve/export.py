import pandas as pd
import os
from src.serve.db_connector import get_engine

def export_data():
    print("🚀 Starting Export...")  # <--- Feedback
    engine = get_engine()
  
    if not os.path.exists('dashboard_data'):
        os.makedirs('dashboard_data')

    print("   - Reading from MySQL...")
    df_seg = pd.read_sql("SELECT * FROM customer_segments", engine)
    
    # Check if data exists
    if df_seg.empty:
        print("⚠️ WARNING: The table is empty! Run src/main.py first.")
    else:
        print(f"   - Found {len(df_seg)} rows.")
        df_seg.to_csv('dashboard_data/customer_segments.csv', index=False)
        print("✅ Success! File saved to: dashboard_data/customer_segments.csv")
    
if __name__ == "__main__":
    export_data()