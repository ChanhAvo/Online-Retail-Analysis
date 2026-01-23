import time 
from src.ingest import run_ingestion 
from src.transform import run_transformation 
from src.model import rfm_clustering 

def main():
    try: 
        run_ingestion()
        run_transformation()
        rfm_clustering()
        
    except Exception as e:
        print(f"Pipeline failed: {e}")

if __name__ == "__main__":
    main()