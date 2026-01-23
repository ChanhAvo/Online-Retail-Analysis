from sqlalchemy import text 
from src.db_connector import get_engine 

def run_transformation():
    engine = get_engine()
    with open('sql/preprocess.sql', 'r') as file:
        sql_script = file.read()
        
    commands = sql_script.split(';')
    with engine.connect() as connector:
        for command in commands:
            if command.strip():
                connector.execute(text(command))
                connector.commit()
    print("Data transformation completed successfully.")

if __name__ == "__main__":
    run_transformation()