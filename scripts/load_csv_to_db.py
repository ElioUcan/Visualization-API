import os
import csv

from pathlib import Path
from psycopg import connect, Connection, Cursor
from dotenv import load_dotenv

load_dotenv()

def required(name: str) -> str:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        raise RuntimeError(f"Missing environment variable: {name}")
    return v.strip()


def optional(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else v.strip()

def conn_params() -> dict:
    return {
        "host": optional("DB_HOST", "localhost"),
        "port": int(optional("DB_PORT", "5432")),
        "dbname": required("DB_NAME"),
        "user": required("DB_USER"),
        "password": optional("DB_PASSWORD", ""),
    }
    
def create_table_from_csv(csv_file: str, table_name: str, conn: Connection, cursor: Cursor):
    try:
        with open(csv_file, 'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f)
            headers = next(reader)
        
        columns = ', '.join([f"{h} TEXT" for h in headers])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        cursor.execute(query)
        
        conn.commit()
        print(f"  Successfully created table {table_name} from {csv_file}")
    except Exception as e:
        conn.rollback()
        print(f"  Error creating table {table_name} from {csv_file}: {e}")
            
def csv_data_to_db(csv_file: str, table_name: str, conn: Connection, cursor: Cursor):    
    try: 
        with open(csv_file, 'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f)
            headers = next(reader)
            columns = ', '.join(headers)
            
            for row in reader:
                values = ', '.join(['%s'] * len(row))
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
                cursor.execute(query, row)
    
        conn.commit()
        print(f"  Successfully loaded {csv_file} into {table_name}")
    except Exception as e:
        conn.rollback()
        print(f"  Error loading {csv_file} into {table_name}: {e}")
    
def main():
    print("Loading CSV data into the database...")
    
    conn = connect(**conn_params())
    cursor = conn.cursor()
    
    data_folder = Path(__file__).parent.parent / "data"
    for data in data_folder.glob("*.csv"):
        table_name = data.stem
        
        create_table_from_csv(str(data), table_name, conn, cursor)
        csv_data_to_db(str(data), table_name, conn, cursor)
            
    print("Done.")
    
if __name__ == "__main__":
    main()