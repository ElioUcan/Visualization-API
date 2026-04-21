import os
import csv

from pathlib import Path
from psycopg import connect, Connection, Cursor
from dotenv import load_dotenv

load_dotenv()

PRIMARY_KEYS = {
    "accounts": "account_id",
    "branches": "branch_id",
    "cards": "card_id",
    "customers": "customer_id",
    "loans": "loan_id",
    "merchants": "merchant_id",
}

FOREIGN_KEYS = {
    "accounts": [("customer_id", "customers", "customer_id")],
    "cards": [("account_id", "accounts", "account_id")],
    "loans": [("customer_id", "customers", "customer_id")],
}

# Canonical PostgreSQL schema used to create each table from CSV headers.
TABLE_SCHEMAS = {
    "customers": {
        "customer_id": "TEXT",
        "first_name": "TEXT",
        "last_name": "TEXT",
        "email": "TEXT",
        "city": "TEXT",
        "credit_score": "INTEGER",
        "created_at": "DATE",
    },
    "accounts": {
        "account_id": "TEXT",
        "customer_id": "TEXT",
        "account_type": "TEXT",
        "balance_usd": "NUMERIC(14,2)",
        "open_date": "DATE",
    },
    "branches": {
        "branch_id": "TEXT",
        "branch_name": "TEXT",
        "manager_name": "TEXT",
    },
    "cards": {
        "card_id": "TEXT",
        "account_id": "TEXT",
        "card_type": "TEXT",
        "expiration_date": "DATE",
    },
    "loans": {
        "loan_id": "TEXT",
        "customer_id": "TEXT",
        "loan_amount": "NUMERIC(14,2)",
        "interest_rate": "NUMERIC(5,2)",
        "start_date": "DATE",
    },
    "merchants": {
        "merchant_id": "TEXT",
        "merchant_name": "TEXT",
        "city": "TEXT",
    },
}

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

def constraint_exists(cursor: Cursor, constraint_name: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM pg_constraint WHERE conname = %s",
        (constraint_name,),
    )
    return cursor.fetchone() is not None

def create_table_from_csv(csv_file: str, table_name: str, conn: Connection, cursor: Cursor):
    try:
        with open(csv_file, 'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f)
            headers = next(reader)

        table_schema = TABLE_SCHEMAS.get(table_name, {})
        columns = ', '.join([f"{h} {table_schema.get(h, 'TEXT')}" for h in headers])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        cursor.execute(query)

        if PRIMARY_KEYS.get(table_name) in headers:
            constraint_name = f"{table_name}_pkey"
            if not constraint_exists(cursor, constraint_name):
                query = f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} PRIMARY KEY ({PRIMARY_KEYS[table_name]})"
                cursor.execute(query)

        for column_name, reference_table, reference_column in FOREIGN_KEYS.get(table_name, []):
            if column_name in headers:
                constraint_name = f"{table_name}_{column_name}_fkey"
                if not constraint_exists(cursor, constraint_name):
                    query = f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} FOREIGN KEY ({column_name}) REFERENCES {reference_table} ({reference_column})"
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
    data_files = {data.stem: data for data in data_folder.glob("*.csv")}
    table_order = ["customers", "branches", "merchants", "accounts", "cards", "loans"]
    table_order.extend(sorted(table_name for table_name in data_files if table_name not in table_order))

    for table_name in table_order:
        data = data_files.get(table_name)
        if data is None:
            continue
        create_table_from_csv(str(data), table_name, conn, cursor)

    for table_name in table_order:
        data = data_files.get(table_name)
        if data is None:
            continue
        csv_data_to_db(str(data), table_name, conn, cursor)
            
    print("Done.")
    
if __name__ == "__main__":
    main()