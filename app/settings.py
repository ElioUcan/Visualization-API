import os
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


APP_NAME = "simple-api"

DB_HOST = required("DB_HOST")
DB_PORT = int(required("DB_PORT"))
DB_NAME = required("DB_NAME")
DB_USER = required("DB_USER")
DB_PASSWORD = optional("DB_PASSWORD", "")

# DSN para Postgres (psycopg3)
PSYCOPG_DSN = (
    f"host={DB_HOST} "
    f"port={DB_PORT} "
    f"dbname={DB_NAME} "
    f"user={DB_USER} "
    f"password={DB_PASSWORD}"
)