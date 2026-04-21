from collections.abc import Generator
from app import settings


def get_conn() -> Generator[object, None, None]:
    """
    Devuelve una conexión PostgreSQL (psycopg, row_factory=dict).
    """
    import psycopg
    from psycopg.rows import dict_row

    conn = psycopg.connect(settings.PSYCOPG_DSN, row_factory=dict_row)
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn.close()