import psycopg2
import pytest

DB_CONFIG = {
    "dbname": "mta_db",
    "user": "mta_user",
    "password": "mta_pass",
    "host": "localhost",
    "port": 5432,
}

def test_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.close()
    except Exception as e:
        pytest.fail(f"Cannot connect to DB: {e}")
