import psycopg2
import pytest

DB_CONFIG = {
    "dbname": "mta_db",
    "user": "mta_user",
    "password": "mta_pass",
    "host": "localhost",
    "port": 5432,
}

def test_tables_not_empty():
    tables = ["routes", "stops", "trips"]
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table};")
            count = cur.fetchone()[0]
            assert count > 0, f"Table {table} is empty"
        cur.close()
        conn.close()
    except Exception as e:
        pytest.fail(f"DB test failed: {e}")
