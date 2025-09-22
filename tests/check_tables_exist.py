import psycopg2
import pytest

DB_CONFIG = {
    "dbname": "mta_db",
    "user": "mta_user",
    "password": "mta_pass",
    "host": "localhost",
    "port": 5432,
}

def test_tables_exist():
    tables = ["routes", "stops", "trips", "realtime_stop_updates"]
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        for table in tables:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name=%s
                );
            """, (table,))
            exists = cur.fetchone()[0]
            assert exists, f"Table {table} does not exist"
        cur.close()
        conn.close()
    except Exception as e:
        pytest.fail(f"DB test failed: {e}")
