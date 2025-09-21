import psycopg2
import pytest

DB_CONFIG = {
    "dbname": "mta_db",
    "user": "mta_user",
    "password": "mta_pass",
    "host": "localhost",
    "port": 5432,
}

def test_realtime_updates_have_trip_ids():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM realtime_stop_updates WHERE trip_id IS NULL;")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    assert count == 0, f"Found {count} rows with NULL trip_id!"
