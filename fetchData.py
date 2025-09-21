import requests
import psycopg2
import zipfile
import io
import csv
from datetime import datetime
from google.transit import gtfs_realtime_pb2
import os

# Config
REALTIME_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm"
STATIC_URL   = "https://transitfeeds.com/p/mta/79/latest/download"  # GTFS static ZIP

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "mta_db"),
    "user": os.getenv("POSTGRES_USER", "mta_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "mta_pass"),
    "host": os.getenv("POSTGRES_HOST", "db"),  # <-- use service name
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}

#DB conn
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# --- Load static GTFS ---
def load_static_gtfs():
    print("⬇️ Downloading static GTFS…")
    resp = requests.get(STATIC_URL)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        with z.open("routes.txt") as f:
            load_routes_csv(f)
        with z.open("stops.txt") as f:
            load_stops_csv(f)
        with z.open("trips.txt") as f:
            load_trips_csv(f)

def load_routes_csv(f):
    conn = get_db_connection()
    cur = conn.cursor()
    reader = csv.DictReader(io.TextIOWrapper(f, "utf-8"))
    for row in reader:
        cur.execute("""
            INSERT INTO routes (route_id, route_short_name, route_long_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (route_id) DO NOTHING
        """, (row["route_id"], row.get("route_short_name"), row.get("route_long_name")))
    conn.commit()
    cur.close()
    conn.close()

def load_stops_csv(f):
    conn = get_db_connection()
    cur = conn.cursor()
    reader = csv.DictReader(io.TextIOWrapper(f, "utf-8"))
    for row in reader:
        cur.execute("""
            INSERT INTO stops (stop_id, stop_name, stop_lat, stop_lon)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (stop_id) DO NOTHING
        """, (row["stop_id"], row["stop_name"], row["stop_lat"], row["stop_lon"]))
    conn.commit()
    cur.close()
    conn.close()

def load_trips_csv(f):
    conn = get_db_connection()
    cur = conn.cursor()
    reader = csv.DictReader(io.TextIOWrapper(f, "utf-8"))
    for row in reader:
        cur.execute("""
            INSERT INTO trips (route_id, service_id, trip_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (trip_id) DO NOTHING
        """, (row["route_id"], row["service_id"], row["trip_id"]))
    conn.commit()
    cur.close()
    conn.close()

# --- Fetch GTFS-RT ---
def fetch_realtime():
    resp = requests.get(REALTIME_URL)
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed

# --- Load realtime stop updates ---
def load_realtime_stop_updates(feed):
    conn = get_db_connection()
    cur = conn.cursor()
    for entity in feed.entity:
        if entity.HasField("trip_update"):
            trip_update = entity.trip_update
            trip_id = trip_update.trip.trip_id
            for stu in trip_update.stop_time_update:
                stop_id = stu.stop_id
                arrival = datetime.fromtimestamp(stu.arrival.time) if stu.HasField("arrival") else None
                departure = datetime.fromtimestamp(stu.departure.time) if stu.HasField("departure") else None
                stop_sequence = stu.stop_sequence if stu.HasField("stop_sequence") else None
                timestamp = datetime.now()
                cur.execute("""
                    INSERT INTO realtime_stop_updates (trip_id, stop_id, arrival_time, departure_time, stop_sequence, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trip_id, stop_id, arrival_time) DO UPDATE
                    SET departure_time = EXCLUDED.departure_time,
                        stop_sequence = EXCLUDED.stop_sequence,
                        timestamp = EXCLUDED.timestamp
                """, (trip_id, stop_id, arrival, departure, stop_sequence, timestamp))
    conn.commit()
    cur.close()
    conn.close()

# --- Load vehicle positions ---
def load_vehicle_positions(feed):
    conn = get_db_connection()
    cur = conn.cursor()
    for entity in feed.entity:
        if entity.HasField("vehicle"):
            vehicle = entity.vehicle
            vehicle_id = vehicle.vehicle.id
            trip_id = vehicle.trip.trip_id if vehicle.trip else None
            stop_id = vehicle.stop_id if vehicle.HasField("stop_id") else None
            timestamp = datetime.fromtimestamp(vehicle.timestamp) if vehicle.HasField("timestamp") else datetime.now()
            lat = vehicle.position.latitude if vehicle.HasField("position") else None
            lon = vehicle.position.longitude if vehicle.HasField("position") else None
            cur.execute("""
                INSERT INTO vehicles (vehicle_id, trip_id, current_stop_id, latitude, longitude, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (vehicle_id) DO UPDATE
                SET trip_id = EXCLUDED.trip_id,
                    current_stop_id = EXCLUDED.current_stop_id,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    timestamp = EXCLUDED.timestamp
            """, (vehicle_id, trip_id, stop_id, lat, lon, timestamp))
    conn.commit()
    cur.close()
    conn.close()

# --- Main ---
if __name__ == "__main__":
    print("📥 Loading static GTFS into Postgres…")
    load_static_gtfs()
    print("✅ Static GTFS loaded.")

    print("📡 Fetching realtime GTFS-RT…")
    feed = fetch_realtime()
    load_realtime_stop_updates(feed)
    load_vehicle_positions(feed)
    print("✅ Realtime updates loaded.")
