# dashboard.py
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import pydeck as pdk
import numpy as np

# --- DB config ---
DB_CONFIG = {
    "dbname": "mta_db",
    "user": "mta_user",
    "password": "mta_pass",
    "host": "db",
    "port": 5432,
}

engine = create_engine(
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)

st.set_page_config(page_title="MTA Realtime Dashboard", layout="wide")
st.title("🚆 MTA Realtime Dashboard")
st.markdown(
    "Simple ETL & analytics pipeline using GTFS-RT data in PostgreSQL."
)

# --- Fetch data ---
def fetch_data(query):
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()


st.subheader("🗺 Stop Locations and Activity (Update Counts with Color)")

stop_map_query = """
SELECT s.stop_name, s.stop_lat, s.stop_lon, COUNT(*) AS num_updates
FROM realtime_stop_updates rsu
JOIN stops s ON rsu.stop_id = s.stop_id
GROUP BY s.stop_name, s.stop_lat, s.stop_lon
ORDER BY num_updates DESC
LIMIT 100;
"""
stop_map_df = fetch_data(stop_map_query)

if not stop_map_df.empty:
    # Normalize num_updates to 0-255 for color
    max_updates = stop_map_df["num_updates"].max()
    stop_map_df["color_scale"] = ((stop_map_df["num_updates"] / max_updates) * 255).astype(int)

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=stop_map_df,
        get_position=["stop_lon", "stop_lat"],
        get_radius="num_updates * 5",  # bigger circle for more updates
        get_fill_color="[color_scale, 0, 255 - color_scale, 80]",  # gradient from blue to red
        pickable=True,
    )

    view_state = pdk.ViewState(
        latitude=stop_map_df["stop_lat"].mean(),
        longitude=stop_map_df["stop_lon"].mean(),
        zoom=11,
        pitch=0
    )

    r = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={"text": "{stop_name}\nUpdates: {num_updates}"}
    )

    st.pydeck_chart(r)
else:
    st.info("No stop location data available yet.")


st.subheader("Trips per Route")
trips_per_route_query = """
SELECT r.route_long_name, COUNT(*) AS num_trips
FROM trips t
JOIN routes r ON t.route_id = r.route_id
GROUP BY r.route_long_name
ORDER BY num_trips DESC;
"""
trips_per_route_df = fetch_data(trips_per_route_query)

if not trips_per_route_df.empty:
    st.bar_chart(trips_per_route_df.set_index("route_long_name"))
else:
    st.info("No trip data available yet.")


# --- 1. Updates per Route ---
st.subheader("📊 Updates per Trip (Top 20)")

updates_per_trip_query = """
SELECT trip_id, COUNT(*) AS num_updates
FROM realtime_stop_updates
GROUP BY trip_id
ORDER BY num_updates DESC
LIMIT 20;
"""
updates_per_trip_df = fetch_data(updates_per_trip_query)

if not updates_per_trip_df.empty:
    st.bar_chart(updates_per_trip_df.set_index("trip_id"))
else:
    st.info("No updates available yet.")

# --- 2. Top Stops by Update Frequency ---
st.subheader("Top 10 Stops by Update Frequency")
top_stops_query = """
SELECT s.stop_name, COUNT(*) AS num_updates
FROM realtime_stop_updates rsu
LEFT JOIN stops s ON rsu.stop_id = s.stop_id
GROUP BY s.stop_name
ORDER BY num_updates DESC
LIMIT 10;
"""
top_stops_df = fetch_data(top_stops_query)
st.table(top_stops_df)

# --- 3. Updates Over Time ---
st.subheader("Updates Over Time (Last Hour, 5-min buckets)")
updates_over_time_query = """
SELECT date_trunc('minute', rsu.timestamp) AS minute_bucket,
       COUNT(*) AS updates
FROM realtime_stop_updates rsu
WHERE rsu.timestamp > now() - interval '1 hour'
GROUP BY minute_bucket
ORDER BY minute_bucket;
"""
updates_over_time_df = fetch_data(updates_over_time_query)

if not updates_over_time_df.empty:
    st.line_chart(updates_over_time_df.set_index("minute_bucket"))
else:
    st.info("No recent updates available yet.")

# --- 4. Latest Realtime Stop Updates ---
st.subheader("Latest Stop Updates (Last 50)")
latest_query = """
SELECT rsu.trip_id, rsu.stop_id, s.stop_name, rsu.arrival_time, rsu.departure_time
FROM realtime_stop_updates rsu
LEFT JOIN stops s ON rsu.stop_id = s.stop_id
LIMIT 50;
"""
latest_df = fetch_data(latest_query)
st.dataframe(latest_df)

st.subheader("Active Trips in Last Hour")
active_trips_query = """
SELECT COUNT(DISTINCT trip_id) AS active_trips
FROM realtime_stop_updates
WHERE timestamp > now() - interval '1 hour';
"""
active_trips_df = fetch_data(active_trips_query)
st.metric("Active Trips (Last Hour)", int(active_trips_df["active_trips"].iloc[0]) if not active_trips_df.empty else 0)


st.subheader("📊 Average Updates per Stop")
avg_updates_query = """
SELECT s.stop_name, COUNT(*)::float / (SELECT COUNT(DISTINCT trip_id) FROM realtime_stop_updates) AS avg_updates
FROM realtime_stop_updates rsu
JOIN stops s ON rsu.stop_id = s.stop_id
GROUP BY s.stop_name
ORDER BY avg_updates DESC
LIMIT 10;
"""
avg_updates_df = fetch_data(avg_updates_query)
st.bar_chart(avg_updates_df.set_index("stop_name"))


st.subheader("⚠ Stops with No Updates in Last Hour")
no_updates_query = """
SELECT s.stop_name
FROM stops s
LEFT JOIN realtime_stop_updates rsu 
  ON s.stop_id = rsu.stop_id 
  AND rsu.timestamp > now() - interval '1 hour'
WHERE rsu.stop_id IS NULL
LIMIT 10;
"""
no_updates_df = fetch_data(no_updates_query)
st.table(no_updates_df)





# --- Footer ---
st.markdown("---")
st.caption("Data updates every time the ETL pipeline fetches new GTFS-RT updates.")
