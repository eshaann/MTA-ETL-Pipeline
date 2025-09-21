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
SELECT rsu.trip_id, rsu.stop_id, s.stop_name, rsu.arrival_time, rsu.departure_time, rsu.timestamp
FROM realtime_stop_updates rsu
LEFT JOIN stops s ON rsu.stop_id = s.stop_id
ORDER BY rsu.timestamp DESC
LIMIT 50;
"""
latest_df = fetch_data(latest_query)
st.dataframe(latest_df)


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



st.subheader("🚍 Active Trips by Service Type")
active_trips_query = """
SELECT t.service_id, COUNT(DISTINCT rsu.trip_id) AS active_trips
FROM realtime_stop_updates rsu
JOIN trips t ON rsu.trip_id = t.trip_id
GROUP BY t.service_id
ORDER BY active_trips DESC;
"""
active_trips_df = fetch_data(active_trips_query)
st.bar_chart(active_trips_df.set_index("service_id"))




# --- Footer ---
st.markdown("---")
st.caption("Data updates every time the ETL pipeline fetches new GTFS-RT updates.")
