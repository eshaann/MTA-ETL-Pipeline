****MTA Realtime Transit ETL Pipeline****

This project is an end-to-end ETL pipeline that ingests GTFS and GTFS-RT transit data into PostgreSQL, providing real-time monitoring and visualization of NYC subway stop activity and route performance. It includes data ingestion, transformation, storage, and interactive dashboards for analytics.

**Features**

**ETL Pipeline**: Ingests 500k+ GTFS-RT updates daily, validates and stores them in PostgreSQL.

**Realtime Dashboards**: Visualize stop-level congestion, delays, and update frequency using Streamlit + Pydeck.

**Automation**: Containerized with Docker, scheduled with cron, and integrated with GitHub Actions CI/CD for continuous ingestion.

**Impact**: Enables low-latency monitoring and actionable insights for thousands of daily transit updates.

****Usage****

Configure database connection in config.py.

**Build and run Docker containers**:

docker compose up --build


Access dashboards via Streamlit to explore transit data in real time.

Tech Stack

Python, PostgreSQL, Streamlit, Pydeck, Docker, GitHub Actions
