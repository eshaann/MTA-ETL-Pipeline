FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Install cron
RUN apt-get update && apt-get install -y cron

# Copy cron job file into container
COPY crontab /etc/cron.d/mta-etl-cron
RUN chmod 0644 /etc/cron.d/mta-etl-cron
RUN crontab /etc/cron.d/mta-etl-cron

# Start cron in foreground
CMD ["cron", "-f"]