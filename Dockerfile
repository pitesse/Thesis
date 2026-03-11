# python runtime for the f1 telemetry producer, dashboard, and ml pipeline.
# single image shared by all python services to avoid dependency drift.
FROM python:3.12-slim

# ca-certificates: required by fastf1 for https api calls
# fontconfig: required by matplotlib for font rendering in ml pipeline plots
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates fontconfig && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# install python deps first (layer cache: source changes don't invalidate this)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy all python source
COPY f1-telemetry-producer/ f1-telemetry-producer/
COPY dashboard/ dashboard/
COPY ml_pipeline/ ml_pipeline/

# stream_race.py imports from prepare_race.py via `from prepare_race import parquet_filename`.
# both live in f1-telemetry-producer/src/, so that directory must be on PYTHONPATH.
ENV PYTHONPATH="/app/f1-telemetry-producer/src"

# default kafka broker for container-to-container communication.
# override with KAFKA_BROKER=localhost:9092 when running on the host.
ENV KAFKA_BROKER="kafka:29092"

# streamlit default port
EXPOSE 8501
