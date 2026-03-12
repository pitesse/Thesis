#!/usr/bin/env bash
# chmod +x run_simulation.sh
#
# end-to-end automation: tears down old containers, builds all docker images
# (flink fat jar compiled inside the container, python env frozen in image),
# starts the full stack, submits the flink job, and launches the python producer.
#
# no local java, maven, or python installation required. everything runs in docker.
#
# usage:
#   ./run_simulation.sh                                          # defaults: monza 2023, speed 50
#   ./run_simulation.sh --year 2023 --race "Australian Grand Prix" --speed 50
#   ./run_simulation.sh --year 2024 --race "British Grand Prix" --session Q --speed 10

set -euo pipefail

# ===========================
# defaults
# ===========================
YEAR=2023
RACE="Italian Grand Prix"
SESSION="R"
SPEED=50
START_LAP=1

# ===========================
# parse arguments
# ===========================
while [[ $# -gt 0 ]]; do
	case "$1" in
	--year)
		YEAR="$2"
		shift 2
		;;
	--race)
		RACE="$2"
		shift 2
		;;
	--session)
		SESSION="$2"
		shift 2
		;;
	--speed)
		SPEED="$2"
		shift 2
		;;
	--start-lap)
		START_LAP="$2"
		shift 2
		;;
	*)
		echo "Unknown argument: $1"
		exit 1
		;;
	esac
done

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.yml"

# stop the dashboard container on exit
cleanup() {
	echo ""
	echo "Stopping dashboard container..."
	docker compose -f "$COMPOSE_FILE" stop dashboard 2>/dev/null || true
}
trap cleanup EXIT

echo "========================================"
echo " F1 Strategy Simulation"
echo "========================================"
echo " Year:      $YEAR"
echo " Race:      $RACE"
echo " Session:   $SESSION"
echo " Speed:     ${SPEED}x"
echo " Start Lap: $START_LAP"
echo "========================================"

# ===========================
# 1. tear down existing stack
# ===========================
echo "[1/7] Tearing down existing containers..."
docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true

# ===========================
# 2. build all docker images (flink + python)
# ===========================
# flink image: multi-stage build compiles the fat jar inside maven:3.9-eclipse-temurin-17
# python image: installs all deps from requirements.txt into python:3.12-slim
echo "[2/7] Building Docker images (Flink + Python)..."
docker compose -f "$COMPOSE_FILE" build

# ===========================
# 3. prepare data_lake directory + start docker stack
# ===========================
# flink:1.20-java17 runs as user "flink" (uid 9999). the bind-mounted data_lake
# directory must be writable by that uid. chmod 777 avoids requiring sudo/chown
# and is acceptable here since this is a local development mount, not a shared server.
echo "[3/7] Starting Docker stack..."
mkdir -p "$PROJECT_DIR/data_lake"
chmod -R 777 "$PROJECT_DIR/data_lake" 2>/dev/null || true
docker compose -f "$COMPOSE_FILE" up -d

# wait for flink jobmanager rest api to be ready
echo "       Waiting for Flink JobManager REST API..."
for i in $(seq 1 30); do
	if curl -sf http://localhost:8081/overview >/dev/null 2>&1; then
		echo "       JobManager ready."
		break
	fi
	if [ "$i" -eq 30 ]; then
		echo "ERROR: Flink JobManager did not start within 30s"
		exit 1
	fi
	sleep 1
done

# ===========================
# 4. pre-create kafka topics
# ===========================
echo "[4/7] Creating Kafka topics..."
for topic in f1-telemetry f1-laps f1-track-status f1-alerts; do
	docker exec kafka kafka-topics \
		--bootstrap-server localhost:29092 \
		--create --topic "$topic" \
		--partitions 1 --replication-factor 1 \
		--if-not-exists 2>/dev/null || true
done

# ===========================
# 5. submit flink job (jar is baked into the image)
# ===========================
echo "[5/7] Submitting Flink job..."
docker exec flink-jobmanager flink run \
	-d /opt/flink/usrlib/f1-stream-processor.jar

# ===========================
# 6. dashboard is already running via docker compose up
# ===========================
echo "[6/7] Dashboard is running at http://localhost:8501"

# open the dashboard in the default browser after a short delay so streamlit
# has time to bind the port. uses python's webbrowser module which works on
# linux, macos, and windows/wsl without platform-specific commands.
(sleep 3 && python3 -m webbrowser "http://localhost:8501" 2>/dev/null) &

# ===========================
# 7. prepare data + start python producer
# ===========================
echo "[7/7] Starting Python producer (two-stage pipeline)..."
echo "       Args: --year $YEAR --race \"$RACE\" --session $SESSION --speed $SPEED --start-lap $START_LAP"

# stage 1: prepare parquet (skip if already exists for this race/year/session)
SAFE_RACE=$(echo "$RACE" | tr ' ' '_')
PARQUET_FILE="$PROJECT_DIR/data/${YEAR}_${SAFE_RACE}_${SESSION}_prepared.parquet"

if [ -f "$PARQUET_FILE" ]; then
	echo "       Parquet already exists: $PARQUET_FILE (skipping prepare)"
else
	echo "       Running prepare_race.py..."
	docker compose -f "$COMPOSE_FILE" run --rm producer \
		python f1-telemetry-producer/src/prepare_race.py \
		--year "$YEAR" \
		--race "$RACE" \
		--session "$SESSION"
fi

# stage 2: stream to kafka
docker compose -f "$COMPOSE_FILE" run --rm producer \
	python f1-telemetry-producer/src/stream_race.py \
	--year "$YEAR" \
	--race "$RACE" \
	--session "$SESSION" \
	--speed "$SPEED" \
	--start-lap "$START_LAP"

echo ""
echo "========================================"
echo " Simulation complete."
echo " Dashboard:  http://localhost:8501"
echo " Flink UI:   http://localhost:8081"
echo " Flink logs: docker logs -f flink-taskmanager"
echo "========================================"
