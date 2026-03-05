#!/usr/bin/env bash
# chmod +x run_simulation.sh
#
# end-to-end automation: tears down old containers, rebuilds the flink jar,
# starts the docker stack, submits the flink job, and launches the python producer.
#
# usage:
#   ./run_simulation.sh                                          # defaults: monza 2023, speed 50, pit-loss 25
#   ./run_simulation.sh --year 2023 --race "Australian Grand Prix" --speed 50 --pit-loss 22
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
PIT_LOSS=25.0

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
	--pit-loss)
		PIT_LOSS="$2"
		shift 2
		;;
	*)
		echo "Unknown argument: $1"
		exit 1
		;;
	esac
done

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROCESSOR_DIR="$PROJECT_DIR/f1-telemetry-processor"
PRODUCER_DIR="$PROJECT_DIR/f1-telemetry-producer"
DASHBOARD_DIR="$PROJECT_DIR/dashboard"
JAR_NAME="f1-telemetry-processor-1.0-SNAPSHOT.jar"
FLINK_MAIN_CLASS="com.polimi.f1.F1StreamingJob"

# track background pids for cleanup on exit
DASHBOARD_PID=""
cleanup() {
	if [ -n "$DASHBOARD_PID" ] && kill -0 "$DASHBOARD_PID" 2>/dev/null; then
		echo ""
		echo "Stopping dashboard (PID $DASHBOARD_PID)..."
		kill "$DASHBOARD_PID" 2>/dev/null || true
	fi
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
echo " Pit Loss:  ${PIT_LOSS}s"
echo "========================================"

# ===========================
# 1. tear down existing stack
# ===========================
echo "[1/7] Tearing down existing containers..."
sudo docker compose -f "$PROJECT_DIR/docker-compose.yml" down -v 2>/dev/null || true

# ===========================
# 2. build the flink fat jar
# ===========================
echo "[2/7] Building Flink JAR..."
cd "$PROCESSOR_DIR"
mvn clean package -q -DskipTests
cd "$PROJECT_DIR"

# ===========================
# 3. prepare data_lake directory + start docker stack
# ===========================
# flink:1.20-java17 runs as user "flink" (uid 9999). the bind-mounted data_lake
# directory must be writable by that uid, otherwise FileSink silently fails.
echo "[3/7] Starting Docker stack..."
mkdir -p "$PROJECT_DIR/data_lake"
sudo chown 9999:9999 "$PROJECT_DIR/data_lake"
sudo docker compose -f "$PROJECT_DIR/docker-compose.yml" up -d

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
	sudo docker exec kafka kafka-topics \
		--bootstrap-server localhost:29092 \
		--create --topic "$topic" \
		--partitions 1 --replication-factor 1 \
		--if-not-exists 2>/dev/null || true
done

# ===========================
# 5. copy jar and submit flink job
# ===========================
echo "[5/7] Submitting Flink job (pit-loss=${PIT_LOSS})..."
sudo docker exec flink-jobmanager mkdir -p /opt/flink/usrlib
sudo docker cp "$PROCESSOR_DIR/target/$JAR_NAME" flink-jobmanager:/opt/flink/usrlib/
sudo docker exec flink-jobmanager flink run \
	-d \
	"/opt/flink/usrlib/$JAR_NAME" \
	--pit-loss "$PIT_LOSS"

# ===========================
# 6. launch streamlit dashboard
# ===========================
echo "[6/7] Launching Streamlit dashboard..."

# activate dashboard venv if available, otherwise rely on system/producer venv
if [ -f "$DASHBOARD_DIR/venv/bin/activate" ]; then
	source "$DASHBOARD_DIR/venv/bin/activate"
elif [ -f "$DASHBOARD_DIR/.venv/bin/activate" ]; then
	source "$DASHBOARD_DIR/.venv/bin/activate"
fi

streamlit run "$DASHBOARD_DIR/app.py" --server.headless true &
DASHBOARD_PID=$!
echo "       Dashboard running (PID $DASHBOARD_PID) at http://localhost:8501"

# open the dashboard in the default browser after a short delay so streamlit
# has time to bind the port. uses xdg-open (freedesktop.org standard on linux),
# falls back silently if no display is available (e.g., headless server).
(sleep 2 && xdg-open "http://localhost:8501" 2>/dev/null) &

# ===========================
# 7. start python producer
# ===========================
echo "[7/7] Starting Python producer..."
echo "       Args: --year $YEAR --race \"$RACE\" --session $SESSION --speed $SPEED --start-lap $START_LAP"

# activate venv if it exists, otherwise use system python
if [ -f "$PRODUCER_DIR/venv/bin/activate" ]; then
	source "$PRODUCER_DIR/venv/bin/activate"
elif [ -f "$PRODUCER_DIR/.venv/bin/activate" ]; then
	source "$PRODUCER_DIR/.venv/bin/activate"
fi

python "$PRODUCER_DIR/src/main.py" \
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
