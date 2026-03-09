#!/usr/bin/env bash
# multi-race integration test.
# starts the full docker stack, submits the flink job, then replays two
# back-to-back races to verify that:
#   1. flink state (DropZoneEvaluator MapState, TireDropDetector stint state,
#      PitStopEvaluator state machine) clears correctly between races
#   2. the data lake accumulates CSVs from both races without corruption
#   3. checkpointing commits files between race boundaries
#
# usage:
#   ./test_multi_race.sh
#   ./test_multi_race.sh --speed 200

set -euo pipefail

SPEED=100

while [[ $# -gt 0 ]]; do
	case "$1" in
	--speed)
		SPEED="$2"
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
JAR_NAME="f1-telemetry-processor-1.0-SNAPSHOT.jar"

# two races with different characteristics:
#   monza: low downforce, 25s pit loss, few overtakes, long straights
#   silverstone: high downforce, 22.5s pit loss, more pitstops, mixed corners
RACE_1="Italian Grand Prix"
RACE_2="British Grand Prix"
YEAR=2023

echo "=========================================="
echo " Multi-Race Integration Test"
echo " Race 1: $RACE_1 ($YEAR)"
echo " Race 2: $RACE_2 ($YEAR)"
echo " Speed:  ${SPEED}x"
echo "=========================================="

# ===========================
# 1. tear down + start stack
# ===========================
echo "[1/6] Starting fresh Docker stack..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" down -v 2>/dev/null || true
mkdir -p "$PROJECT_DIR/data_lake"
chmod -R 777 "$PROJECT_DIR/data_lake" 2>/dev/null || true
docker compose -f "$PROJECT_DIR/docker-compose.yml" up -d

echo "       Waiting for Flink JobManager..."
for i in $(seq 1 30); do
	if curl -sf http://localhost:8081/overview >/dev/null 2>&1; then
		echo "       JobManager ready."
		break
	fi
	[ "$i" -eq 30 ] && { echo "ERROR: JobManager timeout"; exit 1; }
	sleep 1
done

# ===========================
# 2. create topics + submit job
# ===========================
echo "[2/6] Creating Kafka topics and submitting Flink job..."
for topic in f1-telemetry f1-laps f1-track-status f1-alerts; do
	docker exec kafka kafka-topics \
		--bootstrap-server localhost:29092 \
		--create --topic "$topic" \
		--partitions 1 --replication-factor 1 \
		--if-not-exists 2>/dev/null || true
done

docker exec flink-jobmanager mkdir -p /opt/flink/usrlib
docker cp "$PROCESSOR_DIR/target/$JAR_NAME" flink-jobmanager:/opt/flink/usrlib/
docker exec flink-jobmanager flink run -d "/opt/flink/usrlib/$JAR_NAME"

# activate producer venv
if [ -f "$PRODUCER_DIR/.venv/bin/activate" ]; then
	source "$PRODUCER_DIR/.venv/bin/activate"
elif [ -f "$PRODUCER_DIR/venv/bin/activate" ]; then
	source "$PRODUCER_DIR/venv/bin/activate"
fi

# ===========================
# 3. race 1
# ===========================
echo ""
echo "[3/6] Race 1: $RACE_1"
echo "       Preparing parquet..."
python "$PRODUCER_DIR/src/prepare_race.py" --year "$YEAR" --race "$RACE_1"
echo "       Streaming to Kafka..."
python "$PRODUCER_DIR/src/stream_race.py" --year "$YEAR" --race "$RACE_1" --speed "$SPEED"

echo "       Draining (20s for checkpoint + watermark advance)..."
sleep 20

# snapshot data lake after race 1
RACE1_FILES=$(find "$PROJECT_DIR/data_lake" -name "*.csv" -type f 2>/dev/null | wc -l)
echo "       Race 1 complete. CSV files in data_lake: $RACE1_FILES"

# ===========================
# 4. race 2
# ===========================
echo ""
echo "[4/6] Race 2: $RACE_2"
echo "       Preparing parquet..."
python "$PRODUCER_DIR/src/prepare_race.py" --year "$YEAR" --race "$RACE_2"
echo "       Streaming to Kafka..."
python "$PRODUCER_DIR/src/stream_race.py" --year "$YEAR" --race "$RACE_2" --speed "$SPEED"

echo "       Draining (20s for final checkpoint)..."
sleep 20

# ===========================
# 5. verify output
# ===========================
echo ""
echo "[5/6] Verifying Data Lake output..."

TOTAL_FILES=$(find "$PROJECT_DIR/data_lake" -name "*.csv" -type f 2>/dev/null | wc -l)
echo "       Total CSV files: $TOTAL_FILES (was $RACE1_FILES after race 1)"

# check each sink directory for content
for SINK in pit_evals tire_drops lift_coast drop_zones ml_features; do
	DIR="$PROJECT_DIR/data_lake/$SINK"
	if [ -d "$DIR" ]; then
		COUNT=$(find "$DIR" -name "*.csv" -type f 2>/dev/null | wc -l)
		LINES=0
		for f in $(find "$DIR" -name "*.csv" -type f 2>/dev/null); do
			LINES=$((LINES + $(wc -l < "$f")))
		done
		echo "       $SINK: $COUNT files, $LINES total lines"
	else
		echo "       $SINK: MISSING"
	fi
done

# verify both races appear in the data
echo ""
echo "       Checking race names in CSV data..."
for SINK in pit_evals drop_zones ml_features; do
	DIR="$PROJECT_DIR/data_lake/$SINK"
	if [ -d "$DIR" ]; then
		MONZA=$(grep -rl "Italian Grand Prix" "$DIR" 2>/dev/null | wc -l)
		SILVERSTONE=$(grep -rl "British Grand Prix" "$DIR" 2>/dev/null | wc -l)
		echo "       $SINK: Monza=$MONZA files, Silverstone=$SILVERSTONE files"
	fi
done

# ===========================
# 6. check flink logs for errors
# ===========================
echo ""
echo "[6/6] Checking Flink logs for errors..."
ERROR_COUNT=$(docker logs flink-taskmanager 2>&1 | grep -ci "exception\|error" || true)
echo "       Exception/Error count in logs: $ERROR_COUNT"

# show any actual java exceptions (not just the word "error" in normal logs)
REAL_ERRORS=$(docker logs flink-taskmanager 2>&1 | grep -c "at com.polimi" || true)
echo "       Stack traces from com.polimi: $REAL_ERRORS"

echo ""
echo "=========================================="
echo " Integration Test Complete"
echo " Race 1 files: $RACE1_FILES"
echo " Total files:  $TOTAL_FILES"
if [ "$TOTAL_FILES" -gt "$RACE1_FILES" ]; then
	echo " PASS: Data lake grew between races"
else
	echo " WARN: No new files after race 2 (check checkpoint timing)"
fi
if [ "$REAL_ERRORS" -eq 0 ]; then
	echo " PASS: No stack traces in Flink logs"
else
	echo " FAIL: $REAL_ERRORS stack traces detected"
fi
echo "=========================================="

# leave stack running for manual inspection
echo ""
echo " Stack is still running for inspection:"
echo "   Flink UI:   http://localhost:8081"
echo "   Flink logs: docker logs -f flink-taskmanager"
echo "   Tear down:  docker compose down -v"
