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
WITH_ML_INFERENCE=0
PREPARE_MODE="container"
SKIP_FASTF1_PREFLIGHT=0
JOBMANAGER_OVERVIEW_URL="http://localhost:8081/overview"
JOBMANAGER_JOBS_URL="http://localhost:8081/jobs"
JOBMANAGER_READY_TIMEOUT_SECONDS=30
JOB_READY_TIMEOUT_SECONDS=60
KAFKA_TOPICS=(f1-telemetry f1-laps f1-track-status f1-alerts f1-ml-features f1-ml-predictions)
SINK_DIRS=(pit_evals pit_suggestions tire_drops lift_coast drop_zones ml_features)

wait_for_jobmanager() {
	echo "       Waiting for Flink JobManager REST API..."
	for i in $(seq 1 "$JOBMANAGER_READY_TIMEOUT_SECONDS"); do
		if curl -sf "$JOBMANAGER_OVERVIEW_URL" >/dev/null 2>&1; then
			echo "       JobManager ready."
			return
		fi
		if [ "$i" -eq "$JOBMANAGER_READY_TIMEOUT_SECONDS" ]; then
			echo "ERROR: Flink JobManager did not start within ${JOBMANAGER_READY_TIMEOUT_SECONDS}s"
			exit 1
		fi
		sleep 1
	done
}

wait_for_flink_job_running() {
	echo "       Waiting for Flink job to reach RUNNING state..."
	for i in $(seq 1 "$JOB_READY_TIMEOUT_SECONDS"); do
		JOBS_JSON=$(curl -sf "$JOBMANAGER_JOBS_URL" 2>/dev/null || true)
		if [ -z "$JOBS_JSON" ]; then
			sleep 1
			continue
		fi
		RUNNING_COUNT=$(printf '%s' "$JOBS_JSON" | python3 -c "
import json,sys
try:
    data=json.load(sys.stdin)
except Exception:
    print(0); raise SystemExit
jobs=data.get('jobs',[])
running=sum(1 for j in jobs if j.get('status') == 'RUNNING')
print(running)
")
		if [ "${RUNNING_COUNT:-0}" -ge 1 ]; then
			echo "       Flink job is RUNNING."
			return
		fi
		sleep 1
	done

	echo "ERROR: Flink job did not reach RUNNING within ${JOB_READY_TIMEOUT_SECONDS}s"
	echo "       Check logs: docker logs -f flink-jobmanager"
	exit 1
}

create_kafka_topics() {
	for topic in "${KAFKA_TOPICS[@]}"; do
		docker exec kafka kafka-topics \
			--bootstrap-server localhost:29092 \
			--create --topic "$topic" \
			--partitions 1 --replication-factor 1 \
			--if-not-exists 2>/dev/null || true
	done
}

consolidate_sink_outputs() {
	local sink_dir="$1"
	local year="$2"
	local race_slug="$3"
	local session="$4"
	local timestamp="$5"
	local target_dir="$PROJECT_DIR/data_lake/$sink_dir"

	if [ ! -d "$target_dir" ]; then
		return
	fi

	local merged_file="$PROJECT_DIR/data_lake/${sink_dir}_${year}_${race_slug}_${session}_${timestamp}.jsonl"
	local files=()
	while IFS= read -r -d '' file; do
		files+=("$file")
	done < <(
		find "$target_dir" -type f \( -name "*.jsonl" -o -name "*.inprogress*" \) -print0 | sort -z
	)

	if [ "${#files[@]}" -eq 0 ]; then
		return 1
	fi

	: >"$merged_file"
	for file in "${files[@]}"; do
		cat "$file" >>"$merged_file"
	done

	if [ -s "$merged_file" ]; then
		echo "       Merged: $merged_file" >&2
		echo "$merged_file"
	else
		rm -f "$merged_file"
		return 1
	fi
}

clean_sink_directories() {
	local base_dir="$PROJECT_DIR/data_lake"
	for sink_dir in "${SINK_DIRS[@]}"; do
		local sink_path="$base_dir/$sink_dir"
		mkdir -p "$sink_path"
		find "$sink_path" -mindepth 1 -delete
	done
}

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
	--with-ml-inference)
		WITH_ML_INFERENCE=1
		shift 1
		;;
	--prepare-host)
		PREPARE_MODE="host"
		shift 1
		;;
	--prepare-container-then-host)
		PREPARE_MODE="container_then_host"
		shift 1
		;;
	--skip-fastf1-preflight)
		SKIP_FASTF1_PREFLIGHT=1
		shift 1
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
	echo "Stopping optional UI/inference containers..."
	docker compose -f "$COMPOSE_FILE" stop dashboard ml-inference 2>/dev/null || true
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
if [ "$WITH_ML_INFERENCE" -eq 1 ]; then
	echo " ML Inference: enabled"
else
	echo " ML Inference: disabled"
fi
echo " Prepare mode: $PREPARE_MODE"
if [ "$SKIP_FASTF1_PREFLIGHT" -eq 1 ]; then
	echo " FastF1 preflight: skipped"
else
	echo " FastF1 preflight: enabled"
fi
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
if [ "$WITH_ML_INFERENCE" -eq 1 ]; then
	MODEL_BUNDLE="$PROJECT_DIR/data_lake/models/pit_strategy_serving_bundle.joblib"
	if [ ! -f "$MODEL_BUNDLE" ]; then
		echo "WARNING: ML inference requested but model bundle not found at $MODEL_BUNDLE"
		echo "         Build it first with: python ml_pipeline/train_model.py"
	fi
	docker compose -f "$COMPOSE_FILE" --profile inference up -d
else
	docker compose -f "$COMPOSE_FILE" up -d
fi
echo "       Resetting sink directories for strict run isolation..."
clean_sink_directories

# wait for flink jobmanager rest api to be ready
wait_for_jobmanager

# ===========================
# 4. pre-create kafka topics
# ===========================
echo "[4/7] Creating Kafka topics..."
create_kafka_topics

# ===========================
# 5. submit flink job (jar is baked into the image)
# ===========================
echo "[5/7] Submitting Flink job..."
docker exec flink-jobmanager flink run \
	-d /opt/flink/usrlib/f1-stream-processor.jar
wait_for_flink_job_running

# ===========================
# 6. dashboard is already running via docker compose up
# ===========================
echo "[6/7] Dashboard is running at http://localhost:8501"
if [ "$WITH_ML_INFERENCE" -eq 1 ]; then
	echo "       ML inference consumer is running in parallel (topic: f1-ml-predictions)"
fi

# open the dashboard in the default browser after a short delay so streamlit
# has time to bind the port. uses python's webbrowser module which works on
# linux, macos, and windows/wsl without platform-specific commands.
(sleep 3 && python3 -m webbrowser "http://localhost:8501" 2>/dev/null) &

# ===========================
# 7. prepare data + start python producer
# ===========================
echo "[7/7] Starting Python producer (two-stage pipeline)..."
echo "       Args: --year $YEAR --race \"$RACE\" --session $SESSION --speed $SPEED --start-lap $START_LAP"

if [ "$SKIP_FASTF1_PREFLIGHT" -ne 1 ]; then
	echo "       Running FastF1 upstream preflight..."
	if ! "$PROJECT_DIR/scripts/check_f1_api_health.sh"; then
		echo "ERROR: FastF1 upstream unavailable for required endpoints."
		echo "       Aborting before replay to avoid partial/invalid artifacts."
		echo "       Retry later or rerun with --skip-fastf1-preflight if you explicitly want to force."
		exit 1
	fi
fi

# stage 1: prepare parquet (skip if already exists for this race/year/session)
SAFE_RACE=$(echo "$RACE" | tr ' ' '_')
PARQUET_FILE="$PROJECT_DIR/data/${YEAR}_${SAFE_RACE}_${SESSION}_prepared.parquet"

run_prepare_stage() {
	local year="$1"
	local race="$2"
	local session="$3"

	if [ "$PREPARE_MODE" = "host" ]; then
		.venv/bin/python f1-telemetry-producer/src/prepare_race.py \
			--year "$year" \
			--race "$race" \
			--session "$session"
		return $?
	fi

	if docker compose -f "$COMPOSE_FILE" run --rm producer \
		python f1-telemetry-producer/src/prepare_race.py \
		--year "$year" \
		--race "$race" \
		--session "$session"; then
		return 0
	fi

	if [ "$PREPARE_MODE" = "container_then_host" ]; then
		echo "       Container prepare failed, retrying prepare on host .venv..."
		.venv/bin/python f1-telemetry-producer/src/prepare_race.py \
			--year "$year" \
			--race "$race" \
			--session "$session"
		return $?
	fi

	return 1
}

if [ -f "$PARQUET_FILE" ]; then
	echo "       Parquet already exists: $PARQUET_FILE (skipping prepare)"
else
	echo "       Running prepare_race.py..."
	run_prepare_stage "$YEAR" "$RACE" "$SESSION"
fi

# stage 2: stream to kafka
docker compose -f "$COMPOSE_FILE" run --rm producer \
	python f1-telemetry-producer/src/stream_race.py \
	--year "$YEAR" \
	--race "$RACE" \
	--session "$SESSION" \
	--speed "$SPEED" \
	--start-lap "$START_LAP"

# ===========================
# 8. consolidate sink outputs for this race
# ===========================
# FileSink may leave the latest valid records in .inprogress parts when the
# stream becomes idle after the producer exits. consolidate both committed
# jsonl and hidden .inprogress parts into a single race-level jsonl per sink.
echo "[8/8] Consolidating race outputs (jsonl + inprogress)..."
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
ML_FEATURES_MERGED=""

for SINK_DIR in pit_evals tire_drops lift_coast drop_zones ml_features pit_suggestions; do
	if MERGED_PATH=$(consolidate_sink_outputs "$SINK_DIR" "$YEAR" "$SAFE_RACE" "$SESSION" "$TIMESTAMP"); then
		if [ "$SINK_DIR" = "ml_features" ]; then
			ML_FEATURES_MERGED="$MERGED_PATH"
		fi
	else
		echo "       WARNING: no files found to consolidate for sink '$SINK_DIR'"
	fi
done

if [ -n "$ML_FEATURES_MERGED" ] && [ -f "$ML_FEATURES_MERGED" ]; then
	MANIFEST_DIR="$PROJECT_DIR/data_lake/replay_manifests"
	mkdir -p "$MANIFEST_DIR"
	RUN_ID="${YEAR}_${SAFE_RACE}_${SESSION}_${TIMESTAMP}"
	MANIFEST_PATH="$MANIFEST_DIR/replay_manifest_${YEAR}_single_race_${TIMESTAMP}.json"
	echo "       Building replay manifest: $MANIFEST_PATH"
	python "$PROJECT_DIR/ml_pipeline/build_replay_manifest.py" \
		--year "$YEAR" \
		--season-tag single_race \
		--run-id "$RUN_ID" \
		--ml-features "$ML_FEATURES_MERGED" \
		--output "$MANIFEST_PATH" \
		--races "$RACE"
fi

echo ""
echo "========================================"
echo " Simulation complete."
echo " Dashboard:  http://localhost:8501"
echo " Flink UI:   http://localhost:8081"
echo " Flink logs: docker logs -f flink-taskmanager"
if [ "$WITH_ML_INFERENCE" -eq 1 ]; then
	echo " ML topic:   f1-ml-predictions"
fi
echo "========================================"
