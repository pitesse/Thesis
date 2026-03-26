#!/usr/bin/env bash
# simulate_season.sh, bulk data generation for ml training.
# dynamically fetches the full race calendar for the given year using fastf1,
# then runs the two-stage python producer for each race to accumulate
# ground truth jsonl data in the data_lake directory.
#
# pit loss thresholds are embedded per-track by prepare_race.py (upstream enrichment),
# no manual specification needed.
#
# this script is self-contained: it boots kafka/flink, submits the flink job,
# then streams every race in the selected season.
#
# usage
#   ./simulate_season.sh
#   ./simulate_season.sh --speed 200
#   ./simulate_season.sh --year 2024
#   ./simulate_season.sh --races "Italian Grand Prix,British Grand Prix"
#   ./simulate_season.sh --post-race-buffer-seconds 120

set -euo pipefail

YEAR=2023
SPEED=100
SESSION="R"
RACES_FILTER=""
POST_RACE_BUFFER_SECONDS=120
JOBMANAGER_OVERVIEW_URL="http://localhost:8081/overview"
JOBMANAGER_READY_TIMEOUT_SECONDS=30
KAFKA_TOPICS=(f1-telemetry f1-laps f1-track-status f1-alerts)

wait_for_jobmanager() {
	echo "         Waiting for Flink JobManager REST API..."
	for i in $(seq 1 "$JOBMANAGER_READY_TIMEOUT_SECONDS"); do
		if curl -sf "$JOBMANAGER_OVERVIEW_URL" >/dev/null 2>&1; then
			echo "         JobManager ready."
			return
		fi
		if [ "$i" -eq "$JOBMANAGER_READY_TIMEOUT_SECONDS" ]; then
			echo "ERROR: Flink JobManager did not start within ${JOBMANAGER_READY_TIMEOUT_SECONDS}s"
			exit 1
		fi
		sleep 1
	done
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
	local timestamp="$3"
	local target_dir="$PROJECT_DIR/data_lake/$sink_dir"

	if [ ! -d "$target_dir" ]; then
		return
	fi

	local merged_file="$PROJECT_DIR/data_lake/${sink_dir}_${year}_season_${timestamp}.jsonl"
	find "$target_dir" -type f \( -name "*.jsonl" -o -name "*.inprogress*" \) -exec cat {} + >"$merged_file"
	if [ -s "$merged_file" ]; then
		echo " Merged: $merged_file"
	else
		rm -f "$merged_file"
	fi
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	--year)
		YEAR="$2"
		shift 2
		;;
	--speed)
		SPEED="$2"
		shift 2
		;;
	--session)
		SESSION="$2"
		shift 2
		;;
	--races)
		RACES_FILTER="$2"
		shift 2
		;;
	--post-race-buffer-seconds)
		POST_RACE_BUFFER_SECONDS="$2"
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

echo "=========================================="
echo " F1 Season Simulator"
echo " Year:   $YEAR"
echo " Speed:  ${SPEED}x"
echo " Buffer: ${POST_RACE_BUFFER_SECONDS}s"
echo "=========================================="

# 1. tear down existing stack
echo "[1/10] Tearing down existing containers..."
docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true

# 2. build all docker images (flink + python)
echo "[2/10] Building Docker images (Flink + Python)..."
docker compose -f "$COMPOSE_FILE" build

# 3. prepare data_lake directory + start docker stack
echo "[3/10] Starting Docker stack..."
mkdir -p "$PROJECT_DIR/data_lake"
chmod -R 777 "$PROJECT_DIR/data_lake" 2>/dev/null || true
docker compose -f "$COMPOSE_FILE" up -d

# wait for flink jobmanager rest api to be ready
wait_for_jobmanager

# 4. pre-create kafka topics
echo "[4/10] Creating Kafka topics..."
create_kafka_topics

# 5. submit flink job (jar is baked into the image)
echo "[5/10] Submitting Flink job..."
docker exec flink-jobmanager flink run \
	-d /opt/flink/usrlib/f1-stream-processor.jar

# dynamically fetch the full race calendar for the given year from fastf1.
# filters to actual race weekends (excludes pre-season testing).
# -T disables pseudo-tty so stdout capture works correctly.
echo "[6/10] Fetching $YEAR race calendar from fastf1..."
RACES_JSON=$(docker compose -f "$COMPOSE_FILE" run --rm -T producer \
	python -c "
import fastf1, json
schedule = fastf1.get_event_schedule($YEAR, include_testing=False)
races = schedule[schedule['EventFormat'].isin(['conventional', 'sprint_shootout', 'sprint_qualifying', 'sprint'])]['EventName'].tolist()
print(json.dumps(races))
")

if [ -z "$RACES_JSON" ] || [ "$RACES_JSON" = "[]" ]; then
	echo "ERROR: no races found for $YEAR"
	exit 1
fi

# parse json array into bash array using stdin, avoids shell quoting issues
RACES=()
while IFS= read -r race; do
	[ -n "$race" ] && RACES+=("$race")
done < <(
	printf '%s\n' "$RACES_JSON" | docker compose -f "$COMPOSE_FILE" run --rm -T producer \
		python -c "import json, sys; [print(r) for r in json.load(sys.stdin)]"
)

# if --races filter is set, only keep races that match the comma-separated list
if [ -n "$RACES_FILTER" ]; then
	IFS=',' read -ra FILTER_LIST <<<"$RACES_FILTER"
	FILTERED=()
	for RACE in "${RACES[@]}"; do
		for F in "${FILTER_LIST[@]}"; do
			# trim leading/trailing whitespace from filter entry
			F="${F#${F%%[![:space:]]*}}"
			F="${F%${F##*[![:space:]]}}"
			if [ "$RACE" = "$F" ]; then
				FILTERED+=("$RACE")
				break
			fi
		done
	done
	RACES=("${FILTERED[@]}")
fi

TOTAL=${#RACES[@]}

echo "=========================================="
echo " F1 Season Simulator"
echo " Year:   $YEAR"
echo " Speed:  ${SPEED}x"
echo " Races:  $TOTAL"
echo "=========================================="

echo "[7/10] Running full-season replay..."

FAILED=0
for i in "${!RACES[@]}"; do
	RACE="${RACES[$i]}"
	NUM=$((i + 1))
	echo ""
	echo "[$NUM/$TOTAL] $RACE"
	echo "------------------------------------------"

	echo "       Stage 1/2, prepare parquet"
	docker compose -f "$COMPOSE_FILE" run --rm producer \
		python f1-telemetry-producer/src/prepare_race.py \
		--year "$YEAR" \
		--race "$RACE" \
		--session "$SESSION" \
		--post-race-buffer-seconds "$POST_RACE_BUFFER_SECONDS" || {
		echo "WARNING: prepare failed for $RACE, skipping..."
		FAILED=$((FAILED + 1))
		continue
	}

	echo "       Stage 2/2, stream to kafka"
	docker compose -f "$COMPOSE_FILE" run --rm producer \
		python f1-telemetry-producer/src/stream_race.py \
		--year "$YEAR" \
		--race "$RACE" \
		--session "$SESSION" \
		--speed "$SPEED" || {
		echo "WARNING: stream failed for $RACE, skipping..."
		FAILED=$((FAILED + 1))
		continue
	}

	# brief pause between races so flink can drain pending events and
	# advance watermarks past any state timers from the completed race.
	# race timestamps are weeks apart, so watermarks jump forward cleanly.
	if [ "$NUM" -lt "$TOTAL" ]; then
		echo "       Draining (15s)..."
		sleep 15
	fi
done

# flink's FileSink produces files like "pit-eval-0-0.jsonl" plus transient
# .inprogress parts while the stream is active.
# under date-partitioned directories (e.g., 2026-03-06--14/). consolidate
# all part-files into a single jsonl per sink type with a clear name.
echo "[8/10] Consolidating sink outputs..."
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

for SINK_DIR in pit_evals pit_suggestions tire_drops lift_coast drop_zones ml_features; do
	consolidate_sink_outputs "$SINK_DIR" "$YEAR" "$TIMESTAMP"
done

echo ""
echo "[9/10] Final summary"
echo "=========================================="
echo " Season simulation complete."
echo " Processed: $((TOTAL - FAILED))/$TOTAL races"
if [ "$FAILED" -gt 0 ]; then
	echo " Failed:    $FAILED"
fi
echo ""
echo " Dashboard:     http://localhost:8501"
echo " Flink UI:      http://localhost:8081"
echo " Raw output:    data_lake/{pit_evals,pit_suggestions,tire_drops,lift_coast,drop_zones,ml_features}/"
echo " Merged JSONLs: data_lake/pit_evals_${YEAR}_season_${TIMESTAMP}.jsonl"
echo "                 data_lake/pit_suggestions_${YEAR}_season_${TIMESTAMP}.jsonl"
echo "                 data_lake/tire_drops_${YEAR}_season_${TIMESTAMP}.jsonl"
echo ""
echo " Wait ~3 min for Flink's rolling policy to finalize the"
echo " last JSONL file, then run the ML pipeline:"
echo "   docker compose run --rm producer python ml_pipeline/train_pit_strategy.py"
echo "[10/10] Done"
echo "=========================================="
