#!/usr/bin/env bash
# simulate_season.sh, bulk data generation for ml training.
# fetches the race calendar for the given year (from fastf1 or local fallback),
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
#   ./simulate_season.sh --year 2024 --weather-mode required

set -euo pipefail

YEAR_ARG=""
SPEED=100
SESSION="R"
RACES_FILTER=""
POST_RACE_BUFFER_SECONDS=120
FINAL_DRAIN_SECONDS=20
WITH_ML_INFERENCE=0
ALLOW_PARTIAL_YEAR=0
PREPARE_MODE="container"
REUSE_PREPARED_PARQUET=0
REQUIRE_PREPARED_PARQUET=0
SKIP_FASTF1_PREFLIGHT=0
CALENDAR_SOURCE="fallback"
WEATHER_MODE="optional"
JOBMANAGER_OVERVIEW_URL="http://localhost:8081/overview"
JOBMANAGER_JOBS_URL="http://localhost:8081/jobs"
JOBMANAGER_READY_TIMEOUT_SECONDS=30
JOB_READY_TIMEOUT_SECONDS=60
KAFKA_TOPICS=(f1-telemetry f1-laps f1-track-status f1-alerts f1-ml-features f1-ml-predictions)
SINK_DIRS=(pit_evals pit_suggestions pit_timings tire_drops lift_coast drop_zones ml_features)

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

wait_for_flink_job_running() {
	echo "         Waiting for Flink job to reach RUNNING state..."
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
			echo "         Flink job is RUNNING."
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
	local timestamp="$3"
	local target_dir="$PROJECT_DIR/data_lake/$sink_dir"

	if [ ! -d "$target_dir" ]; then
		return
	fi

	local merged_file="$PROJECT_DIR/data_lake/${sink_dir}_${year}_season_${timestamp}.jsonl"
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
		echo " Merged: $merged_file" >&2
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
		# Flink taskmanager runs as uid/gid 9999 ("flink") and needs write/execute
		# permissions to create time-bucket subdirectories (YYYY-MM-DD--HH).
		# Keep this world-writable to avoid uid/gid mismatch between host and container.
		chmod 0777 "$sink_path" 2>/dev/null || true
	done
}

prepared_parquet_has_weather() {
	local parquet_path="$1"
	.venv/bin/python - "$parquet_path" <<'PY'
import sys
from pathlib import Path
import pandas as pd

path = Path(sys.argv[1])
if not path.exists():
    raise SystemExit(1)

try:
    frame = pd.read_parquet(path, columns=["AirTemp", "TrackTemp", "Humidity"])
except Exception:
    raise SystemExit(1)

# Require core weather signals to contain at least one non-null sample each.
ok = frame[["AirTemp", "TrackTemp", "Humidity"]].notna().any().all()
raise SystemExit(0 if bool(ok) else 1)
PY
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	--year)
		YEAR_ARG="$2"
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
	--final-drain-seconds)
		FINAL_DRAIN_SECONDS="$2"
		shift 2
		;;
	--with-ml-inference)
		WITH_ML_INFERENCE=1
		shift 1
		;;
	--allow-partial-year)
		ALLOW_PARTIAL_YEAR=1
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
	--reuse-prepared-parquet)
		REUSE_PREPARED_PARQUET=1
		shift 1
		;;
	--require-prepared-parquet)
		REUSE_PREPARED_PARQUET=1
		REQUIRE_PREPARED_PARQUET=1
		shift 1
		;;
	--skip-fastf1-preflight)
		SKIP_FASTF1_PREFLIGHT=1
		shift 1
		;;
	--calendar-source)
		CALENDAR_SOURCE="$2"
		if [ "$CALENDAR_SOURCE" != "fallback" ] && [ "$CALENDAR_SOURCE" != "fastf1" ] && [ "$CALENDAR_SOURCE" != "auto" ]; then
			echo "ERROR: --calendar-source must be one of: fallback, fastf1, auto"
			exit 1
		fi
		shift 2
		;;
	--weather-mode)
		WEATHER_MODE="$2"
		if [ "$WEATHER_MODE" != "off" ] && [ "$WEATHER_MODE" != "optional" ] && [ "$WEATHER_MODE" != "required" ]; then
			echo "ERROR: --weather-mode must be one of: off, optional, required"
			exit 1
		fi
		shift 2
		;;
	*)
		echo "Unknown argument: $1"
		exit 1
		;;
	esac
done

YEAR_EXPLICIT=0
if [ -n "$YEAR_ARG" ]; then
	YEAR_EXPLICIT=1
	YEARS=("$YEAR_ARG")
else
	YEARS=(2022 2023 2024 2025)
fi

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.yml"

echo "=========================================="
echo " F1 Season Simulator"
if [ "$YEAR_EXPLICIT" -eq 1 ]; then
	echo " Year:   ${YEARS[0]}"
else
	echo " Years:  ${YEARS[*]}"
fi
echo " Speed:  ${SPEED}x"
echo " Buffer: ${POST_RACE_BUFFER_SECONDS}s"
echo " Final drain: ${FINAL_DRAIN_SECONDS}s"
if [ "$WITH_ML_INFERENCE" -eq 1 ]; then
	echo " ML Inference: enabled"
else
	echo " ML Inference: disabled"
fi
if [ "$ALLOW_PARTIAL_YEAR" -eq 1 ]; then
	echo " Partial-year mode: enabled (continues on per-race failures)"
else
	echo " Partial-year mode: disabled (fail-fast on first race failure)"
fi
echo " Prepare mode: $PREPARE_MODE"
echo " Calendar source: $CALENDAR_SOURCE"
echo " Weather mode: $WEATHER_MODE"
if [ "$REUSE_PREPARED_PARQUET" -eq 1 ]; then
	echo " Prepared parquet reuse: enabled"
else
	echo " Prepared parquet reuse: disabled"
fi
if [ "$REQUIRE_PREPARED_PARQUET" -eq 1 ]; then
	echo " Prepared parquet requirement: strict (fail if missing)"
fi
if [ "$SKIP_FASTF1_PREFLIGHT" -eq 1 ]; then
	echo " FastF1 preflight: skipped"
else
	echo " FastF1 preflight: enabled"
fi
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

# wait for flink jobmanager rest api to be ready
wait_for_jobmanager

# 4. pre-create kafka topics
echo "[4/10] Creating Kafka topics..."
create_kafka_topics

# 5. submit flink job (jar is baked into the image)
echo "[5/10] Submitting Flink job..."
docker exec flink-jobmanager flink run \
	-d /opt/flink/usrlib/f1-stream-processor.jar
wait_for_flink_job_running

# dynamically fetch the full race calendar for each selected year from fastf1.
# filters to actual race weekends (excludes pre-season testing).
# -T disables pseudo-tty so stdout capture works correctly.

TOTAL_RACES=0
TOTAL_FAILED=0

run_prepare_stage() {
	local year="$1"
	local race="$2"
	local session="$3"
	local buffer="$4"
	local weather_mode="$5"
	local safe_race="${race// /_}"
	local parquet_path="$PROJECT_DIR/data/${year}_${safe_race}_${session}_prepared.parquet"

	if [ "$REUSE_PREPARED_PARQUET" -eq 1 ] && [ -f "$parquet_path" ]; then
		if [ "$weather_mode" = "required" ]; then
			if ! prepared_parquet_has_weather "$parquet_path"; then
				echo "       Reuse rejected (missing required weather): $parquet_path"
				return 1
			fi
		fi
		echo "       Reusing prepared parquet: $parquet_path"
		return 0
	fi

	if [ "$REQUIRE_PREPARED_PARQUET" -eq 1 ] && [ ! -f "$parquet_path" ]; then
		echo "       Missing required prepared parquet: $parquet_path"
		return 1
	fi
	if [ "$REQUIRE_PREPARED_PARQUET" -eq 1 ] && [ "$weather_mode" = "required" ] && [ -f "$parquet_path" ]; then
		if ! prepared_parquet_has_weather "$parquet_path"; then
			echo "       Required prepared parquet missing weather: $parquet_path"
			return 1
		fi
	fi

	if [ "$PREPARE_MODE" = "host" ]; then
		.venv/bin/python f1-telemetry-producer/src/prepare_race.py \
			--year "$year" \
			--race "$race" \
			--session "$session" \
			--post-race-buffer-seconds "$buffer" \
			--weather-mode "$weather_mode"
		return $?
	fi

	if docker compose -f "$COMPOSE_FILE" run --rm producer \
		python f1-telemetry-producer/src/prepare_race.py \
		--year "$year" \
		--race "$race" \
		--session "$session" \
		--post-race-buffer-seconds "$buffer" \
		--weather-mode "$weather_mode"; then
		return 0
	fi

	if [ "$PREPARE_MODE" = "container_then_host" ]; then
		echo "       Container prepare failed, retrying prepare on host .venv..."
		.venv/bin/python f1-telemetry-producer/src/prepare_race.py \
			--year "$year" \
			--race "$race" \
			--session "$session" \
			--post-race-buffer-seconds "$buffer" \
			--weather-mode "$weather_mode"
		return $?
	fi

	return 1
}

fetch_races_fastf1() {
	local year="$1"
	docker compose -f "$COMPOSE_FILE" run --rm -T producer \
		python -c "
import fastf1, json
schedule = fastf1.get_event_schedule($year, include_testing=False)
races = schedule[schedule['EventFormat'].isin(['conventional', 'sprint_shootout', 'sprint_qualifying', 'sprint'])]['EventName'].tolist()
print(json.dumps(races))
"
}

fetch_races_fallback() {
	local year="$1"
	local calendar_file="$PROJECT_DIR/scripts/f1_race_calendar_2022_2025.json"
	if [ ! -f "$calendar_file" ]; then
		echo "ERROR: fallback calendar file not found: $calendar_file" >&2
		return 1
	fi

	python3 - "$calendar_file" "$year" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
year = sys.argv[2]
payload = json.loads(path.read_text(encoding="utf-8"))
races = payload.get(year)
if not isinstance(races, list) or not races:
    raise SystemExit(1)
print(json.dumps(races, ensure_ascii=False))
PY
}

fetch_races_for_year() {
	local year="$1"
	case "$CALENDAR_SOURCE" in
	fastf1)
		fetch_races_fastf1 "$year"
		;;
	fallback)
		fetch_races_fallback "$year"
		;;
	auto)
		if races_json="$(fetch_races_fastf1 "$year" 2>/dev/null)" && [ -n "$races_json" ] && [ "$races_json" != "[]" ]; then
			echo "$races_json"
			return 0
		fi
		echo "WARNING: FastF1 calendar fetch failed for $year; using fallback calendar" >&2
		fetch_races_fallback "$year"
		;;
	*)
		echo "ERROR: unknown calendar source '$CALENDAR_SOURCE'" >&2
		return 1
		;;
	esac
}

for YEAR in "${YEARS[@]}"; do
	NEEDS_FASTF1_PREFLIGHT=1
	if [ "$SKIP_FASTF1_PREFLIGHT" -eq 1 ]; then
		NEEDS_FASTF1_PREFLIGHT=0
	elif [ "$CALENDAR_SOURCE" = "fallback" ] && [ "$REQUIRE_PREPARED_PARQUET" -eq 1 ]; then
		NEEDS_FASTF1_PREFLIGHT=0
	fi

	if [ "$NEEDS_FASTF1_PREFLIGHT" -eq 1 ]; then
		echo "[6/10] FastF1 upstream preflight for $YEAR..."
		if ! "$PROJECT_DIR/scripts/check_f1_api_health.sh"; then
			echo "ERROR: FastF1 upstream unavailable for required endpoints."
			echo "       Aborting before replay to avoid partial/invalid artifacts."
			echo "       Retry later or rerun with --skip-fastf1-preflight if you explicitly want to force."
			exit 1
		fi
	fi

	echo "[6/10] Fetching $YEAR race calendar from $CALENDAR_SOURCE source..."
	RACES_JSON="$(fetch_races_for_year "$YEAR" || true)"

	if [ -z "$RACES_JSON" ] || [ "$RACES_JSON" = "[]" ]; then
		if [ "$YEAR_EXPLICIT" -eq 1 ]; then
			echo "ERROR: no races found for $YEAR"
			exit 1
		fi
		echo "WARNING: no races found for $YEAR, skipping year"
		continue
	fi

	RACES=()
	while IFS= read -r race; do
		[ -n "$race" ] && RACES+=("$race")
	done < <(
		printf '%s\n' "$RACES_JSON" | python3 -c "import json, sys; [print(r) for r in json.load(sys.stdin)]"
	)

	if [ "${#RACES[@]}" -eq 0 ]; then
		if [ "$YEAR_EXPLICIT" -eq 1 ]; then
			echo "ERROR: failed to parse race list for $YEAR from $CALENDAR_SOURCE source"
			exit 1
		fi
		echo "WARNING: failed to parse race list for $YEAR, skipping year"
		continue
	fi

	if [ -n "$RACES_FILTER" ]; then
		IFS=',' read -ra FILTER_LIST <<<"$RACES_FILTER"
		FILTERED=()
		for RACE in "${RACES[@]}"; do
			for F in "${FILTER_LIST[@]}"; do
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
	if [ "$TOTAL" -eq 0 ]; then
		echo "WARNING: no races selected for $YEAR after filtering, skipping year"
		continue
	fi

	echo "=========================================="
	echo " F1 Season Simulator"
	echo " Year:   $YEAR"
	echo " Speed:  ${SPEED}x"
	echo " Races:  $TOTAL"
	echo "=========================================="

	echo "[7/10] Running full-season replay for $YEAR..."
	echo "       Resetting sink directories for strict year isolation..."
	clean_sink_directories

	FAILED=0
	for i in "${!RACES[@]}"; do
		RACE="${RACES[$i]}"
		NUM=$((i + 1))
		echo ""
		echo "[$NUM/$TOTAL][$YEAR] $RACE"
		echo "------------------------------------------"

		echo "       Stage 1/2, prepare parquet"
		run_prepare_stage "$YEAR" "$RACE" "$SESSION" "$POST_RACE_BUFFER_SECONDS" "$WEATHER_MODE" || {
			if [ "$ALLOW_PARTIAL_YEAR" -eq 1 ]; then
				echo "WARNING: prepare failed for $RACE, skipping..."
				FAILED=$((FAILED + 1))
				continue
			fi
			echo "ERROR: prepare failed for $RACE (fail-fast mode)."
			exit 1
		}

		echo "       Stage 2/2, stream to kafka"
		docker compose -f "$COMPOSE_FILE" run --rm producer \
			python f1-telemetry-producer/src/stream_race.py \
			--year "$YEAR" \
			--race "$RACE" \
			--session "$SESSION" \
			--speed "$SPEED" || {
			if [ "$ALLOW_PARTIAL_YEAR" -eq 1 ]; then
				echo "WARNING: stream failed for $RACE, skipping..."
				FAILED=$((FAILED + 1))
				continue
			fi
			echo "ERROR: stream failed for $RACE (fail-fast mode)."
			exit 1
		}

		if [ "$NUM" -lt "$TOTAL" ]; then
			echo "       Draining (15s)..."
			sleep 15
		fi
	done

	if [ "$FINAL_DRAIN_SECONDS" -gt 0 ]; then
		echo "       Final drain (${FINAL_DRAIN_SECONDS}s) before consolidation..."
		sleep "$FINAL_DRAIN_SECONDS"
	fi

	echo "[8/10] Consolidating sink outputs for $YEAR..."
	TIMESTAMP=$(date +%Y%m%d_%H%M%S)
	ML_FEATURES_MERGED=""
	for SINK_DIR in pit_evals pit_suggestions pit_timings tire_drops lift_coast drop_zones ml_features; do
		if MERGED_PATH=$(consolidate_sink_outputs "$SINK_DIR" "$YEAR" "$TIMESTAMP"); then
			if [ "$SINK_DIR" = "ml_features" ]; then
				ML_FEATURES_MERGED="$MERGED_PATH"
			fi
		else
			echo " WARNING: no files found to consolidate for sink '$SINK_DIR' in year $YEAR"
			echo "          debug listing (up to 8 entries) under data_lake/$SINK_DIR:"
			find "$PROJECT_DIR/data_lake/$SINK_DIR" -maxdepth 4 -mindepth 1 2>/dev/null | head -n 8 || true
		fi
	done

	if [ -z "$ML_FEATURES_MERGED" ] || [ ! -f "$ML_FEATURES_MERGED" ]; then
		echo "ERROR: failed to consolidate ml_features for year $YEAR"
		exit 1
	fi

	MANIFEST_DIR="$PROJECT_DIR/data_lake/replay_manifests"
	mkdir -p "$MANIFEST_DIR"
	YEAR_RUN_ID="${YEAR}_${TIMESTAMP}"
	MANIFEST_PATH="$MANIFEST_DIR/replay_manifest_${YEAR}_season_${TIMESTAMP}.json"
	echo "       Building replay manifest: $MANIFEST_PATH"
	python "$PROJECT_DIR/ml_pipeline/build_replay_manifest.py" \
		--year "$YEAR" \
		--season-tag season \
		--run-id "$YEAR_RUN_ID" \
		--ml-features "$ML_FEATURES_MERGED" \
		--output "$MANIFEST_PATH" \
		--races "${RACES[@]}"

	PROCESSED=$((TOTAL - FAILED))
	TOTAL_RACES=$((TOTAL_RACES + TOTAL))
	TOTAL_FAILED=$((TOTAL_FAILED + FAILED))

	echo ""
	echo " Year summary: $YEAR"
	echo " Processed: $PROCESSED/$TOTAL races"
	if [ "$FAILED" -gt 0 ]; then
		echo " Failed:    $FAILED"
	fi
	echo " Merged JSONLs: data_lake/pit_evals_${YEAR}_season_${TIMESTAMP}.jsonl"
	echo "                data_lake/pit_suggestions_${YEAR}_season_${TIMESTAMP}.jsonl"
	echo "                data_lake/pit_timings_${YEAR}_season_${TIMESTAMP}.jsonl"
	echo " Replay manifest: data_lake/replay_manifests/replay_manifest_${YEAR}_season_${TIMESTAMP}.json"
done

echo ""
echo "[9/10] Final summary"
echo "=========================================="
echo " Season simulation complete."
echo " Processed: $((TOTAL_RACES - TOTAL_FAILED))/$TOTAL_RACES races"
if [ "$TOTAL_FAILED" -gt 0 ]; then
	echo " Failed:    $TOTAL_FAILED"
fi
echo ""
echo " Dashboard:     http://localhost:8501"
echo " Flink UI:      http://localhost:8081"
if [ "$WITH_ML_INFERENCE" -eq 1 ]; then
	echo " ML topic:      f1-ml-predictions"
fi
echo " Raw output:    data_lake/{pit_evals,pit_suggestions,tire_drops,lift_coast,drop_zones,ml_features}/"
echo " Merged JSONLs: data_lake/{pit_evals,pit_suggestions,pit_timings,tire_drops,lift_coast,drop_zones,ml_features}_{YEAR}_season_{TIMESTAMP}.jsonl"
echo ""
echo " Wait ~3 min for Flink's rolling policy to finalize the"
echo " last JSONL file, then run the ML pipeline:"
echo "   docker compose run --rm producer python ml_pipeline/train_pit_strategy.py"
echo "[10/10] Done"
echo "=========================================="
