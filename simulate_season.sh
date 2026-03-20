#!/usr/bin/env bash
# simulate_season.sh, bulk data generation for ml training.
# dynamically fetches the full race calendar for the given year using fastf1,
# then runs the two-stage python producer for each race to accumulate
# ground truth jsonl data in the data_lake directory.
#
# pit loss thresholds are embedded per-track by prepare_race.py (upstream enrichment),
# no manual specification needed.
#
# prerequisite, the docker stack and flink job must already be running.
#   ./run_simulation.sh, starts kafka, flink, submits the job
#
# usage
#   ./simulate_season.sh
#   ./simulate_season.sh --speed 200
#   ./simulate_season.sh --year 2024
#   ./simulate_season.sh --races "Italian Grand Prix,British Grand Prix"

set -euo pipefail

YEAR=2023
SPEED=100
SESSION="R"
RACES_FILTER=""

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
	*)
		echo "Unknown argument: $1"
		exit 1
		;;
	esac
done

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.yml"

# dynamically fetch the full race calendar for the given year from fastf1.
# filters to actual race weekends (excludes pre-season testing).
# -T disables pseudo-tty so stdout capture works correctly.
echo "Fetching $YEAR race calendar from fastf1..."
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

# parse json array into bash array
mapfile -t RACES < <(docker compose -f "$COMPOSE_FILE" run --rm -T producer \
	python -c "import json; [print(r) for r in json.loads('$RACES_JSON')]")

# if --races filter is set, only keep races that match the comma-separated list
if [ -n "$RACES_FILTER" ]; then
	IFS=',' read -ra FILTER_LIST <<<"$RACES_FILTER"
	FILTERED=()
	for RACE in "${RACES[@]}"; do
		for F in "${FILTER_LIST[@]}"; do
			# trim whitespace from filter entry
			F="$(echo "$F" | xargs)"
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
		--session "$SESSION" || {
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
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

for SINK_DIR in pit_evals tire_drops lift_coast drop_zones ml_features; do
	TARGET_DIR="$PROJECT_DIR/data_lake/$SINK_DIR"
	if [ -d "$TARGET_DIR" ]; then
		MERGED_FILE="$PROJECT_DIR/data_lake/${SINK_DIR}_${YEAR}_season_${TIMESTAMP}.jsonl"
		find "$TARGET_DIR" -type f \( -name "*.jsonl" -o -name "*.inprogress*" \) -exec cat {} + > "$MERGED_FILE"
		if [ -s "$MERGED_FILE" ]; then
			echo " Merged: $MERGED_FILE"
		else
			rm -f "$MERGED_FILE"
		fi
	fi
done

echo ""
echo "=========================================="
echo " Season simulation complete."
echo " Processed: $((TOTAL - FAILED))/$TOTAL races"
if [ "$FAILED" -gt 0 ]; then
	echo " Failed:    $FAILED"
fi
echo ""
echo " Raw output:    data_lake/{pit_evals,tire_drops,lift_coast,drop_zones,ml_features}/"
echo " Merged JSONLs: data_lake/pit_evals_${YEAR}_season_${TIMESTAMP}.jsonl"
echo "                 data_lake/tire_drops_${YEAR}_season_${TIMESTAMP}.jsonl"
echo ""
echo " Wait ~3 min for Flink's rolling policy to finalize the"
echo " last JSONL file, then run the ML pipeline:"
echo "   docker compose run --rm producer python ml_pipeline/train_pit_strategy.py"
echo "=========================================="
