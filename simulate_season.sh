#!/usr/bin/env bash
# simulate_season.sh, bulk data generation for ml training.
# dynamically fetches the full race calendar for the given year using fastf1,
# then runs the two-stage python producer for each race to accumulate
# ground truth csv data in the data_lake directory.
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

set -uo pipefail

YEAR=2023
SPEED=100
SESSION="R"

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
	*)
		echo "Unknown argument: $1"
		exit 1
		;;
	esac
done

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
PRODUCER_DIR="$PROJECT_DIR/f1-telemetry-producer"
PREPARE_SCRIPT="$PRODUCER_DIR/src/prepare_race.py"
STREAM_SCRIPT="$PRODUCER_DIR/src/stream_race.py"

# activate producer venv
if [ -f "$PRODUCER_DIR/venv/bin/activate" ]; then
	source "$PRODUCER_DIR/venv/bin/activate"
elif [ -f "$PRODUCER_DIR/.venv/bin/activate" ]; then
	source "$PRODUCER_DIR/.venv/bin/activate"
fi

# dynamically fetch the full race calendar for the given year from fastf1.
# filters to actual race weekends (excludes pre-season testing).
echo "Fetching $YEAR race calendar from fastf1..."
RACES_JSON=$(python -c "
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
mapfile -t RACES < <(python -c "import json; [print(r) for r in json.loads('$RACES_JSON')]")
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
	python "$PREPARE_SCRIPT" \
		--year "$YEAR" \
		--race "$RACE" \
		--session "$SESSION" || {
		echo "WARNING: prepare failed for $RACE, skipping..."
		FAILED=$((FAILED + 1))
		continue
	}

	echo "       Stage 2/2, stream to kafka"
	python "$STREAM_SCRIPT" \
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

# flink's FileSink produces files like "pit-eval-0-0.csv" or "tire-drop-0-0.csv"
# under date-partitioned directories (e.g., 2026-03-06--14/). consolidate
# all part-files into a single csv per sink type with a clear name.
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

for SINK_DIR in pit_evals tire_drops; do
	TARGET_DIR="$PROJECT_DIR/data_lake/$SINK_DIR"
	if [ -d "$TARGET_DIR" ]; then
		MERGED_FILE="$PROJECT_DIR/data_lake/${SINK_DIR}_${YEAR}_season_${TIMESTAMP}.csv"
		HEADER_WRITTEN=false
		for f in $(find "$TARGET_DIR" -name "*.csv" -type f | sort); do
			if [ "$HEADER_WRITTEN" = false ]; then
				cat "$f" >"$MERGED_FILE"
				HEADER_WRITTEN=true
			else
				# skip header line from subsequent files to avoid duplicate headers
				tail -n +2 "$f" >>"$MERGED_FILE"
			fi
		done
		if [ "$HEADER_WRITTEN" = true ]; then
			echo " Merged: $MERGED_FILE"
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
echo " Raw output:    data_lake/pit_evals/ , data_lake/tire_drops/"
echo " Merged CSVs:   data_lake/pit_evals_${YEAR}_season_${TIMESTAMP}.csv"
echo "                 data_lake/tire_drops_${YEAR}_season_${TIMESTAMP}.csv"
echo ""
echo " Wait ~3 min for Flink's rolling policy to finalize the"
echo " last CSV file, then run the ML pipeline:"
echo "   cd ml_pipeline && python train_pit_strategy.py"
echo "=========================================="
