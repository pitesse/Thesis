#!/usr/bin/env bash
# simulate_season.sh — bulk data generation for ml training.
# runs the python producer for a full season of races to accumulate
# ground truth csv data in the data_lake/ directory.
#
# prerequisite: the docker stack and flink job must already be running.
#   ./run_simulation.sh  (starts kafka, flink, submits the job)
#
# usage:
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

# 2023 season races in calendar order.
# note: pit-loss is fixed in the flink job (set at submission time via run_simulation.sh).
# it only affects PitWindowDetector alerts, not the ground truth PitStopEvaluationAlert data
# used for ML training.
RACES=(
	"Bahrain Grand Prix"
	"Saudi Arabian Grand Prix"
	"Australian Grand Prix"
	"Azerbaijan Grand Prix"
	"Miami Grand Prix"
	"Monaco Grand Prix"
	"Spanish Grand Prix"
	"Canadian Grand Prix"
	"Austrian Grand Prix"
	"British Grand Prix"
	"Hungarian Grand Prix"
	"Belgian Grand Prix"
)

# activate producer venv
if [ -f "$PRODUCER_DIR/venv/bin/activate" ]; then
	source "$PRODUCER_DIR/venv/bin/activate"
elif [ -f "$PRODUCER_DIR/.venv/bin/activate" ]; then
	source "$PRODUCER_DIR/.venv/bin/activate"
fi

echo "=========================================="
echo " F1 Season Simulator"
echo " Year:   $YEAR"
echo " Speed:  ${SPEED}x"
echo " Races:  ${#RACES[@]}"
echo "=========================================="

FAILED=0
for i in "${!RACES[@]}"; do
	RACE="${RACES[$i]}"
	NUM=$((i + 1))
	echo ""
	echo "[$NUM/${#RACES[@]}] $RACE"
	echo "------------------------------------------"

	python "$PRODUCER_DIR/src/main.py" \
		--year "$YEAR" \
		--race "$RACE" \
		--session "$SESSION" \
		--speed "$SPEED" || {
		echo "WARNING: $RACE failed, skipping..."
		FAILED=$((FAILED + 1))
		continue
	}

	# brief pause between races so flink can drain pending events and
	# advance watermarks past any state timers from the completed race.
	# race timestamps are weeks apart so watermarks jump forward cleanly.
	if [ "$NUM" -lt "${#RACES[@]}" ]; then
		echo "       Draining (15s)..."
		sleep 15
	fi
done

echo ""
echo "=========================================="
echo " Season simulation complete."
echo " Processed: $((${#RACES[@]} - FAILED))/${#RACES[@]} races"
if [ "$FAILED" -gt 0 ]; then
	echo " Failed:    $FAILED"
fi
echo " Output:    data_lake/pit_evals/ , data_lake/tire_drops/"
echo ""
echo " Wait ~3 min for Flink's rolling policy to finalize the"
echo " last CSV file, then run the ML pipeline:"
echo "   cd ml_pipeline && python train_pit_strategy.py"
echo "=========================================="
