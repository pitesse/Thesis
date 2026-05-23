#!/usr/bin/env bash
set -euo pipefail

# Canonical end-to-end runner for the final thesis experiment flow.
# Safe by default: no heavy stage runs unless --execute is passed.

EXECUTE=0
if [[ "${1:-}" == "--execute" ]]; then
  EXECUTE=1
elif [[ "${1:-}" == "--dry-run" ]]; then
  EXECUTE=0
elif [[ -n "${1:-}" ]]; then
  echo "Usage: $0 [--dry-run|--execute]"
  exit 2
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

VENV_PY=".venv/bin/python"
if [[ ! -x "$VENV_PY" ]]; then
  echo "ERROR: .venv/bin/python not found. Create/activate venv first."
  exit 1
fi

TS="$(date +%Y%m%d_%H%M%S)"
YEARS=(2022 2023 2024 2025)

BATCH_ROOT="data_lake/reports/ml_phase2b_dual_contract_2022_2025"
SML_ROOT="data_lake/reports/sml_phase2b_dual_contract_2022_2025"
FIG_ROOT="data_lake/reports/phase2b_presentation_figures"
FINAL_REFRESH_ROOT="${FIG_ROOT}/final_refresh"
TIMING_DIR_REAL="data_lake/reports/phase2b_timing"
if [[ "$EXECUTE" -eq 1 ]]; then
  TIMING_DIR="$TIMING_DIR_REAL"
else
  TIMING_DIR="${TIMING_DIR_REAL}/dry_run/${TS}"
fi
LOG_DIR="${TIMING_DIR}/logs"
RUNTIME_CSV="${TIMING_DIR}/training_runtime_2022_2025.csv"

RUN_E0_ANY="e0_no_source_year__target_pit_any_h2_clean_actionable"
RUN_E0_SUCCESS="e0_no_source_year__target_pit_success_h2_clean_actionable"
RUN_P1_ANY="p1_percent_conservative_v1__target_pit_any_h2_clean_actionable"
RUN_P1_SUCCESS="p1_percent_conservative_v1__target_pit_success_h2_clean_actionable"
RUN_IDS=("$RUN_E0_ANY" "$RUN_E0_SUCCESS" "$RUN_P1_ANY" "$RUN_P1_SUCCESS")

DATASET_E0="data_lake/ml_training_dataset_2022_2025_dual_contract.parquet"
DATASET_P1="data_lake/ml_training_dataset_2022_2025_dual_contract_p1_percent.parquet"

TRUTH_EVENT_CSVS=(
  "data_lake/reports/pit_truth_eligibility_audit_2022_c6_cfg120_fixed.csv"
  "data_lake/reports/pit_truth_eligibility_audit_2023_c6_cfg120_fixed.csv"
  "data_lake/reports/pit_truth_eligibility_audit_2024_c6_cfg120_fixed.csv"
  "data_lake/reports/pit_truth_eligibility_audit_2025_c6_cfg120_fixed.csv"
)

mkdir -p "$TIMING_DIR" "$LOG_DIR"
printf "stage,learner,profile,target,command_name,start_time_iso,end_time_iso,wall_seconds,wall_hms,log_file,output_file,status\n" > "$RUNTIME_CSV"

run_timed() {
  local stage="$1" learner="$2" profile="$3" target="$4" cname="$5" logfile="$6" outfile="$7"
  shift 7
  if [[ "${1:-}" == "--" ]]; then shift; fi

  local start end siso eiso secs hms status rc
  start="$(date +%s)"; siso="$(date -Iseconds)"

  if [[ "$EXECUTE" -eq 1 ]]; then
    set +e
    "$@" 2>&1 | tee "$logfile"
    rc=${PIPESTATUS[0]}
    set -e
  else
    echo "[DRY-RUN] $*" | tee "$logfile"
    rc=0
  fi

  end="$(date +%s)"; eiso="$(date -Iseconds)"
  secs="$((end-start))"
  hms="$(printf '%02d:%02d:%02d' $((secs/3600)) $(((secs%3600)/60)) $((secs%60)))"
  status="PASS"; [[ "$rc" -ne 0 ]] && status="FAIL"

  printf '"%s","%s","%s","%s","%s","%s","%s",%s,"%s","%s","%s","%s"\n' \
    "$stage" "$learner" "$profile" "$target" "$cname" "$siso" "$eiso" "$secs" "$hms" "$logfile" "$outfile" "$status" >> "$RUNTIME_CSV"

  if [[ "$rc" -ne 0 ]]; then
    echo "ERROR: command failed -> $cname"
    exit "$rc"
  fi
}

assert_not_stale() {
  local path="$1" expected_rows="$2" label="$3"
  if [[ ! -f "$path" ]]; then
    echo "WARN: missing $label ($path)"
    return 0
  fi
  local rows
  rows="$($VENV_PY - <<PY
import pandas as pd
from pathlib import Path
p=Path('$path')
if p.suffix.lower()=='.csv':
    n=len(pd.read_csv(p))
elif p.suffix.lower()=='.parquet':
    n=len(pd.read_parquet(p))
else:
    n=-1
print(n)
PY
)"
  if [[ "$rows" != "$expected_rows" ]]; then
    echo "ERROR: stale row count for $label: got $rows expected $expected_rows"
    exit 1
  fi
}

echo "=== Stage 0: preflight checks ==="
if [[ -f "${FIG_ROOT}/phase2b_figures_manifest.csv" ]]; then
  if grep -q "ml_phase2a" "${FIG_ROOT}/phase2b_figures_manifest.csv"; then
    echo "WARN: phase2a path appears in legacy figure manifest snapshot."
  fi
fi

echo "=== Stage 1: prepare datasets ==="
run_timed "data_prep" "dataset" "no_year" "dual_contract" "prep_no_year" \
  "${LOG_DIR}/prep_no_year.log" "$DATASET_E0" -- \
  "$VENV_PY" ml_pipeline/prep_data.py --years "${YEARS[@]}" --season-tag season --horizon 2 \
  --feature-profile baseline --track-agnostic-mode off --output "$DATASET_E0"

run_timed "data_prep" "dataset" "percent" "dual_contract" "prep_percent" \
  "${LOG_DIR}/prep_percent.log" "$DATASET_P1" -- \
  "$VENV_PY" ml_pipeline/prep_data.py --years "${YEARS[@]}" --season-tag season --horizon 2 \
  --feature-profile percent_conservative_v1 --track-agnostic-mode track_percentage_v1 --output "$DATASET_P1"

echo "=== Stage 2: batch train (race-sequential OOF) ==="
run_timed "train" "batch" "no_year" "target_pit_any_h2_clean_actionable" "batch_${RUN_E0_ANY}" \
  "${LOG_DIR}/batch_${RUN_E0_ANY}.log" "${BATCH_ROOT}/oof/${RUN_E0_ANY}.csv" -- \
  "$VENV_PY" ml_pipeline/train_model.py --dataset "$DATASET_E0" --skip-prepare-data \
  --split-protocol expanding_race_sequential --folds 5 --drop-source-year-feature \
  --feature-profile baseline --track-agnostic-mode off --target-column target_pit_any_h2_clean_actionable \
  --oof-output "${BATCH_ROOT}/oof/${RUN_E0_ANY}.csv" --leaderboard-output "${BATCH_ROOT}/leaderboard/${RUN_E0_ANY}.csv"

run_timed "train" "batch" "no_year" "target_pit_success_h2_clean_actionable" "batch_${RUN_E0_SUCCESS}" \
  "${LOG_DIR}/batch_${RUN_E0_SUCCESS}.log" "${BATCH_ROOT}/oof/${RUN_E0_SUCCESS}.csv" -- \
  "$VENV_PY" ml_pipeline/train_model.py --dataset "$DATASET_E0" --skip-prepare-data \
  --split-protocol expanding_race_sequential --folds 5 --drop-source-year-feature \
  --feature-profile baseline --track-agnostic-mode off --target-column target_pit_success_h2_clean_actionable \
  --oof-output "${BATCH_ROOT}/oof/${RUN_E0_SUCCESS}.csv" --leaderboard-output "${BATCH_ROOT}/leaderboard/${RUN_E0_SUCCESS}.csv"

run_timed "train" "batch" "percent" "target_pit_any_h2_clean_actionable" "batch_${RUN_P1_ANY}" \
  "${LOG_DIR}/batch_${RUN_P1_ANY}.log" "${BATCH_ROOT}/oof/${RUN_P1_ANY}.csv" -- \
  "$VENV_PY" ml_pipeline/train_model.py --dataset "$DATASET_P1" --skip-prepare-data \
  --split-protocol expanding_race_sequential --folds 5 --drop-source-year-feature \
  --feature-profile percent_conservative_v1 --track-agnostic-mode track_percentage_v1 --target-column target_pit_any_h2_clean_actionable \
  --oof-output "${BATCH_ROOT}/oof/${RUN_P1_ANY}.csv" --leaderboard-output "${BATCH_ROOT}/leaderboard/${RUN_P1_ANY}.csv"

run_timed "train" "batch" "percent" "target_pit_success_h2_clean_actionable" "batch_${RUN_P1_SUCCESS}" \
  "${LOG_DIR}/batch_${RUN_P1_SUCCESS}.log" "${BATCH_ROOT}/oof/${RUN_P1_SUCCESS}.csv" -- \
  "$VENV_PY" ml_pipeline/train_model.py --dataset "$DATASET_P1" --skip-prepare-data \
  --split-protocol expanding_race_sequential --folds 5 --drop-source-year-feature \
  --feature-profile percent_conservative_v1 --track-agnostic-mode track_percentage_v1 --target-column target_pit_success_h2_clean_actionable \
  --oof-output "${BATCH_ROOT}/oof/${RUN_P1_SUCCESS}.csv" --leaderboard-output "${BATCH_ROOT}/leaderboard/${RUN_P1_SUCCESS}.csv"

echo "=== Stage 3: MOA export + prequential ==="
for rid in "${RUN_IDS[@]}"; do
  prof="baseline"; mode="off"; ds="$DATASET_E0"
  [[ "$rid" == p1_* ]] && prof="percent_conservative_v1" && mode="track_percentage_v1" && ds="$DATASET_P1"
  tgt="${rid#*__}"

  run_timed "export" "moa" "$prof" "$tgt" "export_${rid}" \
    "${LOG_DIR}/export_${rid}.log" "${SML_ROOT}/exports/${rid}.csv" -- \
    "$VENV_PY" ml_pipeline/export_moa_dataset.py --skip-prepare-data --dataset "$ds" --target-column "$tgt" \
    --feature-profile "$prof" --track-agnostic-mode "$mode" --drop-source-year-feature \
    --output-csv "${SML_ROOT}/exports/${rid}.csv" --output-arff "${SML_ROOT}/exports/${rid}.arff" --schema-output "${SML_ROOT}/exports/${rid}.json"

  run_timed "train_eval" "moa" "$prof" "$tgt" "vote_${rid}" \
    "${LOG_DIR}/moa_vote_${rid}.log" "${SML_ROOT}/moa/predictions/${rid}.csv" -- \
    "$VENV_PY" ml_pipeline/run_moa_vote_logger.py --input-arff "${SML_ROOT}/exports/${rid}.arff" \
    --moa-jar data_lake/tools/moa.jar --predictions-output "${SML_ROOT}/moa/predictions/${rid}.csv" \
    --summary-output "${SML_ROOT}/moa/vote_summary/${rid}.csv" --metadata-output "${SML_ROOT}/moa/vote_metadata/${rid}.json"
done

echo "=== Stage 4: shared universe + evaluation + frontier ==="
run_timed "truth_universe" "shared" "canonical" "all" "build_shared_truth_universe" \
  "${LOG_DIR}/build_shared_truth_universe.log" "${BATCH_ROOT}/audits/shared_universe_summary_2022_2025.csv" -- \
  "$VENV_PY" ml_pipeline/build_shared_truth_universe.py --ml-oof-csv "${BATCH_ROOT}/oof/${RUN_E0_ANY}.csv" --years "${YEARS[@]}" \
  --output-ml-universe-csv "${BATCH_ROOT}/audits/ml_universe_2022_2025.csv" \
  --output-sde-universe-csv "${BATCH_ROOT}/audits/sde_universe_2022_2025.csv" \
  --output-shared-universe-csv "${BATCH_ROOT}/audits/shared_universe_sde_ml_2022_2025.csv" \
  --output-summary-csv "${BATCH_ROOT}/audits/shared_universe_summary_2022_2025.csv"

TRUTH_UNIVERSE_CSV="${BATCH_ROOT}/audits/sde_universe_2022_2025.csv"

for rid in "${RUN_IDS[@]}"; do
  prof="e0_no_source_year"; ds="$DATASET_E0"
  [[ "$rid" == p1_* ]] && prof="p1_percent_conservative_v1" && ds="$DATASET_P1"
  tgt="${rid#*__}"

  run_timed "eval" "batch" "$prof" "$tgt" "eval_batch_${rid}" \
    "${LOG_DIR}/eval_batch_${rid}.log" "${BATCH_ROOT}/eval/${rid}.csv" -- \
    "$VENV_PY" ml_pipeline/evaluate_batch_dual_contract_run.py --data-lake data_lake --years "${YEARS[@]}" --season-tag season \
    --oof-csv "${BATCH_ROOT}/oof/${rid}.csv" --target-column "$tgt" --profile "$prof" \
    --truth-universe-race-driver-csv "$TRUTH_UNIVERSE_CSV" --truth-universe-events-csvs "${TRUTH_EVENT_CSVS[@]}" \
    --output-summary-csv "${BATCH_ROOT}/eval/${rid}.csv" --output-by-year-csv "${BATCH_ROOT}/by_year/${rid}.csv"

  run_timed "eval" "moa" "$prof" "$tgt" "eval_moa_${rid}" \
    "${LOG_DIR}/eval_moa_${rid}.log" "${SML_ROOT}/eval/${rid}.csv" -- \
    "$VENV_PY" ml_pipeline/evaluate_moa_dual_contract_run.py --data-lake data_lake --years "${YEARS[@]}" --season-tag season \
    --dataset "$ds" --target-column "$tgt" --profile "$prof" --moa-predictions "${SML_ROOT}/moa/predictions/${rid}.csv" \
    --truth-universe-race-driver-csv "$TRUTH_UNIVERSE_CSV" --truth-universe-events-csvs "${TRUTH_EVENT_CSVS[@]}" \
    --output-oof-csv "${SML_ROOT}/oof/${rid}.csv" --output-summary-csv "${SML_ROOT}/eval/${rid}.csv" --output-by-year-csv "${SML_ROOT}/by_year/${rid}.csv"
done

run_timed "frontier" "batch" "all" "all" "phase2b_frontier_batch" \
  "${LOG_DIR}/frontier_batch.log" "${BATCH_ROOT}/recommended/phase2b_recommended_operating_points.csv" -- \
  "$VENV_PY" ml_pipeline/phase2b_threshold_frontier.py --data-lake data_lake --years "${YEARS[@]}" --season-tag season \
  --oof-dir "${BATCH_ROOT}/oof" --truth-universe-race-driver-csv "$TRUTH_UNIVERSE_CSV" --truth-universe-events-csvs "${TRUTH_EVENT_CSVS[@]}" \
  --truth-universe-mode-label canonical_sde_truth \
  --output-compact-csv "${BATCH_ROOT}/frontier/phase2b_threshold_frontier_compact.csv" \
  --output-by-year-csv "${BATCH_ROOT}/by_year/phase2b_threshold_frontier_by_year.csv" \
  --output-recommended-csv "${BATCH_ROOT}/recommended/phase2b_recommended_operating_points.csv" \
  --output-md "${BATCH_ROOT}/phase2b_threshold_frontier_report.md"

echo "=== Stage 5: final results package ==="
run_timed "results" "reports" "final_refresh" "all" "build_final_refresh" \
  "${LOG_DIR}/build_final_refresh.log" "${FINAL_REFRESH_ROOT}/final_refresh_summary.md" -- \
  "$VENV_PY" ml_pipeline/build_final_refresh_package.py

run_timed "results" "reports" "final_refresh" "all" "build_final_slide_assets" \
  "${LOG_DIR}/build_final_slide_assets.log" "${FINAL_REFRESH_ROOT}/final_slide_assets_manifest.csv" -- \
  "$VENV_PY" ml_pipeline/build_final_slide_assets.py --output-dir "$FINAL_REFRESH_ROOT"

run_timed "audit" "reports" "phase2b" "all" "delivery_audit" \
  "${LOG_DIR}/delivery_audit.log" "${FIG_ROOT}/phase2b_delivery_audit.csv" -- \
  "$VENV_PY" ml_pipeline/audit_phase2b_delivery_coverage.py

echo "=== Stage 6: stale checks ==="
assert_not_stale "${SML_ROOT}/exports/${RUN_E0_SUCCESS}.csv" 91473 "MOA export E0 pit_success"
assert_not_stale "${SML_ROOT}/exports/${RUN_P1_SUCCESS}.csv" 91473 "MOA export P1 pit_success"

if [[ "$EXECUTE" -eq 1 ]]; then
  if [[ -f "${FINAL_REFRESH_ROOT}/final_refresh_manifest.csv" ]] && grep -q "ml_phase2a" "${FINAL_REFRESH_ROOT}/final_refresh_manifest.csv"; then
    echo "ERROR: phase2a fallback detected in final_refresh manifest"
    exit 1
  fi
fi

echo "=== Stage 7: terminal summary ==="
run_timed "summary" "reports" "final_refresh" "all" "print_final_results" \
  "${LOG_DIR}/print_final_results.log" "${FINAL_REFRESH_ROOT}/final_refresh_summary.md" -- \
  "$VENV_PY" ml_pipeline/print_final_results.py

echo "\nDone."
echo "Runtime CSV: $RUNTIME_CSV"
echo "Final refresh summary: ${FINAL_REFRESH_ROOT}/final_refresh_summary.md"
if [[ "$EXECUTE" -eq 0 ]]; then
  echo "Note: dry-run mode. Real timing/artifact files were not modified."
  echo "Dry-run outputs are under: $TIMING_DIR"
fi
