# Thesis Reproducibility Guide (Final Phase 2B Dual-Contract)

This README is the **current, minimal, reproducible path** for the experiment that produced `results_comparison_newest.md`.

It intentionally excludes plotting/report-polish tooling and focuses only on:
- data replay/dataset generation,
- Batch ML training/evaluation,
- MOA/SML training/evaluation,
- canonical truth-universe evaluation outputs.

## 1) What This Reproduces

- Final protocol: dual-contract `H=2`
  - `pit_any_h2` (episode-level, `pit_now_only`)
  - `pit_success_h2` (row-level, `pit_now_plus_good_pit`)
- Final profiles:
  - `e0_no_source_year` (`baseline`, drop `_source_year`)
  - `p1_percent_conservative_v1` (`percent_conservative_v1`, drop `_source_year`)
- Final headline truth mode:
  - `canonical_sde_truth` using SDE c6 fixed truth universe/events

## 2) Minimal Code Files To Keep In GitHub

If you want a **straight necessary** commit for this pipeline, keep at least the files below.

### 2.1 Infra + replay (Flink/Kafka + producer)

- `docker-compose.yml`
- `run_simulation.sh`
- `simulate_season.sh`
- `scripts/precache_2022_2025.sh`
- `scripts/f1_race_calendar_2022_2025.json`
- `f1-telemetry-producer/src/prepare_race.py`
- `f1-telemetry-producer/src/stream_race.py`
- `f1-telemetry-producer/src/precache_seasons.py`

Flink processor (keep the full job module):
- `f1-telemetry-processor/pom.xml`
- `f1-telemetry-processor/src/main/java/com/polimi/f1/F1StreamingJob.java`
- `f1-telemetry-processor/src/main/java/com/polimi/f1/model/**/*.java`
- `f1-telemetry-processor/src/main/java/com/polimi/f1/operators/**/*.java`
- `f1-telemetry-processor/src/main/java/com/polimi/f1/state/**/*.java`
- `f1-telemetry-processor/src/main/java/com/polimi/f1/utils/**/*.java`

### 2.2 ML/MOA pipeline core

Top-level scripts:
- `ml_pipeline/pipeline_config.py`
- `ml_pipeline/prep_data.py`
- `ml_pipeline/train_model.py`
- `ml_pipeline/export_moa_dataset.py`
- `ml_pipeline/run_moa_arf.py`
- `ml_pipeline/run_moa_vote_logger.py`
- `ml_pipeline/evaluate_batch_dual_contract_run.py`
- `ml_pipeline/evaluate_moa_dual_contract_run.py`
- `ml_pipeline/build_batch_phase2a_matrix.py`
- `ml_pipeline/build_shared_truth_universe.py`
- `ml_pipeline/phase2b_threshold_frontier.py`
- `ml_pipeline/run_sml_phase2b_dual_contract.py`
- `ml_pipeline/java_src/MoaPrequentialVoteLogger.java`

Required libs used by the scripts above:
- `ml_pipeline/lib/data_preparation.py`
- `ml_pipeline/lib/feature_profiles.py`
- `ml_pipeline/lib/replay_manifest.py`
- `ml_pipeline/lib/report_label_contract_summary.py`
- `ml_pipeline/lib/race_metadata.py`
- `ml_pipeline/lib/model_training_cv.py`
- `ml_pipeline/lib/comparator_heuristic.py`
- `ml_pipeline/lib/pit_truth_eligibility.py`
- `ml_pipeline/lib/moa_predictions.py`
- `ml_pipeline/lib/evaluate_batch_dual_contract_run.py`
- `ml_pipeline/lib/evaluate_moa_dual_contract_run.py`
- `ml_pipeline/lib/build_batch_phase2a_matrix.py`
- `ml_pipeline/lib/build_shared_truth_universe.py`
- `ml_pipeline/lib/phase2b_threshold_frontier.py`
- `ml_pipeline/lib/run_moa_vote_logger.py`
- `ml_pipeline/lib/run_sml_phase2b_dual_contract.py`

### 2.3 Root essentials

- `requirements.txt`
- `README.md`
- `results_comparison_newest.md`

## 3) Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

MOA jar required:

```bash
mkdir -p data_lake/tools
# put jar here:
# data_lake/tools/moa.jar
```

## 4) Data Inputs Required

You need either:

1. Replayed stream JSONL artifacts in `data_lake/` (`ml_features_*`, `drop_zones_*`, `pit_evals_*`, `pit_timings_*`) plus replay manifests.

or

2. Already prepared parquet datasets:
- `data_lake/ml_training_dataset_2022_2025_dual_contract.parquet`
- `data_lake/ml_training_dataset_2022_2025_dual_contract_p1_percent.parquet`

For canonical SDE-truth evaluation also required:
- `data_lake/reports/pit_truth_eligibility_audit_2022_c6_cfg120_fixed.csv`
- `data_lake/reports/pit_truth_eligibility_audit_2023_c6_cfg120_fixed.csv`
- `data_lake/reports/pit_truth_eligibility_audit_2024_c6_cfg120_fixed.csv`
- `data_lake/reports/pit_truth_eligibility_audit_2025_c6_cfg120_fixed.csv`
- `data_lake/reports/fastf1_prepared_pit_stats_2022_2025/fastf1_prepared_pit_events.csv`

## 5) Reproduce Dataset Preparation (Dual-Contract)

### E0 dataset

```bash
.venv/bin/python ml_pipeline/prep_data.py \
  --data-lake data_lake \
  --years 2022 2023 2024 2025 \
  --season-tag season \
  --output data_lake/ml_training_dataset_2022_2025_dual_contract.parquet \
  --feature-profile baseline \
  --track-agnostic-mode off
```

### P1 dataset

```bash
.venv/bin/python ml_pipeline/prep_data.py \
  --data-lake data_lake \
  --years 2022 2023 2024 2025 \
  --season-tag season \
  --output data_lake/ml_training_dataset_2022_2025_dual_contract_p1_percent.parquet \
  --feature-profile percent_conservative_v1 \
  --track-agnostic-mode track_percentage_v1
```

## 6) Reproduce Batch ML (Phase 2A matrix)

Use racewise sequential only (`expanding_race_sequential`), no pretrain.

```bash
bash <<'BASH'
set -euo pipefail

OUT="data_lake/reports/ml_phase2a_dual_contract_2022_2025"
YEARS=(2022 2023 2024 2025)
mkdir -p "$OUT"/{oof,leaderboard,eval,by_year,matrix,logs}

run_batch () {
  local profile="$1"
  local feature_profile="$2"
  local dataset="$3"
  local target="$4"
  local run_id="${profile}__${target}"

  .venv/bin/python ml_pipeline/train_model.py \
    --data-lake data_lake \
    --years "${YEARS[@]}" \
    --season-tag season \
    --dataset "$dataset" \
    --skip-prepare-data \
    --skip-replay-validation \
    --split-protocol expanding_race_sequential \
    --feature-profile "$feature_profile" \
    --drop-source-year-feature \
    --target-column "$target" \
    --leaderboard-output "$OUT/leaderboard/${run_id}.csv" \
    --oof-output "$OUT/oof/${run_id}.csv" \
    --skip-serving-bundle

  .venv/bin/python ml_pipeline/evaluate_batch_dual_contract_run.py \
    --data-lake data_lake \
    --years "${YEARS[@]}" \
    --season-tag season \
    --oof-csv "$OUT/oof/${run_id}.csv" \
    --target-column "$target" \
    --profile "$profile" \
    --prepared-pit-events-csv data_lake/reports/fastf1_prepared_pit_stats_2022_2025/fastf1_prepared_pit_events.csv \
    --output-summary-csv "$OUT/eval/${run_id}.csv" \
    --output-by-year-csv "$OUT/by_year/${run_id}.csv"
}

TARGETS=(
  target_pit_any_h2_raw
  target_pit_any_h2_clean_actionable
  target_pit_any_h2_clean_dry_strategy
  target_pit_success_h2_raw
  target_pit_success_h2_clean_actionable
  target_pit_success_h2_clean_dry_strategy
)

for t in "${TARGETS[@]}"; do
  run_batch e0_no_source_year baseline data_lake/ml_training_dataset_2022_2025_dual_contract.parquet "$t"
  run_batch p1_percent_conservative_v1 percent_conservative_v1 data_lake/ml_training_dataset_2022_2025_dual_contract_p1_percent.parquet "$t"
done
BASH
```

Build compact matrix:

```bash
bash <<'BASH'
set -euo pipefail
OUT="data_lake/reports/ml_phase2a_dual_contract_2022_2025"
summary_args=()
by_year_args=()
for f in "$OUT"/eval/*.csv; do
  run_id="$(basename "$f" .csv)"
  y="$OUT/by_year/${run_id}.csv"
  summary_args+=(--run-summary "${run_id}=${f}")
  by_year_args+=(--run-by-year "${run_id}=${y}")
done
.venv/bin/python ml_pipeline/build_batch_phase2a_matrix.py \
  "${summary_args[@]}" \
  "${by_year_args[@]}" \
  --output-matrix-csv "$OUT/matrix/batch_phase2a_matrix_compact.csv" \
  --output-by-year-csv "$OUT/matrix/batch_phase2a_matrix_by_year.csv"
BASH
```

## 7) Canonical SDE-Truth Universe + Batch Phase 2B Frontier

Build universe CSVs:

```bash
.venv/bin/python ml_pipeline/build_shared_truth_universe.py \
  --ml-oof-csv data_lake/reports/ml_phase2a_dual_contract_2022_2025/oof/e0_no_source_year__target_pit_any_h2_raw.csv \
  --years 2022 2023 2024 2025 \
  --output-ml-universe-csv data_lake/reports/ml_phase2b_dual_contract_2022_2025/audits/ml_universe_2022_2025.csv \
  --output-sde-universe-csv data_lake/reports/ml_phase2b_dual_contract_2022_2025/audits/sde_universe_2022_2025.csv \
  --output-shared-universe-csv data_lake/reports/ml_phase2b_dual_contract_2022_2025/audits/shared_universe_sde_ml_2022_2025.csv \
  --output-summary-csv data_lake/reports/ml_phase2b_dual_contract_2022_2025/audits/shared_universe_summary_2022_2025.csv
```

Batch Phase 2B threshold frontier (canonical SDE truth):

```bash
.venv/bin/python ml_pipeline/phase2b_threshold_frontier.py \
  --data-lake data_lake \
  --years 2022 2023 2024 2025 \
  --season-tag season \
  --oof-dir data_lake/reports/ml_phase2a_dual_contract_2022_2025/oof \
  --truth-universe-race-driver-csv data_lake/reports/ml_phase2b_dual_contract_2022_2025/audits/sde_universe_2022_2025.csv \
  --truth-universe-events-csvs \
    data_lake/reports/pit_truth_eligibility_audit_2022_c6_cfg120_fixed.csv \
    data_lake/reports/pit_truth_eligibility_audit_2023_c6_cfg120_fixed.csv \
    data_lake/reports/pit_truth_eligibility_audit_2024_c6_cfg120_fixed.csv \
    data_lake/reports/pit_truth_eligibility_audit_2025_c6_cfg120_fixed.csv \
  --truth-universe-mode-label canonical_sde_truth \
  --output-compact-csv data_lake/reports/ml_phase2b_dual_contract_2022_2025/frontier/phase2b_threshold_frontier_compact.csv \
  --output-by-year-csv data_lake/reports/ml_phase2b_dual_contract_2022_2025/by_year/phase2b_threshold_frontier_by_year.csv \
  --output-recommended-csv data_lake/reports/ml_phase2b_dual_contract_2022_2025/recommended/phase2b_recommended_operating_points.csv \
  --output-md data_lake/reports/ml_phase2b_dual_contract_2022_2025/phase2b_threshold_frontier_report.md
```

## 8) Reproduce MOA/SML Phase 2B (Canonical + Native Sensitivity)

This is the exact orchestrator path used:

```bash
.venv/bin/python ml_pipeline/run_sml_phase2b_dual_contract.py \
  --years 2022 2023 2024 2025 \
  --run-export --run-moa --run-eval --run-matrix --run-frontier --run-prequential \
  --run-native-sensitivity \
  --truth-universe-race-driver-csv data_lake/reports/ml_phase2b_dual_contract_2022_2025/audits/sde_universe_2022_2025.csv \
  --truth-universe-events-csvs \
    data_lake/reports/pit_truth_eligibility_audit_2022_c6_cfg120_fixed.csv \
    data_lake/reports/pit_truth_eligibility_audit_2023_c6_cfg120_fixed.csv \
    data_lake/reports/pit_truth_eligibility_audit_2024_c6_cfg120_fixed.csv \
    data_lake/reports/pit_truth_eligibility_audit_2025_c6_cfg120_fixed.csv \
  --moa-jar data_lake/tools/moa.jar \
  --jobs auto \
  --resume
```

## 9) Expected Output Roots (Experiment Only)

- Batch Phase 2A:
  - `data_lake/reports/ml_phase2a_dual_contract_2022_2025/`
- Batch Phase 2B frontier:
  - `data_lake/reports/ml_phase2b_dual_contract_2022_2025/`
- MOA/SML Phase 2B:
  - `data_lake/reports/sml_phase2b_dual_contract_2022_2025/`

## 10) Out Of Scope In This README

Not included on purpose:
- figure generation,
- presentation graph polishing,
- markdown/image report assembly,
- thesis-writing helper scripts.

Those are downstream of the core experiment and are not required to reproduce model/data/comparator results.
