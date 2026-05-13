# F1 Thesis Experiment (Final Phase 2B)

This repo contains the final experiment used for the thesis comparison between:
- Final SDE (deterministic baseline)
- Batch ML
- MOA (streaming)

Main setup:
- Years: `2022 2023 2024 2025`
- Horizon: `H=2`
- Contracts:
  - `pit_any_h2` (episode-level, `pit_now_only`)
  - `pit_success_h2` (row-level, `pit_now_plus_good_pit`)
- Headline truth universe: `canonical_sde_truth`
- Profiles:
  - `e0_no_source_year` (`baseline`, drop `_source_year`)
  - `p1_percent_conservative_v1` (`percent_conservative_v1`, drop `_source_year`)

## 1) Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

MOA jar is required at:

```bash
data_lake/tools/moa.jar
```

## 2) Required inputs

You need either:
- replayed JSONL streams in `data_lake/` (`ml_features_*`, `drop_zones_*`, `pit_evals_*`, `pit_timings_*`) + replay manifests,

or already prepared datasets:
- `data_lake/ml_training_dataset_2022_2025_dual_contract.parquet`
- `data_lake/ml_training_dataset_2022_2025_dual_contract_p1_percent.parquet`

For canonical SDE-truth evaluation, these files must exist:
- `data_lake/reports/pit_truth_eligibility_audit_2022_c6_cfg120_fixed.csv`
- `data_lake/reports/pit_truth_eligibility_audit_2023_c6_cfg120_fixed.csv`
- `data_lake/reports/pit_truth_eligibility_audit_2024_c6_cfg120_fixed.csv`
- `data_lake/reports/pit_truth_eligibility_audit_2025_c6_cfg120_fixed.csv`
- `data_lake/reports/fastf1_prepared_pit_stats_2022_2025/fastf1_prepared_pit_events.csv`

## 3) Build the two Phase 2B datasets

### E0

```bash
.venv/bin/python ml_pipeline/prep_data.py \
  --data-lake data_lake \
  --years 2022 2023 2024 2025 \
  --season-tag season \
  --output data_lake/ml_training_dataset_2022_2025_dual_contract.parquet \
  --feature-profile baseline \
  --track-agnostic-mode off
```

### P1

```bash
.venv/bin/python ml_pipeline/prep_data.py \
  --data-lake data_lake \
  --years 2022 2023 2024 2025 \
  --season-tag season \
  --output data_lake/ml_training_dataset_2022_2025_dual_contract_p1_percent.parquet \
  --feature-profile percent_conservative_v1 \
  --track-agnostic-mode track_percentage_v1
```

## 4) Run Batch ML (Phase 2A matrix)

This runs both profiles and all six targets with `expanding_race_sequential`.

```bash
bash <<'BASH'
set -euo pipefail

OUT="data_lake/reports/ml_phase2a_dual_contract_2022_2025"
YEARS=(2022 2023 2024 2025)
mkdir -p "$OUT"/{oof,leaderboard,eval,by_year,matrix,logs}

TARGETS=(
  target_pit_any_h2_raw
  target_pit_any_h2_clean_actionable
  target_pit_any_h2_clean_dry_strategy
  target_pit_success_h2_raw
  target_pit_success_h2_clean_actionable
  target_pit_success_h2_clean_dry_strategy
)

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

## 5) Build canonical SDE truth universe + Batch Phase 2B frontier

```bash
.venv/bin/python ml_pipeline/build_shared_truth_universe.py \
  --ml-oof-csv data_lake/reports/ml_phase2a_dual_contract_2022_2025/oof/e0_no_source_year__target_pit_any_h2_raw.csv \
  --years 2022 2023 2024 2025 \
  --output-ml-universe-csv data_lake/reports/ml_phase2b_dual_contract_2022_2025/audits/ml_universe_2022_2025.csv \
  --output-sde-universe-csv data_lake/reports/ml_phase2b_dual_contract_2022_2025/audits/sde_universe_2022_2025.csv \
  --output-shared-universe-csv data_lake/reports/ml_phase2b_dual_contract_2022_2025/audits/shared_universe_sde_ml_2022_2025.csv \
  --output-summary-csv data_lake/reports/ml_phase2b_dual_contract_2022_2025/audits/shared_universe_summary_2022_2025.csv
```

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

## 6) Run MOA/SML Phase 2B

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

## 7) Main outputs

- Batch Phase 2A:
  - `data_lake/reports/ml_phase2a_dual_contract_2022_2025/`
- Batch Phase 2B:
  - `data_lake/reports/ml_phase2b_dual_contract_2022_2025/`
- MOA/SML Phase 2B:
  - `data_lake/reports/sml_phase2b_dual_contract_2022_2025/`

Current master report:
- `results_comparison_newest.md`
