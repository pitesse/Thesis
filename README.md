# Reproducible Streaming Pit-Decision Evaluation for Formula 1

This repository implements a reproducible race-replay and pit-decision evaluation pipeline.

The project compares a Flink Strategy Engine, race-wise Batch XGBoost, and a MOA Online Learner.

## Research Goal
Build and evaluate streaming/offline decision systems for two prediction contracts:
- `pit_any_h2`: predict any pit soon.
- `pit_success_h2`: predict a successful evaluated pit soon.

For `pit_success_h2`, no-match positive predictions are false positives under the strict operational contract.

## Architecture Overview
1. Historical race data is prepared for replay.
2. Replay events are streamed through Kafka.
3. Flink computes race context and strategy actions/outcomes.
4. JSONL outputs are converted into ML datasets.
5. Batch and MOA learners are trained/evaluated on matched contracts.
6. Final result package is generated under `final_refresh/`.

## Compared Systems
- **Flink Strategy Engine**: rule-based streaming decision logic.
- **Batch XGBoost**: race-sequential offline OOF learning.
- **MOA Online Learner**: prequential test-then-train stream learning.

## Repository Structure
- `f1-telemetry-producer/`: replay preparation and Kafka replay sender.
- `f1-telemetry-processor/`: Flink job and Java operators.
- `ml_pipeline/`: dataset prep, Batch/MOA training, evaluation, comparators, final packaging.
- `data_lake/`: generated artifacts and report outputs.
- `thesis/`: LaTeX thesis sources.

## Requirements
- Python 3.11+
- Java 17+
- Docker + Docker Compose
- MOA jar at `data_lake/tools/moa.jar`

## Quick Start
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run Streaming Replay
1. Prepare replay snapshot:
```bash
.venv/bin/python f1-telemetry-producer/src/prepare_race.py --help
```
2. Stream replay to Kafka:
```bash
.venv/bin/python f1-telemetry-producer/src/stream_race.py --help
```
3. Run Flink processor via Docker Compose:
```bash
docker compose up -d
```

## Build ML Datasets
```bash
.venv/bin/python ml_pipeline/prep_data.py --help
```
Use profile `baseline` (No-Year) and `percent_conservative_v1` (Percent).

## Run Batch Experiments
```bash
.venv/bin/python ml_pipeline/train_model.py --help
.venv/bin/python ml_pipeline/evaluate_batch_dual_contract_run.py --help
```
Use `--split-protocol expanding_race_sequential` for race-sequential OOF.

## Run MOA Experiments
```bash
.venv/bin/python ml_pipeline/export_moa_dataset.py --help
.venv/bin/python ml_pipeline/run_moa_vote_logger.py --help
.venv/bin/python ml_pipeline/evaluate_moa_dual_contract_run.py --help
```

## Build Final Results
```bash
.venv/bin/python ml_pipeline/build_final_refresh_package.py
.venv/bin/python ml_pipeline/build_final_slide_assets.py
```

## Print Final Results
```bash
.venv/bin/python ml_pipeline/print_final_results.py
```

## Reproducibility / Audit Notes
- Keep truth universe fixed to `canonical_sde_truth` for headline comparisons.
- Use `clean_actionable` as the main reporting lens.
- For `pit_success_h2` strict contract:
  - positives: successful evaluated pit soon,
  - negatives: no pit soon OR failed/disadvantage pit,
  - unknown/unresolved outcomes excluded from clean training/eval,
  - no-match positive predictions are FP.
- Matched-pit success rate is diagnostic only; strict operational precision is the fair model-comparison precision.

## Known Limitations
- Race-level metrics for rare events can be support-sensitive.
- MOA vote score is continuous but not calibrated probability by default.
- Threshold policies can strongly change precision/coverage trade-offs.

## Citation / Thesis Reference
Use `thesis/` for the full written document and `thesis/Thesis_bibliography.bib` for references.
