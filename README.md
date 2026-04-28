# Real-Time F1 Strategy Operations Thesis Repository

Student: **Pietro Pizzoccheri**

## 1) Executive Summary

This repository implements and evaluates an end-to-end real-time Formula 1 pit-strategy pipeline:

1. Event-time race replay (`fastf1` -> Kafka -> Flink).
2. Deterministic strategy/event operators (SDE baseline + labeled outcomes).
3. Batch ML pipeline (dataset preparation, grouped CV training, calibration, policy constraints, serving bundle).
4. Streaming ML baseline (MOA Adaptive Random Forest).
5. Methodological evaluation and thesis-ready synthesis artifacts.

The thesis objective is a defensible comparison among:
- **SDE deterministic baseline**,
- **Batch ML**,
- **Streaming ML (MOA)**,
under fixed comparison invariants:
- horizon `H=2`,
- actionable-only matching,
- one-to-one pit consumption,
- split/leakage integrity checks,
- integrated validity gates.

## 2) Scientific Scope and Evaluation Contract

### Core research question
Under a fixed decision contract, how do SDE, Batch ML, and Streaming ML compare in predictive utility, decision quality, and operational feasibility?

### Non-negotiable protocol constraints
- Comparator contract: `H=2`, actionable-only, one-to-one target consumption.
- Validation rigor: grouped race-level logic and explicit split-integrity audits.
- Deployment rigor: calibration checks, train-serve parity, and runtime feasibility gates.

### Methodological grounding
- Leakage/split validity: Roberts et al. (2017), Brookshire et al. (2024).
- Imbalance-aware precision focus: Elkan (2001), Saito & Rehmsmeier (2015), Davis & Goadrich (2006).
- Comparative significance framing: Dietterich (1998), Walters (2022).
- Calibration validity: Brier (1950), Platt (1999), Guo et al. (2017), Kull et al. (2017).

## 3) Latest Validated Snapshot (April 28, 2026)

From current `data_lake/reports` artifacts:

### Master comparison (2022-2025)
- SDE: precision `0.734314` (`TP=749`, `FP=271`, `scored=1020`).
- ML-pretrain-base: precision `0.942857` (`TP=561`, `FP=34`, `scored=595`).
- ML-pretrain-extended: precision `0.843867` (`TP=3151`, `FP=583`, `scored=3734`).
- ML-racewise-base: precision `0.914242` (`TP=597`, `FP=56`, `scored=653`).
- ML-racewise-extended: precision `0.834143` (`TP=3264`, `FP=649`, `scored=3913`).
- MOA: precision `0.904196` (`TP=1293`, `FP=137`, `scored=1430`).


Canonical source files:
- `data_lake/reports/model_evaluation_2022_2025_merged.csv`
- `data_lake/reports/thesis_master_results_2022_2025.md`

## 4) Repository Architecture

```text
.
├── run_simulation.sh                  # single-race full stack runner (Docker)
├── simulate_season.sh                 # full-season replay runner (Docker)
├── season_data_audit.py               # raw stream-level quality audit
├── f1-telemetry-producer/
│   └── src/
│       ├── prepare_race.py            # stage-1 fastf1 extraction/enrichment
│       └── stream_race.py             # stage-2 event-time replay to Kafka
├── f1-telemetry-processor/            # Flink Java operators
├── ml_pipeline/
│   ├── prep_data.py
│   ├── train_model.py
│   ├── export_moa_dataset.py
│   ├── run_moa_arf.py
│   ├── build_three_way_comparator.py
│   ├── evaluate_model.py
│   ├── build_thesis_synthesis.py
│   ├── generate_thesis_master_results.py
│   ├── explain_shap.py
│   ├── explain_moa_shap_proxy.py
│   ├── explain_moa_temporal_permutation.py
│   ├── plot_temporal_dynamics.py
│   ├── plot_trust_diagnostics.py
│   ├── serve_model.py
│   └── lib/                           # granular comparator/audit/evaluation modules
└── data_lake/                         # generated artifacts and reports
```

## 5) Environment and Setup

### Python environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Java/Flink/Kafka stack
Use Docker Compose for infrastructure and Flink job lifecycle.

```bash
docker compose up -d
```

If you need MOA baseline runs, place the jar at:
- `data_lake/tools/moa.jar`

Example:
```bash
mkdir -p data_lake/tools
curl -fL https://repo1.maven.org/maven2/nz/ac/waikato/cms/moa/moa/2024.07.0/moa-2024.07.0.jar -o data_lake/tools/moa.jar
```

## 6) Quickstart by Objective

### A) Freeze streaming data from race replay
Single race:
```bash
./run_simulation.sh --year 2023 --race "Italian Grand Prix" --session R --speed 100 --start-lap 1
```

Full season(s):
```bash
./simulate_season.sh --speed 50

### B) Build dataset + train Batch ML + serving bundle
```bash
python ml_pipeline/train_model.py --years 2022 2023 2024 2025 --season-tag season
```

Main outputs:
- `data_lake/ml_training_dataset_2022_2025_merged.parquet`
- `data_lake/reports/ml_ablation_phase31c_2022_2025_merged.csv`
- `data_lake/reports/ml_oof_winner_2022_2025_merged.csv`
- `data_lake/models/pit_strategy_serving_bundle.joblib`

### C) Run streaming ML baseline (MOA ARF)
```bash
python ml_pipeline/export_moa_dataset.py --years 2022 2023 2024 2025 --season-tag season --skip-prepare-data
python ml_pipeline/run_moa_arf.py --years 2022 2023 2024 2025 --season-tag season
python ml_pipeline/build_three_way_comparator.py --years 2022 2023 2024 2025 --season-tag season
```

### D) Run integrated methodological evaluation
```bash
python ml_pipeline/evaluate_model.py --years 2022 2023 2024 2025 --season-tag merged
```

Primary outputs:
- `data_lake/reports/model_evaluation_2022_2025_merged.csv`
- `data_lake/reports/model_evaluation_2022_2025_merged.md`
- `data_lake/reports/integrated_gate_2022_2025_merged.csv`
- `data_lake/reports/integrated_gate_report_2022_2025_merged.txt`

### E) Generate thesis synthesis reports
```bash
python ml_pipeline/build_thesis_synthesis.py --suffix 2022_2025_merged
python ml_pipeline/generate_thesis_master_results.py --suffix 2022_2025_merged --racewise-suffix 2022_2025_racewise
```

Primary outputs:
- `data_lake/reports/thesis_synthesis_2022_2025_merged.csv`
- `data_lake/reports/thesis_synthesis_checks_2022_2025_merged.csv`
- `data_lake/reports/thesis_synthesis_2022_2025_merged.md`
- `data_lake/reports/thesis_master_results_2022_2025.md`

### F) Generate explainability + figures
```bash
python ml_pipeline/explain_shap.py
python ml_pipeline/explain_moa_shap_proxy.py --years 2022 2023 2024 2025 --season-tag merged --moa-dataset-csv data_lake/reports/moa_dataset_2022_2025_merged.csv --moa-predictions data_lake/reports/moa_arf_predictions_2022_2025_merged.pred --reports-dir data_lake/reports
python ml_pipeline/explain_moa_temporal_permutation.py --years 2022 2023 2024 2025 --season-tag season
python ml_pipeline/plot_temporal_dynamics.py
python ml_pipeline/plot_trust_diagnostics.py
```

### G) Run live ML consumer on Kafka stream
```bash
python ml_pipeline/serve_model.py --bootstrap localhost:9092 --model-bundle data_lake/models/pit_strategy_serving_bundle.joblib
```

## 7) Tool-by-Tool Runbook

This is the practical reference for all major tools in the repository.

### 7.1 Infrastructure and replay tools

| Tool | Purpose | Typical command | Main outputs |
|---|---|---|---|
| `run_simulation.sh` | End-to-end single-race run (build stack, submit Flink, replay race). | `./run_simulation.sh --year 2023 --race "Italian Grand Prix" --speed 100` | JSONL sinks in `data_lake/` + Kafka topics |
| `simulate_season.sh` | Bulk replay for one or multiple full seasons. | `./simulate_season.sh --year 2024 --speed 50` | Season-level merged JSONL artifacts |
| `season_data_audit.py` | Data-contract and stream-health audit on raw JSONL outputs. | `python season_data_audit.py --year 2023` | Audit summary (console, optional JSON) |
| `f1-telemetry-producer/src/prepare_race.py` | Build enriched replay-ready parquet snapshot. | `python .../prepare_race.py --year 2023 --race "Italian Grand Prix" --session R` | Prepared parquet in `data/` |
| `f1-telemetry-producer/src/stream_race.py` | Event-time replay of prepared race into Kafka. | `python .../stream_race.py --year 2023 --race "Italian Grand Prix" --session R --speed 100` | Kafka events |

### 7.2 ML pipeline tools (top-level)

| Tool | Role | Command skeleton | Main artifacts |
|---|---|---|---|
| `ml_pipeline/prep_data.py` | Build leakage-safe training dataset from JSONL streams. | `python ml_pipeline/prep_data.py --years ... --season-tag ...` | `ml_training_dataset_*.parquet` |
| `ml_pipeline/train_model.py` | Train grouped-CV batch model, policy selection, winner OOF export. | `python ml_pipeline/train_model.py --years ... --season-tag ...` | `ml_ablation_*.csv`, `ml_oof_winner_*.csv`, serving bundle |
| `ml_pipeline/export_moa_dataset.py` | Export MOA-ready matrix + schema contract. | `python ml_pipeline/export_moa_dataset.py --years ... --season-tag ...` | `moa_dataset_*.csv/.arff/.json` |
| `ml_pipeline/run_moa_arf.py` | Execute MOA ARF prequential baseline. | `python ml_pipeline/run_moa_arf.py --years ... --season-tag ...` | `moa_arf_*` reports |
| `ml_pipeline/build_three_way_comparator.py` | Build compact SDE vs Batch vs MOA comparison. | `python ml_pipeline/build_three_way_comparator.py --years ... --season-tag ...` | `three_way_comparator_*.csv/.md` |
| `ml_pipeline/evaluate_model.py` | Unified evaluation orchestration (significance, threshold, calibration, parity, runtime, integrated gate, closure audits). | `python ml_pipeline/evaluate_model.py --years ... --season-tag ...` | `model_evaluation_*.csv/.md` + all gate artifacts |
| `ml_pipeline/build_thesis_synthesis.py` | Build thesis-level claim matrix + correctness checks. | `python ml_pipeline/build_thesis_synthesis.py --suffix ...` | `thesis_synthesis_*.csv/.md`, checks csv |
| `ml_pipeline/generate_thesis_master_results.py` | Build comprehensive thesis master markdown from current artifacts. | `python ml_pipeline/generate_thesis_master_results.py --suffix ... --racewise-suffix ...` | `thesis_master_results_*.md` |
| `ml_pipeline/explain_shap.py` | Batch model TreeSHAP artifacts. | `python ml_pipeline/explain_shap.py` | `shap_*` plots/tables |
| `ml_pipeline/explain_moa_shap_proxy.py` | Surrogate SHAP for MOA predictions. | `python ml_pipeline/explain_moa_shap_proxy.py --years ...` | `moa_shap_proxy_*` plots/tables |
| `ml_pipeline/explain_moa_temporal_permutation.py` | Temporal permutation explainability for MOA. | `python ml_pipeline/explain_moa_temporal_permutation.py --years ...` | `moa_temporal_permutation_*` artifacts |
| `ml_pipeline/plot_temporal_dynamics.py` | Temporal accuracy/kappa and comparator dynamics plots. | `python ml_pipeline/plot_temporal_dynamics.py` | `paper_fig0..` time-series figures |
| `ml_pipeline/plot_trust_diagnostics.py` | Calibration + latency trust diagnostics plots. | `python ml_pipeline/plot_trust_diagnostics.py` | `paper_fig2..paper_fig5` |
| `ml_pipeline/serve_model.py` | Live Kafka inference consumer for online predictions. | `python ml_pipeline/serve_model.py --bootstrap localhost:9092 --model-bundle ...` | `f1-ml-predictions` topic payloads |

### 7.3 Advanced granular evaluation tools (`ml_pipeline/lib`)

Use these directly when you need only one methodological block instead of the full `evaluate_model.py` orchestration.

| Tool | Focus |
|---|---|
| `evaluate_significance.py` | SDE vs ML significance tests |
| `report_sde_ml_comparison.py` | dedicated SDE/ML meeting markdown + summary tables |
| `evaluate_threshold_frontier.py` | threshold sweep + selected threshold comparator |
| `evaluate_calibration_policy.py` | reliability + constrained-policy diagnostics |
| `evaluate_feature_parity.py` | train-serve schema/PIT/parity audits |
| `evaluate_live_latency.py` | replay latency/availability/overhead audit |
| `evaluate_integrated_gate.py` | integrated GO/HOLD/NO_GO synthesis |
| `audit_split_integrity.py` | split protocol and OOF integrity closure checks |
| `audit_comparator_invariance.py` | comparator fairness/invariance closure checks |
| `comparator_heuristic.py`, `comparator_ml.py`, `comparator_moa.py` | comparator construction under fixed contract |

## 8) Artifact Navigation (Where to Look for What)

| Question | Primary artifact |
|---|---|
| Final thesis master table | `data_lake/reports/thesis_master_results_2022_2025.md` |
| Unified pass/fail methodological status | `data_lake/reports/model_evaluation_2022_2025_merged.csv` |
| Integrated deployment decision | `data_lake/reports/integrated_gate_2022_2025_merged.csv` |
| Statistical significance details | `data_lake/reports/significance_summary_2022_2025_merged.csv`, `.../significance_tests_2022_2025_merged.csv` |
| Threshold trade-off frontier | `data_lake/reports/threshold_frontier_2022_2025_merged.csv` |
| Calibration + constrained policy | `data_lake/reports/calibration_policy_summary_2022_2025_merged.csv` |
| Train-serve feature parity | `data_lake/reports/feature_parity_summary_2022_2025_merged.csv` |
| Runtime feasibility | `data_lake/reports/live_latency_summary_2022_2025_merged.csv` |
| Split integrity closure audit | `data_lake/reports/split_integrity_summary_2022_2025_merged.csv` |
| Comparator invariance closure audit | `data_lake/reports/comparator_invariance_summary_2022_2025_merged.csv` |
| Batch SHAP explanations | `data_lake/reports/shap_summary.csv`, `shap_feature_importance.csv`, `shap_*.png` |
| MOA proxy explainability | `data_lake/reports/moa_shap_proxy_summary.csv`, `moa_temporal_permutation_summary.csv` |

## 9) Troubleshooting

### `ModuleNotFoundError: pandas` (or similar)
Ensure virtualenv is active before running tools:
```bash
source .venv/bin/activate
python <script>.py ...
```

### `NoBrokersAvailable` in `serve_model.py`
- Host shell: use `--bootstrap localhost:9092`.
- Container-to-container traffic: `kafka:29092`.

### MOA run fails (`moa.jar` missing)
Ensure:
- `data_lake/tools/moa.jar` exists,
- Java is available in your runtime path (or use containerized flow).

### Integrated gate returns `NO_GO`
Check first:
- `data_lake/reports/model_evaluation_2022_2025_merged.csv`
- failing rows (`status=FAIL`) and linked artifact paths.

## 10) UI Endpoints

- Flink UI: `http://localhost:8081`
- Dashboard: `http://localhost:8501`

## 11) Citation Anchors for Thesis Writing

Recommended anchors for methods section:
- Roberts et al. (2017), Brookshire et al. (2024) for temporal/leakage-safe split protocol.
- Elkan (2001), Saito & Rehmsmeier (2015), Davis & Goadrich (2006) for imbalance-aware precision-first evaluation.
- Dietterich (1998), Walters (2022) for comparative test rigor.
- Brier (1950), Platt (1999), Guo et al. (2017), Kull et al. (2017) for probability calibration interpretation.
