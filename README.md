# Real-time F1 Strategy Operations

Optional project of the [Streaming Data Analytics](https://emanueledellavalle.org/teaching/streaming-data-analytics-2025-26/) course provided by [Politecnico di Milano](https://www11.ceda.polimi.it/schedaincarico/schedaincarico/controller/scheda_pubblica/SchedaPublic.do?&evn_default=evento&c_classe=837284&__pj0=0&__pj1=36cd41e96fcd065c47b49d18e46e3110).

Student: **Pietro Pizzoccheri**

## Project Overview

This project designs and implements a real-time pit wall simulation for Formula 1 strategy operations.
Historical races are replayed as event streams, processed through Apache Kafka and Apache Flink, and transformed into both:

- real-time strategic alerts (for dashboard monitoring), and
- high-quality, labeled JSONL datasets for downstream machine learning.

The project idea and architecture were proposed specifically for this work, with a strong focus on engineering rigor, reproducibility, and domain-aligned strategy logic.

## Background

F1 strategy decisions require continuous reasoning over heterogeneous signals:

- high-frequency telemetry (speed, throttle, brake, gear),
- lap-level timing and tire state,
- global track incidents (yellow/VSC/SC),
- dynamic traffic and rival gaps.

Traditional post-race analysis is mostly offline and manual. This project addresses the gap by building a deterministic stream-processing architecture that can replay races at different speeds while preserving event-time correctness.

## Goals and Objectives

### 1. Event-Time Streaming Architecture (Core Goal)

Build an end-to-end producer-broker-consumer pipeline:

- Producer: Python + fastf1 replay with original inter-event timing.
- Broker: Kafka three-topic decoupling.
- Consumer: Flink 1.20 stateful/event-time processing.

### 2. Context-Aware Race Understanding (Module A)

Implement operators for:

- track status enrichment via broadcast state,
- rival identification per driver/lap,
- DRS train detection from contiguous sub-1s gaps.

### 3. Ground Truth Generation (Module B)

Implement a pit-cycle evaluator that labels historical pit outcomes with strategy-aware logic:

- dual-rival cluster tracking (ahead + behind),
- offset strategy handling,
- track-agnostic normalization via gap delta percentage.

### 4. Real-Time Alerting (Module C)

Implement real-time strategy signals for:

- lift-and-coast detection via CEP,
- tire drop alerts using rolling averages,
- drop-zone emergence position analysis,
- fuzzy-logic pit suggestion scoring with broadcast SC/VSC urgency path.

### 5. ML-Oriented Data Lake (Key Deliverable)

Persist all outputs as JSON Lines for reproducible ML workflows:

- pit evaluations,
- tire drops,
- lift & coast,
- drop zones,
- pit suggestions,
- per-lap ML features,
- consolidated debug alerts.

## Architecture

```
                        Kafka                    Flink 1.20 (Java 17)
                    ┌──────────────┐
 Python Producer    │f1-telemetry  │──► Track Status Enrichment (broadcast state)
 (fastf1 replay)───►│ ~4 Hz/driver │──► Lift & Coast Detection (CEP)
                    ├──────────────┤
                    │f1-laps       │──► Rival Identification ──► DRS Train Detection
                    │~1/80s/driver │──► Drop Zone Evaluator (leader-driven)
                    │              │──► Tire Drop Detector
                    ├──────────────┤──► Pit Stop Evaluator (ground truth)
                    │f1-track-status│──► Pit Strategy Evaluator (broadcast + fuzzy logic)
                    │~1-5/race     │
                    └──────────────┘
                                          │
                          ┌───────────────┼───────────────┐
                          ▼               ▼               ▼
                    FileSink JSONL    KafkaSink      Streamlit Dashboard
                     (data_lake/)     (f1-alerts)     (localhost:8501)

                    Additional ML live path (Phase 3.1):
                    f1-ml-features (Kafka) -> ml consumer -> f1-ml-predictions (Kafka)
```

### Two-Stage Python Producer

1. **prepare_race.py**
   - Extracts telemetry/laps/track status from fastf1.
   - Merges and enriches data (gaps, weather, track-specific pit loss values).
   - Writes a replay-ready Parquet snapshot.

2. **stream_race.py**
   - Replays Parquet rows to Kafka using event-time-aware pacing.
   - Supports `--speed` and `--start-lap` for controlled experiments.

## Flink Modules

### Module A: Context Awareness

- `TrackStatusEnricher`
- `RivalIdentificationFunction`
- `DrsTrainDetector`

### Module B: Ground Truth

- `PitStopEvaluator`

Labels include:

- `SUCCESS_UNDERCUT`
- `SUCCESS_OVERCUT`
- `SUCCESS_DEFEND`
- `SUCCESS_FREE_STOP`
- `OFFSET_ADVANTAGE`
- `OFFSET_DISADVANTAGE`
- `FAILURE_PACE_DEFICIT`
- `FAILURE_TRAFFIC`
- `WEATHER_SURVIVAL_STOP`
- `UNRESOLVED_MISSING_RIVAL`
- `UNRESOLVED_MISSING_PRE_GAP`
- `UNRESOLVED_MISSING_POST_GAP`
- `UNRESOLVED_INVALID_BASELINE`
- `UNRESOLVED_INCIDENT_FILTER`
- `UNRESOLVED_INSUFFICIENT_DATA`

### Module C: Real-Time Alerts

- `LiftCoastDetector`
- `TireDropDetector`
- `DropZoneEvaluator`
- `PitStrategyEvaluator`

## Data Lake Outputs

All streams are persisted as JSONL (`.jsonl`) through Flink `FileSink.forRowFormat` and periodic checkpointing.

| Output | Path | Purpose |
|---|---|---|
| Pit Evaluations | `data_lake/pit_evals/` | Labeled pit outcome ground truth |
| Tire Drops | `data_lake/tire_drops/` | Tire degradation alerts |
| Lift & Coast | `data_lake/lift_coast/` | Fuel-saving pattern detections |
| Drop Zones | `data_lake/drop_zones/` | Physical emergence position analysis |
| Pit Suggestions | `data_lake/pit_suggestions/` | Continuous strategy scoring alerts |
| ML Features | `data_lake/ml_features/` | Denormalized lap-level ML features |
| Debug Alerts | `data_lake/debug_alerts/` | Unified debug stream of all alerts |

## Live ML Topics (Phase 3.1)

The pipeline now exposes a dedicated feature topic for online ML serving.

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `f1-ml-features` | Flink (`RivalIdentificationFunction` side output) | Python ML consumer | Feature payload parity between offline training and live serving |
| `f1-ml-predictions` | Python ML consumer | Dashboard/analysis tools | Real-time prediction stream for heuristic vs ML comparison |

## Data Quality Contracts

The pipeline enforces explicit output contracts to keep downstream ML behavior stable and reproducible.

1. `ml_features.gapAhead` and `ml_features.gapBehind` are exported as non-negative magnitudes.
2. `tire_drops` includes `trackStatus` for context-aware filtering.
3. Pit stops on warm-up laps (`lapNumber <= 2`) are explicitly marked as `UNRESOLVED_MISSING_PRE_GAP` via `EARLY_LAP_FILTER`.
4. Pit-cycle classification uses strict guardrails (`SETTLE_LAPS`, incident thresholding, safety timer) and a conservative pace-shift recovery path (`RIVAL_PIT_PACE_SHIFT`, `SAFETY_TIMER_PACE_SHIFT`) when post-gap is missing; if pace evidence is insufficient, the cycle remains unresolved.
5. Lift/coast timestamp audits use mixed-format ISO-8601 parsing with normalization fallback (`Z`, fractional seconds, and explicit timezone offsets).

## Audit Workflow

The canonical season audit entrypoint is:

1. `season_data_audit.py`: unified quality-contract + stream-health + forensic diagnostics across all six JSONL outputs.

Example run after a full-season replay:

```bash
python season_data_audit.py
python season_data_audit.py --summary-only
python season_data_audit.py --json-out data_lake/audits/season_audit_2023.json
```

Recent validation highlights (latest full 2023 run):

1. Full season coverage: 22/22 races in both `ml_features` and `pit_evals`.
2. Gap contract satisfied: negative gap rows in `ml_features` are 0.
3. `tire_drops` now includes track status context.
4. `EARLY_LAP_FILTER` path is active for opening-lap pit-stop noise suppression.

## Tech Stack

| Component | Version |
|---|---|
| Java | 17 |
| Apache Flink | 1.20.0 |
| flink-connector-kafka | 3.3.0-1.20 |
| Python | 3.10+ |
| fastf1 | latest |
| Apache Kafka | 7.7.1 |
| Jackson | 2.17.2 |

## How to Run

### 1) Freeze Three Seasons (recommended lower-speed replay)

For final data freezing, replay each season at a lower speed to reduce runtime pressure and keep artifacts consistent.
Recommended speed for freeze runs: `50`.

```bash
./simulate_season.sh --year 2022 --speed 50
./simulate_season.sh --year 2023 --speed 50
./simulate_season.sh --year 2024 --speed 50
```

After each run, the season artifacts are written under `data_lake/` with timestamped JSONL files.

### 2) Train the Model (consolidated pipeline)

This command prepares merged training data, runs grouped-CV training and policy selection, exports winner OOF, and builds the serving bundle.

```bash
python ml_pipeline/train_model.py --years 2022 2023 2024 --season-tag season
```

Main artifacts:
1. `data_lake/ml_training_dataset_2022_2024_merged.parquet`
2. `data_lake/reports/ml_ablation_phase31c_2022_2024_merged.csv`
3. `data_lake/reports/ml_oof_winner_2022_2024_merged.csv`
4. `data_lake/models/pit_strategy_serving_bundle.joblib`

### 3) Assess Correctness End-to-End (all methodological gates)

Run unified evaluation to produce Phase B, C, D, F, G, H, and J outputs in one pass:

```bash
python ml_pipeline/evaluate_model.py --years 2022 2023 2024 --season-tag season
```

This checks correctness across all major aspects:
1. Statistical significance and uncertainty (Phase B)
2. Threshold frontier and lookahead diagnostics (Phase C)
3. Calibration and constrained-policy behavior (Phase D)
4. Training-serving parity and point-in-time legality (Phase F)
5. Latency and availability feasibility (Phase G)
6. Integrated GO/HOLD/NO_GO synthesis (Phase H)
7. Split integrity and comparator invariance closure audits (Phase J)

Primary summary artifacts:
1. `data_lake/reports/model_evaluation_2022_2024_merged.csv`
2. `data_lake/reports/model_evaluation_2022_2024_merged.md`
3. `data_lake/reports/phase_h_unified_gate_2022_2024_merged.csv`
4. `data_lake/reports/phase_h_unified_gate_report_2022_2024_merged.txt`

Optional data-quality audit for raw stream outputs:

```bash
python season_data_audit.py
```

### 4) Start Flink + ML Predictions Together

#### Automated single race with live ML inference

```bash
./run_simulation.sh --year 2023 --race "Italian Grand Prix" --session R --speed 100 --start-lap 1 --with-ml-inference
```

#### Automated full season with live ML inference

```bash
./simulate_season.sh --year 2023 --speed 100 --with-ml-inference
```

#### Manual startup (infrastructure + Flink + producer stream + ML service)

```bash
# 1) Start infrastructure including optional ml-inference service
docker compose --profile inference up -d

# 2) Build Flink processor
cd f1-telemetry-processor && mvn clean package -DskipTests && cd ..

# 3) Submit Flink job
docker cp f1-telemetry-processor/target/f1-telemetry-processor-1.0-SNAPSHOT.jar flink-jobmanager:/opt/flink/usrlib/
docker exec flink-jobmanager flink run -d /opt/flink/usrlib/f1-telemetry-processor-1.0-SNAPSHOT.jar

# 4) Prepare and stream a race
python f1-telemetry-producer/src/prepare_race.py --year 2023 --race "Italian Grand Prix" --session R
python f1-telemetry-producer/src/stream_race.py --year 2023 --race "Italian Grand Prix" --session R --speed 100
```

Optional local ML consumer (outside Docker profile):

```bash
python ml_pipeline/serve_model.py --bootstrap localhost:9092 --model-bundle data_lake/models/pit_strategy_serving_bundle.joblib
```

Optional prediction-topic probe:

```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic f1-ml-predictions --from-beginning --max-messages 5
```

Flink UI: `http://localhost:8081`  
Dashboard: `http://localhost:8501`

## Project Structure

```
.
├── docker-compose.yml
├── run_simulation.sh
├── simulate_season.sh
├── dashboard/
│   └── app.py
├── f1-telemetry-producer/
│   └── src/
│       ├── prepare_race.py
│       └── stream_race.py
├── f1-telemetry-processor/
│   └── src/main/java/com/polimi/f1/
│       ├── F1StreamingJob.java
│       ├── model/
│       │   ├── input/
│       │   └── output/
│       ├── operators/
│       │   ├── context/
│       │   ├── groundtruth/
│       │   └── realtime/
│       ├── state/
│       └── utils/
├── data_lake/
├── ml_pipeline/
│   ├── prep_data.py
│   ├── train_model.py
│   ├── evaluate_model.py
│   ├── serve_model.py
│   └── pipeline_config.py
└── report/
    └── main.tex
```
