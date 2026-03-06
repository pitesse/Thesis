# 2025-2026 Real-time F1 Strategy Operations

Optional project of the [Streaming Data Analytics](https://emanueledellavalle.org/teaching/streaming-data-analytics-2025-26/) course provided by [Politecnico di Milano](https://www11.ceda.polimi.it/schedaincarico/schedaincarico/controller/scheda_pubblica/SchedaPublic.do?&evn_default=evento&c_classe=837284&__pj0=0&__pj1=36cd41e96fcd065c47b49d18e46e3110).

Student: **Pietro Pizzoccheri**

## Background

This project focuses on real-time F1 monitoring to simulate pit wall operations. Using Python, Kafka, and Apache Flink, it processes telemetry data to create a "Ground Truth" for strategy evaluation and a "Strategic Baseline". The system identifies track status, tire wear, and pit windows to provide logic-based alerts.

## Goals and objectives

The project's primary aim is to develop a real-time monitoring system for Formula 1 that simulates pit wall operations. This system serves as a technical foundation for a future thesis through two main objectives:

* **Ground Truth Generation (Ex-Post):** Automatically analyzing historical race data to classify whether specific strategies, such as pit stops, were successful or not.
* **Strategic Baseline (Real-Time):** Issuing live strategic alerts based on fixed "Crisp Logic" rules. These alerts act as a benchmark for comparison against future Machine Learning models.

The implementation involves three modules of increasing complexity:

1. **Module A - Context Awareness:**
    * **Track Status:** Real-time detection of global track status (Green, Yellow, VSC, Safety Car) to dynamically adapt delta time calculations.
    * **Direct Rival Identification:** Identifying the car immediately ahead and behind, filtering out lapped cars to focus on actual race position.
    * **"DRS Train" Detection:** Identifying if the driver is stuck in a group of cars (Gap < 1s) to determine pushing potential.

2. **Module B - Historical Analysis (Ground Truth):**
    * **Pit Stop Evaluation:** Analyzing sector times post-PitExit to classify the strategy.
    * **Classification:**
        * *Success:* The driver gained a position (Undercut) or successfully defended one.
        * *Failure:* The driver re-entered in traffic or lost track position.

3. **Module C - Real-Time Alerts (Strategic Baseline):**
    * **"Open Box Window":** Triggered if the gap to the car behind is sufficient for a pit stop without losing position. The time threshold adapts dynamically based on VSC/SC status (from Module A).
    * **"Tire Drop":** Triggered if the rolling average of the last 3 laps is worse than the stint's Best Lap by a specific threshold ($X$ seconds).
    * **"Fuel Saving" / Lift & Coast:** Detection of fuel-saving maneuvers by analyzing the sequence of pedal inputs (throttle release before braking).

## Architecture

The system follows a multi-topic producer-consumer architecture with Apache Kafka as the decoupling layer. Three independent Kafka topics carry data at different granularities, enabling Flink to apply appropriate watermark strategies and processing semantics to each stream.

```
┌──────────────────────────┐       ┌─────────────────┐       ┌──────────────────────────────────┐
│  Python Producer         │       │      Kafka      │       │   Apache Flink (Java)            │
│  (fastf1 replay)         │       │                 │       │                                  │
│                          │──────>│ f1-telemetry    │──────>│ Module C: Lift & Coast (CEP)     │
│  - car telemetry (~4 Hz) │  JSON │ (~4 Hz/driver)  │       │                                  │
│  - DRS status            │       │                 │       │ Module A: Track Status Enrichment│
│                          │       ├─────────────────┤       │   (broadcast state)              │
│  - lap completions       │──────>│ f1-laps         │──────>│                                  │
│  - sector times, tires   │       │ (~1/80s/driver) │       │ Module A: Rival ID + DRS Train   │
│  - position, gap         │       │                 │       │   (session windows)              │
│                          │       ├─────────────────┤       │                                  │
│  - track status changes  │──────>│ f1-track-status │──────>│ Module C: Tire Drop, Pit Window  │
│  - SC/VSC/green flags    │       │ (~1-5/race)     │       │   (keyed state + broadcast)      │
│                          │       │                 │       │                                  │
│  --speed 50 --start-lap  │       └─────────────────┘       │ Module B: Pit Stop Evaluation    │
│    (replay control)      │                                 │   (keyed process function)       │
└──────────────────────────┘                                 └──────────────────────────────────┘
```

### Data Flow

The Python producer loads a historical race session from the fastf1 library and constructs three event streams:

1. **Telemetry** (`f1-telemetry`): High-frequency car data (~4 Hz per driver) including speed, RPM, throttle, brake, gear, DRS, and GPS coordinates. Telemetry and position data are merged via a temporal asof-join with 100ms tolerance. Each row is assigned a `LapNumber` through a backward asof-join against lap start dates.

2. **Lap Events** (`f1-laps`): One event per completed lap per driver, containing lap time, sector times, tire compound, stint number, tyre life, position, pit in/out times, and the computed gap to the car ahead. The gap is approximated from cumulative race time differences between adjacent-position drivers.

3. **Track Status** (`f1-track-status`): Global track status changes (Green, Yellow, VSC, SC, Red) with absolute timestamps derived from session start time plus the event's relative timedelta.

All three streams are interleaved chronologically and replayed with real $\Delta t$ between events (configurable via `--speed` multiplier). The producer supports `--start-lap` to skip ahead in the replay for faster development iteration.

### Why Three Topics

The multi-topic architecture is motivated by three Flink-specific constraints:

- **Broadcast state requires a separate stream.** Track status is a global dimension (not keyed by driver) that must be broadcast to all parallel telemetry/lap processing instances. Flink's `BroadcastProcessFunction` requires the broadcast data to originate from a distinct `DataStream`, which maps cleanly to a dedicated Kafka topic.

- **Different watermark semantics.** Telemetry is dense and continuous (5-second bounded out-of-orderness). Track status events are sparse (minutes apart between status changes). If they shared a topic, watermarks would either stall waiting for the next track status event or advance too aggressively. The track status source uses `withIdleness(30s)` to avoid blocking the global watermark.

- **Different keying requirements.** Telemetry and laps are keyed by `Driver`. Track status is global. Mixing them in one topic would require a union type with routing logic, adding complexity without benefit.

### Infrastructure

Kafka (single-broker, Zookeeper-based) and Flink (1 JobManager + 1 TaskManager with 6 task slots) run as Docker containers via `docker-compose.yml`. The Python producer runs on the host, connecting to Kafka at `localhost:9092` (EXTERNAL listener). The Flink job runs inside the cluster, connecting to Kafka at `kafka:29092` (INTERNAL listener).

### ML Dataset Generation (FileSink)

In addition to the real-time print sinks used during development, the pipeline persists two key streams to disk as CSV files using Flink's `FileSink.forRowFormat` API. A custom `CsvHeaderMapper` emits a CSV header on the first record, followed by comma-separated data rows produced by each POJO's `toCsvRow()` method. This serves the thesis goal of constructing a labeled ground truth dataset for machine learning model training.

**Persisted Streams:**
| Stream | Output Path | Content |
|--------|-------------|---------|
| `PitStopEvaluation` | `/opt/flink/data_lake/pit_evals/` | Labeled pit stop outcomes with ML context features (track status, tyre age, gap) |
| `TireDropAlert` | `/opt/flink/data_lake/tire_drops/` | Tire degradation alerts with rolling averages and deltas |

**Rolling Policy:** Files roll every 5 minutes, after 3 minutes of inactivity, or upon reaching 128 MB, whichever occurs first. The inactivity interval ensures files are finalized promptly during low-traffic periods (e.g., between pit stop events), avoiding indefinitely open part files.

**Volume Mapping:** The TaskManager container mounts `./data_lake:/opt/flink/data_lake`, so all output files appear directly on the host filesystem under the `data_lake/` project directory. This enables direct consumption by the ML training pipeline (`ml_pipeline/train_pit_strategy.py`) without requiring container access.

### ML Proof-of-Concept

The `ml_pipeline/` directory contains a scikit-learn pipeline that trains a Random Forest binary classifier on the pit stop ground truth CSVs. It demonstrates the end-to-end loop: Flink generates labeled data, the ML pipeline consumes it.

- **Leakage prevention:** `postPitPosition`, `pitLapNumber`, and `driver` are dropped before training.
- **Target:** `SUCCESS_UNDERCUT` + `SUCCESS_DEFEND` = 1 (Success), `FAILURE_LOST_POSITION` = 0 (Failure).
- **Output:** Feature importance plot, confusion matrix, serialized model (`.pkl`).

A season simulator (`simulate_season.sh`) replays 12 races through the running Flink cluster to bulk-generate training data for the ML pipeline.

## Flink Processing Modules

### Module A: Context Awareness

**Track Status Enrichment** (`TrackStatusEnricher`): Uses Flink's broadcast state pattern to join the sparse track status stream with the high-frequency telemetry stream. Each parallel telemetry instance maintains a local copy of the current track status via a `MapStateDescriptor`. Defaults to "1" (green) if no status change has been received. This enrichment is a prerequisite for the pit window alert, which adapts its threshold based on track status.

**Rival Identification** (`RivalIdentificationFunction`): Collects all lap events for the same lap number in a session window (30-second gap), sorts by position, and identifies the car directly ahead and behind each driver. Emits `RivalInfo` events containing gap-to-car-ahead and gap-to-car-behind. The gap data originates from the Python producer's cumulative race time computation.

**DRS Train Detection** (`DrsTrainDetector`): Operates on the `RivalInfo` output, re-windowed by lap number. Scans for contiguous groups of drivers where consecutive gaps are below 1 second. A group of 2+ cars within DRS range is classified as a DRS train.

### Module B: Ground Truth (Historical Analysis)

**Pit Stop Evaluation** (`PitStopEvaluator`): Keyed process function that tracks each driver's pit stop lifecycle:
1. Detects pit entry (lap with `pitInTime != null`), records pre-pit position.
2. Skips the out-lap (unrepresentative times due to pit lane speed and cold tires).
3. Collects 3 post-pit clean laps.
4. Classifies the outcome:
   - `SUCCESS_UNDERCUT`: post-pit position improved (gained places)
   - `SUCCESS_DEFEND`: position maintained
   - `FAILURE_LOST_POSITION`: position worsened

A 10-minute event-time timer acts as a safety net to clear stale state if post-pit laps never arrive (e.g., due to retirement).

### Module C: Real-Time Alerts

**Lift & Coast Detection** (CEP): Flink CEP pattern matching a pedal input sequence: full throttle (100%) → throttle release (0%) → braking (>0), all within a 2-second window. The `followedBy` quantifier (not `next`) allows non-matching events between steps, which is essential for real telemetry where intermediate throttle values and gear changes occur between pedal transitions.

**Tire Drop Detection** (`TireDropDetector`): Keyed process function maintaining per-driver state: current stint number, stint best lap time, and a rolling buffer of the last 3 clean lap times. Alerts when the rolling average exceeds the stint best by more than 1.5 seconds. State resets on stint change (pit stop). Pit in-laps and out-laps are excluded from the calculation.

**Open Box Window** (`PitWindowDetector`): Broadcast process function combining rival gap data with track status. The pit window gap threshold adapts dynamically:
| Track Status | Threshold | Rationale |
|---|---|---|
| Green (1) | 25.0s | Typical pit stop time loss |
| VSC (6/7) | 15.0s | Reduced pit lane delta under VSC |
| SC (4) | 5.0s | Field bunches up, minimal time loss |
| Yellow/Red | Suppressed | Pit lane may be closed |

## Tech Stack

| Component | Version | Notes |
|-----------|---------|-------|
| Java | 17 | Strict requirement |
| Apache Flink | 1.20.0 | Streaming runtime + CEP library |
| flink-connector-kafka | 3.3.0-1.20 | Modern `KafkaSource` builder API |
| flink-connector-files | 1.20.0 | `FileSink` for CSV dataset persistence |
| Python | 3.10+ | Producer runtime |
| fastf1 | latest | Historical F1 telemetry source |
| Apache Kafka | 7.7.1 (Confluent) | Single-broker, Dockerized |
| Jackson | 2.17.2 | JSON deserialization (`jackson-databind` + `jackson-datatype-jsr310`) |

## Datasets

The dataset is sourced from the [**OpenF1 API**](https://openf1.org/) via the [**fastf1** Python library](https://github.com/theOehrly/Fast-F1). It includes:

* **Telemetry:** Speed, RPM, Throttle/Brake, DRS status, gear.
* **Positioning:** GPS-style coordinates (X, Y, Z).
* **Timing & Session:** Lap times, sector times, tire compound, stint data, pit events, track status.

To simulate real-time conditions, the producer reads this historical data and injects it into Kafka using a replay method that respects original time intervals. The `--speed` multiplier and `--start-lap` options allow fast iteration during development.

## Methodologies and Evaluation metrics

The project utilizes a stream processing methodology focused on **Complex Event Processing (CEP)** to transform raw telemetry into strategic insights.

### Core Processing Methodology

* **Complex Event Processing (CEP):** The system uses **Apache Flink (CEP Library)** to run EPL (Event Processing Language) queries on the incoming data stream.
* **Real-Time Simulation (Replay):** A Python-based Producer reads historical data and re-injects it into **Apache Kafka**, maintaining the original time intervals ($\Delta t$) to simulate live race conditions.

### Logical Models

* **Crisp Logic (Fixed Rules):** Instead of black-box AI, the system uses fixed logical rules to trigger alerts. This serves as a "Baseline" to evaluate future Machine Learning models.
* **Dynamic Thresholding:** Calculations for pit stop windows are not static; they adapt dynamically based on the **Track Status** (e.g., VSC or Safety Car conditions).

### Operational Modules Details

* **Contextual Awareness:** Models used to filter out lapped cars and identify direct rivals.
* **Performance Metrics:**
    * *Tire Degradation:* A rolling average model comparing the last 3 laps against the stint's best lap.
    * *Fuel Strategy:* A "Lift & Coast" detection model that analyzes the sequence of pedal inputs.
    * *Success Classification:* An ex-post classification model that evaluates pit stop efficacy (Undercut success) based on subsequent sector times and track position.

## Deliverable

* Source code (Python Producer + Flink Job).
* System logs showing generated alerts synchronized with real race events.
* Detailed technical report on latency management and rule correctness.
* Instructions on how to reproduce the experiment (`requirements.txt` and setup guide).

## How to Run

```bash
# 1) start the infrastructure (kafka + flink)
docker-compose up -d

# 2) build the flink processor fat jar
cd f1-telemetry-processor
mvn clean package
cd ..

# 3) submit the flink job to the cluster
sudo docker cp f1-telemetry-processor/target/f1-telemetry-processor-1.0-SNAPSHOT.jar flink-jobmanager:/tmp/f1-job.jar
sudo docker exec flink-jobmanager flink run /tmp/f1-job.jar

# 4) start the python producer (in a separate terminal)
cd f1-telemetry-producer
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/main.py

# fast replay for development (50x speed, start from lap 30)
python src/main.py --speed 50 --start-lap 30
```

The Flink Web UI is available at `http://localhost:8081` for monitoring job status and inspecting task metrics. Alerts appear in the TaskManager stdout logs: `docker logs -f flink-taskmanager`.

### Live Dashboard

A Streamlit-based dashboard provides a real-time visual overlay on the race simulation, acting as the visual counterpart to the Flink processing pipeline. It subscribes to three Kafka topics: `f1-laps` and `f1-track-status` for raw race data, and `f1-alerts` for Flink-generated strategic alerts routed through a `KafkaSink`.

```bash
# in a separate terminal, while run_simulation.sh is running:
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

The dashboard renders a two-column layout:
- **Track Status Banner:** Current global flag status (Green, Yellow, VSC, SC, Red) with color-coded indicator.
- **Leaderboard Table:** Live-updating driver standings with position, lap time, tire compound, tyre life, and gap to car ahead.
- **Live Strategic Alerts:** Real-time feed of Flink-generated alerts (Tire Drop, Pit Window, Lift & Coast) classified by JSON field inspection.
- **Gap to Leader Evolution:** Line chart tracking the cumulative gap to the race leader for the top 3 drivers, rebuilt per-lap from individual gap-to-car-ahead values.
- **Tire Age Tracker:** Horizontal bar chart showing tire life (laps) for the top 5 drivers.

## Project Structure

```
├── docker-compose.yml                          # kafka + flink infrastructure (6 task slots)
├── run_simulation.sh                           # one-command automation: build, deploy, launch dashboard, run producer
├── simulate_season.sh                          # bulk data generation: replays 12 races for ML training
├── data_lake/                                  # flink FileSink output (git-ignored, created at runtime)
│   ├── pit_evals/                              # csv: pit stop success/failure classifications with ml features
│   └── tire_drops/                             # csv: tire degradation alerts
├── dashboard/                                  # live race monitor (python, streamlit)
│   ├── app.py                                  # streamlit app: kafka consumer + two-column pit wall ui
│   └── requirements.txt                        # streamlit, kafka-python, pandas
├── ml_pipeline/                                # ml proof-of-concept (python, scikit-learn)
│   ├── train_pit_strategy.py                   # random forest classifier on pit stop ground truth
│   └── requirements.txt                        # pandas, scikit-learn, matplotlib, seaborn, ipykernel
├── report/                                     # technical report (latex, ieee format)
│   └── main.tex                                # latency management, rule correctness, validation
├── presentation/                               # defense slides (latex, beamer)
│   └── slides.tex                              # 10-slide presentation with speaker notes
├── f1-telemetry-processor/                     # flink stream consumer (java)
│   ├── pom.xml                                 # maven build (flink 1.20, kafka connector, cep, jackson)
│   └── src/main/java/com/polimi/f1/
│       ├── F1StreamingJob.java                 # entry point — kafka sources, watermarks, job graph wiring
│       ├── events/
│       │   ├── TelemetryEvent.java             # pojo: car telemetry + drs + track status (enriched)
│       │   ├── LapEvent.java                   # pojo: lap completion with timing, tire, position, gap
│       │   └── TrackStatusEvent.java           # pojo: global track status change (green/vsc/sc)
│       ├── context/                            # module a: context awareness
│       │   ├── TrackStatusEnricher.java        # broadcast state: enriches telemetry with track status
│       │   ├── RivalIdentificationFunction.java # session window: identifies car ahead/behind per lap
│       │   └── DrsTrainDetector.java           # detects groups of cars within 1s drs range
│       ├── alerts/                             # module c: real-time alerts
│       │   ├── TireDropDetector.java           # keyed state: rolling avg vs stint best (1.5s threshold)
│       │   └── PitWindowDetector.java          # broadcast + gap: dynamic pit window (green/vsc/sc)
│       ├── groundtruth/                        # module b: historical analysis
│       │   └── PitStopEvaluator.java           # keyed state: classifies pit stops as success/failure
│       └── model/                              # output event types
│           ├── RivalInfo.java                  # driver ahead/behind with gaps
│           ├── TireDropAlert.java              # tire degradation alert (csv-serializable)
│           ├── PitWindowAlert.java             # open box window alert
│           ├── LiftCoastAlert.java             # lift & coast detection alert
│           └── PitStopEvaluation.java          # pit stop success/failure classification (csv-serializable)
└── f1-telemetry-producer/                      # kafka producer (python)
    ├── requirements.txt                        # fastf1, kafka-python, pandas, numpy
    └── src/
        └── main.py                             # loads fastf1 session, full 20-car grid replay with --speed/--start-lap
```
