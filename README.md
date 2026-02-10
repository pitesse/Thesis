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

## Datasets

The dataset is sourced from the [**OpenF1 API**](https://openf1.org/) via the [**fastf1** Python library](https://github.com/theOehrly/Fast-F1). It includes:

* **Telemetry:** Speed, RPM, and Throttle/Brake data.
* **Positioning:** GPS-style coordinates (X, Y, Z).
* **Timing & Session:** Lap times, weather conditions, and session status.

To simulate real-time conditions, a **Producer** reads this historical data and injects it into **Apache Kafka** using a "Replay" method that respects original time intervals.

## Methodologies and Evaluation metrics

The project utilizes a stream processing methodology focused on **Complex Event Processing (CEP)** to transform raw telemetry into strategic insights.

Here are the specific methodologies and models applied:

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