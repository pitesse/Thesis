# 2025-2026 Real-time F1 Strategy Operations

Optional project of the [Streaming Data Analytics](https://emanueledellavalle.org/teaching/streaming-data-analytics-2025-26/) course provided by [Politecnico di Milano](https://www11.ceda.polimi.it/schedaincarico/schedaincarico/controller/scheda_pubblica/SchedaPublic.do?&evn_default=evento&c_classe=837284&__pj0=0&__pj1=36cd41e96fcd065c47b49d18e46e3110).

Student: **Pietro Pizzoccheri**

## Background

This project focuses on real-time F1 monitoring to simulate pit wall operations. Using Python, Kafka, and Flink, it processes telemetry to create a "Ground Truth" for strategy evaluation and a "Strategic Baseline". The system identifies track status, tire wear, and pit windows to provide logic-based alerts.

## Goals and objectives

The project's primary aim is to develop a real-time monitoring system for Formula 1 that simulates pit wall operations. This system serves as a technical foundation for a future thesis through two main objectives:

* **Ground Truth Generation (Ex-Post):** Automatically analyzing historical race data to classify whether specific strategies, such as pit stops, were successful or not.
* **Strategic Baseline (Real-Time):** Issuing live strategic alerts based on fixed "Crisp Logic" rules. These alerts act as a benchmark for comparison against future Machine Learning models.

The implementation involves three modules of increasing complexity:
1. **Context Awareness:** Monitoring track status (Green, Yellow, VSC, SC), identifying direct rivals, and detecting "DRS trains" where gaps are less than 1s.
2. **Historical Analysis:** Evaluating past pit stops to determine success (e.g., a successful Undercut) or failure based on post-exit performance.
3. **Real-Time Alerts:** Triggering notifications for "Open Box Windows," tire performance drops, and fuel-saving maneuvers like "Lift & Coast".

## Datasets

The dataset is sourced from the [**OpenF1 API**](https://openf1.org/) via the [**fastf1** Python library](https://github.com/theOehrly/Fast-F1). It includes:

* **Telemetry:** Speed, RPM, and Throttle data.
* **Positioning:** GPS-style coordinates (X, Y, Z).
* **Timing & Session:** Lap times, weather conditions, and session status.

To simulate real-time conditions, a Producer reads this historical data and injects it into **Apache Kafka** using a "Replay" method that respects original time intervals.

## Methodologies and Evaluation metrics

The project utilizes a stream processing methodology focused on **Complex Event Processing (CEP)** to transform raw telemetry into strategic insights.

Here are the specific methodologies and models applied:

### Core Processing Methodology

* **Complex Event Processing (CEP):** The system uses **Apache Flink** or **Esper** to run EPL (Event Processing Language) queries on the incoming data stream.
* **Real-Time Simulation (Replay):** A Python-based Producer reads historical data and re-injects it into **Apache Kafka**, maintaining the original time intervals ( t) to simulate live race conditions.

### Logical Models
* **Crisp Logic (Fixed Rules):** Instead of black-box AI, the system currently uses fixed logical rules to trigger alerts. This serves as a "Baseline" to evaluate future Machine Learning models.
* **Dynamic Thresholding:** Calculations for pit stop windows are not static; they adapt dynamically based on the **Track Status** (e.g., VSC or Safety Car conditions).

### Operational Modules

* **Contextual Awareness:** Models used to identify "DRS Trains" (gaps ) and filter out lapped cars to find direct rivals.
* **Performance Metrics:**
  * **Tire Degradation:** A rolling average model comparing the last 3 laps against the stint's best lap.
  * **Fuel Strategy:** A "Lift & Coast" detection model that analyzes the sequence of pedal inputs (throttle/brake).
  * **Success Classification:** An ex-post classification model that evaluates pit stop efficacy (Undercut success) based on subsequent sector times and track position.

## Deliverable 

* source code
* results of the analyses
* detailed documentation about how to reproce the experiment

## Note for Students (to be deleted in the 

* Clone the created repository offline;
* Add your name and surname into the Readme file;
* Make any changes to your repository, according to the specific assignment;
* Add a `requirement.txt` file for code reproducibility and instructions on how to replicate the results;
* Commit your changes to your local repository;
* Push your changes to your online repository.
