"""
f1 telemetry producer — simulates a live race replay via kafka.

loads historical telemetry and position data from the fastf1 library for selected
drivers, merges them by timestamp, then streams each row to the "f1-telemetry" kafka
topic respecting the original inter-sample time deltas.

this replay approach lets the flink consumer operate on realistic event-time data
without needing a live f1 session.
"""

import json
import logging
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from fastf1 import Cache, get_session


# drivers to include in the replay — add/remove abbreviations to control scope
DRIVERS = ["VER", "LEC", "SAI"]
TOPIC_NAME = "f1-telemetry"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def configure_cache() -> Path:
    """enable fastf1 disk cache in the project-level data/ directory to avoid redundant api calls."""
    cache_dir = Path(__file__).resolve().parents[2] / "data"
    cache_dir.mkdir(parents=True, exist_ok=True)
    Cache.enable_cache(str(cache_dir))
    return cache_dir


def load_session():
    """load the 2023 monza race session — change year/gp/session type here to replay a different race."""
    session = get_session(2023, "Italian Grand Prix", "R")
    session.load()
    return session


def build_driver_dataframe(session, driver: str) -> pd.DataFrame:
    """
    merge car telemetry (speed, rpm, throttle, brake, gear) with position data (x, y, z)
    for a single driver using an asof join on the Date column.

    tolerance of 100ms because telemetry and position are sampled at slightly different rates.
    """
    driver_laps = session.laps.pick_drivers(driver)

    telemetry = driver_laps.get_car_data().copy()
    position = driver_laps.get_pos_data().copy()

    telemetry_columns = ["Date", "SessionTime", "Speed", "RPM", "Throttle", "Brake", "nGear"]
    position_columns = ["Date", "SessionTime", "X", "Y", "Z"]

    telemetry = telemetry[[column for column in telemetry_columns if column in telemetry.columns]]
    position = position[[column for column in position_columns if column in position.columns]]

    if "Date" not in telemetry.columns or "Date" not in position.columns:
        raise ValueError(f"Date column is required for merge but missing for driver {driver}.")

    telemetry = telemetry.sort_values("Date").reset_index(drop=True)
    position = position.sort_values("Date").reset_index(drop=True)

    merged = pd.merge_asof(
        telemetry,
        position,
        on="Date",
        suffixes=("", "_pos"),
        direction="nearest",
        tolerance=pd.Timedelta(milliseconds=100),
    )

    if "SessionTime_pos" in merged.columns and "SessionTime" in merged.columns:
        merged["SessionTime"] = merged["SessionTime"].fillna(merged["SessionTime_pos"])
        merged = merged.drop(columns=["SessionTime_pos"])

    merged["Driver"] = driver
    return merged


def build_replay_dataframe(session) -> pd.DataFrame:
    """concatenate all driver dataframes and sort globally by Date for chronological replay."""
    driver_frames = []
    for driver in DRIVERS:
        logging.info("Extracting telemetry/position for %s", driver)
        driver_frame = build_driver_dataframe(session, driver)
        driver_frames.append(driver_frame)

    replay_df = pd.concat(driver_frames, ignore_index=True)
    replay_df = replay_df.sort_values("Date").reset_index(drop=True)
    return replay_df


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
    )


def serialize_value(value):
    """
    convert pandas/numpy types to json-safe python primitives.
    ex: pd.Timestamp -> iso-8601 string, np.int64 -> int, pd.Timedelta -> float seconds.
    """
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, pd.Timedelta):
        return value.total_seconds()
    if hasattr(value, "item"):
        return value.item()
    return value


def row_to_payload(row: pd.Series) -> dict:
    payload = {column: serialize_value(row[column]) for column in row.index}
    return payload


def stream_replay(replay_df: pd.DataFrame, producer: KafkaProducer) -> None:
    """
    iterate through the sorted replay dataframe and publish each row to kafka.
    sleeps for the real delta-t between consecutive samples to simulate live timing.
    """
    previous_timestamp = None

    for _, row in replay_df.iterrows():
        current_timestamp = row["Date"]

        if previous_timestamp is not None:
            delta_seconds = (current_timestamp - previous_timestamp).total_seconds()
            if delta_seconds > 0:
                time.sleep(delta_seconds)

        payload = row_to_payload(row)
        producer.send(TOPIC_NAME, value=payload)
        logging.info("Sent event | driver=%s | date=%s | speed=%s", payload.get("Driver"), payload.get("Date"), payload.get("Speed"))

        previous_timestamp = current_timestamp

    producer.flush()


if __name__ == "__main__":
    configure_logging()
    cache_path = configure_cache()
    logging.info("FastF1 cache enabled at: %s", cache_path)

    logging.info("Loading FastF1 session: 2023 Italian Grand Prix - Race")
    race_session = load_session()
    replay_dataframe = build_replay_dataframe(race_session)
    logging.info("Replay dataframe prepared with %d rows", len(replay_dataframe))

    kafka_producer = create_producer()
    logging.info("Kafka producer initialized on localhost:9092, topic=%s", TOPIC_NAME)

    try:
        stream_replay(replay_dataframe, kafka_producer)
    except KeyboardInterrupt:
        logging.info("Streaming interrupted by user")
    finally:
        kafka_producer.close()
        logging.info("Kafka producer closed")
