"""
stream_race: streaming stage of the two-stage producer pipeline.

loads a pre-computed parquet file (produced by prepare_race.py) and replays
events to kafka topics respecting the original inter-event timing.

optimized for high-speed replay:
  - itertuples instead of iterrows (~10-50x faster row iteration)
  - absolute wall-clock anchoring instead of per-row delta sleep
    (prevents cumulative drift at high speed multipliers)
  - pre-cached column names for fast dict construction via getattr
"""

import argparse
import json
import logging
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

from prepare_race import parquet_filename

TOPIC_TELEMETRY = "f1-telemetry"
TOPIC_LAPS = "f1-laps"
TOPIC_TRACK_STATUS = "f1-track-status"

# columns internal to the pipeline, not sent to kafka
_INTERNAL_COLUMNS = {"event_topic"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream pre-computed race data to Kafka"
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="replay speed multiplier, ex: 50 replays the race ~50x faster (default: 1.0)",
    )
    parser.add_argument(
        "--start-lap",
        type=int,
        default=1,
        help="skip to this lap number before starting the replay (default: 1)",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2023,
        help="season year (default: 2023)",
    )
    parser.add_argument(
        "--race",
        type=str,
        default="Italian Grand Prix",
        help="grand prix name (default: Italian Grand Prix)",
    )
    parser.add_argument(
        "--session",
        type=str,
        default="R",
        help="session type: R=race, Q=qualifying, FP1/FP2/FP3=practice (default: R)",
    )
    return parser.parse_args()


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def load_parquet(year: int, race: str, session: str) -> pd.DataFrame:
    """load the pre-computed parquet file from the data/ directory."""
    data_dir = Path(__file__).resolve().parents[2] / "data"
    parquet_path = data_dir / parquet_filename(year, race, session)
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Parquet file not found: {parquet_path}\n"
            f'Run prepare_race.py first: python prepare_race.py --year {year} --race "{race}" --session {session}'
        )
    logging.info("Loading parquet: %s", parquet_path)
    return pd.read_parquet(parquet_path, engine="pyarrow")


def filter_by_start_lap(replay_df: pd.DataFrame, start_lap: int) -> pd.DataFrame:
    """filter rows to skip laps before start_lap, same logic as the old build_replay_dataframe."""
    if start_lap <= 1:
        return replay_df

    pre_filter = len(replay_df)
    # only filter telemetry rows by lap number, lap/track-status events lack LapNumber
    has_lap = replay_df["LapNumber"].notna()
    keep = ~has_lap | (replay_df["LapNumber"] >= start_lap)
    # for lap events (topic=f1-laps), filter by their own LapNumber field
    is_lap_event = replay_df["event_topic"] == TOPIC_LAPS
    if "LapNumber" in replay_df.columns:
        keep = keep & (~is_lap_event | (replay_df["LapNumber"] >= start_lap))
    replay_df = replay_df[keep].reset_index(drop=True)
    logging.info(
        "--start-lap %d: filtered %d -> %d rows",
        start_lap,
        pre_filter,
        len(replay_df),
    )
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


def stream_replay(
    replay_df: pd.DataFrame, producer: KafkaProducer, speed: float = 1.0
) -> None:
    """
    iterate through the sorted replay dataframe and publish each row to its target kafka topic.
    uses absolute wall-clock anchoring: the first event's timestamp is pinned to time.time(),
    and each subsequent event sleeps only until its exact target wall-clock time. this prevents
    cumulative drift that per-row delta sleeping causes at high speed multipliers.
    """
    topic_counts = {TOPIC_TELEMETRY: 0, TOPIC_LAPS: 0, TOPIC_TRACK_STATUS: 0}

    # pre-cache the payload column names (all columns except internal routing ones)
    payload_columns = tuple(c for c in replay_df.columns if c not in _INTERNAL_COLUMNS)

    # anchor: pin the first event's simulated time to the current wall clock
    start_event_time = replay_df.iloc[0]["Date"]
    start_wall_time = time.time()

    for row in replay_df.itertuples(index=False):
        current_timestamp = row.Date
        topic = getattr(row, "event_topic", TOPIC_TELEMETRY)

        # absolute timeline: compute how far this event is from the start in simulated time,
        # then sleep only the remaining difference vs wall clock
        elapsed_sim = (current_timestamp - start_event_time).total_seconds() / speed
        target_wall = start_wall_time + elapsed_sim
        drift = target_wall - time.time()
        if drift > 0:
            time.sleep(drift)

        # build payload dict via getattr on the namedtuple (avoids iterrows overhead)
        payload = {}
        for col in payload_columns:
            val = serialize_value(getattr(row, col))
            if val is not None:
                payload[col] = val

        producer.send(topic, value=payload)
        topic_counts[topic] = topic_counts.get(topic, 0) + 1

        if topic == TOPIC_TELEMETRY:
            logging.debug(
                "-> %s | driver=%s | speed=%s",
                topic,
                payload.get("Driver"),
                payload.get("Speed"),
            )
        else:
            logging.info("-> %s | %s", topic, json.dumps(payload, default=str)[:200])

    producer.flush()
    logging.info("Replay complete. Events sent: %s", topic_counts)


if __name__ == "__main__":
    args = parse_args()
    configure_logging()

    replay_df = load_parquet(args.year, args.race, args.session)
    logging.info("Loaded %d rows from parquet", len(replay_df))

    replay_df = filter_by_start_lap(replay_df, args.start_lap)

    kafka_producer = create_producer()
    logging.info(
        "Kafka producer initialized, speed=%.1fx, start_lap=%d",
        args.speed,
        args.start_lap,
    )

    try:
        stream_replay(replay_df, kafka_producer, speed=args.speed)
    except KeyboardInterrupt:
        logging.info("Streaming interrupted by user")
    finally:
        kafka_producer.close()
        logging.info("Kafka producer closed")
