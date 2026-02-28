"""
f1 telemetry producer — simulates a live race replay via kafka.

loads historical telemetry, lap, and track status data from the fastf1 library,
then streams events across three kafka topics (f1-telemetry, f1-laps, f1-track-status)
respecting the original inter-event time deltas.

this replay approach lets the flink consumer operate on realistic event-time data
without needing a live f1 session.
"""

import argparse
import json
import logging
import time
from pathlib import Path

import numpy as np
import pandas as pd
from kafka import KafkaProducer
from fastf1 import Cache, get_session


# drivers to include in the replay, add/remove abbreviations to control scope
DRIVERS = ["VER", "LEC", "SAI"]

TOPIC_TELEMETRY = "f1-telemetry"
TOPIC_LAPS = "f1-laps"
TOPIC_TRACK_STATUS = "f1-track-status"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="F1 telemetry replay producer")
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
        help="season year for the fastf1 session (default: 2023)",
    )
    parser.add_argument(
        "--race",
        type=str,
        default="Italian Grand Prix",
        help='grand prix name, ex: "Italian Grand Prix", "Australian Grand Prix" (default: Italian Grand Prix)',
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


def configure_cache() -> Path:
    """enable fastf1 disk cache in the project-level data/ directory to avoid redundant api calls."""
    cache_dir = Path(__file__).resolve().parents[2] / "data"
    cache_dir.mkdir(parents=True, exist_ok=True)
    Cache.enable_cache(str(cache_dir))
    return cache_dir


def load_session(
    year: int = 2023, race: str = "Italian Grand Prix", session_type: str = "R"
):
    """load a fastf1 session — configurable via cli args for replaying different historical races."""
    session = get_session(year, race, session_type)
    session.load()
    return session


def build_driver_telemetry(session, driver: str) -> pd.DataFrame:
    """
    merge car telemetry (speed, rpm, throttle, brake, gear, drs) with position data (x, y, z)
    for a single driver using an asof join on the Date column.

    tolerance of 100ms because telemetry and position are sampled at slightly different rates.
    also assigns LapNumber to each telemetry row via asof join with lap start dates.
    """
    driver_laps = session.laps.pick_drivers(driver)

    telemetry = driver_laps.get_car_data().copy()
    position = driver_laps.get_pos_data().copy()

    telemetry_columns = [
        "Date",
        "SessionTime",
        "Speed",
        "RPM",
        "Throttle",
        "Brake",
        "nGear",
        "DRS",
    ]
    position_columns = ["Date", "SessionTime", "X", "Y", "Z"]

    telemetry = telemetry[
        [column for column in telemetry_columns if column in telemetry.columns]
    ]
    position = position[
        [column for column in position_columns if column in position.columns]
    ]

    if "Date" not in telemetry.columns or "Date" not in position.columns:
        raise ValueError(
            f"Date column is required for merge but missing for driver {driver}."
        )

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

    # assign lap number to each telemetry row by matching against lap start dates.
    # uses a backward asof join: each telemetry sample belongs to the lap that started
    # at or before it, ex: sample at T=120s with laps starting at T=80s and T=160s -> lap at T=80s
    lap_starts = (
        driver_laps[["LapNumber", "LapStartDate"]]
        .dropna(subset=["LapStartDate"])
        .copy()
    )
    lap_starts = (
        lap_starts.rename(columns={"LapStartDate": "Date"})
        .sort_values("Date")
        .reset_index(drop=True)
    )
    merged = pd.merge_asof(
        merged.sort_values("Date"),
        lap_starts,
        on="Date",
        direction="backward",
    )

    # fastf1 Brake is boolean (False/True), java TelemetryEvent expects int (0/1).
    # json serializes bools as true/false which jackson cannot coerce to int.
    if "Brake" in merged.columns:
        merged["Brake"] = merged["Brake"].astype(int)

    merged["Driver"] = driver
    merged["_topic"] = TOPIC_TELEMETRY
    return merged


def build_lap_events(session) -> pd.DataFrame:
    """
    extract per-driver lap completion events from session.laps.
    each row = one completed lap with timing, tire, and position data.
    also computes GapToCarAhead from cumulative race time differences.
    """
    lap_columns = [
        "Driver",
        "LapNumber",
        "LapTime",
        "Sector1Time",
        "Sector2Time",
        "Sector3Time",
        "Stint",
        "Compound",
        "TyreLife",
        "FreshTyre",
        "Position",
        "PitInTime",
        "PitOutTime",
        "LapStartDate",
        "TrackStatus",
    ]

    all_laps = session.laps
    # filter to our target drivers
    laps_df = all_laps[all_laps["Driver"].isin(DRIVERS)].copy()
    available = [col for col in lap_columns if col in laps_df.columns]
    laps_df = laps_df[available].copy()

    # use LapStartDate as the event time (Date field for flink watermarks)
    if "LapStartDate" in laps_df.columns:
        laps_df["Date"] = laps_df["LapStartDate"]
        laps_df = laps_df.drop(columns=["LapStartDate"])
    else:
        logging.warning("LapStartDate not available, lap events will lack event time")
        return pd.DataFrame()

    laps_df = laps_df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)

    # compute gap to car ahead: for each lap number, sort by position and
    # calculate the cumulative race time difference between adjacent positions.
    # cumulative time = sum of all LapTime values up to this lap for each driver.
    laps_df = _compute_gap_to_car_ahead(laps_df)

    laps_df["_topic"] = TOPIC_LAPS
    return laps_df


def _compute_gap_to_car_ahead(laps_df: pd.DataFrame) -> pd.DataFrame:
    """
    approximate gap to car ahead using cumulative lap times.
    for each lap number, drivers are sorted by position and the gap is the
    difference in cumulative race time between consecutive positions.
    ex: VER cumulative 1200.5s at P1, LEC cumulative 1205.3s at P2 -> LEC gap = 4.8s
    """
    # convert LapTime timedelta to seconds for arithmetic
    if "LapTime" not in laps_df.columns or "Position" not in laps_df.columns:
        laps_df["GapToCarAhead"] = None
        return laps_df

    # build cumulative race time per driver
    laps_df = laps_df.sort_values(["Driver", "LapNumber"]).copy()
    laps_df["_lap_seconds"] = laps_df["LapTime"].apply(
        lambda x: (
            x.total_seconds() if isinstance(x, pd.Timedelta) and pd.notna(x) else np.nan
        )
    )
    laps_df["_cumulative"] = laps_df.groupby("Driver")["_lap_seconds"].cumsum()

    # for each lap, sort by position and compute gap between adjacent positions
    gaps = []
    for lap_num, group in laps_df.groupby("LapNumber"):
        sorted_group = group.sort_values("Position")
        cum_times = sorted_group["_cumulative"].values
        positions = sorted_group.index

        gap_values = [None]  # leader has no car ahead
        for i in range(1, len(cum_times)):
            if pd.notna(cum_times[i]) and pd.notna(cum_times[i - 1]):
                gap_values.append(round(cum_times[i] - cum_times[i - 1], 3))
            else:
                gap_values.append(None)

        for idx, gap in zip(positions, gap_values):
            gaps.append((idx, gap))

    gap_series = pd.Series(dict(gaps), name="GapToCarAhead")
    laps_df["GapToCarAhead"] = gap_series

    laps_df = laps_df.drop(columns=["_lap_seconds", "_cumulative"])
    return laps_df


def build_track_status_events(session) -> pd.DataFrame:
    """
    extract global track status changes from the session.
    fastf1 provides session.track_status with columns: Time (timedelta), Status (code), Message.
    we convert Time to an absolute timestamp for event-time alignment.
    """
    try:
        track_status = session.track_status
    except Exception:
        logging.warning("track_status not available for this session")
        return pd.DataFrame()

    if track_status is None or track_status.empty:
        logging.info("no track status events in this session")
        return pd.DataFrame()

    ts_df = track_status[["Time", "Status", "Message"]].copy()

    # convert session-relative timedelta to absolute timestamp
    # session.date is the session start datetime (timezone-aware or naive)
    session_start = pd.Timestamp(session.date)
    ts_df["Date"] = session_start + ts_df["Time"]
    ts_df = ts_df.drop(columns=["Time"])
    ts_df = ts_df.sort_values("Date").reset_index(drop=True)

    ts_df["_topic"] = TOPIC_TRACK_STATUS
    logging.info("Found %d track status events", len(ts_df))
    return ts_df


def build_replay_dataframe(session, start_lap: int = 1) -> pd.DataFrame:
    """
    build the unified replay dataframe by interleaving telemetry, lap, and track status events.
    all three event types are sorted chronologically by Date for realistic replay timing.
    each row is tagged with _topic to route to the correct kafka topic during streaming.
    """
    frames = []

    # telemetry (high frequency, ~4 Hz per driver)
    for driver in DRIVERS:
        logging.info("Extracting telemetry/position for %s", driver)
        driver_frame = build_driver_telemetry(session, driver)
        frames.append(driver_frame)

    # lap events (~1 per driver per ~80s)
    lap_events = build_lap_events(session)
    if not lap_events.empty:
        frames.append(lap_events)
        logging.info("Lap events: %d rows", len(lap_events))

    # track status (sparse, ~1-5 per race)
    track_status_events = build_track_status_events(session)
    if not track_status_events.empty:
        frames.append(track_status_events)

    replay_df = pd.concat(frames, ignore_index=True)
    replay_df = replay_df.sort_values("Date").reset_index(drop=True)

    if start_lap > 1:
        pre_filter = len(replay_df)
        # only filter telemetry rows by lap number, lap/track-status events lack LapNumber
        has_lap = replay_df["LapNumber"].notna()
        keep = ~has_lap | (replay_df["LapNumber"] >= start_lap)
        # for lap events (topic=f1-laps), filter by their own LapNumber field
        is_lap_event = replay_df["_topic"] == TOPIC_LAPS
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


# internal columns not sent to kafka (used for routing and filtering only)
_INTERNAL_COLUMNS = {"_topic"}


def row_to_payload(row: pd.Series) -> dict:
    return {
        column: serialize_value(row[column])
        for column in row.index
        if column not in _INTERNAL_COLUMNS and serialize_value(row[column]) is not None
    }


def stream_replay(
    replay_df: pd.DataFrame, producer: KafkaProducer, speed: float = 1.0
) -> None:
    """
    iterate through the sorted replay dataframe and publish each row to its target kafka topic.
    sleeps for the real delta-t between consecutive samples divided by the speed multiplier.
    """
    previous_timestamp = None
    topic_counts = {TOPIC_TELEMETRY: 0, TOPIC_LAPS: 0, TOPIC_TRACK_STATUS: 0}

    for _, row in replay_df.iterrows():
        current_timestamp = row["Date"]
        topic = row.get("_topic", TOPIC_TELEMETRY)

        if previous_timestamp is not None:
            delta_seconds = (current_timestamp - previous_timestamp).total_seconds()
            if delta_seconds > 0:
                time.sleep(delta_seconds / speed)

        payload = row_to_payload(row)
        producer.send(topic, value=payload)
        topic_counts[topic] = topic_counts.get(topic, 0) + 1

        # log at different verbosity depending on event type
        if topic == TOPIC_TELEMETRY:
            logging.debug(
                "-> %s | driver=%s | speed=%s",
                topic,
                payload.get("Driver"),
                payload.get("Speed"),
            )
        else:
            logging.info("-> %s | %s", topic, json.dumps(payload, default=str)[:200])

        previous_timestamp = current_timestamp

    producer.flush()
    logging.info("Replay complete. Events sent: %s", topic_counts)


if __name__ == "__main__":
    args = parse_args()
    configure_logging()
    cache_path = configure_cache()
    logging.info("FastF1 cache enabled at: %s", cache_path)

    logging.info(
        "Loading FastF1 session: %d %s - %s", args.year, args.race, args.session
    )
    race_session = load_session(
        year=args.year, race=args.race, session_type=args.session
    )
    replay_dataframe = build_replay_dataframe(race_session, start_lap=args.start_lap)
    logging.info("Replay dataframe prepared with %d rows", len(replay_dataframe))

    kafka_producer = create_producer()
    logging.info(
        "Kafka producer initialized, speed=%.1fx, start_lap=%d",
        args.speed,
        args.start_lap,
    )

    try:
        stream_replay(replay_dataframe, kafka_producer, speed=args.speed)
    except KeyboardInterrupt:
        logging.info("Streaming interrupted by user")
    finally:
        kafka_producer.close()
        logging.info("Kafka producer closed")
