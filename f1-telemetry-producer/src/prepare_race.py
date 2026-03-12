"""
prepare_race: batch etl stage of the two-stage producer pipeline.

extracts telemetry, lap, and track status data from a fastf1 session,
merges and interleaves them chronologically, then saves the result as a
compressed parquet file for the streaming stage (stream_race.py).

separating etl from streaming avoids re-running the expensive fastf1 extraction
(~30-60s of api calls and dataframe merges) every time we want to replay a race.
"""

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from fastf1 import Cache, get_session

TOPIC_TELEMETRY = "f1-telemetry"
TOPIC_LAPS = "f1-laps"
TOPIC_TRACK_STATUS = "f1-track-status"

# pit loss times (seconds) per track under green, vsc, and safety car conditions.
# values reflect the pit lane time delta: how much time a driver loses by entering
# the pit lane vs staying on track. depends on pit entry/exit geometry, speed limit
# zone length, and whether the pit lane bypasses slow corners.
# covers all circuits used in the 2022-2025 ground-effect era.
PIT_LOSS_BY_RACE = {
    # 2022-2025 calendar (ground-effect era)
    "Bahrain Grand Prix":        {"green": 22.0, "vsc": 13.0, "sc": 10.0},
    "Saudi Arabian Grand Prix":  {"green": 24.0, "vsc": 14.5, "sc": 11.5},
    "Australian Grand Prix":     {"green": 23.0, "vsc": 14.0, "sc": 11.0},
    "Japanese Grand Prix":       {"green": 24.0, "vsc": 14.5, "sc": 11.5},
    "Chinese Grand Prix":        {"green": 23.5, "vsc": 14.0, "sc": 11.0},
    "Miami Grand Prix":          {"green": 23.0, "vsc": 14.0, "sc": 11.0},
    "Emilia Romagna Grand Prix": {"green": 23.0, "vsc": 13.5, "sc": 10.5},
    "Monaco Grand Prix":         {"green": 20.0, "vsc": 12.0, "sc": 9.5},
    "Spanish Grand Prix":        {"green": 22.5, "vsc": 13.5, "sc": 10.5},
    "Canadian Grand Prix":       {"green": 21.5, "vsc": 13.0, "sc": 10.0},
    "Austrian Grand Prix":       {"green": 21.0, "vsc": 12.5, "sc": 9.5},
    "British Grand Prix":        {"green": 22.5, "vsc": 13.5, "sc": 10.5},
    "Hungarian Grand Prix":      {"green": 21.5, "vsc": 13.0, "sc": 10.0},
    "Belgian Grand Prix":        {"green": 23.5, "vsc": 14.0, "sc": 11.0},
    "Dutch Grand Prix":          {"green": 21.5, "vsc": 13.0, "sc": 10.0},
    "Italian Grand Prix":        {"green": 25.0, "vsc": 15.0, "sc": 12.0},
    "Azerbaijan Grand Prix":     {"green": 24.5, "vsc": 15.0, "sc": 12.0},
    "Singapore Grand Prix":      {"green": 23.0, "vsc": 14.0, "sc": 11.0},
    "United States Grand Prix":  {"green": 22.5, "vsc": 13.5, "sc": 10.5},
    "Mexico City Grand Prix":    {"green": 22.0, "vsc": 13.0, "sc": 10.0},
    "São Paulo Grand Prix":      {"green": 23.0, "vsc": 14.0, "sc": 11.0},
    "Las Vegas Grand Prix":      {"green": 24.0, "vsc": 14.5, "sc": 11.5},
    "Qatar Grand Prix":          {"green": 22.5, "vsc": 13.5, "sc": 10.5},
    "Abu Dhabi Grand Prix":      {"green": 23.0, "vsc": 14.0, "sc": 11.0},
    # 2022 circuits not carried into later years
    "French Grand Prix":         {"green": 23.0, "vsc": 14.0, "sc": 11.0},
    # 2023+ additions
    "Portuguese Grand Prix":     {"green": 22.5, "vsc": 13.5, "sc": 10.5},
}

PIT_LOSS_DEFAULT = {"green": 22.0, "vsc": 14.0, "sc": 11.0}


def get_pit_losses(race_name: str) -> dict:
    """look up pit loss times for a given race name, fall back to default if unknown."""
    return PIT_LOSS_BY_RACE.get(race_name, PIT_LOSS_DEFAULT)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare race data for streaming replay"
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
        help='grand prix name, ex: "Italian Grand Prix" (default: Italian Grand Prix)',
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
    """load a fastf1 session, configurable via cli args for replaying different historical races."""
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
    merged["event_topic"] = TOPIC_TELEMETRY
    return merged


def build_lap_events(session) -> pd.DataFrame:
    """
    extract per-driver lap completion events from session.laps.
    each row = one completed lap with timing, tire, and position data.
    also computes GapToCarAhead from cumulative race time differences,
    and enriches with per-lap weather (AirTemp, TrackTemp, Humidity, Rainfall),
    speed trap on the longest straight (SpeedST), and team name.
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
        "SpeedST",
        "Team",
        "IsAccurate",
    ]

    all_laps = session.laps
    available = [col for col in lap_columns if col in all_laps.columns]
    laps_df = all_laps[available].copy()

    # filter out inaccurate laps (sc-affected, timing anomalies) to ensure clean ml features.
    # pit in/out laps are preserved regardless of accuracy because PitStopEvaluator needs
    # pitInTime != null to detect pit entry and trigger the ground truth evaluation pipeline.
    if "IsAccurate" in laps_df.columns:
        pre_count = len(laps_df)
        is_pit_lap = laps_df["PitInTime"].notna() | laps_df["PitOutTime"].notna()
        laps_df = laps_df[is_pit_lap | (laps_df["IsAccurate"] == True)].copy()
        logging.info("IsAccurate filter: %d -> %d laps (pit laps preserved)", pre_count, len(laps_df))
        laps_df = laps_df.drop(columns=["IsAccurate"])

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

    # enrich with per-lap weather data from fastf1 (AirTemp, TrackTemp, Humidity, Rainfall).
    # get_weather_data joins the closest weather sample to each lap's timestamp.
    laps_df = _enrich_with_weather(session, laps_df)

    # total race laps derived from the highest lap number in the loaded session data.
    # session.total_laps is unreliable (often None) because fastf1 only populates it
    # from a specific api endpoint not always requested. max(LapNumber) is always
    # available after session.load() and equals total laps for completed races.
    # for red-flagged races it reflects laps actually raced, which is correct for strategy.
    total_laps = int(all_laps["LapNumber"].max())
    laps_df["TotalLaps"] = total_laps

    laps_df["event_topic"] = TOPIC_LAPS
    return laps_df


def _enrich_with_weather(session, laps_df: pd.DataFrame) -> pd.DataFrame:
    """
    join per-lap weather data (AirTemp, TrackTemp, Humidity, Rainfall) from fastf1.
    fastf1's get_weather_data() returns the closest weather sample for each lap.
    if weather data is unavailable (e.g., older sessions), columns are added as NaN.
    """
    weather_cols = ["AirTemp", "TrackTemp", "Humidity", "Rainfall"]
    try:
        weather = session.laps.get_weather_data()
        if weather is not None and not weather.empty:
            available_weather = [c for c in weather_cols if c in weather.columns]
            if available_weather:
                # get_weather_data returns one row per lap with the same index as session.laps.
                # align by building a (Driver, LapNumber) key on both sides.
                weather_subset = session.laps[["Driver", "LapNumber"]].copy()
                for col in available_weather:
                    weather_subset[col] = weather[col].values
                laps_df = laps_df.merge(weather_subset, on=["Driver", "LapNumber"], how="left")
                logging.info("Weather enrichment added: %s", available_weather)
                return laps_df
    except Exception as e:
        logging.warning("Weather enrichment failed: %s", e)

    # fallback: add empty columns
    for col in weather_cols:
        if col not in laps_df.columns:
            laps_df[col] = np.nan
    return laps_df


def _compute_gap_to_car_ahead(laps_df: pd.DataFrame) -> pd.DataFrame:
    """
    approximate gap to car ahead using cumulative lap times.
    for each lap number, drivers are sorted by position and the gap is the
    difference in cumulative race time between consecutive positions.
    ex: VER cumulative 1200.5s at P1, LEC cumulative 1205.3s at P2 -> LEC gap = 4.8s
    """
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

    ts_df["event_topic"] = TOPIC_TRACK_STATUS
    logging.info("Found %d track status events", len(ts_df))
    return ts_df


def build_replay_dataframe(session) -> pd.DataFrame:
    """
    build the unified replay dataframe by interleaving telemetry, lap, and track status events.
    all three event types are sorted chronologically by Date for realistic replay timing.
    each row is tagged with event_topic to route to the correct kafka topic during streaming.
    """
    frames = []

    # extract the full grid dynamically from session lap data
    drivers = session.laps["Driver"].dropna().unique().tolist()
    logging.info(
        "Found %d drivers in session: %s", len(drivers), ", ".join(sorted(drivers))
    )

    # telemetry (high frequency, ~4 Hz per driver)
    for driver in drivers:
        logging.info("Extracting telemetry/position for %s", driver)
        try:
            driver_frame = build_driver_telemetry(session, driver)
            frames.append(driver_frame)
        except Exception as e:
            logging.warning("Skipping %s: %s", driver, e)

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

    return replay_df


def enrich_with_pit_losses(replay_df: pd.DataFrame, race_name: str) -> pd.DataFrame:
    """inject Race, PitLoss, VscPitLoss, ScPitLoss columns into every row."""
    losses = get_pit_losses(race_name)
    replay_df["Race"] = race_name
    replay_df["PitLoss"] = losses["green"]
    replay_df["VscPitLoss"] = losses["vsc"]
    replay_df["ScPitLoss"] = losses["sc"]
    return replay_df


def parquet_filename(year: int, race: str, session: str) -> str:
    """deterministic filename for the prepared parquet file.
    ex: 2023_Italian_Grand_Prix_R_prepared.parquet"""
    safe_race = race.replace(" ", "_")
    return f"{year}_{safe_race}_{session}_prepared.parquet"


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
    replay_df = build_replay_dataframe(race_session)
    replay_df = enrich_with_pit_losses(replay_df, args.race)
    logging.info("Replay dataframe prepared with %d rows", len(replay_df))
    logging.info(
        "Pit losses for %s: green=%.1fs, vsc=%.1fs, sc=%.1fs",
        args.race,
        replay_df["PitLoss"].iloc[0],
        replay_df["VscPitLoss"].iloc[0],
        replay_df["ScPitLoss"].iloc[0],
    )

    # save to the project-level data/ directory (same as fastf1 cache)
    out_dir = Path(__file__).resolve().parents[2] / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / parquet_filename(args.year, args.race, args.session)

    replay_df.to_parquet(out_path, engine="pyarrow", index=False)
    logging.info(
        "Saved prepared replay to: %s (%.1f MB)",
        out_path,
        out_path.stat().st_size / 1e6,
    )
