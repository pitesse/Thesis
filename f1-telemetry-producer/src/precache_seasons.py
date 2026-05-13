"""
Pre-download and prepare replay parquet files for full seasons.

This script is designed for unstable FastF1 upstream availability: it can use
an offline race calendar (2022-2025) and resumes race-by-race with retries.
It also validates each prepared parquet for basic data correctness.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from fastf1 import get_event_schedule

from prepare_race import (
    TOPIC_LAPS,
    TOPIC_TELEMETRY,
    TOPIC_TRACK_STATUS,
    WEATHER_MODE_CHOICES,
    WEATHER_MODE_OPTIONAL,
    configure_cache,
    configure_logging,
    enrich_with_pit_losses,
    load_session,
    parquet_filename,
    prune_post_race_tail,
    build_replay_dataframe,
)


DEFAULT_YEARS = (2022, 2023, 2024, 2025)
DEFAULT_SESSION = "R"
DEFAULT_SLEEP_SECONDS = 2.0
DEFAULT_RETRY_ATTEMPTS = 4
DEFAULT_RETRY_DELAY_SECONDS = 20.0
VALID_EVENT_TOPICS = {TOPIC_TELEMETRY, TOPIC_LAPS, TOPIC_TRACK_STATUS}
FALLBACK_CALENDAR_FILE = (
    Path(__file__).resolve().parents[2] / "scripts" / "f1_race_calendar_2022_2025.json"
)
FASTF1_EVENT_FORMATS = {
    "conventional",
    "sprint",
    "sprint_shootout",
    "sprint_qualifying",
}


# Static, realized race order to avoid schedule endpoint dependency.
# Sources:
# - Formula 1 official race calendars (2022/2023/2024) on formula1.com/racing/<year>
# - Formula 1 corporate announcement for 2025 calendar:
#   https://corp.formula1.com/fia-and-formula-1-announces-2025-calendar/
FALLBACK_RACE_CALENDAR: dict[int, list[str]] = {
    2022: [
        "Bahrain Grand Prix",
        "Saudi Arabian Grand Prix",
        "Australian Grand Prix",
        "Emilia Romagna Grand Prix",
        "Miami Grand Prix",
        "Spanish Grand Prix",
        "Monaco Grand Prix",
        "Azerbaijan Grand Prix",
        "Canadian Grand Prix",
        "British Grand Prix",
        "Austrian Grand Prix",
        "French Grand Prix",
        "Hungarian Grand Prix",
        "Belgian Grand Prix",
        "Dutch Grand Prix",
        "Italian Grand Prix",
        "Singapore Grand Prix",
        "Japanese Grand Prix",
        "United States Grand Prix",
        "Mexico City Grand Prix",
        "São Paulo Grand Prix",
        "Abu Dhabi Grand Prix",
    ],
    2023: [
        "Bahrain Grand Prix",
        "Saudi Arabian Grand Prix",
        "Australian Grand Prix",
        "Azerbaijan Grand Prix",
        "Miami Grand Prix",
        "Monaco Grand Prix",
        "Spanish Grand Prix",
        "Canadian Grand Prix",
        "Austrian Grand Prix",
        "British Grand Prix",
        "Hungarian Grand Prix",
        "Belgian Grand Prix",
        "Dutch Grand Prix",
        "Italian Grand Prix",
        "Singapore Grand Prix",
        "Japanese Grand Prix",
        "Qatar Grand Prix",
        "United States Grand Prix",
        "Mexico City Grand Prix",
        "São Paulo Grand Prix",
        "Las Vegas Grand Prix",
        "Abu Dhabi Grand Prix",
    ],
    2024: [
        "Bahrain Grand Prix",
        "Saudi Arabian Grand Prix",
        "Australian Grand Prix",
        "Japanese Grand Prix",
        "Chinese Grand Prix",
        "Miami Grand Prix",
        "Emilia Romagna Grand Prix",
        "Monaco Grand Prix",
        "Canadian Grand Prix",
        "Spanish Grand Prix",
        "Austrian Grand Prix",
        "British Grand Prix",
        "Hungarian Grand Prix",
        "Belgian Grand Prix",
        "Dutch Grand Prix",
        "Italian Grand Prix",
        "Azerbaijan Grand Prix",
        "Singapore Grand Prix",
        "United States Grand Prix",
        "Mexico City Grand Prix",
        "São Paulo Grand Prix",
        "Las Vegas Grand Prix",
        "Qatar Grand Prix",
        "Abu Dhabi Grand Prix",
    ],
    2025: [
        "Australian Grand Prix",
        "Chinese Grand Prix",
        "Japanese Grand Prix",
        "Bahrain Grand Prix",
        "Saudi Arabian Grand Prix",
        "Miami Grand Prix",
        "Emilia Romagna Grand Prix",
        "Monaco Grand Prix",
        "Spanish Grand Prix",
        "Canadian Grand Prix",
        "Austrian Grand Prix",
        "British Grand Prix",
        "Belgian Grand Prix",
        "Hungarian Grand Prix",
        "Dutch Grand Prix",
        "Italian Grand Prix",
        "Azerbaijan Grand Prix",
        "Singapore Grand Prix",
        "United States Grand Prix",
        "Mexico City Grand Prix",
        "São Paulo Grand Prix",
        "Las Vegas Grand Prix",
        "Qatar Grand Prix",
        "Abu Dhabi Grand Prix",
    ],
}


@dataclass
class RaceReportRow:
    year: int
    race: str
    status: str
    source: str
    parquet_path: str
    rows_total: int
    rows_telemetry: int
    rows_laps: int
    rows_track_status: int
    unique_lap_driver_pairs: int
    duplicate_lap_driver_pairs: int
    laps_min: int
    laps_max: int
    date_monotonic: int
    validation_ok: int
    notes: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pre-cache and validate prepared replay parquet files for seasons"
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=list(DEFAULT_YEARS),
        help="season years to process (default: 2022 2023 2024 2025)",
    )
    parser.add_argument(
        "--session",
        default=DEFAULT_SESSION,
        help="session identifier (default: R)",
    )
    parser.add_argument(
        "--weather-mode",
        choices=WEATHER_MODE_CHOICES,
        default=WEATHER_MODE_OPTIONAL,
        help=(
            "weather enrichment behavior for prepare_race/load_session "
            "(default: optional)"
        ),
    )
    parser.add_argument(
        "--calendar-source",
        choices=("fallback", "fastf1", "auto"),
        default="fallback",
        help="race calendar source (default: fallback)",
    )
    parser.add_argument(
        "--post-race-buffer-seconds",
        type=int,
        default=120,
        help="tail buffer for replay trimming (default: 120)",
    )
    parser.add_argument(
        "--out-dir",
        default="data",
        help="directory for prepared parquet files (default: data)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="skip rebuilding existing parquet files (still validates them)",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="validate existing parquet files only; fail rows with missing parquet",
    )
    parser.add_argument(
        "--retry-attempts",
        type=int,
        default=DEFAULT_RETRY_ATTEMPTS,
        help="prepare retry attempts per race (default: 4)",
    )
    parser.add_argument(
        "--retry-delay-seconds",
        type=float,
        default=DEFAULT_RETRY_DELAY_SECONDS,
        help="delay between retries for a race (default: 20)",
    )
    parser.add_argument(
        "--sleep-between-races",
        type=float,
        default=DEFAULT_SLEEP_SECONDS,
        help="delay between races to reduce upstream pressure (default: 2.0)",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="continue on race failure instead of failing fast",
    )
    parser.add_argument(
        "--report-json",
        default="",
        help="optional report json path",
    )
    parser.add_argument(
        "--report-csv",
        default="",
        help="optional report csv path",
    )
    return parser.parse_args()


def _load_race_list_fastf1(year: int) -> list[str]:
    schedule = get_event_schedule(year, include_testing=False)
    if "EventName" not in schedule.columns:
        raise ValueError(f"FastF1 schedule for {year} missing EventName")
    if "EventFormat" in schedule.columns:
        mask = schedule["EventFormat"].astype(str).isin(FASTF1_EVENT_FORMATS)
        schedule = schedule[mask]
    races = [str(name) for name in schedule["EventName"].tolist() if str(name).strip()]
    if not races:
        raise ValueError(f"FastF1 schedule returned no races for {year}")
    return races


def _race_list_for_year(year: int, calendar_source: str) -> tuple[list[str], str]:
    file_calendar = _load_fallback_calendar_file()

    if calendar_source == "fallback":
        races = file_calendar.get(year) or FALLBACK_RACE_CALENDAR.get(year)
        if not races:
            raise ValueError(f"no fallback calendar configured for year {year}")
        return list(races), "fallback"

    if calendar_source == "fastf1":
        return _load_race_list_fastf1(year), "fastf1"

    try:
        return _load_race_list_fastf1(year), "fastf1"
    except Exception as exc:
        logging.warning(
            "FastF1 schedule load failed for %d (%s), falling back to local calendar",
            year,
            exc,
        )
        races = file_calendar.get(year) or FALLBACK_RACE_CALENDAR.get(year)
        if not races:
            raise
        return list(races), "fallback"


def _load_fallback_calendar_file() -> dict[int, list[str]]:
    if not FALLBACK_CALENDAR_FILE.exists():
        return {}
    try:
        payload = json.loads(FALLBACK_CALENDAR_FILE.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        logging.warning("failed to parse %s: %s", FALLBACK_CALENDAR_FILE, exc)
        return {}

    result: dict[int, list[str]] = {}
    for key, races in payload.items():
        try:
            year = int(key)
        except Exception:
            continue
        if not isinstance(races, list):
            continue
        cleaned = [str(race) for race in races if str(race).strip()]
        if cleaned:
            result[year] = cleaned
    return result


def _validate_replay_df(
    df: pd.DataFrame, *, require_weather: bool = False
) -> tuple[bool, dict[str, int | str]]:
    required = {"Date", "event_topic"}
    missing = sorted(required - set(df.columns))
    if missing:
        return False, {"error": f"missing_columns:{','.join(missing)}"}

    if df.empty:
        return False, {"error": "empty_replay_dataframe"}

    if not df["Date"].is_monotonic_increasing:
        return False, {"error": "date_not_monotonic_increasing"}

    topics = set(df["event_topic"].dropna().astype(str).unique())
    unknown_topics = sorted(topics - VALID_EVENT_TOPICS)
    if unknown_topics:
        return False, {"error": f"unknown_event_topics:{','.join(unknown_topics)}"}

    lap_rows = df[df["event_topic"] == TOPIC_LAPS].copy()
    telemetry_rows = df[df["event_topic"] == TOPIC_TELEMETRY]
    track_rows = df[df["event_topic"] == TOPIC_TRACK_STATUS]

    if lap_rows.empty:
        return False, {"error": "no_lap_rows"}
    if telemetry_rows.empty:
        return False, {"error": "no_telemetry_rows"}

    if require_weather:
        # Rainfall can be all-zero/NaN in dry races depending on source coverage.
        # Require core weather context columns to exist and contain at least one
        # non-null value to avoid accepting stale weather-disabled artifacts.
        core_weather_cols = ("AirTemp", "TrackTemp", "Humidity")
        missing_weather = [c for c in core_weather_cols if c not in lap_rows.columns]
        if missing_weather:
            return False, {
                "error": f"missing_weather_columns:{','.join(missing_weather)}"
            }
        if not lap_rows[list(core_weather_cols)].notna().any().all():
            return False, {
                "error": "weather_required_but_empty",
            }

    for column in ("Driver", "LapNumber"):
        if column not in lap_rows.columns:
            return False, {"error": f"lap_rows_missing_column:{column}"}

    lap_rows["Driver"] = lap_rows["Driver"].astype(str)
    lap_rows["LapNumber"] = pd.to_numeric(lap_rows["LapNumber"], errors="coerce")
    if lap_rows["LapNumber"].isna().any():
        return False, {"error": "lap_rows_non_numeric_lapnumber"}

    lap_rows["LapNumber"] = lap_rows["LapNumber"].astype(int)
    dup_count = int(lap_rows.duplicated(subset=["Driver", "LapNumber"]).sum())

    return True, {
        "rows_total": int(len(df)),
        "rows_telemetry": int(len(telemetry_rows)),
        "rows_laps": int(len(lap_rows)),
        "rows_track_status": int(len(track_rows)),
        "unique_lap_driver_pairs": int(
            lap_rows.drop_duplicates(subset=["Driver", "LapNumber"]).shape[0]
        ),
        "duplicate_lap_driver_pairs": dup_count,
        "laps_min": int(lap_rows["LapNumber"].min()),
        "laps_max": int(lap_rows["LapNumber"].max()),
        "date_monotonic": 1,
    }


def _prepare_one_race(
    year: int,
    race: str,
    session: str,
    weather_mode: str,
    post_race_buffer_seconds: int,
    out_dir: Path,
) -> tuple[pd.DataFrame, Path]:
    race_session = load_session(
        year=year,
        race=race,
        session_type=session,
        weather_mode=weather_mode,
    )
    replay_df = build_replay_dataframe(race_session)
    replay_df = enrich_with_pit_losses(replay_df, race)
    replay_df = prune_post_race_tail(
        replay_df, post_race_buffer_seconds=post_race_buffer_seconds
    )

    out_path = out_dir / parquet_filename(year, race, session)
    replay_df.to_parquet(out_path, engine="pyarrow", index=False)
    return replay_df, out_path


def _validate_existing_parquet(
    path: Path, *, require_weather: bool = False
) -> tuple[bool, dict[str, int | str]]:
    frame = pd.read_parquet(path)
    return _validate_replay_df(frame, require_weather=require_weather)


def _default_report_paths(out_dir: Path, years: list[int]) -> tuple[Path, Path]:
    suffix = f"{min(years)}_{max(years)}"
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return (
        out_dir / f"precache_report_{suffix}_{stamp}.json",
        out_dir / f"precache_report_{suffix}_{stamp}.csv",
    )


def _write_reports(rows: list[RaceReportRow], json_path: Path, csv_path: Path) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "generated_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "total_races": len(rows),
        "ok_races": sum(1 for row in rows if row.status in {"prepared", "skipped_existing"}),
        "failed_races": sum(1 for row in rows if row.status == "failed"),
        "rows": [asdict(row) for row in rows],
    }
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    fieldnames = list(asdict(rows[0]).keys()) if rows else list(asdict(RaceReportRow(
        year=0,
        race="",
        status="",
        source="",
        parquet_path="",
        rows_total=0,
        rows_telemetry=0,
        rows_laps=0,
        rows_track_status=0,
        unique_lap_driver_pairs=0,
        duplicate_lap_driver_pairs=0,
        laps_min=0,
        laps_max=0,
        date_monotonic=0,
        validation_ok=0,
        notes="",
    )).keys())

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))

    logging.info("Wrote report JSON: %s", json_path)
    logging.info("Wrote report CSV:  %s", csv_path)


def main() -> None:
    args = parse_args()
    configure_logging()
    cache_dir = configure_cache()
    logging.info("FastF1 cache directory: %s", cache_dir)

    years = sorted({int(year) for year in args.years})
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    default_json, default_csv = _default_report_paths(out_dir, years)
    report_json = Path(args.report_json) if args.report_json else default_json
    report_csv = Path(args.report_csv) if args.report_csv else default_csv

    report_rows: list[RaceReportRow] = []

    for year in years:
        races, source = _race_list_for_year(year, args.calendar_source)
        logging.info("[%d] using %d races from %s calendar", year, len(races), source)

        for idx, race in enumerate(races, start=1):
            target_path = out_dir / parquet_filename(year, race, args.session)
            logging.info("[%d][%d/%d] %s", year, idx, len(races), race)

            if args.validate_only:
                if not target_path.exists():
                    report_rows.append(
                        RaceReportRow(
                            year=year,
                            race=race,
                            status="failed",
                            source=source,
                            parquet_path=str(target_path),
                            rows_total=0,
                            rows_telemetry=0,
                            rows_laps=0,
                            rows_track_status=0,
                            unique_lap_driver_pairs=0,
                            duplicate_lap_driver_pairs=0,
                            laps_min=0,
                            laps_max=0,
                            date_monotonic=0,
                            validation_ok=0,
                            notes="missing_parquet",
                        )
                    )
                    if not args.continue_on_error:
                        _write_reports(report_rows, report_json, report_csv)
                        raise RuntimeError(f"missing parquet for validation-only mode: {target_path}")
                else:
                    ok, stats = _validate_existing_parquet(
                        target_path, require_weather=(args.weather_mode == "required")
                    )
                    report_rows.append(
                        RaceReportRow(
                            year=year,
                            race=race,
                            status="skipped_existing" if ok else "failed",
                            source=source,
                            parquet_path=str(target_path),
                            rows_total=int(stats.get("rows_total", 0)),
                            rows_telemetry=int(stats.get("rows_telemetry", 0)),
                            rows_laps=int(stats.get("rows_laps", 0)),
                            rows_track_status=int(stats.get("rows_track_status", 0)),
                            unique_lap_driver_pairs=int(stats.get("unique_lap_driver_pairs", 0)),
                            duplicate_lap_driver_pairs=int(stats.get("duplicate_lap_driver_pairs", 0)),
                            laps_min=int(stats.get("laps_min", 0)),
                            laps_max=int(stats.get("laps_max", 0)),
                            date_monotonic=int(stats.get("date_monotonic", 0)),
                            validation_ok=1 if ok else 0,
                            notes="" if ok else str(stats.get("error", "validation_failed")),
                        )
                    )
                    if not ok and not args.continue_on_error:
                        _write_reports(report_rows, report_json, report_csv)
                        raise RuntimeError(f"validation failed for existing parquet: {target_path}")
                if idx < len(races):
                    time.sleep(max(0.0, args.sleep_between_races))
                continue

            if args.skip_existing and target_path.exists():
                ok, stats = _validate_existing_parquet(
                    target_path, require_weather=(args.weather_mode == "required")
                )
                report_rows.append(
                    RaceReportRow(
                        year=year,
                        race=race,
                        status="skipped_existing" if ok else "failed",
                        source=source,
                        parquet_path=str(target_path),
                        rows_total=int(stats.get("rows_total", 0)),
                        rows_telemetry=int(stats.get("rows_telemetry", 0)),
                        rows_laps=int(stats.get("rows_laps", 0)),
                        rows_track_status=int(stats.get("rows_track_status", 0)),
                        unique_lap_driver_pairs=int(stats.get("unique_lap_driver_pairs", 0)),
                        duplicate_lap_driver_pairs=int(stats.get("duplicate_lap_driver_pairs", 0)),
                        laps_min=int(stats.get("laps_min", 0)),
                        laps_max=int(stats.get("laps_max", 0)),
                        date_monotonic=int(stats.get("date_monotonic", 0)),
                        validation_ok=1 if ok else 0,
                        notes="" if ok else str(stats.get("error", "validation_failed")),
                    )
                )
                if not ok and not args.continue_on_error:
                    _write_reports(report_rows, report_json, report_csv)
                    raise RuntimeError(f"validation failed for existing parquet: {target_path}")
                if idx < len(races):
                    time.sleep(max(0.0, args.sleep_between_races))
                continue

            last_error = "unknown_error"
            prepared_ok = False
            stats: dict[str, int | str] = {}

            for attempt in range(1, max(1, args.retry_attempts) + 1):
                try:
                    frame, path = _prepare_one_race(
                        year=year,
                        race=race,
                        session=args.session,
                        weather_mode=args.weather_mode,
                        post_race_buffer_seconds=args.post_race_buffer_seconds,
                        out_dir=out_dir,
                    )
                    ok, stats = _validate_replay_df(
                        frame, require_weather=(args.weather_mode == "required")
                    )
                    if not ok:
                        raise RuntimeError(str(stats.get("error", "validation_failed")))
                    prepared_ok = True
                    target_path = path
                    break
                except Exception as exc:  # noqa: BLE001
                    last_error = str(exc)
                    logging.warning(
                        "prepare failed [%d/%d] for %d %s: %s",
                        attempt,
                        args.retry_attempts,
                        year,
                        race,
                        exc,
                    )
                    if attempt < args.retry_attempts:
                        time.sleep(max(0.0, args.retry_delay_seconds))

            report_rows.append(
                RaceReportRow(
                    year=year,
                    race=race,
                    status="prepared" if prepared_ok else "failed",
                    source=source,
                    parquet_path=str(target_path),
                    rows_total=int(stats.get("rows_total", 0)),
                    rows_telemetry=int(stats.get("rows_telemetry", 0)),
                    rows_laps=int(stats.get("rows_laps", 0)),
                    rows_track_status=int(stats.get("rows_track_status", 0)),
                    unique_lap_driver_pairs=int(stats.get("unique_lap_driver_pairs", 0)),
                    duplicate_lap_driver_pairs=int(stats.get("duplicate_lap_driver_pairs", 0)),
                    laps_min=int(stats.get("laps_min", 0)),
                    laps_max=int(stats.get("laps_max", 0)),
                    date_monotonic=int(stats.get("date_monotonic", 0)),
                    validation_ok=1 if prepared_ok else 0,
                    notes="" if prepared_ok else last_error,
                )
            )

            if not prepared_ok and not args.continue_on_error:
                _write_reports(report_rows, report_json, report_csv)
                raise RuntimeError(f"failed to prepare {year} {race}: {last_error}")

            if idx < len(races):
                time.sleep(max(0.0, args.sleep_between_races))

    _write_reports(report_rows, report_json, report_csv)
    failed = [row for row in report_rows if row.status == "failed"]
    if failed:
        raise SystemExit(f"completed with failures: {len(failed)} races failed")

    logging.info("All requested races prepared and validated successfully.")


if __name__ == "__main__":
    main()
