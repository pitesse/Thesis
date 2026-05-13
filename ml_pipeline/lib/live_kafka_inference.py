"""stream ml inference consumer.

consumes flink-generated ml feature rows from kafka and publishes per-lap
probability predictions to a dedicated kafka topic.

this script keeps serving-time feature engineering aligned with the offline
training dataset contract to reduce training-serving skew.
"""

from __future__ import annotations

import argparse
from collections import deque
import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
try:
    from .race_metadata import get_scheduled_laps, race_name_without_year_prefix
except ImportError:
    from race_metadata import get_scheduled_laps, race_name_without_year_prefix

try:
    from kafka import KafkaConsumer, KafkaProducer  # type: ignore[import-not-found]
except ImportError as exc:  # pragma: no cover
    raise RuntimeError(
        "missing dependency 'kafka' (install kafka-python-ng from requirements.txt)"
    ) from exc

DEFAULT_BOOTSTRAP = "kafka:29092"
DEFAULT_INPUT_TOPIC = "f1-ml-features"
DEFAULT_OUTPUT_TOPIC = "f1-ml-predictions"
DEFAULT_GROUP_ID = "f1-ml-inference-consumer"
DEFAULT_MODEL_BUNDLE = "data_lake/models/pit_strategy_serving_bundle.joblib"
DEFAULT_DROP_ZONES = ""
DEFAULT_THRESHOLD = 0.50
STRUCTURAL_GAP_FILL = 999.0
DEFAULT_COMPOUND_STINT = 30.0

COMPOUND_MAX_STINT = {
    "SOFT": 18.0,
    "MEDIUM": 30.0,
    "HARD": 40.0,
    "WET": 25.0,
    "INTERMEDIATE": 25.0,
}

TRACK_AGNOSTIC_OFF = "off"
TRACK_AGNOSTIC_V1 = "track_agnostic_v1"
TRACK_PERCENTAGE_V1 = "track_percentage_v1"
TRACK_PERCENTAGE_TEAM_V1 = "track_percentage_team_v1"
TRACK_PERCENTAGE_RACE_TEAM_V1 = "track_percentage_race_team_v1"
TRACK_AGNOSTIC_AUTO = "auto"
TRACK_AGNOSTIC_MODES = {
    TRACK_AGNOSTIC_OFF,
    TRACK_AGNOSTIC_V1,
    TRACK_PERCENTAGE_V1,
    TRACK_PERCENTAGE_TEAM_V1,
    TRACK_PERCENTAGE_RACE_TEAM_V1,
}
TRACK_AGNOSTIC_ARG_CHOICES = {TRACK_AGNOSTIC_AUTO, *TRACK_AGNOSTIC_MODES}
TRACK_AGNOSTIC_SOURCE_FEATURES = (
    "trackTemp",
    "airTemp",
    "humidity",
    "speedTrap",
    "lapTime",
)


def _normalize_label(value: object) -> str:
    if value is None:
        return ""

    text = str(value).strip()
    if text == "" or text.lower() == "nan" or text == "<NA>":
        return ""
    return text.upper().replace(" ", "_")


def _to_float(value: object, default: float | None = None) -> float | None:
    if value is None:
        return default

    if isinstance(value, (int, float, np.integer, np.floating)):
        num = float(value)
    elif isinstance(value, str):
        try:
            num = float(value)
        except ValueError:
            return default
    else:
        return default

    if math.isnan(num):
        return default
    return num


def _to_int(value: object, default: int = 0) -> int:
    if value is None:
        return default

    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        if math.isnan(float(value)):
            return default
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value))
        except ValueError:
            return default

    return default


@dataclass
class DriverStintState:
    prev_tyre_life: float | None = None
    prev_compound_norm: str = ""
    stint_best_lap_so_far: float | None = None
    stint_lap_times: dict[int, float] = field(default_factory=dict)
    pace_drop_history: deque[float] = field(default_factory=lambda: deque(maxlen=3))
    gap_ahead_history: deque[float] = field(default_factory=lambda: deque(maxlen=3))
    last_lap_number: int | None = None


@dataclass
class RaceTrackAgnosticState:
    stats: dict[str, dict[str, float]] = field(default_factory=dict)
    seen_driver_lap: dict[tuple[str, int], dict[str, float | None]] = field(default_factory=dict)
    driver_lap_times: dict[str, dict[int, float]] = field(default_factory=dict)
    team_lap_driver_times: dict[tuple[str, int], dict[str, float]] = field(default_factory=dict)
    race_lap_driver_times: dict[int, dict[str, float]] = field(default_factory=dict)
    pct_seen_driver_lap: dict[tuple[str, int], tuple[str, float]] = field(default_factory=dict)


def _source_year_from_race(race: str) -> int:
    token = str(race).split(" :: ", maxsplit=1)[0]
    return _to_int(token, default=0)


def _build_drop_zone_lookup(
    path: Path,
) -> dict[tuple[str, str, int], tuple[int, float, int, int, str]]:
    if not path.exists():
        raise FileNotFoundError(f"drop_zones file not found: {path}")

    df = pd.read_json(path, lines=True)
    required = {
        "race",
        "driver",
        "lapNumber",
        "positionsLost",
        "gapToPhysicalCar",
        "emergencePosition",
        "physicalCarTyreLife",
        "dropZoneStatus",
    }
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"drop_zones file missing required columns: {missing}")

    work = df.copy()
    work["race"] = work["race"].astype(str)
    work["driver"] = work["driver"].astype(str)
    work["lapNumber"] = pd.to_numeric(work["lapNumber"], errors="coerce")
    work["positionsLost"] = pd.to_numeric(work["positionsLost"], errors="coerce")
    work["gapToPhysicalCar"] = pd.to_numeric(work["gapToPhysicalCar"], errors="coerce")
    work["emergencePosition"] = pd.to_numeric(work["emergencePosition"], errors="coerce")
    work["physicalCarTyreLife"] = pd.to_numeric(work["physicalCarTyreLife"], errors="coerce")
    work["dropZoneStatus"] = work["dropZoneStatus"].astype(str)
    work = work[work["lapNumber"].notna()].copy()
    work["lapNumber"] = work["lapNumber"].astype(int)
    work.sort_values(by=["race", "driver", "lapNumber"], inplace=True)
    work.drop_duplicates(subset=["race", "driver", "lapNumber"], keep="last", inplace=True)

    lookup: dict[tuple[str, str, int], tuple[int, float, int, int, str]] = {}
    for row in work.itertuples(index=False):
        lap = int(getattr(row, "lapNumber"))
        key = (str(getattr(row, "race")), str(getattr(row, "driver")), lap)
        positions_lost = _to_int(getattr(row, "positionsLost"), default=0)
        gap_to_physical_car = _to_float(getattr(row, "gapToPhysicalCar"), default=STRUCTURAL_GAP_FILL)
        emergence_position = _to_int(getattr(row, "emergencePosition"), default=0)
        physical_car_tyre_life = _to_int(getattr(row, "physicalCarTyreLife"), default=-1)
        drop_zone_status = _normalize_label(getattr(row, "dropZoneStatus"))
        if not drop_zone_status:
            drop_zone_status = "INSUFFICIENT_GAP_CONTEXT"
        lookup[key] = (
            positions_lost,
            STRUCTURAL_GAP_FILL if gap_to_physical_car is None else float(gap_to_physical_car),
            emergence_position,
            physical_car_tyre_life,
            drop_zone_status,
        )

    return lookup


class OnlineFeatureEngineer:
    """builds serving-time features aligned with prepare_ml_dataset semantics."""

    def __init__(
        self,
        drop_zone_lookup: dict[tuple[str, str, int], tuple[int, float, int, int, str]] | None = None,
        track_agnostic_mode: str = TRACK_AGNOSTIC_OFF,
    ) -> None:
        if track_agnostic_mode not in TRACK_AGNOSTIC_MODES:
            raise ValueError(
                f"unsupported track_agnostic_mode={track_agnostic_mode!r}; "
                f"expected one of {sorted(TRACK_AGNOSTIC_MODES)}"
            )
        self._state: dict[tuple[str, str], DriverStintState] = {}
        self._drop_zone_lookup = drop_zone_lookup or {}
        self._track_agnostic_mode = track_agnostic_mode
        self._race_track_stats: dict[str, RaceTrackAgnosticState] = {}

    @staticmethod
    def _is_valid_track_value(value: float | None) -> bool:
        return value is not None and np.isfinite(float(value))

    def _track_stats_remove(
        self,
        race_state: RaceTrackAgnosticState,
        prior_values: dict[str, float | None],
    ) -> None:
        for feature_name, value in prior_values.items():
            if not self._is_valid_track_value(value):
                continue
            feature_stats = race_state.stats.get(feature_name)
            if feature_stats is None:
                continue
            feature_stats["count"] = max(0.0, float(feature_stats["count"]) - 1.0)
            feature_stats["sum"] = float(feature_stats["sum"]) - float(value)
            feature_stats["sum_sq"] = float(feature_stats["sum_sq"]) - (float(value) ** 2)

    def _track_stats_observe(
        self,
        race_state: RaceTrackAgnosticState,
        current_values: dict[str, float | None],
    ) -> None:
        for feature_name, value in current_values.items():
            if not self._is_valid_track_value(value):
                continue
            feature_stats = race_state.stats.setdefault(
                feature_name,
                {"count": 0.0, "sum": 0.0, "sum_sq": 0.0},
            )
            feature_stats["count"] = float(feature_stats["count"]) + 1.0
            feature_stats["sum"] = float(feature_stats["sum"]) + float(value)
            feature_stats["sum_sq"] = float(feature_stats["sum_sq"]) + (float(value) ** 2)

    def _track_stats_z(
        self,
        race_state: RaceTrackAgnosticState,
        current_values: dict[str, float | None],
    ) -> dict[str, float]:
        output: dict[str, float] = {}
        for feature_name, value in current_values.items():
            col = f"{feature_name}_expanding_z"
            if not self._is_valid_track_value(value):
                output[col] = 0.0
                continue
            feature_stats = race_state.stats.get(feature_name)
            if feature_stats is None:
                output[col] = 0.0
                continue
            count = float(feature_stats["count"])
            if count <= 1.0:
                output[col] = 0.0
                continue
            mean = float(feature_stats["sum"]) / count
            var = (float(feature_stats["sum_sq"]) / count) - (mean**2)
            var = max(var, 0.0)
            std = float(np.sqrt(var))
            if std <= 1e-9:
                output[col] = 0.0
                continue
            output[col] = float((float(value) - mean) / std)
        return output

    @staticmethod
    def _clip_ratio(value: float | None, *, default: float = 1.0) -> float:
        if value is None or not np.isfinite(value):
            return float(default)
        return float(np.clip(value, 0.5, 2.0))

    @staticmethod
    def _mean(values: list[float]) -> float | None:
        if not values:
            return None
        return float(np.mean(np.asarray(values, dtype=float)))

    def _driver_prev_lap_mean(
        self,
        race_state: RaceTrackAgnosticState,
        driver: str,
        lap_number: int,
    ) -> float | None:
        lap_map = race_state.driver_lap_times.get(driver, {})
        prior_values = [
            float(value)
            for lap, value in lap_map.items()
            if lap < lap_number and np.isfinite(float(value))
        ]
        return self._mean(prior_values)

    def _team_prev_lapbucket_mean(
        self,
        race_state: RaceTrackAgnosticState,
        team: str,
        lap_number: int,
    ) -> float | None:
        lapbucket_means: list[float] = []
        for (team_name, lap), driver_values in race_state.team_lap_driver_times.items():
            if team_name != team or lap >= lap_number:
                continue
            values = [float(v) for v in driver_values.values() if np.isfinite(float(v))]
            lap_mean = self._mean(values)
            if lap_mean is not None:
                lapbucket_means.append(lap_mean)
        return self._mean(lapbucket_means)

    def _race_prev_lapbucket_mean(
        self,
        race_state: RaceTrackAgnosticState,
        lap_number: int,
    ) -> float | None:
        lapbucket_means: list[float] = []
        for lap, driver_values in race_state.race_lap_driver_times.items():
            if lap >= lap_number:
                continue
            values = [float(v) for v in driver_values.values() if np.isfinite(float(v))]
            lap_mean = self._mean(values)
            if lap_mean is not None:
                lapbucket_means.append(lap_mean)
        return self._mean(lapbucket_means)

    def transform(self, event: dict[str, Any]) -> dict[str, Any]:
        race = str(event.get("race", ""))
        driver = str(event.get("driver", ""))
        lap_number = _to_int(event.get("lapNumber"), default=0)
        compound = event.get("compound", "")
        compound_norm = _normalize_label(compound)

        tyre_life = _to_float(event.get("tyreLife"))
        lap_time = _to_float(event.get("lapTime"))

        key = (race, driver)
        state = self._state.get(key, DriverStintState())
        duplicate_lap_event = (
            state.last_lap_number is not None and lap_number == state.last_lap_number
        )

        stint_break = (
            state.prev_tyre_life is None
            or tyre_life is None
            or tyre_life < state.prev_tyre_life
            or compound_norm != state.prev_compound_norm
        )

        if stint_break:
            state.stint_lap_times.clear()

        if lap_time is not None and lap_time > 0 and lap_number > 0:
            # mirror offline keep-last dedup semantics for same race-driver-lap keys.
            state.stint_lap_times[lap_number] = float(lap_time)

        if state.stint_lap_times:
            state.stint_best_lap_so_far = min(state.stint_lap_times.values())
        else:
            state.stint_best_lap_so_far = None

        if (
            lap_time is not None
            and lap_time > 0
            and state.stint_best_lap_so_far is not None
            and state.stint_best_lap_so_far > 0
        ):
            pace_drop_ratio = lap_time / state.stint_best_lap_so_far
        else:
            pace_drop_ratio = 1.0
        pace_drop_ratio = float(np.clip(pace_drop_ratio, 0.5, 2.0))

        expected_max_stint = COMPOUND_MAX_STINT.get(compound_norm, DEFAULT_COMPOUND_STINT)
        if tyre_life is not None and expected_max_stint > 0:
            tire_life_ratio = tyre_life / expected_max_stint
        else:
            tire_life_ratio = 0.0
        tire_life_ratio = float(np.clip(tire_life_ratio, 0.0, 2.0))

        gap_ahead_raw = _to_float(event.get("gapAhead"))
        gap_behind_raw = _to_float(event.get("gapBehind"))

        gap_ahead_value = gap_ahead_raw if gap_ahead_raw is not None else STRUCTURAL_GAP_FILL
        gap_behind_value = gap_behind_raw if gap_behind_raw is not None else STRUCTURAL_GAP_FILL

        pace_history_values = list(state.pace_drop_history)
        gap_history_values = list(state.gap_ahead_history)
        if duplicate_lap_event:
            if pace_history_values:
                pace_history_values = pace_history_values[:-1]
            if gap_history_values:
                gap_history_values = gap_history_values[:-1]

        # mirror offline shift(2): compare against the value from two laps earlier.
        if len(pace_history_values) >= 2:
            pace_trend = pace_drop_ratio - pace_history_values[-2]
        else:
            pace_trend = 0.0

        if len(gap_history_values) >= 2:
            gap_ahead_trend = gap_ahead_value - gap_history_values[-2]
        else:
            gap_ahead_trend = 0.0

        drop_zone_key = (race, driver, lap_number)
        drop_zone_values = self._drop_zone_lookup.get(drop_zone_key)
        if drop_zone_values is None:
            positions_lost = 0
            gap_to_physical_car = STRUCTURAL_GAP_FILL
            emergence_position = 0
            physical_car_tyre_life = -1
            drop_zone_status = "INSUFFICIENT_GAP_CONTEXT"
        else:
            positions_lost = int(drop_zone_values[0])
            gap_to_physical_car = float(drop_zone_values[1])
            emergence_position = int(drop_zone_values[2])
            physical_car_tyre_life = int(drop_zone_values[3])
            drop_zone_status = str(drop_zone_values[4]).strip() or "INSUFFICIENT_GAP_CONTEXT"
        has_drop_zone_data = drop_zone_status == "LOSS_ESTIMATED"

        source_year = _source_year_from_race(race)
        track_temp = _to_float(event.get("trackTemp"), default=np.nan)
        air_temp = _to_float(event.get("airTemp"), default=np.nan)
        humidity = _to_float(event.get("humidity"), default=np.nan)
        speed_trap = _to_float(event.get("speedTrap"), default=np.nan)
        lap_time_num = _to_float(event.get("lapTime"), default=np.nan)
        team_name = str(event.get("team", ""))
        race_name = race_name_without_year_prefix(race)

        track_agnostic_cols: dict[str, float] = {}
        race_state = self._race_track_stats.setdefault(
            race,
            RaceTrackAgnosticState(),
        )

        if self._track_agnostic_mode == TRACK_AGNOSTIC_V1:
            driver_lap_key = (driver, lap_number)
            prior_values = race_state.seen_driver_lap.get(driver_lap_key)
            if prior_values is not None:
                self._track_stats_remove(race_state, prior_values)
            current_track_values: dict[str, float | None] = {
                "trackTemp": track_temp,
                "airTemp": air_temp,
                "humidity": humidity,
                "speedTrap": speed_trap,
                "lapTime": lap_time_num,
            }
            track_agnostic_cols = self._track_stats_z(race_state, current_track_values)
            self._track_stats_observe(race_state, current_track_values)
            race_state.seen_driver_lap[driver_lap_key] = current_track_values
        elif self._track_agnostic_mode in {
            TRACK_PERCENTAGE_V1,
            TRACK_PERCENTAGE_TEAM_V1,
            TRACK_PERCENTAGE_RACE_TEAM_V1,
        }:
            try:
                scheduled_race_laps = float(get_scheduled_laps(source_year, race_name))
            except ValueError as exc:
                raise ValueError(
                    "missing scheduled-laps metadata for live percentage mode: "
                    f"year={source_year}, race={race_name!r}"
                ) from exc

            if scheduled_race_laps <= 0:
                raise ValueError(
                    f"invalid scheduled laps for year={source_year}, race={race_name!r}"
                )

            race_progress_pct = float(np.clip(float(lap_number) / scheduled_race_laps, 0.0, 1.5))
            driver_prev_mean = self._driver_prev_lap_mean(race_state, driver, lap_number)

            driver_ratio = None
            if lap_time_num is not None and np.isfinite(lap_time_num) and lap_time_num > 0 and driver_prev_mean is not None and driver_prev_mean > 0:
                driver_ratio = float(lap_time_num / driver_prev_mean)

            track_agnostic_cols["race_progress_pct"] = race_progress_pct
            track_agnostic_cols["lapTime_vs_driver_prev_mean_pct"] = self._clip_ratio(driver_ratio)

            if self._track_agnostic_mode in {
                TRACK_PERCENTAGE_TEAM_V1,
                TRACK_PERCENTAGE_RACE_TEAM_V1,
            }:
                team_prev_mean = self._team_prev_lapbucket_mean(race_state, team_name, lap_number)
                team_ratio = None
                if lap_time_num is not None and np.isfinite(lap_time_num) and lap_time_num > 0 and team_prev_mean is not None and team_prev_mean > 0:
                    team_ratio = float(lap_time_num / team_prev_mean)
                track_agnostic_cols["lapTime_vs_team_prev_lapbucket_mean_pct"] = self._clip_ratio(team_ratio)

            if self._track_agnostic_mode == TRACK_PERCENTAGE_RACE_TEAM_V1:
                race_prev_mean = self._race_prev_lapbucket_mean(race_state, lap_number)
                race_ratio = None
                if lap_time_num is not None and np.isfinite(lap_time_num) and lap_time_num > 0 and race_prev_mean is not None and race_prev_mean > 0:
                    race_ratio = float(lap_time_num / race_prev_mean)
                track_agnostic_cols["lapTime_vs_race_prev_lapbucket_mean_pct"] = self._clip_ratio(race_ratio)

        feature_row = {
            "position": _to_int(event.get("position"), default=0),
            "tyreLife": _to_int(event.get("tyreLife"), default=0),
            "trackTemp": track_temp,
            "airTemp": air_temp,
            "humidity": humidity,
            "rainfall": bool(event.get("rainfall")) if event.get("rainfall") is not None else False,
            "speedTrap": speed_trap,
            "team": team_name,
            "gapAhead": gap_ahead_value,
            "gapBehind": gap_behind_value,
            "lapTime": lap_time_num,
            "pitLoss": _to_float(event.get("pitLoss"), default=np.nan),
            "pace_drop_ratio": pace_drop_ratio,
            "tire_life_ratio": tire_life_ratio,
            "pace_trend": float(pace_trend),
            "gapAhead_trend": float(gap_ahead_trend),
            "positions_lost": int(positions_lost),
            "gap_to_physical_car": float(gap_to_physical_car),
            "emergence_position": int(emergence_position),
            "physical_car_tyre_life": int(physical_car_tyre_life),
            "drop_zone_status": drop_zone_status,
            "has_drop_zone_data": bool(has_drop_zone_data),
            "_source_year": int(source_year),
        }
        feature_row.update(track_agnostic_cols)

        # update state after feature emission so the current lap never informs itself.
        if duplicate_lap_event and len(state.pace_drop_history) > 0:
            state.pace_drop_history[-1] = pace_drop_ratio
        else:
            state.pace_drop_history.append(pace_drop_ratio)

        if duplicate_lap_event and len(state.gap_ahead_history) > 0:
            state.gap_ahead_history[-1] = gap_ahead_value
        else:
            state.gap_ahead_history.append(gap_ahead_value)

        state.prev_tyre_life = tyre_life
        state.prev_compound_norm = compound_norm
        state.last_lap_number = lap_number
        self._state[key] = state

        if (
            self._track_agnostic_mode in {
                TRACK_PERCENTAGE_V1,
                TRACK_PERCENTAGE_TEAM_V1,
                TRACK_PERCENTAGE_RACE_TEAM_V1,
            }
            and lap_time_num is not None
            and np.isfinite(lap_time_num)
            and lap_time_num > 0
            and lap_number > 0
        ):
            driver_lap_map = race_state.driver_lap_times.setdefault(driver, {})
            driver_lap_map[lap_number] = float(lap_time_num)

            if self._track_agnostic_mode in {
                TRACK_PERCENTAGE_TEAM_V1,
                TRACK_PERCENTAGE_RACE_TEAM_V1,
            }:
                pct_key = (driver, lap_number)
                prev_pct_meta = race_state.pct_seen_driver_lap.get(pct_key)
                if prev_pct_meta is not None:
                    prev_team, _ = prev_pct_meta
                    if prev_team != team_name:
                        prev_team_key = (prev_team, lap_number)
                        prev_team_map = race_state.team_lap_driver_times.get(prev_team_key)
                        if prev_team_map is not None:
                            prev_team_map.pop(driver, None)
                            if len(prev_team_map) == 0:
                                race_state.team_lap_driver_times.pop(prev_team_key, None)

                team_key = (team_name, lap_number)
                team_driver_map = race_state.team_lap_driver_times.setdefault(team_key, {})
                team_driver_map[driver] = float(lap_time_num)
                race_state.pct_seen_driver_lap[pct_key] = (team_name, float(lap_time_num))

            if self._track_agnostic_mode == TRACK_PERCENTAGE_RACE_TEAM_V1:
                race_driver_map = race_state.race_lap_driver_times.setdefault(lap_number, {})
                race_driver_map[driver] = float(lap_time_num)

        return feature_row


def _build_model_matrix(feature_row: dict[str, Any], feature_columns: list[str]) -> pd.DataFrame:
    df = pd.DataFrame([feature_row])

    object_cols = df.select_dtypes(include=["object", "category", "string"]).columns.tolist()
    if object_cols:
        X = pd.get_dummies(df, columns=object_cols, dummy_na=True)
    else:
        X = df

    bool_cols = X.select_dtypes(include=["bool"]).columns
    if len(bool_cols) > 0:
        X[bool_cols] = X[bool_cols].astype(int)

    # enforce exact training schema and zero-fill unseen dummies for stable inference.
    X = X.reindex(columns=feature_columns, fill_value=0)
    return X


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="serve ml predictions from flink feature topic")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP, help="kafka bootstrap server")
    parser.add_argument("--input-topic", default=DEFAULT_INPUT_TOPIC, help="source topic with ml features")
    parser.add_argument("--output-topic", default=DEFAULT_OUTPUT_TOPIC, help="destination topic for predictions")
    parser.add_argument("--group-id", default=DEFAULT_GROUP_ID, help="kafka consumer group id")
    parser.add_argument("--model-bundle", default=DEFAULT_MODEL_BUNDLE, help="joblib path for serving bundle")
    parser.add_argument(
        "--drop-zones",
        default=DEFAULT_DROP_ZONES,
        help="optional drop_zones jsonl path for serving-time enrichment",
    )
    parser.add_argument(
        "--track-agnostic-mode",
        choices=sorted(TRACK_AGNOSTIC_ARG_CHOICES),
        default=TRACK_AGNOSTIC_AUTO,
        help="optional race-relative causal normalization mode",
    )
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD, help="decision threshold")
    parser.add_argument("--max-messages", type=int, default=0, help="optional max number of consumed messages")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    bundle_path = Path(args.model_bundle)
    if not bundle_path.exists():
        raise FileNotFoundError(f"model bundle not found: {bundle_path}")

    bundle = joblib.load(bundle_path)
    model = bundle["model"]
    calibrator = bundle.get("calibrator")
    feature_columns = list(bundle["feature_columns"])
    model_name = str(bundle.get("model_name", "xgboost"))
    model_version = str(bundle.get("model_version", "v1"))
    threshold = float(bundle.get("threshold", args.threshold))
    bundle_track_agnostic = str(bundle.get("track_agnostic_mode", TRACK_AGNOSTIC_OFF))
    resolved_track_agnostic_mode = (
        bundle_track_agnostic
        if args.track_agnostic_mode == TRACK_AGNOSTIC_AUTO
        else args.track_agnostic_mode
    )

    consumer = KafkaConsumer(
        args.input_topic,
        bootstrap_servers=args.bootstrap,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=args.group_id,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=1000,
    )

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
    )

    drop_zone_lookup: dict[
        tuple[str, str, int],
        tuple[int, float, int, int, str],
    ] | None = None
    if args.drop_zones:
        drop_zone_lookup = _build_drop_zone_lookup(Path(args.drop_zones))

    feature_engineer = OnlineFeatureEngineer(
        drop_zone_lookup=drop_zone_lookup,
        track_agnostic_mode=resolved_track_agnostic_mode,
    )
    consumed = 0

    try:
        while True:
            records = consumer.poll(timeout_ms=500, max_records=200)
            if not records:
                continue

            for _, batch in records.items():
                for record in batch:
                    event = record.value
                    feature_row = feature_engineer.transform(event)
                    matrix = _build_model_matrix(feature_row, feature_columns)

                    proba = float(model.predict_proba(matrix)[:, 1][0])
                    if calibrator is not None:
                        proba = float(calibrator.predict(np.array([proba]))[0])

                    pred = int(proba >= threshold)
                    prediction_payload = {
                        "type": "ML_PIT_PREDICTION",
                        "race": event.get("race"),
                        "driver": event.get("driver"),
                        "lapNumber": event.get("lapNumber"),
                        "prediction": pred,
                        "predictionProba": round(proba, 6),
                        "threshold": threshold,
                        "modelName": model_name,
                        "modelVersion": model_version,
                        "generatedAt": datetime.now(timezone.utc).isoformat(),
                    }

                    producer.send(args.output_topic, value=prediction_payload)
                    consumed += 1

                    if args.max_messages > 0 and consumed >= args.max_messages:
                        producer.flush()
                        return
    finally:
        producer.flush()
        producer.close()
        consumer.close()


if __name__ == "__main__":
    main()
