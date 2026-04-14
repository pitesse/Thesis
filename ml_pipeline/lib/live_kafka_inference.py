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


def _source_year_from_race(race: str) -> int:
    token = str(race).split(" :: ", maxsplit=1)[0]
    return _to_int(token, default=0)


def _build_drop_zone_lookup(path: Path) -> dict[tuple[str, str, int], tuple[int, float]]:
    if not path.exists():
        raise FileNotFoundError(f"drop_zones file not found: {path}")

    df = pd.read_json(path, lines=True)
    required = {"race", "driver", "lapNumber", "positionsLost", "gapToPhysicalCar"}
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"drop_zones file missing required columns: {missing}")

    work = df.copy()
    work["race"] = work["race"].astype(str)
    work["driver"] = work["driver"].astype(str)
    work["lapNumber"] = pd.to_numeric(work["lapNumber"], errors="coerce")
    work["positionsLost"] = pd.to_numeric(work["positionsLost"], errors="coerce")
    work["gapToPhysicalCar"] = pd.to_numeric(work["gapToPhysicalCar"], errors="coerce")
    work = work[work["lapNumber"].notna()].copy()
    work["lapNumber"] = work["lapNumber"].astype(int)
    work.sort_values(by=["race", "driver", "lapNumber"], inplace=True)
    work.drop_duplicates(subset=["race", "driver", "lapNumber"], keep="last", inplace=True)

    lookup: dict[tuple[str, str, int], tuple[int, float]] = {}
    for row in work.itertuples(index=False):
        lap = int(getattr(row, "lapNumber"))
        key = (str(getattr(row, "race")), str(getattr(row, "driver")), lap)
        positions_lost = _to_int(getattr(row, "positionsLost"), default=0)
        gap_to_physical_car = _to_float(getattr(row, "gapToPhysicalCar"), default=STRUCTURAL_GAP_FILL)
        lookup[key] = (positions_lost, STRUCTURAL_GAP_FILL if gap_to_physical_car is None else float(gap_to_physical_car))

    return lookup


class OnlineFeatureEngineer:
    """builds serving-time features aligned with prepare_ml_dataset semantics."""

    def __init__(self, drop_zone_lookup: dict[tuple[str, str, int], tuple[int, float]] | None = None) -> None:
        self._state: dict[tuple[str, str], DriverStintState] = {}
        self._drop_zone_lookup = drop_zone_lookup or {}

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

        has_gap_ahead = gap_ahead_raw is not None
        has_gap_behind = gap_behind_raw is not None
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
            has_drop_zone_data = False
        else:
            positions_lost = int(drop_zone_values[0])
            gap_to_physical_car = float(drop_zone_values[1])
            has_drop_zone_data = True

        source_year = _source_year_from_race(race)

        feature_row = {
            "position": _to_int(event.get("position"), default=0),
            "tyreLife": _to_int(event.get("tyreLife"), default=0),
            "trackTemp": _to_float(event.get("trackTemp"), default=np.nan),
            "airTemp": _to_float(event.get("airTemp"), default=np.nan),
            "humidity": _to_float(event.get("humidity"), default=np.nan),
            "rainfall": bool(event.get("rainfall")) if event.get("rainfall") is not None else False,
            "speedTrap": _to_float(event.get("speedTrap"), default=np.nan),
            "team": str(event.get("team", "")),
            "gapAhead": gap_ahead_value,
            "gapBehind": gap_behind_value,
            "lapTime": _to_float(event.get("lapTime"), default=np.nan),
            "pitLoss": _to_float(event.get("pitLoss"), default=np.nan),
            "hasGapAhead": has_gap_ahead,
            "hasGapBehind": has_gap_behind,
            "pace_drop_ratio": pace_drop_ratio,
            "tire_life_ratio": tire_life_ratio,
            "pace_trend": float(pace_trend),
            "gapAhead_trend": float(gap_ahead_trend),
            "positions_lost": int(positions_lost),
            "gap_to_physical_car": float(gap_to_physical_car),
            "has_drop_zone_data": bool(has_drop_zone_data),
            "_source_year": int(source_year),
        }

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

    drop_zone_lookup: dict[tuple[str, str, int], tuple[int, float]] | None = None
    if args.drop_zones:
        drop_zone_lookup = _build_drop_zone_lookup(Path(args.drop_zones))

    feature_engineer = OnlineFeatureEngineer(drop_zone_lookup=drop_zone_lookup)
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
