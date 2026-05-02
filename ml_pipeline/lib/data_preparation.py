"""prepare a leakage-safe ml training dataset from ml_features and pit_evals.

method notes:
- this temporal alignment follows brookshire 2024 and roberts et al 2017,
  labels for lap k are derived only from pit outcomes in future window [k+1, k+h].
- this class imbalance handling follows elkan 2001,
  use cost-sensitive weighting via scale_pos_weight instead of synthetic oversampling.
- structural null handling keeps boundary semantics explicit,
  p1 has no gapAhead and p20 has no gapBehind by race physics, not missing collection.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

try:
    from .race_metadata import get_scheduled_laps, race_name_without_year_prefix
except ImportError:
    from race_metadata import get_scheduled_laps, race_name_without_year_prefix


DEFAULT_DATA_LAKE = "data_lake"
DEFAULT_YEAR = 2023
DEFAULT_SEASON_TAG = "season"
DEFAULT_HORIZON = 2
DEFAULT_OUTPUT = "ml_training_dataset.parquet"
STRUCTURAL_GAP_FILL = 999.0
DEFAULT_COMPOUND_STINT = 30.0

COMPOUND_MAX_STINT = {
    "SOFT": 18.0,
    "MEDIUM": 30.0,
    "HARD": 40.0,
    "WET": 25.0,
    "INTERMEDIATE": 25.0,
}

POSITIVE_RESULTS = {
    "SUCCESS_UNDERCUT",
    "SUCCESS_OVERCUT",
    "SUCCESS_DEFEND",
    "SUCCESS_FREE_STOP",
    "OFFSET_ADVANTAGE",
}

REQUIRED_FEATURE_COLUMNS = [
    "race",
    "driver",
    "lapNumber",
    "compound",
    "tyreLife",
    "lapTime",
    "gapAhead",
    "gapBehind",
]

REQUIRED_PIT_EVAL_COLUMNS = [
    "race",
    "driver",
    "pitLapNumber",
    "result",
]

REQUIRED_DROP_ZONE_COLUMNS = [
    "race",
    "driver",
    "lapNumber",
    "positionsLost",
    "gapToPhysicalCar",
    "emergencePosition",
    "physicalCarTyreLife",
    "dropZoneStatus",
]

DROP_ZONE_STATUS_LOSS_ESTIMATED = "LOSS_ESTIMATED"
DROP_ZONE_STATUS_NO_LOSS_FEASIBLE = "NO_LOSS_FEASIBLE"
DROP_ZONE_STATUS_INELIGIBLE_TYRE_AGE = "INELIGIBLE_TYRE_AGE"
DROP_ZONE_STATUS_INELIGIBLE_TRACK_STATUS = "INELIGIBLE_TRACK_STATUS"
DROP_ZONE_STATUS_INSUFFICIENT_GAP_CONTEXT = "INSUFFICIENT_GAP_CONTEXT"

DROP_ZONE_STATUS_SET = {
    DROP_ZONE_STATUS_LOSS_ESTIMATED,
    DROP_ZONE_STATUS_NO_LOSS_FEASIBLE,
    DROP_ZONE_STATUS_INELIGIBLE_TYRE_AGE,
    DROP_ZONE_STATUS_INELIGIBLE_TRACK_STATUS,
    DROP_ZONE_STATUS_INSUFFICIENT_GAP_CONTEXT,
}

TRACK_AGNOSTIC_OFF = "off"
TRACK_AGNOSTIC_V1 = "track_agnostic_v1"
TRACK_PERCENTAGE_V1 = "track_percentage_v1"
TRACK_PERCENTAGE_TEAM_V1 = "track_percentage_team_v1"
TRACK_PERCENTAGE_RACE_TEAM_V1 = "track_percentage_race_team_v1"
TRACK_AGNOSTIC_MODES = {
    TRACK_AGNOSTIC_OFF,
    TRACK_AGNOSTIC_V1,
    TRACK_PERCENTAGE_V1,
    TRACK_PERCENTAGE_TEAM_V1,
    TRACK_PERCENTAGE_RACE_TEAM_V1,
}


def _causal_expanding_z_by_race(
    frame: pd.DataFrame,
    *,
    source_column: str,
    output_column: str,
) -> None:
    timeline = frame[["race", "driver", "lapNumber"]].copy()
    timeline["_value"] = pd.to_numeric(frame[source_column], errors="coerce")
    timeline["_row_id"] = np.arange(len(frame), dtype=int)

    # Use race timeline order (lap, driver) for race-relative context features.
    timeline.sort_values(
        by=["race", "lapNumber", "driver"],
        kind="mergesort",
        inplace=True,
    )

    past_values = timeline.groupby("race", sort=False)["_value"].shift(1)
    count = (
        past_values.notna()
        .groupby(timeline["race"], sort=False)
        .cumsum()
        .astype(float)
    )
    csum = past_values.groupby(timeline["race"], sort=False).cumsum()
    csum_sq = (past_values**2).groupby(timeline["race"], sort=False).cumsum()

    mean = csum / count
    var = (csum_sq / count) - (mean**2)
    var = var.clip(lower=0.0)
    std = np.sqrt(var)
    std = std.where(std > 1e-9, np.nan)

    z = (timeline["_value"] - mean) / std
    z = z.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    restore = timeline[["_row_id"]].copy()
    restore[output_column] = z.values
    restore.sort_values("_row_id", kind="mergesort", inplace=True)
    frame[output_column] = restore[output_column].to_numpy(dtype=float)


def _clip_pct_feature(
    values: pd.Series,
    *,
    default_value: float = 1.0,
    lower: float = 0.5,
    upper: float = 2.0,
) -> pd.Series:
    return (
        pd.to_numeric(values, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .fillna(default_value)
        .clip(lower=lower, upper=upper)
    )


def _scheduled_laps_series(frame: pd.DataFrame) -> pd.Series:
    years = pd.to_numeric(frame["_source_year"], errors="coerce")
    if years.isna().any():
        raise ValueError("missing `_source_year` values required for scheduled-laps lookup")

    race_names = frame["race"].astype(str).map(race_name_without_year_prefix)
    laps: list[float] = []
    missing: list[tuple[int, str]] = []

    for year, race_name in zip(years.astype(int).tolist(), race_names.tolist(), strict=False):
        try:
            laps.append(float(get_scheduled_laps(year, race_name)))
        except ValueError:
            missing.append((year, race_name))
            laps.append(np.nan)

    if missing:
        uniq = sorted(set(missing))
        preview = ", ".join([f"{year}:{race}" for year, race in uniq[:10]])
        raise ValueError(
            "scheduled laps metadata missing for race/year pairs: "
            f"{preview}"
        )

    result = pd.Series(laps, index=frame.index, dtype=float)
    if (result <= 0).any():
        raise ValueError("scheduled laps metadata contains non-positive values")
    return result


def _apply_track_percentage_features(
    frame: pd.DataFrame,
    *,
    track_agnostic_mode: str,
) -> None:
    lap_time_clean = pd.to_numeric(frame["lap_time_clean"], errors="coerce")
    lap_number = pd.to_numeric(frame["lapNumber"], errors="coerce")

    scheduled_laps = _scheduled_laps_series(frame)
    race_progress = lap_number / scheduled_laps
    frame["race_progress_pct"] = (
        pd.to_numeric(race_progress, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
        .clip(lower=0.0, upper=1.5)
    )

    driver_prev_mean = frame.groupby(["race", "driver"], sort=False)[
        "lap_time_clean"
    ].transform(lambda s: s.shift(1).expanding(min_periods=1).mean())
    driver_ratio = lap_time_clean / pd.to_numeric(driver_prev_mean, errors="coerce")
    frame["lapTime_vs_driver_prev_mean_pct"] = _clip_pct_feature(driver_ratio)

    if track_agnostic_mode in {
        TRACK_PERCENTAGE_TEAM_V1,
        TRACK_PERCENTAGE_RACE_TEAM_V1,
    }:
        team_lap = (
            frame.groupby(["race", "team", "lapNumber"], sort=False, dropna=False)[
                "lap_time_clean"
            ]
            .mean()
            .reset_index(name="_team_lap_mean")
        )
        team_lap.sort_values(
            by=["race", "team", "lapNumber"],
            kind="mergesort",
            inplace=True,
        )
        team_lap["_team_prev_lapbucket_mean"] = team_lap.groupby(
            ["race", "team"], sort=False
        )["_team_lap_mean"].transform(lambda s: s.shift(1).expanding(min_periods=1).mean())

        team_map = team_lap.set_index(["race", "team", "lapNumber"])[
            "_team_prev_lapbucket_mean"
        ]
        team_index = pd.MultiIndex.from_frame(frame[["race", "team", "lapNumber"]])
        team_prev_mean = pd.Series(team_index.map(team_map), index=frame.index, dtype=float)
        team_ratio = lap_time_clean / pd.to_numeric(team_prev_mean, errors="coerce")
        frame["lapTime_vs_team_prev_lapbucket_mean_pct"] = _clip_pct_feature(team_ratio)

    if track_agnostic_mode == TRACK_PERCENTAGE_RACE_TEAM_V1:
        race_lap = (
            frame.groupby(["race", "lapNumber"], sort=False)["lap_time_clean"]
            .mean()
            .reset_index(name="_race_lap_mean")
        )
        race_lap.sort_values(
            by=["race", "lapNumber"],
            kind="mergesort",
            inplace=True,
        )
        race_lap["_race_prev_lapbucket_mean"] = race_lap.groupby(
            ["race"], sort=False
        )["_race_lap_mean"].transform(lambda s: s.shift(1).expanding(min_periods=1).mean())

        race_map = race_lap.set_index(["race", "lapNumber"])["_race_prev_lapbucket_mean"]
        race_index = pd.MultiIndex.from_frame(frame[["race", "lapNumber"]])
        race_prev_mean = pd.Series(race_index.map(race_map), index=frame.index, dtype=float)
        race_ratio = lap_time_clean / pd.to_numeric(race_prev_mean, errors="coerce")
        frame["lapTime_vs_race_prev_lapbucket_mean_pct"] = _clip_pct_feature(race_ratio)


def _apply_track_agnostic_features(
    frame: pd.DataFrame,
    *,
    track_agnostic_mode: str,
) -> None:
    if track_agnostic_mode == TRACK_AGNOSTIC_OFF:
        return
    if track_agnostic_mode not in TRACK_AGNOSTIC_MODES:
        raise ValueError(
            f"unsupported track_agnostic_mode={track_agnostic_mode!r}; "
            f"expected one of {sorted(TRACK_AGNOSTIC_MODES)}"
        )

    if track_agnostic_mode == TRACK_AGNOSTIC_V1:
        transforms = [
            ("trackTemp", "trackTemp_expanding_z"),
            ("airTemp", "airTemp_expanding_z"),
            ("humidity", "humidity_expanding_z"),
            ("speedTrap", "speedTrap_expanding_z"),
            ("lapTime", "lapTime_expanding_z"),
        ]
        for source_column, output_column in transforms:
            if source_column in frame.columns:
                _causal_expanding_z_by_race(
                    frame,
                    source_column=source_column,
                    output_column=output_column,
                )
        return

    _apply_track_percentage_features(
        frame,
        track_agnostic_mode=track_agnostic_mode,
    )

def _duplicate_key_stats(df: pd.DataFrame, key_columns: list[str]) -> tuple[int, int]:
    counts = df.groupby(key_columns, dropna=False).size()
    duplicate_keys = int((counts > 1).sum())
    duplicate_excess = int((counts[counts > 1] - 1).sum())
    return duplicate_keys, duplicate_excess


def _normalize_label(value: object) -> str:
    if value is None:
        return ""

    text = str(value).strip()
    if text == "" or text.lower() == "nan" or text == "<NA>":
        return ""
    return text.upper().replace(" ", "_")


def _latest_jsonl(data_lake: Path, stream: str, year: int, season_tag: str) -> Path:
    pattern = f"{stream}_{year}_{season_tag}_*.jsonl"
    matches = list(data_lake.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"no files found for pattern: {data_lake / pattern}")
    return max(matches, key=lambda p: p.stat().st_mtime)


def _load_jsonl(path: Path) -> pd.DataFrame:
    df = pd.read_json(path, lines=True)
    if df.empty:
        raise ValueError(f"input file is empty: {path}")
    return df


def _require_columns(df: pd.DataFrame, required: Iterable[str], name: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")


def _prepare_features(
    features: pd.DataFrame,
    drop_zones: pd.DataFrame | None = None,
    source_year_fallback: int = DEFAULT_YEAR,
    track_agnostic_mode: str = TRACK_AGNOSTIC_OFF,
) -> pd.DataFrame:
    _require_columns(features, REQUIRED_FEATURE_COLUMNS, "ml_features")

    work = features.copy()
    work["race"] = work["race"].astype(str)
    work["driver"] = work["driver"].astype(str)

    # keep season context explicit for multi-season calibration stability.
    parsed_year = pd.to_numeric(
        work["race"].str.split(" :: ", n=1).str[0], errors="coerce"
    )
    work["_source_year"] = parsed_year.fillna(int(source_year_fallback)).astype(int)

    work["lapNumber"] = pd.to_numeric(work["lapNumber"], errors="coerce")
    work = work[work["lapNumber"].notna()].copy()
    work["lapNumber"] = work["lapNumber"].astype(int)

    dedup_keys_before, dedup_excess_before = _duplicate_key_stats(
        work, ["race", "driver", "lapNumber"]
    )
    work = work.drop_duplicates(
        subset=["race", "driver", "lapNumber"],
        keep="last",
    ).copy()
    dedup_keys_after, dedup_excess_after = _duplicate_key_stats(
        work, ["race", "driver", "lapNumber"]
    )

    work["compound_norm"] = work["compound"].map(_normalize_label)
    work["tyre_life_num"] = pd.to_numeric(work["tyreLife"], errors="coerce")
    work["lap_time_num"] = pd.to_numeric(work["lapTime"], errors="coerce")

    work["gapAhead"] = pd.to_numeric(work["gapAhead"], errors="coerce")
    work["gapBehind"] = pd.to_numeric(work["gapBehind"], errors="coerce")
    work["gapAhead"] = work["gapAhead"].fillna(STRUCTURAL_GAP_FILL)
    work["gapBehind"] = work["gapBehind"].fillna(STRUCTURAL_GAP_FILL)

    if drop_zones is not None:
        work = work.merge(
            drop_zones,
            how="left",
            on=["race", "driver", "lapNumber"],
            validate="m:1",
            indicator="_drop_zone_join",
        )
        work.drop(columns=["_drop_zone_join"], inplace=True)
    else:
        work["positions_lost"] = 0
        work["gap_to_physical_car"] = STRUCTURAL_GAP_FILL
        work["emergence_position"] = 0
        work["physical_car_tyre_life"] = -1
        work["drop_zone_status"] = DROP_ZONE_STATUS_INSUFFICIENT_GAP_CONTEXT

    if "positions_lost" not in work.columns:
        work["positions_lost"] = 0
    if "gap_to_physical_car" not in work.columns:
        work["gap_to_physical_car"] = STRUCTURAL_GAP_FILL
    if "emergence_position" not in work.columns:
        work["emergence_position"] = 0
    if "physical_car_tyre_life" not in work.columns:
        work["physical_car_tyre_life"] = -1
    if "drop_zone_status" not in work.columns:
        work["drop_zone_status"] = DROP_ZONE_STATUS_INSUFFICIENT_GAP_CONTEXT

    work["positions_lost"] = (
        pd.to_numeric(work["positions_lost"], errors="coerce").fillna(0).astype(int)
    )
    work["gap_to_physical_car"] = (
        pd.to_numeric(work["gap_to_physical_car"], errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .fillna(STRUCTURAL_GAP_FILL)
    )
    work["emergence_position"] = (
        pd.to_numeric(work["emergence_position"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    work["physical_car_tyre_life"] = (
        pd.to_numeric(work["physical_car_tyre_life"], errors="coerce")
        .fillna(-1)
        .astype(int)
    )
    work["drop_zone_status"] = work["drop_zone_status"].map(_normalize_label)
    work.loc[~work["drop_zone_status"].isin(DROP_ZONE_STATUS_SET), "drop_zone_status"] = (
        DROP_ZONE_STATUS_INSUFFICIENT_GAP_CONTEXT
    )
    work["has_drop_zone_data"] = work["drop_zone_status"].eq(
        DROP_ZONE_STATUS_LOSS_ESTIMATED
    )

    work.sort_values(by=["race", "driver", "lapNumber"], inplace=True)

    # this pace proxy follows heilmeier 2020 style decomposition,
    # use best lap so far within the current stint to avoid future leakage.
    prev_tyre_life = work.groupby(["race", "driver"], sort=False)[
        "tyre_life_num"
    ].shift(1)
    prev_compound = work.groupby(["race", "driver"], sort=False)["compound_norm"].shift(
        1
    )
    stint_break = (
        prev_tyre_life.isna()
        | (work["tyre_life_num"] < prev_tyre_life)
        | (work["compound_norm"] != prev_compound)
    )

    work["stint_break"] = stint_break.astype(int)
    work["stint_id"] = (
        work.groupby(["race", "driver"], sort=False)["stint_break"].cumsum().astype(int)
    )

    work["lap_time_clean"] = work["lap_time_num"].where(
        work["lap_time_num"] > 0, np.nan
    )
    work["stint_best_lap_so_far"] = work.groupby(
        ["race", "driver", "stint_id"], sort=False
    )["lap_time_clean"].cummin()
    work["pace_drop_ratio"] = work["lap_time_clean"] / work["stint_best_lap_so_far"]
    work["pace_drop_ratio"] = (
        work["pace_drop_ratio"]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(1.0)
        .clip(lower=0.5, upper=2.0)
    )

    # this tire aging proxy follows degradation ratio framing from cappello and hoegh 2025.
    work["expected_max_tyre_life"] = (
        work["compound_norm"].map(COMPOUND_MAX_STINT).fillna(DEFAULT_COMPOUND_STINT)
    )
    work["tire_life_ratio"] = work["tyre_life_num"] / work["expected_max_tyre_life"]
    work["tire_life_ratio"] = (
        work["tire_life_ratio"]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
        .clip(lower=0.0, upper=2.0)
    )

    # keep momentum causal by using only lagged values within each race-driver stream.
    work["pace_trend"] = work["pace_drop_ratio"] - work.groupby(
        ["race", "driver"], sort=False
    )["pace_drop_ratio"].shift(2)
    work["pace_trend"] = (
        work["pace_trend"].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    )

    work["gapAhead_trend"] = work["gapAhead"] - work.groupby(
        ["race", "driver"], sort=False
    )["gapAhead"].shift(2)
    work["gapAhead_trend"] = (
        work["gapAhead_trend"].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    )

    # Optional race-relative normalization for track-agnostic experiments.
    _apply_track_agnostic_features(
        work,
        track_agnostic_mode=track_agnostic_mode,
    )

    work.drop(
        columns=[
            "compound_norm",
            "tyre_life_num",
            "lap_time_num",
            "stint_break",
            "stint_id",
            "lap_time_clean",
            "stint_best_lap_so_far",
            "expected_max_tyre_life",
        ],
        inplace=True,
        errors="ignore",
    )

    work.reset_index(drop=True, inplace=True)
    work.attrs["dedup_stats"] = {
        "key_columns": "race,driver,lapNumber",
        "dedup_keys_before": dedup_keys_before,
        "dedup_excess_rows_before": dedup_excess_before,
        "dedup_keys_after": dedup_keys_after,
        "dedup_excess_rows_after": dedup_excess_after,
        "track_agnostic_mode": track_agnostic_mode,
    }
    return work


def _prepare_pit_evals(pit_evals: pd.DataFrame) -> pd.DataFrame:
    _require_columns(pit_evals, REQUIRED_PIT_EVAL_COLUMNS, "pit_evals")

    work = pit_evals.copy()
    work["race"] = work["race"].astype(str)
    work["driver"] = work["driver"].astype(str)

    work["pitLapNumber"] = pd.to_numeric(work["pitLapNumber"], errors="coerce")
    work = work[work["pitLapNumber"].notna()].copy()
    work["pitLapNumber"] = work["pitLapNumber"].astype(int)

    dedup_keys_before, dedup_excess_before = _duplicate_key_stats(
        work, ["race", "driver", "pitLapNumber"]
    )
    work = work.drop_duplicates(
        subset=["race", "driver", "pitLapNumber"],
        keep="last",
    ).copy()
    dedup_keys_after, dedup_excess_after = _duplicate_key_stats(
        work, ["race", "driver", "pitLapNumber"]
    )

    work["result_norm"] = work["result"].map(_normalize_label)

    work.sort_values(by=["race", "driver", "pitLapNumber"], inplace=True)
    work.reset_index(drop=True, inplace=True)
    work.attrs["dedup_stats"] = {
        "key_columns": "race,driver,pitLapNumber",
        "dedup_keys_before": dedup_keys_before,
        "dedup_excess_rows_before": dedup_excess_before,
        "dedup_keys_after": dedup_keys_after,
        "dedup_excess_rows_after": dedup_excess_after,
    }
    return work


def _prepare_drop_zones(drop_zones: pd.DataFrame) -> pd.DataFrame:
    _require_columns(drop_zones, REQUIRED_DROP_ZONE_COLUMNS, "drop_zones")

    work = drop_zones.copy()
    work["race"] = work["race"].astype(str)
    work["driver"] = work["driver"].astype(str)

    work["lapNumber"] = pd.to_numeric(work["lapNumber"], errors="coerce")
    work = work[work["lapNumber"].notna()].copy()
    work["lapNumber"] = work["lapNumber"].astype(int)

    work["positions_lost"] = pd.to_numeric(work["positionsLost"], errors="coerce")
    work["gap_to_physical_car"] = pd.to_numeric(
        work["gapToPhysicalCar"], errors="coerce"
    )
    work["emergence_position"] = pd.to_numeric(
        work["emergencePosition"], errors="coerce"
    )
    work["physical_car_tyre_life"] = pd.to_numeric(
        work["physicalCarTyreLife"], errors="coerce"
    )
    work["drop_zone_status"] = (
        work["dropZoneStatus"].map(_normalize_label).astype(str)
    )

    work = work[
        [
            "race",
            "driver",
            "lapNumber",
            "positions_lost",
            "gap_to_physical_car",
            "emergence_position",
            "physical_car_tyre_life",
            "drop_zone_status",
        ]
    ]

    work.loc[~work["drop_zone_status"].isin(DROP_ZONE_STATUS_SET), "drop_zone_status"] = (
        DROP_ZONE_STATUS_INSUFFICIENT_GAP_CONTEXT
    )

    # replay retries can duplicate keys, keep the latest row for deterministic joins.
    work.drop_duplicates(
        subset=["race", "driver", "lapNumber"], keep="last", inplace=True
    )
    work.sort_values(by=["race", "driver", "lapNumber"], inplace=True)
    work.reset_index(drop=True, inplace=True)
    return work


def _build_targets(
    features: pd.DataFrame, pit_evals: pd.DataFrame, horizon: int
) -> pd.DataFrame:
    dataset = features.copy()
    dataset["target_y"] = 0
    dataset["matched_pit_lap"] = pd.NA
    dataset["matched_pit_result"] = "NO_PIT_IN_WINDOW"
    dataset["label_horizon_laps"] = horizon

    grouped_pits: dict[tuple[str, str], tuple[np.ndarray, np.ndarray]] = {}
    for key, grp in pit_evals.groupby(["race", "driver"], sort=False):
        if not isinstance(key, tuple) or len(key) != 2:
            continue
        race_key = str(key[0])
        driver_key = str(key[1])
        laps = grp["pitLapNumber"].to_numpy(dtype=np.int64)
        results = grp["result_norm"].to_numpy(dtype=object)
        grouped_pits[(race_key, driver_key)] = (laps, results)

    positive_array = np.array(sorted(POSITIVE_RESULTS), dtype=object)

    for key, idx in dataset.groupby(["race", "driver"], sort=False).groups.items():
        if not isinstance(key, tuple) or len(key) != 2:
            continue
        race = str(key[0])
        driver = str(key[1])

        pit_data = grouped_pits.get((race, driver))
        if pit_data is None:
            continue

        pit_laps, pit_results = pit_data
        if pit_laps.size == 0:
            continue

        row_idx = np.asarray(list(idx), dtype=np.int64)
        row_laps = dataset.loc[row_idx, "lapNumber"].to_numpy(dtype=np.int64)

        # pick the first pit strictly after lap k, matching the [k+1, k+h] window contract.
        search_idx = np.searchsorted(pit_laps, row_laps + 1, side="left")
        valid = search_idx < pit_laps.size
        if not np.any(valid):
            continue

        safe_idx = np.where(valid, search_idx, pit_laps.size - 1)
        candidate_laps = pit_laps[safe_idx]
        in_window = valid & (candidate_laps <= (row_laps + horizon))
        if not np.any(in_window):
            continue

        # assign labels from the nearest valid future pit only to avoid multi-target leakage.
        candidate_results = pit_results[safe_idx]
        positive_mask = in_window & np.isin(candidate_results, positive_array)

        dataset.loc[row_idx, "target_y"] = positive_mask.astype(int)
        dataset.loc[row_idx, "matched_pit_lap"] = np.where(
            in_window,
            candidate_laps.astype(float),
            np.nan,
        )
        dataset.loc[row_idx, "matched_pit_result"] = np.where(
            in_window,
            candidate_results,
            "NO_PIT_IN_WINDOW",
        )

    dataset["target_y"] = dataset["target_y"].astype(int)
    dataset["matched_pit_lap"] = pd.to_numeric(
        dataset["matched_pit_lap"], errors="coerce"
    ).astype("Int64")
    return dataset


def _write_dataset(
    dataset: pd.DataFrame, output_path: Path, strict_parquet: bool
) -> tuple[Path, str]:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    suffix = output_path.suffix.lower()
    if suffix == ".csv":
        dataset.to_csv(output_path, index=False)
        return output_path, "csv"

    try:
        dataset.to_parquet(output_path, index=False)
        return output_path, "parquet"
    except Exception as exc:
        if strict_parquet:
            raise RuntimeError(
                "failed to write parquet, install pyarrow or fastparquet in the active environment"
            ) from exc

        fallback_path = output_path.with_suffix(".csv")
        dataset.to_csv(fallback_path, index=False)
        print("warning: parquet backend unavailable, wrote csv fallback instead")
        print(f"warning details: {type(exc).__name__}: {exc}")
        return fallback_path, "csv_fallback"


def _print_summary(
    dataset: pd.DataFrame,
    ml_features_path: Path,
    drop_zones_path: Path,
    pit_evals_path: Path,
    output_path: Path,
    output_format: str,
    track_agnostic_mode: str = TRACK_AGNOSTIC_OFF,
    feature_dedup_stats: dict[str, int | str] | None = None,
    pit_eval_dedup_stats: dict[str, int | str] | None = None,
) -> None:
    positives = int((dataset["target_y"] == 1).sum())
    negatives = int((dataset["target_y"] == 0).sum())
    total = positives + negatives

    pos_ratio = (positives / total) if total else 0.0
    neg_ratio = (negatives / total) if total else 0.0
    scale_pos_weight = (negatives / positives) if positives else float("inf")

    matched = int(dataset["matched_pit_lap"].notna().sum())

    print("=== ML TRAINING DATASET SUMMARY ===")
    print(f"ml_features input : {ml_features_path}")
    print(f"drop_zones input  : {drop_zones_path}")
    print(f"pit_evals input   : {pit_evals_path}")
    print(f"output            : {output_path} ({output_format})")
    print(f"shape             : {dataset.shape}")
    print(f"track agnostic    : {track_agnostic_mode}")

    print("\nclass distribution")
    print(f"y=1 positives     : {positives} ({pos_ratio:.4%})")
    print(f"y=0 negatives     : {negatives} ({neg_ratio:.4%})")
    print(f"scale_pos_weight  : {scale_pos_weight:.6f}")

    print("\nlabel coverage diagnostics")
    print(f"rows with pit in look-ahead window: {matched}")
    print(f"rows without pit in look-ahead     : {total - matched}")

    with_drop_zone = (
        int(dataset["has_drop_zone_data"].sum())
        if "has_drop_zone_data" in dataset.columns
        else 0
    )
    print("\ntraffic context diagnostics")
    print(f"rows with drop-zones context       : {with_drop_zone}")
    print(f"rows without drop-zones context    : {total - with_drop_zone}")
    if feature_dedup_stats is not None or pit_eval_dedup_stats is not None:
        print("\ndedup diagnostics")
    if feature_dedup_stats is not None:
        print(
            "ml_features keys               : "
            f"before={feature_dedup_stats.get('dedup_keys_before', 0)}, "
            f"excess_before={feature_dedup_stats.get('dedup_excess_rows_before', 0)}, "
            f"after={feature_dedup_stats.get('dedup_keys_after', 0)}, "
            f"excess_after={feature_dedup_stats.get('dedup_excess_rows_after', 0)}"
        )
    if pit_eval_dedup_stats is not None:
        print(
            "pit_evals keys                 : "
            f"before={pit_eval_dedup_stats.get('dedup_keys_before', 0)}, "
            f"excess_before={pit_eval_dedup_stats.get('dedup_excess_rows_before', 0)}, "
            f"after={pit_eval_dedup_stats.get('dedup_keys_after', 0)}, "
            f"excess_after={pit_eval_dedup_stats.get('dedup_excess_rows_after', 0)}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="prepare ml-ready training dataset")
    parser.add_argument(
        "--data-lake", default=DEFAULT_DATA_LAKE, help="data lake directory"
    )
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR, help="season year")
    parser.add_argument(
        "--season-tag", default=DEFAULT_SEASON_TAG, help="season tag token"
    )
    parser.add_argument(
        "--horizon",
        type=int,
        default=DEFAULT_HORIZON,
        help="look-ahead horizon in laps",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="output parquet file name or absolute path",
    )
    parser.add_argument(
        "--strict-parquet",
        action="store_true",
        help="fail if parquet backend is not available",
    )
    parser.add_argument(
        "--track-agnostic-mode",
        choices=sorted(TRACK_AGNOSTIC_MODES),
        default=TRACK_AGNOSTIC_OFF,
        help="optional race-relative feature mode (causal, past-only normalization)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_lake = Path(args.data_lake)

    ml_features_path = _latest_jsonl(
        data_lake, "ml_features", args.year, args.season_tag
    )
    drop_zones_path = _latest_jsonl(data_lake, "drop_zones", args.year, args.season_tag)
    pit_evals_path = _latest_jsonl(data_lake, "pit_evals", args.year, args.season_tag)

    features_raw = _load_jsonl(ml_features_path)
    drop_zones_raw = _load_jsonl(drop_zones_path)
    pit_evals_raw = _load_jsonl(pit_evals_path)

    drop_zones = _prepare_drop_zones(drop_zones_raw)
    features = _prepare_features(
        features_raw,
        drop_zones,
        source_year_fallback=args.year,
        track_agnostic_mode=args.track_agnostic_mode,
    )
    pit_evals = _prepare_pit_evals(pit_evals_raw)

    dataset = _build_targets(features, pit_evals, args.horizon)

    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = data_lake / output_path

    saved_path, output_format = _write_dataset(
        dataset, output_path, strict_parquet=args.strict_parquet
    )

    _print_summary(
        dataset=dataset,
        ml_features_path=ml_features_path,
        drop_zones_path=drop_zones_path,
        pit_evals_path=pit_evals_path,
        output_path=saved_path,
        output_format=output_format,
        track_agnostic_mode=args.track_agnostic_mode,
        feature_dedup_stats=features.attrs.get("dedup_stats"),
        pit_eval_dedup_stats=pit_evals.attrs.get("dedup_stats"),
    )


if __name__ == "__main__":
    main()
