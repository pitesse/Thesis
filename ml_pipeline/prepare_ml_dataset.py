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


def _prepare_features(features: pd.DataFrame) -> pd.DataFrame:
    _require_columns(features, REQUIRED_FEATURE_COLUMNS, "ml_features")

    work = features.copy()
    work["race"] = work["race"].astype(str)
    work["driver"] = work["driver"].astype(str)

    work["lapNumber"] = pd.to_numeric(work["lapNumber"], errors="coerce")
    work = work[work["lapNumber"].notna()].copy()
    work["lapNumber"] = work["lapNumber"].astype(int)
    work["compound_norm"] = work["compound"].map(_normalize_label)
    work["tyre_life_num"] = pd.to_numeric(work["tyreLife"], errors="coerce")
    work["lap_time_num"] = pd.to_numeric(work["lapTime"], errors="coerce")

    # keep structural boundary semantics explicit before numeric fill.
    work["hasGapAhead"] = work["gapAhead"].notna()
    work["hasGapBehind"] = work["gapBehind"].notna()

    work["gapAhead"] = pd.to_numeric(work["gapAhead"], errors="coerce")
    work["gapBehind"] = pd.to_numeric(work["gapBehind"], errors="coerce")
    work["gapAhead"] = work["gapAhead"].fillna(STRUCTURAL_GAP_FILL)
    work["gapBehind"] = work["gapBehind"].fillna(STRUCTURAL_GAP_FILL)

    work.sort_values(by=["race", "driver", "lapNumber"], inplace=True)

    # this pace proxy follows heilmeier 2020 style decomposition,
    # use best lap so far within the current stint to avoid future leakage.
    prev_tyre_life = work.groupby(["race", "driver"], sort=False)["tyre_life_num"].shift(1)
    prev_compound = work.groupby(["race", "driver"], sort=False)["compound_norm"].shift(1)
    stint_break = (
        prev_tyre_life.isna()
        | (work["tyre_life_num"] < prev_tyre_life)
        | (work["compound_norm"] != prev_compound)
    )

    work["stint_break"] = stint_break.astype(int)
    work["stint_id"] = (
        work.groupby(["race", "driver"], sort=False)["stint_break"].cumsum().astype(int)
    )

    work["lap_time_clean"] = work["lap_time_num"].where(work["lap_time_num"] > 0, np.nan)
    work["stint_best_lap_so_far"] = (
        work.groupby(["race", "driver", "stint_id"], sort=False)["lap_time_clean"].cummin()
    )
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
    return work


def _prepare_pit_evals(pit_evals: pd.DataFrame) -> pd.DataFrame:
    _require_columns(pit_evals, REQUIRED_PIT_EVAL_COLUMNS, "pit_evals")

    work = pit_evals.copy()
    work["race"] = work["race"].astype(str)
    work["driver"] = work["driver"].astype(str)

    work["pitLapNumber"] = pd.to_numeric(work["pitLapNumber"], errors="coerce")
    work = work[work["pitLapNumber"].notna()].copy()
    work["pitLapNumber"] = work["pitLapNumber"].astype(int)

    work["result_norm"] = work["result"].map(_normalize_label)

    work.sort_values(by=["race", "driver", "pitLapNumber"], inplace=True)
    work.reset_index(drop=True, inplace=True)
    return work


def _build_targets(features: pd.DataFrame, pit_evals: pd.DataFrame, horizon: int) -> pd.DataFrame:
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

        search_idx = np.searchsorted(pit_laps, row_laps + 1, side="left")
        valid = search_idx < pit_laps.size
        if not np.any(valid):
            continue

        safe_idx = np.where(valid, search_idx, pit_laps.size - 1)
        candidate_laps = pit_laps[safe_idx]
        in_window = valid & (candidate_laps <= (row_laps + horizon))
        if not np.any(in_window):
            continue

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
    dataset["matched_pit_lap"] = pd.to_numeric(dataset["matched_pit_lap"], errors="coerce").astype("Int64")
    return dataset


def _write_dataset(dataset: pd.DataFrame, output_path: Path, strict_parquet: bool) -> tuple[Path, str]:
    output_path.parent.mkdir(parents=True, exist_ok=True)

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
    pit_evals_path: Path,
    output_path: Path,
    output_format: str,
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
    print(f"pit_evals input   : {pit_evals_path}")
    print(f"output            : {output_path} ({output_format})")
    print(f"shape             : {dataset.shape}")

    print("\nclass distribution")
    print(f"y=1 positives     : {positives} ({pos_ratio:.4%})")
    print(f"y=0 negatives     : {negatives} ({neg_ratio:.4%})")
    print(f"scale_pos_weight  : {scale_pos_weight:.6f}")

    print("\nlabel coverage diagnostics")
    print(f"rows with pit in look-ahead window: {matched}")
    print(f"rows without pit in look-ahead     : {total - matched}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="prepare ml-ready training dataset")
    parser.add_argument("--data-lake", default=DEFAULT_DATA_LAKE, help="data lake directory")
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR, help="season year")
    parser.add_argument("--season-tag", default=DEFAULT_SEASON_TAG, help="season tag token")
    parser.add_argument("--horizon", type=int, default=DEFAULT_HORIZON, help="look-ahead horizon in laps")
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_lake = Path(args.data_lake)

    ml_features_path = _latest_jsonl(data_lake, "ml_features", args.year, args.season_tag)
    pit_evals_path = _latest_jsonl(data_lake, "pit_evals", args.year, args.season_tag)

    features_raw = _load_jsonl(ml_features_path)
    pit_evals_raw = _load_jsonl(pit_evals_path)

    features = _prepare_features(features_raw)
    pit_evals = _prepare_pit_evals(pit_evals_raw)

    dataset = _build_targets(features, pit_evals, args.horizon)

    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = data_lake / output_path

    saved_path, output_format = _write_dataset(dataset, output_path, strict_parquet=args.strict_parquet)

    _print_summary(
        dataset=dataset,
        ml_features_path=ml_features_path,
        pit_evals_path=pit_evals_path,
        output_path=saved_path,
        output_format=output_format,
    )


if __name__ == "__main__":
    main()
