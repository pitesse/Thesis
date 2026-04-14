"""run Phase F training-serving parity audits on merged artifacts.

this script enforces three core parity gates:
- schema parity between training matrix and serving bundle columns,
- point-in-time legality checks for lag-based offline features,
- offline-vs-live parity on sampled race-driver-lap keys.
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from .data_preparation import _load_jsonl, _prepare_drop_zones, _prepare_features
from .live_kafka_inference import OnlineFeatureEngineer, _build_drop_zone_lookup
from .model_training_cv import METADATA_DROP_COLUMNS, TARGET_COLUMN, _prepare_matrix

DEFAULT_DATASET = "data_lake/ml_training_dataset_2022_2024_merged.parquet"
DEFAULT_BUNDLE = "data_lake/models/pit_strategy_serving_bundle.joblib"
DEFAULT_ML_FEATURES = "data_lake/ml_features_9999_merged_20260404_195653.jsonl"
DEFAULT_DROP_ZONES = "data_lake/drop_zones_9999_merged_20260404_195653.jsonl"

DEFAULT_SUMMARY_OUTPUT = "data_lake/reports/phase_f_parity_summary_2022_2024_merged.csv"
DEFAULT_DETAILS_OUTPUT = "data_lake/reports/phase_f_parity_details_2022_2024_merged.csv"
DEFAULT_REPORT_OUTPUT = "data_lake/reports/phase_f_parity_report_2022_2024_merged.txt"

KEY_COLUMNS = ["race", "driver", "lapNumber"]
SERVING_PAYLOAD_REQUIRED_FIELDS = {
    "race",
    "driver",
    "lapNumber",
    "position",
    "compound",
    "tyreLife",
    "trackTemp",
    "airTemp",
    "humidity",
    "rainfall",
    "speedTrap",
    "team",
    "gapAhead",
    "gapBehind",
    "lapTime",
    "pitLoss",
    "trackStatus",
}
PARITY_COMPARE_COLUMNS = [
    "position",
    "tyreLife",
    "trackTemp",
    "airTemp",
    "humidity",
    "rainfall",
    "speedTrap",
    "team",
    "gapAhead",
    "gapBehind",
    "lapTime",
    "pitLoss",
    "hasGapAhead",
    "hasGapBehind",
    "pace_drop_ratio",
    "tire_life_ratio",
    "pace_trend",
    "gapAhead_trend",
    "positions_lost",
    "gap_to_physical_car",
    "has_drop_zone_data",
    "_source_year",
]


def _safe_ratio(num: float, den: float) -> float:
    if den <= 0:
        return float("nan")
    return float(num / den)


def _status(ok: bool) -> str:
    return "PASS" if ok else "FAIL"


def _normalize_key_frame(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["race"] = work["race"].astype(str)
    work["driver"] = work["driver"].astype(str)
    work["lapNumber"] = pd.to_numeric(work["lapNumber"], errors="coerce")
    work = work[work["lapNumber"].notna()].copy()
    work["lapNumber"] = work["lapNumber"].astype(int)
    return work


def _collect_serving_features(
    ml_features: pd.DataFrame,
    drop_zone_lookup: dict[tuple[str, str, int], tuple[int, float]] | None,
) -> pd.DataFrame:
    work = _normalize_key_frame(ml_features)
    work.sort_values(by=KEY_COLUMNS, inplace=True)

    engineer = OnlineFeatureEngineer(drop_zone_lookup=drop_zone_lookup)
    rows: list[dict[str, object]] = []
    for event in work.to_dict(orient="records"):
        transformed = engineer.transform(event)
        rows.append(
            {
                "race": str(event["race"]),
                "driver": str(event["driver"]),
                "lapNumber": int(event["lapNumber"]),
                **transformed,
            }
        )

    serving_df = pd.DataFrame(rows)
    serving_df.drop_duplicates(subset=KEY_COLUMNS, keep="last", inplace=True)
    serving_df.reset_index(drop=True, inplace=True)
    return serving_df


def _numeric_equal(a: pd.Series, b: pd.Series, atol: float) -> pd.Series:
    a_num = pd.to_numeric(a, errors="coerce")
    b_num = pd.to_numeric(b, errors="coerce")
    both_nan = a_num.isna() & b_num.isna()
    close = (a_num - b_num).abs() <= atol
    return (both_nan | close.fillna(False)).astype(bool)


def _bool_equal(a: pd.Series, b: pd.Series) -> pd.Series:
    a_bool = a.fillna(False).astype(bool)
    b_bool = b.fillna(False).astype(bool)
    return (a_bool == b_bool).astype(bool)


def _string_equal(a: pd.Series, b: pd.Series) -> pd.Series:
    return (a.astype(str) == b.astype(str)).astype(bool)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="run Phase F feature parity audits")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="merged training dataset path")
    parser.add_argument("--bundle", default=DEFAULT_BUNDLE, help="serving bundle joblib path")
    parser.add_argument("--ml-features", default=DEFAULT_ML_FEATURES, help="merged ml_features jsonl path")
    parser.add_argument("--drop-zones", default=DEFAULT_DROP_ZONES, help="merged drop_zones jsonl path")
    parser.add_argument("--sample-size", type=int, default=3000, help="sample size for parity rows")
    parser.add_argument("--random-seed", type=int, default=42, help="sampling random seed")
    parser.add_argument("--float-atol", type=float, default=1e-6, help="absolute tolerance for numeric parity")
    parser.add_argument(
        "--min-overall-parity",
        type=float,
        default=0.95,
        help="minimum overall parity match rate",
    )
    parser.add_argument("--summary-output", default=DEFAULT_SUMMARY_OUTPUT, help="summary output csv")
    parser.add_argument("--details-output", default=DEFAULT_DETAILS_OUTPUT, help="details output csv")
    parser.add_argument("--report-output", default=DEFAULT_REPORT_OUTPUT, help="text report output")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.sample_size < 1:
        raise ValueError("--sample-size must be >= 1")
    if args.float_atol <= 0:
        raise ValueError("--float-atol must be > 0")
    if not (0.0 < args.min_overall_parity <= 1.0):
        raise ValueError("--min-overall-parity must satisfy 0 < value <= 1")

    dataset_path = Path(args.dataset)
    bundle_path = Path(args.bundle)
    ml_features_path = Path(args.ml_features)
    drop_zones_path = Path(args.drop_zones)

    if not dataset_path.exists():
        raise FileNotFoundError(f"training dataset not found: {dataset_path}")
    if not bundle_path.exists():
        raise FileNotFoundError(f"serving bundle not found: {bundle_path}")
    if not ml_features_path.exists():
        raise FileNotFoundError(f"ml_features file not found: {ml_features_path}")
    if not drop_zones_path.exists():
        raise FileNotFoundError(f"drop_zones file not found: {drop_zones_path}")

    dataset = pd.read_parquet(dataset_path) if dataset_path.suffix.lower() == ".parquet" else pd.read_csv(dataset_path)
    bundle = joblib.load(bundle_path)
    ml_features = _load_jsonl(ml_features_path)
    drop_zones = _prepare_drop_zones(_load_jsonl(drop_zones_path))
    drop_zone_lookup = _build_drop_zone_lookup(drop_zones_path)
    offline_features = _prepare_features(ml_features, drop_zones)

    training_matrix, _, _, _ = _prepare_matrix(dataset)
    training_feature_columns = list(training_matrix.columns)
    bundle_feature_columns = list(bundle.get("feature_columns", []))

    summary_rows: list[dict[str, object]] = []
    details_rows: list[dict[str, object]] = []

    bundle_training_exact = training_feature_columns == bundle_feature_columns
    training_only = sorted(set(training_feature_columns) - set(bundle_feature_columns))
    bundle_only = sorted(set(bundle_feature_columns) - set(training_feature_columns))

    summary_rows.append(
        {
            "check": "schema_bundle_training_exact",
            "status": _status(bundle_training_exact),
            "value": int(bundle_training_exact),
            "note": "order-sensitive exact column match",
        }
    )
    details_rows.append(
        {
            "section": "schema",
            "name": "training_only_columns_count",
            "value": len(training_only),
            "note": ";".join(training_only[:50]),
        }
    )
    details_rows.append(
        {
            "section": "schema",
            "name": "bundle_only_columns_count",
            "value": len(bundle_only),
            "note": ";".join(bundle_only[:50]),
        }
    )

    live_fields = set(ml_features.columns.astype(str).tolist())
    missing_payload_fields = sorted(SERVING_PAYLOAD_REQUIRED_FIELDS - live_fields)
    payload_ok = len(missing_payload_fields) == 0
    summary_rows.append(
        {
            "check": "schema_live_payload_required_fields",
            "status": _status(payload_ok),
            "value": int(payload_ok),
            "note": f"missing={';'.join(missing_payload_fields)}",
        }
    )

    offline_for_pit = _normalize_key_frame(offline_features)
    offline_for_pit.sort_values(by=KEY_COLUMNS, inplace=True)

    # recompute lag-2 trends from base columns to verify point-in-time legality directly.
    pace_expected = (
        offline_for_pit["pace_drop_ratio"]
        - offline_for_pit.groupby(["race", "driver"], sort=False)["pace_drop_ratio"].shift(2)
    ).fillna(0.0)
    gap_expected = (
        offline_for_pit["gapAhead"]
        - offline_for_pit.groupby(["race", "driver"], sort=False)["gapAhead"].shift(2)
    ).fillna(0.0)

    pace_diff = (pace_expected - pd.to_numeric(offline_for_pit["pace_trend"], errors="coerce").fillna(0.0)).abs()
    gap_diff = (gap_expected - pd.to_numeric(offline_for_pit["gapAhead_trend"], errors="coerce").fillna(0.0)).abs()
    pace_mismatch = int((pace_diff > args.float_atol).sum())
    gap_mismatch = int((gap_diff > args.float_atol).sum())

    row_order = offline_for_pit.groupby(["race", "driver"], sort=False).cumcount()
    initial_rows_mask = row_order < 2
    pace_initial_violation = int((offline_for_pit.loc[initial_rows_mask, "pace_trend"].abs() > args.float_atol).sum())
    gap_initial_violation = int((offline_for_pit.loc[initial_rows_mask, "gapAhead_trend"].abs() > args.float_atol).sum())

    pit_pace_ok = pace_mismatch == 0 and pace_initial_violation == 0
    pit_gap_ok = gap_mismatch == 0 and gap_initial_violation == 0

    summary_rows.append(
        {
            "check": "point_in_time_pace_trend_lag2",
            "status": _status(pit_pace_ok),
            "value": 1.0 - _safe_ratio(pace_mismatch + pace_initial_violation, max(1, len(offline_for_pit))),
            "note": f"mismatch_rows={pace_mismatch}, initial_rows_violation={pace_initial_violation}",
        }
    )
    summary_rows.append(
        {
            "check": "point_in_time_gapAhead_trend_lag2",
            "status": _status(pit_gap_ok),
            "value": 1.0 - _safe_ratio(gap_mismatch + gap_initial_violation, max(1, len(offline_for_pit))),
            "note": f"mismatch_rows={gap_mismatch}, initial_rows_violation={gap_initial_violation}",
        }
    )

    serving_features = _collect_serving_features(ml_features, drop_zone_lookup)

    raw_training_features = [
        column
        for column in dataset.columns
        if column != TARGET_COLUMN and column not in METADATA_DROP_COLUMNS
    ]
    serving_transform_features = sorted(set(serving_features.columns) - set(KEY_COLUMNS))
    missing_in_serving_transform = sorted(set(raw_training_features) - set(serving_transform_features))
    transform_coverage_ok = len(missing_in_serving_transform) == 0
    summary_rows.append(
        {
            "check": "schema_serving_transform_feature_coverage",
            "status": _status(transform_coverage_ok),
            "value": 1.0 - _safe_ratio(len(missing_in_serving_transform), max(1, len(raw_training_features))),
            "note": f"missing={';'.join(missing_in_serving_transform)}",
        }
    )

    details_rows.append(
        {
            "section": "schema",
            "name": "missing_raw_training_features_in_serving_transform",
            "value": len(missing_in_serving_transform),
            "note": ";".join(missing_in_serving_transform),
        }
    )
    offline_compare = _normalize_key_frame(offline_features)
    offline_compare.drop_duplicates(subset=KEY_COLUMNS, keep="last", inplace=True)

    compare_columns = [column for column in PARITY_COMPARE_COLUMNS if column in offline_compare.columns and column in serving_features.columns]
    # merge on exact race-driver-lap keys so differences reflect transform drift only.
    merged = offline_compare[KEY_COLUMNS + compare_columns].merge(
        serving_features[KEY_COLUMNS + compare_columns],
        on=KEY_COLUMNS,
        how="inner",
        suffixes=("_offline", "_serving"),
    )

    if merged.empty:
        raise RuntimeError("offline-vs-serving merge is empty, cannot compute parity")

    sample_n = min(args.sample_size, len(merged))
    sampled = merged.sample(n=sample_n, random_state=args.random_seed).copy()

    feature_match_rates: list[float] = []
    for column in compare_columns:
        left = sampled[f"{column}_offline"]
        right = sampled[f"{column}_serving"]

        if column in {"rainfall", "hasGapAhead", "hasGapBehind", "has_drop_zone_data"}:
            match_mask = _bool_equal(left, right)
        elif pd.api.types.is_numeric_dtype(left) or pd.api.types.is_numeric_dtype(right):
            match_mask = _numeric_equal(left, right, atol=args.float_atol)
        else:
            match_mask = _string_equal(left, right)

        match_count = int(match_mask.sum())
        total_count = int(len(match_mask))
        match_rate = _safe_ratio(match_count, total_count)
        feature_match_rates.append(match_rate)

        details_rows.append(
            {
                "section": "parity",
                "name": f"feature_match_rate::{column}",
                "value": match_rate,
                "note": f"matches={match_count}/{total_count}",
            }
        )

    overall_parity = float(np.mean(feature_match_rates)) if feature_match_rates else float("nan")
    parity_ok = bool(not math.isnan(overall_parity) and overall_parity >= args.min_overall_parity)

    summary_rows.append(
        {
            "check": "offline_live_parity_overall",
            "status": _status(parity_ok),
            "value": overall_parity,
            "note": f"sample_rows={sample_n}, features={len(compare_columns)}, threshold={args.min_overall_parity:.4f}",
        }
    )

    all_required_checks = [
        bundle_training_exact,
        payload_ok,
        transform_coverage_ok,
        pit_pace_ok,
        pit_gap_ok,
        parity_ok,
    ]
    overall_ok = all(all_required_checks)

    summary_rows.append(
        {
            "check": "phase_f_overall_gate",
            "status": _status(overall_ok),
            "value": int(overall_ok),
            "note": "all schema, PIT, and parity checks must pass",
        }
    )

    summary_df = pd.DataFrame(summary_rows)
    details_df = pd.DataFrame(details_rows)

    summary_output = Path(args.summary_output)
    details_output = Path(args.details_output)
    report_output = Path(args.report_output)
    summary_output.parent.mkdir(parents=True, exist_ok=True)
    details_output.parent.mkdir(parents=True, exist_ok=True)
    report_output.parent.mkdir(parents=True, exist_ok=True)

    summary_df.to_csv(summary_output, index=False)
    details_df.to_csv(details_output, index=False)

    lines = [
        "=== PHASE F FEATURE PARITY AUDIT REPORT ===",
        f"dataset            : {dataset_path}",
        f"bundle             : {bundle_path}",
        f"ml_features        : {ml_features_path}",
        f"drop_zones         : {drop_zones_path}",
        f"sample_size        : {sample_n}",
        f"float_atol         : {args.float_atol}",
        "",
        "check summary",
    ]

    for row in summary_rows:
        lines.append(f"- {row['check']}: {row['status']} ({row['value']})")
        lines.append(f"  note: {row['note']}")

    lines.extend(
        [
            "",
            f"overall_gate       : {_status(overall_ok)}",
            "",
            "artifacts",
            f"summary csv        : {summary_output}",
            f"details csv        : {details_output}",
            f"report txt         : {report_output}",
        ]
    )

    report_output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()