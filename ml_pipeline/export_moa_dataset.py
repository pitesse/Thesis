"""Export a MOA-ready dataset plus schema contract from the prepared ML matrix."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from pipeline_config import (
    DEFAULT_DATA_LAKE,
    DEFAULT_HORIZON,
    DEFAULT_SEASON_TAG,
    DEFAULT_YEARS,
    default_dataset_path,
    default_report_csv,
    normalize_years,
)
from prep_data import prepare_dataset
from lib.model_training_cv import TARGET_COLUMN, _load_dataset, _prepare_matrix
from lib.feature_profiles import (
    DEFAULT_FEATURE_PROFILE,
    DEFAULT_TRACK_AGNOSTIC_MODE,
    available_track_agnostic_modes,
    build_feature_plan,
    ensure_track_agnostic_columns,
    parse_exclude_features,
)


def _escape_arff_identifier(name: str) -> str:
    return "'" + str(name).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _format_arff_value(value: object) -> str:
    if value is None:
        return "?"
    if isinstance(value, float) and np.isnan(value):
        return "?"
    if value is pd.NA:
        return "?"
    if isinstance(value, (np.floating, np.integer)):
        if pd.isna(value):
            return "?"
        return f"{value:.12g}" if isinstance(value, np.floating) else str(int(value))
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value)


def _write_arff(output_path: Path, features: pd.DataFrame, target: pd.Series) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        handle.write("@RELATION moa_pit_strategy\n\n")
        for column in features.columns:
            handle.write(f"@ATTRIBUTE {_escape_arff_identifier(column)} NUMERIC\n")
        handle.write(f"@ATTRIBUTE {_escape_arff_identifier(TARGET_COLUMN)} {{0,1}}\n\n")
        handle.write("@DATA\n")

        matrix = pd.concat([features.reset_index(drop=True), target.reset_index(drop=True)], axis=1)
        for _, row in matrix.iterrows():
            values = [_format_arff_value(value) for value in row.tolist()]
            handle.write(",".join(values))
            handle.write("\n")


def _write_schema_manifest(
    output_path: Path,
    input_dataset: Path,
    csv_output: Path,
    arff_output: Path,
    features: pd.DataFrame,
    target: pd.Series,
    source_years: list[int],
    feature_profile: str,
    excluded_features: list[str],
    drop_source_year_feature: bool,
    track_agnostic_mode: str,
) -> None:
    manifest = {
        "relation": "moa_pit_strategy",
        "input_dataset": str(input_dataset),
        "csv_output": str(csv_output),
        "arff_output": str(arff_output),
        "target_column": TARGET_COLUMN,
        "target_labels": [0, 1],
        "row_count": int(len(target)),
        "feature_count": int(features.shape[1]),
        "feature_columns": list(features.columns),
        "source_years": source_years,
        "feature_profile": feature_profile,
        "excluded_features": excluded_features,
        "drop_source_year_feature": bool(drop_source_year_feature),
        "track_agnostic_mode": track_agnostic_mode,
        "input_schema_contract": {
            "race_prefix_year": True,
            "identifiers_removed_from_moa_matrix": True,
            "one_hot_encoding": True,
            "numeric_only_matrix": True,
        },
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="export a MOA-ready dataset and schema contract from the prepared ML matrix"
    )
    parser.add_argument("--data-lake", default=DEFAULT_DATA_LAKE, help="data lake directory")
    parser.add_argument("--years", type=int, nargs="+", default=list(DEFAULT_YEARS), help="season years")
    parser.add_argument("--season-tag", default=DEFAULT_SEASON_TAG, help="season tag token in JSONL filenames")
    parser.add_argument("--horizon", type=int, default=DEFAULT_HORIZON, help="look-ahead horizon in laps")
    parser.add_argument("--dataset", default="", help="prepared training dataset path")
    parser.add_argument(
        "--prepare-data",
        dest="prepare_data",
        action="store_true",
        help="prepare dataset before exporting the MOA matrix",
    )
    parser.add_argument(
        "--skip-prepare-data",
        dest="prepare_data",
        action="store_false",
        help="use an existing prepared dataset",
    )
    parser.set_defaults(prepare_data=True)
    parser.add_argument("--strict-parquet", action="store_true", help="fail if parquet backend is unavailable")
    parser.add_argument(
        "--drop-source-year-feature",
        action="store_true",
        help="exclude `_source_year` from exported MOA feature matrix for ablation runs",
    )
    parser.add_argument(
        "--feature-profile",
        default=DEFAULT_FEATURE_PROFILE,
        help=(
            "shared feature-profile token for reproducible ablations "
            "(e.g. baseline, drop_medium_v1, drop_aggressive_v1_candidate, "
            "track_agnostic_v1, percent_conservative_v1, percent_team_v1, percent_race_team_v1)"
        ),
    )
    parser.add_argument(
        "--exclude-features",
        nargs="*",
        default=[],
        help=(
            "optional additional raw feature names to exclude; accepts whitespace and/or comma-separated tokens"
        ),
    )
    parser.add_argument(
        "--track-agnostic-mode",
        choices=available_track_agnostic_modes(),
        default=DEFAULT_TRACK_AGNOSTIC_MODE,
        help=(
            "controls causal race-relative feature generation during data preparation "
            "(z-scores or percentage/ratio modes); "
            "'auto' follows feature-profile defaults"
        ),
    )
    parser.add_argument("--output-csv", default="", help="exported MOA-ready csv path")
    parser.add_argument("--output-arff", default="", help="exported MOA-ready arff path")
    parser.add_argument("--schema-output", default="", help="schema manifest json path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    years = normalize_years(args.years)
    data_lake = Path(args.data_lake)
    exclude_features = parse_exclude_features(args.exclude_features)
    feature_plan = build_feature_plan(
        feature_profile=args.feature_profile,
        exclude_features=exclude_features,
        track_agnostic_mode=args.track_agnostic_mode,
    )

    dataset_path = (
        Path(args.dataset)
        if args.dataset
        else default_dataset_path(data_lake, years, args.season_tag)
    )

    if args.prepare_data:
        dataset_path = prepare_dataset(
            data_lake=data_lake,
            years=years,
            season_tag=args.season_tag,
            horizon=args.horizon,
            output_path=dataset_path,
            strict_parquet=args.strict_parquet,
            feature_profile=feature_plan.feature_profile,
            exclude_features=list(feature_plan.excluded_features),
            track_agnostic_mode=feature_plan.track_agnostic_mode,
        )
    elif not dataset_path.exists():
        raise FileNotFoundError(f"dataset not found: {dataset_path}")

    df = _load_dataset(dataset_path)
    ensure_track_agnostic_columns(
        list(df.columns),
        track_agnostic_mode=feature_plan.track_agnostic_mode,
        context_label=f"dataset {dataset_path}",
    )
    X, y, groups, source_year, _ = _prepare_matrix(
        df,
        drop_source_year_feature=bool(args.drop_source_year_feature),
        feature_profile=feature_plan.feature_profile,
        exclude_features=list(feature_plan.excluded_features),
    )

    output_csv = (
        Path(args.output_csv)
        if args.output_csv
        else default_report_csv(data_lake, "moa_dataset", years, args.season_tag)
    )
    output_arff = Path(args.output_arff) if args.output_arff else output_csv.with_suffix(".arff")
    schema_output = Path(args.schema_output) if args.schema_output else output_csv.with_suffix(".json")

    moa_df = pd.concat([X.reset_index(drop=True), y.rename(TARGET_COLUMN).reset_index(drop=True)], axis=1)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    moa_df.to_csv(output_csv, index=False)
    _write_arff(output_arff, X, y)
    _write_schema_manifest(
        output_path=schema_output,
        input_dataset=dataset_path,
        csv_output=output_csv,
        arff_output=output_arff,
        features=X,
        target=y,
        source_years=sorted(source_year.dropna().astype(int).unique().tolist()),
        feature_profile=feature_plan.feature_profile,
        excluded_features=list(feature_plan.excluded_features),
        drop_source_year_feature=bool(args.drop_source_year_feature),
        track_agnostic_mode=feature_plan.track_agnostic_mode,
    )

    print("=== MOA DATASET EXPORT SUMMARY ===")
    print(f"input dataset : {dataset_path}")
    print(f"rows          : {len(moa_df)}")
    print(f"features      : {X.shape[1]}")
    print(f"drop `_source_year`: {bool(args.drop_source_year_feature)}")
    print(f"feature profile: {feature_plan.feature_profile}")
    print(
        "excluded feats : "
        f"{feature_plan.excluded_features_csv() or 'none'}"
    )
    print(f"track agnostic : {feature_plan.track_agnostic_mode}")
    print(f"csv output    : {output_csv}")
    print(f"arff output   : {output_arff}")
    print(f"schema output : {schema_output}")


if __name__ == "__main__":
    main()
