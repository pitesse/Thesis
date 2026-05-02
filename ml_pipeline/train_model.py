"""Unified model training entrypoint with optional multi-season data preparation."""

from __future__ import annotations

import argparse
import importlib
import sys
from contextlib import contextmanager
from pathlib import Path

from pipeline_config import (
    DEFAULT_DATA_LAKE,
    DEFAULT_HORIZON,
    DEFAULT_SEASON_TAG,
    DEFAULT_YEARS,
    default_dataset_path,
    default_report_csv,
    models_dir,
    normalize_years,
)
from prep_data import prepare_dataset
from lib.model_training_cv import (
    DEFAULT_CALIBRATION_POLICY,
    DEFAULT_CONSTRAINED_FP_COST,
    DEFAULT_FOLDS,
    DEFAULT_GRID_COLSAMPLE_BYTREE,
    DEFAULT_GRID_LEARNING_RATE,
    DEFAULT_GRID_MAX_DELTA_STEP,
    DEFAULT_GRID_MAX_DEPTH,
    DEFAULT_GRID_N_ESTIMATORS,
    DEFAULT_GRID_SCALE_POS_WEIGHT,
    DEFAULT_GRID_SUBSAMPLE,
    DEFAULT_LEADERBOARD_TOP_K,
    DEFAULT_MIN_CALIBRATION_POSITIVES,
    DEFAULT_PRECISION_FLOOR,
    DEFAULT_PROBA_THRESHOLD,
    DEFAULT_ROLLING_MIN_TRAIN_YEARS,
    DEFAULT_SPLIT_PROTOCOL,
    DEFAULT_SWEEP_MAX,
    DEFAULT_SWEEP_MIN,
    DEFAULT_SWEEP_POINTS,
)
from lib.feature_profiles import (
    DEFAULT_FEATURE_PROFILE,
    DEFAULT_TRACK_AGNOSTIC_MODE,
    available_track_agnostic_modes,
    build_feature_plan,
    parse_exclude_features,
)


@contextmanager
def _patched_argv(argv: list[str]):
    # invoke module mains as if called from cli, avoids duplicate parser code paths.
    original = sys.argv[:]
    sys.argv = argv
    try:
        yield
    finally:
        sys.argv = original


def _run_step(step_name: str, module_name: str, module_args: list[str]) -> None:
    print(f"\n=== {step_name} ===")
    print("command:")
    print(f"{module_name} {' '.join(module_args)}")

    # load by module name so top-level scripts and lib modules share one execution mechanism.
    module = importlib.import_module(module_name)
    main_fn = getattr(module, "main", None)
    if not callable(main_fn):
        raise RuntimeError(f"module {module_name} does not expose a callable main()")

    with _patched_argv([f"{module_name}.py", *module_args]):
        main_fn()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="train the pit strategy model with optional multi-season dataset preparation"
    )

    parser.add_argument(
        "--data-lake", default=DEFAULT_DATA_LAKE, help="data lake directory"
    )
    parser.add_argument(
        "--years", type=int, nargs="+", default=list(DEFAULT_YEARS), help="season years"
    )
    parser.add_argument(
        "--season-tag",
        default=DEFAULT_SEASON_TAG,
        help="season tag token in JSONL filenames",
    )
    parser.add_argument(
        "--horizon",
        type=int,
        default=DEFAULT_HORIZON,
        help="look-ahead horizon in laps",
    )
    parser.add_argument("--dataset", default="", help="training dataset path")
    parser.add_argument(
        "--strict-parquet",
        action="store_true",
        help="fail if parquet backend is unavailable",
    )

    parser.add_argument(
        "--prepare-data",
        dest="prepare_data",
        action="store_true",
        help="prepare dataset before training",
    )
    parser.add_argument(
        "--skip-prepare-data",
        dest="prepare_data",
        action="store_false",
        help="use existing dataset",
    )
    parser.set_defaults(prepare_data=True)

    parser.add_argument("--folds", type=int, default=DEFAULT_FOLDS)
    parser.add_argument(
        "--split-protocol",
        choices=[
            "grouped_race",
            "holdout_race",
            "rolling_year",
            "expanding_race",
            "expanding_race_sequential",
        ],
        default=DEFAULT_SPLIT_PROTOCOL,
        help="cross-validation protocol used during model selection",
    )
    parser.add_argument(
        "--rolling-min-train-years",
        type=int,
        default=DEFAULT_ROLLING_MIN_TRAIN_YEARS,
        help="minimum history years required for each rolling-year split",
    )
    parser.add_argument("--threshold", type=float, default=DEFAULT_PROBA_THRESHOLD)
    parser.add_argument("--sweep-min", type=float, default=DEFAULT_SWEEP_MIN)
    parser.add_argument("--sweep-max", type=float, default=DEFAULT_SWEEP_MAX)
    parser.add_argument("--sweep-points", type=int, default=DEFAULT_SWEEP_POINTS)
    parser.add_argument(
        "--precision-floor", type=float, default=DEFAULT_PRECISION_FLOOR
    )
    parser.add_argument(
        "--constrained-fp-cost", type=float, default=DEFAULT_CONSTRAINED_FP_COST
    )
    parser.add_argument(
        "--calibration-policy",
        choices=["auto", "isotonic", "sigmoid"],
        default=DEFAULT_CALIBRATION_POLICY,
    )
    parser.add_argument(
        "--min-calibration-positives",
        type=int,
        default=DEFAULT_MIN_CALIBRATION_POSITIVES,
    )
    parser.add_argument(
        "--drop-source-year-feature",
        action="store_true",
        help="exclude `_source_year` from model features for ablation runs",
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

    parser.add_argument(
        "--grid-max-delta-step",
        type=int,
        nargs="+",
        default=list(DEFAULT_GRID_MAX_DELTA_STEP),
    )
    parser.add_argument(
        "--grid-subsample", type=float, nargs="+", default=list(DEFAULT_GRID_SUBSAMPLE)
    )
    parser.add_argument(
        "--grid-colsample-bytree",
        type=float,
        nargs="+",
        default=list(DEFAULT_GRID_COLSAMPLE_BYTREE),
    )
    parser.add_argument(
        "--grid-scale-pos-weight",
        nargs="+",
        default=list(DEFAULT_GRID_SCALE_POS_WEIGHT),
    )
    parser.add_argument(
        "--grid-max-depth",
        type=int,
        nargs="+",
        default=list(DEFAULT_GRID_MAX_DEPTH),
    )
    parser.add_argument(
        "--grid-learning-rate",
        type=float,
        nargs="+",
        default=list(DEFAULT_GRID_LEARNING_RATE),
    )
    parser.add_argument(
        "--grid-n-estimators",
        type=int,
        nargs="+",
        default=list(DEFAULT_GRID_N_ESTIMATORS),
    )
    parser.add_argument("--top-k", type=int, default=DEFAULT_LEADERBOARD_TOP_K)

    parser.add_argument(
        "--leaderboard-output", default="", help="ablation leaderboard output csv"
    )
    parser.add_argument("--oof-output", default="", help="winner OOF output csv")

    parser.add_argument(
        "--build-serving-bundle", dest="build_serving_bundle", action="store_true"
    )
    parser.add_argument(
        "--skip-serving-bundle", dest="build_serving_bundle", action="store_false"
    )
    parser.set_defaults(build_serving_bundle=True)

    parser.add_argument(
        "--serving-bundle-output", default="", help="serving bundle output path"
    )
    parser.add_argument("--serving-threshold", type=float, default=0.50)
    parser.add_argument("--serving-max-depth", type=int, default=6)
    parser.add_argument("--serving-learning-rate", type=float, default=0.05)
    parser.add_argument("--serving-n-estimators", type=int, default=400)
    parser.add_argument("--serving-max-delta-step", type=int, default=1)
    parser.add_argument("--serving-subsample", type=float, default=0.7)
    parser.add_argument("--serving-colsample-bytree", type=float, default=0.8)
    parser.add_argument(
        "--serving-scale-pos-weight-mode",
        choices=["auto", "fixed"],
        default="fixed",
    )
    parser.add_argument("--serving-scale-pos-weight-value", type=float, default=30.0)
    parser.add_argument("--serving-with-calibration", action="store_true")

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    parsed_exclude_features = parse_exclude_features(args.exclude_features)
    feature_plan = build_feature_plan(
        feature_profile=args.feature_profile,
        exclude_features=parsed_exclude_features,
        track_agnostic_mode=args.track_agnostic_mode,
    )

    if args.rolling_min_train_years < 1:
        raise ValueError("--rolling-min-train-years must be at least 1")

    if any(value <= 0 for value in args.grid_max_depth):
        raise ValueError("--grid-max-depth values must be >= 1")
    if any(value <= 0.0 or value > 1.0 for value in args.grid_learning_rate):
        raise ValueError("--grid-learning-rate values must satisfy 0 < value <= 1")
    if any(value < 1 for value in args.grid_n_estimators):
        raise ValueError("--grid-n-estimators values must be >= 1")

    if args.serving_max_depth <= 0:
        raise ValueError("--serving-max-depth must be >= 1")
    if not (0.0 < args.serving_learning_rate <= 1.0):
        raise ValueError("--serving-learning-rate must satisfy 0 < value <= 1")
    if args.serving_n_estimators < 1:
        raise ValueError("--serving-n-estimators must be >= 1")

    years = normalize_years(args.years)
    data_lake = Path(args.data_lake)

    dataset_path = (
        Path(args.dataset)
        if args.dataset
        else default_dataset_path(data_lake, years, args.season_tag)
    )

    if args.prepare_data:
        # keep prep optional, useful for fast reruns when dataset is already frozen.
        dataset_path = prepare_dataset(
            data_lake=data_lake,
            years=years,
            season_tag=args.season_tag,
            horizon=args.horizon,
            output_path=dataset_path,
            strict_parquet=args.strict_parquet,
            feature_profile=feature_plan.feature_profile,
            exclude_features=parsed_exclude_features,
            track_agnostic_mode=feature_plan.track_agnostic_mode,
        )
    else:
        if not dataset_path.exists():
            raise FileNotFoundError(f"dataset not found: {dataset_path}")

    leaderboard_output = (
        Path(args.leaderboard_output)
        if args.leaderboard_output
        else default_report_csv(
            data_lake, "ml_ablation_phase31c", years, args.season_tag
        )
    )
    oof_output = (
        Path(args.oof_output)
        if args.oof_output
        else default_report_csv(data_lake, "ml_oof_winner", years, args.season_tag)
    )

    # forward all training options to the cv module, keeps one source of truth for model selection logic.
    train_cmd = [
        "--dataset",
        str(dataset_path),
        "--folds",
        str(args.folds),
        "--split-protocol",
        args.split_protocol,
        "--rolling-min-train-years",
        str(args.rolling_min_train_years),
        "--threshold",
        str(args.threshold),
        "--sweep-min",
        str(args.sweep_min),
        "--sweep-max",
        str(args.sweep_max),
        "--sweep-points",
        str(args.sweep_points),
        "--precision-floor",
        str(args.precision_floor),
        "--constrained-fp-cost",
        str(args.constrained_fp_cost),
        "--calibration-policy",
        args.calibration_policy,
        "--min-calibration-positives",
        str(args.min_calibration_positives),
        "--grid-max-delta-step",
        *[str(value) for value in args.grid_max_delta_step],
        "--grid-subsample",
        *[str(value) for value in args.grid_subsample],
        "--grid-colsample-bytree",
        *[str(value) for value in args.grid_colsample_bytree],
        "--grid-scale-pos-weight",
        *[str(value) for value in args.grid_scale_pos_weight],
        "--grid-max-depth",
        *[str(value) for value in args.grid_max_depth],
        "--grid-learning-rate",
        *[str(value) for value in args.grid_learning_rate],
        "--grid-n-estimators",
        *[str(value) for value in args.grid_n_estimators],
        "--top-k",
        str(args.top_k),
        "--leaderboard-output",
        str(leaderboard_output),
        "--oof-output",
        str(oof_output),
    ]
    if args.drop_source_year_feature:
        train_cmd.append("--drop-source-year-feature")
    train_cmd.extend(
        [
            "--feature-profile",
            feature_plan.feature_profile,
            "--exclude-features",
            *list(parsed_exclude_features),
            "--track-agnostic-mode",
            feature_plan.track_agnostic_mode,
        ]
    )
    _run_step(
        "Training and cross-validated policy selection",
        "lib.model_training_cv",
        train_cmd,
    )

    serving_bundle_output = (
        Path(args.serving_bundle_output)
        if args.serving_bundle_output
        else models_dir(data_lake) / "pit_strategy_serving_bundle.joblib"
    )

    if args.build_serving_bundle:
        # bundle build is separate from cv winner selection, serving may use a different threshold policy.
        serving_cmd = [
            "--dataset",
            str(dataset_path),
            "--output",
            str(serving_bundle_output),
            "--threshold",
            str(args.serving_threshold),
            "--max-depth",
            str(args.serving_max_depth),
            "--learning-rate",
            str(args.serving_learning_rate),
            "--n-estimators",
            str(args.serving_n_estimators),
            "--max-delta-step",
            str(args.serving_max_delta_step),
            "--subsample",
            str(args.serving_subsample),
            "--colsample-bytree",
            str(args.serving_colsample_bytree),
            "--scale-pos-weight-mode",
            args.serving_scale_pos_weight_mode,
            "--scale-pos-weight-value",
            str(args.serving_scale_pos_weight_value),
        ]
        if args.serving_with_calibration:
            serving_cmd.append("--with-calibration")
        if args.drop_source_year_feature:
            serving_cmd.append("--drop-source-year-feature")
        serving_cmd.extend(
            [
                "--feature-profile",
                feature_plan.feature_profile,
                "--exclude-features",
                *list(parsed_exclude_features),
                "--track-agnostic-mode",
                feature_plan.track_agnostic_mode,
            ]
        )

        _run_step("Building serving bundle", "lib.serving_bundle_builder", serving_cmd)

    print("\n=== TRAIN MODEL SUMMARY ===")
    print(f"dataset             : {dataset_path}")
    print(f"leaderboard csv     : {leaderboard_output}")
    print(f"winner oof csv      : {oof_output}")
    print(f"drop `_source_year` : {bool(args.drop_source_year_feature)}")
    print(f"feature profile     : {feature_plan.feature_profile}")
    print(
        "excluded features   : "
        f"{','.join(str(v) for v in parsed_exclude_features) if parsed_exclude_features else 'none'}"
    )
    print(f"track agnostic mode : {feature_plan.track_agnostic_mode}")
    if args.build_serving_bundle:
        print(f"serving bundle      : {serving_bundle_output}")


if __name__ == "__main__":
    main()
