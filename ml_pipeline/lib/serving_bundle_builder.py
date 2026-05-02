"""build a deployable ml serving bundle from the prepared training dataset.

this script trains one model on the full dataset and writes a joblib bundle with:
- fitted model
- optional isotonic calibrator
- finalized one-hot feature column order
- metadata for online inference
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import joblib
from xgboost import XGBClassifier

from .model_training_cv import (
    DEFAULT_DATASET,
    DEFAULT_RANDOM_STATE,
    _compute_scale_pos_weight,
    _fit_isotonic_calibrator,
    _load_dataset,
    _prepare_matrix,
)
from .feature_profiles import (
    DEFAULT_FEATURE_PROFILE,
    DEFAULT_TRACK_AGNOSTIC_MODE,
    available_track_agnostic_modes,
    build_feature_plan,
    ensure_track_agnostic_columns,
    parse_exclude_features,
)

DEFAULT_OUTPUT = "data_lake/models/pit_strategy_serving_bundle.joblib"
DEFAULT_THRESHOLD = 0.50
DEFAULT_N_ESTIMATORS = 400
DEFAULT_LEARNING_RATE = 0.05
DEFAULT_MAX_DEPTH = 6
DEFAULT_MAX_DELTA_STEP = 1
DEFAULT_SUBSAMPLE = 0.7
DEFAULT_COLSAMPLE_BYTREE = 0.8
DEFAULT_SCALE_POS_WEIGHT_MODE = "fixed"
DEFAULT_SCALE_POS_WEIGHT_VALUE = 30.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="build serving bundle for live ml inference")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="path to prepared training dataset")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="output joblib path")
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD, help="default serving threshold")
    parser.add_argument("--n-estimators", type=int, default=DEFAULT_N_ESTIMATORS, help="model n_estimators")
    parser.add_argument("--learning-rate", type=float, default=DEFAULT_LEARNING_RATE, help="model learning_rate")
    parser.add_argument("--max-depth", type=int, default=DEFAULT_MAX_DEPTH, help="model max_depth")
    parser.add_argument("--max-delta-step", type=int, default=DEFAULT_MAX_DELTA_STEP, help="model max_delta_step")
    parser.add_argument("--subsample", type=float, default=DEFAULT_SUBSAMPLE, help="model subsample")
    parser.add_argument(
        "--colsample-bytree",
        type=float,
        default=DEFAULT_COLSAMPLE_BYTREE,
        help="model colsample_bytree",
    )
    parser.add_argument(
        "--scale-pos-weight-mode",
        choices=["auto", "fixed"],
        default=DEFAULT_SCALE_POS_WEIGHT_MODE,
        help="whether scale_pos_weight is inferred from class balance or forced to a fixed value",
    )
    parser.add_argument(
        "--scale-pos-weight-value",
        type=float,
        default=DEFAULT_SCALE_POS_WEIGHT_VALUE,
        help="required when --scale-pos-weight-mode=fixed",
    )
    parser.add_argument(
        "--with-calibration",
        action="store_true",
        help=(
            "fit isotonic calibrator on full training predictions. "
            "useful for first live dry-runs, but final thesis reporting should calibrate on held-out folds"
        ),
    )
    parser.add_argument(
        "--drop-source-year-feature",
        action="store_true",
        help="exclude `_source_year` from serving-model features for ablation runs",
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
            "metadata flag for resolved track-agnostic dataset mode; "
            "'auto' follows feature-profile defaults"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not (0.0 < args.threshold < 1.0):
        raise ValueError("--threshold must be between 0 and 1")
    if args.n_estimators < 1:
        raise ValueError("--n-estimators must be >= 1")
    if not (0.0 < args.learning_rate <= 1.0):
        raise ValueError("--learning-rate must satisfy 0 < value <= 1")
    if args.max_depth <= 0:
        raise ValueError("--max-depth must be >= 1")
    if args.max_delta_step < 0:
        raise ValueError("--max-delta-step must be >= 0")
    if not (0.0 < args.subsample <= 1.0):
        raise ValueError("--subsample must satisfy 0 < value <= 1")
    if not (0.0 < args.colsample_bytree <= 1.0):
        raise ValueError("--colsample-bytree must satisfy 0 < value <= 1")
    if args.scale_pos_weight_mode == "fixed":
        if args.scale_pos_weight_value is None:
            raise ValueError("--scale-pos-weight-value is required when --scale-pos-weight-mode=fixed")
        if args.scale_pos_weight_value <= 0:
            raise ValueError("--scale-pos-weight-value must be > 0")

    dataset_path = Path(args.dataset)
    output_path = Path(args.output)
    exclude_features = parse_exclude_features(args.exclude_features)
    feature_plan = build_feature_plan(
        feature_profile=args.feature_profile,
        exclude_features=exclude_features,
        track_agnostic_mode=args.track_agnostic_mode,
    )

    df = _load_dataset(dataset_path)
    ensure_track_agnostic_columns(
        list(df.columns),
        track_agnostic_mode=feature_plan.track_agnostic_mode,
        context_label=f"dataset {dataset_path}",
    )
    X, y, _, _, _ = _prepare_matrix(
        df,
        drop_source_year_feature=bool(args.drop_source_year_feature),
        feature_profile=feature_plan.feature_profile,
        exclude_features=list(feature_plan.excluded_features),
    )

    if args.scale_pos_weight_mode == "auto":
        spw = _compute_scale_pos_weight(y)
    else:
        spw = float(args.scale_pos_weight_value)

    model = XGBClassifier(
        n_estimators=args.n_estimators,
        learning_rate=args.learning_rate,
        max_depth=args.max_depth,
        min_child_weight=1,
        max_delta_step=args.max_delta_step,
        subsample=args.subsample,
        colsample_bytree=args.colsample_bytree,
        objective="binary:logistic",
        eval_metric="aucpr",
        scale_pos_weight=spw,
        random_state=DEFAULT_RANDOM_STATE,
        n_jobs=-1,
    )

    model.fit(X, y)

    calibrator = None
    # this full-fit calibrator is for dry-runs; final thesis metrics remain fold-calibrated.
    if args.with_calibration:
        train_scores = model.predict_proba(X)[:, 1]
        calibrator = _fit_isotonic_calibrator(train_scores, y)

    # persist schema and training context so serving and parity checks stay reproducible.
    bundle = {
        "model": model,
        "calibrator": calibrator,
        "feature_columns": list(X.columns),
        "threshold": float(args.threshold),
        "model_name": "xgboost",
        "model_version": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "row_count": int(len(y)),
        "positive_count": int((y == 1).sum()),
        "negative_count": int((y == 0).sum()),
        "scale_pos_weight": float(spw),
        "scale_pos_weight_mode": args.scale_pos_weight_mode,
        "n_estimators": int(args.n_estimators),
        "learning_rate": float(args.learning_rate),
        "max_depth": int(args.max_depth),
        "max_delta_step": int(args.max_delta_step),
        "subsample": float(args.subsample),
        "colsample_bytree": float(args.colsample_bytree),
        "calibrated": bool(calibrator is not None),
        "drop_source_year_feature": bool(args.drop_source_year_feature),
        "feature_profile": feature_plan.feature_profile,
        "excluded_features": list(feature_plan.excluded_features),
        "track_agnostic_mode": feature_plan.track_agnostic_mode,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, output_path)

    print("=== SERVING BUNDLE SUMMARY ===")
    print(f"dataset          : {dataset_path}")
    print(f"output           : {output_path}")
    print(f"rows             : {bundle['row_count']}")
    print(f"feature columns  : {len(bundle['feature_columns'])}")
    print(f"n_estimators     : {bundle['n_estimators']}")
    print(f"learning_rate    : {bundle['learning_rate']:.4f}")
    print(f"max_depth        : {bundle['max_depth']}")
    print(f"max_delta_step   : {bundle['max_delta_step']}")
    print(f"subsample        : {bundle['subsample']:.4f}")
    print(f"colsample_bytree : {bundle['colsample_bytree']:.4f}")
    print(f"scale_pos_weight : {bundle['scale_pos_weight']:.6f}")
    print(f"spw mode         : {bundle['scale_pos_weight_mode']}")
    print(f"calibrated       : {bundle['calibrated']}")
    print(f"threshold        : {bundle['threshold']:.4f}")
    print(f"drop `_source_year`: {bundle['drop_source_year_feature']}")
    print(f"feature profile  : {bundle['feature_profile']}")
    print(
        "excluded features: "
        f"{','.join(bundle['excluded_features']) if bundle['excluded_features'] else 'none'}"
    )
    print(f"track agnostic  : {bundle['track_agnostic_mode']}")


if __name__ == "__main__":
    main()
