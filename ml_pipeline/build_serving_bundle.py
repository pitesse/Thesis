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

from train_pit_strategy import (
    DEFAULT_DATASET,
    DEFAULT_RANDOM_STATE,
    _compute_scale_pos_weight,
    _fit_isotonic_calibrator,
    _load_dataset,
    _prepare_matrix,
)

DEFAULT_OUTPUT = "data_lake/models/pit_strategy_serving_bundle.joblib"
DEFAULT_THRESHOLD = 0.50


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="build serving bundle for live ml inference")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="path to prepared training dataset")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="output joblib path")
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD, help="default serving threshold")
    parser.add_argument(
        "--with-calibration",
        action="store_true",
        help=(
            "fit isotonic calibrator on full training predictions. "
            "useful for first live dry-runs, but final thesis reporting should calibrate on held-out folds"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not (0.0 < args.threshold < 1.0):
        raise ValueError("--threshold must be between 0 and 1")

    dataset_path = Path(args.dataset)
    output_path = Path(args.output)

    df = _load_dataset(dataset_path)
    X, y, _ = _prepare_matrix(df)

    spw = _compute_scale_pos_weight(y)
    model = XGBClassifier(
        n_estimators=400,
        learning_rate=0.05,
        max_depth=6,
        min_child_weight=1,
        subsample=0.9,
        colsample_bytree=0.9,
        objective="binary:logistic",
        eval_metric="aucpr",
        scale_pos_weight=spw,
        random_state=DEFAULT_RANDOM_STATE,
        n_jobs=-1,
    )

    model.fit(X, y)

    calibrator = None
    if args.with_calibration:
        train_scores = model.predict_proba(X)[:, 1]
        calibrator = _fit_isotonic_calibrator(train_scores, y)

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
        "calibrated": bool(calibrator is not None),
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, output_path)

    print("=== SERVING BUNDLE SUMMARY ===")
    print(f"dataset          : {dataset_path}")
    print(f"output           : {output_path}")
    print(f"rows             : {bundle['row_count']}")
    print(f"feature columns  : {len(bundle['feature_columns'])}")
    print(f"scale_pos_weight : {bundle['scale_pos_weight']:.6f}")
    print(f"calibrated       : {bundle['calibrated']}")
    print(f"threshold        : {bundle['threshold']:.4f}")


if __name__ == "__main__":
    main()
