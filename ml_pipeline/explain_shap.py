"""Generate reproducible SHAP artifacts for the serving bundle winner model."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import cast

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shap


DEFAULT_DATASET = Path("../data_lake/ml_training_dataset_2022_2025_merged.parquet")
DEFAULT_MODEL = Path("../data_lake/models/pit_strategy_serving_bundle.joblib")
DEFAULT_REPORTS_DIR = Path("../data_lake/reports")
DEFAULT_SAMPLE_ROWS = 5000
DEFAULT_TOP_DEPENDENCE = 3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="generate thesis-grade SHAP artifacts for pit-stop model interpretation"
    )
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--reports-dir", type=Path, default=DEFAULT_REPORTS_DIR)
    parser.add_argument("--sample-rows", type=int, default=DEFAULT_SAMPLE_ROWS)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--top-dependence", type=int, default=DEFAULT_TOP_DEPENDENCE)
    parser.add_argument(
        "--dependence-features",
        nargs="*",
        default=None,
        help="explicit feature list for dependence plots, overrides top-ranked auto selection",
    )
    return parser.parse_args()


def _extract_model_and_features(bundle: object) -> tuple[object, list[str] | None]:
    if isinstance(bundle, dict):
        model = bundle.get("model", bundle)
        feature_names = bundle.get("features", None)
    else:
        model = bundle
        feature_names = None

    if feature_names is None:
        maybe_feature_names = getattr(model, "feature_names_in_", None)
        if maybe_feature_names is not None:
            feature_names = [str(name) for name in cast(list[object], maybe_feature_names)]
    return model, feature_names


def _prepare_matrix(df: pd.DataFrame, feature_names: list[str] | None) -> pd.DataFrame:
    encoded = pd.get_dummies(df)
    if feature_names is not None:
        missing = [name for name in feature_names if name not in encoded.columns]
        for name in missing:
            encoded[name] = 0
        matrix = encoded[feature_names]
    else:
        drop_cols = ["race", "driver", "lapNumber", "outcome_class", "target"]
        matrix = encoded.drop(columns=[col for col in drop_cols if col in encoded.columns])
    return matrix.astype(float)


def _to_shap_matrix(shap_values: object) -> np.ndarray:
    if isinstance(shap_values, list):
        if len(shap_values) < 2:
            return np.asarray(shap_values[0])
        return np.asarray(shap_values[1])
    values_attr = getattr(shap_values, "values", None)
    if values_attr is not None:
        values = np.asarray(values_attr)
        if values.ndim == 3 and values.shape[2] >= 2:
            return values[:, :, 1]
        return values
    return np.asarray(shap_values)


def _save_global_bar(shap_obj: object, X_sample: pd.DataFrame, output_path: Path) -> None:
    plt.figure(figsize=(11, 7))
    shap.summary_plot(shap_obj, X_sample, plot_type="bar", show=False)
    plt.savefig(output_path, bbox_inches="tight", dpi=300)
    plt.close()


def _save_beeswarm(shap_obj: object, X_sample: pd.DataFrame, output_path: Path) -> None:
    plt.figure(figsize=(11, 7))
    shap.summary_plot(shap_obj, X_sample, show=False)
    plt.savefig(output_path, bbox_inches="tight", dpi=300)
    plt.close()


def _select_dependence_features(
    shap_matrix: np.ndarray,
    X_sample: pd.DataFrame,
    top_k: int,
    explicit: list[str] | None,
) -> list[str]:
    if explicit:
        return [name for name in explicit if name in X_sample.columns]

    if shap_matrix.ndim != 2 or shap_matrix.shape[1] != X_sample.shape[1]:
        return []

    mean_abs = np.mean(np.abs(shap_matrix), axis=0)
    ranked_idx = np.argsort(mean_abs)[::-1]
    return [X_sample.columns[i] for i in ranked_idx[: max(0, top_k)]]


def _save_dependence_plots(
    features: list[str],
    shap_matrix: np.ndarray,
    X_sample: pd.DataFrame,
    reports_dir: Path,
) -> list[Path]:
    outputs: list[Path] = []
    for feature in features:
        output_path = reports_dir / f"shap_dependence_{feature}.png"
        plt.figure(figsize=(9, 6))
        shap.dependence_plot(feature, shap_matrix, X_sample, show=False)
        plt.savefig(output_path, bbox_inches="tight", dpi=300)
        plt.close()
        outputs.append(output_path)
    return outputs


def _build_feature_importance_table(
    shap_matrix: np.ndarray,
    X_sample: pd.DataFrame,
) -> pd.DataFrame:
    if shap_matrix.ndim != 2 or shap_matrix.shape[1] != X_sample.shape[1]:
        raise ValueError("shap matrix shape does not match sampled feature matrix")

    return (
        pd.DataFrame(
            {
                "feature": X_sample.columns,
                "mean_abs_shap": np.mean(np.abs(shap_matrix), axis=0),
                "mean_shap": np.mean(shap_matrix, axis=0),
                "std_shap": np.std(shap_matrix, axis=0, ddof=0),
                "mean_feature_value": X_sample.mean(axis=0).to_numpy(),
            }
        )
        .sort_values("mean_abs_shap", ascending=False)
        .reset_index(drop=True)
    )


def main() -> None:
    args = parse_args()

    if args.sample_rows < 1:
        raise ValueError("--sample-rows must be >= 1")
    if args.top_dependence < 0:
        raise ValueError("--top-dependence must be >= 0")

    args.reports_dir.mkdir(parents=True, exist_ok=True)

    print("Loading dataset and model...")
    df = pd.read_parquet(args.dataset)
    bundle = joblib.load(args.model)

    model, feature_names = _extract_model_and_features(bundle)
    X = _prepare_matrix(df, feature_names)

    if len(X) > args.sample_rows:
        X_sample = X.sample(n=args.sample_rows, random_state=args.seed)
    else:
        X_sample = X.copy()

    print(f"Computing SHAP values on {len(X_sample)} rows...")
    explainer = shap.TreeExplainer(model)
    shap_obj = explainer(X_sample)
    shap_matrix = _to_shap_matrix(shap_obj)
    feature_importance = _build_feature_importance_table(shap_matrix, X_sample)

    global_bar_path = args.reports_dir / "shap_global_bar.png"
    beeswarm_path = args.reports_dir / "shap_beeswarm.png"
    _save_global_bar(shap_obj, X_sample, global_bar_path)
    _save_beeswarm(shap_obj, X_sample, beeswarm_path)

    dep_features = _select_dependence_features(
        shap_matrix=shap_matrix,
        X_sample=X_sample,
        top_k=args.top_dependence,
        explicit=args.dependence_features,
    )
    dep_outputs = _save_dependence_plots(dep_features, shap_matrix, X_sample, args.reports_dir)

    summary_csv = args.reports_dir / "shap_summary.csv"
    summary_json = args.reports_dir / "shap_summary.json"
    feature_importance_csv = args.reports_dir / "shap_feature_importance.csv"

    feature_importance.to_csv(feature_importance_csv, index=False)
    pd.DataFrame(
        [
            {
                "dataset": str(args.dataset),
                "model": str(args.model),
                "sample_rows": int(len(X_sample)),
                "feature_count": int(X_sample.shape[1]),
                "top_dependence_features": ";".join(dep_features),
                "feature_importance_csv": str(feature_importance_csv),
                "global_bar": str(global_bar_path),
                "beeswarm": str(beeswarm_path),
            }
        ]
    ).to_csv(summary_csv, index=False)
    summary_json.write_text(
        json.dumps(
            {
                "dataset": str(args.dataset),
                "model": str(args.model),
                "sample_rows": int(len(X_sample)),
                "feature_count": int(X_sample.shape[1]),
                "top_dependence_features": dep_features,
                "feature_importance_csv": str(feature_importance_csv),
                "artifacts": [
                    str(global_bar_path),
                    str(beeswarm_path),
                    *[str(path) for path in dep_outputs],
                    str(feature_importance_csv),
                ],
            },
            indent=2,
            ensure_ascii=True,
        )
        + "\n",
        encoding="utf-8",
    )

    print("=== SHAP ARTIFACT SUMMARY ===")
    print(f"dataset: {args.dataset}")
    print(f"model: {args.model}")
    print(f"sample_rows: {len(X_sample)}")
    print(f"global_bar: {global_bar_path}")
    print(f"beeswarm: {beeswarm_path}")
    print(f"dependence_features: {dep_features}")
    print(f"summary csv: {summary_csv}")
    print(f"summary json: {summary_json}")
    print(f"feature_importance csv: {feature_importance_csv}")
    for path in dep_outputs:
        print(f"dependence_plot: {path}")
    print("caveat: SHAP explains model behavior, not causal race dynamics")


if __name__ == "__main__":
    main()