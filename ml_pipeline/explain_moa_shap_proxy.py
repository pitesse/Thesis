"""Generate SHAP explanations for a surrogate model fitted to MOA ARF predictions."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shap
from sklearn.model_selection import train_test_split

from lib.moa_predictions import decode_moa_predictions
from lib.moa_surrogate_models import evaluate_and_select_surrogate
from lib.model_training_cv import TARGET_COLUMN
from pipeline_config import (
    DEFAULT_DATA_LAKE,
    DEFAULT_SEASON_TAG,
    DEFAULT_YEARS,
    default_report_csv,
    normalize_years,
    reports_dir,
)


DEFAULT_TOP_DEPENDENCE = 3
DEFAULT_SAMPLE_ROWS = 5000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="fit a surrogate model on MOA predictions and generate SHAP artifacts"
    )
    parser.add_argument("--data-lake", default=DEFAULT_DATA_LAKE)
    parser.add_argument("--years", type=int, nargs="+", default=list(DEFAULT_YEARS))
    parser.add_argument("--season-tag", default=DEFAULT_SEASON_TAG)

    parser.add_argument("--moa-dataset-csv", default="")
    parser.add_argument("--moa-predictions", default="")

    parser.add_argument("--reports-dir", default="")
    parser.add_argument("--sample-rows", type=int, default=DEFAULT_SAMPLE_ROWS)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--top-dependence", type=int, default=DEFAULT_TOP_DEPENDENCE)
    parser.add_argument(
        "--min-f1-gain",
        type=float,
        default=0.01,
        help="minimum F1 gain over baseline surrogate required to switch from baseline model",
    )

    return parser.parse_args()


def _safe_float(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float("nan")
    if pd.isna(number):
        return float("nan")
    return number


def _load_paths(args: argparse.Namespace) -> tuple[Path, Path, Path]:
    years = normalize_years(args.years)
    data_lake = Path(args.data_lake)

    dataset_csv = (
        Path(args.moa_dataset_csv)
        if args.moa_dataset_csv
        else default_report_csv(data_lake, "moa_dataset", years, args.season_tag)
    )
    pred_path = (
        Path(args.moa_predictions)
        if args.moa_predictions
        else default_report_csv(data_lake, "moa_arf_predictions", years, args.season_tag).with_suffix(".pred")
    )
    output_dir = Path(args.reports_dir) if args.reports_dir else reports_dir(data_lake)

    for path in [dataset_csv, pred_path]:
        if not path.exists():
            raise FileNotFoundError(f"required file not found: {path}")

    return dataset_csv, pred_path, output_dir


def _to_shap_values(shap_values: Any) -> np.ndarray:
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


def _save_shap_artifacts(
    reports_dir: Path,
    shap_matrix: np.ndarray,
    X_sample: pd.DataFrame,
    top_dependence: int,
) -> list[Path]:
    outputs: list[Path] = []

    if shap_matrix.ndim != 2 or shap_matrix.shape[1] != X_sample.shape[1]:
        raise ValueError("unexpected SHAP matrix shape for summary plot export")

    bar_path = reports_dir / "moa_shap_proxy_global_bar.png"
    plt.figure(figsize=(11, 7))
    shap.summary_plot(shap_matrix, X_sample, plot_type="bar", show=False)
    plt.savefig(bar_path, bbox_inches="tight", dpi=300)
    plt.close()
    outputs.append(bar_path)

    beeswarm_path = reports_dir / "moa_shap_proxy_beeswarm.png"
    plt.figure(figsize=(11, 7))
    shap.summary_plot(shap_matrix, X_sample, show=False)
    plt.savefig(beeswarm_path, bbox_inches="tight", dpi=300)
    plt.close()
    outputs.append(beeswarm_path)

    if shap_matrix.ndim == 2 and shap_matrix.shape[1] == X_sample.shape[1] and top_dependence > 0:
        mean_abs = np.mean(np.abs(shap_matrix), axis=0)
        ranked_idx = np.argsort(mean_abs)[::-1][:top_dependence]
        for idx in ranked_idx:
            feature = X_sample.columns[int(idx)]
            path = reports_dir / f"moa_shap_proxy_dependence_{feature}.png"
            plt.figure(figsize=(9, 6))
            shap.dependence_plot(feature, shap_matrix, X_sample, show=False)
            plt.savefig(path, bbox_inches="tight", dpi=300)
            plt.close()
            outputs.append(path)

    return outputs


def _build_feature_importance_table(shap_matrix: np.ndarray, X_sample: pd.DataFrame) -> pd.DataFrame:
    if shap_matrix.ndim != 2 or shap_matrix.shape[1] != X_sample.shape[1]:
        raise ValueError("unexpected SHAP matrix shape for feature importance export")

    mean_abs = np.mean(np.abs(shap_matrix), axis=0)
    mean_signed = np.mean(shap_matrix, axis=0)
    frame = pd.DataFrame(
        {
            "feature": list(X_sample.columns),
            "mean_abs_shap": mean_abs,
            "mean_signed_shap": mean_signed,
        }
    )
    frame = frame.sort_values("mean_abs_shap", ascending=False).reset_index(drop=True)
    frame.insert(0, "rank", np.arange(1, len(frame) + 1))
    return frame


def main() -> None:
    args = parse_args()
    if args.sample_rows < 1:
        raise ValueError("--sample-rows must be >= 1")
    if args.top_dependence < 0:
        raise ValueError("--top-dependence must be >= 0")
    if args.min_f1_gain < 0:
        raise ValueError("--min-f1-gain must be >= 0")

    dataset_csv, pred_path, output_dir = _load_paths(args)
    output_dir.mkdir(parents=True, exist_ok=True)

    frame = pd.read_csv(dataset_csv)
    if TARGET_COLUMN not in frame.columns:
        raise ValueError(f"target column not found in dataset: {TARGET_COLUMN}")

    y_true = pd.to_numeric(frame[TARGET_COLUMN], errors="coerce").fillna(0).astype(int)
    X = frame.drop(columns=[TARGET_COLUMN]).astype(float)

    y_moa_pred, decode_diag = decode_moa_predictions(
        pred_path=pred_path,
        y_true=y_true,
        min_mapping_purity=0.99,
    )

    n = min(len(X), len(y_moa_pred))
    X = X.iloc[:n].reset_index(drop=True)
    y_moa_pred = y_moa_pred.iloc[:n].reset_index(drop=True)

    known_mask = y_moa_pred.notna()
    if int(known_mask.sum()) < 2:
        raise ValueError("not enough known MOA predictions after decoding")
    X = X.loc[known_mask].reset_index(drop=True)
    y_moa_pred = y_moa_pred.loc[known_mask].astype(int).reset_index(drop=True)

    class_count = int(y_moa_pred.nunique())
    if class_count < 2:
        raise ValueError(
            "MOA decoded predictions contain a single class only, "
            "surrogate SHAP would be non-informative"
        )

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y_moa_pred,
        test_size=0.2,
        random_state=args.seed,
        stratify=y_moa_pred,
    )

    sweep_df, surrogate, selection_info = evaluate_and_select_surrogate(
        X_train,
        X_test,
        y_train,
        y_test,
        seed=args.seed,
        min_f1_gain=args.min_f1_gain,
    )

    sweep_csv = output_dir / "moa_surrogate_model_sweep.csv"
    sweep_df.to_csv(sweep_csv, index=False)

    selected_row = sweep_df[sweep_df["selected"] == 1].iloc[0]
    fidelity_acc = float(selected_row["fidelity_accuracy"])
    fidelity_f1 = float(selected_row["fidelity_f1"])

    if len(X) > args.sample_rows:
        X_sample = X.sample(n=args.sample_rows, random_state=args.seed)
    else:
        X_sample = X.copy()

    supports_tree = bool(int(selected_row["supports_tree_shap"]))
    if supports_tree:
        explainer = shap.TreeExplainer(surrogate)
        shap_obj = explainer(X_sample)
    else:
        # keep non-tree fallback bounded, generic explainers can be expensive on large samples.
        if len(X_sample) > 1200:
            X_sample = X_sample.sample(n=1200, random_state=args.seed)
        explainer = shap.Explainer(surrogate, X_sample)
        shap_obj = explainer(X_sample)

    shap_matrix = _to_shap_values(shap_obj)

    artifact_paths = _save_shap_artifacts(
        reports_dir=output_dir,
        shap_matrix=shap_matrix,
        X_sample=X_sample,
        top_dependence=args.top_dependence,
    )

    feature_importance = _build_feature_importance_table(shap_matrix, X_sample)
    feature_csv = output_dir / "moa_shap_proxy_feature_importance.csv"
    feature_importance.to_csv(feature_csv, index=False)

    summary_csv = output_dir / "moa_shap_proxy_summary.csv"
    summary_json = output_dir / "moa_shap_proxy_summary.json"

    summary_row = {
        "dataset_csv": str(dataset_csv),
        "predictions_file": str(pred_path),
        "rows_aligned": int(n),
        "rows_used": int(len(X)),
        "unknown_prediction_rows": int(decode_diag.get("unknown_prediction_rows", 0)),
        "known_positive_rate": _safe_float(decode_diag.get("known_positive_rate")),
        "sample_rows": int(len(X_sample)),
        "fidelity_accuracy": fidelity_acc,
        "fidelity_f1": fidelity_f1,
        "selected_surrogate_id": str(selection_info["selected_model_id"]),
        "selected_surrogate_family": str(selection_info["selected_family"]),
        "selected_supports_tree_shap": int(bool(selection_info["selected_supports_tree_shap"])),
        "surrogate_selection_reason": str(selection_info["selection_reason"]),
        "surrogate_min_f1_gain": float(selection_info["min_f1_gain"]),
        "notes": "SHAP is computed on a selected surrogate fitted to MOA predictions, not on MOA internals",
    }
    pd.DataFrame([summary_row]).to_csv(summary_csv, index=False)
    summary_json.write_text(
        json.dumps(
            {
                **summary_row,
                "decode_diagnostics": decode_diag,
                "surrogate_sweep_csv": str(sweep_csv),
                "surrogate_sweep": sweep_df.to_dict(orient="records"),
                "feature_importance_csv": str(feature_csv),
                "artifacts": [str(feature_csv), str(sweep_csv), *[str(p) for p in artifact_paths]],
            },
            indent=2,
            ensure_ascii=True,
        )
        + "\n",
        encoding="utf-8",
    )

    print("=== MOA SHAP PROXY SUMMARY ===")
    print(f"dataset                 : {dataset_csv}")
    print(f"predictions             : {pred_path}")
    print(f"rows used               : {len(X)}")
    print(f"selected surrogate      : {selection_info['selected_model_id']}")
    print(f"selection reason        : {selection_info['selection_reason']}")
    print(f"fidelity accuracy       : {fidelity_acc:.6f}")
    print(f"fidelity f1             : {fidelity_f1:.6f}")
    print(f"sweep csv               : {sweep_csv}")
    print(f"feature importance csv  : {feature_csv}")
    print(f"summary csv             : {summary_csv}")
    print(f"summary json            : {summary_json}")
    for path in artifact_paths:
        print(f"artifact                : {path}")
    print("caveat: explanation is surrogate-based, it describes MOA decision behavior approximately")


if __name__ == "__main__":
    main()
