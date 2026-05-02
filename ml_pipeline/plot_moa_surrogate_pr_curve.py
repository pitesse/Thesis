"""Plot precision-recall fidelity curves for MOA surrogate explainability runs.

This script does not run MOA or Batch training. It:
1) loads an existing exported MOA dataset and decoded MOA predictions,
2) re-fits the fixed surrogate sweep used by SHAP proxy scripts,
3) computes PR metrics on the held-out split against decoded MOA labels,
4) writes a publication-ready PR curve figure and machine-readable metrics.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, auc, precision_recall_curve
from sklearn.model_selection import train_test_split

from lib.moa_predictions import decode_moa_predictions
from lib.moa_surrogate_models import evaluate_and_select_surrogate

TARGET_COLUMN = "target_y"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate MOA surrogate fidelity PR curve and metrics from existing artifacts"
    )
    parser.add_argument(
        "--reports-dir",
        type=Path,
        default=Path("data_lake/reports"),
        help="Run-level reports directory containing MOA artifacts",
    )
    parser.add_argument(
        "--moa-dataset-csv",
        type=Path,
        default=None,
        help="MOA exported dataset CSV. If omitted, resolved from moa_shap_proxy_summary.csv",
    )
    parser.add_argument(
        "--moa-predictions",
        type=Path,
        default=None,
        help="MOA predictions .pred file. If omitted, resolved from moa_shap_proxy_summary.csv",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--min-f1-gain", type=float, default=0.01)
    parser.add_argument(
        "--formats",
        nargs="+",
        choices=["png", "pdf"],
        default=["png", "pdf"],
        help="Output figure formats",
    )
    return parser.parse_args()


def _safe_float(value: Any) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float("nan")
    if pd.isna(out):
        return float("nan")
    return out


def _resolve_path(base: Path, candidate: str | Path) -> Path:
    path = Path(candidate)
    if path.exists():
        return path
    alt = (base / path).resolve()
    if alt.exists():
        return alt
    return path


def _resolve_inputs(args: argparse.Namespace) -> tuple[Path, Path]:
    reports_dir = args.reports_dir
    if args.moa_dataset_csv is not None and args.moa_predictions is not None:
        dataset_csv = _resolve_path(Path.cwd(), args.moa_dataset_csv)
        pred_path = _resolve_path(Path.cwd(), args.moa_predictions)
    else:
        summary_csv = reports_dir / "moa_shap_proxy_summary.csv"
        if not summary_csv.exists():
            raise FileNotFoundError(
                "moa_shap_proxy_summary.csv not found and explicit --moa-dataset-csv/--moa-predictions not provided"
            )
        summary = pd.read_csv(summary_csv)
        if summary.empty:
            raise ValueError(f"empty summary file: {summary_csv}")
        row = summary.iloc[0]
        dataset_csv = _resolve_path(Path.cwd(), str(row["dataset_csv"]))
        pred_path = _resolve_path(Path.cwd(), str(row["predictions_file"]))

    if not dataset_csv.exists():
        raise FileNotFoundError(f"MOA dataset csv not found: {dataset_csv}")
    if not pred_path.exists():
        raise FileNotFoundError(f"MOA predictions file not found: {pred_path}")
    return dataset_csv, pred_path


def _load_aligned_inputs(dataset_csv: Path, pred_path: Path) -> tuple[pd.DataFrame, pd.Series, dict[str, object]]:
    frame = pd.read_csv(dataset_csv)
    if TARGET_COLUMN not in frame.columns:
        raise ValueError(f"target column not found in dataset: {TARGET_COLUMN}")

    y_true = pd.to_numeric(frame[TARGET_COLUMN], errors="coerce").fillna(0).astype(int)
    y_moa_pred, diagnostics = decode_moa_predictions(
        pred_path=pred_path,
        y_true=y_true,
        min_mapping_purity=0.99,
    )

    n = min(len(frame), len(y_moa_pred))
    frame = frame.iloc[:n].reset_index(drop=True)
    y_moa_pred = y_moa_pred.iloc[:n].reset_index(drop=True)

    known_mask = y_moa_pred.notna()
    frame = frame.loc[known_mask].reset_index(drop=True)
    y = y_moa_pred.loc[known_mask].astype(int).reset_index(drop=True)

    if y.nunique() < 2:
        raise ValueError("decoded MOA predictions contain one class only; PR curve is undefined")

    diagnostics["rows_aligned"] = int(n)
    diagnostics["rows_used_after_known_filter"] = int(len(frame))
    return frame, y, diagnostics


def _positive_scores(model: object, X: pd.DataFrame) -> np.ndarray:
    if hasattr(model, "predict_proba"):
        proba = np.asarray(model.predict_proba(X))
        if proba.ndim == 2 and proba.shape[1] >= 2:
            return proba[:, 1].astype(float)
        if proba.ndim == 1:
            return proba.astype(float)
    if hasattr(model, "decision_function"):
        scores = np.asarray(model.decision_function(X)).astype(float)
        return scores
    raise ValueError("selected surrogate does not expose predict_proba or decision_function")


def _save_figure(fig: plt.Figure, reports_dir: Path, stem: str, formats: list[str]) -> list[Path]:
    outputs: list[Path] = []
    for ext in formats:
        out = reports_dir / f"{stem}.{ext}"
        fig.savefig(out, dpi=300, bbox_inches="tight")
        outputs.append(out)
    plt.close(fig)
    return outputs


def _safe_trapz_auc(precision: np.ndarray, recall: np.ndarray) -> float:
    if precision.size == 0 or recall.size == 0:
        return float("nan")
    order = np.argsort(recall)
    return float(auc(recall[order], precision[order]))


def main() -> None:
    args = parse_args()
    if not (0.0 < args.test_size < 1.0):
        raise ValueError("--test-size must be in (0, 1)")
    if args.min_f1_gain < 0:
        raise ValueError("--min-f1-gain must be >= 0")

    reports_dir = args.reports_dir
    reports_dir.mkdir(parents=True, exist_ok=True)

    dataset_csv, pred_path = _resolve_inputs(args)
    frame, y, diagnostics = _load_aligned_inputs(dataset_csv, pred_path)
    X = frame.drop(columns=[TARGET_COLUMN]).astype(float)

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=args.test_size,
        random_state=args.seed,
        stratify=y,
    )

    sweep_df, surrogate, selection_info = evaluate_and_select_surrogate(
        X_train,
        X_test,
        y_train,
        y_test,
        seed=args.seed,
        min_f1_gain=args.min_f1_gain,
    )
    selected_row = sweep_df[sweep_df["selected"] == 1].iloc[0]

    y_score = _positive_scores(surrogate, X_test)
    precision, recall, thresholds = precision_recall_curve(y_test, y_score)
    ap = float(average_precision_score(y_test, y_score))
    trapz_auc = _safe_trapz_auc(precision, recall)
    prevalence = float(np.mean(np.asarray(y_test, dtype=int)))

    if thresholds.size > 0:
        f1_vec = (2.0 * precision[:-1] * recall[:-1]) / (precision[:-1] + recall[:-1] + 1e-12)
        best_idx = int(np.argmax(f1_vec))
        best_thr = float(thresholds[best_idx])
        best_p = float(precision[best_idx])
        best_r = float(recall[best_idx])
        best_f1 = float(f1_vec[best_idx])
    else:
        best_thr = float("nan")
        best_p = float("nan")
        best_r = float("nan")
        best_f1 = float("nan")

    fig, ax = plt.subplots(figsize=(9.5, 7))
    ax.plot(recall, precision, color="#1f77b4", linewidth=2.0, label=f"Surrogate PR (AP={ap:.3f})")
    ax.axhline(
        prevalence,
        color="#555555",
        linestyle="--",
        linewidth=1.2,
        alpha=0.85,
        label=f"Prevalence baseline ({prevalence:.3f})",
    )
    if np.isfinite(best_p) and np.isfinite(best_r):
        ax.scatter(
            best_r,
            best_p,
            color="#d62728",
            marker="o",
            s=55,
            edgecolors="black",
            linewidths=0.5,
            label=f"Best F1 point ({best_f1:.3f})",
        )
    ax.set_title("MOA Surrogate Fidelity PR Curve (Holdout)", fontsize=14, pad=12)
    ax.set_xlabel("Recall", fontsize=12)
    ax.set_ylabel("Precision", fontsize=12)
    ax.set_xlim(0.0, 1.01)
    ax.set_ylim(0.0, 1.01)
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend(loc="lower left", fontsize=9)
    fig.tight_layout()

    figure_paths = _save_figure(fig, reports_dir, "moa_surrogate_pr_curve", args.formats)

    curve_points = pd.DataFrame(
        {
            "precision": precision,
            "recall": recall,
        }
    )
    threshold_col = np.full(len(curve_points), np.nan, dtype=float)
    threshold_col[: len(thresholds)] = thresholds
    curve_points["threshold"] = threshold_col
    curve_points_csv = reports_dir / "moa_surrogate_pr_curve_points.csv"
    curve_points.to_csv(curve_points_csv, index=False)

    if np.isfinite(best_thr):
        y_pred_best_f1 = (np.asarray(y_score, dtype=float) >= float(best_thr)).astype(int)
    else:
        y_pred_best_f1 = np.asarray(surrogate.predict(X_test), dtype=int)
    holdout_preds = pd.DataFrame(
        {
            "y_true": np.asarray(y_test, dtype=int),
            "y_score": np.asarray(y_score, dtype=float),
            "y_pred_best_f1": y_pred_best_f1,
            "best_f1_threshold": float(best_thr) if np.isfinite(best_thr) else float("nan"),
            "seed": int(args.seed),
            "test_size": float(args.test_size),
            "selected_surrogate_id": str(selection_info["selected_model_id"]),
            "selected_surrogate_family": str(selection_info["selected_family"]),
        }
    )
    holdout_preds_csv = reports_dir / "moa_surrogate_holdout_predictions.csv"
    holdout_preds.to_csv(holdout_preds_csv, index=False)

    metrics_row = {
        "dataset_csv": str(dataset_csv),
        "predictions_file": str(pred_path),
        "seed": int(args.seed),
        "test_size": float(args.test_size),
        "min_f1_gain": float(args.min_f1_gain),
        "rows_aligned": int(diagnostics.get("rows_aligned", len(frame))),
        "rows_used": int(diagnostics.get("rows_used_after_known_filter", len(frame))),
        "n_test_rows": int(len(y_test)),
        "n_test_positive": int(np.asarray(y_test, dtype=int).sum()),
        "prevalence_test": prevalence,
        "pr_auc_ap": ap,
        "pr_auc_trapz": trapz_auc,
        "best_f1_threshold": best_thr,
        "best_f1": best_f1,
        "best_f1_precision": best_p,
        "best_f1_recall": best_r,
        "selected_surrogate_id": str(selection_info["selected_model_id"]),
        "selected_surrogate_family": str(selection_info["selected_family"]),
        "selection_reason": str(selection_info["selection_reason"]),
        "fidelity_accuracy": _safe_float(selected_row["fidelity_accuracy"]),
        "fidelity_f1": _safe_float(selected_row["fidelity_f1"]),
        "fidelity_precision": _safe_float(selected_row["fidelity_precision"]),
        "fidelity_recall": _safe_float(selected_row["fidelity_recall"]),
        "fidelity_balanced_accuracy": _safe_float(selected_row["fidelity_balanced_accuracy"]),
    }
    metrics_csv = reports_dir / "moa_surrogate_pr_metrics.csv"
    pd.DataFrame([metrics_row]).to_csv(metrics_csv, index=False)

    sweep_csv = reports_dir / "moa_surrogate_model_sweep.csv"
    sweep_df.to_csv(sweep_csv, index=False)

    print("=== MOA SURROGATE PR CURVE GENERATED ===")
    print(f"dataset csv             : {dataset_csv}")
    print(f"predictions file        : {pred_path}")
    print(f"rows used               : {metrics_row['rows_used']}")
    print(f"test rows               : {metrics_row['n_test_rows']}")
    print(f"selected surrogate      : {metrics_row['selected_surrogate_id']}")
    print(f"selection reason        : {metrics_row['selection_reason']}")
    print(f"fidelity accuracy       : {metrics_row['fidelity_accuracy']:.6f}")
    print(f"fidelity f1             : {metrics_row['fidelity_f1']:.6f}")
    print(f"holdout AP              : {metrics_row['pr_auc_ap']:.6f}")
    print(f"holdout PR-AUC trapz    : {metrics_row['pr_auc_trapz']:.6f}")
    print(f"metrics csv             : {metrics_csv}")
    print(f"curve points csv        : {curve_points_csv}")
    print(f"holdout preds csv       : {holdout_preds_csv}")
    for path in figure_paths:
        print(f"figure                  : {path}")


if __name__ == "__main__":
    main()
