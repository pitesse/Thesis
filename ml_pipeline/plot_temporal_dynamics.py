"""Generate local temporal dynamics for Streaming ML and two Batch ML policies."""

from __future__ import annotations

from pathlib import Path
import warnings

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lib.moa_predictions import decode_moa_predictions

TARGET_COLUMN = "target_y"

warnings.filterwarnings("ignore")


def calculate_rolling_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    window_size: int = 3000,
) -> tuple[np.ndarray, np.ndarray]:
    """Compute rolling Accuracy and Kappa on a local temporal window."""
    y_true_s = pd.Series(y_true)
    y_pred_s = pd.Series(y_pred)

    acc = y_true_s.eq(y_pred_s).rolling(window=window_size, min_periods=500).mean()

    p_o = acc
    p_true_1 = y_true_s.rolling(window=window_size, min_periods=500).mean()
    p_pred_1 = y_pred_s.rolling(window=window_size, min_periods=500).mean()
    p_e = (p_true_1 * p_pred_1) + ((1 - p_true_1) * (1 - p_pred_1))

    kappa = (p_o - p_e) / (1 - p_e + 1e-9)
    return acc.bfill().values, kappa.fillna(0).values


def _build_key(frame: pd.DataFrame, lap_column: str = "lapNumber") -> pd.Series:
    race = frame["race"].astype(str)
    driver = frame["driver"].astype(str)
    lap = (
        pd.to_numeric(frame[lap_column], errors="coerce")
        .fillna(-1)
        .astype(int)
        .astype(str)
    )
    return race + "||" + driver + "||" + lap


def _load_best_threshold_preds(df_ml: pd.DataFrame, best_comparator_path: Path) -> np.ndarray:
    comparator = pd.read_csv(best_comparator_path)
    required = ["race", "driver", "suggestion_lap"]
    missing = [column for column in required if column not in comparator.columns]
    if missing:
        raise ValueError(f"best-threshold comparator missing columns: {missing}")

    comparator_keys = set(_build_key(comparator, lap_column="suggestion_lap"))
    ml_keys = _build_key(df_ml)
    return ml_keys.isin(comparator_keys).astype(int).values


def main() -> None:
    reports_dir = Path("data_lake/reports")
    dataset_path = Path("data_lake/ml_training_dataset_2022_2025_merged.parquet")
    moa_pred_path = reports_dir / "moa_arf_predictions_2022_2025_merged.pred"
    ml_oof_path = reports_dir / "ml_oof_winner_2022_2025_merged.csv"
    best_comparator_path = reports_dir / "ml_comparator_best_threshold_2022_2025_merged.csv"

    # Ground truth timeline used by streaming predictions.
    df_true = pd.read_parquet(dataset_path)
    y_true_full = df_true[TARGET_COLUMN].astype(int).values
    total_laps = len(y_true_full)

    # Streaming predictions are available on the full merged timeline.
    y_moa_pred_series, _ = decode_moa_predictions(
        moa_pred_path,
        df_true[TARGET_COLUMN].astype(int),
    )
    y_moa_pred = y_moa_pred_series.fillna(0).astype(int).values

    print("Computing rolling metrics for Streaming ML...")
    sml_acc, sml_kappa = calculate_rolling_metrics(y_true_full, y_moa_pred)
    instances_sml = np.arange(total_laps)

    # Batch metrics are computed on the OOF timeline then aligned to the tail.
    df_ml = pd.read_csv(ml_oof_path)
    y_ml_true = df_ml["target_y"].astype(int).values
    y_ml_pred_constrained = df_ml["constrained_pred"].astype(int).values
    y_ml_pred_best = _load_best_threshold_preds(df_ml, best_comparator_path)

    print("Computing rolling metrics for Batch ML constrained policy...")
    batch_constrained_acc, batch_constrained_kappa = calculate_rolling_metrics(
        y_ml_true,
        y_ml_pred_constrained,
    )

    print("Computing rolling metrics for Batch ML best-threshold policy...")
    batch_best_acc, batch_best_kappa = calculate_rolling_metrics(
        y_ml_true,
        y_ml_pred_best,
    )

    start_idx = total_laps - len(y_ml_true)
    instances_batch = np.arange(start_idx, total_laps)

    plt.figure(figsize=(14, 6))
    plt.plot(
        instances_sml,
        sml_acc,
        color="#2ca02c",
        linewidth=1.5,
        alpha=0.9,
        label="Streaming ML",
    )
    plt.plot(
        instances_batch,
        batch_constrained_acc,
        color="#1f77b4",
        linewidth=1.5,
        alpha=0.8,
        label="Batch ML (Constrained Policy)",
    )
    plt.plot(
        instances_batch,
        batch_best_acc,
        color="#d62728",
        linewidth=1.5,
        alpha=0.8,
        label="Batch ML (Best Threshold)",
    )

    plt.title("Local Accuracy over Time (3000-lap window)", fontsize=14, pad=15)
    plt.xlabel("Processed Laps (Timeline)", fontsize=12)
    plt.ylabel("Local Accuracy", fontsize=12)
    plt.ylim(0.70, 1.01)
    plt.legend(loc="lower right")
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.savefig(reports_dir / "paper_fig0_accuracy_over_time_real.pdf", dpi=300)
    plt.close()

    plt.figure(figsize=(14, 6))
    plt.plot(
        instances_sml,
        sml_kappa,
        color="#ff7f0e",
        linewidth=1.5,
        alpha=0.9,
        label="Streaming ML",
    )
    plt.plot(
        instances_batch,
        batch_constrained_kappa,
        color="#9467bd",
        linewidth=1.5,
        alpha=0.8,
        label="Batch ML (Constrained Policy)",
    )
    plt.plot(
        instances_batch,
        batch_best_kappa,
        color="#8c564b",
        linewidth=1.5,
        alpha=0.8,
        label="Batch ML (Best Threshold)",
    )

    plt.title("Local Kappa Statistic over Time", fontsize=14, pad=15)
    plt.xlabel("Processed Laps (Timeline)", fontsize=12)
    plt.ylabel("Local Kappa", fontsize=12)
    plt.ylim(-0.1, 0.6)
    plt.legend(loc="upper right")
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.savefig(reports_dir / "paper_fig1_kappa_over_time_real.pdf", dpi=300)
    plt.close()

    print(f"Best-threshold positives on OOF timeline: {int(y_ml_pred_best.sum())}")
    print("Temporal dynamics figures generated in data_lake/reports/")


if __name__ == "__main__":
    main()
