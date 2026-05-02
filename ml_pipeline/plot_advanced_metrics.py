"""Generate advanced no-retraining evaluation figures from existing run artifacts.

Outputs (per run directory):
- advanced_calibration_batch.(pdf|png)
- advanced_calibration_moa_surrogate.(pdf|png)
- advanced_pr_gain_batch.(pdf|png)
- advanced_decision_curve_batch.(pdf|png)
- advanced_decision_curve_moa_surrogate.(pdf|png)
- advanced_temporal_drift_by_race.(pdf|png)
- companion CSV files for plotted data
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import precision_recall_curve

try:
    from lib.run_catalog import racewise_suffix_for
except ImportError:
    from ml_pipeline.lib.run_catalog import racewise_suffix_for  # type: ignore

SCORED_CLASSES = {"0", "1"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate advanced evaluation metrics/figures from run artifacts")
    parser.add_argument("--reports-dir", type=Path, required=True, help="Run-level reports directory")
    parser.add_argument("--suffix", required=True, help="Run suffix (e.g., 2022_2025_no_source_year)")
    parser.add_argument(
        "--formats",
        nargs="+",
        choices=["pdf", "png"],
        default=["pdf", "png"],
        help="Figure formats",
    )
    parser.add_argument("--threshold-min", type=float, default=0.01)
    parser.add_argument("--threshold-max", type=float, default=0.50)
    parser.add_argument("--threshold-step", type=float, default=0.01)
    parser.add_argument("--n-bins", type=int, default=10)
    parser.add_argument(
        "--binning-strategy",
        choices=["quantile", "uniform"],
        default="quantile",
    )
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def _load_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"missing {label}: {path}")
    frame = pd.read_csv(path)
    if frame.empty:
        raise ValueError(f"empty {label}: {path}")
    return frame


def _save_fig(fig: plt.Figure, reports_dir: Path, stem: str, formats: list[str]) -> list[Path]:
    outputs: list[Path] = []
    for ext in formats:
        path = reports_dir / f"{stem}.{ext}"
        fig.savefig(path, dpi=300, bbox_inches="tight")
        outputs.append(path)
    plt.close(fig)
    return outputs


def _parse_year_from_race(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.split(" :: ", n=1).str[0], errors="coerce")


def _load_oof(path: Path) -> pd.DataFrame:
    frame = _load_csv(path, "OOF")
    required = {"race", "target_y", "raw_proba", "calibrated_proba"}
    missing = [col for col in required if col not in frame.columns]
    if missing:
        raise ValueError(f"OOF missing columns {missing}: {path}")

    out = frame.copy()
    out["target_y"] = pd.to_numeric(out["target_y"], errors="coerce").fillna(0).astype(int)
    out["raw_proba"] = pd.to_numeric(out["raw_proba"], errors="coerce")
    out["calibrated_proba"] = pd.to_numeric(out["calibrated_proba"], errors="coerce")
    out["year"] = _parse_year_from_race(out["race"])
    return out


def _resolve_moa_comparator(reports_dir: Path, suffix: str) -> Path:
    name = f"moa_comparator_{suffix}.csv"
    primary = reports_dir / name
    legacy = reports_dir.parent.parent / "data_lake" / "reports" / reports_dir.name / name
    if primary.exists():
        return primary
    if legacy.exists():
        return legacy
    return primary


def _resolve_sde_comparator(reports_dir: Path, suffix: str, racewise_suffix: str) -> Path:
    primary = reports_dir / f"heuristic_comparator_{suffix}.csv"
    alt = reports_dir / f"heuristic_comparator_{racewise_suffix}.csv"
    if primary.exists():
        return primary
    if alt.exists():
        return alt
    return primary


def _calibration_bins(
    y_true: np.ndarray,
    y_score: np.ndarray,
    n_bins: int,
    strategy: str,
) -> pd.DataFrame:
    y = np.asarray(y_true, dtype=int)
    s = np.clip(np.asarray(y_score, dtype=float), 0.0, 1.0)
    valid = np.isfinite(s)
    y = y[valid]
    s = s[valid]

    if len(y) == 0:
        return pd.DataFrame(columns=["bin_index", "count", "mean_pred", "observed_rate", "bin_lower", "bin_upper"])

    if strategy == "quantile":
        q = np.linspace(0.0, 1.0, n_bins + 1)
        edges = np.quantile(s, q)
        edges = np.unique(edges)
        if len(edges) <= 2:
            edges = np.linspace(0.0, 1.0, n_bins + 1)
    else:
        edges = np.linspace(0.0, 1.0, n_bins + 1)

    if len(edges) < 2:
        edges = np.array([0.0, 1.0])

    # Ensure coverage to [0,1]
    edges[0] = 0.0
    edges[-1] = 1.0

    bin_index = np.digitize(s, edges[1:-1], right=False)

    rows: list[dict[str, float | int]] = []
    n_effective_bins = len(edges) - 1
    for idx in range(n_effective_bins):
        mask = bin_index == idx
        count = int(mask.sum())
        if count > 0:
            mean_pred = float(s[mask].mean())
            obs_rate = float(y[mask].mean())
        else:
            mean_pred = float("nan")
            obs_rate = float("nan")
        rows.append(
            {
                "bin_index": int(idx),
                "count": count,
                "mean_pred": mean_pred,
                "observed_rate": obs_rate,
                "bin_lower": float(edges[idx]),
                "bin_upper": float(edges[idx + 1]),
            }
        )

    return pd.DataFrame(rows)


def _ece_mce(bins_df: pd.DataFrame) -> tuple[float, float]:
    work = bins_df.copy()
    if work.empty:
        return float("nan"), float("nan")
    total = float(work["count"].sum())
    if total <= 0:
        return float("nan"), float("nan")
    work["weight"] = work["count"] / total
    work["abs_gap"] = (work["mean_pred"] - work["observed_rate"]).abs()
    ece = float((work["weight"] * work["abs_gap"]).sum())
    mce = float(work["abs_gap"].max())
    return ece, mce


def _pr_gain(precision: np.ndarray, recall: np.ndarray, prevalence: float) -> tuple[np.ndarray, np.ndarray]:
    p = np.asarray(precision, dtype=float)
    r = np.asarray(recall, dtype=float)
    pi = float(prevalence)

    denom_p = (1.0 - pi) * p
    denom_r = (1.0 - pi) * r

    with np.errstate(divide="ignore", invalid="ignore"):
        precision_gain = (p - pi) / denom_p
        recall_gain = (r - pi) / denom_r

    mask = np.isfinite(precision_gain) & np.isfinite(recall_gain)
    return recall_gain[mask], precision_gain[mask]


def _decision_curve_rows(y_true: np.ndarray, y_score: np.ndarray, thresholds: np.ndarray) -> pd.DataFrame:
    y = np.asarray(y_true, dtype=int)
    s = np.asarray(y_score, dtype=float)
    n = len(y)
    if n == 0:
        return pd.DataFrame(columns=["threshold", "net_benefit_model", "net_benefit_all", "net_benefit_none"])

    prevalence = float(y.mean())
    rows: list[dict[str, float]] = []
    for pt in thresholds:
        pred = s >= pt
        tp = float(((pred == 1) & (y == 1)).sum())
        fp = float(((pred == 1) & (y == 0)).sum())
        odds = float(pt / (1.0 - pt))

        nb_model = (tp / n) - (fp / n) * odds
        nb_all = prevalence - (1.0 - prevalence) * odds
        rows.append(
            {
                "threshold": float(pt),
                "net_benefit_model": float(nb_model),
                "net_benefit_all": float(nb_all),
                "net_benefit_none": 0.0,
            }
        )

    return pd.DataFrame(rows)


def _comparator_summary_by_race(frame: pd.DataFrame, model: str) -> pd.DataFrame:
    work = frame.copy()
    work["race"] = work["race"].astype(str)
    work["outcome_class"] = work["outcome_class"].astype(str)
    work["year"] = _parse_year_from_race(work["race"])

    rows: list[dict[str, object]] = []
    for race, group in work.groupby("race", sort=False):
        scored = group[group["outcome_class"].isin(SCORED_CLASSES)]
        tp = int((scored["outcome_class"] == "1").sum())
        fp = int((scored["outcome_class"] == "0").sum())
        actionable = int(len(group))
        scored_n = int(len(scored))
        precision = float(tp / scored_n) if scored_n > 0 else float("nan")
        coverage = float(scored_n / actionable) if actionable > 0 else float("nan")
        year_values = group["year"].dropna().astype(int)
        year = int(year_values.iloc[0]) if not year_values.empty else -1
        rows.append(
            {
                "model": model,
                "race": race,
                "year": year,
                "actionable": actionable,
                "scored": scored_n,
                "tp": tp,
                "fp": fp,
                "precision": precision,
                "coverage": coverage,
            }
        )

    out = pd.DataFrame(rows)
    out = out.sort_values(by=["year", "race"], ascending=[True, True]).reset_index(drop=True)
    out["race_index"] = np.arange(1, len(out) + 1)
    return out


def _load_comparator(path: Path, label: str) -> pd.DataFrame:
    frame = _load_csv(path, label)
    required = {"race", "driver", "suggestion_lap", "outcome_class", "exclusion_reason", "nearest_future_pit_distance"}
    missing = [col for col in required if col not in frame.columns]
    if missing:
        raise ValueError(f"{label} missing columns {missing}: {path}")
    return frame


def _calibration_batch(
    reports_dir: Path,
    pretrain_oof: pd.DataFrame,
    racewise_oof: pd.DataFrame,
    n_bins: int,
    binning_strategy: str,
    formats: list[str],
) -> tuple[pd.DataFrame, list[Path], list[dict[str, object]]]:
    frames = {
        "ml_pretrain": pretrain_oof,
        "ml_racewise": racewise_oof,
    }
    score_cols = {
        "raw_proba": "Raw",
        "calibrated_proba": "Calibrated",
    }

    points_rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []

    fig, axes = plt.subplots(1, 2, figsize=(13, 6), sharex=True, sharey=True)
    cmap = {"raw_proba": "#1f77b4", "calibrated_proba": "#d62728"}

    for ax, (protocol, frame) in zip(axes, frames.items(), strict=True):
        y = frame["target_y"].to_numpy(dtype=int)
        for score_col, score_label in score_cols.items():
            bins_df = _calibration_bins(y, frame[score_col].to_numpy(dtype=float), n_bins, binning_strategy)
            ece, mce = _ece_mce(bins_df)
            summary_rows.append(
                {
                    "metric_group": "batch_calibration",
                    "protocol": protocol,
                    "score_type": score_col,
                    "ece": ece,
                    "mce": mce,
                    "rows": int(len(frame)),
                }
            )

            for _, row in bins_df.iterrows():
                points_rows.append(
                    {
                        "protocol": protocol,
                        "score_type": score_col,
                        **row.to_dict(),
                    }
                )

            ax.plot(
                bins_df["mean_pred"],
                bins_df["observed_rate"],
                marker="o",
                linewidth=1.8,
                color=cmap[score_col],
                label=f"{score_label} (ECE={ece:.3f})",
            )

        ax.plot([0, 1], [0, 1], linestyle="--", color="#666666", linewidth=1.0, label="Perfect calibration")
        ax.set_title(protocol.replace("_", "-"), fontsize=12)
        ax.set_xlabel("Mean predicted probability")
        ax.set_ylabel("Observed positive rate")
        ax.grid(True, linestyle="--", alpha=0.4)
        ax.set_xlim(0.0, 1.0)
        ax.set_ylim(0.0, 1.0)
        ax.legend(loc="upper left", fontsize=8)

    fig.suptitle("Batch OOF Reliability Diagrams", fontsize=14)
    fig.tight_layout()

    points_df = pd.DataFrame(points_rows)
    points_path = reports_dir / "advanced_calibration_batch_points.csv"
    points_df.to_csv(points_path, index=False)

    outputs = _save_fig(fig, reports_dir, "advanced_calibration_batch", formats)
    return points_df, outputs, summary_rows


def _calibration_moa_surrogate(
    reports_dir: Path,
    holdout_df: pd.DataFrame,
    n_bins: int,
    binning_strategy: str,
    formats: list[str],
) -> tuple[pd.DataFrame, list[Path], list[dict[str, object]]]:
    required = {"y_true", "y_score"}
    missing = [col for col in required if col not in holdout_df.columns]
    if missing:
        raise ValueError(f"moa_surrogate_holdout_predictions missing columns: {missing}")

    y = pd.to_numeric(holdout_df["y_true"], errors="coerce").fillna(0).astype(int).to_numpy()
    s = pd.to_numeric(holdout_df["y_score"], errors="coerce").to_numpy(dtype=float)

    bins_df = _calibration_bins(y, s, n_bins, binning_strategy)
    ece, mce = _ece_mce(bins_df)

    bins_df.to_csv(reports_dir / "advanced_calibration_moa_surrogate_points.csv", index=False)

    fig, ax = plt.subplots(figsize=(7, 6))
    ax.plot(
        bins_df["mean_pred"],
        bins_df["observed_rate"],
        marker="o",
        linewidth=1.8,
        color="#2ca02c",
        label=f"MOA surrogate (ECE={ece:.3f})",
    )
    ax.plot([0, 1], [0, 1], linestyle="--", color="#666666", linewidth=1.0, label="Perfect calibration")
    ax.set_title("MOA Surrogate Reliability Diagram", fontsize=13)
    ax.set_xlabel("Mean predicted probability")
    ax.set_ylabel("Observed positive rate")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.set_xlim(0.0, 1.0)
    ax.set_ylim(0.0, 1.0)
    ax.legend(loc="upper left", fontsize=9)
    fig.tight_layout()

    outputs = _save_fig(fig, reports_dir, "advanced_calibration_moa_surrogate", formats)
    summary_rows = [
        {
            "metric_group": "moa_surrogate_calibration",
            "protocol": "moa_surrogate",
            "score_type": "y_score",
            "ece": float(ece),
            "mce": float(mce),
            "rows": int(len(holdout_df)),
        }
    ]
    return bins_df, outputs, summary_rows


def _pr_gain_batch(
    reports_dir: Path,
    pretrain_oof: pd.DataFrame,
    racewise_oof: pd.DataFrame,
    formats: list[str],
) -> tuple[pd.DataFrame, list[Path], list[dict[str, object]]]:
    frames = {
        "ml_pretrain": pretrain_oof,
        "ml_racewise": racewise_oof,
    }
    score_cols = ["raw_proba", "calibrated_proba"]
    colors = {
        ("ml_pretrain", "raw_proba"): "#1f77b4",
        ("ml_pretrain", "calibrated_proba"): "#d62728",
        ("ml_racewise", "raw_proba"): "#17becf",
        ("ml_racewise", "calibrated_proba"): "#9467bd",
    }

    rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []

    fig, ax = plt.subplots(figsize=(8, 7))
    for protocol, frame in frames.items():
        y = frame["target_y"].to_numpy(dtype=int)
        prevalence = float(y.mean())
        for score_col in score_cols:
            precision, recall, _ = precision_recall_curve(y, frame[score_col].to_numpy(dtype=float))
            recall_gain, precision_gain = _pr_gain(precision, recall, prevalence)

            curve_df = pd.DataFrame(
                {
                    "protocol": protocol,
                    "score_type": score_col,
                    "recall_gain": recall_gain,
                    "precision_gain": precision_gain,
                    "prevalence": prevalence,
                }
            )
            rows.extend(curve_df.to_dict(orient="records"))

            ax.plot(
                recall_gain,
                precision_gain,
                linewidth=1.8,
                color=colors[(protocol, score_col)],
                label=f"{protocol} {score_col}",
            )

            summary_rows.append(
                {
                    "metric_group": "batch_pr_gain",
                    "protocol": protocol,
                    "score_type": score_col,
                    "prevalence": prevalence,
                    "n_points": int(len(curve_df)),
                }
            )

    ax.axhline(0.0, color="#666666", linestyle="--", linewidth=1.0)
    ax.axvline(0.0, color="#666666", linestyle="--", linewidth=1.0)
    ax.set_title("Batch OOF PR-Gain Curves", fontsize=13)
    ax.set_xlabel("Recall Gain")
    ax.set_ylabel("Precision Gain")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend(loc="lower left", fontsize=8)
    fig.tight_layout()

    points_df = pd.DataFrame(rows)
    points_df.to_csv(reports_dir / "advanced_pr_gain_batch_points.csv", index=False)
    outputs = _save_fig(fig, reports_dir, "advanced_pr_gain_batch", formats)
    return points_df, outputs, summary_rows


def _decision_curve_batch(
    reports_dir: Path,
    pretrain_oof: pd.DataFrame,
    racewise_oof: pd.DataFrame,
    thresholds: np.ndarray,
    formats: list[str],
) -> tuple[pd.DataFrame, list[Path], list[dict[str, object]]]:
    frames = {
        "ml_pretrain": pretrain_oof,
        "ml_racewise": racewise_oof,
    }

    rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []

    fig, axes = plt.subplots(1, 2, figsize=(13, 6), sharey=True)
    for ax, (protocol, frame) in zip(axes, frames.items(), strict=True):
        y = frame["target_y"].to_numpy(dtype=int)
        for score_col, color in (("raw_proba", "#1f77b4"), ("calibrated_proba", "#d62728")):
            dca = _decision_curve_rows(y, frame[score_col].to_numpy(dtype=float), thresholds)
            dca["protocol"] = protocol
            dca["score_type"] = score_col
            rows.extend(dca.to_dict(orient="records"))

            ax.plot(
                dca["threshold"],
                dca["net_benefit_model"],
                color=color,
                linewidth=1.8,
                label=f"{score_col}",
            )

            best_row = dca.loc[dca["net_benefit_model"].idxmax()]
            summary_rows.append(
                {
                    "metric_group": "batch_decision_curve",
                    "protocol": protocol,
                    "score_type": score_col,
                    "best_net_benefit": float(best_row["net_benefit_model"]),
                    "best_threshold": float(best_row["threshold"]),
                }
            )

        baseline = _decision_curve_rows(y, frame["calibrated_proba"].to_numpy(dtype=float), thresholds)
        ax.plot(
            baseline["threshold"],
            baseline["net_benefit_all"],
            color="#444444",
            linestyle="--",
            linewidth=1.2,
            label="Treat all",
        )
        ax.plot(
            baseline["threshold"],
            baseline["net_benefit_none"],
            color="#777777",
            linestyle=":",
            linewidth=1.2,
            label="Treat none",
        )
        ax.set_title(protocol.replace("_", "-"), fontsize=12)
        ax.set_xlabel("Threshold probability")
        ax.set_ylabel("Net benefit")
        ax.grid(True, linestyle="--", alpha=0.4)
        ax.legend(loc="upper right", fontsize=8)

    fig.suptitle("Batch Decision Curves", fontsize=14)
    fig.tight_layout()

    dca_df = pd.DataFrame(rows)
    dca_df.to_csv(reports_dir / "advanced_decision_curve_batch_points.csv", index=False)
    outputs = _save_fig(fig, reports_dir, "advanced_decision_curve_batch", formats)
    return dca_df, outputs, summary_rows


def _decision_curve_moa_surrogate(
    reports_dir: Path,
    holdout_df: pd.DataFrame,
    thresholds: np.ndarray,
    formats: list[str],
) -> tuple[pd.DataFrame, list[Path], list[dict[str, object]]]:
    y = pd.to_numeric(holdout_df["y_true"], errors="coerce").fillna(0).astype(int).to_numpy()
    s = pd.to_numeric(holdout_df["y_score"], errors="coerce").to_numpy(dtype=float)

    dca = _decision_curve_rows(y, s, thresholds)
    dca.to_csv(reports_dir / "advanced_decision_curve_moa_surrogate_points.csv", index=False)

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.plot(dca["threshold"], dca["net_benefit_model"], color="#2ca02c", linewidth=1.8, label="MOA surrogate")
    ax.plot(dca["threshold"], dca["net_benefit_all"], color="#444444", linestyle="--", linewidth=1.2, label="Treat all")
    ax.plot(dca["threshold"], dca["net_benefit_none"], color="#777777", linestyle=":", linewidth=1.2, label="Treat none")
    ax.set_title("MOA Surrogate Decision Curve", fontsize=13)
    ax.set_xlabel("Threshold probability")
    ax.set_ylabel("Net benefit")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend(loc="upper right", fontsize=9)
    fig.tight_layout()

    outputs = _save_fig(fig, reports_dir, "advanced_decision_curve_moa_surrogate", formats)

    best_row = dca.loc[dca["net_benefit_model"].idxmax()]
    summary_rows = [
        {
            "metric_group": "moa_surrogate_decision_curve",
            "protocol": "moa_surrogate",
            "score_type": "y_score",
            "best_net_benefit": float(best_row["net_benefit_model"]),
            "best_threshold": float(best_row["threshold"]),
        }
    ]
    return dca, outputs, summary_rows


def _temporal_drift_by_race(
    reports_dir: Path,
    sde_cmp: pd.DataFrame,
    ml_cmp: pd.DataFrame,
    moa_cmp: pd.DataFrame,
    formats: list[str],
) -> tuple[pd.DataFrame, list[Path], list[dict[str, object]]]:
    sde_r = _comparator_summary_by_race(sde_cmp, "SDE")
    ml_r = _comparator_summary_by_race(ml_cmp, "Batch ML")
    moa_r = _comparator_summary_by_race(moa_cmp, "MOA")

    drift_df = pd.concat([sde_r, ml_r, moa_r], ignore_index=True)
    drift_df.to_csv(reports_dir / "advanced_temporal_drift_by_race.csv", index=False)

    fig, axes = plt.subplots(2, 1, figsize=(14, 9), sharex=True)
    color_map = {"SDE": "#1f77b4", "Batch ML": "#d62728", "MOA": "#2ca02c"}

    for model, group in drift_df.groupby("model"):
        axes[0].plot(
            group["race_index"],
            group["precision"],
            marker="o",
            markersize=2.8,
            linewidth=1.2,
            label=model,
            color=color_map.get(str(model), "#333333"),
        )
        axes[1].plot(
            group["race_index"],
            group["coverage"],
            marker="o",
            markersize=2.8,
            linewidth=1.2,
            label=model,
            color=color_map.get(str(model), "#333333"),
        )

    axes[0].set_title("Per-Race Precision Stability")
    axes[0].set_ylabel("Precision")
    axes[0].grid(True, linestyle="--", alpha=0.4)
    axes[0].legend(loc="lower right")

    axes[1].set_title("Per-Race Scored/Actionable Coverage Stability")
    axes[1].set_xlabel("Race index (chronological by year/race)")
    axes[1].set_ylabel("Coverage")
    axes[1].grid(True, linestyle="--", alpha=0.4)

    fig.tight_layout()
    outputs = _save_fig(fig, reports_dir, "advanced_temporal_drift_by_race", formats)

    summary_rows: list[dict[str, object]] = []
    for model, group in drift_df.groupby("model"):
        summary_rows.append(
            {
                "metric_group": "temporal_drift",
                "protocol": model,
                "score_type": "comparator",
                "mean_precision": float(group["precision"].mean()),
                "std_precision": float(group["precision"].std(ddof=0)),
                "mean_coverage": float(group["coverage"].mean()),
                "std_coverage": float(group["coverage"].std(ddof=0)),
                "n_races": int(len(group)),
            }
        )
    return drift_df, outputs, summary_rows


def main() -> None:
    args = parse_args()
    if args.n_bins < 2:
        raise ValueError("--n-bins must be at least 2")
    if not (0.0 < args.threshold_min < args.threshold_max < 1.0):
        raise ValueError("threshold range must satisfy 0 < min < max < 1")
    if args.threshold_step <= 0:
        raise ValueError("--threshold-step must be positive")

    reports_dir = args.reports_dir.resolve()
    suffix = args.suffix
    racewise_suffix = racewise_suffix_for(suffix)

    pretrain_oof = _load_oof(reports_dir / f"ml_oof_winner_{suffix}.csv")
    racewise_oof = _load_oof(reports_dir / f"ml_oof_winner_{racewise_suffix}.csv")

    sde_cmp = _load_comparator(
        _resolve_sde_comparator(reports_dir, suffix, racewise_suffix),
        "SDE comparator",
    )
    ml_cmp = _load_comparator(reports_dir / f"ml_comparator_{suffix}.csv", "Batch comparator")
    moa_cmp = _load_comparator(_resolve_moa_comparator(reports_dir, suffix), "MOA comparator")

    holdout_path = reports_dir / "moa_surrogate_holdout_predictions.csv"
    holdout_df = _load_csv(holdout_path, "MOA surrogate holdout predictions")

    thresholds = np.arange(args.threshold_min, args.threshold_max + (args.threshold_step / 2.0), args.threshold_step)

    summary_rows: list[dict[str, object]] = []
    generated: list[Path] = []

    _, outputs, rows = _calibration_batch(
        reports_dir,
        pretrain_oof,
        racewise_oof,
        n_bins=args.n_bins,
        binning_strategy=args.binning_strategy,
        formats=args.formats,
    )
    generated.extend(outputs)
    summary_rows.extend(rows)

    _, outputs, rows = _calibration_moa_surrogate(
        reports_dir,
        holdout_df,
        n_bins=args.n_bins,
        binning_strategy=args.binning_strategy,
        formats=args.formats,
    )
    generated.extend(outputs)
    summary_rows.extend(rows)

    _, outputs, rows = _pr_gain_batch(reports_dir, pretrain_oof, racewise_oof, args.formats)
    generated.extend(outputs)
    summary_rows.extend(rows)

    _, outputs, rows = _decision_curve_batch(
        reports_dir,
        pretrain_oof,
        racewise_oof,
        thresholds,
        args.formats,
    )
    generated.extend(outputs)
    summary_rows.extend(rows)

    _, outputs, rows = _decision_curve_moa_surrogate(
        reports_dir,
        holdout_df,
        thresholds,
        args.formats,
    )
    generated.extend(outputs)
    summary_rows.extend(rows)

    _, outputs, rows = _temporal_drift_by_race(reports_dir, sde_cmp, ml_cmp, moa_cmp, args.formats)
    generated.extend(outputs)
    summary_rows.extend(rows)

    summary_df = pd.DataFrame(summary_rows)
    summary_path = reports_dir / f"advanced_metrics_summary_{suffix}.csv"
    summary_df.to_csv(summary_path, index=False)

    print("=== ADVANCED METRICS PACK GENERATED ===")
    print(f"reports dir              : {reports_dir}")
    print(f"suffix                   : {suffix}")
    print(f"racewise suffix          : {racewise_suffix}")
    print(f"threshold grid           : [{args.threshold_min:.3f}, {args.threshold_max:.3f}] step {args.threshold_step:.3f}")
    print(f"binning strategy         : {args.binning_strategy}")
    print(f"summary csv              : {summary_path}")
    for path in generated:
        print(f"figure                   : {path}")


if __name__ == "__main__":
    main()
