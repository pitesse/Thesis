"""Generate thesis-grade temporal and comparator dynamics figures from existing artifacts.

This script does not train any model. It consumes already-generated report files and
produces publication-ready plots for:
- local temporal dynamics (accuracy, kappa, coverage),
- master-method comparison (precision vs coverage, TP/FP bars),
- per-year precision trajectories,
- threshold frontier comparison (pretrain vs racewise),
- kappa distribution boxplot across methods.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import warnings

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lib.moa_predictions import decode_moa_predictions

warnings.filterwarnings("ignore")

TARGET_COLUMN = "target_y"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate thesis-grade comparison plots from existing report artifacts"
    )
    parser.add_argument(
        "--reports-dir",
        type=Path,
        default=Path("data_lake/reports"),
        help="Directory containing report artifacts",
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("data_lake/ml_training_dataset_2022_2025_merged.parquet"),
        help="Merged training dataset used as timeline reference",
    )
    parser.add_argument(
        "--window-size",
        type=int,
        default=3000,
        help="Rolling window size for temporal metrics",
    )
    parser.add_argument(
        "--min-periods",
        type=int,
        default=500,
        help="Minimum available rows required inside each rolling window",
    )
    parser.add_argument(
        "--formats",
        nargs="+",
        choices=["pdf", "png"],
        default=["pdf", "png"],
        help="Output formats for generated figures",
    )
    parser.add_argument(
        "--suffix",
        default="2022_2025",
        help="Suffix token used in comparison figure names",
    )
    return parser.parse_args()


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


def _load_positive_key_set(comparator_path: Path) -> set[str]:
    comparator = pd.read_csv(comparator_path)
    required = ["race", "driver", "suggestion_lap"]
    missing = [column for column in required if column not in comparator.columns]
    if missing:
        raise ValueError(f"comparator missing columns {missing}: {comparator_path}")
    keys = _build_key(comparator, lap_column="suggestion_lap")
    return set(keys.tolist())


def _load_available_key_set_from_oof(oof_path: Path) -> tuple[set[str], pd.DataFrame]:
    frame = pd.read_csv(oof_path)
    required = ["race", "driver", "lapNumber", "target_y"]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"OOF file missing columns {missing}: {oof_path}")
    keys = _build_key(frame, lap_column="lapNumber")
    return set(keys.tolist()), frame


def _pred_series_from_keys(
    timeline_keys: pd.Series,
    positive_keys: set[str],
    available_keys: set[str] | None,
) -> pd.Series:
    key_series = timeline_keys.astype(str)
    pred = pd.Series(np.nan, index=key_series.index, dtype="float64")

    if available_keys is None:
        available_mask = pd.Series(True, index=key_series.index)
    else:
        available_mask = key_series.isin(available_keys)

    pred.loc[available_mask] = 0.0
    pred.loc[available_mask & key_series.isin(positive_keys)] = 1.0
    return pred


def _rolling_metrics_nullable(
    y_true: np.ndarray,
    y_pred_nullable: pd.Series,
    window_size: int,
    min_periods: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    y_true_s = pd.Series(y_true, dtype="float64")
    y_pred_s = pd.Series(y_pred_nullable, dtype="float64")

    available = y_pred_s.notna()
    n = available.astype(float).rolling(window=window_size, min_periods=min_periods).sum()

    correct = ((y_pred_s == y_true_s) & available).astype(float)
    p_o = correct.rolling(window=window_size, min_periods=min_periods).sum() / n

    true_pos = ((y_true_s == 1) & available).astype(float)
    pred_pos = ((y_pred_s == 1) & available).astype(float)

    p_true_1 = true_pos.rolling(window=window_size, min_periods=min_periods).sum() / n
    p_pred_1 = pred_pos.rolling(window=window_size, min_periods=min_periods).sum() / n
    p_e = (p_true_1 * p_pred_1) + ((1.0 - p_true_1) * (1.0 - p_pred_1))

    kappa = (p_o - p_e) / (1.0 - p_e + 1e-9)
    coverage = n / float(window_size)

    return p_o.to_numpy(), kappa.to_numpy(), coverage.to_numpy()


def _safe_ylim(values: list[np.ndarray], low_q: float, high_q: float, pad: float) -> tuple[float, float]:
    valid_parts = [v[np.isfinite(v)] for v in values if np.isfinite(v).any()]
    if not valid_parts:
        return 0.0, 1.0

    merged = np.concatenate(valid_parts)
    low = float(np.quantile(merged, low_q)) - pad
    high = float(np.quantile(merged, high_q)) + pad
    return low, high


def _save_fig(fig: plt.Figure, reports_dir: Path, stem: str, formats: list[str]) -> list[Path]:
    output_paths: list[Path] = []
    for ext in formats:
        path = reports_dir / f"{stem}.{ext}"
        fig.savefig(path, dpi=300, bbox_inches="tight")
        output_paths.append(path)
    plt.close(fig)
    return output_paths


def _comparator_metrics(comparator_path: Path) -> dict[str, float | int]:
    comp = pd.read_csv(comparator_path)
    scored = comp[comp["outcome_class"].isin(["1", "0"])]
    actionable = int(len(comp))
    scored_n = int(len(scored))
    tp = int((scored["outcome_class"] == "1").sum())
    fp = int((scored["outcome_class"] == "0").sum())
    precision = float(tp / scored_n) if scored_n > 0 else float("nan")
    coverage = float(scored_n / actionable) if actionable > 0 else float("nan")
    return {
        "actionable": actionable,
        "scored": scored_n,
        "tp": tp,
        "fp": fp,
        "precision": precision,
        "coverage": coverage,
    }


def _extract_year(race_value: str) -> int | None:
    text = str(race_value)
    if " :: " not in text:
        return None
    token = text.split(" :: ", maxsplit=1)[0]
    if not token.isdigit() or len(token) != 4:
        return None
    return int(token)


def _by_year_metrics(comparator_path: Path, method_name: str) -> pd.DataFrame:
    comp = pd.read_csv(comparator_path).copy()
    comp["year"] = comp["race"].astype(str).map(_extract_year)
    comp = comp[comp["year"].notna()].copy()

    rows: list[dict[str, object]] = []
    for year, group in comp.groupby("year", sort=True):
        scored = group[group["outcome_class"].isin(["1", "0"])]
        tp = int((scored["outcome_class"] == "1").sum())
        fp = int((scored["outcome_class"] == "0").sum())
        scored_n = int(len(scored))
        precision = float(tp / scored_n) if scored_n > 0 else float("nan")
        rows.append(
            {
                "method": method_name,
                "year": int(year),
                "precision": precision,
            }
        )
    return pd.DataFrame(rows)


def _select_phase_c_best(sweep: pd.DataFrame) -> pd.Series:
    valid = sweep[(sweep["meets_precision_floor"] == 1) & (sweep["scored"] > 0)].copy()
    if valid.empty:
        valid = sweep[sweep["scored"] > 0].copy()
    if valid.empty:
        return sweep.iloc[0]

    valid.sort_values(
        by=["scored", "tp", "precision", "threshold"],
        ascending=[False, False, False, True],
        inplace=True,
    )
    return valid.iloc[0]


def main() -> None:
    args = parse_args()

    if args.window_size < 200:
        raise ValueError("--window-size must be >= 200")
    if args.min_periods < 50:
        raise ValueError("--min-periods must be >= 50")
    if args.min_periods > args.window_size:
        raise ValueError("--min-periods must be <= --window-size")

    reports_dir = args.reports_dir
    reports_dir.mkdir(parents=True, exist_ok=True)

    dataset_path = args.dataset
    if not dataset_path.exists():
        raise FileNotFoundError(f"dataset not found: {dataset_path}")

    # Required artifacts.
    paths = {
        "sde": reports_dir / "heuristic_comparator_2022_2025_merged.csv",
        "ml_pretrain_base": reports_dir / "ml_comparator_2022_2025_merged.csv",
        "ml_pretrain_extended": reports_dir / "ml_comparator_best_threshold_2022_2025_merged.csv",
        "ml_racewise_base": reports_dir / "ml_comparator_2022_2025_racewise.csv",
        "ml_racewise_extended": reports_dir / "ml_comparator_best_threshold_2022_2025_racewise.csv",
        "moa": reports_dir / "moa_comparator_2022_2025_merged.csv",
        "ml_oof_pretrain": reports_dir / "ml_oof_winner_2022_2025_merged.csv",
        "ml_oof_racewise": reports_dir / "ml_oof_winner_2022_2025_racewise.csv",
        "moa_pred": reports_dir / "moa_arf_predictions_2022_2025_merged.pred",
        "phase_c_merged": reports_dir / "phase_c_threshold_sweep_2022_2025_merged.csv",
        "phase_c_racewise": reports_dir / "phase_c_threshold_sweep_2022_2025_racewise.csv",
    }
    for name, path in paths.items():
        if not path.exists():
            raise FileNotFoundError(f"missing required artifact {name}: {path}")

    print("Loading timeline dataset...")
    df_true = pd.read_parquet(dataset_path)
    required_dataset_cols = ["race", "driver", "lapNumber", TARGET_COLUMN]
    missing_dataset = [c for c in required_dataset_cols if c not in df_true.columns]
    if missing_dataset:
        raise ValueError(f"dataset missing required columns: {missing_dataset}")

    y_true = df_true[TARGET_COLUMN].astype(int).to_numpy()
    timeline_keys = _build_key(df_true, lap_column="lapNumber")
    total_rows = len(df_true)
    timeline_x = np.arange(total_rows)

    print("Building method prediction series on shared timeline...")
    positive_sets = {
        "SDE": _load_positive_key_set(paths["sde"]),
        "ML-pretrain-base": _load_positive_key_set(paths["ml_pretrain_base"]),
        "ML-pretrain-extended": _load_positive_key_set(paths["ml_pretrain_extended"]),
        "ML-racewise-base": _load_positive_key_set(paths["ml_racewise_base"]),
        "ML-racewise-extended": _load_positive_key_set(paths["ml_racewise_extended"]),
        "MOA": _load_positive_key_set(paths["moa"]),
    }

    available_pretrain, _ = _load_available_key_set_from_oof(paths["ml_oof_pretrain"])
    available_racewise, _ = _load_available_key_set_from_oof(paths["ml_oof_racewise"])

    moa_decoded, moa_diag = decode_moa_predictions(paths["moa_pred"], df_true[TARGET_COLUMN].astype(int))
    available_moa = set(timeline_keys[moa_decoded.notna()].astype(str).tolist())

    prediction_series: dict[str, pd.Series] = {
        "SDE": _pred_series_from_keys(timeline_keys, positive_sets["SDE"], available_keys=None),
        "ML-pretrain-base": _pred_series_from_keys(
            timeline_keys,
            positive_sets["ML-pretrain-base"],
            available_keys=available_pretrain,
        ),
        "ML-pretrain-extended": _pred_series_from_keys(
            timeline_keys,
            positive_sets["ML-pretrain-extended"],
            available_keys=available_pretrain,
        ),
        "ML-racewise-base": _pred_series_from_keys(
            timeline_keys,
            positive_sets["ML-racewise-base"],
            available_keys=available_racewise,
        ),
        "ML-racewise-extended": _pred_series_from_keys(
            timeline_keys,
            positive_sets["ML-racewise-extended"],
            available_keys=available_racewise,
        ),
        "MOA": _pred_series_from_keys(timeline_keys, positive_sets["MOA"], available_keys=available_moa),
    }

    method_colors = {
        "SDE": "#444444",
        "ML-pretrain-base": "#1f77b4",
        "ML-pretrain-extended": "#d62728",
        "ML-racewise-base": "#17becf",
        "ML-racewise-extended": "#ff9896",
        "MOA": "#2ca02c",
    }

    rolling_acc: dict[str, np.ndarray] = {}
    rolling_kappa: dict[str, np.ndarray] = {}
    rolling_cov: dict[str, np.ndarray] = {}
    kappa_samples: dict[str, np.ndarray] = {}

    for method, preds in prediction_series.items():
        print(f"Computing rolling metrics for {method}...")
        acc, kappa, cov = _rolling_metrics_nullable(
            y_true=y_true,
            y_pred_nullable=preds,
            window_size=args.window_size,
            min_periods=args.min_periods,
        )
        rolling_acc[method] = acc
        rolling_kappa[method] = kappa
        rolling_cov[method] = cov

        valid_kappa = kappa[np.isfinite(kappa)]
        kappa_samples[method] = valid_kappa[::200] if len(valid_kappa) > 0 else valid_kappa

    # Figure 0: local accuracy over time.
    fig, ax = plt.subplots(figsize=(14, 6))
    for method in prediction_series:
        ax.plot(
            timeline_x,
            rolling_acc[method],
            label=method,
            linewidth=1.5,
            alpha=0.9,
            color=method_colors[method],
        )

    y_low, y_high = _safe_ylim(list(rolling_acc.values()), low_q=0.02, high_q=0.98, pad=0.02)
    ax.set_ylim(max(0.0, y_low), min(1.0, y_high))
    ax.set_title(f"Local Accuracy over Time ({args.window_size}-row window)", fontsize=14, pad=12)
    ax.set_xlabel("Timeline row index", fontsize=12)
    ax.set_ylabel("Local Accuracy", fontsize=12)
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend(loc="lower right", ncol=2)

    out0 = _save_fig(
        fig,
        reports_dir,
        stem=f"paper_fig0_accuracy_over_time_{args.suffix}",
        formats=args.formats,
    )

    # Figure 1: local kappa over time.
    fig, ax = plt.subplots(figsize=(14, 6))
    for method in prediction_series:
        ax.plot(
            timeline_x,
            rolling_kappa[method],
            label=method,
            linewidth=1.5,
            alpha=0.9,
            color=method_colors[method],
        )

    y_low, y_high = _safe_ylim(list(rolling_kappa.values()), low_q=0.02, high_q=0.98, pad=0.05)
    ax.set_ylim(max(-1.0, y_low), min(1.0, y_high))
    ax.set_title(f"Local Kappa over Time ({args.window_size}-row window)", fontsize=14, pad=12)
    ax.set_xlabel("Timeline row index", fontsize=12)
    ax.set_ylabel("Local Kappa", fontsize=12)
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend(loc="upper right", ncol=2)

    out1 = _save_fig(
        fig,
        reports_dir,
        stem=f"paper_fig1_kappa_over_time_{args.suffix}",
        formats=args.formats,
    )

    # Figure 2: local coverage over time (availability-aware windows).
    fig, ax = plt.subplots(figsize=(14, 6))
    for method in prediction_series:
        ax.plot(
            timeline_x,
            rolling_cov[method],
            label=method,
            linewidth=1.5,
            alpha=0.9,
            color=method_colors[method],
        )
    ax.set_ylim(0.0, 1.02)
    ax.set_title(f"Rolling Evaluation Coverage over Time ({args.window_size}-row window)", fontsize=14, pad=12)
    ax.set_xlabel("Timeline row index", fontsize=12)
    ax.set_ylabel("Available rows / window", fontsize=12)
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend(loc="lower right", ncol=2)

    out2 = _save_fig(
        fig,
        reports_dir,
        stem=f"paper_fig2_coverage_over_time_{args.suffix}",
        formats=args.formats,
    )

    # Figure 3: precision-coverage scatter for the six methods.
    method_to_comparator = {
        "SDE": paths["sde"],
        "ML-pretrain-base": paths["ml_pretrain_base"],
        "ML-pretrain-extended": paths["ml_pretrain_extended"],
        "ML-racewise-base": paths["ml_racewise_base"],
        "ML-racewise-extended": paths["ml_racewise_extended"],
        "MOA": paths["moa"],
    }

    metric_rows: list[dict[str, object]] = []
    for method, comparator_path in method_to_comparator.items():
        m = _comparator_metrics(comparator_path)
        metric_rows.append({"method": method, **m})
    metric_df = pd.DataFrame(metric_rows)

    fig, ax = plt.subplots(figsize=(10, 7))
    size_base = 40.0
    sizes = size_base + 0.08 * metric_df["scored"].astype(float)

    ax.scatter(
        metric_df["coverage"],
        metric_df["precision"],
        s=sizes,
        c=[method_colors[m] for m in metric_df["method"]],
        alpha=0.85,
        edgecolor="black",
        linewidth=0.8,
    )

    for _, row in metric_df.iterrows():
        ax.annotate(
            row["method"],
            (float(row["coverage"]), float(row["precision"])),
            xytext=(6, 6),
            textcoords="offset points",
            fontsize=9,
        )

    sde_precision = float(metric_df.loc[metric_df["method"] == "SDE", "precision"].iloc[0])
    ax.axhline(sde_precision, linestyle="--", color="#444444", linewidth=1.2, alpha=0.7, label="SDE precision")

    ax.set_title("Method Tradeoff: Precision vs Scored/Actionable Coverage", fontsize=14, pad=12)
    ax.set_xlabel("Scored/Actionable coverage", fontsize=12)
    ax.set_ylabel("Precision", fontsize=12)
    ax.set_xlim(left=0.0)
    ax.set_ylim(bottom=0.5, top=1.0)
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend(loc="lower left")

    out3 = _save_fig(
        fig,
        reports_dir,
        stem=f"paper_fig3_precision_vs_coverage_{args.suffix}",
        formats=args.formats,
    )

    # Figure 4: TP/FP grouped bars.
    fig, ax = plt.subplots(figsize=(11, 6))
    order = metric_df["method"].tolist()
    x = np.arange(len(order))
    width = 0.38

    tp_vals = metric_df["tp"].astype(float).to_numpy()
    fp_vals = metric_df["fp"].astype(float).to_numpy()

    ax.bar(x - width / 2.0, tp_vals, width=width, color="#2ca02c", alpha=0.85, label="TP")
    ax.bar(x + width / 2.0, fp_vals, width=width, color="#d62728", alpha=0.85, label="FP")

    ax.set_xticks(x)
    ax.set_xticklabels(order, rotation=25, ha="right")
    ax.set_title("True Positives and False Positives by Method", fontsize=14, pad=12)
    ax.set_ylabel("Count", fontsize=12)
    ax.grid(True, axis="y", linestyle="--", alpha=0.5)
    ax.legend(loc="upper right")

    out4 = _save_fig(
        fig,
        reports_dir,
        stem=f"paper_fig4_tp_fp_counts_{args.suffix}",
        formats=args.formats,
    )

    # Figure 5: per-year precision trajectories.
    by_year = pd.concat(
        [
            _by_year_metrics(paths["sde"], "SDE"),
            _by_year_metrics(paths["ml_pretrain_base"], "ML-pretrain-base"),
            _by_year_metrics(paths["ml_pretrain_extended"], "ML-pretrain-extended"),
            _by_year_metrics(paths["ml_racewise_base"], "ML-racewise-base"),
            _by_year_metrics(paths["ml_racewise_extended"], "ML-racewise-extended"),
            _by_year_metrics(paths["moa"], "MOA"),
        ],
        ignore_index=True,
    )

    fig, ax = plt.subplots(figsize=(11, 6))
    for method in order:
        subset = by_year[by_year["method"] == method].sort_values("year")
        if subset.empty:
            continue
        ax.plot(
            subset["year"].astype(int),
            subset["precision"].astype(float),
            marker="o",
            linewidth=2.0,
            label=method,
            color=method_colors[method],
        )

    ax.set_title("Per-Year Precision by Method", fontsize=14, pad=12)
    ax.set_xlabel("Year", fontsize=12)
    ax.set_ylabel("Precision", fontsize=12)
    ax.set_xticks(sorted(by_year["year"].dropna().astype(int).unique().tolist()))
    ax.set_ylim(0.45, 1.0)
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend(loc="lower right", ncol=2)

    out5 = _save_fig(
        fig,
        reports_dir,
        stem=f"paper_fig5_precision_by_year_{args.suffix}",
        formats=args.formats,
    )

    # Figure 6: threshold frontier pretrain vs racewise.
    sweep_merged = pd.read_csv(paths["phase_c_merged"]).sort_values("threshold")
    sweep_racewise = pd.read_csv(paths["phase_c_racewise"]).sort_values("threshold")
    best_merged = _select_phase_c_best(sweep_merged)
    best_racewise = _select_phase_c_best(sweep_racewise)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharex=True)

    axes[0].plot(
        sweep_merged["threshold"],
        sweep_merged["precision"],
        color="#1f77b4",
        linewidth=2.0,
        label="Pretrain protocol",
    )
    axes[0].plot(
        sweep_racewise["threshold"],
        sweep_racewise["precision"],
        color="#d62728",
        linewidth=2.0,
        label="Racewise protocol",
    )
    axes[0].scatter(
        [float(best_merged["threshold"])],
        [float(best_merged["precision"])],
        color="#1f77b4",
        s=60,
        zorder=5,
    )
    axes[0].scatter(
        [float(best_racewise["threshold"])],
        [float(best_racewise["precision"])],
        color="#d62728",
        s=60,
        zorder=5,
    )
    axes[0].set_title("Threshold vs Precision")
    axes[0].set_xlabel("Threshold")
    axes[0].set_ylabel("Precision")
    axes[0].set_ylim(0.5, 1.0)
    axes[0].grid(True, linestyle="--", alpha=0.5)
    axes[0].legend(loc="lower left")

    axes[1].plot(
        sweep_merged["threshold"],
        sweep_merged["scored"],
        color="#1f77b4",
        linewidth=2.0,
        label="Pretrain protocol",
    )
    axes[1].plot(
        sweep_racewise["threshold"],
        sweep_racewise["scored"],
        color="#d62728",
        linewidth=2.0,
        label="Racewise protocol",
    )
    axes[1].scatter(
        [float(best_merged["threshold"])],
        [float(best_merged["scored"])],
        color="#1f77b4",
        s=60,
        zorder=5,
    )
    axes[1].scatter(
        [float(best_racewise["threshold"])],
        [float(best_racewise["scored"])],
        color="#d62728",
        s=60,
        zorder=5,
    )
    axes[1].set_title("Threshold vs Scored Rows")
    axes[1].set_xlabel("Threshold")
    axes[1].set_ylabel("Scored rows")
    axes[1].grid(True, linestyle="--", alpha=0.5)

    fig.suptitle("Threshold Frontier Comparison: Pretrain vs Racewise", fontsize=14, y=1.02)
    fig.tight_layout()

    out6 = _save_fig(
        fig,
        reports_dir,
        stem=f"paper_fig6_threshold_frontier_{args.suffix}",
        formats=args.formats,
    )

    # Figure 7: windowed kappa distribution boxplot (paper-style stability summary).
    labels: list[str] = []
    values: list[np.ndarray] = []
    for method in order:
        sample = kappa_samples[method]
        if len(sample) == 0:
            continue
        labels.append(method)
        values.append(sample)

    fig, ax = plt.subplots(figsize=(11, 6))
    ax.boxplot(values, labels=labels, showfliers=False)
    ax.set_title("Distribution of Local Kappa over Time", fontsize=14, pad=12)
    ax.set_ylabel("Local Kappa")
    ax.grid(True, axis="y", linestyle="--", alpha=0.5)
    plt.setp(ax.get_xticklabels(), rotation=25, ha="right")

    out7 = _save_fig(
        fig,
        reports_dir,
        stem=f"paper_fig7_kappa_boxplot_{args.suffix}",
        formats=args.formats,
    )

    outputs = [*out0, *out1, *out2, *out3, *out4, *out5, *out6, *out7]

    print("=== TEMPORAL AND COMPARISON FIGURES GENERATED ===")
    print(f"timeline rows: {total_rows}")
    print(f"moa known predictions: {int(moa_diag.get('known_prediction_rows', 0))}")
    for method in order:
        avail_count = int(prediction_series[method].notna().sum())
        pos_count = int((prediction_series[method] == 1).sum())
        print(f"{method}: available_rows={avail_count}, positive_rows={pos_count}")
    for path in outputs:
        print(f"figure: {path}")


if __name__ == "__main__":
    main()
