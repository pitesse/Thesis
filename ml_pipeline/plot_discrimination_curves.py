"""Generate thesis-grade PR discrimination curves and PR-AUC metrics from existing OOF artifacts.

This script does not retrain any model. It reads existing OOF probability files and
produces:
- overall precision-recall curves for merged and racewise protocols,
- per-year precision-recall curves by protocol,
- a composite panel figure,
- machine-readable PR metrics and threshold operating-point tables.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, precision_recall_curve, auc

DEFAULT_THRESHOLD_SPECS: tuple[tuple[str, float], ...] = (
    ("threshold_0.50", 0.50),
    ("threshold_0.10", 0.10),
    ("threshold_0.05", 0.05),
)
SCORE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("raw_proba", "Raw probability"),
    ("calibrated_proba", "Calibrated probability"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate PR curves, PR-AUC tables, and threshold operating-point overlays from OOF artifacts"
    )
    parser.add_argument(
        "--reports-dir",
        type=Path,
        default=Path("data_lake/reports"),
        help="Directory containing OOF artifacts and where outputs will be written",
    )
    parser.add_argument(
        "--merged-oof",
        type=Path,
        default=None,
        help="OOF CSV for expanding_race protocol. Defaults to reports-dir/ml_oof_winner_2022_2025_merged.csv",
    )
    parser.add_argument(
        "--racewise-oof",
        type=Path,
        default=None,
        help="OOF CSV for expanding_race_sequential protocol. Defaults to reports-dir/ml_oof_winner_2022_2025_racewise.csv",
    )
    parser.add_argument(
        "--suffix",
        default="2022_2025",
        help="Suffix token used for output filenames",
    )
    parser.add_argument(
        "--formats",
        nargs="+",
        choices=["pdf", "png"],
        default=["pdf", "png"],
        help="Output formats for generated figures",
    )
    return parser.parse_args()


def _extract_year(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.split(" :: ", n=1).str[0], errors="coerce")


def _load_oof(path: Path, protocol_name: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"missing required OOF file for {protocol_name}: {path}")

    frame = pd.read_csv(path)
    required = {
        "race",
        "target_y",
        "raw_proba",
        "calibrated_proba",
        "constrained_threshold",
    }
    missing = [col for col in required if col not in frame.columns]
    if missing:
        raise ValueError(f"OOF file missing required columns {missing}: {path}")

    frame = frame.copy()
    frame["target_y"] = pd.to_numeric(frame["target_y"], errors="coerce").fillna(0).astype(int)
    frame["raw_proba"] = pd.to_numeric(frame["raw_proba"], errors="coerce")
    frame["calibrated_proba"] = pd.to_numeric(frame["calibrated_proba"], errors="coerce")
    frame["constrained_threshold"] = pd.to_numeric(frame["constrained_threshold"], errors="coerce")
    frame["year"] = _extract_year(frame["race"])
    frame["protocol"] = protocol_name

    return frame


def _safe_pr_auc_trapz(recall: np.ndarray, precision: np.ndarray) -> float:
    if recall.size == 0 or precision.size == 0:
        return float("nan")
    order = np.argsort(recall)
    return float(auc(recall[order], precision[order]))


def _compute_curve(y_true: pd.Series, y_score: pd.Series) -> dict[str, object]:
    valid = y_score.notna()
    y = y_true.loc[valid].astype(int).to_numpy()
    s = y_score.loc[valid].astype(float).to_numpy()

    if len(y) == 0:
        raise ValueError("empty score vector after NaN filtering")
    if int(np.unique(y).size) < 2:
        raise ValueError("target vector has a single class, PR curve undefined")

    precision, recall, thresholds = precision_recall_curve(y, s)
    ap = float(average_precision_score(y, s))
    trapz_auc = _safe_pr_auc_trapz(recall, precision)

    return {
        "precision": precision,
        "recall": recall,
        "thresholds": thresholds,
        "pr_auc_ap": ap,
        "pr_auc_trapz": trapz_auc,
        "n_rows": int(len(y)),
        "n_positive": int(y.sum()),
        "prevalence": float(y.mean()),
    }


def _threshold_specs(frame: pd.DataFrame) -> list[tuple[str, float]]:
    specs = list(DEFAULT_THRESHOLD_SPECS)
    constrained = pd.to_numeric(frame["constrained_threshold"], errors="coerce").dropna()
    if not constrained.empty:
        specs.append(("constrained_median", float(constrained.median())))

    dedup: list[tuple[str, float]] = []
    seen: set[float] = set()
    for label, value in specs:
        key = round(float(value), 8)
        if key in seen:
            continue
        seen.add(key)
        dedup.append((label, float(value)))
    return dedup


def _nearest_threshold_point(curve: dict[str, object], requested_threshold: float) -> dict[str, float]:
    thresholds = np.asarray(curve["thresholds"], dtype=float)
    precision = np.asarray(curve["precision"], dtype=float)
    recall = np.asarray(curve["recall"], dtype=float)

    if thresholds.size == 0:
        return {
            "effective_threshold": float("nan"),
            "precision": float("nan"),
            "recall": float("nan"),
        }

    idx = int(np.argmin(np.abs(thresholds - float(requested_threshold))))
    return {
        "effective_threshold": float(thresholds[idx]),
        "precision": float(precision[idx]),
        "recall": float(recall[idx]),
    }


def _save_fig(fig: plt.Figure, reports_dir: Path, stem: str, formats: list[str]) -> list[Path]:
    output_paths: list[Path] = []
    for ext in formats:
        path = reports_dir / f"{stem}.{ext}"
        fig.savefig(path, dpi=300, bbox_inches="tight")
        output_paths.append(path)
    plt.close(fig)
    return output_paths


def _plot_overall(
    curves: dict[tuple[str, str, str], dict[str, object]],
    operating_points: pd.DataFrame,
    protocols: list[str],
    reports_dir: Path,
    suffix: str,
    formats: list[str],
) -> list[Path]:
    colors = {
        "raw_proba": "#1f77b4",
        "calibrated_proba": "#d62728",
    }
    marker_map = {
        "threshold_0.50": "o",
        "threshold_0.10": "s",
        "threshold_0.05": "^",
        "constrained_median": "D",
    }

    fig, axes = plt.subplots(1, len(protocols), figsize=(14, 6), sharex=True, sharey=True)
    if len(protocols) == 1:
        axes = np.asarray([axes])

    for ax, protocol in zip(axes, protocols, strict=True):
        for score_col, score_label in SCORE_COLUMNS:
            curve = curves[(protocol, "overall", score_col)]
            ax.plot(
                curve["recall"],
                curve["precision"],
                linewidth=2.0,
                color=colors[score_col],
                label=f"{score_label} (AP={curve['pr_auc_ap']:.3f})",
            )

        prevalence = float(curves[(protocol, "overall", "calibrated_proba")]["prevalence"])
        ax.axhline(
            prevalence,
            linestyle="--",
            linewidth=1.2,
            color="#555555",
            alpha=0.8,
            label=f"Prevalence baseline ({prevalence:.3f})",
        )

        subset = operating_points[
            (operating_points["protocol"] == protocol)
            & (operating_points["year"] == "overall")
        ].copy()
        for _, row in subset.iterrows():
            score_col = str(row["score_type"])
            color = colors.get(score_col, "#333333")
            marker = marker_map.get(str(row["point_label"]), "x")
            ax.scatter(
                float(row["recall"]),
                float(row["precision"]),
                color=color,
                marker=marker,
                s=55,
                alpha=0.9,
                edgecolors="black",
                linewidths=0.4,
            )

        ax.set_title(
            f"{protocol.replace('ml_', '').replace('_', '-')} protocol",
            fontsize=13,
            pad=12,
        )
        ax.set_xlabel("Recall", fontsize=12)
        ax.set_ylabel("Precision", fontsize=12)
        ax.set_xlim(0.0, 1.01)
        ax.set_ylim(0.0, 1.01)
        ax.grid(True, linestyle="--", alpha=0.5)
        ax.legend(loc="lower left", fontsize=9)

    fig.suptitle("Precision-Recall Curves with Threshold Operating Points", fontsize=14, y=1.02)
    fig.tight_layout()

    return _save_fig(fig, reports_dir, f"pr_curves_overall_{suffix}", formats)


def _plot_by_year(
    curves: dict[tuple[str, str, str], dict[str, object]],
    protocols: list[str],
    protocol_years: dict[str, list[int]],
    reports_dir: Path,
    suffix: str,
    formats: list[str],
) -> list[Path]:
    fig, axes = plt.subplots(1, len(protocols), figsize=(14, 6), sharex=True, sharey=True)
    if len(protocols) == 1:
        axes = np.asarray([axes])

    cmap = plt.get_cmap("tab10")

    for ax, protocol in zip(axes, protocols, strict=True):
        years = protocol_years[protocol]
        for idx, year in enumerate(years):
            key = (protocol, str(year), "calibrated_proba")
            if key not in curves:
                continue
            curve = curves[key]
            ax.plot(
                curve["recall"],
                curve["precision"],
                linewidth=2.0,
                color=cmap(idx % 10),
                label=f"{year} (AP={curve['pr_auc_ap']:.3f}, n={int(curve['n_rows'])})",
            )

        ax.set_title(
            f"{protocol.replace('ml_', '').replace('_', '-')} by year (calibrated)",
            fontsize=13,
            pad=12,
        )
        ax.set_xlabel("Recall", fontsize=12)
        ax.set_ylabel("Precision", fontsize=12)
        ax.set_xlim(0.0, 1.01)
        ax.set_ylim(0.0, 1.01)
        ax.grid(True, linestyle="--", alpha=0.5)
        ax.legend(loc="lower left", fontsize=9)

    fig.suptitle("Per-Year Precision-Recall Curves (Calibrated Probabilities)", fontsize=14, y=1.02)
    fig.tight_layout()

    return _save_fig(fig, reports_dir, f"pr_curves_by_year_{suffix}", formats)


def _plot_panel(
    curves: dict[tuple[str, str, str], dict[str, object]],
    operating_points: pd.DataFrame,
    protocols: list[str],
    protocol_years: dict[str, list[int]],
    reports_dir: Path,
    suffix: str,
    formats: list[str],
) -> list[Path]:
    colors = {
        "raw_proba": "#1f77b4",
        "calibrated_proba": "#d62728",
    }

    fig, axes = plt.subplots(2, len(protocols), figsize=(16, 11), sharex=False, sharey=False)

    for col_idx, protocol in enumerate(protocols):
        ax_top = axes[0, col_idx]
        ax_bottom = axes[1, col_idx]

        for score_col, score_label in SCORE_COLUMNS:
            curve = curves[(protocol, "overall", score_col)]
            ax_top.plot(
                curve["recall"],
                curve["precision"],
                linewidth=2.0,
                color=colors[score_col],
                label=f"{score_label} (AP={curve['pr_auc_ap']:.3f})",
            )

        prevalence = float(curves[(protocol, "overall", "calibrated_proba")]["prevalence"])
        ax_top.axhline(prevalence, linestyle="--", linewidth=1.2, color="#555555", alpha=0.8)

        subset = operating_points[
            (operating_points["protocol"] == protocol)
            & (operating_points["year"] == "overall")
            & (operating_points["score_type"] == "calibrated_proba")
        ].copy()
        for _, row in subset.iterrows():
            ax_top.scatter(float(row["recall"]), float(row["precision"]), color="#111111", s=36)

        ax_top.set_title(f"{protocol.replace('ml_', '').replace('_', '-')} overall")
        ax_top.set_xlabel("Recall")
        ax_top.set_ylabel("Precision")
        ax_top.set_xlim(0.0, 1.01)
        ax_top.set_ylim(0.0, 1.01)
        ax_top.grid(True, linestyle="--", alpha=0.45)
        ax_top.legend(loc="lower left", fontsize=8)

        years = protocol_years[protocol]
        cmap = plt.get_cmap("tab10")
        for idx, year in enumerate(years):
            key = (protocol, str(year), "calibrated_proba")
            if key not in curves:
                continue
            curve = curves[key]
            ax_bottom.plot(
                curve["recall"],
                curve["precision"],
                linewidth=2.0,
                color=cmap(idx % 10),
                label=f"{year} AP={curve['pr_auc_ap']:.3f}",
            )

        ax_bottom.set_title(f"{protocol.replace('ml_', '').replace('_', '-')} by year")
        ax_bottom.set_xlabel("Recall")
        ax_bottom.set_ylabel("Precision")
        ax_bottom.set_xlim(0.0, 1.01)
        ax_bottom.set_ylim(0.0, 1.01)
        ax_bottom.grid(True, linestyle="--", alpha=0.45)
        ax_bottom.legend(loc="lower left", fontsize=8)

    fig.suptitle("PR Evidence Panel, Overall and Year-Split Views", fontsize=15, y=1.01)
    fig.tight_layout()

    return _save_fig(fig, reports_dir, f"pr_curves_panel_{suffix}", formats)


def main() -> None:
    args = parse_args()
    reports_dir = args.reports_dir
    reports_dir.mkdir(parents=True, exist_ok=True)

    merged_oof = args.merged_oof if args.merged_oof is not None else reports_dir / "ml_oof_winner_2022_2025_merged.csv"
    racewise_oof = args.racewise_oof if args.racewise_oof is not None else reports_dir / "ml_oof_winner_2022_2025_racewise.csv"

    protocol_frames = {
        "ml_pretrain": _load_oof(merged_oof, "ml_pretrain"),
        "ml_racewise": _load_oof(racewise_oof, "ml_racewise"),
    }

    curves: dict[tuple[str, str, str], dict[str, object]] = {}
    metric_rows: list[dict[str, object]] = []
    operating_rows: list[dict[str, object]] = []
    protocol_years: dict[str, list[int]] = {}

    for protocol, frame in protocol_frames.items():
        years = sorted(frame["year"].dropna().astype(int).unique().tolist())
        protocol_years[protocol] = years

        threshold_specs = _threshold_specs(frame)

        for score_col, _ in SCORE_COLUMNS:
            curve = _compute_curve(frame["target_y"], frame[score_col])
            curves[(protocol, "overall", score_col)] = curve
            metric_rows.append(
                {
                    "protocol": protocol,
                    "year": "overall",
                    "score_type": score_col,
                    "pr_auc_ap": float(curve["pr_auc_ap"]),
                    "pr_auc_trapz": float(curve["pr_auc_trapz"]),
                    "prevalence": float(curve["prevalence"]),
                    "n_rows": int(curve["n_rows"]),
                    "n_positive": int(curve["n_positive"]),
                }
            )

            for label, requested in threshold_specs:
                point = _nearest_threshold_point(curve, requested)
                operating_rows.append(
                    {
                        "protocol": protocol,
                        "year": "overall",
                        "score_type": score_col,
                        "point_label": label,
                        "requested_threshold": float(requested),
                        "effective_threshold": float(point["effective_threshold"]),
                        "precision": float(point["precision"]),
                        "recall": float(point["recall"]),
                    }
                )

        for year in years:
            subset = frame[frame["year"] == year]
            for score_col, _ in SCORE_COLUMNS:
                curve = _compute_curve(subset["target_y"], subset[score_col])
                curves[(protocol, str(year), score_col)] = curve
                metric_rows.append(
                    {
                        "protocol": protocol,
                        "year": str(year),
                        "score_type": score_col,
                        "pr_auc_ap": float(curve["pr_auc_ap"]),
                        "pr_auc_trapz": float(curve["pr_auc_trapz"]),
                        "prevalence": float(curve["prevalence"]),
                        "n_rows": int(curve["n_rows"]),
                        "n_positive": int(curve["n_positive"]),
                    }
                )

    metrics_df = pd.DataFrame(metric_rows)
    operating_df = pd.DataFrame(operating_rows)

    metrics_csv = reports_dir / f"pr_metrics_{args.suffix}.csv"
    operating_csv = reports_dir / f"pr_operating_points_{args.suffix}.csv"
    metrics_df.to_csv(metrics_csv, index=False)
    operating_df.to_csv(operating_csv, index=False)

    protocols = ["ml_pretrain", "ml_racewise"]
    outputs: list[Path] = []
    outputs.extend(_plot_overall(curves, operating_df, protocols, reports_dir, args.suffix, args.formats))
    outputs.extend(_plot_by_year(curves, protocols, protocol_years, reports_dir, args.suffix, args.formats))
    outputs.extend(_plot_panel(curves, operating_df, protocols, protocol_years, reports_dir, args.suffix, args.formats))

    print("=== PR DISCRIMINATION ARTIFACTS GENERATED ===")
    print(f"merged oof             : {merged_oof}")
    print(f"racewise oof           : {racewise_oof}")
    print(f"metrics csv            : {metrics_csv}")
    print(f"operating points csv   : {operating_csv}")

    for protocol in protocols:
        merged_rows = metrics_df[
            (metrics_df["protocol"] == protocol)
            & (metrics_df["year"] == "overall")
            & (metrics_df["score_type"] == "calibrated_proba")
        ]
        if not merged_rows.empty:
            row = merged_rows.iloc[0]
            print(
                f"{protocol} calibrated overall: AP={float(row['pr_auc_ap']):.6f}, "
                f"trapz={float(row['pr_auc_trapz']):.6f}, prevalence={float(row['prevalence']):.6f}, "
                f"n={int(row['n_rows'])}"
            )

    for path in outputs:
        print(f"figure                 : {path}")


if __name__ == "__main__":
    main()
