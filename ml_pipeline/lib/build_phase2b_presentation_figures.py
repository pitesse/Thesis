"""Build Phase 2B presentation figures from canonical dual-contract artifacts.

This script is rerunnable and resilient:
- missing inputs produce warnings and SKIP manifest entries,
- independent figure jobs can run in parallel (`--jobs`),
- outputs are written under one delivery folder for professor-facing review.
"""

from __future__ import annotations

import argparse
import math
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Rectangle
from sklearn.metrics import average_precision_score, precision_recall_curve


PROFILE_E0 = "e0_no_source_year"
PROFILE_P1 = "p1_percent_conservative_v1"
TARGET_PIT_ANY_CLEAN = "target_pit_any_h2_clean_actionable"
TARGET_PIT_SUCCESS_CLEAN = "target_pit_success_h2_clean_actionable"

PROFILE_DISPLAY = {
    PROFILE_E0: "No-Year Baseline",
    PROFILE_P1: "Percent Features",
}
PUBLIC_PROFILE_LABELS = {
    ("batch", PROFILE_E0): "Batch No-Year",
    ("batch", PROFILE_P1): "Batch Percent",
    ("moa", PROFILE_E0): "MOA No-Year",
    ("moa", PROFILE_P1): "MOA Percent",
}


@dataclass
class FigureResult:
    figure: str
    status: str  # PASS/WARN/SKIP/FAIL
    source_artifact: str
    note: str
    png_path: str = ""
    pdf_path: str = ""
    svg_path: str = ""
    formats: str = ""
    png_dpi: int | None = None
    width_in: float | None = None
    height_in: float | None = None


SUPPORTED_FORMATS = ("png", "pdf", "svg")
SAVE_FORMATS: tuple[str, ...] = ("png", "pdf")
SAVE_DPI = 350
SAVE_FIG_SCALE = 1.25
SAVE_PNG_PAD = 0.12
SAVE_META_BY_STEM: dict[str, dict[str, Any]] = {}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Phase 2B presentation figure pack (headline canonical + sensitivity)."
    )
    parser.add_argument(
        "--sde-aggregate-csv",
        default="data_lake/reports/final_sde_c6_cfg120_fixed_raw_clean_aggregate_2022_2025.csv",
    )
    parser.add_argument(
        "--sde-per-year-csv",
        default="data_lake/reports/final_sde_c6_cfg120_fixed_raw_clean_per_year_2022_2025.csv",
    )
    parser.add_argument(
        "--batch-compact-csv",
        default="data_lake/reports/ml_phase2b_dual_contract_2022_2025/recommended/phase2b_e0_vs_p1_canonical_compact.csv",
    )
    parser.add_argument(
        "--batch-frontier-csv",
        default="data_lake/reports/ml_phase2b_dual_contract_2022_2025/frontier/phase2b_threshold_frontier_compact.csv",
    )
    parser.add_argument(
        "--batch-by-year-csv",
        default="data_lake/reports/ml_phase2b_dual_contract_2022_2025/by_year/phase2b_threshold_frontier_by_year.csv",
    )
    parser.add_argument(
        "--batch-recommended-csv",
        default="data_lake/reports/ml_phase2b_dual_contract_2022_2025/recommended/phase2b_recommended_operating_points.csv",
    )
    parser.add_argument(
        "--batch-oof-dir",
        default="data_lake/reports/ml_phase2b_dual_contract_2022_2025/oof",
    )
    parser.add_argument(
        "--batch-oof-fallback-dir",
        default="data_lake/reports/ml_phase2a_dual_contract_2022_2025/oof",
        help="Fallback Batch OOF directory when Phase2B OOF is unavailable.",
    )

    parser.add_argument(
        "--sml-compact-csv",
        default="data_lake/reports/sml_phase2b_dual_contract_2022_2025/recommended/phase2b_e0_vs_p1_canonical_compact.csv",
    )
    parser.add_argument(
        "--sml-frontier-csv",
        default="data_lake/reports/sml_phase2b_dual_contract_2022_2025/frontier/phase2b_threshold_frontier_compact.csv",
    )
    parser.add_argument(
        "--sml-by-year-csv",
        default="data_lake/reports/sml_phase2b_dual_contract_2022_2025/by_year/phase2b_threshold_frontier_by_year.csv",
    )
    parser.add_argument(
        "--sml-recommended-csv",
        default="data_lake/reports/sml_phase2b_dual_contract_2022_2025/recommended/phase2b_recommended_operating_points.csv",
    )
    parser.add_argument(
        "--sml-matrix-csv",
        default="data_lake/reports/sml_phase2b_dual_contract_2022_2025/matrix_sde_truth/sml_phase2b_matrix_compact.csv",
    )
    parser.add_argument(
        "--sml-prequential-csv",
        default="data_lake/reports/sml_phase2b_dual_contract_2022_2025/prequential/sml_phase2b_preq_summary.csv",
    )
    parser.add_argument(
        "--sml-oof-dir",
        default="data_lake/reports/sml_phase2b_dual_contract_2022_2025/oof",
    )

    parser.add_argument(
        "--universe-summary-csv",
        default="data_lake/reports/ml_phase2b_dual_contract_2022_2025/audits/shared_universe_summary_2022_2025.csv",
    )
    parser.add_argument(
        "--pit-success-policy-diagnostic-csv",
        default="data_lake/reports/phase2b_presentation_figures/phase2b_pit_success_threshold_diagnostic.csv",
    )
    parser.add_argument(
        "--training-dataset-parquet",
        default="data_lake/ml_training_dataset_2022_2025_dual_contract.parquet",
    )
    parser.add_argument(
        "--race-by-race-metrics-csv",
        default="data_lake/reports/phase2b_presentation_figures/phase2b_race_by_race_metrics_clean_actionable.csv",
    )

    parser.add_argument(
        "--output-dir",
        default="data_lake/reports/phase2b_presentation_figures",
    )
    parser.add_argument(
        "--jobs",
        default="auto",
        help="parallel figure jobs: integer or 'auto'",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=350,
        help="PNG output DPI (default: 350).",
    )
    parser.add_argument(
        "--fig-scale",
        type=float,
        default=1.25,
        help="Global figure-size scale factor before saving (default: 1.25).",
    )
    parser.add_argument(
        "--formats",
        default="png,pdf,svg",
        help="Comma-separated output formats from: png,pdf,svg (default: png,pdf,svg).",
    )
    return parser.parse_args()


def _resolve_jobs(spec: str) -> int:
    s = str(spec).strip().lower()
    if s == "auto":
        cpu = os.cpu_count() or 2
        return max(1, min(8, cpu - 2))
    value = int(s)
    if value < 1:
        raise ValueError("--jobs must be >= 1 or 'auto'")
    return value


def _resolve_formats(spec: str) -> tuple[str, ...]:
    parts = [p.strip().lower() for p in str(spec).split(",") if p.strip()]
    if not parts:
        raise ValueError("--formats cannot be empty")
    bad = sorted(set(parts).difference(SUPPORTED_FORMATS))
    if bad:
        raise ValueError(f"--formats has unsupported values: {bad}; supported={SUPPORTED_FORMATS}")
    dedup: list[str] = []
    for p in parts:
        if p not in dedup:
            dedup.append(p)
    return tuple(dedup)


def _load_csv(path: Path, warnings: list[str], label: str) -> pd.DataFrame:
    if not path.exists():
        warnings.append(f"missing input ({label}): {path}")
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as exc:
        warnings.append(f"failed reading ({label}) {path}: {exc}")
        return pd.DataFrame()


def _save_figure(fig: plt.Figure, out_dir: Path, stem: str) -> tuple[Path, Path]:
    # Apply global presentation scaling once at save-time so all plots benefit.
    if SAVE_FIG_SCALE != 1.0:
        fig.set_size_inches(fig.get_size_inches() * SAVE_FIG_SCALE, forward=True)
    try:
        fig.canvas.draw()
    except Exception:
        pass

    width_in, height_in = fig.get_size_inches()
    png = out_dir / f"{stem}.png"
    pdf = out_dir / f"{stem}.pdf"
    svg = out_dir / f"{stem}.svg"
    produced: dict[str, Path] = {}
    for fmt in SAVE_FORMATS:
        path = out_dir / f"{stem}.{fmt}"
        if fmt == "png":
            fig.savefig(path, dpi=SAVE_DPI, format=fmt, bbox_inches="tight", pad_inches=SAVE_PNG_PAD)
        else:
            fig.savefig(path, format=fmt, bbox_inches="tight", pad_inches=0.04)
        produced[fmt] = path
    SAVE_META_BY_STEM[stem] = {
        "formats": ",".join(SAVE_FORMATS),
        "png_dpi": SAVE_DPI if "png" in SAVE_FORMATS else None,
        "width_in": float(width_in),
        "height_in": float(height_in),
        "png_path": str(produced.get("png", "")),
        "pdf_path": str(produced.get("pdf", "")),
        "svg_path": str(produced.get("svg", "")),
    }
    plt.close(fig)
    return produced.get("png", png), produced.get("pdf", pdf)


def _parse_year(race_value: Any) -> int | None:
    text = str(race_value)
    token = text.split(" :: ", maxsplit=1)[0]
    if token.isdigit() and len(token) == 4:
        return int(token)
    return None


def _safe_float(value: Any, default: float = float("nan")) -> float:
    try:
        v = float(value)
    except Exception:
        return default
    if math.isnan(v):
        return default
    return v


def _display_profile(profile: str, method: str | None = None) -> str:
    short = PROFILE_DISPLAY.get(profile, profile.replace("_", " "))
    if method is None:
        return short
    key = str(method).strip().lower()
    if key.startswith("batch"):
        key = "batch"
    elif key.startswith("sml") or key.startswith("moa"):
        key = "moa"
    return PUBLIC_PROFILE_LABELS.get((key, profile), f"{key.upper()} {short}")


def _apply_panel_layout(fig: plt.Figure, *, top: float = 0.84, bottom: float = 0.14, left: float = 0.07, right: float = 0.98, hspace: float = 0.34, wspace: float = 0.28) -> None:
    fig.subplots_adjust(top=top, bottom=bottom, left=left, right=right, hspace=hspace, wspace=wspace)


def _resolve_batch_oof_dir(primary: Path, fallback: Path, warnings: list[str]) -> tuple[Path, str]:
    if primary.exists():
        return primary, "phase2b"
    if fallback.exists():
        warnings.append(
            "phase2b batch OOF directory not present; using validated OOF probability-diagnostic source: phase2a OOF (explicit audited caveat)."
        )
        return fallback, "phase2a_fallback"
    warnings.append("missing batch OOF directory in both phase2b primary path and validated phase2a source path.")
    return primary, "missing"


def _pick_row(frame: pd.DataFrame, outcome_mode: str, truth_lens: str) -> pd.Series | None:
    if frame.empty:
        return None
    if "outcome_mode" not in frame.columns or "truth_lens" not in frame.columns:
        return None
    out = frame[frame["outcome_mode"].astype(str).eq(outcome_mode) & frame["truth_lens"].astype(str).eq(truth_lens)]
    if out.empty:
        return None
    return out.iloc[0]


def _batch_oof_source_note(context: dict[str, Any]) -> str:
    mode = str(context.get("batch_oof_source_mode", ""))
    if mode == "phase2a_fallback":
        return "Phase2A OOF reused intentionally for Phase2B Batch probability diagnostics (validated OOF probability-diagnostic source)"
    if mode == "phase2b":
        return "Phase2B Batch OOF source"
    return "Batch OOF source unavailable"


def _compute_prgain(precision: np.ndarray, recall: np.ndarray, prevalence: float) -> tuple[np.ndarray, np.ndarray]:
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
    rows: list[dict[str, float]] = []
    if len(y) == 0:
        return pd.DataFrame(rows)
    prevalence = float(y.mean())
    for threshold in thresholds:
        if threshold <= 0.0 or threshold >= 1.0:
            continue
        pred = s >= threshold
        tp = float(((pred == 1) & (y == 1)).sum())
        fp = float(((pred == 1) & (y == 0)).sum())
        odds = float(threshold / (1.0 - threshold))
        nb_model = (tp / len(y)) - (fp / len(y)) * odds
        nb_all = prevalence - (1.0 - prevalence) * odds
        rows.append(
            {
                "threshold": float(threshold),
                "net_benefit_model": float(nb_model),
                "net_benefit_all": float(nb_all),
                "net_benefit_none": 0.0,
            }
        )
    return pd.DataFrame(rows)


def _calibration_bins(y_true: np.ndarray, y_score: np.ndarray, n_bins: int = 10) -> pd.DataFrame:
    y = np.asarray(y_true, dtype=int)
    s = np.asarray(y_score, dtype=float)
    valid = np.isfinite(s)
    y = y[valid]
    s = np.clip(s[valid], 0.0, 1.0)
    if len(y) == 0:
        return pd.DataFrame(columns=["mean_pred", "observed_rate", "count"])

    q = np.linspace(0.0, 1.0, n_bins + 1)
    edges = np.quantile(s, q)
    edges = np.unique(edges)
    if len(edges) < 3:
        edges = np.linspace(0.0, 1.0, n_bins + 1)

    idx = np.digitize(s, edges[1:-1], right=False)
    rows: list[dict[str, float]] = []
    for b in range(len(edges) - 1):
        mask = idx == b
        count = int(mask.sum())
        rows.append(
            {
                "bin": int(b),
                "count": count,
                "mean_pred": float(s[mask].mean()) if count else float("nan"),
                "observed_rate": float(y[mask].mean()) if count else float("nan"),
            }
        )
    return pd.DataFrame(rows)


def _load_oof(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    required = {"race", "driver", "lapNumber", "target_y", "calibrated_proba", "raw_proba"}
    missing = sorted(required.difference(set(frame.columns)))
    if missing:
        raise ValueError(f"OOF missing columns {missing}: {path}")
    out = frame.copy()
    out["target_y"] = pd.to_numeric(out["target_y"], errors="coerce").fillna(0).astype(int)
    out["calibrated_proba"] = pd.to_numeric(out["calibrated_proba"], errors="coerce")
    out["raw_proba"] = pd.to_numeric(out["raw_proba"], errors="coerce")
    out["year"] = out["race"].map(_parse_year)
    out = out[out["year"].notna()].copy()
    out["year"] = out["year"].astype(int)
    return out


def _read_oof_pair(oof_dir: Path, target_col: str) -> tuple[pd.DataFrame | None, pd.DataFrame | None, str | None]:
    e0 = oof_dir / f"{PROFILE_E0}__{target_col}.csv"
    p1 = oof_dir / f"{PROFILE_P1}__{target_col}.csv"
    if not e0.exists() or not p1.exists():
        return None, None, f"missing OOF run files for {target_col} in {oof_dir}"
    return _load_oof(e0), _load_oof(p1), None


def _manifest_fail(stem: str, source: str, note: str) -> FigureResult:
    return FigureResult(figure=stem, status="SKIP", source_artifact=source, note=note)


def _cohen_kappa_from_counts(tp: float, fp: float, fn: float, tn: float) -> float:
    n = tp + fp + fn + tn
    if n <= 0:
        return float("nan")
    po = (tp + tn) / n
    pe = (((tp + fp) * (tp + fn)) + ((fn + tn) * (fp + tn))) / (n * n)
    den = 1.0 - pe
    if den <= 1e-12:
        return float("nan")
    return (po - pe) / den


def _gmean_from_counts(tp: float, fp: float, fn: float, tn: float) -> float:
    tpr_den = tp + fn
    tnr_den = tn + fp
    if tpr_den <= 0 or tnr_den <= 0:
        return float("nan")
    tpr = tp / tpr_den
    tnr = tn / tnr_den
    if tpr < 0 or tnr < 0:
        return float("nan")
    return float(np.sqrt(tpr * tnr))


def _load_race_metric_frame(context: dict[str, Any]) -> tuple[pd.DataFrame, str | None]:
    df = context.get("race_by_race_metrics", pd.DataFrame())
    if df.empty:
        return df, "race-by-race metrics csv missing or empty"

    need = {
        "system",
        "target",
        "race",
        "race_order",
        "year",
        "row_tp",
        "row_fp",
        "row_fn",
        "row_tn",
        "positives",
        "kappa",
        "gmean",
        "f0_5_row",
        "balanced_accuracy",
    }
    missing = sorted(need.difference(set(df.columns)))
    if missing:
        return pd.DataFrame(), f"race-by-race metrics missing columns {missing}"

    out = df.copy()
    numeric_cols = [
        "race_order",
        "year",
        "row_tp",
        "row_fp",
        "row_fn",
        "row_tn",
        "positives",
        "kappa",
        "gmean",
        "f0_5_row",
        "balanced_accuracy",
    ]
    for col in numeric_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out[out["target"].astype(str).isin(["pit_any_h2", "pit_success_h2"])].copy()
    out = out[out["year"].notna()].copy()
    out["year"] = out["year"].astype(int)

    # Build a target-level global race index so all systems align on the same x-axis.
    pieces: list[pd.DataFrame] = []
    for target, sub in out.groupby("target", sort=False):
        race_ref = (
            sub.groupby("race", as_index=False)
            .agg(
                year=("year", "min"),
                order_hint=("race_order", "median"),
            )
            .sort_values(["year", "order_hint", "race"], kind="mergesort")
            .reset_index(drop=True)
        )
        race_ref["race_idx"] = np.arange(1, len(race_ref) + 1, dtype=int)
        merged = sub.merge(race_ref[["race", "race_idx"]], on="race", how="left")
        pieces.append(merged)

    if not pieces:
        return pd.DataFrame(), "no supported targets in race-by-race metrics csv"
    return pd.concat(pieces, ignore_index=True), None


def _metric_ylim(values: list[float], metric: str) -> tuple[float, float]:
    arr = np.asarray([v for v in values if np.isfinite(v)], dtype=float)
    if arr.size == 0:
        return (0.0, 1.0)

    if metric == "kappa":
        lo = min(float(arr.min()), 0.0) - 0.04
        hi = max(float(arr.max()), 0.0) + 0.04
        lo = max(-0.25, lo)
        hi = min(0.85, hi)
        if hi - lo < 0.16:
            mid = 0.5 * (hi + lo)
            lo = max(-0.25, mid - 0.08)
            hi = min(0.85, mid + 0.08)
        return (lo, hi)

    lo = max(0.0, float(arr.min()) - 0.06)
    hi = min(1.0, float(arr.max()) + 0.06)
    if hi - lo < 0.16:
        mid = 0.5 * (hi + lo)
        lo = max(0.0, mid - 0.08)
        hi = min(1.0, mid + 0.08)
    return (lo, hi)


def _figure_seasonal_kappa_gmean_presentation(context: dict[str, Any]) -> FigureResult:
    stem = "phase2b_seasonal_kappa_gmean_presentation"
    df, err = _load_race_metric_frame(context)
    if err is not None:
        return _manifest_fail(stem, context["paths"]["race_by_race_metrics_csv"], err)

    systems = ["Batch No-Year", "Batch Percent", "MOA No-Year", "MOA Percent"]
    colors = {
        "Batch No-Year": "#4C78A8",
        "Batch Percent": "#F58518",
        "MOA No-Year": "#54A24B",
        "MOA Percent": "#E45756",
    }
    targets = ["pit_any_h2", "pit_success_h2"]

    pooled = (
        df.groupby(["system", "target", "year"], as_index=False)[["row_tp", "row_fp", "row_fn", "row_tn", "positives"]]
        .sum()
        .sort_values(["target", "year", "system"], kind="mergesort")
    )
    pooled["kappa"] = pooled.apply(
        lambda r: _cohen_kappa_from_counts(
            _safe_float(r["row_tp"], 0.0),
            _safe_float(r["row_fp"], 0.0),
            _safe_float(r["row_fn"], 0.0),
            _safe_float(r["row_tn"], 0.0),
        ),
        axis=1,
    )
    pooled["gmean"] = pooled.apply(
        lambda r: _gmean_from_counts(
            _safe_float(r["row_tp"], 0.0),
            _safe_float(r["row_fp"], 0.0),
            _safe_float(r["row_fn"], 0.0),
            _safe_float(r["row_tn"], 0.0),
        ),
        axis=1,
    )

    fig, axes = plt.subplots(2, 2, figsize=(15.2, 8.8), sharex=False)
    panel_specs = [
        ("pit_any_h2", "kappa", "pit_any_h2 - Kappa"),
        ("pit_success_h2", "kappa", "pit_success_h2 - Kappa"),
        ("pit_any_h2", "gmean", "pit_any_h2 - G-Mean"),
        ("pit_success_h2", "gmean", "pit_success_h2 - G-Mean"),
    ]
    for ax, (target, metric_col, title) in zip(axes.ravel(), panel_specs):
        sub = pooled[pooled["target"].astype(str).eq(target)].copy()
        years = sorted(sub["year"].dropna().astype(int).unique().tolist())
        panel_vals: list[float] = []
        for system in systems:
            s = sub[sub["system"].astype(str).eq(system)].sort_values("year", kind="mergesort")
            yv = pd.to_numeric(s[metric_col], errors="coerce").to_numpy(dtype=float)
            panel_vals.extend([float(v) for v in yv if np.isfinite(v)])
            if len(yv) == 0:
                continue
            ax.plot(
                s["year"].to_numpy(dtype=float),
                yv,
                marker="o",
                linewidth=2.2,
                markersize=6.5,
                color=colors[system],
                label=system,
            )
        lo, hi = _metric_ylim(panel_vals, "kappa" if metric_col == "kappa" else "gmean")
        ax.set_ylim(lo, hi)
        ax.set_xticks(years)
        ax.set_xlabel("Season")
        ax.set_ylabel("Score")
        ax.set_title(title)
        ax.grid(axis="y", alpha=0.25)
        ax.grid(axis="x", alpha=0.10)
        if metric_col == "kappa":
            ax.axhline(0.0, color="#666666", linewidth=0.9, linestyle="--", alpha=0.8)

    fig.suptitle("Seasonal Kappa and G-Mean (clean_actionable)", y=0.985)
    from matplotlib.lines import Line2D

    handles = [
        Line2D([0], [0], color=colors[s], marker="o", linewidth=2.2, markersize=6.5, label=s)
        for s in systems
    ]
    fig.legend(handles, systems, loc="upper center", bbox_to_anchor=(0.5, 0.955), ncol=4, frameon=False)
    _apply_panel_layout(fig, top=0.88, bottom=0.08, left=0.07, right=0.99, hspace=0.34, wspace=0.16)

    png, pdf = _save_figure(fig, context["output_dir"], stem)
    return FigureResult(
        figure=stem,
        status="PASS",
        source_artifact=context["paths"]["race_by_race_metrics_csv"],
        note="Season-level pooled confusion metrics for Kappa and G-Mean (clean_actionable).",
        png_path=str(png),
        pdf_path=str(pdf),
    )


def _seasonal_candle_frame(df: pd.DataFrame, metric_col: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (target, system, year), grp in df.groupby(["target", "system", "year"], sort=False):
        ordered = grp.sort_values("race_idx", kind="mergesort").copy()
        values = pd.to_numeric(ordered[metric_col], errors="coerce")
        valid = values[np.isfinite(values.to_numpy(dtype=float))]
        if valid.empty:
            continue
        rows.append(
            {
                "target": str(target),
                "system": str(system),
                "year": int(year),
                "low": float(valid.min()),
                "high": float(valid.max()),
                "n_races": int(len(valid)),
            }
        )
    return pd.DataFrame(rows)


def _figure_seasonal_metric_candles(
    context: dict[str, Any],
    *,
    metric_col: str,
    title: str,
    ylabel: str,
    stem: str,
) -> FigureResult:
    df, err = _load_race_metric_frame(context)
    if err is not None:
        return _manifest_fail(stem, context["paths"]["race_by_race_metrics_csv"], err)

    candle = _seasonal_candle_frame(df, metric_col=metric_col)
    if candle.empty:
        return _manifest_fail(stem, context["paths"]["race_by_race_metrics_csv"], f"no finite seasonal values for {metric_col}")

    systems = ["Batch No-Year", "Batch Percent", "MOA No-Year", "MOA Percent"]
    colors = {
        "Batch No-Year": "#4C78A8",
        "Batch Percent": "#F58518",
        "MOA No-Year": "#54A24B",
        "MOA Percent": "#E45756",
    }
    targets = ["pit_any_h2", "pit_success_h2"]
    metric_kind = "kappa" if metric_col == "kappa" else "gmean"

    years = sorted(candle["year"].dropna().astype(int).unique().tolist())
    if not years:
        return _manifest_fail(stem, context["paths"]["race_by_race_metrics_csv"], f"no seasons available for {metric_col}")

    offsets = np.linspace(-0.33, 0.33, len(systems))
    body_w = 0.15
    target_titles = {
        "pit_any_h2": "Pit Timing Contract (pit_any_h2)",
        "pit_success_h2": "Pit Success Contract (pit_success_h2)",
    }

    fig, axes = plt.subplots(2, 1, figsize=(15.4, 8.8), sharex=True)
    for ax, target in zip(axes, targets):
        sub = candle[candle["target"].astype(str).eq(target)].copy()
        panel_vals = (
            pd.concat([pd.to_numeric(sub["low"], errors="coerce"), pd.to_numeric(sub["high"], errors="coerce")], ignore_index=True)
            .dropna()
            .tolist()
        )
        lo, hi = _metric_ylim([float(v) for v in panel_vals if np.isfinite(v)], metric_kind)
        ax.set_ylim(lo, hi)

        # Alternate year shading to make seasonal grouping easier to read.
        for i, y in enumerate(years):
            if i % 2 == 0:
                ax.axvspan(y - 0.5, y + 0.5, color="#F6F8FA", alpha=0.75, zorder=0)

        for system, off in zip(systems, offsets):
            ss = sub[sub["system"].astype(str).eq(system)].sort_values("year", kind="mergesort")
            for _, row in ss.iterrows():
                x = float(row["year"]) + float(off)
                low = _safe_float(row["low"], float("nan"))
                high = _safe_float(row["high"], float("nan"))
                if not (np.isfinite(low) and np.isfinite(high)):
                    continue

                rect = Rectangle(
                    (x - (body_w / 2.0), low),
                    body_w,
                    max(0.0, high - low),
                    facecolor=colors[system],
                    edgecolor=colors[system],
                    linewidth=1.1,
                    alpha=0.55,
                    zorder=3,
                )
                ax.add_patch(rect)
                ax.vlines(x, low, high, color=colors[system], linewidth=1.8, alpha=0.95, zorder=4)

        ax.set_title(target_titles.get(target, target))
        ax.set_ylabel(ylabel)
        ax.set_xticks(years)
        ax.set_xticklabels([str(y) for y in years], fontsize=10)
        ax.grid(axis="y", alpha=0.25)
        ax.grid(axis="x", alpha=0.12)
        if metric_kind == "kappa":
            ax.axhline(0.0, color="#666666", linewidth=0.9, linestyle="--", alpha=0.8)

    axes[-1].set_xlabel("Season")
    fig.suptitle(title, y=0.985)

    from matplotlib.lines import Line2D

    system_handles = [
        Line2D([0], [0], color=colors[s], marker="s", linewidth=1.8, markersize=7, label=s)
        for s in systems
    ]
    fig.legend(system_handles, systems, loc="upper center", bbox_to_anchor=(0.5, 0.962), ncol=4, frameon=False)
    _apply_panel_layout(fig, top=0.89, bottom=0.10, left=0.07, right=0.99, hspace=0.30)

    png, pdf = _save_figure(fig, context["output_dir"], stem)
    return FigureResult(
        figure=stem,
        status="PASS",
        source_artifact=context["paths"]["race_by_race_metrics_csv"],
        note=f"Season-level candle bars for {metric_col} (seasonal low/high range by model).",
        png_path=str(png),
        pdf_path=str(pdf),
    )


def _figure_seasonal_kappa_candles_presentation(context: dict[str, Any]) -> FigureResult:
    return _figure_seasonal_metric_candles(
        context,
        metric_col="kappa",
        title="Seasonal Kappa Candlestick Bars (clean_actionable)",
        ylabel="Cohen's kappa",
        stem="phase2b_seasonal_kappa_candles_presentation",
    )


def _figure_seasonal_gmean_candles_presentation(context: dict[str, Any]) -> FigureResult:
    return _figure_seasonal_metric_candles(
        context,
        metric_col="gmean",
        title="Seasonal G-Mean Candlestick Bars (clean_actionable)",
        ylabel="G-Mean",
        stem="phase2b_seasonal_gmean_candles_presentation",
    )


def _figure_race_metric_v2(
    context: dict[str, Any],
    *,
    metric_col: str,
    ylabel: str,
    title: str,
    stem: str,
) -> FigureResult:
    df, err = _load_race_metric_frame(context)
    if err is not None:
        return _manifest_fail(stem, context["paths"]["race_by_race_metrics_csv"], err)

    systems = ["Batch No-Year", "Batch Percent", "MOA No-Year", "MOA Percent"]
    colors = {
        "Batch No-Year": "#4C78A8",
        "Batch Percent": "#F58518",
        "MOA No-Year": "#54A24B",
        "MOA Percent": "#E45756",
    }
    targets = ["pit_any_h2", "pit_success_h2"]

    fig, axes = plt.subplots(2, 1, figsize=(15.4, 9.2), sharex=False)
    for ax, target in zip(axes, targets):
        sub = df[df["target"].astype(str).eq(target)].copy()
        panel_vals = pd.to_numeric(sub[metric_col], errors="coerce").to_numpy(dtype=float)
        ylim_metric = "kappa" if metric_col == "kappa" else "gmean"
        lo, hi = _metric_ylim([float(v) for v in panel_vals if np.isfinite(v)], ylim_metric)
        if metric_col == "balanced_accuracy":
            lo = max(0.35, lo)
        ax.set_ylim(lo, hi)

        max_idx = int(pd.to_numeric(sub["race_idx"], errors="coerce").max()) if not sub.empty else 0
        for system in systems:
            s = sub[sub["system"].astype(str).eq(system)].sort_values("race_idx", kind="mergesort")
            if s.empty:
                continue
            x = pd.to_numeric(s["race_idx"], errors="coerce").to_numpy(dtype=float)
            y = pd.to_numeric(s[metric_col], errors="coerce").to_numpy(dtype=float)
            pos = pd.to_numeric(s["positives"], errors="coerce").fillna(0.0).to_numpy(dtype=float)

            stable = pos >= 5.0
            ax.scatter(
                x[~stable],
                y[~stable],
                s=22,
                color=colors[system],
                alpha=0.18,
                linewidths=0,
            )
            ax.scatter(
                x[stable],
                y[stable],
                s=34,
                color=colors[system],
                alpha=0.82,
                linewidths=0,
                label=system,
            )
            roll = (
                pd.Series(y, index=x)
                .sort_index()
                .rolling(window=7, min_periods=3)
                .median()
                .to_numpy(dtype=float)
            )
            ax.plot(x, roll, color=colors[system], linewidth=1.6, alpha=0.55)

        if metric_col == "kappa":
            ax.axhline(0.0, color="#666666", linewidth=0.9, linestyle="--", alpha=0.75)
        ax.set_xlim(0.0, max(5.0, float(max_idx) + 1.5))
        ax.set_ylabel(ylabel)
        ax.set_title(f"{target} (scatter + 7-race rolling median)")
        ax.grid(axis="y", alpha=0.25)
        ax.grid(axis="x", alpha=0.10)
        ax.set_xlabel("Race order (2022 to 2025)")

    fig.suptitle(title, y=0.985)
    from matplotlib.lines import Line2D

    handles = [
        Line2D(
            [0],
            [0],
            color=colors[s],
            marker="o",
            linewidth=1.6,
            markersize=6,
            alpha=0.85,
            label=s,
        )
        for s in systems
    ]
    fig.legend(handles, systems, loc="upper center", bbox_to_anchor=(0.5, 0.955), ncol=4, frameon=False)
    _apply_panel_layout(fig, top=0.89, bottom=0.08, left=0.07, right=0.99, hspace=0.32)

    png, pdf = _save_figure(fig, context["output_dir"], stem)
    return FigureResult(
        figure=stem,
        status="PASS",
        source_artifact=context["paths"]["race_by_race_metrics_csv"],
        note=f"Race-level {metric_col} diagnostic (scatter with rolling median; low-support points faded).",
        png_path=str(png),
        pdf_path=str(pdf),
    )


def _figure_race_by_race_kappa_v2(context: dict[str, Any]) -> FigureResult:
    return _figure_race_metric_v2(
        context,
        metric_col="kappa",
        ylabel="Cohen's kappa",
        title="Race-by-Race Kappa (clean_actionable)",
        stem="phase2b_race_by_race_kappa_clean_actionable_v2",
    )


def _figure_race_by_race_gmean_v2(context: dict[str, Any]) -> FigureResult:
    return _figure_race_metric_v2(
        context,
        metric_col="gmean",
        ylabel="G-Mean",
        title="Race-by-Race G-Mean (clean_actionable)",
        stem="phase2b_race_by_race_gmean_clean_actionable_v2",
    )


def _figure_race_by_race_f05_v2(context: dict[str, Any]) -> FigureResult:
    return _figure_race_metric_v2(
        context,
        metric_col="f0_5_row",
        ylabel="F0.5 (row-level)",
        title="Race-by-Race F0.5 (clean_actionable)",
        stem="phase2b_race_by_race_f05_clean_actionable_v2",
    )


def _figure_race_by_race_balanced_accuracy_v2(context: dict[str, Any]) -> FigureResult:
    return _figure_race_metric_v2(
        context,
        metric_col="balanced_accuracy",
        ylabel="Balanced accuracy",
        title="Race-by-Race Balanced Accuracy (clean_actionable)",
        stem="phase2b_race_by_race_balanced_accuracy_clean_actionable_v2",
    )


def _figure_universe_alignment(context: dict[str, Any]) -> FigureResult:
    stem = "universe_alignment_bar"
    df = context["universe"]
    if df.empty:
        return _manifest_fail(stem, context["paths"]["universe_summary_csv"], "missing universe summary csv")

    need = {"ml_oof", "sde_variant", "shared_intersection"}
    lookup = {
        str(r["universe"]): int(r["race_driver_count"])
        for _, r in df.iterrows()
        if pd.notna(r.get("universe")) and pd.notna(r.get("race_driver_count"))
    }
    if not need.issubset(set(lookup.keys())):
        return _manifest_fail(stem, context["paths"]["universe_summary_csv"], "required universe rows not found")

    labels = ["ML OOF", "SDE Variant", "Shared"]
    vals = [lookup["ml_oof"], lookup["sde_variant"], lookup["shared_intersection"]]

    fig, ax = plt.subplots(figsize=(7.6, 4.4))
    bars = ax.bar(labels, vals, color=["#4C78A8", "#F58518", "#54A24B"])
    ax.set_title("Universe Alignment (Race/Driver Pairs)")
    ax.set_ylabel("Count")
    ax.grid(axis="y", alpha=0.25)
    for bar, v in zip(bars, vals):
        ax.text(bar.get_x() + bar.get_width() / 2.0, v, f"{v}", ha="center", va="bottom", fontsize=9)

    png, pdf = _save_figure(fig, context["output_dir"], stem)
    return FigureResult(
        figure=stem,
        status="PASS",
        source_artifact=str(context["paths"]["universe_summary_csv"]),
        note="ML/SDE/shared race-driver coverage snapshot.",
        png_path=str(png),
        pdf_path=str(pdf),
    )


def _sde_metrics_for_outcome(sde_row: pd.Series, outcome_mode: str) -> dict[str, float]:
    if outcome_mode == "pit_any_h2":
        row_tp = _safe_float(sde_row.get("pit_any_row_tp"), 0.0)
        tp_for_recall = _safe_float(sde_row.get("pit_any_tp_for_recall"), 0.0)
        fp = _safe_float(sde_row.get("pit_any_fp"), 0.0)
        fn = _safe_float(sde_row.get("pit_any_fn"), 0.0)
        scored = row_tp + fp
        p = _safe_float(sde_row.get("pit_any_precision"), 0.0)
        r = _safe_float(sde_row.get("pit_any_recall"), 0.0)
        f05 = _safe_float(sde_row.get("pit_any_f0_5"), 0.0)
        eligible_actual_pit_count = _safe_float(sde_row.get("pit_any_eligible_actual_pits"), tp_for_recall + fn)
    else:
        row_tp = _safe_float(sde_row.get("pit_success_tp"), 0.0)
        tp_for_recall = row_tp
        fp = _safe_float(sde_row.get("pit_success_fp"), 0.0)
        fn = _safe_float(sde_row.get("pit_any_fn"), 0.0)
        scored = _safe_float(sde_row.get("pit_success_scored"), row_tp + fp)
        p = _safe_float(sde_row.get("pit_success_precision"), 0.0)
        r = (tp_for_recall / (tp_for_recall + fn)) if (tp_for_recall + fn) > 0 else 0.0
        b2 = 0.25
        denom = b2 * p + r
        f05 = ((1 + b2) * p * r / denom) if denom > 0 else 0.0
        eligible_actual_pit_count = tp_for_recall + fn
    return {
        "row_tp": row_tp,
        "tp_for_recall": tp_for_recall,
        "fp": fp,
        "fn": fn,
        "scored": scored,
        "precision": p,
        "recall": r,
        "f0_5": f05,
        "eligible_actual_pit_count": eligible_actual_pit_count,
    }


def _figure_clean_actionable_precision_recall_f05(context: dict[str, Any]) -> FigureResult:
    stem = "phase2b_clean_actionable_precision_recall_f05"
    sde = context["sde_agg"]
    batch = context["batch_compact"]
    sml = context["sml_compact"]
    if sde.empty or batch.empty or sml.empty:
        return _manifest_fail(stem, "multiple", "missing SDE/Batch/SML compact inputs")

    sde_row = sde[sde["truth_lens"].astype(str).eq("clean_actionable")]
    if sde_row.empty:
        return _manifest_fail(stem, context["paths"]["sde_aggregate_csv"], "missing SDE clean_actionable row")
    sde_row = sde_row.iloc[0]

    rows: dict[str, dict[str, float]] = {}
    for outcome in ["pit_any_h2", "pit_success_h2"]:
        b = _pick_row(batch, outcome, "clean_actionable")
        m = _pick_row(sml, outcome, "clean_actionable")
        if b is None or m is None:
            return _manifest_fail(stem, "compact CSVs", f"missing compact rows for {outcome} clean_actionable")
        rows[f"sde__{outcome}"] = _sde_metrics_for_outcome(sde_row, outcome)
        rows[f"batch_e0__{outcome}"] = {
            "precision": _safe_float(b.get("e0_precision"), 0.0),
            "recall": _safe_float(b.get("e0_recall"), 0.0),
            "f0_5": _safe_float(b.get("e0_f0_5"), 0.0),
        }
        rows[f"batch_p1__{outcome}"] = {
            "precision": _safe_float(b.get("p1_precision"), 0.0),
            "recall": _safe_float(b.get("p1_recall"), 0.0),
            "f0_5": _safe_float(b.get("p1_f0_5"), 0.0),
        }
        rows[f"sml_e0__{outcome}"] = {
            "precision": _safe_float(m.get("e0_precision"), 0.0),
            "recall": _safe_float(m.get("e0_recall"), 0.0),
            "f0_5": _safe_float(m.get("e0_f0_5"), 0.0),
        }
        rows[f"sml_p1__{outcome}"] = {
            "precision": _safe_float(m.get("p1_precision"), 0.0),
            "recall": _safe_float(m.get("p1_recall"), 0.0),
            "f0_5": _safe_float(m.get("p1_f0_5"), 0.0),
        }

    labels = ["Final SDE", "Batch No-Year", "Batch Percent", "MOA No-Year", "MOA Percent"]
    outcomes = [("pit_any_h2", "pit_any_h2"), ("pit_success_h2", "pit_success_h2")]
    fig, axes = plt.subplots(1, 2, figsize=(14.6, 5.6), sharey=True)
    width = 0.24
    for ax, (outcome, title) in zip(axes, outcomes):
        prec = [
            rows[f"sde__{outcome}"]["precision"],
            rows[f"batch_e0__{outcome}"]["precision"],
            rows[f"batch_p1__{outcome}"]["precision"],
            rows[f"sml_e0__{outcome}"]["precision"],
            rows[f"sml_p1__{outcome}"]["precision"],
        ]
        rec = [
            rows[f"sde__{outcome}"]["recall"],
            rows[f"batch_e0__{outcome}"]["recall"],
            rows[f"batch_p1__{outcome}"]["recall"],
            rows[f"sml_e0__{outcome}"]["recall"],
            rows[f"sml_p1__{outcome}"]["recall"],
        ]
        f05 = [
            rows[f"sde__{outcome}"]["f0_5"],
            rows[f"batch_e0__{outcome}"]["f0_5"],
            rows[f"batch_p1__{outcome}"]["f0_5"],
            rows[f"sml_e0__{outcome}"]["f0_5"],
            rows[f"sml_p1__{outcome}"]["f0_5"],
        ]
        x = np.arange(len(labels))
        ax.bar(x - width, prec, width=width, color="#4C78A8", label="Precision")
        ax.bar(x, rec, width=width, color="#F58518", label="Recall")
        ax.bar(x + width, f05, width=width, color="#54A24B", label="F0.5")
        ax.set_title(title)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=18, ha="right")
        ax.grid(axis="y", alpha=0.25)
        ax.set_ylim(0.0, 1.0)
    axes[0].set_ylabel("Metric")
    fig.legend(loc="upper center", bbox_to_anchor=(0.5, 0.92), ncol=3, frameon=False)
    fig.suptitle("Phase 2B Headline (clean_actionable): Precision / Recall / F0.5", y=0.98)
    _apply_panel_layout(fig, top=0.78, bottom=0.22)

    png, pdf = _save_figure(fig, context["output_dir"], stem)
    return FigureResult(
        figure=stem,
        status="PASS",
        source_artifact="SDE aggregate + Batch/SML canonical compact",
        note="Headline comparator bars under canonical SDE truth universe.",
        png_path=str(png),
        pdf_path=str(pdf),
    )


def _figure_clean_actionable_scored_tp_fp(context: dict[str, Any]) -> FigureResult:
    stem = "phase2b_clean_actionable_scored_tp_fp"
    sde = context["sde_agg"]
    batch = context["batch_compact"]
    sml = context["sml_compact"]
    if sde.empty or batch.empty or sml.empty:
        return _manifest_fail(stem, "multiple", "missing SDE/Batch/SML compact inputs")

    sde_row = sde[sde["truth_lens"].astype(str).eq("clean_actionable")]
    if sde_row.empty:
        return _manifest_fail(stem, context["paths"]["sde_aggregate_csv"], "missing SDE clean_actionable row")
    sde_row = sde_row.iloc[0]

    fig, axes = plt.subplots(2, 2, figsize=(16.0, 9.0), sharey=False)
    outcomes = ["pit_any_h2", "pit_success_h2"]

    for row_i, outcome in enumerate(outcomes):
        ax_dec = axes[row_i, 0]
        ax_evt = axes[row_i, 1]
        b = _pick_row(batch, outcome, "clean_actionable")
        m = _pick_row(sml, outcome, "clean_actionable")
        if b is None or m is None:
            return _manifest_fail(stem, "compact CSVs", f"missing compact rows for {outcome} clean_actionable")
        sde_metrics = _sde_metrics_for_outcome(sde_row, outcome)

        labels = ["Final SDE", "Batch No-Year", "Batch Percent", "MOA No-Year", "MOA Percent"]
        scored = [
            sde_metrics["scored"],
            _safe_float(b.get("e0_scored"), 0.0),
            _safe_float(b.get("p1_scored"), 0.0),
            _safe_float(m.get("e0_scored"), 0.0),
            _safe_float(m.get("p1_scored"), 0.0),
        ]
        row_tp = [
            sde_metrics["row_tp"],
            _safe_float(b.get("e0_row_tp"), 0.0),
            _safe_float(b.get("p1_row_tp"), 0.0),
            _safe_float(m.get("e0_row_tp"), 0.0),
            _safe_float(m.get("p1_row_tp"), 0.0),
        ]
        tp_recall = [
            sde_metrics["tp_for_recall"],
            _safe_float(b.get("e0_tp_for_recall"), 0.0),
            _safe_float(b.get("p1_tp_for_recall"), 0.0),
            _safe_float(m.get("e0_tp_for_recall"), 0.0),
            _safe_float(m.get("p1_tp_for_recall"), 0.0),
        ]
        fp = [
            sde_metrics["fp"],
            _safe_float(b.get("e0_fp"), 0.0),
            _safe_float(b.get("p1_fp"), 0.0),
            _safe_float(m.get("e0_fp"), 0.0),
            _safe_float(m.get("p1_fp"), 0.0),
        ]

        x = np.arange(len(labels))

        # Decision-row accounting panel.
        w_dec = 0.25
        ax_dec.bar(x - w_dec, scored, width=w_dec, label="scored rows", color="#4C78A8")
        ax_dec.bar(x, row_tp, width=w_dec, label="row TP", color="#59A14F")
        ax_dec.bar(x + w_dec, fp, width=w_dec, label="row FP", color="#E15759")
        ax_dec.set_xticks(x)
        ax_dec.set_xticklabels(labels, rotation=18, ha="right")
        ax_dec.set_title(f"{outcome} - decision-row accounting")
        ax_dec.grid(axis="y", alpha=0.25)

        # Event-coverage accounting panel.
        fn = [max(_safe_float(sde_metrics["eligible_actual_pit_count"], 0.0) - _safe_float(v, 0.0), 0.0) for v in tp_recall]
        eligible = _safe_float(sde_metrics["eligible_actual_pit_count"], 0.0)
        w_evt = 0.30
        ax_evt.bar(x - 0.5 * w_evt, tp_recall, width=w_evt, label="truth events covered", color="#F28E2B")
        ax_evt.bar(x + 0.5 * w_evt, fn, width=w_evt, label="FN events", color="#B07AA1")
        ax_evt.axhline(eligible, linestyle="--", color="#6B6B6B", linewidth=1.1, label="eligible truth events")
        ax_evt.set_xticks(x)
        ax_evt.set_xticklabels(labels, rotation=18, ha="right")
        ax_evt.set_title(f"{outcome} - event-level recall accounting")
        ax_evt.grid(axis="y", alpha=0.25)

    axes[0, 0].set_ylabel("Count")
    axes[1, 0].set_ylabel("Count")
    axes[0, 1].set_ylabel("Count")
    axes[1, 1].set_ylabel("Count")

    h_dec, l_dec = axes[0, 0].get_legend_handles_labels()
    h_evt, l_evt = axes[0, 1].get_legend_handles_labels()
    fig.legend(h_dec + h_evt, l_dec + l_evt, loc="upper center", bbox_to_anchor=(0.5, 0.95), ncol=6, frameon=False)
    fig.suptitle(
        "Phase 2B Headline (clean_actionable): decision rows vs event coverage\n"
        "(row TP and truth events covered are different accounting units)",
        y=0.99,
    )
    _apply_panel_layout(fig, top=0.83, bottom=0.19)

    png, pdf = _save_figure(fig, context["output_dir"], stem)
    return FigureResult(
        figure=stem,
        status="PASS",
        source_artifact="SDE aggregate + Batch/SML canonical compact",
        note="Separates decision-row counts from event-level truth coverage in one figure.",
        png_path=str(png),
        pdf_path=str(pdf),
    )


def _figure_delta_clean_actionable(context: dict[str, Any]) -> FigureResult:
    stem = "phase2b_e0_vs_p1_delta_clean_actionable"
    batch = context["batch_compact"]
    sml = context["sml_compact"]
    if batch.empty or sml.empty:
        return _manifest_fail(stem, "compact CSVs", "missing Batch/SML canonical compact")

    rows = []
    for outcome in ["pit_any_h2", "pit_success_h2"]:
        b = _pick_row(batch, outcome, "clean_actionable")
        m = _pick_row(sml, outcome, "clean_actionable")
        if b is None or m is None:
            return _manifest_fail(stem, "compact CSVs", f"missing rows for {outcome}")
        rows.append(("Batch", outcome, b))
        rows.append(("MOA", outcome, m))

    metrics = [
        ("delta_AP_p1_minus_e0", "AP"),
        ("delta_precision_p1_minus_e0", "precision"),
        ("delta_recall_p1_minus_e0", "recall"),
        ("delta_f0_5_p1_minus_e0", "f0.5"),
        ("delta_scored_p1_minus_e0", "scored"),
    ]

    fig, axes = plt.subplots(1, 2, figsize=(14.6, 5.4), sharey=False)
    for ax, outcome in zip(axes, ["pit_any_h2", "pit_success_h2"]):
        subset = [r for r in rows if r[1] == outcome]
        x = np.arange(len(metrics))
        w = 0.35
        batch_vals = [_safe_float(subset[0][2].get(m[0]), 0.0) for m in metrics]
        sml_vals = [_safe_float(subset[1][2].get(m[0]), 0.0) for m in metrics]
        ax.bar(x - w / 2, batch_vals, width=w, label="Batch Percent - Batch No-Year", color="#4C78A8")
        ax.bar(x + w / 2, sml_vals, width=w, label="MOA Percent - MOA No-Year", color="#F58518")
        ax.axhline(0.0, color="black", linewidth=0.8)
        ax.set_xticks(x)
        ax.set_xticklabels([m[1] for m in metrics], rotation=24, ha="right")
        ax.set_title(outcome)
        ax.grid(axis="y", alpha=0.25)
    fig.legend(loc="upper center", bbox_to_anchor=(0.5, 0.92), ncol=2, frameon=False)
    fig.suptitle("No-Year vs Percent Delta (clean_actionable)", y=0.98)
    _apply_panel_layout(fig, top=0.78, bottom=0.24)

    png, pdf = _save_figure(fig, context["output_dir"], stem)
    return FigureResult(
        figure=stem,
        status="PASS",
        source_artifact="phase2b_e0_vs_p1_canonical_compact.csv",
        note="Highlights profile trade-offs for Batch and MOA separately.",
        png_path=str(png),
        pdf_path=str(pdf),
    )


def _pr_curve_figure(
    *,
    method_name: str,
    oof_dir: Path,
    stem: str,
    by_year: bool,
    context: dict[str, Any],
) -> FigureResult:
    targets = [
        (TARGET_PIT_ANY_CLEAN, "pit_any_h2"),
        (TARGET_PIT_SUCCESS_CLEAN, "pit_success_h2"),
    ]
    if not oof_dir.exists():
        return _manifest_fail(stem, str(oof_dir), "OOF directory missing")

    frames: dict[str, dict[str, pd.DataFrame]] = {}
    for target_col, outcome in targets:
        e0, p1, err = _read_oof_pair(oof_dir, target_col)
        if err:
            return _manifest_fail(stem, str(oof_dir), err)
        assert e0 is not None and p1 is not None
        frames[outcome] = {PROFILE_E0: e0, PROFILE_P1: p1}

    if by_year:
        years = sorted(
            set(frames["pit_any_h2"][PROFILE_E0]["year"].unique())
            | set(frames["pit_any_h2"][PROFILE_P1]["year"].unique())
            | set(frames["pit_success_h2"][PROFILE_E0]["year"].unique())
            | set(frames["pit_success_h2"][PROFILE_P1]["year"].unique())
        )
        if not years:
            return _manifest_fail(stem, str(oof_dir), "no year values available in OOF")
        fig, axes = plt.subplots(2, len(years), figsize=(4.9 * len(years), 8.8), sharex=True, sharey=True)
        if len(years) == 1:
            axes = np.array([[axes[0]], [axes[1]]])
        for row_i, outcome in enumerate(["pit_any_h2", "pit_success_h2"]):
            for col_i, year in enumerate(years):
                ax = axes[row_i, col_i]
                for profile, color in [(PROFILE_E0, "#4C78A8"), (PROFILE_P1, "#F58518")]:
                    df = frames[outcome][profile]
                    sub = df[df["year"] == int(year)]
                    if sub.empty:
                        continue
                    y = sub["target_y"].to_numpy()
                    s = pd.to_numeric(sub["calibrated_proba"], errors="coerce").fillna(0.0).to_numpy()
                    if len(np.unique(y)) < 2:
                        continue
                    precision, recall, _ = precision_recall_curve(y, s)
                    ap = float(average_precision_score(y, s))
                    ax.plot(
                        recall,
                        precision,
                        linewidth=1.6,
                        label=f"{_display_profile(profile, method_name)} AP={ap:.3f}",
                        color=color,
                    )
                ax.set_title(f"{outcome} - {year}")
                ax.grid(alpha=0.25)
                ax.set_xlim(0.0, 1.0)
                ax.set_ylim(0.0, 1.0)
                ax.legend(loc="lower left", frameon=False, fontsize=8)
        axes[1, 0].set_xlabel("Recall")
        axes[0, 0].set_ylabel("Precision")
        fig.suptitle(f"{method_name} PR Curves by Year (clean_actionable)", y=0.98)
        _apply_panel_layout(fig, top=0.90, bottom=0.12, left=0.07, right=0.99, hspace=0.38, wspace=0.25)
    else:
        fig, axes = plt.subplots(1, 2, figsize=(14.4, 5.4), sharex=True, sharey=True)
        for ax, outcome in zip(axes, ["pit_any_h2", "pit_success_h2"]):
            for profile, color in [(PROFILE_E0, "#4C78A8"), (PROFILE_P1, "#F58518")]:
                df = frames[outcome][profile]
                y = df["target_y"].to_numpy()
                s = pd.to_numeric(df["calibrated_proba"], errors="coerce").fillna(0.0).to_numpy()
                if len(np.unique(y)) < 2:
                    continue
                precision, recall, _ = precision_recall_curve(y, s)
                ap = float(average_precision_score(y, s))
                ax.plot(
                    recall,
                    precision,
                    linewidth=2.0,
                    label=f"{_display_profile(profile, method_name)} AP={ap:.3f}",
                    color=color,
                )
            ax.set_title(outcome)
            ax.grid(alpha=0.25)
            ax.set_xlim(0.0, 1.0)
            ax.set_ylim(0.0, 1.0)
            ax.legend(loc="lower left", frameon=False)
        axes[0].set_xlabel("Recall")
        axes[0].set_ylabel("Precision")
        fig.suptitle(f"{method_name} PR Curves (clean_actionable)", y=0.98)
        _apply_panel_layout(fig, top=0.88, bottom=0.14)

    png, pdf = _save_figure(fig, context["output_dir"], stem)
    note = f"{method_name} PR curve family under clean_actionable lens."
    if method_name.lower().startswith("batch"):
        note = f"{note} {_batch_oof_source_note(context)}."
    return FigureResult(
        figure=stem,
        status="PASS",
        source_artifact=str(oof_dir),
        note=note,
        png_path=str(png),
        pdf_path=str(pdf),
    )


def _read_flink_pr_operating_points() -> dict[str, tuple[float, float]]:
    """Return Flink Strategy Engine operating points for PR overlays.

    Values are read from final_refresh canonical slide tables:
    - pit_any: precision + event_recall (from pit_any_final_recap.csv)
    - pit_success: strict_precision + successful_event_coverage
      (from pit_success_apples_to_apples.csv)
    """
    final_dir = Path("data_lake/reports/phase2b_presentation_figures/final_refresh")
    pit_any_path = final_dir / "pit_any_final_recap.csv"
    pit_success_path = final_dir / "pit_success_apples_to_apples.csv"

    points: dict[str, tuple[float, float]] = {}

    if pit_any_path.exists():
        any_df = pd.read_csv(pit_any_path)
        row = any_df[any_df["System"].astype(str).eq("Flink Strategy Engine")]
        if not row.empty:
            p = _safe_float(row.iloc[0].get("precision"), float("nan"))
            r = _safe_float(row.iloc[0].get("event_recall"), float("nan"))
            if np.isfinite(p) and np.isfinite(r):
                points["pit_any_h2"] = (r, p)

    if pit_success_path.exists():
        suc_df = pd.read_csv(pit_success_path)
        row = suc_df[suc_df["System"].astype(str).eq("Flink Strategy Engine")]
        if not row.empty:
            p = _safe_float(row.iloc[0].get("strict_precision"), float("nan"))
            r = _safe_float(row.iloc[0].get("successful_event_coverage"), float("nan"))
            if np.isfinite(p) and np.isfinite(r):
                points["pit_success_h2"] = (r, p)

    return points


def build_final_refresh_pr_assets(
    output_dir: Path | None = None,
) -> tuple[list[Path], pd.DataFrame]:
    """Build slide-ready PR/AP assets in old presentation style.

    Outputs:
    - pr_curve_pit_any_final.(png|pdf)
    - pr_curve_pit_success_final.(png|pdf)
    - ap_summary_final.csv/.md
    """
    out_dir = output_dir or Path("data_lake/reports/phase2b_presentation_figures/final_refresh")
    out_dir.mkdir(parents=True, exist_ok=True)

    sources = {
        "pit_any_h2": {
            "Batch No-Year": Path("data_lake/reports/ml_phase2b_dual_contract_2022_2025/oof")
            / f"{PROFILE_E0}__{TARGET_PIT_ANY_CLEAN}.csv",
            "Batch Percent": Path("data_lake/reports/ml_phase2b_dual_contract_2022_2025/oof")
            / f"{PROFILE_P1}__{TARGET_PIT_ANY_CLEAN}.csv",
            "MOA No-Year": Path("data_lake/reports/sml_phase2b_dual_contract_2022_2025/oof")
            / f"{PROFILE_E0}__{TARGET_PIT_ANY_CLEAN}.csv",
            "MOA Percent": Path("data_lake/reports/sml_phase2b_dual_contract_2022_2025/oof")
            / f"{PROFILE_P1}__{TARGET_PIT_ANY_CLEAN}.csv",
        },
        "pit_success_h2": {
            "Batch No-Year": Path("data_lake/reports/ml_phase2b_dual_contract_2022_2025/oof")
            / f"{PROFILE_E0}__{TARGET_PIT_SUCCESS_CLEAN}.csv",
            "Batch Percent": Path("data_lake/reports/ml_phase2b_dual_contract_2022_2025/oof")
            / f"{PROFILE_P1}__{TARGET_PIT_SUCCESS_CLEAN}.csv",
            "MOA No-Year": Path("data_lake/reports/sml_phase2b_dual_contract_2022_2025/oof")
            / f"{PROFILE_E0}__{TARGET_PIT_SUCCESS_CLEAN}.csv",
            "MOA Percent": Path("data_lake/reports/sml_phase2b_dual_contract_2022_2025/oof")
            / f"{PROFILE_P1}__{TARGET_PIT_SUCCESS_CLEAN}.csv",
        },
    }

    # Strict stale guard for corrected pit_success MOA OOF rows.
    for label in ("MOA No-Year", "MOA Percent"):
        p = sources["pit_success_h2"][label]
        if not p.exists():
            raise FileNotFoundError(f"missing required OOF: {p}")
        n = len(pd.read_csv(p, usecols=["target_y"]))
        if n != 91473:
            raise RuntimeError(f"stale pit_success MOA OOF detected: {p} rows={n}, expected 91473")

    curve_colors = {
        "Batch No-Year": "#4C78A8",
        "Batch Percent": "#F58518",
        "MOA No-Year": "#59A14F",
        "MOA Percent": "#E15759",
    }
    flink_points = _read_flink_pr_operating_points()
    ap_rows: list[dict[str, object]] = []
    outputs: list[Path] = []

    for outcome in ("pit_any_h2", "pit_success_h2"):
        fig, ax = plt.subplots(figsize=(10.4, 7.0))
        for label in ("Batch No-Year", "Batch Percent", "MOA No-Year", "MOA Percent"):
            path = sources[outcome][label]
            if not path.exists():
                raise FileNotFoundError(f"missing required OOF: {path}")
            df = _load_oof(path)
            y = df["target_y"].to_numpy()
            s = pd.to_numeric(df["calibrated_proba"], errors="coerce").fillna(0.0).to_numpy()
            if len(np.unique(y)) < 2:
                continue
            precision, recall, _ = precision_recall_curve(y, s)
            ap = float(average_precision_score(y, s))
            ax.plot(recall, precision, linewidth=2.0, color=curve_colors[label], label=f"{label} AP={ap:.3f}")
            ap_rows.append({"target": outcome, "system": label, "AP": ap})

        if outcome in flink_points:
            r, p = flink_points[outcome]
            ax.scatter([r], [p], marker="x", s=110, linewidths=2.4, color="black", label="Flink Strategy Engine point")

        ax.set_xlim(0.0, 1.0)
        ax.set_ylim(0.0, 1.0)
        ax.grid(alpha=0.25)
        ax.set_xlabel("Recall")
        ax.set_ylabel("Precision")
        ax.set_title(f"PR Curve — {outcome}", pad=10)
        ax.legend(loc="upper right", frameon=False, fontsize=10)
        _apply_panel_layout(fig, top=0.90, bottom=0.14, left=0.10, right=0.98, hspace=0.25, wspace=0.25)

        stem = f"pr_curve_{'pit_any' if outcome == 'pit_any_h2' else 'pit_success'}_final"
        png, pdf = _save_figure(fig, out_dir, stem)
        outputs.extend([png, pdf])

    ap_df = pd.DataFrame(ap_rows)
    ap_wide = (
        ap_df.pivot(index="target", columns="system", values="AP")
        .reset_index()
        .rename(
            columns={
                "Batch No-Year": "Batch No-Year AP",
                "Batch Percent": "Batch Percent AP",
                "MOA No-Year": "MOA No-Year AP",
                "MOA Percent": "MOA Percent AP",
            }
        )
    )
    ap_wide["interpretation"] = ap_wide.apply(
        lambda r: (
            f"{max(['Batch No-Year AP','Batch Percent AP','MOA No-Year AP','MOA Percent AP'], key=lambda c: _safe_float(r.get(c), -1.0)).replace(' AP','')} has highest AP."
        ),
        axis=1,
    )
    ap_wide = ap_wide[
        [
            "target",
            "Batch No-Year AP",
            "Batch Percent AP",
            "MOA No-Year AP",
            "MOA Percent AP",
            "interpretation",
        ]
    ]
    ap_csv = out_dir / "ap_summary_final.csv"
    ap_md = out_dir / "ap_summary_final.md"
    ap_wide.to_csv(ap_csv, index=False)
    lines = [
        "# AP Summary Final",
        "",
        "| target | Batch No-Year AP | Batch Percent AP | MOA No-Year AP | MOA Percent AP | interpretation |",
        "| --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for _, row in ap_wide.iterrows():
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row["target"]),
                    f"{_safe_float(row['Batch No-Year AP'], float('nan')):.6f}",
                    f"{_safe_float(row['Batch Percent AP'], float('nan')):.6f}",
                    f"{_safe_float(row['MOA No-Year AP'], float('nan')):.6f}",
                    f"{_safe_float(row['MOA Percent AP'], float('nan')):.6f}",
                    str(row["interpretation"]),
                ]
            )
            + " |"
        )
    lines.append("")
    ap_md.write_text("\n".join(lines), encoding="utf-8")
    outputs.extend([ap_csv, ap_md])
    return outputs, ap_wide


def _frontier_figure(
    *,
    method_name: str,
    frontier: pd.DataFrame,
    stem: str,
    context: dict[str, Any],
    is_sml: bool,
) -> FigureResult:
    if frontier.empty:
        return _manifest_fail(stem, f"{method_name} frontier", "frontier csv missing")
    req_cols = {"outcome_mode", "truth_lens", "selected_threshold", "precision", "recall", "f0_5", "profile"}
    if not req_cols.issubset(set(frontier.columns)):
        return _manifest_fail(stem, f"{method_name} frontier", "required columns missing")

    f = frontier[
        frontier["truth_lens"].astype(str).eq("clean_actionable")
        & frontier["outcome_mode"].astype(str).isin(["pit_any_h2", "pit_success_h2"])
    ].copy()
    if "truth_universe_mode" in f.columns:
        f = f[f["truth_universe_mode"].astype(str).eq("canonical_sde_truth")].copy()
    if f.empty:
        return _manifest_fail(stem, f"{method_name} frontier", "no canonical clean_actionable rows")

    title_suffix = ""
    if is_sml and "score_frontier_quality" in f.columns:
        qualities = set(f["score_frontier_quality"].astype(str).unique())
        if qualities == {"hard_decision_only"}:
            title_suffix = " (hard-decision diagnostic only)"
        elif "hard_decision_only" in qualities:
            title_suffix = " (mixed score quality)"
        else:
            title_suffix = " (continuous MOA vote-score frontier)"

    fig, axes = plt.subplots(2, 3, figsize=(15.6, 9.6), sharex=False, sharey=False)
    metrics = [("precision", "Precision"), ("recall", "Recall"), ("f0_5", "F0.5")]
    profiles = [(PROFILE_E0, "#4C78A8"), (PROFILE_P1, "#F58518")]
    reco = context["sml_reco"] if is_sml else context["batch_reco"]

    def _selected_threshold(outcome_mode: str, profile: str) -> float | None:
        if reco.empty:
            return None
        sub = reco[
            reco["truth_lens"].astype(str).eq("clean_actionable")
            & reco["outcome_mode"].astype(str).eq(outcome_mode)
            & reco["profile"].astype(str).eq(profile)
        ].copy()
        if "truth_universe_mode" in sub.columns:
            sub = sub[sub["truth_universe_mode"].astype(str).eq("canonical_sde_truth")]
        if sub.empty:
            return None
        return _safe_float(sub.iloc[0].get("selected_threshold"), float("nan"))

    note_text = "tail masked where scored count is below operating minimum"
    for row_i, outcome in enumerate(["pit_any_h2", "pit_success_h2"]):
        sub = f[f["outcome_mode"].astype(str).eq(outcome)].copy()
        if sub.empty:
            continue
        min_scored = 60 if outcome == "pit_any_h2" else 40
        for col_i, (metric_col, metric_label) in enumerate(metrics):
            ax = axes[row_i, col_i]
            for profile, color in profiles:
                grp = sub[sub["profile"].astype(str).eq(profile)].copy()
                if grp.empty:
                    continue
                grp = grp.sort_values("selected_threshold")
                x = pd.to_numeric(grp["selected_threshold"], errors="coerce")
                y = pd.to_numeric(grp[metric_col], errors="coerce")
                scored = pd.to_numeric(grp.get("scored", pd.Series(np.nan, index=grp.index)), errors="coerce")
                valid = (scored >= min_scored) & np.isfinite(x) & np.isfinite(y)
                if metric_col == "precision":
                    valid = valid & (scored > 0)
                y_masked = y.where(valid, np.nan)
                ax.plot(
                    x,
                    y_masked,
                    linewidth=1.6,
                    marker="o",
                    markersize=2.6,
                    label=_display_profile(profile, method_name),
                    color=color,
                )
                thr = _selected_threshold(outcome, profile)
                if thr is not None and np.isfinite(thr):
                    ax.axvline(float(thr), color=color, linestyle="--", linewidth=1.0, alpha=0.8)
                    idx = (x - float(thr)).abs().idxmin()
                    if idx in grp.index:
                        y_sel = y_masked.loc[idx]
                        if np.isfinite(y_sel):
                            ax.scatter([float(thr)], [float(y_sel)], color=color, edgecolor="black", s=30, zorder=5)
            ax.grid(alpha=0.25)
            ax.set_title(f"{outcome} - {metric_label}")
            ax.set_ylim(0.0, 1.0)
            ax.set_xlabel("Threshold")
            if is_sml:
                ax.set_xlim(0.05, 0.70)
            ax.text(0.01, 0.02, note_text, transform=ax.transAxes, fontsize=8, alpha=0.8)
    handles, labels = axes[0, 0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.93), ncol=2, frameon=False)
    if is_sml:
        fig.suptitle(
            f"{method_name} Threshold Frontier (clean_actionable){title_suffix}\ncontinuous MOA vote-score; uncalibrated score",
            y=0.99,
        )
    else:
        fig.suptitle(f"{method_name} Threshold Frontier (clean_actionable){title_suffix}", y=0.98)
    _apply_panel_layout(fig, top=0.82, bottom=0.10, left=0.06, right=0.99, hspace=0.42, wspace=0.25)

    png, pdf = _save_figure(fig, context["output_dir"], stem)
    return FigureResult(
        figure=stem,
        status="PASS",
        source_artifact=f"{method_name} frontier compact csv",
        note="Threshold sweep diagnostics with explicit score-quality caveat for MOA.",
        png_path=str(png),
        pdf_path=str(pdf),
    )


def _calibration_figure(
    *,
    method_name: str,
    oof_dir: Path,
    stem: str,
    context: dict[str, Any],
    is_sml: bool,
) -> FigureResult:
    targets = [
        (TARGET_PIT_ANY_CLEAN, "pit_any_h2"),
        (TARGET_PIT_SUCCESS_CLEAN, "pit_success_h2"),
    ]
    if not oof_dir.exists():
        return _manifest_fail(stem, str(oof_dir), "OOF directory missing")

    fig, axes = plt.subplots(1, 2, figsize=(14.2, 5.6), sharex=True, sharey=True)
    caveat = ""
    for ax, (target_col, outcome) in zip(axes, targets):
        e0, p1, err = _read_oof_pair(oof_dir, target_col)
        if err:
            return _manifest_fail(stem, str(oof_dir), err)
        assert e0 is not None and p1 is not None
        for profile, df, color in [
            (PROFILE_E0, e0, "#4C78A8"),
            (PROFILE_P1, p1, "#F58518"),
        ]:
            y = df["target_y"].to_numpy()
            s = pd.to_numeric(df["calibrated_proba"], errors="coerce").fillna(0.0).to_numpy()
            bins = _calibration_bins(y, s, n_bins=10)
            bins = bins[bins["count"] > 0]
            if bins.empty:
                continue
            ax.plot(
                bins["mean_pred"],
                bins["observed_rate"],
                marker="o",
                linewidth=1.4,
                label=_display_profile(profile, method_name),
                color=color,
            )
        ax.plot([0, 1], [0, 1], linestyle="--", color="#666666", linewidth=1.0)
        ax.set_title(outcome)
        ax.set_xlim(0.0, 1.0)
        ax.set_ylim(0.0, 1.0)
        ax.grid(alpha=0.25)
    axes[0].set_xlabel("Predicted score")
    axes[0].set_ylabel("Observed positive rate")
    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.92), ncol=2, frameon=False)

    if is_sml:
        caveat = " (reliability diagnostic, uncalibrated score)"
    fig.suptitle(f"{method_name} Calibration (clean_actionable){caveat}", y=0.98)
    fig.text(0.5, 0.03, "Below diagonal indicates overconfident scores.", ha="center", fontsize=9, alpha=0.85)
    _apply_panel_layout(fig, top=0.80, bottom=0.16)

    png, pdf = _save_figure(fig, context["output_dir"], stem)
    note = f"{method_name} reliability plot{caveat}."
    if method_name.lower().startswith("batch"):
        note = f"{note} {_batch_oof_source_note(context)}."
    return FigureResult(
        figure=stem,
        status="PASS",
        source_artifact=str(oof_dir),
        note=note,
        png_path=str(png),
        pdf_path=str(pdf),
    )


def _pr_gain_figure(
    *,
    method_name: str,
    oof_dir: Path,
    stem: str,
    context: dict[str, Any],
    is_sml: bool,
) -> FigureResult:
    targets = [
        (TARGET_PIT_ANY_CLEAN, "pit_any_h2"),
        (TARGET_PIT_SUCCESS_CLEAN, "pit_success_h2"),
    ]
    fig, axes = plt.subplots(1, 2, figsize=(14.2, 5.4), sharex=False, sharey=False)
    for ax, (target_col, outcome) in zip(axes, targets):
        e0, p1, err = _read_oof_pair(oof_dir, target_col)
        if err:
            return _manifest_fail(stem, str(oof_dir), err)
        assert e0 is not None and p1 is not None
        for profile, df, color in [
            (PROFILE_E0, e0, "#4C78A8"),
            (PROFILE_P1, p1, "#F58518"),
        ]:
            y = df["target_y"].to_numpy()
            s = pd.to_numeric(df["calibrated_proba"], errors="coerce").fillna(0.0).to_numpy()
            if len(np.unique(y)) < 2:
                continue
            precision, recall, _ = precision_recall_curve(y, s)
            rg, pg = _compute_prgain(precision, recall, float(y.mean()))
            if len(rg) == 0:
                continue
            ax.plot(rg, pg, linewidth=1.5, label=_display_profile(profile, method_name), color=color)
        ax.axhline(0.0, color="#666666", linewidth=0.8)
        ax.axvline(0.0, color="#666666", linewidth=0.8)
        ax.set_title(outcome)
        ax.grid(alpha=0.25)
        ax.set_xlabel("Recall gain")
        ax.set_ylabel("Precision gain")
    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.92), ncol=2, frameon=False)

    caveat = " (diagnostic)" if is_sml else ""
    fig.suptitle(f"{method_name} PR-Gain (clean_actionable){caveat}", y=0.98)
    _apply_panel_layout(fig, top=0.80, bottom=0.14)

    png, pdf = _save_figure(fig, context["output_dir"], stem)
    note = f"{method_name} PR-gain curves{caveat}."
    if method_name.lower().startswith("batch"):
        note = f"{note} {_batch_oof_source_note(context)}."
    return FigureResult(
        figure=stem,
        status="PASS",
        source_artifact=str(oof_dir),
        note=note,
        png_path=str(png),
        pdf_path=str(pdf),
    )


def _decision_curve_figure(
    *,
    method_name: str,
    oof_dir: Path,
    stem: str,
    context: dict[str, Any],
    is_sml: bool,
) -> FigureResult:
    thresholds = np.arange(0.01, 0.81, 0.02)
    targets = [
        (TARGET_PIT_ANY_CLEAN, "pit_any_h2"),
        (TARGET_PIT_SUCCESS_CLEAN, "pit_success_h2"),
    ]
    fig, axes = plt.subplots(1, 2, figsize=(14.2, 5.4), sharex=True, sharey=False)
    for ax, (target_col, outcome) in zip(axes, targets):
        e0, p1, err = _read_oof_pair(oof_dir, target_col)
        if err:
            return _manifest_fail(stem, str(oof_dir), err)
        assert e0 is not None and p1 is not None

        treat_all_drawn = False
        for profile, df, color in [
            (PROFILE_E0, e0, "#4C78A8"),
            (PROFILE_P1, p1, "#F58518"),
        ]:
            y = df["target_y"].to_numpy()
            s = pd.to_numeric(df["calibrated_proba"], errors="coerce").fillna(0.0).to_numpy()
            d = _decision_curve_rows(y, s, thresholds)
            if d.empty:
                continue
            ax.plot(
                d["threshold"],
                d["net_benefit_model"],
                linewidth=1.6,
                label=_display_profile(profile, method_name),
                color=color,
            )
            if not treat_all_drawn:
                ax.plot(d["threshold"], d["net_benefit_all"], linewidth=1.0, linestyle="--", color="#666666", label="treat_all")
                ax.plot(d["threshold"], d["net_benefit_none"], linewidth=1.0, linestyle=":", color="#999999", label="treat_none")
                treat_all_drawn = True
        ax.set_title(outcome)
        ax.set_xlabel("Threshold")
        ax.set_ylabel("Net benefit")
        ax.grid(alpha=0.25)
    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.92), ncol=4, frameon=False)

    caveat = " (diagnostic, uncalibrated score)" if is_sml else ""
    fig.suptitle(f"{method_name} Decision Curves (clean_actionable){caveat}", y=0.98)
    _apply_panel_layout(fig, top=0.80, bottom=0.14)

    png, pdf = _save_figure(fig, context["output_dir"], stem)
    note = f"{method_name} decision-curve diagnostic{caveat}."
    if method_name.lower().startswith("batch"):
        note = f"{note} {_batch_oof_source_note(context)}."
    return FigureResult(
        figure=stem,
        status="PASS",
        source_artifact=str(oof_dir),
        note=note,
        png_path=str(png),
        pdf_path=str(pdf),
    )


def _pick_by_year_at_recommended(by_year: pd.DataFrame, reco: pd.DataFrame) -> pd.DataFrame:
    if by_year.empty or reco.empty:
        return pd.DataFrame()
    req = {"run_id", "selected_threshold", "year", "precision", "recall", "f0_5", "outcome_mode", "profile", "truth_lens"}
    if not req.issubset(set(by_year.columns)) or not {"run_id", "selected_threshold"}.issubset(set(reco.columns)):
        return pd.DataFrame()

    rows: list[pd.Series] = []
    for _, r in reco.iterrows():
        run_id = str(r.get("run_id", ""))
        thr = _safe_float(r.get("selected_threshold"), float("nan"))
        sub = by_year[by_year["run_id"].astype(str).eq(run_id)].copy()
        if sub.empty:
            continue
        sub["thr_dist"] = (pd.to_numeric(sub["selected_threshold"], errors="coerce") - thr).abs()
        for year, g in sub.groupby("year", sort=True):
            g = g.sort_values("thr_dist", kind="mergesort")
            rows.append(g.iloc[0])
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows).drop(columns=["thr_dist"], errors="ignore")
    return out


def _figure_per_year_metrics(context: dict[str, Any]) -> FigureResult:
    stem = "phase2b_per_year_precision_recall_f05_clean_actionable"
    sde_year = context["sde_per_year"]
    b_year = context["batch_by_year"]
    b_reco = context["batch_reco"]
    s_year = context["sml_by_year"]
    s_reco = context["sml_reco"]
    if sde_year.empty or b_year.empty or b_reco.empty or s_year.empty or s_reco.empty:
        return _manifest_fail(stem, "per-year inputs", "missing SDE/Batch/SML per-year or recommended inputs")

    b_pick = _pick_by_year_at_recommended(b_year, b_reco)
    s_pick = _pick_by_year_at_recommended(s_year, s_reco)
    if b_pick.empty or s_pick.empty:
        return _manifest_fail(stem, "by-year frontier", "unable to select by-year rows at recommended thresholds")

    sde_ca = sde_year[sde_year["truth_lens"].astype(str).eq("clean_actionable")].copy()
    sde_ca = sde_ca[sde_ca["year"].astype(str).str.fullmatch(r"\d{4}")].copy()
    if sde_ca.empty:
        return _manifest_fail(stem, context["paths"]["sde_per_year_csv"], "missing SDE clean_actionable per-year rows")
    sde_ca["year"] = sde_ca["year"].astype(int)

    fig, axes = plt.subplots(1, 3, figsize=(16.8, 5.6), sharex=True)
    metrics = [("precision", "Precision"), ("recall", "Recall"), ("f0_5", "F0.5")]
    years = sorted(set(sde_ca["year"].tolist()))
    x = np.array(years, dtype=int)

    # SDE pit_any only for clean_actionable yearly; use direct metric columns.
    sde_p = [
        _safe_float(
            sde_ca[sde_ca["year"] == y]["pit_any_precision"].iloc[0] if not sde_ca[sde_ca["year"] == y].empty else np.nan
        )
        for y in years
    ]
    sde_r = [
        _safe_float(
            sde_ca[sde_ca["year"] == y]["pit_any_recall"].iloc[0] if not sde_ca[sde_ca["year"] == y].empty else np.nan
        )
        for y in years
    ]
    sde_f = [
        _safe_float(
            sde_ca[sde_ca["year"] == y]["pit_any_f0_5"].iloc[0] if not sde_ca[sde_ca["year"] == y].empty else np.nan
        )
        for y in years
    ]

    def _series(df: pd.DataFrame, profile: str, metric: str) -> list[float]:
        out = []
        for y in years:
            sub = df[
                df["truth_lens"].astype(str).eq("clean_actionable")
                & df["outcome_mode"].astype(str).eq("pit_any_h2")
                & df["profile"].astype(str).eq(profile)
                & (pd.to_numeric(df["year"], errors="coerce") == int(y))
            ]
            out.append(_safe_float(sub[metric].iloc[0] if not sub.empty else np.nan))
        return out

    b_e0_p = _series(b_pick, PROFILE_E0, "precision")
    b_p1_p = _series(b_pick, PROFILE_P1, "precision")
    s_e0_p = _series(s_pick, PROFILE_E0, "precision")
    s_p1_p = _series(s_pick, PROFILE_P1, "precision")

    b_e0_r = _series(b_pick, PROFILE_E0, "recall")
    b_p1_r = _series(b_pick, PROFILE_P1, "recall")
    s_e0_r = _series(s_pick, PROFILE_E0, "recall")
    s_p1_r = _series(s_pick, PROFILE_P1, "recall")

    b_e0_f = _series(b_pick, PROFILE_E0, "f0_5")
    b_p1_f = _series(b_pick, PROFILE_P1, "f0_5")
    s_e0_f = _series(s_pick, PROFILE_E0, "f0_5")
    s_p1_f = _series(s_pick, PROFILE_P1, "f0_5")

    all_series = [
        (sde_p, sde_r, sde_f, "Final SDE", "#2F4B7C"),
        (b_e0_p, b_e0_r, b_e0_f, _display_profile(PROFILE_E0, "batch"), "#4C78A8"),
        (b_p1_p, b_p1_r, b_p1_f, _display_profile(PROFILE_P1, "batch"), "#F58518"),
        (s_e0_p, s_e0_r, s_e0_f, _display_profile(PROFILE_E0, "moa"), "#59A14F"),
        (s_p1_p, s_p1_r, s_p1_f, _display_profile(PROFILE_P1, "moa"), "#E15759"),
    ]

    for ax, metric_idx, title in [(axes[0], 0, "Precision"), (axes[1], 1, "Recall"), (axes[2], 2, "F0.5")]:
        for p, r, f, label, color in all_series:
            seq = [p, r, f][metric_idx]
            ax.plot(x, seq, marker="o", linewidth=1.7, label=label, color=color)
        ax.set_title(title)
        ax.set_xlabel("Year")
        ax.set_ylim(0.0, 1.0)
        ax.grid(alpha=0.25)
    axes[0].set_ylabel("Metric")
    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.92), ncol=5, frameon=False)
    fig.suptitle("Per-Year Metrics (clean_actionable, pit_any_h2)", y=0.98)
    _apply_panel_layout(fig, top=0.80, bottom=0.16, wspace=0.22)

    png, pdf = _save_figure(fig, context["output_dir"], stem)
    return FigureResult(
        figure=stem,
        status="PASS",
        source_artifact="SDE per-year + Batch/SML by-year at recommended threshold",
        note="Temporal consistency on pit_any_h2 clean_actionable headline lens.",
        png_path=str(png),
        pdf_path=str(pdf),
    )


def _figure_temporal_drift(context: dict[str, Any], *, slide: bool = False) -> FigureResult:
    stem = "phase2b_temporal_drift_by_race_clean_actionable_slide" if slide else "phase2b_temporal_drift_by_race_clean_actionable"
    batch_reco = context["batch_reco"]
    sml_reco = context["sml_reco"]
    batch_oof_dir = Path(context["paths"]["batch_oof_dir"])
    sml_oof_dir = Path(context["paths"]["sml_oof_dir"])
    if batch_reco.empty or sml_reco.empty:
        return _manifest_fail(stem, "recommended csv", "missing recommended operating points")

    def _load_run(df_reco: pd.DataFrame, oof_dir: Path, profile: str) -> pd.DataFrame | None:
        sub = df_reco[
            df_reco["profile"].astype(str).eq(profile)
            & df_reco["target_column"].astype(str).eq(TARGET_PIT_ANY_CLEAN)
            & df_reco["truth_lens"].astype(str).eq("clean_actionable")
        ]
        if sub.empty:
            return None
        thr = _safe_float(sub.iloc[0].get("selected_threshold"), 0.5)
        path = oof_dir / f"{profile}__{TARGET_PIT_ANY_CLEAN}.csv"
        if not path.exists():
            return None
        oof = _load_oof(path)
        score = pd.to_numeric(oof["calibrated_proba"], errors="coerce").fillna(0.0)
        oof["decision"] = (score >= thr).astype(int)
        return oof

    be0 = _load_run(batch_reco, batch_oof_dir, PROFILE_E0)
    bp1 = _load_run(batch_reco, batch_oof_dir, PROFILE_P1)
    se0 = _load_run(sml_reco, sml_oof_dir, PROFILE_E0)
    sp1 = _load_run(sml_reco, sml_oof_dir, PROFILE_P1)
    if any(v is None for v in [be0, bp1, se0, sp1]):
        return _manifest_fail(stem, "OOF+recommended", "missing OOF/recommended for one or more runs")

    def _series(frame: pd.DataFrame, col: str) -> pd.DataFrame:
        g = frame.groupby("race", sort=False)[col].mean().reset_index(name=col)
        g["year"] = g["race"].map(_parse_year)
        g = g.sort_values(["year", "race"], kind="mergesort").reset_index(drop=True)
        g["race_idx"] = np.arange(len(g))
        return g

    # use decision rate and target prevalence for drift context
    batch_e0_label = f"{_display_profile(PROFILE_E0, 'batch')} decision_rate"
    batch_p1_label = f"{_display_profile(PROFILE_P1, 'batch')} decision_rate"
    moa_e0_label = f"{_display_profile(PROFILE_E0, 'moa')} decision_rate"
    moa_p1_label = f"{_display_profile(PROFILE_P1, 'moa')} decision_rate"
    series = {
        batch_e0_label: _series(be0, "decision"),
        batch_p1_label: _series(bp1, "decision"),
        moa_e0_label: _series(se0, "decision"),
        moa_p1_label: _series(sp1, "decision"),
    }
    target_series = _series(be0, "target_y")

    if slide:
        fig, axes = plt.subplots(2, 1, figsize=(14.0, 7.6), sharex=True)
        panel_specs = [
            ("Batch", [(batch_e0_label, "#4C78A8"), (batch_p1_label, "#F58518")]),
            ("MOA", [(moa_e0_label, "#59A14F"), (moa_p1_label, "#E15759")]),
        ]
        y_max = 0.0
        for ax, (title, lines) in zip(axes, panel_specs):
            for label, color in lines:
                g = series[label].copy()
                g["smooth"] = g["decision"].rolling(window=5, min_periods=1).mean()
                y_max = max(y_max, float(g["smooth"].max()))
                ax.plot(g["race_idx"], g["smooth"], linewidth=1.9, label=label.replace(" decision_rate", ""), color=color)
            t = target_series.copy()
            t["smooth"] = t["target_y"].rolling(window=5, min_periods=1).mean()
            y_max = max(y_max, float(t["smooth"].max()))
            ax.plot(t["race_idx"], t["smooth"], linewidth=1.2, linestyle="--", color="#222222", label="Target prevalence (5-race MA)")
            ax.set_title(f"{title} decision-rate drift (5-race rolling mean)")
            ax.set_ylabel("Mean rate")
            ax.grid(alpha=0.25)
            ax.legend(loc="upper right", frameon=False)
        y_top = min(1.0, max(0.08, y_max * 1.18))
        for ax in axes:
            ax.set_ylim(0.0, y_top)
        axes[-1].set_xlabel("Race sequence index")
        fig.suptitle("Temporal Drift by Race (clean_actionable pit_any_h2) - slide view", y=0.98)
        _apply_panel_layout(fig, top=0.90, bottom=0.10, hspace=0.30)
    else:
        fig, ax = plt.subplots(figsize=(14.0, 5.8))
        colors = {
            batch_e0_label: "#4C78A8",
            batch_p1_label: "#F58518",
            moa_e0_label: "#59A14F",
            moa_p1_label: "#E15759",
        }
        y_max = 0.0
        for label, g in series.items():
            h = g.copy()
            h["smooth"] = h["decision"].rolling(window=3, min_periods=1).mean()
            y_max = max(y_max, float(h["smooth"].max()))
            ax.plot(h["race_idx"], h["smooth"], linewidth=1.5, label=label.replace(" decision_rate", ""), color=colors[label])
        t = target_series.copy()
        t["smooth"] = t["target_y"].rolling(window=3, min_periods=1).mean()
        y_max = max(y_max, float(t["smooth"].max()))
        ax.plot(t["race_idx"], t["smooth"], linewidth=1.2, linestyle="--", color="#222222", label="Target prevalence")
        ax.set_title("Temporal Drift by Race (clean_actionable pit_any_h2)")
        ax.set_xlabel("Race sequence index")
        ax.set_ylabel("Mean rate")
        ax.set_ylim(0.0, min(1.0, max(0.08, y_max * 1.22)))
        ax.grid(alpha=0.25)
        ax.legend(loc="upper right", ncol=2, frameon=False)
        _apply_panel_layout(fig, top=0.90, bottom=0.14)

    png, pdf = _save_figure(fig, context["output_dir"], stem)
    return FigureResult(
        figure=stem,
        status="PASS",
        source_artifact="Batch/SML OOF + recommended thresholds",
        note="Decision-rate drift by race under fixed recommended thresholds (smoothed view).",
        png_path=str(png),
        pdf_path=str(pdf),
    )


def _figure_sml_hard_decision_summary(context: dict[str, Any]) -> FigureResult:
    stem = "phase2b_sml_hard_decision_summary"
    oof_dir = Path(context["paths"]["sml_oof_dir"])
    if not oof_dir.exists():
        return _manifest_fail(stem, str(oof_dir), "SML OOF directory missing")

    rows = []
    for path in sorted(oof_dir.glob("*.csv")):
        try:
            score = pd.read_csv(path, usecols=["raw_proba"])["raw_proba"]
            unique = int(pd.to_numeric(score, errors="coerce").dropna().nunique())
            profile = path.stem.split("__", 1)[0]
            rows.append(
                {
                    "run_id": path.stem,
                    "profile": profile,
                    "score_unique_count": unique,
                    "hard": bool(unique <= 2),
                }
            )
        except Exception:
            continue
    if not rows:
        return _manifest_fail(stem, str(oof_dir), "no readable SML OOF raw_proba columns")

    df = pd.DataFrame(rows)
    grp = df.groupby("profile", as_index=False).agg(
        hard_decision_rate=("hard", "mean"),
        median_unique_scores=("score_unique_count", "median"),
    )
    grp["hard_decision_rate"] = grp["hard_decision_rate"] * 100.0

    fig, ax1 = plt.subplots(figsize=(8.5, 4.8))
    ax2 = ax1.twinx()
    x = np.arange(len(grp))
    labels = grp["profile"].astype(str).map(lambda p: _display_profile(str(p), "moa"))
    ax1.bar(x - 0.18, grp["hard_decision_rate"], width=0.36, color="#B279A2", label="hard-decision %")
    ax2.bar(x + 0.18, grp["median_unique_scores"], width=0.36, color="#4C78A8", alpha=0.7, label="median unique scores")
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, rotation=15, ha="right")
    ax1.set_ylabel("Hard-decision runs (%)")
    ax2.set_ylabel("Median unique score count")
    ax1.set_ylim(0, 100)
    ax1.set_title("MOA Score Quality Diagnostic")
    ax1.grid(axis="y", alpha=0.25)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right", frameon=False)
    _apply_panel_layout(fig, top=0.90, bottom=0.18)

    png, pdf = _save_figure(fig, context["output_dir"], stem)
    mode = "hard_decision_only" if bool(df["hard"].all()) else "continuous_or_mixed"
    return FigureResult(
        figure=stem,
        status="PASS",
        source_artifact=str(oof_dir),
        note=f"MOA score quality = {mode}; frontier interpretation should follow this flag.",
        png_path=str(png),
        pdf_path=str(pdf),
    )


def _figure_preq_accuracy_kappa(context: dict[str, Any]) -> FigureResult:
    stem = "phase2b_preq_accuracy_kappa"
    preq = context["sml_preq"]
    if preq.empty:
        return _manifest_fail(stem, context["paths"]["sml_prequential_csv"], "missing SML prequential summary")
    need = {"profile", "target_column", "accuracy_pct", "kappa_pct"}
    if not need.issubset(set(preq.columns)):
        return _manifest_fail(stem, context["paths"]["sml_prequential_csv"], "prequential columns missing")

    f = preq.copy()
    f["target_short"] = (
        f["target_column"].astype(str)
        .str.replace("target_", "", regex=False)
        .str.replace("_h2_", "_", regex=False)
    )
    acc = f.pivot_table(index="target_short", columns="profile", values="accuracy_pct", aggfunc="first")
    kap = f.pivot_table(index="target_short", columns="profile", values="kappa_pct", aggfunc="first")
    if acc.empty or kap.empty:
        return _manifest_fail(stem, context["paths"]["sml_prequential_csv"], "empty prequential pivots")

    labels = acc.index.tolist()
    x = np.arange(len(labels))
    w = 0.35
    fig, axes = plt.subplots(2, 1, figsize=(12.2, 7.8), sharex=True)
    for ax, pivot, ylabel, title in [
        (axes[0], acc, "Accuracy (%)", "Prequential Accuracy"),
        (axes[1], kap, "Kappa (%)", "Prequential Kappa"),
    ]:
        e0 = pd.to_numeric(pivot.get(PROFILE_E0, pd.Series(index=labels)), errors="coerce").reindex(labels).fillna(0.0)
        p1 = pd.to_numeric(pivot.get(PROFILE_P1, pd.Series(index=labels)), errors="coerce").reindex(labels).fillna(0.0)
        ax.bar(
            x - w / 2,
            e0.values,
            width=w,
            color="#4C78A8",
            label=_display_profile(PROFILE_E0, "moa"),
        )
        ax.bar(
            x + w / 2,
            p1.values,
            width=w,
            color="#F58518",
            label=_display_profile(PROFILE_P1, "moa"),
        )
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.grid(axis="y", alpha=0.25)
        ax.legend()

    axes[1].set_xticks(x)
    axes[1].set_xticklabels(labels, rotation=20, ha="right")
    fig.suptitle("MOA Prequential Summary by Target", y=0.98)
    _apply_panel_layout(fig, top=0.90, bottom=0.18, hspace=0.36)

    png, pdf = _save_figure(fig, context["output_dir"], stem)
    return FigureResult(
        figure=stem,
        status="PASS",
        source_artifact=context["paths"]["sml_prequential_csv"],
        note="Stream learning stability snapshot for MOA No-Year vs MOA Percent.",
        png_path=str(png),
        pdf_path=str(pdf),
    )


def _figure_sensitivity_metrics(context: dict[str, Any], lens: str) -> FigureResult:
    stem = f"phase2b_{lens}_precision_recall_f05"
    sde = context["sde_agg"]
    batch = context["batch_compact"]
    sml = context["sml_compact"]
    if sde.empty or batch.empty or sml.empty:
        return _manifest_fail(stem, "multiple", "missing SDE/Batch/SML compact inputs")
    sde_row = sde[sde["truth_lens"].astype(str).eq(lens)]
    if sde_row.empty:
        return _manifest_fail(stem, context["paths"]["sde_aggregate_csv"], f"missing SDE {lens} row")
    sde_row = sde_row.iloc[0]

    b_any = _pick_row(batch, "pit_any_h2", lens)
    s_any = _pick_row(sml, "pit_any_h2", lens)
    if b_any is None or s_any is None:
        return _manifest_fail(stem, "compact CSVs", f"missing pit_any {lens} rows")

    labels = ["Final SDE", "Batch No-Year", "Batch Percent", "MOA No-Year", "MOA Percent"]
    sde_m = _sde_metrics_for_outcome(sde_row, "pit_any_h2")
    precision = [
        sde_m["precision"],
        _safe_float(b_any.get("e0_precision"), 0.0),
        _safe_float(b_any.get("p1_precision"), 0.0),
        _safe_float(s_any.get("e0_precision"), 0.0),
        _safe_float(s_any.get("p1_precision"), 0.0),
    ]
    recall = [
        sde_m["recall"],
        _safe_float(b_any.get("e0_recall"), 0.0),
        _safe_float(b_any.get("p1_recall"), 0.0),
        _safe_float(s_any.get("e0_recall"), 0.0),
        _safe_float(s_any.get("p1_recall"), 0.0),
    ]
    f05 = [
        sde_m["f0_5"],
        _safe_float(b_any.get("e0_f0_5"), 0.0),
        _safe_float(b_any.get("p1_f0_5"), 0.0),
        _safe_float(s_any.get("e0_f0_5"), 0.0),
        _safe_float(s_any.get("p1_f0_5"), 0.0),
    ]

    x = np.arange(len(labels))
    w = 0.24
    fig, ax = plt.subplots(figsize=(11.2, 5.1))
    ax.bar(x - w, precision, width=w, color="#4C78A8", label="Precision")
    ax.bar(x, recall, width=w, color="#F58518", label="Recall")
    ax.bar(x + w, f05, width=w, color="#54A24B", label="F0.5")
    ax.set_title(f"Sensitivity Lens: {lens} (pit_any_h2)")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=18, ha="right")
    ax.set_ylim(0.0, 1.0)
    ax.grid(axis="y", alpha=0.25)
    ax.legend(loc="upper right", frameon=False)
    _apply_panel_layout(fig, top=0.88, bottom=0.20)

    png, pdf = _save_figure(fig, context["output_dir"], stem)
    return FigureResult(
        figure=stem,
        status="PASS",
        source_artifact="SDE aggregate + Batch/SML canonical compact",
        note=f"Sensitivity comparison under {lens} truth lens.",
        png_path=str(png),
        pdf_path=str(pdf),
    )


def _figure_sensitivity_delta(context: dict[str, Any], lens: str) -> FigureResult:
    stem = f"phase2b_{lens}_e0_vs_p1_delta"
    batch = context["batch_compact"]
    sml = context["sml_compact"]
    if batch.empty or sml.empty:
        return _manifest_fail(stem, "compact CSVs", "missing Batch/SML compact")

    b = _pick_row(batch, "pit_any_h2", lens)
    s = _pick_row(sml, "pit_any_h2", lens)
    if b is None or s is None:
        return _manifest_fail(stem, "compact CSVs", f"missing pit_any {lens} rows")

    metrics = [
        ("delta_AP_p1_minus_e0", "AP"),
        ("delta_precision_p1_minus_e0", "precision"),
        ("delta_recall_p1_minus_e0", "recall"),
        ("delta_f0_5_p1_minus_e0", "f0.5"),
        ("delta_scored_p1_minus_e0", "scored"),
    ]
    batch_vals = [_safe_float(b.get(m[0]), 0.0) for m in metrics]
    sml_vals = [_safe_float(s.get(m[0]), 0.0) for m in metrics]

    x = np.arange(len(metrics))
    w = 0.35
    fig, ax = plt.subplots(figsize=(10.8, 5.1))
    ax.bar(x - w / 2, batch_vals, width=w, color="#4C78A8", label="Batch Percent - Batch No-Year")
    ax.bar(x + w / 2, sml_vals, width=w, color="#F58518", label="MOA Percent - MOA No-Year")
    ax.axhline(0.0, color="black", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels([m[1] for m in metrics], rotation=24, ha="right")
    ax.set_title(f"Sensitivity Delta (pit_any_h2, {lens})")
    ax.grid(axis="y", alpha=0.25)
    ax.legend(loc="upper right", frameon=False)
    _apply_panel_layout(fig, top=0.88, bottom=0.22)

    png, pdf = _save_figure(fig, context["output_dir"], stem)
    return FigureResult(
        figure=stem,
        status="PASS",
        source_artifact="phase2b_e0_vs_p1_canonical_compact.csv",
        note=f"No-Year vs Percent deltas in sensitivity lens {lens}.",
        png_path=str(png),
        pdf_path=str(pdf),
    )


def _figure_pit_success_policy_sensitivity(context: dict[str, Any]) -> FigureResult:
    stem = "phase2b_pit_success_operating_policy_sensitivity_clean_actionable"
    diag = context.get("pit_success_policy_diag", pd.DataFrame())
    if diag.empty:
        return _manifest_fail(stem, context["paths"]["pit_success_policy_diagnostic_csv"], "missing pit_success policy diagnostic csv")

    need = {"row_type", "model_label", "criterion", "precision", "row_tp", "tp_for_recall", "f0_5", "selected_threshold"}
    if not need.issubset(set(diag.columns)):
        return _manifest_fail(stem, context["paths"]["pit_success_policy_diagnostic_csv"], f"diagnostic csv missing columns {sorted(need - set(diag.columns))}")

    sde = diag[
        (diag["row_type"].astype(str) == "selected_operating_point")
        & (diag["model_label"].astype(str) == "Final SDE")
    ]
    if sde.empty:
        return _manifest_fail(stem, context["paths"]["pit_success_policy_diagnostic_csv"], "missing Final SDE row in diagnostic csv")
    sde = sde.iloc[0]

    selected = diag[diag["row_type"].astype(str).eq("selected_operating_point")].copy()
    alt = diag[diag["row_type"].astype(str).eq("alternative_threshold")].copy()
    alt = alt[alt["criterion"].astype(str).eq("precision_closest_to_sde")].copy()

    profiles = [("MOA No-Year", "#59A14F"), ("MOA Percent", "#F58518")]
    metrics = [("precision", "Precision"), ("row_tp", "Row TP"), ("tp_for_recall", "Truth Events Covered"), ("f0_5", "F0.5")]

    fig, axes = plt.subplots(1, 2, figsize=(15.4, 6.2), sharey=False)
    for ax, (model_label, color) in zip(axes, profiles):
        sel = selected[selected["model_label"].astype(str).eq(model_label)]
        eq = alt[alt["model_label"].astype(str).eq(model_label)]
        if sel.empty or eq.empty:
            return _manifest_fail(stem, context["paths"]["pit_success_policy_diagnostic_csv"], f"missing selected/equal-precision rows for {model_label}")
        sel = sel.iloc[0]
        eq = eq.iloc[0]

        x = np.arange(len(metrics))
        w = 0.26
        sde_vals = [_safe_float(sde.get(m[0]), 0.0) for m in metrics]
        sel_vals = [_safe_float(sel.get(m[0]), 0.0) for m in metrics]
        eq_vals = [_safe_float(eq.get(m[0]), 0.0) for m in metrics]

        ax.bar(x - w, sde_vals, width=w, label="Final SDE", color="#4C78A8")
        ax.bar(x, sel_vals, width=w, label=f"{model_label} selected", color=color, alpha=0.75)
        ax.bar(x + w, eq_vals, width=w, label=f"{model_label} @ SDE-equivalent precision", color=color)
        ax.set_xticks(x)
        ax.set_xticklabels([m[1] for m in metrics], rotation=18, ha="right")
        ax.set_title(
            f"{model_label}: selected vs SDE-equivalent precision\n"
            f"selected thr={_safe_float(sel.get('selected_threshold'), float('nan')):.2f}, "
            f"eq thr={_safe_float(eq.get('selected_threshold'), float('nan')):.2f}"
        )
        ax.grid(axis="y", alpha=0.25)

    fig.legend(loc="upper center", bbox_to_anchor=(0.5, 0.97), ncol=3, frameon=False)
    fig.suptitle("Phase 2B pit_success_h2 Operating-Policy Sensitivity (clean_actionable, canonical_sde_truth)", y=0.995)
    fig.text(
        0.02,
        0.01,
        "Shows how conservative selected thresholds suppress MOA reach versus SDE-equivalent precision points.",
        fontsize=9,
    )
    _apply_panel_layout(fig, top=0.80, bottom=0.21)

    png, pdf = _save_figure(fig, context["output_dir"], stem)
    return FigureResult(
        figure=stem,
        status="PASS",
        source_artifact=context["paths"]["pit_success_policy_diagnostic_csv"],
        note="Compares Final SDE, selected MOA points, and SDE-equivalent precision MOA points for pit_success_h2.",
        png_path=str(png),
        pdf_path=str(pdf),
    )


def _figure_pit_success_policy_sensitivity_batch_moa(context: dict[str, Any]) -> FigureResult:
    stem = "phase2b_pit_success_operating_policy_sensitivity_batch_moa_clean_actionable"
    diag = context.get("pit_success_policy_diag", pd.DataFrame())
    if diag.empty:
        return _manifest_fail(stem, context["paths"]["pit_success_policy_diagnostic_csv"], "missing pit_success policy diagnostic csv")

    need = {"row_type", "model_label", "criterion", "precision", "row_tp", "tp_for_recall", "f0_5", "selected_threshold"}
    if not need.issubset(set(diag.columns)):
        return _manifest_fail(stem, context["paths"]["pit_success_policy_diagnostic_csv"], f"diagnostic csv missing columns {sorted(need - set(diag.columns))}")

    models = ["Batch No-Year", "Batch Percent", "MOA No-Year", "MOA Percent"]
    selected = diag[diag["row_type"].astype(str).eq("selected_operating_point")].copy()
    alt = diag[diag["row_type"].astype(str).eq("alternative_threshold")].copy()
    if selected.empty or alt.empty:
        return _manifest_fail(stem, context["paths"]["pit_success_policy_diagnostic_csv"], "missing selected/alternative rows")
    sde = selected[selected["model_label"].astype(str).eq("Final SDE")].copy()
    if sde.empty:
        return _manifest_fail(stem, context["paths"]["pit_success_policy_diagnostic_csv"], "missing Final SDE selected row in diagnostic csv")
    sde = sde.iloc[0]

    criteria = [
        ("selected_phase2b", "Selected precision-first", "#4C78A8"),
        ("precision_closest_to_sde", "Precision ≈ Final SDE", "#59A14F"),
        ("max_f0_5", "Max F0.5", "#F28E2B"),
    ]
    metrics = [
        ("precision", "Precision"),
        ("row_tp", "Row TP"),
        ("tp_for_recall", "Truth Events Covered"),
        ("f0_5", "F0.5"),
    ]

    fig, axes = plt.subplots(2, 2, figsize=(15.8, 9.4), sharex=False, sharey=False)
    x = np.arange(len(models))
    w = 0.24
    flat_axes = axes.flatten()
    for ax, (metric_col, metric_title) in zip(flat_axes, metrics):
        for j, (criterion, label, color) in enumerate(criteria):
            vals: list[float] = []
            for model in models:
                if criterion == "selected_phase2b":
                    row = selected[selected["model_label"].astype(str).eq(model)]
                else:
                    row = alt[
                        alt["model_label"].astype(str).eq(model)
                        & alt["criterion"].astype(str).eq(criterion)
                    ]
                if row.empty:
                    vals.append(float("nan"))
                else:
                    vals.append(_safe_float(row.iloc[0].get(metric_col), float("nan")))
            ax.bar(x + (j - 1) * w, vals, width=w, color=color, alpha=0.92)

        ax.set_xticks(x)
        ax.set_xticklabels(models, rotation=18, ha="right")
        ax.set_title(metric_title)
        ax.grid(axis="y", alpha=0.25)
        if metric_col in {"precision", "f0_5"}:
            ax.set_ylim(0.0, 1.0)

        # Final SDE dotted reference line for direct policy-context comparison.
        sde_val = _safe_float(sde.get(metric_col), float("nan"))
        if np.isfinite(sde_val):
            ax.axhline(sde_val, linestyle="--", linewidth=1.2, color="#6B6B6B", alpha=0.9)
            ymin, ymax = ax.get_ylim()
            span = max(ymax - ymin, 1e-9)
            y_text = min(ymax - 0.02 * span, sde_val + 0.03 * span)
            ax.text(
                0.02,
                y_text,
                f"Final SDE {metric_title}",
                transform=ax.get_yaxis_transform(),
                fontsize=9,
                color="#4F4F4F",
                va="bottom",
            )

    # De-duplicated legend with exactly three policy labels.
    from matplotlib.patches import Patch

    legend_handles = [
        Patch(facecolor=criteria[0][2], label=criteria[0][1]),
        Patch(facecolor=criteria[1][2], label=criteria[1][1]),
        Patch(facecolor=criteria[2][2], label=criteria[2][1]),
    ]
    fig.legend(legend_handles, [h.get_label() for h in legend_handles], loc="upper center", bbox_to_anchor=(0.5, 0.93), ncol=3, frameon=False)
    fig.suptitle("pit_success_h2 operating-policy sensitivity", y=0.985)
    fig.text(
        0.02,
        0.01,
        "MOA sensitivity points reveal latent coverage; Batch shift is smaller and more signal-limited.",
        fontsize=9,
    )
    _apply_panel_layout(fig, top=0.83, bottom=0.17)

    png, pdf = _save_figure(fig, context["output_dir"], stem)
    return FigureResult(
        figure=stem,
        status="PASS",
        source_artifact=context["paths"]["pit_success_policy_diagnostic_csv"],
        note="Compares selected precision-first points vs SDE-equivalent/max-F0.5 sensitivity for Batch and MOA.",
        png_path=str(png),
        pdf_path=str(pdf),
    )


def _write_operating_policy_audit_md(context: dict[str, Any], output_dir: Path) -> Path:
    out = output_dir / "phase2b_operating_policy_audit.md"
    diag = context.get("pit_success_policy_diag", pd.DataFrame())
    lines = [
        "# Phase 2B Operating-Policy Audit",
        "",
        "Scope: `pit_success_h2`, `clean_actionable`, `canonical_sde_truth`.",
        "",
        "## Selection Logic (Code Truth)",
        "",
        "- `pit_any_h2`: selection uses max `f0.5` under minimum scored constraint.",
        "- `pit_success_h2`: selection uses max `precision` under minimum scored constraint.",
        "- This is encoded in `ml_pipeline/lib/phase2b_threshold_frontier.py` via:",
        "  - `selection_rule = pit_any_max_f0_5_scored>=...`",
        "  - `selection_rule = pit_success_max_precision_scored>=...`",
        "",
        "Conclusion: MOA `pit_success` selected thresholds near `0.55` are policy-consistent with a precision-first guardrail. "
        "They are not a sort/selection bug if precision-first was intended.",
        "",
    ]
    if not diag.empty:
        sel = diag[diag["row_type"].astype(str).eq("selected_operating_point")].copy()
        alt = diag[
            (diag["row_type"].astype(str).eq("alternative_threshold"))
            & (diag["criterion"].astype(str).eq("precision_closest_to_sde"))
        ].copy()
        lines += [
            "## Evidence From Diagnostic",
            "",
            "- At selected MOA thresholds (`~0.55`), precision is high but reach is conservative.",
            "- At SDE-equivalent precision thresholds (`~0.24-0.29`), MOA row/event coverage is much larger.",
            "",
            "This indicates a **policy trade-off** (guardrail precision vs reach), not an absence of MOA signal.",
            "",
        ]
        if not sel.empty:
            lines.append("Selected rows considered: " + ", ".join(sorted(sel["model_label"].astype(str).unique().tolist())))
        if not alt.empty:
            lines.append("Alternative rows considered: " + ", ".join(sorted(alt["model_label"].astype(str).unique().tolist())))
        lines.append("")
    lines += [
        "## Reporting Recommendation",
        "",
        "- Keep current selected MOA point labeled as **conservative high-precision guardrail**.",
        "- Add sensitivity table/figure showing SDE-equivalent precision operation to avoid underestimating MOA capability.",
    ]
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out


def _copy_image_as_png_pdf(src: Path, out_dir: Path, stem: str) -> tuple[Path, Path]:
    img = plt.imread(src)
    fig, ax = plt.subplots(figsize=(10.8, 6.4))
    ax.imshow(img)
    ax.axis("off")
    if "batch" in stem and "shap" in stem:
        ax.set_title("Batch direct SHAP", pad=10)
    if "sml" in stem and "surrogate" in stem:
        ax.set_title("MOA surrogate SHAP (fidelity caveat)", pad=10)
    _apply_panel_layout(fig, top=0.92, bottom=0.04, left=0.02, right=0.98)
    return _save_figure(fig, out_dir, stem)


def _figure_shap_family(context: dict[str, Any]) -> list[FigureResult]:
    out: list[FigureResult] = []
    out_dir = context["output_dir"]

    # Candidate source images from existing profile report families.
    candidates: dict[str, list[str]] = {
        "phase2b_batch_e0_shap_global_bar": [
            "data_lake/reports/no_source_year_baseline/shap_global_bar.png",
        ],
        "phase2b_batch_p1_shap_global_bar": [
            "data_lake/reports/no_source_year_percent_conservative_v1/shap_global_bar.png",
        ],
        "phase2b_batch_e0_shap_beeswarm": [
            "data_lake/reports/no_source_year_baseline/shap_beeswarm.png",
        ],
        "phase2b_batch_p1_shap_beeswarm": [
            "data_lake/reports/no_source_year_percent_conservative_v1/shap_beeswarm.png",
        ],
        "phase2b_sml_e0_surrogate_shap_global_bar": [
            "data_lake/reports/no_source_year_baseline/moa_shap_proxy_global_bar.png",
        ],
        "phase2b_sml_p1_surrogate_shap_global_bar": [
            "data_lake/reports/no_source_year_percent_conservative_v1/moa_shap_proxy_global_bar.png",
        ],
    }

    for stem, choices in candidates.items():
        src = next((Path(c) for c in choices if Path(c).exists()), None)
        if src is None:
            out.append(_manifest_fail(stem, " | ".join(choices), "source SHAP image missing"))
            continue
        png, pdf = _copy_image_as_png_pdf(src, out_dir, stem)
        out.append(
            FigureResult(
                figure=stem,
                status="PASS",
                source_artifact=str(src),
                note="Copied from existing validated SHAP/surrogate artifact.",
                png_path=str(png),
                pdf_path=str(pdf),
            )
        )

    def _feature_compare(
        *,
        stem: str,
        e0_csv: Path,
        p1_csv: Path,
        source_label: str,
        title: str,
        value_col: str = "mean_abs_shap",
    ) -> FigureResult:
        if not e0_csv.exists() or not p1_csv.exists():
            return _manifest_fail(stem, f"{e0_csv} | {p1_csv}", "feature-level SHAP CSV inputs not found")
        try:
            e0 = pd.read_csv(e0_csv)
            p1 = pd.read_csv(p1_csv)
        except Exception as exc:
            return _manifest_fail(stem, source_label, f"failed reading feature-importance csvs: {exc}")
        req = {"feature", value_col}
        if not req.issubset(set(e0.columns)) or not req.issubset(set(p1.columns)):
            return _manifest_fail(stem, source_label, f"missing required columns {sorted(req)}")

        e0v = e0[["feature", value_col]].rename(columns={value_col: "e0_value"})
        p1v = p1[["feature", value_col]].rename(columns={value_col: "p1_value"})
        merged = e0v.merge(p1v, on="feature", how="outer").fillna(0.0)
        merged["delta_p1_minus_e0"] = merged["p1_value"] - merged["e0_value"]
        merged["rank_score"] = merged[["e0_value", "p1_value"]].max(axis=1)
        top = merged.sort_values("rank_score", ascending=False).head(12).copy()
        if top.empty:
            return _manifest_fail(stem, source_label, "merged feature table empty")
        top = top.sort_values("rank_score", ascending=True)

        y = np.arange(len(top))
        h = 0.35
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14.2, 5.6), gridspec_kw={"width_ratios": [1.8, 1.2]})
        ax1.barh(
            y - h / 2,
            top["e0_value"],
            height=h,
            color="#4C78A8",
            label=_display_profile(PROFILE_E0),
        )
        ax1.barh(
            y + h / 2,
            top["p1_value"],
            height=h,
            color="#F58518",
            label=_display_profile(PROFILE_P1),
        )
        ax1.set_yticks(y)
        ax1.set_yticklabels(top["feature"].astype(str))
        ax1.set_xlabel(value_col)
        ax1.set_title("Top features: No-Year Baseline vs Percent Features")
        ax1.grid(axis="x", alpha=0.25)
        ax1.legend(frameon=False)

        colors = np.where(top["delta_p1_minus_e0"] >= 0, "#59A14F", "#E15759")
        ax2.barh(y, top["delta_p1_minus_e0"], color=colors)
        ax2.axvline(0.0, color="black", linewidth=0.8)
        ax2.set_yticks(y)
        ax2.set_yticklabels([])
        ax2.set_xlabel("Delta (Percent Features - No-Year Baseline)")
        ax2.set_title("Delta by feature")
        ax2.grid(axis="x", alpha=0.25)
        fig.suptitle(title, y=0.98)
        _apply_panel_layout(fig, top=0.86, bottom=0.12, left=0.14, right=0.98, wspace=0.28)

        png, pdf = _save_figure(fig, out_dir, stem)
        return FigureResult(
            figure=stem,
            status="PASS",
            source_artifact=source_label,
            note="Feature-level comparison generated from structured SHAP importance CSVs.",
            png_path=str(png),
            pdf_path=str(pdf),
        )

    out.append(
        _feature_compare(
            stem="phase2b_batch_shap_top_feature_comparison_e0_p1",
            e0_csv=Path("data_lake/reports/no_source_year_baseline/shap_feature_importance.csv"),
            p1_csv=Path("data_lake/reports/no_source_year_percent_conservative_v1/shap_feature_importance.csv"),
            source_label="no_source_year_baseline/shap_feature_importance.csv + no_source_year_percent_conservative_v1/shap_feature_importance.csv",
            title="Batch direct-SHAP top-feature comparison (No-Year vs Percent)",
        )
    )
    out.append(
        _feature_compare(
            stem="phase2b_sml_surrogate_shap_top_feature_comparison_e0_p1",
            e0_csv=Path("data_lake/reports/no_source_year_baseline/moa_shap_proxy_feature_importance.csv"),
            p1_csv=Path("data_lake/reports/no_source_year_percent_conservative_v1/moa_shap_proxy_feature_importance.csv"),
            source_label="no_source_year_baseline/moa_shap_proxy_feature_importance.csv + no_source_year_percent_conservative_v1/moa_shap_proxy_feature_importance.csv",
            title="MOA surrogate-SHAP proxy top-feature comparison (No-Year vs Percent)",
        )
    )

    fidelity_stem = "phase2b_sml_surrogate_fidelity_summary"
    fidelity_inputs = [
        Path("data_lake/reports/no_source_year_baseline/moa_shap_proxy_summary.csv"),
        Path("data_lake/reports/no_source_year_percent_conservative_v1/moa_shap_proxy_summary.csv"),
    ]
    if all(p.exists() for p in fidelity_inputs):
        try:
            rows = []
            for profile, p in [(PROFILE_E0, fidelity_inputs[0]), (PROFILE_P1, fidelity_inputs[1])]:
                df = pd.read_csv(p)
                if df.empty:
                    continue
                r = df.iloc[0]
                rows.append(
                    {
                        "profile": profile,
                        "fidelity_accuracy": _safe_float(r.get("fidelity_accuracy"), float("nan")),
                        "fidelity_f1": _safe_float(r.get("fidelity_f1"), float("nan")),
                        "rows_used": _safe_float(r.get("rows_used"), float("nan")),
                        "sample_rows": _safe_float(r.get("sample_rows"), float("nan")),
                    }
                )
            fidelity = pd.DataFrame(rows)
            if fidelity.empty or {"profile", "fidelity_accuracy", "fidelity_f1"}.difference(fidelity.columns):
                out.append(_manifest_fail(fidelity_stem, "surrogate summary csv", "surrogate fidelity rows unavailable"))
            else:
                x = np.arange(len(fidelity))
                w = 0.35
                fig, ax = plt.subplots(figsize=(8.4, 4.8))
                ax.bar(x - w / 2, fidelity["fidelity_accuracy"], width=w, color="#4C78A8", label="Fidelity accuracy")
                ax.bar(x + w / 2, fidelity["fidelity_f1"], width=w, color="#F58518", label="Fidelity F1")
                ax.set_xticks(x)
                ax.set_xticklabels(fidelity["profile"].astype(str).str.replace("_", " "), rotation=15)
                ax.set_ylim(0.0, 1.0)
                ax.set_ylabel("Score")
                ax.set_title("MOA surrogate fidelity summary")
                ax.grid(axis="y", alpha=0.25)
                ax.legend(frameon=False)
                ax.text(0.5, -0.16, "Surrogate-only explainability; interpret with fidelity caveat.", transform=ax.transAxes, ha="center", fontsize=9)
                _apply_panel_layout(fig, top=0.88, bottom=0.22)
                png, pdf = _save_figure(fig, out_dir, fidelity_stem)
                out.append(
                    FigureResult(
                        figure=fidelity_stem,
                        status="PASS",
                        source_artifact="no_source_year_baseline/moa_shap_proxy_summary.csv + no_source_year_percent_conservative_v1/moa_shap_proxy_summary.csv",
                        note="Surrogate fidelity metrics from available summary CSVs.",
                        png_path=str(png),
                        pdf_path=str(pdf),
                    )
                )
        except Exception as exc:
            out.append(_manifest_fail(fidelity_stem, "surrogate summary csv", f"failed building fidelity summary: {exc}"))
    else:
        out.append(_manifest_fail(fidelity_stem, "surrogate summary csv", "surrogate fidelity summary CSV not found"))

    return out


def _error_analysis_outputs(context: dict[str, Any]) -> list[FigureResult]:
    out: list[FigureResult] = []
    out_dir = context["output_dir"]
    batch_oof_dir = Path(context["paths"]["batch_oof_dir"])
    sml_oof_dir = Path(context["paths"]["sml_oof_dir"])
    batch_reco = context["batch_reco"]
    sml_reco = context["sml_reco"]
    dataset_path = Path(context["paths"]["training_dataset_parquet"])

    taxonomy_template = out_dir / "phase2b_error_taxonomy_manual_template_clean_actionable.csv"
    taxonomy_summary = out_dir / "phase2b_error_taxonomy_summary_clean_actionable.csv"
    overlap_csv = out_dir / "phase2b_error_consensus_overlap_counts_clean_actionable.csv"
    near_miss_csv = out_dir / "phase2b_error_near_miss_distribution_clean_actionable.csv"

    run_specs = [
        ("batch", PROFILE_E0, batch_oof_dir, batch_reco),
        ("batch", PROFILE_P1, batch_oof_dir, batch_reco),
        ("sml", PROFILE_E0, sml_oof_dir, sml_reco),
        ("sml", PROFILE_P1, sml_oof_dir, sml_reco),
    ]
    targets = [TARGET_PIT_ANY_CLEAN, TARGET_PIT_SUCCESS_CLEAN]
    decision_rows: list[pd.DataFrame] = []

    for method, profile, oof_dir, reco in run_specs:
        for target_col in targets:
            if reco.empty:
                continue
            reco_row = reco[
                reco["profile"].astype(str).eq(profile)
                & reco["target_column"].astype(str).eq(target_col)
                & reco["truth_lens"].astype(str).eq("clean_actionable")
            ]
            if reco_row.empty:
                continue
            thr = _safe_float(reco_row.iloc[0].get("selected_threshold"), float("nan"))
            run_id = f"{profile}__{target_col}"
            oof_path = oof_dir / f"{run_id}.csv"
            if not oof_path.exists() or math.isnan(thr):
                continue
            try:
                oof = pd.read_csv(oof_path, usecols=["race", "driver", "lapNumber", "target_y", "calibrated_proba", "raw_proba"])
            except Exception:
                continue
            score = pd.to_numeric(oof["calibrated_proba"], errors="coerce")
            if score.isna().all():
                score = pd.to_numeric(oof["raw_proba"], errors="coerce")
            oof = oof.assign(score=score.fillna(0.0))
            oof = oof.assign(predicted_positive=(oof["score"] >= thr).astype(int))
            oof = oof[oof["predicted_positive"] == 1].copy()
            if oof.empty:
                continue
            oof["tp_row"] = (pd.to_numeric(oof["target_y"], errors="coerce").fillna(0).astype(int) == 1).astype(int)
            oof["fp_row"] = 1 - oof["tp_row"]
            oof["method"] = method
            oof["profile"] = profile
            oof["target_column"] = target_col
            oof["run_id"] = run_id
            decision_rows.append(oof)

    if not decision_rows:
        # Keep scaffolds, but mark figures as SKIP with explicit reason.
        pd.DataFrame(columns=["method", "profile", "target_column", "race", "driver", "lapNumber", "score"]).to_csv(
            taxonomy_template, index=False
        )
        pd.DataFrame([{"status": "UNAVAILABLE", "reason": "no decision rows reconstructed from OOF + recommended thresholds"}]).to_csv(
            taxonomy_summary, index=False
        )
        pd.DataFrame([{"status": "UNAVAILABLE", "reason": "no decision rows"}]).to_csv(overlap_csv, index=False)
        pd.DataFrame([{"status": "UNAVAILABLE", "reason": "no decision rows"}]).to_csv(near_miss_csv, index=False)
        out.append(FigureResult(figure="phase2b_error_taxonomy_manual_template_clean_actionable", status="PASS", source_artifact=str(taxonomy_template), note="Template emitted; no reconstructed decision rows found."))
        out.append(FigureResult(figure="phase2b_error_taxonomy_summary_clean_actionable", status="PASS", source_artifact=str(taxonomy_summary), note="Summary scaffold emitted with unavailable status."))
        out.append(_manifest_fail("phase2b_error_consensus_upset_clean_actionable", "phase2b error-analysis row-level sources", "no reconstructed decision rows"))
        out.append(_manifest_fail("phase2b_error_near_miss_distribution_clean_actionable", "phase2b error-analysis row-level sources", "no reconstructed decision rows"))
        out.append(_manifest_fail("phase2b_error_batch_fp_tp_feature_profiles_clean_actionable", "phase2b error-analysis row-level sources", "no reconstructed decision rows"))
        out.append(_manifest_fail("phase2b_error_sml_fp_tp_feature_profiles_clean_actionable", "phase2b error-analysis row-level sources", "no reconstructed decision rows"))
        return out

    decisions = pd.concat(decision_rows, ignore_index=True)
    key_cols = ["race", "driver", "lapNumber"]
    decisions["key"] = (
        decisions["race"].astype(str) + "|" + decisions["driver"].astype(str) + "|" + decisions["lapNumber"].astype(str)
    )

    # Taxonomy template from FP decision rows.
    fp = decisions[decisions["fp_row"] == 1].copy()
    template_cols = ["method", "profile", "target_column", "race", "driver", "lapNumber", "score"]
    taxonomy = fp[template_cols].copy()
    taxonomy["predicted_positive"] = 1
    taxonomy["truth_label_or_match_status"] = "fp_row_level"
    taxonomy["nearest_pit_delta_if_available"] = np.nan
    taxonomy["suggested_category"] = ""
    taxonomy["manual_category"] = ""
    taxonomy["notes"] = ""
    taxonomy.to_csv(taxonomy_template, index=False)

    summary = (
        decisions.groupby(["method", "profile", "target_column"], as_index=False)
        .agg(scored=("predicted_positive", "sum"), row_tp=("tp_row", "sum"), row_fp=("fp_row", "sum"))
        .sort_values(["method", "profile", "target_column"], kind="mergesort")
    )
    summary.to_csv(taxonomy_summary, index=False)

    out.append(
        FigureResult(
            figure="phase2b_error_taxonomy_manual_template_clean_actionable",
            status="PASS",
            source_artifact=str(taxonomy_template),
            note="Row-level FP template from OOF + recommended thresholds (diagnostic).",
        )
    )
    out.append(
        FigureResult(
            figure="phase2b_error_taxonomy_summary_clean_actionable",
            status="PASS",
            source_artifact=str(taxonomy_summary),
            note="Row-level diagnostic summary (not event-level comparator taxonomy).",
        )
    )

    # Consensus overlap for pit_any clean_actionable row-level predicted positives.
    any_rows = decisions[decisions["target_column"].astype(str).eq(TARGET_PIT_ANY_CLEAN)].copy()
    if any_rows.empty:
        pd.DataFrame([{"status": "UNAVAILABLE", "reason": "pit_any clean_actionable rows unavailable"}]).to_csv(overlap_csv, index=False)
        out.append(_manifest_fail("phase2b_error_consensus_upset_clean_actionable", "phase2b error-analysis row-level sources", "pit_any clean_actionable rows unavailable"))
    else:
        any_rows["label"] = any_rows.apply(
            lambda r: _display_profile(str(r.get("profile", "")), str(r.get("method", ""))),
            axis=1,
        )
        overlap = (
            any_rows.groupby("key")["label"]
            .apply(lambda s: "+".join(sorted(set(s.tolist()))))
            .reset_index(name="overlap_bucket")
        )
        counts = overlap.groupby("overlap_bucket", as_index=False).size().rename(columns={"size": "count"}).sort_values(
            "count", ascending=False
        )
        counts.to_csv(overlap_csv, index=False)
        top = counts.head(10).sort_values("count", ascending=True)
        def _wrap_bucket(s: str, max_len: int = 28) -> str:
            parts = s.split("+")
            out = []
            cur = ""
            for p in parts:
                candidate = p if not cur else f"{cur}+{p}"
                if len(candidate) > max_len and cur:
                    out.append(cur)
                    cur = p
                else:
                    cur = candidate
            if cur:
                out.append(cur)
            return "\n".join(out)
        top["bucket_short"] = top["overlap_bucket"].astype(str).map(_wrap_bucket)
        fig, ax = plt.subplots(figsize=(12.8, 6.4))
        ax.barh(top["bucket_short"], top["count"], color="#4C78A8")
        ax.set_xlabel("Count")
        ax.set_title("Phase 2B Error Consensus Overlap (clean_actionable)")
        ax.text(0.0, 1.02, "row-level diagnostic false-positive overlap", transform=ax.transAxes, fontsize=9, alpha=0.85)
        ax.grid(axis="x", alpha=0.25)
        _apply_panel_layout(fig, top=0.86, bottom=0.15, left=0.26, right=0.98)
        png, pdf = _save_figure(fig, out_dir, "phase2b_error_consensus_upset_clean_actionable")
        out.append(
            FigureResult(
                figure="phase2b_error_consensus_upset_clean_actionable",
                status="PASS",
                source_artifact=str(overlap_csv),
                note="Row-level overlap of predicted-positive decisions across Batch and MOA profiles.",
                png_path=str(png),
                pdf_path=str(pdf),
            )
        )

    # Near-miss distribution from matched_pit_lap_any if available after join.
    if dataset_path.exists():
        try:
            ds = pd.read_parquet(dataset_path, columns=["race", "driver", "lapNumber", "matched_pit_lap_any"])
            joined = any_rows.merge(ds, on=["race", "driver", "lapNumber"], how="left")
            joined["delta_laps"] = pd.to_numeric(joined["matched_pit_lap_any"], errors="coerce") - pd.to_numeric(
                joined["lapNumber"], errors="coerce"
            )
            near = joined[joined["delta_laps"].notna()].copy()
            if near.empty:
                pd.DataFrame([{"status": "UNAVAILABLE", "reason": "matched_pit_lap_any not available on joined decision rows"}]).to_csv(
                    near_miss_csv, index=False
                )
                out.append(
                    _manifest_fail(
                        "phase2b_error_near_miss_distribution_clean_actionable",
                        str(dataset_path),
                        "matched_pit_lap_any unavailable for reconstructed decision rows",
                    )
                )
            else:
                bins = [-100, -3, -2, -1, 0, 1, 2, 3, 100]
                labels = ["<=-3", "-2", "-1", "0", "+1", "+2", "+3", ">=+4"]
                near["bucket"] = pd.cut(near["delta_laps"], bins=bins, labels=labels)
                dist = near.groupby("bucket", observed=True).size().reset_index(name="count")
                dist.to_csv(near_miss_csv, index=False)
                fig, ax = plt.subplots(figsize=(10.4, 5.4))
                ax.bar(dist["bucket"].astype(str), dist["count"], color="#F58518")
                ax.set_xlabel("Matched pit lap delta (matched_pit_lap_any - lapNumber)")
                ax.set_ylabel("Count")
                ax.set_title("Near-miss distribution (clean_actionable, row-level diagnostic)")
                ax.grid(axis="y", alpha=0.25)
                _apply_panel_layout(fig, top=0.88, bottom=0.22)
                png, pdf = _save_figure(fig, out_dir, "phase2b_error_near_miss_distribution_clean_actionable")
                out.append(
                    FigureResult(
                        figure="phase2b_error_near_miss_distribution_clean_actionable",
                        status="PASS",
                        source_artifact=str(near_miss_csv),
                        note="Row-level near-miss proxy from matched_pit_lap_any (diagnostic).",
                        png_path=str(png),
                        pdf_path=str(pdf),
                    )
                )
        except Exception as exc:
            pd.DataFrame([{"status": "UNAVAILABLE", "reason": f"join/read failure: {exc}"}]).to_csv(near_miss_csv, index=False)
            out.append(_manifest_fail("phase2b_error_near_miss_distribution_clean_actionable", str(dataset_path), f"near-miss join failed: {exc}"))
    else:
        pd.DataFrame([{"status": "UNAVAILABLE", "reason": "training dataset parquet missing"}]).to_csv(near_miss_csv, index=False)
        out.append(_manifest_fail("phase2b_error_near_miss_distribution_clean_actionable", str(dataset_path), "training dataset parquet missing"))

    def _feature_profile_figure(method_name: str, stem: str) -> FigureResult:
        sub = decisions[
            decisions["method"].astype(str).eq(method_name)
            & decisions["target_column"].astype(str).eq(TARGET_PIT_ANY_CLEAN)
        ].copy()
        if sub.empty:
            return _manifest_fail(stem, "decision rows", f"{method_name} pit_any clean_actionable rows unavailable")
        if not dataset_path.exists():
            return _manifest_fail(stem, str(dataset_path), "training dataset parquet missing")
        try:
            ds = pd.read_parquet(dataset_path)
        except Exception as exc:
            return _manifest_fail(stem, str(dataset_path), f"failed reading parquet: {exc}")
        joined = sub.merge(ds, on=key_cols, how="left", indicator=True)
        join_rate = float((joined["_merge"] == "both").mean()) if len(joined) else 0.0
        if join_rate < 0.95:
            return _manifest_fail(stem, str(dataset_path), f"feature join rate below 95% ({join_rate:.3f})")
        joined = joined.drop(columns=["_merge"], errors="ignore")

        banned_patterns = (
            "target_",
            "pit_any_h2_",
            "pit_success_h2_",
            "matched_pit",
            "eligib",
            "_source_year",
        )
        num_cols = [
            c
            for c in joined.columns
            if c not in key_cols + ["method", "profile", "target_column", "run_id", "score", "predicted_positive", "tp_row", "fp_row", "key", "target_y", "calibrated_proba", "raw_proba"]
            and pd.api.types.is_numeric_dtype(joined[c])
            and not any(pat in c for pat in banned_patterns)
        ]
        if not num_cols:
            return _manifest_fail(stem, "joined decision features", "no numeric non-leakage features available")

        tp = joined[joined["tp_row"] == 1]
        fpj = joined[joined["fp_row"] == 1]
        if tp.empty or fpj.empty:
            return _manifest_fail(stem, "joined decision features", "tp/fp groups unavailable for feature profile")

        rows = []
        for c in num_cols:
            a = pd.to_numeric(tp[c], errors="coerce").dropna()
            b = pd.to_numeric(fpj[c], errors="coerce").dropna()
            if len(a) < 10 or len(b) < 10:
                continue
            pooled = pd.to_numeric(joined[c], errors="coerce").dropna().std()
            if pooled is None or not np.isfinite(pooled) or pooled <= 1e-12:
                continue
            rows.append({"feature": c, "tp_median": float(a.median()), "fp_median": float(b.median()), "std_diff": float((a.median() - b.median()) / pooled)})
        if not rows:
            return _manifest_fail(stem, "joined decision features", "insufficient numeric signal for TP/FP comparison")
        diff = pd.DataFrame(rows).sort_values("std_diff", key=lambda s: s.abs(), ascending=False).head(10).copy()
        diff = diff.sort_values("std_diff", ascending=True)
        colors = np.where(diff["std_diff"] >= 0, "#59A14F", "#E15759")
        diff["feature_short"] = diff["feature"].astype(str).str.replace("_", " ", regex=False).str.slice(0, 40)
        fig, ax = plt.subplots(figsize=(11.4, 5.8))
        ax.barh(diff["feature_short"], diff["std_diff"], color=colors)
        ax.axvline(0.0, color="black", linewidth=0.8)
        ax.set_xlabel("Standardized median diff (TP - FP)")
        ax.set_title(f"{method_name.upper()} FP/TP feature profile (clean_actionable, row-level diagnostic)")
        ax.grid(axis="x", alpha=0.25)
        _apply_panel_layout(fig, top=0.88, bottom=0.16, left=0.25, right=0.98)
        png, pdf = _save_figure(fig, out_dir, stem)
        return FigureResult(
            figure=stem,
            status="PASS",
            source_artifact=str(dataset_path),
            note=f"Top feature differences for TP vs FP decisions ({method_name}, row-level diagnostic).",
            png_path=str(png),
            pdf_path=str(pdf),
        )

    out.append(_feature_profile_figure("batch", "phase2b_error_batch_fp_tp_feature_profiles_clean_actionable"))
    out.append(_feature_profile_figure("sml", "phase2b_error_sml_fp_tp_feature_profiles_clean_actionable"))

    return out


def _write_index_md(output_dir: Path, manifest: list[FigureResult], warnings: list[str]) -> Path:
    out = output_dir / "phase2b_figures_index.md"
    lines = [
        "# Phase 2B Figure Pack (2022-2025)",
        "",
        "| Figure | Status | Source Artifact | Thesis Interpretation |",
        "| --- | --- | --- | --- |",
    ]
    for m in manifest:
        meta = SAVE_META_BY_STEM.get(m.figure, {})
        formats = str(m.formats or meta.get("formats", "")).strip()
        fmt_note = f" (formats: {formats})" if formats else ""
        lines.append(f"| `{m.figure}` | {m.status} | `{m.source_artifact}` | {m.note}{fmt_note} |")
    if warnings:
        lines.extend(["", "## Warnings", ""])
        for warning in warnings:
            lines.append(f"- {warning}")
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out


def _write_manifest_csv(output_dir: Path, manifest: list[FigureResult]) -> Path:
    out = output_dir / "phase2b_figures_manifest.csv"
    pd.DataFrame(
        [
            {
                "figure": m.figure,
                "status": m.status,
                "source_artifact": m.source_artifact,
                "note": m.note,
                "png_path": m.png_path or str(SAVE_META_BY_STEM.get(m.figure, {}).get("png_path", "")),
                "pdf_path": m.pdf_path or str(SAVE_META_BY_STEM.get(m.figure, {}).get("pdf_path", "")),
                "svg_path": m.svg_path or str(SAVE_META_BY_STEM.get(m.figure, {}).get("svg_path", "")),
                "formats": m.formats or str(SAVE_META_BY_STEM.get(m.figure, {}).get("formats", "")),
                "png_dpi": (
                    m.png_dpi
                    if m.png_dpi is not None
                    else SAVE_META_BY_STEM.get(m.figure, {}).get("png_dpi", None)
                ),
                "width_in": (
                    m.width_in
                    if m.width_in is not None
                    else SAVE_META_BY_STEM.get(m.figure, {}).get("width_in", None)
                ),
                "height_in": (
                    m.height_in
                    if m.height_in is not None
                    else SAVE_META_BY_STEM.get(m.figure, {}).get("height_in", None)
                ),
            }
            for m in manifest
        ]
    ).to_csv(out, index=False)
    return out


def _write_label_map_files(output_dir: Path) -> tuple[Path, Path]:
    rows = [
        {
            "internal_name": "c6_cfg120_fixed",
            "report_label": "Final SDE",
            "meaning": "frozen deterministic rule baseline",
        },
        {
            "internal_name": "e0_no_source_year",
            "report_label": "No-Year Baseline",
            "meaning": "Phase 2B profile with `_source_year` removed",
        },
        {
            "internal_name": "p1_percent_conservative_v1",
            "report_label": "Percent Features",
            "meaning": "conservative percentage/race-progress feature profile",
        },
        {
            "internal_name": "Batch E0",
            "report_label": "Batch No-Year",
            "meaning": "Batch ML using No-Year Baseline",
        },
        {
            "internal_name": "Batch P1",
            "report_label": "Batch Percent",
            "meaning": "Batch ML using Percent Features",
        },
        {
            "internal_name": "SML/MOA E0",
            "report_label": "MOA No-Year",
            "meaning": "streaming MOA using No-Year Baseline",
        },
        {
            "internal_name": "SML/MOA P1",
            "report_label": "MOA Percent",
            "meaning": "streaming MOA using Percent Features",
        },
    ]
    csv_path = output_dir / "phase2b_label_map.csv"
    md_path = output_dir / "phase2b_label_map.md"
    pd.DataFrame(rows).to_csv(csv_path, index=False)
    lines = [
        "# Phase 2B Label Map",
        "",
        "| Internal name | Report label | Meaning |",
        "| --- | --- | --- |",
    ]
    for row in rows:
        lines.append(f"| `{row['internal_name']}` | {row['report_label']} | {row['meaning']} |")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return csv_path, md_path


def _write_professor_brief(output_dir: Path) -> Path:
    out = output_dir / "phase2b_professor_brief.md"
    lines = [
        "# Phase 2B Professor Brief",
        "",
        "## 1) What Changed From The Original SDE Work",
        "",
        "- We started by improving deterministic SDE `pit_any_h2`.",
        "- We tested `c123`, `c5`, and `c6_cfg120_fixed`.",
        "- `c6_cfg120_fixed` became **Final SDE** because it improved pit-any while preserving pit-success behavior.",
        "",
        "| Variant | Row TP | FP | Precision | Recall | F0.5 |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
        "| c123 | 54 | 179 | 0.231760 | 0.067839 | 0.156250 |",
        "| c5 | 77 | 277 | 0.217514 | 0.096734 | 0.174051 |",
        "| Final SDE / c6_cfg120_fixed | 63 | 191 | 0.248031 | 0.079146 | 0.173841 |",
        "",
        "Final SDE aggregate:",
        "- `pit_any_h2 clean_actionable`: precision=0.270574, recall=0.071194, F0.5=0.173434, row_tp=217, FP=585.",
        "- `pit_success_h2 clean_actionable`: precision=0.628283, TP=311, FP=184, scored=495.",
        "",
        "## 2) Why Phase 2B Exists",
        "",
        "- Dual-contract evaluation separates strict timing detection from strategy-success advice.",
        "- `pit_any_h2 = episode-level PIT_NOW_ONLY H=2`.",
        "- `pit_success_h2 = row-level PIT_NOW_ONLY H=2 (strict future [k+1,k+2])`.",
        "- `canonical_sde_truth + clean_actionable` is the main thesis headline universe.",
        "- `raw` and `clean_dry_strategy` are sensitivity lenses.",
        "",
        "## 3) Batch Phase 2B Headline",
        "",
        "Readable profile labels:",
        "- Batch No-Year",
        "- Batch Percent",
        "",
        "`pit_any_h2 clean_actionable`",
        "",
        "| Model | Threshold | AP | Row TP | TP recall | FP | Scored | Precision | Recall | F0.5 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        "| Batch No-Year | 0.20 | 0.131343 | 386 | 372 | 1502 | 1888 | 0.204449 | 0.122047 | 0.180126 |",
        "| Batch Percent | 0.19 | 0.190228 | 700 | 670 | 2195 | 2895 | 0.241796 | 0.219816 | 0.237055 |",
        "",
        "`pit_success_h2 clean_actionable`",
        "",
        "| Model | Threshold | AP | Row TP | TP recall | FP | Scored | Precision | Recall | F0.5 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        "| Batch No-Year | 0.19 | 0.062195 | 64 | 107 | 25 | 89 | 0.719101 | 0.035105 | 0.146849 |",
        "| Batch Percent | 0.19 | 0.080373 | 114 | 237 | 69 | 183 | 0.622951 | 0.077756 | 0.259312 |",
        "",
        "Interpretation:",
        "- Batch Percent is stronger for `pit_any_h2`.",
        "- For `pit_success_h2`, Batch No-Year is the higher-precision guardrail; Batch Percent has better reach, recall, F0.5, and AP.",
        "",
        "## 4) MOA Phase 2B Headline",
        "",
        "Readable profile labels:",
        "- MOA No-Year",
        "- MOA Percent",
        "",
        "MOA notes:",
        "- Initial MOA outputs were hard 0/1 labels, making threshold frontiers diagnostic only.",
        "- A custom MOA vote logger was added.",
        "- Final OOF files now have continuous vote-score outputs.",
        "- `calibrated_proba` is currently uncalibrated passthrough.",
        "",
        "`pit_any_h2 clean_actionable`",
        "",
        "| Model | Threshold | AP | Row TP | TP recall | FP | Scored | Precision | Recall | F0.5 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        "| MOA No-Year | 0.53 | 0.160907 | 588 | 559 | 1094 | 1682 | 0.349584 | 0.183399 | 0.295950 |",
        "| MOA Percent | 0.53 | 0.195114 | 585 | 555 | 1115 | 1700 | 0.344118 | 0.182087 | 0.292127 |",
        "",
        "`pit_success_h2 clean_actionable`",
        "",
        "| Model | Threshold | AP | Row TP | TP recall | FP | Scored | Precision | Recall | F0.5 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        "| MOA No-Year | 0.56 | 0.112108 | 38 | 51 | 4 | 42 | 0.904762 | 0.016732 | 0.077899 |",
        "| MOA Percent | 0.55 | 0.125342 | 66 | 85 | 14 | 80 | 0.825000 | 0.027887 | 0.122828 |",
        "",
        "Interpretation:",
        "- MOA No-Year is the strongest high-precision streaming guardrail.",
        "- MOA Percent has better AP and broader reach, especially for `pit_success_h2`.",
        "",
        "## 5) Final Story In One Paragraph",
        "",
        "Final SDE improved deterministic pit timing detection while preserving pit-success behavior. Batch Phase 2B shows that the Percent Features profile improves reach and ranking quality, especially for pit_any_h2. MOA Phase 2B now uses continuous vote-score outputs rather than hard labels, giving a real threshold frontier. Under the clean_actionable canonical SDE truth lens, MOA provides the strongest high-precision streaming comparator, while Batch Percent provides the broader reach/ranking improvement.",
        "",
        "## 6) Figures To Show First",
        "",
        "- `data_lake/reports/phase2b_presentation_figures/phase2b_clean_actionable_precision_recall_f05.png`",
        "- `data_lake/reports/phase2b_presentation_figures/phase2b_clean_actionable_scored_tp_fp.png`",
        "- `data_lake/reports/phase2b_presentation_figures/phase2b_threshold_frontier_batch_clean_actionable.png`",
        "- `data_lake/reports/phase2b_presentation_figures/phase2b_threshold_frontier_sml_clean_actionable.png`",
        "- `data_lake/reports/phase2b_presentation_figures/phase2b_batch_pr_curves_clean_actionable.png`",
        "- `data_lake/reports/phase2b_presentation_figures/phase2b_sml_pr_curves_clean_actionable.png`",
        "- `data_lake/reports/phase2b_presentation_figures/phase2b_temporal_drift_by_race_clean_actionable_slide.png`",
        "- `data_lake/reports/phase2b_presentation_figures/phase2b_batch_shap_top_feature_comparison_e0_p1.png`",
        "- `data_lake/reports/phase2b_presentation_figures/phase2b_sml_surrogate_shap_top_feature_comparison_e0_p1.png`",
        "",
        "## 7) Caveats",
        "",
        "- Batch SHAP is direct model explainability.",
        "- MOA SHAP is surrogate explainability only.",
        "- MOA scores are continuous vote scores, not calibrated probabilities.",
        "- Batch probability-diagnostic plots use Phase2A OOF as a validated OOF probability-diagnostic source for Phase 2B dual-contract/frontier reporting, with explicit source-integrity audit checks.",
        "- Error-analysis plots are row-level diagnostics.",
    ]
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out


def _build_context(args: argparse.Namespace, warnings: list[str]) -> dict[str, Any]:
    resolved_batch_oof_dir, batch_oof_source_mode = _resolve_batch_oof_dir(
        Path(args.batch_oof_dir),
        Path(args.batch_oof_fallback_dir),
        warnings,
    )
    paths = {
        "sde_aggregate_csv": args.sde_aggregate_csv,
        "sde_per_year_csv": args.sde_per_year_csv,
        "batch_compact_csv": args.batch_compact_csv,
        "batch_frontier_csv": args.batch_frontier_csv,
        "batch_by_year_csv": args.batch_by_year_csv,
        "batch_recommended_csv": args.batch_recommended_csv,
        "batch_oof_dir": str(resolved_batch_oof_dir),
        "batch_oof_primary_dir": args.batch_oof_dir,
        "batch_oof_fallback_dir": args.batch_oof_fallback_dir,
        "sml_compact_csv": args.sml_compact_csv,
        "sml_frontier_csv": args.sml_frontier_csv,
        "sml_by_year_csv": args.sml_by_year_csv,
        "sml_recommended_csv": args.sml_recommended_csv,
        "sml_matrix_csv": args.sml_matrix_csv,
        "sml_prequential_csv": args.sml_prequential_csv,
        "sml_oof_dir": args.sml_oof_dir,
        "universe_summary_csv": args.universe_summary_csv,
        "pit_success_policy_diagnostic_csv": args.pit_success_policy_diagnostic_csv,
        "training_dataset_parquet": args.training_dataset_parquet,
        "race_by_race_metrics_csv": args.race_by_race_metrics_csv,
    }
    ctx = {
        "paths": paths,
        "output_dir": Path(args.output_dir),
        "batch_oof_source_mode": batch_oof_source_mode,
        "sde_agg": _load_csv(Path(args.sde_aggregate_csv), warnings, "sde_aggregate"),
        "sde_per_year": _load_csv(Path(args.sde_per_year_csv), warnings, "sde_per_year"),
        "batch_compact": _load_csv(Path(args.batch_compact_csv), warnings, "batch_compact"),
        "batch_frontier": _load_csv(Path(args.batch_frontier_csv), warnings, "batch_frontier"),
        "batch_by_year": _load_csv(Path(args.batch_by_year_csv), warnings, "batch_by_year"),
        "batch_reco": _load_csv(Path(args.batch_recommended_csv), warnings, "batch_recommended"),
        "sml_compact": _load_csv(Path(args.sml_compact_csv), warnings, "sml_compact"),
        "sml_frontier": _load_csv(Path(args.sml_frontier_csv), warnings, "sml_frontier"),
        "sml_by_year": _load_csv(Path(args.sml_by_year_csv), warnings, "sml_by_year"),
        "sml_reco": _load_csv(Path(args.sml_recommended_csv), warnings, "sml_recommended"),
        "sml_matrix": _load_csv(Path(args.sml_matrix_csv), warnings, "sml_matrix"),
        "sml_preq": _load_csv(Path(args.sml_prequential_csv), warnings, "sml_prequential"),
        "universe": _load_csv(Path(args.universe_summary_csv), warnings, "universe_summary"),
        "pit_success_policy_diag": _load_csv(
            Path(args.pit_success_policy_diagnostic_csv), warnings, "pit_success_policy_diagnostic"
        ),
        "race_by_race_metrics": _load_csv(
            Path(args.race_by_race_metrics_csv), warnings, "race_by_race_metrics"
        ),
    }
    return ctx


def main() -> None:
    args = parse_args()
    global SAVE_FORMATS, SAVE_DPI, SAVE_FIG_SCALE, SAVE_META_BY_STEM
    SAVE_FORMATS = _resolve_formats(args.formats)
    SAVE_DPI = max(72, int(args.dpi))
    SAVE_FIG_SCALE = max(0.5, float(args.fig_scale))
    SAVE_META_BY_STEM = {}

    font_scale = max(1.0, min(1.8, SAVE_FIG_SCALE))
    matplotlib.rcParams.update(
        {
            "axes.titlesize": max(14, int(round(12 * font_scale))),
            "axes.labelsize": max(11, int(round(10 * font_scale))),
            "xtick.labelsize": max(9, int(round(9 * font_scale))),
            "ytick.labelsize": max(9, int(round(9 * font_scale))),
            "legend.fontsize": max(9, int(round(9 * font_scale))),
            "figure.titlesize": max(14, int(round(13 * font_scale))),
        }
    )

    warnings: list[str] = []
    context = _build_context(args, warnings)
    out_dir: Path = context["output_dir"]
    out_dir.mkdir(parents=True, exist_ok=True)

    jobs = _resolve_jobs(args.jobs)

    jobs_list: list[tuple[str, Callable[..., FigureResult], dict[str, Any]]] = [
        ("universe_alignment_bar", _figure_universe_alignment, {"context": context}),
        ("phase2b_clean_actionable_precision_recall_f05", _figure_clean_actionable_precision_recall_f05, {"context": context}),
        ("phase2b_clean_actionable_scored_tp_fp", _figure_clean_actionable_scored_tp_fp, {"context": context}),
        ("phase2b_e0_vs_p1_delta_clean_actionable", _figure_delta_clean_actionable, {"context": context}),
        (
            "phase2b_batch_pr_curves_clean_actionable",
            _pr_curve_figure,
            {
                "method_name": "Batch",
                "oof_dir": Path(context["paths"]["batch_oof_dir"]),
                "stem": "phase2b_batch_pr_curves_clean_actionable",
                "by_year": False,
                "context": context,
            },
        ),
        (
            "phase2b_batch_pr_curves_by_year_clean_actionable",
            _pr_curve_figure,
            {
                "method_name": "Batch",
                "oof_dir": Path(context["paths"]["batch_oof_dir"]),
                "stem": "phase2b_batch_pr_curves_by_year_clean_actionable",
                "by_year": True,
                "context": context,
            },
        ),
        (
            "phase2b_sml_pr_curves_clean_actionable",
            _pr_curve_figure,
            {
                "method_name": "MOA",
                "oof_dir": Path(context["paths"]["sml_oof_dir"]),
                "stem": "phase2b_sml_pr_curves_clean_actionable",
                "by_year": False,
                "context": context,
            },
        ),
        (
            "phase2b_sml_pr_curves_by_year_clean_actionable",
            _pr_curve_figure,
            {
                "method_name": "MOA",
                "oof_dir": Path(context["paths"]["sml_oof_dir"]),
                "stem": "phase2b_sml_pr_curves_by_year_clean_actionable",
                "by_year": True,
                "context": context,
            },
        ),
        (
            "phase2b_threshold_frontier_batch_clean_actionable",
            _frontier_figure,
            {
                "method_name": "Batch",
                "frontier": context["batch_frontier"],
                "stem": "phase2b_threshold_frontier_batch_clean_actionable",
                "context": context,
                "is_sml": False,
            },
        ),
        (
            "phase2b_threshold_frontier_sml_clean_actionable",
            _frontier_figure,
            {
                "method_name": "MOA",
                "frontier": context["sml_frontier"],
                "stem": "phase2b_threshold_frontier_sml_clean_actionable",
                "context": context,
                "is_sml": True,
            },
        ),
        (
            "phase2b_calibration_batch_clean_actionable",
            _calibration_figure,
            {
                "method_name": "Batch",
                "oof_dir": Path(context["paths"]["batch_oof_dir"]),
                "stem": "phase2b_calibration_batch_clean_actionable",
                "context": context,
                "is_sml": False,
            },
        ),
        (
            "phase2b_calibration_sml_clean_actionable",
            _calibration_figure,
            {
                "method_name": "MOA",
                "oof_dir": Path(context["paths"]["sml_oof_dir"]),
                "stem": "phase2b_calibration_sml_clean_actionable",
                "context": context,
                "is_sml": True,
            },
        ),
        (
            "phase2b_pr_gain_batch_clean_actionable",
            _pr_gain_figure,
            {
                "method_name": "Batch",
                "oof_dir": Path(context["paths"]["batch_oof_dir"]),
                "stem": "phase2b_pr_gain_batch_clean_actionable",
                "context": context,
                "is_sml": False,
            },
        ),
        (
            "phase2b_pr_gain_sml_clean_actionable",
            _pr_gain_figure,
            {
                "method_name": "MOA",
                "oof_dir": Path(context["paths"]["sml_oof_dir"]),
                "stem": "phase2b_pr_gain_sml_clean_actionable",
                "context": context,
                "is_sml": True,
            },
        ),
        (
            "phase2b_decision_curve_batch_clean_actionable",
            _decision_curve_figure,
            {
                "method_name": "Batch",
                "oof_dir": Path(context["paths"]["batch_oof_dir"]),
                "stem": "phase2b_decision_curve_batch_clean_actionable",
                "context": context,
                "is_sml": False,
            },
        ),
        (
            "phase2b_decision_curve_sml_clean_actionable",
            _decision_curve_figure,
            {
                "method_name": "MOA",
                "oof_dir": Path(context["paths"]["sml_oof_dir"]),
                "stem": "phase2b_decision_curve_sml_clean_actionable",
                "context": context,
                "is_sml": True,
            },
        ),
        ("phase2b_per_year_precision_recall_f05_clean_actionable", _figure_per_year_metrics, {"context": context}),
        ("phase2b_temporal_drift_by_race_clean_actionable", _figure_temporal_drift, {"context": context, "slide": False}),
        ("phase2b_temporal_drift_by_race_clean_actionable_slide", _figure_temporal_drift, {"context": context, "slide": True}),
        ("phase2b_sml_hard_decision_summary", _figure_sml_hard_decision_summary, {"context": context}),
        ("phase2b_preq_accuracy_kappa", _figure_preq_accuracy_kappa, {"context": context}),
        ("phase2b_raw_precision_recall_f05", _figure_sensitivity_metrics, {"context": context, "lens": "raw"}),
        (
            "phase2b_clean_dry_strategy_precision_recall_f05",
            _figure_sensitivity_metrics,
            {"context": context, "lens": "clean_dry_strategy"},
        ),
        ("phase2b_raw_e0_vs_p1_delta", _figure_sensitivity_delta, {"context": context, "lens": "raw"}),
        (
            "phase2b_clean_dry_strategy_e0_vs_p1_delta",
            _figure_sensitivity_delta,
            {"context": context, "lens": "clean_dry_strategy"},
        ),
        (
            "phase2b_pit_success_operating_policy_sensitivity_clean_actionable",
            _figure_pit_success_policy_sensitivity,
            {"context": context},
        ),
        (
            "phase2b_pit_success_operating_policy_sensitivity_batch_moa_clean_actionable",
            _figure_pit_success_policy_sensitivity_batch_moa,
            {"context": context},
        ),
        (
            "phase2b_seasonal_kappa_gmean_presentation",
            _figure_seasonal_kappa_gmean_presentation,
            {"context": context},
        ),
        (
            "phase2b_seasonal_kappa_candles_presentation",
            _figure_seasonal_kappa_candles_presentation,
            {"context": context},
        ),
        (
            "phase2b_seasonal_gmean_candles_presentation",
            _figure_seasonal_gmean_candles_presentation,
            {"context": context},
        ),
        (
            "phase2b_race_by_race_kappa_clean_actionable_v2",
            _figure_race_by_race_kappa_v2,
            {"context": context},
        ),
        (
            "phase2b_race_by_race_gmean_clean_actionable_v2",
            _figure_race_by_race_gmean_v2,
            {"context": context},
        ),
        (
            "phase2b_race_by_race_f05_clean_actionable_v2",
            _figure_race_by_race_f05_v2,
            {"context": context},
        ),
        (
            "phase2b_race_by_race_balanced_accuracy_clean_actionable_v2",
            _figure_race_by_race_balanced_accuracy_v2,
            {"context": context},
        ),
    ]

    manifest: list[FigureResult] = []

    def _run_job(func: Callable[..., FigureResult], kwargs: dict[str, Any]) -> FigureResult:
        return func(**kwargs)

    with ThreadPoolExecutor(max_workers=jobs) as executor:
        future_map = {
            executor.submit(_run_job, func, kwargs): name
            for name, func, kwargs in jobs_list
        }
        for future in as_completed(future_map):
            name = future_map[future]
            try:
                result = future.result()
            except Exception as exc:
                result = FigureResult(
                    figure=name,
                    status="FAIL",
                    source_artifact="internal",
                    note=f"figure generation crashed: {exc}",
                )
            manifest.append(result)

    # Sequential extras: SHAP/surrogate copy and error-analysis scaffolds.
    manifest.extend(_figure_shap_family(context))
    manifest.extend(_error_analysis_outputs(context))

    # Deterministic order in outputs.
    manifest.sort(key=lambda r: r.figure)

    manifest_csv = _write_manifest_csv(out_dir, manifest)
    index_md = _write_index_md(out_dir, manifest, warnings)
    label_map_csv, label_map_md = _write_label_map_files(out_dir)
    professor_brief_md = _write_professor_brief(out_dir)
    policy_audit_md = _write_operating_policy_audit_md(context, out_dir)

    print("=== PHASE2B PRESENTATION FIGURE PACK GENERATED ===")
    print(f"output dir       : {out_dir}")
    print(f"jobs             : {jobs}")
    print(f"formats          : {','.join(SAVE_FORMATS)}")
    print(f"png dpi          : {SAVE_DPI if 'png' in SAVE_FORMATS else 'n/a'}")
    print(f"figure scale     : {SAVE_FIG_SCALE}")
    print(f"batch oof source : {context.get('paths', {}).get('batch_oof_dir', '')} ({context.get('batch_oof_source_mode', '')})")
    print(f"manifest csv     : {manifest_csv}")
    print(f"index markdown   : {index_md}")
    print(f"label map csv    : {label_map_csv}")
    print(f"label map md     : {label_map_md}")
    print(f"professor brief  : {professor_brief_md}")
    print(f"policy audit md  : {policy_audit_md}")
    print(f"figure entries   : {len(manifest)}")
    if warnings:
        print("warnings:")
        for w in warnings:
            print(f"- {w}")


if __name__ == "__main__":
    main()
