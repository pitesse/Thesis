"""Generate thesis-ready trust diagnostics plots (calibration and latency)."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate trust diagnostics figures from Phase D/G report CSVs"
    )
    parser.add_argument(
        "--reports-dir",
        type=Path,
        default=Path("data_lake/reports"),
        help="Directory containing phase report CSV files",
    )
    parser.add_argument(
        "--suffix",
        default="2022_2025_merged",
        help="Artifact suffix, e.g. 2022_2025_merged",
    )
    parser.add_argument(
        "--latency-target-ms",
        type=float,
        default=10.0,
        help="Operational target for p95 latency reference line",
    )
    parser.add_argument(
        "--save-format",
        choices=["pdf", "png"],
        default="pdf",
        help="Output format for generated figures",
    )
    return parser.parse_args()


def _load_inputs(reports_dir: Path, suffix: str) -> dict[str, pd.DataFrame]:
    paths = {
        "phase_d_bins": reports_dir / f"calibration_reliability_bins_{suffix}.csv",
        "phase_d_summary": reports_dir / f"calibration_policy_summary_{suffix}.csv",
        "phase_g_details": reports_dir / f"live_latency_details_{suffix}.csv",
        "phase_g_by_year": reports_dir / f"live_latency_by_year_{suffix}.csv",
        "phase_g_summary": reports_dir / f"live_latency_summary_{suffix}.csv",
    }

    for label, path in paths.items():
        if not path.exists():
            raise FileNotFoundError(f"Missing required input for {label}: {path}")

    return {label: pd.read_csv(path) for label, path in paths.items()}


def _metric_from_bins(bins: pd.DataFrame, score_type: str) -> tuple[float, float]:
    subset = bins[(bins["scope"] == "overall") & (bins["score_type"] == score_type)].copy()
    if subset.empty:
        raise ValueError(f"No reliability rows found for score_type={score_type}")

    subset["weight"] = pd.to_numeric(subset["weight"], errors="coerce")
    subset["abs_gap"] = pd.to_numeric(subset["abs_gap"], errors="coerce")

    ece = float((subset["weight"] * subset["abs_gap"]).sum())
    mce = float(subset["abs_gap"].max())
    return ece, mce


def _validate_correctness(inputs: dict[str, pd.DataFrame]) -> None:
    phase_d_bins = inputs["phase_d_bins"]
    phase_d_summary = inputs["phase_d_summary"]
    phase_g_details = inputs["phase_g_details"]
    phase_g_summary = inputs["phase_g_summary"]

    if len(phase_d_summary) != 1:
        raise ValueError("Phase D summary must contain exactly one row")

    summary_row = phase_d_summary.iloc[0]

    raw_ece_bins, raw_mce_bins = _metric_from_bins(phase_d_bins, "raw")
    cal_ece_bins, cal_mce_bins = _metric_from_bins(phase_d_bins, "calibrated")

    # Cross-check reliability metrics computed from bins against summary metrics.
    if not np.isclose(raw_ece_bins, float(summary_row["raw_ece"]), atol=1e-9):
        raise ValueError(
            f"raw_ece mismatch: bins={raw_ece_bins}, summary={float(summary_row['raw_ece'])}"
        )
    if not np.isclose(cal_ece_bins, float(summary_row["calibrated_ece"]), atol=1e-9):
        raise ValueError(
            f"calibrated_ece mismatch: bins={cal_ece_bins}, summary={float(summary_row['calibrated_ece'])}"
        )
    if not np.isclose(raw_mce_bins, float(summary_row["raw_mce"]), atol=1e-9):
        raise ValueError(
            f"raw_mce mismatch: bins={raw_mce_bins}, summary={float(summary_row['raw_mce'])}"
        )
    if not np.isclose(cal_mce_bins, float(summary_row["calibrated_mce"]), atol=1e-9):
        raise ValueError(
            f"calibrated_mce mismatch: bins={cal_mce_bins}, summary={float(summary_row['calibrated_mce'])}"
        )

    total_row = phase_g_details[phase_g_details["component"] == "total_ms"]
    if total_row.empty:
        raise ValueError("Phase G details missing component=total_ms")

    latency_p95_details = float(total_row.iloc[0]["p95_ms"])
    latency_row = phase_g_summary[phase_g_summary["check"] == "latency_p95_total_ms"]
    if latency_row.empty:
        raise ValueError("Phase G summary missing check=latency_p95_total_ms")
    latency_p95_summary = float(latency_row.iloc[0]["value"])

    if not np.isclose(latency_p95_details, latency_p95_summary, atol=1e-9):
        raise ValueError(
            "latency p95 mismatch between live_latency_details and live_latency_summary"
        )


def _plot_reliability_curves(
    phase_d_bins: pd.DataFrame,
    phase_d_summary: pd.DataFrame,
    output_path: Path,
) -> None:
    fig, ax = plt.subplots(figsize=(8, 6))

    for score_type, color, label in [
        ("raw", "#ff7f0e", "Raw probability"),
        ("calibrated", "#1f77b4", "Calibrated probability"),
    ]:
        subset = phase_d_bins[
            (phase_d_bins["scope"] == "overall") & (phase_d_bins["score_type"] == score_type)
        ].copy()
        subset.sort_values("bin_index", inplace=True)
        x = (subset["bin_lower"] + subset["bin_upper"]) / 2.0
        y = subset["observed_rate"]
        ax.plot(x, y, marker="o", linewidth=2.0, color=color, label=label)

    ax.plot([0, 1], [0, 1], linestyle="--", color="gray", linewidth=1.0, label="Perfect calibration")

    row = phase_d_summary.iloc[0]
    ax.set_title("Calibration Reliability Curve", fontsize=14, pad=12)
    ax.set_xlabel("Predicted probability bin center", fontsize=12)
    ax.set_ylabel("Observed positive rate", fontsize=12)
    ax.set_xlim(0.0, 1.0)
    ax.set_ylim(0.0, 1.0)
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend(loc="upper left")

    annotation = (
        f"Raw ECE={float(row['raw_ece']):.4f}, Cal ECE={float(row['calibrated_ece']):.4f}\n"
        f"Raw Brier={float(row['raw_brier']):.4f}, Cal Brier={float(row['calibrated_brier']):.4f}"
    )
    ax.text(0.03, 0.92, annotation, transform=ax.transAxes, fontsize=10, va="top")

    fig.tight_layout()
    fig.savefig(output_path, dpi=300)
    plt.close(fig)


def _plot_reliability_gap_bars(phase_d_bins: pd.DataFrame, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5))

    raw = phase_d_bins[(phase_d_bins["scope"] == "overall") & (phase_d_bins["score_type"] == "raw")].copy()
    cal = phase_d_bins[(phase_d_bins["scope"] == "overall") & (phase_d_bins["score_type"] == "calibrated")].copy()
    raw.sort_values("bin_index", inplace=True)
    cal.sort_values("bin_index", inplace=True)

    idx = raw["bin_index"].to_numpy()
    width = 0.38
    ax.bar(idx - width / 2.0, raw["abs_gap"], width=width, color="#ff7f0e", alpha=0.85, label="Raw")
    ax.bar(idx + width / 2.0, cal["abs_gap"], width=width, color="#1f77b4", alpha=0.85, label="Calibrated")

    ax.set_title("Calibration Absolute Gap by Probability Bin", fontsize=14, pad=12)
    ax.set_xlabel("Bin index", fontsize=12)
    ax.set_ylabel("|mean_pred - observed_rate|", fontsize=12)
    ax.grid(True, axis="y", linestyle="--", alpha=0.5)
    ax.legend(loc="upper right")

    fig.tight_layout()
    fig.savefig(output_path, dpi=300)
    plt.close(fig)


def _plot_latency_by_year(
    phase_g_by_year: pd.DataFrame,
    phase_g_summary: pd.DataFrame,
    latency_target_ms: float,
    output_path: Path,
) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))

    work = phase_g_by_year.copy()
    work["year"] = work["year"].astype(str)
    work.sort_values("year", inplace=True)

    years = work["year"].tolist()
    p95 = work["p95_total_ms"].astype(float).to_numpy()

    ax.bar(years, p95, color="#2ca02c", alpha=0.85, label="Yearly p95 latency")
    ax.axhline(latency_target_ms, color="#d62728", linestyle="--", linewidth=1.5, label=f"Target p95 {latency_target_ms:.1f} ms")

    gate_row = phase_g_summary[phase_g_summary["check"] == "latency_p95_total_ms"].iloc[0]
    measured_global_p95 = float(gate_row["value"])
    gate_threshold = float(gate_row["threshold"])
    ax.axhline(measured_global_p95, color="#1f77b4", linestyle=":", linewidth=1.5, label=f"Global p95 {measured_global_p95:.2f} ms")

    ax.set_title("Latency Stability by Year (p95)", fontsize=14, pad=12)
    ax.set_xlabel("Year", fontsize=12)
    ax.set_ylabel("Latency (ms)", fontsize=12)
    ax.grid(True, axis="y", linestyle="--", alpha=0.5)
    ax.legend(loc="upper left")

    ax.text(
        0.02,
        0.95,
        f"Phase-G gate threshold: {gate_threshold:.1f} ms",
        transform=ax.transAxes,
        fontsize=9,
        va="top",
    )

    fig.tight_layout()
    fig.savefig(output_path, dpi=300)
    plt.close(fig)


def _plot_latency_component_breakdown(phase_g_details: pd.DataFrame, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 6))

    component_order = [
        "parse_ms",
        "feature_ms",
        "matrix_ms",
        "inference_ms",
        "calibration_ms",
        "packaging_ms",
    ]

    subset = phase_g_details[phase_g_details["component"].isin(component_order)].copy()
    subset["component"] = pd.Categorical(subset["component"], categories=component_order, ordered=True)
    subset.sort_values("component", inplace=True)

    p50 = subset["p50_ms"].astype(float).to_numpy()
    p95 = subset["p95_ms"].astype(float).to_numpy()

    y = np.arange(len(subset))
    ax.barh(y - 0.18, p50, height=0.34, color="#17becf", alpha=0.85, label="p50")
    ax.barh(y + 0.18, p95, height=0.34, color="#9467bd", alpha=0.85, label="p95")

    ax.set_yticks(y)
    ax.set_yticklabels(subset["component"].tolist())
    ax.set_xlabel("Latency (ms)", fontsize=12)
    ax.set_title("Latency Component Breakdown", fontsize=14, pad=12)
    ax.grid(True, axis="x", linestyle="--", alpha=0.5)
    ax.legend(loc="lower right")

    fig.tight_layout()
    fig.savefig(output_path, dpi=300)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    inputs = _load_inputs(args.reports_dir, args.suffix)
    _validate_correctness(inputs)

    ext = args.save_format
    reports_dir = args.reports_dir

    outputs = {
        "reliability_curve": reports_dir / f"paper_fig2_calibration_reliability_{args.suffix}.{ext}",
        "reliability_gap": reports_dir / f"paper_fig3_calibration_gap_{args.suffix}.{ext}",
        "latency_by_year": reports_dir / f"paper_fig4_latency_by_year_p95_{args.suffix}.{ext}",
        "latency_components": reports_dir / f"paper_fig5_latency_components_{args.suffix}.{ext}",
    }

    _plot_reliability_curves(
        phase_d_bins=inputs["phase_d_bins"],
        phase_d_summary=inputs["phase_d_summary"],
        output_path=outputs["reliability_curve"],
    )
    _plot_reliability_gap_bars(
        phase_d_bins=inputs["phase_d_bins"],
        output_path=outputs["reliability_gap"],
    )
    _plot_latency_by_year(
        phase_g_by_year=inputs["phase_g_by_year"],
        phase_g_summary=inputs["phase_g_summary"],
        latency_target_ms=args.latency_target_ms,
        output_path=outputs["latency_by_year"],
    )
    _plot_latency_component_breakdown(
        phase_g_details=inputs["phase_g_details"],
        output_path=outputs["latency_components"],
    )

    print("=== TRUST DIAGNOSTICS PLOTS GENERATED ===")
    for name, path in outputs.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()
