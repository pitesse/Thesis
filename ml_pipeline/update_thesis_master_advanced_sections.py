"""Auto-build sections 13 and 14 in thesis master markdown from advanced artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

try:
    from lib.run_catalog import RUN_SET_ALL_FAMILIES, RunSpec, list_runs
except ImportError:
    from ml_pipeline.lib.run_catalog import (  # type: ignore
        RUN_SET_ALL_FAMILIES,
        RunSpec,
        list_runs,
    )

BEGIN_13 = "<!-- BEGIN AUTO_SECTION_13 -->"
END_13 = "<!-- END AUTO_SECTION_13 -->"
BEGIN_14 = "<!-- BEGIN AUTO_SECTION_14 -->"
END_14 = "<!-- END AUTO_SECTION_14 -->"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update thesis master markdown with advanced auto sections")
    parser.add_argument("--reports-root", type=Path, default=Path("data_lake/reports"))
    parser.add_argument("--master-md", type=Path, required=True)
    parser.add_argument("--run-set", default=RUN_SET_ALL_FAMILIES)
    return parser.parse_args()


def _load_optional_csv(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    frame = pd.read_csv(path)
    if frame.empty:
        return None
    return frame


def _fmt(value: float | int | None, digits: int = 6) -> str:
    if value is None:
        return "N/A"
    try:
        fval = float(value)
    except (TypeError, ValueError):
        return "N/A"
    if pd.isna(fval):
        return "N/A"
    return f"{fval:.{digits}f}"


def _rel(path: Path, base_dir: Path) -> str:
    try:
        return str(path.resolve().relative_to(base_dir.resolve()))
    except Exception:
        return str(path)


def _replace_or_append(text: str, begin: str, end: str, block: str) -> str:
    if begin in text and end in text:
        start = text.index(begin)
        finish = text.index(end, start) + len(end)
        return text[:start] + block + text[finish:]

    if not text.endswith("\n"):
        text += "\n"
    return text + "\n" + block + "\n"


def _extract_advanced_row(summary_df: pd.DataFrame, metric_group: str, protocol: str, score_type: str, metric: str) -> float | None:
    subset = summary_df[
        (summary_df["metric_group"].astype(str) == metric_group)
        & (summary_df["protocol"].astype(str) == protocol)
        & (summary_df["score_type"].astype(str) == score_type)
    ]
    if subset.empty:
        return None
    value = pd.to_numeric(subset.iloc[0].get(metric), errors="coerce")
    if pd.isna(value):
        return None
    return float(value)


def _extract_temporal_means(summary_df: pd.DataFrame, model_name: str) -> tuple[float | None, float | None]:
    subset = summary_df[
        (summary_df["metric_group"].astype(str) == "temporal_drift")
        & (summary_df["protocol"].astype(str) == model_name)
    ]
    if subset.empty:
        return None, None
    row = subset.iloc[0]
    p = pd.to_numeric(row.get("mean_precision"), errors="coerce")
    c = pd.to_numeric(row.get("mean_coverage"), errors="coerce")
    return (None if pd.isna(p) else float(p), None if pd.isna(c) else float(c))


def _section_13(
    runs: list[RunSpec],
    reports_root: Path,
    master_dir: Path,
) -> str:
    lines: list[str] = []
    lines.append(BEGIN_13)
    lines.append("## 13) Advanced Statistical Diagnostics")
    lines.append("")
    lines.append("This section is auto-generated from artifact-only scripts (no retraining).")
    lines.append("It summarizes calibration reliability, PR-Gain, decision-curve utility, AP-delta uncertainty, and per-race temporal drift across all completed families.")
    lines.append("")

    bootstrap = _load_optional_csv(reports_root / "ap_delta_bootstrap_cluster_summary.csv")
    if bootstrap is not None:
        lines.append("### 13.1 AP Delta Uncertainty (Race-Cluster Bootstrap)")
        lines.append("")
        lines.append("| Run | Reference | Protocol | Score | AP Delta | 95% CI Low | 95% CI High | Point AP (Run) | Point AP (Ref) |")
        lines.append("| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |")
        for _, row in bootstrap.sort_values(["run_order", "protocol", "score_type"]).iterrows():
            lines.append(
                f"| {row['run_label']} | {row['reference_run_label']} | {row['protocol']} | {row['score_type']} | {_fmt(row['ap_delta_point'])} | {_fmt(row['ap_delta_ci95_low'])} | {_fmt(row['ap_delta_ci95_high'])} | {_fmt(row['ap_run_point'])} | {_fmt(row['ap_ref_point'])} |"
            )
        lines.append("")
        for fig_name in ("ap_delta_forestplot.png", "ap_delta_forestplot.pdf"):
            fig_path = reports_root / fig_name
            if fig_path.exists():
                lines.append(f"- AP-delta forest plot: `{_rel(fig_path, master_dir)}`")
        lines.append("")

    lines.append("### 13.2 Per-Run Advanced Metric Snapshot")
    lines.append("")
    lines.append("| Run | Pretrain Calibrated ECE | Racewise Calibrated ECE | MOA Surrogate ECE | Mean Precision (SDE/Batch/MOA) | Mean Coverage (SDE/Batch/MOA) |")
    lines.append("| --- | ---: | ---: | ---: | --- | --- |")

    for run in runs:
        summary_path = run.reports_dir / f"advanced_metrics_summary_{run.suffix}.csv"
        summary_df = _load_optional_csv(summary_path)
        if summary_df is None:
            continue

        pre_ece = _extract_advanced_row(summary_df, "batch_calibration", "ml_pretrain", "calibrated_proba", "ece")
        race_ece = _extract_advanced_row(summary_df, "batch_calibration", "ml_racewise", "calibrated_proba", "ece")
        moa_ece = _extract_advanced_row(summary_df, "moa_surrogate_calibration", "moa_surrogate", "y_score", "ece")

        p_sde, c_sde = _extract_temporal_means(summary_df, "SDE")
        p_batch, c_batch = _extract_temporal_means(summary_df, "Batch ML")
        p_moa, c_moa = _extract_temporal_means(summary_df, "MOA")

        lines.append(
            f"| {run.label} | {_fmt(pre_ece)} | {_fmt(race_ece)} | {_fmt(moa_ece)} | {_fmt(p_sde, 4)}/{_fmt(p_batch, 4)}/{_fmt(p_moa, 4)} | {_fmt(c_sde, 4)}/{_fmt(c_batch, 4)}/{_fmt(c_moa, 4)} |"
        )

    lines.append("")
    lines.append("### 13.3 Artifact Links by Run")
    lines.append("")
    for run in runs:
        if not run.reports_dir.exists():
            continue
        lines.append(f"- {run.label} (`{_rel(run.reports_dir, master_dir)}`):")
        for name in (
            "advanced_calibration_batch.png",
            "advanced_calibration_moa_surrogate.png",
            "advanced_pr_gain_batch.png",
            "advanced_decision_curve_batch.png",
            "advanced_decision_curve_moa_surrogate.png",
            "advanced_temporal_drift_by_race.png",
        ):
            path = run.reports_dir / name
            if path.exists():
                lines.append(f"  - `{_rel(path, master_dir)}`")

    lines.append("")
    lines.append("### 13.4 Key Figure Embeds (PNG)")
    lines.append("")
    lines.append("The following figures are embedded directly for quick review.")
    lines.append("")
    for run in runs:
        if not run.reports_dir.exists():
            continue
        fig_paths = [
            run.reports_dir / "advanced_calibration_batch.png",
            run.reports_dir / "advanced_pr_gain_batch.png",
            run.reports_dir / "advanced_decision_curve_batch.png",
            run.reports_dir / "advanced_temporal_drift_by_race.png",
        ]
        existing = [path for path in fig_paths if path.exists()]
        if not existing:
            continue
        lines.append(f"#### {run.label}")
        for fig in existing:
            rel = _rel(fig, master_dir)
            lines.append(f"![{run.label} - {fig.stem}]({rel})")
        lines.append("")
    lines.append("")
    lines.append(END_13)
    return "\n".join(lines)


def _section_14(
    runs: list[RunSpec],
    master_dir: Path,
) -> str:
    lines: list[str] = []
    lines.append(BEGIN_14)
    lines.append("## 14) Cross-Paradigm Error Analysis")
    lines.append("")
    lines.append("This section is auto-generated from decision-level comparator rows (`H=2`) with SDE/Batch/MOA alignment on `(race, driver, suggestion_lap)`.")
    lines.append("")

    lines.append("### 14.1 Run-Level Error Summary")
    lines.append("")
    lines.append("| Run | Model | Actionable | Scored | TP | FP | Precision | FP Rate (Scored) | Excluded NO_MATCH |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")

    for run in runs:
        summary = _load_optional_csv(run.reports_dir / "error_taxonomy_summary.csv")
        if summary is None:
            continue
        for _, row in summary.iterrows():
            lines.append(
                f"| {run.label} | {row['model']} | {int(row['actionable_total'])} | {int(row['scored_total'])} | {int(row['tp'])} | {int(row['fp'])} | {_fmt(row['precision'])} | {_fmt(row['fp_rate_within_scored'])} | {int(row['excluded_no_match'])} |"
            )

    lines.append("")
    lines.append("### 14.2 FP Consensus Overlap Highlights")
    lines.append("")
    lines.append("| Run | SDE only | Batch only | MOA only | SDE+Batch only | SDE+MOA only | Batch+MOA only | All three |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")

    subset_order = [
        "SDE_only",
        "Batch_only",
        "MOA_only",
        "SDE_and_Batch_only",
        "SDE_and_MOA_only",
        "Batch_and_MOA_only",
        "All_three",
    ]

    for run in runs:
        overlap = _load_optional_csv(run.reports_dir / "error_consensus_overlap_counts.csv")
        if overlap is None:
            continue
        counts = {str(r["subset"]): int(r["count"]) for _, r in overlap.iterrows()}
        vals = [counts.get(k, 0) for k in subset_order]
        lines.append(
            f"| {run.label} | {vals[0]} | {vals[1]} | {vals[2]} | {vals[3]} | {vals[4]} | {vals[5]} | {vals[6]} |"
        )

    lines.append("")
    lines.append("### 14.3 Error-Taxonomy Artifacts")
    lines.append("")
    for run in runs:
        if not run.reports_dir.exists():
            continue
        lines.append(f"- {run.label} (`{_rel(run.reports_dir, master_dir)}`):")
        for name in (
            "error_consensus_upset.png",
            "error_near_miss_distribution.png",
            "error_batch_fp_tp_feature_profiles.png",
            "error_taxonomy_manual_template.csv",
            "error_taxonomy_summary.csv",
            "error_consensus_overlap_counts.csv",
            "error_near_miss_distribution.csv",
        ):
            path = run.reports_dir / name
            if path.exists():
                lines.append(f"  - `{_rel(path, master_dir)}`")

    lines.append("")
    lines.append("### 14.4 Error Figure Embeds (PNG)")
    lines.append("")
    for run in runs:
        if not run.reports_dir.exists():
            continue
        fig_paths = [
            run.reports_dir / "error_consensus_upset.png",
            run.reports_dir / "error_near_miss_distribution.png",
            run.reports_dir / "error_batch_fp_tp_feature_profiles.png",
        ]
        existing = [path for path in fig_paths if path.exists()]
        if not existing:
            continue
        lines.append(f"#### {run.label}")
        for fig in existing:
            rel = _rel(fig, master_dir)
            lines.append(f"![{run.label} - {fig.stem}]({rel})")
        lines.append("")
    lines.append("")
    lines.append(END_14)
    return "\n".join(lines)


def main() -> None:
    args = parse_args()

    reports_root = args.reports_root.resolve()
    master_path = args.master_md.resolve()
    master_dir = master_path.parent
    runs = list_runs(reports_root, args.run_set)

    if not master_path.exists():
        raise FileNotFoundError(f"master markdown not found: {master_path}")
    text = master_path.read_text(encoding="utf-8")

    block_13 = _section_13(runs, reports_root, master_dir)
    block_14 = _section_14(runs, master_dir)

    updated = _replace_or_append(text, BEGIN_13, END_13, block_13)
    updated = _replace_or_append(updated, BEGIN_14, END_14, block_14)

    master_path.write_text(updated, encoding="utf-8")

    print("=== THESIS MASTER ADVANCED SECTIONS UPDATED ===")
    print(f"master markdown          : {master_path}")
    print(f"runs covered             : {len(runs)}")
    print(f"section13 marker present : {BEGIN_13 in updated and END_13 in updated}")
    print(f"section14 marker present : {BEGIN_14 in updated and END_14 in updated}")


if __name__ == "__main__":
    main()
