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

    # Optional deep-dive block from manually reviewed P1 taxonomy files.
    p1_run = next((run for run in runs if run.run_id == "p1"), None)
    if p1_run is not None:
        random_template_path = p1_run.reports_dir / "error_taxonomy_manual_template.csv"
        random_agg_path = p1_run.reports_dir / "error_taxonomy_manual_aggregate.csv"
        random_fig_path = p1_run.reports_dir / "error_taxonomy_manual_composition.png"

        strat_template_path = p1_run.reports_dir / "error_taxonomy_manual_template_stratified.csv"
        strat_prefill_path = p1_run.reports_dir / "error_taxonomy_manual_template_stratified_prefill.csv"
        strat_plausibility_path = p1_run.reports_dir / "error_taxonomy_manual_template_stratified_plausibility.csv"
        strat_diag_path = p1_run.reports_dir / "error_taxonomy_manual_template_stratified_diagnostics.csv"
        strat_agg_path = p1_run.reports_dir / "error_taxonomy_manual_stratified_aggregate.csv"
        strat_fig_path = p1_run.reports_dir / "error_taxonomy_manual_stratified_composition.png"

        pool_path = p1_run.reports_dir / "error_taxonomy_summary.csv"

        random_template_df = _load_optional_csv(random_template_path)
        random_agg_df = _load_optional_csv(random_agg_path)
        strat_template_df = _load_optional_csv(strat_template_path)
        strat_prefill_df = _load_optional_csv(strat_prefill_path)
        strat_plausibility_df = _load_optional_csv(strat_plausibility_path)
        strat_agg_df = _load_optional_csv(strat_agg_path)
        pool_df = _load_optional_csv(pool_path)

        if (random_template_df is not None and random_agg_df is not None) or (
            strat_template_df is not None and strat_agg_df is not None
        ):
            lines.append("### 14.5 Manual Taxonomy Deep-Dive (P1, Human-in-the-Loop)")
            lines.append("")
            lines.append(
                "This subsection summarizes two complementary sampling strategies on `P1 percent_conservative_v1`: "
                "an unstratified random sample (volume realism) and a stratified sample (balanced failure-mode diagnosis)."
            )
            lines.append("")

            if pool_df is not None and {"model", "excluded_no_match", "fp"}.issubset(pool_df.columns):
                pool = pool_df.copy()
                pool["pool_total"] = (
                    pd.to_numeric(pool["excluded_no_match"], errors="coerce").fillna(0)
                    + pd.to_numeric(pool["fp"], errors="coerce").fillna(0)
                )
                pmap = {
                    str(row["model"]): int(row["pool_total"])
                    for _, row in pool.iterrows()
                }
                p_total = int(sum(pmap.values()))
                if random_template_df is not None and p_total > 0:
                    batch_pool = int(pmap.get("Batch", 0))
                    expected_batch = batch_pool / p_total * len(random_template_df)
                    observed_batch = int((random_template_df["model"].astype(str) == "Batch").sum())
                    lines.append(
                        f"Random-sample volume note: pooled P1 errors were `SDE={pmap.get('SDE', 0)}`, "
                        f"`Batch={pmap.get('Batch', 0)}`, `MOA={pmap.get('MOA', 0)}` (total `{p_total}`). "
                        f"For `n={len(random_template_df)}` random rows, expected Batch rows were `{expected_batch:.2f}`, "
                        f"observed `{observed_batch}`."
                    )
                    lines.append("")

            if random_agg_df is not None and random_template_df is not None:
                lines.append("Random sample taxonomy mix:")
                lines.append("")
                lines.append("| Model | Manual Tag | Count | Model Sample Size | Within-Model Share |")
                lines.append("| --- | --- | ---: | ---: | ---: |")
                random_show = random_agg_df.sort_values(
                    by=["model", "count", "failure_type_manual"],
                    ascending=[True, False, True],
                )
                for _, row in random_show.iterrows():
                    lines.append(
                        f"| {row['model']} | {row['failure_type_manual']} | {int(row['count'])} | "
                        f"{int(row['model_total'])} | {_fmt(row['within_model_share'])} |"
                    )
                lines.append("")
                lines.append(f"- Random template: `{_rel(random_template_path, master_dir)}`")
                lines.append(f"- Random aggregate table: `{_rel(random_agg_path, master_dir)}`")
                if random_fig_path.exists():
                    rel_random_fig = _rel(random_fig_path, master_dir)
                    lines.append(f"- Random composition figure: `{rel_random_fig}`")
                    lines.append(f"![P1 Manual Taxonomy Composition (Random)]({rel_random_fig})")
                lines.append("")

            if strat_agg_df is not None and (strat_template_df is not None or strat_prefill_df is not None):
                sample_n = (
                    len(strat_template_df)
                    if strat_template_df is not None
                    else len(strat_prefill_df)  # type: ignore[arg-type]
                )
                lines.append("Stratified sample taxonomy mix (`10/10/10`):")
                lines.append("")
                lines.append("Note: this block is based on the stratified manual-review file (pre-annotated, then selectively corrected).")
                lines.append(
                    "Sample policy in current run: true-FP-only rows (`outcome_class=0`) with balanced "
                    "`SDE/Batch/MOA = 10/10/10`. Batch FP rows are sourced from the racewise comparator pool "
                    "to avoid undersampling from the tiny pretrain FP pool."
                )
                lines.append(
                    "Interpretation note: because this is stratified, percentages below describe within-model diagnostic composition, "
                    "not natural population frequency."
                )
                lines.append("")
                lines.append("| Model | Manual Tag | Count | Model Sample Size | Within-Model Share |")
                lines.append("| --- | --- | ---: | ---: | ---: |")
                strat_show = strat_agg_df.sort_values(
                    by=["model", "count", "failure_type_manual"],
                    ascending=[True, False, True],
                )
                for _, row in strat_show.iterrows():
                    lines.append(
                        f"| {row['model']} | {row['failure_type_manual']} | {int(row['count'])} | "
                        f"{int(row['model_total'])} | {_fmt(row['within_model_share'])} |"
                    )
                lines.append("")
                lines.append(f"- Stratified template: `{_rel(strat_template_path, master_dir)}`")
                if strat_prefill_path.exists():
                    lines.append(f"- Stratified prefill: `{_rel(strat_prefill_path, master_dir)}`")
                lines.append(f"- Stratified aggregate table: `{_rel(strat_agg_path, master_dir)}`")
                if strat_diag_path.exists():
                    lines.append(f"- Stratified diagnostics: `{_rel(strat_diag_path, master_dir)}`")
                lines.append(f"- Stratified sample size: `{sample_n}`")
                if strat_fig_path.exists():
                    rel_strat_fig = _rel(strat_fig_path, master_dir)
                    lines.append(f"- Stratified composition figure: `{rel_strat_fig}`")
                    lines.append(f"![P1 Manual Taxonomy Composition (Stratified)]({rel_strat_fig})")
                lines.append("")

            if strat_plausibility_df is not None and "plausibility_status" in strat_plausibility_df.columns:
                lines.append("### 14.6 Stratified Plausibility Validation (P1)")
                lines.append("")
                lines.append(
                    "This block validates each stratified sampled row against official race calendar, "
                    "driver race-entry/result presence, and pit-stop summary where available."
                )
                lines.append("")

                total_rows = len(strat_plausibility_df)
                lines.append(f"- Validated sample rows: `{total_rows}`")
                lines.append(f"- Source file: `{_rel(strat_plausibility_path, master_dir)}`")
                lines.append("")

                lines.append("Plausibility status counts:")
                lines.append("")
                lines.append("| Status | Count | Share |")
                lines.append("| --- | ---: | ---: |")
                status_counts = strat_plausibility_df["plausibility_status"].astype(str).value_counts()
                for status, count in status_counts.items():
                    share = (float(count) / float(total_rows)) if total_rows > 0 else None
                    lines.append(f"| {status} | {int(count)} | {_fmt(share, 4)} |")
                lines.append("")

                model_status = (
                    strat_plausibility_df.groupby(["model", "plausibility_status"])
                    .size()
                    .reset_index(name="count")
                    .sort_values(["model", "count", "plausibility_status"], ascending=[True, False, True])
                )
                if not model_status.empty:
                    lines.append("Model-by-status breakdown:")
                    lines.append("")
                    lines.append("| Model | Status | Count |")
                    lines.append("| --- | --- | ---: |")
                    for _, row in model_status.iterrows():
                        lines.append(f"| {row['model']} | {row['plausibility_status']} | {int(row['count'])} |")
                    lines.append("")

                invalid_rows = strat_plausibility_df[
                    strat_plausibility_df["plausibility_status"].astype(str) != "VALIDATED_WITH_PIT_SUMMARY"
                ]
                if not invalid_rows.empty:
                    lines.append("Flagged non-valid rows (for explicit manual discussion):")
                    lines.append("")
                    lines.append("| Model | Race | Driver | Suggestion Lap | Status | Official Pit Laps | Nearest Official Pit Distance |")
                    lines.append("| --- | --- | --- | ---: | --- | --- | ---: |")
                    show_cols = [
                        "model",
                        "race",
                        "driver",
                        "suggestion_lap",
                        "plausibility_status",
                        "official_pit_laps",
                        "suggestion_to_nearest_official_pit_distance",
                    ]
                    invalid_show = invalid_rows[show_cols].sort_values(
                        ["model", "race", "driver", "suggestion_lap"],
                        ascending=[True, True, True, True],
                    )
                    for _, row in invalid_show.iterrows():
                        distance = pd.to_numeric(
                            row.get("suggestion_to_nearest_official_pit_distance"),
                            errors="coerce",
                        )
                        distance_out = "N/A" if pd.isna(distance) else f"{float(distance):.1f}"
                        pit_laps = row.get("official_pit_laps")
                        pit_laps_out = "N/A" if pd.isna(pit_laps) else str(pit_laps)
                        lines.append(
                            f"| {row['model']} | {row['race']} | {row['driver']} | {int(row['suggestion_lap'])} | "
                            f"{row['plausibility_status']} | {pit_laps_out} | {distance_out} |"
                        )
                    lines.append("")

                if "validation_source" in strat_plausibility_df.columns:
                    lines.append(
                        "Validation source links are captured row-wise in the plausibility CSV (`validation_source` column) "
                        "for reproducible audit trails."
                    )
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
