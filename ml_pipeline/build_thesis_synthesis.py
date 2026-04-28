"""Build a thesis-ready synthesis report and correctness audit.

Outputs:
- thesis_synthesis_<suffix>.csv
- thesis_synthesis_<suffix>.md
- thesis_synthesis_checks_<suffix>.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build final thesis synthesis from existing phase artifacts"
    )
    parser.add_argument(
        "--reports-dir",
        type=Path,
        default=Path("data_lake/reports"),
        help="Directory containing report artifacts",
    )
    parser.add_argument(
        "--suffix",
        default="2022_2025_merged",
        help="Artifact suffix, e.g. 2022_2025_merged",
    )
    parser.add_argument(
        "--latency-operational-target-ms",
        type=float,
        default=10.0,
        help="Operational p95 target for deployment discussion",
    )
    return parser.parse_args()


def _safe_ratio(num: float, den: float) -> float:
    if den <= 0:
        return float("nan")
    return float(num / den)


def _to_bool_status(ok: bool) -> str:
    return "PASS" if ok else "FAIL"


def _load_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required {label}: {path}")
    frame = pd.read_csv(path)
    if frame.empty:
        raise ValueError(f"Required {label} is empty: {path}")
    return frame


def _comparator_metrics(frame: pd.DataFrame, model_name: str) -> dict[str, object]:
    scored = frame[frame["outcome_class"].astype(str).isin(["1", "0"])].copy()
    excluded = frame[~frame["outcome_class"].astype(str).isin(["1", "0"])].copy()

    tp = int((scored["outcome_class"].astype(str) == "1").sum())
    fp = int((scored["outcome_class"].astype(str) == "0").sum())
    actionable = int(len(frame))
    scored_n = int(len(scored))

    precision = _safe_ratio(tp, scored_n)
    scored_rate = _safe_ratio(scored_n, actionable)

    return {
        "model": model_name,
        "actionable": actionable,
        "scored": scored_n,
        "excluded": int(len(excluded)),
        "tp": tp,
        "fp": fp,
        "precision": precision,
        "scored_rate": scored_rate,
        "tp_per_actionable": _safe_ratio(tp, actionable),
    }


def _build_check_rows(
    reports_dir: Path,
    suffix: str,
    metrics_df: pd.DataFrame,
    phase_b_summary: pd.DataFrame,
    phase_c_sweep: pd.DataFrame,
    phase_d_summary: pd.DataFrame,
    phase_d_bins: pd.DataFrame,
    phase_g_summary: pd.DataFrame,
    phase_g_details: pd.DataFrame,
    phase_h_gate: pd.DataFrame,
    phase_f_parity: pd.DataFrame,
    phase_j_split: pd.DataFrame,
    moa_perm_summary: pd.DataFrame,
) -> pd.DataFrame:
    checks: list[dict[str, object]] = []

    def add_check(name: str, ok: bool, note: str) -> None:
        checks.append({"check": name, "status": _to_bool_status(ok), "note": note})

    # 1) Recompute SDE/ML constrained precision from comparators and compare with Phase B summary.
    sde_prec_recomputed = float(metrics_df.loc[metrics_df["model"] == "SDE Heuristic", "precision"].iloc[0])
    ml_prec_recomputed = float(metrics_df.loc[metrics_df["model"] == "Batch ML (Constrained)", "precision"].iloc[0])
    sde_prec_phase_b = float(phase_b_summary.iloc[0]["sde_precision"])
    ml_prec_phase_b = float(phase_b_summary.iloc[0]["ml_precision"])

    add_check(
        "phase_b_sde_precision_consistency",
        bool(np.isclose(sde_prec_recomputed, sde_prec_phase_b, atol=1e-12)),
        f"recomputed={sde_prec_recomputed:.12f}, phase_b={sde_prec_phase_b:.12f}",
    )
    add_check(
        "phase_b_ml_precision_consistency",
        bool(np.isclose(ml_prec_recomputed, ml_prec_phase_b, atol=1e-12)),
        f"recomputed={ml_prec_recomputed:.12f}, phase_b={ml_prec_phase_b:.12f}",
    )

    # 2) Validate best-threshold row selection in Phase C.
    sweep = phase_c_sweep.copy()
    floor_ok = sweep[(sweep["meets_precision_floor"] == 1) & (sweep["scored"] > 0)].copy()
    floor_ok.sort_values(
        by=["scored", "tp", "precision", "threshold"],
        ascending=[False, False, False, True],
        inplace=True,
    )
    best_row = floor_ok.iloc[0]
    best_threshold = float(best_row["threshold"])

    ml_best_row = metrics_df[metrics_df["model"] == "Batch ML (Best Threshold)"]
    ml_best_scored = int(ml_best_row["scored"].iloc[0])
    add_check(
        "phase_c_best_threshold_selected",
        bool(np.isclose(best_threshold, 0.05, atol=1e-12)),
        f"selected_threshold={best_threshold:.6f}",
    )
    add_check(
        "phase_c_best_scored_consistency",
        ml_best_scored == int(best_row["scored"]),
        f"best_comparator_scored={ml_best_scored}, phase_c_scored={int(best_row['scored'])}",
    )

    # 3) Validate Phase D reliability bins against summary (ECE and MCE).
    def metric_from_bins(score_type: str, value_col: str) -> float:
        subset = phase_d_bins[
            (phase_d_bins["scope"] == "overall") & (phase_d_bins["score_type"] == score_type)
        ].copy()
        if value_col == "ece":
            return float((subset["weight"] * subset["abs_gap"]).sum())
        return float(subset["abs_gap"].max())

    raw_ece_bins = metric_from_bins("raw", "ece")
    cal_ece_bins = metric_from_bins("calibrated", "ece")
    raw_mce_bins = metric_from_bins("raw", "mce")
    cal_mce_bins = metric_from_bins("calibrated", "mce")

    add_check(
        "phase_d_raw_ece_consistency",
        bool(np.isclose(raw_ece_bins, float(phase_d_summary.iloc[0]["raw_ece"]), atol=1e-12)),
        f"bins={raw_ece_bins:.12f}, summary={float(phase_d_summary.iloc[0]['raw_ece']):.12f}",
    )
    add_check(
        "phase_d_calibrated_ece_consistency",
        bool(np.isclose(cal_ece_bins, float(phase_d_summary.iloc[0]["calibrated_ece"]), atol=1e-12)),
        f"bins={cal_ece_bins:.12f}, summary={float(phase_d_summary.iloc[0]['calibrated_ece']):.12f}",
    )
    add_check(
        "phase_d_raw_mce_consistency",
        bool(np.isclose(raw_mce_bins, float(phase_d_summary.iloc[0]["raw_mce"]), atol=1e-12)),
        f"bins={raw_mce_bins:.12f}, summary={float(phase_d_summary.iloc[0]['raw_mce']):.12f}",
    )
    add_check(
        "phase_d_calibrated_mce_consistency",
        bool(np.isclose(cal_mce_bins, float(phase_d_summary.iloc[0]["calibrated_mce"]), atol=1e-12)),
        f"bins={cal_mce_bins:.12f}, summary={float(phase_d_summary.iloc[0]['calibrated_mce']):.12f}",
    )

    # 4) Validate Phase G p95 consistency.
    p95_from_summary = float(
        phase_g_summary[phase_g_summary["check"] == "latency_p95_total_ms"].iloc[0]["value"]
    )
    p95_from_details = float(
        phase_g_details[phase_g_details["component"] == "total_ms"].iloc[0]["p95_ms"]
    )
    add_check(
        "phase_g_p95_consistency",
        bool(np.isclose(p95_from_summary, p95_from_details, atol=1e-12)),
        f"summary={p95_from_summary:.12f}, details={p95_from_details:.12f}",
    )

    # 5) Gate checks propagated from previous phases.
    if "phase" in phase_h_gate.columns and (phase_h_gate["phase"] == "PHASE_H_DECISION").any():
        phase_h_status = str(
            phase_h_gate[phase_h_gate["phase"] == "PHASE_H_DECISION"].iloc[0]["status"]
        )
    else:
        phase_h_status = str(phase_h_gate.iloc[0]["status"])
    add_check(
        "integrated_gate_go",
        phase_h_status in {"GO", "PASS"},
        f"phase_h_status={phase_h_status}",
    )

    phase_f_status = str(phase_f_parity[phase_f_parity["check"] == "feature_parity_overall_gate"].iloc[0]["status"])
    add_check(
        "phase_f_parity_gate",
        phase_f_status == "PASS",
        f"phase_f_status={phase_f_status}",
    )

    # 6) Residual risk visibility: split-integrity currently flags fold-count mismatch.
    split_overall = str(
        phase_j_split[phase_j_split["check"] == "split_integrity_overall"].iloc[0]["status"]
    )
    add_check(
        "split_integrity_overall",
        split_overall == "PASS",
        f"phase_j_split_status={split_overall}",
    )

    # 7) Ensure new figure and explainability artifacts exist.
    expected_artifacts = [
        reports_dir / "paper_fig0_accuracy_over_time_real.pdf",
        reports_dir / "paper_fig1_kappa_over_time_real.pdf",
        reports_dir / f"paper_fig2_calibration_reliability_{suffix}.pdf",
        reports_dir / f"paper_fig3_calibration_gap_{suffix}.pdf",
        reports_dir / f"paper_fig4_latency_by_year_p95_{suffix}.pdf",
        reports_dir / f"paper_fig5_latency_components_{suffix}.pdf",
        reports_dir / "moa_temporal_permutation_summary.csv",
        reports_dir / "moa_temporal_permutation_global.csv",
        reports_dir / "moa_temporal_permutation_by_window.csv",
        reports_dir / "moa_temporal_permutation_heatmap.pdf",
    ]

    missing_artifacts = [str(path) for path in expected_artifacts if not path.exists()]
    add_check(
        "new_artifacts_presence",
        len(missing_artifacts) == 0,
        "missing=" + ";".join(missing_artifacts),
    )

    # 8) Consistency check for new MOA permutation summary.
    rows_used = int(moa_perm_summary.iloc[0]["rows_used"])
    unknown_rows = int(moa_perm_summary.iloc[0]["decoded_unknown_prediction_rows"])
    add_check(
        "moa_permutation_rows_nonzero",
        rows_used > 0,
        f"rows_used={rows_used}, unknown_rows={unknown_rows}",
    )

    return pd.DataFrame(checks)


def _build_synthesis_markdown(
    suffix: str,
    synthesis_df: pd.DataFrame,
    checks_df: pd.DataFrame,
    phase_d_summary: pd.DataFrame,
    phase_g_summary: pd.DataFrame,
    phase_h_gate: pd.DataFrame,
    moa_perm_summary: pd.DataFrame,
    latency_operational_target_ms: float,
) -> str:
    row_d = phase_d_summary.iloc[0]
    if "phase" in phase_h_gate.columns and (phase_h_gate["phase"] == "PHASE_H_DECISION").any():
        row_h = phase_h_gate[phase_h_gate["phase"] == "PHASE_H_DECISION"].iloc[0]
    else:
        row_h = phase_h_gate.iloc[0]
    row_moa = moa_perm_summary.iloc[0]

    p95_gate = float(phase_g_summary[phase_g_summary["check"] == "latency_p95_total_ms"].iloc[0]["value"])
    availability = float(phase_g_summary[phase_g_summary["check"] == "availability_pct"].iloc[0]["value"])

    pass_count = int((checks_df["status"] == "PASS").sum())
    fail_count = int((checks_df["status"] == "FAIL").sum())

    lines: list[str] = []
    lines.append(f"# Thesis Final Synthesis ({suffix})")
    lines.append("")
    lines.append("## Executive Summary")
    lines.append(
        f"- Integrated gate decision: **{row_h['status']}** ({row_h['note']})."
    )
    lines.append(
        f"- Batch constrained policy: precision={float(row_d['constrained_precision']):.6f}, recall={float(row_d['constrained_recall']):.6f}, positive_rate={float(row_d['constrained_positive_rate']):.6f}."
    )
    lines.append(
        f"- Latency: p95={p95_gate:.6f} ms, availability={availability:.2f}% (operational target p95<{latency_operational_target_ms:.1f} ms)."
    )
    lines.append(
        f"- MOA second explainability method: fidelity_accuracy={float(row_moa['fidelity_accuracy']):.6f}, fidelity_f1={float(row_moa['fidelity_f1']):.6f}."
    )
    lines.append("")

    lines.append("## Claim Matrix")
    lines.append("| Model | Actionable | Scored | Precision | TP | FP | Scored Rate |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for _, row in synthesis_df.iterrows():
        lines.append(
            f"| {row['model']} | {int(row['actionable'])} | {int(row['scored'])} | {float(row['precision']):.6f} | {int(row['tp'])} | {int(row['fp'])} | {float(row['scored_rate']):.6f} |"
        )
    lines.append("")

    lines.append("## Correctness Audit")
    lines.append(f"- Checks passed: {pass_count}")
    lines.append(f"- Checks failed: {fail_count}")
    lines.append("")
    lines.append("| Check | Status | Note |")
    lines.append("| --- | --- | --- |")
    for _, row in checks_df.iterrows():
        lines.append(f"| {row['check']} | {row['status']} | {row['note']} |")

    lines.append("## Interpretation Notes")
    lines.append("- Batch best-threshold policy maximizes scored recovery under precision-floor constraints and should be used for competitive-capability comparisons.")
    lines.append("- Batch constrained policy should be used for deployment-readiness claims (calibrated conservative actions).")
    lines.append("- MOA explainability is proxy-based: surrogate SHAP plus temporal permutation importance; treat as behavioral attribution, not internal-model attribution.")
    lines.append("- Split-integrity fold-count mismatch remains a visible residual risk in current artifacts and should be discussed explicitly in thesis limitations.")
    lines.append("")

    lines.append("## Produced by")
    lines.append("- ml_pipeline/build_thesis_synthesis.py")
    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    reports_dir = args.reports_dir
    suffix = args.suffix

    paths = {
        "heuristic_cmp": reports_dir / f"heuristic_comparator_{suffix}.csv",
        "ml_cmp": reports_dir / f"ml_comparator_{suffix}.csv",
        "ml_best_cmp": reports_dir / f"ml_comparator_best_threshold_{suffix}.csv",
        "moa_cmp": reports_dir / f"moa_comparator_{suffix}.csv",
        "phase_b_summary": reports_dir / f"sde_ml_comparison_summary_{suffix}.csv",
        "phase_c_sweep": reports_dir / f"threshold_frontier_{suffix}.csv",
        "phase_d_summary": reports_dir / f"calibration_policy_summary_{suffix}.csv",
        "phase_d_bins": reports_dir / f"calibration_reliability_bins_{suffix}.csv",
        "phase_f_parity": reports_dir / f"feature_parity_summary_{suffix}.csv",
        "phase_g_summary": reports_dir / f"live_latency_summary_{suffix}.csv",
        "phase_g_details": reports_dir / f"live_latency_details_{suffix}.csv",
        "phase_h_gate": reports_dir / f"integrated_gate_{suffix}.csv",
        "phase_j_split": reports_dir / f"split_integrity_summary_{suffix}.csv",
        "moa_perm_summary": reports_dir / "moa_temporal_permutation_summary.csv",
        "moa_arf_summary": reports_dir / f"moa_arf_summary_{suffix}.csv",
    }

    heuristic_cmp = _load_csv(paths["heuristic_cmp"], "heuristic comparator")
    ml_cmp = _load_csv(paths["ml_cmp"], "ml comparator")
    ml_best_cmp = _load_csv(paths["ml_best_cmp"], "ml best-threshold comparator")
    moa_cmp = _load_csv(paths["moa_cmp"], "moa comparator")

    phase_b_summary = _load_csv(paths["phase_b_summary"], "phase B summary")
    phase_c_sweep = _load_csv(paths["phase_c_sweep"], "phase C sweep")
    phase_d_summary = _load_csv(paths["phase_d_summary"], "phase D summary")
    phase_d_bins = _load_csv(paths["phase_d_bins"], "phase D bins")
    phase_f_parity = _load_csv(paths["phase_f_parity"], "phase F summary")
    phase_g_summary = _load_csv(paths["phase_g_summary"], "phase G summary")
    phase_g_details = _load_csv(paths["phase_g_details"], "phase G details")
    phase_h_gate = _load_csv(paths["phase_h_gate"], "phase H gate")
    phase_j_split = _load_csv(paths["phase_j_split"], "phase J split summary")
    moa_perm_summary = _load_csv(paths["moa_perm_summary"], "MOA permutation summary")
    moa_arf_summary = _load_csv(paths["moa_arf_summary"], "MOA ARF summary")

    synthesis_rows = [
        _comparator_metrics(heuristic_cmp, "SDE Heuristic"),
        _comparator_metrics(ml_cmp, "Batch ML (Constrained)"),
        _comparator_metrics(ml_best_cmp, "Batch ML (Best Threshold)"),
        _comparator_metrics(moa_cmp, "MOA Streaming"),
    ]

    synthesis_df = pd.DataFrame(synthesis_rows)

    # Add MOA stream-level diagnostics to the MOA row for side-by-side interpretation.
    moa_idx = synthesis_df["model"] == "MOA Streaming"
    moa_cpu_sec = float(moa_arf_summary.iloc[0]["final_evaluation time (cpu seconds)"])
    moa_instances = float(moa_arf_summary.iloc[0]["final_classified instances"])
    moa_avg_cpu_ms = _safe_ratio(moa_cpu_sec * 1000.0, moa_instances)
    synthesis_df.loc[moa_idx, "moa_final_accuracy_percent"] = float(
        moa_arf_summary.iloc[0]["final_classifications correct (percent)"]
    )
    synthesis_df.loc[moa_idx, "moa_final_kappa_percent"] = float(
        moa_arf_summary.iloc[0]["final_Kappa Statistic (percent)"]
    )
    synthesis_df.loc[moa_idx, "moa_avg_cpu_ms_per_instance"] = moa_avg_cpu_ms

    checks_df = _build_check_rows(
        reports_dir=reports_dir,
        suffix=suffix,
        metrics_df=synthesis_df,
        phase_b_summary=phase_b_summary,
        phase_c_sweep=phase_c_sweep,
        phase_d_summary=phase_d_summary,
        phase_d_bins=phase_d_bins,
        phase_g_summary=phase_g_summary,
        phase_g_details=phase_g_details,
        phase_h_gate=phase_h_gate,
        phase_f_parity=phase_f_parity,
        phase_j_split=phase_j_split,
        moa_perm_summary=moa_perm_summary,
    )

    output_csv = reports_dir / f"thesis_synthesis_{suffix}.csv"
    checks_csv = reports_dir / f"thesis_synthesis_checks_{suffix}.csv"
    output_md = reports_dir / f"thesis_synthesis_{suffix}.md"

    synthesis_df.to_csv(output_csv, index=False)
    checks_df.to_csv(checks_csv, index=False)

    markdown = _build_synthesis_markdown(
        suffix=suffix,
        synthesis_df=synthesis_df,
        checks_df=checks_df,
        phase_d_summary=phase_d_summary,
        phase_g_summary=phase_g_summary,
        phase_h_gate=phase_h_gate,
        moa_perm_summary=moa_perm_summary,
        latency_operational_target_ms=args.latency_operational_target_ms,
    )
    output_md.write_text(markdown, encoding="utf-8")

    print("=== THESIS SYNTHESIS GENERATED ===")
    print(f"synthesis csv : {output_csv}")
    print(f"checks csv    : {checks_csv}")
    print(f"report md     : {output_md}")
    print(f"checks pass   : {(checks_df['status'] == 'PASS').sum()}")
    print(f"checks fail   : {(checks_df['status'] == 'FAIL').sum()}")


if __name__ == "__main__":
    main()
