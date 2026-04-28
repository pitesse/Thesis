"""Build the integrated go/no-go gate from merged evaluation artifacts.

this script consolidates evidence from:
- significance and uncertainty,
- threshold and lookahead diagnostics,
- calibration and policy behavior,
- training-serving parity,
- latency and availability feasibility.

it emits:
- per-check by-layer gate table,
- per-layer unified gate summary,
- integrated decision report (go, go_with_conditions, hold, no_go).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

DEFAULT_SIGNIFICANCE_SUMMARY = "data_lake/reports/significance_summary_2022_2025_merged.csv"
DEFAULT_SIGNIFICANCE_TESTS = "data_lake/reports/significance_tests_2022_2025_merged.csv"
DEFAULT_THRESHOLD_FRONTIER = "data_lake/reports/threshold_frontier_2022_2025_merged.csv"
DEFAULT_CALIBRATION_SUMMARY = "data_lake/reports/calibration_policy_summary_2022_2025_merged.csv"
DEFAULT_FEATURE_PARITY_SUMMARY = "data_lake/reports/feature_parity_summary_2022_2025_merged.csv"
DEFAULT_LATENCY_SUMMARY = "data_lake/reports/live_latency_summary_2022_2025_merged.csv"
DEFAULT_AVAILABILITY_SUMMARY = "data_lake/reports/live_availability_summary_2022_2025_merged.csv"

DEFAULT_UNIFIED_OUTPUT = "data_lake/reports/integrated_gate_2022_2025_merged.csv"
DEFAULT_BY_LAYER_OUTPUT = "data_lake/reports/integrated_gate_by_layer_2022_2025_merged.csv"
DEFAULT_REPORT_OUTPUT = "data_lake/reports/integrated_gate_report_2022_2025_merged.txt"

PHASE_SEQUENCE = ["B", "C", "D", "F", "G"]


def _status_pass_fail(value: bool) -> str:
    return "PASS" if value else "FAIL"


def _margin(value: float, threshold: float, direction: str) -> float:
    if direction == "gte":
        return float(value - threshold)
    if direction == "lte":
        return float(threshold - value)
    raise ValueError(f"unsupported direction: {direction}")


def _find_threshold_row(df: pd.DataFrame, threshold: float) -> pd.Series:
    idx = (df["threshold"] - float(threshold)).abs().idxmin()
    return df.loc[idx]


def _read_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    frame = pd.read_csv(path)
    if frame.empty:
        raise ValueError(f"{label} is empty: {path}")
    return frame


def _append_check(
    rows: list[dict[str, object]],
    phase: str,
    check: str,
    value: float,
    threshold: float,
    direction: str,
    status: str,
    artifact: str,
    note: str,
) -> None:
    rows.append(
        {
            "phase": phase,
            "check": check,
            "status": status,
            "value": float(value),
            "threshold": float(threshold),
            "direction": direction,
            "margin_to_threshold": _margin(float(value), float(threshold), direction),
            "artifact": artifact,
            "note": note,
        }
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="generate integrated gate outputs")
    parser.add_argument(
        "--significance-summary",
        "--phase-b-summary",
        dest="significance_summary",
        default=DEFAULT_SIGNIFICANCE_SUMMARY,
        help="significance summary csv",
    )
    parser.add_argument(
        "--significance-tests",
        "--phase-b-tests",
        dest="significance_tests",
        default=DEFAULT_SIGNIFICANCE_TESTS,
        help="significance tests csv",
    )
    parser.add_argument(
        "--threshold-frontier",
        "--phase-c-sweep",
        dest="threshold_frontier",
        default=DEFAULT_THRESHOLD_FRONTIER,
        help="threshold frontier csv",
    )
    parser.add_argument(
        "--calibration-summary",
        "--phase-d-summary",
        dest="calibration_summary",
        default=DEFAULT_CALIBRATION_SUMMARY,
        help="calibration summary csv",
    )
    parser.add_argument(
        "--feature-parity-summary",
        "--phase-f-summary",
        dest="feature_parity_summary",
        default=DEFAULT_FEATURE_PARITY_SUMMARY,
        help="feature parity summary csv",
    )
    parser.add_argument(
        "--latency-summary",
        "--phase-g-summary",
        dest="latency_summary",
        default=DEFAULT_LATENCY_SUMMARY,
        help="latency summary csv",
    )
    parser.add_argument(
        "--availability-summary",
        "--phase-g-availability",
        dest="availability_summary",
        default=DEFAULT_AVAILABILITY_SUMMARY,
        help="availability summary csv",
    )
    parser.add_argument("--alpha", type=float, default=0.05, help="significance p-value threshold")
    parser.add_argument("--reference-threshold", type=float, default=0.10, help="threshold-frontier operating threshold")
    parser.add_argument("--c-no-match-min", type=float, default=0.90, help="minimum no-match dominance rate at reference threshold")
    parser.add_argument("--d-precision-min", type=float, default=0.60, help="minimum constrained precision for calibration policy")
    parser.add_argument("--d-reachability-min", type=float, default=0.90, help="minimum reachability ratio for calibration policy")
    parser.add_argument("--d-fallback-max", type=float, default=0.10, help="maximum fallback rate for calibration policy")
    parser.add_argument("--f-parity-min", type=float, default=0.95, help="minimum offline-live parity score")
    parser.add_argument("--g-latency-pass-ms", type=float, default=500.0, help="strict pass latency threshold")
    parser.add_argument("--g-latency-hold-ms", type=float, default=800.0, help="conditional hold latency threshold")
    parser.add_argument("--g-availability-pass", type=float, default=99.0, help="strict pass availability threshold")
    parser.add_argument("--g-availability-hold", type=float, default=95.0, help="conditional hold availability threshold")
    parser.add_argument("--unified-output", default=DEFAULT_UNIFIED_OUTPUT, help="integrated gate summary csv")
    parser.add_argument("--by-layer-output", default=DEFAULT_BY_LAYER_OUTPUT, help="integrated gate by-layer csv")
    parser.add_argument("--report-output", default=DEFAULT_REPORT_OUTPUT, help="integrated gate report txt")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    b_summary_path = Path(args.significance_summary)
    b_tests_path = Path(args.significance_tests)
    c_sweep_path = Path(args.threshold_frontier)
    d_summary_path = Path(args.calibration_summary)
    f_summary_path = Path(args.feature_parity_summary)
    g_summary_path = Path(args.latency_summary)
    g_availability_path = Path(args.availability_summary)

    b_summary = _read_csv(b_summary_path, "phase b summary")
    b_tests = _read_csv(b_tests_path, "phase b tests")
    c_sweep = _read_csv(c_sweep_path, "phase c sweep")
    d_summary = _read_csv(d_summary_path, "phase d summary")
    f_summary = _read_csv(f_summary_path, "phase f summary")
    g_summary = _read_csv(g_summary_path, "phase g summary")
    g_availability = _read_csv(g_availability_path, "phase g availability")

    rows: list[dict[str, object]] = []

    # phase b
    sde = b_summary[b_summary["model"] == "SDE"].iloc[0]
    ml = b_summary[b_summary["model"] == "ML"].iloc[0]
    z_row = b_tests[b_tests["test"] == "two_proportion_z"].iloc[0]

    precision_delta = float(ml["precision"] - sde["precision"])
    p_value = float(z_row["p_value"])
    z_stat = float(z_row["statistic"])

    _append_check(
        rows,
        phase="B",
        check="ml_precision_delta_vs_sde",
        value=precision_delta,
        threshold=0.0,
        direction="gte",
        status=_status_pass_fail(precision_delta > 0.0),
        artifact=str(b_summary_path),
        note="positive precision delta required",
    )
    _append_check(
        rows,
        phase="B",
        check="two_proportion_pvalue",
        value=p_value,
        threshold=float(args.alpha),
        direction="lte",
        status=_status_pass_fail(p_value <= args.alpha),
        artifact=str(b_tests_path),
        note=f"z={z_stat:.6f}",
    )

    # phase c
    c_ref = _find_threshold_row(c_sweep, args.reference_threshold)
    c_precision = float(c_ref["precision"])
    c_no_match_rate = float(c_ref["no_match_rate"])
    sde_precision_floor = float(sde["precision"])

    _append_check(
        rows,
        phase="C",
        check="reference_threshold_precision_vs_sde_floor",
        value=c_precision,
        threshold=sde_precision_floor,
        direction="gte",
        status=_status_pass_fail(c_precision >= sde_precision_floor),
        artifact=str(c_sweep_path),
        note=f"threshold={float(c_ref['threshold']):.4f}",
    )
    _append_check(
        rows,
        phase="C",
        check="lookahead_no_match_dominance_rate",
        value=c_no_match_rate,
        threshold=float(args.c_no_match_min),
        direction="gte",
        status=_status_pass_fail(c_no_match_rate >= args.c_no_match_min),
        artifact=str(c_sweep_path),
        note="higher values indicate horizon-lag dominates exclusions",
    )

    # phase d
    d = d_summary.iloc[0]
    d_precision = float(d["constrained_precision"])
    d_reachability = float(d["winner_mean_precision_floor_reachable_ratio"])
    d_fallback_rate = float(d["winner_fallback_rate"])

    _append_check(
        rows,
        phase="D",
        check="constrained_precision_min",
        value=d_precision,
        threshold=float(args.d_precision_min),
        direction="gte",
        status=_status_pass_fail(d_precision >= args.d_precision_min),
        artifact=str(d_summary_path),
        note="policy precision gate",
    )
    _append_check(
        rows,
        phase="D",
        check="precision_floor_reachability_ratio",
        value=d_reachability,
        threshold=float(args.d_reachability_min),
        direction="gte",
        status=_status_pass_fail(d_reachability >= args.d_reachability_min),
        artifact=str(d_summary_path),
        note="fraction of candidate thresholds reaching floor",
    )
    _append_check(
        rows,
        phase="D",
        check="fallback_rate_max",
        value=d_fallback_rate,
        threshold=float(args.d_fallback_max),
        direction="lte",
        status=_status_pass_fail(d_fallback_rate <= args.d_fallback_max),
        artifact=str(d_summary_path),
        note="fallbacks should remain bounded",
    )

    # phase f
    f_overall = f_summary[f_summary["check"] == "feature_parity_overall_gate"].iloc[0]
    f_parity = f_summary[f_summary["check"] == "offline_live_parity_overall"].iloc[0]

    f_overall_pass = str(f_overall["status"]).upper() == "PASS"
    f_parity_value = float(f_parity["value"])
    f_parity_pass = str(f_parity["status"]).upper() == "PASS" and f_parity_value >= args.f_parity_min

    _append_check(
        rows,
        phase="F",
        check="feature_parity_overall_gate",
        value=float(f_overall["value"]),
        threshold=1.0,
        direction="gte",
        status=_status_pass_fail(f_overall_pass),
        artifact=str(f_summary_path),
        note="schema, PIT, and parity checks",
    )
    _append_check(
        rows,
        phase="F",
        check="offline_live_parity_score",
        value=f_parity_value,
        threshold=float(args.f_parity_min),
        direction="gte",
        status=_status_pass_fail(f_parity_pass),
        artifact=str(f_summary_path),
        note="sampled parity gate",
    )

    # phase g with pass/hold/fail semantics
    g_latency = g_summary[g_summary["check"] == "latency_p95_total_ms"].iloc[0]
    g_avail = g_summary[g_summary["check"] == "availability_pct"].iloc[0]

    g_latency_value = float(g_latency["value"])
    g_avail_value = float(g_avail["value"])

    if g_latency_value < args.g_latency_pass_ms:
        latency_status = "PASS"
    elif g_latency_value < args.g_latency_hold_ms:
        latency_status = "HOLD"
    else:
        latency_status = "FAIL"

    if g_avail_value >= args.g_availability_pass:
        avail_status = "PASS"
    elif g_avail_value >= args.g_availability_hold:
        avail_status = "HOLD"
    else:
        avail_status = "FAIL"

    _append_check(
        rows,
        phase="G",
        check="latency_p95_total_ms",
        value=g_latency_value,
        threshold=float(args.g_latency_pass_ms),
        direction="lte",
        status=latency_status,
        artifact=str(g_summary_path),
        note=f"hold_if<{args.g_latency_hold_ms}",
    )
    _append_check(
        rows,
        phase="G",
        check="availability_pct",
        value=g_avail_value,
        threshold=float(args.g_availability_pass),
        direction="gte",
        status=avail_status,
        artifact=str(g_summary_path),
        note=f"hold_if>={args.g_availability_hold}",
    )

    failed_events_row = g_availability[g_availability["metric"] == "failed_events"]
    failed_events = float(failed_events_row.iloc[0]["value"]) if not failed_events_row.empty else float("nan")
    _append_check(
        rows,
        phase="G",
        check="failed_events_count",
        value=failed_events,
        threshold=0.0,
        direction="lte",
        status=_status_pass_fail(failed_events <= 0.0),
        artifact=str(g_availability_path),
        note="observed replay failures",
    )

    by_layer_df = pd.DataFrame(rows)

    phase_rows: list[dict[str, object]] = []
    phase_status_map: dict[str, str] = {}
    for phase in PHASE_SEQUENCE:
        block = by_layer_df[by_layer_df["phase"] == phase]
        statuses = block["status"].tolist()
        # precedence is fail > hold > pass so risk signals are never hidden by passes.
        if any(status == "FAIL" for status in statuses):
            phase_status = "FAIL"
        elif any(status == "HOLD" for status in statuses):
            phase_status = "HOLD"
        else:
            phase_status = "PASS"
        phase_status_map[phase] = phase_status

        phase_rows.append(
            {
                "phase": phase,
                "status": phase_status,
                "checks_passed": int((block["status"] == "PASS").sum()),
                "checks_total": int(len(block)),
                "checks_hold": int((block["status"] == "HOLD").sum()),
                "checks_failed": int((block["status"] == "FAIL").sum()),
                "note": "integrated from by-layer criteria",
            }
        )

    core_phases = ["B", "C", "D", "F"]
    failed_core_phases = [
        phase for phase in core_phases if phase_status_map[phase] == "FAIL"
    ]
    core_fail = len(failed_core_phases) > 0
    core_pass = all(phase_status_map[phase] == "PASS" for phase in core_phases)
    g_state = phase_status_map["G"]

    # core validity phases gate deployment first, then phase G controls operational posture.
    if core_fail:
        decision = "NO_GO"
        confidence = "LOW"
        decision_note = f"core validity gate failed in {'/'.join(failed_core_phases)}"
    elif core_pass and g_state == "PASS":
        decision = "GO"
        confidence = "HIGH"
        decision_note = "all integrated gates passed"
    elif core_pass and g_state == "HOLD":
        decision = "GO_WITH_CONDITIONS"
        confidence = "MEDIUM"
        decision_note = "core validity passed, operational gate in conditional band"
    else:
        decision = "HOLD"
        confidence = "MEDIUM"
        decision_note = "requires remediation before stronger deployment claims"

    phase_rows.append(
        {
            "phase": "PHASE_H_DECISION",
            "status": decision,
            "checks_passed": int((by_layer_df["status"] == "PASS").sum()),
            "checks_total": int(len(by_layer_df)),
            "checks_hold": int((by_layer_df["status"] == "HOLD").sum()),
            "checks_failed": int((by_layer_df["status"] == "FAIL").sum()),
            "note": f"confidence={confidence}; {decision_note}",
        }
    )

    unified_df = pd.DataFrame(phase_rows)

    unified_output = Path(args.unified_output)
    by_layer_output = Path(args.by_layer_output)
    report_output = Path(args.report_output)
    for output in [unified_output, by_layer_output, report_output]:
        output.parent.mkdir(parents=True, exist_ok=True)

    unified_df.to_csv(unified_output, index=False)
    by_layer_df.to_csv(by_layer_output, index=False)

    timestamp = datetime.now(timezone.utc).isoformat()
    report_lines = [
        "=== INTEGRATED GATE REPORT ===",
        f"timestamp                 : {timestamp}",
        f"decision                  : {decision}",
        f"confidence                : {confidence}",
        f"decision_note             : {decision_note}",
        "",
        "layer status",
    ]

    for phase in PHASE_SEQUENCE:
        report_lines.append(f"- layer {phase}: {phase_status_map[phase]}")

    report_lines.extend(
        [
            "",
            "key metrics",
            f"- significance precision delta (ml-sde): {precision_delta:.6f}",
            f"- significance p-value (two proportion): {p_value:.6e}",
            f"- threshold frontier {float(c_ref['threshold']):.4f} precision: {c_precision:.6f}",
            f"- threshold no-match dominance rate: {c_no_match_rate:.6f}",
            f"- calibration constrained precision: {d_precision:.6f}",
            f"- calibration reachability ratio: {d_reachability:.6f}",
            f"- calibration fallback rate: {d_fallback_rate:.6f}",
            f"- feature parity score: {f_parity_value:.6f}",
            f"- latency p95 (ms): {g_latency_value:.6f}",
            f"- availability (%): {g_avail_value:.6f}",
            f"- failed events: {failed_events:.0f}",
            "",
            "artifacts",
            f"- unified csv             : {unified_output}",
            f"- by-layer csv            : {by_layer_output}",
            f"- report txt              : {report_output}",
        ]
    )

    report_output.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    print("\n".join(report_lines))


if __name__ == "__main__":
    main()
