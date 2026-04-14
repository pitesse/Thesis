"""run phase j comparator-invariance audit on merged comparator artifacts.

this script verifies the frozen fairness contract remains unchanged:
- actionable-only decision labels,
- fixed horizon h=2 for matched outcomes,
- one-to-one pit consumption behavior,
- consistent outcome and exclusion semantics.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from .comparator_heuristic import ACTIONABLE_LABELS, DEFAULT_HORIZON

DEFAULT_HEURISTIC = "data_lake/reports/heuristic_comparator_2022_2024_merged.csv"
DEFAULT_ML = "data_lake/reports/ml_comparator_2022_2024_merged.csv"

DEFAULT_SUMMARY_OUTPUT = "data_lake/reports/phase_j_comparator_invariance_summary_2022_2024_merged.csv"
DEFAULT_DETAILS_OUTPUT = "data_lake/reports/phase_j_comparator_invariance_details_2022_2024_merged.csv"
DEFAULT_REPORT_OUTPUT = "data_lake/reports/phase_j_comparator_invariance_report_2022_2024_merged.txt"

REQUIRED_COLUMNS = [
    "race",
    "driver",
    "suggestion_lap",
    "suggestion_label",
    "totalScore",
    "matched_pit_lap",
    "match_distance",
    "nearest_future_pit_lap",
    "nearest_future_pit_distance",
    "pit_in_window_before_consumption",
    "outcome_class",
    "exclusion_reason",
]
ALLOWED_OUTCOMES = {"0", "1", "EXCLUDED"}


def _status(ok: bool) -> str:
    return "PASS" if ok else "FAIL"


def _safe_ratio(num: float, den: float) -> float:
    if den <= 0:
        return float("nan")
    return float(num / den)


def _normalize_label(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text == "" or text.lower() == "nan" or text == "<NA>":
        return ""
    return text.upper().replace(" ", "_")


def _is_allowed_exclusion_reason(reason: str) -> bool:
    if reason == "":
        return True
    if reason == "NO_MATCH_WITHIN_HORIZON":
        return True
    if reason == "WEATHER_SURVIVAL_STOP":
        return True
    if reason == "MISSING_RESULT":
        return True
    if reason.startswith("UNRESOLVED_"):
        return True
    if reason.startswith("UNMAPPED_RESULT_"):
        return True
    return False


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"comparator file not found: {path}")
    frame = pd.read_csv(path)
    if frame.empty:
        raise ValueError(f"comparator file is empty: {path}")
    return frame


def _append_check(
    rows: list[dict[str, object]],
    check: str,
    ok: bool,
    value: float,
    threshold: float,
    note: str,
) -> None:
    rows.append(
        {
            "check": check,
            "status": _status(ok),
            "value": float(value),
            "threshold": float(threshold),
            "note": note,
        }
    )


def _audit_single_dataset(
    frame: pd.DataFrame,
    label: str,
    horizon: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    summary_rows: list[dict[str, object]] = []
    detail_rows: list[dict[str, object]] = []

    work = frame.copy()
    total_rows = int(len(work))

    for column in ["race", "driver", "suggestion_label", "outcome_class"]:
        work[column] = work[column].astype(str)

    work["match_distance"] = pd.to_numeric(work["match_distance"], errors="coerce")
    work["matched_pit_lap"] = pd.to_numeric(work["matched_pit_lap"], errors="coerce")

    label_norm = work["suggestion_label"].map(_normalize_label)
    invalid_label_mask = ~label_norm.isin(ACTIONABLE_LABELS)
    invalid_label_rows = int(invalid_label_mask.sum())
    invalid_labels = sorted(label_norm[invalid_label_mask].dropna().unique().tolist())
    _append_check(
        summary_rows,
        check=f"{label}_actionable_label_contract",
        ok=invalid_label_rows == 0,
        value=1.0 - _safe_ratio(invalid_label_rows, total_rows),
        threshold=1.0,
        note=f"invalid_rows={invalid_label_rows}, invalid_labels={';'.join(invalid_labels[:5])}",
    )

    outcome_norm = work["outcome_class"].map(_normalize_label)
    invalid_outcome_mask = ~outcome_norm.isin(ALLOWED_OUTCOMES)
    invalid_outcome_rows = int(invalid_outcome_mask.sum())
    _append_check(
        summary_rows,
        check=f"{label}_outcome_domain",
        ok=invalid_outcome_rows == 0,
        value=1.0 - _safe_ratio(invalid_outcome_rows, total_rows),
        threshold=1.0,
        note=f"invalid_rows={invalid_outcome_rows}",
    )

    matched = work[work["matched_pit_lap"].notna()].copy()
    matched_rows = int(len(matched))

    invalid_distance_mask = matched["match_distance"].isna() | (matched["match_distance"] < 0) | (matched["match_distance"] > horizon)
    invalid_distance_rows = int(invalid_distance_mask.sum())
    _append_check(
        summary_rows,
        check=f"{label}_match_distance_within_horizon",
        ok=invalid_distance_rows == 0,
        value=1.0 - _safe_ratio(invalid_distance_rows, max(1, matched_rows)),
        threshold=1.0,
        note=f"invalid_rows={invalid_distance_rows}, horizon={horizon}",
    )

    matched_group = (
        matched.groupby(["race", "driver", "matched_pit_lap"], dropna=False)
        .size()
        .reset_index(name="count")
    )
    # duplicate targets indicate comparator fairness drift from the one-to-one contract.
    duplicate_targets = matched_group[matched_group["count"] > 1]
    duplicate_target_count = int(len(duplicate_targets))
    _append_check(
        summary_rows,
        check=f"{label}_one_to_one_target_consumption",
        ok=duplicate_target_count == 0,
        value=1.0 - _safe_ratio(duplicate_target_count, max(1, matched_rows)),
        threshold=1.0,
        note=f"duplicate_target_rows={duplicate_target_count}",
    )

    matched_window_false = int((~matched["pit_in_window_before_consumption"].fillna(False).astype(bool)).sum())
    _append_check(
        summary_rows,
        check=f"{label}_matched_rows_window_flag",
        ok=matched_window_false == 0,
        value=1.0 - _safe_ratio(matched_window_false, max(1, matched_rows)),
        threshold=1.0,
        note=f"matched_rows_without_window_flag={matched_window_false}",
    )

    scored = work[outcome_norm.isin({"0", "1"})].copy()
    scored_reason_norm = scored["exclusion_reason"].map(_normalize_label)
    scored_with_reason = int((scored_reason_norm != "").sum())
    _append_check(
        summary_rows,
        check=f"{label}_scored_rows_have_empty_exclusion_reason",
        ok=scored_with_reason == 0,
        value=1.0 - _safe_ratio(scored_with_reason, max(1, int(len(scored)))),
        threshold=1.0,
        note=f"scored_rows_with_reason={scored_with_reason}",
    )

    excluded = work[outcome_norm == "EXCLUDED"].copy()
    exclusion_norm = excluded["exclusion_reason"].fillna("").astype(str).map(_normalize_label)
    invalid_exclusion_mask = ~exclusion_norm.map(_is_allowed_exclusion_reason)
    invalid_exclusion_rows = int(invalid_exclusion_mask.sum())
    invalid_exclusion_labels = sorted(exclusion_norm[invalid_exclusion_mask].dropna().unique().tolist())
    _append_check(
        summary_rows,
        check=f"{label}_exclusion_reason_domain",
        ok=invalid_exclusion_rows == 0,
        value=1.0 - _safe_ratio(invalid_exclusion_rows, max(1, int(len(excluded)))),
        threshold=1.0,
        note=(
            f"invalid_rows={invalid_exclusion_rows}, "
            f"sample_invalid={';'.join(invalid_exclusion_labels[:5])}"
        ),
    )

    detail_rows.extend(
        [
            {
                "section": label,
                "name": "rows_total",
                "value": total_rows,
                "note": "all comparator rows",
            },
            {
                "section": label,
                "name": "rows_scored",
                "value": int(len(scored)),
                "note": "outcome_class in {0,1}",
            },
            {
                "section": label,
                "name": "rows_excluded",
                "value": int(len(excluded)),
                "note": "outcome_class == EXCLUDED",
            },
            {
                "section": label,
                "name": "rows_matched",
                "value": matched_rows,
                "note": "matched_pit_lap not null",
            },
            {
                "section": label,
                "name": "max_match_distance",
                "value": float(matched["match_distance"].max()) if not matched.empty else float("nan"),
                "note": "expected <= horizon",
            },
            {
                "section": label,
                "name": "min_match_distance",
                "value": float(matched["match_distance"].min()) if not matched.empty else float("nan"),
                "note": "expected >= 0",
            },
        ]
    )

    return summary_rows, detail_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="run phase j comparator-invariance audit")
    parser.add_argument("--heuristic", default=DEFAULT_HEURISTIC, help="heuristic comparator csv")
    parser.add_argument("--ml", default=DEFAULT_ML, help="ml comparator csv")
    parser.add_argument("--horizon", type=int, default=DEFAULT_HORIZON, help="expected fixed comparator horizon")
    parser.add_argument("--summary-output", default=DEFAULT_SUMMARY_OUTPUT, help="summary output csv")
    parser.add_argument("--details-output", default=DEFAULT_DETAILS_OUTPUT, help="details output csv")
    parser.add_argument("--report-output", default=DEFAULT_REPORT_OUTPUT, help="text report output")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.horizon < 1:
        raise ValueError("--horizon must be >= 1")

    heuristic_path = Path(args.heuristic)
    ml_path = Path(args.ml)

    heuristic = _load_csv(heuristic_path)
    ml = _load_csv(ml_path)

    for label, frame in [("heuristic", heuristic), ("ml", ml)]:
        missing = sorted(set(REQUIRED_COLUMNS) - set(frame.columns))
        if missing:
            raise ValueError(f"{label} comparator missing required columns: {missing}")

    summary_rows: list[dict[str, object]] = []
    detail_rows: list[dict[str, object]] = []

    # keep schema order aligned so downstream reports can consume both artifacts interchangeably.
    schema_exact = list(heuristic.columns) == list(ml.columns)
    _append_check(
        summary_rows,
        check="comparator_schema_exact_match",
        ok=schema_exact,
        value=1.0 if schema_exact else 0.0,
        threshold=1.0,
        note="heuristic and ml comparator column order must match",
    )

    horizon_constant_ok = DEFAULT_HORIZON == args.horizon
    _append_check(
        summary_rows,
        check="fixed_horizon_contract_constant",
        ok=horizon_constant_ok,
        value=float(DEFAULT_HORIZON),
        threshold=float(args.horizon),
        note=f"comparator_heuristic.DEFAULT_HORIZON={DEFAULT_HORIZON}",
    )

    heur_summary, heur_details = _audit_single_dataset(heuristic, "heuristic", args.horizon)
    ml_summary, ml_details = _audit_single_dataset(ml, "ml", args.horizon)
    summary_rows.extend(heur_summary)
    summary_rows.extend(ml_summary)
    detail_rows.extend(heur_details)
    detail_rows.extend(ml_details)

    all_checks_pass = all(str(row["status"]).upper() == "PASS" for row in summary_rows)
    summary_rows.append(
        {
            "check": "phase_j_comparator_invariance_overall",
            "status": _status(all_checks_pass),
            "value": 1.0 if all_checks_pass else 0.0,
            "threshold": 1.0,
            "note": "all comparator-invariance checks must pass",
        }
    )

    summary_df = pd.DataFrame(summary_rows)
    details_df = pd.DataFrame(detail_rows)

    summary_output = Path(args.summary_output)
    details_output = Path(args.details_output)
    report_output = Path(args.report_output)
    for output in [summary_output, details_output, report_output]:
        output.parent.mkdir(parents=True, exist_ok=True)

    summary_df.to_csv(summary_output, index=False)
    details_df.to_csv(details_output, index=False)

    lines = [
        "=== PHASE J COMPARATOR-INVARIANCE AUDIT REPORT ===",
        f"heuristic comparator    : {heuristic_path}",
        f"ml comparator           : {ml_path}",
        f"expected horizon        : {args.horizon}",
        "",
        "check summary",
    ]

    for row in summary_rows:
        lines.append(f"- {row['check']}: {row['status']} ({row['value']})")
        lines.append(f"  note: {row['note']}")

    lines.extend(
        [
            "",
            f"overall gate            : {_status(all_checks_pass)}",
            "",
            "artifacts",
            f"summary csv             : {summary_output}",
            f"details csv             : {details_output}",
            f"report txt              : {report_output}",
        ]
    )

    report_output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
