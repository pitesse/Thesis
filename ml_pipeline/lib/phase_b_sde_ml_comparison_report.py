"""generate a dedicated, meeting-ready SDE vs ML comparison report.

this script consumes already-generated phase b and comparator artifacts and emits:
- a markdown report for presentation,
- a compact summary csv,
- a by-year comparison csv.

method guardrails:
- comparator semantics are inherited from upstream artifacts (fixed H=2, actionable-only, one-to-one matching),
- inferential claims reuse phase b tests (two-proportion z as primary, overlap mcnemar as sensitivity),
- descriptive diagnostics are separated from inferential results.
"""

from __future__ import annotations

import argparse
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

try:
    from .phase_b_significance import KEY_COLUMNS, SCORED_CLASSES
except ImportError:
    from phase_b_significance import KEY_COLUMNS, SCORED_CLASSES

DEFAULT_PHASE_B_SUMMARY = "data_lake/reports/phase_b_significance_summary_2022_2025_merged.csv"
DEFAULT_PHASE_B_TESTS = "data_lake/reports/phase_b_significance_tests_2022_2025_merged.csv"
DEFAULT_HEURISTIC_COMPARATOR = "data_lake/reports/heuristic_comparator_2022_2025_merged.csv"
DEFAULT_ML_COMPARATOR = "data_lake/reports/ml_comparator_2022_2025_merged.csv"
DEFAULT_OUTPUT_MD = "data_lake/reports/phase_b_sde_ml_comparison_2022_2025_merged.md"
DEFAULT_OUTPUT_SUMMARY_CSV = "data_lake/reports/phase_b_sde_ml_comparison_summary_2022_2025_merged.csv"
DEFAULT_OUTPUT_BY_YEAR_CSV = "data_lake/reports/phase_b_sde_ml_comparison_by_year_2022_2025_merged.csv"


def _load_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    frame = pd.read_csv(path)
    if frame.empty:
        raise ValueError(f"{label} is empty: {path}")
    return frame


def _load_phase_b_summary(path: Path) -> pd.DataFrame:
    frame = _load_csv(path, "phase b summary")
    required = {
        "model",
        "scored",
        "tp",
        "fp",
        "precision",
        "wilson_ci_low",
        "wilson_ci_high",
    }
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"phase b summary missing columns: {missing}")

    expected_models = {"SDE", "ML", "ML_minus_SDE"}
    found_models = set(frame["model"].astype(str))
    if not expected_models.issubset(found_models):
        raise ValueError(
            "phase b summary missing required model rows: "
            f"expected {sorted(expected_models)}, found {sorted(found_models)}"
        )

    return frame


def _load_phase_b_tests(path: Path) -> pd.DataFrame:
    frame = _load_csv(path, "phase b tests")
    required = {"test", "pairing_scope", "statistic", "p_value", "note"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"phase b tests missing columns: {missing}")

    expected_tests = {"two_proportion_z", "mcnemar_cc", "mcnemar_exact"}
    found_tests = set(frame["test"].astype(str))
    if not expected_tests.issubset(found_tests):
        raise ValueError(
            "phase b tests missing required rows: "
            f"expected {sorted(expected_tests)}, found {sorted(found_tests)}"
        )

    return frame


def _load_comparator(path: Path, label: str) -> pd.DataFrame:
    frame = _load_csv(path, label)

    required = set(KEY_COLUMNS + ["outcome_class", "exclusion_reason"])
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{label} missing columns: {missing}")

    work = frame.copy()
    work["race"] = work["race"].astype(str)
    work["driver"] = work["driver"].astype(str)
    work["suggestion_lap"] = pd.to_numeric(work["suggestion_lap"], errors="coerce")
    work = work[work["suggestion_lap"].notna()].copy()
    work["suggestion_lap"] = work["suggestion_lap"].astype(int)
    work["outcome_class"] = work["outcome_class"].astype(str)
    work["exclusion_reason"] = work["exclusion_reason"].fillna("").astype(str)
    return work


def _extract_year(series: pd.Series) -> pd.Series:
    years = series.str.extract(r"^(\d{4})\s::\s", expand=False)
    return pd.to_numeric(years, errors="coerce").astype("Int64")


def _scored_rows(frame: pd.DataFrame) -> pd.DataFrame:
    scored = frame[frame["outcome_class"].isin(SCORED_CLASSES)].copy()
    scored["outcome_bin"] = (scored["outcome_class"] == "1").astype(int)
    return scored


def _comparator_metrics(frame: pd.DataFrame) -> dict[str, float | int]:
    scored = _scored_rows(frame)
    actionable = int(len(frame))
    scored_n = int(len(scored))
    excluded_n = actionable - scored_n
    tp = int((scored["outcome_bin"] == 1).sum())
    fp = int((scored["outcome_bin"] == 0).sum())
    precision = (tp / scored_n) if scored_n > 0 else float("nan")

    return {
        "actionable": actionable,
        "scored": scored_n,
        "excluded": excluded_n,
        "tp": tp,
        "fp": fp,
        "precision": float(precision),
    }


def _by_year_metrics(frame: pd.DataFrame, label: str) -> pd.DataFrame:
    work = frame.copy()
    work["year"] = _extract_year(work["race"])
    work = work[work["year"].notna()].copy()
    if work.empty:
        return pd.DataFrame(
            columns=["year", "model", "actionable", "scored", "excluded", "tp", "fp", "precision"]
        )

    rows: list[dict[str, float | int | str]] = []
    for year, group in work.groupby("year", sort=True):
        metrics = _comparator_metrics(group)
        rows.append(
            {
                "year": int(year),
                "model": label,
                "actionable": int(metrics["actionable"]),
                "scored": int(metrics["scored"]),
                "excluded": int(metrics["excluded"]),
                "tp": int(metrics["tp"]),
                "fp": int(metrics["fp"]),
                "precision": float(metrics["precision"]),
            }
        )

    return pd.DataFrame(rows)


def _build_wide_by_year(sde_by_year: pd.DataFrame, ml_by_year: pd.DataFrame) -> pd.DataFrame:
    sde = sde_by_year.rename(
        columns={
            "actionable": "sde_actionable",
            "scored": "sde_scored",
            "excluded": "sde_excluded",
            "tp": "sde_tp",
            "fp": "sde_fp",
            "precision": "sde_precision",
        }
    ).drop(columns=["model"])

    ml = ml_by_year.rename(
        columns={
            "actionable": "ml_actionable",
            "scored": "ml_scored",
            "excluded": "ml_excluded",
            "tp": "ml_tp",
            "fp": "ml_fp",
            "precision": "ml_precision",
        }
    ).drop(columns=["model"])

    merged = sde.merge(ml, on="year", how="outer").sort_values("year").reset_index(drop=True)

    merged["delta_precision_ml_minus_sde"] = merged["ml_precision"] - merged["sde_precision"]
    merged["delta_scored_ml_minus_sde"] = merged["ml_scored"] - merged["sde_scored"]
    merged["delta_actionable_ml_minus_sde"] = merged["ml_actionable"] - merged["sde_actionable"]

    merged["ml_vs_sde_scored_ratio"] = merged["ml_scored"] / merged["sde_scored"].replace({0: pd.NA})
    merged["ml_vs_sde_actionable_ratio"] = merged["ml_actionable"] / merged["sde_actionable"].replace({0: pd.NA})

    return merged


def _exclusion_distribution(frame: pd.DataFrame, model_label: str, top_n: int) -> pd.DataFrame:
    excluded = frame[frame["outcome_class"] == "EXCLUDED"].copy()
    if excluded.empty:
        return pd.DataFrame(columns=["model", "exclusion_reason", "count", "share"])

    counts = (
        excluded.assign(exclusion_reason=excluded["exclusion_reason"].replace("", "MISSING_REASON"))
        .groupby("exclusion_reason", dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )
    total = int(len(excluded))
    counts["share"] = counts["count"] / total if total > 0 else 0.0
    counts.insert(0, "model", model_label)
    return counts


def _parse_overlap_n(note_text: str) -> int | None:
    match = re.search(r"overlap_n=(\d+)", note_text)
    if not match:
        return None
    return int(match.group(1))


def _compute_overlap_stats(sde_frame: pd.DataFrame, ml_frame: pd.DataFrame) -> dict[str, float | int]:
    sde_scored = _scored_rows(sde_frame)
    ml_scored = _scored_rows(ml_frame)

    overlap = sde_scored.merge(
        ml_scored,
        on=KEY_COLUMNS,
        how="inner",
        suffixes=("_sde", "_ml"),
    )

    sde_key_count = int(len(sde_scored))
    ml_key_count = int(len(ml_scored))
    overlap_n = int(len(overlap))

    return {
        "overlap_scored_keys": overlap_n,
        "overlap_vs_sde_scored_ratio": (overlap_n / sde_key_count) if sde_key_count > 0 else float("nan"),
        "overlap_vs_ml_scored_ratio": (overlap_n / ml_key_count) if ml_key_count > 0 else float("nan"),
    }


def _to_float(value: object) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return float("nan")
    if pd.isna(result):
        return float("nan")
    return result


def _fmt(value: object, digits: int = 6) -> str:
    number = _to_float(value)
    if pd.isna(number):
        return "N/A"
    return f"{number:.{digits}f}"


def _fmt_p_value(value: object) -> str:
    number = _to_float(value)
    if pd.isna(number):
        return "N/A"
    if number == 0.0:
        return "<1e-16 (underflow)"
    return f"{number:.6g}"


def _build_markdown(
    generated_at: str,
    phase_b_summary_path: Path,
    phase_b_tests_path: Path,
    heuristic_path: Path,
    ml_path: Path,
    headline_table: pd.DataFrame,
    tests_table: pd.DataFrame,
    by_year_table: pd.DataFrame,
    exclusion_table: pd.DataFrame,
    summary_row: dict[str, float | int],
    output_summary_csv: Path,
    output_by_year_csv: Path,
) -> str:
    lines: list[str] = []
    lines.append("# Dedicated SDE vs ML Comparison Report")
    lines.append("")
    lines.append(f"Generated at (UTC): {generated_at}")
    lines.append("")

    lines.append("## Scope")
    lines.append("- Purpose: meeting-ready, fairness-locked SDE vs ML comparison summary.")
    lines.append("- Comparator contract: fixed H=2, actionable-only matching, one-to-one pit consumption.")
    lines.append("- Inference protocol: two-proportion z test as primary, overlap McNemar as paired sensitivity.")
    lines.append("")

    lines.append("## Inputs")
    lines.append(f"- Phase B summary: {phase_b_summary_path}")
    lines.append(f"- Phase B tests: {phase_b_tests_path}")
    lines.append(f"- SDE comparator: {heuristic_path}")
    lines.append(f"- ML comparator: {ml_path}")
    lines.append("")

    lines.append("## Headline Comparison")
    lines.append("| Model | Actionable | Scored | Excluded | TP | FP | Precision | Wilson CI 95% |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |")

    for _, row in headline_table.iterrows():
        lines.append(
            "| {model} | {actionable} | {scored} | {excluded} | {tp} | {fp} | {precision} | [{ci_low}, {ci_high}] |".format(
                model=row["model"],
                actionable=int(row["actionable"]),
                scored=int(row["scored"]),
                excluded=int(row["excluded"]),
                tp=int(row["tp"]),
                fp=int(row["fp"]),
                precision=_fmt(row["precision"], 6),
                ci_low=_fmt(row["wilson_ci_low"], 6),
                ci_high=_fmt(row["wilson_ci_high"], 6),
            )
        )

    lines.append("")
    lines.append("## Statistical Evidence")
    lines.append("| Test | Pairing Scope | Statistic | P-value | Note |")
    lines.append("| --- | --- | ---: | ---: | --- |")
    for _, row in tests_table.iterrows():
        lines.append(
            "| {test} | {scope} | {stat} | {pval} | {note} |".format(
                test=row["test"],
                scope=row["pairing_scope"],
                stat=_fmt(row["statistic"], 6),
                pval=_fmt_p_value(row["p_value"]),
                note=str(row["note"]).replace("|", "/"),
            )
        )

    lines.append("")
    lines.append("## Coverage and Overlap Diagnostics")
    lines.append(f"- Precision delta (ML - SDE): {_fmt(summary_row['precision_delta_ml_minus_sde'], 6)}")
    lines.append(f"- Scored-row delta (ML - SDE): {int(summary_row['scored_delta_ml_minus_sde'])}")
    lines.append(f"- Actionable-row delta (ML - SDE): {int(summary_row['actionable_delta_ml_minus_sde'])}")
    lines.append(f"- Scored ratio (ML / SDE): {_fmt(summary_row['ml_vs_sde_scored_ratio'], 6)}")
    lines.append(f"- Actionable ratio (ML / SDE): {_fmt(summary_row['ml_vs_sde_actionable_ratio'], 6)}")
    lines.append(f"- Overlap scored keys: {int(summary_row['overlap_scored_keys'])}")
    lines.append(
        f"- Overlap ratio vs SDE scored: {_fmt(summary_row['overlap_vs_sde_scored_ratio'], 6)}, "
        f"vs ML scored: {_fmt(summary_row['overlap_vs_ml_scored_ratio'], 6)}"
    )
    lines.append("")

    lines.append("## Per-Year Comparison")
    if by_year_table.empty:
        lines.append("No year-prefixed race keys found in comparator artifacts.")
    else:
        lines.append(
            "| Year | SDE Scored | SDE Precision | ML Scored | ML Precision | Delta Precision (ML-SDE) | Delta Scored (ML-SDE) |"
        )
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
        for _, row in by_year_table.iterrows():
            lines.append(
                "| {year} | {sde_scored} | {sde_precision} | {ml_scored} | {ml_precision} | {delta_precision} | {delta_scored} |".format(
                    year=int(row["year"]),
                    sde_scored=int(row["sde_scored"]),
                    sde_precision=_fmt(row["sde_precision"], 6),
                    ml_scored=int(row["ml_scored"]),
                    ml_precision=_fmt(row["ml_precision"], 6),
                    delta_precision=_fmt(row["delta_precision_ml_minus_sde"], 6),
                    delta_scored=int(row["delta_scored_ml_minus_sde"]),
                )
            )

    lines.append("")
    lines.append("## Top Exclusion Reasons")
    if exclusion_table.empty:
        lines.append("No excluded rows found in comparator artifacts.")
    else:
        lines.append("| Model | Exclusion Reason | Count | Share Within Model Exclusions |")
        lines.append("| --- | --- | ---: | ---: |")
        for _, row in exclusion_table.iterrows():
            lines.append(
                "| {model} | {reason} | {count} | {share} |".format(
                    model=row["model"],
                    reason=str(row["exclusion_reason"]).replace("|", "/"),
                    count=int(row["count"]),
                    share=_fmt(row["share"], 6),
                )
            )

    lines.append("")
    lines.append("## Interpretation and Limits")
    lines.append("- Primary inferential claim should be based on two-proportion z under independent scored-row assumption.")
    lines.append("- McNemar results are overlap-only sensitivity checks and should not replace the primary inference when overlap is limited.")
    lines.append("- Coverage deltas should be discussed jointly with precision deltas to avoid selective reporting.")
    lines.append("- Per-year rows with small scored support should be treated as directional, not as stand-alone inferential evidence.")
    lines.append("")

    lines.append("## Paper Grounding")
    lines.append("- Fair comparator and leakage-safe evaluation: Roberts 2017, Brookshire 2024.")
    lines.append("- Comparative test rigor and uncertainty: Dietterich 1998, Walters 2022.")
    lines.append("- Imbalance-aware precision focus: Saito and Rehmsmeier 2015, Davis and Goadrich 2006.")
    lines.append("")

    lines.append("## Generated Artifacts")
    lines.append(f"- Summary CSV: {output_summary_csv}")
    lines.append(f"- By-year CSV: {output_by_year_csv}")

    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="generate dedicated SDE vs ML comparison meeting report")
    parser.add_argument("--phase-b-summary", default=DEFAULT_PHASE_B_SUMMARY, help="phase b summary csv")
    parser.add_argument("--phase-b-tests", default=DEFAULT_PHASE_B_TESTS, help="phase b tests csv")
    parser.add_argument(
        "--heuristic-comparator",
        default=DEFAULT_HEURISTIC_COMPARATOR,
        help="heuristic comparator csv",
    )
    parser.add_argument("--ml-comparator", default=DEFAULT_ML_COMPARATOR, help="ML comparator csv")
    parser.add_argument("--output-md", default=DEFAULT_OUTPUT_MD, help="output markdown report path")
    parser.add_argument(
        "--output-summary-csv",
        default=DEFAULT_OUTPUT_SUMMARY_CSV,
        help="output compact summary csv path",
    )
    parser.add_argument(
        "--output-by-year-csv",
        default=DEFAULT_OUTPUT_BY_YEAR_CSV,
        help="output by-year comparison csv path",
    )
    parser.add_argument(
        "--top-reasons",
        type=int,
        default=10,
        help="top exclusion reasons per model to include in report",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    phase_b_summary_path = Path(args.phase_b_summary)
    phase_b_tests_path = Path(args.phase_b_tests)
    heuristic_path = Path(args.heuristic_comparator)
    ml_path = Path(args.ml_comparator)

    output_md = Path(args.output_md)
    output_summary_csv = Path(args.output_summary_csv)
    output_by_year_csv = Path(args.output_by_year_csv)

    if args.top_reasons < 1:
        raise ValueError("--top-reasons must be >= 1")

    summary_df = _load_phase_b_summary(phase_b_summary_path)
    tests_df = _load_phase_b_tests(phase_b_tests_path)
    sde_comp = _load_comparator(heuristic_path, "heuristic comparator")
    ml_comp = _load_comparator(ml_path, "ml comparator")

    sde_row = summary_df[summary_df["model"] == "SDE"].iloc[0]
    ml_row = summary_df[summary_df["model"] == "ML"].iloc[0]
    delta_row = summary_df[summary_df["model"] == "ML_minus_SDE"].iloc[0]

    sde_metrics = _comparator_metrics(sde_comp)
    ml_metrics = _comparator_metrics(ml_comp)

    z_row = tests_df[tests_df["test"] == "two_proportion_z"].iloc[0]
    mcnemar_exact_row = tests_df[tests_df["test"] == "mcnemar_exact"].iloc[0]
    mcnemar_cc_row = tests_df[tests_df["test"] == "mcnemar_cc"].iloc[0]

    overlap = _compute_overlap_stats(sde_comp, ml_comp)

    # keep inferential values pinned to phase b outputs, comparators are used for descriptive diagnostics.
    summary_row = {
        "sde_actionable": int(sde_metrics["actionable"]),
        "ml_actionable": int(ml_metrics["actionable"]),
        "actionable_delta_ml_minus_sde": int(ml_metrics["actionable"] - sde_metrics["actionable"]),
        "sde_scored": int(sde_row["scored"]),
        "ml_scored": int(ml_row["scored"]),
        "scored_delta_ml_minus_sde": int(ml_row["scored"] - sde_row["scored"]),
        "sde_tp": int(sde_row["tp"]),
        "sde_fp": int(sde_row["fp"]),
        "ml_tp": int(ml_row["tp"]),
        "ml_fp": int(ml_row["fp"]),
        "sde_precision": float(sde_row["precision"]),
        "ml_precision": float(ml_row["precision"]),
        "precision_delta_ml_minus_sde": float(delta_row["precision"]),
        "sde_wilson_ci_low": float(sde_row["wilson_ci_low"]),
        "sde_wilson_ci_high": float(sde_row["wilson_ci_high"]),
        "ml_wilson_ci_low": float(ml_row["wilson_ci_low"]),
        "ml_wilson_ci_high": float(ml_row["wilson_ci_high"]),
        "two_proportion_z_stat": float(z_row["statistic"]),
        "two_proportion_z_p_value": float(z_row["p_value"]),
        "mcnemar_cc_stat": float(mcnemar_cc_row["statistic"]),
        "mcnemar_cc_p_value": float(mcnemar_cc_row["p_value"]),
        "mcnemar_exact_stat": float(mcnemar_exact_row["statistic"]),
        "mcnemar_exact_p_value": float(mcnemar_exact_row["p_value"]),
        "overlap_scored_keys": int(overlap["overlap_scored_keys"]),
        "overlap_scored_keys_from_note": _parse_overlap_n(str(mcnemar_cc_row["note"])) or int(overlap["overlap_scored_keys"]),
        "overlap_vs_sde_scored_ratio": float(overlap["overlap_vs_sde_scored_ratio"]),
        "overlap_vs_ml_scored_ratio": float(overlap["overlap_vs_ml_scored_ratio"]),
        "ml_vs_sde_scored_ratio": (float(ml_row["scored"]) / float(sde_row["scored"]))
        if float(sde_row["scored"]) > 0
        else float("nan"),
        "ml_vs_sde_actionable_ratio": (float(ml_metrics["actionable"]) / float(sde_metrics["actionable"]))
        if float(sde_metrics["actionable"]) > 0
        else float("nan"),
    }

    summary_out_df = pd.DataFrame([summary_row])

    sde_by_year = _by_year_metrics(sde_comp, "SDE")
    ml_by_year = _by_year_metrics(ml_comp, "ML")
    by_year_out_df = _build_wide_by_year(sde_by_year, ml_by_year)

    exclusions_sde = _exclusion_distribution(sde_comp, "SDE", args.top_reasons)
    exclusions_ml = _exclusion_distribution(ml_comp, "ML", args.top_reasons)
    exclusions_out_df = pd.concat([exclusions_sde, exclusions_ml], ignore_index=True)

    headline_table = pd.DataFrame(
        [
            {
                "model": "SDE",
                "actionable": int(sde_metrics["actionable"]),
                "scored": int(sde_row["scored"]),
                "excluded": int(sde_metrics["excluded"]),
                "tp": int(sde_row["tp"]),
                "fp": int(sde_row["fp"]),
                "precision": float(sde_row["precision"]),
                "wilson_ci_low": float(sde_row["wilson_ci_low"]),
                "wilson_ci_high": float(sde_row["wilson_ci_high"]),
            },
            {
                "model": "ML",
                "actionable": int(ml_metrics["actionable"]),
                "scored": int(ml_row["scored"]),
                "excluded": int(ml_metrics["excluded"]),
                "tp": int(ml_row["tp"]),
                "fp": int(ml_row["fp"]),
                "precision": float(ml_row["precision"]),
                "wilson_ci_low": float(ml_row["wilson_ci_low"]),
                "wilson_ci_high": float(ml_row["wilson_ci_high"]),
            },
        ]
    )

    generated_at = datetime.now(timezone.utc).isoformat()
    markdown = _build_markdown(
        generated_at=generated_at,
        phase_b_summary_path=phase_b_summary_path,
        phase_b_tests_path=phase_b_tests_path,
        heuristic_path=heuristic_path,
        ml_path=ml_path,
        headline_table=headline_table,
        tests_table=tests_df,
        by_year_table=by_year_out_df,
        exclusion_table=exclusions_out_df,
        summary_row=summary_row,
        output_summary_csv=output_summary_csv,
        output_by_year_csv=output_by_year_csv,
    )

    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_summary_csv.parent.mkdir(parents=True, exist_ok=True)
    output_by_year_csv.parent.mkdir(parents=True, exist_ok=True)

    output_md.write_text(markdown, encoding="utf-8")
    summary_out_df.to_csv(output_summary_csv, index=False)
    by_year_out_df.to_csv(output_by_year_csv, index=False)

    print("=== PHASE B DEDICATED SDE VS ML COMPARISON ===")
    print(f"phase b summary input   : {phase_b_summary_path}")
    print(f"phase b tests input     : {phase_b_tests_path}")
    print(f"heuristic comparator    : {heuristic_path}")
    print(f"ml comparator           : {ml_path}")
    print(f"output markdown         : {output_md}")
    print(f"output summary csv      : {output_summary_csv}")
    print(f"output by-year csv      : {output_by_year_csv}")

    print("\nheadline numbers")
    print(
        "SDE precision={sde_prec:.6f}, ML precision={ml_prec:.6f}, delta={delta:+.6f}, "
        "z_p={zp}, mcnemar_exact_p={mp}".format(
            sde_prec=float(summary_row["sde_precision"]),
            ml_prec=float(summary_row["ml_precision"]),
            delta=float(summary_row["precision_delta_ml_minus_sde"]),
            zp=_fmt_p_value(summary_row["two_proportion_z_p_value"]),
            mp=_fmt_p_value(summary_row["mcnemar_exact_p_value"]),
        )
    )


if __name__ == "__main__":
    main()
