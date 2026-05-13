"""Build dual-contract label summary tables for raw/clean truth-lens targets."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class TargetSpec:
    target_column: str
    alias_column: str
    outcome_mode: str
    truth_lens: str
    positive_rule: str
    raw_target_column: str


TARGET_SPECS: list[TargetSpec] = [
    TargetSpec(
        target_column="target_pit_any_h2_raw",
        alias_column="pit_any_h2_raw",
        outcome_mode="pit_any_h2",
        truth_lens="raw",
        positive_rule="any_future_pit_in_window_[k+1,k+h]",
        raw_target_column="target_pit_any_h2_raw",
    ),
    TargetSpec(
        target_column="target_pit_any_h2_clean_actionable",
        alias_column="pit_any_h2_clean_actionable",
        outcome_mode="pit_any_h2",
        truth_lens="clean_actionable",
        positive_rule="any_future_eligible_pit_in_window_[k+1,k+h]",
        raw_target_column="target_pit_any_h2_raw",
    ),
    TargetSpec(
        target_column="target_pit_any_h2_clean_dry_strategy",
        alias_column="pit_any_h2_clean_dry_strategy",
        outcome_mode="pit_any_h2",
        truth_lens="clean_dry_strategy",
        positive_rule="any_future_eligible_pit_in_window_[k+1,k+h]",
        raw_target_column="target_pit_any_h2_raw",
    ),
    TargetSpec(
        target_column="target_pit_success_h2_raw",
        alias_column="pit_success_h2_raw",
        outcome_mode="pit_success_h2",
        truth_lens="raw",
        positive_rule="matched_success_pit_in_window_[k+1,k+h]",
        raw_target_column="target_pit_success_h2_raw",
    ),
    TargetSpec(
        target_column="target_pit_success_h2_clean_actionable",
        alias_column="pit_success_h2_clean_actionable",
        outcome_mode="pit_success_h2",
        truth_lens="clean_actionable",
        positive_rule="raw_success_match_retained_only_if_matched_pit_is_lens_eligible",
        raw_target_column="target_pit_success_h2_raw",
    ),
    TargetSpec(
        target_column="target_pit_success_h2_clean_dry_strategy",
        alias_column="pit_success_h2_clean_dry_strategy",
        outcome_mode="pit_success_h2",
        truth_lens="clean_dry_strategy",
        positive_rule="raw_success_match_retained_only_if_matched_pit_is_lens_eligible",
        raw_target_column="target_pit_success_h2_raw",
    ),
]


def _load_dataset(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"dataset not found: {path}")
    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"dataset is empty: {path}")
    return df


def _extract_year(frame: pd.DataFrame) -> pd.Series:
    if "race" not in frame.columns:
        raise ValueError("dataset must contain `race` column for coverage reporting")
    years = pd.to_numeric(
        frame["race"].astype(str).str.extract(r"^(\d{4})\s::")[0],
        errors="coerce",
    )
    if years.isna().any() and "_source_year" in frame.columns:
        fallback = pd.to_numeric(frame["_source_year"], errors="coerce")
        years = years.fillna(fallback)
    if years.isna().any():
        bad = frame.loc[years.isna(), "race"].astype(str).drop_duplicates().head(8).tolist()
        raise ValueError(f"unable to infer source year from race key; examples={bad}")
    return years.astype(int)


def _validate_target_columns(frame: pd.DataFrame) -> None:
    required = {spec.target_column for spec in TARGET_SPECS}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"dataset missing required target columns: {missing}")


def _validate_monotonicity(frame: pd.DataFrame) -> list[str]:
    warnings: list[str] = []
    outcomes = ["pit_any_h2", "pit_success_h2"]
    for outcome in outcomes:
        raw_col = f"target_{outcome}_raw"
        clean_actionable_col = f"target_{outcome}_clean_actionable"
        clean_dry_col = f"target_{outcome}_clean_dry_strategy"
        raw_pos = int((frame[raw_col] == 1).sum())
        clean_actionable_pos = int((frame[clean_actionable_col] == 1).sum())
        clean_dry_pos = int((frame[clean_dry_col] == 1).sum())
        if clean_actionable_pos > raw_pos:
            warnings.append(
                f"{outcome}: clean_actionable positives ({clean_actionable_pos}) exceed raw ({raw_pos})"
            )
        if clean_dry_pos > clean_actionable_pos:
            warnings.append(
                f"{outcome}: clean_dry_strategy positives ({clean_dry_pos}) exceed clean_actionable ({clean_actionable_pos})"
            )
        if raw_pos <= 0 or clean_actionable_pos <= 0 or clean_dry_pos <= 0:
            warnings.append(
                f"{outcome}: one or more truth-lens targets have zero positives "
                f"(raw={raw_pos}, clean_actionable={clean_actionable_pos}, clean_dry_strategy={clean_dry_pos})"
            )
    return warnings


def _coverage_text(years: pd.Series) -> str:
    values = sorted({int(x) for x in years.dropna().astype(int).tolist()})
    return ",".join(str(x) for x in values)


def build_label_summaries(
    frame: pd.DataFrame,
    *,
    horizon: int = 2,
    strict_invariants: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    _validate_target_columns(frame)
    years = _extract_year(frame)
    warnings = _validate_monotonicity(frame)
    if strict_invariants and warnings:
        raise ValueError("label summary invariants failed:\n- " + "\n- ".join(warnings))

    rows_total = int(len(frame))
    years_covered = _coverage_text(years)
    races_covered = int(frame["race"].astype(str).nunique())

    summary_rows: list[dict[str, object]] = []
    by_year_rows: list[dict[str, object]] = []
    for spec in TARGET_SPECS:
        positives = int((frame[spec.target_column] == 1).sum())
        raw_positives = int((frame[spec.raw_target_column] == 1).sum())
        demoted = max(raw_positives - positives, 0)

        summary_rows.append(
            {
                "target_column": spec.target_column,
                "alias_column": spec.alias_column,
                "outcome_mode": spec.outcome_mode,
                "truth_lens": spec.truth_lens,
                "horizon": int(horizon),
                "positive_rule": spec.positive_rule,
                "rows": rows_total,
                "positives": positives,
                "prevalence": float(positives / rows_total) if rows_total else 0.0,
                "years_covered": years_covered,
                "races_covered": races_covered,
                "raw_positives": raw_positives,
                "demoted_from_raw_positive_count": demoted,
            }
        )

        work = pd.DataFrame(
            {
                "target_value": frame[spec.target_column].astype(int),
                "raw_value": frame[spec.raw_target_column].astype(int),
                "race": frame["race"].astype(str),
            }
        )
        work["year"] = years.values
        for year, group in work.groupby("year", sort=True):
            year_rows = int(len(group))
            year_pos = int((group["target_value"] == 1).sum())
            year_raw_pos = int((group["raw_value"] == 1).sum())
            by_year_rows.append(
                {
                    "year": int(year),
                    "target_column": spec.target_column,
                    "alias_column": spec.alias_column,
                    "outcome_mode": spec.outcome_mode,
                    "truth_lens": spec.truth_lens,
                    "horizon": int(horizon),
                    "positive_rule": spec.positive_rule,
                    "rows": year_rows,
                    "positives": year_pos,
                    "prevalence": float(year_pos / year_rows) if year_rows else 0.0,
                    "races_covered": int(group["race"].astype(str).nunique()),
                    "raw_positives": year_raw_pos,
                    "demoted_from_raw_positive_count": max(year_raw_pos - year_pos, 0),
                }
            )

    summary_df = pd.DataFrame(summary_rows)
    by_year_df = pd.DataFrame(by_year_rows).sort_values(
        by=["year", "outcome_mode", "truth_lens", "target_column"],
        kind="mergesort",
    )
    return summary_df, by_year_df, warnings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate dual-contract label summary tables from prepared dataset.")
    parser.add_argument("--dataset", required=True, help="prepared dataset parquet/csv path")
    parser.add_argument("--horizon", type=int, default=2, help="label horizon to report")
    parser.add_argument("--output-csv", required=True, help="summary output csv")
    parser.add_argument("--output-by-year-csv", default="", help="optional by-year summary csv")
    parser.add_argument(
        "--strict-invariants",
        dest="strict_invariants",
        action="store_true",
        help="fail if monotonicity/positives invariants fail",
    )
    parser.add_argument(
        "--no-strict-invariants",
        dest="strict_invariants",
        action="store_false",
        help="emit warnings instead of failing invariant checks",
    )
    parser.set_defaults(strict_invariants=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dataset_path = Path(args.dataset)
    out_summary = Path(args.output_csv)
    out_by_year = Path(args.output_by_year_csv) if args.output_by_year_csv else None

    frame = _load_dataset(dataset_path)
    summary_df, by_year_df, warnings = build_label_summaries(
        frame,
        horizon=int(args.horizon),
        strict_invariants=bool(args.strict_invariants),
    )

    out_summary.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(out_summary, index=False)
    if out_by_year is not None:
        out_by_year.parent.mkdir(parents=True, exist_ok=True)
        by_year_df.to_csv(out_by_year, index=False)

    print("=== LABEL CONTRACT SUMMARY GENERATED ===")
    print(f"dataset             : {dataset_path}")
    print(f"summary csv         : {out_summary}")
    if out_by_year is not None:
        print(f"summary by year csv : {out_by_year}")
    if warnings:
        print("invariant warnings:")
        for item in warnings:
            print(f"- {item}")


if __name__ == "__main__":
    main()
