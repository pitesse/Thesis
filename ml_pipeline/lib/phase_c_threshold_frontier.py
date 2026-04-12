"""run threshold sweep and lookahead diagnostics for merged ML OOF decisions.

this script keeps comparator semantics fixed and evaluates recovery tradeoffs:
- threshold vs actionable and scored coverage,
- threshold vs precision against a floor,
- exclusion decomposition for lookahead interpretation.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from .comparator_heuristic import (
    DEFAULT_DATA_LAKE,
    DEFAULT_HORIZON,
    DEFAULT_SEASON_TAG,
    DEFAULT_YEAR,
    _build_comparator_dataset,
    _latest_jsonl,
    _load_jsonl,
    _prepare_pit_evals,
)
from .comparator_ml import _load_ml_decisions

DEFAULT_DECISIONS = "data_lake/reports/ml_oof_winner_2022_2024_merged.csv"
DEFAULT_SCORE_COLUMN = "calibrated_proba"
DEFAULT_OUTPUT = "data_lake/reports/phase_c_threshold_sweep_2022_2024_merged.csv"
DEFAULT_BY_YEAR_OUTPUT = "data_lake/reports/phase_c_threshold_sweep_by_year_2022_2024_merged.csv"
DEFAULT_BEST_COMPARATOR_OUTPUT = "data_lake/reports/ml_comparator_best_threshold_2022_2024_merged.csv"
DEFAULT_REPORT_OUTPUT = "data_lake/reports/phase_c_threshold_sweep_report_2022_2024_merged.txt"
DEFAULT_MIN_THRESHOLD = 0.05
DEFAULT_MAX_THRESHOLD = 0.95
DEFAULT_POINTS = 37
DEFAULT_PRECISION_FLOOR = 0.7149187592319055
DEFAULT_SDE_SCORED_BASELINE = 677
ACTIONABLE_LABEL = "PIT_NOW"


def _prepare_decision_rows(decisions: pd.DataFrame, score_column: str) -> pd.DataFrame:
    required = ["race", "driver", "lapNumber", score_column]
    missing = [column for column in required if column not in decisions.columns]
    if missing:
        raise ValueError(f"decisions are missing required columns: {missing}")

    work = decisions.copy()
    work["race"] = work["race"].astype(str)
    work["driver"] = work["driver"].astype(str)
    work["lapNumber"] = pd.to_numeric(work["lapNumber"], errors="coerce")
    work = work[work["lapNumber"].notna()].copy()
    work["lapNumber"] = work["lapNumber"].astype(int)
    work[score_column] = pd.to_numeric(work[score_column], errors="coerce")
    work = work[work[score_column].notna()].copy()

    if work.empty:
        raise ValueError("no valid decision rows found after numeric conversion")

    return work


def _suggestions_at_threshold(
    prepared: pd.DataFrame,
    score_column: str,
    threshold: float,
) -> pd.DataFrame:
    actionable = prepared[prepared[score_column] >= threshold].copy()
    if actionable.empty:
        return pd.DataFrame(columns=["race", "driver", "lapNumber", "suggestionLabel", "totalScore"])

    return pd.DataFrame(
        {
            "race": actionable["race"],
            "driver": actionable["driver"],
            "lapNumber": actionable["lapNumber"],
            "suggestionLabel": ACTIONABLE_LABEL,
            "totalScore": actionable[score_column],
        }
    )


def _empty_summary(threshold: float, precision_floor: float, sde_scored_baseline: int) -> dict[str, float | int]:
    return {
        "threshold": threshold,
        "actionable": 0,
        "scored": 0,
        "excluded": 0,
        "tp": 0,
        "fp": 0,
        "precision": float("nan"),
        "scored_rate": 0.0,
        "tp_per_actionable": 0.0,
        "meets_precision_floor": 0,
        "precision_delta_vs_floor": float("nan"),
        "scored_vs_sde_ratio": 0.0,
        "no_match_count": 0,
        "no_match_rate": 0.0,
        "no_match_one_lap_beyond": 0,
        "no_match_two_to_three_laps_beyond": 0,
        "no_match_four_plus_laps_beyond": 0,
        "no_match_pit_in_window_but_consumed": 0,
        "excluded_incident": 0,
        "excluded_missing_post_gap": 0,
        "excluded_weather": 0,
    }


def _summarize_comparator(
    comparator: pd.DataFrame,
    threshold: float,
    precision_floor: float,
    sde_scored_baseline: int,
) -> dict[str, float | int]:
    scored_mask = comparator["outcome_class"].isin(["1", "0"])
    scored = comparator[scored_mask].copy()
    excluded = comparator[~scored_mask].copy()

    actionable = int(len(comparator))
    scored_n = int(len(scored))
    excluded_n = int(len(excluded))
    tp = int((scored["outcome_class"] == "1").sum())
    fp = int((scored["outcome_class"] == "0").sum())
    precision = (tp / scored_n) if scored_n > 0 else float("nan")
    scored_rate = (scored_n / actionable) if actionable > 0 else 0.0
    tp_per_actionable = (tp / actionable) if actionable > 0 else 0.0

    no_match = comparator[comparator["exclusion_reason"] == "NO_MATCH_WITHIN_HORIZON"].copy()
    no_match_count = int(len(no_match))
    no_match_rate = (no_match_count / excluded_n) if excluded_n > 0 else 0.0

    nearest = pd.to_numeric(no_match["nearest_future_pit_distance"], errors="coerce")
    one_lap = int((nearest == 3).sum())
    two_to_three = int(((nearest >= 4) & (nearest <= 5)).sum())
    four_plus = int((nearest >= 6).sum())
    pit_in_window_consumed = int(
        (
            no_match["pit_in_window_before_consumption"].fillna(False).astype(bool)
        ).sum()
    )

    excluded_incident = int((comparator["exclusion_reason"] == "UNRESOLVED_INCIDENT_FILTER").sum())
    excluded_missing_post_gap = int((comparator["exclusion_reason"] == "UNRESOLVED_MISSING_POST_GAP").sum())
    excluded_weather = int((comparator["exclusion_reason"] == "WEATHER_SURVIVAL_STOP").sum())

    meets_floor = int(scored_n > 0 and precision >= precision_floor)
    precision_delta = (precision - precision_floor) if scored_n > 0 else float("nan")

    return {
        "threshold": float(threshold),
        "actionable": actionable,
        "scored": scored_n,
        "excluded": excluded_n,
        "tp": tp,
        "fp": fp,
        "precision": float(precision),
        "scored_rate": float(scored_rate),
        "tp_per_actionable": float(tp_per_actionable),
        "meets_precision_floor": meets_floor,
        "precision_delta_vs_floor": float(precision_delta),
        "scored_vs_sde_ratio": float((scored_n / sde_scored_baseline) if sde_scored_baseline > 0 else float("nan")),
        "no_match_count": no_match_count,
        "no_match_rate": float(no_match_rate),
        "no_match_one_lap_beyond": one_lap,
        "no_match_two_to_three_laps_beyond": two_to_three,
        "no_match_four_plus_laps_beyond": four_plus,
        "no_match_pit_in_window_but_consumed": pit_in_window_consumed,
        "excluded_incident": excluded_incident,
        "excluded_missing_post_gap": excluded_missing_post_gap,
        "excluded_weather": excluded_weather,
    }


def _summarize_by_year(comparator: pd.DataFrame, threshold: float) -> list[dict[str, float | int | str]]:
    if comparator.empty:
        return []

    work = comparator.copy()
    work["year"] = work["race"].astype(str).str.split(" :: ", n=1).str[0]

    rows: list[dict[str, float | int | str]] = []
    for year, group in work.groupby("year"):
        scored_mask = group["outcome_class"].isin(["1", "0"])
        scored = group[scored_mask]
        actionable = int(len(group))
        scored_n = int(len(scored))
        tp = int((scored["outcome_class"] == "1").sum())
        fp = int((scored["outcome_class"] == "0").sum())
        precision = (tp / scored_n) if scored_n > 0 else float("nan")
        scored_rate = (scored_n / actionable) if actionable > 0 else 0.0
        tp_per_actionable = (tp / actionable) if actionable > 0 else 0.0

        rows.append(
            {
                "threshold": float(threshold),
                "year": str(year),
                "actionable": actionable,
                "scored": scored_n,
                "tp": tp,
                "fp": fp,
                "precision": float(precision),
                "scored_rate": float(scored_rate),
                "tp_per_actionable": float(tp_per_actionable),
            }
        )

    return rows


def _select_best_threshold(summary: pd.DataFrame, floor: float) -> pd.Series:
    floor_ok = summary[(summary["meets_precision_floor"] == 1) & (summary["scored"] > 0)].copy()
    if not floor_ok.empty:
        floor_ok.sort_values(
            by=["scored", "tp", "precision", "threshold"],
            ascending=[False, False, False, True],
            inplace=True,
        )
        return floor_ok.iloc[0]

    fallback = summary[summary["scored"] > 0].copy()
    if fallback.empty:
        return summary.iloc[0]
    fallback.sort_values(
        by=["precision", "scored", "tp", "threshold"],
        ascending=[False, False, False, True],
        inplace=True,
    )
    return fallback.iloc[0]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="run threshold sweep and lookahead diagnostics on ML OOF decisions")
    parser.add_argument("--decisions", default=DEFAULT_DECISIONS, help="path to OOF decisions csv/parquet")
    parser.add_argument("--score-column", default=DEFAULT_SCORE_COLUMN, help="score column used for thresholding")
    parser.add_argument("--data-lake", default=DEFAULT_DATA_LAKE, help="data lake directory")
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR, help="season year token")
    parser.add_argument("--season-tag", default=DEFAULT_SEASON_TAG, help="season tag token")
    parser.add_argument("--horizon", type=int, default=DEFAULT_HORIZON, help="matching horizon in laps")
    parser.add_argument("--min-threshold", type=float, default=DEFAULT_MIN_THRESHOLD, help="minimum threshold")
    parser.add_argument("--max-threshold", type=float, default=DEFAULT_MAX_THRESHOLD, help="maximum threshold")
    parser.add_argument("--points", type=int, default=DEFAULT_POINTS, help="number of threshold points")
    parser.add_argument("--thresholds", type=float, nargs="*", default=None, help="optional explicit thresholds")
    parser.add_argument("--precision-floor", type=float, default=DEFAULT_PRECISION_FLOOR, help="minimum precision floor")
    parser.add_argument("--sde-scored-baseline", type=int, default=DEFAULT_SDE_SCORED_BASELINE, help="SDE scored baseline for ratio")
    parser.add_argument("--reference-threshold", type=float, default=0.50, help="reference threshold for delta reporting")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="summary output csv")
    parser.add_argument("--by-year-output", default=DEFAULT_BY_YEAR_OUTPUT, help="by-year output csv")
    parser.add_argument("--best-comparator-output", default=DEFAULT_BEST_COMPARATOR_OUTPUT, help="best threshold comparator output csv")
    parser.add_argument("--report-output", default=DEFAULT_REPORT_OUTPUT, help="formatted txt report output")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not (0.0 < args.min_threshold < args.max_threshold < 1.0):
        raise ValueError("min-threshold and max-threshold must satisfy 0 < min < max < 1")
    if args.points < 2:
        raise ValueError("points must be at least 2")
    if not (0.0 < args.precision_floor < 1.0):
        raise ValueError("precision-floor must be between 0 and 1")

    data_lake = Path(args.data_lake)
    decisions_path = Path(args.decisions)

    decisions = _load_ml_decisions(decisions_path)
    prepared = _prepare_decision_rows(decisions, args.score_column)

    if args.thresholds:
        thresholds = sorted({float(x) for x in args.thresholds if 0.0 < float(x) < 1.0})
        if not thresholds:
            raise ValueError("thresholds list is empty after filtering to 0 < threshold < 1")
    else:
        thresholds = [float(x) for x in np.linspace(args.min_threshold, args.max_threshold, args.points)]

    pit_evals_path = _latest_jsonl(data_lake, "pit_evals", args.year, args.season_tag)
    pit_evals = _prepare_pit_evals(_load_jsonl(pit_evals_path))

    summary_rows: list[dict[str, float | int]] = []
    by_year_rows: list[dict[str, float | int | str]] = []

    for threshold in thresholds:
        suggestions = _suggestions_at_threshold(prepared, args.score_column, threshold)
        if suggestions.empty:
            summary_rows.append(_empty_summary(threshold, args.precision_floor, args.sde_scored_baseline))
            continue

        comparator = _build_comparator_dataset(suggestions, pit_evals, args.horizon)
        summary_rows.append(
            _summarize_comparator(
                comparator,
                threshold,
                args.precision_floor,
                args.sde_scored_baseline,
            )
        )
        by_year_rows.extend(_summarize_by_year(comparator, threshold))

    summary_df = pd.DataFrame(summary_rows)
    by_year_df = pd.DataFrame(by_year_rows)

    best_row = _select_best_threshold(summary_df, args.precision_floor)
    best_threshold = float(best_row["threshold"])

    reference_idx = (summary_df["threshold"] - float(args.reference_threshold)).abs().idxmin()
    reference_row = summary_df.loc[reference_idx]

    best_suggestions = _suggestions_at_threshold(prepared, args.score_column, best_threshold)
    if best_suggestions.empty:
        best_comparator = pd.DataFrame()
    else:
        best_comparator = _build_comparator_dataset(best_suggestions, pit_evals, args.horizon)

    output_path = Path(args.output)
    by_year_output_path = Path(args.by_year_output)
    best_output_path = Path(args.best_comparator_output)
    report_output_path = Path(args.report_output)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    by_year_output_path.parent.mkdir(parents=True, exist_ok=True)
    best_output_path.parent.mkdir(parents=True, exist_ok=True)
    report_output_path.parent.mkdir(parents=True, exist_ok=True)

    summary_df.to_csv(output_path, index=False)
    by_year_df.to_csv(by_year_output_path, index=False)
    best_comparator.to_csv(best_output_path, index=False)

    recovered_scored = int(best_row["scored"] - reference_row["scored"])
    recovered_actionable = int(best_row["actionable"] - reference_row["actionable"])

    report_lines = [
        "=== PHASE C THRESHOLD SWEEP REPORT ===",
        f"decisions input    : {decisions_path}",
        f"pit evals input    : {pit_evals_path}",
        f"horizon            : {args.horizon}",
        f"precision floor    : {args.precision_floor:.6f}",
        f"threshold points   : {len(thresholds)}",
        "",
        "selected threshold",
        f"best_threshold     : {best_threshold:.6f}",
        f"best_actionable    : {int(best_row['actionable'])}",
        f"best_scored        : {int(best_row['scored'])}",
        f"best_tp            : {int(best_row['tp'])}",
        f"best_fp            : {int(best_row['fp'])}",
        f"best_precision     : {float(best_row['precision']):.6f}",
        f"best_scored_rate   : {float(best_row['scored_rate']):.6f}",
        f"meets_floor        : {int(best_row['meets_precision_floor'])}",
        "",
        "delta vs reference threshold",
        f"reference_threshold: {float(reference_row['threshold']):.6f}",
        f"reference_scored   : {int(reference_row['scored'])}",
        f"reference_precision: {float(reference_row['precision']):.6f}",
        f"delta_scored       : {recovered_scored:+d}",
        f"delta_actionable   : {recovered_actionable:+d}",
        "",
        "lookahead diagnostics at selected threshold",
        f"no_match_count     : {int(best_row['no_match_count'])}",
        f"no_match_rate      : {float(best_row['no_match_rate']):.6f}",
        f"one_lap_beyond     : {int(best_row['no_match_one_lap_beyond'])}",
        f"two_to_three_beyond: {int(best_row['no_match_two_to_three_laps_beyond'])}",
        f"four_plus_beyond   : {int(best_row['no_match_four_plus_laps_beyond'])}",
        f"in_window_consumed : {int(best_row['no_match_pit_in_window_but_consumed'])}",
        "",
        "artifacts",
        f"summary csv        : {output_path}",
        f"by year csv        : {by_year_output_path}",
        f"best comparator    : {best_output_path}",
    ]

    report_output_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    print("\n".join(report_lines))


if __name__ == "__main__":
    main()