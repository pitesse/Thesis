"""build comparator-fair ML decision dataset under locked H=2 matching semantics.

this script converts out-of-fold ML decisions into suggestion-like rows and then
reuses the same one-to-one horizon matcher used by the heuristic comparator.
"""

from __future__ import annotations

import argparse
from pathlib import Path

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
    _print_summary,
)

DEFAULT_DECISIONS = "data_lake/reports/ml_oof_winner.csv"
DEFAULT_OUTPUT = "ml_comparator_dataset.csv"
DEFAULT_DECISION_COLUMN = "constrained_pred"
DEFAULT_SCORE_COLUMN = "calibrated_proba"
DEFAULT_ACTIONABLE_LABEL = "PIT_NOW"


def _load_ml_decisions(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"ML decisions file not found: {path}")

    if path.suffix.lower() == ".parquet":
        decisions = pd.read_parquet(path)
    else:
        decisions = pd.read_csv(path)

    if decisions.empty:
        raise ValueError(f"ML decisions file is empty: {path}")
    return decisions


def _prepare_ml_suggestions(
    decisions: pd.DataFrame,
    decision_column: str,
    score_column: str,
    actionable_label: str,
) -> pd.DataFrame:
    required = ["race", "driver", "lapNumber", decision_column, score_column]
    missing = [column for column in required if column not in decisions.columns]
    if missing:
        raise ValueError(f"ML decisions are missing required columns: {missing}")

    work = decisions.copy()
    work["race"] = work["race"].astype(str)
    work["driver"] = work["driver"].astype(str)
    work["lapNumber"] = pd.to_numeric(work["lapNumber"], errors="coerce")
    work = work[work["lapNumber"].notna()].copy()
    work["lapNumber"] = work["lapNumber"].astype(int)

    work[decision_column] = pd.to_numeric(work[decision_column], errors="coerce").fillna(0).astype(int)
    work[score_column] = pd.to_numeric(work[score_column], errors="coerce")

    actionable = work[work[decision_column] == 1].copy()
    if actionable.empty:
        raise ValueError("No actionable ML decisions were found (decision column has no positive rows)")

    suggestions = pd.DataFrame(
        {
            "race": actionable["race"],
            "driver": actionable["driver"],
            "lapNumber": actionable["lapNumber"],
            "suggestionLabel": actionable_label,
            "totalScore": actionable[score_column],
        }
    )

    return suggestions


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="build comparator dataset from ML out-of-fold decisions")
    parser.add_argument("--decisions", default=DEFAULT_DECISIONS, help="path to ML decisions csv/parquet")
    parser.add_argument("--decision-column", default=DEFAULT_DECISION_COLUMN, help="binary decision column")
    parser.add_argument("--score-column", default=DEFAULT_SCORE_COLUMN, help="score/probability column")
    parser.add_argument("--actionable-label", default=DEFAULT_ACTIONABLE_LABEL, help="label assigned to ML actions")
    parser.add_argument("--data-lake", default=DEFAULT_DATA_LAKE, help="data lake directory")
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR, help="season year")
    parser.add_argument("--season-tag", default=DEFAULT_SEASON_TAG, help="season tag token")
    parser.add_argument("--horizon", type=int, default=DEFAULT_HORIZON, help="look ahead horizon in laps")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="output csv name or absolute path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    decisions_path = Path(args.decisions)
    data_lake = Path(args.data_lake)

    decisions = _load_ml_decisions(decisions_path)
    suggestions = _prepare_ml_suggestions(
        decisions=decisions,
        decision_column=args.decision_column,
        score_column=args.score_column,
        actionable_label=args.actionable_label,
    )

    pit_evals_path = _latest_jsonl(data_lake, "pit_evals", args.year, args.season_tag)
    pit_evals = _prepare_pit_evals(_load_jsonl(pit_evals_path))

    comparator = _build_comparator_dataset(suggestions, pit_evals, args.horizon)

    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = data_lake / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    comparator.to_csv(output_path, index=False)

    print(f"ML decisions input: {decisions_path}")
    print(f"pit evals input  : {pit_evals_path}")
    print(f"output csv       : {output_path}")
    _print_summary(comparator, suggestions, args.horizon)


if __name__ == "__main__":
    main()
