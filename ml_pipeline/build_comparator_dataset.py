"""build a leakage safe comparator dataset between heuristic pit suggestions and pit evaluations.

method notes:
- this temporal horizon matching is from brookshire 2024 and roberts et al 2017,
  evaluate each decision only against future outcomes inside a fixed window.
- this dedup priority is from elkan 2001,
  keep higher urgency suggestions when multiple decisions collide on the same lap.
- this binary success mapping follows f1 pit stop benchmark framing in hettmann 2024
  and sasikumar et al 2025, while keeping unresolved and weather rows excluded.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import pandas as pd


DEFAULT_DATA_LAKE = "data_lake"
DEFAULT_YEAR = 2023
DEFAULT_SEASON_TAG = "season"
DEFAULT_HORIZON = 2
DEFAULT_OUTPUT = "heuristic_comparator_dataset.csv"

LABEL_PRIORITY = {
    "PIT_NOW": 4,
    "GOOD_PIT": 3,
    "LOST_CHANCE": 2,
    "MONITOR": 1,
}
ACTIONABLE_LABELS = {"PIT_NOW", "GOOD_PIT"}

POSITIVE_RESULTS = {
    "SUCCESS_UNDERCUT",
    "SUCCESS_OVERCUT",
    "SUCCESS_DEFEND",
    "SUCCESS_FREE_STOP",
    "OFFSET_ADVANTAGE",
}
NEGATIVE_RESULTS = {
    "FAILURE_PACE_DEFICIT",
    "FAILURE_TRAFFIC",
    "OFFSET_DISADVANTAGE",
}


def _normalize_label(value: object) -> str:
    if value is None:
        return ""

    text = str(value).strip()
    if text == "" or text.lower() == "nan" or text == "<NA>":
        return ""
    return text.upper().replace(" ", "_")


def _latest_jsonl(data_lake: Path, stream: str, year: int, season_tag: str) -> Path:
    pattern = f"{stream}_{year}_{season_tag}_*.jsonl"
    matches = list(data_lake.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"no files found for pattern: {data_lake / pattern}")
    return max(matches, key=lambda p: p.stat().st_mtime)


def _load_jsonl(path: Path) -> pd.DataFrame:
    df = pd.read_json(path, lines=True)
    if df.empty:
        raise ValueError(f"input file is empty: {path}")
    return df


def _require_columns(df: pd.DataFrame, required: Iterable[str], name: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")


def _dedup_suggestions(suggestions: pd.DataFrame) -> pd.DataFrame:
    _require_columns(
        suggestions,
        ["race", "driver", "lapNumber", "suggestionLabel", "totalScore"],
        "pit_suggestions",
    )

    work = suggestions.copy()
    work["label_norm"] = work["suggestionLabel"].map(_normalize_label)
    work["priority_rank"] = work["label_norm"].map(LABEL_PRIORITY).fillna(0).astype(int)
    work["score_num"] = pd.to_numeric(work["totalScore"], errors="coerce").fillna(float("-inf"))
    work["lap_num"] = pd.to_numeric(work["lapNumber"], errors="coerce")
    work = work[work["lap_num"].notna()].copy()
    work["lap_num"] = work["lap_num"].astype(int)
    if "eventDate" in work.columns:
        work["event_dt"] = pd.to_datetime(work["eventDate"], errors="coerce", utc=True)
    else:
        work["event_dt"] = pd.NaT
    work["_row_id"] = range(len(work))

    # this tie break reasoning is from elkan 2001, prefer higher urgency when decision cost differs.
    work.sort_values(
        by=["race", "driver", "lap_num", "priority_rank", "score_num", "event_dt", "_row_id"],
        ascending=[True, True, True, False, False, False, False],
        inplace=True,
    )

    deduped = work.drop_duplicates(subset=["race", "driver", "lap_num"], keep="first").copy()
    deduped.sort_values(
        by=["race", "driver", "lap_num", "priority_rank", "score_num", "event_dt", "_row_id"],
        ascending=[True, True, True, False, False, False, False],
        inplace=True,
    )
    deduped.reset_index(drop=True, inplace=True)
    return deduped


def _prepare_pit_evals(pit_evals: pd.DataFrame) -> pd.DataFrame:
    _require_columns(pit_evals, ["race", "driver", "pitLapNumber", "result"], "pit_evals")

    work = pit_evals.copy()
    work["pit_lap_num"] = pd.to_numeric(work["pitLapNumber"], errors="coerce")
    work = work[work["pit_lap_num"].notna()].copy()
    work["pit_lap_num"] = work["pit_lap_num"].astype(int)
    work["result_norm"] = work["result"].map(_normalize_label)
    work["_eval_id"] = range(len(work))

    work.sort_values(
        by=["race", "driver", "pit_lap_num", "_eval_id"],
        ascending=[True, True, True, True],
        inplace=True,
    )
    work.reset_index(drop=True, inplace=True)
    return work


def _map_outcome(result_norm: str) -> tuple[str, str]:
    if result_norm in POSITIVE_RESULTS:
        return "1", ""
    if result_norm in NEGATIVE_RESULTS:
        return "0", ""
    if result_norm.startswith("UNRESOLVED_"):
        return "EXCLUDED", result_norm
    if result_norm == "WEATHER_SURVIVAL_STOP":
        return "EXCLUDED", "WEATHER_SURVIVAL_STOP"
    if not result_norm:
        return "EXCLUDED", "MISSING_RESULT"
    return "EXCLUDED", f"UNMAPPED_RESULT_{result_norm}"


def _build_comparator_dataset(
    suggestions: pd.DataFrame,
    pit_evals: pd.DataFrame,
    horizon: int,
) -> pd.DataFrame:
    deduped = _dedup_suggestions(suggestions)
    evals = _prepare_pit_evals(pit_evals)

    actionable = deduped[deduped["label_norm"].isin(ACTIONABLE_LABELS)].copy()

    grouped_evals = {key: grp.copy() for key, grp in evals.groupby(["race", "driver"], sort=False)}

    used_eval_ids: set[int] = set()
    rows: list[dict[str, object]] = []

    # this horizon reasoning is from brookshire 2024 and roberts et al 2017,
    # each decision at lap k can only consume outcomes in [k, k plus horizon].
    for _, decision in actionable.iterrows():
        race = str(decision["race"])
        driver = str(decision["driver"])
        lap = int(decision["lap_num"])
        candidates_df = grouped_evals.get((race, driver))

        matched_pit_lap: int | None = None
        nearest_future_pit_lap: int | None = None
        nearest_future_pit_distance: int | None = None
        pit_in_window_before_consumption = False
        outcome_class = "EXCLUDED"
        exclusion_reason = "NO_MATCH_WITHIN_HORIZON"

        if candidates_df is not None and not candidates_df.empty:
            future = candidates_df[candidates_df["pit_lap_num"] >= lap]
            if not future.empty:
                nearest_future_pit_lap = int(future.iloc[0]["pit_lap_num"])
                nearest_future_pit_distance = nearest_future_pit_lap - lap

            window_all = candidates_df[
                (candidates_df["pit_lap_num"] >= lap)
                & (candidates_df["pit_lap_num"] <= (lap + horizon))
            ]
            pit_in_window_before_consumption = not window_all.empty

            window = candidates_df[
                (candidates_df["pit_lap_num"] >= lap)
                & (candidates_df["pit_lap_num"] <= (lap + horizon))
                & (~candidates_df["_eval_id"].isin(used_eval_ids))
            ]

            if not window.empty:
                matched = window.iloc[0]
                matched_pit_lap = int(matched["pit_lap_num"])
                used_eval_ids.add(int(matched["_eval_id"]))
                outcome_class, exclusion_reason = _map_outcome(str(matched["result_norm"]))

        rows.append(
            {
                "race": race,
                "driver": driver,
                "suggestion_lap": lap,
                "suggestion_label": str(decision["suggestionLabel"]),
                "totalScore": float(decision["score_num"])
                if decision["score_num"] != float("-inf")
                else None,
                "matched_pit_lap": matched_pit_lap,
                "match_distance": (matched_pit_lap - lap) if matched_pit_lap is not None else None,
                "nearest_future_pit_lap": nearest_future_pit_lap,
                "nearest_future_pit_distance": nearest_future_pit_distance,
                "pit_in_window_before_consumption": pit_in_window_before_consumption,
                "outcome_class": outcome_class,
                "exclusion_reason": exclusion_reason,
            }
        )

    result = pd.DataFrame(rows)

    if not result.empty:
        matched = result[result["matched_pit_lap"].notna()].copy()
        if not matched.empty:
            dup = (
                matched.groupby(["race", "driver", "matched_pit_lap"], dropna=False)
                .size()
                .reset_index(name="count")
            )
            if int((dup["count"] > 1).sum()) > 0:
                raise RuntimeError("one to one mapping violated, at least one target was matched multiple times")

    return result


def _print_summary(dataset: pd.DataFrame, suggestions: pd.DataFrame, horizon: int) -> None:
    deduped = _dedup_suggestions(suggestions)
    actionable_total = int(deduped[deduped["label_norm"].isin(ACTIONABLE_LABELS)].shape[0])

    scored = dataset[dataset["outcome_class"].isin(["1", "0"])]
    excluded = dataset[dataset["outcome_class"] == "EXCLUDED"]

    tp = int((scored["outcome_class"] == "1").sum())
    fp = int((scored["outcome_class"] == "0").sum())
    precision = (tp / (tp + fp)) if (tp + fp) else 0.0

    print("=== HEURISTIC COMPARATOR SUMMARY ===")
    print(f"deduped suggestions rows         : {len(deduped)}")
    print(f"actionable suggestions rows      : {actionable_total}")
    print(f"scored comparator rows           : {len(scored)}")
    print(f"excluded comparator rows         : {len(excluded)}")
    print(f"matching horizon                 : {horizon} laps")

    print("\nconfusion matrix, actionable only")
    print(f"true_positive  : {tp}")
    print(f"false_positive : {fp}")
    print("true_negative  : N/A")
    print("false_negative : N/A")
    print(f"precision      : {precision:.4f}")

    if not excluded.empty:
        reason_counts = excluded["exclusion_reason"].fillna("MISSING_REASON").value_counts()
        print("\nexcluded reason distribution")
        for reason, count in reason_counts.items():
            print(f"{reason}: {int(count)}")

        no_match = excluded[excluded["exclusion_reason"] == "NO_MATCH_WITHIN_HORIZON"].copy()
        if not no_match.empty and "nearest_future_pit_distance" in no_match.columns:
            lead = pd.to_numeric(no_match["nearest_future_pit_distance"], errors="coerce")
            no_future = int(lead.isna().sum())
            valid_lead = lead.dropna().astype(int)

            print("\nno-match timing diagnostics")
            print(f"no-match rows                  : {len(no_match)}")
            print(f"no future pit after suggestion : {no_future}")
            if not valid_lead.empty:
                print(
                    "exactly one lap beyond horizon: "
                    f"{int((valid_lead == (horizon + 1)).sum())}"
                )
                print(
                    "two to three laps beyond      : "
                    f"{int(((valid_lead >= (horizon + 2)) & (valid_lead <= (horizon + 3))).sum())}"
                )
                print(
                    "four or more laps beyond      : "
                    f"{int((valid_lead >= (horizon + 4)).sum())}"
                )

            if "pit_in_window_before_consumption" in no_match.columns:
                consumed = int(no_match["pit_in_window_before_consumption"].fillna(False).sum())
                print(f"pit in window but already used : {consumed}")

        matched_but_excluded = excluded[excluded["matched_pit_lap"].notna()].copy()
        if not matched_but_excluded.empty:
            print("\nmatched-but-excluded diagnostics")
            print(f"rows with matched pit lap       : {len(matched_but_excluded)}")
            matched_reason = matched_but_excluded["exclusion_reason"].fillna("MISSING_REASON").value_counts()
            for reason, count in matched_reason.items():
                print(f"{reason}: {int(count)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="build phase 2 heuristic comparator dataset")
    parser.add_argument("--data-lake", default=DEFAULT_DATA_LAKE, help="data lake directory")
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR, help="season year")
    parser.add_argument("--season-tag", default=DEFAULT_SEASON_TAG, help="season tag token")
    parser.add_argument("--horizon", type=int, default=DEFAULT_HORIZON, help="look ahead horizon in laps")
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="output csv name or absolute path",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_lake = Path(args.data_lake)

    suggestions_path = _latest_jsonl(data_lake, "pit_suggestions", args.year, args.season_tag)
    pit_evals_path = _latest_jsonl(data_lake, "pit_evals", args.year, args.season_tag)

    suggestions = _load_jsonl(suggestions_path)
    pit_evals = _load_jsonl(pit_evals_path)

    comparator = _build_comparator_dataset(suggestions, pit_evals, args.horizon)

    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = data_lake / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    comparator.to_csv(output_path, index=False)

    print(f"suggestions input: {suggestions_path}")
    print(f"pit evals input  : {pit_evals_path}")
    print(f"output csv       : {output_path}")
    _print_summary(comparator, suggestions, args.horizon)


if __name__ == "__main__":
    main()
