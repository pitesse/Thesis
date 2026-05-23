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

ACTIONABLE_MODE_PIT_NOW_ONLY = "pit_now_only"
ACTIONABLE_MODE_GOOD_PIT_ONLY = "good_pit_only"
ACTIONABLE_MODE_PIT_NOW_PLUS_GOOD_PIT = "pit_now_plus_good_pit"
ACTIONABLE_MODES = {
    ACTIONABLE_MODE_PIT_NOW_ONLY,
    ACTIONABLE_MODE_GOOD_PIT_ONLY,
    ACTIONABLE_MODE_PIT_NOW_PLUS_GOOD_PIT,
}
DEFAULT_ACTIONABLE_MODE = ACTIONABLE_MODE_PIT_NOW_PLUS_GOOD_PIT

DEFAULT_EPISODE_COOLDOWN_LAPS = 5

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

OUTCOME_PIT_SUCCESS_H2 = "pit_success_h2"
OUTCOME_PIT_ANY_H2 = "pit_any_h2"
OUTCOME_MODES = {OUTCOME_PIT_SUCCESS_H2, OUTCOME_PIT_ANY_H2}


def _resolve_actionable_labels(actionable_mode: str) -> set[str]:
    if actionable_mode == ACTIONABLE_MODE_PIT_NOW_ONLY:
        return {"PIT_NOW"}
    if actionable_mode == ACTIONABLE_MODE_GOOD_PIT_ONLY:
        return {"GOOD_PIT"}
    if actionable_mode == ACTIONABLE_MODE_PIT_NOW_PLUS_GOOD_PIT:
        return {"PIT_NOW", "GOOD_PIT"}
    raise ValueError(
        f"unsupported actionable_mode={actionable_mode!r}; expected one of {sorted(ACTIONABLE_MODES)}"
    )


def _empty_comparator_dataset() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "race",
            "driver",
            "suggestion_lap",
            "suggestion_label",
            "totalScore",
            "trackStatus",
            "matched_pit_lap",
            "match_distance",
            "nearest_future_pit_lap",
            "nearest_future_pit_distance",
            "pit_in_window_before_consumption",
            "outcome_class",
            "exclusion_reason",
        ]
    )


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


def _prepare_pit_timings(pit_timings: pd.DataFrame) -> pd.DataFrame:
    _require_columns(
        pit_timings,
        ["race", "driver", "lapNumber", "pitInTime", "pitOutTime"],
        "pit_timings",
    )

    work = pit_timings.copy()
    work["pit_lap_num"] = pd.to_numeric(work["lapNumber"], errors="coerce")
    work = work[work["pit_lap_num"].notna()].copy()
    work["pit_lap_num"] = work["pit_lap_num"].astype(int)
    work["pitInTime"] = pd.to_numeric(work["pitInTime"], errors="coerce")
    work["pitOutTime"] = pd.to_numeric(work["pitOutTime"], errors="coerce")
    work["_eval_id"] = range(len(work))

    work.sort_values(
        by=["race", "driver", "pit_lap_num", "_eval_id"],
        ascending=[True, True, True, True],
        inplace=True,
    )
    work.reset_index(drop=True, inplace=True)
    return work


def _extract_eval_lap_arrays(evals: pd.DataFrame) -> dict[tuple[str, str], list[int]]:
    grouped: dict[tuple[str, str], list[int]] = {}
    for (race, driver), grp in evals.groupby(["race", "driver"], sort=False):
        grouped[(str(race), str(driver))] = [
            int(value)
            for value in pd.to_numeric(grp["pit_lap_num"], errors="coerce").dropna().astype(int).tolist()
        ]
    return grouped


def _build_episode_actionable(
    actionable: pd.DataFrame,
    pit_laps_by_driver: dict[tuple[str, str], list[int]],
    episode_cooldown_laps: int,
) -> pd.DataFrame:
    if actionable.empty:
        return actionable.copy()

    rows: list[pd.Series] = []
    for (race, driver), grp in actionable.groupby(["race", "driver"], sort=False):
        work = grp.sort_values(by=["lap_num", "priority_rank", "score_num"], ascending=[True, False, False]).copy()
        pit_laps = pit_laps_by_driver.get((str(race), str(driver)), [])

        episode_active = False
        episode_start = -1
        episode_expiry = -1
        pit_index = 0

        for _, row in work.iterrows():
            lap = int(row["lap_num"])

            if episode_active:
                while pit_index < len(pit_laps) and pit_laps[pit_index] < episode_start:
                    pit_index += 1

                pit_closes_episode = (
                    pit_index < len(pit_laps)
                    and pit_laps[pit_index] >= episode_start
                    and pit_laps[pit_index] <= lap
                )
                horizon_expired = lap >= episode_expiry

                if pit_closes_episode or horizon_expired:
                    episode_active = False
                    if pit_closes_episode:
                        pit_index += 1

            if not episode_active:
                rows.append(row)
                episode_active = True
                episode_start = lap
                episode_expiry = lap + episode_cooldown_laps

    if not rows:
        return actionable.iloc[0:0].copy()

    episode_df = pd.DataFrame(rows)
    episode_df.sort_values(
        by=["race", "driver", "lap_num", "priority_rank", "score_num", "_row_id"],
        ascending=[True, True, True, False, False, True],
        inplace=True,
    )
    episode_df.reset_index(drop=True, inplace=True)
    return episode_df


def _map_outcome(result_norm: str) -> tuple[str, str]:
    if result_norm in POSITIVE_RESULTS:
        return "1", ""
    if result_norm in NEGATIVE_RESULTS:
        return "0", ""
    # unresolved and weather cases stay excluded so binary labels remain audit-safe.
    if result_norm.startswith("UNRESOLVED_"):
        return "EXCLUDED", result_norm
    if result_norm == "WEATHER_SURVIVAL_STOP":
        return "EXCLUDED", "WEATHER_SURVIVAL_STOP"
    if not result_norm:
        return "EXCLUDED", "MISSING_RESULT"
    return "EXCLUDED", f"UNMAPPED_RESULT_{result_norm}"


def _match_actionable_to_outcomes(
    actionable: pd.DataFrame,
    evals: pd.DataFrame,
    horizon: int,
    *,
    outcome_mode: str,
    include_same_lap: bool = True,
    pit_success_no_match_as_negative: bool = True,
) -> pd.DataFrame:
    if actionable.empty:
        return _empty_comparator_dataset()

    grouped_evals = {key: grp.copy() for key, grp in evals.groupby(["race", "driver"], sort=False)}

    # consume each pit evaluation at most once across the full comparator stream.
    used_eval_ids: set[int] = set()
    rows: list[dict[str, object]] = []

    for _, decision in actionable.iterrows():
        race = str(decision["race"])
        driver = str(decision["driver"])
        lap = int(decision["lap_num"])
        window_start = lap if include_same_lap else (lap + 1)
        candidates_df = grouped_evals.get((race, driver))

        matched_pit_lap: int | None = None
        nearest_future_pit_lap: int | None = None
        nearest_future_pit_distance: int | None = None
        pit_in_window_before_consumption = False
        outcome_class = "EXCLUDED"
        exclusion_reason = "NO_MATCH_WITHIN_HORIZON"

        if candidates_df is not None and not candidates_df.empty:
            future = candidates_df[candidates_df["pit_lap_num"] >= window_start]
            if not future.empty:
                nearest_future_pit_lap = int(future.iloc[0]["pit_lap_num"])
                nearest_future_pit_distance = nearest_future_pit_lap - lap

            window_all = candidates_df[
                (candidates_df["pit_lap_num"] >= window_start)
                & (candidates_df["pit_lap_num"] <= (lap + horizon))
            ]
            pit_in_window_before_consumption = not window_all.empty

            # keep one-to-one pairing by dropping targets already consumed by earlier rows.
            window = candidates_df[
                (candidates_df["pit_lap_num"] >= window_start)
                & (candidates_df["pit_lap_num"] <= (lap + horizon))
                & (~candidates_df["_eval_id"].isin(used_eval_ids))
            ]

            if not window.empty:
                matched = window.iloc[0]
                matched_pit_lap = int(matched["pit_lap_num"])
                used_eval_ids.add(int(matched["_eval_id"]))
                if outcome_mode == OUTCOME_PIT_SUCCESS_H2:
                    outcome_class, exclusion_reason = _map_outcome(str(matched["result_norm"]))
                else:
                    outcome_class, exclusion_reason = "1", ""

        if matched_pit_lap is None:
            if outcome_mode == OUTCOME_PIT_ANY_H2:
                outcome_class = "0"
                exclusion_reason = ""
            elif outcome_mode == OUTCOME_PIT_SUCCESS_H2 and pit_success_no_match_as_negative:
                # Strict operational pit_success contract:
                # no matched pit in horizon is a false positive prediction.
                outcome_class = "0"
                # Keep diagnostic reason so downstream audits can count no-match FPs.
                exclusion_reason = "NO_MATCH_WITHIN_HORIZON"

        rows.append(
            {
                "race": race,
                "driver": driver,
                "suggestion_lap": lap,
                "suggestion_label": str(decision["suggestionLabel"]),
                "totalScore": float(decision["score_num"])
                if decision["score_num"] != float("-inf")
                else None,
                "trackStatus": str(decision.get("trackStatus", "")).strip(),
                "matched_pit_lap": matched_pit_lap,
                "match_distance": (matched_pit_lap - lap) if matched_pit_lap is not None else None,
                "nearest_future_pit_lap": nearest_future_pit_lap,
                "nearest_future_pit_distance": nearest_future_pit_distance,
                "pit_in_window_before_consumption": pit_in_window_before_consumption,
                "outcome_class": outcome_class,
                "exclusion_reason": exclusion_reason,
            }
        )

    return pd.DataFrame(rows)


def _build_comparator_dataset(
    suggestions: pd.DataFrame,
    pit_evals: pd.DataFrame,
    horizon: int,
    *,
    outcome_mode: str = OUTCOME_PIT_SUCCESS_H2,
    pit_timings: pd.DataFrame | None = None,
    actionable_mode: str = DEFAULT_ACTIONABLE_MODE,
    episode_level: bool = False,
    episode_cooldown_laps: int = DEFAULT_EPISODE_COOLDOWN_LAPS,
    include_same_lap: bool = True,
    pit_success_no_match_as_negative: bool = True,
) -> pd.DataFrame:
    deduped = _dedup_suggestions(suggestions)
    if outcome_mode not in OUTCOME_MODES:
        raise ValueError(
            f"unsupported outcome_mode={outcome_mode!r}; expected one of {sorted(OUTCOME_MODES)}"
        )
    actionable_labels = _resolve_actionable_labels(actionable_mode)
    if episode_cooldown_laps < 1:
        raise ValueError("episode_cooldown_laps must be >= 1")

    if outcome_mode == OUTCOME_PIT_SUCCESS_H2:
        evals = _prepare_pit_evals(pit_evals)
    else:
        if pit_timings is None:
            raise ValueError("pit_timings is required for outcome_mode=pit_any_h2")
        evals = _prepare_pit_timings(pit_timings)

    actionable = deduped[deduped["label_norm"].isin(actionable_labels)].copy()
    if episode_level:
        pit_laps_by_driver = _extract_eval_lap_arrays(evals)
        actionable = _build_episode_actionable(
            actionable,
            pit_laps_by_driver,
            episode_cooldown_laps=episode_cooldown_laps,
        )

    result = _match_actionable_to_outcomes(
        actionable,
        evals,
        horizon,
        outcome_mode=outcome_mode,
        include_same_lap=include_same_lap,
        pit_success_no_match_as_negative=pit_success_no_match_as_negative,
    )

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


def _print_summary(
    dataset: pd.DataFrame,
    suggestions: pd.DataFrame,
    horizon: int,
    *,
    comparator_view: str = "row_level",
    actionable_mode: str = DEFAULT_ACTIONABLE_MODE,
) -> None:
    deduped = _dedup_suggestions(suggestions)
    actionable_labels = _resolve_actionable_labels(actionable_mode)
    actionable_total_raw = int(deduped[deduped["label_norm"].isin(actionable_labels)].shape[0])
    actionable_total = int(len(dataset))

    scored = dataset[dataset["outcome_class"].isin(["1", "0"])]
    excluded = dataset[dataset["outcome_class"] == "EXCLUDED"]

    tp = int((scored["outcome_class"] == "1").sum())
    fp = int((scored["outcome_class"] == "0").sum())
    precision = (tp / (tp + fp)) if (tp + fp) else 0.0

    print("=== HEURISTIC COMPARATOR SUMMARY ===")
    print(f"comparator view                 : {comparator_view}")
    print(f"actionable mode                 : {actionable_mode}")
    print(f"deduped suggestions rows         : {len(deduped)}")
    print(f"actionable suggestions rows      : {actionable_total}")
    print(f"raw actionable before view gate  : {actionable_total_raw}")
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

        no_match = dataset[dataset["exclusion_reason"] == "NO_MATCH_WITHIN_HORIZON"].copy()
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
        "--actionable-mode",
        choices=sorted(ACTIONABLE_MODES),
        default=DEFAULT_ACTIONABLE_MODE,
        help="which suggestion labels are treated as actionable for comparator scoring",
    )
    parser.add_argument(
        "--episode-cooldown-laps",
        type=int,
        default=DEFAULT_EPISODE_COOLDOWN_LAPS,
        help="episode suppression cooldown in laps (independent from scoring horizon)",
    )
    parser.add_argument(
        "--strict-future",
        action="store_true",
        help="diagnostic mode only: match in [suggestion_lap+1, suggestion_lap+h] instead of including same lap",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="output csv name or absolute path",
    )
    parser.add_argument(
        "--outcome-mode",
        choices=sorted(OUTCOME_MODES),
        default=OUTCOME_PIT_SUCCESS_H2,
        help="comparator outcome contract: success-only pits or any pit timing in window",
    )
    parser.add_argument(
        "--pit-timings",
        default="",
        help="optional pit_timings jsonl path (required for --outcome-mode pit_any_h2)",
    )
    parser.add_argument(
        "--episode-output",
        default="",
        help="optional output csv for episode-level comparator view",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_lake = Path(args.data_lake)

    suggestions_path = _latest_jsonl(data_lake, "pit_suggestions", args.year, args.season_tag)
    pit_evals_path = _latest_jsonl(data_lake, "pit_evals", args.year, args.season_tag)

    suggestions = _load_jsonl(suggestions_path)
    pit_evals = _load_jsonl(pit_evals_path)
    pit_timings_path: Path | None = None
    pit_timings_df: pd.DataFrame | None = None
    if args.outcome_mode == OUTCOME_PIT_ANY_H2:
        pit_timings_path = (
            Path(args.pit_timings)
            if args.pit_timings
            else _latest_jsonl(data_lake, "pit_timings", args.year, args.season_tag)
        )
        pit_timings_df = _load_jsonl(pit_timings_path)

    comparator = _build_comparator_dataset(
        suggestions,
        pit_evals,
        args.horizon,
        outcome_mode=args.outcome_mode,
        pit_timings=pit_timings_df,
        actionable_mode=args.actionable_mode,
        include_same_lap=not args.strict_future,
    )

    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = data_lake / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    comparator.to_csv(output_path, index=False)

    episode_output_path: Path | None = None
    episode_comparator: pd.DataFrame | None = None
    if args.episode_output:
        episode_comparator = _build_comparator_dataset(
            suggestions,
            pit_evals,
            args.horizon,
            outcome_mode=args.outcome_mode,
            pit_timings=pit_timings_df,
            actionable_mode=args.actionable_mode,
            episode_level=True,
            episode_cooldown_laps=args.episode_cooldown_laps,
            include_same_lap=not args.strict_future,
        )
        episode_output_path = Path(args.episode_output)
        if not episode_output_path.is_absolute():
            episode_output_path = data_lake / episode_output_path
        episode_output_path.parent.mkdir(parents=True, exist_ok=True)
        episode_comparator.to_csv(episode_output_path, index=False)

    print(f"suggestions input: {suggestions_path}")
    print(f"pit evals input  : {pit_evals_path}")
    if pit_timings_path is not None:
        print(f"pit timings input: {pit_timings_path}")
    print(f"outcome mode     : {args.outcome_mode}")
    print(f"actionable mode  : {args.actionable_mode}")
    print(f"strict future    : {args.strict_future}")
    print(f"output csv       : {output_path}")
    _print_summary(
        comparator,
        suggestions,
        args.horizon,
        comparator_view="row_level",
        actionable_mode=args.actionable_mode,
    )
    if episode_output_path is not None and episode_comparator is not None:
        print(f"episode csv      : {episode_output_path}")
        _print_summary(
            episode_comparator,
            suggestions,
            args.horizon,
            comparator_view="episode_level",
            actionable_mode=args.actionable_mode,
        )


if __name__ == "__main__":
    main()
