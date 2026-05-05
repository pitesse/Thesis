"""Generate an SDE upgrade report with row-level and episode-level diagnostics.

This script is artifact-only: it reads comparator outputs and pit suggestions,
then emits side-by-side metrics and deltas for upgrade analysis.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

SCORED_CLASSES = {"0", "1"}


def _load_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"{label} is empty: {path}")
    return df


def _normalize_comparator(df: pd.DataFrame, label: str) -> pd.DataFrame:
    required = {
        "race",
        "driver",
        "suggestion_lap",
        "outcome_class",
        "exclusion_reason",
    }
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(f"{label} missing columns: {missing}")

    work = df.copy()
    work["race"] = work["race"].astype(str)
    work["driver"] = work["driver"].astype(str)
    work["suggestion_lap"] = pd.to_numeric(work["suggestion_lap"], errors="coerce")
    work = work[work["suggestion_lap"].notna()].copy()
    work["suggestion_lap"] = work["suggestion_lap"].astype(int)
    work["outcome_class"] = work["outcome_class"].astype(str)
    work["exclusion_reason"] = work["exclusion_reason"].fillna("").astype(str)
    return work


def _comparator_metrics(df: pd.DataFrame) -> dict[str, float | int]:
    actionable = int(len(df))
    scored = df[df["outcome_class"].isin(SCORED_CLASSES)].copy()
    scored_n = int(len(scored))
    tp = int((scored["outcome_class"] == "1").sum())
    fp = int((scored["outcome_class"] == "0").sum())
    no_match = int((df["exclusion_reason"] == "NO_MATCH_WITHIN_HORIZON").sum())

    precision = (tp / scored_n) if scored_n else 0.0
    coverage = (scored_n / actionable) if actionable else 0.0
    no_match_rate = (no_match / actionable) if actionable else 0.0

    return {
        "actionable": actionable,
        "scored": scored_n,
        "tp": tp,
        "fp": fp,
        "precision": float(precision),
        "coverage": float(coverage),
        "no_match_rows": no_match,
        "no_match_rate": float(no_match_rate),
    }


def _metrics_by_race(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for race, grp in df.groupby("race", sort=True):
        m = _comparator_metrics(grp)
        rows.append({"race": race, **m})
    return pd.DataFrame(rows)


def _load_pit_suggestions(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"pit suggestions file not found: {path}")
    df = pd.read_json(path, lines=True)
    if df.empty:
        raise ValueError(f"pit suggestions file is empty: {path}")

    required = {"race", "driver", "lapNumber", "trackStatus", "suggestionLabel", "totalScore"}
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(f"pit suggestions missing columns: {missing}")

    work = df.copy()
    work["race"] = work["race"].astype(str)
    work["driver"] = work["driver"].astype(str)
    work["lapNumber"] = pd.to_numeric(work["lapNumber"], errors="coerce")
    work = work[work["lapNumber"].notna()].copy()
    work["lapNumber"] = work["lapNumber"].astype(int)
    work["trackStatus"] = work["trackStatus"].fillna("UNKNOWN").astype(str)
    work["suggestionLabel"] = work["suggestionLabel"].fillna("").astype(str)
    work["totalScore"] = pd.to_numeric(work["totalScore"], errors="coerce").fillna(float("-inf"))

    # mirror comparator heuristic dedup logic at key level.
    work["priority"] = work["suggestionLabel"].map({"PIT_NOW": 4, "GOOD_PIT": 3, "LOST_CHANCE": 2, "MONITOR": 1}).fillna(0)
    work.sort_values(
        by=["race", "driver", "lapNumber", "priority", "totalScore"],
        ascending=[True, True, True, False, False],
        inplace=True,
    )
    dedup = work.drop_duplicates(subset=["race", "driver", "lapNumber"], keep="first").copy()
    return dedup[["race", "driver", "lapNumber", "trackStatus"]]


def _metrics_by_track_status(comparator_df: pd.DataFrame, suggestions_df: pd.DataFrame) -> pd.DataFrame:
    joined = comparator_df.merge(
        suggestions_df,
        how="left",
        left_on=["race", "driver", "suggestion_lap"],
        right_on=["race", "driver", "lapNumber"],
    )
    joined["trackStatus"] = joined["trackStatus"].fillna("UNKNOWN")

    rows: list[dict[str, object]] = []
    for status, grp in joined.groupby("trackStatus", sort=True):
        m = _comparator_metrics(grp)
        rows.append({"trackStatus": status, **m})
    return pd.DataFrame(rows)


def _metrics_table_rows(
    before_row: pd.DataFrame,
    after_row: pd.DataFrame,
    before_episode: pd.DataFrame | None,
    after_episode: pd.DataFrame | None,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    br = _comparator_metrics(before_row)
    ar = _comparator_metrics(after_row)
    rows.append({"view": "row_level_before", **br})
    rows.append({"view": "row_level_after", **ar})
    rows.append(
        {
            "view": "row_level_delta_after_minus_before",
            "actionable": ar["actionable"] - br["actionable"],
            "scored": ar["scored"] - br["scored"],
            "tp": ar["tp"] - br["tp"],
            "fp": ar["fp"] - br["fp"],
            "precision": ar["precision"] - br["precision"],
            "coverage": ar["coverage"] - br["coverage"],
            "no_match_rows": ar["no_match_rows"] - br["no_match_rows"],
            "no_match_rate": ar["no_match_rate"] - br["no_match_rate"],
        }
    )

    if before_episode is not None and after_episode is not None:
        be = _comparator_metrics(before_episode)
        ae = _comparator_metrics(after_episode)
        rows.append({"view": "episode_level_before", **be})
        rows.append({"view": "episode_level_after", **ae})
        rows.append(
            {
                "view": "episode_level_delta_after_minus_before",
                "actionable": ae["actionable"] - be["actionable"],
                "scored": ae["scored"] - be["scored"],
                "tp": ae["tp"] - be["tp"],
                "fp": ae["fp"] - be["fp"],
                "precision": ae["precision"] - be["precision"],
                "coverage": ae["coverage"] - be["coverage"],
                "no_match_rows": ae["no_match_rows"] - be["no_match_rows"],
                "no_match_rate": ae["no_match_rate"] - be["no_match_rate"],
            }
        )

    return pd.DataFrame(rows)


def _fmt_float(value: object, digits: int = 6) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "N/A"
    if pd.isna(number):
        return "N/A"
    return f"{number:.{digits}f}"


def _build_markdown(
    summary_df: pd.DataFrame,
    race_delta_df: pd.DataFrame,
    status_before_df: pd.DataFrame,
    status_after_df: pd.DataFrame,
    output_paths: dict[str, Path],
) -> str:
    now = datetime.now(timezone.utc).isoformat()
    lines: list[str] = []
    lines.append("# SDE Precision Upgrade Report")
    lines.append("")
    lines.append(f"Generated at (UTC): {now}")
    lines.append("")
    lines.append("## Global Metrics")
    lines.append("| View | Actionable | Scored | TP | FP | Precision | Coverage | NO_MATCH rows | NO_MATCH rate |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for _, row in summary_df.iterrows():
        lines.append(
            "| {view} | {actionable} | {scored} | {tp} | {fp} | {precision} | {coverage} | {no_match_rows} | {no_match_rate} |".format(
                view=row["view"],
                actionable=int(row["actionable"]),
                scored=int(row["scored"]),
                tp=int(row["tp"]),
                fp=int(row["fp"]),
                precision=_fmt_float(row["precision"], 6),
                coverage=_fmt_float(row["coverage"], 6),
                no_match_rows=int(row["no_match_rows"]),
                no_match_rate=_fmt_float(row["no_match_rate"], 6),
            )
        )

    lines.append("")
    lines.append("## Per-Race Delta (After - Before, Row-Level)")
    lines.append("| Race | Precision Delta | Coverage Delta | NO_MATCH rate Delta | Scored Delta | TP Delta | FP Delta |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for _, row in race_delta_df.iterrows():
        lines.append(
            "| {race} | {p} | {c} | {n} | {s} | {tp} | {fp} |".format(
                race=row["race"],
                p=_fmt_float(row["precision_delta"], 6),
                c=_fmt_float(row["coverage_delta"], 6),
                n=_fmt_float(row["no_match_rate_delta"], 6),
                s=int(row["scored_delta"]),
                tp=int(row["tp_delta"]),
                fp=int(row["fp_delta"]),
            )
        )

    lines.append("")
    lines.append("## Track-Status Breakdown (Row-Level)")
    lines.append("| TrackStatus | Before Precision | After Precision | Before Coverage | After Coverage | Before NO_MATCH rate | After NO_MATCH rate |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    merged_status = status_before_df.merge(
        status_after_df,
        how="outer",
        on="trackStatus",
        suffixes=("_before", "_after"),
    ).fillna(0)
    for _, row in merged_status.iterrows():
        lines.append(
            "| {status} | {bp} | {ap} | {bc} | {ac} | {bn} | {an} |".format(
                status=row["trackStatus"],
                bp=_fmt_float(row["precision_before"], 6),
                ap=_fmt_float(row["precision_after"], 6),
                bc=_fmt_float(row["coverage_before"], 6),
                ac=_fmt_float(row["coverage_after"], 6),
                bn=_fmt_float(row["no_match_rate_before"], 6),
                an=_fmt_float(row["no_match_rate_after"], 6),
            )
        )

    lines.append("")
    lines.append("## Mathematical Definitions")
    lines.append("- Episode gate: for driver d, episode starts at first actionable lap k; no new actionable is opened until pit lap p >= k or expiry lap k+H (H=2).")
    lines.append("- Row-level precision = TP / (TP + FP) on scored rows only.")
    lines.append("- Coverage = scored / actionable.")
    lines.append("- NO_MATCH rate = count(exclusion_reason == NO_MATCH_WITHIN_HORIZON) / actionable.")

    lines.append("")
    lines.append("## References")
    lines.append("- FastF1 API semantics: https://docs.fastf1.dev/api.html")
    lines.append("- FastF1 timing caveats: https://docs.fastf1.dev/time_explanation.html")
    lines.append("- EJOR 2024 competition-aware strategy: https://doi.org/10.1016/j.ejor.2024.07.011")
    lines.append("- IFAC 2020 tire-management control framing: https://doi.org/10.1016/j.ifacol.2020.12.1446")
    lines.append("- State-space degradation model (2026): https://doi.org/10.1177/22150218261446170")
    lines.append("- Discrete-event strategy planning precedent: https://doi.org/10.1057/palgrave.jors.2602626")

    lines.append("")
    lines.append("## Artifact Paths")
    lines.append(f"- Summary CSV: {output_paths['summary_csv']}")
    lines.append(f"- Race Delta CSV: {output_paths['race_delta_csv']}")
    lines.append(f"- Track Status CSV: {output_paths['status_csv']}")

    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="generate SDE precision-upgrade report")
    parser.add_argument("--before-row", required=True, help="row-level comparator csv before upgrade")
    parser.add_argument("--after-row", required=True, help="row-level comparator csv after upgrade")
    parser.add_argument("--before-episode", default="", help="optional episode-level comparator csv before")
    parser.add_argument("--after-episode", default="", help="optional episode-level comparator csv after")
    parser.add_argument("--pit-suggestions", required=True, help="pit_suggestions jsonl path for status breakdown")
    parser.add_argument("--output-md", required=True, help="output markdown report path")
    parser.add_argument("--summary-csv", required=True, help="output summary csv path")
    parser.add_argument("--race-delta-csv", required=True, help="output race-level delta csv path")
    parser.add_argument("--status-csv", required=True, help="output track-status metrics csv path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    before_row = _normalize_comparator(_load_csv(Path(args.before_row), "before row comparator"), "before row comparator")
    after_row = _normalize_comparator(_load_csv(Path(args.after_row), "after row comparator"), "after row comparator")

    before_episode: pd.DataFrame | None = None
    after_episode: pd.DataFrame | None = None
    if args.before_episode and args.after_episode:
        before_episode = _normalize_comparator(
            _load_csv(Path(args.before_episode), "before episode comparator"),
            "before episode comparator",
        )
        after_episode = _normalize_comparator(
            _load_csv(Path(args.after_episode), "after episode comparator"),
            "after episode comparator",
        )

    suggestions = _load_pit_suggestions(Path(args.pit_suggestions))

    summary_df = _metrics_table_rows(before_row, after_row, before_episode, after_episode)

    before_race = _metrics_by_race(before_row)
    after_race = _metrics_by_race(after_row)
    race_delta = before_race.merge(after_race, on="race", suffixes=("_before", "_after"), how="outer").fillna(0)
    race_delta_df = pd.DataFrame(
        {
            "race": race_delta["race"],
            "precision_delta": race_delta["precision_after"] - race_delta["precision_before"],
            "coverage_delta": race_delta["coverage_after"] - race_delta["coverage_before"],
            "no_match_rate_delta": race_delta["no_match_rate_after"] - race_delta["no_match_rate_before"],
            "scored_delta": race_delta["scored_after"] - race_delta["scored_before"],
            "tp_delta": race_delta["tp_after"] - race_delta["tp_before"],
            "fp_delta": race_delta["fp_after"] - race_delta["fp_before"],
        }
    ).sort_values(by="precision_delta", ascending=False)

    status_before = _metrics_by_track_status(before_row, suggestions)
    status_after = _metrics_by_track_status(after_row, suggestions)

    summary_path = Path(args.summary_csv)
    race_delta_path = Path(args.race_delta_csv)
    status_path = Path(args.status_csv)
    md_path = Path(args.output_md)

    for path in (summary_path, race_delta_path, status_path, md_path):
        path.parent.mkdir(parents=True, exist_ok=True)

    summary_df.to_csv(summary_path, index=False)
    race_delta_df.to_csv(race_delta_path, index=False)

    status_before_out = status_before.rename(
        columns={
            "actionable": "actionable_before",
            "scored": "scored_before",
            "tp": "tp_before",
            "fp": "fp_before",
            "precision": "precision_before",
            "coverage": "coverage_before",
            "no_match_rows": "no_match_rows_before",
            "no_match_rate": "no_match_rate_before",
        }
    )
    status_after_out = status_after.rename(
        columns={
            "actionable": "actionable_after",
            "scored": "scored_after",
            "tp": "tp_after",
            "fp": "fp_after",
            "precision": "precision_after",
            "coverage": "coverage_after",
            "no_match_rows": "no_match_rows_after",
            "no_match_rate": "no_match_rate_after",
        }
    )
    status_merged = status_before_out.merge(status_after_out, on="trackStatus", how="outer").fillna(0)
    status_merged.to_csv(status_path, index=False)

    report = _build_markdown(
        summary_df=summary_df,
        race_delta_df=race_delta_df,
        status_before_df=status_before_out,
        status_after_df=status_after_out,
        output_paths={
            "summary_csv": summary_path,
            "race_delta_csv": race_delta_path,
            "status_csv": status_path,
        },
    )
    md_path.write_text(report, encoding="utf-8")

    print("=== SDE UPGRADE REPORT ===")
    print(f"before row comparator : {Path(args.before_row)}")
    print(f"after row comparator  : {Path(args.after_row)}")
    if args.before_episode and args.after_episode:
        print(f"before episode cmp    : {Path(args.before_episode)}")
        print(f"after episode cmp     : {Path(args.after_episode)}")
    print(f"pit suggestions       : {Path(args.pit_suggestions)}")
    print(f"summary csv           : {summary_path}")
    print(f"race delta csv        : {race_delta_path}")
    print(f"status csv            : {status_path}")
    print(f"markdown report       : {md_path}")


if __name__ == "__main__":
    main()
