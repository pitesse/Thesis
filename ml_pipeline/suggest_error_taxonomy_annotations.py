"""Create assistant-prefilled error-taxonomy sheets for manual review.

This does not change ground-truth outcomes; it only adds suggested manual labels
and context hints to accelerate human-in-the-loop annotation.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from lib.race_metadata import get_scheduled_laps, race_name_without_year_prefix
except ImportError:
    from ml_pipeline.lib.race_metadata import get_scheduled_laps, race_name_without_year_prefix  # type: ignore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate prefilled error-taxonomy suggestions")
    parser.add_argument(
        "--template-csv",
        type=Path,
        required=True,
        help="Input manual template CSV",
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("data_lake/ml_training_dataset_2022_2025_merged.parquet"),
        help="Dataset parquet for contextual feature lookup",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=None,
        help="Output prefilled CSV; default = <template>_prefill.csv",
    )
    return parser.parse_args()


def _safe_num(value: object) -> float:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return float("nan")
    return float(number)


def _derive_progress(race: str, lap: int) -> float:
    race_text = str(race)
    year_text = race_text.split(" :: ", 1)[0]
    race_name = race_name_without_year_prefix(race_text)
    try:
        year = int(year_text)
        scheduled = int(get_scheduled_laps(year, race_name))
        if scheduled > 0:
            return float(lap / scheduled)
    except Exception:
        pass
    return float("nan")


def _suggest_tag(row: pd.Series) -> tuple[str, str, str, int]:
    model = str(row.get("model", ""))
    case_origin = str(row.get("case_origin", ""))
    subtype = str(row.get("subtype_auto", ""))

    rainfall = bool(row.get("rainfall", False)) if pd.notna(row.get("rainfall", np.nan)) else False
    track_status = _safe_num(row.get("trackStatus"))
    gap_ahead = _safe_num(row.get("gapAhead"))
    gap_behind = _safe_num(row.get("gapBehind"))
    progress = _safe_num(row.get("race_progress_pct"))
    nearest_dist = _safe_num(row.get("nearest_future_pit_distance"))

    source = "F1 race report + lap chart + pit-stop summary"
    priority = 0

    if case_origin == "scored_fp":
        priority += 5
        if model == "Batch":
            priority += 3
        elif model == "MOA":
            priority += 2

        if (pd.notna(track_status) and track_status > 1) or rainfall:
            tag = "Weather / Safety Car Disruption"
            note = "Pace shock likely exogenous (rain/neutralization) rather than clean tire-cliff signal."
        elif pd.notna(progress) and progress >= 0.80:
            tag = "End-of-Race Tire Management"
            note = "Late-race pace decay can be intentional tire nursing with no planned extra stop."
        elif (pd.notna(gap_ahead) and gap_ahead <= 1.2) or (pd.notna(gap_behind) and gap_behind <= 1.2):
            tag = "Traffic / DRS Train"
            note = "Local pace loss likely induced by traffic constraints, not pure degradation."
        else:
            tag = "Strategic Offset / Gamble"
            note = "Model flagged degradation signal but team likely optimized for track-position timing."
        return tag, source, note, priority

    # excluded_no_match
    priority += 2
    if model == "Batch":
        priority += 2

    if subtype == "unexpected_within_horizon":
        tag = "Duplicate Trigger (Comparator One-to-One Consumption)"
        note = "A pit existed within H=2, but likely consumed by an earlier suggestion for same driver stint."
        priority += 4
    elif subtype == "near_miss_3_5":
        tag = "Near-Miss Strategic Extension"
        note = "Decision appears directionally correct; stop executed 3-5 laps later (overcut/position management)."
        priority += 3
    elif subtype == "far_miss_6_plus":
        if pd.notna(progress) and progress >= 0.70:
            tag = "Long-Stint Conservation"
            note = "Late/mid-late race extension indicates tire conservation and one-stop style risk management."
        else:
            tag = "Early/Mid Stint Strategic Deferral"
            note = "Stop deferred substantially; likely traffic, undercut defense, or planned offset window."
        priority += 1
    elif subtype == "no_future":
        if pd.notna(progress) and progress >= 0.65:
            tag = "One-Stop Stretch / No Further Stop"
            note = "Model flagged degradation, but strategy committed to no additional stop until finish."
        else:
            tag = "No-Future Stop (Unrealized Signal)"
            note = "No subsequent pit recorded; inspect race context for external events or conservative strategy."
    else:
        tag = "Unclassified Exclusion"
        note = "Review lap context manually (weather, incidents, traffic, team strategy)."

    return tag, source, note, priority


def main() -> None:
    args = parse_args()

    template_path = args.template_csv
    dataset_path = args.dataset

    if not template_path.exists():
        raise FileNotFoundError(f"template csv not found: {template_path}")
    if not dataset_path.exists():
        raise FileNotFoundError(f"dataset parquet not found: {dataset_path}")

    out_path = args.output_csv
    if out_path is None:
        out_path = template_path.with_name(template_path.stem + "_prefill.csv")

    template = pd.read_csv(template_path)
    required = {
        "model",
        "case_origin",
        "subtype_auto",
        "race",
        "driver",
        "suggestion_lap",
        "nearest_future_pit_distance",
    }
    missing = [c for c in required if c not in template.columns]
    if missing:
        raise ValueError(f"template missing required columns: {missing}")

    features = pd.read_parquet(dataset_path)
    keep = [
        "race",
        "driver",
        "lapNumber",
        "trackStatus",
        "rainfall",
        "gapAhead",
        "gapBehind",
        "position",
        "pace_drop_ratio",
        "tire_life_ratio",
        "lapTime",
        "trackTemp",
        "airTemp",
        "humidity",
        "speedTrap",
    ]
    keep_present = [c for c in keep if c in features.columns]
    features = features[keep_present].copy()
    features["race"] = features["race"].astype(str)
    features["driver"] = features["driver"].astype(str)
    features["lapNumber"] = pd.to_numeric(features["lapNumber"], errors="coerce").fillna(-1).astype(int)

    work = template.copy()
    work["race"] = work["race"].astype(str)
    work["driver"] = work["driver"].astype(str)
    work["suggestion_lap"] = pd.to_numeric(work["suggestion_lap"], errors="coerce").fillna(-1).astype(int)

    merged = work.merge(
        features,
        left_on=["race", "driver", "suggestion_lap"],
        right_on=["race", "driver", "lapNumber"],
        how="left",
    )

    merged["race_progress_pct"] = [
        _derive_progress(race, int(lap)) for race, lap in zip(merged["race"], merged["suggestion_lap"], strict=True)
    ]

    tags = []
    for _, row in merged.iterrows():
        tags.append(_suggest_tag(row))

    merged["failure_type_suggested"] = [x[0] for x in tags]
    merged["context_source_suggested"] = [x[1] for x in tags]
    merged["notes_suggested"] = [x[2] for x in tags]
    merged["review_priority"] = [x[3] for x in tags]

    if "failure_type_manual" in merged.columns:
        merged["failure_type_manual"] = merged["failure_type_manual"].fillna(merged["failure_type_suggested"])
    if "context_source" in merged.columns:
        merged["context_source"] = merged["context_source"].fillna(merged["context_source_suggested"])
    if "notes" in merged.columns:
        merged["notes"] = merged["notes"].fillna(merged["notes_suggested"])

    sort_cols = ["review_priority", "model", "case_origin", "race", "driver", "suggestion_lap"]
    merged = merged.sort_values(by=sort_cols, ascending=[False, True, True, True, True, True]).reset_index(drop=True)

    merged.to_csv(out_path, index=False)

    priority_counts = merged["failure_type_suggested"].value_counts().to_dict()

    print("=== PREFILLED TAXONOMY SUGGESTIONS GENERATED ===")
    print(f"template input           : {template_path}")
    print(f"dataset                  : {dataset_path}")
    print(f"output                   : {out_path}")
    print(f"rows                     : {len(merged)}")
    print(f"suggested-tag breakdown  : {priority_counts}")


if __name__ == "__main__":
    main()
