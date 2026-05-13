"""Build a per-year replay manifest from merged ml_features output."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def _strip_year_prefix(race: str) -> str:
    text = str(race).strip()
    if len(text) > 7 and text[:4].isdigit() and text[4:8] == " :: ":
        return text[8:].strip()
    return text


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="build a replay manifest with race order and driver participation"
    )
    parser.add_argument("--year", type=int, required=True, help="season year")
    parser.add_argument(
        "--season-tag",
        default="season",
        help="season token used for replay generation",
    )
    parser.add_argument(
        "--run-id",
        default="",
        help="optional replay run identifier",
    )
    parser.add_argument(
        "--ml-features",
        required=True,
        help="merged ml_features jsonl for one season",
    )
    parser.add_argument(
        "--races",
        nargs="+",
        required=True,
        help="official race names in chronological order",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="output manifest json path",
    )
    parser.add_argument(
        "--allow-extra-races",
        action="store_true",
        help="allow races in ml_features that are not in the provided race list",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ml_features_path = Path(args.ml_features)
    output_path = Path(args.output)

    if not ml_features_path.exists():
        raise FileNotFoundError(f"ml_features input not found: {ml_features_path}")

    listed_races = [str(race).strip() for race in args.races if str(race).strip()]
    if not listed_races:
        raise ValueError("--races cannot be empty")

    listed_set = set(listed_races)
    drivers_by_race: dict[str, set[str]] = defaultdict(set)
    rows_scanned = 0

    with ml_features_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            row = json.loads(text)
            if not isinstance(row, dict):
                continue

            race_raw = row.get("race")
            driver_raw = row.get("driver")
            if race_raw is None or driver_raw is None:
                continue

            race = _strip_year_prefix(str(race_raw))
            driver = str(driver_raw).strip()
            if not race or not driver:
                continue

            drivers_by_race[race].add(driver)
            rows_scanned += 1

    if rows_scanned == 0:
        raise ValueError("ml_features input has no usable race/driver rows")

    discovered_races = set(drivers_by_race)
    missing_races = sorted(listed_set - discovered_races)
    extra_races = sorted(discovered_races - listed_set)

    if missing_races:
        preview = ", ".join(missing_races[:8])
        raise ValueError(
            "manifest build failed: races from schedule are missing in ml_features: "
            f"{preview}"
        )
    if extra_races and not args.allow_extra_races:
        preview = ", ".join(extra_races[:8])
        raise ValueError(
            "manifest build failed: unexpected races found in ml_features: "
            f"{preview}. Use --allow-extra-races only for debugging."
        )

    ordered_drivers: dict[str, list[str]] = {}
    race_row_counts: dict[str, int] = {}
    # second pass for deterministic row counts by race
    with ml_features_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            row = json.loads(text)
            if not isinstance(row, dict) or row.get("race") is None:
                continue
            race = _strip_year_prefix(str(row.get("race")))
            if race:
                race_row_counts[race] = race_row_counts.get(race, 0) + 1

    for race in listed_races:
        drivers = sorted(drivers_by_race.get(race, set()))
        if not drivers:
            raise ValueError(
                f"manifest build failed: race {race!r} has zero drivers in ml_features"
            )
        ordered_drivers[race] = drivers

    run_id = str(args.run_id).strip() or datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )
    payload = {
        "manifest_version": 1,
        "year": int(args.year),
        "season_tag": str(args.season_tag),
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "races_in_order": listed_races,
        "drivers_by_race": ordered_drivers,
        "race_row_counts": {race: int(race_row_counts.get(race, 0)) for race in listed_races},
        "source": {
            "ml_features": str(ml_features_path),
            "race_list_origin": "fastf1.get_event_schedule(include_testing=False).EventName",
        },
        "extra_races_detected": extra_races,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    print("=== REPLAY MANIFEST SUMMARY ===")
    print(f"year            : {args.year}")
    print(f"season_tag      : {args.season_tag}")
    print(f"run_id          : {run_id}")
    print(f"rows_scanned    : {rows_scanned}")
    print(f"races           : {len(listed_races)}")
    print(f"extra_races     : {len(extra_races)}")
    print(f"output          : {output_path}")


if __name__ == "__main__":
    main()
