"""Prepare a training dataset from one or multiple seasons with leakage-safe labels."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from pipeline_config import (
    DEFAULT_DATA_LAKE,
    DEFAULT_HORIZON,
    DEFAULT_SEASON_TAG,
    DEFAULT_YEARS,
    default_dataset_path,
    normalize_years,
)
from lib.data_preparation import (
    _build_targets,
    _latest_jsonl,
    _load_jsonl,
    _prepare_drop_zones,
    _prepare_features,
    _prepare_pit_evals,
    _print_summary,
    _write_dataset,
)
from lib.feature_profiles import (
    DEFAULT_FEATURE_PROFILE,
    DEFAULT_TRACK_AGNOSTIC_MODE,
    available_track_agnostic_modes,
    build_feature_plan,
    parse_exclude_features,
)

# F1 calendar order (chronological 2022-2025 for expanding_race protocol temporal integrity)
F1_CALENDAR = {
    2022: [
        "Bahrain Grand Prix", "Saudi Arabian Grand Prix", "Australian Grand Prix",
        "Emilia Romagna Grand Prix", "Miami Grand Prix", "Spanish Grand Prix",
        "Monaco Grand Prix", "Azerbaijan Grand Prix", "Canadian Grand Prix",
        "British Grand Prix", "Austrian Grand Prix", "French Grand Prix",
        "Hungarian Grand Prix", "Belgian Grand Prix", "Dutch Grand Prix",
        "Italian Grand Prix", "Singapore Grand Prix", "Japanese Grand Prix",
        "United States Grand Prix", "Mexico City Grand Prix", "São Paulo Grand Prix",
        "Abu Dhabi Grand Prix"
    ],
    2023: [
        "Bahrain Grand Prix", "Saudi Arabian Grand Prix", "Australian Grand Prix",
        "Azerbaijan Grand Prix", "Miami Grand Prix", "Monaco Grand Prix",
        "Spanish Grand Prix", "Canadian Grand Prix", "Austrian Grand Prix",
        "British Grand Prix", "Hungarian Grand Prix", "Belgian Grand Prix",
        "Dutch Grand Prix", "Italian Grand Prix", "Singapore Grand Prix",
        "Japanese Grand Prix", "Qatar Grand Prix", "United States Grand Prix",
        "Mexico City Grand Prix", "São Paulo Grand Prix", "Abu Dhabi Grand Prix"
    ],
    2024: [
        "Bahrain Grand Prix", "Saudi Arabian Grand Prix", "Australian Grand Prix",
        "Japanese Grand Prix", "Chinese Grand Prix", "Miami Grand Prix",
        "Emilia Romagna Grand Prix", "Monaco Grand Prix", "Canadian Grand Prix",
        "Spanish Grand Prix", "Austrian Grand Prix", "British Grand Prix",
        "Hungarian Grand Prix", "Belgian Grand Prix", "Dutch Grand Prix",
        "Italian Grand Prix", "Azerbaijan Grand Prix", "Singapore Grand Prix",
        "United States Grand Prix", "Mexico City Grand Prix", "São Paulo Grand Prix",
        "Abu Dhabi Grand Prix"
    ],
    2025: [
        "Bahrain Grand Prix", "Saudi Arabian Grand Prix", "Australian Grand Prix",
        "Japanese Grand Prix", "Shanghai Grand Prix", "Miami Grand Prix",
        "Monaco Grand Prix", "Canadian Grand Prix", "Spanish Grand Prix",
        "Austrian Grand Prix", "British Grand Prix", "Hungarian Grand Prix",
        "Belgian Grand Prix", "Dutch Grand Prix", "Italian Grand Prix",
        "Monza Grand Prix", "Baku Grand Prix", "Singapore Grand Prix",
        "United States Grand Prix", "Mexico City Grand Prix", "Brazil Grand Prix",
        "Abu Dhabi Grand Prix"
    ]
}


@dataclass(frozen=True)
class PreparedSeason:
    year: int
    ml_features_path: Path
    drop_zones_path: Path
    pit_evals_path: Path
    feature_dedup_stats: dict[str, int | str]
    pit_eval_dedup_stats: dict[str, int | str]
    dataset: pd.DataFrame


def _with_year_prefixed_race(frame: pd.DataFrame, year: int) -> pd.DataFrame:
    work = frame.copy()
    race = work["race"].astype(str)
    prefix = f"{year} :: "
    already_prefixed = race.str.match(r"^\d{4}\s::\s")
    work.loc[~already_prefixed, "race"] = prefix + race[~already_prefixed]
    return work


def _prepare_one_season(
    data_lake: Path,
    year: int,
    season_tag: str,
    horizon: int,
    track_agnostic_mode: str,
) -> PreparedSeason:
    # always select latest stream snapshots per season token, this keeps retraining aligned with latest validated exports.
    ml_features_path = _latest_jsonl(data_lake, "ml_features", year, season_tag)
    drop_zones_path = _latest_jsonl(data_lake, "drop_zones", year, season_tag)
    pit_evals_path = _latest_jsonl(data_lake, "pit_evals", year, season_tag)

    features_raw = _load_jsonl(ml_features_path)
    drop_zones_raw = _load_jsonl(drop_zones_path)
    pit_evals_raw = _load_jsonl(pit_evals_path)

    drop_zones = _prepare_drop_zones(drop_zones_raw)
    features = _prepare_features(
        features_raw,
        drop_zones,
        source_year_fallback=year,
        track_agnostic_mode=track_agnostic_mode,
    )
    pit_evals = _prepare_pit_evals(pit_evals_raw)
    feature_dedup_stats = dict(features.attrs.get("dedup_stats", {}))
    pit_eval_dedup_stats = dict(pit_evals.attrs.get("dedup_stats", {}))

    # keep race keys unique across seasons to preserve grouped-CV integrity.
    features = _with_year_prefixed_race(features, year)
    pit_evals = _with_year_prefixed_race(pit_evals, year)

    dataset = _build_targets(features, pit_evals, horizon)

    return PreparedSeason(
        year=year,
        ml_features_path=ml_features_path,
        drop_zones_path=drop_zones_path,
        pit_evals_path=pit_evals_path,
        feature_dedup_stats=feature_dedup_stats,
        pit_eval_dedup_stats=pit_eval_dedup_stats,
        dataset=dataset,
    )


def _merge_seasons(prepared: list[PreparedSeason]) -> pd.DataFrame:
    frames = [item.dataset for item in prepared]
    merged = pd.concat(frames, ignore_index=True)
    
    # extract year and race name for calendar ordering
    merged["_extracted_year"] = merged["race"].str.extract(r"^(\d{4})")
    merged["_extracted_year"] = pd.to_numeric(merged["_extracted_year"], errors="coerce").astype("Int64")
    merged["_extracted_race"] = merged["race"].str.replace(r"^\d{4}\s::\s", "", regex=True)
    
    # add calendar order index for chronological sorting (fixes temporal leakage in expanding_race)
    def _get_calendar_order(year: int | float, race_name: str) -> int:
        if pd.isna(year):
            return 999999
        year_int = int(year)
        calendar = F1_CALENDAR.get(year_int, [])
        try:
            return calendar.index(race_name)
        except (ValueError, KeyError):
            return 999998  # unknown race, sort last
    
    merged["_calendar_order"] = merged.apply(
        lambda row: _get_calendar_order(row["_extracted_year"], row["_extracted_race"]),
        axis=1
    )
    
    # sort chronologically by year then calendar order, then driver/lap for stability
    merged.sort_values(
        by=["_extracted_year", "_calendar_order", "driver", "lapNumber"],
        inplace=True
    )
    merged.drop(columns=["_extracted_year", "_extracted_race", "_calendar_order"], inplace=True)
    merged.reset_index(drop=True, inplace=True)
    return merged


def _print_multi_season_summary(
    prepared: list[PreparedSeason],
    merged: pd.DataFrame,
    output_path: Path,
    output_format: str,
    track_agnostic_mode: str,
) -> None:
    positives = int((merged["target_y"] == 1).sum())
    negatives = int((merged["target_y"] == 0).sum())
    total = positives + negatives
    pos_ratio = (positives / total) if total else 0.0
    scale_pos_weight = (negatives / positives) if positives else float("inf")

    print("=== PREP DATA SUMMARY (MULTI-SEASON) ===")
    print(f"output            : {output_path} ({output_format})")
    print(f"shape             : {merged.shape}")
    print(f"years             : {[item.year for item in prepared]}")
    print(f"track agnostic    : {track_agnostic_mode}")

    print("\nsource files by season")
    for item in prepared:
        print(f"{item.year}: ml_features={item.ml_features_path}")
        print(f"{item.year}: drop_zones={item.drop_zones_path}")
        print(f"{item.year}: pit_evals={item.pit_evals_path}")
        print(
            f"{item.year}: ml_features_dedup_excess={item.feature_dedup_stats.get('dedup_excess_rows_before', 0)} "
            f"-> {item.feature_dedup_stats.get('dedup_excess_rows_after', 0)}"
        )
        print(
            f"{item.year}: pit_evals_dedup_excess={item.pit_eval_dedup_stats.get('dedup_excess_rows_before', 0)} "
            f"-> {item.pit_eval_dedup_stats.get('dedup_excess_rows_after', 0)}"
        )
        print(f"{item.year}: rows={len(item.dataset)}")

    print("\nclass distribution")
    print(f"y=1 positives     : {positives} ({pos_ratio:.4%})")
    print(f"y=0 negatives     : {negatives} ({1.0 - pos_ratio:.4%})")
    print(f"scale_pos_weight  : {scale_pos_weight:.6f}")


def prepare_dataset(
    data_lake: Path,
    years: Iterable[int],
    season_tag: str,
    horizon: int,
    output_path: Path,
    strict_parquet: bool,
    feature_profile: str = DEFAULT_FEATURE_PROFILE,
    exclude_features: list[str] | None = None,
    track_agnostic_mode: str = DEFAULT_TRACK_AGNOSTIC_MODE,
) -> Path:
    feature_plan = build_feature_plan(
        feature_profile=feature_profile,
        exclude_features=exclude_features or [],
        track_agnostic_mode=track_agnostic_mode,
    )
    normalized_years = normalize_years(years)
    prepared = [
        _prepare_one_season(
            data_lake=data_lake,
            year=year,
            season_tag=season_tag,
            horizon=horizon,
            track_agnostic_mode=feature_plan.track_agnostic_mode,
        )
        for year in normalized_years
    ]

    if len(prepared) == 1:
        season = prepared[0]
        # single season path preserves the same summary format used by legacy training runs.
        saved_path, output_format = _write_dataset(
            season.dataset,
            output_path,
            strict_parquet=strict_parquet,
        )
        _print_summary(
            dataset=season.dataset,
            ml_features_path=season.ml_features_path,
            drop_zones_path=season.drop_zones_path,
            pit_evals_path=season.pit_evals_path,
            output_path=saved_path,
            output_format=output_format,
            track_agnostic_mode=feature_plan.track_agnostic_mode,
            feature_dedup_stats=season.feature_dedup_stats,
            pit_eval_dedup_stats=season.pit_eval_dedup_stats,
        )
        print(f"feature profile    : {feature_plan.feature_profile}")
        print(
            "excluded features  : "
            f"{feature_plan.excluded_features_csv() or 'none'}"
        )
        return saved_path

    # multi season path merges first, then writes one canonical artifact consumed by training and evaluation.
    merged = _merge_seasons(prepared)
    saved_path, output_format = _write_dataset(
        merged,
        output_path,
        strict_parquet=strict_parquet,
    )
    _print_multi_season_summary(
        prepared,
        merged,
        saved_path,
        output_format,
        feature_plan.track_agnostic_mode,
    )
    print(f"feature profile    : {feature_plan.feature_profile}")
    print(
        "excluded features  : "
        f"{feature_plan.excluded_features_csv() or 'none'}"
    )
    return saved_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="prepare leakage-safe training data for one or multiple seasons"
    )
    parser.add_argument(
        "--data-lake",
        default=DEFAULT_DATA_LAKE,
        help="data lake directory",
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=list(DEFAULT_YEARS),
        help="season years to include",
    )
    parser.add_argument(
        "--season-tag",
        default=DEFAULT_SEASON_TAG,
        help="season tag token in JSONL filenames",
    )
    parser.add_argument(
        "--horizon",
        type=int,
        default=DEFAULT_HORIZON,
        help="look-ahead horizon in laps",
    )
    parser.add_argument(
        "--output",
        default="",
        help="output parquet/csv path. if omitted, a default path is derived from years",
    )
    parser.add_argument(
        "--strict-parquet",
        action="store_true",
        help="fail if parquet backend is not available",
    )
    parser.add_argument(
        "--feature-profile",
        default=DEFAULT_FEATURE_PROFILE,
        help=(
            "shared feature-profile token for reproducible ablations "
            "(e.g. baseline, drop_medium_v1, drop_aggressive_v1_candidate, track_agnostic_v1)"
        ),
    )
    parser.add_argument(
        "--exclude-features",
        nargs="*",
        default=[],
        help=(
            "optional additional raw feature names to exclude; accepts whitespace and/or comma-separated tokens"
        ),
    )
    parser.add_argument(
        "--track-agnostic-mode",
        choices=available_track_agnostic_modes(),
        default=DEFAULT_TRACK_AGNOSTIC_MODE,
        help=(
            "controls causal race-relative z-score feature generation; "
            "'auto' follows feature-profile defaults"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_lake = Path(args.data_lake)
    years = normalize_years(args.years)

    output_path = (
        Path(args.output)
        if args.output
        else default_dataset_path(data_lake, years, args.season_tag)
    )
    if not output_path.is_absolute():
        output_path = Path(output_path)

    saved_path = prepare_dataset(
        data_lake=data_lake,
        years=years,
        season_tag=args.season_tag,
        horizon=args.horizon,
        output_path=output_path,
        strict_parquet=args.strict_parquet,
        feature_profile=args.feature_profile,
        exclude_features=parse_exclude_features(args.exclude_features),
        track_agnostic_mode=args.track_agnostic_mode,
    )

    print(f"prepared dataset   : {saved_path}")


if __name__ == "__main__":
    main()
