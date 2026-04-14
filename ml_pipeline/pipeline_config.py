"""Shared defaults and path helpers for consolidated ML pipeline entrypoints."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

DEFAULT_DATA_LAKE = "data_lake"
DEFAULT_SEASON_TAG = "season"
DEFAULT_HORIZON = 2
DEFAULT_YEARS = (2022, 2023, 2024)
# merged runs use a synthetic year token, keeps artifact names stable while preserving single season paths.
DEFAULT_MERGED_YEAR_TOKEN = 9999
DEFAULT_MERGED_SEASON_TAG = "merged"


def normalize_years(years: Iterable[int]) -> list[int]:
    normalized = sorted({int(year) for year in years})
    if not normalized:
        raise ValueError("at least one year must be provided")
    return normalized


def run_suffix(years: Iterable[int], season_tag: str) -> str:
    normalized = normalize_years(years)
    # keep single season names short, merged names explicit for reproducibility in reports.
    if len(normalized) == 1:
        year = normalized[0]
        if season_tag == DEFAULT_SEASON_TAG:
            return str(year)
        return f"{year}_{season_tag}"
    return f"{normalized[0]}_{normalized[-1]}_merged"


def reports_dir(data_lake: str | Path) -> Path:
    return Path(data_lake) / "reports"


def models_dir(data_lake: str | Path) -> Path:
    return Path(data_lake) / "models"


def default_dataset_path(data_lake: str | Path, years: Iterable[int], season_tag: str) -> Path:
    normalized = normalize_years(years)
    base = Path(data_lake)
    if len(normalized) == 1 and season_tag == DEFAULT_SEASON_TAG:
        return base / "ml_training_dataset.parquet"
    return base / f"ml_training_dataset_{run_suffix(normalized, season_tag)}.parquet"


def default_report_csv(data_lake: str | Path, stem: str, years: Iterable[int], season_tag: str) -> Path:
    return reports_dir(data_lake) / f"{stem}_{run_suffix(years, season_tag)}.csv"


def default_report_txt(data_lake: str | Path, stem: str, years: Iterable[int], season_tag: str) -> Path:
    return reports_dir(data_lake) / f"{stem}_{run_suffix(years, season_tag)}.txt"


def default_report_md(data_lake: str | Path, stem: str, years: Iterable[int], season_tag: str) -> Path:
    return reports_dir(data_lake) / f"{stem}_{run_suffix(years, season_tag)}.md"


def comparator_source_year_and_tag(years: Iterable[int], season_tag: str) -> tuple[int, str]:
    normalized = normalize_years(years)
    # comparator builders consume one year and one tag, merged evaluations route through the synthetic merged token.
    if len(normalized) == 1:
        return normalized[0], season_tag
    return DEFAULT_MERGED_YEAR_TOKEN, DEFAULT_MERGED_SEASON_TAG
