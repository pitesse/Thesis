"""Run catalog and artifact resolvers for cross-run thesis analysis scripts.

This module provides one source of truth for all completed run families used in
advanced evidence and error-analysis scripts.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


RUN_SET_ALL_FAMILIES = "all_families"


@dataclass(frozen=True)
class RunSpec:
    run_id: str
    family: str
    label: str
    reports_dir: Path
    suffix: str
    racewise_suffix: str


def racewise_suffix_for(suffix: str) -> str:
    if suffix == "2022_2025_merged":
        return "2022_2025_racewise"

    prefix = "2022_2025_"
    if suffix.startswith(prefix):
        tail = suffix[len(prefix):]
        return f"2022_2025_racewise_{tail}"

    if suffix.startswith("2022_2025"):
        return suffix.replace("2022_2025", "2022_2025_racewise", 1)

    return f"{suffix}_racewise"


def _build_specs(reports_root: Path) -> list[RunSpec]:
    root = reports_root
    rows: list[tuple[str, str, str, str, str]] = [
        (
            "full",
            "full_feature_baseline",
            "Full-feature baseline",
            ".",
            "2022_2025_merged",
        ),
        (
            "e0",
            "e_family",
            "E0 no_source_year",
            "no_source_year",
            "2022_2025_no_source_year",
        ),
        (
            "e1",
            "e_family",
            "E1 drop_medium_v1",
            "no_source_year_drop_medium_v1",
            "2022_2025_no_source_year_drop_medium_v1",
        ),
        (
            "e2a",
            "e_family",
            "E2a drop_medium_track_agnostic_v1",
            "no_source_year_drop_medium_track_agnostic_v1",
            "2022_2025_no_source_year_drop_medium_track_agnostic_v1",
        ),
        (
            "e2b",
            "e_family",
            "E2b track_agnostic_v1_strict",
            "no_source_year_track_agnostic_v1_strict",
            "2022_2025_no_source_year_track_agnostic_v1_strict",
        ),
        (
            "e3",
            "e_family",
            "E3 drop_aggressive_v1_candidate",
            "no_source_year_drop_aggressive_v1_candidate",
            "2022_2025_no_source_year_drop_aggressive_v1_candidate",
        ),
        (
            "p1",
            "p_family",
            "P1 percent_conservative_v1",
            "no_source_year_percent_conservative_v1",
            "2022_2025_no_source_year_percent_conservative_v1",
        ),
        (
            "p2",
            "p_family",
            "P2 percent_team_v1",
            "no_source_year_percent_team_v1",
            "2022_2025_no_source_year_percent_team_v1",
        ),
        (
            "p3",
            "p_family",
            "P3 percent_race_team_v1",
            "no_source_year_percent_race_team_v1",
            "2022_2025_no_source_year_percent_race_team_v1",
        ),
        (
            "p4",
            "p_family",
            "P4 percent_race_team_v1_strict",
            "no_source_year_percent_race_team_v1_strict",
            "2022_2025_no_source_year_percent_race_team_v1_strict",
        ),
        (
            "p5",
            "p_family",
            "P5 percent_race_team_aggressive_v1",
            "no_source_year_percent_race_team_aggressive_v1",
            "2022_2025_no_source_year_percent_race_team_aggressive_v1",
        ),
    ]

    specs: list[RunSpec] = []
    for run_id, family, label, rel_dir, suffix in rows:
        reports_dir = (root if rel_dir == "." else (root / rel_dir)).resolve()
        specs.append(
            RunSpec(
                run_id=run_id,
                family=family,
                label=label,
                reports_dir=reports_dir,
                suffix=suffix,
                racewise_suffix=racewise_suffix_for(suffix),
            )
        )
    return specs


def list_runs(reports_root: Path, run_set: str = RUN_SET_ALL_FAMILIES) -> list[RunSpec]:
    specs = _build_specs(reports_root)
    if run_set != RUN_SET_ALL_FAMILIES:
        raise ValueError(f"unsupported --run-set '{run_set}'. supported values: {RUN_SET_ALL_FAMILIES}")
    return specs


def run_by_id(reports_root: Path, run_id: str) -> RunSpec:
    for run in _build_specs(reports_root):
        if run.run_id == run_id:
            return run
    raise KeyError(f"unknown run_id: {run_id}")


def _first_existing(candidates: Iterable[Path]) -> Path:
    first: Path | None = None
    for path in candidates:
        if first is None:
            first = path
        if path.exists():
            return path
    if first is None:
        raise ValueError("no candidate paths were provided")
    return first


def oof_path(run: RunSpec, protocol: str) -> Path:
    if protocol == "ml_pretrain":
        return run.reports_dir / f"ml_oof_winner_{run.suffix}.csv"
    if protocol == "ml_racewise":
        return run.reports_dir / f"ml_oof_winner_{run.racewise_suffix}.csv"
    raise ValueError(f"unsupported protocol: {protocol}")


def comparator_path(run: RunSpec, model: str) -> Path:
    if model == "sde":
        return _first_existing(
            [
                run.reports_dir / f"heuristic_comparator_{run.suffix}.csv",
                run.reports_dir / f"heuristic_comparator_{run.racewise_suffix}.csv",
            ]
        )

    if model == "ml":
        return run.reports_dir / f"ml_comparator_{run.suffix}.csv"

    if model == "ml_best":
        return run.reports_dir / f"ml_comparator_best_threshold_{run.suffix}.csv"

    if model == "ml_racewise":
        return run.reports_dir / f"ml_comparator_{run.racewise_suffix}.csv"

    if model == "moa":
        name = f"moa_comparator_{run.suffix}.csv"
        reports_root = run.reports_dir.parent if run.reports_dir.name != "reports" else run.reports_dir
        legacy_base = reports_root.parent / "data_lake" / "reports"
        return _first_existing(
            [
                run.reports_dir / name,
                legacy_base / run.reports_dir.name / name,
            ]
        )

    raise ValueError(f"unsupported comparator model: {model}")


def moa_summary_path(run: RunSpec) -> Path:
    return run.reports_dir / f"moa_arf_summary_{run.suffix}.csv"


def pr_metrics_path(run: RunSpec) -> Path:
    # historical root run stores pr_metrics_2022_2025.csv (without _merged suffix)
    if run.suffix.endswith("_merged"):
        short = run.suffix[: -len("_merged")]
        return _first_existing(
            [
                run.reports_dir / f"pr_metrics_{short}.csv",
                run.reports_dir / f"pr_metrics_{run.suffix}.csv",
            ]
        )
    return run.reports_dir / f"pr_metrics_{run.suffix}.csv"


def pr_operating_points_path(run: RunSpec) -> Path:
    if run.suffix.endswith("_merged"):
        short = run.suffix[: -len("_merged")]
        return _first_existing(
            [
                run.reports_dir / f"pr_operating_points_{short}.csv",
                run.reports_dir / f"pr_operating_points_{run.suffix}.csv",
            ]
        )
    return run.reports_dir / f"pr_operating_points_{run.suffix}.csv"


def moa_surrogate_holdout_path(run: RunSpec) -> Path:
    return run.reports_dir / "moa_surrogate_holdout_predictions.csv"


def moa_surrogate_pr_metrics_path(run: RunSpec) -> Path:
    return run.reports_dir / "moa_surrogate_pr_metrics.csv"
