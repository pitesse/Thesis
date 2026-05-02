"""Shared feature-profile and exclusion planning for ablation runs.

This module centralizes profile semantics so training, serving, export, and
evaluation scripts apply the same feature contract.
"""

from __future__ import annotations

from dataclasses import dataclass

FEATURE_PROFILE_BASELINE = "baseline"
FEATURE_PROFILE_DROP_MEDIUM_V1 = "drop_medium_v1"
FEATURE_PROFILE_DROP_AGGRESSIVE_V1_CANDIDATE = "drop_aggressive_v1_candidate"
FEATURE_PROFILE_TRACK_AGNOSTIC_V1 = "track_agnostic_v1"
FEATURE_PROFILE_PERCENT_CONSERVATIVE_V1 = "percent_conservative_v1"
FEATURE_PROFILE_PERCENT_TEAM_V1 = "percent_team_v1"
FEATURE_PROFILE_PERCENT_RACE_TEAM_V1 = "percent_race_team_v1"
FEATURE_PROFILE_PERCENT_RACE_TEAM_V1_STRICT = "percent_race_team_v1_strict"
FEATURE_PROFILE_PERCENT_RACE_TEAM_AGGRESSIVE_V1 = "percent_race_team_aggressive_v1"

TRACK_AGNOSTIC_OFF = "off"
TRACK_AGNOSTIC_V1 = "track_agnostic_v1"
TRACK_PERCENTAGE_V1 = "track_percentage_v1"
TRACK_PERCENTAGE_TEAM_V1 = "track_percentage_team_v1"
TRACK_PERCENTAGE_RACE_TEAM_V1 = "track_percentage_race_team_v1"
TRACK_AGNOSTIC_AUTO = "auto"

TRACK_AGNOSTIC_V1_COLUMNS: tuple[str, ...] = (
    "trackTemp_expanding_z",
    "airTemp_expanding_z",
    "humidity_expanding_z",
    "speedTrap_expanding_z",
    "lapTime_expanding_z",
)
TRACK_PERCENTAGE_V1_COLUMNS: tuple[str, ...] = (
    "race_progress_pct",
    "lapTime_vs_driver_prev_mean_pct",
)
TRACK_PERCENTAGE_TEAM_V1_COLUMNS: tuple[str, ...] = (
    "race_progress_pct",
    "lapTime_vs_driver_prev_mean_pct",
    "lapTime_vs_team_prev_lapbucket_mean_pct",
)
TRACK_PERCENTAGE_RACE_TEAM_V1_COLUMNS: tuple[str, ...] = (
    "race_progress_pct",
    "lapTime_vs_driver_prev_mean_pct",
    "lapTime_vs_team_prev_lapbucket_mean_pct",
    "lapTime_vs_race_prev_lapbucket_mean_pct",
)

DEFAULT_FEATURE_PROFILE = FEATURE_PROFILE_BASELINE
DEFAULT_TRACK_AGNOSTIC_MODE = TRACK_AGNOSTIC_AUTO

_PROFILE_BASE_EXCLUDES: dict[str, tuple[str, ...]] = {
    FEATURE_PROFILE_BASELINE: (),
    FEATURE_PROFILE_DROP_MEDIUM_V1: (
        "team",
        "rainfall",
    ),
    FEATURE_PROFILE_DROP_AGGRESSIVE_V1_CANDIDATE: (
        "team",
        "rainfall",
        "pitLoss",
        "pace_trend",
        "has_drop_zone_data",
    ),
    FEATURE_PROFILE_TRACK_AGNOSTIC_V1: (
        "team",
        "rainfall",
    ),
    FEATURE_PROFILE_PERCENT_CONSERVATIVE_V1: (
        "team",
        "rainfall",
    ),
    FEATURE_PROFILE_PERCENT_TEAM_V1: (
        "team",
        "rainfall",
    ),
    FEATURE_PROFILE_PERCENT_RACE_TEAM_V1: (
        "team",
        "rainfall",
    ),
    FEATURE_PROFILE_PERCENT_RACE_TEAM_V1_STRICT: (
        "team",
        "rainfall",
        "lapTime",
    ),
    FEATURE_PROFILE_PERCENT_RACE_TEAM_AGGRESSIVE_V1: (
        "team",
        "rainfall",
        "lapTime",
        "pitLoss",
        "pace_trend",
        "has_drop_zone_data",
    ),
}

_PROFILE_TRACK_AGNOSTIC_MODE: dict[str, str] = {
    FEATURE_PROFILE_BASELINE: TRACK_AGNOSTIC_OFF,
    FEATURE_PROFILE_DROP_MEDIUM_V1: TRACK_AGNOSTIC_OFF,
    FEATURE_PROFILE_DROP_AGGRESSIVE_V1_CANDIDATE: TRACK_AGNOSTIC_OFF,
    FEATURE_PROFILE_TRACK_AGNOSTIC_V1: TRACK_AGNOSTIC_V1,
    FEATURE_PROFILE_PERCENT_CONSERVATIVE_V1: TRACK_PERCENTAGE_V1,
    FEATURE_PROFILE_PERCENT_TEAM_V1: TRACK_PERCENTAGE_TEAM_V1,
    FEATURE_PROFILE_PERCENT_RACE_TEAM_V1: TRACK_PERCENTAGE_RACE_TEAM_V1,
    FEATURE_PROFILE_PERCENT_RACE_TEAM_V1_STRICT: TRACK_PERCENTAGE_RACE_TEAM_V1,
    FEATURE_PROFILE_PERCENT_RACE_TEAM_AGGRESSIVE_V1: TRACK_PERCENTAGE_RACE_TEAM_V1,
}


@dataclass(frozen=True)
class FeaturePlan:
    feature_profile: str
    excluded_features: tuple[str, ...]
    track_agnostic_mode: str

    def excluded_features_csv(self) -> str:
        return ",".join(self.excluded_features)


def available_feature_profiles() -> list[str]:
    return list(_PROFILE_BASE_EXCLUDES.keys())


def available_track_agnostic_modes() -> list[str]:
    return [
        TRACK_AGNOSTIC_AUTO,
        TRACK_AGNOSTIC_OFF,
        TRACK_AGNOSTIC_V1,
        TRACK_PERCENTAGE_V1,
        TRACK_PERCENTAGE_TEAM_V1,
        TRACK_PERCENTAGE_RACE_TEAM_V1,
    ]


def parse_exclude_features(tokens: list[str] | None) -> list[str]:
    if not tokens:
        return []

    parsed: list[str] = []
    for token in tokens:
        parts = str(token).split(",")
        for part in parts:
            text = part.strip()
            if text:
                parsed.append(text)
    return parsed


def ensure_track_agnostic_columns(
    columns: list[str] | tuple[str, ...],
    *,
    track_agnostic_mode: str,
    context_label: str,
) -> None:
    if track_agnostic_mode == TRACK_AGNOSTIC_OFF:
        return

    required_columns_by_mode: dict[str, tuple[str, ...]] = {
        TRACK_AGNOSTIC_V1: TRACK_AGNOSTIC_V1_COLUMNS,
        TRACK_PERCENTAGE_V1: TRACK_PERCENTAGE_V1_COLUMNS,
        TRACK_PERCENTAGE_TEAM_V1: TRACK_PERCENTAGE_TEAM_V1_COLUMNS,
        TRACK_PERCENTAGE_RACE_TEAM_V1: TRACK_PERCENTAGE_RACE_TEAM_V1_COLUMNS,
    }
    required_columns = required_columns_by_mode.get(track_agnostic_mode)
    if required_columns is None:
        raise ValueError(
            f"unsupported track_agnostic_mode={track_agnostic_mode!r}; "
            f"expected one of {available_track_agnostic_modes()}"
        )

    missing = [column for column in required_columns if column not in columns]
    if missing:
        raise ValueError(
            f"{context_label}: {track_agnostic_mode} requires columns {missing}. "
            f"prepare the dataset with --track-agnostic-mode {track_agnostic_mode} "
            "before training/evaluation/export."
        )


def build_feature_plan(
    feature_profile: str,
    exclude_features: list[str] | None = None,
    *,
    track_agnostic_mode: str = DEFAULT_TRACK_AGNOSTIC_MODE,
) -> FeaturePlan:
    profile = str(feature_profile).strip()
    if profile not in _PROFILE_BASE_EXCLUDES:
        raise ValueError(
            "unknown feature profile: "
            f"{profile}. supported={available_feature_profiles()}"
        )

    mode = str(track_agnostic_mode).strip()
    if mode not in available_track_agnostic_modes():
        raise ValueError(
            "unknown track-agnostic mode: "
            f"{mode}. supported={available_track_agnostic_modes()}"
        )
    if mode == TRACK_AGNOSTIC_AUTO:
        mode = _PROFILE_TRACK_AGNOSTIC_MODE[profile]

    merged: list[str] = []
    seen: set[str] = set()
    for feature in list(_PROFILE_BASE_EXCLUDES[profile]) + list(exclude_features or []):
        item = str(feature).strip()
        if not item:
            continue
        if item not in seen:
            seen.add(item)
            merged.append(item)

    return FeaturePlan(
        feature_profile=profile,
        excluded_features=tuple(merged),
        track_agnostic_mode=mode,
    )
