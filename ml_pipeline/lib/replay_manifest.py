"""Replay manifest loading and validation helpers."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Iterable

import pandas as pd

DEFAULT_MANIFEST_DIRNAME = "replay_manifests"
_RACE_PREFIX_RE = re.compile(r"^\d{4}\s::\s")


@dataclass(frozen=True)
class ReplayManifest:
    year: int
    season_tag: str
    run_id: str
    races_in_order: tuple[str, ...]
    drivers_by_race: dict[str, tuple[str, ...]]
    path: Path

    @property
    def race_order_index(self) -> dict[str, int]:
        return {race: idx for idx, race in enumerate(self.races_in_order)}


def manifest_dir(data_lake: Path) -> Path:
    return Path(data_lake) / DEFAULT_MANIFEST_DIRNAME


def strip_year_prefix(race: object) -> str:
    return _RACE_PREFIX_RE.sub("", str(race).strip())


def parse_year_prefix(race: object) -> int | None:
    text = str(race).strip()
    if len(text) < 4:
        return None
    token = text[:4]
    if token.isdigit() and len(text) > 7 and text[4:8] == " :: ":
        return int(token)
    return None


def latest_manifest_path(
    data_lake: Path,
    *,
    year: int,
    season_tag: str = "season",
) -> Path:
    base = manifest_dir(Path(data_lake))
    pattern = f"replay_manifest_{int(year)}_{season_tag}_*.json"
    matches = list(base.glob(pattern))
    if not matches:
        raise FileNotFoundError(
            f"no replay manifest found for year={year}, season_tag={season_tag}; "
            f"expected pattern: {base / pattern}"
        )
    return max(matches, key=lambda path: path.stat().st_mtime)


def load_manifest(path: Path) -> ReplayManifest:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"invalid replay manifest payload: {path}")

    year = int(payload.get("year"))
    season_tag = str(payload.get("season_tag", "season"))
    run_id = str(payload.get("run_id", ""))

    races_raw = payload.get("races_in_order")
    if not isinstance(races_raw, list) or not races_raw:
        raise ValueError(f"manifest {path} has empty or invalid races_in_order")
    races: list[str] = [str(race).strip() for race in races_raw if str(race).strip()]
    if len(races) != len(races_raw):
        raise ValueError(f"manifest {path} has blank race names")
    if len(set(races)) != len(races):
        raise ValueError(f"manifest {path} has duplicate race names")

    drivers_raw = payload.get("drivers_by_race")
    if not isinstance(drivers_raw, dict):
        raise ValueError(f"manifest {path} has invalid drivers_by_race")

    drivers_by_race: dict[str, tuple[str, ...]] = {}
    for race in races:
        items = drivers_raw.get(race)
        if not isinstance(items, list) or len(items) == 0:
            raise ValueError(
                f"manifest {path} missing non-empty driver list for race={race!r}"
            )
        drivers = tuple(sorted({str(driver).strip() for driver in items if str(driver).strip()}))
        if not drivers:
            raise ValueError(
                f"manifest {path} has empty driver list for race={race!r}"
            )
        drivers_by_race[race] = drivers

    return ReplayManifest(
        year=year,
        season_tag=season_tag,
        run_id=run_id,
        races_in_order=tuple(races),
        drivers_by_race=drivers_by_race,
        path=Path(path),
    )


def load_latest_manifest(
    data_lake: Path,
    *,
    year: int,
    season_tag: str = "season",
) -> ReplayManifest:
    return load_manifest(latest_manifest_path(data_lake, year=year, season_tag=season_tag))


def validate_frame_against_manifest(
    frame: pd.DataFrame,
    manifest: ReplayManifest,
    *,
    race_column: str = "race",
    driver_column: str = "driver",
    context_label: str = "frame",
    allow_prefixed_race: bool = True,
    require_full_race_coverage: bool = True,
) -> dict[str, object]:
    if race_column not in frame.columns:
        raise ValueError(f"{context_label}: missing race column: {race_column}")
    if driver_column not in frame.columns:
        raise ValueError(f"{context_label}: missing driver column: {driver_column}")

    work = frame[[race_column, driver_column]].copy()
    work[race_column] = work[race_column].astype(str)
    work[driver_column] = work[driver_column].astype(str)

    if not allow_prefixed_race:
        prefixed = work[race_column].str.match(r"^\d{4}\s::\s")
        if prefixed.any():
            examples = work.loc[prefixed, race_column].drop_duplicates().head(5).tolist()
            raise ValueError(
                f"{context_label}: race keys must be unprefixed; examples={examples}"
            )

    work["_race_unprefixed"] = work[race_column].map(strip_year_prefix)

    known_races = set(manifest.races_in_order)
    observed_races = set(work["_race_unprefixed"].dropna().astype(str))
    missing_races = sorted(known_races - observed_races)
    extra_races = sorted(observed_races - known_races)

    invalid_pairs: list[tuple[str, str]] = []
    pair_frame = work[["_race_unprefixed", driver_column]].drop_duplicates()
    for row in pair_frame.itertuples(index=False):
        race = str(getattr(row, "_race_unprefixed"))
        driver = str(getattr(row, driver_column))
        if race not in known_races:
            continue
        allowed = set(manifest.drivers_by_race.get(race, ()))
        if driver not in allowed:
            invalid_pairs.append((race, driver))

    if extra_races or invalid_pairs or (require_full_race_coverage and missing_races):
        parts: list[str] = []
        if missing_races and require_full_race_coverage:
            parts.append(f"missing_races={missing_races[:8]}")
        if extra_races:
            parts.append(f"extra_races={extra_races[:8]}")
        if invalid_pairs:
            parts.append(f"invalid_driver_race_pairs={invalid_pairs[:8]}")
        raise ValueError(f"{context_label}: manifest contract failed: " + ", ".join(parts))

    return {
        "rows": int(len(work)),
        "races": int(len(observed_races)),
        "invalid_driver_race_pairs": 0,
        "missing_races": int(len(missing_races)),
        "extra_races": int(len(extra_races)),
    }


def expected_year_prefix_race_keys(manifest: ReplayManifest) -> set[str]:
    return {f"{manifest.year} :: {race}" for race in manifest.races_in_order}


def validate_prefixed_race_keys_against_manifest(
    race_values: Iterable[object],
    manifest: ReplayManifest,
    *,
    context_label: str,
) -> None:
    observed = {str(value).strip() for value in race_values if str(value).strip()}
    expected = expected_year_prefix_race_keys(manifest)
    missing = sorted(expected - observed)
    extra = sorted(observed - expected)
    if missing or extra:
        parts: list[str] = []
        if missing:
            parts.append(f"missing_prefixed_races={missing[:8]}")
        if extra:
            parts.append(f"unexpected_prefixed_races={extra[:8]}")
        raise ValueError(f"{context_label}: manifest-prefixed race set mismatch: " + ", ".join(parts))
