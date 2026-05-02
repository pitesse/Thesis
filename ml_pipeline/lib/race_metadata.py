"""Static race metadata used by causal, race-agnostic feature engineering."""

from __future__ import annotations

import re

SCHEDULED_RACE_LAPS_BASE: dict[str, int] = {
    "Abu Dhabi Grand Prix": 58,
    "Australian Grand Prix": 58,
    "Austrian Grand Prix": 71,
    "Azerbaijan Grand Prix": 51,
    "Bahrain Grand Prix": 57,
    "Belgian Grand Prix": 44,
    "British Grand Prix": 52,
    "Canadian Grand Prix": 70,
    "Chinese Grand Prix": 56,
    "Dutch Grand Prix": 72,
    "Emilia Romagna Grand Prix": 63,
    "French Grand Prix": 53,
    "Hungarian Grand Prix": 70,
    "Italian Grand Prix": 53,
    "Japanese Grand Prix": 53,
    "Las Vegas Grand Prix": 50,
    "Mexico City Grand Prix": 71,
    "Miami Grand Prix": 57,
    "Monaco Grand Prix": 78,
    "Qatar Grand Prix": 57,
    "Saudi Arabian Grand Prix": 50,
    "Singapore Grand Prix": 62,
    "Spanish Grand Prix": 66,
    "São Paulo Grand Prix": 71,
    "United States Grand Prix": 56,
}

# Source: Formula 1 official circuit/race pages (number of laps).
SCHEDULED_RACE_LAPS: dict[int, dict[str, int]] = {
    year: dict(SCHEDULED_RACE_LAPS_BASE) for year in (2022, 2023, 2024, 2025)
}

_RACE_PREFIX_RE = re.compile(r"^\d{4}\s::\s")


def race_name_without_year_prefix(race: object) -> str:
    return _RACE_PREFIX_RE.sub("", str(race))


def get_scheduled_laps(year: int, race_name: str) -> int:
    try:
        per_year = SCHEDULED_RACE_LAPS[int(year)]
    except (TypeError, ValueError, KeyError) as exc:
        raise ValueError(
            f"scheduled laps metadata missing year={year}; "
            f"available_years={sorted(SCHEDULED_RACE_LAPS)}"
        ) from exc

    name = str(race_name).strip()
    if name not in per_year:
        raise ValueError(
            f"scheduled laps metadata missing race={name!r} for year={year}; "
            f"known_races={sorted(per_year)}"
        )
    laps = int(per_year[name])
    if laps <= 0:
        raise ValueError(
            f"invalid scheduled laps value for year={year}, race={name!r}: {laps}"
        )
    return laps
