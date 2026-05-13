"""Validate sampled taxonomy rows against official Formula 1 sources.

This script enriches a sampled error-taxonomy CSV with plausibility fields:
1) official race calendar presence for the row year,
2) official race result/entry presence for the row driver,
3) official pit-stop summary laps (when available).

Output columns added:
- race_year_valid
- driver_race_valid
- official_pit_laps
- suggestion_to_nearest_official_pit_distance
- plausibility_status
- validation_source
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

import pandas as pd


F1_BASE = "https://www.formula1.com"
DEFAULT_TIMEOUT_SECONDS = 25
DEFAULT_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) F1-Plausibility-Checker/1.0"

# Maps project race names to Formula1.com slug conventions used in /results URLs.
RACE_NAME_TO_SLUG: dict[str, str] = {
    "Abu Dhabi Grand Prix": "abu-dhabi",
    "Australian Grand Prix": "australia",
    "Austrian Grand Prix": "austria",
    "Azerbaijan Grand Prix": "azerbaijan",
    "Bahrain Grand Prix": "bahrain",
    "Belgian Grand Prix": "belgium",
    "British Grand Prix": "great-britain",
    "Canadian Grand Prix": "canada",
    "Chinese Grand Prix": "china",
    "Dutch Grand Prix": "netherlands",
    "Emilia Romagna Grand Prix": "emilia-romagna",
    "French Grand Prix": "france",
    "Hungarian Grand Prix": "hungary",
    "Italian Grand Prix": "italy",
    "Japanese Grand Prix": "japan",
    "Las Vegas Grand Prix": "las-vegas",
    "Mexico City Grand Prix": "mexico",
    "Miami Grand Prix": "miami",
    "Monaco Grand Prix": "monaco",
    "Qatar Grand Prix": "qatar",
    "Saudi Arabian Grand Prix": "saudi-arabia",
    "Singapore Grand Prix": "singapore",
    "Spanish Grand Prix": "spain",
    "São Paulo Grand Prix": "brazil",
    "United States Grand Prix": "usa",
}

RACE_NAME_FALLBACK_SLUGS: dict[str, list[str]] = {
    "Mexico City Grand Prix": ["mexico-city", "mexico"],
    "United States Grand Prix": ["united-states", "usa"],
    "São Paulo Grand Prix": ["brazil", "sao-paulo", "sao-paulo-grand-prix"],
    "Emilia Romagna Grand Prix": ["emilia-romagna", "italy", "imola"],
}

CODE_BLACKLIST = {
    "DNF",
    "DNS",
    "DSQ",
    "DQ",
    "NC",
    "FIA",
    "FIA",
    "F1",
    "PTS",
    "POS",
}


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        text = data.strip()
        if text:
            self.parts.append(text)

    def as_text(self) -> str:
        joined = " ".join(self.parts)
        joined = unescape(joined)
        joined = joined.replace("\xa0", " ")
        joined = re.sub(r"\s+", " ", joined).strip()
        return joined


@dataclass(frozen=True)
class FetchResult:
    ok: bool
    url: str
    html: str
    error: str
    from_cache: bool


@dataclass(frozen=True)
class RaceIndex:
    year: int
    index_url: str
    race_result_urls_by_slug: dict[str, str]
    fetch_error: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check sampled taxonomy rows against official F1 pages")
    parser.add_argument("--input-csv", type=Path, required=True, help="Sample taxonomy CSV")
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=None,
        help="Output CSV path (default: <input stem>_plausibility.csv)",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("data_lake/reports/_official_f1_cache"),
        help="Directory for cached official HTML pages",
    )
    parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    parser.add_argument(
        "--overwrite-input",
        action="store_true",
        help="Write enrichment columns back into --input-csv instead of separate output file",
    )
    return parser.parse_args()


def _safe_int(value: object) -> int | None:
    try:
        number = int(float(value))
    except (TypeError, ValueError):
        return None
    return number


def _split_race_field(value: object) -> tuple[int | None, str]:
    text = str(value).strip()
    if " :: " in text:
        year_part, race_name = text.split(" :: ", 1)
        year = _safe_int(year_part)
        return year, race_name.strip()
    return None, text


def _slug_candidates(race_name: str) -> list[str]:
    race_name = race_name.strip()
    out: list[str] = []
    canonical = RACE_NAME_TO_SLUG.get(race_name)
    if canonical:
        out.append(canonical)
    out.extend(RACE_NAME_FALLBACK_SLUGS.get(race_name, []))

    generic = (
        race_name.lower()
        .replace("’", "")
        .replace("'", "")
        .replace(".", "")
        .replace(" grand prix", "")
        .replace(" ", "-")
    )
    if generic and generic not in out:
        out.append(generic)
    full_generic = race_name.lower().replace("’", "").replace("'", "").replace(".", "").replace(" ", "-")
    if full_generic and full_generic not in out:
        out.append(full_generic)
    return out


def _extract_text(html: str) -> str:
    parser = _TextExtractor()
    parser.feed(html)
    return parser.as_text()


def _cache_path(cache_dir: Path, url: str) -> Path:
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()
    return cache_dir / f"{digest}.html"


def _fetch_html(url: str, cache_dir: Path, timeout_seconds: int, user_agent: str) -> FetchResult:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cpath = _cache_path(cache_dir, url)
    if cpath.exists():
        return FetchResult(ok=True, url=url, html=cpath.read_text(encoding="utf-8"), error="", from_cache=True)

    request = Request(url, headers={"User-Agent": user_agent})
    try:
        with urlopen(request, timeout=timeout_seconds) as resp:
            payload = resp.read().decode("utf-8", errors="ignore")
        cpath.write_text(payload, encoding="utf-8")
        return FetchResult(ok=True, url=url, html=payload, error="", from_cache=False)
    except HTTPError as exc:
        return FetchResult(ok=False, url=url, html="", error=f"HTTPError {exc.code}", from_cache=False)
    except URLError as exc:
        return FetchResult(ok=False, url=url, html="", error=f"URLError {exc.reason}", from_cache=False)
    except Exception as exc:  # pragma: no cover - defensive path
        return FetchResult(ok=False, url=url, html="", error=f"{type(exc).__name__}: {exc}", from_cache=False)


def _extract_race_result_links(index_html: str, year: int, index_url: str) -> dict[str, str]:
    pattern = re.compile(
        rf"(https?://www\.formula1\.com)?(/en/results/{year}/races/\d+/([a-z0-9-]+)/race-result)\b",
        flags=re.IGNORECASE,
    )
    links: dict[str, str] = {}
    for match in pattern.finditer(index_html):
        rel = match.group(2)
        slug = match.group(3).lower()
        full_url = urljoin(index_url, rel)
        links[slug] = full_url
    return links


def _build_race_index(year: int, cache_dir: Path, timeout_seconds: int, user_agent: str) -> RaceIndex:
    index_url = f"{F1_BASE}/en/results/{year}/races"
    fetched = _fetch_html(index_url, cache_dir, timeout_seconds, user_agent)
    if not fetched.ok:
        # Fallback to historical URL flavor.
        fallback_url = f"{F1_BASE}/en/results.html/{year}/races.html"
        fallback = _fetch_html(fallback_url, cache_dir, timeout_seconds, user_agent)
        if not fallback.ok:
            return RaceIndex(year=year, index_url=index_url, race_result_urls_by_slug={}, fetch_error=fetched.error)
        links = _extract_race_result_links(fallback.html, year, fallback_url)
        return RaceIndex(year=year, index_url=fallback_url, race_result_urls_by_slug=links, fetch_error="")

    links = _extract_race_result_links(fetched.html, year, index_url)
    return RaceIndex(year=year, index_url=index_url, race_result_urls_by_slug=links, fetch_error="")


def _extract_section(text: str, starts: Iterable[str], end_markers: Iterable[str]) -> str:
    lower = text.lower()
    start_idx = -1
    for marker in starts:
        pos = lower.find(marker.lower())
        if pos >= 0:
            start_idx = pos
            break
    if start_idx < 0:
        return ""
    end_idx = len(text)
    for marker in end_markers:
        pos = lower.find(marker.lower(), start_idx + 1)
        if pos >= 0:
            end_idx = min(end_idx, pos)
    return text[start_idx:end_idx]


def _extract_driver_codes_from_race_result_text(text: str, allowed_codes: set[str]) -> set[str]:
    section = _extract_section(
        text,
        starts=[
            "Pos.No.Driver Team Laps Time / Retired Pts.",
            "Pos. No. Driver Team Laps Time / Retired Pts.",
            "Pos.No.Driver Team Laps Time/Retired Pts.",
        ],
        end_markers=["OUR PARTNERS", "Download the Official F1 App"],
    )
    if not section:
        section = text
    codes = set(re.findall(r"\b[A-Z]{3}\b", section))
    codes = {c for c in codes if c not in CODE_BLACKLIST}
    if allowed_codes:
        codes = {c for c in codes if c in allowed_codes}
    return codes


def _extract_pit_laps_for_driver(text: str, driver_code: str) -> list[int]:
    section = _extract_section(
        text,
        starts=[
            "Stops No.Driver Team Lap Time of Day Time Total",
            "Stops No. Driver Team Lap Time of Day Time Total",
        ],
        end_markers=["OUR PARTNERS", "Download the Official F1 App"],
    )
    if not section or "No results available" in section:
        return []

    # Row-like pattern in plain text:
    # "1 4 Lando Norris NOR McLaren 2 15:22:58 13.341 13.341"
    pattern = re.compile(
        r"\b\d+\s+\d+\s+[A-Za-zÀ-ÿ' .-]+?\s+([A-Z]{3})\s+[A-Za-zÀ-ÿ0-9' .&-]+?\s+(\d+)\s+\d{1,2}:\d{2}:\d{2}\b"
    )
    laps: list[int] = []
    for code, lap in pattern.findall(section):
        if code == driver_code:
            try:
                laps.append(int(lap))
            except ValueError:
                continue
    return sorted(set(laps))


def _resolve_race_result_url(index: RaceIndex, race_name: str) -> str | None:
    if not index.race_result_urls_by_slug:
        return None
    candidates = _slug_candidates(race_name)
    for slug in candidates:
        if slug in index.race_result_urls_by_slug:
            return index.race_result_urls_by_slug[slug]
    return None


def _nearest_signed_distance(suggestion_lap: int, pit_laps: list[int]) -> int | None:
    if not pit_laps:
        return None
    best = min(pit_laps, key=lambda p: (abs(p - suggestion_lap), p))
    return int(best - suggestion_lap)


def _status(
    race_year_valid: bool | None,
    driver_race_valid: bool | None,
    pit_laps: list[int],
    source_error: str,
) -> str:
    if source_error:
        return "UNVERIFIED_SOURCE_ERROR"
    if race_year_valid is False:
        return "INVALID_RACE_YEAR"
    if driver_race_valid is False:
        return "INVALID_DRIVER_RACE"
    if driver_race_valid is None:
        return "UNVERIFIED_DRIVER_RACE"
    if pit_laps:
        return "VALIDATED_WITH_PIT_SUMMARY"
    return "VALIDATED_WITHOUT_PIT_SUMMARY"


def main() -> None:
    args = parse_args()
    input_csv = args.input_csv
    if not input_csv.exists():
        raise FileNotFoundError(f"input csv not found: {input_csv}")

    output_csv = input_csv if args.overwrite_input else (
        args.output_csv
        if args.output_csv is not None
        else input_csv.with_name(input_csv.stem + "_plausibility.csv")
    )

    df = pd.read_csv(input_csv)
    required = {"race", "driver", "suggestion_lap"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"input csv missing required columns: {missing}")

    allowed_codes = set(df["driver"].astype(str).str.strip().str.upper().unique())

    years = sorted(
        {
            year
            for year, _ in (_split_race_field(race) for race in df["race"].astype(str))
            if year is not None
        }
    )
    race_indexes: dict[int, RaceIndex] = {
        year: _build_race_index(year, args.cache_dir, args.timeout_seconds, args.user_agent)
        for year in years
    }

    out_rows: list[dict[str, object]] = []

    for _, row in df.iterrows():
        race_raw = row.get("race")
        driver = str(row.get("driver", "")).strip().upper()
        suggestion_lap = _safe_int(row.get("suggestion_lap"))
        year, race_name = _split_race_field(race_raw)

        race_year_valid: bool | None = None
        driver_race_valid: bool | None = None
        official_pit_laps: list[int] = []
        nearest_distance: int | None = None
        validation_parts: list[str] = []
        source_error = ""

        if year is None:
            source_error = "invalid_race_key_no_year_prefix"
            validation_parts.append("calendar=missing_year")
        else:
            index = race_indexes.get(year)
            if index is None:
                source_error = f"no_index_for_year_{year}"
                validation_parts.append(f"calendar={F1_BASE}/en/results/{year}/races")
            elif index.fetch_error:
                source_error = index.fetch_error
                validation_parts.append(f"calendar={index.index_url} (error: {index.fetch_error})")
            else:
                result_url = _resolve_race_result_url(index, race_name)
                race_year_valid = result_url is not None
                validation_parts.append(f"calendar={index.index_url}")
                if result_url is None:
                    validation_parts.append("race_result=unresolved")
                else:
                    validation_parts.append(f"race_result={result_url}")
                    race_fetch = _fetch_html(result_url, args.cache_dir, args.timeout_seconds, args.user_agent)
                    if not race_fetch.ok:
                        source_error = race_fetch.error
                        validation_parts.append(f"race_result_error={race_fetch.error}")
                    else:
                        race_text = _extract_text(race_fetch.html)
                        race_codes = _extract_driver_codes_from_race_result_text(race_text, allowed_codes)
                        if race_codes:
                            driver_race_valid = driver in race_codes
                        else:
                            # Fallback to starting grid (entry proxy) if race result has no rows.
                            grid_url = result_url.replace("/race-result", "/starting-grid")
                            validation_parts.append(f"starting_grid={grid_url}")
                            grid_fetch = _fetch_html(grid_url, args.cache_dir, args.timeout_seconds, args.user_agent)
                            if grid_fetch.ok:
                                grid_codes = _extract_driver_codes_from_race_result_text(
                                    _extract_text(grid_fetch.html),
                                    allowed_codes,
                                )
                                if grid_codes:
                                    driver_race_valid = driver in grid_codes
                                else:
                                    driver_race_valid = None
                            else:
                                driver_race_valid = None
                                validation_parts.append(f"starting_grid_error={grid_fetch.error}")

                        pit_url = result_url.replace("/race-result", "/pit-stop-summary")
                        validation_parts.append(f"pit_stop_summary={pit_url}")
                        pit_fetch = _fetch_html(pit_url, args.cache_dir, args.timeout_seconds, args.user_agent)
                        if pit_fetch.ok:
                            pit_text = _extract_text(pit_fetch.html)
                            official_pit_laps = _extract_pit_laps_for_driver(pit_text, driver)
                        else:
                            validation_parts.append(f"pit_stop_error={pit_fetch.error}")

                        if suggestion_lap is not None:
                            nearest_distance = _nearest_signed_distance(suggestion_lap, official_pit_laps)

        status = _status(race_year_valid, driver_race_valid, official_pit_laps, source_error)

        updated = dict(row)
        updated["race_year_valid"] = race_year_valid
        updated["driver_race_valid"] = driver_race_valid
        updated["official_pit_laps"] = ";".join(str(x) for x in official_pit_laps)
        updated["suggestion_to_nearest_official_pit_distance"] = nearest_distance
        updated["plausibility_status"] = status
        updated["validation_source"] = " ; ".join(validation_parts)
        out_rows.append(updated)

    out_df = pd.DataFrame(out_rows)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(output_csv, index=False)

    status_counts = out_df["plausibility_status"].value_counts(dropna=False).to_dict()
    print("=== PLAUSIBILITY CHECK COMPLETE ===")
    print(f"input csv              : {input_csv}")
    print(f"output csv             : {output_csv}")
    print(f"rows                   : {len(out_df)}")
    print(f"years indexed          : {years}")
    print(f"status counts          : {status_counts}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.", file=sys.stderr)
        raise
