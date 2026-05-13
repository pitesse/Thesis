"""
Quick FastF1 connectivity probe for a single session.

Usage:
  python f1-telemetry-producer/src/fastf1_probe.py --year 2022 --race "Abu Dhabi Grand Prix" --session R
"""

from __future__ import annotations

import argparse
from pathlib import Path

from fastf1 import Cache, get_session
from fastf1 import _api  # private but useful for low-level diagnostics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe FastF1 session endpoints")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--race", type=str, required=True)
    parser.add_argument("--session", type=str, default="R")
    return parser.parse_args()


def _status(url: str) -> int:
    response = Cache.requests_get(url, headers=_api.headers)
    return int(response.status_code)


def main() -> None:
    args = parse_args()
    cache_dir = Path(__file__).resolve().parents[2] / "data" / "fastf1_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    Cache.enable_cache(str(cache_dir))

    session = get_session(args.year, args.race, args.session)
    print(f"FastF1 version probe for {args.year} {args.race} {args.session}")
    print(f"api_path: {session.api_path}")
    print(f"f1_api_support: {session.f1_api_support}")

    checks = ("session_info", "driver_list", "timing_data", "car_data", "position")
    for check in checks:
        page = _api.pages[check]
        base_url = _api.base_url + session.api_path + page
        mirror_url = _api.base_url_mirror + session.api_path + page

        base_status = _status(base_url)
        mirror_status = _status(mirror_url)
        print(f"{check:>12} | base={base_status} | mirror={mirror_status}")


if __name__ == "__main__":
    main()
