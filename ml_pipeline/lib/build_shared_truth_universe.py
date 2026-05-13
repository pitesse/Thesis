"""Build explicit ML/SDE race-driver truth-universe CSVs for cross-paradigm alignment."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd


_YEAR_PREFIX_RE = re.compile(r"^\d{4}\s::\s")


def _prefix_race_year(frame: pd.DataFrame, year: int) -> pd.DataFrame:
    out = frame.copy()
    out["race"] = out["race"].astype(str)
    mask = ~out["race"].str.match(_YEAR_PREFIX_RE)
    out.loc[mask, "race"] = f"{int(year)} :: " + out.loc[mask, "race"]
    return out


def _load_jsonl(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"jsonl not found: {path}")
    return pd.read_json(path, lines=True)


def _load_sde_universe(variant_runs_root: Path, variant_prefix: str, years: list[int]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for year in years:
        run_dir = variant_runs_root / f"{variant_prefix}{int(year)}"
        suggestions_path = run_dir / f"pit_suggestions_{int(year)}_season.jsonl"
        if not suggestions_path.exists():
            raise FileNotFoundError(f"missing archived suggestions: {suggestions_path}")
        frame = _load_jsonl(suggestions_path)
        if not {"race", "driver"}.issubset(frame.columns):
            raise ValueError(f"suggestions missing race/driver columns: {suggestions_path}")
        frame = frame[["race", "driver"]].copy()
        frame = _prefix_race_year(frame, int(year))
        frames.append(frame)

    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["race", "driver"])
    out["race"] = out["race"].astype(str)
    out["driver"] = out["driver"].astype(str)
    out = out.dropna(subset=["race", "driver"]).drop_duplicates(subset=["race", "driver"], keep="first")
    out.reset_index(drop=True, inplace=True)
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build ML/SDE/shared race-driver truth-universe CSVs.")
    parser.add_argument("--ml-oof-csv", required=True, help="Batch OOF csv to define ML race/driver universe")
    parser.add_argument("--years", type=int, nargs="+", required=True, help="years to include for SDE universe")
    parser.add_argument(
        "--sde-variant-runs-root",
        default="data_lake/reports/variant_runs",
        help="root path for archived SDE variant run folders",
    )
    parser.add_argument(
        "--sde-variant-prefix",
        default="c6_cfg120_fixed_",
        help="prefix for per-year SDE variant folders, e.g. c6_cfg120_fixed_",
    )
    parser.add_argument("--output-ml-universe-csv", required=True)
    parser.add_argument("--output-sde-universe-csv", required=True)
    parser.add_argument("--output-shared-universe-csv", required=True)
    parser.add_argument("--output-summary-csv", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ml_oof_path = Path(args.ml_oof_csv)
    sde_root = Path(args.sde_variant_runs_root)

    ml = pd.read_csv(ml_oof_path, usecols=["race", "driver"])
    ml["race"] = ml["race"].astype(str)
    ml["driver"] = ml["driver"].astype(str)
    ml = ml.dropna(subset=["race", "driver"]).drop_duplicates(subset=["race", "driver"], keep="first")
    ml.reset_index(drop=True, inplace=True)

    sde = _load_sde_universe(sde_root, str(args.sde_variant_prefix), [int(y) for y in args.years])

    ml_keys = set(map(tuple, ml[["race", "driver"]].itertuples(index=False, name=None)))
    sde_keys = set(map(tuple, sde[["race", "driver"]].itertuples(index=False, name=None)))
    shared_keys = ml_keys & sde_keys

    shared = pd.DataFrame(sorted(shared_keys), columns=["race", "driver"])
    ml_only = len(ml_keys - sde_keys)
    sde_only = len(sde_keys - ml_keys)

    summary = pd.DataFrame(
        [
            {"universe": "ml_oof", "race_driver_count": int(len(ml_keys))},
            {"universe": "sde_variant", "race_driver_count": int(len(sde_keys))},
            {"universe": "shared_intersection", "race_driver_count": int(len(shared_keys))},
            {"universe": "ml_only", "race_driver_count": int(ml_only)},
            {"universe": "sde_only", "race_driver_count": int(sde_only)},
        ]
    )

    out_ml = Path(args.output_ml_universe_csv)
    out_sde = Path(args.output_sde_universe_csv)
    out_shared = Path(args.output_shared_universe_csv)
    out_summary = Path(args.output_summary_csv)
    for path in (out_ml, out_sde, out_shared, out_summary):
        path.parent.mkdir(parents=True, exist_ok=True)

    ml.sort_values(by=["race", "driver"], kind="mergesort").to_csv(out_ml, index=False)
    sde.sort_values(by=["race", "driver"], kind="mergesort").to_csv(out_sde, index=False)
    shared.sort_values(by=["race", "driver"], kind="mergesort").to_csv(out_shared, index=False)
    summary.to_csv(out_summary, index=False)

    print("=== SHARED TRUTH UNIVERSE BUILT ===")
    print(f"ml oof universe        : {out_ml}")
    print(f"sde variant universe   : {out_sde}")
    print(f"shared intersection    : {out_shared}")
    print(f"summary                : {out_summary}")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
