"""Fail-fast validator for Flink stream artifacts and prepared ML datasets."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from pipeline_config import (
    DEFAULT_DATA_LAKE,
    DEFAULT_SEASON_TAG,
    DEFAULT_YEARS,
    default_dataset_path,
    normalize_years,
    reports_dir,
    run_suffix,
)
from lib.data_preparation import _latest_jsonl, _load_jsonl
from lib.replay_manifest import (
    ReplayManifest,
    load_latest_manifest,
    strip_year_prefix,
    validate_frame_against_manifest,
)


STREAM_KEY_FIELDS: dict[str, tuple[str, ...]] = {
    "ml_features": ("race", "driver", "lapNumber"),
    "pit_suggestions": ("race", "driver", "lapNumber"),
    "drop_zones": ("race", "driver", "lapNumber"),
    "pit_evals": ("race", "driver", "pitLapNumber"),
}


@dataclass(frozen=True)
class CheckRow:
    check: str
    status: str
    value: float
    note: str


def _ok(ok: bool) -> str:
    return "PASS" if ok else "FAIL"


def _manifest_for_year(data_lake: Path, year: int, season_tag: str) -> ReplayManifest:
    try:
        return load_latest_manifest(data_lake, year=year, season_tag=season_tag)
    except FileNotFoundError:
        if season_tag == DEFAULT_SEASON_TAG:
            raise
    return load_latest_manifest(data_lake, year=year, season_tag=DEFAULT_SEASON_TAG)


def _duplicate_excess_rows(df: pd.DataFrame, keys: tuple[str, ...]) -> int:
    missing = [column for column in keys if column not in df.columns]
    if missing:
        raise ValueError(f"missing key columns {missing}")
    counts = df.groupby(list(keys), dropna=False).size()
    return int((counts[counts > 1] - 1).sum())


def _stream_year_checks(
    data_lake: Path,
    year: int,
    season_tag: str,
    manifest: ReplayManifest,
) -> tuple[list[CheckRow], dict[str, Path]]:
    rows: list[CheckRow] = []
    stream_paths: dict[str, Path] = {}

    for stream, keys in STREAM_KEY_FIELDS.items():
        path = _latest_jsonl(data_lake, stream, year, season_tag)
        stream_paths[stream] = path
        frame = _load_jsonl(path)
        dup_excess = _duplicate_excess_rows(frame, keys)
        rows.append(
            CheckRow(
                check=f"{year}:{stream}:duplicate_excess_rows",
                status=_ok(dup_excess == 0),
                value=float(dup_excess),
                note=f"keys={','.join(keys)} file={path}",
            )
        )

        require_full = stream in {"ml_features", "pit_evals"}
        try:
            validate_frame_against_manifest(
                frame,
                manifest,
                context_label=f"{stream} year={year}",
                allow_prefixed_race=False,
                require_full_race_coverage=require_full,
            )
            manifest_ok = True
            manifest_note = "manifest coverage/identity OK"
        except Exception as exc:  # noqa: BLE001 - fail-fast diagnostics
            manifest_ok = False
            manifest_note = str(exc)

        rows.append(
            CheckRow(
                check=f"{year}:{stream}:manifest_contract",
                status=_ok(manifest_ok),
                value=1.0 if manifest_ok else 0.0,
                note=manifest_note,
            )
        )

    # cross-stream consistency on shared lap keys
    ml = _load_jsonl(stream_paths["ml_features"]).copy()
    drop = _load_jsonl(stream_paths["drop_zones"]).copy()
    sug = _load_jsonl(stream_paths["pit_suggestions"]).copy()

    for frame in (ml, drop, sug):
        frame["race"] = frame["race"].astype(str)
        frame["driver"] = frame["driver"].astype(str)
        frame["lapNumber"] = pd.to_numeric(frame["lapNumber"], errors="coerce")
    ml = ml[ml["lapNumber"].notna()].copy()
    drop = drop[drop["lapNumber"].notna()].copy()
    sug = sug[sug["lapNumber"].notna()].copy()
    ml["lapNumber"] = ml["lapNumber"].astype(int)
    drop["lapNumber"] = drop["lapNumber"].astype(int)
    sug["lapNumber"] = sug["lapNumber"].astype(int)

    md = ml.merge(drop, on=["race", "driver", "lapNumber"], how="inner", suffixes=("_ml", "_drop"))
    ms = ml.merge(sug, on=["race", "driver", "lapNumber"], how="inner", suffixes=("_ml", "_sug"))

    if not md.empty and {"pitLoss_ml", "pitLoss_drop"}.issubset(md.columns):
        pitloss_diff = (
            pd.to_numeric(md["pitLoss_ml"], errors="coerce")
            - pd.to_numeric(md["pitLoss_drop"], errors="coerce")
        ).abs()
        mismatch = int((pitloss_diff > 0.05).sum())
        rows.append(
            CheckRow(
                check=f"{year}:cross_stream:ml_vs_drop_pitLoss_tolerance",
                status=_ok(mismatch == 0),
                value=float(mismatch),
                note=f"rows={len(md)} tolerance=0.05",
            )
        )

    if not md.empty and {"position_ml", "currentPosition"}.issubset(md.columns):
        pos_match = (
            pd.to_numeric(md["position_ml"], errors="coerce")
            == pd.to_numeric(md["currentPosition"], errors="coerce")
        )
        mismatch = int((~pos_match.fillna(False)).sum())
        rows.append(
            CheckRow(
                check=f"{year}:cross_stream:ml_vs_drop_position_match",
                status=_ok(mismatch == 0),
                value=float(mismatch),
                note=f"rows={len(md)}",
            )
        )

    if not ms.empty and {"trackStatus_ml", "trackStatus_sug"}.issubset(ms.columns):
        track_match = ms["trackStatus_ml"].astype(str) == ms["trackStatus_sug"].astype(str)
        mismatch = int((~track_match).sum())
        rows.append(
            CheckRow(
                check=f"{year}:cross_stream:ml_vs_suggestions_trackStatus_match",
                status=_ok(mismatch == 0),
                value=float(mismatch),
                note=f"rows={len(ms)}",
            )
        )

    return rows, stream_paths


def _dataset_checks(dataset: pd.DataFrame, manifests: dict[int, ReplayManifest]) -> list[CheckRow]:
    rows: list[CheckRow] = []
    required = ["race", "driver", "lapNumber"]
    missing = [column for column in required if column not in dataset.columns]
    if missing:
        raise ValueError(f"dataset missing required columns: {missing}")

    work = dataset.copy()
    work["race"] = work["race"].astype(str)
    work["driver"] = work["driver"].astype(str)
    work["lapNumber"] = pd.to_numeric(work["lapNumber"], errors="coerce")
    work = work[work["lapNumber"].notna()].copy()
    work["lapNumber"] = work["lapNumber"].astype(int)

    dup_excess = _duplicate_excess_rows(work, ("race", "driver", "lapNumber"))
    rows.append(
        CheckRow(
            check="dataset:unique_race_driver_lap",
            status=_ok(dup_excess == 0),
            value=float(dup_excess),
            note="keys=race,driver,lapNumber",
        )
    )

    work["_year"] = pd.to_numeric(work["race"].str.extract(r"^(\\d{4})")[0], errors="coerce")
    missing_year = int(work["_year"].isna().sum())
    rows.append(
        CheckRow(
            check="dataset:race_prefix_year_parse",
            status=_ok(missing_year == 0),
            value=float(missing_year),
            note="all race keys must be prefixed: 'YYYY :: Race Name'",
        )
    )

    work["_year"] = work["_year"].astype(int)
    work["_race_unprefixed"] = work["race"].map(strip_year_prefix)

    order_lookup: dict[tuple[int, str], int] = {}
    for year, manifest in manifests.items():
        for index, race in enumerate(manifest.races_in_order):
            order_lookup[(int(year), race)] = int(index)

    unknown_mask = ~work[["_year", "_race_unprefixed"]].apply(
        lambda row: (int(row["_year"]), str(row["_race_unprefixed"])) in order_lookup,
        axis=1,
    )
    unknown_rows = int(unknown_mask.sum())
    rows.append(
        CheckRow(
            check="dataset:race_in_manifest",
            status=_ok(unknown_rows == 0),
            value=float(unknown_rows),
            note="all prefixed races must exist in year-specific replay manifests",
        )
    )

    work["_race_order"] = work.apply(
        lambda row: order_lookup.get((int(row["_year"]), str(row["_race_unprefixed"])), 999999),
        axis=1,
    )

    sorted_view = work.sort_values(
        by=["_year", "_race_order", "lapNumber", "driver"],
        kind="mergesort",
    )[["race", "driver", "lapNumber"]].reset_index(drop=True)
    current_view = work[["race", "driver", "lapNumber"]].reset_index(drop=True)
    chronology_ok = bool(sorted_view.equals(current_view))
    rows.append(
        CheckRow(
            check="dataset:chronological_order",
            status=_ok(chronology_ok),
            value=1.0 if chronology_ok else 0.0,
            note="expected ordering: year -> manifest race order -> lapNumber -> driver",
        )
    )

    for year, manifest in manifests.items():
        observed = set(work.loc[work["_year"] == int(year), "_race_unprefixed"].dropna().astype(str))
        expected = set(manifest.races_in_order)
        missing_races = sorted(expected - observed)
        extra_races = sorted(observed - expected)
        ok = len(missing_races) == 0 and len(extra_races) == 0
        rows.append(
            CheckRow(
                check=f"dataset:race_set_year_{year}",
                status=_ok(ok),
                value=1.0 if ok else 0.0,
                note=f"missing={missing_races[:6]} extra={extra_races[:6]}",
            )
        )

    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="validate replay contracts before training")
    parser.add_argument("--data-lake", default=DEFAULT_DATA_LAKE, help="data lake directory")
    parser.add_argument("--years", type=int, nargs="+", default=list(DEFAULT_YEARS), help="season years")
    parser.add_argument("--season-tag", default=DEFAULT_SEASON_TAG, help="season tag for stream files")
    parser.add_argument("--dataset", default="", help="prepared dataset path (parquet/csv)")
    parser.add_argument("--summary-output", default="", help="output summary csv path")
    parser.add_argument("--report-output", default="", help="output text report path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_lake = Path(args.data_lake)
    years = normalize_years(args.years)
    dataset_path = (
        Path(args.dataset)
        if args.dataset
        else default_dataset_path(data_lake, years, args.season_tag)
    )

    suffix = run_suffix(years, args.season_tag)
    summary_output = (
        Path(args.summary_output)
        if args.summary_output
        else reports_dir(data_lake) / f"replay_contract_summary_{suffix}.csv"
    )
    report_output = (
        Path(args.report_output)
        if args.report_output
        else reports_dir(data_lake) / f"replay_contract_report_{suffix}.txt"
    )

    if not dataset_path.exists():
        raise FileNotFoundError(f"dataset not found: {dataset_path}")

    checks: list[CheckRow] = []
    manifests = {
        int(year): _manifest_for_year(data_lake, int(year), args.season_tag)
        for year in years
    }

    stream_paths: dict[str, Path] = {}
    for year in years:
        year_checks, year_paths = _stream_year_checks(
            data_lake,
            year,
            args.season_tag,
            manifests[int(year)],
        )
        checks.extend(year_checks)
        stream_paths.update({f"{year}:{stream}": path for stream, path in year_paths.items()})

    dataset = pd.read_parquet(dataset_path) if dataset_path.suffix.lower() == ".parquet" else pd.read_csv(dataset_path)
    checks.extend(_dataset_checks(dataset, manifests))

    summary_df = pd.DataFrame([row.__dict__ for row in checks])
    summary_output.parent.mkdir(parents=True, exist_ok=True)
    report_output.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(summary_output, index=False)

    failed = summary_df[summary_df["status"] == "FAIL"]
    lines = [
        "=== REPLAY CONTRACT VALIDATION ===",
        f"data_lake         : {data_lake}",
        f"years             : {years}",
        f"season_tag        : {args.season_tag}",
        f"dataset           : {dataset_path}",
        f"summary csv       : {summary_output}",
        "",
        "stream inputs",
    ]
    for key, path in sorted(stream_paths.items()):
        lines.append(f"- {key}: {path}")
    lines.extend(["", "checks"])
    for row in checks:
        lines.append(f"- {row.check}: {row.status} ({row.value})")
        lines.append(f"  note: {row.note}")

    overall_ok = failed.empty
    lines.extend(["", f"overall_gate      : {_ok(overall_ok)}"])
    report_output.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("\n".join(lines))
    if not overall_ok:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
