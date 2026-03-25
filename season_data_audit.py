import argparse
import glob
import json
from pathlib import Path

import pandas as pd


STREAMS = (
    "ml_features",
    "pit_evals",
    "pit_suggestions",
    "drop_zones",
    "lift_coast",
    "tire_drops",
)


def _pct(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return (float(numerator) / float(denominator)) * 100.0


def _latest_file(pattern: str) -> str | None:
    matches = glob.glob(pattern)
    if not matches:
        return None
    return max(matches, key=lambda path: Path(path).stat().st_mtime)


def _value_counts_as_dict(series: pd.Series) -> dict[str, int]:
    counts = series.value_counts(dropna=False)
    result: dict[str, int] = {}
    for key, value in counts.items():
        result[str(key)] = int(value)
    return result


def _safe_to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def _numeric_stats(series: pd.Series) -> dict[str, float | None]:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return {"min": None, "mean": None, "p95": None, "max": None}
    return {
        "min": round(float(clean.min()), 3),
        "mean": round(float(clean.mean()), 3),
        "p95": round(float(clean.quantile(0.95)), 3),
        "max": round(float(clean.max()), 3),
    }


def _generic_metrics(df: pd.DataFrame) -> dict:
    rows = len(df)
    null_rates = (df.isna().sum() / rows * 100.0) if rows else pd.Series(dtype=float)
    null_rates = null_rates.sort_values(ascending=False)
    top_nulls = {
        str(col): round(float(rate), 2)
        for col, rate in null_rates.head(8).items()
        if rate > 0
    }

    return {
        "rows": rows,
        "columns": int(len(df.columns)),
        "exact_duplicate_rows": int(df.duplicated().sum()),
        "top_null_percent_columns": top_nulls,
    }


def _audit_ml_features(df: pd.DataFrame) -> dict:
    negative_gap_mask = pd.Series([False] * len(df), index=df.index)
    if "gapAhead" in df.columns:
        negative_gap_mask |= pd.to_numeric(df["gapAhead"], errors="coerce") < 0
    if "gapBehind" in df.columns:
        negative_gap_mask |= pd.to_numeric(df["gapBehind"], errors="coerce") < 0

    null_gap_mask = pd.Series([False] * len(df), index=df.index)
    if "gapAhead" in df.columns:
        null_gap_mask |= df["gapAhead"].isna()
    if "gapBehind" in df.columns:
        null_gap_mask |= df["gapBehind"].isna()

    metrics: dict[str, object] = {
        "negative_gap_rows": int(negative_gap_mask.sum()),
        "negative_gap_rate_pct": round(_pct(int(negative_gap_mask.sum()), len(df)), 2),
        "rows_with_any_null_gap": int(null_gap_mask.sum()),
        "rows_with_any_null_gap_pct": round(_pct(int(null_gap_mask.sum()), len(df)), 2),
    }

    if "trackStatus" in df.columns:
        metrics["track_status_distribution"] = _value_counts_as_dict(
            df["trackStatus"].astype(str)
        )

    if "race" in df.columns:
        race_counts = df["race"].astype(str).value_counts(dropna=False)
        metrics["unique_races"] = int(race_counts.shape[0])
        metrics["top_races_by_rows"] = {
            str(race): int(count) for race, count in race_counts.head(10).items()
        }

    return metrics


def _audit_pit_evals(df: pd.DataFrame) -> dict:
    metrics: dict[str, object] = {}

    if "result" in df.columns:
        metrics["result_distribution"] = _value_counts_as_dict(df["result"].astype(str))

    if "resolvedVia" in df.columns:
        metrics["resolved_via_distribution"] = _value_counts_as_dict(
            df["resolvedVia"].astype(str)
        )

    if "result" in df.columns:
        unresolved = int((df["result"] == "UNRESOLVED_INSUFFICIENT_DATA").sum())
        metrics["unresolved_rows"] = unresolved
        metrics["unresolved_rate_pct"] = round(_pct(unresolved, len(df)), 2)

    if "postPitGapToRival" in df.columns:
        null_post = int(df["postPitGapToRival"].isna().sum())
        metrics["null_post_pit_gap_rows"] = null_post
        metrics["null_post_pit_gap_rate_pct"] = round(_pct(null_post, len(df)), 2)

    if "compound" in df.columns:
        null_compound = int(df["compound"].isna().sum())
        metrics["null_compound_rows"] = null_compound
        metrics["null_compound_rate_pct"] = round(_pct(null_compound, len(df)), 2)

    if "race" in df.columns:
        race_counts = df["race"].astype(str).value_counts(dropna=False)
        metrics["unique_races"] = int(race_counts.shape[0])

    return metrics


def _audit_pit_suggestions(df: pd.DataFrame) -> dict:
    metrics: dict[str, object] = {}

    if "suggestionLabel" in df.columns:
        metrics["suggestion_label_distribution"] = _value_counts_as_dict(
            df["suggestionLabel"].astype(str)
        )

    if {"driver", "lapNumber"}.issubset(df.columns):
        counts = df.groupby(["driver", "lapNumber"], dropna=False).size()
        duplicate_keys = int((counts > 1).sum())
        duplicate_excess = int((counts[counts > 1] - 1).sum())
        metrics["driver_lap_duplicate_keys"] = duplicate_keys
        metrics["driver_lap_duplicate_excess_rows"] = duplicate_excess

    if "trackStatus" in df.columns:
        metrics["track_status_distribution"] = _value_counts_as_dict(
            df["trackStatus"].astype(str)
        )

    return metrics


def _audit_drop_zones(df: pd.DataFrame) -> dict:
    metrics: dict[str, object] = {}

    if "trackStatus" in df.columns:
        metrics["track_status_distribution"] = _value_counts_as_dict(
            df["trackStatus"].astype(str)
        )

    if "positionsLost" in df.columns:
        negative_positions_lost = int(
            (pd.to_numeric(df["positionsLost"], errors="coerce") < 0).sum()
        )
        metrics["negative_positions_lost_rows"] = negative_positions_lost

    if "gapToPhysicalCar" in df.columns:
        negative_gap = int(
            (pd.to_numeric(df["gapToPhysicalCar"], errors="coerce") < 0).sum()
        )
        metrics["negative_gap_to_physical_car_rows"] = negative_gap

    return metrics


def _audit_lift_coast(df: pd.DataFrame) -> dict:
    metrics: dict[str, object] = {}

    if "trackStatus" in df.columns:
        metrics["track_status_distribution"] = _value_counts_as_dict(
            df["trackStatus"].astype(str)
        )

    required = {"fullThrottleDate", "liftDate", "brakeDate"}
    if required.issubset(df.columns):
        full = _safe_to_datetime(df["fullThrottleDate"])
        lift = _safe_to_datetime(df["liftDate"])
        brake = _safe_to_datetime(df["brakeDate"])

        valid = full.notna() & lift.notna() & brake.notna()
        full = full[valid]
        lift = lift[valid]
        brake = brake[valid]

        full_to_lift = (lift - full).dt.total_seconds()
        lift_to_brake = (brake - lift).dt.total_seconds()
        bad_order = int((~((full < lift) & (lift < brake))).sum())

        metrics["timestamp_parse_failures"] = int((~valid).sum())
        metrics["bad_time_order_rows"] = bad_order
        metrics["full_to_lift_seconds"] = _numeric_stats(full_to_lift)
        metrics["lift_to_brake_seconds"] = _numeric_stats(lift_to_brake)

    return metrics


def _audit_tire_drops(df: pd.DataFrame) -> dict:
    metrics: dict[str, object] = {}
    if "delta" in df.columns:
        delta = pd.to_numeric(df["delta"], errors="coerce")
        metrics["negative_delta_rows"] = int((delta < 0).sum())
        metrics["delta_seconds"] = _numeric_stats(delta)
    metrics["has_track_status_column"] = bool("trackStatus" in df.columns)
    return metrics


def _representativeness_summary(report: dict, expected_races: int) -> dict:
    ml = report.get("streams", {}).get("ml_features", {}).get("metrics", {})
    pe = report.get("streams", {}).get("pit_evals", {}).get("metrics", {})

    ml_races = int(ml.get("unique_races", 0) or 0)
    pe_races = int(pe.get("unique_races", 0) or 0)

    coverage = min(ml_races, pe_races)
    return {
        "expected_races": expected_races,
        "ml_features_unique_races": ml_races,
        "pit_evals_unique_races": pe_races,
        "calendar_coverage_pct": round(_pct(coverage, expected_races), 2),
    }


def run_audit(data_lake: str, year: int, season_tag: str, expected_races: int) -> dict:
    report: dict[str, object] = {
        "inputs": {
            "data_lake": data_lake,
            "year": year,
            "season_tag": season_tag,
        },
        "streams": {},
    }

    for stream in STREAMS:
        pattern = f"{data_lake}/{stream}_{year}_{season_tag}_*.jsonl"
        latest = _latest_file(pattern)
        if latest is None:
            report["streams"][stream] = {
                "status": "missing",
                "pattern": pattern,
            }
            continue

        df = pd.read_json(latest, lines=True)
        stream_report = {
            "status": "ok",
            "file": latest,
            "generic": _generic_metrics(df),
            "metrics": {},
        }

        if stream == "ml_features":
            stream_report["metrics"] = _audit_ml_features(df)
        elif stream == "pit_evals":
            stream_report["metrics"] = _audit_pit_evals(df)
        elif stream == "pit_suggestions":
            stream_report["metrics"] = _audit_pit_suggestions(df)
        elif stream == "drop_zones":
            stream_report["metrics"] = _audit_drop_zones(df)
        elif stream == "lift_coast":
            stream_report["metrics"] = _audit_lift_coast(df)
        elif stream == "tire_drops":
            stream_report["metrics"] = _audit_tire_drops(df)

        report["streams"][stream] = stream_report

    report["representativeness"] = _representativeness_summary(report, expected_races)
    return report


def _print_report(report: dict) -> None:
    print("=== SEASON DATA AUDIT ===")
    print(json.dumps(report["inputs"], indent=2))
    print()

    for stream in STREAMS:
        stream_report = report["streams"].get(stream, {})
        print(f"--- {stream} ---")
        if stream_report.get("status") != "ok":
            print(json.dumps(stream_report, indent=2))
            continue

        print(f"file: {stream_report['file']}")
        print("generic:")
        print(json.dumps(stream_report["generic"], indent=2))
        print("metrics:")
        print(json.dumps(stream_report["metrics"], indent=2))
        print()

    print("--- representativeness ---")
    print(json.dumps(report["representativeness"], indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit season-level JSONL outputs for quality and representativeness."
    )
    parser.add_argument("--data-lake", default="data_lake")
    parser.add_argument("--year", type=int, default=2023)
    parser.add_argument("--season-tag", default="season")
    parser.add_argument("--expected-races", type=int, default=22)
    parser.add_argument("--json-out", default=None)
    args = parser.parse_args()

    report = run_audit(
        data_lake=args.data_lake,
        year=args.year,
        season_tag=args.season_tag,
        expected_races=args.expected_races,
    )
    _print_report(report)

    if args.json_out:
        output_path = Path(args.json_out)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"\njson report written to: {output_path}")


if __name__ == "__main__":
    main()