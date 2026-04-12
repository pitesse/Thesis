import argparse
import glob
import json
from pathlib import Path
from typing import Any

import pandas as pd


STREAMS = (
    "ml_features",
    "pit_evals",
    "pit_suggestions",
    "drop_zones",
    "lift_coast",
    "tire_drops",
)

GREEN_STATUS_CODE = "1"
EXTREME_GAP_THRESHOLD_PCT = 20.0
WET_COMPOUNDS = ("INTERMEDIATE", "WET")
DEFAULT_DATA_LAKE = "data_lake"
DEFAULT_YEAR = 2023
DEFAULT_SEASON_TAG = "season"
DEFAULT_EXPECTED_RACES = 22


def _is_scalar(value: Any) -> bool:
    return not isinstance(value, (dict, list, tuple))


def _fmt_scalar(value: Any) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:.3f}".rstrip("0").rstrip(".")
    return str(value)


def _label(name: str) -> str:
    return name.replace("_", " ")


def _print_list(items: list[Any], indent: int = 0, max_items: int = 6) -> None:
    pad = " " * indent
    if not items:
        print(f"{pad}(none)")
        return

    if all(_is_scalar(item) for item in items):
        preview = ", ".join(_fmt_scalar(item) for item in items[:max_items])
        if len(items) > max_items:
            preview += f", ... (+{len(items) - max_items} more)"
        print(f"{pad}{preview}")
        return

    for idx, item in enumerate(items[:max_items], start=1):
        print(f"{pad}{idx}:")
        if isinstance(item, dict):
            _print_mapping(item, indent=indent + 2)
        elif isinstance(item, list):
            _print_list(item, indent=indent + 2)
        else:
            print(f"{' ' * (indent + 2)}{_fmt_scalar(item)}")

    if len(items) > max_items:
        print(f"{pad}... (+{len(items) - max_items} more)")


def _print_mapping(data: dict[str, Any], indent: int = 0) -> None:
    if not data:
        print(f"{' ' * indent}(none)")
        return

    scalars = {k: v for k, v in data.items() if _is_scalar(v)}
    nested = {k: v for k, v in data.items() if not _is_scalar(v)}

    if scalars:
        width = max(len(_label(str(key))) for key in scalars)
        for key, value in scalars.items():
            print(
                f"{' ' * indent}{_label(str(key)):<{width}} : {_fmt_scalar(value)}"
            )

    for key, value in nested.items():
        print(f"{' ' * indent}{_label(str(key))}:")
        if isinstance(value, dict):
            _print_mapping(value, indent=indent + 2)
        elif isinstance(value, list):
            _print_list(value, indent=indent + 2)
        else:
            print(f"{' ' * (indent + 2)}{_fmt_scalar(value)}")


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
    # pandas mixed parser handles common iso-8601 variations across flink/kafka exports.
    try:
        parsed = pd.to_datetime(series, errors="coerce", format="mixed", utc=True)
    except TypeError:
        # fallback for older pandas versions without format="mixed" support.
        parsed = pd.to_datetime(series, errors="coerce", utc=True)

    if parsed.isna().any():
        # normalize timezone suffixes like +0000 and whitespace separators before retry.
        normalized = series.astype("string").str.strip()
        normalized = normalized.str.replace(" ", "T", regex=False)
        normalized = normalized.str.replace(r"Z$", "+00:00", regex=True)
        normalized = normalized.str.replace(r"([+-]\d{2})(\d{2})$", r"\1:\2", regex=True)

        reparsed = pd.to_datetime(normalized, errors="coerce", utc=True)
        parsed = parsed.fillna(reparsed)

    return parsed


def _series_stats(series: pd.Series) -> dict[str, float | None]:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return {
            "mean": None,
            "median": None,
            "min": None,
            "max": None,
        }
    return {
        "mean": round(float(clean.mean()), 3),
        "median": round(float(clean.median()), 3),
        "min": round(float(clean.min()), 3),
        "max": round(float(clean.max()), 3),
    }


def _safe_col(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([pd.NA] * len(df), index=df.index)
    return df[col]


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


def _duplicate_key_stats(df: pd.DataFrame, key_columns: list[str]) -> tuple[int, int]:
    counts = df.groupby(key_columns, dropna=False).size()
    duplicate_keys = int((counts > 1).sum())
    duplicate_excess = int((counts[counts > 1] - 1).sum())
    return duplicate_keys, duplicate_excess


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
        result_series = df["result"].astype(str)
        unresolved_mask = result_series.str.startswith("UNRESOLVED_")
        unresolved = int(unresolved_mask.sum())
        metrics["unresolved_rows"] = unresolved
        metrics["unresolved_rate_pct"] = round(_pct(unresolved, len(df)), 2)
        metrics["unresolved_reason_distribution"] = _value_counts_as_dict(
            result_series[unresolved_mask]
        )

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

    if {"race", "driver", "lapNumber"}.issubset(df.columns):
        duplicate_keys, duplicate_excess = _duplicate_key_stats(
            df, ["race", "driver", "lapNumber"]
        )
        metrics["race_driver_lap_duplicate_keys"] = duplicate_keys
        metrics["race_driver_lap_duplicate_excess_rows"] = duplicate_excess
    elif {"driver", "lapNumber"}.issubset(df.columns):
        duplicate_keys, duplicate_excess = _duplicate_key_stats(df, ["driver", "lapNumber"])
        metrics["driver_lap_duplicate_keys"] = duplicate_keys
        metrics["driver_lap_duplicate_excess_rows"] = duplicate_excess

    if "trackStatus" in df.columns:
        status = df["trackStatus"].astype(str)
        metrics["track_status_distribution"] = _value_counts_as_dict(status)
        non_green = int((~status.eq(GREEN_STATUS_CODE)).sum())
        metrics["non_green_rows"] = non_green
        metrics["non_green_rate_pct"] = round(_pct(non_green, len(df)), 2)

    if "totalScore" in df.columns:
        metrics["total_score"] = {
            "mean": round(float(df["totalScore"].mean()), 3),
            "median": round(float(df["totalScore"].median()), 3),
            "p90": round(float(df["totalScore"].quantile(0.90)), 3),
            "p99": round(float(df["totalScore"].quantile(0.99)), 3),
            "min": round(float(df["totalScore"].min()), 3),
            "max": round(float(df["totalScore"].max()), 3),
        }

    return metrics


def _audit_drop_zones(df: pd.DataFrame) -> dict:
    metrics: dict[str, object] = {}

    if {"race", "driver", "lapNumber"}.issubset(df.columns):
        duplicate_keys, duplicate_excess = _duplicate_key_stats(
            df, ["race", "driver", "lapNumber"]
        )
        metrics["race_driver_lap_duplicate_keys"] = duplicate_keys
        metrics["race_driver_lap_duplicate_excess_rows"] = duplicate_excess
    elif {"driver", "lapNumber"}.issubset(df.columns):
        duplicate_keys, duplicate_excess = _duplicate_key_stats(df, ["driver", "lapNumber"])
        metrics["driver_lap_duplicate_keys"] = duplicate_keys
        metrics["driver_lap_duplicate_excess_rows"] = duplicate_excess

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
        metrics["gap_to_physical_car"] = _numeric_stats(df["gapToPhysicalCar"])
        negative_gap = int(
            (pd.to_numeric(df["gapToPhysicalCar"], errors="coerce") < 0).sum()
        )
        metrics["negative_gap_to_physical_car_rows"] = negative_gap

    if "currentPosition" in df.columns:
        metrics["current_position"] = _series_stats(df["currentPosition"])

    if "emergencePosition" in df.columns:
        metrics["emergence_position"] = _series_stats(df["emergencePosition"])

    if "positionsLost" in df.columns:
        metrics["positions_lost"] = _series_stats(df["positionsLost"])

    if {"currentPosition", "emergencePosition", "positionsLost"}.issubset(df.columns):
        implied = pd.to_numeric(df["emergencePosition"], errors="coerce") - pd.to_numeric(
            df["currentPosition"], errors="coerce"
        )
        given = pd.to_numeric(df["positionsLost"], errors="coerce")
        metrics["positions_lost_mismatch_rows"] = int((implied != given).sum())

    return metrics


def _audit_lift_coast(df: pd.DataFrame) -> dict:
    metrics: dict[str, object] = {}

    if {"race", "driver", "brakeDate"}.issubset(df.columns):
        duplicate_keys, duplicate_excess = _duplicate_key_stats(
            df, ["race", "driver", "brakeDate"]
        )
        metrics["race_driver_brake_duplicate_keys"] = duplicate_keys
        metrics["race_driver_brake_duplicate_excess_rows"] = duplicate_excess
    elif {"driver", "brakeDate"}.issubset(df.columns):
        duplicate_keys, duplicate_excess = _duplicate_key_stats(df, ["driver", "brakeDate"])
        metrics["driver_brake_duplicate_keys"] = duplicate_keys
        metrics["driver_brake_duplicate_excess_rows"] = duplicate_excess

    if "trackStatus" in df.columns:
        status = df["trackStatus"].astype(str)
        metrics["track_status_distribution"] = _value_counts_as_dict(status)
        non_green = int((~status.eq(GREEN_STATUS_CODE)).sum())
        metrics["non_green_rows"] = non_green
        metrics["non_green_rate_pct"] = round(_pct(non_green, len(df)), 2)

    if "driver" in df.columns:
        metrics["top_drivers"] = {
            str(driver): int(count)
            for driver, count in df["driver"].astype(str).value_counts().head(10).items()
        }

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

    if {"race", "driver", "lapNumber"}.issubset(df.columns):
        duplicate_keys, duplicate_excess = _duplicate_key_stats(
            df, ["race", "driver", "lapNumber"]
        )
        metrics["race_driver_lap_duplicate_keys"] = duplicate_keys
        metrics["race_driver_lap_duplicate_excess_rows"] = duplicate_excess
    elif {"driver", "lapNumber"}.issubset(df.columns):
        duplicate_keys, duplicate_excess = _duplicate_key_stats(df, ["driver", "lapNumber"])
        metrics["driver_lap_duplicate_keys"] = duplicate_keys
        metrics["driver_lap_duplicate_excess_rows"] = duplicate_excess

    if "delta" in df.columns:
        delta = pd.to_numeric(df["delta"], errors="coerce")
        metrics["negative_delta_rows"] = int((delta < 0).sum())
        metrics["delta_seconds"] = _numeric_stats(delta)

    if "compound" in df.columns:
        metrics["alerts_by_compound"] = _value_counts_as_dict(df["compound"].astype(str))

    if {"compound", "tyreLife"}.issubset(df.columns):
        tyre_stats = (
            df.groupby("compound", dropna=False)["tyreLife"]
            .agg(["count", "mean", "median", "min", "max"])
            .sort_values("count", ascending=False)
        )
        metrics["tyre_life_by_compound"] = {
            str(idx): {
                "count": int(row["count"]),
                "mean": round(float(row["mean"]), 3),
                "median": round(float(row["median"]), 3),
                "min": int(row["min"]),
                "max": int(row["max"]),
            }
            for idx, row in tyre_stats.iterrows()
        }

    if "tyreLife" in df.columns:
        early = int((pd.to_numeric(df["tyreLife"], errors="coerce").fillna(0) <= 3).sum())
        metrics["very_early_alert_rows"] = early
        metrics["very_early_alert_rate_pct"] = round(_pct(early, len(df)), 2)

    metrics["has_track_status_column"] = bool("trackStatus" in df.columns)
    return metrics


def _audit_forensics(df_pit: pd.DataFrame, df_ml: pd.DataFrame) -> dict:
    result_col = _safe_col(df_pit, "result")
    defend_mask = result_col.eq("SUCCESS_DEFEND")

    pre_pit = _safe_col(df_pit, "prePitGapAhead")
    post_pit = _safe_col(df_pit, "postPitGapToRival")
    baseline = _safe_col(df_pit, "baselineLapTime")
    resolved = _safe_col(df_pit, "resolvedVia")

    gap = pd.to_numeric(_safe_col(df_pit, "gapDeltaPct"), errors="coerce")
    extreme = df_pit[gap.abs() > EXTREME_GAP_THRESHOLD_PCT].copy()

    compound = _safe_col(df_pit, "compound")
    wet_mask = compound.isin(WET_COMPOUNDS)
    wet = df_pit[wet_mask]
    dry = df_pit[~wet_mask]

    wet_gap = pd.to_numeric(_safe_col(wet, "gapDeltaPct"), errors="coerce")
    dry_gap = pd.to_numeric(_safe_col(dry, "gapDeltaPct"), errors="coerce")

    pit_status = _safe_col(df_pit, "trackStatusAtPit")
    ml_status = _safe_col(df_ml, "trackStatus")

    top_extreme_rows = []
    if not extreme.empty:
        cols = [
            "race",
            "driver",
            "pitLapNumber",
            "rivalAhead",
            "prePitGapAhead",
            "postPitGapToRival",
            "gapDeltaPct",
            "result",
            "resolvedVia",
        ]
        cols = [c for c in cols if c in extreme.columns]
        for _, row in extreme.reindex(
            gap.abs().sort_values(ascending=False).index
        ).head(5).iterrows():
            entry = {}
            for col in cols:
                value = row[col]
                if pd.isna(value):
                    entry[col] = None
                elif isinstance(value, float):
                    entry[col] = round(float(value), 3)
                else:
                    entry[col] = value
            top_extreme_rows.append(entry)

    return {
        "success_defend_count": int(defend_mask.sum()),
        "success_defend_rate_pct": round(_pct(int(defend_mask.sum()), len(df_pit)), 2),
        "nulls_in_success_defend": {
            "prePitGapAhead": int(pre_pit[defend_mask].isna().sum()),
            "postPitGapToRival": int(post_pit[defend_mask].isna().sum()),
            "baselineLapTime": int(baseline[defend_mask].isna().sum()),
        },
        "resolved_via_all": _value_counts_as_dict(resolved.astype(str)),
        "resolved_via_success_defend": _value_counts_as_dict(
            resolved[defend_mask].astype(str)
        ),
        "extreme_gap_rows_abs_gt_20pct": int(len(extreme)),
        "extreme_gap_top5": top_extreme_rows,
        "wet_vs_dry": {
            "wet_rows": int(len(wet)),
            "dry_rows": int(len(dry)),
            "wet_gapDeltaPct_mean": round(float(wet_gap.mean()), 3)
            if wet_gap.dropna().size
            else None,
            "dry_gapDeltaPct_mean": round(float(dry_gap.mean()), 3)
            if dry_gap.dropna().size
            else None,
            "wet_gapDeltaPct_median": round(float(wet_gap.median()), 3)
            if wet_gap.dropna().size
            else None,
            "dry_gapDeltaPct_median": round(float(dry_gap.median()), 3)
            if dry_gap.dropna().size
            else None,
        },
        "status_propagation": {
            "pit_status_unique": sorted(pit_status.dropna().astype(str).unique().tolist()),
            "ml_status_unique": sorted(ml_status.dropna().astype(str).unique().tolist()),
            "pit_status_null_count": int(pit_status.isna().sum()),
            "ml_status_null_count": int(ml_status.isna().sum()),
        },
        "additional_checks": {
            "rivalAhead_null_rate_pct": round(float(_safe_col(df_pit, "rivalAhead").isna().mean() * 100.0), 2),
            "postPitGapToRival_null_rate_pct": round(float(post_pit.isna().mean() * 100.0), 2),
            "baselineLapTime_le_zero_or_null": int((pd.to_numeric(baseline, errors="coerce").fillna(0) <= 0).sum()),
            "offsetStrategy_true_rate_pct": round(float(_safe_col(df_pit, "offsetStrategy").fillna(False).mean() * 100.0), 2),
        },
    }


def _representativeness_summary(report: dict[str, Any], expected_races: int) -> dict:
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


def run_audit(
    data_lake: str = DEFAULT_DATA_LAKE,
    year: int = DEFAULT_YEAR,
    season_tag: str = DEFAULT_SEASON_TAG,
    expected_races: int = DEFAULT_EXPECTED_RACES,
) -> dict:
    report: dict[str, Any] = {
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

    pit_stream = report["streams"].get("pit_evals", {})
    ml_stream = report["streams"].get("ml_features", {})
    if pit_stream.get("status") == "ok" and ml_stream.get("status") == "ok":
        pit_df = pd.read_json(pit_stream["file"], lines=True)
        ml_df = pd.read_json(ml_stream["file"], lines=True)
        report["forensics"] = _audit_forensics(pit_df, ml_df)

    return report


def _print_report(report: dict) -> None:
    print("=== SEASON DATA AUDIT ===")
    print("inputs:")
    _print_mapping(report["inputs"], indent=2)

    for stream in STREAMS:
        stream_report = report["streams"].get(stream, {})
        print(f"\n--- {stream} ---")
        if stream_report.get("status") != "ok":
            _print_mapping(stream_report, indent=2)
            continue

        print(f"file: {stream_report['file']}")
        print("generic:")
        _print_mapping(stream_report["generic"], indent=2)
        print("metrics:")
        _print_mapping(stream_report["metrics"], indent=2)

    print("\n--- representativeness ---")
    _print_mapping(report["representativeness"], indent=2)

    if "forensics" in report:
        print("\n--- forensics ---")
        _print_mapping(report["forensics"], indent=2)


def _print_summary(report: dict) -> None:
    print("=== SEASON DATA AUDIT SUMMARY ===")

    rep = report.get("representativeness", {})
    print(
        "coverage: "
        f"{rep.get('calendar_coverage_pct', 'N/A')}% "
        f"({rep.get('ml_features_unique_races', 0)}/{rep.get('expected_races', 0)} races in ml_features, "
        f"{rep.get('pit_evals_unique_races', 0)}/{rep.get('expected_races', 0)} in pit_evals)"
    )

    for stream in STREAMS:
        stream_report = report.get("streams", {}).get(stream, {})
        if stream_report.get("status") != "ok":
            print(f"{stream}: missing")
            continue

        generic = stream_report.get("generic", {})
        print(
            f"{stream}: rows={generic.get('rows', 0)}, "
            f"dups={generic.get('exact_duplicate_rows', 0)}"
        )

    ml = report.get("streams", {}).get("ml_features", {}).get("metrics", {})
    pe = report.get("streams", {}).get("pit_evals", {}).get("metrics", {})
    ps = report.get("streams", {}).get("pit_suggestions", {}).get("metrics", {})
    dz = report.get("streams", {}).get("drop_zones", {}).get("metrics", {})
    td = report.get("streams", {}).get("tire_drops", {}).get("metrics", {})
    lc = report.get("streams", {}).get("lift_coast", {}).get("metrics", {})

    ps_dup = ps.get(
        "race_driver_lap_duplicate_excess_rows",
        ps.get("driver_lap_duplicate_excess_rows", "N/A"),
    )
    dz_dup = dz.get(
        "race_driver_lap_duplicate_excess_rows",
        dz.get("driver_lap_duplicate_excess_rows", "N/A"),
    )
    td_dup = td.get(
        "race_driver_lap_duplicate_excess_rows",
        td.get("driver_lap_duplicate_excess_rows", "N/A"),
    )

    print("--- key checks ---")
    print(
        "ml gaps: "
        f"negative={ml.get('negative_gap_rows', 'N/A')}, "
        f"null_any={ml.get('rows_with_any_null_gap', 'N/A')}"
    )
    print(
        "pit evals: "
        f"unresolved={pe.get('unresolved_rows', 'N/A')} "
        f"({pe.get('unresolved_rate_pct', 'N/A')}%), "
        f"null_post_gap={pe.get('null_post_pit_gap_rows', 'N/A')}"
    )
    print(
        "pit suggestions: "
        f"key_duplicate_excess={ps_dup}"
    )
    print(
        "drop zones: "
        f"key_duplicate_excess={dz_dup}"
    )
    print(
        "tire drops: "
        f"negative_delta={td.get('negative_delta_rows', 'N/A')}, "
        f"key_duplicate_excess={td_dup}, "
        f"has_track_status={td.get('has_track_status_column', 'N/A')}"
    )
    print(
        "lift/coast: "
        f"bad_time_order={lc.get('bad_time_order_rows', 'N/A')}, "
        f"parse_failures={lc.get('timestamp_parse_failures', 'N/A')}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Unified season audit for quality, representativeness, and forensics."
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="print only a compact summary instead of the full report",
    )
    parser.add_argument("--json-out", default=None)
    args = parser.parse_args()

    report = run_audit()
    if args.summary_only:
        _print_summary(report)
    else:
        _print_report(report)

    if args.json_out:
        output_path = Path(args.json_out)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"\njson report written to: {output_path}")


if __name__ == "__main__":
    main()