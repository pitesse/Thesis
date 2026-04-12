"""run Phase G latency and operational feasibility audit on merged artifacts.

this script replays merged ml feature rows through the serving transform and
model inference path, then reports:
- end-to-end latency percentiles (p50, p95, p99),
- prediction availability and failure breakdown,
- heuristic-proxy vs ml-path overhead decomposition,
- gate status under fixed latency and availability thresholds.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import time
from typing import Any

import joblib
import numpy as np
import pandas as pd

from .live_kafka_inference import OnlineFeatureEngineer, _build_drop_zone_lookup, _build_model_matrix

DEFAULT_ML_FEATURES = "data_lake/ml_features_9999_merged_20260404_195653.jsonl"
DEFAULT_DROP_ZONES = "data_lake/drop_zones_9999_merged_20260404_195653.jsonl"
DEFAULT_BUNDLE = "data_lake/models/pit_strategy_serving_bundle.joblib"

DEFAULT_SUMMARY_OUTPUT = "data_lake/reports/phase_g_latency_summary_2022_2024_merged.csv"
DEFAULT_DETAILS_OUTPUT = "data_lake/reports/phase_g_latency_details_2022_2024_merged.csv"
DEFAULT_BY_YEAR_OUTPUT = "data_lake/reports/phase_g_latency_by_year_2022_2024_merged.csv"
DEFAULT_AVAILABILITY_OUTPUT = "data_lake/reports/phase_g_availability_summary_2022_2024_merged.csv"
DEFAULT_OVERHEAD_OUTPUT = "data_lake/reports/phase_g_overhead_comparison_2022_2024_merged.csv"
DEFAULT_REPORT_OUTPUT = "data_lake/reports/phase_g_latency_report_2022_2024_merged.txt"

DEFAULT_SAMPLE_SIZE = 3000
DEFAULT_RANDOM_SEED = 42
DEFAULT_WARMUP_EVENTS = 50
DEFAULT_LATENCY_BUDGET_MS = 500.0
DEFAULT_AVAILABILITY_FLOOR = 99.0

REQUIRED_FIELDS = {
    "race",
    "driver",
    "lapNumber",
    "position",
    "compound",
    "tyreLife",
    "trackTemp",
    "airTemp",
    "humidity",
    "rainfall",
    "speedTrap",
    "team",
    "gapAhead",
    "gapBehind",
    "lapTime",
    "pitLoss",
    "trackStatus",
}


def _status(ok: bool) -> str:
    return "PASS" if ok else "FAIL"


def _safe_ratio(num: float, den: float) -> float:
    if den <= 0:
        return float("nan")
    return float(num / den)


def _percentile(values: np.ndarray, pct: float) -> float:
    if values.size == 0:
        return float("nan")
    return float(np.percentile(values, pct))


def _year_from_race(race: object) -> str:
    token = str(race).split(" :: ", maxsplit=1)[0]
    if token.isdigit():
        return token
    return "UNKNOWN"


def _load_lines(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"ml_features file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        lines = [line for line in handle if line.strip()]

    if not lines:
        raise ValueError(f"ml_features file is empty: {path}")
    return lines


def _sample_lines(lines: list[str], sample_size: int, random_seed: int) -> list[str]:
    if sample_size <= 0:
        raise ValueError("--sample-size must be >= 1")

    if sample_size >= len(lines):
        return lines

    rng = np.random.default_rng(random_seed)
    indices = rng.choice(len(lines), size=sample_size, replace=False)
    indices.sort()
    return [lines[int(idx)] for idx in indices]


def _component_summary(name: str, series: pd.Series) -> dict[str, object]:
    arr = series.to_numpy(dtype=float)
    return {
        "component": name,
        "samples": int(arr.size),
        "mean_ms": float(np.mean(arr)) if arr.size > 0 else float("nan"),
        "p50_ms": _percentile(arr, 50),
        "p95_ms": _percentile(arr, 95),
        "p99_ms": _percentile(arr, 99),
        "max_ms": float(np.max(arr)) if arr.size > 0 else float("nan"),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="run Phase G live latency audit")
    parser.add_argument("--ml-features", default=DEFAULT_ML_FEATURES, help="merged ml_features jsonl")
    parser.add_argument("--drop-zones", default=DEFAULT_DROP_ZONES, help="merged drop_zones jsonl")
    parser.add_argument("--bundle", default=DEFAULT_BUNDLE, help="serving bundle joblib")
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE, help="number of replay events")
    parser.add_argument("--random-seed", type=int, default=DEFAULT_RANDOM_SEED, help="sampling seed")
    parser.add_argument("--warmup-events", type=int, default=DEFAULT_WARMUP_EVENTS, help="events excluded from percentile gates")
    parser.add_argument("--latency-budget-ms", type=float, default=DEFAULT_LATENCY_BUDGET_MS, help="p95 latency budget")
    parser.add_argument("--availability-floor", type=float, default=DEFAULT_AVAILABILITY_FLOOR, help="minimum availability percent")
    parser.add_argument("--summary-output", default=DEFAULT_SUMMARY_OUTPUT, help="latency gate summary output")
    parser.add_argument("--details-output", default=DEFAULT_DETAILS_OUTPUT, help="latency component details output")
    parser.add_argument("--by-year-output", default=DEFAULT_BY_YEAR_OUTPUT, help="latency by-year output")
    parser.add_argument("--availability-output", default=DEFAULT_AVAILABILITY_OUTPUT, help="availability output")
    parser.add_argument("--overhead-output", default=DEFAULT_OVERHEAD_OUTPUT, help="overhead output")
    parser.add_argument("--report-output", default=DEFAULT_REPORT_OUTPUT, help="text report output")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.warmup_events < 0:
        raise ValueError("--warmup-events must be >= 0")
    if args.latency_budget_ms <= 0:
        raise ValueError("--latency-budget-ms must be > 0")
    if not (0 < args.availability_floor <= 100):
        raise ValueError("--availability-floor must satisfy 0 < value <= 100")

    ml_features_path = Path(args.ml_features)
    drop_zones_path = Path(args.drop_zones)
    bundle_path = Path(args.bundle)

    if not bundle_path.exists():
        raise FileNotFoundError(f"serving bundle not found: {bundle_path}")

    lines = _load_lines(ml_features_path)
    replay_lines = _sample_lines(lines, args.sample_size, args.random_seed)

    drop_zone_lookup: dict[tuple[str, str, int], tuple[int, float]] | None = None
    if args.drop_zones:
        if not drop_zones_path.exists():
            raise FileNotFoundError(f"drop_zones file not found: {drop_zones_path}")
        drop_zone_lookup = _build_drop_zone_lookup(drop_zones_path)

    bundle = joblib.load(bundle_path)
    model = bundle["model"]
    calibrator = bundle.get("calibrator")
    feature_columns = list(bundle["feature_columns"])
    threshold = float(bundle.get("threshold", 0.50))

    engineer = OnlineFeatureEngineer(drop_zone_lookup=drop_zone_lookup)

    timing_rows: list[dict[str, object]] = []
    failures: dict[str, int] = {}

    for idx, line in enumerate(replay_lines):
        year = "UNKNOWN"
        race = ""
        driver = ""
        lap_number = -1

        t_total_start = time.perf_counter()
        try:
            t_parse_start = time.perf_counter()
            event: dict[str, Any] = json.loads(line)
            parse_ms = (time.perf_counter() - t_parse_start) * 1000.0

            race = str(event.get("race", ""))
            driver = str(event.get("driver", ""))
            lap_number = int(event.get("lapNumber", -1)) if event.get("lapNumber") is not None else -1
            year = _year_from_race(race)

            missing_required = sorted(REQUIRED_FIELDS - set(event.keys()))
            if missing_required:
                raise ValueError(f"missing_required_fields:{';'.join(missing_required)}")

            t_feature_start = time.perf_counter()
            feature_row = engineer.transform(event)
            feature_ms = (time.perf_counter() - t_feature_start) * 1000.0

            t_matrix_start = time.perf_counter()
            matrix = _build_model_matrix(feature_row, feature_columns)
            matrix_ms = (time.perf_counter() - t_matrix_start) * 1000.0

            t_infer_start = time.perf_counter()
            proba = float(model.predict_proba(matrix)[:, 1][0])
            inference_ms = (time.perf_counter() - t_infer_start) * 1000.0

            t_cal_start = time.perf_counter()
            if calibrator is not None:
                proba = float(calibrator.predict(np.array([proba]))[0])
            calibration_ms = (time.perf_counter() - t_cal_start) * 1000.0

            t_pack_start = time.perf_counter()
            pred = int(proba >= threshold)
            payload = {
                "type": "ML_PIT_PREDICTION",
                "race": race,
                "driver": driver,
                "lapNumber": event.get("lapNumber"),
                "prediction": pred,
                "predictionProba": round(proba, 6),
                "threshold": threshold,
            }
            _ = json.dumps(payload)
            packaging_ms = (time.perf_counter() - t_pack_start) * 1000.0

            total_ms = (time.perf_counter() - t_total_start) * 1000.0
            heuristic_proxy_ms = parse_ms + feature_ms + packaging_ms
            ml_overhead_ms = matrix_ms + inference_ms + calibration_ms

            timing_rows.append(
                {
                    "event_index": idx,
                    "year": year,
                    "race": race,
                    "driver": driver,
                    "lapNumber": lap_number,
                    "status": "ok",
                    "parse_ms": parse_ms,
                    "feature_ms": feature_ms,
                    "matrix_ms": matrix_ms,
                    "inference_ms": inference_ms,
                    "calibration_ms": calibration_ms,
                    "packaging_ms": packaging_ms,
                    "heuristic_proxy_ms": heuristic_proxy_ms,
                    "ml_overhead_ms": ml_overhead_ms,
                    "total_ms": total_ms,
                    "error_type": "",
                }
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            total_ms = (time.perf_counter() - t_total_start) * 1000.0
            error_type = type(exc).__name__
            failures[error_type] = failures.get(error_type, 0) + 1
            timing_rows.append(
                {
                    "event_index": idx,
                    "year": year,
                    "race": race,
                    "driver": driver,
                    "lapNumber": lap_number,
                    "status": "error",
                    "parse_ms": float("nan"),
                    "feature_ms": float("nan"),
                    "matrix_ms": float("nan"),
                    "inference_ms": float("nan"),
                    "calibration_ms": float("nan"),
                    "packaging_ms": float("nan"),
                    "heuristic_proxy_ms": float("nan"),
                    "ml_overhead_ms": float("nan"),
                    "total_ms": total_ms,
                    "error_type": error_type,
                }
            )

    frame = pd.DataFrame(timing_rows)
    ok = frame[frame["status"] == "ok"].copy()

    if ok.empty:
        raise RuntimeError("no successful replay events, cannot evaluate latency gate")

    # exclude warmup rows from gate percentiles to avoid cold-start inflation.
    gate_ok = ok[ok["event_index"] >= args.warmup_events].copy()
    if gate_ok.empty:
        gate_ok = ok.copy()

    latency_p50 = _percentile(gate_ok["total_ms"].to_numpy(dtype=float), 50)
    latency_p95 = _percentile(gate_ok["total_ms"].to_numpy(dtype=float), 95)
    latency_p99 = _percentile(gate_ok["total_ms"].to_numpy(dtype=float), 99)

    total_events = int(len(frame))
    ok_events = int(len(ok))
    failed_events = int(total_events - ok_events)
    availability_pct = float(_safe_ratio(ok_events * 100.0, total_events))

    latency_gate_pass = bool(latency_p95 < args.latency_budget_ms)
    availability_gate_pass = bool(availability_pct >= args.availability_floor)
    overall_pass = latency_gate_pass and availability_gate_pass

    summary_rows = [
        {
            "check": "latency_p95_total_ms",
            "status": _status(latency_gate_pass),
            "value": latency_p95,
            "threshold": args.latency_budget_ms,
            "note": f"warmup_excluded={args.warmup_events}, p50={latency_p50:.6f}, p99={latency_p99:.6f}",
        },
        {
            "check": "availability_pct",
            "status": _status(availability_gate_pass),
            "value": availability_pct,
            "threshold": args.availability_floor,
            "note": f"ok={ok_events}, failed={failed_events}, total={total_events}",
        },
        {
            "check": "phase_g_overall_gate",
            "status": _status(overall_pass),
            "value": int(overall_pass),
            "threshold": 1,
            "note": "requires latency and availability gate pass",
        },
    ]
    summary_df = pd.DataFrame(summary_rows)

    details_components = [
        "parse_ms",
        "feature_ms",
        "matrix_ms",
        "inference_ms",
        "calibration_ms",
        "packaging_ms",
        "heuristic_proxy_ms",
        "ml_overhead_ms",
        "total_ms",
    ]
    details_df = pd.DataFrame(
        [_component_summary(component, gate_ok[component]) for component in details_components]
    )

    by_year_rows: list[dict[str, object]] = []
    for year, group_all in frame.groupby("year"):
        group_ok = group_all[group_all["status"] == "ok"].copy()
        if group_ok.empty:
            by_year_rows.append(
                {
                    "year": year,
                    "total_events": int(len(group_all)),
                    "ok_events": 0,
                    "failed_events": int(len(group_all)),
                    "availability_pct": 0.0,
                    "p50_total_ms": float("nan"),
                    "p95_total_ms": float("nan"),
                    "p99_total_ms": float("nan"),
                }
            )
            continue

        group_gate = group_ok[group_ok["event_index"] >= args.warmup_events].copy()
        if group_gate.empty:
            group_gate = group_ok

        arr = group_gate["total_ms"].to_numpy(dtype=float)
        by_year_rows.append(
            {
                "year": year,
                "total_events": int(len(group_all)),
                "ok_events": int(len(group_ok)),
                "failed_events": int(len(group_all) - len(group_ok)),
                "availability_pct": float(_safe_ratio(len(group_ok) * 100.0, len(group_all))),
                "p50_total_ms": _percentile(arr, 50),
                "p95_total_ms": _percentile(arr, 95),
                "p99_total_ms": _percentile(arr, 99),
            }
        )
    by_year_df = pd.DataFrame(by_year_rows).sort_values(by="year")

    availability_rows = [
        {
            "metric": "total_events",
            "value": total_events,
            "threshold": float("nan"),
            "status": "INFO",
            "note": "sampled replay events",
        },
        {
            "metric": "successful_events",
            "value": ok_events,
            "threshold": float("nan"),
            "status": "INFO",
            "note": "events with full inference output",
        },
        {
            "metric": "failed_events",
            "value": failed_events,
            "threshold": float("nan"),
            "status": "INFO",
            "note": ";".join([f"{key}:{value}" for key, value in sorted(failures.items())]),
        },
        {
            "metric": "availability_pct",
            "value": availability_pct,
            "threshold": args.availability_floor,
            "status": _status(availability_gate_pass),
            "note": "availability gate metric",
        },
        {
            "metric": "fallback_to_heuristic_events",
            "value": failed_events,
            "threshold": float("nan"),
            "status": "INFO",
            "note": "proxy fallback count if failed ml predictions route to heuristic path",
        },
    ]
    availability_df = pd.DataFrame(availability_rows)

    overhead_rows: list[dict[str, object]] = []
    for label, pct in [("p50", 50), ("p95", 95), ("p99", 99)]:
        heuristic_value = _percentile(gate_ok["heuristic_proxy_ms"].to_numpy(dtype=float), pct)
        ml_total_value = _percentile(gate_ok["total_ms"].to_numpy(dtype=float), pct)
        ml_overhead_value = _percentile(gate_ok["ml_overhead_ms"].to_numpy(dtype=float), pct)
        overhead_rows.append(
            {
                "statistic": label,
                "heuristic_proxy_ms": heuristic_value,
                "ml_total_ms": ml_total_value,
                "ml_added_overhead_ms": ml_overhead_value,
                "ml_overhead_share": _safe_ratio(ml_overhead_value, ml_total_value),
            }
        )

    overhead_rows.append(
        {
            "statistic": "mean",
            "heuristic_proxy_ms": float(gate_ok["heuristic_proxy_ms"].mean()),
            "ml_total_ms": float(gate_ok["total_ms"].mean()),
            "ml_added_overhead_ms": float(gate_ok["ml_overhead_ms"].mean()),
            "ml_overhead_share": float(_safe_ratio(gate_ok["ml_overhead_ms"].mean(), gate_ok["total_ms"].mean())),
        }
    )
    overhead_df = pd.DataFrame(overhead_rows)

    summary_output = Path(args.summary_output)
    details_output = Path(args.details_output)
    by_year_output = Path(args.by_year_output)
    availability_output = Path(args.availability_output)
    overhead_output = Path(args.overhead_output)
    report_output = Path(args.report_output)

    for output in [summary_output, details_output, by_year_output, availability_output, overhead_output, report_output]:
        output.parent.mkdir(parents=True, exist_ok=True)

    summary_df.to_csv(summary_output, index=False)
    details_df.to_csv(details_output, index=False)
    by_year_df.to_csv(by_year_output, index=False)
    availability_df.to_csv(availability_output, index=False)
    overhead_df.to_csv(overhead_output, index=False)

    report_lines = [
        "=== PHASE G LIVE LATENCY REPORT ===",
        f"ml_features input      : {ml_features_path}",
        f"drop_zones input       : {drop_zones_path if args.drop_zones else 'disabled'}",
        f"bundle input           : {bundle_path}",
        f"sample events          : {total_events}",
        f"successful events      : {ok_events}",
        f"failed events          : {failed_events}",
        f"warmup excluded        : {args.warmup_events}",
        "",
        "latency gate",
        f"p50 total latency ms   : {latency_p50:.6f}",
        f"p95 total latency ms   : {latency_p95:.6f}",
        f"p99 total latency ms   : {latency_p99:.6f}",
        f"latency budget ms      : {args.latency_budget_ms:.6f}",
        f"latency gate status    : {_status(latency_gate_pass)}",
        "",
        "availability gate",
        f"availability pct       : {availability_pct:.6f}",
        f"availability floor pct : {args.availability_floor:.6f}",
        f"availability status    : {_status(availability_gate_pass)}",
        "",
        f"overall Phase G gate   : {_status(overall_pass)}",
        "",
        "artifacts",
        f"summary csv            : {summary_output}",
        f"details csv            : {details_output}",
        f"by-year csv            : {by_year_output}",
        f"availability csv       : {availability_output}",
        f"overhead csv           : {overhead_output}",
        f"report txt             : {report_output}",
    ]

    report_output.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    print("\n".join(report_lines))


if __name__ == "__main__":
    main()