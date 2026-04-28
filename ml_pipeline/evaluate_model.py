"""Unified and verbose evaluator for the pit-strategy ML pipeline."""

from __future__ import annotations

import argparse
import importlib
import json
import re
import sys
from contextlib import contextmanager
from pathlib import Path

import pandas as pd

from pipeline_config import (
    DEFAULT_DATA_LAKE,
    DEFAULT_HORIZON,
    DEFAULT_MERGED_SEASON_TAG,
    DEFAULT_SEASON_TAG,
    DEFAULT_YEARS,
    comparator_source_year_and_tag,
    default_dataset_path,
    default_report_csv,
    default_report_md,
    default_report_txt,
    models_dir,
    normalize_years,
    reports_dir,
    run_suffix,
)
from lib.data_preparation import _latest_jsonl


@contextmanager
def _patched_argv(argv: list[str]):
    # run nested module mains with script-like argv, keeps argparse behavior consistent.
    original = sys.argv[:]
    sys.argv = argv
    try:
        yield
    finally:
        sys.argv = original


def _run_step(step_name: str, command: list[str]) -> None:
    print(f"\n=== {step_name} ===")
    print("command:")
    print(" ".join(command))

    if len(command) < 2:
        raise RuntimeError(f"invalid command for {step_name}: {command}")

    script_path = Path(command[1])
    # map file paths to importable module names, this keeps one execution path for cli and orchestrator modes.
    if script_path.parent.name == "lib":
        module_name = f"lib.{script_path.stem}"
    else:
        module_name = script_path.stem

    module = importlib.import_module(module_name)
    main_fn = getattr(module, "main", None)
    if not callable(main_fn):
        raise RuntimeError(f"module {module_name} does not expose a callable main()")

    with _patched_argv([script_path.name, *command[2:]]):
        main_fn()


def _read_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    frame = pd.read_csv(path)
    if frame.empty:
        raise ValueError(f"{label} is empty: {path}")
    return frame


def _safe_float(value: object) -> float:
    if value is None:
        return float("nan")

    if isinstance(value, (int, float)):
        number = float(value)
        if pd.isna(number):
            return float("nan")
        return number

    text = str(value).strip()
    if text == "":
        return float("nan")

    try:
        return float(text)
    except ValueError:
        return float("nan")


def _safe_status(value: object) -> str:
    text = str(value).strip().upper()
    if not text:
        return "UNKNOWN"
    return text


def _status_from_condition(ok: bool) -> str:
    return "PASS" if ok else "FAIL"


def _latest_jsonl_or_none(data_lake: Path, stream: str, year: int, season_tag: str) -> Path | None:
    try:
        return _latest_jsonl(data_lake, stream, year, season_tag)
    except FileNotFoundError:
        return None


def _race_is_year_prefixed(value: object) -> bool:
    return bool(re.match(r"^\d{4}\s::\s", str(value)))


def _existing_merged_has_prefixed_race(path: Path) -> bool:
    checked = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            if "race" in row and not _race_is_year_prefixed(row.get("race")):
                return False
            checked += 1
            if checked >= 500:
                break
    return True


def _existing_merged_stream_is_deduped(path: Path, lap_field: str) -> bool:
    seen: set[tuple[str, str, int | str]] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue

            race = str(row.get("race", ""))
            driver = str(row.get("driver", ""))
            lap_raw = row.get(lap_field)
            if not race or not driver or lap_raw is None:
                continue

            try:
                lap_key: int | str = int(float(lap_raw))
            except (TypeError, ValueError):
                lap_key = str(lap_raw)

            key = (race, driver, lap_key)
            if key in seen:
                return False
            seen.add(key)

    return True


def _existing_merged_pit_evals_is_deduped(path: Path) -> bool:
    return _existing_merged_stream_is_deduped(path, "pitLapNumber")


def _existing_merged_ml_features_is_deduped(path: Path) -> bool:
    return _existing_merged_stream_is_deduped(path, "lapNumber")


def _ensure_merged_jsonl(
    data_lake: Path,
    stream: str,
    years: list[int],
    season_tag: str,
    merged_year: int,
    merged_tag: str,
    merge_stamp: str,
) -> Path:
    source_paths = [(year, _latest_jsonl(data_lake, stream, year, season_tag)) for year in years]
    existing_merged = _latest_jsonl_or_none(data_lake, stream, merged_year, merged_tag)

    newest_source_mtime = max(path.stat().st_mtime for _, path in source_paths)
    existing_valid = False
    # reuse merged files when source streams did not change, avoids hidden drift across repeated evaluations.
    if existing_merged is not None and existing_merged.stat().st_mtime >= newest_source_mtime:
        existing_valid = _existing_merged_has_prefixed_race(existing_merged)
        if existing_valid and stream == "pit_evals":
            existing_valid = _existing_merged_pit_evals_is_deduped(existing_merged)
        if existing_valid and stream == "ml_features":
            existing_valid = _existing_merged_ml_features_is_deduped(existing_merged)

    if existing_valid:
        return existing_merged

    output_path = data_lake / f"{stream}_{merged_year}_{merged_tag}_{merge_stamp}.jsonl"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if stream in {"pit_evals", "ml_features"}:
        dedup_field = "pitLapNumber" if stream == "pit_evals" else "lapNumber"
        deduped_rows: dict[tuple[str, str, int | str], dict[str, object]] = {}
        passthrough_rows: list[dict[str, object]] = []

        for year, src in source_paths:
            with src.open("r", encoding="utf-8") as handle:
                for line in handle:
                    text = line.strip()
                    if not text:
                        continue
                    row = json.loads(text)
                    if not isinstance(row, dict):
                        continue

                    if "race" in row and not _race_is_year_prefixed(row.get("race")):
                        row["race"] = f"{year} :: {row['race']}"

                    race = str(row.get("race", ""))
                    driver = str(row.get("driver", ""))
                    lap_raw = row.get(dedup_field)
                    try:
                        lap_key: int | str = int(float(lap_raw))
                    except (TypeError, ValueError):
                        lap_key = str(lap_raw)

                    if race and driver and lap_raw is not None:
                        # keep the latest row per pit key, enforces deterministic one to one comparator inputs.
                        deduped_rows[(race, driver, lap_key)] = row
                    else:
                        passthrough_rows.append(row)

        ordered_keys = sorted(deduped_rows.keys(), key=lambda key: (key[0], key[1], str(key[2])))
        with output_path.open("w", encoding="utf-8") as dst:
            for key in ordered_keys:
                dst.write(json.dumps(deduped_rows[key]) + "\n")
            for row in passthrough_rows:
                dst.write(json.dumps(row) + "\n")
    else:
        with output_path.open("w", encoding="utf-8") as dst:
            for year, src in source_paths:
                with src.open("r", encoding="utf-8") as handle:
                    for line in handle:
                        text = line.strip()
                        if not text:
                            continue
                        row = json.loads(text)
                        if isinstance(row, dict) and "race" in row and not _race_is_year_prefixed(row.get("race")):
                            row["race"] = f"{year} :: {row['race']}"
                        dst.write(json.dumps(row) + "\n")

    print(f"merged {stream} jsonl: {output_path}")
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "run a full, thesis-aligned evaluation campaign and emit both "
            "machine-readable and narrative reports"
        )
    )

    parser.add_argument("--data-lake", default=DEFAULT_DATA_LAKE)
    parser.add_argument("--years", type=int, nargs="+", default=list(DEFAULT_YEARS))
    parser.add_argument("--season-tag", default=DEFAULT_SEASON_TAG)
    parser.add_argument("--horizon", type=int, default=DEFAULT_HORIZON)

    parser.add_argument("--dataset", default="", help="training dataset used for parity/split audits")
    parser.add_argument("--oof-input", default="", help="winner OOF decisions csv")
    parser.add_argument("--ablation-input", default="", help="ablation leaderboard csv")
    parser.add_argument("--bundle", default="", help="serving bundle joblib path")
    parser.add_argument("--ml-features", default="", help="ml_features jsonl path for parity/latency")
    parser.add_argument("--drop-zones", default="", help="drop_zones jsonl path for parity/latency")

    parser.add_argument("--reference-threshold", type=float, default=0.10)
    parser.add_argument("--precision-floor", type=float, default=0.7149187592319055)
    parser.add_argument("--latency-budget-ms", type=float, default=500.0)
    parser.add_argument("--availability-floor", type=float, default=99.0)

    parser.add_argument("--evaluation-summary-output", default="", help="unified evaluation summary csv")
    parser.add_argument("--evaluation-markdown-output", default="", help="unified narrative markdown report")

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    years = normalize_years(args.years)
    data_lake = Path(args.data_lake)
    reports = reports_dir(data_lake)

    suffix = run_suffix(years, args.season_tag)

    dataset_path = Path(args.dataset) if args.dataset else default_dataset_path(data_lake, years, args.season_tag)
    oof_input = Path(args.oof_input) if args.oof_input else default_report_csv(data_lake, "ml_oof_winner", years, args.season_tag)
    ablation_input = (
        Path(args.ablation_input)
        if args.ablation_input
        else default_report_csv(data_lake, "ml_ablation_phase31c", years, args.season_tag)
    )
    bundle_path = Path(args.bundle) if args.bundle else models_dir(data_lake) / "pit_strategy_serving_bundle.joblib"

    comparator_year, comparator_tag = comparator_source_year_and_tag(years, args.season_tag)

    ensured_merged: dict[str, Path] = {}
    # multi-season evaluations must operate on aligned merged streams, avoids cross-season key collisions.
    if len(years) > 1:
        merge_stamp = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
        for stream in ("pit_suggestions", "pit_evals", "ml_features", "drop_zones"):
            # for merged-tag runs, prefer existing merged streams and only rebuild from per-year season streams if missing.
            if args.season_tag == DEFAULT_MERGED_SEASON_TAG:
                existing_merged = _latest_jsonl_or_none(data_lake, stream, comparator_year, comparator_tag)
                if existing_merged is not None:
                    ensured_merged[stream] = existing_merged
                    continue
                source_tag = DEFAULT_SEASON_TAG
            else:
                source_tag = args.season_tag

            ensured_merged[stream] = _ensure_merged_jsonl(
                data_lake=data_lake,
                stream=stream,
                years=years,
                season_tag=source_tag,
                merged_year=comparator_year,
                merged_tag=comparator_tag,
                merge_stamp=merge_stamp,
            )

    ml_features_path = (
        Path(args.ml_features)
        if args.ml_features
        else ensured_merged.get("ml_features", _latest_jsonl(data_lake, "ml_features", comparator_year, comparator_tag))
    )
    drop_zones_path = (
        Path(args.drop_zones)
        if args.drop_zones
        else ensured_merged.get("drop_zones", _latest_jsonl(data_lake, "drop_zones", comparator_year, comparator_tag))
    )

    heuristic_comparator = reports / f"heuristic_comparator_{suffix}.csv"
    ml_comparator = reports / f"ml_comparator_{suffix}.csv"

    phase_b_summary = reports / f"significance_summary_{suffix}.csv"
    phase_b_tests = reports / f"significance_tests_{suffix}.csv"
    phase_b_report = reports / f"significance_report_{suffix}.txt"
    phase_b_meeting_report = reports / f"sde_ml_comparison_{suffix}.md"
    phase_b_meeting_summary = reports / f"sde_ml_comparison_summary_{suffix}.csv"
    phase_b_meeting_by_year = reports / f"sde_ml_comparison_by_year_{suffix}.csv"

    phase_c_sweep = reports / f"threshold_frontier_{suffix}.csv"
    phase_c_by_year = reports / f"threshold_frontier_by_year_{suffix}.csv"
    phase_c_best_comparator = reports / f"ml_comparator_best_threshold_{suffix}.csv"
    phase_c_report = reports / f"threshold_frontier_report_{suffix}.txt"

    phase_d_summary = reports / f"calibration_policy_summary_{suffix}.csv"
    phase_d_reliability = reports / f"calibration_reliability_bins_{suffix}.csv"
    phase_d_by_year = reports / f"calibration_policy_by_year_{suffix}.csv"
    phase_d_by_fold = reports / f"calibration_policy_by_fold_{suffix}.csv"
    phase_d_reference = reports / f"calibration_policy_reference_thresholds_{suffix}.csv"
    phase_d_report = reports / f"calibration_policy_report_{suffix}.txt"

    phase_f_summary = reports / f"feature_parity_summary_{suffix}.csv"
    phase_f_details = reports / f"feature_parity_details_{suffix}.csv"
    phase_f_report = reports / f"feature_parity_report_{suffix}.txt"

    phase_g_summary = reports / f"live_latency_summary_{suffix}.csv"
    phase_g_details = reports / f"live_latency_details_{suffix}.csv"
    phase_g_by_year = reports / f"live_latency_by_year_{suffix}.csv"
    phase_g_availability = reports / f"live_availability_summary_{suffix}.csv"
    phase_g_overhead = reports / f"live_overhead_comparison_{suffix}.csv"
    phase_g_report = reports / f"live_latency_report_{suffix}.txt"

    phase_h_unified = reports / f"integrated_gate_{suffix}.csv"
    phase_h_by_layer = reports / f"integrated_gate_by_layer_{suffix}.csv"
    phase_h_report = reports / f"integrated_gate_report_{suffix}.txt"

    phase_j_split_summary = reports / f"split_integrity_summary_{suffix}.csv"
    phase_j_split_details = reports / f"split_integrity_details_{suffix}.csv"
    phase_j_split_report = reports / f"split_integrity_report_{suffix}.txt"

    phase_j_comp_summary = reports / f"comparator_invariance_summary_{suffix}.csv"
    phase_j_comp_details = reports / f"comparator_invariance_details_{suffix}.csv"
    phase_j_comp_report = reports / f"comparator_invariance_report_{suffix}.txt"

    evaluation_summary_output = (
        Path(args.evaluation_summary_output)
        if args.evaluation_summary_output
        else default_report_csv(data_lake, "model_evaluation", years, args.season_tag)
    )
    evaluation_markdown_output = (
        Path(args.evaluation_markdown_output)
        if args.evaluation_markdown_output
        else default_report_md(data_lake, "model_evaluation", years, args.season_tag)
    )

    script_dir = Path(__file__).resolve().parent / "lib"

    def pipeline_script(name: str) -> str:
        return str(script_dir / name)

    _run_step(
        "Build heuristic comparator",
        [
            sys.executable,
            pipeline_script("comparator_heuristic.py"),
            "--data-lake",
            str(data_lake),
            "--year",
            str(comparator_year),
            "--season-tag",
            comparator_tag,
            "--horizon",
            str(args.horizon),
            "--output",
            str(heuristic_comparator.resolve()),
        ],
    )

    _run_step(
        "Build ML comparator",
        [
            sys.executable,
            pipeline_script("comparator_ml.py"),
            "--decisions",
            str(oof_input),
            "--data-lake",
            str(data_lake),
            "--year",
            str(comparator_year),
            "--season-tag",
            comparator_tag,
            "--horizon",
            str(args.horizon),
            "--output",
            str(ml_comparator.resolve()),
        ],
    )

    _run_step(
        "Significance evaluation",
        [
            sys.executable,
            pipeline_script("evaluate_significance.py"),
            "--sde-comparator",
            str(heuristic_comparator),
            "--ml-comparator",
            str(ml_comparator),
            "--summary-output",
            str(phase_b_summary),
            "--tests-output",
            str(phase_b_tests),
            "--report-output",
            str(phase_b_report),
        ],
    )

    _run_step(
        "Dedicated SDE vs ML report",
        [
            sys.executable,
            pipeline_script("report_sde_ml_comparison.py"),
            "--significance-summary",
            str(phase_b_summary),
            "--significance-tests",
            str(phase_b_tests),
            "--heuristic-comparator",
            str(heuristic_comparator),
            "--ml-comparator",
            str(ml_comparator),
            "--output-md",
            str(phase_b_meeting_report),
            "--output-summary-csv",
            str(phase_b_meeting_summary),
            "--output-by-year-csv",
            str(phase_b_meeting_by_year),
        ],
    )

    _run_step(
        "Threshold frontier sweep",
        [
            sys.executable,
            pipeline_script("evaluate_threshold_frontier.py"),
            "--decisions",
            str(oof_input),
            "--data-lake",
            str(data_lake),
            "--year",
            str(comparator_year),
            "--season-tag",
            comparator_tag,
            "--horizon",
            str(args.horizon),
            "--precision-floor",
            str(args.precision_floor),
            "--reference-threshold",
            str(args.reference_threshold),
            "--output",
            str(phase_c_sweep),
            "--by-year-output",
            str(phase_c_by_year),
            "--best-comparator-output",
            str(phase_c_best_comparator),
            "--report-output",
            str(phase_c_report),
        ],
    )

    _run_step(
        "Calibration and policy evaluation",
        [
            sys.executable,
            pipeline_script("evaluate_calibration_policy.py"),
            "--oof",
            str(oof_input),
            "--ablation",
            str(ablation_input),
            "--summary-output",
            str(phase_d_summary),
            "--reliability-output",
            str(phase_d_reliability),
            "--by-year-output",
            str(phase_d_by_year),
            "--by-fold-output",
            str(phase_d_by_fold),
            "--reference-output",
            str(phase_d_reference),
            "--report-output",
            str(phase_d_report),
        ],
    )

    _run_step(
        "Feature parity audit",
        [
            sys.executable,
            pipeline_script("evaluate_feature_parity.py"),
            "--dataset",
            str(dataset_path),
            "--bundle",
            str(bundle_path),
            "--ml-features",
            str(ml_features_path),
            "--drop-zones",
            str(drop_zones_path),
            "--summary-output",
            str(phase_f_summary),
            "--details-output",
            str(phase_f_details),
            "--report-output",
            str(phase_f_report),
        ],
    )

    _run_step(
        "Latency and availability audit",
        [
            sys.executable,
            pipeline_script("evaluate_live_latency.py"),
            "--ml-features",
            str(ml_features_path),
            "--drop-zones",
            str(drop_zones_path),
            "--bundle",
            str(bundle_path),
            "--latency-budget-ms",
            str(args.latency_budget_ms),
            "--availability-floor",
            str(args.availability_floor),
            "--summary-output",
            str(phase_g_summary),
            "--details-output",
            str(phase_g_details),
            "--by-year-output",
            str(phase_g_by_year),
            "--availability-output",
            str(phase_g_availability),
            "--overhead-output",
            str(phase_g_overhead),
            "--report-output",
            str(phase_g_report),
        ],
    )

    _run_step(
        "Integrated gate",
        [
            sys.executable,
            pipeline_script("evaluate_integrated_gate.py"),
            "--significance-summary",
            str(phase_b_summary),
            "--significance-tests",
            str(phase_b_tests),
            "--threshold-frontier",
            str(phase_c_sweep),
            "--calibration-summary",
            str(phase_d_summary),
            "--feature-parity-summary",
            str(phase_f_summary),
            "--latency-summary",
            str(phase_g_summary),
            "--availability-summary",
            str(phase_g_availability),
            "--reference-threshold",
            str(args.reference_threshold),
            "--unified-output",
            str(phase_h_unified),
            "--by-layer-output",
            str(phase_h_by_layer),
            "--report-output",
            str(phase_h_report),
        ],
    )

    _run_step(
        "Split-integrity audit",
        [
            sys.executable,
            pipeline_script("audit_split_integrity.py"),
            "--dataset",
            str(dataset_path),
            "--oof",
            str(oof_input),
            "--summary-output",
            str(phase_j_split_summary),
            "--details-output",
            str(phase_j_split_details),
            "--report-output",
            str(phase_j_split_report),
        ],
    )

    _run_step(
        "Comparator-invariance audit",
        [
            sys.executable,
            pipeline_script("audit_comparator_invariance.py"),
            "--heuristic",
            str(heuristic_comparator),
            "--ml",
            str(ml_comparator),
            "--horizon",
            str(args.horizon),
            "--summary-output",
            str(phase_j_comp_summary),
            "--details-output",
            str(phase_j_comp_details),
            "--report-output",
            str(phase_j_comp_report),
        ],
    )

    b_summary = _read_csv(phase_b_summary, "phase b summary")
    b_tests = _read_csv(phase_b_tests, "phase b tests")
    c_sweep = _read_csv(phase_c_sweep, "phase c sweep")
    d_summary = _read_csv(phase_d_summary, "phase d summary")
    f_summary = _read_csv(phase_f_summary, "phase f summary")
    g_summary = _read_csv(phase_g_summary, "phase g summary")
    h_unified = _read_csv(phase_h_unified, "phase h unified gate")
    j_split = _read_csv(phase_j_split_summary, "phase j split integrity")
    j_comp = _read_csv(phase_j_comp_summary, "phase j comparator invariance")

    sde_row = b_summary[b_summary["model"] == "SDE"].iloc[0]
    ml_delta_row = b_summary[b_summary["model"] == "ML_minus_SDE"].iloc[0]
    z_test_row = b_tests[b_tests["test"] == "two_proportion_z"].iloc[0]

    threshold_series = pd.to_numeric(c_sweep["threshold"], errors="coerce")
    if threshold_series.isna().all():
        raise ValueError("phase c threshold column has no numeric values")
    c_ref_idx = int((threshold_series - float(args.reference_threshold)).abs().idxmin())
    c_ref = c_sweep.iloc[c_ref_idx]
    sde_precision = _safe_float(sde_row["precision"])
    ref_precision = _safe_float(c_ref["precision"])
    no_match_rate = _safe_float(c_ref["no_match_rate"])

    d_row = d_summary.iloc[0]

    f_gate = f_summary[f_summary["check"] == "feature_parity_overall_gate"].iloc[0]
    g_latency = g_summary[g_summary["check"] == "latency_p95_total_ms"].iloc[0]
    g_availability = g_summary[g_summary["check"] == "availability_pct"].iloc[0]

    h_decision = h_unified[h_unified["phase"] == "PHASE_H_DECISION"].iloc[0]

    j_split_gate = j_split[j_split["check"] == "split_integrity_overall"].iloc[0]
    j_comp_gate = j_comp[j_comp["check"] == "comparator_invariance_overall"].iloc[0]

    summary_rows = [
        {
            "test_id": "B1",
            "test_name": "ML precision delta vs SDE",
            "why_this_test": "Checks if ML improves precision under the same comparator semantics.",
            "status": _status_from_condition(_safe_float(ml_delta_row["precision"]) > 0.0),
            "metric": "precision_delta",
            "value": _safe_float(ml_delta_row["precision"]),
            "threshold": 0.0,
            "artifact": str(phase_b_summary),
        },
        {
            "test_id": "B2",
            "test_name": "Two-proportion z significance",
            "why_this_test": "Tests whether observed precision difference is statistically significant.",
            "status": _status_from_condition(_safe_float(z_test_row["p_value"]) <= 0.05),
            "metric": "p_value",
            "value": _safe_float(z_test_row["p_value"]),
            "threshold": 0.05,
            "artifact": str(phase_b_tests),
        },
        {
            "test_id": "C1",
            "test_name": "Reference precision floor",
            "why_this_test": "Verifies threshold policy remains at or above the SDE precision floor.",
            "status": _status_from_condition(ref_precision >= sde_precision),
            "metric": "reference_precision",
            "value": ref_precision,
            "threshold": sde_precision,
            "artifact": str(phase_c_sweep),
        },
        {
            "test_id": "C2",
            "test_name": "Lookahead no-match dominance",
            "why_this_test": "Checks that most exclusions are horizon-related, supporting comparator interpretation.",
            "status": _status_from_condition(no_match_rate >= 0.90),
            "metric": "no_match_rate",
            "value": no_match_rate,
            "threshold": 0.90,
            "artifact": str(phase_c_sweep),
        },
        {
            "test_id": "D1",
            "test_name": "Constrained precision",
            "why_this_test": "Ensures calibrated constrained policy is precise enough for strategy actions.",
            "status": _status_from_condition(_safe_float(d_row["constrained_precision"]) >= 0.60),
            "metric": "constrained_precision",
            "value": _safe_float(d_row["constrained_precision"]),
            "threshold": 0.60,
            "artifact": str(phase_d_summary),
        },
        {
            "test_id": "D2",
            "test_name": "Precision-floor reachability",
            "why_this_test": "Measures whether candidate thresholds can reliably satisfy precision constraints.",
            "status": _status_from_condition(_safe_float(d_row["winner_mean_precision_floor_reachable_ratio"]) >= 0.90),
            "metric": "reachability_ratio",
            "value": _safe_float(d_row["winner_mean_precision_floor_reachable_ratio"]),
            "threshold": 0.90,
            "artifact": str(phase_d_summary),
        },
        {
            "test_id": "D3",
            "test_name": "Fallback rate",
            "why_this_test": "Checks constrained policy stability when precision floor is hard to satisfy.",
            "status": _status_from_condition(_safe_float(d_row["winner_fallback_rate"]) <= 0.10),
            "metric": "fallback_rate",
            "value": _safe_float(d_row["winner_fallback_rate"]),
            "threshold": 0.10,
            "artifact": str(phase_d_summary),
        },
        {
            "test_id": "F1",
            "test_name": "Training-serving parity gate",
            "why_this_test": "Guards against feature/schema skew between offline training and live serving.",
            "status": _safe_status(f_gate["status"]),
            "metric": "feature_parity_overall_gate",
            "value": _safe_float(f_gate["value"]),
            "threshold": 1.0,
            "artifact": str(phase_f_summary),
        },
        {
            "test_id": "G1",
            "test_name": "Latency gate",
            "why_this_test": "Checks p95 end-to-end inference latency against operational budget.",
            "status": _safe_status(g_latency["status"]),
            "metric": "latency_p95_total_ms",
            "value": _safe_float(g_latency["value"]),
            "threshold": _safe_float(g_latency["threshold"]),
            "artifact": str(phase_g_summary),
        },
        {
            "test_id": "G2",
            "test_name": "Availability gate",
            "why_this_test": "Ensures prediction path remains available across replayed events.",
            "status": _safe_status(g_availability["status"]),
            "metric": "availability_pct",
            "value": _safe_float(g_availability["value"]),
            "threshold": _safe_float(g_availability["threshold"]),
            "artifact": str(phase_g_summary),
        },
        {
            "test_id": "H1",
            "test_name": "Integrated deployment decision",
            "why_this_test": "Combines B/C/D/F/G evidence into one actionable readiness decision.",
            "status": _safe_status(h_decision["status"]),
            "metric": "integrated_gate_decision",
            "value": float("nan"),
            "threshold": float("nan"),
            "artifact": str(phase_h_unified),
        },
        {
            "test_id": "J1",
            "test_name": "Split-integrity gate",
            "why_this_test": "Verifies grouped race CV and OOF coverage assumptions are preserved.",
            "status": _safe_status(j_split_gate["status"]),
            "metric": "split_integrity_overall",
            "value": _safe_float(j_split_gate["value"]),
            "threshold": _safe_float(j_split_gate["threshold"]),
            "artifact": str(phase_j_split_summary),
        },
        {
            "test_id": "J2",
            "test_name": "Comparator-invariance gate",
            "why_this_test": "Ensures fairness contract (actionable-only, fixed horizon, one-to-one matching) stays frozen.",
            "status": _safe_status(j_comp_gate["status"]),
            "metric": "comparator_invariance_overall",
            "value": _safe_float(j_comp_gate["value"]),
            "threshold": _safe_float(j_comp_gate["threshold"]),
            "artifact": str(phase_j_comp_summary),
        },
    ]

    summary_df = pd.DataFrame(summary_rows)
    evaluation_summary_output.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(evaluation_summary_output, index=False)

    decision_status = _safe_status(h_decision["status"])
    decision_note = str(h_decision.get("note", "")).strip()

    markdown_lines = [
        f"# Unified Model Evaluation Report ({suffix})",
        "",
        "## Scope",
        f"- Years: {years}",
        f"- Horizon: H={args.horizon}",
        f"- Comparator source token: year={comparator_year}, season_tag={comparator_tag}",
        "",
        "## Why These Tests",
        "- Leakage-safe grouped validation by race (Roberts et al., 2017).",
        "- Horizon-based comparator matching and temporal validity checks (Brookshire, 2024).",
        "- Imbalance-aware precision-focused policy evaluation (Elkan, 2001; Saito and Rehmsmeier, 2015).",
        "- Calibration reliability and operational readiness gates before deployment claims.",
        "",
        "## Results",
        "| Test ID | Test | Why | Status | Metric | Value | Threshold | Artifact |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]

    for row in summary_rows:
        value = row["value"]
        threshold = row["threshold"]
        value_text = "N/A" if pd.isna(value) else f"{float(value):.6f}"
        threshold_text = "N/A" if pd.isna(threshold) else f"{float(threshold):.6f}"
        markdown_lines.append(
            "| {test_id} | {test_name} | {why_this_test} | {status} | {metric} | {value_text} | {threshold_text} | {artifact} |".format(
                test_id=row["test_id"],
                test_name=row["test_name"],
                why_this_test=row["why_this_test"],
                status=row["status"],
                metric=row["metric"],
                value_text=value_text,
                threshold_text=threshold_text,
                artifact=row["artifact"],
            )
        )

    markdown_lines.extend(
        [
            "",
            "## Integrated Decision",
            f"- Decision: {decision_status}",
            f"- Note: {decision_note}",
            "",
            "## Core Artifacts",
            f"- Unified summary CSV: {evaluation_summary_output}",
            f"- Integrated gate report: {phase_h_report}",
            f"- Dedicated SDE vs ML report: {phase_b_meeting_report}",
            f"- Dedicated SDE vs ML summary: {phase_b_meeting_summary}",
            f"- Dedicated SDE vs ML by year: {phase_b_meeting_by_year}",
            f"- Comparator files: {heuristic_comparator}, {ml_comparator}",
            f"- Threshold sweep report: {phase_c_report}",
            f"- Calibration report: {phase_d_report}",
            f"- Parity report: {phase_f_report}",
            f"- Latency report: {phase_g_report}",
            f"- Split-integrity report: {phase_j_split_report}",
            f"- Comparator-invariance report: {phase_j_comp_report}",
        ]
    )

    evaluation_markdown_output.parent.mkdir(parents=True, exist_ok=True)
    evaluation_markdown_output.write_text("\n".join(markdown_lines) + "\n", encoding="utf-8")

    print("\n=== EVALUATE MODEL SUMMARY ===")
    print(f"decision            : {decision_status}")
    print(f"decision note       : {decision_note}")
    print(f"summary csv         : {evaluation_summary_output}")
    print(f"markdown report     : {evaluation_markdown_output}")
    print(f"integrated gate rpt : {phase_h_report}")
    print(f"sde ml report md    : {phase_b_meeting_report}")
    print(f"sde ml summary csv  : {phase_b_meeting_summary}")
    print(f"sde ml by year csv  : {phase_b_meeting_by_year}")


if __name__ == "__main__":
    main()
