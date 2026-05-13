"""Run SML/MOA Phase 2B dual-contract matrix with canonical SDE truth headline.

This orchestrator is resume-safe and does not retrain batch models.
It reuses existing prepared datasets, exports MOA datasets per target/profile,
runs MOA, evaluates dual-contract metrics (canonical + optional native),
builds compact matrices, threshold frontiers, and compact E0-vs-P1 tables.
"""

from __future__ import annotations

import argparse
import os
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from ..pipeline_config import normalize_years
except ImportError:
    import sys as _sys

    _LIB_DIR = Path(__file__).resolve().parent
    _PIPELINE_DIR = _LIB_DIR.parent
    for _path in (_PIPELINE_DIR, _LIB_DIR):
        _text = str(_path)
        if _text not in _sys.path:
            _sys.path.insert(0, _text)
    from pipeline_config import normalize_years  # type: ignore


DEFAULT_TARGETS: tuple[str, ...] = (
    "target_pit_any_h2_raw",
    "target_pit_any_h2_clean_actionable",
    "target_pit_any_h2_clean_dry_strategy",
    "target_pit_success_h2_raw",
    "target_pit_success_h2_clean_actionable",
    "target_pit_success_h2_clean_dry_strategy",
)


@dataclass(frozen=True)
class ProfileConfig:
    name: str
    feature_profile: str
    track_agnostic_mode: str


PROFILE_CONFIGS: dict[str, ProfileConfig] = {
    "e0_no_source_year": ProfileConfig(
        name="e0_no_source_year",
        feature_profile="baseline",
        track_agnostic_mode="off",
    ),
    "p1_percent_conservative_v1": ProfileConfig(
        name="p1_percent_conservative_v1",
        feature_profile="percent_conservative_v1",
        track_agnostic_mode="track_percentage_v1",
    ),
}


def _resolve_jobs(spec: str) -> int:
    text = str(spec).strip().lower()
    if text == "auto":
        cpu = os.cpu_count() or 2
        return max(1, min(8, cpu - 2))
    value = int(text)
    if value < 1:
        raise ValueError("--jobs must be >= 1 or 'auto'")
    return value


def _worker_env() -> dict[str, str]:
    env = os.environ.copy()
    env["OMP_NUM_THREADS"] = "1"
    env["OPENBLAS_NUM_THREADS"] = "1"
    env["MKL_NUM_THREADS"] = "1"
    env["NUMEXPR_NUM_THREADS"] = "1"
    return env


def _run(cmd: list[str], *, dry_run: bool, log_path: Path | None = None) -> None:
    print("[RUN]", " ".join(cmd))
    if dry_run:
        return
    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write("$ " + " ".join(cmd) + "\n")
            handle.flush()
            subprocess.run(
                cmd,
                check=True,
                stdout=handle,
                stderr=handle,
                env=_worker_env(),
            )
            handle.write("\n")
    else:
        subprocess.run(cmd, check=True, env=_worker_env())


def _run_parallel(tasks: list[tuple[str, list[str], Path]], *, jobs: int, dry_run: bool) -> None:
    if not tasks:
        return
    if jobs <= 1 or dry_run:
        for _, cmd, log_path in tasks:
            _run(cmd, dry_run=dry_run, log_path=log_path)
        return

    with ThreadPoolExecutor(max_workers=jobs) as executor:
        future_map = {
            executor.submit(_run, cmd, dry_run=False, log_path=log_path): name
            for name, cmd, log_path in tasks
        }
        for future in as_completed(future_map):
            name = future_map[future]
            try:
                future.result()
            except Exception as exc:
                raise RuntimeError(f"parallel task failed for {name}: {exc}") from exc


def _default_output_dir(data_lake: Path, years: list[int]) -> Path:
    return data_lake / "reports" / f"sml_phase2b_dual_contract_{years[0]}_{years[-1]}"


def _default_truth_universe_csv(data_lake: Path, years: list[int]) -> Path:
    return (
        data_lake
        / "reports"
        / f"ml_phase2b_dual_contract_{years[0]}_{years[-1]}"
        / "audits"
        / f"sde_universe_{years[0]}_{years[-1]}.csv"
    )


def _default_truth_events_csvs(data_lake: Path, years: list[int]) -> list[Path]:
    return [
        data_lake / "reports" / f"pit_truth_eligibility_audit_{year}_c6_cfg120_fixed.csv"
        for year in years
    ]


def _default_dataset_for_profile(
    data_lake: Path,
    years: list[int],
    profile_name: str,
) -> Path:
    suffix = f"{years[0]}_{years[-1]}"
    if profile_name == "e0_no_source_year":
        return data_lake / f"ml_training_dataset_{suffix}_dual_contract.parquet"
    if profile_name == "p1_percent_conservative_v1":
        return data_lake / f"ml_training_dataset_{suffix}_dual_contract_p1_percent.parquet"
    raise ValueError(f"unsupported profile for default dataset resolution: {profile_name}")


def _parse_targets(raw_targets: list[str]) -> list[str]:
    if not raw_targets:
        return list(DEFAULT_TARGETS)
    tokens = [str(t).strip() for t in raw_targets if str(t).strip()]
    if not tokens:
        return list(DEFAULT_TARGETS)
    if len(tokens) == 1 and tokens[0].lower() == "all":
        return list(DEFAULT_TARGETS)
    invalid = sorted(set(tokens).difference(set(DEFAULT_TARGETS)))
    if invalid:
        raise ValueError(
            f"unsupported targets: {invalid}; supported={list(DEFAULT_TARGETS)}"
        )
    return tokens


def _parse_profiles(raw_profiles: list[str]) -> list[str]:
    if not raw_profiles:
        return ["e0_no_source_year", "p1_percent_conservative_v1"]
    profiles = [str(p).strip() for p in raw_profiles if str(p).strip()]
    invalid = sorted(set(profiles).difference(set(PROFILE_CONFIGS.keys())))
    if invalid:
        raise ValueError(
            f"unsupported profiles: {invalid}; supported={list(PROFILE_CONFIGS.keys())}"
        )
    return profiles


def _run_id(profile: str, target: str) -> str:
    return f"{profile}__{target}"


def _resolve_stage_flags(args: argparse.Namespace) -> dict[str, bool]:
    explicit = any(
        [
            args.run_export,
            args.run_moa,
            args.run_eval,
            args.run_matrix,
            args.run_frontier,
            args.run_prequential,
        ]
    )
    if explicit:
        return {
            "export": bool(args.run_export),
            "moa": bool(args.run_moa),
            "eval": bool(args.run_eval),
            "matrix": bool(args.run_matrix),
            "frontier": bool(args.run_frontier),
            "prequential": bool(args.run_prequential),
        }
    return {
        "export": True,
        "moa": True,
        "eval": True,
        "matrix": True,
        "frontier": True,
        "prequential": True,
    }


def _skip_if_resume(paths: list[Path], *, resume: bool) -> bool:
    return bool(resume and paths and all(path.exists() for path in paths))


def _ensure_parent(paths: list[Path]) -> None:
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)


def _build_prequential_summary(
    *,
    run_ids: list[str],
    summary_dir: Path,
    output_csv: Path,
) -> None:
    rows: list[dict[str, object]] = []
    for run_id in run_ids:
        path = summary_dir / f"{run_id}.csv"
        if not path.exists():
            continue
        frame = pd.read_csv(path)
        if frame.empty:
            continue
        row = frame.iloc[0]
        profile, target = run_id.split("__", 1)
        rows.append(
            {
                "run_id": run_id,
                "profile": profile,
                "target_column": target,
                "status": row.get("status"),
                "exit_code": row.get("exit_code"),
                "duration_sec": row.get("duration_sec"),
                "eval_instances": row.get("final_learning evaluation instances"),
                "accuracy_pct": row.get("final_classifications correct (percent)"),
                "kappa_pct": row.get("final_Kappa Statistic (percent)"),
                "kappa_temporal_pct": row.get("final_Kappa Temporal Statistic (percent)"),
                "kappa_m_pct": row.get("final_Kappa M Statistic (percent)"),
                "predictions_output": row.get("predictions_output"),
                "learning_curve_output": row.get("learning_curve_output"),
            }
        )

    out = pd.DataFrame(rows)
    if not out.empty:
        out.sort_values(
            by=["profile", "target_column"],
            kind="mergesort",
            inplace=True,
        )
        out.reset_index(drop=True, inplace=True)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_csv, index=False)


def _build_profile_comparison(
    *,
    recommended_csv: Path,
    output_csv: Path,
) -> None:
    if not recommended_csv.exists():
        raise FileNotFoundError(f"recommended csv not found: {recommended_csv}")
    frame = pd.read_csv(recommended_csv)
    if frame.empty:
        raise ValueError(f"recommended csv is empty: {recommended_csv}")

    required = {"profile", "target_column", "outcome_mode", "truth_lens"}
    missing = sorted(required.difference(set(frame.columns)))
    if missing:
        raise ValueError(f"recommended csv missing required columns {missing}: {recommended_csv}")

    rows: list[dict[str, object]] = []
    keys = ["target_column", "outcome_mode", "truth_lens"]
    metrics = [
        "selected_threshold",
        "AP",
        "row_tp",
        "tp_for_recall",
        "fp",
        "fn",
        "scored",
        "precision",
        "recall",
        "f1",
        "f0_5",
        "eligible_actual_pit_count",
    ]
    for key, group in frame.groupby(keys, sort=True):
        e0 = group[group["profile"] == "e0_no_source_year"]
        p1 = group[group["profile"] == "p1_percent_conservative_v1"]
        if e0.empty or p1.empty:
            continue
        e0_row = e0.iloc[0]
        p1_row = p1.iloc[0]
        out: dict[str, object] = {
            "target_column": key[0],
            "outcome_mode": key[1],
            "truth_lens": key[2],
        }
        for metric in metrics:
            e0_value = e0_row.get(metric, pd.NA)
            p1_value = p1_row.get(metric, pd.NA)
            out[f"e0_{metric}"] = e0_value
            out[f"p1_{metric}"] = p1_value
            if metric == "eligible_actual_pit_count":
                try:
                    out[f"delta_{metric}_p1_minus_e0"] = int(p1_value) - int(e0_value)  # type: ignore[arg-type]
                except Exception:
                    out[f"delta_{metric}_p1_minus_e0"] = pd.NA
            else:
                try:
                    out[f"delta_{metric}_p1_minus_e0"] = float(p1_value) - float(e0_value)  # type: ignore[arg-type]
                except Exception:
                    out[f"delta_{metric}_p1_minus_e0"] = pd.NA
        rows.append(out)

    output = pd.DataFrame(rows)
    if not output.empty:
        output.sort_values(by=["outcome_mode", "truth_lens", "target_column"], inplace=True, kind="mergesort")
        output.reset_index(drop=True, inplace=True)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(output_csv, index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run SML/MOA Phase 2B dual-contract flow (export/run/eval/matrix/frontier) "
            "with canonical SDE truth as headline and optional native sensitivity."
        )
    )
    parser.add_argument("--data-lake", default="data_lake")
    parser.add_argument("--years", type=int, nargs="+", required=True)
    parser.add_argument("--season-tag", default="season")

    parser.add_argument("--output-dir", default="")
    parser.add_argument("--profiles", nargs="*", default=["e0_no_source_year", "p1_percent_conservative_v1"])
    parser.add_argument("--targets", nargs="*", default=["all"])

    parser.add_argument("--dataset-e0", default="")
    parser.add_argument("--dataset-p1", default="")
    parser.add_argument("--prepared-pit-events-csv", default="")

    parser.add_argument("--truth-universe-race-driver-csv", default="")
    parser.add_argument("--truth-universe-events-csvs", nargs="*", default=[])

    parser.add_argument("--pit-evals-jsonl", default="")
    parser.add_argument("--pit-timings-jsonl", default="")
    parser.add_argument("--ml-features-jsonl", default="")

    parser.add_argument("--moa-jar", default="")
    parser.add_argument("--java-bin", default="java")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--sample-frequency", type=int, default=1000)
    parser.add_argument("--instance-limit", type=int, default=-1)
    parser.add_argument("--class-index", type=int, default=-1)

    parser.add_argument("--run-export", action="store_true")
    parser.add_argument("--run-moa", action="store_true")
    parser.add_argument("--run-eval", action="store_true")
    parser.add_argument("--run-matrix", action="store_true")
    parser.add_argument("--run-frontier", action="store_true")
    parser.add_argument("--run-prequential", action="store_true")
    parser.add_argument(
        "--run-native-sensitivity",
        action="store_true",
        help="also compute native-universe sensitivity outputs (non-headline)",
    )
    parser.add_argument(
        "--jobs",
        default="auto",
        help="parallel workers for independent profile/target tasks (int or 'auto')",
    )
    parser.add_argument("--resume", action="store_true", help="skip steps when expected outputs already exist")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    years = normalize_years(args.years)
    data_lake = Path(args.data_lake)
    output_dir = Path(args.output_dir) if args.output_dir.strip() else _default_output_dir(data_lake, years)
    profiles = _parse_profiles(args.profiles)
    targets = _parse_targets(args.targets)
    stages = _resolve_stage_flags(args)
    run_native = bool(args.run_native_sensitivity)
    jobs = _resolve_jobs(args.jobs)

    dataset_by_profile = {
        "e0_no_source_year": Path(args.dataset_e0) if args.dataset_e0.strip() else _default_dataset_for_profile(data_lake, years, "e0_no_source_year"),
        "p1_percent_conservative_v1": Path(args.dataset_p1) if args.dataset_p1.strip() else _default_dataset_for_profile(data_lake, years, "p1_percent_conservative_v1"),
    }

    prepared_pit_events_csv = (
        Path(args.prepared_pit_events_csv)
        if args.prepared_pit_events_csv.strip()
        else data_lake / "reports" / f"fastf1_prepared_pit_stats_{years[0]}_{years[-1]}" / "fastf1_prepared_pit_events.csv"
    )
    truth_universe_csv = (
        Path(args.truth_universe_race_driver_csv)
        if args.truth_universe_race_driver_csv.strip()
        else _default_truth_universe_csv(data_lake, years)
    )
    truth_event_csvs = (
        [Path(text) for text in args.truth_universe_events_csvs]
        if args.truth_universe_events_csvs
        else _default_truth_events_csvs(data_lake, years)
    )

    py = sys.executable
    run_ids = [_run_id(profile, target) for profile in profiles for target in targets]

    # Stage 1: export + MOA run + dual-contract eval (parallel per run_id where safe)
    run_contexts: list[dict[str, Any]] = []
    for profile in profiles:
        config = PROFILE_CONFIGS[profile]
        dataset = dataset_by_profile[profile]
        if stages["export"] or stages["moa"] or stages["eval"]:
            if not args.dry_run and not dataset.exists():
                raise FileNotFoundError(f"dataset for profile {profile} not found: {dataset}")
        for target in targets:
            run_id = _run_id(profile, target)
            run_contexts.append(
                {
                    "run_id": run_id,
                    "profile": profile,
                    "target": target,
                    "config": config,
                    "dataset": dataset,
                    "export_csv": output_dir / "exports" / f"{run_id}.csv",
                    "export_arff": output_dir / "exports" / f"{run_id}.arff",
                    "export_schema": output_dir / "exports" / f"{run_id}.json",
                    "pred_path": output_dir / "moa" / "predictions" / f"{run_id}.pred",
                    "hard_pred_path": output_dir / "moa" / "predictions_hard" / f"{run_id}.pred",
                    "lc_path": output_dir / "moa" / "learning_curve" / f"{run_id}.csv",
                    "summary_path": output_dir / "moa" / "summary" / f"{run_id}.csv",
                    "stdout_path": output_dir / "moa" / "stdout" / f"{run_id}.txt",
                    "stderr_path": output_dir / "moa" / "stderr" / f"{run_id}.txt",
                    "metadata_path": output_dir / "moa" / "metadata" / f"{run_id}.json",
                    "vote_summary_path": output_dir / "moa" / "vote_summary" / f"{run_id}.csv",
                    "vote_stdout_path": output_dir / "moa" / "vote_stdout" / f"{run_id}.txt",
                    "vote_stderr_path": output_dir / "moa" / "vote_stderr" / f"{run_id}.txt",
                    "vote_metadata_path": output_dir / "moa" / "vote_metadata" / f"{run_id}.json",
                    "eval_oof": output_dir / "oof" / f"{run_id}.csv",
                    "eval_summary": output_dir / "eval" / f"{run_id}.csv",
                    "eval_by_year": output_dir / "by_year" / f"{run_id}.csv",
                    "eval_oof_native": output_dir / "native" / "oof" / f"{run_id}.csv",
                    "eval_summary_native": output_dir / "native" / "eval" / f"{run_id}.csv",
                    "eval_by_year_native": output_dir / "native" / "by_year" / f"{run_id}.csv",
                }
            )

    logs_root = output_dir / "logs"
    logs_root.mkdir(parents=True, exist_ok=True)

    if stages["export"]:
        export_tasks: list[tuple[str, list[str], Path]] = []
        for rc in run_contexts:
            _ensure_parent([rc["export_csv"], rc["export_arff"], rc["export_schema"]])
            if _skip_if_resume([rc["export_csv"], rc["export_arff"], rc["export_schema"]], resume=args.resume):
                print(f"[SKIP] export exists for {rc['run_id']}")
                continue
            cmd = [
                py,
                "ml_pipeline/export_moa_dataset.py",
                "--data-lake",
                str(data_lake),
                "--years",
                *[str(year) for year in years],
                "--season-tag",
                args.season_tag,
                "--skip-prepare-data",
                "--dataset",
                str(rc["dataset"]),
                "--target-column",
                rc["target"],
                "--feature-profile",
                rc["config"].feature_profile,
                "--track-agnostic-mode",
                rc["config"].track_agnostic_mode,
                "--drop-source-year-feature",
                "--output-csv",
                str(rc["export_csv"]),
                "--output-arff",
                str(rc["export_arff"]),
                "--schema-output",
                str(rc["export_schema"]),
            ]
            export_tasks.append((rc["run_id"], cmd, logs_root / "export" / f"{rc['run_id']}.log"))
        _run_parallel(export_tasks, jobs=jobs, dry_run=args.dry_run)

    if stages["moa"]:
        # 1) classic EvaluatePrequential run (learning-curve + hard predictions)
        moa_arf_tasks: list[tuple[str, list[str], Path]] = []
        for rc in run_contexts:
            _ensure_parent(
                [
                    rc["pred_path"],
                    rc["hard_pred_path"],
                    rc["lc_path"],
                    rc["summary_path"],
                    rc["stdout_path"],
                    rc["stderr_path"],
                    rc["metadata_path"],
                    rc["vote_summary_path"],
                    rc["vote_stdout_path"],
                    rc["vote_stderr_path"],
                    rc["vote_metadata_path"],
                ]
            )
            if _skip_if_resume(
                [rc["hard_pred_path"], rc["lc_path"], rc["summary_path"], rc["metadata_path"]],
                resume=args.resume,
            ):
                print(f"[SKIP] moa-arf exists for {rc['run_id']}")
                continue

            cmd = [
                py,
                "ml_pipeline/run_moa_arf.py",
                "--data-lake",
                str(data_lake),
                "--years",
                *[str(year) for year in years],
                "--season-tag",
                args.season_tag,
                "--input-arff",
                str(rc["export_arff"]),
                "--java-bin",
                args.java_bin,
                "--seed",
                str(args.seed),
                "--sample-frequency",
                str(args.sample_frequency),
                "--instance-limit",
                str(args.instance_limit),
                "--class-index",
                str(args.class_index),
                "--learning-curve-output",
                str(rc["lc_path"]),
                "--summary-output",
                str(rc["summary_path"]),
                "--stdout-output",
                str(rc["stdout_path"]),
                "--stderr-output",
                str(rc["stderr_path"]),
                "--predictions-output",
                str(rc["hard_pred_path"]),
                "--metadata-output",
                str(rc["metadata_path"]),
            ]
            if args.moa_jar.strip():
                cmd.extend(["--moa-jar", args.moa_jar])
            moa_arf_tasks.append((rc["run_id"], cmd, logs_root / "moa_arf" / f"{rc['run_id']}.log"))
        _run_parallel(moa_arf_tasks, jobs=jobs, dry_run=args.dry_run)

        # 2) vote logger run (continuous scores)
        moa_vote_tasks: list[tuple[str, list[str], Path]] = []
        for rc in run_contexts:
            if _skip_if_resume(
                [rc["pred_path"], rc["vote_summary_path"], rc["vote_metadata_path"]],
                resume=args.resume,
            ):
                print(f"[SKIP] moa-vote exists for {rc['run_id']}")
                continue

            expected_rows = -1
            if not args.dry_run:
                export_schema = Path(rc["export_schema"])
                export_csv = Path(rc["export_csv"])
                if export_schema.exists():
                    try:
                        schema_payload = json.loads(export_schema.read_text(encoding="utf-8"))
                        expected_rows = int(schema_payload.get("row_count", -1))
                    except Exception:
                        expected_rows = -1
                if expected_rows < 0 and export_csv.exists():
                    try:
                        expected_rows = int(pd.read_csv(export_csv, usecols=["target_y"]).shape[0])
                    except Exception:
                        expected_rows = -1

            learner_cli = f"meta.OzaBoostAdwin -s {int(args.seed)}"
            cmd_vote = [
                py,
                "ml_pipeline/run_moa_vote_logger.py",
                "--data-lake",
                str(data_lake),
                "--years",
                *[str(year) for year in years],
                "--season-tag",
                args.season_tag,
                "--input-arff",
                str(rc["export_arff"]),
                "--java-bin",
                args.java_bin,
                "--class-index",
                str(args.class_index),
                "--instance-limit",
                str(args.instance_limit),
                "--learner-cli",
                learner_cli,
                "--predictions-output",
                str(rc["pred_path"]),
                "--summary-output",
                str(rc["vote_summary_path"]),
                "--stdout-output",
                str(rc["vote_stdout_path"]),
                "--stderr-output",
                str(rc["vote_stderr_path"]),
                "--metadata-output",
                str(rc["vote_metadata_path"]),
                "--validate-target-csv",
                str(rc["export_csv"]),
                "--expected-rows",
                str(expected_rows),
                "--compiled-classes-dir",
                str(output_dir / "moa" / "classes" / rc["run_id"]),
            ]
            if args.moa_jar.strip():
                cmd_vote.extend(["--moa-jar", args.moa_jar])
            moa_vote_tasks.append((rc["run_id"], cmd_vote, logs_root / "moa_vote" / f"{rc['run_id']}.log"))
        _run_parallel(moa_vote_tasks, jobs=jobs, dry_run=args.dry_run)

    if stages["eval"]:
        eval_tasks: list[tuple[str, list[str], Path]] = []
        for rc in run_contexts:
            _ensure_parent([rc["eval_oof"], rc["eval_summary"], rc["eval_by_year"]])
            if _skip_if_resume([rc["eval_oof"], rc["eval_summary"], rc["eval_by_year"]], resume=args.resume):
                print(f"[SKIP] canonical eval exists for {rc['run_id']}")
                continue
            cmd = [
                py,
                "-m",
                "ml_pipeline.evaluate_moa_dual_contract_run",
                "--data-lake",
                str(data_lake),
                "--years",
                *[str(year) for year in years],
                "--season-tag",
                args.season_tag,
                "--dataset",
                str(rc["dataset"]),
                "--target-column",
                rc["target"],
                "--profile",
                rc["profile"],
                "--moa-predictions",
                str(rc["pred_path"]),
                "--prepared-pit-events-csv",
                str(prepared_pit_events_csv),
                "--truth-universe-race-driver-csv",
                str(truth_universe_csv),
                "--truth-universe-events-csvs",
                *[str(path) for path in truth_event_csvs],
                "--output-oof-csv",
                str(rc["eval_oof"]),
                "--output-summary-csv",
                str(rc["eval_summary"]),
                "--output-by-year-csv",
                str(rc["eval_by_year"]),
            ]
            if args.pit_evals_jsonl.strip():
                cmd.extend(["--pit-evals-jsonl", args.pit_evals_jsonl])
            if args.pit_timings_jsonl.strip():
                cmd.extend(["--pit-timings-jsonl", args.pit_timings_jsonl])
            if args.ml_features_jsonl.strip():
                cmd.extend(["--ml-features-jsonl", args.ml_features_jsonl])
            eval_tasks.append((rc["run_id"], cmd, logs_root / "eval_canonical" / f"{rc['run_id']}.log"))
        _run_parallel(eval_tasks, jobs=jobs, dry_run=args.dry_run)

        if run_native:
            eval_native_tasks: list[tuple[str, list[str], Path]] = []
            for rc in run_contexts:
                _ensure_parent([rc["eval_oof_native"], rc["eval_summary_native"], rc["eval_by_year_native"]])
                if _skip_if_resume(
                    [rc["eval_oof_native"], rc["eval_summary_native"], rc["eval_by_year_native"]],
                    resume=args.resume,
                ):
                    print(f"[SKIP] native eval exists for {rc['run_id']}")
                    continue
                cmd = [
                    py,
                    "-m",
                    "ml_pipeline.evaluate_moa_dual_contract_run",
                    "--data-lake",
                    str(data_lake),
                    "--years",
                    *[str(year) for year in years],
                    "--season-tag",
                    args.season_tag,
                    "--dataset",
                    str(rc["dataset"]),
                    "--target-column",
                    rc["target"],
                    "--profile",
                    rc["profile"],
                    "--moa-predictions",
                    str(rc["pred_path"]),
                    "--prepared-pit-events-csv",
                    str(prepared_pit_events_csv),
                    "--output-oof-csv",
                    str(rc["eval_oof_native"]),
                    "--output-summary-csv",
                    str(rc["eval_summary_native"]),
                    "--output-by-year-csv",
                    str(rc["eval_by_year_native"]),
                ]
                if args.pit_evals_jsonl.strip():
                    cmd.extend(["--pit-evals-jsonl", args.pit_evals_jsonl])
                if args.pit_timings_jsonl.strip():
                    cmd.extend(["--pit-timings-jsonl", args.pit_timings_jsonl])
                if args.ml_features_jsonl.strip():
                    cmd.extend(["--ml-features-jsonl", args.ml_features_jsonl])
                eval_native_tasks.append((rc["run_id"], cmd, logs_root / "eval_native" / f"{rc['run_id']}.log"))
            _run_parallel(eval_native_tasks, jobs=jobs, dry_run=args.dry_run)

    # Stage 2: matrix builders (summary + by-year aggregation)
    if stages["matrix"]:
        canonical_summary_args: list[str] = []
        canonical_year_args: list[str] = []
        for run_id in run_ids:
            canonical_summary_args.extend(
                ["--run-summary", f"{run_id}={output_dir / 'eval' / f'{run_id}.csv'}"]
            )
            canonical_year_args.extend(
                ["--run-by-year", f"{run_id}={output_dir / 'by_year' / f'{run_id}.csv'}"]
            )
        matrix_compact = output_dir / "matrix_sde_truth" / "sml_phase2b_matrix_compact.csv"
        matrix_by_year = output_dir / "matrix_sde_truth" / "sml_phase2b_matrix_by_year.csv"
        _ensure_parent([matrix_compact, matrix_by_year])
        if _skip_if_resume([matrix_compact, matrix_by_year], resume=args.resume):
            print("[SKIP] canonical matrix exists")
        else:
            cmd = [
                py,
                "ml_pipeline/build_batch_phase2a_matrix.py",
                *canonical_summary_args,
                *canonical_year_args,
                "--output-matrix-csv",
                str(matrix_compact),
                "--output-by-year-csv",
                str(matrix_by_year),
            ]
            _run(cmd, dry_run=args.dry_run)

        if run_native:
            native_summary_args: list[str] = []
            native_year_args: list[str] = []
            for run_id in run_ids:
                native_summary_args.extend(
                    ["--run-summary", f"{run_id}={output_dir / 'native' / 'eval' / f'{run_id}.csv'}"]
                )
                native_year_args.extend(
                    ["--run-by-year", f"{run_id}={output_dir / 'native' / 'by_year' / f'{run_id}.csv'}"]
                )
            matrix_compact_native = output_dir / "matrix_native" / "sml_phase2b_matrix_compact_native.csv"
            matrix_by_year_native = output_dir / "matrix_native" / "sml_phase2b_matrix_by_year_native.csv"
            _ensure_parent([matrix_compact_native, matrix_by_year_native])
            if _skip_if_resume([matrix_compact_native, matrix_by_year_native], resume=args.resume):
                print("[SKIP] native matrix exists")
            else:
                cmd = [
                    py,
                    "ml_pipeline/build_batch_phase2a_matrix.py",
                    *native_summary_args,
                    *native_year_args,
                    "--output-matrix-csv",
                    str(matrix_compact_native),
                    "--output-by-year-csv",
                    str(matrix_by_year_native),
                ]
                _run(cmd, dry_run=args.dry_run)

    # Stage 3: threshold frontier sweep (canonical headline + optional native sensitivity)
    if stages["frontier"]:
        frontier_compact = output_dir / "frontier" / "phase2b_threshold_frontier_compact.csv"
        frontier_by_year = output_dir / "by_year" / "phase2b_threshold_frontier_by_year.csv"
        frontier_reco = output_dir / "recommended" / "phase2b_recommended_operating_points.csv"
        frontier_md = output_dir / "phase2b_threshold_frontier_report.md"
        _ensure_parent([frontier_compact, frontier_by_year, frontier_reco, frontier_md])
        if _skip_if_resume([frontier_compact, frontier_by_year, frontier_reco, frontier_md], resume=args.resume):
            print("[SKIP] canonical frontier exists")
        else:
            cmd = [
                py,
                "ml_pipeline/phase2b_threshold_frontier.py",
                "--data-lake",
                str(data_lake),
                "--years",
                *[str(year) for year in years],
                "--season-tag",
                args.season_tag,
                "--oof-dir",
                str(output_dir / "oof"),
                "--score-column",
                "calibrated_proba",
                "--truth-universe-race-driver-csv",
                str(truth_universe_csv),
                "--truth-universe-events-csvs",
                *[str(path) for path in truth_event_csvs],
                "--truth-universe-mode-label",
                "canonical_sde_truth",
                "--output-compact-csv",
                str(frontier_compact),
                "--output-by-year-csv",
                str(frontier_by_year),
                "--output-recommended-csv",
                str(frontier_reco),
                "--output-md",
                str(frontier_md),
            ]
            if args.pit_evals_jsonl.strip():
                cmd.extend(["--pit-evals-jsonl", args.pit_evals_jsonl])
            if args.pit_timings_jsonl.strip():
                cmd.extend(["--pit-timings-jsonl", args.pit_timings_jsonl])
            if args.ml_features_jsonl.strip():
                cmd.extend(["--ml-features-jsonl", args.ml_features_jsonl])
            if args.prepared_pit_events_csv.strip():
                cmd.extend(["--prepared-pit-events-csv", args.prepared_pit_events_csv])
            _run(cmd, dry_run=args.dry_run)

        if run_native:
            frontier_compact_native = output_dir / "frontier" / "phase2b_threshold_frontier_compact_native.csv"
            frontier_by_year_native = output_dir / "by_year" / "phase2b_threshold_frontier_by_year_native.csv"
            frontier_reco_native = output_dir / "recommended" / "phase2b_recommended_operating_points_native.csv"
            frontier_md_native = output_dir / "phase2b_threshold_frontier_report_native.md"
            _ensure_parent([frontier_compact_native, frontier_by_year_native, frontier_reco_native, frontier_md_native])
            if _skip_if_resume(
                [frontier_compact_native, frontier_by_year_native, frontier_reco_native, frontier_md_native],
                resume=args.resume,
            ):
                print("[SKIP] native frontier exists")
            else:
                cmd = [
                    py,
                    "ml_pipeline/phase2b_threshold_frontier.py",
                    "--data-lake",
                    str(data_lake),
                    "--years",
                    *[str(year) for year in years],
                    "--season-tag",
                    args.season_tag,
                    "--oof-dir",
                    str(output_dir / "native" / "oof"),
                    "--score-column",
                    "calibrated_proba",
                    "--truth-universe-mode-label",
                    "native_universe",
                    "--output-compact-csv",
                    str(frontier_compact_native),
                    "--output-by-year-csv",
                    str(frontier_by_year_native),
                    "--output-recommended-csv",
                    str(frontier_reco_native),
                    "--output-md",
                    str(frontier_md_native),
                ]
                if args.pit_evals_jsonl.strip():
                    cmd.extend(["--pit-evals-jsonl", args.pit_evals_jsonl])
                if args.pit_timings_jsonl.strip():
                    cmd.extend(["--pit-timings-jsonl", args.pit_timings_jsonl])
                if args.ml_features_jsonl.strip():
                    cmd.extend(["--ml-features-jsonl", args.ml_features_jsonl])
                if args.prepared_pit_events_csv.strip():
                    cmd.extend(["--prepared-pit-events-csv", args.prepared_pit_events_csv])
                _run(cmd, dry_run=args.dry_run)

        if not args.dry_run:
            canonical_compact = (
                output_dir / "recommended" / "phase2b_e0_vs_p1_canonical_compact.csv"
            )
            _build_profile_comparison(
                recommended_csv=output_dir / "recommended" / "phase2b_recommended_operating_points.csv",
                output_csv=canonical_compact,
            )
            if run_native:
                native_compact = (
                    output_dir / "recommended" / "phase2b_e0_vs_p1_native_compact.csv"
                )
                _build_profile_comparison(
                    recommended_csv=output_dir / "recommended" / "phase2b_recommended_operating_points_native.csv",
                    output_csv=native_compact,
                )

    # Stage 4: MOA prequential summary
    if stages["prequential"]:
        prequential_csv = output_dir / "prequential" / "sml_phase2b_preq_summary.csv"
        if _skip_if_resume([prequential_csv], resume=args.resume):
            print("[SKIP] prequential summary exists")
        else:
            if not args.dry_run:
                _build_prequential_summary(
                    run_ids=run_ids,
                    summary_dir=output_dir / "moa" / "summary",
                    output_csv=prequential_csv,
                )
            else:
                print("[RUN]", f"build prequential summary -> {prequential_csv}")

    print("=== SML PHASE2B ORCHESTRATION READY ===")
    print(f"output dir              : {output_dir}")
    print(f"profiles                : {profiles}")
    print(f"targets                 : {targets}")
    print(f"jobs                    : {jobs}")
    print(f"run native sensitivity  : {run_native}")
    print(f"canonical truth csv     : {truth_universe_csv}")
    print(f"canonical truth events  : {[str(p) for p in truth_event_csvs]}")
    print(f"dry run                 : {bool(args.dry_run)}")


if __name__ == "__main__":
    main()
