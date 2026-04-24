"""Run a reproducible MOA Adaptive Random Forest baseline on exported ARFF data."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline_config import (
    DEFAULT_DATA_LAKE,
    DEFAULT_SEASON_TAG,
    DEFAULT_YEARS,
    default_report_csv,
    default_report_txt,
    normalize_years,
    reports_dir,
)


DEFAULT_MOA_JAR = Path("data_lake/tools/moa.jar")
DEFAULT_JAVA_BIN = "java"
DEFAULT_SAMPLE_FREQUENCY = 1000
DEFAULT_INSTANCE_LIMIT = -1
DEFAULT_RANDOM_SEED = 42
DEFAULT_CLASS_INDEX = -1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="run MOA EvaluatePrequential with AdaptiveRandomForest on exported ARFF"
    )
    parser.add_argument("--data-lake", default=DEFAULT_DATA_LAKE, help="data lake directory")
    parser.add_argument("--years", type=int, nargs="+", default=list(DEFAULT_YEARS), help="season years")
    parser.add_argument("--season-tag", default=DEFAULT_SEASON_TAG, help="season tag token")

    parser.add_argument(
        "--input-arff",
        default="",
        help="MOA input arff path, defaults to exported moa_dataset_<suffix>.arff",
    )
    parser.add_argument(
        "--moa-jar",
        default=os.environ.get("MOA_JAR", str(DEFAULT_MOA_JAR)),
        help="path to MOA jar (or set MOA_JAR env var)",
    )
    parser.add_argument("--java-bin", default=DEFAULT_JAVA_BIN, help="java executable")

    parser.add_argument("--seed", type=int, default=DEFAULT_RANDOM_SEED, help="ARF seed")
    parser.add_argument(
        "--sample-frequency",
        type=int,
        default=DEFAULT_SAMPLE_FREQUENCY,
        help="MOA evaluation frequency in processed instances",
    )
    parser.add_argument(
        "--instance-limit",
        type=int,
        default=DEFAULT_INSTANCE_LIMIT,
        help="instance limit for EvaluatePrequential, use -1 for full stream",
    )
    parser.add_argument(
        "--class-index",
        type=int,
        default=DEFAULT_CLASS_INDEX,
        help="ARFF class attribute index, use -1 for last column",
    )

    parser.add_argument("--dry-run", action="store_true", help="emit command artifacts without executing MOA")

    parser.add_argument("--learning-curve-output", default="", help="output csv for MOA learning curve")
    parser.add_argument("--summary-output", default="", help="output csv for run summary")
    parser.add_argument("--stdout-output", default="", help="captured stdout path")
    parser.add_argument("--stderr-output", default="", help="captured stderr path")
    parser.add_argument("--predictions-output", default="", help="output prediction rows from MOA")
    parser.add_argument("--metadata-output", default="", help="output json metadata path")

    return parser.parse_args()


def _build_defaults(args: argparse.Namespace) -> dict[str, Path]:
    years = normalize_years(args.years)
    data_lake = Path(args.data_lake)

    default_arff = default_report_csv(data_lake, "moa_dataset", years, args.season_tag).with_suffix(".arff")
    learning_curve = (
        Path(args.learning_curve_output)
        if args.learning_curve_output
        else default_report_csv(data_lake, "moa_arf_learning_curve", years, args.season_tag)
    )
    summary = (
        Path(args.summary_output)
        if args.summary_output
        else default_report_csv(data_lake, "moa_arf_summary", years, args.season_tag)
    )
    stdout_path = (
        Path(args.stdout_output)
        if args.stdout_output
        else default_report_txt(data_lake, "moa_arf_stdout", years, args.season_tag)
    )
    stderr_path = (
        Path(args.stderr_output)
        if args.stderr_output
        else default_report_txt(data_lake, "moa_arf_stderr", years, args.season_tag)
    )
    predictions_path = (
        Path(args.predictions_output)
        if args.predictions_output
        else default_report_csv(data_lake, "moa_arf_predictions", years, args.season_tag).with_suffix(".pred")
    )
    metadata = (
        Path(args.metadata_output)
        if args.metadata_output
        else reports_dir(data_lake) / f"moa_arf_run_{'_'.join(str(y) for y in years)}_{args.season_tag}.json"
    )

    input_arff = Path(args.input_arff) if args.input_arff else default_arff

    return {
        "input_arff": input_arff,
        "learning_curve": learning_curve,
        "summary": summary,
        "stdout": stdout_path,
        "stderr": stderr_path,
        "predictions": predictions_path,
        "metadata": metadata,
        "moa_jar": Path(args.moa_jar),
    }


def _build_moa_task(
    input_arff: Path,
    learning_curve: Path,
    predictions_output: Path,
    seed: int,
    class_index: int,
    sample_frequency: int,
    instance_limit: int,
) -> str:
    return (
        "EvaluatePrequential "
        # --- STORICO ESPERIMENTI (Non cancellare, utile per la tesi) ---
        # f"-l (meta.AdaptiveRandomForest -s {seed}) "           # 1. Troppo pigro (Trovati 3 pit stop su 124k)
        # f"-l (meta.LeveragingBag -s {seed}) "                  # 2. Collassato totalmente (Trovati 0 pit stop)
        # f"-l (meta.OOB -l trees.HoeffdingAdaptiveTree -s 15) " # 3. Crash (Classe assente nel jar di base MOA)
        # ----------------------------------------------------------------
        # SOLUZIONE ATTUALE: Online Boosting con ADWIN Drift Detection
        # Il Boosting penalizza fortemente gli errori sulle classi minoritarie
        f"-l (meta.OzaBoostAdwin -s {seed}) "
        "-s (ArffFileStream "
        f"-f \"{input_arff}\" "
        f"-c {class_index}) "
        "-e (BasicClassificationPerformanceEvaluator) "
        f"-i {instance_limit} "
        f"-f {sample_frequency} "
        f"-d \"{learning_curve}\" "
        f"-o \"{predictions_output}\""
    )


def _json_safe(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _extract_final_row(learning_curve_path: Path) -> dict[str, Any]:
    if not learning_curve_path.exists():
        return {}
    frame = pd.read_csv(learning_curve_path)
    if frame.empty:
        return {}
    return {str(k): _json_safe(v) for k, v in frame.iloc[-1].to_dict().items()}


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def main() -> None:
    args = parse_args()

    if args.sample_frequency < 1:
        raise ValueError("--sample-frequency must be >= 1")
    if args.instance_limit == 0 or args.instance_limit < -1:
        raise ValueError("--instance-limit must be -1 or >= 1")

    paths = _build_defaults(args)

    input_arff = paths["input_arff"]
    learning_curve = paths["learning_curve"]
    summary_output = paths["summary"]
    stdout_output = paths["stdout"]
    stderr_output = paths["stderr"]
    predictions_output = paths["predictions"]
    metadata_output = paths["metadata"]
    moa_jar = paths["moa_jar"]

    if not input_arff.exists():
        raise FileNotFoundError(
            f"input ARFF not found: {input_arff}. Run export_moa_dataset.py first."
        )

    task = _build_moa_task(
        input_arff=input_arff,
        learning_curve=learning_curve,
        predictions_output=predictions_output,
        seed=args.seed,
        class_index=args.class_index,
        sample_frequency=args.sample_frequency,
        instance_limit=args.instance_limit,
    )

    command = [args.java_bin, "-cp", str(moa_jar), "moa.DoTask", task]
    command_str = " ".join(shlex.quote(part) for part in command)

    started_at = datetime.now(UTC)
    run_status = "DRY_RUN"
    exit_code = 0
    stdout_text = ""
    stderr_text = ""

    if args.dry_run:
        stdout_text = "dry-run, command was not executed\n"
    else:
        if not moa_jar.exists():
            raise FileNotFoundError(
                f"MOA jar not found: {moa_jar}. Provide --moa-jar or set MOA_JAR."
            )
        
        print("Cleaning up old MOA output files to prevent append corruption...")
        for p in [learning_curve, summary_output, stdout_output, stderr_output, predictions_output, metadata_output]:
            p.unlink(missing_ok=True)

        completed = subprocess.run(
            command,
            text=True,
            capture_output=True,
            check=False,
        )
        stdout_text = completed.stdout
        stderr_text = completed.stderr
        exit_code = int(completed.returncode)
        run_status = "SUCCESS" if exit_code == 0 else "FAILED"

    finished_at = datetime.now(UTC)
    duration_sec = (finished_at - started_at).total_seconds()

    _write_text(stdout_output, stdout_text)
    _write_text(stderr_output, stderr_text)

    final_row = _extract_final_row(learning_curve)

    summary_row: dict[str, Any] = {
        "status": run_status,
        "exit_code": exit_code,
        "started_at_utc": started_at.isoformat(),
        "finished_at_utc": finished_at.isoformat(),
        "duration_sec": duration_sec,
        "input_arff": str(input_arff),
        "moa_jar": str(moa_jar),
        "seed": args.seed,
        "sample_frequency": args.sample_frequency,
        "instance_limit": args.instance_limit,
        "class_index": args.class_index,
        "learning_curve_output": str(learning_curve),
        "stdout_output": str(stdout_output),
        "stderr_output": str(stderr_output),
        "predictions_output": str(predictions_output),
    }
    for key, value in final_row.items():
        summary_row[f"final_{key}"] = value

    summary_output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([summary_row]).to_csv(summary_output, index=False)

    metadata = {
        "status": run_status,
        "exit_code": exit_code,
        "command": command,
        "command_string": command_str,
        "task": task,
        "summary_output": str(summary_output),
        "learning_curve_output": str(learning_curve),
        "stdout_output": str(stdout_output),
        "stderr_output": str(stderr_output),
        "predictions_output": str(predictions_output),
        "final_learning_curve_row": final_row,
        "started_at_utc": started_at.isoformat(),
        "finished_at_utc": finished_at.isoformat(),
        "duration_sec": duration_sec,
    }
    metadata_output.parent.mkdir(parents=True, exist_ok=True)
    metadata_output.write_text(json.dumps(metadata, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    print("=== MOA ARF RUN SUMMARY ===")
    print(f"status             : {run_status}")
    print(f"command            : {command_str}")
    print(f"summary output     : {summary_output}")
    print(f"learning curve csv : {learning_curve}")
    print(f"stdout output      : {stdout_output}")
    print(f"stderr output      : {stderr_output}")
    print(f"predictions output : {predictions_output}")
    print(f"metadata output    : {metadata_output}")

    if run_status == "FAILED":
        raise RuntimeError(f"MOA ARF run failed with exit code {exit_code}")


if __name__ == "__main__":
    main()