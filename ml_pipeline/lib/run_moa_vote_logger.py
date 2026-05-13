"""Run custom MOA prequential vote logger with continuous-score output."""

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
DEFAULT_JAVAC_BIN = "javac"
DEFAULT_CLASS_INDEX = -1
DEFAULT_INSTANCE_LIMIT = -1
DEFAULT_LEARNER_CLI = "meta.OzaBoostAdwin -s 42"
DEFAULT_CLASS_NAME = "MoaPrequentialVoteLogger"
DEFAULT_SOURCE_PATH = Path("ml_pipeline/java_src/MoaPrequentialVoteLogger.java")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "run custom MOA prequential vote logger that emits per-instance "
            "vote-based score signals for dual-contract evaluation"
        )
    )
    parser.add_argument("--data-lake", default=DEFAULT_DATA_LAKE)
    parser.add_argument("--years", type=int, nargs="+", default=list(DEFAULT_YEARS))
    parser.add_argument("--season-tag", default=DEFAULT_SEASON_TAG)

    parser.add_argument("--input-arff", default="", help="input ARFF path")
    parser.add_argument("--moa-jar", default=os.environ.get("MOA_JAR", str(DEFAULT_MOA_JAR)))
    parser.add_argument("--java-bin", default=DEFAULT_JAVA_BIN)
    parser.add_argument("--javac-bin", default=DEFAULT_JAVAC_BIN)
    parser.add_argument("--class-index", type=int, default=DEFAULT_CLASS_INDEX)
    parser.add_argument("--instance-limit", type=int, default=DEFAULT_INSTANCE_LIMIT)
    parser.add_argument("--learner-cli", default=DEFAULT_LEARNER_CLI)

    parser.add_argument(
        "--logger-source",
        default=str(DEFAULT_SOURCE_PATH),
        help="path to MoaPrequentialVoteLogger.java source",
    )
    parser.add_argument(
        "--compiled-classes-dir",
        default="",
        help="directory for compiled logger class files",
    )
    parser.add_argument("--expected-rows", type=int, default=-1)
    parser.add_argument(
        "--validate-target-csv",
        default="",
        help="optional exported moa csv to verify true_label alignment against target_y",
    )
    parser.add_argument("--min-target-purity", type=float, default=0.99)

    parser.add_argument("--dry-run", action="store_true")

    parser.add_argument("--predictions-output", default="")
    parser.add_argument("--summary-output", default="")
    parser.add_argument("--stdout-output", default="")
    parser.add_argument("--stderr-output", default="")
    parser.add_argument("--metadata-output", default="")
    return parser.parse_args()


def _json_safe(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _build_defaults(args: argparse.Namespace) -> dict[str, Path]:
    years = normalize_years(args.years)
    data_lake = Path(args.data_lake)

    input_arff = (
        Path(args.input_arff)
        if args.input_arff.strip()
        else default_report_csv(data_lake, "moa_dataset", years, args.season_tag).with_suffix(".arff")
    )
    predictions_output = (
        Path(args.predictions_output)
        if args.predictions_output.strip()
        else default_report_csv(data_lake, "moa_vote_predictions", years, args.season_tag).with_suffix(".csv")
    )
    summary_output = (
        Path(args.summary_output)
        if args.summary_output.strip()
        else default_report_csv(data_lake, "moa_vote_summary", years, args.season_tag)
    )
    stdout_output = (
        Path(args.stdout_output)
        if args.stdout_output.strip()
        else default_report_txt(data_lake, "moa_vote_stdout", years, args.season_tag)
    )
    stderr_output = (
        Path(args.stderr_output)
        if args.stderr_output.strip()
        else default_report_txt(data_lake, "moa_vote_stderr", years, args.season_tag)
    )
    metadata_output = (
        Path(args.metadata_output)
        if args.metadata_output.strip()
        else reports_dir(data_lake)
        / f"moa_vote_run_{'_'.join(str(y) for y in years)}_{args.season_tag}.json"
    )
    compiled_dir = (
        Path(args.compiled_classes_dir)
        if args.compiled_classes_dir.strip()
        else data_lake / "tools" / "moa_vote_logger_classes"
    )

    return {
        "input_arff": input_arff,
        "predictions_output": predictions_output,
        "summary_output": summary_output,
        "stdout_output": stdout_output,
        "stderr_output": stderr_output,
        "metadata_output": metadata_output,
        "compiled_dir": compiled_dir,
        "source_path": Path(args.logger_source),
        "moa_jar": Path(args.moa_jar),
    }


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _inspect_predictions_csv(path: Path) -> dict[str, Any]:
    info: dict[str, Any] = {
        "prediction_rows": 0,
        "prediction_columns": [],
        "row_index_contiguous": False,
        "score_unique_count": 0,
        "score_is_hard_decision": True,
        "score_frontier_quality": "hard_decision_only",
    }
    if not path.exists():
        return info

    frame = pd.read_csv(path)
    required = {
        "row_index",
        "true_label",
        "predicted_label",
        "vote_0",
        "vote_1",
        "positive_score",
    }
    missing = sorted(required.difference(set(frame.columns)))
    if missing:
        raise ValueError(f"predictions csv missing required columns {missing}: {path}")

    info["prediction_rows"] = int(len(frame))
    info["prediction_columns"] = list(frame.columns)
    row_index = pd.to_numeric(frame["row_index"], errors="coerce")
    contiguous = bool(
        row_index.notna().all()
        and len(row_index) == int(row_index.max() + 1) if len(row_index) else True
    )
    if contiguous and len(row_index):
        contiguous = bool((row_index.astype(int) == pd.RangeIndex(start=0, stop=len(row_index))).all())
    info["row_index_contiguous"] = contiguous
    if not contiguous:
        raise ValueError(f"row_index is not contiguous from 0..N-1: {path}")

    score = pd.to_numeric(frame["positive_score"], errors="coerce")
    unique = int(score.dropna().nunique())
    info["score_unique_count"] = unique
    info["score_is_hard_decision"] = bool(unique <= 2)
    info["score_frontier_quality"] = (
        "hard_decision_only" if unique <= 2 else "continuous_score_frontier"
    )
    return info


def _validate_target_alignment(
    predictions_path: Path,
    target_csv_path: Path,
    min_purity: float,
) -> dict[str, Any]:
    pred = pd.read_csv(predictions_path, usecols=["row_index", "true_label"])
    target = pd.read_csv(target_csv_path, usecols=["target_y"])
    if len(pred) != len(target):
        raise ValueError(
            "target alignment row mismatch: "
            f"predictions={len(pred)} target_csv={len(target)}"
        )

    target_y = pd.to_numeric(target["target_y"], errors="coerce")
    if target_y.isna().any():
        raise ValueError(f"target_y contains non-numeric values: {target_csv_path}")
    target_y = target_y.astype(int)

    frame = pd.DataFrame(
        {
            "true_label": pd.to_numeric(pred["true_label"], errors="coerce"),
            "target_y": target_y,
        }
    )
    frame = frame.dropna(subset=["true_label"]).copy()
    if frame.empty:
        raise ValueError("cannot validate target alignment: no valid true_label rows")

    purity: dict[float, float] = {}
    mapping: dict[float, int] = {}
    for code, grp in frame.groupby("true_label"):
        counts = grp["target_y"].value_counts(normalize=True)
        mapping[float(code)] = int(counts.index[0])
        purity[float(code)] = float(counts.iloc[0])

    low = {str(k): v for k, v in purity.items() if v < min_purity}
    if low:
        raise ValueError(
            "target alignment purity below threshold: "
            f"{low}, min_target_purity={min_purity}"
        )

    return {
        "target_alignment_codes": {str(k): int(v) for k, v in mapping.items()},
        "target_alignment_purity": {str(k): float(v) for k, v in purity.items()},
    }


def main() -> None:
    args = parse_args()
    if args.instance_limit == 0 or args.instance_limit < -1:
        raise ValueError("--instance-limit must be -1 or >= 1")

    paths = _build_defaults(args)
    input_arff = paths["input_arff"]
    predictions_output = paths["predictions_output"]
    summary_output = paths["summary_output"]
    stdout_output = paths["stdout_output"]
    stderr_output = paths["stderr_output"]
    metadata_output = paths["metadata_output"]
    compiled_dir = paths["compiled_dir"]
    source_path = paths["source_path"]
    moa_jar = paths["moa_jar"]

    if not input_arff.exists():
        raise FileNotFoundError(f"input ARFF not found: {input_arff}")
    if not source_path.exists():
        raise FileNotFoundError(f"logger source not found: {source_path}")
    if not args.dry_run and not moa_jar.exists():
        raise FileNotFoundError(f"MOA jar not found: {moa_jar}")

    compiled_dir.mkdir(parents=True, exist_ok=True)
    for p in [predictions_output, summary_output, stdout_output, stderr_output, metadata_output]:
        p.parent.mkdir(parents=True, exist_ok=True)

    compile_cmd = [
        args.javac_bin,
        "-cp",
        str(moa_jar),
        "-d",
        str(compiled_dir),
        str(source_path),
    ]
    run_cmd = [
        args.java_bin,
        "-cp",
        f"{moa_jar}:{compiled_dir}",
        DEFAULT_CLASS_NAME,
        str(input_arff),
        args.learner_cli,
        str(args.class_index),
        str(args.instance_limit),
        str(predictions_output),
    ]

    compile_cmd_str = " ".join(shlex.quote(part) for part in compile_cmd)
    run_cmd_str = " ".join(shlex.quote(part) for part in run_cmd)

    started_at = datetime.now(UTC)
    run_status = "DRY_RUN"
    exit_code = 0
    stdout_text = ""
    stderr_text = ""

    if args.dry_run:
        stdout_text = "dry-run, command was not executed\n"
    else:
        # clean previous files to avoid stale artifacts
        for p in [predictions_output, summary_output, stdout_output, stderr_output, metadata_output]:
            p.unlink(missing_ok=True)

        subprocess.run(compile_cmd, check=True, text=True, capture_output=True)
        completed = subprocess.run(run_cmd, text=True, capture_output=True, check=False)
        stdout_text = completed.stdout
        stderr_text = completed.stderr
        exit_code = int(completed.returncode)
        run_status = "SUCCESS" if exit_code == 0 else "FAILED"

    finished_at = datetime.now(UTC)
    duration_sec = (finished_at - started_at).total_seconds()

    _write_text(stdout_output, stdout_text)
    _write_text(stderr_output, stderr_text)

    inspection: dict[str, Any] = {}
    target_alignment: dict[str, Any] = {}
    rows_written = pd.NA
    if not args.dry_run and run_status == "SUCCESS":
        inspection = _inspect_predictions_csv(predictions_output)
        rows_written = int(inspection.get("prediction_rows", 0))
        if args.expected_rows >= 0 and int(rows_written) != int(args.expected_rows):
            raise ValueError(
                f"rows_written mismatch: expected_rows={args.expected_rows} actual_rows={rows_written}"
            )
        if args.validate_target_csv.strip():
            target_alignment = _validate_target_alignment(
                predictions_path=predictions_output,
                target_csv_path=Path(args.validate_target_csv),
                min_purity=float(args.min_target_purity),
            )

    summary_row: dict[str, Any] = {
        "status": run_status,
        "exit_code": exit_code,
        "started_at_utc": started_at.isoformat(),
        "finished_at_utc": finished_at.isoformat(),
        "duration_sec": duration_sec,
        "input_arff": str(input_arff),
        "moa_jar": str(moa_jar),
        "logger_source": str(source_path),
        "compiled_classes_dir": str(compiled_dir),
        "learner_cli": str(args.learner_cli),
        "class_index": int(args.class_index),
        "instance_limit": int(args.instance_limit),
        "predictions_output": str(predictions_output),
        "rows_written": rows_written,
        "summary_output": str(summary_output),
        "stdout_output": str(stdout_output),
        "stderr_output": str(stderr_output),
        "metadata_output": str(metadata_output),
        "expected_rows": int(args.expected_rows),
    }
    for key, value in inspection.items():
        summary_row[key] = _json_safe(value)
    for key, value in target_alignment.items():
        summary_row[key] = _json_safe(value)

    pd.DataFrame([summary_row]).to_csv(summary_output, index=False)

    metadata = {
        "status": run_status,
        "exit_code": exit_code,
        "compile_command": compile_cmd,
        "compile_command_string": compile_cmd_str,
        "run_command": run_cmd,
        "run_command_string": run_cmd_str,
        "summary_row": summary_row,
    }
    metadata_output.write_text(json.dumps(metadata, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    print("=== MOA VOTE LOGGER RUN SUMMARY ===")
    print(f"status             : {run_status}")
    print(f"compile command    : {compile_cmd_str}")
    print(f"run command        : {run_cmd_str}")
    print(f"predictions output : {predictions_output}")
    print(f"summary output     : {summary_output}")
    print(f"stdout output      : {stdout_output}")
    print(f"stderr output      : {stderr_output}")
    print(f"metadata output    : {metadata_output}")
    if inspection:
        print(f"rows written       : {inspection.get('prediction_rows')}")
        print(f"score unique count : {inspection.get('score_unique_count')}")
        print(f"score frontier     : {inspection.get('score_frontier_quality')}")

    if run_status == "FAILED":
        raise RuntimeError(f"MOA vote logger failed with exit code {exit_code}")


if __name__ == "__main__":
    main()

