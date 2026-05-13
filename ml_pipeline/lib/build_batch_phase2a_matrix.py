"""Combine per-run Batch dual-contract evaluation rows into matrix tables."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def _parse_named_csv(tokens: list[str], label: str) -> list[tuple[str, Path]]:
    out: list[tuple[str, Path]] = []
    for token in tokens:
        if "=" not in token:
            raise ValueError(f"invalid {label}: {token!r}; expected run_id=path.csv")
        run_id, path_text = token.split("=", 1)
        run = run_id.strip()
        path = Path(path_text.strip())
        if not run:
            raise ValueError(f"invalid {label}: empty run id in {token!r}")
        if not path.exists():
            raise FileNotFoundError(f"{label} path not found for {run}: {path}")
        out.append((run, path))
    if not out:
        raise ValueError(f"at least one {label} must be provided")
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build compact Phase-2A Batch matrix outputs from per-run CSVs.")
    parser.add_argument("--run-summary", action="append", default=[], help="run_id=summary_csv")
    parser.add_argument("--run-by-year", action="append", default=[], help="run_id=by_year_csv")
    parser.add_argument("--output-matrix-csv", required=True, help="compact matrix output csv")
    parser.add_argument("--output-by-year-csv", required=True, help="combined by-year output csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summaries = _parse_named_csv(args.run_summary, "--run-summary")
    by_year = _parse_named_csv(args.run_by_year, "--run-by-year")
    by_year_map = {run: path for run, path in by_year}

    summary_frames: list[pd.DataFrame] = []
    by_year_frames: list[pd.DataFrame] = []

    for run_id, summary_path in summaries:
        frame = pd.read_csv(summary_path)
        if frame.empty:
            raise ValueError(f"summary csv is empty for {run_id}: {summary_path}")
        frame = frame.copy()
        frame["run_id"] = run_id
        summary_frames.append(frame)

        if run_id not in by_year_map:
            raise ValueError(f"missing --run-by-year for run_id={run_id}")
        year_path = by_year_map[run_id]
        year_frame = pd.read_csv(year_path)
        if year_frame.empty:
            raise ValueError(f"by-year csv is empty for {run_id}: {year_path}")
        year_frame = year_frame.copy()
        year_frame["run_id"] = run_id
        by_year_frames.append(year_frame)

    matrix = pd.concat(summary_frames, ignore_index=True)
    matrix.sort_values(
        by=["profile", "outcome_mode", "truth_lens", "target_column"],
        kind="mergesort",
        inplace=True,
    )
    matrix.reset_index(drop=True, inplace=True)
    if "AP" not in matrix.columns:
        matrix["AP"] = pd.Series([pd.NA] * len(matrix), index=matrix.index)
    matrix["AP"] = pd.to_numeric(matrix["AP"], errors="coerce")
    if "ap_calibrated" in matrix.columns:
        matrix["AP"] = matrix["AP"].fillna(
            pd.to_numeric(matrix["ap_calibrated"], errors="coerce")
        )
    if "ap_raw" in matrix.columns:
        matrix["AP"] = matrix["AP"].fillna(
            pd.to_numeric(matrix["ap_raw"], errors="coerce")
        )

    compact_cols = [
        "profile",
        "target_column",
        "outcome_mode",
        "truth_lens",
        "rows",
        "positives",
        "prevalence",
        "AP",
        "selected_threshold",
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
        "comparator_actual_count",
        "tp_for_recall_plus_fn_equals_eligible",
        "row_tp_plus_fn_equals_eligible",
        "recall_count_mode",
    ]
    if "row_tp" not in matrix.columns and "tp" in matrix.columns:
        matrix["row_tp"] = pd.to_numeric(matrix["tp"], errors="coerce").fillna(0).astype(int)
    if "tp_for_recall" not in matrix.columns:
        matrix["tp_for_recall"] = pd.to_numeric(matrix.get("row_tp"), errors="coerce").fillna(0).astype(int)
    if "comparator_actual_count" not in matrix.columns:
        matrix["comparator_actual_count"] = (
            pd.to_numeric(matrix.get("tp_for_recall"), errors="coerce").fillna(0).astype(int)
            + pd.to_numeric(matrix.get("fn"), errors="coerce").fillna(0).astype(int)
        )
    if "tp_for_recall_plus_fn_equals_eligible" not in matrix.columns and "eligible_actual_pit_count" in matrix.columns:
        matrix["tp_for_recall_plus_fn_equals_eligible"] = (
            pd.to_numeric(matrix.get("tp_for_recall"), errors="coerce").fillna(0).astype(int)
            + pd.to_numeric(matrix.get("fn"), errors="coerce").fillna(0).astype(int)
            == pd.to_numeric(matrix.get("eligible_actual_pit_count"), errors="coerce").fillna(0).astype(int)
        )
    if "row_tp_plus_fn_equals_eligible" not in matrix.columns and "eligible_actual_pit_count" in matrix.columns:
        matrix["row_tp_plus_fn_equals_eligible"] = (
            pd.to_numeric(matrix.get("row_tp"), errors="coerce").fillna(0).astype(int)
            + pd.to_numeric(matrix.get("fn"), errors="coerce").fillna(0).astype(int)
            == pd.to_numeric(matrix.get("eligible_actual_pit_count"), errors="coerce").fillna(0).astype(int)
        )
    if "recall_count_mode" not in matrix.columns:
        matrix["recall_count_mode"] = "event_level_unique_pits"
    for col in compact_cols:
        if col not in matrix.columns:
            raise ValueError(f"compact matrix missing required column: {col}")
    matrix_compact = matrix[compact_cols].copy()

    by_year_df = pd.concat(by_year_frames, ignore_index=True)
    by_year_df.sort_values(
        by=["year", "profile", "outcome_mode", "truth_lens", "target_column"],
        kind="mergesort",
        inplace=True,
    )
    by_year_df.reset_index(drop=True, inplace=True)

    out_matrix = Path(args.output_matrix_csv)
    out_year = Path(args.output_by_year_csv)
    out_matrix.parent.mkdir(parents=True, exist_ok=True)
    out_year.parent.mkdir(parents=True, exist_ok=True)
    matrix_compact.to_csv(out_matrix, index=False)
    by_year_df.to_csv(out_year, index=False)

    print("=== BATCH PHASE2A MATRIX GENERATED ===")
    print(f"matrix csv  : {out_matrix}")
    print(f"by-year csv : {out_year}")
    print(matrix_compact.to_string(index=False))


if __name__ == "__main__":
    main()
