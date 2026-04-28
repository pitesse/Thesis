"""Run split-integrity audit on merged training and OOF artifacts.

this script verifies grouped temporal validation assumptions are preserved:
- merged race keys use year-prefixed format,
- grouped k-fold split has zero race overlap across train and test,
- winner oof fold assignment keeps each race in one fold only,
- oof coverage matches the merged training dataset.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.model_selection import GroupKFold

DEFAULT_DATASET = "data_lake/ml_training_dataset_2022_2025_merged.parquet"
DEFAULT_OOF = "data_lake/reports/ml_oof_winner_2022_2025_merged.csv"
DEFAULT_FOLDS = 5

DEFAULT_SUMMARY_OUTPUT = "data_lake/reports/split_integrity_summary_2022_2025_merged.csv"
DEFAULT_DETAILS_OUTPUT = "data_lake/reports/split_integrity_details_2022_2025_merged.csv"
DEFAULT_REPORT_OUTPUT = "data_lake/reports/split_integrity_report_2022_2025_merged.txt"


def _status(ok: bool) -> str:
    return "PASS" if ok else "FAIL"


def _safe_ratio(num: float, den: float) -> float:
    if den <= 0:
        return float("nan")
    return float(num / den)


def _load_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"input file not found: {path}")

    if path.suffix.lower() == ".parquet":
        frame = pd.read_parquet(path)
    else:
        frame = pd.read_csv(path)

    if frame.empty:
        raise ValueError(f"input file is empty: {path}")

    return frame


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="run split-integrity audit")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="merged training dataset path")
    parser.add_argument("--oof", default=DEFAULT_OOF, help="winner oof decisions csv path")
    parser.add_argument("--folds", type=int, default=DEFAULT_FOLDS, help="expected grouped fold count")
    parser.add_argument("--summary-output", default=DEFAULT_SUMMARY_OUTPUT, help="summary output csv")
    parser.add_argument("--details-output", default=DEFAULT_DETAILS_OUTPUT, help="details output csv")
    parser.add_argument("--report-output", default=DEFAULT_REPORT_OUTPUT, help="text report output")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.folds < 2:
        raise ValueError("--folds must be at least 2")

    dataset_path = Path(args.dataset)
    oof_path = Path(args.oof)

    dataset = _load_table(dataset_path)
    if "race" not in dataset.columns:
        raise ValueError("dataset is missing required column: race")

    races = dataset["race"].astype(str)
    row_count = int(len(dataset))
    unique_races = int(races.nunique())

    summary_rows: list[dict[str, object]] = []
    detail_rows: list[dict[str, object]] = []

    prefix_mask = races.str.match(r"^\d{4}\s::\s.+$")
    invalid_prefix_count = int((~prefix_mask).sum())
    prefix_ok = invalid_prefix_count == 0
    summary_rows.append(
        {
            "check": "race_key_prefix_format",
            "status": _status(prefix_ok),
            "value": 1.0 - _safe_ratio(invalid_prefix_count, row_count),
            "threshold": 1.0,
            "note": f"invalid_prefix_rows={invalid_prefix_count}",
        }
    )

    source_year_consistent = True
    source_year_mismatch = 0
    if "_source_year" in dataset.columns:
        parsed_year = pd.to_numeric(races.str.split(" :: ", n=1).str[0], errors="coerce")
        source_year = pd.to_numeric(dataset["_source_year"], errors="coerce")
        mismatch_mask = (~parsed_year.isna()) & (~source_year.isna()) & (parsed_year != source_year)
        source_year_mismatch = int(mismatch_mask.sum())
        source_year_consistent = source_year_mismatch == 0

    summary_rows.append(
        {
            "check": "source_year_prefix_consistency",
            "status": _status(source_year_consistent),
            "value": 1.0 - _safe_ratio(source_year_mismatch, row_count),
            "threshold": 1.0,
            "note": f"mismatch_rows={source_year_mismatch}",
        }
    )

    grouped_overlap_count = 0
    grouped_overlap_folds = 0
    # labels are irrelevant here; GroupKFold only needs row count and race groups.
    dummy = np.zeros(row_count, dtype=np.int8)
    gkf = GroupKFold(n_splits=args.folds)

    for fold_idx, (train_idx, test_idx) in enumerate(gkf.split(dummy, dummy, groups=races), start=1):
        train_races = set(races.iloc[train_idx].unique().tolist())
        test_races = set(races.iloc[test_idx].unique().tolist())
        overlap = sorted(train_races.intersection(test_races))
        overlap_count = int(len(overlap))
        if overlap_count > 0:
            grouped_overlap_folds += 1
            grouped_overlap_count += overlap_count

        detail_rows.append(
            {
                "section": "grouped_split",
                "name": f"fold_{fold_idx}",
                "value": overlap_count,
                "note": (
                    f"train_rows={len(train_idx)},test_rows={len(test_idx)},"
                    f"train_races={len(train_races)},test_races={len(test_races)},"
                    f"overlap_sample={';'.join(overlap[:5])}"
                ),
            }
        )

    grouped_overlap_ok = grouped_overlap_count == 0
    summary_rows.append(
        {
            "check": "grouped_kfold_no_race_overlap",
            "status": _status(grouped_overlap_ok),
            "value": 1.0 if grouped_overlap_ok else 0.0,
            "threshold": 1.0,
            "note": f"overlap_folds={grouped_overlap_folds}, overlap_races={grouped_overlap_count}",
        }
    )

    oof = _load_table(oof_path)
    required_oof = {"race", "fold"}
    missing_oof = sorted(required_oof - set(oof.columns))
    if missing_oof:
        raise ValueError(f"oof file missing required columns: {missing_oof}")

    oof["race"] = oof["race"].astype(str)
    oof["fold"] = pd.to_numeric(oof["fold"], errors="coerce")
    if oof["fold"].isna().any():
        raise ValueError("oof fold column contains non-numeric values")
    oof["fold"] = oof["fold"].astype(int)

    # each race should map to exactly one fold in winner OOF outputs.
    race_fold_cardinality = oof.groupby("race", dropna=False)["fold"].nunique()
    multi_fold_races = race_fold_cardinality[race_fold_cardinality > 1]
    multi_fold_race_count = int(len(multi_fold_races))
    oof_uniqueness_ok = multi_fold_race_count == 0

    summary_rows.append(
        {
            "check": "oof_fold_uniqueness_per_race",
            "status": _status(oof_uniqueness_ok),
            "value": 1.0 - _safe_ratio(multi_fold_race_count, int(race_fold_cardinality.shape[0])),
            "threshold": 1.0,
            "note": f"races_in_multiple_folds={multi_fold_race_count}",
        }
    )

    oof_row_match = int(len(oof)) == row_count
    summary_rows.append(
        {
            "check": "oof_row_coverage_vs_dataset",
            "status": _status(oof_row_match),
            "value": _safe_ratio(int(len(oof)), row_count),
            "threshold": 1.0,
            "note": f"oof_rows={len(oof)}, dataset_rows={row_count}",
        }
    )

    dataset_race_set = set(races.unique().tolist())
    oof_race_set = set(oof["race"].unique().tolist())
    missing_in_oof = sorted(dataset_race_set - oof_race_set)
    extra_in_oof = sorted(oof_race_set - dataset_race_set)
    race_universe_ok = len(missing_in_oof) == 0 and len(extra_in_oof) == 0

    summary_rows.append(
        {
            "check": "oof_race_universe_match",
            "status": _status(race_universe_ok),
            "value": 1.0 if race_universe_ok else 0.0,
            "threshold": 1.0,
            "note": (
                f"missing_in_oof={len(missing_in_oof)},extra_in_oof={len(extra_in_oof)},"
                f"sample_missing={';'.join(missing_in_oof[:5])},sample_extra={';'.join(extra_in_oof[:5])}"
            ),
        }
    )

    observed_folds = sorted(oof["fold"].unique().tolist())
    fold_count_ok = len(observed_folds) == args.folds
    summary_rows.append(
        {
            "check": "oof_fold_count_match",
            "status": _status(fold_count_ok),
            "value": _safe_ratio(len(observed_folds), args.folds),
            "threshold": 1.0,
            "note": f"observed_folds={observed_folds}",
        }
    )

    for fold_value, frame in oof.groupby("fold", dropna=False):
        fold_num = pd.to_numeric(fold_value, errors="coerce")
        fold_label = f"fold_{int(fold_num)}" if pd.notna(fold_num) else f"fold_{fold_value}"
        detail_rows.append(
            {
                "section": "oof_distribution",
                "name": fold_label,
                "value": int(len(frame)),
                "note": f"race_count={frame['race'].nunique()}",
            }
        )

    all_checks_pass = all(str(row["status"]).upper() == "PASS" for row in summary_rows)
    summary_rows.append(
        {
            "check": "split_integrity_overall",
            "status": _status(all_checks_pass),
            "value": 1.0 if all_checks_pass else 0.0,
            "threshold": 1.0,
            "note": "all split-integrity checks must pass",
        }
    )

    summary_df = pd.DataFrame(summary_rows)
    details_df = pd.DataFrame(detail_rows)

    summary_output = Path(args.summary_output)
    details_output = Path(args.details_output)
    report_output = Path(args.report_output)
    for output in [summary_output, details_output, report_output]:
        output.parent.mkdir(parents=True, exist_ok=True)

    summary_df.to_csv(summary_output, index=False)
    details_df.to_csv(details_output, index=False)

    lines = [
        "=== SPLIT-INTEGRITY AUDIT REPORT ===",
        f"dataset input          : {dataset_path}",
        f"oof input              : {oof_path}",
        f"rows                   : {row_count}",
        f"unique races           : {unique_races}",
        f"expected folds         : {args.folds}",
        "",
        "check summary",
    ]

    for row in summary_rows:
        lines.append(f"- {row['check']}: {row['status']} ({row['value']})")
        lines.append(f"  note: {row['note']}")

    lines.extend(
        [
            "",
            f"overall gate           : {_status(all_checks_pass)}",
            "",
            "artifacts",
            f"summary csv            : {summary_output}",
            f"details csv            : {details_output}",
            f"report txt             : {report_output}",
        ]
    )

    report_output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
