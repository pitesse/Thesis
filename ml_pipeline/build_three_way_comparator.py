"""Build a compact comparison artifact: SDE vs batch ML vs MOA decision and stream baselines."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline_config import (
    DEFAULT_DATA_LAKE,
    DEFAULT_SEASON_TAG,
    DEFAULT_YEARS,
    default_report_csv,
    default_report_md,
    normalize_years,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="generate compact 3-way comparison artifacts for meeting-ready reporting"
    )
    parser.add_argument("--data-lake", default=DEFAULT_DATA_LAKE)
    parser.add_argument("--years", type=int, nargs="+", default=list(DEFAULT_YEARS))
    parser.add_argument("--season-tag", default=DEFAULT_SEASON_TAG)

    parser.add_argument("--sde-comparator", default="")
    parser.add_argument("--ml-comparator", default="")
    parser.add_argument("--moa-comparator", default="")
    parser.add_argument("--moa-summary", default="")

    parser.add_argument("--output-csv", default="")
    parser.add_argument("--output-md", default="")
    return parser.parse_args()


def _safe_float(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float("nan")
    if pd.isna(number):
        return float("nan")
    return number


def _compute_comparator_metrics(path: Path, paradigm: str) -> dict[str, Any]:
    frame = pd.read_csv(path)
    if frame.empty:
        raise ValueError(f"empty comparator csv: {path}")

    required = {"outcome_class"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"comparator missing columns {missing}: {path}")

    scored = frame[frame["outcome_class"].isin(["1", "0", 1, 0])].copy()
    actionable = int(len(frame))
    scored_n = int(len(scored))
    excluded_n = int(actionable - scored_n)
    tp = int((scored["outcome_class"].astype(str) == "1").sum())
    fp = int((scored["outcome_class"].astype(str) == "0").sum())
    precision = (tp / scored_n) if scored_n > 0 else float("nan")

    return {
        "paradigm": paradigm,
        "comparison_mode": "full_h2_actionable_one_to_one",
        "actionable": actionable,
        "scored": scored_n,
        "excluded": excluded_n,
        "tp": tp,
        "fp": fp,
        "precision": precision,
        "eval_instances": float("nan"),
        "accuracy_pct": float("nan"),
        "kappa_pct": float("nan"),
        "source_artifact": str(path),
        "notes": "direct comparator metrics under fixed H=2 contract",
    }


def _compute_moa_metrics(path: Path) -> dict[str, Any]:
    frame = pd.read_csv(path)
    if frame.empty:
        raise ValueError(f"empty MOA summary csv: {path}")

    row = frame.iloc[0]
    eval_instances = _safe_float(row.get("final_learning evaluation instances"))
    accuracy_pct = _safe_float(row.get("final_classifications correct (percent)"))
    kappa_pct = _safe_float(row.get("final_Kappa Statistic (percent)"))

    return {
        "paradigm": "MOA_ARF",
        "comparison_mode": "prequential_stream_baseline",
        "actionable": pd.NA,
        "scored": pd.NA,
        "excluded": pd.NA,
        "tp": pd.NA,
        "fp": pd.NA,
        "precision": pd.NA,
        "eval_instances": eval_instances,
        "accuracy_pct": accuracy_pct,
        "kappa_pct": kappa_pct,
        "source_artifact": str(path),
        "notes": (
            "prequential stream metrics, not yet mapped to comparator semantics; "
            "use as baseline until MOA decision mapping is added"
        ),
    }


def _compute_moa_decision_metrics(path: Path) -> dict[str, Any]:
    row = _compute_comparator_metrics(path, paradigm="MOA_ARF_DECISION")
    row["notes"] = "direct comparator metrics under fixed H=2 contract from decoded MOA decisions"
    return row


def _fmt(value: Any, digits: int = 6) -> str:
    number = _safe_float(value)
    if pd.isna(number):
        return "N/A"
    return f"{number:.{digits}f}"


def _write_markdown(output: Path, table: pd.DataFrame, years: list[int], season_tag: str) -> None:
    lines: list[str] = []
    lines.append("# 3-way Compact Comparison")
    lines.append("")
    lines.append(f"Years: {years}")
    lines.append(f"Season tag: {season_tag}")
    lines.append("")
    lines.append("| Paradigm | Mode | Actionable | Scored | Excluded | TP | FP | Precision | Eval instances | Accuracy % | Kappa % |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")

    for _, row in table.iterrows():
        lines.append(
            "| {paradigm} | {mode} | {actionable} | {scored} | {excluded} | {tp} | {fp} | {precision} | {eval_instances} | {accuracy_pct} | {kappa_pct} |".format(
                paradigm=row["paradigm"],
                mode=row["comparison_mode"],
                actionable="N/A" if pd.isna(row["actionable"]) else int(row["actionable"]),
                scored="N/A" if pd.isna(row["scored"]) else int(row["scored"]),
                excluded="N/A" if pd.isna(row["excluded"]) else int(row["excluded"]),
                tp="N/A" if pd.isna(row["tp"]) else int(row["tp"]),
                fp="N/A" if pd.isna(row["fp"]) else int(row["fp"]),
                precision=_fmt(row["precision"]),
                eval_instances=_fmt(row["eval_instances"], digits=0),
                accuracy_pct=_fmt(row["accuracy_pct"]),
                kappa_pct=_fmt(row["kappa_pct"]),
            )
        )

    lines.append("")
    lines.append("## Caveats")
    lines.append("- SDE, batch ML, and MOA decision rows are directly comparable under the fixed H=2 comparator contract.")
    lines.append("- MOA prequential row remains useful as a stream quality baseline but is not itself a decision-level comparator metric.")
    lines.append("- SHAP for MOA is surrogate-based and must be interpreted as behavior approximation, not MOA internal attribution.")

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    years = normalize_years(args.years)
    data_lake = Path(args.data_lake)

    sde_comparator = (
        Path(args.sde_comparator)
        if args.sde_comparator
        else default_report_csv(data_lake, "heuristic_comparator", years, args.season_tag)
    )
    ml_comparator = (
        Path(args.ml_comparator)
        if args.ml_comparator
        else default_report_csv(data_lake, "ml_comparator", years, args.season_tag)
    )
    moa_comparator = (
        Path(args.moa_comparator)
        if args.moa_comparator
        else default_report_csv(data_lake, "moa_comparator", years, args.season_tag)
    )
    moa_summary = (
        Path(args.moa_summary)
        if args.moa_summary
        else default_report_csv(data_lake, "moa_arf_summary", years, args.season_tag)
    )

    output_csv = (
        Path(args.output_csv)
        if args.output_csv
        else default_report_csv(data_lake, "three_way_comparator", years, args.season_tag)
    )
    output_md = (
        Path(args.output_md)
        if args.output_md
        else default_report_md(data_lake, "three_way_comparator", years, args.season_tag)
    )

    for path in [sde_comparator, ml_comparator, moa_summary]:
        if not path.exists():
            raise FileNotFoundError(f"required input not found: {path}")

    rows: list[dict[str, Any]] = [
        _compute_comparator_metrics(sde_comparator, paradigm="SDE"),
        _compute_comparator_metrics(ml_comparator, paradigm="ML_BATCH"),
    ]
    if moa_comparator.exists():
        rows.append(_compute_moa_decision_metrics(moa_comparator))
    rows.append(_compute_moa_metrics(moa_summary))
    table = pd.DataFrame(rows)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    table.to_csv(output_csv, index=False)
    _write_markdown(output_md, table, years, args.season_tag)

    print("=== THREE WAY COMPARATOR SUMMARY ===")
    print(f"sde comparator : {sde_comparator}")
    print(f"ml comparator  : {ml_comparator}")
    if moa_comparator.exists():
        print(f"moa comparator : {moa_comparator}")
    print(f"moa summary    : {moa_summary}")
    print(f"output csv     : {output_csv}")
    print(f"output md      : {output_md}")


if __name__ == "__main__":
    main()
