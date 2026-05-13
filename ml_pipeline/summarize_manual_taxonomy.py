"""Aggregate manually-labeled taxonomy CSV and produce summary chart/table."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize manually-labeled error taxonomy CSV")
    parser.add_argument("--input-csv", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--prefix", default="error_taxonomy_manual")
    parser.add_argument(
        "--formats",
        nargs="+",
        choices=["png", "pdf"],
        default=["png", "pdf"],
    )
    return parser.parse_args()


def _save(fig: plt.Figure, out_dir: Path, stem: str, formats: list[str]) -> list[Path]:
    out: list[Path] = []
    for ext in formats:
        path = out_dir / f"{stem}.{ext}"
        fig.savefig(path, dpi=300, bbox_inches="tight")
        out.append(path)
    plt.close(fig)
    return out


def main() -> None:
    args = parse_args()
    input_csv = args.input_csv
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_csv)
    required = {"model", "failure_type_manual", "case_origin", "subtype_auto"}
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"missing required columns: {missing}")

    work = df.copy()
    work["model"] = work["model"].astype(str)
    work["failure_type_manual"] = work["failure_type_manual"].fillna("UNLABELED").astype(str)
    work["case_origin"] = work["case_origin"].astype(str)
    work["subtype_auto"] = work["subtype_auto"].astype(str)

    summary = (
        work.groupby(["model", "failure_type_manual"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["model", "count", "failure_type_manual"], ascending=[True, False, True])
        .reset_index(drop=True)
    )

    model_totals = work.groupby("model", dropna=False).size().rename("model_total").reset_index()
    summary = summary.merge(model_totals, on="model", how="left")
    summary["within_model_share"] = summary["count"] / summary["model_total"]

    case_origin_summary = (
        work.groupby(["model", "case_origin"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["model", "count"], ascending=[True, False])
        .reset_index(drop=True)
    )

    subtype_summary = (
        work.groupby(["model", "subtype_auto"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["model", "count"], ascending=[True, False])
        .reset_index(drop=True)
    )

    summary_csv = output_dir / f"{args.prefix}_aggregate.csv"
    case_origin_csv = output_dir / f"{args.prefix}_case_origin_summary.csv"
    subtype_csv = output_dir / f"{args.prefix}_subtype_summary.csv"
    summary.to_csv(summary_csv, index=False)
    case_origin_summary.to_csv(case_origin_csv, index=False)
    subtype_summary.to_csv(subtype_csv, index=False)

    # Stacked percentage chart by model.
    pivot = summary.pivot(index="model", columns="failure_type_manual", values="count").fillna(0.0)
    if pivot.empty:
        raise ValueError("no rows to plot")

    row_sums = pivot.sum(axis=1)
    pivot_pct = pivot.div(row_sums.replace(0, np.nan), axis=0).fillna(0.0)

    fig, ax = plt.subplots(figsize=(12, 6))
    bottom = np.zeros(len(pivot_pct), dtype=float)
    colors = plt.get_cmap("tab20")(np.linspace(0, 1, pivot_pct.shape[1]))

    for color, col in zip(colors, pivot_pct.columns, strict=False):
        vals = pivot_pct[col].to_numpy(dtype=float)
        ax.bar(pivot_pct.index, vals, bottom=bottom, label=str(col), color=color, alpha=0.95)
        bottom += vals

    ax.set_ylim(0.0, 1.0)
    ax.set_ylabel("Within-model share")
    ax.set_xlabel("Model")
    ax.set_title("Manual Error Taxonomy Composition by Model")
    ax.grid(True, axis="y", linestyle="--", alpha=0.35)
    ax.legend(loc="center left", bbox_to_anchor=(1.02, 0.5), fontsize=8)
    fig.tight_layout()

    fig_paths = _save(fig, output_dir, f"{args.prefix}_composition", args.formats)

    print("=== MANUAL TAXONOMY SUMMARY GENERATED ===")
    print(f"input csv               : {input_csv}")
    print(f"summary csv             : {summary_csv}")
    print(f"case-origin csv         : {case_origin_csv}")
    print(f"subtype csv             : {subtype_csv}")
    for p in fig_paths:
        print(f"figure                  : {p}")


if __name__ == "__main__":
    main()
