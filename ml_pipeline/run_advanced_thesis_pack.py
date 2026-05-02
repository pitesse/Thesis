"""Orchestrate advanced evidence + error-analysis scripts across all run families."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

try:
    from lib.run_catalog import RUN_SET_ALL_FAMILIES, list_runs
except ImportError:
    from ml_pipeline.lib.run_catalog import RUN_SET_ALL_FAMILIES, list_runs  # type: ignore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run advanced thesis artifact pack without retraining")
    parser.add_argument("--reports-root", type=Path, default=Path("data_lake/reports"))
    parser.add_argument("--run-set", default=RUN_SET_ALL_FAMILIES)
    parser.add_argument(
        "--formats",
        nargs="+",
        choices=["pdf", "png"],
        default=["pdf", "png"],
    )
    parser.add_argument("--bootstrap-reps", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--horizon", type=int, default=2)
    parser.add_argument("--sample-size", type=int, default=30)
    parser.add_argument(
        "--update-master-md",
        type=Path,
        default=None,
        help="Optional path to thesis_master_results markdown for section 13/14 auto-update",
    )
    return parser.parse_args()


def _run(cmd: list[str]) -> None:
    print("[RUN]", " ".join(cmd))
    subprocess.run(cmd, check=True)


def main() -> None:
    args = parse_args()
    reports_root = args.reports_root.resolve()
    runs = list_runs(reports_root, args.run_set)
    formats = [str(x) for x in args.formats]

    py = sys.executable

    for run in runs:
        print(f"\n=== RUN {run.run_id.upper()} :: {run.label} ===")

        _run(
            [
                py,
                "ml_pipeline/plot_moa_surrogate_pr_curve.py",
                "--reports-dir",
                str(run.reports_dir),
                "--seed",
                str(args.seed),
                "--formats",
                *formats,
            ]
        )

        _run(
            [
                py,
                "ml_pipeline/plot_advanced_metrics.py",
                "--reports-dir",
                str(run.reports_dir),
                "--suffix",
                run.suffix,
                "--formats",
                *formats,
                "--seed",
                str(args.seed),
            ]
        )

        _run(
            [
                py,
                "ml_pipeline/analyze_error_taxonomy.py",
                "--reports-dir",
                str(run.reports_dir),
                "--suffix",
                run.suffix,
                "--horizon",
                str(args.horizon),
                "--sample-size",
                str(args.sample_size),
                "--seed",
                str(args.seed),
                "--formats",
                *formats,
            ]
        )

    _run(
        [
            py,
            "ml_pipeline/compute_ap_delta_bootstrap.py",
            "--reports-root",
            str(reports_root),
            "--run-set",
            args.run_set,
            "--bootstrap-reps",
            str(args.bootstrap_reps),
            "--seed",
            str(args.seed),
            "--formats",
            *formats,
        ]
    )

    if args.update_master_md is not None:
        _run(
            [
                py,
                "ml_pipeline/update_thesis_master_advanced_sections.py",
                "--reports-root",
                str(reports_root),
                "--master-md",
                str(args.update_master_md),
                "--run-set",
                args.run_set,
            ]
        )

    print("\n=== ADVANCED THESIS PACK COMPLETE ===")
    print(f"reports root : {reports_root}")
    print(f"run count    : {len(runs)}")
    if args.update_master_md is not None:
        print(f"master md    : {args.update_master_md.resolve()}")


if __name__ == "__main__":
    main()
