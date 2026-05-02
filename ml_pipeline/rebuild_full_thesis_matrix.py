"""Full end-to-end thesis matrix orchestrator on clean replay artifacts."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys


def _run(cmd: list[str], *, env: dict[str, str] | None = None) -> None:
    print("[RUN]", " ".join(cmd))
    subprocess.run(cmd, check=True, env=env)


def _ablation_run(
    py: str,
    *,
    data_lake: Path,
    years: list[int],
    tag: str,
    profile: str,
    track_mode: str,
    prepare_data: bool,
    extra_excludes: list[str],
) -> None:
    years_args = [str(year) for year in years]
    suffix = f"{years[0]}_{years[-1]}_no_source_year_{tag}"
    racewise_suffix = f"{years[0]}_{years[-1]}_racewise_no_source_year_{tag}"
    reports_dir = data_lake / "reports" / f"no_source_year_{tag}"
    reports_dir.mkdir(parents=True, exist_ok=True)

    dataset_path = data_lake / f"ml_training_dataset_{suffix}.parquet"
    if not prepare_data:
        dataset_path = data_lake / f"ml_training_dataset_{years[0]}_{years[-1]}_merged.parquet"

    bundle_path = data_lake / "models" / f"pit_strategy_serving_bundle_no_source_year_{tag}.joblib"

    prep_flags = ["--prepare-data", "--dataset", str(dataset_path)] if prepare_data else ["--skip-prepare-data", "--dataset", str(dataset_path)]

    base_cmd = [
        py,
        "ml_pipeline/train_model.py",
        "--data-lake",
        str(data_lake),
        "--years",
        *years_args,
        "--season-tag",
        "season",
        *prep_flags,
        "--drop-source-year-feature",
        "--feature-profile",
        profile,
        "--track-agnostic-mode",
        track_mode,
    ]
    if extra_excludes:
        base_cmd.extend(["--exclude-features", *extra_excludes])

    _run(
        [
            *base_cmd,
            "--split-protocol",
            "expanding_race",
            "--leaderboard-output",
            str(reports_dir / f"ml_ablation_phase31c_{suffix}.csv"),
            "--oof-output",
            str(reports_dir / f"ml_oof_winner_{suffix}.csv"),
            "--serving-bundle-output",
            str(bundle_path),
        ]
    )

    _run(
        [
            *base_cmd,
            "--split-protocol",
            "expanding_race_sequential",
            "--skip-prepare-data",
            "--dataset",
            str(dataset_path),
            "--leaderboard-output",
            str(reports_dir / f"ml_ablation_phase31c_{racewise_suffix}.csv"),
            "--oof-output",
            str(reports_dir / f"ml_oof_winner_{racewise_suffix}.csv"),
            "--skip-serving-bundle",
        ]
    )

    eval_base = [
        py,
        "ml_pipeline/evaluate_model.py",
        "--data-lake",
        str(data_lake),
        "--reports-dir",
        str(reports_dir),
        "--years",
        *years_args,
        "--season-tag",
        "season",
        "--dataset",
        str(dataset_path),
        "--bundle",
        str(bundle_path),
        "--drop-source-year-feature",
        "--feature-profile",
        profile,
        "--track-agnostic-mode",
        track_mode,
    ]
    if extra_excludes:
        eval_base.extend(["--exclude-features", *extra_excludes])

    _run(
        [
            *eval_base,
            "--artifact-suffix",
            suffix,
            "--oof-input",
            str(reports_dir / f"ml_oof_winner_{suffix}.csv"),
            "--ablation-input",
            str(reports_dir / f"ml_ablation_phase31c_{suffix}.csv"),
        ]
    )
    _run(
        [
            *eval_base,
            "--artifact-suffix",
            racewise_suffix,
            "--oof-input",
            str(reports_dir / f"ml_oof_winner_{racewise_suffix}.csv"),
            "--ablation-input",
            str(reports_dir / f"ml_ablation_phase31c_{racewise_suffix}.csv"),
        ]
    )

    _run(
        [
            py,
            "ml_pipeline/plot_discrimination_curves.py",
            "--reports-dir",
            str(reports_dir),
            "--merged-oof",
            str(reports_dir / f"ml_oof_winner_{suffix}.csv"),
            "--racewise-oof",
            str(reports_dir / f"ml_oof_winner_{racewise_suffix}.csv"),
            "--suffix",
            suffix,
            "--formats",
            "pdf",
            "png",
        ]
    )

    moa_csv = reports_dir / f"moa_dataset_{suffix}.csv"
    moa_arff = reports_dir / f"moa_dataset_{suffix}.arff"
    moa_schema = reports_dir / f"moa_dataset_{suffix}.json"

    export_cmd = [
        py,
        "ml_pipeline/export_moa_dataset.py",
        "--data-lake",
        str(data_lake),
        "--years",
        *years_args,
        "--season-tag",
        "season",
        "--skip-prepare-data",
        "--dataset",
        str(dataset_path),
        "--drop-source-year-feature",
        "--feature-profile",
        profile,
        "--track-agnostic-mode",
        track_mode,
        "--output-csv",
        str(moa_csv),
        "--output-arff",
        str(moa_arff),
        "--schema-output",
        str(moa_schema),
    ]
    if extra_excludes:
        export_cmd.extend(["--exclude-features", *extra_excludes])
    _run(export_cmd)

    moa_pred = reports_dir / f"moa_arf_predictions_{suffix}.pred"
    moa_summary = reports_dir / f"moa_arf_summary_{suffix}.csv"

    _run(
        [
            py,
            "ml_pipeline/run_moa_arf.py",
            "--data-lake",
            str(data_lake),
            "--years",
            *years_args,
            "--season-tag",
            "season",
            "--input-arff",
            str(moa_arff),
            "--learning-curve-output",
            str(reports_dir / f"moa_arf_learning_curve_{suffix}.csv"),
            "--summary-output",
            str(moa_summary),
            "--stdout-output",
            str(reports_dir / f"moa_arf_stdout_{suffix}.txt"),
            "--stderr-output",
            str(reports_dir / f"moa_arf_stderr_{suffix}.txt"),
            "--predictions-output",
            str(moa_pred),
            "--metadata-output",
            str(reports_dir / f"moa_arf_run_{suffix}.json"),
        ]
    )

    _run(
        [
            py,
            "-m",
            "ml_pipeline.lib.comparator_moa",
            "--data-lake",
            str(data_lake),
            "--years",
            *years_args,
            "--season-tag",
            "merged",
            "--dataset",
            str(dataset_path),
            "--moa-predictions",
            str(moa_pred),
            "--year",
            "9999",
            "--output",
            str(reports_dir / f"moa_comparator_{suffix}.csv"),
            "--diagnostics-output",
            str(reports_dir / f"moa_comparator_diagnostics_{suffix}.json"),
        ]
    )

    _run(
        [
            py,
            "ml_pipeline/build_three_way_comparator.py",
            "--data-lake",
            str(data_lake),
            "--years",
            *years_args,
            "--season-tag",
            "season",
            "--sde-comparator",
            str(data_lake / "reports" / "heuristic_comparator_2022_2025_merged.csv"),
            "--ml-comparator",
            str(reports_dir / f"ml_comparator_{suffix}.csv"),
            "--moa-comparator",
            str(reports_dir / f"moa_comparator_{suffix}.csv"),
            "--moa-summary",
            str(moa_summary),
            "--output-csv",
            str(reports_dir / f"three_way_comparator_{suffix}.csv"),
            "--output-md",
            str(reports_dir / f"three_way_comparator_{suffix}.md"),
        ]
    )

    _run(
        [
            py,
            "ml_pipeline/explain_shap.py",
            "--dataset",
            str(dataset_path),
            "--model",
            str(bundle_path),
            "--reports-dir",
            str(reports_dir),
        ]
    )
    _run(
        [
            py,
            "ml_pipeline/explain_moa_shap_proxy.py",
            "--data-lake",
            str(data_lake),
            "--years",
            *years_args,
            "--season-tag",
            "season",
            "--moa-dataset-csv",
            str(moa_csv),
            "--moa-predictions",
            str(moa_pred),
            "--reports-dir",
            str(reports_dir),
        ]
    )
    _run(
        [
            py,
            "ml_pipeline/explain_moa_temporal_permutation.py",
            "--data-lake",
            str(data_lake),
            "--years",
            *years_args,
            "--season-tag",
            "season",
            "--moa-dataset-csv",
            str(moa_csv),
            "--moa-predictions",
            str(moa_pred),
            "--moa-shap-summary-json",
            str(reports_dir / "moa_shap_proxy_summary.json"),
            "--reports-dir",
            str(reports_dir),
        ]
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="rebuild full thesis matrix from clean replay outputs")
    parser.add_argument("--data-lake", default="data_lake")
    parser.add_argument("--years", type=int, nargs="+", default=[2022, 2023, 2024, 2025])
    parser.add_argument(
        "--skip-advanced-pack",
        action="store_true",
        help="skip advanced artifact pack generation",
    )
    parser.add_argument(
        "--skip-master-refresh",
        action="store_true",
        help="skip thesis markdown synthesis refresh",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_lake = Path(args.data_lake)
    years = [int(year) for year in args.years]
    py = sys.executable

    env = dict(os.environ)
    env.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")

    # Baseline pretrain + racewise
    _run(
        [
            py,
            "ml_pipeline/train_model.py",
            "--data-lake",
            str(data_lake),
            "--years",
            *[str(year) for year in years],
            "--season-tag",
            "season",
            "--prepare-data",
            "--dataset",
            str(data_lake / "ml_training_dataset_2022_2025_merged.parquet"),
            "--split-protocol",
            "expanding_race",
            "--leaderboard-output",
            str(data_lake / "reports" / "ml_ablation_phase31c_2022_2025_merged.csv"),
            "--oof-output",
            str(data_lake / "reports" / "ml_oof_winner_2022_2025_merged.csv"),
            "--serving-bundle-output",
            str(data_lake / "models" / "pit_strategy_serving_bundle.joblib"),
        ],
        env=env,
    )
    _run(
        [
            py,
            "ml_pipeline/train_model.py",
            "--data-lake",
            str(data_lake),
            "--years",
            *[str(year) for year in years],
            "--season-tag",
            "season",
            "--skip-prepare-data",
            "--dataset",
            str(data_lake / "ml_training_dataset_2022_2025_merged.parquet"),
            "--split-protocol",
            "expanding_race_sequential",
            "--leaderboard-output",
            str(data_lake / "reports" / "ml_ablation_phase31c_2022_2025_racewise.csv"),
            "--oof-output",
            str(data_lake / "reports" / "ml_oof_winner_2022_2025_racewise.csv"),
            "--skip-serving-bundle",
        ],
        env=env,
    )

    _run(
        [
            py,
            "ml_pipeline/evaluate_model.py",
            "--data-lake",
            str(data_lake),
            "--years",
            *[str(year) for year in years],
            "--season-tag",
            "season",
            "--artifact-suffix",
            "2022_2025_merged",
            "--dataset",
            str(data_lake / "ml_training_dataset_2022_2025_merged.parquet"),
            "--oof-input",
            str(data_lake / "reports" / "ml_oof_winner_2022_2025_merged.csv"),
            "--ablation-input",
            str(data_lake / "reports" / "ml_ablation_phase31c_2022_2025_merged.csv"),
            "--bundle",
            str(data_lake / "models" / "pit_strategy_serving_bundle.joblib"),
        ],
        env=env,
    )
    _run(
        [
            py,
            "ml_pipeline/evaluate_model.py",
            "--data-lake",
            str(data_lake),
            "--years",
            *[str(year) for year in years],
            "--season-tag",
            "season",
            "--artifact-suffix",
            "2022_2025_racewise",
            "--dataset",
            str(data_lake / "ml_training_dataset_2022_2025_merged.parquet"),
            "--oof-input",
            str(data_lake / "reports" / "ml_oof_winner_2022_2025_racewise.csv"),
            "--ablation-input",
            str(data_lake / "reports" / "ml_ablation_phase31c_2022_2025_racewise.csv"),
            "--bundle",
            str(data_lake / "models" / "pit_strategy_serving_bundle.joblib"),
        ],
        env=env,
    )

    # E-family + P-family
    _ablation_run(py, data_lake=data_lake, years=years, tag="baseline", profile="baseline", track_mode="off", prepare_data=False, extra_excludes=[])
    _ablation_run(py, data_lake=data_lake, years=years, tag="drop_medium_v1", profile="drop_medium_v1", track_mode="off", prepare_data=False, extra_excludes=[])
    _ablation_run(py, data_lake=data_lake, years=years, tag="drop_medium_track_agnostic_v1", profile="track_agnostic_v1", track_mode="track_agnostic_v1", prepare_data=True, extra_excludes=[])
    _ablation_run(py, data_lake=data_lake, years=years, tag="track_agnostic_v1_strict", profile="track_agnostic_v1", track_mode="track_agnostic_v1", prepare_data=True, extra_excludes=["trackTemp", "airTemp", "humidity", "speedTrap", "lapTime"])
    _ablation_run(py, data_lake=data_lake, years=years, tag="drop_aggressive_v1_candidate", profile="drop_aggressive_v1_candidate", track_mode="off", prepare_data=False, extra_excludes=[])

    _ablation_run(py, data_lake=data_lake, years=years, tag="percent_conservative_v1", profile="percent_conservative_v1", track_mode="track_percentage_v1", prepare_data=True, extra_excludes=[])
    _ablation_run(py, data_lake=data_lake, years=years, tag="percent_team_v1", profile="percent_team_v1", track_mode="track_percentage_team_v1", prepare_data=True, extra_excludes=[])
    _ablation_run(py, data_lake=data_lake, years=years, tag="percent_race_team_v1", profile="percent_race_team_v1", track_mode="track_percentage_race_team_v1", prepare_data=True, extra_excludes=[])
    _ablation_run(py, data_lake=data_lake, years=years, tag="percent_race_team_v1_strict", profile="percent_race_team_v1_strict", track_mode="track_percentage_race_team_v1", prepare_data=True, extra_excludes=["lapTime"])
    _ablation_run(py, data_lake=data_lake, years=years, tag="percent_race_team_aggressive_v1", profile="percent_race_team_aggressive_v1", track_mode="track_percentage_race_team_v1", prepare_data=True, extra_excludes=[])

    if not args.skip_advanced_pack:
        _run(
            [
                py,
                "ml_pipeline/run_advanced_thesis_pack.py",
                "--reports-root",
                str(data_lake / "reports"),
                "--run-set",
                "all_families",
                "--formats",
                "pdf",
                "png",
                "--bootstrap-reps",
                "2000",
                "--seed",
                "42",
                "--update-master-md",
                str(data_lake / "reports" / "thesis_master_results_2022_2025.md"),
            ],
            env=env,
        )

    if not args.skip_master_refresh:
        _run(
            [
                py,
                "ml_pipeline/build_thesis_synthesis.py",
                "--suffix",
                "2022_2025_merged",
            ],
            env=env,
        )
        _run(
            [
                py,
                "ml_pipeline/generate_thesis_master_results.py",
                "--reports-dir",
                str(data_lake / "reports"),
                "--output-md",
                str(data_lake / "reports" / "thesis_master_results_2022_2025.md"),
            ],
            env=env,
        )


if __name__ == "__main__":
    main()
