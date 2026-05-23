#!/usr/bin/env python3
"""Print final thesis results from final_refresh package in terminal."""

from __future__ import annotations

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
FINAL_REFRESH = ROOT / "data_lake/reports/phase2b_presentation_figures/final_refresh"

FILES = {
    "pit_any_final_recap": FINAL_REFRESH / "pit_any_final_recap.csv",
    "pit_success_apples_to_apples": FINAL_REFRESH / "pit_success_apples_to_apples.csv",
    "pit_success_sde_diagnostic": FINAL_REFRESH / "pit_success_sde_diagnostic.csv",
    "pit_success_threshold_sensitivity": FINAL_REFRESH / "pit_success_threshold_sensitivity.csv",
    "training_runtime": FINAL_REFRESH / "training_runtime_slide_table.csv",
    "provenance": FINAL_REFRESH / "final_refresh_input_provenance.csv",
}


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return pd.read_csv(path)


def _print_section(title: str, df: pd.DataFrame, cols: list[str] | None = None) -> None:
    print(f"\n=== {title} ===")
    view = df.copy()
    if cols is not None:
        existing = [c for c in cols if c in view.columns]
        view = view[existing]
    with pd.option_context("display.max_colwidth", 120):
        print(view.to_string(index=False))


def main() -> None:
    print("Final thesis result summary")
    print(f"Source folder: {FINAL_REFRESH}")

    frames = {k: _load_csv(v) for k, v in FILES.items()}

    _print_section(
        "pit_any_h2 final recap",
        frames["pit_any_final_recap"],
        ["System", "selected_threshold", "precision", "event_recall", "F0.5", "AP", "notes"],
    )

    _print_section(
        "pit_success_h2 apples-to-apples (strict operational)",
        frames["pit_success_apples_to_apples"],
        [
            "System",
            "positive_call_definition",
            "predicted_positives",
            "TP",
            "FP_no_match",
            "FP_failure",
            "strict_precision",
            "successful_event_coverage",
            "F0.5",
            "notes",
        ],
    )

    _print_section(
        "Flink Strategy Engine diagnostic (matched-pit success rate)",
        frames["pit_success_sde_diagnostic"],
        [
            "System",
            "positive_call_definition",
            "predicted_positives",
            "matched_known_pits",
            "matched_success",
            "matched_failure",
            "matched_pit_success_rate",
            "strict_precision",
            "notes",
        ],
    )

    _print_section(
        "pit_success_h2 threshold sensitivity",
        frames["pit_success_threshold_sensitivity"],
        ["System", "threshold/policy", "predicted_positives", "strict_precision", "successful_event_coverage", "F0.5", "notes"],
    )

    _print_section(
        "Training runtimes",
        frames["training_runtime"],
        ["learner", "profile", "target", "timed_stage", "wall_seconds", "wall_hms", "status", "command_name"],
    )

    prov = frames["provenance"]
    _print_section(
        "Authoritative source paths",
        prov,
        ["artifact_role", "system", "profile", "target", "path", "exists", "row_count_if_applicable", "class0_if_applicable", "class1_if_applicable", "stale_warning"],
    )

    print("\nDone.")


if __name__ == "__main__":
    main()
