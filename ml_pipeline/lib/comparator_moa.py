"""Build comparator-fair MOA decision dataset under locked H=2 matching semantics."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

try:
    from .comparator_heuristic import (
        DEFAULT_DATA_LAKE,
        DEFAULT_HORIZON,
        DEFAULT_SEASON_TAG,
        DEFAULT_YEAR,
        _build_comparator_dataset,
        _latest_jsonl,
        _load_jsonl,
        _prepare_pit_evals,
        _print_summary,
    )
    from .moa_predictions import decode_moa_predictions
    from .model_training_cv import _load_dataset, _prepare_matrix
    from ..pipeline_config import default_dataset_path, normalize_years
except ImportError:
    import sys

    _LIB_DIR = Path(__file__).resolve().parent
    _PIPELINE_DIR = _LIB_DIR.parent
    for _path in (_PIPELINE_DIR, _LIB_DIR):
        _path_text = str(_path)
        if _path_text not in sys.path:
            sys.path.insert(0, _path_text)

    from lib.comparator_heuristic import (  # type: ignore
        DEFAULT_DATA_LAKE,
        DEFAULT_HORIZON,
        DEFAULT_SEASON_TAG,
        DEFAULT_YEAR,
        _build_comparator_dataset,
        _latest_jsonl,
        _load_jsonl,
        _prepare_pit_evals,
        _print_summary,
    )
    from lib.moa_predictions import decode_moa_predictions  # type: ignore
    from lib.model_training_cv import _load_dataset, _prepare_matrix  # type: ignore
    from pipeline_config import default_dataset_path, normalize_years  # type: ignore


DEFAULT_YEARS = (2022, 2023, 2024)
DEFAULT_MOA_PREDICTIONS = "data_lake/reports/moa_arf_predictions_2022_2025_merged.pred"
DEFAULT_OUTPUT = "data_lake/reports/moa_comparator_2022_2025_merged.csv"
DEFAULT_OUTPUT_DIAGNOSTICS = "data_lake/reports/moa_comparator_diagnostics_2022_2025_merged.json"
DEFAULT_ACTIONABLE_LABEL = "PIT_NOW"


def _resolve_output_path(data_lake: Path, requested: str) -> Path:
    path = Path(requested)
    if path.is_absolute():
        return path
    if path.parts and path.parts[0] == data_lake.name:
        return data_lake.parent / path
    return data_lake / path


def _prepare_moa_suggestions(
    dataset_path: Path,
    pred_path: Path,
    actionable_label: str,
    min_mapping_purity: float,
) -> tuple[pd.DataFrame, dict[str, object]]:
    df = _load_dataset(dataset_path)
    _, y_true, _, _, meta = _prepare_matrix(df)

    pred_binary, diagnostics = decode_moa_predictions(
        pred_path=pred_path,
        y_true=y_true,
        min_mapping_purity=min_mapping_purity,
    )

    n = min(len(meta), len(pred_binary))
    aligned = meta.iloc[:n].copy().reset_index(drop=True)
    aligned["moa_pred"] = pred_binary.iloc[:n].reset_index(drop=True)

    actionable = aligned[aligned["moa_pred"] == 1].copy()
    if actionable.empty:
        raise ValueError("No actionable MOA decisions were found after decoding")

    suggestions = pd.DataFrame(
        {
            "race": actionable["race"].astype(str),
            "driver": actionable["driver"].astype(str),
            "lapNumber": pd.to_numeric(actionable["lapNumber"], errors="coerce").astype(int),
            "suggestionLabel": actionable_label,
            "totalScore": 1.0,
        }
    )
    diagnostics["actionable_rows"] = int(len(suggestions))
    diagnostics["dataset_rows"] = int(len(meta))
    diagnostics["aligned_rows"] = int(n)
    return suggestions, diagnostics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="build comparator dataset from MOA prequential predictions"
    )
    parser.add_argument("--data-lake", default=DEFAULT_DATA_LAKE, help="data lake directory")
    parser.add_argument("--years", type=int, nargs="+", default=list(DEFAULT_YEARS), help="season years")
    parser.add_argument("--season-tag", default=DEFAULT_SEASON_TAG, help="season tag token")
    parser.add_argument("--dataset", default="", help="prepared dataset path used to align race/driver/lap rows")
    parser.add_argument("--moa-predictions", default=DEFAULT_MOA_PREDICTIONS, help="MOA prediction file path")
    parser.add_argument("--actionable-label", default=DEFAULT_ACTIONABLE_LABEL, help="label for MOA actions")
    parser.add_argument("--min-mapping-purity", type=float, default=0.99, help="minimum purity to accept MOA code map")
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR, help="comparator source year token for pit_evals")
    parser.add_argument("--horizon", type=int, default=DEFAULT_HORIZON, help="look ahead horizon in laps")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="output comparator csv path")
    parser.add_argument("--diagnostics-output", default=DEFAULT_OUTPUT_DIAGNOSTICS, help="output diagnostics json path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    years = normalize_years(args.years)
    data_lake = Path(args.data_lake)

    dataset_path = Path(args.dataset) if args.dataset else default_dataset_path(data_lake, years, args.season_tag)
    pred_path = Path(args.moa_predictions)
    if not dataset_path.exists():
        raise FileNotFoundError(f"dataset not found: {dataset_path}")
    if not pred_path.exists():
        raise FileNotFoundError(f"MOA predictions file not found: {pred_path}")

    suggestions, diagnostics = _prepare_moa_suggestions(
        dataset_path=dataset_path,
        pred_path=pred_path,
        actionable_label=args.actionable_label,
        min_mapping_purity=args.min_mapping_purity,
    )

    pit_evals_path = _latest_jsonl(data_lake, "pit_evals", args.year, args.season_tag)
    pit_evals = _prepare_pit_evals(_load_jsonl(pit_evals_path))
    comparator = _build_comparator_dataset(suggestions, pit_evals, args.horizon)

    output_path = _resolve_output_path(data_lake, args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    comparator.to_csv(output_path, index=False)

    diagnostics_path = _resolve_output_path(data_lake, args.diagnostics_output)
    diagnostics_path.parent.mkdir(parents=True, exist_ok=True)
    diagnostics_payload = {
        "dataset": str(dataset_path),
        "moa_predictions": str(pred_path),
        "pit_evals": str(pit_evals_path),
        "horizon": int(args.horizon),
        "diagnostics": diagnostics,
    }
    diagnostics_path.write_text(
        json.dumps(diagnostics_payload, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )

    print(f"MOA predictions input: {pred_path}")
    print(f"dataset input        : {dataset_path}")
    print(f"pit evals input      : {pit_evals_path}")
    print(f"output csv           : {output_path}")
    print(f"diagnostics json     : {diagnostics_path}")
    _print_summary(comparator, suggestions, args.horizon)


if __name__ == "__main__":
    main()
