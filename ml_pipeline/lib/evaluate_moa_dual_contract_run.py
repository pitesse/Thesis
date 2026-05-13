"""Evaluate one MOA run under locked dual-contract metrics."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from sklearn.metrics import average_precision_score

try:
    from .comparator_heuristic import (
        OUTCOME_PIT_ANY_H2,
        _build_comparator_dataset,
        _latest_jsonl,
        _load_jsonl,
    )
    from .evaluate_batch_dual_contract_run import (
        _build_metrics_row,
        _extract_year,
        _load_multi_year_stream,
        _load_truth_universe_events_csvs,
        _oof_to_suggestions,
        _target_to_contract,
    )
    from .moa_predictions import decode_moa_predictions_with_scores
    from .model_training_cv import _load_dataset
    from .pit_truth_eligibility import (
        build_pit_truth_universe,
        load_prepared_events_from_csv,
    )
    from ..pipeline_config import comparator_source_year_and_tag, normalize_years
except ImportError:
    import sys

    _LIB_DIR = Path(__file__).resolve().parent
    _PIPELINE_DIR = _LIB_DIR.parent
    for _path in (_PIPELINE_DIR, _LIB_DIR):
        _path_text = str(_path)
        if _path_text not in sys.path:
            sys.path.insert(0, _path_text)

    from comparator_heuristic import (  # type: ignore
        OUTCOME_PIT_ANY_H2,
        _build_comparator_dataset,
        _latest_jsonl,
        _load_jsonl,
    )
    from evaluate_batch_dual_contract_run import (  # type: ignore
        _build_metrics_row,
        _extract_year,
        _load_multi_year_stream,
        _load_truth_universe_events_csvs,
        _oof_to_suggestions,
        _target_to_contract,
    )
    from moa_predictions import decode_moa_predictions_with_scores  # type: ignore
    from model_training_cv import _load_dataset  # type: ignore
    from pit_truth_eligibility import (  # type: ignore
        build_pit_truth_universe,
        load_prepared_events_from_csv,
    )
    from pipeline_config import comparator_source_year_and_tag, normalize_years  # type: ignore


def _extract_meta_and_target(dataset: pd.DataFrame, target_column: str) -> tuple[pd.DataFrame, pd.Series]:
    if target_column not in dataset.columns:
        raise ValueError(f"dataset missing target column: {target_column}")
    required = {"race", "driver", "lapNumber"}
    missing = sorted(required.difference(set(dataset.columns)))
    if missing:
        raise ValueError(f"dataset missing required metadata columns: {missing}")

    work = dataset.copy()
    work[target_column] = pd.to_numeric(work[target_column], errors="coerce")
    work = work[work[target_column].isin([0, 1])].copy()
    work[target_column] = work[target_column].astype(int)

    meta = work[["race", "driver", "lapNumber"]].copy()
    meta["race"] = meta["race"].astype(str)
    meta["driver"] = meta["driver"].astype(str)
    meta["lapNumber"] = pd.to_numeric(meta["lapNumber"], errors="coerce")
    meta = meta[meta["lapNumber"].notna()].copy()
    meta["lapNumber"] = meta["lapNumber"].astype(int)

    y_true = work.loc[meta.index, target_column].astype(int)
    return meta.reset_index(drop=True), y_true.reset_index(drop=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate one MOA run under dual-contract metrics.")
    parser.add_argument("--data-lake", default="data_lake")
    parser.add_argument("--years", type=int, nargs="+", required=True)
    parser.add_argument("--season-tag", default="season")
    parser.add_argument("--dataset", required=True, help="prepared dataset used for MOA export/alignment")
    parser.add_argument("--target-column", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--moa-predictions", required=True)
    parser.add_argument("--pred-column", type=int, default=0)
    parser.add_argument("--true-column", type=int, default=1)
    parser.add_argument("--min-mapping-purity", type=float, default=0.99)
    parser.add_argument("--decision-column", default="constrained_pred")
    parser.add_argument("--score-column", default="calibrated_proba")
    parser.add_argument("--pit-evals-jsonl", default="")
    parser.add_argument("--pit-timings-jsonl", default="")
    parser.add_argument("--ml-features-jsonl", default="")
    parser.add_argument("--prepared-pit-events-csv", default="")
    parser.add_argument(
        "--truth-universe-race-driver-csv",
        default="",
        help="optional csv with columns race,driver to override default OOF race/driver truth universe",
    )
    parser.add_argument(
        "--truth-universe-events-csvs",
        nargs="*",
        default=[],
        help=(
            "optional explicit truth-universe event csv list "
            "(must include eligibility columns); when set, these rows drive denominator logic"
        ),
    )
    parser.add_argument("--output-oof-csv", default="", help="optional aligned MOA pseudo-OOF output")
    parser.add_argument("--output-summary-csv", required=True)
    parser.add_argument("--output-by-year-csv", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_lake = Path(args.data_lake)
    years = normalize_years(args.years)
    comparator_year, comparator_tag = comparator_source_year_and_tag(years, args.season_tag)
    spec = _target_to_contract(args.target_column)

    dataset_path = Path(args.dataset)
    pred_path = Path(args.moa_predictions)
    dataset = _load_dataset(dataset_path)
    meta, y_true = _extract_meta_and_target(dataset, args.target_column)
    pred_binary, score_values, diagnostics = decode_moa_predictions_with_scores(
        pred_path=pred_path,
        y_true=y_true,
        pred_column=int(args.pred_column),
        true_column=int(args.true_column),
        min_mapping_purity=float(args.min_mapping_purity),
    )
    if not (len(meta) == len(y_true) == len(pred_binary) == len(score_values)):
        raise ValueError(
            "aligned MOA evaluation length mismatch: "
            f"meta={len(meta)}, y_true={len(y_true)}, pred={len(pred_binary)}, score={len(score_values)}"
        )
    meta = meta.reset_index(drop=True)
    y_true = y_true.reset_index(drop=True)
    pred = pred_binary.reset_index(drop=True)

    oof = meta.copy()
    oof["target_y"] = y_true.astype(int)
    oof["raw_proba"] = pd.to_numeric(score_values, errors="coerce").fillna(0.0).astype(float)
    # No temporal calibration is applied to MOA stream outputs yet.
    # For now calibrated_proba is an explicit passthrough of raw stream scores.
    oof[args.score_column] = oof["raw_proba"]
    oof["constrained_threshold"] = 0.5
    oof[args.decision_column] = (
        pd.to_numeric(pred, errors="coerce").fillna(0).astype(int).clip(lower=0, upper=1)
    )
    oof["score_mode"] = str(diagnostics.get("score_mode", "unknown"))
    oof["score_is_hard_decision"] = bool(diagnostics.get("score_is_hard_decision", True))
    oof["score_source"] = str(diagnostics.get("score_source", "unknown"))
    oof["score_frontier_quality"] = (
        "hard_decision_only"
        if bool(diagnostics.get("score_is_hard_decision", True))
        else "continuous_score_frontier"
    )
    oof["score_uncalibrated_passthrough"] = True

    rows = int(len(oof))
    positives = int((oof["target_y"] == 1).sum())
    prevalence = float(positives / rows) if rows else 0.0
    ap_raw = float(average_precision_score(oof["target_y"], oof["raw_proba"])) if rows else float("nan")
    ap_cal = float(average_precision_score(oof["target_y"], oof[args.score_column])) if rows else float("nan")
    selected_threshold = 0.5
    score_unique = int(pd.to_numeric(oof[args.score_column], errors="coerce").dropna().nunique())

    suggestions = _oof_to_suggestions(oof, args.decision_column, args.score_column)

    pit_evals_path = Path(args.pit_evals_jsonl) if args.pit_evals_jsonl.strip() else None
    pit_timings_path = Path(args.pit_timings_jsonl) if args.pit_timings_jsonl.strip() else None
    try:
        if pit_evals_path is None:
            pit_evals_path = _latest_jsonl(data_lake, "pit_evals", comparator_year, comparator_tag)
        pit_evals = _load_jsonl(pit_evals_path)
    except FileNotFoundError:
        pit_evals = _load_multi_year_stream(data_lake, "pit_evals", years, args.season_tag)
        pit_evals_path = Path(f"in_memory_merged:pit_evals:{years[0]}_{years[-1]}")

    try:
        if pit_timings_path is None:
            pit_timings_path = _latest_jsonl(data_lake, "pit_timings", comparator_year, comparator_tag)
        pit_timings = _load_jsonl(pit_timings_path)
    except FileNotFoundError:
        pit_timings = _load_multi_year_stream(data_lake, "pit_timings", years, args.season_tag)
        pit_timings_path = Path(f"in_memory_merged:pit_timings:{years[0]}_{years[-1]}")

    ml_features = pd.DataFrame()
    if args.ml_features_jsonl.strip():
        ml_features = _load_jsonl(Path(args.ml_features_jsonl))
    else:
        try:
            ml_features_path = _latest_jsonl(data_lake, "ml_features", comparator_year, comparator_tag)
            ml_features = _load_jsonl(ml_features_path)
        except Exception:
            try:
                ml_features = _load_multi_year_stream(data_lake, "ml_features", years, args.season_tag)
            except Exception:
                ml_features = pd.DataFrame()

    prepared = load_prepared_events_from_csv(args.prepared_pit_events_csv or None)
    truth_universe_source = "moa_oof_race_driver_universe"
    if args.truth_universe_race_driver_csv.strip():
        truth_universe_path = Path(args.truth_universe_race_driver_csv)
        truth_universe_df = pd.read_csv(truth_universe_path)
        required = {"race", "driver"}
        missing = sorted(required.difference(set(truth_universe_df.columns)))
        if missing:
            raise ValueError(
                f"truth universe csv missing required columns {missing}: {truth_universe_path}"
            )
        race_driver_universe = truth_universe_df[["race", "driver"]].copy()
        truth_universe_source = f"explicit_csv:{truth_universe_path}"
    else:
        race_driver_universe = oof[["race", "driver"]].copy()

    race_driver_universe["race"] = race_driver_universe["race"].astype(str)
    race_driver_universe["driver"] = race_driver_universe["driver"].astype(str)
    race_driver_universe = race_driver_universe.dropna(subset=["race", "driver"]).drop_duplicates(
        subset=["race", "driver"], keep="first"
    )

    if args.truth_universe_events_csvs:
        pit_truth_universe = _load_truth_universe_events_csvs(args.truth_universe_events_csvs)
        truth_universe_source = "explicit_truth_events_csvs"
        if args.truth_universe_race_driver_csv.strip():
            allowed = set(
                map(
                    tuple,
                    race_driver_universe[["race", "driver"]].itertuples(index=False, name=None),
                )
            )
            pit_truth_universe["eligible_universe"] = [
                (str(race), str(driver)) in allowed
                for race, driver in pit_truth_universe[["race", "driver"]].itertuples(index=False, name=None)
            ]
        else:
            pit_truth_universe["eligible_universe"] = (
                pit_truth_universe["eligible_universe"].fillna(False).astype(bool)
            )
    else:
        pit_truth_universe = build_pit_truth_universe(
            pit_timings=pit_timings,
            suggestions_source=race_driver_universe,
            pit_evals=pit_evals,
            ml_features=ml_features,
            prepared_pit_events=prepared,
            split_tag=f"moa_{args.profile}_{args.target_column}",
        )

    pit_timings_for_comparator = pit_timings if spec.outcome_mode == OUTCOME_PIT_ANY_H2 else None
    comparator = _build_comparator_dataset(
        suggestions=suggestions,
        pit_evals=pit_evals,
        horizon=2,
        outcome_mode=spec.outcome_mode,
        pit_timings=pit_timings_for_comparator,
        actionable_mode=spec.actionable_mode,
        episode_level=(spec.view == "episode_level"),
        include_same_lap=True,
    )

    summary_row = _build_metrics_row(
        comparator,
        pit_truth_universe,
        spec=spec,
        profile=args.profile,
        target_column=args.target_column,
        ap_raw=ap_raw,
        ap_calibrated=ap_cal,
        rows=rows,
        positives=positives,
        prevalence=prevalence,
        selected_threshold=selected_threshold,
    )
    summary_row["score_mode"] = str(diagnostics.get("score_mode", "unknown"))
    summary_row["score_is_hard_decision"] = bool(diagnostics.get("score_is_hard_decision", True))
    summary_row["score_unique_values"] = int(score_unique)
    summary_row["score_frontier_quality"] = (
        "hard_decision_only"
        if bool(diagnostics.get("score_is_hard_decision", True))
        else "continuous_score_frontier"
    )
    summary_row["score_uncalibrated_passthrough"] = True

    by_year_rows: list[dict[str, object]] = []
    if not comparator.empty:
        comp = comparator.copy()
        comp["year"] = _extract_year(comp)
        truth = pit_truth_universe.copy()
        truth["year"] = pd.to_numeric(truth["year"], errors="coerce")
        oof_year = oof.copy()
        oof_year["year"] = _extract_year(oof_year)

        for year, comp_year in comp.groupby("year", sort=True):
            truth_year = truth[truth["year"] == int(year)].copy()
            oof_slice = oof_year[oof_year["year"] == int(year)].copy()
            y_slice = pd.to_numeric(oof_slice["target_y"], errors="coerce").fillna(0).astype(int)
            rows_y = int(len(y_slice))
            pos_y = int((y_slice == 1).sum())
            prev_y = float(pos_y / rows_y) if rows_y else 0.0
            raw_y = pd.to_numeric(oof_slice.get("raw_proba"), errors="coerce")
            cal_y = pd.to_numeric(oof_slice.get(args.score_column), errors="coerce")
            ap_raw_y = (
                float(average_precision_score(y_slice, raw_y))
                if rows_y and raw_y.notna().any()
                else float("nan")
            )
            ap_cal_y = (
                float(average_precision_score(y_slice, cal_y))
                if rows_y and cal_y.notna().any()
                else float("nan")
            )

            row_y = _build_metrics_row(
                comp_year,
                truth_year,
                spec=spec,
                profile=args.profile,
                target_column=args.target_column,
                ap_raw=ap_raw_y,
                ap_calibrated=ap_cal_y,
                rows=rows_y,
                positives=pos_y,
                prevalence=prev_y,
                selected_threshold=selected_threshold,
            )
            row_y["score_mode"] = str(diagnostics.get("score_mode", "unknown"))
            row_y["score_is_hard_decision"] = bool(diagnostics.get("score_is_hard_decision", True))
            row_y["score_unique_values"] = int(
                pd.to_numeric(oof_slice.get(args.score_column), errors="coerce").dropna().nunique()
            )
            row_y["score_frontier_quality"] = (
                "hard_decision_only"
                if bool(diagnostics.get("score_is_hard_decision", True))
                else "continuous_score_frontier"
            )
            row_y["score_uncalibrated_passthrough"] = True
            row_y["year"] = int(year)
            by_year_rows.append(row_y)

    summary_df = pd.DataFrame([summary_row])
    by_year_df = pd.DataFrame(by_year_rows)

    if args.output_oof_csv.strip():
        out_oof = Path(args.output_oof_csv)
        out_oof.parent.mkdir(parents=True, exist_ok=True)
        oof.to_csv(out_oof, index=False)

    out_summary = Path(args.output_summary_csv)
    out_by_year = Path(args.output_by_year_csv)
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    out_by_year.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(out_summary, index=False)
    by_year_df.to_csv(out_by_year, index=False)

    print("=== MOA DUAL-CONTRACT RUN EVALUATION GENERATED ===")
    print(f"dataset          : {dataset_path}")
    print(f"moa predictions  : {pred_path}")
    print(f"pit_evals input  : {pit_evals_path}")
    print(f"pit_timings input: {pit_timings_path}")
    print(f"profile          : {args.profile}")
    print(f"target           : {args.target_column}")
    print(
        "contract         : "
        f"outcome={spec.outcome_mode}, view={spec.view}, mode={spec.actionable_mode}, lens={spec.truth_lens}"
    )
    print(f"truth universe   : {truth_universe_source}")
    print(f"moa rows aligned : {diagnostics.get('rows_aligned')}")
    print(f"score mode       : {diagnostics.get('score_mode')}")
    print(f"score unique vals: {score_unique}")
    print(f"summary csv      : {out_summary}")
    print(f"by-year csv      : {out_by_year}")
    print(summary_df.to_string(index=False))


if __name__ == "__main__":
    main()
