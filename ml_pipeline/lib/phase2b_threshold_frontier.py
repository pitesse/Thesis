"""Phase 2B threshold/frontier sweep on existing Batch OOF runs.

This script does not retrain models. It re-evaluates existing OOF predictions
across thresholds under dual-contract metrics and configurable truth universes.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score

try:
    from .comparator_heuristic import (
        ACTIONABLE_MODE_PIT_NOW_ONLY,
        ACTIONABLE_MODE_PIT_NOW_PLUS_GOOD_PIT,
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
        _target_to_contract,
    )
    from .pit_truth_eligibility import build_pit_truth_universe, load_prepared_events_from_csv
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
        ACTIONABLE_MODE_PIT_NOW_ONLY,
        ACTIONABLE_MODE_PIT_NOW_PLUS_GOOD_PIT,
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
        _target_to_contract,
    )
    from pit_truth_eligibility import build_pit_truth_universe, load_prepared_events_from_csv  # type: ignore
    from pipeline_config import comparator_source_year_and_tag, normalize_years  # type: ignore


def _parse_thresholds(raw: str, min_thr: float, max_thr: float, step: float) -> list[float]:
    text = (raw or "").strip()
    if text:
        values = []
        for item in text.split(","):
            item = item.strip()
            if not item:
                continue
            values.append(float(item))
        uniq = sorted({round(v, 6) for v in values})
        if not uniq:
            raise ValueError("parsed empty --thresholds-csv")
        return uniq
    if step <= 0:
        raise ValueError("--threshold-step must be > 0")
    if max_thr <= min_thr:
        raise ValueError("--threshold-max must be > --threshold-min")
    count = int(np.floor((max_thr - min_thr) / step)) + 1
    vals = [min_thr + i * step for i in range(count)]
    vals = [v for v in vals if v <= max_thr + 1e-9]
    return [round(float(v), 6) for v in vals]


def _discover_runs(oof_dir: Path, run_ids: list[str]) -> list[tuple[str, Path]]:
    if run_ids:
        out: list[tuple[str, Path]] = []
        for run_id in run_ids:
            path = oof_dir / f"{run_id}.csv"
            if not path.exists():
                raise FileNotFoundError(f"OOF csv not found for run_id={run_id}: {path}")
            out.append((run_id, path))
        return out

    paths = sorted(oof_dir.glob("*.csv"))
    out = []
    for path in paths:
        run_id = path.stem
        if "__target_" not in run_id:
            continue
        out.append((run_id, path))
    if not out:
        raise FileNotFoundError(f"no OOF run csvs found in {oof_dir}")
    return out


def _run_id_parts(run_id: str, oof: pd.DataFrame) -> tuple[str, str]:
    if "__" in run_id:
        profile, target = run_id.split("__", 1)
        return profile, target
    profile = str(oof.get("feature_profile", pd.Series(["unknown"])).iloc[0])
    target = str(oof.get("target_column", pd.Series(["target_y"])).iloc[0])
    return profile, target


def _to_suggestions(
    oof: pd.DataFrame,
    score_column: str,
    threshold: float,
) -> pd.DataFrame:
    required = {"race", "driver", "lapNumber", score_column}
    missing = sorted(required.difference(set(oof.columns)))
    if missing:
        raise ValueError(f"oof csv missing required columns: {missing}")

    work = oof.copy()
    work["race"] = work["race"].astype(str)
    work["driver"] = work["driver"].astype(str)
    work["lapNumber"] = pd.to_numeric(work["lapNumber"], errors="coerce")
    score = pd.to_numeric(work[score_column], errors="coerce")
    keep = work["lapNumber"].notna() & score.notna() & (score >= float(threshold))
    out = work.loc[keep, ["race", "driver", "lapNumber"]].copy()
    out["lapNumber"] = out["lapNumber"].astype(int)
    out["suggestionLabel"] = "PIT_NOW"
    out["totalScore"] = score.loc[keep].astype(float).values
    out["trackStatus"] = ""
    return out


def _safe_ap(y_true: pd.Series, y_score: pd.Series) -> float:
    if len(y_true) == 0:
        return float("nan")
    if y_score.notna().sum() == 0:
        return float("nan")
    return float(average_precision_score(y_true, y_score))


def _choose_recommended(
    frame: pd.DataFrame,
    *,
    min_scored_pit_any: int,
    min_scored_pit_success: int,
) -> pd.Series:
    work = frame.copy()
    outcome_mode = str(work["outcome_mode"].iloc[0])
    if outcome_mode == OUTCOME_PIT_ANY_H2:
        subset = work[work["scored"] >= int(min_scored_pit_any)].copy()
        if subset.empty:
            subset = work
        subset = subset.sort_values(
            by=["f0_5", "recall", "precision", "scored", "selected_threshold"],
            ascending=[False, False, False, False, True],
            kind="mergesort",
        )
        winner = subset.iloc[0].copy()
        winner["selection_rule"] = f"pit_any_max_f0_5_scored>={int(min_scored_pit_any)}"
        return winner

    subset = work[work["scored"] >= int(min_scored_pit_success)].copy()
    if subset.empty:
        subset = work
    subset = subset.sort_values(
        by=["precision", "f0_5", "recall", "scored", "selected_threshold"],
        ascending=[False, False, False, False, True],
        kind="mergesort",
    )
    winner = subset.iloc[0].copy()
    winner["selection_rule"] = f"pit_success_max_precision_scored>={int(min_scored_pit_success)}"
    return winner


def _render_md(
    recommended: pd.DataFrame,
    *,
    universe_mode_label: str,
) -> str:
    lines: list[str] = []
    lines.append("# Phase 2B Threshold Frontier")
    lines.append("")
    lines.append(f"- Universe mode: `{universe_mode_label}`")
    lines.append("- Comparator contracts:")
    lines.append("  - `pit_any_h2`: episode_level + pit_now_only + H=2")
    lines.append("  - `pit_success_h2`: row_level + pit_now_only + H=2 (strict future default)")
    lines.append("")
    lines.append("## Recommended Operating Points")
    lines.append(
        "| run_id | outcome_mode | truth_lens | threshold | row_tp | tp_for_recall | fp | fn | scored | precision | recall | F0.5 | selection_rule |"
    )
    lines.append("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |")
    for _, row in recommended.iterrows():
        lines.append(
            "| {run} | {outcome} | {lens} | {thr:.4f} | {row_tp} | {tp_recall} | {fp} | {fn} | {scored} | {p:.6f} | {r:.6f} | {f05:.6f} | {rule} |".format(
                run=str(row["run_id"]),
                outcome=str(row["outcome_mode"]),
                lens=str(row["truth_lens"]),
                thr=float(row["selected_threshold"]),
                row_tp=int(row["row_tp"]),
                tp_recall=int(row["tp_for_recall"]),
                fp=int(row["fp"]),
                fn=int(row["fn"]),
                scored=int(row["scored"]),
                p=float(row["precision"]),
                r=float(row["recall"]),
                f05=float(row["f0_5"]),
                rule=str(row.get("selection_rule", "")),
            )
        )
    lines.append("")
    if "score_is_hard_decision" in recommended.columns and bool(
        recommended["score_is_hard_decision"].fillna(False).astype(bool).any()
    ):
        lines.append(
            "Warning: at least one run uses hard-decision-only scores (<=2 unique values). "
            "Threshold frontier for those runs is diagnostic only."
        )
        lines.append("")
    lines.append("Notes: `row_tp` is row-level TP for precision; `tp_for_recall` is event-level successful-pit coverage for pit_success_h2.")
    return "\n".join(lines).strip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase 2B threshold/frontier sweep on existing Batch OOF runs (no retraining)."
    )
    parser.add_argument("--data-lake", default="data_lake")
    parser.add_argument("--years", type=int, nargs="+", required=True)
    parser.add_argument("--season-tag", default="season")
    parser.add_argument("--oof-dir", required=True)
    parser.add_argument("--run-ids", nargs="*", default=[], help="optional explicit run_id list")
    parser.add_argument("--score-column", default="calibrated_proba")
    parser.add_argument("--thresholds-csv", default="", help="optional explicit thresholds csv")
    parser.add_argument("--threshold-min", type=float, default=0.05)
    parser.add_argument("--threshold-max", type=float, default=0.95)
    parser.add_argument("--threshold-step", type=float, default=0.01)

    parser.add_argument("--pit-evals-jsonl", default="")
    parser.add_argument("--pit-timings-jsonl", default="")
    parser.add_argument("--ml-features-jsonl", default="")
    parser.add_argument("--prepared-pit-events-csv", default="")
    parser.add_argument("--truth-universe-race-driver-csv", default="")
    parser.add_argument("--truth-universe-events-csvs", nargs="*", default=[])
    parser.add_argument(
        "--truth-universe-mode-label",
        default="native_universe",
        help="label written into outputs, e.g. native_universe or shared_sde_ml_universe",
    )

    parser.add_argument("--min-scored-pit-any", type=int, default=60)
    parser.add_argument("--min-scored-pit-success", type=int, default=40)
    parser.add_argument(
        "--pit-success-include-same-lap",
        action="store_true",
        help=(
            "when set, pit_success_h2 comparator matching uses [k,k+H]; "
            "default is strict-future [k+1,k+H]"
        ),
    )
    parser.add_argument(
        "--pit-success-actionable-mode",
        choices=[ACTIONABLE_MODE_PIT_NOW_ONLY, ACTIONABLE_MODE_PIT_NOW_PLUS_GOOD_PIT],
        default=ACTIONABLE_MODE_PIT_NOW_ONLY,
        help=(
            "action label set for pit_success_h2 comparator rows; "
            "default is PIT_NOW only, use pit_now_plus_good_pit for sensitivity runs"
        ),
    )

    parser.add_argument("--output-compact-csv", required=True)
    parser.add_argument("--output-by-year-csv", required=True)
    parser.add_argument("--output-recommended-csv", required=True)
    parser.add_argument("--output-md", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_lake = Path(args.data_lake)
    years = normalize_years(args.years)
    thresholds = _parse_thresholds(args.thresholds_csv, args.threshold_min, args.threshold_max, args.threshold_step)

    oof_dir = Path(args.oof_dir)
    runs = _discover_runs(oof_dir, list(args.run_ids))
    comparator_year, comparator_tag = comparator_source_year_and_tag(years, args.season_tag)

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

    all_rows: list[dict[str, object]] = []
    all_year_rows: list[dict[str, object]] = []

    for run_id, oof_path in runs:
        oof = pd.read_csv(oof_path)
        if oof.empty:
            continue
        if "target_y" not in oof.columns:
            raise ValueError(f"oof csv missing target_y: {oof_path}")
        profile, target_column = _run_id_parts(run_id, oof)
        spec = _target_to_contract(target_column)
        if spec.outcome_mode != OUTCOME_PIT_ANY_H2:
            spec = type(spec)(
                outcome_mode=spec.outcome_mode,
                view=spec.view,
                actionable_mode=str(args.pit_success_actionable_mode),
                truth_lens=spec.truth_lens,
            )
        include_same_lap = (
            True
            if spec.outcome_mode == OUTCOME_PIT_ANY_H2
            else bool(args.pit_success_include_same_lap)
        )
        window_semantics = (
            "official_same_lap_inclusive"
            if include_same_lap
            else "strict_future_kplus1_to_kplusH"
        )

        y_true = pd.to_numeric(oof["target_y"], errors="coerce").fillna(0).astype(int)
        rows = int(len(y_true))
        positives = int((y_true == 1).sum())
        prevalence = float(positives / rows) if rows else 0.0
        raw_score = pd.to_numeric(oof.get("raw_proba"), errors="coerce")
        cal_score = pd.to_numeric(oof.get(args.score_column), errors="coerce")
        ap_raw = _safe_ap(y_true, raw_score) if raw_score.notna().any() else float("nan")
        ap_cal = _safe_ap(y_true, cal_score) if cal_score.notna().any() else float("nan")
        score_unique_count = int(cal_score.dropna().nunique()) if cal_score.notna().any() else 0
        score_is_hard_frontier = bool(score_unique_count <= 2)
        if score_is_hard_frontier:
            print(
                "[WARN] hard-decision frontier detected for run_id="
                f"{run_id}: score_column={args.score_column}, unique_values={score_unique_count}. "
                "Threshold sweep is diagnostic only."
            )

        oof_universe = oof[["race", "driver"]].copy()
        oof_universe["race"] = oof_universe["race"].astype(str)
        oof_universe["driver"] = oof_universe["driver"].astype(str)
        oof_universe = oof_universe.dropna(subset=["race", "driver"]).drop_duplicates(
            subset=["race", "driver"],
            keep="first",
        )

        if args.truth_universe_race_driver_csv.strip():
            truth_universe_df = pd.read_csv(Path(args.truth_universe_race_driver_csv))
            required = {"race", "driver"}
            missing = sorted(required.difference(set(truth_universe_df.columns)))
            if missing:
                raise ValueError(
                    f"truth universe csv missing required columns {missing}: {args.truth_universe_race_driver_csv}"
                )
            race_driver_universe = truth_universe_df[["race", "driver"]].copy()
            race_driver_universe["race"] = race_driver_universe["race"].astype(str)
            race_driver_universe["driver"] = race_driver_universe["driver"].astype(str)
            race_driver_universe = race_driver_universe.dropna(subset=["race", "driver"]).drop_duplicates(
                subset=["race", "driver"], keep="first"
            )
        else:
            race_driver_universe = oof_universe

        if args.truth_universe_events_csvs:
            pit_truth_universe = _load_truth_universe_events_csvs(args.truth_universe_events_csvs)
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
            truth_source = "explicit_truth_events_csvs"
        else:
            pit_truth_universe = build_pit_truth_universe(
                pit_timings=pit_timings,
                suggestions_source=race_driver_universe,
                pit_evals=pit_evals,
                ml_features=ml_features,
                prepared_pit_events=prepared,
                split_tag=f"phase2b_{profile}_{target_column}",
            )
            truth_source = "recomputed_truth_universe"

        for threshold in thresholds:
            suggestions = _to_suggestions(oof, args.score_column, float(threshold))
            comparator = _build_comparator_dataset(
                suggestions=suggestions,
                pit_evals=pit_evals,
                horizon=2,
                outcome_mode=spec.outcome_mode,
                pit_timings=pit_timings if spec.outcome_mode == OUTCOME_PIT_ANY_H2 else None,
                actionable_mode=spec.actionable_mode,
                episode_level=(spec.view == "episode_level"),
                include_same_lap=include_same_lap,
                pit_success_no_match_as_negative=True,
            )
            row = _build_metrics_row(
                comparator=comparator,
                pit_truth_universe=pit_truth_universe,
                spec=spec,
                profile=profile,
                target_column=target_column,
                ap_raw=ap_raw,
                ap_calibrated=ap_cal,
                rows=rows,
                positives=positives,
                prevalence=prevalence,
                selected_threshold=float(threshold),
                window_semantics=window_semantics,
            )
            row["run_id"] = run_id
            row["score_column"] = args.score_column
            row["score_unique_count"] = int(score_unique_count)
            row["score_is_hard_decision"] = bool(score_is_hard_frontier)
            row["score_frontier_quality"] = (
                "hard_decision_only" if score_is_hard_frontier else "continuous_score_frontier"
            )
            row["truth_universe_mode"] = str(args.truth_universe_mode_label)
            row["truth_universe_source"] = truth_source
            all_rows.append(row)

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
                    ap_raw_y = _safe_ap(y_slice, raw_y) if rows_y and raw_y.notna().any() else float("nan")
                    ap_cal_y = _safe_ap(y_slice, cal_y) if rows_y and cal_y.notna().any() else float("nan")

                    row_y = _build_metrics_row(
                        comparator=comp_year,
                        pit_truth_universe=truth_year,
                        spec=spec,
                        profile=profile,
                        target_column=target_column,
                        ap_raw=ap_raw_y,
                        ap_calibrated=ap_cal_y,
                        rows=rows_y,
                        positives=pos_y,
                        prevalence=prev_y,
                        selected_threshold=float(threshold),
                        window_semantics=window_semantics,
                    )
                    row_y["year"] = int(year)
                    row_y["run_id"] = run_id
                    row_y["score_column"] = args.score_column
                    row_y["score_unique_count"] = int(score_unique_count)
                    row_y["score_is_hard_decision"] = bool(score_is_hard_frontier)
                    row_y["score_frontier_quality"] = (
                        "hard_decision_only" if score_is_hard_frontier else "continuous_score_frontier"
                    )
                    row_y["truth_universe_mode"] = str(args.truth_universe_mode_label)
                    row_y["truth_universe_source"] = truth_source
                    all_year_rows.append(row_y)

    if not all_rows:
        raise ValueError("no frontier rows generated; check OOF directory and run-id filters")

    compact = pd.DataFrame(all_rows)
    compact.sort_values(
        by=["run_id", "selected_threshold"],
        kind="mergesort",
        inplace=True,
    )
    compact.reset_index(drop=True, inplace=True)

    by_year = pd.DataFrame(all_year_rows)
    if not by_year.empty:
        by_year.sort_values(
            by=["run_id", "year", "selected_threshold"],
            kind="mergesort",
            inplace=True,
        )
        by_year.reset_index(drop=True, inplace=True)

    recommended_rows: list[pd.Series] = []
    for run_id, grp in compact.groupby("run_id", sort=True):
        winner = _choose_recommended(
            grp,
            min_scored_pit_any=int(args.min_scored_pit_any),
            min_scored_pit_success=int(args.min_scored_pit_success),
        )
        recommended_rows.append(winner)
    recommended = pd.DataFrame(recommended_rows).copy()
    recommended.sort_values(by=["run_id"], kind="mergesort", inplace=True)
    recommended.reset_index(drop=True, inplace=True)

    out_compact = Path(args.output_compact_csv)
    out_by_year = Path(args.output_by_year_csv)
    out_reco = Path(args.output_recommended_csv)
    out_md = Path(args.output_md)
    for p in (out_compact, out_by_year, out_reco, out_md):
        p.parent.mkdir(parents=True, exist_ok=True)

    compact.to_csv(out_compact, index=False)
    by_year.to_csv(out_by_year, index=False)
    recommended.to_csv(out_reco, index=False)
    out_md.write_text(
        _render_md(
            recommended,
            universe_mode_label=str(args.truth_universe_mode_label),
        ),
        encoding="utf-8",
    )

    print("=== PHASE2B THRESHOLD FRONTIER GENERATED ===")
    print(f"runs discovered         : {len(runs)}")
    print(f"threshold count         : {len(thresholds)}")
    print(f"score column            : {args.score_column}")
    print(f"pit_evals input         : {pit_evals_path}")
    print(f"pit_timings input       : {pit_timings_path}")
    print(f"truth universe mode     : {args.truth_universe_mode_label}")
    print(f"compact csv             : {out_compact}")
    print(f"by-year csv             : {out_by_year}")
    print(f"recommended csv         : {out_reco}")
    print(f"markdown report         : {out_md}")
    print("")
    print("Top recommended points:")
    cols = [
        "run_id",
        "outcome_mode",
        "truth_lens",
        "selected_threshold",
        "row_tp",
        "tp_for_recall",
        "fp",
        "fn",
        "scored",
        "precision",
        "recall",
        "f0_5",
        "selection_rule",
    ]
    show = [c for c in cols if c in recommended.columns]
    print(recommended[show].to_string(index=False))


if __name__ == "__main__":
    main()
