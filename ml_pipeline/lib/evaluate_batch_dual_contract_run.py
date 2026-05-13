"""Evaluate one Batch ML OOF run under locked dual-contract metrics."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import re

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score

try:
    from .comparator_heuristic import (
        ACTIONABLE_MODE_PIT_NOW_ONLY,
        ACTIONABLE_MODE_PIT_NOW_PLUS_GOOD_PIT,
        OUTCOME_PIT_ANY_H2,
        OUTCOME_PIT_SUCCESS_H2,
        _build_comparator_dataset,
        _latest_jsonl,
        _load_jsonl,
    )
    from .pit_truth_eligibility import (
        build_pit_truth_universe,
        eligible_actual_counts,
        eligible_pit_key_set,
        load_prepared_events_from_csv,
        regime_from_status,
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
        ACTIONABLE_MODE_PIT_NOW_ONLY,
        ACTIONABLE_MODE_PIT_NOW_PLUS_GOOD_PIT,
        OUTCOME_PIT_ANY_H2,
        OUTCOME_PIT_SUCCESS_H2,
        _build_comparator_dataset,
        _latest_jsonl,
        _load_jsonl,
    )
    from pit_truth_eligibility import (  # type: ignore
        build_pit_truth_universe,
        eligible_actual_counts,
        eligible_pit_key_set,
        load_prepared_events_from_csv,
        regime_from_status,
    )
    from pipeline_config import comparator_source_year_and_tag, normalize_years  # type: ignore


@dataclass(frozen=True)
class ContractSpec:
    outcome_mode: str
    view: str
    actionable_mode: str
    truth_lens: str


_YEAR_PREFIX_RE = re.compile(r"^\d{4}\s::\s")


def _target_to_contract(target_column: str) -> ContractSpec:
    target = str(target_column).strip()
    if target.startswith("target_pit_any_h2_"):
        truth_lens = target.replace("target_pit_any_h2_", "", 1)
        return ContractSpec(
            outcome_mode=OUTCOME_PIT_ANY_H2,
            view="episode_level",
            actionable_mode=ACTIONABLE_MODE_PIT_NOW_ONLY,
            truth_lens=truth_lens,
        )
    if target.startswith("target_pit_success_h2_"):
        truth_lens = target.replace("target_pit_success_h2_", "", 1)
        return ContractSpec(
            outcome_mode=OUTCOME_PIT_SUCCESS_H2,
            view="row_level",
            actionable_mode=ACTIONABLE_MODE_PIT_NOW_PLUS_GOOD_PIT,
            truth_lens=truth_lens,
        )
    raise ValueError(
        f"unsupported target column for dual contract: {target!r}; "
        "expected target_pit_any_h2_* or target_pit_success_h2_*"
    )


def _oof_to_suggestions(oof: pd.DataFrame, decision_column: str, score_column: str) -> pd.DataFrame:
    required = {"race", "driver", "lapNumber", decision_column, score_column}
    missing = sorted(required.difference(set(oof.columns)))
    if missing:
        raise ValueError(f"oof csv missing required columns: {missing}")

    work = oof.copy()
    work["race"] = work["race"].astype(str)
    work["driver"] = work["driver"].astype(str)
    work["lapNumber"] = pd.to_numeric(work["lapNumber"], errors="coerce")
    work[decision_column] = pd.to_numeric(work[decision_column], errors="coerce").fillna(0).astype(int)
    work[score_column] = pd.to_numeric(work[score_column], errors="coerce")
    work = work[work["lapNumber"].notna()].copy()
    work["lapNumber"] = work["lapNumber"].astype(int)
    work = work[work[decision_column] == 1].copy()
    if work.empty:
        return pd.DataFrame(columns=["race", "driver", "lapNumber", "suggestionLabel", "totalScore", "trackStatus"])
    return pd.DataFrame(
        {
            "race": work["race"],
            "driver": work["driver"],
            "lapNumber": work["lapNumber"],
            "suggestionLabel": "PIT_NOW",
            "totalScore": work[score_column].fillna(0.0),
            "trackStatus": "",
        }
    )


def _extract_year(frame: pd.DataFrame) -> pd.Series:
    years = pd.to_numeric(frame["race"].astype(str).str.extract(r"^(\d{4})\s::")[0], errors="coerce")
    if years.isna().any():
        bad = frame.loc[years.isna(), "race"].astype(str).drop_duplicates().head(10).tolist()
        raise ValueError(f"unable to parse year from race keys in comparator rows, examples={bad}")
    return years.astype(int)


def _with_year_prefixed_race(frame: pd.DataFrame, year: int) -> pd.DataFrame:
    work = frame.copy()
    if "race" not in work.columns:
        return work
    race = work["race"].astype(str)
    prefixed = race.str.match(r"^\d{4}\s::\s")
    work.loc[~prefixed, "race"] = f"{year} :: " + race[~prefixed]
    return work


def _load_multi_year_stream(
    data_lake: Path,
    stream: str,
    years: list[int],
    season_tag: str,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for year in years:
        src = _latest_jsonl(data_lake, stream, int(year), season_tag)
        frame = _load_jsonl(src)
        frame = _with_year_prefixed_race(frame, int(year))
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _load_truth_universe_events_csvs(paths: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    required = {
        "race",
        "driver",
        "pit_lap_num",
        "eligible_universe",
        "eligible_raw",
        "eligible_clean_actionable",
        "eligible_clean_dry_strategy",
    }
    for path_text in paths:
        path = Path(path_text)
        if not path.exists():
            raise FileNotFoundError(f"truth-universe events csv not found: {path}")
        frame = pd.read_csv(path)
        missing = sorted(required.difference(set(frame.columns)))
        if missing:
            raise ValueError(
                f"truth-universe events csv missing required columns {missing}: {path}"
            )
        if "year" in frame.columns:
            year = pd.to_numeric(frame["year"], errors="coerce")
            race = frame["race"].astype(str)
            mask = (~race.str.match(_YEAR_PREFIX_RE)) & year.notna()
            frame.loc[mask, "race"] = (
                year[mask].astype(int).astype(str) + " :: " + race[mask]
            )
        frame["race"] = frame["race"].astype(str)
        frame["driver"] = frame["driver"].astype(str)
        frame["pit_lap_num"] = pd.to_numeric(frame["pit_lap_num"], errors="coerce")
        frame = frame[frame["pit_lap_num"].notna()].copy()
        frame["pit_lap_num"] = frame["pit_lap_num"].astype(int)
        for col in (
            "eligible_universe",
            "eligible_raw",
            "eligible_clean_actionable",
            "eligible_clean_dry_strategy",
        ):
            if col in frame.columns:
                frame[col] = (
                    frame[col]
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .map({"true": True, "false": False, "1": True, "0": False})
                )
        frames.append(frame)

    if not frames:
        return pd.DataFrame(columns=list(required))

    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["race", "driver", "pit_lap_num"], keep="first").copy()
    for col in (
        "eligible_universe",
        "eligible_raw",
        "eligible_clean_actionable",
        "eligible_clean_dry_strategy",
    ):
        out[col] = out[col].fillna(False).astype(bool)
    out.reset_index(drop=True, inplace=True)
    return out


def _build_metrics_row(
    comparator: pd.DataFrame,
    pit_truth_universe: pd.DataFrame,
    *,
    spec: ContractSpec,
    profile: str,
    target_column: str,
    ap_raw: float,
    ap_calibrated: float,
    rows: int,
    positives: int,
    prevalence: float,
    selected_threshold: float,
) -> dict[str, object]:
    def _safe_div(num: float, den: float) -> float:
        return float(num / den) if den else 0.0

    def _f_beta(precision: float, recall: float, beta: float) -> float:
        beta2 = beta * beta
        den = (beta2 * precision) + recall
        return float((1.0 + beta2) * precision * recall / den) if den > 0 else 0.0

    work = comparator.copy()
    if "trackStatus" not in work.columns:
        work["trackStatus"] = ""
    work["regime"] = work["trackStatus"].map(regime_from_status)

    scored = work[work["outcome_class"].isin(["1", "0"])].copy()
    row_tp = int((scored["outcome_class"] == "1").sum())
    fp = int((scored["outcome_class"] == "0").sum())
    precision = _safe_div(row_tp, row_tp + fp)

    matched_keys: set[tuple[str, str, int]] = set()
    matched_rows = work[work["matched_pit_lap"].notna()].copy()
    if not matched_rows.empty:
        for _, row in matched_rows.iterrows():
            matched_keys.add((str(row["race"]), str(row["driver"]), int(row["matched_pit_lap"])))

    eligible_keys = eligible_pit_key_set(
        pit_truth_universe,
        truth_lens=spec.truth_lens,
        regime="ALL",
    )
    tp_for_recall = int(len(matched_keys & eligible_keys))
    _, eligible_actual_pit_count = eligible_actual_counts(
        pit_truth_universe,
        truth_lens=spec.truth_lens,
        regime="ALL",
    )
    fn = max(eligible_actual_pit_count - tp_for_recall, 0)
    recall = _safe_div(tp_for_recall, eligible_actual_pit_count)
    f1 = _f_beta(precision, recall, beta=1.0)
    f05 = _f_beta(precision, recall, beta=0.5)
    tp_for_recall_plus_fn_equals_eligible = bool(
        (tp_for_recall + fn) == eligible_actual_pit_count
    )
    row_tp_plus_fn_equals_eligible = bool(
        (row_tp + fn) == eligible_actual_pit_count
    )

    return {
        "profile": profile,
        "target_column": target_column,
        "outcome_mode": spec.outcome_mode,
        "truth_lens": spec.truth_lens,
        "view": spec.view,
        "actionable_mode": spec.actionable_mode,
        "horizon": 2,
        "window_semantics": "official_same_lap_inclusive",
        "rows": int(rows),
        "positives": int(positives),
        "prevalence": float(prevalence),
        "ap_raw": float(ap_raw),
        "ap_calibrated": float(ap_calibrated),
        "AP": float(ap_calibrated),
        "selected_threshold": float(selected_threshold),
        # Backward-compatible alias retained for downstream readers.
        "tp": int(row_tp),
        "row_tp": int(row_tp),
        "tp_for_recall": int(tp_for_recall),
        "fp": int(fp),
        "fn": int(fn),
        "scored": int(len(scored)),
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
        "f0_5": float(f05),
        "eligible_actual_pit_count": int(eligible_actual_pit_count),
        "comparator_actual_count": int(tp_for_recall + fn),
        "tp_for_recall_plus_fn_equals_eligible": tp_for_recall_plus_fn_equals_eligible,
        "row_tp_plus_fn_equals_eligible": row_tp_plus_fn_equals_eligible,
        "recall_count_mode": "event_level_unique_pits",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate one Batch ML OOF run under dual-contract metrics.")
    parser.add_argument("--data-lake", default="data_lake")
    parser.add_argument("--years", type=int, nargs="+", required=True)
    parser.add_argument("--season-tag", default="season")
    parser.add_argument("--oof-csv", required=True)
    parser.add_argument("--target-column", required=True)
    parser.add_argument("--profile", required=True)
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
    parser.add_argument("--output-summary-csv", required=True)
    parser.add_argument("--output-by-year-csv", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_lake = Path(args.data_lake)
    years = normalize_years(args.years)
    comparator_year, comparator_tag = comparator_source_year_and_tag(years, args.season_tag)

    spec = _target_to_contract(args.target_column)

    oof_path = Path(args.oof_csv)
    oof = pd.read_csv(oof_path)
    if oof.empty:
        raise ValueError(f"oof csv is empty: {oof_path}")
    if "target_y" not in oof.columns:
        raise ValueError("oof csv missing target_y")

    y_true = pd.to_numeric(oof["target_y"], errors="coerce").fillna(0).astype(int)
    rows = int(len(y_true))
    positives = int((y_true == 1).sum())
    prevalence = float(positives / rows) if rows else 0.0
    raw_score = pd.to_numeric(oof.get("raw_proba"), errors="coerce")
    cal_score = pd.to_numeric(oof.get(args.score_column), errors="coerce")
    ap_raw = float(average_precision_score(y_true, raw_score)) if raw_score.notna().any() else float("nan")
    ap_cal = float(average_precision_score(y_true, cal_score)) if cal_score.notna().any() else float("nan")
    selected_threshold = float(pd.to_numeric(oof.get("constrained_threshold"), errors="coerce").median())

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
    # Denominator must be model-profile invariant for the same target/lens/year scope.
    # Default: full OOF race/driver universe (prediction-independent).
    # Optional overrides:
    #   1) explicit race/driver universe csv
    #   2) explicit truth-universe event csvs with eligibility flags
    truth_universe_source = "oof_race_driver_universe"
    if args.truth_universe_race_driver_csv.strip():
        truth_universe_path = Path(args.truth_universe_race_driver_csv)
        truth_universe_df = pd.read_csv(truth_universe_path)
        required = {"race", "driver"}
        missing = sorted(required.difference(set(truth_universe_df.columns)))
        if missing:
            raise ValueError(
                f"truth universe csv missing required columns {missing}: {truth_universe_path}"
            )
        oof_universe = truth_universe_df[["race", "driver"]].copy()
        truth_universe_source = f"explicit_csv:{truth_universe_path}"
    else:
        oof_universe = oof[["race", "driver"]].copy()

    oof_universe["race"] = oof_universe["race"].astype(str)
    oof_universe["driver"] = oof_universe["driver"].astype(str)
    oof_universe = oof_universe.dropna(subset=["race", "driver"]).drop_duplicates(
        subset=["race", "driver"],
        keep="first",
    )

    if args.truth_universe_events_csvs:
        truth_universe = _load_truth_universe_events_csvs(args.truth_universe_events_csvs)
        truth_universe_source = "explicit_truth_events_csvs"
        # Optional universe override for shared-intersection denominators.
        if args.truth_universe_race_driver_csv.strip():
            allowed = set(
                map(tuple, oof_universe[["race", "driver"]].itertuples(index=False, name=None))
            )
            truth_universe["eligible_universe"] = [
                (str(race), str(driver)) in allowed
                for race, driver in truth_universe[["race", "driver"]].itertuples(index=False, name=None)
            ]
        else:
            truth_universe["eligible_universe"] = truth_universe["eligible_universe"].fillna(False).astype(bool)
    else:
        truth_universe = build_pit_truth_universe(
            pit_timings=pit_timings,
            suggestions_source=oof_universe,
            pit_evals=pit_evals,
            ml_features=ml_features,
            prepared_pit_events=prepared,
            split_tag=f"batch_{args.profile}_{args.target_column}",
        )

    comparator = _build_comparator_dataset(
        suggestions=suggestions,
        pit_evals=pit_evals,
        horizon=2,
        outcome_mode=spec.outcome_mode,
        pit_timings=pit_timings if spec.outcome_mode == OUTCOME_PIT_ANY_H2 else None,
        actionable_mode=spec.actionable_mode,
        episode_level=(spec.view == "episode_level"),
        include_same_lap=True,
    )

    summary_row = _build_metrics_row(
        comparator,
        truth_universe,
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

    by_year_rows: list[dict[str, object]] = []
    if not comparator.empty:
        comp = comparator.copy()
        comp["year"] = _extract_year(comp)
        truth = truth_universe.copy()
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
            ap_raw_y = float(average_precision_score(y_slice, raw_y)) if rows_y and raw_y.notna().any() else float("nan")
            ap_cal_y = float(average_precision_score(y_slice, cal_y)) if rows_y and cal_y.notna().any() else float("nan")

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
            row_y["year"] = int(year)
            by_year_rows.append(row_y)

    summary_df = pd.DataFrame([summary_row])
    by_year_df = pd.DataFrame(by_year_rows)

    out_summary = Path(args.output_summary_csv)
    out_by_year = Path(args.output_by_year_csv)
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    out_by_year.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(out_summary, index=False)
    by_year_df.to_csv(out_by_year, index=False)

    print("=== BATCH DUAL-CONTRACT RUN EVALUATION GENERATED ===")
    print(f"oof csv          : {oof_path}")
    print(f"pit_evals input  : {pit_evals_path}")
    print(f"pit_timings input: {pit_timings_path}")
    print(f"profile          : {args.profile}")
    print(f"target           : {args.target_column}")
    print(f"contract         : outcome={spec.outcome_mode}, view={spec.view}, mode={spec.actionable_mode}, lens={spec.truth_lens}")
    print(f"oof race/driver  : {len(oof_universe)}")
    print(f"truth universe   : {truth_universe_source}")
    print(f"summary csv      : {out_summary}")
    print(f"by-year csv      : {out_by_year}")
    print(summary_df.to_string(index=False))


if __name__ == "__main__":
    main()
