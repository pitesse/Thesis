"""Build the final pit_success_h2 accounting package from existing artifacts.

This script is intentionally artifact-driven:
- no retraining
- no heavy recomputation
- strict use of regenerated Phase 2B outputs on disk
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score

try:
    from .comparator_heuristic import (
        ACTIONABLE_MODE_PIT_NOW_ONLY,
        ACTIONABLE_MODE_PIT_NOW_PLUS_GOOD_PIT,
        OUTCOME_PIT_SUCCESS_H2,
        POSITIVE_RESULTS,
        _build_comparator_dataset,
        _load_jsonl,
    )
    from .evaluate_batch_dual_contract_run import _oof_to_suggestions
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
        OUTCOME_PIT_SUCCESS_H2,
        POSITIVE_RESULTS,
        _build_comparator_dataset,
        _load_jsonl,
    )
    from evaluate_batch_dual_contract_run import _oof_to_suggestions  # type: ignore


PROFILE_E0 = "e0_no_source_year"
PROFILE_P1 = "p1_percent_conservative_v1"
TARGET_SUCCESS = "target_pit_success_h2_clean_actionable"
TARGET_ANY = "target_pit_any_h2_clean_actionable"


@dataclass(frozen=True)
class LearnerPoint:
    system: str
    learner: str
    profile: str
    target: str
    threshold: float
    oof_path: Path
    score_column: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build final accounting package for corrected pit_success_h2 contract."
    )
    parser.add_argument(
        "--root",
        default=".",
        help="repository root",
    )
    parser.add_argument(
        "--output-dir",
        default="data_lake/reports/phase2b_presentation_figures",
    )
    return parser.parse_args()


def _md_table(df: pd.DataFrame, float_precision: int = 6) -> str:
    if df.empty:
        return "_No rows._\n"
    cols = list(df.columns)
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |"
    rows: list[str] = [header, sep]
    for _, row in df.iterrows():
        vals: list[str] = []
        for c in cols:
            v = row[c]
            if isinstance(v, (float, np.floating)):
                if np.isnan(v):
                    vals.append("")
                else:
                    vals.append(f"{float(v):.{float_precision}f}")
            else:
                vals.append(str(v))
        rows.append("| " + " | ".join(vals) + " |")
    return "\n".join(rows) + "\n"


def _write_csv_md(df: pd.DataFrame, csv_path: Path, md_path: Path, title: str) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    md = [f"# {title}", "", _md_table(df)]
    md_path.write_text("\n".join(md), encoding="utf-8")


def _prefix_year_race(frame: pd.DataFrame, year: int) -> pd.DataFrame:
    out = frame.copy()
    if "race" not in out.columns:
        return out
    race = out["race"].astype(str)
    needs = ~race.str.match(r"^\d{4}\s::\s")
    out.loc[needs, "race"] = f"{int(year)} :: " + race[needs]
    return out


def _load_jsonls_with_year_prefix(paths: Iterable[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in paths:
        if not path.exists():
            continue
        year = None
        for token in [path.name, str(path.parent)]:
            import re

            m = re.search(r"(20\d{2})", token)
            if m:
                year = int(m.group(1))
                break
        if year is None:
            raise ValueError(f"cannot infer year from {path}")
        df = _load_jsonl(path)
        frames.append(_prefix_year_race(df, year))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _load_truth_events(paths: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in paths:
        if not path.exists():
            continue
        df = pd.read_csv(path)
        if "year" in df.columns and "race" in df.columns:
            race = df["race"].astype(str)
            year = pd.to_numeric(df["year"], errors="coerce")
            needs = ~race.str.match(r"^\d{4}\s::\s") & year.notna()
            df.loc[needs, "race"] = year[needs].astype(int).astype(str) + " :: " + race[needs]
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["race"] = out["race"].astype(str)
    out["driver"] = out["driver"].astype(str)
    out["pit_lap_num"] = pd.to_numeric(out["pit_lap_num"], errors="coerce")
    out = out[out["pit_lap_num"].notna()].copy()
    out["pit_lap_num"] = out["pit_lap_num"].astype(int)
    out = out.drop_duplicates(subset=["race", "driver", "pit_lap_num"], keep="first").copy()
    return out


def _load_sde_universe(path: Path) -> set[tuple[str, str]]:
    if not path.exists():
        return set()
    df = pd.read_csv(path)
    if not {"race", "driver"}.issubset(df.columns):
        return set()
    return set(
        (str(race), str(driver))
        for race, driver in df[["race", "driver"]].itertuples(index=False, name=None)
    )


def _filter_race_driver_universe(frame: pd.DataFrame, universe: set[tuple[str, str]]) -> pd.DataFrame:
    if frame.empty or not universe:
        return frame.copy()
    out = frame.copy()
    out["race"] = out["race"].astype(str)
    out["driver"] = out["driver"].astype(str)
    mask = [
        (str(race), str(driver)) in universe
        for race, driver in out[["race", "driver"]].itertuples(index=False, name=None)
    ]
    return out.loc[mask].copy()


def _event_key_set(df: pd.DataFrame) -> set[tuple[str, str, int]]:
    if df.empty:
        return set()
    out: set[tuple[str, str, int]] = set()
    for race, driver, lap in df[["race", "driver", "pit_lap_num"]].itertuples(index=False, name=None):
        out.add((str(race), str(driver), int(lap)))
    return out


def _safe_div(num: float, den: float) -> float:
    return float(num / den) if den else 0.0


def _f_beta(precision: float, recall: float, beta: float = 0.5) -> float:
    beta2 = beta * beta
    den = (beta2 * precision) + recall
    return float((1.0 + beta2) * precision * recall / den) if den > 0 else 0.0


def _comparator_metrics(
    comparator: pd.DataFrame,
    *,
    eligible_success_keys: set[tuple[str, str, int]],
    positive_call_definition: str,
) -> dict[str, object]:
    work = comparator.copy()
    work["outcome_class"] = work["outcome_class"].astype(str)
    work["exclusion_reason"] = work["exclusion_reason"].fillna("").astype(str)

    predicted_positives = int(len(work))
    scored = work[work["outcome_class"].isin(["1", "0"])].copy()
    excluded = work[work["outcome_class"].eq("EXCLUDED")].copy()
    tp = int((scored["outcome_class"] == "1").sum())
    fp_total = int((scored["outcome_class"] == "0").sum())
    no_match_fp = int(
        (
            scored["outcome_class"].eq("0")
            & scored["exclusion_reason"].str.upper().eq("NO_MATCH_WITHIN_HORIZON")
        ).sum()
    )
    fp_failure = int(fp_total - no_match_fp)

    matched_known = scored[scored["matched_pit_lap"].notna()].copy()
    matched_success = int((matched_known["outcome_class"] == "1").sum())
    matched_failure = int((matched_known["outcome_class"] == "0").sum())
    matched_unknown_excluded = int(
        (excluded["matched_pit_lap"].notna()).sum()
    )
    matched_pit_success_rate = _safe_div(matched_success, matched_success + matched_failure)

    matched_success_keys = set(
        (
            str(race),
            str(driver),
            int(lap),
        )
        for race, driver, lap in matched_known[
            matched_known["outcome_class"].eq("1")
        ][["race", "driver", "matched_pit_lap"]].itertuples(index=False, name=None)
    )
    successful_events_covered_unique = int(len(matched_success_keys & eligible_success_keys))
    eligible_successful_pit_events = int(len(eligible_success_keys))
    successful_event_coverage = _safe_div(
        successful_events_covered_unique, eligible_successful_pit_events
    )
    fn_events = int(max(eligible_successful_pit_events - successful_events_covered_unique, 0))

    strict_precision = _safe_div(tp, tp + fp_total)
    f05 = _f_beta(strict_precision, successful_event_coverage, beta=0.5)

    return {
        "positive_call_definition": positive_call_definition,
        "predicted_positives": predicted_positives,
        "calls_matching_any_pit_in_horizon": int(work["matched_pit_lap"].notna().sum()),
        "calls_matching_known_pit_outcome": int(len(matched_known)),
        "no_match_calls": int(work["matched_pit_lap"].isna().sum()),
        "unknown_excluded_calls": int(len(excluded)),
        "matched_success": matched_success,
        "matched_failure": matched_failure,
        "matched_unknown_excluded": matched_unknown_excluded,
        "matched_pit_success_rate": matched_pit_success_rate,
        "TP": tp,
        "FP_no_match": no_match_fp,
        "FP_failure": fp_failure,
        "FP_total": fp_total,
        "unknown_excluded": int(len(excluded)),
        "strict_precision": strict_precision,
        "successful_events_covered_unique": successful_events_covered_unique,
        "eligible_successful_pit_events": eligible_successful_pit_events,
        "successful_event_coverage": successful_event_coverage,
        "FN_events": fn_events,
        "F0_5": f05,
    }


def _row_metrics_from_oof(
    oof: pd.DataFrame,
    threshold: float,
    score_col: str = "calibrated_proba",
) -> dict[str, float | int]:
    score = pd.to_numeric(oof[score_col], errors="coerce").fillna(0.0).astype(float)
    y = pd.to_numeric(oof["target_y"], errors="coerce").fillna(0).astype(int)
    pred = (score >= float(threshold)).astype(int)

    tp = int(((pred == 1) & (y == 1)).sum())
    fp = int(((pred == 1) & (y == 0)).sum())
    fn = int(((pred == 0) & (y == 1)).sum())
    tn = int(((pred == 0) & (y == 0)).sum())
    precision = _safe_div(tp, tp + fp)
    recall = _safe_div(tp, tp + fn)
    f05 = _f_beta(precision, recall, beta=0.5)

    return {
        "row_TP": tp,
        "row_FP": fp,
        "row_FN": fn,
        "row_TN": tn,
        "row_precision": precision,
        "row_recall": recall,
        "row_F0_5": f05,
        "predicted_positive_rows": int(pred.sum()),
        "total_rows": int(len(oof)),
        "target_y_positives": int((y == 1).sum()),
    }


def _build_learner_comparator(
    *,
    oof_path: Path,
    threshold: float,
    pit_evals: pd.DataFrame,
    universe: set[tuple[str, str]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    oof = pd.read_csv(oof_path)
    oof = _filter_race_driver_universe(oof, universe)
    oof = oof.copy()
    oof["_diag_score"] = pd.to_numeric(oof["calibrated_proba"], errors="coerce").fillna(0.0)
    oof["_diag_pred"] = (oof["_diag_score"] >= float(threshold)).astype(int)
    suggestions = _oof_to_suggestions(oof, "_diag_pred", "_diag_score")
    comparator = _build_comparator_dataset(
        suggestions=suggestions,
        pit_evals=pit_evals,
        horizon=2,
        outcome_mode=OUTCOME_PIT_SUCCESS_H2,
        pit_timings=None,
        actionable_mode=ACTIONABLE_MODE_PIT_NOW_ONLY,
        episode_level=False,
        include_same_lap=False,
        pit_success_no_match_as_negative=True,
    )
    return comparator, oof


def _artifact_row_count(path: Path) -> tuple[int | None, int | None, int | None]:
    if not path.exists():
        return None, None, None
    try:
        if path.suffix.lower() == ".parquet":
            df = pd.read_parquet(path)
        elif path.suffix.lower() == ".jsonl":
            df = pd.read_json(path, lines=True)
        elif path.suffix.lower() == ".csv":
            df = pd.read_csv(path)
        else:
            return None, None, None
        row_count = int(len(df))
        class0 = None
        class1 = None
        for target_col in ("target_y", TARGET_SUCCESS):
            if target_col in df.columns:
                vc = pd.to_numeric(df[target_col], errors="coerce").value_counts(dropna=False)
                class0 = int(vc.get(0, 0))
                class1 = int(vc.get(1, 0))
                break
        return row_count, class0, class1
    except Exception:
        return None, None, None


def _build_provenance_rows(root: Path, output_dir: Path) -> pd.DataFrame:
    items: list[dict[str, object]] = []

    def add(
        *,
        artifact_role: str,
        system: str,
        profile: str,
        target: str,
        path_rel: str,
        notes: str = "",
        stale_warning: str = "",
    ) -> None:
        path = root / path_rel
        exists = path.exists()
        row_count, class0, class1 = _artifact_row_count(path)
        items.append(
            {
                "artifact_role": artifact_role,
                "system": system,
                "profile": profile,
                "target": target,
                "path": path_rel,
                "exists": bool(exists),
                "row_count_if_applicable": row_count if row_count is not None else "",
                "class0_if_applicable": class0 if class0 is not None else "",
                "class1_if_applicable": class1 if class1 is not None else "",
                "stale_warning": stale_warning,
                "notes": notes,
            }
        )

    # SDE source streams.
    for y in [2022, 2023, 2024, 2025]:
        add(
            artifact_role="sde_pit_suggestions",
            system="Final SDE",
            profile="c6_cfg120_fixed",
            target="pit_success_h2",
            path_rel=f"data_lake/reports/variant_runs/c6_cfg120_fixed_{y}/pit_suggestions_{y}_season.jsonl",
            notes="action labels include PIT_NOW/GOOD_PIT/MONITOR/LOST_CHANCE",
        )
        add(
            artifact_role="sde_pit_evals",
            system="Final SDE",
            profile="c6_cfg120_fixed",
            target="pit_success_h2",
            path_rel=f"data_lake/reports/variant_runs/c6_cfg120_fixed_{y}/pit_evals_{y}_season.jsonl",
            notes="post-pit outcome truth labels",
        )
        add(
            artifact_role="sde_pit_timings",
            system="Final SDE",
            profile="c6_cfg120_fixed",
            target="pit_any_h2",
            path_rel=f"data_lake/reports/variant_runs/c6_cfg120_fixed_{y}/pit_timings_{y}_season.jsonl",
            notes="actual pit events",
        )

    # Truth universe / denominator sources.
    add(
        artifact_role="truth_universe_race_driver",
        system="shared",
        profile="canonical_sde_truth",
        target="all",
        path_rel="data_lake/reports/ml_phase2b_dual_contract_2022_2025/audits/sde_universe_2022_2025.csv",
        notes="canonical race-driver evaluation universe",
    )
    for y in [2022, 2023, 2024, 2025]:
        add(
            artifact_role="truth_universe_events",
            system="shared",
            profile="canonical_sde_truth",
            target="all",
            path_rel=f"data_lake/reports/pit_truth_eligibility_audit_{y}_c6_cfg120_fixed.csv",
            notes="eligible event denominator source",
        )

    # Batch.
    for profile in [PROFILE_E0, PROFILE_P1]:
        for target in [TARGET_ANY, TARGET_SUCCESS]:
            add(
                artifact_role="batch_oof",
                system="Batch",
                profile=profile,
                target=target,
                path_rel=f"data_lake/reports/ml_phase2b_dual_contract_2022_2025/oof/{profile}__{target}.csv",
                notes="race-sequential OOF predictions",
            )
    add(
        artifact_role="batch_recommended",
        system="Batch",
        profile="both",
        target="both",
        path_rel="data_lake/reports/ml_phase2b_dual_contract_2022_2025/recommended/phase2b_recommended_operating_points.csv",
    )
    add(
        artifact_role="batch_frontier",
        system="Batch",
        profile="both",
        target="both",
        path_rel="data_lake/reports/ml_phase2b_dual_contract_2022_2025/frontier/phase2b_threshold_frontier_compact.csv",
    )

    # MOA.
    for profile in [PROFILE_E0, PROFILE_P1]:
        for target in [TARGET_ANY, TARGET_SUCCESS]:
            add(
                artifact_role="moa_export_csv",
                system="MOA",
                profile=profile,
                target=target,
                path_rel=f"data_lake/reports/sml_phase2b_dual_contract_2022_2025/exports/{profile}__{target}.csv",
                notes="ARFF/CSV export matrix for MOA",
            )
            add(
                artifact_role="moa_oof",
                system="MOA",
                profile=profile,
                target=target,
                path_rel=f"data_lake/reports/sml_phase2b_dual_contract_2022_2025/oof/{profile}__{target}.csv",
                notes="aligned MOA pseudo-OOF with vote scores",
            )
            add(
                artifact_role="moa_predictions",
                system="MOA",
                profile=profile,
                target=target,
                path_rel=f"data_lake/reports/sml_phase2b_dual_contract_2022_2025/moa/predictions/{profile}__{target}.csv",
                notes="raw MOA vote logger outputs",
            )
    add(
        artifact_role="moa_recommended",
        system="MOA",
        profile="both",
        target="both",
        path_rel="data_lake/reports/sml_phase2b_dual_contract_2022_2025/recommended/phase2b_recommended_operating_points.csv",
    )
    add(
        artifact_role="moa_frontier",
        system="MOA",
        profile="both",
        target="both",
        path_rel="data_lake/reports/sml_phase2b_dual_contract_2022_2025/frontier/phase2b_threshold_frontier_compact.csv",
    )

    # Timing.
    add(
        artifact_role="runtime_csv",
        system="shared",
        profile="all",
        target="all",
        path_rel="data_lake/reports/phase2b_timing/training_runtime_2022_2025.csv",
        notes="batch/moa runtime recap",
    )

    # Figure manifest source-integrity check.
    add(
        artifact_role="figure_manifest",
        system="shared",
        profile="presentation",
        target="all",
        path_rel="data_lake/reports/phase2b_presentation_figures/phase2b_figures_manifest.csv",
        notes="source-integrity check for phase2a fallback",
    )

    df = pd.DataFrame(items)

    # Stale checks.
    expected_rows = 91473
    expected_c0 = 89361
    expected_c1 = 2112
    moa_success_mask = (
        (df["system"] == "MOA")
        & (df["target"] == TARGET_SUCCESS)
        & (df["artifact_role"].isin(["moa_export_csv", "moa_oof"]))
    )
    for idx in df[moa_success_mask].index:
        rows = pd.to_numeric(df.at[idx, "row_count_if_applicable"], errors="coerce")
        c0 = pd.to_numeric(df.at[idx, "class0_if_applicable"], errors="coerce")
        c1 = pd.to_numeric(df.at[idx, "class1_if_applicable"], errors="coerce")
        warns: list[str] = []
        if int(rows) != expected_rows:
            warns.append(f"rows={int(rows)} expected={expected_rows}")
        if int(c0) != expected_c0 or int(c1) != expected_c1:
            warns.append(f"class_split={int(c0)}/{int(c1)} expected={expected_c0}/{expected_c1}")
        if warns:
            df.at[idx, "stale_warning"] = "; ".join(warns)

    # Batch pit_success OOF shortfall explanation check (warm-up race).
    batch_path = root / f"data_lake/reports/ml_phase2b_dual_contract_2022_2025/oof/{PROFILE_P1}__{TARGET_SUCCESS}.csv"
    dataset_path = root / "data_lake/ml_training_dataset_2022_2025_dual_contract.parquet"
    if batch_path.exists() and dataset_path.exists():
        batch_oof = pd.read_csv(batch_path)
        full = pd.read_parquet(dataset_path)
        elig = full[full[f"{TARGET_SUCCESS}_train_eligible"].astype(bool)].copy()
        key_cols = ["race", "driver", "lapNumber"]
        oof_keys = set(map(tuple, batch_oof[key_cols].itertuples(index=False, name=None)))
        full_keys = set(map(tuple, elig[key_cols].itertuples(index=False, name=None)))
        missing = full_keys - oof_keys
        if missing:
            from collections import Counter

            race_counts = Counter([str(r) for r, _, _ in missing])
            top_race, top_count = race_counts.most_common(1)[0]
            note = (
                f"batch_oof_missing_rows={len(missing)} (expected warm-up omission); "
                f"dominant_missing_race={top_race} ({top_count} rows)"
            )
        else:
            note = "no missing rows vs train-eligible universe"
        mask = (
            (df["artifact_role"] == "batch_oof")
            & (df["target"] == TARGET_SUCCESS)
        )
        df.loc[mask, "notes"] = df.loc[mask, "notes"].astype(str) + " | " + note

    # Phase2A fallback check in current manifest.
    manifest_path = root / "data_lake/reports/phase2b_presentation_figures/phase2b_figures_manifest.csv"
    if manifest_path.exists():
        manifest = pd.read_csv(manifest_path)
        has_phase2a = manifest["source_artifact"].astype(str).str.contains("ml_phase2a", na=False).any()
        m = df["artifact_role"] == "figure_manifest"
        if has_phase2a:
            df.loc[m, "stale_warning"] = "phase2a source artifact detected in current manifest"
        else:
            df.loc[m, "notes"] = df.loc[m, "notes"].astype(str) + " | no phase2a fallback detected"

    df.sort_values(by=["artifact_role", "system", "profile", "target"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def _selected_thresholds(reco_path: Path) -> pd.DataFrame:
    df = pd.read_csv(reco_path)
    return df[
        (df["truth_lens"].astype(str) == "clean_actionable")
        & (df["target_column"].astype(str).isin([TARGET_ANY, TARGET_SUCCESS]))
        & (df["profile"].astype(str).isin([PROFILE_E0, PROFILE_P1]))
    ].copy()


def main() -> None:
    args = parse_args()
    root = Path(args.root).resolve()
    output_dir = (root / args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Canonical sources.
    truth_universe_csv = root / "data_lake/reports/ml_phase2b_dual_contract_2022_2025/audits/sde_universe_2022_2025.csv"
    truth_events_csvs = [
        root / "data_lake/reports/pit_truth_eligibility_audit_2022_c6_cfg120_fixed.csv",
        root / "data_lake/reports/pit_truth_eligibility_audit_2023_c6_cfg120_fixed.csv",
        root / "data_lake/reports/pit_truth_eligibility_audit_2024_c6_cfg120_fixed.csv",
        root / "data_lake/reports/pit_truth_eligibility_audit_2025_c6_cfg120_fixed.csv",
    ]
    universe = _load_sde_universe(truth_universe_csv)
    truth_events = _load_truth_events(truth_events_csvs)
    truth_events = _filter_race_driver_universe(truth_events, universe)
    eligible_events = truth_events[
        (truth_events["eligible_universe"].astype(bool))
        & (truth_events["eligible_clean_actionable"].astype(bool))
    ].copy()
    eligible_any_keys = _event_key_set(eligible_events)
    result_norm = (
        eligible_events["result"]
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace(" ", "_", regex=False)
    )
    eligible_success_events = eligible_events[result_norm.isin(POSITIVE_RESULTS)].copy()
    eligible_success_keys = _event_key_set(eligible_success_events)

    # 1) Input provenance.
    provenance = _build_provenance_rows(root, output_dir)
    _write_csv_md(
        provenance,
        output_dir / "final_accounting_input_provenance.csv",
        output_dir / "final_accounting_input_provenance.md",
        "Final Accounting Input Provenance",
    )

    # Shared pit_evals used by learner comparator scoring.
    pit_evals_merged = pd.read_json(
        root / "data_lake/pit_evals_9999_merged_20260508_145741.jsonl",
        lines=True,
    )
    pit_evals_merged["race"] = pit_evals_merged["race"].astype(str)
    pit_evals_merged["driver"] = pit_evals_merged["driver"].astype(str)
    pit_evals_merged = _filter_race_driver_universe(pit_evals_merged, universe)

    # 2) SDE pit_success accounting.
    sde_suggestion_paths = [
        root / "data_lake/reports/variant_runs/c6_cfg120_fixed_2022/pit_suggestions_2022_season.jsonl",
        root / "data_lake/reports/variant_runs/c6_cfg120_fixed_2023/pit_suggestions_2023_season.jsonl",
        root / "data_lake/reports/variant_runs/c6_cfg120_fixed_2024/pit_suggestions_2024_season.jsonl",
        root / "data_lake/reports/variant_runs/c6_cfg120_fixed_2025/pit_suggestions_2025_season.jsonl",
    ]
    sde_eval_paths = [
        root / "data_lake/reports/variant_runs/c6_cfg120_fixed_2022/pit_evals_2022_season.jsonl",
        root / "data_lake/reports/variant_runs/c6_cfg120_fixed_2023/pit_evals_2023_season.jsonl",
        root / "data_lake/reports/variant_runs/c6_cfg120_fixed_2024/pit_evals_2024_season.jsonl",
        root / "data_lake/reports/variant_runs/c6_cfg120_fixed_2025/pit_evals_2025_season.jsonl",
    ]
    sde_suggestions = _load_jsonls_with_year_prefix(sde_suggestion_paths)
    sde_pit_evals = _load_jsonls_with_year_prefix(sde_eval_paths)
    sde_suggestions = _filter_race_driver_universe(sde_suggestions, universe)
    sde_pit_evals = _filter_race_driver_universe(sde_pit_evals, universe)

    sde_rows: list[dict[str, object]] = []
    for actionable_mode, definition in [
        (ACTIONABLE_MODE_PIT_NOW_ONLY, "PIT_NOW"),
        (ACTIONABLE_MODE_PIT_NOW_PLUS_GOOD_PIT, "PIT_NOW + GOOD_PIT"),
    ]:
        comparator = _build_comparator_dataset(
            suggestions=sde_suggestions,
            pit_evals=sde_pit_evals,
            horizon=2,
            outcome_mode=OUTCOME_PIT_SUCCESS_H2,
            pit_timings=None,
            actionable_mode=actionable_mode,
            episode_level=False,
            include_same_lap=False,
            pit_success_no_match_as_negative=True,
        )
        metrics = _comparator_metrics(
            comparator,
            eligible_success_keys=eligible_success_keys,
            positive_call_definition=definition,
        )
        metrics["system"] = "Final SDE"
        metrics["actionable_mode"] = actionable_mode
        sde_rows.append(metrics)

    sde_df = pd.DataFrame(sde_rows)[
        [
            "system",
            "actionable_mode",
            "positive_call_definition",
            "predicted_positives",
            "calls_matching_any_pit_in_horizon",
            "calls_matching_known_pit_outcome",
            "no_match_calls",
            "unknown_excluded_calls",
            "matched_success",
            "matched_failure",
            "matched_unknown_excluded",
            "matched_pit_success_rate",
            "TP",
            "FP_no_match",
            "FP_failure",
            "FP_total",
            "unknown_excluded",
            "strict_precision",
            "successful_events_covered_unique",
            "eligible_successful_pit_events",
            "successful_event_coverage",
            "FN_events",
            "F0_5",
        ]
    ]
    _write_csv_md(
        sde_df,
        output_dir / "sde_pit_success_accounting_corrected.csv",
        output_dir / "sde_pit_success_accounting_corrected.md",
        "SDE pit_success_h2 Accounting (Corrected Contract)",
    )

    # 3) Learner strict operational accounting at selected thresholds.
    batch_reco = _selected_thresholds(
        root / "data_lake/reports/ml_phase2b_dual_contract_2022_2025/recommended/phase2b_recommended_operating_points.csv"
    )
    moa_reco = _selected_thresholds(
        root / "data_lake/reports/sml_phase2b_dual_contract_2022_2025/recommended/phase2b_recommended_operating_points.csv"
    )
    batch_sel = batch_reco[batch_reco["target_column"].astype(str) == TARGET_SUCCESS].copy()
    moa_sel = moa_reco[moa_reco["target_column"].astype(str) == TARGET_SUCCESS].copy()

    points: list[LearnerPoint] = []
    for profile in [PROFILE_E0, PROFILE_P1]:
        b_row = batch_sel[batch_sel["profile"].astype(str) == profile].iloc[0]
        m_row = moa_sel[moa_sel["profile"].astype(str) == profile].iloc[0]
        points.append(
            LearnerPoint(
                system=f"Batch {'No-Year' if profile == PROFILE_E0 else 'Percent'}",
                learner="Batch",
                profile=profile,
                target=TARGET_SUCCESS,
                threshold=float(b_row["selected_threshold"]),
                oof_path=root
                / f"data_lake/reports/ml_phase2b_dual_contract_2022_2025/oof/{profile}__{TARGET_SUCCESS}.csv",
                score_column=str(b_row.get("score_column", "calibrated_proba")),
            )
        )
        points.append(
            LearnerPoint(
                system=f"MOA {'No-Year' if profile == PROFILE_E0 else 'Percent'}",
                learner="MOA",
                profile=profile,
                target=TARGET_SUCCESS,
                threshold=float(m_row["selected_threshold"]),
                oof_path=root
                / f"data_lake/reports/sml_phase2b_dual_contract_2022_2025/oof/{profile}__{TARGET_SUCCESS}.csv",
                score_column=str(m_row.get("score_column", "calibrated_proba")),
            )
        )

    learner_rows: list[dict[str, object]] = []
    for pt in points:
        comparator, oof = _build_learner_comparator(
            oof_path=pt.oof_path,
            threshold=pt.threshold,
            pit_evals=pit_evals_merged,
            universe=universe,
        )
        c = _comparator_metrics(
            comparator,
            eligible_success_keys=eligible_success_keys,
            positive_call_definition="score>=threshold",
        )
        r = _row_metrics_from_oof(oof, threshold=pt.threshold, score_col="calibrated_proba")
        ap = float(
            average_precision_score(
                pd.to_numeric(oof["target_y"], errors="coerce").fillna(0).astype(int),
                pd.to_numeric(oof["calibrated_proba"], errors="coerce").fillna(0.0).astype(float),
            )
        )
        learner_rows.append(
            {
                "system": pt.system,
                "learner": pt.learner,
                "profile": pt.profile,
                "target": pt.target,
                "threshold": float(pt.threshold),
                "score_column": "calibrated_proba",
                "predicted_positives": c["predicted_positives"],
                "TP": c["TP"],
                "FP_no_match": c["FP_no_match"],
                "FP_failure": c["FP_failure"],
                "FP_total": c["FP_total"],
                "unknown_excluded": c["unknown_excluded"],
                "strict_precision": c["strict_precision"],
                "successful_events_covered_unique": c["successful_events_covered_unique"],
                "eligible_successful_pit_events": c["eligible_successful_pit_events"],
                "successful_event_coverage": c["successful_event_coverage"],
                "FN_events": c["FN_events"],
                "F0_5": c["F0_5"],
                "AP": ap,
                "PR_curve_source_file": str(pt.oof_path.relative_to(root)),
                "row_TP": r["row_TP"],
                "row_FP": r["row_FP"],
                "row_FN": r["row_FN"],
                "row_TN": r["row_TN"],
                "row_precision": r["row_precision"],
                "row_recall": r["row_recall"],
                "row_F0_5": r["row_F0_5"],
                "notes": "",
            }
        )

    learner_df = pd.DataFrame(learner_rows).sort_values(by=["learner", "profile"]).reset_index(drop=True)
    _write_csv_md(
        learner_df,
        output_dir / "learner_pit_success_accounting_corrected.csv",
        output_dir / "learner_pit_success_accounting_corrected.md",
        "Learner pit_success_h2 Accounting (Corrected Contract)",
    )

    # 4) Apples-to-apples table.
    apples_rows: list[dict[str, object]] = []
    for _, row in sde_df.iterrows():
        apples_rows.append(
            {
                "system": "Final SDE",
                "family": "SDE",
                "profile": "c6_cfg120_fixed",
                "positive_call_definition": row["positive_call_definition"],
                "selected_threshold": "rule",
                "predicted_positives": row["predicted_positives"],
                "matched_known_pits": row["calls_matching_known_pit_outcome"],
                "no_match_calls": row["no_match_calls"],
                "matched_pit_success_rate_diagnostic": row["matched_pit_success_rate"],
                "strict_precision": row["strict_precision"],
                "successful_event_coverage": row["successful_event_coverage"],
                "F0_5": row["F0_5"],
                "AP_if_score_based": np.nan,
                "notes": "Matched-pit success rate is diagnostic, not strict precision.",
            }
        )
    for _, row in learner_df.iterrows():
        apples_rows.append(
            {
                "system": row["system"],
                "family": row["learner"],
                "profile": row["profile"],
                "positive_call_definition": "score>=threshold",
                "selected_threshold": row["threshold"],
                "predicted_positives": row["predicted_positives"],
                "matched_known_pits": row["TP"] + row["FP_failure"],
                "no_match_calls": row["FP_no_match"],
                "matched_pit_success_rate_diagnostic": _safe_div(
                    float(row["TP"]), float(row["TP"] + row["FP_failure"])
                ),
                "strict_precision": row["strict_precision"],
                "successful_event_coverage": row["successful_event_coverage"],
                "F0_5": row["F0_5"],
                "AP_if_score_based": row["AP"],
                "notes": "",
            }
        )

    apples_df = pd.DataFrame(apples_rows)
    _write_csv_md(
        apples_df,
        output_dir / "pit_success_sde_vs_learners_apples_to_apples.csv",
        output_dir / "pit_success_sde_vs_learners_apples_to_apples.md",
        "pit_success_h2 Apples-to-Apples Comparison",
    )

    # 5) Timing recap.
    runtime_path = root / "data_lake/reports/phase2b_timing/training_runtime_2022_2025.csv"
    runtime_df = pd.read_csv(runtime_path)
    runtime_df.to_csv(output_dir / "training_runtime_full_audit.csv", index=False)
    slide_runtime = runtime_df[
        (
            (runtime_df["learner"].astype(str).eq("batch"))
            & (runtime_df["stage"].astype(str).eq("train"))
            & (runtime_df["target"].astype(str).isin([TARGET_ANY, TARGET_SUCCESS]))
        )
        | (
            (runtime_df["learner"].astype(str).eq("moa"))
            & (runtime_df["stage"].astype(str).eq("train_eval"))
            & (runtime_df["target"].astype(str).isin([TARGET_ANY, TARGET_SUCCESS]))
        )
    ][
        [
            "learner",
            "profile",
            "target",
            "stage",
            "wall_seconds",
            "wall_hms",
            "command_name",
            "log_file",
            "output_file",
            "status",
        ]
    ].copy()
    slide_runtime.rename(columns={"stage": "timed_stage"}, inplace=True)
    _write_csv_md(
        slide_runtime,
        output_dir / "training_runtime_slide_table.csv",
        output_dir / "training_runtime_slide_table.md",
        "Training Runtime Slide Table",
    )

    # 6) pit_any recap.
    sde_agg = pd.read_csv(
        root / "data_lake/reports/final_sde_c6_cfg120_fixed_raw_clean_aggregate_2022_2025.csv"
    )
    sde_clean = sde_agg[sde_agg["truth_lens"].astype(str).eq("clean_actionable")].iloc[0]

    batch_reco_any = batch_reco[batch_reco["target_column"].astype(str).eq(TARGET_ANY)].copy()
    moa_reco_any = moa_reco[moa_reco["target_column"].astype(str).eq(TARGET_ANY)].copy()

    pit_any_rows: list[dict[str, object]] = [
        {
            "system": "Final SDE",
            "profile": "c6_cfg120_fixed",
            "selected_threshold_or_rule": "PIT_NOW rule",
            "precision": float(sde_clean["pit_any_precision"]),
            "recall_or_event_coverage": float(sde_clean["pit_any_recall"]),
            "F0_5": float(sde_clean["pit_any_f0_5"]),
            "AP_if_score_based": np.nan,
            "predicted_positives_or_scored_rows": int(sde_clean["pit_any_row_tp"] + sde_clean["pit_any_fp"]),
        }
    ]
    for df, family in [(batch_reco_any, "Batch"), (moa_reco_any, "MOA")]:
        for _, row in df.iterrows():
            profile = str(row["profile"])
            label = "No-Year" if profile == PROFILE_E0 else "Percent"
            pit_any_rows.append(
                {
                    "system": f"{family} {label}",
                    "profile": profile,
                    "selected_threshold_or_rule": float(row["selected_threshold"]),
                    "precision": float(row["precision"]),
                    "recall_or_event_coverage": float(row["recall"]),
                    "F0_5": float(row["f0_5"]),
                    "AP_if_score_based": float(row["AP"]),
                    "predicted_positives_or_scored_rows": int(row["predicted_positive_rows"]),
                }
            )
    pit_any_df = pd.DataFrame(pit_any_rows)
    _write_csv_md(
        pit_any_df,
        output_dir / "pit_any_final_recap.csv",
        output_dir / "pit_any_final_recap.md",
        "pit_any_h2 Final Recap",
    )

    # 7) Final slide-ready summary.
    sde_primary = sde_df[sde_df["positive_call_definition"].eq("PIT_NOW")].iloc[0]
    batch_percent = learner_df[learner_df["system"].eq("Batch Percent")].iloc[0]
    moa_percent = learner_df[learner_df["system"].eq("MOA Percent")].iloc[0]
    summary_lines = [
        "# Final Slide-Ready Summary",
        "",
        "## 1) pit_any_h2",
        (
            "pit_any_h2 asks whether any pit will happen soon. Under the canonical_sde_truth + "
            "clean_actionable protocol, the score-based Batch/MOA systems provide thresholdable "
            "timing signals, while Final SDE remains the deterministic baseline."
        ),
        "",
        "## 2) Corrected pit_success_h2 contract",
        (
            "pit_success_h2 now means successful pit soon: y=1 only when a pit occurs in [k+1,k+2] "
            "and post-pit outcome is SUCCESS_* or OFFSET_ADVANTAGE; no-pit and failure/disadvantage "
            "cases are clean negatives; unresolved/weather/unmapped outcomes are excluded from clean "
            "training/evaluation."
        ),
        "",
        "## 3) What SDE predicts vs what truth evaluates",
        (
            "SDE does not emit SUCCESS_* labels directly. It emits action labels (PIT_NOW, GOOD_PIT, "
            "etc.), then those actions are scored against future pit outcomes from pit_evals."
        ),
        "",
        "## 4) Two SDE pit_success metrics",
        (
            "Matched-pit success rate is diagnostic strategy quality (only matched known pits). "
            "Strict operational precision is the fair comparison with learners because no-match "
            "positive calls are false positives."
        ),
        "",
        "## 5) Corrected pit_success interpretation",
        (
            f"Final SDE (PIT_NOW) strict precision={float(sde_primary['strict_precision']):.4f}, "
            f"coverage={float(sde_primary['successful_event_coverage']):.4f}. "
            f"Batch Percent strict precision={float(batch_percent['strict_precision']):.4f}, "
            f"coverage={float(batch_percent['successful_event_coverage']):.4f}. "
            f"MOA Percent strict precision={float(moa_percent['strict_precision']):.4f}, "
            f"coverage={float(moa_percent['successful_event_coverage']):.4f}. "
            "Use strict precision for apples-to-apples claims; keep matched-pit success as diagnostic."
        ),
        "",
        "## 6) Slide-safe warning",
        (
            "Do not compare old matched-only SDE success rate directly to learner precision. "
            "The apples-to-apples comparison is strict operational precision where no-match "
            "recommendations are false positives."
        ),
        "",
    ]
    (output_dir / "final_slide_ready_summary.md").write_text(
        "\n".join(summary_lines), encoding="utf-8"
    )

    # 8) Assertions.
    required_outputs = [
        output_dir / "final_accounting_input_provenance.csv",
        output_dir / "final_accounting_input_provenance.md",
        output_dir / "sde_pit_success_accounting_corrected.csv",
        output_dir / "sde_pit_success_accounting_corrected.md",
        output_dir / "learner_pit_success_accounting_corrected.csv",
        output_dir / "learner_pit_success_accounting_corrected.md",
        output_dir / "pit_success_sde_vs_learners_apples_to_apples.csv",
        output_dir / "pit_success_sde_vs_learners_apples_to_apples.md",
        output_dir / "training_runtime_slide_table.csv",
        output_dir / "training_runtime_slide_table.md",
        output_dir / "training_runtime_full_audit.csv",
        output_dir / "pit_any_final_recap.csv",
        output_dir / "pit_any_final_recap.md",
        output_dir / "final_slide_ready_summary.md",
    ]
    missing = [str(p) for p in required_outputs if not p.exists()]
    if missing:
        raise RuntimeError(f"missing required outputs: {missing}")

    # stale MOA row checks.
    moa_success = provenance[
        (provenance["system"] == "MOA")
        & (provenance["target"] == TARGET_SUCCESS)
        & (provenance["artifact_role"].isin(["moa_export_csv", "moa_oof"]))
    ]
    for _, row in moa_success.iterrows():
        rows = int(pd.to_numeric(row["row_count_if_applicable"], errors="coerce"))
        if rows == 93623:
            raise RuntimeError(
                f"stale MOA pit_success artifact detected with 93623 rows: {row['path']}"
            )

    # Unknown outcomes should be non-train-eligible in prepared dataset.
    prep = pd.read_parquet(root / "data_lake/ml_training_dataset_2022_2025_dual_contract.parquet")
    result_norm = (
        prep["matched_pit_result"]
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace(" ", "_", regex=False)
    )
    unknown_mask = (
        result_norm.str.startswith("UNRESOLVED_")
        | result_norm.str.startswith("UNMAPPED_")
        | result_norm.eq("WEATHER_SURVIVAL_STOP")
    )
    leaked_unknown = prep[
        unknown_mask & prep[f"{TARGET_SUCCESS}_train_eligible"].astype(bool)
    ]
    if not leaked_unknown.empty:
        raise RuntimeError(
            f"unknown/noisy pit_success outcomes are train-eligible: {len(leaked_unknown)} rows"
        )

    # No-match must not be EXCLUDED in selected learner comparators.
    for pt in points:
        comp, _ = _build_learner_comparator(
            oof_path=pt.oof_path,
            threshold=pt.threshold,
            pit_evals=pit_evals_merged,
            universe=universe,
        )
        bad = comp[
            comp["outcome_class"].astype(str).eq("EXCLUDED")
            & comp["exclusion_reason"].fillna("").astype(str).str.upper().eq("NO_MATCH_WITHIN_HORIZON")
        ]
        if not bad.empty:
            raise RuntimeError(
                f"NO_MATCH_WITHIN_HORIZON still EXCLUDED for {pt.system}: {len(bad)} rows"
            )

    # Column naming assertions.
    apples_cols = set(apples_df.columns)
    if "matched_pit_success_rate_diagnostic" not in apples_cols:
        raise RuntimeError("apples table missing matched_pit_success_rate_diagnostic column")
    if "strict_precision" not in apples_cols:
        raise RuntimeError("apples table missing strict_precision column")

    # Phase2A fallback check.
    manifest = pd.read_csv(root / "data_lake/reports/phase2b_presentation_figures/phase2b_figures_manifest.csv")
    if manifest["source_artifact"].astype(str).str.contains("ml_phase2a", na=False).any():
        raise RuntimeError("phase2a fallback detected in current phase2b figures manifest")

    # Console summary.
    verdict = {
        "final_audit_verdict": "PASS",
        "generated_files": [str(p.relative_to(root)) for p in required_outputs],
        "apples_to_apples_preview": apples_df.to_dict(orient="records"),
        "timing_preview_rows": slide_runtime.to_dict(orient="records"),
        "warnings": [],
    }
    print(json.dumps(verdict, indent=2))


if __name__ == "__main__":
    main()
