"""Generate the thesis master markdown report from existing report artifacts.

This script does not retrain any model. It only reads already-produced artifacts
in ``data_lake/reports`` and rebuilds ``thesis_master_results_<suffix>.md``.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

SCORED_CLASSES = {"1", "0"}


@dataclass(frozen=True)
class ApproachConfig:
    name: str
    artifact_name: str
    protocol: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate thesis_master_results markdown from existing artifacts")
    parser.add_argument("--reports-dir", type=Path, default=Path("data_lake/reports"))
    parser.add_argument("--suffix", default="2022_2025_merged", help="Primary artifact suffix")
    parser.add_argument("--racewise-suffix", default="2022_2025_racewise", help="Racewise artifact suffix")
    parser.add_argument("--reference-threshold-merged", type=float, default=0.10)
    parser.add_argument("--reference-threshold-racewise", type=float, default=0.50)
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output markdown path. Defaults to reports_dir/thesis_master_results_<suffix>.md",
    )
    return parser.parse_args()


def _load_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required {label}: {path}")
    frame = pd.read_csv(path)
    if frame.empty:
        raise ValueError(f"Required {label} is empty: {path}")
    return frame


def _load_optional_csv(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    frame = pd.read_csv(path)
    if frame.empty:
        return None
    return frame


def _fmt_int(value: float | int) -> str:
    return f"{int(value):,}"


def _fmt_float(value: float, digits: int = 6) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value):.{digits}f}"


def _parse_year(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.split(" :: ", n=1).str[0], errors="coerce")


def _comparator_metrics(frame: pd.DataFrame) -> dict[str, float | int]:
    work = frame.copy()
    work["outcome_class"] = work["outcome_class"].astype(str)
    scored = work[work["outcome_class"].isin(SCORED_CLASSES)].copy()

    actionable = int(len(work))
    scored_n = int(len(scored))
    tp = int((scored["outcome_class"] == "1").sum())
    fp = int((scored["outcome_class"] == "0").sum())
    precision = float(tp / scored_n) if scored_n > 0 else float("nan")
    coverage = float(scored_n / actionable) if actionable > 0 else float("nan")
    unique_races = int(work["race"].astype(str).nunique()) if "race" in work.columns else 0

    return {
        "actionable": actionable,
        "scored": scored_n,
        "tp": tp,
        "fp": fp,
        "precision": precision,
        "coverage": coverage,
        "unique_races": unique_races,
    }


def _per_year_rows(frame: pd.DataFrame, method_name: str) -> list[dict[str, object]]:
    work = frame.copy()
    work["outcome_class"] = work["outcome_class"].astype(str)
    work["year"] = _parse_year(work["race"])
    work = work[work["year"].notna()].copy()
    if work.empty:
        return []

    rows: list[dict[str, object]] = []
    for year, group in work.groupby("year", sort=True):
        scored = group[group["outcome_class"].isin(SCORED_CLASSES)].copy()
        actionable = int(len(group))
        scored_n = int(len(scored))
        tp = int((scored["outcome_class"] == "1").sum())
        fp = int((scored["outcome_class"] == "0").sum())
        precision = float(tp / scored_n) if scored_n > 0 else float("nan")
        coverage = float(scored_n / actionable) if actionable > 0 else float("nan")
        rows.append(
            {
                "year": int(year),
                "method": method_name,
                "actionable": actionable,
                "scored": scored_n,
                "tp": tp,
                "fp": fp,
                "precision": precision,
                "coverage": coverage,
            }
        )
    return rows


def _select_best_threshold_row(sweep: pd.DataFrame) -> pd.Series:
    floor_ok = sweep[(sweep["meets_precision_floor"] == 1) & (sweep["scored"] > 0)].copy()
    if not floor_ok.empty:
        floor_ok = floor_ok.sort_values(
            by=["scored", "tp", "precision", "threshold"],
            ascending=[False, False, False, True],
        )
        return floor_ok.iloc[0]
    fallback = sweep[sweep["scored"] > 0].copy()
    if fallback.empty:
        return sweep.iloc[0]
    fallback = fallback.sort_values(
        by=["precision", "scored", "tp", "threshold"],
        ascending=[False, False, False, True],
    )
    return fallback.iloc[0]


def _closest_threshold_row(sweep: pd.DataFrame, threshold: float) -> pd.Series:
    idx = (pd.to_numeric(sweep["threshold"], errors="coerce") - float(threshold)).abs().idxmin()
    return sweep.loc[idx]


def _render_markdown(args: argparse.Namespace) -> str:
    reports_dir = args.reports_dir
    suffix = args.suffix
    racewise_suffix = args.racewise_suffix

    approaches = [
        ApproachConfig(
            "SDE",
            f"heuristic_comparator_{suffix}.csv",
            "Deterministic stream baseline, fixed H=2 comparator",
        ),
        ApproachConfig(
            "ML-pretrain-base",
            f"ml_comparator_{suffix}.csv",
            "Batch ML, expanding_race (year warmup before race-level expansion)",
        ),
        ApproachConfig(
            "ML-pretrain-extended",
            f"ml_comparator_best_threshold_{suffix}.csv",
            "Batch ML, expanding_race + threshold frontier selection",
        ),
        ApproachConfig(
            "ML-racewise-base",
            f"ml_comparator_{racewise_suffix}.csv",
            "Batch ML, expanding_race_sequential (no year pretrain)",
        ),
        ApproachConfig(
            "ML-racewise-extended",
            f"ml_comparator_best_threshold_{racewise_suffix}.csv",
            "Batch ML, expanding_race_sequential + threshold frontier selection",
        ),
        ApproachConfig(
            "MOA",
            f"moa_comparator_{suffix}.csv",
            "Streaming ML (MOA ARF) decision-mapped comparator",
        ),
    ]

    comparator_frames: dict[str, pd.DataFrame] = {}
    comparator_metrics: dict[str, dict[str, float | int]] = {}
    for cfg in approaches:
        path = reports_dir / cfg.artifact_name
        frame = _load_csv(path, f"{cfg.name} comparator")
        comparator_frames[cfg.name] = frame
        comparator_metrics[cfg.name] = _comparator_metrics(frame)

    significance_summary = _load_csv(reports_dir / f"significance_summary_{suffix}.csv", "significance summary")
    significance_tests = _load_csv(reports_dir / f"significance_tests_{suffix}.csv", "significance tests")

    merged_oof = _load_csv(reports_dir / f"ml_oof_winner_{suffix}.csv", "merged OOF")
    racewise_oof = _load_csv(reports_dir / f"ml_oof_winner_{racewise_suffix}.csv", "racewise OOF")

    merged_sweep = _load_csv(reports_dir / f"threshold_frontier_{suffix}.csv", "merged threshold frontier")
    racewise_sweep = _load_optional_csv(reports_dir / f"threshold_frontier_{racewise_suffix}.csv")
    pr_suffix = suffix[:-7] if suffix.endswith("_merged") else suffix
    pr_metrics = _load_optional_csv(reports_dir / f"pr_metrics_{pr_suffix}.csv")
    pr_operating_points = _load_optional_csv(reports_dir / f"pr_operating_points_{pr_suffix}.csv")

    moa_summary = _load_csv(reports_dir / f"moa_arf_summary_{suffix}.csv", "MOA ARF summary")
    moa_shap_summary = _load_csv(reports_dir / "moa_shap_proxy_summary.csv", "MOA SHAP proxy summary")
    moa_perm_summary = _load_csv(reports_dir / "moa_temporal_permutation_summary.csv", "MOA temporal permutation summary")
    moa_surrogate_sweep = _load_optional_csv(reports_dir / "moa_surrogate_model_sweep.csv")
    model_eval = _load_csv(reports_dir / f"model_evaluation_{suffix}.csv", "model evaluation")
    feature_parity_summary = _load_optional_csv(reports_dir / f"feature_parity_summary_{suffix}.csv")

    shap_batch = _load_csv(reports_dir / "shap_feature_importance.csv", "batch SHAP importance")
    shap_moa = _load_csv(reports_dir / "moa_shap_proxy_feature_importance.csv", "MOA SHAP proxy importance")
    perm_global = _load_csv(reports_dir / "moa_temporal_permutation_global.csv", "MOA permutation global")

    sde_row = significance_summary[significance_summary["model"] == "SDE"].iloc[0]
    ml_row = significance_summary[significance_summary["model"] == "ML"].iloc[0]
    delta_row = significance_summary[significance_summary["model"] == "ML_minus_SDE"].iloc[0]
    z_row = significance_tests[significance_tests["test"] == "two_proportion_z"].iloc[0]
    mcnemar_row = significance_tests[significance_tests["test"] == "mcnemar_cc"].iloc[0]

    merged_best = _select_best_threshold_row(merged_sweep)
    merged_ref = _closest_threshold_row(merged_sweep, args.reference_threshold_merged)
    if racewise_sweep is not None:
        racewise_best = _select_best_threshold_row(racewise_sweep)
        racewise_ref = _closest_threshold_row(racewise_sweep, args.reference_threshold_racewise)
    else:
        racewise_best = None
        racewise_ref = None

    per_year_rows: list[dict[str, object]] = []
    for cfg in approaches:
        per_year_rows.extend(_per_year_rows(comparator_frames[cfg.name], cfg.name))
    per_year_df = pd.DataFrame(per_year_rows).sort_values(["year", "method"]).reset_index(drop=True)

    def protocol_row(oof: pd.DataFrame) -> tuple[int, str, int, str, float]:
        split_protocol = ", ".join(sorted(oof["split_protocol"].astype(str).dropna().unique().tolist()))
        fold_count = int(pd.to_numeric(oof["fold"], errors="coerce").nunique())
        years = sorted(_parse_year(oof["race"]).dropna().astype(int).unique().tolist())
        years_text = ", ".join(str(year) for year in years)
        baseline_threshold = float(pd.to_numeric(oof["baseline_threshold"], errors="coerce").median())
        return int(len(oof)), split_protocol, fold_count, years_text, baseline_threshold

    merged_protocol = protocol_row(merged_oof)
    racewise_protocol = protocol_row(racewise_oof)

    moa_row = moa_summary.iloc[0]
    moa_shap_row = moa_shap_summary.iloc[0]
    moa_perm_row = moa_perm_summary.iloc[0]

    overlap_top10 = sorted(
        set(shap_moa["feature"].astype(str).head(10).tolist())
        & set(perm_global["feature"].astype(str).head(10).tolist())
    )

    def exists(name: str) -> bool:
        return (reports_dir / name).exists()

    lines: list[str] = []
    lines.append(f"# Thesis Master Results Table ({suffix.replace('_merged', '').replace('_', '-')})")
    lines.append("")
    lines.append(f"Generated at (UTC): {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append("No new training runs were executed for this report; all numbers are computed from existing artifacts in `data_lake/reports`.")
    lines.append("")
    lines.append("## 1) Master Comparison Table")
    lines.append("")
    lines.append("| Approach | Actionable | Scored | TP | FP | Precision | Scored/Actionable Coverage | Protocol |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |")
    for cfg in approaches:
        m = comparator_metrics[cfg.name]
        lines.append(
            f"| {cfg.name} | {_fmt_int(m['actionable'])} | {_fmt_int(m['scored'])} | {_fmt_int(m['tp'])} | {_fmt_int(m['fp'])} | {_fmt_float(float(m['precision']))} | {_fmt_float(float(m['coverage']))} | {cfg.protocol} |"
        )
    lines.append("")
    lines.append("Master table artifacts used:")
    for cfg in approaches:
        lines.append(f"- `{cfg.name}`: `{reports_dir / cfg.artifact_name}`")
    lines.append("")
    lines.append("## 2) How This Data Was Produced, and Why It Is Methodologically Valid")
    lines.append("")
    lines.append("Data generation chain (already executed):")
    lines.append("- Batch ML: OOF probabilities from `train_model.py` with leakage-safe grouped structures, then comparator mapping under fixed horizon `H=2` and one-to-one actionable matching.")
    lines.append("- Extended reachability variants: threshold frontier sweep from OOF probabilities, then comparator rebuild at selected threshold (no retraining).")
    lines.append("- MOA: ARF prequential run on exported MOA dataset, decoded predictions mapped into the same comparator contract used by SDE/ML.")
    lines.append("")
    lines.append("Academic and methodological backing used in this pipeline:")
    lines.append("- Split/leakage rigor: grouped race-level separation and temporal constraints (Roberts 2017, Brookshire 2024).")
    lines.append("- Imbalance-aware objective and precision-centric analysis: class-weighted framing and PR-oriented interpretation (Elkan 2001, Saito and Rehmsmeier 2015, Davis and Goadrich 2006).")
    lines.append("- Significance reporting: two-proportion z test as primary evidence and overlap McNemar as paired sensitivity (Dietterich 1998, Walters 2022).")
    lines.append("- Calibration reliability and deployment validity gates: Brier/calibration policy checks, train-serve parity, latency/availability checks before operational claims.")
    lines.append("")
    lines.append("Key inferential evidence (SDE vs ML pretrain-base comparator):")
    lines.append(f"- SDE precision: {_fmt_float(float(sde_row['precision']))}, ML precision: {_fmt_float(float(ml_row['precision']))}, delta: {_fmt_float(float(delta_row['precision']))}")
    lines.append(f"- Two-proportion z statistic: {_fmt_float(float(z_row['statistic']))}, p-value: {'<1e-16 (underflow)' if float(z_row['p_value']) == 0.0 else _fmt_float(float(z_row['p_value']))}")
    lines.append(f"- McNemar cc statistic: {_fmt_float(float(mcnemar_row['statistic']))}, p-value: {_fmt_float(float(mcnemar_row['p_value']))}")
    lines.append(f"- Source files: `{reports_dir / f'significance_summary_{suffix}.csv'}`, `{reports_dir / f'significance_tests_{suffix}.csv'}`")
    lines.append("")
    lines.append("## 3) Batch ML Protocol Differentiation (Pretrain-Year vs Pure Racewise)")
    lines.append("")
    lines.append("| Variant | OOF Rows | Split Protocol | Fold Count | Test Years Present | Baseline Threshold |")
    lines.append("| --- | ---: | --- | ---: | --- | ---: |")
    lines.append(
        f"| ML-pretrain-base | {_fmt_int(merged_protocol[0])} | {merged_protocol[1]} | {_fmt_int(merged_protocol[2])} | {merged_protocol[3]} | {_fmt_float(merged_protocol[4], 2)} |"
    )
    lines.append(
        f"| ML-racewise-base | {_fmt_int(racewise_protocol[0])} | {racewise_protocol[1]} | {_fmt_int(racewise_protocol[2])} | {racewise_protocol[3]} | {_fmt_float(racewise_protocol[4], 2)} |"
    )
    lines.append("")
    lines.append("Protocol note: on current artifacts, pretrain and racewise OOF predictions are identical on 2023-2025 rows; racewise extends evaluation coverage by adding 2022.")
    lines.append("")
    lines.append("Extended reachability (threshold frontier, no retraining):")
    lines.append("| Variant | Selected Threshold | Reference Threshold | Selected Precision | Reference Precision | Sweep Report |")
    lines.append("| --- | ---: | ---: | ---: | ---: | --- |")
    lines.append(
        f"| ML-pretrain-extended | {_fmt_float(float(merged_best['threshold']), 3)} | {_fmt_float(float(merged_ref['threshold']), 3)} | {_fmt_float(float(merged_best['precision']))} | {_fmt_float(float(merged_ref['precision']))} | `{reports_dir / f'threshold_frontier_report_{suffix}.txt'}` |"
    )
    if racewise_best is not None and racewise_ref is not None:
        racewise_threshold = _fmt_float(float(racewise_best["threshold"]), 3)
        racewise_ref_threshold = _fmt_float(float(racewise_ref["threshold"]), 3)
        racewise_precision = _fmt_float(float(racewise_best["precision"]))
        racewise_ref_precision = _fmt_float(float(racewise_ref["precision"]))
    else:
        racewise_threshold = "N/A"
        racewise_ref_threshold = "N/A"
        racewise_precision = _fmt_float(float(comparator_metrics["ML-racewise-extended"]["precision"]))
        racewise_ref_precision = _fmt_float(float(comparator_metrics["ML-racewise-base"]["precision"]))
    racewise_report_path = reports_dir / f"threshold_frontier_report_{racewise_suffix}.txt"
    racewise_report_text = str(racewise_report_path) if racewise_report_path.exists() else "not available (no racewise sweep artifact)"
    lines.append(
        f"| ML-racewise-extended | {racewise_threshold} | {racewise_ref_threshold} | {racewise_precision} | {racewise_ref_precision} | `{racewise_report_text}` |"
    )
    lines.append("")
    lines.append("## 3.1) OOF Discrimination Evidence (PR Curves and PR-AUC)")
    lines.append("")
    lines.append("This section reports **OOF row-level classification discrimination** from probability outputs (`target_y` vs model probabilities).")
    lines.append("It is methodologically distinct from comparator precision tables, which evaluate decision-level matching under the fixed `H=2` contract.")
    lines.append("")
    if pr_metrics is not None:
        pr_overall = pr_metrics[pr_metrics["year"].astype(str) == "overall"].copy()
        if not pr_overall.empty:
            lines.append("| Protocol | Score Type | PR-AUC (AP) | PR-AUC (Trapz) | Prevalence | Rows | Positives |")
            lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: |")
            for _, row in pr_overall.sort_values(["protocol", "score_type"]).iterrows():
                lines.append(
                    f"| {row['protocol']} | {row['score_type']} | {_fmt_float(float(row['pr_auc_ap']))} | {_fmt_float(float(row['pr_auc_trapz']))} | {_fmt_float(float(row['prevalence']))} | {_fmt_int(float(row['n_rows']))} | {_fmt_int(float(row['n_positive']))} |"
                )
            lines.append("")
        pr_by_year = pr_metrics[
            (pr_metrics["year"].astype(str) != "overall")
            & (pr_metrics["score_type"].astype(str) == "calibrated_proba")
        ].copy()
        if not pr_by_year.empty:
            lines.append("Per-year PR-AUC (calibrated probabilities):")
            lines.append("| Protocol | Year | PR-AUC (AP) | Prevalence | Rows | Positives |")
            lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")
            for _, row in pr_by_year.sort_values(["protocol", "year"]).iterrows():
                lines.append(
                    f"| {row['protocol']} | {int(float(row['year']))} | {_fmt_float(float(row['pr_auc_ap']))} | {_fmt_float(float(row['prevalence']))} | {_fmt_int(float(row['n_rows']))} | {_fmt_int(float(row['n_positive']))} |"
                )
            lines.append("")
        lines.append(f"- PR metrics artifact: `{reports_dir / f'pr_metrics_{pr_suffix}.csv'}`")
    else:
        lines.append("- PR metrics artifact not found. Run `ml_pipeline/plot_discrimination_curves.py` to generate it.")
    if pr_operating_points is not None:
        lines.append(f"- PR operating points artifact: `{reports_dir / f'pr_operating_points_{pr_suffix}.csv'}`")
    lines.append("")
    if exists(f"pr_curves_overall_{pr_suffix}.png"):
        lines.append(f"![PR curves overall](pr_curves_overall_{pr_suffix}.png)")
    if exists(f"pr_curves_by_year_{pr_suffix}.png"):
        lines.append(f"![PR curves by year](pr_curves_by_year_{pr_suffix}.png)")
    if exists(f"pr_curves_panel_{pr_suffix}.png"):
        lines.append(f"![PR curves panel](pr_curves_panel_{pr_suffix}.png)")
    lines.append("")
    lines.append("## 4) Per-Year Comparison Across the Three Paradigms (plus Batch ML variants)")
    lines.append("")
    lines.append("| Year | Method | Actionable | Scored | TP | FP | Precision | Coverage |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for _, row in per_year_df.iterrows():
        lines.append(
            f"| {int(row['year'])} | {row['method']} | {_fmt_int(row['actionable'])} | {_fmt_int(row['scored'])} | {_fmt_int(row['tp'])} | {_fmt_int(row['fp'])} | {_fmt_float(float(row['precision']))} | {_fmt_float(float(row['coverage']))} |"
        )
    lines.append("")
    lines.append("## 4.1) Per-Race Access (Raw Decision-Level Comparator Rows)")
    lines.append("")
    lines.append("| Method | Unique Races Covered | Raw Per-Race Artifact |")
    lines.append("| --- | ---: | --- |")
    for cfg in approaches:
        lines.append(
            f"| {cfg.name} | {_fmt_int(comparator_metrics[cfg.name]['unique_races'])} | `{reports_dir / cfg.artifact_name}` |"
        )
    lines.append("")
    lines.append("## 5) MOA Additional Baseline Context (Prequential Stream Metrics)")
    lines.append("")
    lines.append("| Metric | Value | Source |")
    lines.append("| --- | ---: | --- |")
    lines.append(f"| EvaluatePrequential instances | {_fmt_int(float(moa_row['final_classified instances']))} | `{reports_dir / f'moa_arf_summary_{suffix}.csv'}` |")
    lines.append(f"| Final accuracy (%) | {_fmt_float(float(moa_row['final_classifications correct (percent)']))} | `{reports_dir / f'moa_arf_summary_{suffix}.csv'}` |")
    lines.append(f"| Final kappa (%) | {_fmt_float(float(moa_row['final_Kappa Statistic (percent)']))} | `{reports_dir / f'moa_arf_summary_{suffix}.csv'}` |")
    lines.append("")
    lines.append("Note: Prequential metrics are stream-learning diagnostics and are reported separately from comparator-precision metrics.")
    lines.append("")
    lines.append("## 6) SHAP and Explainability Analysis Across Models")
    lines.append("")
    lines.append("Interpretability availability by paradigm:")
    lines.append("- SDE: deterministic rule system, interpretable by rule logic and comparator diagnostics, SHAP not applicable.")
    lines.append("- Batch ML: direct TreeSHAP on serving-bundle gradient-boosted model.")
    lines.append("- MOA: surrogate explainability (SHAP proxy + temporal permutation) on decoded MOA decisions, with explicit fidelity caveat.")
    lines.append("")
    if moa_surrogate_sweep is not None:
        lines.append("MOA surrogate model sweep (fixed holdout protocol):")
        lines.append("| Rank | Model ID | Family | F1 | Accuracy | Precision | Recall | Balanced Accuracy | Selected |")
        lines.append("| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |")
        for _, row in moa_surrogate_sweep.sort_values("rank").iterrows():
            lines.append(
                f"| {int(row['rank'])} | {row['model_id']} | {row['family']} | {_fmt_float(float(row['fidelity_f1']))} | {_fmt_float(float(row['fidelity_accuracy']))} | {_fmt_float(float(row['fidelity_precision']))} | {_fmt_float(float(row['fidelity_recall']))} | {_fmt_float(float(row['fidelity_balanced_accuracy']))} | {'yes' if int(row['selected']) == 1 else 'no'} |"
            )
        lines.append("")
        lines.append(f"- Surrogate sweep artifact: `{reports_dir / 'moa_surrogate_model_sweep.csv'}`")
        lines.append("")

    lines.append("Batch ML SHAP top features (`shap_feature_importance.csv`):")
    lines.append("| Rank | Feature | Mean abs SHAP |")
    lines.append("| --- | --- | ---: |")
    batch_top10 = shap_batch.sort_values("mean_abs_shap", ascending=False).head(10).reset_index(drop=True)
    for idx, row in batch_top10.iterrows():
        lines.append(f"| {idx + 1} | {row['feature']} | {_fmt_float(float(row['mean_abs_shap']))} |")
    lines.append("")
    lines.append("MOA surrogate SHAP proxy top features (`moa_shap_proxy_feature_importance.csv`):")
    lines.append("| Rank | Feature | Mean abs SHAP |")
    lines.append("| --- | --- | ---: |")
    for idx, row in shap_moa.head(10).reset_index(drop=True).iterrows():
        lines.append(f"| {idx + 1} | {row['feature']} | {_fmt_float(float(row['mean_abs_shap']))} |")
    lines.append("")
    lines.append("MOA temporal permutation top features (`moa_temporal_permutation_global.csv`):")
    lines.append("| Rank | Feature | Mean F1 Drop |")
    lines.append("| --- | --- | ---: |")
    for idx, row in perm_global.head(10).reset_index(drop=True).iterrows():
        lines.append(f"| {idx + 1} | {row['feature']} | {_fmt_float(float(row['importance_drop_f1']))} |")
    lines.append("")
    lines.append("MOA explainability fidelity diagnostics:")
    lines.append(
        f"- SHAP proxy fidelity accuracy: {_fmt_float(float(moa_shap_row['fidelity_accuracy']))}, fidelity F1: {_fmt_float(float(moa_shap_row['fidelity_f1']))}, rows used: {_fmt_int(float(moa_shap_row['rows_used']))}"
    )
    lines.append(
        f"- Temporal permutation surrogate fidelity accuracy: {_fmt_float(float(moa_perm_row['fidelity_accuracy']))}, fidelity F1: {_fmt_float(float(moa_perm_row['fidelity_f1']))}, rows used: {_fmt_int(float(moa_perm_row['rows_used']))}"
    )
    lines.append(f"- Top-10 overlap between MOA SHAP proxy and MOA permutation: {', '.join(overlap_top10) if overlap_top10 else 'none'}")
    lines.append("")
    lines.append("SHAP visual artifacts:")
    lines.append("")
    lines.append("Batch ML SHAP:")
    if exists("shap_global_bar.png"):
        lines.append("![Batch ML SHAP global bar](shap_global_bar.png)")
    if exists("shap_beeswarm.png"):
        lines.append("![Batch ML SHAP beeswarm](shap_beeswarm.png)")
    if exists("shap_dependence__source_year.png"):
        lines.append("![Batch ML SHAP dependence source year](shap_dependence__source_year.png)")
    lines.append("")
    lines.append("MOA SHAP proxy:")
    if exists("moa_shap_proxy_global_bar.png"):
        lines.append("![MOA SHAP proxy global bar](moa_shap_proxy_global_bar.png)")
    if exists("moa_shap_proxy_beeswarm.png"):
        lines.append("![MOA SHAP proxy beeswarm](moa_shap_proxy_beeswarm.png)")
    if exists("moa_shap_proxy_dependence__source_year.png"):
        lines.append("![MOA SHAP proxy dependence source year](moa_shap_proxy_dependence__source_year.png)")
    lines.append("")
    if exists("moa_temporal_permutation_heatmap.pdf"):
        lines.append("MOA temporal permutation heatmap:")
        lines.append("- [moa_temporal_permutation_heatmap.pdf](moa_temporal_permutation_heatmap.pdf)")
        lines.append("")
    lines.append("## 6.1) `_source_year` Deployment Note (No Retraining)")
    lines.append("")
    lines.append("`_source_year` appears as a top-ranked feature in both Batch SHAP and MOA surrogate explainability artifacts.")
    lines.append("The current pipeline injects this feature both offline and online, and no retraining is required for this note.")
    lines.append("")
    lines.append("Technical validation from existing code and artifacts:")
    lines.append("- Offline feature construction: `_source_year` is generated in `ml_pipeline/lib/data_preparation.py` and persisted in datasets/OOF artifacts.")
    lines.append("- Online serving path: `_source_year` is derived from race metadata in `ml_pipeline/lib/live_kafka_inference.py` (`_source_year_from_race`).")
    if feature_parity_summary is not None and not feature_parity_summary.empty:
        parity_gate = feature_parity_summary[feature_parity_summary["check"] == "feature_parity_overall_gate"]
        if not parity_gate.empty:
            row = parity_gate.iloc[0]
            threshold_text = "N/A"
            if "threshold" in row.index and not pd.isna(row["threshold"]):
                threshold_text = _fmt_float(float(row["threshold"]))
            lines.append(
                f"- Train-serve parity gate on current artifacts: `{row['status']}` (value={_fmt_float(float(row['value']))}, threshold={threshold_text})."
            )
    lines.append("- Tree-based serving models accept unseen numeric year values at inference without invalidating rows; decisions follow learned split thresholds.")
    lines.append("- Future hardening option (deferred): replace absolute year with `years_since_2022` to make extrapolation semantics explicit.")
    lines.append("")
    lines.append("## 7) Deployment-Readiness and Validity Gates (Current Artifact Snapshot)")
    lines.append("")
    lines.append("| Test ID | Name | Status | Metric | Value | Threshold | Artifact |")
    lines.append("| --- | --- | --- | --- | ---: | ---: | --- |")
    for _, row in model_eval.iterrows():
        value_txt = "N/A" if pd.isna(row["value"]) else _fmt_float(float(row["value"]))
        threshold_txt = "N/A" if pd.isna(row["threshold"]) else _fmt_float(float(row["threshold"]))
        lines.append(
            f"| {row['test_id']} | {row['test_name']} | {row['status']} | {row['metric']} | {value_txt} | {threshold_txt} | {row['artifact']} |"
        )
    lines.append("")
    lines.append("Caveat on split-integrity: `oof_fold_count_match` assumes grouped-kfold fold cardinality and will fail for expanding protocols with many sequential test folds, despite race-overlap checks passing.")
    lines.append("")
    lines.append("## 8) Professor-Friendly Naming Guide")
    lines.append("")
    lines.append("| Current Generator / Report Family | Simple Name for Discussion | One-Sentence Explanation |")
    lines.append("| --- | --- | --- |")
    lines.append("| `significance*`, `sde_ml_comparison*` | Comparative Precision Significance | Checks whether ML truly beats SDE under the same decision contract and whether that gain is statistically credible. |")
    lines.append("| `threshold_frontier*` | Threshold Reachability Frontier | Shows precision-versus-coverage tradeoff when relaxing threshold to increase actionable reach. |")
    lines.append("| `calibration_policy*` | Calibration and Policy Reliability | Verifies probability quality and constrained-policy stability under precision-floor constraints. |")
    lines.append("| `feature_parity*` | Train-Serve Feature Parity | Confirms online feature pipeline matches offline training features to avoid hidden skew. |")
    lines.append("| `live_latency*`, `live_availability*`, `live_overhead*` | Real-Time Runtime Feasibility | Quantifies latency and availability to verify operational viability. |")
    lines.append("| `integrated_gate*` | Integrated Go/No-Go Decision | Aggregates evidence from significance/threshold/calibration/parity/runtime into one deployment-readiness decision. |")
    lines.append("| `split_integrity*`, `comparator_invariance*` | Protocol Closure Audits | Final audit that checks split integrity and comparator fairness invariants before thesis claims. |")
    lines.append("")
    lines.append("Plain-language explanation of comparative significance for your professor:")
    lines.append('- "This is the statistical fairness check: SDE and ML are scored under the exact same matching rules, then we test whether the precision difference is real or just noise."')
    lines.append("")
    lines.append("## 9) Recommended Citation Anchors in Thesis Narrative")
    lines.append("")
    lines.append("- Split integrity and temporal leakage avoidance: Roberts et al. (2017), Brookshire et al. (2024).")
    lines.append("- Imbalance-aware precision-first evaluation: Elkan (2001), Saito and Rehmsmeier (2015), Davis and Goadrich (2006).")
    lines.append("- Comparative test rigor and uncertainty framing: Dietterich (1998), Walters (2022).")
    lines.append("- Calibration reporting and probability validity framing: Brier (1950), Platt (1999), Guo et al. (2017), Kull et al. (2017).")
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    args = _parse_args()
    if args.output is not None:
        output = args.output
    else:
        normalized_suffix = args.suffix[:-7] if args.suffix.endswith("_merged") else args.suffix
        output = args.reports_dir / f"thesis_master_results_{normalized_suffix}.md"
    markdown = _render_markdown(args)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(markdown + "\n", encoding="utf-8")
    print("=== THESIS MASTER REPORT GENERATED ===")
    print(f"output md: {output}")


if __name__ == "__main__":
    main()
