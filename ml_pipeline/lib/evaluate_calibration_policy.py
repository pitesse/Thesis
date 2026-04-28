"""Run calibration and decision-policy diagnostics on merged artifacts.

this script provides reliability and policy diagnostics using the frozen merged
OOF winner outputs and ablation summary. it reports:
- brier, ece, and mce for raw and calibrated probabilities,
- precision-floor reachability and fallback diagnostics from ablation output,
- constrained-policy behavior versus fixed reference thresholds.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

DEFAULT_OOF = "data_lake/reports/ml_oof_winner_2022_2025_merged.csv"
DEFAULT_ABLATION = "data_lake/reports/ml_ablation_phase31c_2022_2025_merged.csv"
DEFAULT_SUMMARY_OUTPUT = "data_lake/reports/calibration_policy_summary_2022_2025_merged.csv"
DEFAULT_RELIABILITY_OUTPUT = "data_lake/reports/calibration_reliability_bins_2022_2025_merged.csv"
DEFAULT_BY_YEAR_OUTPUT = "data_lake/reports/calibration_policy_by_year_2022_2025_merged.csv"
DEFAULT_BY_FOLD_OUTPUT = "data_lake/reports/calibration_policy_by_fold_2022_2025_merged.csv"
DEFAULT_REFERENCE_OUTPUT = "data_lake/reports/calibration_policy_reference_thresholds_2022_2025_merged.csv"
DEFAULT_REPORT_OUTPUT = "data_lake/reports/calibration_policy_report_2022_2025_merged.txt"

RAW_PROBA_COLUMN = "raw_proba"
CALIBRATED_PROBA_COLUMN = "calibrated_proba"


def _safe_div(num: float, den: float) -> float:
    if den <= 0:
        return float("nan")
    return float(num / den)


def _binary_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float | int]:
    y_true_int = y_true.astype(int)
    y_pred_int = y_pred.astype(int)

    tp = int(((y_true_int == 1) & (y_pred_int == 1)).sum())
    tn = int(((y_true_int == 0) & (y_pred_int == 0)).sum())
    fp = int(((y_true_int == 0) & (y_pred_int == 1)).sum())
    fn = int(((y_true_int == 1) & (y_pred_int == 0)).sum())

    predicted_positive = tp + fp
    precision = _safe_div(tp, predicted_positive)
    recall = _safe_div(tp, tp + fn)
    if np.isnan(precision) or np.isnan(recall) or (precision + recall) <= 0:
        f1 = float("nan")
    else:
        f1 = float((2.0 * precision * recall) / (precision + recall))

    return {
        "rows": int(len(y_true_int)),
        "predicted_positive": int(predicted_positive),
        "tp": tp,
        "fp": fp,
        "tn": tn,
        "fn": fn,
        "positive_rate": _safe_div(predicted_positive, len(y_true_int)),
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
    }


def _reliability_bins(
    y_true: np.ndarray,
    proba: np.ndarray,
    n_bins: int,
    score_type: str,
    scope: str,
) -> tuple[list[dict[str, float | int | str]], float, float, float]:
    clipped = np.clip(proba.astype(float), 0.0, 1.0)
    y = y_true.astype(int)
    total = len(y)

    if total <= 0:
        return [], float("nan"), float("nan"), float("nan")

    edges = np.linspace(0.0, 1.0, n_bins + 1)
    bin_idx = np.digitize(clipped, edges[1:-1], right=False)

    rows: list[dict[str, float | int | str]] = []
    ece = 0.0
    mce = 0.0

    for idx in range(n_bins):
        lower = float(edges[idx])
        upper = float(edges[idx + 1])
        mask = bin_idx == idx
        count = int(mask.sum())
        weight = float(count / total)

        if count == 0:
            mean_pred = float("nan")
            observed_rate = float("nan")
            abs_gap = float("nan")
        else:
            mean_pred = float(clipped[mask].mean())
            observed_rate = float(y[mask].mean())
            abs_gap = abs(mean_pred - observed_rate)
            ece += weight * abs_gap
            mce = max(mce, abs_gap)

        rows.append(
            {
                "scope": scope,
                "score_type": score_type,
                "bin_index": int(idx),
                "bin_lower": lower,
                "bin_upper": upper,
                "count": count,
                "weight": weight,
                "mean_pred": mean_pred,
                "observed_rate": observed_rate,
                "abs_gap": abs_gap,
            }
        )

    brier = float(np.mean((clipped - y) ** 2))
    return rows, float(ece), float(mce), brier


def _load_oof(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"OOF file not found: {path}")

    df = pd.read_csv(path)
    required = {
        "race",
        "fold",
        "target_y",
        RAW_PROBA_COLUMN,
        CALIBRATED_PROBA_COLUMN,
        "constrained_pred",
        "constrained_threshold",
        "constrained_mode",
    }
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"OOF file is missing required columns: {missing}")

    work = df.copy()
    work["race"] = work["race"].astype(str)
    work["year"] = work["race"].str.split(" :: ", n=1).str[0]
    work["fold"] = pd.to_numeric(work["fold"], errors="coerce")
    work["target_y"] = pd.to_numeric(work["target_y"], errors="coerce")
    work[RAW_PROBA_COLUMN] = pd.to_numeric(work[RAW_PROBA_COLUMN], errors="coerce")
    work[CALIBRATED_PROBA_COLUMN] = pd.to_numeric(work[CALIBRATED_PROBA_COLUMN], errors="coerce")
    work["constrained_pred"] = pd.to_numeric(work["constrained_pred"], errors="coerce")
    work["constrained_threshold"] = pd.to_numeric(work["constrained_threshold"], errors="coerce")

    work = work[
        work[[
            "fold",
            "target_y",
            RAW_PROBA_COLUMN,
            CALIBRATED_PROBA_COLUMN,
            "constrained_pred",
            "constrained_threshold",
        ]].notna().all(axis=1)
    ].copy()

    work["fold"] = work["fold"].astype(int)
    work["target_y"] = work["target_y"].astype(int)
    work["constrained_pred"] = work["constrained_pred"].astype(int)

    return work


def _load_ablation_winner(path: Path) -> pd.Series:
    if not path.exists():
        raise FileNotFoundError(f"ablation file not found: {path}")

    df = pd.read_csv(path)
    if df.empty:
        raise ValueError("ablation file is empty")

    sort_spec = [
        ("mean_pr_auc", False),
        ("mean_pr_auc_lift_raw", False),
        ("mean_constrained_f1", False),
        ("mean_unconstrained_f1", False),
        ("mean_constrained_precision", False),
        ("mean_brier", True),
        ("fallback_rate", True),
    ]

    available_columns = [column for column, _ in sort_spec if column in df.columns]
    if available_columns:
        ascending = [asc for column, asc in sort_spec if column in df.columns]
        ordered = df.sort_values(by=available_columns, ascending=ascending).reset_index(drop=True)
        return ordered.iloc[0]

    return df.iloc[0]


def _extract_winner_metric(row: pd.Series, column: str) -> float:
    if column not in row.index:
        return float("nan")
    value = pd.to_numeric(row[column], errors="coerce")
    if pd.isna(value):
        return float("nan")
    return float(value)


def _policy_row(
    y_true: np.ndarray,
    proba: np.ndarray,
    threshold: float,
    label: str,
) -> dict[str, float | int | str]:
    pred = (proba >= threshold).astype(int)
    metrics = _binary_metrics(y_true, pred)
    return {
        "policy_label": label,
        "threshold": float(threshold),
        **metrics,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="run calibration and decision-policy diagnostics")
    parser.add_argument("--oof", default=DEFAULT_OOF, help="winner OOF decisions csv")
    parser.add_argument("--ablation", default=DEFAULT_ABLATION, help="ablation leaderboard csv")
    parser.add_argument("--n-bins", type=int, default=10, help="number of reliability bins")
    parser.add_argument(
        "--reference-thresholds",
        type=float,
        nargs="*",
        default=[0.10, 0.50],
        help="fixed calibrated-probability thresholds for policy comparison",
    )
    parser.add_argument("--summary-output", default=DEFAULT_SUMMARY_OUTPUT, help="summary csv output")
    parser.add_argument("--reliability-output", default=DEFAULT_RELIABILITY_OUTPUT, help="reliability bins csv output")
    parser.add_argument("--by-year-output", default=DEFAULT_BY_YEAR_OUTPUT, help="by-year policy csv output")
    parser.add_argument("--by-fold-output", default=DEFAULT_BY_FOLD_OUTPUT, help="by-fold policy csv output")
    parser.add_argument("--reference-output", default=DEFAULT_REFERENCE_OUTPUT, help="reference threshold policy csv output")
    parser.add_argument("--report-output", default=DEFAULT_REPORT_OUTPUT, help="plain-text report output")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.n_bins < 2:
        raise ValueError("--n-bins must be at least 2")

    for threshold in args.reference_thresholds:
        if not (0.0 < threshold < 1.0):
            raise ValueError("all --reference-thresholds must satisfy 0 < threshold < 1")

    oof_path = Path(args.oof)
    ablation_path = Path(args.ablation)

    oof = _load_oof(oof_path)
    winner = _load_ablation_winner(ablation_path)

    y = oof["target_y"].to_numpy(dtype=int)
    raw_proba = oof[RAW_PROBA_COLUMN].to_numpy(dtype=float)
    calibrated_proba = oof[CALIBRATED_PROBA_COLUMN].to_numpy(dtype=float)
    constrained_pred = oof["constrained_pred"].to_numpy(dtype=int)

    rel_rows_raw, raw_ece, raw_mce, raw_brier = _reliability_bins(
        y_true=y,
        proba=raw_proba,
        n_bins=args.n_bins,
        score_type="raw",
        scope="overall",
    )
    rel_rows_cal, cal_ece, cal_mce, cal_brier = _reliability_bins(
        y_true=y,
        proba=calibrated_proba,
        n_bins=args.n_bins,
        score_type="calibrated",
        scope="overall",
    )
    reliability_rows = rel_rows_raw + rel_rows_cal

    # constrained_pred is the fold-tuned deployment policy output from phase 3.1C.
    constrained_metrics = _binary_metrics(y_true=y, y_pred=constrained_pred)

    mode_counts = oof["constrained_mode"].astype(str).value_counts().to_dict()
    fallback_rows_oof = int((oof["constrained_mode"].astype(str) == "fallback_max_precision").sum())

    reference_rows: list[dict[str, float | int | str]] = []
    for threshold in sorted(set(float(x) for x in args.reference_thresholds)):
        # fixed-threshold rows provide interpretable baselines against constrained policy.
        reference_rows.append(
            _policy_row(
                y_true=y,
                proba=calibrated_proba,
                threshold=threshold,
                label=f"fixed_threshold_{threshold:.2f}",
            )
        )

    constrained_policy_row = {
        "policy_label": "constrained_policy",
        "threshold": float("nan"),
        **constrained_metrics,
    }
    reference_with_constrained = pd.DataFrame([constrained_policy_row] + reference_rows)

    threshold_mean = float(oof["constrained_threshold"].mean())
    threshold_min = float(oof["constrained_threshold"].min())
    threshold_max = float(oof["constrained_threshold"].max())
    threshold_p10 = float(oof["constrained_threshold"].quantile(0.10))
    threshold_p90 = float(oof["constrained_threshold"].quantile(0.90))

    summary_row = {
        "oof_rows": int(len(oof)),
        "oof_positive_rows": int((oof["target_y"] == 1).sum()),
        "oof_prevalence": float((oof["target_y"] == 1).mean()),
        "raw_brier": raw_brier,
        "calibrated_brier": cal_brier,
        "brier_delta_cal_minus_raw": float(cal_brier - raw_brier),
        "raw_ece": raw_ece,
        "calibrated_ece": cal_ece,
        "ece_delta_cal_minus_raw": float(cal_ece - raw_ece),
        "raw_mce": raw_mce,
        "calibrated_mce": cal_mce,
        "mce_delta_cal_minus_raw": float(cal_mce - raw_mce),
        "constrained_positive_rate": float(constrained_metrics["positive_rate"]),
        "constrained_precision": float(constrained_metrics["precision"]),
        "constrained_recall": float(constrained_metrics["recall"]),
        "constrained_f1": float(constrained_metrics["f1"]),
        "constrained_predicted_positive": int(constrained_metrics["predicted_positive"]),
        "constrained_threshold_mean": threshold_mean,
        "constrained_threshold_min": threshold_min,
        "constrained_threshold_max": threshold_max,
        "constrained_threshold_p10": threshold_p10,
        "constrained_threshold_p90": threshold_p90,
        "oof_mode_fallback_rows": fallback_rows_oof,
        "oof_mode_fallback_rate": _safe_div(fallback_rows_oof, len(oof)),
        "oof_mode_counts": ";".join([f"{key}:{value}" for key, value in mode_counts.items()]),
        "winner_config_id": _extract_winner_metric(winner, "config_id"),
        "winner_mean_constrained_threshold": _extract_winner_metric(winner, "mean_constrained_threshold"),
        "winner_mean_constrained_margin_to_floor": _extract_winner_metric(winner, "mean_constrained_margin_to_floor"),
        "winner_mean_precision_floor_reachable_ratio": _extract_winner_metric(
            winner, "mean_precision_floor_reachable_ratio"
        ),
        "winner_precision_floor_reachable_folds": _extract_winner_metric(winner, "precision_floor_reachable_folds"),
        "winner_fallback_folds": _extract_winner_metric(winner, "fallback_folds"),
        "winner_total_folds": _extract_winner_metric(winner, "total_folds"),
        "winner_fallback_rate": _extract_winner_metric(winner, "fallback_rate"),
        "winner_mean_constrained_utility": _extract_winner_metric(winner, "mean_constrained_utility"),
        "winner_mean_brier": _extract_winner_metric(winner, "mean_brier"),
        "winner_calibration_mix": str(winner.get("calibration_mix", "")),
    }

    # Add deltas against reference thresholds to quantify conservative policy behavior.
    for row in reference_rows:
        label = str(row["policy_label"])
        rate_key = f"delta_positive_rate_constrained_minus_{label}"
        precision_key = f"delta_precision_constrained_minus_{label}"
        recall_key = f"delta_recall_constrained_minus_{label}"
        summary_row[rate_key] = float(constrained_metrics["positive_rate"] - row["positive_rate"])
        summary_row[precision_key] = float(constrained_metrics["precision"] - row["precision"])
        summary_row[recall_key] = float(constrained_metrics["recall"] - row["recall"])

    by_year_rows: list[dict[str, float | int | str]] = []
    for year, group in oof.groupby("year"):
        y_year = group["target_y"].to_numpy(dtype=int)
        raw_year = group[RAW_PROBA_COLUMN].to_numpy(dtype=float)
        cal_year = group[CALIBRATED_PROBA_COLUMN].to_numpy(dtype=float)
        pred_year = group["constrained_pred"].to_numpy(dtype=int)

        _, raw_ece_y, raw_mce_y, raw_brier_y = _reliability_bins(
            y_true=y_year,
            proba=raw_year,
            n_bins=args.n_bins,
            score_type="raw",
            scope=f"year_{year}",
        )
        _, cal_ece_y, cal_mce_y, cal_brier_y = _reliability_bins(
            y_true=y_year,
            proba=cal_year,
            n_bins=args.n_bins,
            score_type="calibrated",
            scope=f"year_{year}",
        )

        constrained_year = _binary_metrics(y_true=y_year, y_pred=pred_year)

        by_year_rows.append(
            {
                "year": str(year),
                "rows": int(len(group)),
                "positive_rows": int((group["target_y"] == 1).sum()),
                "prevalence": float((group["target_y"] == 1).mean()),
                "raw_brier": raw_brier_y,
                "calibrated_brier": cal_brier_y,
                "raw_ece": raw_ece_y,
                "calibrated_ece": cal_ece_y,
                "raw_mce": raw_mce_y,
                "calibrated_mce": cal_mce_y,
                "constrained_positive_rate": float(constrained_year["positive_rate"]),
                "constrained_precision": float(constrained_year["precision"]),
                "constrained_recall": float(constrained_year["recall"]),
                "constrained_f1": float(constrained_year["f1"]),
                "mean_constrained_threshold": float(group["constrained_threshold"].mean()),
                "fallback_rows": int((group["constrained_mode"].astype(str) == "fallback_max_precision").sum()),
            }
        )

    by_fold_rows: list[dict[str, float | int | str]] = []
    for fold, group in oof.groupby("fold"):
        y_fold = group["target_y"].to_numpy(dtype=int)
        cal_fold = group[CALIBRATED_PROBA_COLUMN].to_numpy(dtype=float)
        pred_fold = group["constrained_pred"].to_numpy(dtype=int)
        fold_metrics = _binary_metrics(y_true=y_fold, y_pred=pred_fold)

        by_fold_rows.append(
            {
                "fold": int(fold),
                "rows": int(len(group)),
                "positive_rows": int((group["target_y"] == 1).sum()),
                "prevalence": float((group["target_y"] == 1).mean()),
                "mean_constrained_threshold": float(group["constrained_threshold"].mean()),
                "min_constrained_threshold": float(group["constrained_threshold"].min()),
                "max_constrained_threshold": float(group["constrained_threshold"].max()),
                "constrained_positive_rate": float(fold_metrics["positive_rate"]),
                "constrained_precision": float(fold_metrics["precision"]),
                "constrained_recall": float(fold_metrics["recall"]),
                "constrained_f1": float(fold_metrics["f1"]),
                "fallback_rows": int((group["constrained_mode"].astype(str) == "fallback_max_precision").sum()),
                "calibrated_score_q90": float(np.quantile(cal_fold, 0.90)),
            }
        )

    summary_df = pd.DataFrame([summary_row])
    reliability_df = pd.DataFrame(reliability_rows)
    by_year_df = pd.DataFrame(by_year_rows).sort_values(by="year").reset_index(drop=True)
    by_fold_df = pd.DataFrame(by_fold_rows).sort_values(by="fold").reset_index(drop=True)

    summary_output = Path(args.summary_output)
    reliability_output = Path(args.reliability_output)
    by_year_output = Path(args.by_year_output)
    by_fold_output = Path(args.by_fold_output)
    reference_output = Path(args.reference_output)
    report_output = Path(args.report_output)

    for output in [
        summary_output,
        reliability_output,
        by_year_output,
        by_fold_output,
        reference_output,
        report_output,
    ]:
        output.parent.mkdir(parents=True, exist_ok=True)

    summary_df.to_csv(summary_output, index=False)
    reliability_df.to_csv(reliability_output, index=False)
    by_year_df.to_csv(by_year_output, index=False)
    by_fold_df.to_csv(by_fold_output, index=False)
    reference_with_constrained.to_csv(reference_output, index=False)

    lines = [
        "=== CALIBRATION AND DECISION-POLICY REPORT ===",
        f"OOF input           : {oof_path}",
        f"ablation input      : {ablation_path}",
        f"rows                : {int(summary_row['oof_rows'])}",
        f"prevalence          : {float(summary_row['oof_prevalence']):.6f}",
        "",
        "reliability summary",
        f"raw brier           : {float(summary_row['raw_brier']):.6f}",
        f"calibrated brier    : {float(summary_row['calibrated_brier']):.6f}",
        f"raw ece             : {float(summary_row['raw_ece']):.6f}",
        f"calibrated ece      : {float(summary_row['calibrated_ece']):.6f}",
        f"raw mce             : {float(summary_row['raw_mce']):.6f}",
        f"calibrated mce      : {float(summary_row['calibrated_mce']):.6f}",
        "",
        "constrained policy",
        f"positive rate       : {float(summary_row['constrained_positive_rate']):.6f}",
        f"precision           : {float(summary_row['constrained_precision']):.6f}",
        f"recall              : {float(summary_row['constrained_recall']):.6f}",
        f"f1                  : {float(summary_row['constrained_f1']):.6f}",
        f"threshold mean/min/max: {threshold_mean:.6f}/{threshold_min:.6f}/{threshold_max:.6f}",
        "",
        "winner diagnostics from ablation",
        f"reachability ratio  : {float(summary_row['winner_mean_precision_floor_reachable_ratio']):.6f}",
        "fallback folds      : "
        f"{int(summary_row['winner_fallback_folds']) if not np.isnan(summary_row['winner_fallback_folds']) else 'nan'}"
        "/"
        f"{int(summary_row['winner_total_folds']) if not np.isnan(summary_row['winner_total_folds']) else 'nan'}",
        f"fallback rate       : {float(summary_row['winner_fallback_rate']):.6f}",
        f"margin to floor     : {float(summary_row['winner_mean_constrained_margin_to_floor']):.6f}",
        f"calibration mix     : {summary_row['winner_calibration_mix']}",
        "",
        "artifacts",
        f"summary csv         : {summary_output}",
        f"reliability bins csv: {reliability_output}",
        f"by-year csv         : {by_year_output}",
        f"by-fold csv         : {by_fold_output}",
        f"reference csv       : {reference_output}",
    ]

    report_output.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("\n".join(lines))


if __name__ == "__main__":
    main()
