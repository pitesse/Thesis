"""evaluate statistical significance for merged SDE vs ML comparator outputs.

this script reports uncertainty and significance for precision-focused comparison.
it computes:
- wilson 95 percent confidence intervals for precision of each model,
- two-proportion z test for precision difference on scored rows,
- overlap-based mcnemar test on shared decision keys.

the overlap mcnemar test is restricted to keys where both models produced
actionable and scored outcomes. this keeps the pairing explicit and auditable.
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import pandas as pd

DEFAULT_SDE_COMPARATOR = "data_lake/reports/heuristic_comparator_2022_2024_merged.csv"
DEFAULT_ML_COMPARATOR = "data_lake/reports/ml_comparator_2022_2024_merged.csv"
DEFAULT_SUMMARY_OUTPUT = "data_lake/reports/phase_b_significance_summary_2022_2024_merged.csv"
DEFAULT_TESTS_OUTPUT = "data_lake/reports/phase_b_significance_tests_2022_2024_merged.csv"
DEFAULT_REPORT_OUTPUT = "data_lake/reports/phase_b_significance_report_2022_2024_merged.txt"

SCORED_CLASSES = {"1", "0"}
KEY_COLUMNS = ["race", "driver", "suggestion_lap"]


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _chi_square_sf_df1(statistic: float) -> float:
    if statistic < 0:
        raise ValueError("chi-square statistic must be non-negative")
    return math.erfc(math.sqrt(statistic / 2.0))


def _binom_two_sided_p_value(successes: int, trials: int) -> float:
    if trials <= 0:
        return 1.0

    observed = math.comb(trials, successes) * (0.5**trials)
    tail = 0.0
    for k in range(trials + 1):
        prob = math.comb(trials, k) * (0.5**trials)
        if prob <= observed + 1e-15:
            tail += prob
    return float(min(1.0, tail))


def _wilson_interval(tp: int, n: int, z: float = 1.959963984540054) -> tuple[float, float]:
    if n <= 0:
        return float("nan"), float("nan")

    p_hat = tp / n
    z2 = z * z
    denom = 1.0 + z2 / n
    center = (p_hat + z2 / (2.0 * n)) / denom
    margin = (z / denom) * math.sqrt((p_hat * (1.0 - p_hat) / n) + (z2 / (4.0 * n * n)))
    lower = max(0.0, center - margin)
    upper = min(1.0, center + margin)
    return lower, upper


def _two_proportion_z_test(tp_a: int, n_a: int, tp_b: int, n_b: int) -> tuple[float, float]:
    if n_a <= 0 or n_b <= 0:
        return float("nan"), float("nan")

    p_a = tp_a / n_a
    p_b = tp_b / n_b
    pooled = (tp_a + tp_b) / (n_a + n_b)
    variance = pooled * (1.0 - pooled) * ((1.0 / n_a) + (1.0 / n_b))
    if variance <= 0:
        return float("nan"), float("nan")

    z_stat = (p_b - p_a) / math.sqrt(variance)
    p_value = 2.0 * (1.0 - _normal_cdf(abs(z_stat)))
    return z_stat, p_value


def _mcnemar_tests(outcomes_a: pd.Series, outcomes_b: pd.Series) -> dict[str, float | int]:
    if len(outcomes_a) != len(outcomes_b):
        raise ValueError("paired outcomes must have equal length")

    a = outcomes_a.astype(int).to_numpy()
    b = outcomes_b.astype(int).to_numpy()

    sde_success_ml_failure = int(((a == 1) & (b == 0)).sum())
    sde_failure_ml_success = int(((a == 0) & (b == 1)).sum())
    discordant = sde_success_ml_failure + sde_failure_ml_success

    if discordant == 0:
        return {
            "discordant_total": 0,
            "sde_success_ml_failure": sde_success_ml_failure,
            "sde_failure_ml_success": sde_failure_ml_success,
            "mcnemar_chi2_cc": 0.0,
            "mcnemar_pvalue_asymptotic": 1.0,
            "mcnemar_pvalue_exact": 1.0,
        }

    chi2_cc = ((abs(sde_success_ml_failure - sde_failure_ml_success) - 1.0) ** 2) / discordant
    p_asymptotic = _chi_square_sf_df1(chi2_cc)
    p_exact = _binom_two_sided_p_value(min(sde_success_ml_failure, sde_failure_ml_success), discordant)

    return {
        "discordant_total": int(discordant),
        "sde_success_ml_failure": sde_success_ml_failure,
        "sde_failure_ml_success": sde_failure_ml_success,
        "mcnemar_chi2_cc": float(chi2_cc),
        "mcnemar_pvalue_asymptotic": float(p_asymptotic),
        "mcnemar_pvalue_exact": float(p_exact),
    }


def _load_scored(path: Path, model_label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"comparator file not found for {model_label}: {path}")

    df = pd.read_csv(path)
    required = set(KEY_COLUMNS + ["outcome_class"])
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{model_label} comparator is missing required columns: {missing}")

    work = df.copy()
    work["outcome_class"] = work["outcome_class"].astype(str)
    work = work[work["outcome_class"].isin(SCORED_CLASSES)].copy()
    if work.empty:
        raise ValueError(f"{model_label} comparator has zero scored rows")

    work["race"] = work["race"].astype(str)
    work["driver"] = work["driver"].astype(str)
    work["suggestion_lap"] = pd.to_numeric(work["suggestion_lap"], errors="coerce")
    work = work[work["suggestion_lap"].notna()].copy()
    work["suggestion_lap"] = work["suggestion_lap"].astype(int)
    work["outcome_bin"] = (work["outcome_class"] == "1").astype(int)

    return work


def _summarize_precision(work: pd.DataFrame, model_label: str) -> dict[str, float | int | str]:
    scored = int(len(work))
    tp = int((work["outcome_bin"] == 1).sum())
    fp = int((work["outcome_bin"] == 0).sum())
    precision = (tp / scored) if scored > 0 else float("nan")
    ci_low, ci_high = _wilson_interval(tp, scored)

    return {
        "model": model_label,
        "scored": scored,
        "tp": tp,
        "fp": fp,
        "precision": float(precision),
        "wilson_ci_low": float(ci_low),
        "wilson_ci_high": float(ci_high),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="evaluate precision significance for SDE vs ML comparator outputs")
    parser.add_argument("--sde-comparator", default=DEFAULT_SDE_COMPARATOR, help="path to SDE comparator csv")
    parser.add_argument("--ml-comparator", default=DEFAULT_ML_COMPARATOR, help="path to ML comparator csv")
    parser.add_argument("--summary-output", default=DEFAULT_SUMMARY_OUTPUT, help="output csv for model summaries")
    parser.add_argument("--tests-output", default=DEFAULT_TESTS_OUTPUT, help="output csv for hypothesis tests")
    parser.add_argument("--report-output", default=DEFAULT_REPORT_OUTPUT, help="output txt for formatted report")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    sde_path = Path(args.sde_comparator)
    ml_path = Path(args.ml_comparator)

    sde_scored = _load_scored(sde_path, "SDE")
    ml_scored = _load_scored(ml_path, "ML")

    sde_summary = _summarize_precision(sde_scored, "SDE")
    ml_summary = _summarize_precision(ml_scored, "ML")

    z_stat, z_pvalue = _two_proportion_z_test(
        tp_a=int(sde_summary["tp"]),
        n_a=int(sde_summary["scored"]),
        tp_b=int(ml_summary["tp"]),
        n_b=int(ml_summary["scored"]),
    )

    overlap = sde_scored.merge(
        ml_scored,
        on=KEY_COLUMNS,
        how="inner",
        suffixes=("_sde", "_ml"),
    )

    if overlap.empty:
        mcnemar = {
            "discordant_total": 0,
            "sde_success_ml_failure": 0,
            "sde_failure_ml_success": 0,
            "mcnemar_chi2_cc": float("nan"),
            "mcnemar_pvalue_asymptotic": float("nan"),
            "mcnemar_pvalue_exact": float("nan"),
        }
    else:
        mcnemar = _mcnemar_tests(overlap["outcome_bin_sde"], overlap["outcome_bin_ml"])

    delta_precision = float(ml_summary["precision"] - sde_summary["precision"])

    summary_rows = [
        sde_summary,
        ml_summary,
        {
            "model": "ML_minus_SDE",
            "scored": int(ml_summary["scored"] - sde_summary["scored"]),
            "tp": int(ml_summary["tp"] - sde_summary["tp"]),
            "fp": int(ml_summary["fp"] - sde_summary["fp"]),
            "precision": delta_precision,
            "wilson_ci_low": float("nan"),
            "wilson_ci_high": float("nan"),
        },
    ]

    tests_rows = [
        {
            "test": "two_proportion_z",
            "pairing_scope": "independent_scored_rows",
            "n_sde": int(sde_summary["scored"]),
            "n_ml": int(ml_summary["scored"]),
            "statistic": z_stat,
            "p_value": z_pvalue,
            "note": "tests precision difference on scored rows, ignores pairing",
        },
        {
            "test": "mcnemar_cc",
            "pairing_scope": "overlap_scored_keys_only",
            "n_sde": int(sde_summary["scored"]),
            "n_ml": int(ml_summary["scored"]),
            "statistic": float(mcnemar["mcnemar_chi2_cc"]),
            "p_value": float(mcnemar["mcnemar_pvalue_asymptotic"]),
            "note": (
                "paired only on shared race driver lap keys, "
                f"overlap_n={len(overlap)}, discordant={int(mcnemar['discordant_total'])}"
            ),
        },
        {
            "test": "mcnemar_exact",
            "pairing_scope": "overlap_scored_keys_only",
            "n_sde": int(sde_summary["scored"]),
            "n_ml": int(ml_summary["scored"]),
            "statistic": float(mcnemar["discordant_total"]),
            "p_value": float(mcnemar["mcnemar_pvalue_exact"]),
            "note": (
                "exact binomial mcnemar p value on discordant overlap pairs, "
                f"sde_success_ml_failure={int(mcnemar['sde_success_ml_failure'])}, "
                f"sde_failure_ml_success={int(mcnemar['sde_failure_ml_success'])}"
            ),
        },
    ]

    summary_df = pd.DataFrame(summary_rows)
    tests_df = pd.DataFrame(tests_rows)

    summary_out = Path(args.summary_output)
    tests_out = Path(args.tests_output)
    report_out = Path(args.report_output)
    summary_out.parent.mkdir(parents=True, exist_ok=True)
    tests_out.parent.mkdir(parents=True, exist_ok=True)
    report_out.parent.mkdir(parents=True, exist_ok=True)

    summary_df.to_csv(summary_out, index=False)
    tests_df.to_csv(tests_out, index=False)

    lines = [
        "=== PHASE B SIGNIFICANCE REPORT ===",
        f"SDE comparator: {sde_path}",
        f"ML comparator : {ml_path}",
        "",
        "precision summaries",
        (
            f"SDE: scored={int(sde_summary['scored'])}, tp={int(sde_summary['tp'])}, fp={int(sde_summary['fp'])}, "
            f"precision={float(sde_summary['precision']):.6f}, "
            f"wilson95=[{float(sde_summary['wilson_ci_low']):.6f}, {float(sde_summary['wilson_ci_high']):.6f}]"
        ),
        (
            f"ML : scored={int(ml_summary['scored'])}, tp={int(ml_summary['tp'])}, fp={int(ml_summary['fp'])}, "
            f"precision={float(ml_summary['precision']):.6f}, "
            f"wilson95=[{float(ml_summary['wilson_ci_low']):.6f}, {float(ml_summary['wilson_ci_high']):.6f}]"
        ),
        f"precision delta (ML minus SDE): {delta_precision:+.6f}",
        "",
        "hypothesis tests",
        f"two proportion z: z={z_stat:.6f}, p={z_pvalue:.6g}",
        (
            "mcnemar overlap: "
            f"overlap_n={len(overlap)}, "
            f"discordant={int(mcnemar['discordant_total'])}, "
            f"chi2_cc={float(mcnemar['mcnemar_chi2_cc']):.6f}, "
            f"p_asymptotic={float(mcnemar['mcnemar_pvalue_asymptotic']):.6g}, "
            f"p_exact={float(mcnemar['mcnemar_pvalue_exact']):.6g}"
        ),
        "",
        "interpretation note",
        "- two proportion z uses independent scored rows and is the primary precision-gap test here.",
        "- mcnemar is reported on overlap keys only and should be interpreted as a paired sensitivity check.",
    ]

    report_out.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("\n".join(lines))
    print("")
    print(f"summary csv: {summary_out}")
    print(f"tests csv  : {tests_out}")
    print(f"report txt : {report_out}")


if __name__ == "__main__":
    main()