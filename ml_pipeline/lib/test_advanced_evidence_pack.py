"""Unit checks for advanced evidence and error-analysis helper logic."""

from __future__ import annotations

import unittest
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from run_catalog import comparator_path, list_runs, pr_metrics_path
except ImportError:
    from ml_pipeline.lib.run_catalog import comparator_path, list_runs, pr_metrics_path  # type: ignore

try:
    from analyze_error_taxonomy import _near_miss_subtype
except ImportError:
    from ml_pipeline.analyze_error_taxonomy import _near_miss_subtype  # type: ignore

try:
    from plot_advanced_metrics import _decision_curve_rows, _pr_gain
except ImportError:
    from ml_pipeline.plot_advanced_metrics import _decision_curve_rows, _pr_gain  # type: ignore

try:
    from compute_ap_delta_bootstrap import _bootstrap_delta
except ImportError:
    from ml_pipeline.compute_ap_delta_bootstrap import _bootstrap_delta  # type: ignore


class AdvancedEvidencePackTest(unittest.TestCase):
    def test_run_catalog_moa_fallback_and_pr_metrics_path(self) -> None:
        reports_root = Path("data_lake/reports")
        runs = list_runs(reports_root)
        run_map = {run.run_id: run for run in runs}

        p1 = run_map["p1"]
        moa_cmp = comparator_path(p1, "moa")
        self.assertTrue(moa_cmp.exists(), f"expected moa comparator fallback to exist, got {moa_cmp}")

        full = run_map["full"]
        pr_path = pr_metrics_path(full)
        self.assertTrue(pr_path.exists(), f"expected root PR metrics to resolve, got {pr_path}")

    def test_pr_gain_zero_at_prevalence_anchor(self) -> None:
        precision = np.array([0.1, 0.2, 0.3], dtype=float)
        recall = np.array([0.1, 0.4, 0.8], dtype=float)
        rg, pg = _pr_gain(precision, recall, prevalence=0.1)

        self.assertEqual(len(rg), len(pg))
        self.assertTrue(np.isfinite(rg).all())
        self.assertTrue(np.isfinite(pg).all())
        self.assertAlmostEqual(float(pg[0]), 0.0, places=9)
        self.assertAlmostEqual(float(rg[0]), 0.0, places=9)

    def test_decision_curve_baselines(self) -> None:
        y = np.array([1, 0], dtype=int)
        s = np.array([0.9, 0.1], dtype=float)
        thresholds = np.array([0.5], dtype=float)

        dca = _decision_curve_rows(y, s, thresholds)
        self.assertEqual(len(dca), 1)

        row = dca.iloc[0]
        self.assertAlmostEqual(float(row["net_benefit_none"]), 0.0, places=12)
        self.assertAlmostEqual(float(row["net_benefit_all"]), 0.0, places=12)
        self.assertAlmostEqual(float(row["net_benefit_model"]), 0.5, places=12)

    def test_cluster_bootstrap_reproducibility(self) -> None:
        run_df = pd.DataFrame(
            {
                "race": ["A", "A", "B", "B"],
                "target_y": [1, 0, 1, 0],
                "raw_proba": [0.9, 0.1, 0.8, 0.2],
            }
        )
        ref_df = pd.DataFrame(
            {
                "race": ["A", "A", "B", "B"],
                "target_y": [1, 0, 1, 0],
                "raw_proba": [0.8, 0.2, 0.7, 0.3],
            }
        )

        s1, m1 = _bootstrap_delta(run_df, ref_df, score_col="raw_proba", reps=200, seed=42)
        s2, m2 = _bootstrap_delta(run_df, ref_df, score_col="raw_proba", reps=200, seed=42)

        self.assertTrue(np.allclose(s1["ap_delta"].to_numpy(), s2["ap_delta"].to_numpy()))
        self.assertAlmostEqual(float(m1["ap_delta_point"]), float(m2["ap_delta_point"]), places=12)
        self.assertAlmostEqual(float(m1["ap_delta_ci95_low"]), float(m2["ap_delta_ci95_low"]), places=12)
        self.assertAlmostEqual(float(m1["ap_delta_ci95_high"]), float(m2["ap_delta_ci95_high"]), places=12)

    def test_near_miss_subtype_rules(self) -> None:
        self.assertEqual(_near_miss_subtype(3, horizon=2), "near_miss_3_5")
        self.assertEqual(_near_miss_subtype(5, horizon=2), "near_miss_3_5")
        self.assertEqual(_near_miss_subtype(6, horizon=2), "far_miss_6_plus")
        self.assertEqual(_near_miss_subtype(np.nan, horizon=2), "no_future")


if __name__ == "__main__":
    unittest.main()
