"""Tests for feature-selection leakage guards in model training."""

from __future__ import annotations

import unittest

try:
    from model_training_cv import _select_feature_columns
except ImportError:
    from ml_pipeline.lib.model_training_cv import _select_feature_columns  # type: ignore


class ModelTrainingLeakageGuardTest(unittest.TestCase):
    def test_select_feature_columns_drops_label_and_matched_metadata(self) -> None:
        columns = [
            "race",
            "driver",
            "lapNumber",
            "speedTrap",
            "target_pit_any_h2_raw",
            "pit_any_h2_clean_actionable",
            "target_pit_success_h2_clean_dry_strategy_train_eligible",
            "matched_pit_lap_success",
            "some_truth_lens_flag",
            "eligibility_note",
            "target_y",
        ]

        feature_cols = _select_feature_columns(
            columns,
            target_column="target_pit_any_h2_clean_actionable",
            drop_source_year_feature=False,
            excluded_features=[],
        )

        self.assertIn("speedTrap", feature_cols)
        self.assertNotIn("race", feature_cols)
        self.assertNotIn("driver", feature_cols)
        self.assertNotIn("lapNumber", feature_cols)
        self.assertNotIn("target_pit_any_h2_raw", feature_cols)
        self.assertNotIn("pit_any_h2_clean_actionable", feature_cols)
        self.assertNotIn("target_pit_success_h2_clean_dry_strategy_train_eligible", feature_cols)
        self.assertNotIn("matched_pit_lap_success", feature_cols)
        self.assertNotIn("some_truth_lens_flag", feature_cols)
        self.assertNotIn("eligibility_note", feature_cols)
        self.assertNotIn("target_y", feature_cols)


if __name__ == "__main__":
    unittest.main()
