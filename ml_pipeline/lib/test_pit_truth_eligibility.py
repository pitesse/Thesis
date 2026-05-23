"""Unit tests for pit truth eligibility helpers."""

from __future__ import annotations

import unittest

import pandas as pd

try:
    from pit_truth_eligibility import (
        TRUTH_LENS_CLEAN_ACTIONABLE,
        TRUTH_LENS_CLEAN_DRY_STRATEGY,
        TRUTH_LENS_RAW,
        build_pit_truth_universe,
        eligible_actual_counts,
        regime_from_status,
    )
except ImportError:
    from ml_pipeline.lib.pit_truth_eligibility import (  # type: ignore
        TRUTH_LENS_CLEAN_ACTIONABLE,
        TRUTH_LENS_CLEAN_DRY_STRATEGY,
        TRUTH_LENS_RAW,
        build_pit_truth_universe,
        eligible_actual_counts,
        regime_from_status,
    )


class PitTruthEligibilityTest(unittest.TestCase):
    def test_regime_from_status_maps_red(self) -> None:
        self.assertEqual(regime_from_status("5"), "RED")
        self.assertEqual(regime_from_status("4"), "CAUTION")
        self.assertEqual(regime_from_status("1"), "GREEN")

    def test_build_universe_flags_lenses(self) -> None:
        suggestions = pd.DataFrame(
            [
                {"race": "Test GP", "driver": "AAA"},
                {"race": "Test GP", "driver": "BBB"},
            ]
        )
        pit_timings = pd.DataFrame(
            [
                # lap-1 red event => excluded clean actionable
                {"race": "Test GP", "driver": "AAA", "lapNumber": 1, "date": "2025-01-01T12:00:00Z"},
                # normal green event => eligible all
                {"race": "Test GP", "driver": "BBB", "lapNumber": 20, "date": "2025-01-01T12:10:00Z"},
                # wet compound event => excluded clean_dry_strategy
                {"race": "Test GP", "driver": "AAA", "lapNumber": 30, "date": "2025-01-01T12:20:00Z"},
            ]
        )
        pit_evals = pd.DataFrame(
            [
                {"race": "Test GP", "driver": "AAA", "pitLapNumber": 1, "trackStatusAtPit": "5", "compound": "SOFT"},
                {"race": "Test GP", "driver": "BBB", "pitLapNumber": 20, "trackStatusAtPit": "1", "compound": "MEDIUM"},
                {"race": "Test GP", "driver": "AAA", "pitLapNumber": 30, "trackStatusAtPit": "1", "compound": "INTERMEDIATE"},
            ]
        )
        universe = build_pit_truth_universe(
            pit_timings,
            suggestions,
            pit_evals,
            split_tag="unit_test",
        )
        self.assertEqual(len(universe), 3)

        raw_all, raw_eligible = eligible_actual_counts(universe, truth_lens=TRUTH_LENS_RAW, regime="ALL")
        _, clean_actionable = eligible_actual_counts(
            universe, truth_lens=TRUTH_LENS_CLEAN_ACTIONABLE, regime="ALL"
        )
        _, clean_dry = eligible_actual_counts(universe, truth_lens=TRUTH_LENS_CLEAN_DRY_STRATEGY, regime="ALL")

        self.assertEqual(raw_all, 3)
        self.assertEqual(raw_eligible, 3)
        self.assertEqual(clean_actionable, 2)  # lap-1 red event removed
        self.assertEqual(clean_dry, 1)  # plus intermediate event removed


if __name__ == "__main__":
    unittest.main()
