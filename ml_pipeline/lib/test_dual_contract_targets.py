"""Unit tests for dual-contract target construction."""

from __future__ import annotations

import unittest

import pandas as pd

try:
    from data_preparation import _build_targets, _prepare_pit_evals, _prepare_pit_timings
except ImportError:
    from ml_pipeline.lib.data_preparation import (  # type: ignore
        _build_targets,
        _prepare_pit_evals,
        _prepare_pit_timings,
    )


class DualContractTargetsTest(unittest.TestCase):
    def test_any_clean_uses_any_eligible_pit_but_success_clean_uses_matched_success_pit(self) -> None:
        features = pd.DataFrame(
            [
                {"race": "2025 :: Test GP", "driver": "HAM", "lapNumber": 1, "compound": "SOFT", "tyreLife": 5, "trackStatus": "1", "rainfall": 0.0},
                {"race": "2025 :: Test GP", "driver": "HAM", "lapNumber": 2, "compound": "SOFT", "tyreLife": 6, "trackStatus": "1", "rainfall": 0.0},
                {"race": "2025 :: Test GP", "driver": "HAM", "lapNumber": 3, "compound": "SOFT", "tyreLife": 7, "trackStatus": "1", "rainfall": 0.0},
            ]
        )
        pit_evals_raw = pd.DataFrame(
            [
                {
                    "race": "2025 :: Test GP",
                    "driver": "HAM",
                    "pitLapNumber": 3,
                    "result": "SUCCESS_DEFEND",
                    "trackStatusAtPit": "5",  # red -> excluded clean_actionable
                    "compound": "SOFT",
                },
                {
                    "race": "2025 :: Test GP",
                    "driver": "HAM",
                    "pitLapNumber": 4,
                    "result": "SUCCESS_DEFEND",
                    "trackStatusAtPit": "1",
                    "compound": "MEDIUM",
                },
            ]
        )
        pit_timings_raw = pd.DataFrame(
            [
                {"race": "2025 :: Test GP", "driver": "HAM", "lapNumber": 3, "pitInTime": 10.0, "pitOutTime": 12.0, "date": "2025-01-01T10:00:00Z"},
                {"race": "2025 :: Test GP", "driver": "HAM", "lapNumber": 4, "pitInTime": 14.0, "pitOutTime": 16.0, "date": "2025-01-01T10:00:30Z"},
            ]
        )

        pit_evals = _prepare_pit_evals(pit_evals_raw)
        pit_timings = _prepare_pit_timings(pit_timings_raw)
        dataset = _build_targets(features, pit_evals, pit_timings, horizon=2)

        row_lap2 = dataset[dataset["lapNumber"] == 2].iloc[0]
        # Raw success sees nearest success pit at lap 3.
        self.assertEqual(int(row_lap2["target_pit_success_h2_raw"]), 1)
        # Clean success follows matched success eligibility (lap 3 red -> excluded).
        self.assertEqual(int(row_lap2["target_pit_success_h2_clean_actionable"]), 0)
        self.assertFalse(bool(row_lap2["target_pit_success_h2_clean_actionable_train_eligible"]))

        # Raw any sees a future pit in window.
        self.assertEqual(int(row_lap2["target_pit_any_h2_raw"]), 1)
        # Clean any uses any eligible pit in window [3,4]; lap 4 is eligible.
        self.assertEqual(int(row_lap2["target_pit_any_h2_clean_actionable"]), 1)

        # Canonical and alias columns are both present.
        required = {
            "target_pit_any_h2_raw",
            "target_pit_any_h2_clean_actionable",
            "target_pit_any_h2_clean_dry_strategy",
            "target_pit_success_h2_raw",
            "target_pit_success_h2_clean_actionable",
            "target_pit_success_h2_clean_dry_strategy",
            "pit_any_h2_raw",
            "pit_any_h2_clean_actionable",
            "pit_any_h2_clean_dry_strategy",
            "pit_success_h2_raw",
            "pit_success_h2_clean_actionable",
            "pit_success_h2_clean_dry_strategy",
        }
        self.assertTrue(required.issubset(set(dataset.columns)))

    def test_success_unknown_outcome_rows_are_not_train_eligible(self) -> None:
        features = pd.DataFrame(
            [
                {"race": "2025 :: Unknown GP", "driver": "NOR", "lapNumber": 1, "compound": "SOFT", "tyreLife": 4, "trackStatus": "1", "rainfall": 0.0},
                {"race": "2025 :: Unknown GP", "driver": "NOR", "lapNumber": 2, "compound": "SOFT", "tyreLife": 5, "trackStatus": "1", "rainfall": 0.0},
            ]
        )
        pit_evals_raw = pd.DataFrame(
            [
                {
                    "race": "2025 :: Unknown GP",
                    "driver": "NOR",
                    "pitLapNumber": 2,
                    "result": "UNRESOLVED_INCIDENT_FILTER",
                    "trackStatusAtPit": "1",
                    "compound": "SOFT",
                }
            ]
        )
        pit_timings_raw = pd.DataFrame(
            [
                {"race": "2025 :: Unknown GP", "driver": "NOR", "lapNumber": 2, "pitInTime": 10.0, "pitOutTime": 12.0, "date": "2025-01-01T10:00:00Z"},
            ]
        )

        pit_evals = _prepare_pit_evals(pit_evals_raw)
        pit_timings = _prepare_pit_timings(pit_timings_raw)
        dataset = _build_targets(features, pit_evals, pit_timings, horizon=2)
        row_lap1 = dataset[dataset["lapNumber"] == 1].iloc[0]

        self.assertEqual(int(row_lap1["target_pit_success_h2_raw"]), 0)
        self.assertFalse(bool(row_lap1["target_pit_success_h2_raw_train_eligible"]))
        self.assertFalse(bool(row_lap1["target_pit_success_h2_clean_actionable_train_eligible"]))
        self.assertFalse(bool(row_lap1["target_pit_success_h2_clean_dry_strategy_train_eligible"]))

    def test_success_outcome_prefix_mapping_for_failure_and_unknown(self) -> None:
        features = pd.DataFrame(
            [
                {"race": "2025 :: Prefix GP", "driver": "NOR", "lapNumber": 1, "compound": "SOFT", "tyreLife": 4, "trackStatus": "1", "rainfall": 0.0},
                {"race": "2025 :: Prefix GP", "driver": "NOR", "lapNumber": 2, "compound": "SOFT", "tyreLife": 5, "trackStatus": "1", "rainfall": 0.0},
                {"race": "2025 :: Prefix GP", "driver": "NOR", "lapNumber": 3, "compound": "SOFT", "tyreLife": 6, "trackStatus": "1", "rainfall": 0.0},
                {"race": "2025 :: Prefix GP", "driver": "NOR", "lapNumber": 4, "compound": "SOFT", "tyreLife": 7, "trackStatus": "1", "rainfall": 0.0},
            ]
        )
        pit_evals_raw = pd.DataFrame(
            [
                {
                    "race": "2025 :: Prefix GP",
                    "driver": "NOR",
                    "pitLapNumber": 2,
                    "result": "FAILURE_ENGINE_OVERHEAT",
                    "trackStatusAtPit": "1",
                    "compound": "SOFT",
                },
                {
                    "race": "2025 :: Prefix GP",
                    "driver": "NOR",
                    "pitLapNumber": 3,
                    "result": "WEATHER_SURVIVAL_STOP",
                    "trackStatusAtPit": "1",
                    "compound": "SOFT",
                },
                {
                    "race": "2025 :: Prefix GP",
                    "driver": "NOR",
                    "pitLapNumber": 4,
                    "result": "UNMAPPED_VENDOR_STATUS",
                    "trackStatusAtPit": "1",
                    "compound": "SOFT",
                },
            ]
        )
        pit_timings_raw = pd.DataFrame(
            [
                {"race": "2025 :: Prefix GP", "driver": "NOR", "lapNumber": 2, "pitInTime": 10.0, "pitOutTime": 12.0, "date": "2025-01-01T10:00:00Z"},
                {"race": "2025 :: Prefix GP", "driver": "NOR", "lapNumber": 3, "pitInTime": 13.0, "pitOutTime": 15.0, "date": "2025-01-01T10:00:30Z"},
                {"race": "2025 :: Prefix GP", "driver": "NOR", "lapNumber": 4, "pitInTime": 16.0, "pitOutTime": 18.0, "date": "2025-01-01T10:01:00Z"},
            ]
        )

        pit_evals = _prepare_pit_evals(pit_evals_raw)
        pit_timings = _prepare_pit_timings(pit_timings_raw)
        dataset = _build_targets(features, pit_evals, pit_timings, horizon=2)

        row_lap1 = dataset[dataset["lapNumber"] == 1].iloc[0]
        # FAILURE_* should be treated as clean train-eligible negatives.
        self.assertEqual(int(row_lap1["target_pit_success_h2_clean_actionable"]), 0)
        self.assertTrue(bool(row_lap1["target_pit_success_h2_clean_actionable_train_eligible"]))

        row_lap2 = dataset[dataset["lapNumber"] == 2].iloc[0]
        # WEATHER_* is unknown/noisy -> not train eligible.
        self.assertEqual(int(row_lap2["target_pit_success_h2_clean_actionable"]), 0)
        self.assertFalse(bool(row_lap2["target_pit_success_h2_clean_actionable_train_eligible"]))

        row_lap3 = dataset[dataset["lapNumber"] == 3].iloc[0]
        # UNMAPPED_* is unknown/noisy -> not train eligible.
        self.assertEqual(int(row_lap3["target_pit_success_h2_clean_actionable"]), 0)
        self.assertFalse(bool(row_lap3["target_pit_success_h2_clean_actionable_train_eligible"]))


if __name__ == "__main__":
    unittest.main()
