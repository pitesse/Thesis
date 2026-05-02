"""Unit checks for causal percentage-based race-agnostic features."""

from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

try:
    from data_preparation import (
        TRACK_PERCENTAGE_RACE_TEAM_V1,
        TRACK_PERCENTAGE_TEAM_V1,
        TRACK_PERCENTAGE_V1,
        _prepare_features,
    )
except ImportError:
    from ml_pipeline.lib.data_preparation import (  # type: ignore
        TRACK_PERCENTAGE_RACE_TEAM_V1,
        TRACK_PERCENTAGE_TEAM_V1,
        TRACK_PERCENTAGE_V1,
        _prepare_features,
    )


def _base_features() -> pd.DataFrame:
    rows = [
        # Unsorted by design: tests should remain stable.
        {
            "race": "2025 :: Bahrain Grand Prix",
            "driver": "B",
            "lapNumber": 1,
            "compound": "MEDIUM",
            "tyreLife": 1,
            "lapTime": 91.0,
            "gapAhead": 1.0,
            "gapBehind": 2.0,
            "trackTemp": 31.0,
            "airTemp": 22.0,
            "humidity": 45.0,
            "speedTrap": 302.0,
            "rainfall": False,
            "team": "TeamA",
            "position": 2,
            "pitLoss": 22.0,
            "trackStatus": 1,
        },
        {
            "race": "2025 :: Bahrain Grand Prix",
            "driver": "A",
            "lapNumber": 1,
            "compound": "MEDIUM",
            "tyreLife": 1,
            "lapTime": 90.0,
            "gapAhead": 0.6,
            "gapBehind": 1.3,
            "trackTemp": 30.0,
            "airTemp": 21.0,
            "humidity": 44.0,
            "speedTrap": 301.0,
            "rainfall": False,
            "team": "TeamA",
            "position": 1,
            "pitLoss": 22.0,
            "trackStatus": 1,
        },
        {
            "race": "2025 :: Bahrain Grand Prix",
            "driver": "C",
            "lapNumber": 1,
            "compound": "MEDIUM",
            "tyreLife": 1,
            "lapTime": 100.0,
            "gapAhead": 2.0,
            "gapBehind": 3.0,
            "trackTemp": 30.5,
            "airTemp": 21.5,
            "humidity": 46.0,
            "speedTrap": 300.0,
            "rainfall": False,
            "team": "TeamB",
            "position": 3,
            "pitLoss": 22.0,
            "trackStatus": 1,
        },
        {
            "race": "2025 :: Bahrain Grand Prix",
            "driver": "A",
            "lapNumber": 2,
            "compound": "MEDIUM",
            "tyreLife": 2,
            "lapTime": 92.0,
            "gapAhead": 0.7,
            "gapBehind": 1.4,
            "trackTemp": 31.0,
            "airTemp": 22.0,
            "humidity": 43.0,
            "speedTrap": 302.0,
            "rainfall": False,
            "team": "TeamA",
            "position": 1,
            "pitLoss": 22.0,
            "trackStatus": 1,
        },
        {
            "race": "2025 :: Bahrain Grand Prix",
            "driver": "B",
            "lapNumber": 2,
            "compound": "MEDIUM",
            "tyreLife": 2,
            "lapTime": 94.0,
            "gapAhead": 1.1,
            "gapBehind": 2.2,
            "trackTemp": 31.0,
            "airTemp": 22.0,
            "humidity": 47.0,
            "speedTrap": 303.0,
            "rainfall": False,
            "team": "TeamA",
            "position": 2,
            "pitLoss": 22.0,
            "trackStatus": 1,
        },
        {
            "race": "2025 :: Bahrain Grand Prix",
            "driver": "C",
            "lapNumber": 2,
            "compound": "MEDIUM",
            "tyreLife": 2,
            "lapTime": 102.0,
            "gapAhead": 2.1,
            "gapBehind": 3.1,
            "trackTemp": 31.0,
            "airTemp": 22.0,
            "humidity": 48.0,
            "speedTrap": 301.0,
            "rainfall": False,
            "team": "TeamB",
            "position": 3,
            "pitLoss": 22.0,
            "trackStatus": 1,
        },
        {
            "race": "2025 :: Bahrain Grand Prix",
            "driver": "A",
            "lapNumber": 3,
            "compound": "MEDIUM",
            "tyreLife": 3,
            "lapTime": 93.0,
            "gapAhead": 0.8,
            "gapBehind": 1.5,
            "trackTemp": 32.0,
            "airTemp": 22.5,
            "humidity": 42.0,
            "speedTrap": 303.0,
            "rainfall": False,
            "team": "TeamA",
            "position": 1,
            "pitLoss": 22.0,
            "trackStatus": 1,
        },
    ]
    return pd.DataFrame(rows)


def _lookup(prepared: pd.DataFrame, driver: str, lap: int, column: str) -> float:
    row = prepared[(prepared["driver"] == driver) & (prepared["lapNumber"] == lap)]
    if row.empty:
        raise AssertionError(f"row not found for driver={driver}, lap={lap}")
    return float(row.iloc[0][column])


class TrackPercentageCausalityTest(unittest.TestCase):
    def test_percentage_v1_generates_expected_columns_and_values(self) -> None:
        prepared = _prepare_features(
            _base_features(),
            drop_zones=None,
            source_year_fallback=2025,
            track_agnostic_mode=TRACK_PERCENTAGE_V1,
        )

        self.assertIn("race_progress_pct", prepared.columns)
        self.assertIn("lapTime_vs_driver_prev_mean_pct", prepared.columns)
        self.assertNotIn("lapTime_vs_team_prev_lapbucket_mean_pct", prepared.columns)
        self.assertNotIn("lapTime_vs_race_prev_lapbucket_mean_pct", prepared.columns)

        # Bahrain scheduled race laps = 57.
        self.assertAlmostEqual(_lookup(prepared, "A", 1, "race_progress_pct"), 1.0 / 57.0, places=9)
        self.assertAlmostEqual(_lookup(prepared, "A", 2, "race_progress_pct"), 2.0 / 57.0, places=9)
        # driver A lap2 denominator uses only lap1 (90.0)
        self.assertAlmostEqual(
            _lookup(prepared, "A", 2, "lapTime_vs_driver_prev_mean_pct"),
            92.0 / 90.0,
            places=9,
        )

    def test_percentage_team_v1_uses_prior_team_lap_buckets_only(self) -> None:
        prepared = _prepare_features(
            _base_features(),
            drop_zones=None,
            source_year_fallback=2025,
            track_agnostic_mode=TRACK_PERCENTAGE_TEAM_V1,
        )

        self.assertIn("lapTime_vs_team_prev_lapbucket_mean_pct", prepared.columns)
        self.assertNotIn("lapTime_vs_race_prev_lapbucket_mean_pct", prepared.columns)

        # TeamA lap1 mean = (90 + 91) / 2 = 90.5 -> used as lap2 denominator.
        self.assertAlmostEqual(
            _lookup(prepared, "A", 2, "lapTime_vs_team_prev_lapbucket_mean_pct"),
            92.0 / 90.5,
            places=9,
        )
        self.assertAlmostEqual(
            _lookup(prepared, "B", 2, "lapTime_vs_team_prev_lapbucket_mean_pct"),
            94.0 / 90.5,
            places=9,
        )

    def test_percentage_race_team_v1_uses_prior_race_lap_buckets_only(self) -> None:
        prepared = _prepare_features(
            _base_features(),
            drop_zones=None,
            source_year_fallback=2025,
            track_agnostic_mode=TRACK_PERCENTAGE_RACE_TEAM_V1,
        )

        self.assertIn("lapTime_vs_race_prev_lapbucket_mean_pct", prepared.columns)

        # race lap1 mean = (90 + 91 + 100) / 3
        lap1_race_mean = (90.0 + 91.0 + 100.0) / 3.0
        self.assertAlmostEqual(
            _lookup(prepared, "A", 2, "lapTime_vs_race_prev_lapbucket_mean_pct"),
            92.0 / lap1_race_mean,
            places=9,
        )

    def test_future_change_does_not_modify_earlier_rows(self) -> None:
        base = _base_features()
        changed = _base_features()
        changed.loc[
            (changed["driver"] == "A") & (changed["lapNumber"] == 3),
            "lapTime",
        ] = 300.0

        prepared_base = _prepare_features(
            base,
            drop_zones=None,
            source_year_fallback=2025,
            track_agnostic_mode=TRACK_PERCENTAGE_RACE_TEAM_V1,
        )
        prepared_changed = _prepare_features(
            changed,
            drop_zones=None,
            source_year_fallback=2025,
            track_agnostic_mode=TRACK_PERCENTAGE_RACE_TEAM_V1,
        )

        cols = [
            "race_progress_pct",
            "lapTime_vs_driver_prev_mean_pct",
            "lapTime_vs_team_prev_lapbucket_mean_pct",
            "lapTime_vs_race_prev_lapbucket_mean_pct",
        ]
        merged = prepared_base[["race", "driver", "lapNumber", *cols]].merge(
            prepared_changed[["race", "driver", "lapNumber", *cols]],
            on=["race", "driver", "lapNumber"],
            suffixes=("_base", "_changed"),
            how="inner",
        )
        early = merged[merged["lapNumber"] <= 2].copy()

        for col in cols:
            self.assertTrue(
                np.allclose(
                    early[f"{col}_base"].to_numpy(dtype=float),
                    early[f"{col}_changed"].to_numpy(dtype=float),
                    atol=1e-9,
                )
            )

    def test_same_lap_reorder_is_invariant(self) -> None:
        base = _base_features()
        reordered = _base_features().copy()
        reordered = reordered.sort_values(
            by=["race", "lapNumber", "driver"],
            ascending=[True, True, False],
            kind="mergesort",
        ).reset_index(drop=True)

        prepared_base = _prepare_features(
            base,
            drop_zones=None,
            source_year_fallback=2025,
            track_agnostic_mode=TRACK_PERCENTAGE_RACE_TEAM_V1,
        )
        prepared_reordered = _prepare_features(
            reordered,
            drop_zones=None,
            source_year_fallback=2025,
            track_agnostic_mode=TRACK_PERCENTAGE_RACE_TEAM_V1,
        )

        cols = [
            "race_progress_pct",
            "lapTime_vs_driver_prev_mean_pct",
            "lapTime_vs_team_prev_lapbucket_mean_pct",
            "lapTime_vs_race_prev_lapbucket_mean_pct",
        ]
        left = prepared_base[["race", "driver", "lapNumber", *cols]].sort_values(
            by=["race", "driver", "lapNumber"], kind="mergesort"
        )
        right = prepared_reordered[["race", "driver", "lapNumber", *cols]].sort_values(
            by=["race", "driver", "lapNumber"], kind="mergesort"
        )
        for col in cols:
            self.assertTrue(
                np.allclose(
                    left[col].to_numpy(dtype=float),
                    right[col].to_numpy(dtype=float),
                    atol=1e-9,
                )
            )

    def test_unknown_race_mapping_fails_fast(self) -> None:
        unknown = _base_features().copy()
        unknown["race"] = "2025 :: Unknown Experimental Grand Prix"
        with self.assertRaises(ValueError):
            _prepare_features(
                unknown,
                drop_zones=None,
                source_year_fallback=2025,
                track_agnostic_mode=TRACK_PERCENTAGE_V1,
            )


if __name__ == "__main__":
    unittest.main()
