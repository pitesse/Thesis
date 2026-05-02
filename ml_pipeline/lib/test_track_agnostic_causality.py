"""Unit checks for causal track-agnostic feature generation."""

from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

try:
    from data_preparation import TRACK_AGNOSTIC_OFF, TRACK_AGNOSTIC_V1, _prepare_features
except ImportError:
    from ml_pipeline.lib.data_preparation import (  # type: ignore
        TRACK_AGNOSTIC_OFF,
        TRACK_AGNOSTIC_V1,
        _prepare_features,
    )


def _base_features() -> pd.DataFrame:
    rows = [
        # Deliberately unsorted input to validate deterministic race-timeline sorting.
        {
            "race": "2025 :: Synthetic GP",
            "driver": "B",
            "lapNumber": 1,
            "compound": "MEDIUM",
            "tyreLife": 1,
            "lapTime": 91.0,
            "gapAhead": 1.0,
            "gapBehind": 2.0,
            "trackTemp": 30.0,
            "airTemp": 20.0,
            "humidity": 40.0,
            "speedTrap": 300.0,
            "rainfall": False,
            "team": "TeamB",
            "position": 2,
            "pitLoss": 22.0,
            "trackStatus": 1,
        },
        {
            "race": "2025 :: Synthetic GP",
            "driver": "A",
            "lapNumber": 1,
            "compound": "MEDIUM",
            "tyreLife": 1,
            "lapTime": 90.0,
            "gapAhead": 0.5,
            "gapBehind": 1.5,
            "trackTemp": 10.0,
            "airTemp": 10.0,
            "humidity": 20.0,
            "speedTrap": 280.0,
            "rainfall": False,
            "team": "TeamA",
            "position": 1,
            "pitLoss": 22.0,
            "trackStatus": 1,
        },
        {
            "race": "2025 :: Synthetic GP",
            "driver": "A",
            "lapNumber": 2,
            "compound": "MEDIUM",
            "tyreLife": 2,
            "lapTime": 92.0,
            "gapAhead": 0.7,
            "gapBehind": 1.2,
            "trackTemp": 20.0,
            "airTemp": 15.0,
            "humidity": 30.0,
            "speedTrap": 290.0,
            "rainfall": False,
            "team": "TeamA",
            "position": 1,
            "pitLoss": 22.0,
            "trackStatus": 1,
        },
        {
            "race": "2025 :: Synthetic GP",
            "driver": "B",
            "lapNumber": 2,
            "compound": "MEDIUM",
            "tyreLife": 2,
            "lapTime": 93.0,
            "gapAhead": 1.2,
            "gapBehind": 2.5,
            "trackTemp": 40.0,
            "airTemp": 25.0,
            "humidity": 50.0,
            "speedTrap": 310.0,
            "rainfall": False,
            "team": "TeamB",
            "position": 2,
            "pitLoss": 22.0,
            "trackStatus": 1,
        },
    ]
    return pd.DataFrame(rows)


def _sorted_keys(df: pd.DataFrame) -> list[tuple[str, int]]:
    work = df[["driver", "lapNumber"]].copy()
    return [(str(r.driver), int(r.lapNumber)) for r in work.itertuples(index=False)]


class TrackAgnosticCausalityTest(unittest.TestCase):
    def test_track_agnostic_off_adds_no_expanding_columns(self) -> None:
        prepared = _prepare_features(
            _base_features(),
            drop_zones=None,
            source_year_fallback=2025,
            track_agnostic_mode=TRACK_AGNOSTIC_OFF,
        )
        unexpected = [col for col in prepared.columns if col.endswith("_expanding_z")]
        self.assertEqual(unexpected, [])

    def test_track_agnostic_v1_generates_expected_causal_z(self) -> None:
        prepared = _prepare_features(
            _base_features(),
            drop_zones=None,
            source_year_fallback=2025,
            track_agnostic_mode=TRACK_AGNOSTIC_V1,
        )

        expected_columns = {
            "trackTemp_expanding_z",
            "airTemp_expanding_z",
            "humidity_expanding_z",
            "speedTrap_expanding_z",
            "lapTime_expanding_z",
        }
        self.assertTrue(expected_columns.issubset(set(prepared.columns)))

        keys = _sorted_keys(prepared)
        lookup = {
            (str(row.driver), int(row.lapNumber)): float(row.trackTemp_expanding_z)
            for row in prepared.itertuples(index=False)
        }

        # Output frame is sorted by (race, driver, lap) in _prepare_features.
        self.assertEqual(keys, [("A", 1), ("A", 2), ("B", 1), ("B", 2)])
        # Causal z-stats are computed internally on race timeline (lap, driver): A1, B1, A2, B2.
        # A1 and B1 do not have enough past race history for stable std.
        self.assertAlmostEqual(lookup[("A", 1)], 0.0, places=8)
        self.assertAlmostEqual(lookup[("B", 1)], 0.0, places=8)
        # A2 should use only past values from A1 and B1 => z ~= 0.0.
        self.assertAlmostEqual(lookup[("A", 2)], 0.0, places=8)
        # B2 should use past {A1, B1, A2} => z ~= 2.449489743.
        self.assertAlmostEqual(lookup[("B", 2)], 2.449489743, places=6)

    def test_future_change_does_not_modify_earlier_z(self) -> None:
        base = _base_features()
        changed = _base_features()
        changed.loc[(changed["driver"] == "B") & (changed["lapNumber"] == 2), "trackTemp"] = 999.0

        prepared_base = _prepare_features(
            base,
            drop_zones=None,
            source_year_fallback=2025,
            track_agnostic_mode=TRACK_AGNOSTIC_V1,
        )
        prepared_changed = _prepare_features(
            changed,
            drop_zones=None,
            source_year_fallback=2025,
            track_agnostic_mode=TRACK_AGNOSTIC_V1,
        )

        key_cols = ["race", "driver", "lapNumber"]
        cols = key_cols + ["trackTemp_expanding_z"]
        merged = prepared_base[cols].merge(
            prepared_changed[cols],
            on=key_cols,
            suffixes=("_base", "_changed"),
            how="inner",
        )
        merged.sort_values(by=["lapNumber", "driver"], inplace=True)

        # Earlier rows (up to A2) must be unchanged when only the future B2 raw value changes.
        earlier = merged.iloc[:3]
        self.assertTrue(
            np.allclose(
                earlier["trackTemp_expanding_z_base"].to_numpy(dtype=float),
                earlier["trackTemp_expanding_z_changed"].to_numpy(dtype=float),
                atol=1e-9,
            )
        )


if __name__ == "__main__":
    unittest.main()
