"""Unit checks for comparator episode gating and pit_any strict scoring."""

from __future__ import annotations

import unittest

import pandas as pd

try:
    from comparator_heuristic import (
        ACTIONABLE_MODE_GOOD_PIT_ONLY,
        ACTIONABLE_MODE_PIT_NOW_ONLY,
        ACTIONABLE_MODE_PIT_NOW_PLUS_GOOD_PIT,
        OUTCOME_PIT_ANY_H2,
        OUTCOME_PIT_SUCCESS_H2,
        _build_comparator_dataset,
    )
except ImportError:
    from ml_pipeline.lib.comparator_heuristic import (  # type: ignore
        ACTIONABLE_MODE_GOOD_PIT_ONLY,
        ACTIONABLE_MODE_PIT_NOW_ONLY,
        ACTIONABLE_MODE_PIT_NOW_PLUS_GOOD_PIT,
        OUTCOME_PIT_ANY_H2,
        OUTCOME_PIT_SUCCESS_H2,
        _build_comparator_dataset,
    )


class ComparatorEpisodeTest(unittest.TestCase):
    def _base_suggestions(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"race": "2025 :: Test GP", "driver": "AAA", "lapNumber": 10, "suggestionLabel": "PIT_NOW", "totalScore": 91.0},
                {"race": "2025 :: Test GP", "driver": "AAA", "lapNumber": 11, "suggestionLabel": "PIT_NOW", "totalScore": 90.0},
                {"race": "2025 :: Test GP", "driver": "AAA", "lapNumber": 13, "suggestionLabel": "GOOD_PIT", "totalScore": 81.0},
            ]
        )

    def _base_pit_evals(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "race": "2025 :: Test GP",
                    "driver": "AAA",
                    "pitLapNumber": 14,
                    "result": "SUCCESS_DEFEND",
                }
            ]
        )

    def _base_pit_timings(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "race": "2025 :: Test GP",
                    "driver": "AAA",
                    "lapNumber": 14,
                    "pitInTime": 1234.0,
                    "pitOutTime": 1251.0,
                }
            ]
        )

    def test_episode_gating_suppresses_repeated_actionables_within_horizon(self) -> None:
        suggestions = self._base_suggestions()
        pit_evals = self._base_pit_evals()
        pit_timings = self._base_pit_timings()

        row_level = _build_comparator_dataset(
            suggestions,
            pit_evals,
            horizon=2,
            outcome_mode=OUTCOME_PIT_SUCCESS_H2,
            pit_timings=pit_timings,
            episode_level=False,
        )
        episode_level = _build_comparator_dataset(
            suggestions,
            pit_evals,
            horizon=2,
            outcome_mode=OUTCOME_PIT_SUCCESS_H2,
            pit_timings=pit_timings,
            episode_level=True,
            episode_cooldown_laps=2,
        )

        self.assertEqual(len(row_level), 3)
        self.assertEqual(len(episode_level), 2)
        self.assertListEqual(
            episode_level["suggestion_lap"].astype(int).tolist(),
            [10, 13],
        )

    def test_episode_reopens_after_pit_event(self) -> None:
        suggestions = pd.DataFrame(
            [
                {"race": "2025 :: Test GP", "driver": "AAA", "lapNumber": 10, "suggestionLabel": "PIT_NOW", "totalScore": 90.0},
                {"race": "2025 :: Test GP", "driver": "AAA", "lapNumber": 11, "suggestionLabel": "PIT_NOW", "totalScore": 89.0},
            ]
        )
        pit_evals = pd.DataFrame(
            [
                {"race": "2025 :: Test GP", "driver": "AAA", "pitLapNumber": 10, "result": "SUCCESS_DEFEND"},
                {"race": "2025 :: Test GP", "driver": "AAA", "pitLapNumber": 11, "result": "SUCCESS_DEFEND"},
            ]
        )
        pit_timings = pd.DataFrame(
            [
                {"race": "2025 :: Test GP", "driver": "AAA", "lapNumber": 10, "pitInTime": 1000.0, "pitOutTime": 1010.0},
                {"race": "2025 :: Test GP", "driver": "AAA", "lapNumber": 11, "pitInTime": 1100.0, "pitOutTime": 1110.0},
            ]
        )

        episode_level = _build_comparator_dataset(
            suggestions,
            pit_evals,
            horizon=2,
            outcome_mode=OUTCOME_PIT_SUCCESS_H2,
            pit_timings=pit_timings,
            episode_level=True,
            episode_cooldown_laps=5,
        )

        self.assertEqual(len(episode_level), 2)
        self.assertListEqual(episode_level["suggestion_lap"].astype(int).tolist(), [10, 11])

    def test_pit_any_mode_scores_no_match_as_false_positive(self) -> None:
        suggestions = pd.DataFrame(
            [
                {"race": "2025 :: Test GP", "driver": "AAA", "lapNumber": 30, "suggestionLabel": "PIT_NOW", "totalScore": 88.0},
            ]
        )
        pit_evals = self._base_pit_evals()
        pit_timings = self._base_pit_timings()

        result = _build_comparator_dataset(
            suggestions,
            pit_evals,
            horizon=2,
            outcome_mode=OUTCOME_PIT_ANY_H2,
            pit_timings=pit_timings,
            episode_level=False,
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(str(result.iloc[0]["outcome_class"]), "0")
        self.assertEqual(str(result.iloc[0]["exclusion_reason"]), "")

    def test_episode_cooldown_independent_from_horizon(self) -> None:
        suggestions = self._base_suggestions()
        pit_evals = self._base_pit_evals()
        pit_timings = self._base_pit_timings()

        episode_level = _build_comparator_dataset(
            suggestions,
            pit_evals,
            horizon=2,
            outcome_mode=OUTCOME_PIT_SUCCESS_H2,
            pit_timings=pit_timings,
            episode_level=True,
            episode_cooldown_laps=5,
        )
        self.assertEqual(len(episode_level), 1)
        self.assertListEqual(episode_level["suggestion_lap"].astype(int).tolist(), [10])

    def test_actionable_modes_filter_labels(self) -> None:
        suggestions = self._base_suggestions()
        pit_evals = self._base_pit_evals()
        pit_timings = self._base_pit_timings()

        pit_now_only = _build_comparator_dataset(
            suggestions,
            pit_evals,
            horizon=2,
            outcome_mode=OUTCOME_PIT_SUCCESS_H2,
            pit_timings=pit_timings,
            actionable_mode=ACTIONABLE_MODE_PIT_NOW_ONLY,
        )
        good_only = _build_comparator_dataset(
            suggestions,
            pit_evals,
            horizon=2,
            outcome_mode=OUTCOME_PIT_SUCCESS_H2,
            pit_timings=pit_timings,
            actionable_mode=ACTIONABLE_MODE_GOOD_PIT_ONLY,
        )
        both = _build_comparator_dataset(
            suggestions,
            pit_evals,
            horizon=2,
            outcome_mode=OUTCOME_PIT_SUCCESS_H2,
            pit_timings=pit_timings,
            actionable_mode=ACTIONABLE_MODE_PIT_NOW_PLUS_GOOD_PIT,
        )

        self.assertListEqual(sorted(pit_now_only["suggestion_label"].unique().tolist()), ["PIT_NOW"])
        self.assertListEqual(sorted(good_only["suggestion_label"].unique().tolist()), ["GOOD_PIT"])
        self.assertSetEqual(set(both["suggestion_label"].unique().tolist()), {"PIT_NOW", "GOOD_PIT"})


if __name__ == "__main__":
    unittest.main()
