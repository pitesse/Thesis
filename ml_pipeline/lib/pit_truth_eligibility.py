"""Pit truth eligibility helpers for raw vs clean actionable diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
import unicodedata

import numpy as np
import pandas as pd

TRUTH_LENS_RAW = "raw"
TRUTH_LENS_CLEAN_ACTIONABLE = "clean_actionable"
TRUTH_LENS_CLEAN_DRY_STRATEGY = "clean_dry_strategy"
TRUTH_LENSES = {
    TRUTH_LENS_RAW,
    TRUTH_LENS_CLEAN_ACTIONABLE,
    TRUTH_LENS_CLEAN_DRY_STRATEGY,
}


@dataclass(frozen=True)
class PitTruthLensRules:
    name: str
    note: str


LENS_RULES = {
    TRUTH_LENS_RAW: PitTruthLensRules(
        name=TRUTH_LENS_RAW,
        note="All deduplicated pit_timings events in evaluated race/driver universe.",
    ),
    TRUTH_LENS_CLEAN_ACTIONABLE: PitTruthLensRules(
        name=TRUTH_LENS_CLEAN_ACTIONABLE,
        note=(
            "Exclude lap<=1, red-flag/suspension, missing/out-of-range lap, and non-universe pits."
        ),
    ),
    TRUTH_LENS_CLEAN_DRY_STRATEGY: PitTruthLensRules(
        name=TRUTH_LENS_CLEAN_DRY_STRATEGY,
        note=(
            "clean_actionable plus exclude wet/intermediate/rain-affected and lap<=3 under non-GREEN."
        ),
    ),
}


def normalize_status(value: object) -> str:
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return "1"
    for token in ("5", "4", "7", "6", "2", "1"):
        if token in text:
            return token
    return text.upper()


def regime_from_status(value: object) -> str:
    code = normalize_status(value)
    if code in {"4", "6", "7"}:
        return "CAUTION"
    if code == "5":
        return "RED"
    if code in {"1", "2"}:
        return "GREEN"
    return "OTHER"


def _norm_race(value: object) -> str:
    text = str(value or "").strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.lower().split())


def _norm_driver(value: object) -> str:
    return str(value or "").strip().upper()


def _normalize_compound(value: object) -> str:
    text = str(value or "").strip().upper()
    if text in {"SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET"}:
        return text
    if text in {"", "NAN", "NONE", "<NA>"}:
        return ""
    return text


def _build_eval_context(pit_evals: pd.DataFrame) -> pd.DataFrame:
    if pit_evals.empty:
        return pd.DataFrame(
            columns=[
                "year",
                "race_norm",
                "race",
                "driver",
                "pit_lap_num",
                "track_status",
                "regime",
                "compound",
                "tyreLife",
                "result",
                "resolvedVia",
            ]
        )

    work = pit_evals.copy()
    if "pitLapNumber" not in work.columns:
        return pd.DataFrame()
    work["pit_lap_num"] = pd.to_numeric(work["pitLapNumber"], errors="coerce")
    work = work[work["pit_lap_num"].notna()].copy()
    if work.empty:
        return pd.DataFrame()

    work["pit_lap_num"] = work["pit_lap_num"].astype(int)
    work["driver"] = work.get("driver", "").map(_norm_driver)
    work["race"] = work.get("race", "").astype(str)
    work["race_norm"] = work["race"].map(_norm_race)
    work["track_status"] = work.get("trackStatusAtPit", "").map(normalize_status)
    work["regime"] = work["track_status"].map(regime_from_status)
    if "compound" in work.columns:
        work["compound"] = work["compound"].map(_normalize_compound)
    else:
        work["compound"] = ""
    work["tyreLife"] = pd.to_numeric(work.get("tyreAgeAtPit"), errors="coerce")
    if "result" in work.columns:
        work["result"] = work["result"].astype(str)
    else:
        work["result"] = ""
    if "resolvedVia" in work.columns:
        work["resolvedVia"] = work["resolvedVia"].astype(str)
    else:
        work["resolvedVia"] = ""

    # year is not guaranteed in pit_evals; keep nullable.
    work["year"] = pd.to_numeric(work.get("year"), errors="coerce")

    keep = [
        "year",
        "race_norm",
        "race",
        "driver",
        "pit_lap_num",
        "track_status",
        "regime",
        "compound",
        "tyreLife",
        "result",
        "resolvedVia",
    ]
    out = work[keep].copy()
    out.sort_values(by=["race_norm", "driver", "pit_lap_num"], kind="mergesort", inplace=True)
    out = out.drop_duplicates(subset=["race_norm", "driver", "pit_lap_num"], keep="first")
    out.reset_index(drop=True, inplace=True)
    return out


def _load_prepared_events(path: Path | None) -> pd.DataFrame:
    if path is None or not path.exists():
        return pd.DataFrame()
    work = pd.read_csv(path)
    required = {"year", "race", "driver", "pit_lap"}
    if not required.issubset(set(work.columns)):
        return pd.DataFrame()

    out = work.copy()
    out["pit_lap_num"] = pd.to_numeric(out["pit_lap"], errors="coerce")
    out = out[out["pit_lap_num"].notna()].copy()
    out["pit_lap_num"] = out["pit_lap_num"].astype(int)
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    out["race_norm"] = out["race"].map(_norm_race)
    out["driver"] = out["driver"].map(_norm_driver)
    out["compound"] = out.get("compound", "").map(_normalize_compound)
    out["tyreLife"] = pd.to_numeric(out.get("tyreLife"), errors="coerce")
    out["track_status"] = out.get("trackStatus", "").map(normalize_status)
    out["regime"] = out.get("regime", "").astype(str).replace("", np.nan)
    out["rainfall"] = pd.to_numeric(out.get("rainfall"), errors="coerce")
    out["race_progress_pct"] = pd.to_numeric(out.get("race_progress_pct"), errors="coerce")
    out = out[
        [
            "year",
            "race_norm",
            "race",
            "driver",
            "pit_lap_num",
            "compound",
            "tyreLife",
            "track_status",
            "regime",
            "rainfall",
            "race_progress_pct",
        ]
    ].drop_duplicates(subset=["year", "race_norm", "driver", "pit_lap_num"], keep="first")
    out.reset_index(drop=True, inplace=True)
    return out


def _build_ml_features_context(ml_features: pd.DataFrame) -> pd.DataFrame:
    if ml_features.empty:
        return pd.DataFrame()
    required = {"race", "driver", "lapNumber"}
    if not required.issubset(set(ml_features.columns)):
        return pd.DataFrame()

    work = ml_features.copy()
    work["lap_num"] = pd.to_numeric(work["lapNumber"], errors="coerce")
    work = work[work["lap_num"].notna()].copy()
    if work.empty:
        return pd.DataFrame()

    work["lap_num"] = work["lap_num"].astype(int)
    work["driver"] = work["driver"].map(_norm_driver)
    work["race"] = work["race"].astype(str)
    work["race_norm"] = work["race"].map(_norm_race)
    work["compound"] = work.get("compound", "").map(_normalize_compound)
    work["tyreLife"] = pd.to_numeric(work.get("tyreLife"), errors="coerce")
    work["track_status"] = work.get("trackStatus", "").map(normalize_status)
    work["regime"] = work["track_status"].map(regime_from_status)
    work["rainfall"] = pd.to_numeric(work.get("rainfall"), errors="coerce")

    keep = [
        "race_norm",
        "race",
        "driver",
        "lap_num",
        "compound",
        "tyreLife",
        "track_status",
        "regime",
        "rainfall",
    ]
    out = work[keep].copy()
    out.sort_values(by=["race_norm", "driver", "lap_num"], kind="mergesort", inplace=True)
    out = out.drop_duplicates(subset=["race_norm", "driver", "lap_num"], keep="last")
    out.reset_index(drop=True, inplace=True)
    return out


def _asof_ml_context(ml_context: pd.DataFrame, race_norm: str, driver: str, pit_lap: int) -> dict[str, object]:
    if ml_context.empty:
        return {}
    subset = ml_context[
        (ml_context["race_norm"] == race_norm)
        & (ml_context["driver"] == driver)
        & (ml_context["lap_num"] <= pit_lap)
    ]
    if subset.empty:
        return {}
    row = subset.iloc[-1]
    return {
        "compound": row.get("compound"),
        "tyreLife": row.get("tyreLife"),
        "track_status": row.get("track_status"),
        "regime": row.get("regime"),
        "rainfall": row.get("rainfall"),
        "context_lap": row.get("lap_num"),
    }


def _pick_first_non_empty(values: Iterable[object]) -> object | None:
    for value in values:
        if value is None:
            continue
        if isinstance(value, float) and np.isnan(value):
            continue
        text = str(value).strip()
        if text == "" or text.lower() == "nan" or text == "<NA>":
            continue
        return value
    return None


def _categorize_row(row: pd.Series) -> tuple[str, str, bool, bool]:
    categories: list[str] = []

    pit_lap = pd.to_numeric(row.get("pit_lap_num"), errors="coerce")
    track_status = normalize_status(row.get("track_status", ""))
    compound = _normalize_compound(row.get("compound", ""))
    rainfall = pd.to_numeric(row.get("rainfall"), errors="coerce")
    regime = str(row.get("regime", "")).upper().strip() or regime_from_status(track_status)
    result = str(row.get("result", "") or "").upper()
    resolved_via = str(row.get("resolvedVia", "") or "").upper()
    universe_eligible = bool(row.get("eligible_universe", False))

    if not np.isfinite(pit_lap):
        categories.append("MISSING_CONTEXT")
        return "MISSING_CONTEXT", "", True, False

    pit_lap = int(pit_lap)
    if pit_lap <= 1:
        categories.append("EARLY_LAP_1")
    elif pit_lap <= 3:
        categories.append("EARLY_LAP_2_3")

    if track_status == "5" or regime == "RED":
        categories.append("RED_FLAG_OR_SUSPENSION")

    if compound in {"WET", "INTERMEDIATE"}:
        categories.append("WET_OR_INTERMEDIATE")

    if np.isfinite(rainfall) and float(rainfall) > 0.0:
        categories.append("RAIN_AFFECTED")

    if "EARLY_LAP_FILTER" in resolved_via or "UNRESOLVED_MISSING_PRE_GAP" in result:
        categories.append("DAMAGE_OR_INCIDENT_SUSPECTED")

    if pit_lap <= 1 and regime in {"RED", "CAUTION", "OTHER"}:
        categories.append("PIT_LANE_START_OR_FORMATION_SUSPECTED")

    if not categories:
        primary = "NORMAL_RACE_PIT" if universe_eligible else "UNKNOWN_NEEDS_REVIEW"
    else:
        precedence = [
            "RED_FLAG_OR_SUSPENSION",
            "EARLY_LAP_1",
            "EARLY_LAP_2_3",
            "WET_OR_INTERMEDIATE",
            "RAIN_AFFECTED",
            "DAMAGE_OR_INCIDENT_SUSPECTED",
            "PIT_LANE_START_OR_FORMATION_SUSPECTED",
            "MISSING_CONTEXT",
        ]
        primary = next((cat for cat in precedence if cat in categories), categories[0])

    secondary = ";".join(sorted(set(cat for cat in categories if cat != primary)))

    eligible_clean_actionable = universe_eligible
    if pit_lap <= 1:
        eligible_clean_actionable = False
    if track_status == "5" or regime == "RED":
        eligible_clean_actionable = False

    eligible_clean_dry_strategy = eligible_clean_actionable
    if compound in {"WET", "INTERMEDIATE"}:
        eligible_clean_dry_strategy = False
    if np.isfinite(rainfall) and float(rainfall) > 0.0:
        eligible_clean_dry_strategy = False
    if pit_lap <= 3 and regime != "GREEN":
        eligible_clean_dry_strategy = False

    return primary, secondary, eligible_clean_actionable, eligible_clean_dry_strategy


def build_pit_truth_universe(
    pit_timings: pd.DataFrame,
    suggestions_source: pd.DataFrame,
    pit_evals: pd.DataFrame,
    *,
    ml_features: pd.DataFrame | None = None,
    prepared_pit_events: pd.DataFrame | None = None,
    split_tag: str = "descriptive_all_years",
) -> pd.DataFrame:
    if pit_timings.empty:
        return pd.DataFrame(
            columns=[
                "race",
                "year",
                "driver",
                "pit_lap_num",
                "pit_key",
                "track_status",
                "regime",
                "compound",
                "stint",
                "tyreLife",
                "rainfall",
                "race_progress_pct",
                "result",
                "resolvedVia",
                "source_fields_used",
                "truth_category",
                "truth_secondary_categories",
                "eligible_universe",
                "eligible_raw",
                "eligible_clean_actionable",
                "eligible_clean_dry_strategy",
                "split_tag",
            ]
        )

    race_driver_universe = {
        (_norm_race(race), _norm_driver(driver))
        for race, driver in suggestions_source[["race", "driver"]].dropna().itertuples(index=False, name=None)
    }

    timings = pit_timings.copy()
    timings["pit_lap_num"] = pd.to_numeric(timings.get("lapNumber"), errors="coerce")
    timings = timings[timings["pit_lap_num"].notna()].copy()
    timings["pit_lap_num"] = timings["pit_lap_num"].astype(int)
    timings["race"] = timings.get("race", "").astype(str)
    timings["race_norm"] = timings["race"].map(_norm_race)
    timings["driver"] = timings.get("driver", "").map(_norm_driver)
    timings["year"] = pd.to_datetime(timings.get("date"), errors="coerce", utc=True).dt.year.astype("Int64")
    timings = timings.drop_duplicates(subset=["race_norm", "driver", "pit_lap_num"], keep="first").copy()

    eval_ctx = _build_eval_context(pit_evals)
    prep_ctx = prepared_pit_events.copy() if prepared_pit_events is not None else pd.DataFrame()
    ml_ctx = _build_ml_features_context(ml_features.copy() if ml_features is not None else pd.DataFrame())

    eval_idx = {
        (row.race_norm, row.driver, int(row.pit_lap_num)): row
        for row in eval_ctx.itertuples(index=False)
    }
    prep_idx = {
        (str(row.year), row.race_norm, row.driver, int(row.pit_lap_num)): row
        for row in prep_ctx.itertuples(index=False)
    }
    prep_idx_noyear = {
        (row.race_norm, row.driver, int(row.pit_lap_num)): row
        for row in prep_ctx.itertuples(index=False)
    }

    rows: list[dict[str, object]] = []
    for _, r in timings.iterrows():
        race = str(r["race"])
        race_norm = str(r["race_norm"])
        driver = str(r["driver"])
        lap = int(r["pit_lap_num"])
        year_val = r.get("year")
        year_str = "" if pd.isna(year_val) else str(int(year_val))

        eval_row = eval_idx.get((race_norm, driver, lap))
        prep_row = prep_idx.get((year_str, race_norm, driver, lap)) if year_str else None
        if prep_row is None:
            prep_row = prep_idx_noyear.get((race_norm, driver, lap))
        ml_asof = _asof_ml_context(ml_ctx, race_norm, driver, lap)

        sources: list[str] = []

        track_status = None
        if eval_row is not None:
            track_status = getattr(eval_row, "track_status", None)
            if track_status not in (None, "", "nan", "NaN"):
                sources.append("pit_evals.trackStatusAtPit")
        if track_status in (None, "", "nan", "NaN") and prep_row is not None:
            track_status = getattr(prep_row, "track_status", None)
            if track_status not in (None, "", "nan", "NaN"):
                sources.append("prepared.trackStatus")
        if track_status in (None, "", "nan", "NaN") and ml_asof:
            track_status = ml_asof.get("track_status")
            if track_status not in (None, "", "nan", "NaN"):
                sources.append("ml_features.trackStatus@<=lap")

        compound = _pick_first_non_empty(
            [
                getattr(eval_row, "compound", None) if eval_row is not None else None,
                getattr(prep_row, "compound", None) if prep_row is not None else None,
                ml_asof.get("compound") if ml_asof else None,
            ]
        )
        if compound is not None:
            if eval_row is not None and _normalize_compound(getattr(eval_row, "compound", None)) == _normalize_compound(compound):
                sources.append("pit_evals.compound")
            elif prep_row is not None and _normalize_compound(getattr(prep_row, "compound", None)) == _normalize_compound(compound):
                sources.append("prepared.compound")
            elif ml_asof and _normalize_compound(ml_asof.get("compound")) == _normalize_compound(compound):
                sources.append("ml_features.compound@<=lap")

        tyre_life = _pick_first_non_empty(
            [
                getattr(eval_row, "tyreLife", None) if eval_row is not None else None,
                getattr(prep_row, "tyreLife", None) if prep_row is not None else None,
                ml_asof.get("tyreLife") if ml_asof else None,
            ]
        )
        if tyre_life is not None:
            if eval_row is not None and getattr(eval_row, "tyreLife", None) == tyre_life:
                sources.append("pit_evals.tyreAgeAtPit")
            elif prep_row is not None and getattr(prep_row, "tyreLife", None) == tyre_life:
                sources.append("prepared.tyreLife")
            elif ml_asof and ml_asof.get("tyreLife") == tyre_life:
                sources.append("ml_features.tyreLife@<=lap")

        rainfall = _pick_first_non_empty(
            [
                getattr(prep_row, "rainfall", None) if prep_row is not None else None,
                ml_asof.get("rainfall") if ml_asof else None,
            ]
        )
        if rainfall is not None:
            if prep_row is not None and getattr(prep_row, "rainfall", None) == rainfall:
                sources.append("prepared.rainfall")
            elif ml_asof and ml_asof.get("rainfall") == rainfall:
                sources.append("ml_features.rainfall@<=lap")

        race_progress = _pick_first_non_empty(
            [getattr(prep_row, "race_progress_pct", None) if prep_row is not None else None]
        )
        if race_progress is not None and prep_row is not None:
            sources.append("prepared.race_progress_pct")

        regime = regime_from_status(track_status)
        universe_eligible = (race_norm, driver) in race_driver_universe

        result = getattr(eval_row, "result", "") if eval_row is not None else ""
        resolved_via = getattr(eval_row, "resolvedVia", "") if eval_row is not None else ""

        row = {
            "race": race,
            "year": int(year_val) if pd.notna(year_val) else pd.NA,
            "driver": driver,
            "pit_lap_num": lap,
            "pit_key": (race, driver, lap),
            "track_status": normalize_status(track_status),
            "regime": regime,
            "compound": _normalize_compound(compound),
            "stint": getattr(prep_row, "stint", pd.NA) if prep_row is not None else pd.NA,
            "tyreLife": pd.to_numeric(tyre_life, errors="coerce"),
            "rainfall": pd.to_numeric(rainfall, errors="coerce"),
            "race_progress_pct": pd.to_numeric(race_progress, errors="coerce"),
            "result": str(result),
            "resolvedVia": str(resolved_via),
            "source_fields_used": ";".join(dict.fromkeys(sources)) if sources else "none",
            "eligible_universe": bool(universe_eligible),
            "eligible_raw": True,
            "split_tag": split_tag,
        }
        row_series = pd.Series(row)
        primary, secondary, eligible_clean_actionable, eligible_clean_dry_strategy = _categorize_row(row_series)
        row["truth_category"] = primary
        row["truth_secondary_categories"] = secondary
        row["eligible_clean_actionable"] = bool(eligible_clean_actionable)
        row["eligible_clean_dry_strategy"] = bool(eligible_clean_dry_strategy)
        rows.append(row)

    out = pd.DataFrame(rows)
    out.sort_values(by=["year", "race", "pit_lap_num", "driver"], kind="mergesort", inplace=True)
    out.reset_index(drop=True, inplace=True)
    return out


def lens_flag_column(truth_lens: str) -> str:
    if truth_lens == TRUTH_LENS_RAW:
        return "eligible_raw"
    if truth_lens == TRUTH_LENS_CLEAN_ACTIONABLE:
        return "eligible_clean_actionable"
    if truth_lens == TRUTH_LENS_CLEAN_DRY_STRATEGY:
        return "eligible_clean_dry_strategy"
    raise ValueError(f"unsupported truth_lens={truth_lens!r}; expected one of {sorted(TRUTH_LENSES)}")


def eligible_actual_counts(
    pit_truth_universe: pd.DataFrame,
    *,
    truth_lens: str,
    regime: str = "ALL",
) -> tuple[int, int]:
    if pit_truth_universe.empty:
        return 0, 0

    raw_count = int(len(pit_truth_universe))
    flag_col = lens_flag_column(truth_lens)
    eligible = pit_truth_universe[
        (pit_truth_universe["eligible_universe"] == True)  # noqa: E712
        & (pit_truth_universe[flag_col] == True)  # noqa: E712
    ]
    if regime != "ALL":
        eligible = eligible[eligible["regime"] == regime]
    return raw_count, int(len(eligible))


def eligible_pit_key_set(
    pit_truth_universe: pd.DataFrame,
    *,
    truth_lens: str,
    regime: str = "ALL",
) -> set[tuple[str, str, int]]:
    if pit_truth_universe.empty:
        return set()
    flag_col = lens_flag_column(truth_lens)
    eligible = pit_truth_universe[
        (pit_truth_universe["eligible_universe"] == True)  # noqa: E712
        & (pit_truth_universe[flag_col] == True)  # noqa: E712
    ]
    if regime != "ALL":
        eligible = eligible[eligible["regime"] == regime]
    out: set[tuple[str, str, int]] = set()
    for _, row in eligible.iterrows():
        out.add((str(row["race"]), str(row["driver"]), int(row["pit_lap_num"])))
    return out


def load_prepared_events_from_csv(path: str | Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    p = Path(path)
    return _load_prepared_events(p)
