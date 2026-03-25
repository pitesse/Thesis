import glob
from pathlib import Path

import pandas as pd


PIT_PATTERN = "data_lake/pit_evals_2023_season_*.jsonl"
ML_PATTERN = "data_lake/ml_features_2023_season_*.jsonl"
EXTREME_GAP_THRESHOLD_PCT = 20.0
WET_COMPOUNDS = ("INTERMEDIATE", "WET")


def _pct(numerator: int | float, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return float(numerator) / denominator * 100.0


def _nan_if_missing_mean(df: pd.DataFrame, column: str) -> float:
    if column not in df.columns:
        return float("nan")
    return df[column].mean()


def _nan_if_missing_median(df: pd.DataFrame, column: str) -> float:
    if column not in df.columns:
        return float("nan")
    return df[column].median()


def _latest_file(pattern: str) -> str | None:
    matches = glob.glob(pattern)
    if not matches:
        return None
    return max(matches, key=lambda p: Path(p).stat().st_mtime)


def _safe_col(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([pd.NA] * len(df), index=df.index)
    return df[col]


def audit_season() -> None:
    print("\n=== F1 FORENSIC DOMAIN LOGIC AUDIT ===")

    pit_file = _latest_file(PIT_PATTERN)
    ml_file = _latest_file(ML_PATTERN)

    if not pit_file:
        print(f"No files found for pattern: {PIT_PATTERN}")
        return
    if not ml_file:
        print(f"No files found for pattern: {ML_PATTERN}")
        return

    print(f"Pit eval file: {pit_file}")
    print(f"ML feature file: {ml_file}")

    df_pit = pd.read_json(pit_file, lines=True)
    df_ml = pd.read_json(ml_file, lines=True)

    print(f"Rows, pit_evals: {len(df_pit):,}")
    print(f"Rows, ml_features: {len(df_ml):,}")

    result_col = _safe_col(df_pit, "result")
    defend_mask = result_col.eq("SUCCESS_DEFEND")
    defend_count = int(defend_mask.sum())
    defend_pct = _pct(defend_count, len(df_pit))

    print("\n[1] SUCCESS_DEFEND epidemic")
    print(f"SUCCESS_DEFEND count: {defend_count:,} ({defend_pct:.2f}%)")

    null_targets = ["prePitGapAhead", "postPitGapToRival", "baselineLapTime"]
    null_summary = []
    for col in null_targets:
        series = _safe_col(df_pit, col)
        total_null = int(series.isna().sum())
        defend_null = int(series[defend_mask].isna().sum())
        null_summary.append((col, total_null, defend_null))

    for col, total_null, defend_null in null_summary:
        print(
            f"  {col}: null total={total_null:,}, null in SUCCESS_DEFEND={defend_null:,}"
        )

    resolved = _safe_col(df_pit, "resolvedVia")
    print("ResolvedVia distribution (all):")
    print(resolved.value_counts(dropna=False).to_string())
    print("ResolvedVia distribution (SUCCESS_DEFEND only):")
    print(resolved[defend_mask].value_counts(dropna=False).to_string())

    sample_cols = [
        "race",
        "driver",
        "pitLapNumber",
        "resolvedVia",
        "prePitGapAhead",
        "postPitGapToRival",
        "baselineLapTime",
        "rivalAhead",
        "result",
    ]
    available_samples = [c for c in sample_cols if c in df_pit.columns]
    print("Sample SUCCESS_DEFEND rows (3):")
    print(df_pit.loc[defend_mask, available_samples].head(3).to_string(index=False))

    print("\n[2] Safety car / yellow status propagation")
    pit_status = _safe_col(df_pit, "trackStatusAtPit")
    ml_status = _safe_col(df_ml, "trackStatus")

    print(f"trackStatusAtPit null count: {int(pit_status.isna().sum()):,}")
    print("trackStatusAtPit unique values:")
    print(sorted(pit_status.dropna().astype(str).unique().tolist()))

    print(f"trackStatus (ml_features) null count: {int(ml_status.isna().sum()):,}")
    print("trackStatus (ml_features) unique values:")
    print(sorted(ml_status.dropna().astype(str).unique().tolist()))

    print("\n[3] Mathematically impossible gaps")
    gap = _safe_col(df_pit, "gapDeltaPct")
    extreme = df_pit[gap.abs() > EXTREME_GAP_THRESHOLD_PCT].copy()
    print(
        f"Rows with |gapDeltaPct| > {EXTREME_GAP_THRESHOLD_PCT:.1f}%: {len(extreme):,}"
    )
    if not extreme.empty:
        offender_cols = [
            "race",
            "driver",
            "pitLapNumber",
            "rivalAhead",
            "prePitGapAhead",
            "postPitGapToRival",
            "gapDeltaPct",
            "result",
            "resolvedVia",
        ]
        offender_cols = [c for c in offender_cols if c in extreme.columns]
        top5 = extreme.sort_values(
            "gapDeltaPct", key=lambda s: s.abs(), ascending=False
        ).head(5)
        print("Top 5 offenders by |gapDeltaPct|:")
        print(top5[offender_cols].to_string(index=False))

    print("\n[4] Wet weather stop distortion")
    compound = _safe_col(df_pit, "compound")
    wet_mask = compound.isin(WET_COMPOUNDS)
    wet = df_pit[wet_mask]
    dry = df_pit[~wet_mask]

    wet_avg = _nan_if_missing_mean(wet, "gapDeltaPct")
    dry_avg = _nan_if_missing_mean(dry, "gapDeltaPct")
    wet_median = _nan_if_missing_median(wet, "gapDeltaPct")
    dry_median = _nan_if_missing_median(dry, "gapDeltaPct")

    print(f"Wet stops: {len(wet):,}")
    print(f"Dry stops: {len(dry):,}")
    print(f"Average gapDeltaPct, wet: {wet_avg:.3f}")
    print(f"Average gapDeltaPct, dry: {dry_avg:.3f}")
    print(f"Median gapDeltaPct, wet: {wet_median:.3f}")
    print(f"Median gapDeltaPct, dry: {dry_median:.3f}")

    print("\n[5] Additional forensic checks")
    rival_ahead = _safe_col(df_pit, "rivalAhead")
    post_gap = _safe_col(df_pit, "postPitGapToRival")
    baseline = _safe_col(df_pit, "baselineLapTime")
    offset = _safe_col(df_pit, "offsetStrategy")

    print(f"rivalAhead null rate: {rival_ahead.isna().mean() * 100.0:.2f}%")
    print(f"postPitGapToRival null rate: {post_gap.isna().mean() * 100.0:.2f}%")
    print(
        f"baselineLapTime <= 0 or null count: {int((baseline.fillna(0) <= 0).sum()):,}"
    )
    print(f"offsetStrategy true rate: {offset.fillna(False).mean() * 100.0:.2f}%")

    malformed_pit_status = pit_status.dropna().astype(str).str.len().gt(1).sum()
    malformed_ml_status = ml_status.dropna().astype(str).str.len().gt(1).sum()
    print(
        "Multi-code track status tokens (>1 char), "
        f"pit_evals={int(malformed_pit_status):,}, ml_features={int(malformed_ml_status):,}"
    )


if __name__ == "__main__":
    audit_season()
