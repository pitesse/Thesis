import glob
from pathlib import Path

import pandas as pd


YEAR = 2023

def latest_file(pattern: str) -> str | None:
    matches = glob.glob(pattern)
    if not matches:
        return None
    return max(matches, key=lambda p: Path(p).stat().st_mtime)


def load_stream(name: str) -> pd.DataFrame | None:
    pattern = f"data_lake/{name}_{YEAR}_season_*.jsonl"
    file_path = latest_file(pattern)
    if file_path is None:
        print(f"[missing] {name}: no files for pattern {pattern}")
        return None

    df = pd.read_json(file_path, lines=True)
    print(f"[{name}] file={file_path}, rows={len(df):,}")
    return df


def audit_tire_drops(df: pd.DataFrame) -> None:
    print("\n=== tire_drops ===")
    if df.empty:
        print("no rows")
        return

    print("alerts by compound:")
    print(df["compound"].value_counts(dropna=False).to_string())

    print("tyreLife stats by compound:")
    stats = (
        df.groupby("compound", dropna=False)["tyreLife"]
        .agg(["count", "mean", "median", "min", "max"])
        .sort_values("count", ascending=False)
    )
    print(stats.to_string())

    print("delta stats by compound:")
    delta_stats = (
        df.groupby("compound", dropna=False)["delta"]
        .agg(["mean", "median", "min", "max"])
        .sort_values("mean", ascending=False)
    )
    print(delta_stats.to_string())

    early_mask = df["tyreLife"].fillna(0) <= 3
    print(f"very-early alerts (tyreLife <= 3): {int(early_mask.sum()):,} ({early_mask.mean() * 100:.2f}%)")


def audit_lift_coast(df: pd.DataFrame) -> None:
    print("\n=== lift_coast ===")
    if df.empty:
        print("no rows")
        return

    if {"liftDate", "brakeDate"}.issubset(df.columns):
        lift_dt = pd.to_datetime(df["liftDate"], errors="coerce", utc=True)
        brake_dt = pd.to_datetime(df["brakeDate"], errors="coerce", utc=True)
        durations = (brake_dt - lift_dt).dt.total_seconds()
        print(
            "coast duration seconds stats:",
            {
                "mean": round(float(durations.mean()), 3),
                "median": round(float(durations.median()), 3),
                "p95": round(float(durations.quantile(0.95)), 3),
                "min": round(float(durations.min()), 3),
                "max": round(float(durations.max()), 3),
            },
        )

    non_green = ~df["trackStatus"].astype(str).eq("1")
    print(f"non-green alerts: {int(non_green.sum()):,} ({non_green.mean() * 100:.2f}%)")
    if non_green.any():
        print("non-green breakdown:")
        print(df.loc[non_green, "trackStatus"].value_counts(dropna=False).to_string())

    if "driver" in df.columns:
        per_driver = df["driver"].value_counts()
        print("alerts per driver (top 10):")
        print(per_driver.head(10).to_string())


def audit_drop_zones(df: pd.DataFrame) -> None:
    print("\n=== drop_zones ===")
    if df.empty:
        print("no rows")
        return

    for col in ["currentPosition", "emergencePosition", "positionsLost"]:
        if col in df.columns:
            print(
                f"{col} stats:",
                {
                    "mean": round(float(df[col].mean()), 3),
                    "median": round(float(df[col].median()), 3),
                    "min": round(float(df[col].min()), 3),
                    "max": round(float(df[col].max()), 3),
                },
            )

    if {"currentPosition", "emergencePosition", "positionsLost"}.issubset(df.columns):
        implied = df["emergencePosition"] - df["currentPosition"]
        mismatch = (implied != df["positionsLost"]).sum()
        print(f"positionsLost mismatch count: {int(mismatch):,}")

    if "gapToPhysicalCar" in df.columns:
        gap = df["gapToPhysicalCar"]
        print(
            "gapToPhysicalCar stats:",
            {
                "mean": round(float(gap.mean()), 3),
                "median": round(float(gap.median()), 3),
                "p95": round(float(gap.quantile(0.95)), 3),
                "min": round(float(gap.min()), 3),
                "max": round(float(gap.max()), 3),
            },
        )
        bad_gap = (gap < 0).sum()
        print(f"negative gapToPhysicalCar rows: {int(bad_gap):,}")


def audit_pit_suggestions(df: pd.DataFrame) -> None:
    print("\n=== pit_suggestions ===")
    if df.empty:
        print("no rows")
        return

    print("suggestionLabel distribution:")
    print(df["suggestionLabel"].value_counts(dropna=False).to_string())

    if "totalScore" in df.columns:
        s = df["totalScore"]
        print(
            "totalScore stats:",
            {
                "mean": round(float(s.mean()), 3),
                "median": round(float(s.median()), 3),
                "p90": round(float(s.quantile(0.90)), 3),
                "p99": round(float(s.quantile(0.99)), 3),
                "min": round(float(s.min()), 3),
                "max": round(float(s.max()), 3),
            },
        )

    if {"driver", "lapNumber"}.issubset(df.columns):
        per_driver_lap = df.groupby(["driver", "lapNumber"]).size()
        spam_rows = (per_driver_lap > 1).sum()
        print(f"driver-lap duplicates (>1 alert same lap): {int(spam_rows):,}")

    if "trackStatus" in df.columns:
        non_green = ~df["trackStatus"].astype(str).eq("1")
        print(f"non-green suggestions: {int(non_green.sum()):,} ({non_green.mean() * 100:.2f}%)")
        if non_green.any():
            print("non-green status breakdown:")
            print(df.loc[non_green, "trackStatus"].value_counts(dropna=False).to_string())


def main() -> None:
    print("=== FULL DATA LAKE AUDIT (REAL-TIME STREAMS) ===")

    tire_drops = load_stream("tire_drops")
    lift_coast = load_stream("lift_coast")
    drop_zones = load_stream("drop_zones")
    pit_suggestions = load_stream("pit_suggestions")

    if tire_drops is not None:
        audit_tire_drops(tire_drops)
    if lift_coast is not None:
        audit_lift_coast(lift_coast)
    if drop_zones is not None:
        audit_drop_zones(drop_zones)
    if pit_suggestions is not None:
        audit_pit_suggestions(pit_suggestions)


if __name__ == "__main__":
    main()
