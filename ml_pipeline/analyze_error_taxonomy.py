"""Cross-paradigm decision-level error taxonomy for one run directory."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

try:
    from lib.run_catalog import racewise_suffix_for
except ImportError:
    from ml_pipeline.lib.run_catalog import racewise_suffix_for  # type: ignore

SCORED_CLASSES = {"0", "1"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze SDE/Batch/MOA error taxonomy from existing comparators")
    parser.add_argument("--reports-dir", type=Path, required=True)
    parser.add_argument("--suffix", required=True)
    parser.add_argument("--horizon", type=int, default=2)
    parser.add_argument("--sample-size", type=int, default=30)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--formats",
        nargs="+",
        choices=["pdf", "png"],
        default=["pdf", "png"],
    )
    return parser.parse_args()


def _load_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"missing {label}: {path}")
    frame = pd.read_csv(path)
    if frame.empty:
        raise ValueError(f"empty {label}: {path}")
    return frame


def _resolve_moa_comparator(reports_dir: Path, suffix: str) -> Path:
    name = f"moa_comparator_{suffix}.csv"
    primary = reports_dir / name
    legacy = reports_dir.parent.parent / "data_lake" / "reports" / reports_dir.name / name
    if primary.exists():
        return primary
    if legacy.exists():
        return legacy
    return primary


def _resolve_sde_comparator(reports_dir: Path, suffix: str, racewise_suffix: str) -> Path:
    primary = reports_dir / f"heuristic_comparator_{suffix}.csv"
    alt = reports_dir / f"heuristic_comparator_{racewise_suffix}.csv"
    if primary.exists():
        return primary
    if alt.exists():
        return alt
    return primary


def _normalize_lap(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(-1).astype(int)


def _add_key(frame: pd.DataFrame, lap_col: str = "suggestion_lap") -> pd.DataFrame:
    out = frame.copy()
    out["race"] = out["race"].astype(str)
    out["driver"] = out["driver"].astype(str)
    out[lap_col] = _normalize_lap(out[lap_col])
    out["decision_key"] = out["race"] + "||" + out["driver"] + "||" + out[lap_col].astype(str)
    return out


def _prepare_comparator(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"race", "driver", "suggestion_lap", "outcome_class", "exclusion_reason", "nearest_future_pit_distance"}
    missing = [c for c in required if c not in frame.columns]
    if missing:
        raise ValueError(f"comparator missing columns: {missing}")

    out = _add_key(frame, lap_col="suggestion_lap")
    out["outcome_class"] = out["outcome_class"].astype(str)
    out["exclusion_reason"] = out["exclusion_reason"].astype(str)
    out["nearest_future_pit_distance"] = pd.to_numeric(out["nearest_future_pit_distance"], errors="coerce")
    return out


def _fp_set(frame: pd.DataFrame) -> set[str]:
    scored = frame[frame["outcome_class"].isin(SCORED_CLASSES)]
    fps = scored[scored["outcome_class"] == "0"]
    return set(fps["decision_key"].astype(str))


def _near_miss_subtype(distance: float | int | np.floating | None, horizon: int) -> str:
    if pd.isna(distance):
        return "no_future"
    value = int(distance)
    if horizon + 1 <= value <= horizon + 3:
        return "near_miss_3_5"
    if value >= horizon + 4:
        return "far_miss_6_plus"
    return "unexpected_within_horizon"


def _plot_bar(
    frame: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    xlabel: str,
    ylabel: str,
    out_base: Path,
    formats: list[str],
) -> list[Path]:
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(frame[x_col].astype(str), frame[y_col].astype(float), color="#1f77b4", alpha=0.9)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid(True, axis="y", linestyle="--", alpha=0.35)
    plt.xticks(rotation=20, ha="right")
    fig.tight_layout()

    outputs: list[Path] = []
    for ext in formats:
        path = out_base.with_suffix(f".{ext}")
        fig.savefig(path, dpi=300, bbox_inches="tight")
        outputs.append(path)
    plt.close(fig)
    return outputs


def _plot_near_miss(frame: pd.DataFrame, out_base: Path, formats: list[str]) -> list[Path]:
    pivot = frame.pivot(index="model", columns="subtype", values="count").fillna(0)
    subtype_order = ["near_miss_3_5", "far_miss_6_plus", "no_future", "unexpected_within_horizon"]
    for col in subtype_order:
        if col not in pivot.columns:
            pivot[col] = 0
    pivot = pivot[subtype_order]

    fig, ax = plt.subplots(figsize=(10, 6))
    bottom = np.zeros(len(pivot), dtype=float)
    colors = {
        "near_miss_3_5": "#2ca02c",
        "far_miss_6_plus": "#ff7f0e",
        "no_future": "#d62728",
        "unexpected_within_horizon": "#9467bd",
    }

    for col in subtype_order:
        vals = pivot[col].to_numpy(dtype=float)
        ax.bar(pivot.index, vals, bottom=bottom, label=col, color=colors[col], alpha=0.9)
        bottom += vals

    ax.set_title("Near-miss / far-miss distribution on excluded NO_MATCH rows")
    ax.set_xlabel("Model")
    ax.set_ylabel("Count")
    ax.grid(True, axis="y", linestyle="--", alpha=0.35)
    ax.legend(loc="upper right", fontsize=8)
    fig.tight_layout()

    outputs: list[Path] = []
    for ext in formats:
        path = out_base.with_suffix(f".{ext}")
        fig.savefig(path, dpi=300, bbox_inches="tight")
        outputs.append(path)
    plt.close(fig)
    return outputs


def _resolve_dataset_path(reports_dir: Path, suffix: str) -> Path:
    if reports_dir.name == "reports":
        data_lake_dir = reports_dir.parent
    else:
        data_lake_dir = reports_dir.parent.parent
    candidate = data_lake_dir / f"ml_training_dataset_{suffix}.parquet"
    if candidate.exists():
        return candidate
    base = data_lake_dir / "ml_training_dataset_2022_2025_merged.parquet"
    return base


def _batch_feature_profiles(
    reports_dir: Path,
    suffix: str,
    ml_cmp: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    oof_path = reports_dir / f"ml_oof_winner_{suffix}.csv"
    oof = _load_csv(oof_path, "Batch OOF")
    oof = _add_key(oof, lap_col="lapNumber")

    dataset_path = _resolve_dataset_path(reports_dir, suffix)
    ds = pd.read_parquet(dataset_path)
    required_cols = {"race", "driver", "lapNumber"}
    missing = [c for c in required_cols if c not in ds.columns]
    if missing:
        raise ValueError(f"dataset missing required columns {missing}: {dataset_path}")
    ds = _add_key(ds, lap_col="lapNumber")

    # Join comparator outcome labels to OOF rows, then enrich with dataset context features.
    cmp_scored = ml_cmp[ml_cmp["outcome_class"].isin(SCORED_CLASSES)][["decision_key", "outcome_class"]].copy()
    cmp_scored["decision_outcome"] = np.where(cmp_scored["outcome_class"] == "0", "FP", "TP")

    merged = oof.merge(cmp_scored[["decision_key", "decision_outcome"]], on="decision_key", how="inner")

    feature_candidates = [
        "tire_life_ratio",
        "pace_drop_ratio",
        "race_progress_pct",
        "lapTime_vs_driver_prev_mean_pct",
        "lapTime_vs_team_prev_lapbucket_mean_pct",
        "lapTime_vs_race_prev_lapbucket_mean_pct",
        "lapTime",
        "trackTemp",
        "airTemp",
        "humidity",
        "speedTrap",
        "gapAhead",
        "gapBehind",
        "position",
        "pitLoss",
    ]

    ds_features = ds[["decision_key"] + [c for c in feature_candidates if c in ds.columns]].copy()
    enriched = merged.merge(ds_features, on="decision_key", how="left")

    available_features = [c for c in feature_candidates if c in enriched.columns]
    profile_rows: list[dict[str, object]] = []
    effect_rows: list[dict[str, object]] = []

    for feature in available_features:
        values = pd.to_numeric(enriched[feature], errors="coerce")
        for cls in ("TP", "FP"):
            subset = values[enriched["decision_outcome"] == cls].dropna()
            profile_rows.append(
                {
                    "feature": feature,
                    "class": cls,
                    "n": int(len(subset)),
                    "mean": float(subset.mean()) if len(subset) else float("nan"),
                    "median": float(subset.median()) if len(subset) else float("nan"),
                    "std": float(subset.std(ddof=0)) if len(subset) else float("nan"),
                    "p10": float(subset.quantile(0.10)) if len(subset) else float("nan"),
                    "p90": float(subset.quantile(0.90)) if len(subset) else float("nan"),
                }
            )

        tp_vals = values[enriched["decision_outcome"] == "TP"].dropna().to_numpy(dtype=float)
        fp_vals = values[enriched["decision_outcome"] == "FP"].dropna().to_numpy(dtype=float)
        tp_mean = float(np.mean(tp_vals)) if len(tp_vals) else float("nan")
        fp_mean = float(np.mean(fp_vals)) if len(fp_vals) else float("nan")
        tp_std = float(np.std(tp_vals, ddof=0)) if len(tp_vals) else float("nan")
        fp_std = float(np.std(fp_vals, ddof=0)) if len(fp_vals) else float("nan")

        pooled = float(np.sqrt((tp_std**2 + fp_std**2) / 2.0)) if np.isfinite(tp_std) and np.isfinite(fp_std) else float("nan")
        effect = float((fp_mean - tp_mean) / pooled) if np.isfinite(pooled) and pooled > 0 else float("nan")

        effect_rows.append(
            {
                "feature": feature,
                "tp_mean": tp_mean,
                "fp_mean": fp_mean,
                "delta_fp_minus_tp": float(fp_mean - tp_mean) if np.isfinite(tp_mean) and np.isfinite(fp_mean) else float("nan"),
                "cohens_d_fp_vs_tp": effect,
                "n_tp": int(len(tp_vals)),
                "n_fp": int(len(fp_vals)),
            }
        )

    profiles = pd.DataFrame(profile_rows)
    effects = pd.DataFrame(effect_rows).sort_values(by="cohens_d_fp_vs_tp", key=lambda s: s.abs(), ascending=False)
    return profiles, effects


def _plot_feature_effects(effects_df: pd.DataFrame, out_base: Path, formats: list[str], top_n: int = 12) -> list[Path]:
    plot_df = effects_df.head(top_n).copy()

    fig, ax = plt.subplots(figsize=(10, 7))
    y = np.arange(len(plot_df))
    ax.barh(y, plot_df["cohens_d_fp_vs_tp"], color="#d62728", alpha=0.9)
    ax.set_yticks(y)
    ax.set_yticklabels(plot_df["feature"].astype(str))
    ax.invert_yaxis()
    ax.axvline(0.0, color="#333333", linestyle="--", linewidth=1.0)
    ax.set_title("Batch FP vs TP contextual profile (Cohen's d)")
    ax.set_xlabel("Effect size (FP minus TP)")
    ax.grid(True, axis="x", linestyle="--", alpha=0.35)
    fig.tight_layout()

    outputs: list[Path] = []
    for ext in formats:
        path = out_base.with_suffix(f".{ext}")
        fig.savefig(path, dpi=300, bbox_inches="tight")
        outputs.append(path)
    plt.close(fig)
    return outputs


def _sample_manual_template(
    seed: int,
    sample_size: int,
    cases_df: pd.DataFrame,
) -> pd.DataFrame:
    if cases_df.empty:
        return cases_df

    sampled = cases_df.sample(n=min(sample_size, len(cases_df)), random_state=seed).copy()
    sampled = sampled.sort_values(by=["model", "race", "driver", "suggestion_lap"]).reset_index(drop=True)
    sampled["failure_type_manual"] = ""
    sampled["context_source"] = ""
    sampled["notes"] = ""
    return sampled


def main() -> None:
    args = parse_args()
    if args.horizon < 1:
        raise ValueError("--horizon must be >= 1")
    if args.sample_size < 1:
        raise ValueError("--sample-size must be >= 1")

    reports_dir = args.reports_dir.resolve()
    suffix = args.suffix
    racewise_suffix = racewise_suffix_for(suffix)

    sde = _prepare_comparator(
        _load_csv(_resolve_sde_comparator(reports_dir, suffix, racewise_suffix), "SDE comparator")
    )
    ml = _prepare_comparator(_load_csv(reports_dir / f"ml_comparator_{suffix}.csv", "Batch comparator"))
    moa = _prepare_comparator(_load_csv(_resolve_moa_comparator(reports_dir, suffix), "MOA comparator"))

    # 1) Consensus overlap on false positives.
    sde_fp = _fp_set(sde)
    ml_fp = _fp_set(ml)
    moa_fp = _fp_set(moa)

    overlap_rows = [
        {"subset": "SDE_only", "count": len(sde_fp - ml_fp - moa_fp)},
        {"subset": "Batch_only", "count": len(ml_fp - sde_fp - moa_fp)},
        {"subset": "MOA_only", "count": len(moa_fp - sde_fp - ml_fp)},
        {"subset": "SDE_and_Batch_only", "count": len((sde_fp & ml_fp) - moa_fp)},
        {"subset": "SDE_and_MOA_only", "count": len((sde_fp & moa_fp) - ml_fp)},
        {"subset": "Batch_and_MOA_only", "count": len((ml_fp & moa_fp) - sde_fp)},
        {"subset": "All_three", "count": len(sde_fp & ml_fp & moa_fp)},
    ]
    overlap_df = pd.DataFrame(overlap_rows)
    overlap_df.to_csv(reports_dir / "error_consensus_overlap_counts.csv", index=False)

    pairwise_df = pd.DataFrame(
        [
            {"pair": "SDE_Batch", "count": len(sde_fp & ml_fp)},
            {"pair": "SDE_MOA", "count": len(sde_fp & moa_fp)},
            {"pair": "Batch_MOA", "count": len(ml_fp & moa_fp)},
            {"pair": "All_three", "count": len(sde_fp & ml_fp & moa_fp)},
        ]
    )
    pairwise_df.to_csv(reports_dir / "error_consensus_pairwise.csv", index=False)

    fig_paths: list[Path] = []
    fig_paths.extend(
        _plot_bar(
            overlap_df.sort_values(by="count", ascending=False),
            x_col="subset",
            y_col="count",
            title="Consensus FP overlap across paradigms",
            xlabel="FP overlap subset",
            ylabel="Count",
            out_base=reports_dir / "error_consensus_upset",
            formats=args.formats,
        )
    )

    # 2) Near-miss diagnostics on excluded no-match rows.
    near_rows: list[dict[str, object]] = []
    case_rows: list[dict[str, object]] = []

    for model_name, frame in (("SDE", sde), ("Batch", ml), ("MOA", moa)):
        excluded = frame[
            (frame["outcome_class"] == "EXCLUDED")
            & (frame["exclusion_reason"] == "NO_MATCH_WITHIN_HORIZON")
        ].copy()

        for _, row in excluded.iterrows():
            subtype = _near_miss_subtype(row["nearest_future_pit_distance"], args.horizon)
            case_rows.append(
                {
                    "model": model_name,
                    "case_origin": "excluded_no_match",
                    "subtype_auto": subtype,
                    "race": row["race"],
                    "driver": row["driver"],
                    "suggestion_lap": int(row["suggestion_lap"]),
                    "outcome_class": row["outcome_class"],
                    "exclusion_reason": row["exclusion_reason"],
                    "nearest_future_pit_distance": row["nearest_future_pit_distance"],
                }
            )

        subtype_counts = (
            excluded.assign(
                subtype=lambda d: d["nearest_future_pit_distance"].apply(
                    lambda x: _near_miss_subtype(x, args.horizon)
                )
            )
            .groupby("subtype", dropna=False)
            .size()
            .reset_index(name="count")
        )

        total = int(subtype_counts["count"].sum())
        for _, row in subtype_counts.iterrows():
            near_rows.append(
                {
                    "model": model_name,
                    "subtype": str(row["subtype"]),
                    "count": int(row["count"]),
                    "rate_within_no_match": float(row["count"] / total) if total > 0 else float("nan"),
                }
            )

    near_df = pd.DataFrame(near_rows)
    near_df.to_csv(reports_dir / "error_near_miss_distribution.csv", index=False)
    fig_paths.extend(_plot_near_miss(near_df, reports_dir / "error_near_miss_distribution", args.formats))

    # 3) Batch FP vs TP contextual profiling.
    profiles_df, effects_df = _batch_feature_profiles(reports_dir, suffix, ml)
    profiles_df.to_csv(reports_dir / "error_batch_fp_tp_feature_profiles.csv", index=False)
    effects_df.to_csv(reports_dir / "error_batch_fp_tp_feature_effects.csv", index=False)
    fig_paths.extend(
        _plot_feature_effects(
            effects_df,
            reports_dir / "error_batch_fp_tp_feature_profiles",
            args.formats,
        )
    )

    # 4) Manual taxonomy template sheet.
    for model_name, frame in (("SDE", sde), ("Batch", ml), ("MOA", moa)):
        fp_rows = frame[(frame["outcome_class"] == "0")].copy()
        for _, row in fp_rows.iterrows():
            case_rows.append(
                {
                    "model": model_name,
                    "case_origin": "scored_fp",
                    "subtype_auto": "fp_scored",
                    "race": row["race"],
                    "driver": row["driver"],
                    "suggestion_lap": int(row["suggestion_lap"]),
                    "outcome_class": row["outcome_class"],
                    "exclusion_reason": row["exclusion_reason"],
                    "nearest_future_pit_distance": row["nearest_future_pit_distance"],
                }
            )

    cases_df = pd.DataFrame(case_rows)
    template_df = _sample_manual_template(args.seed, args.sample_size, cases_df)
    template_df.to_csv(reports_dir / "error_taxonomy_manual_template.csv", index=False)

    summary_rows = []
    for model_name, frame in (("SDE", sde), ("Batch", ml), ("MOA", moa)):
        scored = frame[frame["outcome_class"].isin(SCORED_CLASSES)]
        excluded_no_match = frame[
            (frame["outcome_class"] == "EXCLUDED") & (frame["exclusion_reason"] == "NO_MATCH_WITHIN_HORIZON")
        ]
        fp = scored[scored["outcome_class"] == "0"]
        tp = scored[scored["outcome_class"] == "1"]

        summary_rows.append(
            {
                "model": model_name,
                "actionable_total": int(len(frame)),
                "scored_total": int(len(scored)),
                "tp": int(len(tp)),
                "fp": int(len(fp)),
                "precision": float(len(tp) / len(scored)) if len(scored) > 0 else float("nan"),
                "fp_rate_within_scored": float(len(fp) / len(scored)) if len(scored) > 0 else float("nan"),
                "excluded_no_match": int(len(excluded_no_match)),
            }
        )

    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(reports_dir / "error_taxonomy_summary.csv", index=False)

    print("=== ERROR TAXONOMY ARTIFACTS GENERATED ===")
    print(f"reports dir              : {reports_dir}")
    print(f"suffix                   : {suffix}")
    print(f"horizon                  : {args.horizon}")
    print(f"sample size              : {args.sample_size}")
    print(f"summary csv              : {reports_dir / 'error_taxonomy_summary.csv'}")
    print(f"overlap csv              : {reports_dir / 'error_consensus_overlap_counts.csv'}")
    print(f"near-miss csv            : {reports_dir / 'error_near_miss_distribution.csv'}")
    print(f"batch profile csv        : {reports_dir / 'error_batch_fp_tp_feature_profiles.csv'}")
    print(f"manual template csv      : {reports_dir / 'error_taxonomy_manual_template.csv'}")
    for path in fig_paths:
        print(f"figure                   : {path}")


if __name__ == "__main__":
    main()
