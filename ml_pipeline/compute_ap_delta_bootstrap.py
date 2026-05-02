"""Compute AP-delta uncertainty across run families via race-cluster bootstrap."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score

try:
    from lib.run_catalog import RUN_SET_ALL_FAMILIES, RunSpec, list_runs, oof_path, run_by_id
except ImportError:
    from ml_pipeline.lib.run_catalog import (  # type: ignore
        RUN_SET_ALL_FAMILIES,
        RunSpec,
        list_runs,
        oof_path,
        run_by_id,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute AP delta CIs with race-cluster bootstrap")
    parser.add_argument("--reports-root", type=Path, default=Path("data_lake/reports"))
    parser.add_argument("--run-set", default=RUN_SET_ALL_FAMILIES)
    parser.add_argument("--bootstrap-reps", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--formats",
        nargs="+",
        choices=["pdf", "png"],
        default=["pdf", "png"],
    )
    return parser.parse_args()


def _load_oof(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"missing OOF: {path}")

    frame = pd.read_csv(path)
    required = {"race", "target_y", "raw_proba", "calibrated_proba"}
    missing = [col for col in required if col not in frame.columns]
    if missing:
        raise ValueError(f"OOF missing columns {missing}: {path}")

    out = frame.copy()
    out["race"] = out["race"].astype(str)
    out["target_y"] = pd.to_numeric(out["target_y"], errors="coerce").fillna(0).astype(int)
    out["raw_proba"] = pd.to_numeric(out["raw_proba"], errors="coerce")
    out["calibrated_proba"] = pd.to_numeric(out["calibrated_proba"], errors="coerce")
    out = out.dropna(subset=["raw_proba", "calibrated_proba"]).reset_index(drop=True)
    return out


def _reference_for(run: RunSpec) -> str | None:
    if run.run_id == "full":
        return None
    if run.run_id == "e0":
        return "full"
    if run.family in {"e_family", "p_family"}:
        return "e0"
    return None


def _ap(y: np.ndarray, score: np.ndarray) -> float:
    if len(y) == 0:
        return float("nan")
    if np.unique(y).size < 2:
        return float("nan")
    return float(average_precision_score(y, score))


def _collect_race_blocks(frame: pd.DataFrame, score_col: str, races: list[str]) -> dict[str, tuple[np.ndarray, np.ndarray]]:
    blocks: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    subset = frame[frame["race"].isin(races)].copy()
    for race, group in subset.groupby("race", sort=False):
        y = group["target_y"].to_numpy(dtype=int)
        s = group[score_col].to_numpy(dtype=float)
        blocks[str(race)] = (y, s)
    return blocks


def _concat_blocks(blocks: dict[str, tuple[np.ndarray, np.ndarray]], sampled_races: list[str]) -> tuple[np.ndarray, np.ndarray]:
    y_parts: list[np.ndarray] = []
    s_parts: list[np.ndarray] = []
    for race in sampled_races:
        y, s = blocks[race]
        y_parts.append(y)
        s_parts.append(s)
    if not y_parts:
        return np.array([], dtype=int), np.array([], dtype=float)
    return np.concatenate(y_parts), np.concatenate(s_parts)


def _bootstrap_delta(
    run_df: pd.DataFrame,
    ref_df: pd.DataFrame,
    score_col: str,
    reps: int,
    seed: int,
) -> tuple[pd.DataFrame, dict[str, float | int]]:
    common_races = sorted(set(run_df["race"]).intersection(set(ref_df["race"])))
    if len(common_races) < 2:
        raise ValueError("not enough common races for cluster bootstrap")

    run_blocks = _collect_race_blocks(run_df, score_col, common_races)
    ref_blocks = _collect_race_blocks(ref_df, score_col, common_races)

    y_run_full, s_run_full = _concat_blocks(run_blocks, common_races)
    y_ref_full, s_ref_full = _concat_blocks(ref_blocks, common_races)
    point_run = _ap(y_run_full, s_run_full)
    point_ref = _ap(y_ref_full, s_ref_full)
    point_delta = point_run - point_ref

    rng = np.random.default_rng(seed)
    rows: list[dict[str, float | int]] = []
    deltas: list[float] = []
    for bidx in range(reps):
        sampled = rng.choice(common_races, size=len(common_races), replace=True).tolist()

        y_run, s_run = _concat_blocks(run_blocks, sampled)
        y_ref, s_ref = _concat_blocks(ref_blocks, sampled)
        ap_run = _ap(y_run, s_run)
        ap_ref = _ap(y_ref, s_ref)
        delta = ap_run - ap_ref
        deltas.append(delta)
        rows.append(
            {
                "bootstrap_idx": int(bidx),
                "ap_run": float(ap_run),
                "ap_ref": float(ap_ref),
                "ap_delta": float(delta),
                "n_race_clusters": int(len(common_races)),
                "n_rows_run": int(len(y_run)),
                "n_rows_ref": int(len(y_ref)),
            }
        )

    samples = pd.DataFrame(rows)
    ci_low, ci_high = np.percentile(np.asarray(deltas, dtype=float), [2.5, 97.5])
    summary = {
        "n_race_clusters": int(len(common_races)),
        "n_rows_common_run": int(len(y_run_full)),
        "n_rows_common_ref": int(len(y_ref_full)),
        "ap_run_point": float(point_run),
        "ap_ref_point": float(point_ref),
        "ap_delta_point": float(point_delta),
        "ap_delta_mean_bootstrap": float(np.mean(deltas)),
        "ap_delta_std_bootstrap": float(np.std(deltas, ddof=0)),
        "ap_delta_ci95_low": float(ci_low),
        "ap_delta_ci95_high": float(ci_high),
        "bootstrap_reps": int(reps),
    }
    return samples, summary


def _plot_forest(summary_df: pd.DataFrame, out_base: Path, formats: list[str]) -> list[Path]:
    score_types = ["raw_proba", "calibrated_proba"]
    fig, axes = plt.subplots(1, 2, figsize=(16, 10), sharey=True)

    color_map = {
        "e_family": "#1f77b4",
        "p_family": "#d62728",
        "full_feature_baseline": "#2ca02c",
    }

    for ax, score_type in zip(axes, score_types, strict=True):
        subset = summary_df[summary_df["score_type"] == score_type].copy()
        subset = subset.sort_values(by=["run_order", "protocol"]).reset_index(drop=True)

        y_pos = np.arange(len(subset))
        for idx, row in subset.iterrows():
            color = color_map.get(str(row["run_family"]), "#444444")
            ax.plot(
                [float(row["ap_delta_ci95_low"]), float(row["ap_delta_ci95_high"])],
                [y_pos[idx], y_pos[idx]],
                color=color,
                linewidth=2.0,
            )
            ax.scatter(float(row["ap_delta_point"]), y_pos[idx], color=color, s=45, zorder=3)

        ax.axvline(0.0, color="#333333", linestyle="--", linewidth=1.0)
        ax.set_title(f"AP delta vs reference ({score_type})")
        ax.set_xlabel("AP delta")
        ax.grid(True, axis="x", linestyle="--", alpha=0.35)

        if score_type == "raw_proba":
            labels = [f"{r.run_label} [{r.protocol}]" for r in subset.itertuples(index=False)]
            ax.set_yticks(y_pos)
            ax.set_yticklabels(labels, fontsize=8)
        else:
            ax.set_yticks(y_pos)
            ax.set_yticklabels([])

    fig.suptitle("Race-Cluster Bootstrap AP Delta Forest Plot (95% CI)", fontsize=14)
    fig.tight_layout()

    outputs: list[Path] = []
    for ext in formats:
        path = out_base.with_suffix(f".{ext}")
        fig.savefig(path, dpi=300, bbox_inches="tight")
        outputs.append(path)
    plt.close(fig)
    return outputs


def main() -> None:
    args = parse_args()
    if args.bootstrap_reps < 100:
        raise ValueError("--bootstrap-reps should be at least 100")

    reports_root = args.reports_root.resolve()
    runs = list_runs(reports_root, args.run_set)

    oof_cache: dict[tuple[str, str], pd.DataFrame] = {}
    for run in runs:
        for protocol in ("ml_pretrain", "ml_racewise"):
            oof_cache[(run.run_id, protocol)] = _load_oof(oof_path(run, protocol))

    sample_rows: list[pd.DataFrame] = []
    summary_rows: list[dict[str, object]] = []

    for order_idx, run in enumerate(runs, start=1):
        ref_id = _reference_for(run)
        if ref_id is None:
            continue
        ref = run_by_id(reports_root, ref_id)

        for protocol in ("ml_pretrain", "ml_racewise"):
            run_df = oof_cache[(run.run_id, protocol)]
            ref_df = oof_cache[(ref.run_id, protocol)]

            for score_type in ("raw_proba", "calibrated_proba"):
                samples, summary = _bootstrap_delta(
                    run_df,
                    ref_df,
                    score_col=score_type,
                    reps=int(args.bootstrap_reps),
                    seed=int(args.seed),
                )

                samples.insert(0, "score_type", score_type)
                samples.insert(0, "protocol", protocol)
                samples.insert(0, "reference_run_id", ref.run_id)
                samples.insert(0, "run_id", run.run_id)
                sample_rows.append(samples)

                summary_rows.append(
                    {
                        "run_id": run.run_id,
                        "run_label": run.label,
                        "run_family": run.family,
                        "run_order": int(order_idx),
                        "reference_run_id": ref.run_id,
                        "reference_run_label": ref.label,
                        "protocol": protocol,
                        "score_type": score_type,
                        **summary,
                    }
                )

    all_samples = pd.concat(sample_rows, ignore_index=True)
    all_summary = pd.DataFrame(summary_rows)

    samples_path = reports_root / "ap_delta_bootstrap_cluster_samples.csv"
    summary_path = reports_root / "ap_delta_bootstrap_cluster_summary.csv"
    all_samples.to_csv(samples_path, index=False)
    all_summary.to_csv(summary_path, index=False)

    fig_paths = _plot_forest(
        all_summary,
        reports_root / "ap_delta_forestplot",
        formats=args.formats,
    )

    print("=== AP DELTA CLUSTER BOOTSTRAP COMPLETE ===")
    print(f"reports root            : {reports_root}")
    print(f"run set                 : {args.run_set}")
    print(f"bootstrap reps          : {args.bootstrap_reps}")
    print(f"seed                    : {args.seed}")
    print(f"samples csv             : {samples_path}")
    print(f"summary csv             : {summary_path}")
    for path in fig_paths:
        print(f"figure                  : {path}")


if __name__ == "__main__":
    main()
