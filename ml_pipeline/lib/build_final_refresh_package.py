"""Build final corrected presentation-result package under final_refresh/.

Uses current artifacts only (no heavy retraining).
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import cohen_kappa_score

try:
    from .comparator_heuristic import (
        ACTIONABLE_MODE_PIT_NOW_ONLY,
        ACTIONABLE_MODE_PIT_NOW_PLUS_GOOD_PIT,
        OUTCOME_PIT_SUCCESS_H2,
        POSITIVE_RESULTS,
        _build_comparator_dataset,
        _load_jsonl,
    )
    from .evaluate_batch_dual_contract_run import _oof_to_suggestions
except ImportError:
    import sys

    _LIB_DIR = Path(__file__).resolve().parent
    _PIPELINE_DIR = _LIB_DIR.parent
    for _path in (_PIPELINE_DIR, _LIB_DIR):
        _path_text = str(_path)
        if _path_text not in sys.path:
            sys.path.insert(0, _path_text)

    from comparator_heuristic import (  # type: ignore
        ACTIONABLE_MODE_PIT_NOW_ONLY,
        ACTIONABLE_MODE_PIT_NOW_PLUS_GOOD_PIT,
        OUTCOME_PIT_SUCCESS_H2,
        POSITIVE_RESULTS,
        _build_comparator_dataset,
        _load_jsonl,
    )
    from evaluate_batch_dual_contract_run import _oof_to_suggestions  # type: ignore


PROFILE_E0 = "e0_no_source_year"
PROFILE_P1 = "p1_percent_conservative_v1"
TARGET_ANY = "target_pit_any_h2_clean_actionable"
TARGET_SUCCESS = "target_pit_success_h2_clean_actionable"
TARGET_SUCCESS_ELIG_COL = f"{TARGET_SUCCESS}_train_eligible"

SYSTEM_FLINK = "Flink Strategy Engine"
SYSTEM_BATCH_E0 = "Batch No-Year"
SYSTEM_BATCH_P1 = "Batch Percent"
SYSTEM_MOA_E0 = "MOA No-Year"
SYSTEM_MOA_P1 = "MOA Percent"


@dataclass(frozen=True)
class SourceRef:
    artifact_role: str
    system: str
    profile: str
    target: str
    path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build final corrected presentation-result package under final_refresh/"
    )
    parser.add_argument("--root", default=".")
    parser.add_argument(
        "--outdir",
        default="data_lake/reports/phase2b_presentation_figures/final_refresh",
    )
    parser.add_argument(
        "--race-min-positive-support",
        type=int,
        default=1,
        help="minimum positive support per race to include point in race-level diagnostics",
    )
    return parser.parse_args()


def _safe_div(num: float, den: float) -> float:
    return float(num / den) if den else 0.0


def _f_beta(precision: float, recall: float, beta: float = 0.5) -> float:
    beta2 = beta * beta
    den = beta2 * precision + recall
    return float((1 + beta2) * precision * recall / den) if den > 0 else 0.0


def _md_table(df: pd.DataFrame, float_precision: int = 6) -> str:
    if df.empty:
        return "_No rows._\n"
    cols = list(df.columns)
    lines = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join(["---"] * len(cols)) + " |",
    ]
    for _, row in df.iterrows():
        vals: list[str] = []
        for c in cols:
            v = row[c]
            if isinstance(v, (float, np.floating)):
                if np.isnan(v):
                    vals.append("")
                else:
                    vals.append(f"{float(v):.{float_precision}f}")
            else:
                vals.append(str(v))
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines) + "\n"


def _write_csv_md(df: pd.DataFrame, csv_path: Path, md_path: Path, title: str) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    md_path.write_text(f"# {title}\n\n{_md_table(df)}", encoding="utf-8")


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def _load_universe(path: Path) -> set[tuple[str, str]]:
    df = pd.read_csv(path)
    return set(
        (str(race), str(driver))
        for race, driver in df[["race", "driver"]].itertuples(index=False, name=None)
    )


def _filter_universe(df: pd.DataFrame, universe: set[tuple[str, str]]) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    out["race"] = out["race"].astype(str)
    out["driver"] = out["driver"].astype(str)
    mask = [
        (race, driver) in universe
        for race, driver in out[["race", "driver"]].itertuples(index=False, name=None)
    ]
    return out.loc[mask].copy()


def _load_truth_events(paths: Iterable[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for p in paths:
        if not p.exists():
            continue
        df = pd.read_csv(p)
        if "year" in df.columns:
            year = pd.to_numeric(df["year"], errors="coerce")
            race = df["race"].astype(str)
            needs = ~race.str.match(r"^\d{4}\s::\s") & year.notna()
            df.loc[needs, "race"] = year[needs].astype(int).astype(str) + " :: " + race[needs]
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["race"] = out["race"].astype(str)
    out["driver"] = out["driver"].astype(str)
    out["pit_lap_num"] = pd.to_numeric(out["pit_lap_num"], errors="coerce")
    out = out[out["pit_lap_num"].notna()].copy()
    out["pit_lap_num"] = out["pit_lap_num"].astype(int)
    out = out.drop_duplicates(subset=["race", "driver", "pit_lap_num"], keep="first")
    return out


def _event_key_set(df: pd.DataFrame) -> set[tuple[str, str, int]]:
    out: set[tuple[str, str, int]] = set()
    if df.empty:
        return out
    for race, driver, lap in df[["race", "driver", "pit_lap_num"]].itertuples(index=False, name=None):
        out.add((str(race), str(driver), int(lap)))
    return out


def _prefix_year_race(df: pd.DataFrame, year: int) -> pd.DataFrame:
    out = df.copy()
    race = out["race"].astype(str)
    needs = ~race.str.match(r"^\d{4}\s::\s")
    out.loc[needs, "race"] = f"{year} :: " + race[needs]
    return out


def _load_jsonls_with_year_prefix(paths: Iterable[Path]) -> pd.DataFrame:
    import re

    frames: list[pd.DataFrame] = []
    for p in paths:
        if not p.exists():
            continue
        year = None
        for token in (p.name, str(p.parent)):
            m = re.search(r"(20\d{2})", token)
            if m:
                year = int(m.group(1))
                break
        if year is None:
            raise ValueError(f"cannot parse year from {p}")
        frames.append(_prefix_year_race(_load_jsonl(p), year))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _comparator_metrics(
    comparator: pd.DataFrame,
    eligible_success_keys: set[tuple[str, str, int]],
) -> dict[str, float | int]:
    work = comparator.copy()
    work["outcome_class"] = work["outcome_class"].astype(str)
    work["exclusion_reason"] = work["exclusion_reason"].fillna("").astype(str)

    scored = work[work["outcome_class"].isin(["1", "0"])].copy()
    excluded = work[work["outcome_class"].eq("EXCLUDED")].copy()

    tp = int((scored["outcome_class"] == "1").sum())
    fp_total = int((scored["outcome_class"] == "0").sum())
    fp_no_match = int(
        (
            scored["outcome_class"].eq("0")
            & scored["exclusion_reason"].str.upper().eq("NO_MATCH_WITHIN_HORIZON")
        ).sum()
    )
    fp_failure = int(fp_total - fp_no_match)
    strict_precision = _safe_div(tp, tp + fp_total)

    matched_known = scored[scored["matched_pit_lap"].notna()].copy()
    matched_success = int((matched_known["outcome_class"] == "1").sum())
    matched_failure = int((matched_known["outcome_class"] == "0").sum())
    matched_unknown = int(excluded["matched_pit_lap"].notna().sum())
    matched_rate = _safe_div(matched_success, matched_success + matched_failure)

    matched_success_keys = set(
        (
            str(race),
            str(driver),
            int(lap),
        )
        for race, driver, lap in matched_known[
            matched_known["outcome_class"].eq("1")
        ][["race", "driver", "matched_pit_lap"]].itertuples(index=False, name=None)
    )
    covered = int(len(matched_success_keys & eligible_success_keys))
    eligible = int(len(eligible_success_keys))
    coverage = _safe_div(covered, eligible)
    f05 = _f_beta(strict_precision, coverage, beta=0.5)

    return {
        "predicted_positives": int(len(work)),
        "matched_known_pits": int(len(matched_known)),
        "matched_success": matched_success,
        "matched_failure": matched_failure,
        "matched_unknown": matched_unknown,
        "matched_pit_success_rate": matched_rate,
        "TP": tp,
        "FP_no_match": fp_no_match,
        "FP_failure": fp_failure,
        "unknown_excluded": int(len(excluded)),
        "strict_precision": strict_precision,
        "successful_event_coverage": coverage,
        "F0_5": f05,
    }


def _build_learner_comparator(
    oof: pd.DataFrame,
    threshold: float,
    pit_evals: pd.DataFrame,
) -> pd.DataFrame:
    tmp = oof.copy()
    tmp["_score"] = pd.to_numeric(tmp["calibrated_proba"], errors="coerce").fillna(0.0)
    tmp["_pred"] = (tmp["_score"] >= float(threshold)).astype(int)
    suggestions = _oof_to_suggestions(tmp, "_pred", "_score")
    return _build_comparator_dataset(
        suggestions=suggestions,
        pit_evals=pit_evals,
        horizon=2,
        outcome_mode=OUTCOME_PIT_SUCCESS_H2,
        pit_timings=None,
        actionable_mode=ACTIONABLE_MODE_PIT_NOW_ONLY,
        episode_level=False,
        include_same_lap=False,
        pit_success_no_match_as_negative=True,
    )


def _row_count_and_classes(path: Path, class_col: str) -> tuple[int, int | None, int | None]:
    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    elif path.suffix.lower() == ".jsonl":
        df = pd.read_json(path, lines=True)
    else:
        df = pd.read_csv(path)
    c0 = c1 = None
    if class_col in df.columns:
        vc = pd.to_numeric(df[class_col], errors="coerce").value_counts(dropna=False)
        c0 = int(vc.get(0, 0))
        c1 = int(vc.get(1, 0))
    return int(len(df)), c0, c1


def _confusion_metrics_by_race(df: pd.DataFrame, y_col: str, pred_col: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for race, grp in df.groupby("race", sort=False):
        y = pd.to_numeric(grp[y_col], errors="coerce").fillna(0).astype(int).to_numpy()
        p = pd.to_numeric(grp[pred_col], errors="coerce").fillna(0).astype(int).to_numpy()
        positives = int((y == 1).sum())
        negatives = int((y == 0).sum())
        tp = int(((p == 1) & (y == 1)).sum())
        fp = int(((p == 1) & (y == 0)).sum())
        fn = int(((p == 0) & (y == 1)).sum())
        tn = int(((p == 0) & (y == 0)).sum())
        tpr = _safe_div(tp, tp + fn)
        tnr = _safe_div(tn, tn + fp)
        gmean = float(np.sqrt(tpr * tnr)) if tpr >= 0 and tnr >= 0 else np.nan
        try:
            kappa = float(cohen_kappa_score(y, p))
        except Exception:
            kappa = np.nan
        rows.append(
            {
                "race": str(race),
                "positives": positives,
                "negatives": negatives,
                "tp": tp,
                "fp": fp,
                "fn": fn,
                "tn": tn,
                "kappa": kappa,
                "gmean": gmean,
            }
        )
    out = pd.DataFrame(rows)
    out["year"] = pd.to_numeric(out["race"].str.extract(r"^(\d{4})")[0], errors="coerce")
    out["race_name"] = out["race"].str.replace(r"^\d{4}\s::\s", "", regex=True)
    out.sort_values(by=["year", "race_name"], inplace=True, kind="mergesort")
    out.reset_index(drop=True, inplace=True)
    out["race_index"] = np.arange(1, len(out) + 1)
    return out


def _plot_headline(df: pd.DataFrame, out_png: Path, out_pdf: Path, title: str) -> None:
    systems = df["System"].tolist()
    x = np.arange(len(systems))
    w = 0.24
    fig, ax = plt.subplots(figsize=(12, 6), dpi=220)
    ax.bar(x - w, df["precision"].to_numpy(float), width=w, label="Precision")
    ax.bar(x, df["event_recall"].to_numpy(float), width=w, label="Event Recall")
    ax.bar(x + w, df["F0.5"].to_numpy(float), width=w, label="F0.5")
    ax.set_title(title, fontsize=14)
    ax.set_ylabel("Metric")
    ax.set_xticks(x)
    ax.set_xticklabels(systems, rotation=15, ha="right")
    ax.set_ylim(0, min(1.0, max(0.4, float(df[["precision", "event_recall", "F0.5"]].max().max()) * 1.15)))
    ax.grid(axis="y", alpha=0.25)
    ax.legend(loc="upper right")
    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=400, bbox_inches="tight", pad_inches=0.12)
    fig.savefig(out_pdf, bbox_inches="tight", pad_inches=0.12)
    plt.close(fig)


def _plot_apples(df: pd.DataFrame, out_png: Path, out_pdf: Path) -> None:
    systems = df["System"].tolist()
    x = np.arange(len(systems))
    w = 0.24
    fig, ax = plt.subplots(figsize=(12, 6), dpi=220)
    ax.bar(x - w, df["strict_precision"].to_numpy(float), width=w, label="Strict Precision")
    ax.bar(x, df["successful_event_coverage"].to_numpy(float), width=w, label="Successful Event Coverage")
    ax.bar(x + w, df["F0.5"].to_numpy(float), width=w, label="F0.5")
    ax.set_title("pit_success_h2 Main Comparison (Strict Operational Contract)", fontsize=14)
    ax.set_ylabel("Metric")
    ax.set_xticks(x)
    ax.set_xticklabels(systems, rotation=15, ha="right")
    ax.set_ylim(0, min(1.0, max(0.4, float(df[["strict_precision", "successful_event_coverage", "F0.5"]].max().max()) * 1.15)))
    ax.grid(axis="y", alpha=0.25)
    ax.legend(loc="upper right")
    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=400, bbox_inches="tight", pad_inches=0.12)
    fig.savefig(out_pdf, bbox_inches="tight", pad_inches=0.12)
    plt.close(fig)


def _plot_sde_diag(df: pd.DataFrame, out_png: Path, out_pdf: Path) -> None:
    labels = df["positive_call_definition"].tolist()
    x = np.arange(len(labels))
    w = 0.35
    fig, ax = plt.subplots(figsize=(10, 6), dpi=220)
    ax.bar(x - w / 2, df["matched_pit_success_rate"].to_numpy(float), width=w, label="Matched-Pit Success Rate (Diagnostic)")
    ax.bar(x + w / 2, df["strict_precision"].to_numpy(float), width=w, label="Strict Precision (Operational)")
    ax.set_title("Flink Strategy Engine pit_success_h2: Diagnostic vs Operational", fontsize=14)
    ax.set_ylabel("Rate")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=10)
    ax.set_ylim(0, 1)
    ax.grid(axis="y", alpha=0.25)
    ax.legend(loc="upper right")
    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=400, bbox_inches="tight", pad_inches=0.12)
    fig.savefig(out_pdf, bbox_inches="tight", pad_inches=0.12)
    plt.close(fig)


def _plot_runtime(df: pd.DataFrame, out_png: Path, out_pdf: Path) -> None:
    work = df.copy()
    work["System"] = work.apply(
        lambda r: (
            ("Batch No-Year" if r["learner"] == "batch" and r["profile"] == "no_year" else "")
            or ("Batch Percent" if r["learner"] == "batch" and r["profile"] == "percent" else "")
            or ("MOA No-Year" if r["learner"] == "moa" and r["profile"] == "no_year" else "")
            or ("MOA Percent" if r["learner"] == "moa" and r["profile"] == "percent" else "")
        ),
        axis=1,
    )
    work["Target"] = work["target"].astype(str).str.replace("target_", "", regex=False).str.replace("_clean_actionable", "", regex=False)
    fig, ax = plt.subplots(figsize=(11, 6), dpi=220)
    systems = ["Batch No-Year", "Batch Percent", "MOA No-Year", "MOA Percent"]
    offsets = {"pit_any_h2": -0.18, "pit_success_h2": 0.18}
    widths = 0.34
    x = np.arange(len(systems))
    for tgt in ["pit_any_h2", "pit_success_h2"]:
        sub = work[work["Target"] == tgt].set_index("System").reindex(systems)
        ax.bar(
            x + offsets[tgt],
            sub["wall_seconds"].fillna(0).to_numpy(float),
            width=widths,
            label=tgt,
        )
    ax.set_title("Training Runtime (OOF/Prequential Only)", fontsize=14)
    ax.set_ylabel("Wall time (seconds)")
    ax.set_xticks(x)
    ax.set_xticklabels(systems, rotation=10)
    ax.grid(axis="y", alpha=0.25)
    ax.legend(title="Target")
    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=400, bbox_inches="tight", pad_inches=0.12)
    fig.savefig(out_pdf, bbox_inches="tight", pad_inches=0.12)
    plt.close(fig)


def _plot_race_diag(
    race_df: pd.DataFrame,
    metric_col: str,
    out_png: Path,
    out_pdf: Path,
    min_support: int,
) -> None:
    systems = [SYSTEM_FLINK, SYSTEM_BATCH_E0, SYSTEM_BATCH_P1, SYSTEM_MOA_P1]
    colors = {TARGET_ANY: "#1f77b4", TARGET_SUCCESS: "#d62728"}
    labels = {TARGET_ANY: "pit_any_h2", TARGET_SUCCESS: "pit_success_h2"}
    fig, axes = plt.subplots(2, 2, figsize=(14, 8), dpi=220, sharey=True)
    ax_list = axes.flatten()
    for ax, sys_name in zip(ax_list, systems, strict=True):
        sub = race_df[race_df["system"] == sys_name].copy()
        for target in [TARGET_ANY, TARGET_SUCCESS]:
            t = sub[sub["target"] == target].copy()
            t = t[t["positives"] >= int(min_support)].copy()
            if t.empty:
                continue
            ax.scatter(t["race_index"], t[metric_col], s=16, alpha=0.75, color=colors[target], label=labels[target])
            ax.plot(t["race_index"], t[metric_col], alpha=0.20, color=colors[target], linewidth=0.9)
        ax.set_title(sys_name, fontsize=11)
        ax.grid(alpha=0.2)
        ax.set_xlabel("Race order")
    ax_list[0].set_ylabel(metric_col.upper())
    handles, labs = ax_list[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labs, loc="upper center", ncol=2)
    fig.suptitle(
        f"{metric_col.upper()} Race-Level Diagnostics (support filter: positives >= {min_support})",
        fontsize=13,
    )
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=400, bbox_inches="tight", pad_inches=0.12)
    fig.savefig(out_pdf, bbox_inches="tight", pad_inches=0.12)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    root = Path(args.root).resolve()
    outdir = (root / args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    used_sources: list[SourceRef] = []

    def use(role: str, system: str, profile: str, target: str, rel: str) -> Path:
        p = root / rel
        used_sources.append(SourceRef(role, system, profile, target, p))
        if not p.exists():
            raise FileNotFoundError(f"required source missing: {p}")
        return p

    # Core sources.
    path_sde_agg = use(
        "sde_aggregate",
        SYSTEM_FLINK,
        "c6_cfg120_fixed",
        "both",
        "data_lake/reports/final_sde_c6_cfg120_fixed_raw_clean_aggregate_2022_2025.csv",
    )
    path_batch_reco = use(
        "batch_recommended",
        "Batch",
        "both",
        "both",
        "data_lake/reports/ml_phase2b_dual_contract_2022_2025/recommended/phase2b_recommended_operating_points.csv",
    )
    path_moa_reco = use(
        "moa_recommended",
        "MOA",
        "both",
        "both",
        "data_lake/reports/sml_phase2b_dual_contract_2022_2025/recommended/phase2b_recommended_operating_points.csv",
    )
    path_batch_frontier = use(
        "batch_frontier",
        "Batch",
        "both",
        "both",
        "data_lake/reports/ml_phase2b_dual_contract_2022_2025/frontier/phase2b_threshold_frontier_compact.csv",
    )
    path_moa_frontier = use(
        "moa_frontier",
        "MOA",
        "both",
        "both",
        "data_lake/reports/sml_phase2b_dual_contract_2022_2025/frontier/phase2b_threshold_frontier_compact.csv",
    )
    path_runtime = use(
        "runtime",
        "shared",
        "all",
        "all",
        "data_lake/reports/phase2b_timing/training_runtime_2022_2025.csv",
    )
    path_universe = use(
        "truth_universe_race_driver",
        "shared",
        "canonical_sde_truth",
        "all",
        "data_lake/reports/ml_phase2b_dual_contract_2022_2025/audits/sde_universe_2022_2025.csv",
    )
    truth_event_paths = [
        use("truth_events", "shared", "canonical_sde_truth", "all", f"data_lake/reports/pit_truth_eligibility_audit_{y}_c6_cfg120_fixed.csv")
        for y in [2022, 2023, 2024, 2025]
    ]
    path_manifest = use(
        "figure_manifest",
        "shared",
        "presentation",
        "all",
        "data_lake/reports/phase2b_presentation_figures/phase2b_figures_manifest.csv",
    )
    path_pit_evals_merged = use(
        "pit_evals_merged",
        "shared",
        "merged",
        "pit_success_h2",
        "data_lake/pit_evals_9999_merged_20260508_145741.jsonl",
    )
    path_dataset = use(
        "prepared_dataset_noyear",
        "shared",
        "no_year",
        "both",
        "data_lake/ml_training_dataset_2022_2025_dual_contract.parquet",
    )

    # OOF sources.
    batch_oof_any_e0 = use("batch_oof", SYSTEM_BATCH_E0, PROFILE_E0, TARGET_ANY, f"data_lake/reports/ml_phase2b_dual_contract_2022_2025/oof/{PROFILE_E0}__{TARGET_ANY}.csv")
    batch_oof_any_p1 = use("batch_oof", SYSTEM_BATCH_P1, PROFILE_P1, TARGET_ANY, f"data_lake/reports/ml_phase2b_dual_contract_2022_2025/oof/{PROFILE_P1}__{TARGET_ANY}.csv")
    batch_oof_success_e0 = use("batch_oof", SYSTEM_BATCH_E0, PROFILE_E0, TARGET_SUCCESS, f"data_lake/reports/ml_phase2b_dual_contract_2022_2025/oof/{PROFILE_E0}__{TARGET_SUCCESS}.csv")
    batch_oof_success_p1 = use("batch_oof", SYSTEM_BATCH_P1, PROFILE_P1, TARGET_SUCCESS, f"data_lake/reports/ml_phase2b_dual_contract_2022_2025/oof/{PROFILE_P1}__{TARGET_SUCCESS}.csv")
    moa_export_success_e0 = use("moa_export", SYSTEM_MOA_E0, PROFILE_E0, TARGET_SUCCESS, f"data_lake/reports/sml_phase2b_dual_contract_2022_2025/exports/{PROFILE_E0}__{TARGET_SUCCESS}.csv")
    moa_export_success_p1 = use("moa_export", SYSTEM_MOA_P1, PROFILE_P1, TARGET_SUCCESS, f"data_lake/reports/sml_phase2b_dual_contract_2022_2025/exports/{PROFILE_P1}__{TARGET_SUCCESS}.csv")
    moa_oof_any_p1 = use("moa_oof", SYSTEM_MOA_P1, PROFILE_P1, TARGET_ANY, f"data_lake/reports/sml_phase2b_dual_contract_2022_2025/oof/{PROFILE_P1}__{TARGET_ANY}.csv")
    moa_oof_success_p1 = use("moa_oof", SYSTEM_MOA_P1, PROFILE_P1, TARGET_SUCCESS, f"data_lake/reports/sml_phase2b_dual_contract_2022_2025/oof/{PROFILE_P1}__{TARGET_SUCCESS}.csv")
    moa_oof_success_e0 = use("moa_oof", SYSTEM_MOA_E0, PROFILE_E0, TARGET_SUCCESS, f"data_lake/reports/sml_phase2b_dual_contract_2022_2025/oof/{PROFILE_E0}__{TARGET_SUCCESS}.csv")
    moa_oof_any_e0 = use("moa_oof", SYSTEM_MOA_E0, PROFILE_E0, TARGET_ANY, f"data_lake/reports/sml_phase2b_dual_contract_2022_2025/oof/{PROFILE_E0}__{TARGET_ANY}.csv")

    # SDE per-year jsonl streams.
    sde_suggestion_paths = [
        use("sde_pit_suggestions", SYSTEM_FLINK, "c6_cfg120_fixed", "pit_success_h2", f"data_lake/reports/variant_runs/c6_cfg120_fixed_{y}/pit_suggestions_{y}_season.jsonl")
        for y in [2022, 2023, 2024, 2025]
    ]
    sde_pit_eval_paths = [
        use("sde_pit_evals", SYSTEM_FLINK, "c6_cfg120_fixed", "pit_success_h2", f"data_lake/reports/variant_runs/c6_cfg120_fixed_{y}/pit_evals_{y}_season.jsonl")
        for y in [2022, 2023, 2024, 2025]
    ]

    # Load base inputs.
    sde_agg = _read_csv(path_sde_agg)
    batch_reco = _read_csv(path_batch_reco)
    moa_reco = _read_csv(path_moa_reco)
    batch_frontier = _read_csv(path_batch_frontier)
    moa_frontier = _read_csv(path_moa_frontier)
    runtime = _read_csv(path_runtime)
    universe = _load_universe(path_universe)
    truth_events = _filter_universe(_load_truth_events(truth_event_paths), universe)
    pit_evals_merged = _filter_universe(pd.read_json(path_pit_evals_merged, lines=True), universe)
    dataset = _filter_universe(pd.read_parquet(path_dataset), universe)

    # Global checks.
    manifest = _read_csv(path_manifest)
    if manifest["source_artifact"].astype(str).str.contains("ml_phase2a", na=False).any():
        raise RuntimeError("phase2a fallback found in current figure manifest")

    # Check stale pit_success rows.
    for p in [moa_export_success_e0, moa_export_success_p1, moa_oof_success_e0, moa_oof_success_p1]:
        rows, c0, c1 = _row_count_and_classes(p, "target_y")
        if rows != 91473 or c0 != 89361 or c1 != 2112:
            raise RuntimeError(
                f"stale or mismatched pit_success artifact: {p} rows={rows} class={c0}/{c1}"
            )

    # Verify corrected pit_success eligibility in prepared dataset.
    res_norm = (
        dataset["matched_pit_result"].astype(str).str.strip().str.upper().str.replace(" ", "_", regex=False)
    )
    unknown_mask = (
        res_norm.str.startswith("UNRESOLVED_")
        | res_norm.str.startswith("UNMAPPED_")
        | res_norm.eq("WEATHER_SURVIVAL_STOP")
    )
    unknown_trainable = dataset[unknown_mask & dataset[TARGET_SUCCESS_ELIG_COL].astype(bool)]
    if not unknown_trainable.empty:
        raise RuntimeError(
            f"unknown/unresolved/weather/unmapped pit_success rows still train-eligible: {len(unknown_trainable)}"
        )

    # Eligible successful denominator keys.
    eligible = truth_events[
        truth_events["eligible_universe"].astype(bool)
        & truth_events["eligible_clean_actionable"].astype(bool)
    ].copy()
    pos_norm = (
        eligible["result"].astype(str).str.strip().str.upper().str.replace(" ", "_", regex=False)
    )
    eligible_success = eligible[pos_norm.isin(POSITIVE_RESULTS)].copy()
    eligible_success_keys = _event_key_set(eligible_success)

    # PART 1 provenance table.
    prov_rows: list[dict[str, object]] = []
    for src in used_sources:
        exists = src.path.exists()
        rows = c0 = c1 = np.nan
        stale_warning = ""
        if exists:
            class_col = "target_y"
            if src.path.suffix.lower() == ".parquet":
                class_col = TARGET_SUCCESS
            try:
                rows_i, c0_i, c1_i = _row_count_and_classes(src.path, class_col)
                rows = rows_i
                c0 = c0_i if c0_i is not None else np.nan
                c1 = c1_i if c1_i is not None else np.nan
            except Exception:
                pass
        if "pit_success" in src.target and "MOA" in src.system and not np.isnan(rows):
            if int(rows) == 93623:
                stale_warning = "STALE_93623_ROWS"
        prov_rows.append(
            {
                "artifact_role": src.artifact_role,
                "system": src.system,
                "profile": src.profile,
                "target": src.target,
                "path": str(src.path),
                "exists": bool(exists),
                "row_count_if_applicable": rows,
                "class0_if_applicable": c0,
                "class1_if_applicable": c1,
                "stale_warning": stale_warning,
                "notes": "",
            }
        )
    provenance_df = pd.DataFrame(prov_rows)
    _write_csv_md(
        provenance_df,
        outdir / "final_refresh_input_provenance.csv",
        outdir / "final_refresh_input_provenance.md",
        "Final Refresh Input Provenance",
    )

    # PART 2A pit_any final recap.
    sde_clean = sde_agg[sde_agg["truth_lens"].astype(str).eq("clean_actionable")]
    if sde_clean.empty:
        raise RuntimeError("missing clean_actionable row in SDE aggregate")
    sde_clean = sde_clean.iloc[0]

    def sel(df: pd.DataFrame, profile: str, target: str) -> pd.Series:
        w = df[
            (df["profile"].astype(str) == profile)
            & (df["target_column"].astype(str) == target)
            & (df["truth_lens"].astype(str) == "clean_actionable")
        ].copy()
        if "truth_universe_mode" in w.columns:
            w = w[w["truth_universe_mode"].astype(str) == "canonical_sde_truth"].copy()
        if w.empty:
            raise RuntimeError(f"missing selected row profile={profile} target={target}")
        return w.iloc[0]

    b_any_e0 = sel(batch_reco, PROFILE_E0, TARGET_ANY)
    b_any_p1 = sel(batch_reco, PROFILE_P1, TARGET_ANY)
    m_any_e0 = sel(moa_reco, PROFILE_E0, TARGET_ANY)
    m_any_p1 = sel(moa_reco, PROFILE_P1, TARGET_ANY)

    pit_any_df = pd.DataFrame(
        [
            {
                "System": SYSTEM_FLINK,
                "learning_mode": "deterministic streaming rule",
                "target": "pit_any_h2",
                "selected_threshold": "rule",
                "precision": float(sde_clean["pit_any_precision"]),
                "event_recall": float(sde_clean["pit_any_recall"]),
                "F0.5": float(sde_clean["pit_any_f0_5"]),
                "AP": np.nan,
                "notes": "Rule baseline (no score threshold).",
            },
            {
                "System": SYSTEM_BATCH_E0,
                "learning_mode": "offline batch ML (OOF)",
                "target": "pit_any_h2",
                "selected_threshold": float(b_any_e0["selected_threshold"]),
                "precision": float(b_any_e0["precision"]),
                "event_recall": float(b_any_e0["recall"]),
                "F0.5": float(b_any_e0["f0_5"]),
                "AP": float(b_any_e0["AP"]),
                "notes": "selected operating point",
            },
            {
                "System": SYSTEM_BATCH_P1,
                "learning_mode": "offline batch ML (OOF)",
                "target": "pit_any_h2",
                "selected_threshold": float(b_any_p1["selected_threshold"]),
                "precision": float(b_any_p1["precision"]),
                "event_recall": float(b_any_p1["recall"]),
                "F0.5": float(b_any_p1["f0_5"]),
                "AP": float(b_any_p1["AP"]),
                "notes": "selected operating point",
            },
            {
                "System": SYSTEM_MOA_E0,
                "learning_mode": "online MOA prequential",
                "target": "pit_any_h2",
                "selected_threshold": float(m_any_e0["selected_threshold"]),
                "precision": float(m_any_e0["precision"]),
                "event_recall": float(m_any_e0["recall"]),
                "F0.5": float(m_any_e0["f0_5"]),
                "AP": float(m_any_e0["AP"]),
                "notes": "selected operating point",
            },
            {
                "System": SYSTEM_MOA_P1,
                "learning_mode": "online MOA prequential",
                "target": "pit_any_h2",
                "selected_threshold": float(m_any_p1["selected_threshold"]),
                "precision": float(m_any_p1["precision"]),
                "event_recall": float(m_any_p1["recall"]),
                "F0.5": float(m_any_p1["f0_5"]),
                "AP": float(m_any_p1["AP"]),
                "notes": "selected operating point",
            },
        ]
    )
    _write_csv_md(
        pit_any_df,
        outdir / "pit_any_final_recap.csv",
        outdir / "pit_any_final_recap.md",
        "pit_any Final Recap",
    )

    # PART 2B corrected SDE pit_success diagnostic.
    sde_suggestions = _filter_universe(_load_jsonls_with_year_prefix(sde_suggestion_paths), universe)
    sde_evals = _filter_universe(_load_jsonls_with_year_prefix(sde_pit_eval_paths), universe)

    sde_diag_rows: list[dict[str, object]] = []
    for mode, label in [
        (ACTIONABLE_MODE_PIT_NOW_ONLY, "PIT_NOW"),
        (ACTIONABLE_MODE_PIT_NOW_PLUS_GOOD_PIT, "PIT_NOW + GOOD_PIT"),
    ]:
        comp = _build_comparator_dataset(
            suggestions=sde_suggestions,
            pit_evals=sde_evals,
            horizon=2,
            outcome_mode=OUTCOME_PIT_SUCCESS_H2,
            pit_timings=None,
            actionable_mode=mode,
            episode_level=False,
            include_same_lap=False,
            pit_success_no_match_as_negative=True,
        )
        m = _comparator_metrics(comp, eligible_success_keys)
        sde_diag_rows.append(
            {
                "System": SYSTEM_FLINK,
                "positive_call_definition": label,
                "predicted_positives": m["predicted_positives"],
                "matched_known_pits": m["matched_known_pits"],
                "matched_success": m["matched_success"],
                "matched_failure": m["matched_failure"],
                "matched_unknown": m["matched_unknown"],
                "matched_pit_success_rate": m["matched_pit_success_rate"],
                "strict_precision": m["strict_precision"],
                "notes": "matched_pit_success_rate is diagnostic only",
            }
        )
    sde_diag_df = pd.DataFrame(sde_diag_rows)
    _write_csv_md(
        sde_diag_df,
        outdir / "pit_success_sde_diagnostic.csv",
        outdir / "pit_success_sde_diagnostic.md",
        "pit_success SDE Diagnostic",
    )

    # PART 2C learners + SDE apples-to-apples.
    b_success_e0 = sel(batch_reco, PROFILE_E0, TARGET_SUCCESS)
    b_success_p1 = sel(batch_reco, PROFILE_P1, TARGET_SUCCESS)
    m_success_e0 = sel(moa_reco, PROFILE_E0, TARGET_SUCCESS)
    m_success_p1 = sel(moa_reco, PROFILE_P1, TARGET_SUCCESS)

    oof_batch_e0_success = _filter_universe(pd.read_csv(batch_oof_success_e0), universe)
    oof_batch_p1_success = _filter_universe(pd.read_csv(batch_oof_success_p1), universe)
    oof_moa_e0_success = _filter_universe(pd.read_csv(moa_oof_success_e0), universe)
    oof_moa_p1_success = _filter_universe(pd.read_csv(moa_oof_success_p1), universe)

    comp_batch_e0 = _build_learner_comparator(oof_batch_e0_success, float(b_success_e0["selected_threshold"]), pit_evals_merged)
    comp_batch_p1 = _build_learner_comparator(oof_batch_p1_success, float(b_success_p1["selected_threshold"]), pit_evals_merged)
    comp_moa_e0 = _build_learner_comparator(oof_moa_e0_success, float(m_success_e0["selected_threshold"]), pit_evals_merged)
    comp_moa_p1 = _build_learner_comparator(oof_moa_p1_success, float(m_success_p1["selected_threshold"]), pit_evals_merged)

    sde_pitnow = sde_diag_df[sde_diag_df["positive_call_definition"] == "PIT_NOW"].iloc[0]
    met_batch_e0 = _comparator_metrics(comp_batch_e0, eligible_success_keys)
    met_batch_p1 = _comparator_metrics(comp_batch_p1, eligible_success_keys)
    met_moa_e0 = _comparator_metrics(comp_moa_e0, eligible_success_keys)
    met_moa_p1 = _comparator_metrics(comp_moa_p1, eligible_success_keys)

    apples_df = pd.DataFrame(
        [
            {
                "System": SYSTEM_FLINK,
                "positive_call_definition": "PIT_NOW",
                "predicted_positives": int(sde_pitnow["predicted_positives"]),
                "TP": int(sde_pitnow["matched_success"]),
                "FP_no_match": int(
                    sde_pitnow["predicted_positives"] - sde_pitnow["matched_known_pits"] - sde_pitnow["matched_unknown"]
                ),
                "FP_failure": int(sde_pitnow["matched_failure"]),
                "unknown_excluded": int(sde_pitnow["matched_unknown"]),
                "strict_precision": float(sde_pitnow["strict_precision"]),
                "successful_event_coverage": float(
                    _safe_div(
                        int(sde_pitnow["matched_success"]),
                        len(eligible_success_keys),
                    )
                ),
                "F0.5": float(
                    _f_beta(
                        float(sde_pitnow["strict_precision"]),
                        _safe_div(int(sde_pitnow["matched_success"]), len(eligible_success_keys)),
                    )
                ),
                "notes": "Main rule baseline. Matched-pit rate is diagnostic-only.",
            },
            {
                "System": SYSTEM_BATCH_E0,
                "positive_call_definition": f"score>={float(b_success_e0['selected_threshold']):.2f}",
                "predicted_positives": met_batch_e0["predicted_positives"],
                "TP": met_batch_e0["TP"],
                "FP_no_match": met_batch_e0["FP_no_match"],
                "FP_failure": met_batch_e0["FP_failure"],
                "unknown_excluded": met_batch_e0["unknown_excluded"],
                "strict_precision": met_batch_e0["strict_precision"],
                "successful_event_coverage": met_batch_e0["successful_event_coverage"],
                "F0.5": met_batch_e0["F0_5"],
                "notes": "selected operating point",
            },
            {
                "System": SYSTEM_BATCH_P1,
                "positive_call_definition": f"score>={float(b_success_p1['selected_threshold']):.2f}",
                "predicted_positives": met_batch_p1["predicted_positives"],
                "TP": met_batch_p1["TP"],
                "FP_no_match": met_batch_p1["FP_no_match"],
                "FP_failure": met_batch_p1["FP_failure"],
                "unknown_excluded": met_batch_p1["unknown_excluded"],
                "strict_precision": met_batch_p1["strict_precision"],
                "successful_event_coverage": met_batch_p1["successful_event_coverage"],
                "F0.5": met_batch_p1["F0_5"],
                "notes": "selected operating point",
            },
            {
                "System": SYSTEM_MOA_E0,
                "positive_call_definition": f"score>={float(m_success_e0['selected_threshold']):.2f}",
                "predicted_positives": met_moa_e0["predicted_positives"],
                "TP": met_moa_e0["TP"],
                "FP_no_match": met_moa_e0["FP_no_match"],
                "FP_failure": met_moa_e0["FP_failure"],
                "unknown_excluded": met_moa_e0["unknown_excluded"],
                "strict_precision": met_moa_e0["strict_precision"],
                "successful_event_coverage": met_moa_e0["successful_event_coverage"],
                "F0.5": met_moa_e0["F0_5"],
                "notes": "selected operating point",
            },
            {
                "System": SYSTEM_MOA_P1,
                "positive_call_definition": f"score>={float(m_success_p1['selected_threshold']):.2f}",
                "predicted_positives": met_moa_p1["predicted_positives"],
                "TP": met_moa_p1["TP"],
                "FP_no_match": met_moa_p1["FP_no_match"],
                "FP_failure": met_moa_p1["FP_failure"],
                "unknown_excluded": met_moa_p1["unknown_excluded"],
                "strict_precision": met_moa_p1["strict_precision"],
                "successful_event_coverage": met_moa_p1["successful_event_coverage"],
                "F0.5": met_moa_p1["F0_5"],
                "notes": "selected operating point",
            },
        ]
    )
    _write_csv_md(
        apples_df,
        outdir / "pit_success_apples_to_apples.csv",
        outdir / "pit_success_apples_to_apples.md",
        "pit_success Apples-to-Apples Main Comparison",
    )

    # PART 2D threshold sensitivity diagnostic.
    def frontier_subset(df: pd.DataFrame, profile: str) -> pd.DataFrame:
        w = df[
            (df["profile"].astype(str) == profile)
            & (df["target_column"].astype(str) == TARGET_SUCCESS)
            & (df["truth_lens"].astype(str) == "clean_actionable")
        ].copy()
        if "truth_universe_mode" in w.columns:
            w = w[w["truth_universe_mode"].astype(str) == "canonical_sde_truth"].copy()
        return w

    b_front_p1 = frontier_subset(batch_frontier, PROFILE_P1)
    m_front_p1 = frontier_subset(moa_frontier, PROFILE_P1)
    b_max = b_front_p1.sort_values(by=["f0_5", "recall", "precision"], ascending=[False, False, False]).iloc[0]
    m_max = m_front_p1.sort_values(by=["f0_5", "recall", "precision"], ascending=[False, False, False]).iloc[0]

    sens_rows = [
        {
            "System": SYSTEM_BATCH_P1,
            "threshold/policy": f"{float(b_success_p1['selected_threshold']):.2f} (selected)",
            "predicted_positives": int(met_batch_p1["predicted_positives"]),
            "strict_precision": float(met_batch_p1["strict_precision"]),
            "successful_event_coverage": float(met_batch_p1["successful_event_coverage"]),
            "F0.5": float(met_batch_p1["F0_5"]),
            "notes": "selected operating point",
        },
        {
            "System": SYSTEM_BATCH_P1,
            "threshold/policy": f"{float(b_max['selected_threshold']):.2f} (max F0.5)",
            "predicted_positives": int(b_max["predicted_positive_rows"]),
            "strict_precision": float(b_max["precision"]),
            "successful_event_coverage": float(b_max["recall"]),
            "F0.5": float(b_max["f0_5"]),
            "notes": "relaxed diagnostic point",
        },
        {
            "System": SYSTEM_MOA_P1,
            "threshold/policy": f"{float(m_success_p1['selected_threshold']):.2f} (selected)",
            "predicted_positives": int(met_moa_p1["predicted_positives"]),
            "strict_precision": float(met_moa_p1["strict_precision"]),
            "successful_event_coverage": float(met_moa_p1["successful_event_coverage"]),
            "F0.5": float(met_moa_p1["F0_5"]),
            "notes": "selected operating point",
        },
        {
            "System": SYSTEM_MOA_P1,
            "threshold/policy": f"{float(m_max['selected_threshold']):.2f} (max F0.5 / relaxed)",
            "predicted_positives": int(m_max["predicted_positive_rows"]),
            "strict_precision": float(m_max["precision"]),
            "successful_event_coverage": float(m_max["recall"]),
            "F0.5": float(m_max["f0_5"]),
            "notes": "relaxed diagnostic point",
        },
    ]
    sens_df = pd.DataFrame(sens_rows)
    _write_csv_md(
        sens_df,
        outdir / "pit_success_threshold_sensitivity.csv",
        outdir / "pit_success_threshold_sensitivity.md",
        "pit_success Threshold Sensitivity",
    )

    # PART 3 figures.
    _plot_headline(
        pit_any_df[["System", "precision", "event_recall", "F0.5"]].copy(),
        outdir / "pit_any_headline_comparison.png",
        outdir / "pit_any_headline_comparison.pdf",
        "pit_any_h2 Headline Comparison (canonical_sde_truth + clean_actionable)",
    )
    _plot_apples(
        apples_df[["System", "strict_precision", "successful_event_coverage", "F0.5"]].copy(),
        outdir / "pit_success_apples_to_apples_main.png",
        outdir / "pit_success_apples_to_apples_main.pdf",
    )
    _plot_sde_diag(
        sde_diag_df[["positive_call_definition", "matched_pit_success_rate", "strict_precision"]].copy(),
        outdir / "pit_success_sde_diagnostic_operational_vs_matched.png",
        outdir / "pit_success_sde_diagnostic_operational_vs_matched.pdf",
    )

    runtime_slide = runtime[
        (
            (runtime["learner"].astype(str) == "batch")
            & (runtime["stage"].astype(str) == "train")
            & (runtime["target"].astype(str).isin([TARGET_ANY, TARGET_SUCCESS]))
        )
        | (
            (runtime["learner"].astype(str) == "moa")
            & (runtime["stage"].astype(str) == "train_eval")
            & (runtime["target"].astype(str).isin([TARGET_ANY, TARGET_SUCCESS]))
        )
    ][
        [
            "learner",
            "profile",
            "target",
            "stage",
            "wall_seconds",
            "wall_hms",
            "command_name",
            "log_file",
            "output_file",
            "status",
        ]
    ].copy()
    runtime_slide.rename(columns={"stage": "timed_stage"}, inplace=True)
    _write_csv_md(
        runtime_slide,
        outdir / "training_runtime_slide_table.csv",
        outdir / "training_runtime_slide_table.md",
        "Training Runtime Slide Table",
    )
    _plot_runtime(
        runtime_slide,
        outdir / "training_runtime_comparison.png",
        outdir / "training_runtime_comparison.pdf",
    )

    # Race-level diagnostics setup.
    # SDE row-level predictions from PIT_NOW labels.
    sugg = sde_suggestions.copy()
    sugg["label"] = sugg["suggestionLabel"].astype(str).str.strip().str.upper().str.replace(" ", "_", regex=False)
    pit_now_keys = set(
        (str(r), str(d), int(l))
        for r, d, l in sugg[sugg["label"] == "PIT_NOW"][["race", "driver", "lapNumber"]].itertuples(index=False, name=None)
    )

    ds_any = dataset.copy()
    ds_any[TARGET_ANY] = pd.to_numeric(ds_any[TARGET_ANY], errors="coerce").fillna(0).astype(int)
    ds_any["pred_sde"] = [
        1 if (str(r), str(d), int(l)) in pit_now_keys else 0
        for r, d, l in ds_any[["race", "driver", "lapNumber"]].itertuples(index=False, name=None)
    ]

    ds_success = dataset[dataset[TARGET_SUCCESS_ELIG_COL].astype(bool)].copy()
    ds_success[TARGET_SUCCESS] = pd.to_numeric(ds_success[TARGET_SUCCESS], errors="coerce").fillna(0).astype(int)
    ds_success["pred_sde"] = [
        1 if (str(r), str(d), int(l)) in pit_now_keys else 0
        for r, d, l in ds_success[["race", "driver", "lapNumber"]].itertuples(index=False, name=None)
    ]

    # Batch/MOA predictions at selected thresholds.
    b_any_e0_thr = float(b_any_e0["selected_threshold"])
    b_any_p1_thr = float(b_any_p1["selected_threshold"])
    b_succ_e0_thr = float(b_success_e0["selected_threshold"])
    b_succ_p1_thr = float(b_success_p1["selected_threshold"])
    m_any_p1_thr = float(m_any_p1["selected_threshold"])
    m_succ_p1_thr = float(m_success_p1["selected_threshold"])

    b_any_e0 = _filter_universe(pd.read_csv(batch_oof_any_e0), universe)
    b_any_p1 = _filter_universe(pd.read_csv(batch_oof_any_p1), universe)
    b_succ_e0 = _filter_universe(pd.read_csv(batch_oof_success_e0), universe)
    b_succ_p1 = _filter_universe(pd.read_csv(batch_oof_success_p1), universe)
    m_any_p1 = _filter_universe(pd.read_csv(moa_oof_any_p1), universe)
    m_succ_p1 = _filter_universe(pd.read_csv(moa_oof_success_p1), universe)

    for df, thr in [
        (b_any_e0, b_any_e0_thr),
        (b_any_p1, b_any_p1_thr),
        (b_succ_e0, b_succ_e0_thr),
        (b_succ_p1, b_succ_p1_thr),
        (m_any_p1, m_any_p1_thr),
        (m_succ_p1, m_succ_p1_thr),
    ]:
        df["pred"] = (pd.to_numeric(df["calibrated_proba"], errors="coerce").fillna(0.0) >= float(thr)).astype(int)

    race_metric_rows: list[pd.DataFrame] = []
    # SDE
    sde_any_race = _confusion_metrics_by_race(ds_any, TARGET_ANY, "pred_sde")
    sde_any_race["system"] = SYSTEM_FLINK
    sde_any_race["target"] = TARGET_ANY
    race_metric_rows.append(sde_any_race)

    sde_success_race = _confusion_metrics_by_race(ds_success, TARGET_SUCCESS, "pred_sde")
    sde_success_race["system"] = SYSTEM_FLINK
    sde_success_race["target"] = TARGET_SUCCESS
    race_metric_rows.append(sde_success_race)

    # Batch E0
    be0_any = _confusion_metrics_by_race(b_any_e0.rename(columns={"target_y": TARGET_ANY}), TARGET_ANY, "pred")
    be0_any["system"] = SYSTEM_BATCH_E0
    be0_any["target"] = TARGET_ANY
    race_metric_rows.append(be0_any)

    be0_success = _confusion_metrics_by_race(b_succ_e0.rename(columns={"target_y": TARGET_SUCCESS}), TARGET_SUCCESS, "pred")
    be0_success["system"] = SYSTEM_BATCH_E0
    be0_success["target"] = TARGET_SUCCESS
    race_metric_rows.append(be0_success)

    # Batch P1
    bp1_any = _confusion_metrics_by_race(b_any_p1.rename(columns={"target_y": TARGET_ANY}), TARGET_ANY, "pred")
    bp1_any["system"] = SYSTEM_BATCH_P1
    bp1_any["target"] = TARGET_ANY
    race_metric_rows.append(bp1_any)

    bp1_success = _confusion_metrics_by_race(b_succ_p1.rename(columns={"target_y": TARGET_SUCCESS}), TARGET_SUCCESS, "pred")
    bp1_success["system"] = SYSTEM_BATCH_P1
    bp1_success["target"] = TARGET_SUCCESS
    race_metric_rows.append(bp1_success)

    # MOA Percent
    mp1_any = _confusion_metrics_by_race(m_any_p1.rename(columns={"target_y": TARGET_ANY}), TARGET_ANY, "pred")
    mp1_any["system"] = SYSTEM_MOA_P1
    mp1_any["target"] = TARGET_ANY
    race_metric_rows.append(mp1_any)

    mp1_success = _confusion_metrics_by_race(m_succ_p1.rename(columns={"target_y": TARGET_SUCCESS}), TARGET_SUCCESS, "pred")
    mp1_success["system"] = SYSTEM_MOA_P1
    mp1_success["target"] = TARGET_SUCCESS
    race_metric_rows.append(mp1_success)

    race_metrics = pd.concat(race_metric_rows, ignore_index=True)
    # Keep only requested panel systems.
    race_metrics = race_metrics[race_metrics["system"].isin([SYSTEM_FLINK, SYSTEM_BATCH_E0, SYSTEM_BATCH_P1, SYSTEM_MOA_P1])].copy()

    # Filter support >0 for diagnostics requirement.
    race_metrics = race_metrics[race_metrics["positives"] >= int(args.race_min_positive_support)].copy()
    _write_csv_md(
        race_metrics,
        outdir / "race_level_support_diagnostics.csv",
        outdir / "race_level_support_diagnostics.md",
        "Race-Level Support Diagnostics",
    )

    _plot_race_diag(
        race_metrics,
        "kappa",
        outdir / "race_level_kappa_diagnostics.png",
        outdir / "race_level_kappa_diagnostics.pdf",
        int(args.race_min_positive_support),
    )
    _plot_race_diag(
        race_metrics,
        "gmean",
        outdir / "race_level_gmean_diagnostics.png",
        outdir / "race_level_gmean_diagnostics.pdf",
        int(args.race_min_positive_support),
    )

    # PART 4 checks.
    # no-match should be FP in selected learner points (no EXCLUDED rows with NO_MATCH).
    for name, comp in [
        (SYSTEM_BATCH_E0, comp_batch_e0),
        (SYSTEM_BATCH_P1, comp_batch_p1),
        (SYSTEM_MOA_E0, comp_moa_e0),
        (SYSTEM_MOA_P1, comp_moa_p1),
    ]:
        bad = comp[
            comp["outcome_class"].astype(str).eq("EXCLUDED")
            & comp["exclusion_reason"].fillna("").astype(str).str.upper().eq("NO_MATCH_WITHIN_HORIZON")
        ]
        if not bad.empty:
            raise RuntimeError(f"NO_MATCH_WITHIN_HORIZON still EXCLUDED for {name}: {len(bad)}")

    # Ensure main comparison does not use matched rate as precision.
    if "matched_pit_success_rate" in apples_df.columns:
        raise RuntimeError("apples-to-apples table still contains matched_pit_success_rate column")

    # Ensure ~60% value only appears in diagnostic.
    # We check nearest SDE diagnostic rate appears and main strict precision remains low.
    if not (sde_diag_df["matched_pit_success_rate"].max() > 0.5):
        raise RuntimeError("expected SDE matched-pit diagnostic > 0.5 not found")
    if apples_df[apples_df["System"] == SYSTEM_FLINK]["strict_precision"].max() > 0.5:
        raise RuntimeError("SDE strict precision unexpectedly >0.5 in main comparison")

    # Summary + manifest.
    summary_lines = [
        "# Final Refresh Summary",
        "",
        "## What changed",
        "- Built final presentation package from current corrected pit_success_h2 artifacts only.",
        "- Main pit_success comparison now uses strict operational precision (no-match positives are FP).",
        "- Matched-pit success rate is retained only in SDE diagnostic outputs.",
        "",
        "## Authoritative inputs",
        f"- SDE aggregate: `{path_sde_agg}`",
        f"- Batch recommended/frontier: `{path_batch_reco}`, `{path_batch_frontier}`",
        f"- MOA recommended/frontier: `{path_moa_reco}`, `{path_moa_frontier}`",
        f"- Runtime table: `{path_runtime}`",
        f"- Canonical universe: `{path_universe}` + pit-truth eligibility audits 2022-2025",
        "",
        "## pit_success slide guidance",
        "- Main comparison: use strict_precision + successful_event_coverage + F0.5.",
        "- SDE diagnostic slide: show matched_pit_success_rate next to strict_precision with explicit caveat.",
        "- Never label matched-pit success rate as precision in the main comparison.",
        "",
        "## Difference to explain orally",
        "- matched-pit success rate = quality conditional on matched known pit outcomes (diagnostic).",
        "- strict precision = TP / (TP + FP_no_match + FP_failure) (operational, comparable with learners).",
        "",
        "## Replacement figures",
        f"- `{outdir / 'pit_any_headline_comparison.png'}`",
        f"- `{outdir / 'pit_success_apples_to_apples_main.png'}`",
        f"- `{outdir / 'pit_success_sde_diagnostic_operational_vs_matched.png'}`",
        f"- `{outdir / 'training_runtime_comparison.png'}`",
        f"- `{outdir / 'race_level_kappa_diagnostics.png'}`",
        f"- `{outdir / 'race_level_gmean_diagnostics.png'}`",
        "",
    ]
    (outdir / "final_refresh_summary.md").write_text("\n".join(summary_lines), encoding="utf-8")

    outputs = [
        "final_refresh_summary.md",
        "final_refresh_input_provenance.csv",
        "final_refresh_input_provenance.md",
        "pit_any_final_recap.csv",
        "pit_any_final_recap.md",
        "pit_success_sde_diagnostic.csv",
        "pit_success_sde_diagnostic.md",
        "pit_success_apples_to_apples.csv",
        "pit_success_apples_to_apples.md",
        "pit_success_threshold_sensitivity.csv",
        "pit_success_threshold_sensitivity.md",
        "training_runtime_slide_table.csv",
        "training_runtime_slide_table.md",
        "race_level_support_diagnostics.csv",
        "race_level_support_diagnostics.md",
        "pit_any_headline_comparison.png",
        "pit_any_headline_comparison.pdf",
        "pit_success_apples_to_apples_main.png",
        "pit_success_apples_to_apples_main.pdf",
        "pit_success_sde_diagnostic_operational_vs_matched.png",
        "pit_success_sde_diagnostic_operational_vs_matched.pdf",
        "training_runtime_comparison.png",
        "training_runtime_comparison.pdf",
        "race_level_kappa_diagnostics.png",
        "race_level_kappa_diagnostics.pdf",
        "race_level_gmean_diagnostics.png",
        "race_level_gmean_diagnostics.pdf",
    ]
    manifest_rows: list[dict[str, object]] = []
    for rel in outputs:
        p = outdir / rel
        manifest_rows.append(
            {
                "output_file": str(p),
                "exists": p.exists(),
                "sources": "; ".join(sorted(set(str(s.path) for s in used_sources))),
            }
        )
    manifest_df = pd.DataFrame(manifest_rows)
    manifest_df.to_csv(outdir / "final_refresh_manifest.csv", index=False)

    missing_outputs = [r["output_file"] for r in manifest_rows if not r["exists"]]
    if missing_outputs:
        raise RuntimeError(f"missing expected final_refresh outputs: {missing_outputs}")

    print("Final refresh package built.")
    print(f"Output directory: {outdir}")
    print("Key tables:")
    print(f"- {outdir / 'pit_any_final_recap.csv'}")
    print(f"- {outdir / 'pit_success_sde_diagnostic.csv'}")
    print(f"- {outdir / 'pit_success_apples_to_apples.csv'}")
    print(f"- {outdir / 'pit_success_threshold_sensitivity.csv'}")
    print(f"- {outdir / 'training_runtime_slide_table.csv'}")
    print("Figures:")
    print(f"- {outdir / 'pit_any_headline_comparison.png'}")
    print(f"- {outdir / 'pit_success_apples_to_apples_main.png'}")
    print(f"- {outdir / 'pit_success_sde_diagnostic_operational_vs_matched.png'}")
    print(f"- {outdir / 'training_runtime_comparison.png'}")
    print(f"- {outdir / 'race_level_kappa_diagnostics.png'}")
    print(f"- {outdir / 'race_level_gmean_diagnostics.png'}")


if __name__ == "__main__":
    main()
