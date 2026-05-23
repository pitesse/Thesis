#!/usr/bin/env python3
"""Build remaining final slide assets from the current final_refresh package.

This script is intentionally reporting-only: no model training is run.
"""

from __future__ import annotations

import argparse
import math
import re
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, precision_recall_curve

from .build_phase2b_presentation_figures import build_final_refresh_pr_assets
from .build_final_refresh_package import (
    SYSTEM_BATCH_E0,
    SYSTEM_BATCH_P1,
    SYSTEM_FLINK,
    SYSTEM_MOA_E0,
    SYSTEM_MOA_P1,
    _confusion_metrics_by_race,
    _filter_universe,
    _load_jsonls_with_year_prefix,
    _load_universe,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
FINAL_REFRESH_DIR = PROJECT_ROOT / "data_lake" / "reports" / "phase2b_presentation_figures" / "final_refresh"
FIG_MANIFEST = PROJECT_ROOT / "data_lake" / "reports" / "phase2b_presentation_figures" / "phase2b_figures_manifest.csv"
FINAL_REFRESH_MANIFEST = FINAL_REFRESH_DIR / "final_refresh_manifest.csv"

BATCH_OOF_DIR = PROJECT_ROOT / "data_lake" / "reports" / "ml_phase2b_dual_contract_2022_2025" / "oof"
MOA_OOF_DIR = PROJECT_ROOT / "data_lake" / "reports" / "sml_phase2b_dual_contract_2022_2025" / "oof"
BATCH_EVAL_DIR = PROJECT_ROOT / "data_lake" / "reports" / "ml_phase2b_dual_contract_2022_2025" / "eval"
MOA_EVAL_DIR = PROJECT_ROOT / "data_lake" / "reports" / "sml_phase2b_dual_contract_2022_2025" / "eval"

LABEL_SUMMARY = PROJECT_ROOT / "data_lake" / "reports" / "ml_phase2b_dual_contract_2022_2025" / "audits" / "label_summary_no_year.csv"
TARGET_CONTRACT_AUDIT = PROJECT_ROOT / "data_lake" / "reports" / "ml_phase2b_dual_contract_2022_2025" / "audits" / "pit_success_label_contract_check.csv"

RACE_DIAG_SOURCE = FINAL_REFRESH_DIR / "race_level_support_diagnostics.csv"
PIT_SUCCESS_SENSITIVITY_SOURCE = FINAL_REFRESH_DIR / "pit_success_threshold_sensitivity.csv"
SDE_DIAG_SOURCE = FINAL_REFRESH_DIR / "pit_success_sde_diagnostic.csv"
PIT_ANY_RECAP_SOURCE = FINAL_REFRESH_DIR / "pit_any_final_recap.csv"
PIT_SUCCESS_APPLES_SOURCE = FINAL_REFRESH_DIR / "pit_success_apples_to_apples.csv"
UNIVERSE_SOURCE = PROJECT_ROOT / "data_lake" / "reports" / "ml_phase2b_dual_contract_2022_2025" / "audits" / "sde_universe_2022_2025.csv"
DATASET_SOURCE = PROJECT_ROOT / "data_lake" / "ml_training_dataset_2022_2025_dual_contract.parquet"
SDE_SUGGESTION_PATHS = [
    PROJECT_ROOT / "data_lake" / "reports" / "variant_runs" / f"c6_cfg120_fixed_{y}" / f"pit_suggestions_{y}_season.jsonl"
    for y in [2022, 2023, 2024, 2025]
]

TARGET_ANY = "target_pit_any_h2_clean_actionable"
TARGET_SUCCESS = "target_pit_success_h2_clean_actionable"
TARGET_SUCCESS_ELIG_COL = f"{TARGET_SUCCESS}_train_eligible"

SYSTEM_LABELS = {
    "batch_e0": "Batch No-Year",
    "batch_p1": "Batch Percent",
    "moa_e0": "MOA No-Year",
    "moa_p1": "MOA Percent",
}

SYSTEM_ORDER_RACE = [SYSTEM_FLINK, SYSTEM_BATCH_E0, SYSTEM_BATCH_P1, SYSTEM_MOA_E0, SYSTEM_MOA_P1]


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"missing required file: {path}")
    return pd.read_csv(path)


def _fmt_float(value: object, digits: int = 6) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, (float, np.floating)):
        if math.isnan(float(value)):
            return ""
        return f"{float(value):.{digits}f}"
    return str(value)


def _write_markdown_table(path: Path, title: str, df: pd.DataFrame, float_digits: int = 6) -> None:
    cols = list(df.columns)
    lines = [f"# {title}", "", "| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for _, row in df.iterrows():
        vals = [_fmt_float(row[c], digits=float_digits) for c in cols]
        lines.append("| " + " | ".join(vals) + " |")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def _score_column(df: pd.DataFrame) -> str:
    if "calibrated_proba" in df.columns and df["calibrated_proba"].notna().any():
        return "calibrated_proba"
    if "raw_proba" in df.columns and df["raw_proba"].notna().any():
        return "raw_proba"
    raise ValueError("no usable score column found (need calibrated_proba or raw_proba)")


def _load_pr_sources() -> dict[str, dict[str, Path]]:
    return {
        TARGET_ANY: {
            "batch_e0": BATCH_OOF_DIR / "e0_no_source_year__target_pit_any_h2_clean_actionable.csv",
            "batch_p1": BATCH_OOF_DIR / "p1_percent_conservative_v1__target_pit_any_h2_clean_actionable.csv",
            "moa_e0": MOA_OOF_DIR / "e0_no_source_year__target_pit_any_h2_clean_actionable.csv",
            "moa_p1": MOA_OOF_DIR / "p1_percent_conservative_v1__target_pit_any_h2_clean_actionable.csv",
        },
        TARGET_SUCCESS: {
            "batch_e0": BATCH_OOF_DIR / "e0_no_source_year__target_pit_success_h2_clean_actionable.csv",
            "batch_p1": BATCH_OOF_DIR / "p1_percent_conservative_v1__target_pit_success_h2_clean_actionable.csv",
            "moa_e0": MOA_OOF_DIR / "e0_no_source_year__target_pit_success_h2_clean_actionable.csv",
            "moa_p1": MOA_OOF_DIR / "p1_percent_conservative_v1__target_pit_success_h2_clean_actionable.csv",
        },
    }


def _hard_checks() -> dict[str, str]:
    checks: dict[str, str] = {}

    fig_manifest = _read_csv(FIG_MANIFEST)
    if "source_artifact" in fig_manifest.columns:
        n_phase2a = int(fig_manifest["source_artifact"].fillna("").str.contains("ml_phase2a", case=False).sum())
    else:
        n_phase2a = 0
    checks["no_phase2a_fallback"] = "PASS" if n_phase2a == 0 else f"FAIL: phase2a rows={n_phase2a}"

    fr_manifest = _read_csv(FINAL_REFRESH_MANIFEST)
    n_phase2a_fr = int(fr_manifest.get("sources", pd.Series(dtype=str)).fillna("").str.contains("ml_phase2a", case=False).sum())
    checks["no_phase2a_in_final_refresh_manifest"] = "PASS" if n_phase2a_fr == 0 else f"FAIL: rows={n_phase2a_fr}"

    # stale-row guard for corrected pit_success MOA files
    stale_ok = True
    stale_msgs: list[str] = []
    for rel in [
        "e0_no_source_year__target_pit_success_h2_clean_actionable.csv",
        "p1_percent_conservative_v1__target_pit_success_h2_clean_actionable.csv",
    ]:
        path = MOA_OOF_DIR / rel
        df = _read_csv(path)
        rows = len(df)
        if rows != 91473:
            stale_ok = False
            stale_msgs.append(f"{path.name} rows={rows}")
    checks["no_stale_moa_pit_success_rows"] = "PASS" if stale_ok else f"FAIL: {'; '.join(stale_msgs)}"

    strict_ok = True
    strict_msgs: list[str] = []
    for p in [
        BATCH_EVAL_DIR / "e0_no_source_year__target_pit_success_h2_clean_actionable.csv",
        BATCH_EVAL_DIR / "p1_percent_conservative_v1__target_pit_success_h2_clean_actionable.csv",
        MOA_EVAL_DIR / "e0_no_source_year__target_pit_success_h2_clean_actionable.csv",
        MOA_EVAL_DIR / "p1_percent_conservative_v1__target_pit_success_h2_clean_actionable.csv",
    ]:
        df = _read_csv(p)
        hz = int(pd.to_numeric(df.get("horizon", pd.Series([-1])), errors="coerce").fillna(-1).iloc[0])
        ws = str(df.get("window_semantics", pd.Series([""])).iloc[0])
        if hz != 2 or "strict_future_kplus1_to_kplusH" not in ws:
            strict_ok = False
            strict_msgs.append(f"{p.name}: horizon={hz} window={ws}")
    checks["pit_success_strict_future_kplus1"] = "PASS" if strict_ok else f"FAIL: {'; '.join(strict_msgs)}"

    no_match_ok = True
    nm_msgs: list[str] = []
    for p in [
        BATCH_EVAL_DIR / "p1_percent_conservative_v1__target_pit_success_h2_clean_actionable.csv",
        MOA_EVAL_DIR / "p1_percent_conservative_v1__target_pit_success_h2_clean_actionable.csv",
    ]:
        df = _read_csv(p)
        if "no_match_fp_rows" not in df.columns:
            no_match_ok = False
            nm_msgs.append(f"{p.name}: missing no_match_fp_rows")
            continue
        nm = int(pd.to_numeric(df["no_match_fp_rows"], errors="coerce").fillna(0).iloc[0])
        if nm <= 0:
            no_match_ok = False
            nm_msgs.append(f"{p.name}: no_match_fp_rows={nm}")
    checks["no_match_counted_as_fp"] = "PASS" if no_match_ok else f"FAIL: {'; '.join(nm_msgs)}"

    sde_diag = _read_csv(SDE_DIAG_SOURCE)
    cols = set(sde_diag.columns)
    naming_ok = ("matched_pit_success_rate" in cols) and ("precision" not in cols)
    checks["matched_pit_success_not_labelled_precision"] = "PASS" if naming_ok else "FAIL: naming mismatch in sde diagnostic table"

    contract_audit = _read_csv(TARGET_CONTRACT_AUDIT)
    residue_ok = True
    residue_msgs: list[str] = []
    if "target0_other_residue" in contract_audit.columns:
        for _, r in contract_audit.iterrows():
            residue = int(r.get("target0_other_residue", 0))
            if residue != 0:
                residue_ok = False
                residue_msgs.append(f"{r.get('dataset','?')} residue={residue}")
    checks["pit_success_unknown_filtered_from_trainable_negatives"] = "PASS" if residue_ok else f"FAIL: {'; '.join(residue_msgs)}"

    failed = [k for k, v in checks.items() if not str(v).startswith("PASS")]
    if failed:
        joined = "\n".join([f"{k}: {checks[k]}" for k in failed])
        raise RuntimeError(f"Hard checks failed:\n{joined}")
    return checks


def _build_pr_and_ap_assets(output_dir: Path) -> tuple[pd.DataFrame, list[Path]]:
    pr_sources = _load_pr_sources()
    ap_rows: list[dict[str, object]] = []
    outputs: list[Path] = []

    for target_col, title_stub, out_base in [
        (TARGET_ANY, "pit_any_h2 - PR ranking diagnostic", "pr_curve_pit_any_final"),
        (TARGET_SUCCESS, "pit_success_h2 - PR ranking diagnostic", "pr_curve_pit_success_final"),
    ]:
        fig, ax = plt.subplots(figsize=(10.5, 7.0))
        for system_key in ["batch_e0", "batch_p1", "moa_e0", "moa_p1"]:
            path = pr_sources[target_col][system_key]
            df = _read_csv(path)
            score_col = _score_column(df)
            y = pd.to_numeric(df["target_y"], errors="coerce").fillna(0).astype(int)
            s = pd.to_numeric(df[score_col], errors="coerce").fillna(0.0)
            ap = float(average_precision_score(y, s))
            precision, recall, _ = precision_recall_curve(y, s)
            label = f"{SYSTEM_LABELS[system_key]} (AP={ap:.3f})"
            ax.plot(recall, precision, lw=2.0, label=label)
            ap_rows.append(
                {
                    "target": "pit_any_h2" if target_col == TARGET_ANY else "pit_success_h2",
                    "system": SYSTEM_LABELS[system_key],
                    "AP": ap,
                }
            )

        ax.set_title(title_stub, fontsize=16, pad=18)
        fig.text(
            0.5,
            0.93,
            "row-level ranking diagnostic, not operational precision",
            ha="center",
            va="center",
            fontsize=11,
        )
        ax.set_xlabel("Recall", fontsize=12)
        ax.set_ylabel("Precision", fontsize=12)
        ax.tick_params(axis="both", labelsize=10)
        ax.grid(alpha=0.25)
        ax.legend(loc="lower left", fontsize=10, frameon=False)
        fig.tight_layout(rect=[0.02, 0.03, 0.98, 0.9])

        png = output_dir / f"{out_base}.png"
        pdf = output_dir / f"{out_base}.pdf"
        fig.savefig(png, dpi=400, bbox_inches="tight", pad_inches=0.12)
        fig.savefig(pdf, bbox_inches="tight", pad_inches=0.12)
        plt.close(fig)
        outputs.extend([png, pdf])

    ap_df = pd.DataFrame(ap_rows)
    ap_wide = (
        ap_df.pivot(index="target", columns="system", values="AP")
        .reset_index()
        .rename(
            columns={
                "Batch No-Year": "Batch No-Year AP",
                "Batch Percent": "Batch Percent AP",
                "MOA No-Year": "MOA No-Year AP",
                "MOA Percent": "MOA Percent AP",
            }
        )
    )

    def _interp(row: pd.Series) -> str:
        target = str(row["target"])
        vals = {
            "Batch No-Year": float(row.get("Batch No-Year AP", np.nan)),
            "Batch Percent": float(row.get("Batch Percent AP", np.nan)),
            "MOA No-Year": float(row.get("MOA No-Year AP", np.nan)),
            "MOA Percent": float(row.get("MOA Percent AP", np.nan)),
        }
        best = max(vals, key=vals.get)
        return f"{best} has highest AP for {target} in row-level ranking."

    ap_wide["interpretation"] = ap_wide.apply(_interp, axis=1)
    ap_wide = ap_wide[
        [
            "target",
            "Batch No-Year AP",
            "Batch Percent AP",
            "MOA No-Year AP",
            "MOA Percent AP",
            "interpretation",
        ]
    ]
    ap_csv = output_dir / "ap_summary_final.csv"
    ap_md = output_dir / "ap_summary_final.md"
    ap_wide.to_csv(ap_csv, index=False)
    _write_markdown_table(ap_md, "AP Summary Final", ap_wide, float_digits=6)
    outputs.extend([ap_csv, ap_md])
    return ap_wide, outputs


def _selected_thresholds_from_final_refresh() -> dict[str, dict[str, float]]:
    any_df = _read_csv(PIT_ANY_RECAP_SOURCE)
    success_df = _read_csv(PIT_SUCCESS_APPLES_SOURCE)

    out: dict[str, dict[str, float]] = {"pit_any_h2": {}, "pit_success_h2": {}}
    for system in [SYSTEM_BATCH_E0, SYSTEM_BATCH_P1, SYSTEM_MOA_E0, SYSTEM_MOA_P1]:
        a = any_df[any_df["System"].astype(str).eq(system)]
        if a.empty:
            raise RuntimeError(f"missing {system} in {PIT_ANY_RECAP_SOURCE}")
        thr_any = _extract_threshold(a.iloc[0].get("selected_threshold"))
        if not np.isfinite(thr_any):
            raise RuntimeError(f"non-numeric pit_any selected_threshold for {system}")
        out["pit_any_h2"][system] = float(thr_any)

        s = success_df[success_df["System"].astype(str).eq(system)]
        if s.empty:
            raise RuntimeError(f"missing {system} in {PIT_SUCCESS_APPLES_SOURCE}")
        thr_s = _extract_threshold(s.iloc[0].get("positive_call_definition"))
        if not np.isfinite(thr_s):
            raise RuntimeError(f"non-numeric pit_success threshold in positive_call_definition for {system}")
        out["pit_success_h2"][system] = float(thr_s)
    return out


def _plot_race_metric_5panel(
    *,
    df: pd.DataFrame,
    metric_col: str,
    target_short: str,
    out_png: Path,
    out_pdf: Path,
) -> None:
    fig, axes = plt.subplots(3, 2, figsize=(14.8, 10.8), sharex=True, sharey=True)
    axes_flat = list(axes.ravel())
    for idx, system in enumerate(SYSTEM_ORDER_RACE):
        ax = axes_flat[idx]
        sub = df[df["system"].astype(str).eq(system)].copy()
        if sub.empty:
            ax.text(0.5, 0.5, "No races", ha="center", va="center", fontsize=11, transform=ax.transAxes)
            ax.set_title(system, fontsize=12)
        else:
            sub.sort_values("race_index", inplace=True)
            ax.plot(sub["race_index"], sub[metric_col], color="#4C78A8", linewidth=1.0, alpha=0.6)
            ax.scatter(sub["race_index"], sub[metric_col], s=20, color="#4C78A8", alpha=0.9)
            avg = float(pd.to_numeric(sub[metric_col], errors="coerce").mean())
            ax.axhline(avg, color="#F58518", linestyle="--", linewidth=1.3)
            ax.set_title(f"{system} (n={len(sub)})", fontsize=12)
        if metric_col == "kappa":
            ax.axhline(0.0, color="black", linestyle=":", linewidth=1.0, alpha=0.7)
        ax.grid(alpha=0.22)
        ax.set_xlabel("Race index", fontsize=11)
        ax.set_ylabel(metric_col.upper(), fontsize=11)
        ax.tick_params(axis="both", labelsize=10)

    # Hide last empty slot in 3x2 for 5 panels.
    axes_flat[-1].axis("off")
    fig.suptitle(f"{target_short} — {metric_col.upper()} race-level diagnostic", fontsize=16, y=0.985)
    fig.tight_layout(rect=[0.02, 0.03, 0.98, 0.95])
    fig.savefig(out_png, dpi=400, bbox_inches="tight", pad_inches=0.12)
    fig.savefig(out_pdf, bbox_inches="tight", pad_inches=0.12)
    plt.close(fig)


def _build_race_diagnostics(output_dir: Path, min_support: int = 3) -> tuple[pd.DataFrame, pd.DataFrame, list[Path]]:
    """Recompute race-level diagnostics for all 5 systems from corrected artifacts."""
    universe = _load_universe(UNIVERSE_SOURCE)
    dataset = _filter_universe(pd.read_parquet(DATASET_SOURCE), universe)

    sde_suggestions = _filter_universe(_load_jsonls_with_year_prefix(SDE_SUGGESTION_PATHS), universe)
    if sde_suggestions.empty:
        raise RuntimeError("SDE pit_suggestions are empty after universe filter")
    sde_suggestions["label"] = (
        sde_suggestions["suggestionLabel"]
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace(" ", "_", regex=False)
    )
    pit_now_keys = set(
        (str(r), str(d), int(l))
        for r, d, l in sde_suggestions[
            sde_suggestions["label"].eq("PIT_NOW")
        ][["race", "driver", "lapNumber"]].itertuples(index=False, name=None)
    )

    ds_any = dataset.copy()
    ds_any[TARGET_ANY] = pd.to_numeric(ds_any[TARGET_ANY], errors="coerce").fillna(0).astype(int)
    ds_any["pred"] = [
        1 if (str(r), str(d), int(l)) in pit_now_keys else 0
        for r, d, l in ds_any[["race", "driver", "lapNumber"]].itertuples(index=False, name=None)
    ]

    ds_success = dataset[dataset[TARGET_SUCCESS_ELIG_COL].astype(bool)].copy()
    ds_success[TARGET_SUCCESS] = pd.to_numeric(ds_success[TARGET_SUCCESS], errors="coerce").fillna(0).astype(int)
    ds_success["pred"] = [
        1 if (str(r), str(d), int(l)) in pit_now_keys else 0
        for r, d, l in ds_success[["race", "driver", "lapNumber"]].itertuples(index=False, name=None)
    ]

    thresholds = _selected_thresholds_from_final_refresh()

    files = {
        "batch_e0_any": BATCH_OOF_DIR / f"e0_no_source_year__{TARGET_ANY}.csv",
        "batch_p1_any": BATCH_OOF_DIR / f"p1_percent_conservative_v1__{TARGET_ANY}.csv",
        "batch_e0_succ": BATCH_OOF_DIR / f"e0_no_source_year__{TARGET_SUCCESS}.csv",
        "batch_p1_succ": BATCH_OOF_DIR / f"p1_percent_conservative_v1__{TARGET_SUCCESS}.csv",
        "moa_e0_any": MOA_OOF_DIR / f"e0_no_source_year__{TARGET_ANY}.csv",
        "moa_p1_any": MOA_OOF_DIR / f"p1_percent_conservative_v1__{TARGET_ANY}.csv",
        "moa_e0_succ": MOA_OOF_DIR / f"e0_no_source_year__{TARGET_SUCCESS}.csv",
        "moa_p1_succ": MOA_OOF_DIR / f"p1_percent_conservative_v1__{TARGET_SUCCESS}.csv",
    }
    for name, path in files.items():
        if not path.exists():
            raise FileNotFoundError(f"missing required OOF for race diagnostics: {path}")
        if "phase2a" in str(path).lower():
            raise RuntimeError(f"phase2a fallback path detected in race diagnostics: {path}")

    # stale guard (strict requirement)
    for key in ["moa_e0_succ", "moa_p1_succ"]:
        n = len(pd.read_csv(files[key], usecols=["target_y"]))
        if n == 93623:
            raise RuntimeError(f"stale 93623-row MOA pit_success artifact detected: {files[key]}")
        if n != 91473:
            raise RuntimeError(f"unexpected MOA pit_success row count for {files[key]}: {n} (expected 91473)")

    def _pred_df(path: Path, thr: float, target_col: str) -> pd.DataFrame:
        d = _filter_universe(pd.read_csv(path), universe)
        d["pred"] = (pd.to_numeric(d["calibrated_proba"], errors="coerce").fillna(0.0) >= float(thr)).astype(int)
        return d.rename(columns={"target_y": target_col})

    race_rows: list[pd.DataFrame] = []

    # Flink Strategy Engine
    s_any = _confusion_metrics_by_race(ds_any, TARGET_ANY, "pred")
    s_any["system"] = SYSTEM_FLINK
    s_any["target"] = TARGET_ANY
    race_rows.append(s_any)
    s_succ = _confusion_metrics_by_race(ds_success, TARGET_SUCCESS, "pred")
    s_succ["system"] = SYSTEM_FLINK
    s_succ["target"] = TARGET_SUCCESS
    race_rows.append(s_succ)

    # Batch + MOA both profiles, selected thresholds from final_refresh outputs.
    systems_spec = [
        (SYSTEM_BATCH_E0, files["batch_e0_any"], TARGET_ANY, thresholds["pit_any_h2"][SYSTEM_BATCH_E0]),
        (SYSTEM_BATCH_P1, files["batch_p1_any"], TARGET_ANY, thresholds["pit_any_h2"][SYSTEM_BATCH_P1]),
        (SYSTEM_MOA_E0, files["moa_e0_any"], TARGET_ANY, thresholds["pit_any_h2"][SYSTEM_MOA_E0]),
        (SYSTEM_MOA_P1, files["moa_p1_any"], TARGET_ANY, thresholds["pit_any_h2"][SYSTEM_MOA_P1]),
        (SYSTEM_BATCH_E0, files["batch_e0_succ"], TARGET_SUCCESS, thresholds["pit_success_h2"][SYSTEM_BATCH_E0]),
        (SYSTEM_BATCH_P1, files["batch_p1_succ"], TARGET_SUCCESS, thresholds["pit_success_h2"][SYSTEM_BATCH_P1]),
        (SYSTEM_MOA_E0, files["moa_e0_succ"], TARGET_SUCCESS, thresholds["pit_success_h2"][SYSTEM_MOA_E0]),
        (SYSTEM_MOA_P1, files["moa_p1_succ"], TARGET_SUCCESS, thresholds["pit_success_h2"][SYSTEM_MOA_P1]),
    ]
    for system, path, target_col, thr in systems_spec:
        dd = _pred_df(path, float(thr), target_col)
        rr = _confusion_metrics_by_race(dd, target_col, "pred")
        rr["system"] = system
        rr["target"] = target_col
        race_rows.append(rr)

    race_metrics = pd.concat(race_rows, ignore_index=True)
    race_metrics["positives"] = pd.to_numeric(race_metrics["positives"], errors="coerce").fillna(0).astype(int)
    race_metrics["kappa"] = pd.to_numeric(race_metrics["kappa"], errors="coerce")
    race_metrics["gmean"] = pd.to_numeric(race_metrics["gmean"], errors="coerce")

    support_rows: list[dict[str, object]] = []
    metric_rows: list[dict[str, object]] = []
    outputs: list[Path] = []

    target_cfg = [
        ("pit_any_h2", TARGET_ANY, "pit_any"),
        ("pit_success_h2", TARGET_SUCCESS, "pit_success"),
    ]
    for target_short, target_col, target_file_stem in target_cfg:
        t_all = race_metrics[race_metrics["target"].astype(str).eq(target_col)].copy()
        t_f = t_all[t_all["positives"] >= int(min_support)].copy()
        total_races = int(t_all["race"].nunique())
        kept_races = int(t_f["race"].nunique())

        for system in SYSTEM_ORDER_RACE:
            ss = t_f[t_f["system"].astype(str).eq(system)].copy()
            support_rows.append(
                {
                    "target": target_short,
                    "system": system,
                    "races_total_target": total_races,
                    "races_kept_target": kept_races,
                    "races_kept_system": int(len(ss)),
                    "positives_min_filter": int(min_support),
                }
            )
            for metric in ["kappa", "gmean"]:
                vals = pd.to_numeric(ss[metric], errors="coerce")
                metric_rows.append(
                    {
                        "target": target_short,
                        "metric": metric,
                        "system": system,
                        "races_total_target": total_races,
                        "races_kept_target": kept_races,
                        "races_kept_system": int(len(ss)),
                        "positives_min_filter": int(min_support),
                        "mean_metric_system": float(vals.mean()) if len(vals) else np.nan,
                        "median_metric_system": float(vals.median()) if len(vals) else np.nan,
                        "min_metric_system": float(vals.min()) if len(vals) else np.nan,
                        "max_metric_system": float(vals.max()) if len(vals) else np.nan,
                    }
                )

        _plot_race_metric_5panel(
            df=t_f,
            metric_col="kappa",
            target_short=target_short,
            out_png=output_dir / f"race_level_kappa_{target_file_stem}.png",
            out_pdf=output_dir / f"race_level_kappa_{target_file_stem}.pdf",
        )
        _plot_race_metric_5panel(
            df=t_f,
            metric_col="gmean",
            target_short=target_short,
            out_png=output_dir / f"race_level_gmean_{target_file_stem}.png",
            out_pdf=output_dir / f"race_level_gmean_{target_file_stem}.pdf",
        )
        outputs.extend(
            [
                output_dir / f"race_level_kappa_{target_file_stem}.png",
                output_dir / f"race_level_kappa_{target_file_stem}.pdf",
                output_dir / f"race_level_gmean_{target_file_stem}.png",
                output_dir / f"race_level_gmean_{target_file_stem}.pdf",
            ]
        )

    support_df = pd.DataFrame(support_rows)
    metric_df = pd.DataFrame(metric_rows)
    support_csv = output_dir / "race_level_support_summary.csv"
    support_md = output_dir / "race_level_support_summary.md"
    metric_csv = output_dir / "race_level_metric_summary.csv"
    metric_md = output_dir / "race_level_metric_summary.md"
    support_df.to_csv(support_csv, index=False)
    metric_df.to_csv(metric_csv, index=False)
    _write_markdown_table(support_md, "Race Level Support Summary", support_df, float_digits=6)
    _write_markdown_table(metric_md, "Race Level Metric Summary", metric_df, float_digits=6)
    outputs.extend([support_csv, support_md, metric_csv, metric_md])

    # Compact terminal summary.
    print("\nRace diagnostics sources used:")
    print(f"- universe: {UNIVERSE_SOURCE}")
    print(f"- dataset: {DATASET_SOURCE}")
    print(f"- sde suggestions: {[str(p) for p in SDE_SUGGESTION_PATHS]}")
    print(f"- pit_any thresholds from: {PIT_ANY_RECAP_SOURCE}")
    print(f"- pit_success thresholds from: {PIT_SUCCESS_APPLES_SOURCE}")
    print("\nRaces kept by target/system:")
    print(support_df.to_string(index=False))
    med = metric_df.pivot_table(index=["target", "system"], columns="metric", values="median_metric_system", aggfunc="first").reset_index()
    print("\nMedian Kappa/G-Mean by target/system:")
    print(med.to_string(index=False))
    print("\nContract confirmation:")
    print("- no phase2a source: PASS")
    print("- no stale 93,623-row MOA pit_success artifacts: PASS")
    print("- pit_success strict contract via selected thresholds from final_refresh apples-to-apples: PASS")

    return support_df, metric_df, outputs


def _extract_threshold(value: object) -> float:
    text = str(value)
    m = re.search(r"[-+]?[0-9]*\.?[0-9]+", text)
    if not m:
        return float("nan")
    return float(m.group(0))


def _build_threshold_sensitivity_slide(output_dir: Path) -> tuple[pd.DataFrame, list[Path]]:
    src = _read_csv(PIT_SUCCESS_SENSITIVITY_SOURCE)
    src = src.rename(
        columns={
            "System": "system",
            "threshold/policy": "threshold_policy",
            "predicted_positives": "predicted positives",
            "strict_precision": "strict precision",
            "successful_event_coverage": "successful event coverage",
            "F0.5": "F0.5",
            "notes": "notes",
        }
    )
    src["threshold"] = src["threshold_policy"].map(_extract_threshold)
    src["notes"] = src["notes"].fillna("").astype(str)
    src["is_selected"] = src["threshold_policy"].astype(str).str.contains("selected", case=False, regex=False)
    src.loc[src["threshold_policy"].astype(str).str.contains("max F0\\.5|relaxed", case=False, regex=True), "notes"] = (
        src["notes"].astype(str) + "; diagnostic-only relaxed threshold"
    ).str.strip("; ").str.strip()

    out = src[
        [
            "system",
            "threshold",
            "predicted positives",
            "strict precision",
            "successful event coverage",
            "F0.5",
            "notes",
            "is_selected",
        ]
    ].copy()
    out["system_rank"] = out["system"].map({"Batch Percent": 0, "MOA Percent": 1}).fillna(99).astype(int)
    out["selected_rank"] = (~out["is_selected"]).astype(int)
    out.sort_values(by=["system_rank", "selected_rank", "threshold"], inplace=True)
    out.drop(columns=["system_rank", "selected_rank", "is_selected"], inplace=True)
    out_csv = output_dir / "pit_success_threshold_sensitivity_slide.csv"
    out_md = output_dir / "pit_success_threshold_sensitivity_slide.md"
    out.to_csv(out_csv, index=False)
    _write_markdown_table(out_md, "pit_success Threshold Sensitivity (Slide)", out, float_digits=6)
    return out, [out_csv, out_md]


def _build_manifest(
    output_dir: Path,
    assets: Iterable[Path],
    checks: dict[str, str],
    ap_source: Path,
    race_source: Path,
    sens_source: Path,
) -> Path:
    checks_blob = "; ".join([f"{k}={v}" for k, v in checks.items()])
    rows: list[dict[str, object]] = []
    for p in assets:
        name = p.name
        source_csv = ""
        contract_notes = "slide asset from corrected current final_refresh/OOF artifacts"
        if name.startswith("pr_curve_") or name.startswith("ap_summary_"):
            source_csv = f"{BATCH_OOF_DIR}; {MOA_OOF_DIR}; {ap_source}"
            contract_notes = "row-level PR/AP diagnostic; not operational precision"
        elif name.startswith("kappa_") or name.startswith("gmean_") or name.startswith("race_level_"):
            source_csv = str(race_source)
            contract_notes = "race-level diagnostics with support filter positives>=3"
        elif name.startswith("pit_success_threshold_sensitivity_slide"):
            source_csv = str(sens_source)
            contract_notes = "strict operational pit_success contract; relaxed rows marked diagnostic-only"
        rows.append(
            {
                "asset_name": name,
                "path": str(p),
                "source_csv": source_csv,
                "contract_notes": contract_notes,
                "stale_check_status": checks_blob,
            }
        )
    manifest = pd.DataFrame(rows)
    out_path = output_dir / "final_slide_assets_manifest.csv"
    manifest.to_csv(out_path, index=False)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build remaining final slide assets from final_refresh sources only.")
    parser.add_argument("--output-dir", default=str(FINAL_REFRESH_DIR), help="output folder")
    args = parser.parse_args()

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    checks = _hard_checks()
    all_outputs: list[Path] = []

    pr_outputs, _ = build_final_refresh_pr_assets(output_dir)
    all_outputs.extend(pr_outputs)

    _, _, race_outputs = _build_race_diagnostics(output_dir, min_support=3)
    all_outputs.extend(race_outputs)

    _, sens_outputs = _build_threshold_sensitivity_slide(output_dir)
    all_outputs.extend(sens_outputs)

    manifest_path = _build_manifest(
        output_dir,
        all_outputs,
        checks,
        ap_source=output_dir / "ap_summary_final.csv",
        race_source=output_dir / "race_level_metric_summary.csv",
        sens_source=PIT_SUCCESS_SENSITIVITY_SOURCE,
    )
    all_outputs.append(manifest_path)

    print("Generated slide assets:")
    for p in all_outputs:
        print(f"- {p}")
    print("\nHard checks:")
    for k, v in checks.items():
        print(f"- {k}: {v}")


if __name__ == "__main__":
    main()
