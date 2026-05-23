"""Audit Phase 2B delivery coverage for professor-facing handoff."""

from __future__ import annotations

import argparse
import re
import struct
from pathlib import Path
from typing import Any

import pandas as pd


IMAGE_RE = re.compile(r"!\[[^\]]*\]\(([^)]+)\)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit Phase 2B delivery artifacts and figure coverage.")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument(
        "--figures-dir",
        default="data_lake/reports/phase2b_presentation_figures",
    )
    parser.add_argument(
        "--batch-root",
        default="data_lake/reports/ml_phase2b_dual_contract_2022_2025",
    )
    parser.add_argument(
        "--sml-root",
        default="data_lake/reports/sml_phase2b_dual_contract_2022_2025",
    )
    parser.add_argument(
        "--sde-aggregate-csv",
        default="data_lake/reports/final_sde_c6_cfg120_fixed_raw_clean_aggregate_2022_2025.csv",
    )
    parser.add_argument(
        "--sde-per-year-csv",
        default="data_lake/reports/final_sde_c6_cfg120_fixed_raw_clean_per_year_2022_2025.csv",
    )
    parser.add_argument(
        "--sde-2025-csv",
        default="data_lake/reports/sde_dual_contract_raw_vs_clean_2025_with_deltas.csv",
    )
    parser.add_argument(
        "--reports",
        nargs="*",
        default=["results_comparison.md", "results_comparison_newest.md"],
    )
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--min-png-width", type=int, default=1800)
    parser.add_argument("--min-png-height", type=int, default=1200)
    parser.add_argument(
        "--output-csv",
        default="data_lake/reports/phase2b_presentation_figures/phase2b_delivery_audit.csv",
    )
    parser.add_argument(
        "--output-md",
        default="data_lake/reports/phase2b_presentation_figures/phase2b_delivery_audit.md",
    )
    return parser.parse_args()


def _add(rows: list[dict[str, Any]], *, item: str, typ: str, status: str, expected: str, resolved: str = "", note: str = "") -> None:
    rows.append(
        {
            "item": item,
            "type": typ,
            "status": status,
            "expected_path": expected,
            "resolved_path": resolved,
            "note": note,
        }
    )


def _exists(path: Path) -> bool:
    try:
        return path.exists()
    except Exception:
        return False


def _resolve_image_path(repo_root: Path, report_path: Path, image_ref: str) -> Path | None:
    image_ref = image_ref.strip()
    if not image_ref or image_ref.startswith("http://") or image_ref.startswith("https://"):
        return None
    candidates = [
        repo_root / image_ref,
        report_path.parent / image_ref,
        repo_root / "data_lake" / "reports" / image_ref,
    ]
    for c in candidates:
        if c.exists():
            return c
    return None


def _status_from_manifest(stem: str, manifest: pd.DataFrame) -> tuple[str, str]:
    if manifest.empty or "figure" not in manifest.columns:
        return "", ""
    sub = manifest[manifest["figure"].astype(str).eq(stem)]
    if sub.empty:
        return "", ""
    row = sub.iloc[0]
    return str(row.get("status", "")), str(row.get("note", ""))


def _manifest_row(stem: str, manifest: pd.DataFrame) -> pd.Series | None:
    if manifest.empty or "figure" not in manifest.columns:
        return None
    sub = manifest[manifest["figure"].astype(str).eq(stem)]
    if sub.empty:
        return None
    return sub.iloc[0]


def _parse_formats(value: Any) -> list[str]:
    text = str(value or "").strip().lower()
    if not text:
        return ["png", "pdf"]
    out = [t.strip() for t in text.split(",") if t.strip()]
    return out or ["png", "pdf"]


def _png_dimensions(path: Path) -> tuple[int, int] | None:
    # PNG header: bytes 16..23 contain width/height (big-endian uint32).
    try:
        with path.open("rb") as fh:
            header = fh.read(24)
        if len(header) < 24 or header[:8] != b"\x89PNG\r\n\x1a\n":
            return None
        width = struct.unpack(">I", header[16:20])[0]
        height = struct.unpack(">I", header[20:24])[0]
        return int(width), int(height)
    except Exception:
        return None


def _check_required_files(rows: list[dict[str, Any]], repo_root: Path, batch_root: Path, sml_root: Path, args: argparse.Namespace) -> None:
    required = {
        "batch_recommended": batch_root / "recommended/phase2b_recommended_operating_points.csv",
        "batch_compact": batch_root / "recommended/phase2b_e0_vs_p1_canonical_compact.csv",
        "batch_frontier": batch_root / "frontier/phase2b_threshold_frontier_compact.csv",
        "batch_by_year": batch_root / "by_year/phase2b_threshold_frontier_by_year.csv",
        "batch_report_md": batch_root / "phase2b_threshold_frontier_report.md",
        "sml_recommended": sml_root / "recommended/phase2b_recommended_operating_points.csv",
        "sml_compact": sml_root / "recommended/phase2b_e0_vs_p1_canonical_compact.csv",
        "sml_frontier": sml_root / "frontier/phase2b_threshold_frontier_compact.csv",
        "sml_by_year": sml_root / "by_year/phase2b_threshold_frontier_by_year.csv",
        "sml_report_md": sml_root / "phase2b_threshold_frontier_report.md",
        "sml_matrix": sml_root / "matrix_sde_truth/sml_phase2b_matrix_compact.csv",
        "sml_preq": sml_root / "prequential/sml_phase2b_preq_summary.csv",
        "sde_aggregate": repo_root / args.sde_aggregate_csv,
        "sde_per_year": repo_root / args.sde_per_year_csv,
        "sde_2025": repo_root / args.sde_2025_csv,
    }
    for key, path in required.items():
        if _exists(path):
            _add(rows, item=key, typ="csv", status="PASS", expected=str(path), resolved=str(path))
        else:
            _add(rows, item=key, typ="csv", status="FAIL", expected=str(path), note="required artifact missing")


def _expected_figures() -> list[str]:
    return [
        "universe_alignment_bar",
        "phase2b_clean_actionable_precision_recall_f05",
        "phase2b_clean_actionable_scored_tp_fp",
        "phase2b_e0_vs_p1_delta_clean_actionable",
        "phase2b_batch_pr_curves_clean_actionable",
        "phase2b_batch_pr_curves_by_year_clean_actionable",
        "phase2b_sml_pr_curves_clean_actionable",
        "phase2b_sml_pr_curves_by_year_clean_actionable",
        "phase2b_threshold_frontier_batch_clean_actionable",
        "phase2b_threshold_frontier_sml_clean_actionable",
        "phase2b_calibration_batch_clean_actionable",
        "phase2b_calibration_sml_clean_actionable",
        "phase2b_pr_gain_batch_clean_actionable",
        "phase2b_pr_gain_sml_clean_actionable",
        "phase2b_decision_curve_batch_clean_actionable",
        "phase2b_decision_curve_sml_clean_actionable",
        "phase2b_per_year_precision_recall_f05_clean_actionable",
        "phase2b_temporal_drift_by_race_clean_actionable",
        "phase2b_temporal_drift_by_race_clean_actionable_slide",
        "phase2b_sml_hard_decision_summary",
        "phase2b_preq_accuracy_kappa",
        "phase2b_raw_precision_recall_f05",
        "phase2b_clean_dry_strategy_precision_recall_f05",
        "phase2b_raw_e0_vs_p1_delta",
        "phase2b_clean_dry_strategy_e0_vs_p1_delta",
        "phase2b_batch_e0_shap_global_bar",
        "phase2b_batch_p1_shap_global_bar",
        "phase2b_batch_e0_shap_beeswarm",
        "phase2b_batch_p1_shap_beeswarm",
        "phase2b_batch_shap_top_feature_comparison_e0_p1",
        "phase2b_sml_e0_surrogate_shap_global_bar",
        "phase2b_sml_p1_surrogate_shap_global_bar",
        "phase2b_sml_surrogate_shap_top_feature_comparison_e0_p1",
        "phase2b_sml_surrogate_fidelity_summary",
        "phase2b_error_consensus_upset_clean_actionable",
        "phase2b_error_near_miss_distribution_clean_actionable",
        "phase2b_error_batch_fp_tp_feature_profiles_clean_actionable",
        "phase2b_error_sml_fp_tp_feature_profiles_clean_actionable",
    ]


def _check_figures(rows: list[dict[str, Any]], figures_dir: Path, args: argparse.Namespace) -> pd.DataFrame:
    manifest_path = figures_dir / "phase2b_figures_manifest.csv"
    manifest = pd.DataFrame()
    if manifest_path.exists():
        try:
            manifest = pd.read_csv(manifest_path)
        except Exception:
            manifest = pd.DataFrame()

    for stem in _expected_figures():
        row = _manifest_row(stem, manifest)
        formats = _parse_formats(row.get("formats") if row is not None else "")
        expected_paths = [figures_dir / f"{stem}.{fmt}" for fmt in formats]
        missing = [p for p in expected_paths if not p.exists()]
        if not missing:
            note = ""
            png_path = figures_dir / f"{stem}.png"
            if png_path.exists():
                dims = _png_dimensions(png_path)
                if dims is not None:
                    w, h = dims
                    if (w < int(args.min_png_width)) and (h < int(args.min_png_height)):
                        note = f"png dims below recommended minimum ({w}x{h})"
                        _add(
                            rows,
                            item=f"{stem}__png_dimensions",
                            typ="figure",
                            status="WARN",
                            expected=f">={args.min_png_width}w or >={args.min_png_height}h",
                            resolved=f"{w}x{h}",
                            note="presentation readability may be reduced",
                        )
            _add(
                rows,
                item=stem,
                typ="figure",
                status="PASS",
                expected="; ".join(str(p) for p in expected_paths),
                resolved="; ".join(str(p) for p in expected_paths),
                note=note,
            )
            continue

        m_status, m_note = _status_from_manifest(stem, manifest)
        if m_status.upper() == "SKIP":
            _add(
                rows,
                item=stem,
                typ="figure",
                status="SKIP",
                expected="; ".join(str(p) for p in expected_paths),
                note=f"manifest skip: {m_note}",
            )
        elif m_status.upper() == "WARN":
            _add(
                rows,
                item=stem,
                typ="figure",
                status="WARN",
                expected="; ".join(str(p) for p in expected_paths),
                note=f"manifest warn: {m_note}",
            )
        else:
            _add(
                rows,
                item=stem,
                typ="figure",
                status="FAIL",
                expected="; ".join(str(p) for p in expected_paths),
                note="missing required figure formats and no manifest SKIP",
            )

    return manifest


def _check_phase2b_source_integrity(rows: list[dict[str, Any]], manifest: pd.DataFrame, figures_dir: Path) -> None:
    if manifest.empty:
        _add(
            rows,
            item="phase2b_source_integrity_manifest",
            typ="source_integrity",
            status="FAIL",
            expected=str(figures_dir / "phase2b_figures_manifest.csv"),
            note="missing/empty manifest for source integrity checks",
        )
        return

    target_figs = {
        "phase2b_batch_pr_curves_clean_actionable",
        "phase2b_batch_pr_curves_by_year_clean_actionable",
        "phase2b_calibration_batch_clean_actionable",
        "phase2b_pr_gain_batch_clean_actionable",
        "phase2b_decision_curve_batch_clean_actionable",
    }
    allow_phrase = "Phase2A OOF reused intentionally for Phase2B Batch probability diagnostics"
    required_run_ids = {
        "e0_no_source_year__target_pit_any_h2_clean_actionable.csv",
        "e0_no_source_year__target_pit_success_h2_clean_actionable.csv",
        "p1_percent_conservative_v1__target_pit_any_h2_clean_actionable.csv",
        "p1_percent_conservative_v1__target_pit_success_h2_clean_actionable.csv",
    }

    for fig in sorted(target_figs):
        sub = manifest[manifest["figure"].astype(str).eq(fig)]
        if sub.empty:
            _add(rows, item=f"{fig}:source_integrity", typ="source_integrity", status="FAIL", expected=fig, note="figure missing from manifest")
            continue
        r = sub.iloc[0]
        source = str(r.get("source_artifact", ""))
        note = str(r.get("note", ""))
        status = str(r.get("status", ""))
        if status.upper() != "PASS":
            _add(rows, item=f"{fig}:source_integrity", typ="source_integrity", status="SKIP", expected=source, note=f"figure status={status}")
            continue

        if "ml_phase2a" in source:
            if allow_phrase not in note:
                _add(
                    rows,
                    item=f"{fig}:source_integrity",
                    typ="source_integrity",
                    status="FAIL",
                    expected=source,
                    note="phase2a source used without explicit reuse caveat",
                )
                continue
            oof_dir = Path(source)
            missing = sorted([rid for rid in required_run_ids if not (oof_dir / rid).exists()])
            if missing:
                _add(
                    rows,
                    item=f"{fig}:source_integrity",
                    typ="source_integrity",
                    status="FAIL",
                    expected=source,
                    note=f"phase2a fallback run-id validation failed; missing={missing}",
                )
                continue
            _add(
                rows,
                item=f"{fig}:source_integrity",
                typ="source_integrity",
                status="PASS",
                expected=source,
                resolved=source,
                note="phase2a validated OOF probability-diagnostic source explicitly documented and run-id validated",
            )
        else:
            _add(
                rows,
                item=f"{fig}:source_integrity",
                typ="source_integrity",
                status="PASS",
                expected=source,
                resolved=source,
                note="phase2b/native non-phase2a source",
            )


def _check_markdown_images(rows: list[dict[str, Any]], repo_root: Path, reports: list[str]) -> None:
    for report_rel in reports:
        report = repo_root / report_rel
        is_legacy_report = Path(report_rel).name == "results_comparison.md"
        if not report.exists():
            _add(rows, item=f"markdown:{report_rel}", typ="markdown_image", status="SKIP", expected=str(report), note="report file missing")
            continue

        text = report.read_text(encoding="utf-8", errors="ignore")
        matches = IMAGE_RE.findall(text)
        if not matches:
            _add(rows, item=f"markdown:{report_rel}", typ="markdown_image", status="WARN", expected=str(report), note="no markdown image links found")
            continue

        for img_ref in matches:
            resolved = _resolve_image_path(repo_root, report, img_ref)
            if resolved is None:
                status = "SKIP" if is_legacy_report else "FAIL"
                _add(
                    rows,
                    item=f"{report_rel}:{img_ref}",
                    typ="markdown_image",
                    status=status,
                    expected=img_ref,
                    note=(
                        "unresolved image path (legacy report, non-blocking for Phase 2B)"
                        if is_legacy_report
                        else "unresolved image path"
                    ),
                )
            else:
                _add(
                    rows,
                    item=f"{report_rel}:{img_ref}",
                    typ="markdown_image",
                    status="PASS",
                    expected=img_ref,
                    resolved=str(resolved),
                )


def _check_sml_score_quality(rows: list[dict[str, Any]], sml_root: Path) -> str:
    oof_dir = sml_root / "oof"
    if not oof_dir.exists():
        _add(rows, item="sml_oof_score_quality", typ="score_quality", status="FAIL", expected=str(oof_dir), note="SML OOF directory missing")
        return "unknown"

    mode = "continuous"
    for path in sorted(oof_dir.glob("*.csv")):
        try:
            raw = pd.read_csv(path, usecols=["raw_proba"])["raw_proba"]
            raw = pd.to_numeric(raw, errors="coerce")
            uniq = int(raw.dropna().nunique())
            min_v = float(raw.min()) if raw.notna().any() else float("nan")
            max_v = float(raw.max()) if raw.notna().any() else float("nan")
            run_mode = "hard_decision_only" if uniq <= 2 else "continuous_score"
            if uniq <= 2:
                mode = "hard_decision_only"
            _add(
                rows,
                item=path.stem,
                typ="score_quality",
                status="PASS",
                expected=str(path),
                resolved=str(path),
                note=f"mode={run_mode}; raw_proba_unique={uniq}; min={min_v}; max={max_v}",
            )
        except Exception as exc:
            _add(
                rows,
                item=path.stem,
                typ="score_quality",
                status="WARN",
                expected=str(path),
                note=f"could not parse raw_proba: {exc}",
            )

    # Recommended CSV should carry score-quality fields.
    rec = sml_root / "recommended/phase2b_recommended_operating_points.csv"
    if rec.exists():
        try:
            df = pd.read_csv(rec, nrows=5)
            needed_any = {"score_frontier_quality", "score_unique_count", "score_is_hard_decision"}
            found = needed_any.intersection(set(df.columns))
            if found:
                _add(
                    rows,
                    item="sml_recommended_score_fields",
                    typ="score_quality",
                    status="PASS",
                    expected=str(rec),
                    resolved=str(rec),
                    note=f"found score fields: {sorted(found)}",
                )
            else:
                _add(
                    rows,
                    item="sml_recommended_score_fields",
                    typ="score_quality",
                    status="WARN",
                    expected=str(rec),
                    note="missing score quality fields in recommended CSV",
                )
        except Exception as exc:
            _add(rows, item="sml_recommended_score_fields", typ="score_quality", status="WARN", expected=str(rec), note=f"failed reading csv: {exc}")
    else:
        _add(rows, item="sml_recommended_score_fields", typ="score_quality", status="FAIL", expected=str(rec), note="missing recommended CSV")

    return mode


def _check_report_sections(rows: list[dict[str, Any]], repo_root: Path, sml_mode: str) -> None:
    report = repo_root / "results_comparison_newest.md"
    if not report.exists():
        _add(rows, item="results_comparison_newest.md", typ="report_section", status="FAIL", expected=str(report), note="master report missing")
        return

    text = report.read_text(encoding="utf-8", errors="ignore")
    checks = {
        "batch_phase2b_section": "Batch ML Phase 2B",
        "sml_phase2b_section": "SML/MOA Phase 2B",
        "figure_pack_section": "Figure Pack / Delivery Artifacts",
        "batch_shap_caveat": "Batch direct-SHAP",
        "sml_surrogate_caveat": "SML/MOA surrogate-SHAP",
    }
    for item, token in checks.items():
        status = "PASS" if token in text else "FAIL"
        _add(rows, item=item, typ="report_section", status=status, expected=str(report), note=f"token='{token}'")

    if sml_mode == "hard_decision_only":
        token = "hard-decision"
        status = "PASS" if token in text.lower() else "FAIL"
        _add(rows, item="sml_hard_decision_caveat", typ="report_section", status=status, expected=str(report), note="required when SML scores are hard-decision")
    elif sml_mode == "continuous":
        token = "continuous"
        status = "PASS" if token in text.lower() else "WARN"
        _add(rows, item="sml_continuous_score_note", typ="report_section", status=status, expected=str(report), note="should mention continuous vote-score availability")
    else:
        _add(rows, item="sml_score_mode_note", typ="report_section", status="WARN", expected=str(report), note="could not infer SML score mode")


def _write_md(path: Path, frame: pd.DataFrame) -> None:
    lines = [
        "# Phase 2B Delivery Audit",
        "",
        "| item | type | status | expected_path | resolved_path | note |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for _, r in frame.iterrows():
        lines.append(
            "| {item} | {typ} | {status} | `{expected}` | `{resolved}` | {note} |".format(
                item=str(r.get("item", "")),
                typ=str(r.get("type", "")),
                status=str(r.get("status", "")),
                expected=str(r.get("expected_path", "")),
                resolved=str(r.get("resolved_path", "")),
                note=str(r.get("note", "")),
            )
        )

    summary = frame.groupby("status", dropna=False).size().to_dict()
    lines.extend(["", "## Summary", ""])
    for key in ["PASS", "WARN", "SKIP", "FAIL"]:
        lines.append(f"- {key}: {int(summary.get(key, 0))}")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    figures_dir = (repo_root / args.figures_dir).resolve()
    batch_root = (repo_root / args.batch_root).resolve()
    sml_root = (repo_root / args.sml_root).resolve()

    rows: list[dict[str, Any]] = []
    _check_required_files(rows, repo_root, batch_root, sml_root, args)
    manifest = _check_figures(rows, figures_dir, args)
    _check_phase2b_source_integrity(rows, manifest, figures_dir)
    _check_markdown_images(rows, repo_root, args.reports)
    sml_mode = _check_sml_score_quality(rows, sml_root)
    _check_report_sections(rows, repo_root, sml_mode)

    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame = frame.sort_values(["type", "status", "item"], kind="mergesort").reset_index(drop=True)

    out_csv = (repo_root / args.output_csv).resolve()
    out_md = (repo_root / args.output_md).resolve()
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(out_csv, index=False)
    _write_md(out_md, frame)

    fail_count = int((frame["status"] == "FAIL").sum()) if not frame.empty else 0
    warn_count = int((frame["status"] == "WARN").sum()) if not frame.empty else 0

    print("=== PHASE2B DELIVERY AUDIT ===")
    print(f"csv : {out_csv}")
    print(f"md  : {out_md}")
    print(f"PASS={int((frame['status'] == 'PASS').sum()) if not frame.empty else 0} "
          f"WARN={warn_count} SKIP={int((frame['status'] == 'SKIP').sum()) if not frame.empty else 0} FAIL={fail_count}")

    if fail_count > 0:
        raise SystemExit(1)
    if args.strict and warn_count > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
