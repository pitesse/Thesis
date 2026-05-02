"""Build a stratified manual error-taxonomy template (fixed rows per model).

Creates a CSV with exactly N rows for each of SDE, Batch, and MOA from the
same error pool used by analyze_error_taxonomy:
- excluded_no_match: EXCLUDED + NO_MATCH_WITHIN_HORIZON
- scored_fp: outcome_class == 0
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

try:
    from lib.run_catalog import racewise_suffix_for
except ImportError:
    from ml_pipeline.lib.run_catalog import racewise_suffix_for  # type: ignore


COLUMNS = [
    "model",
    "case_origin",
    "subtype_auto",
    "race",
    "driver",
    "suggestion_lap",
    "outcome_class",
    "exclusion_reason",
    "nearest_future_pit_distance",
    "failure_type_manual",
    "context_source",
    "notes",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build stratified manual taxonomy template")
    parser.add_argument("--reports-dir", type=Path, required=True)
    parser.add_argument("--suffix", required=True)
    parser.add_argument("--horizon", type=int, default=2)
    parser.add_argument("--rows-per-model", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--pool-mode",
        choices=["mixed", "true_fp_only"],
        default="mixed",
        help="mixed = EXCLUDED no-match + scored FP; true_fp_only = scored FP only",
    )
    parser.add_argument(
        "--batch-source",
        choices=["pretrain", "racewise"],
        default="pretrain",
        help="Batch comparator source. Use racewise when true FP pool in pretrain is too small.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=None,
        help="Defaults to reports-dir/error_taxonomy_manual_template_stratified.csv",
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


def _near_miss_subtype(distance: float | int | None, horizon: int) -> str:
    if pd.isna(distance):
        return "no_future"
    value = int(distance)
    if horizon + 1 <= value <= horizon + 3:
        return "near_miss_3_5"
    if value >= horizon + 4:
        return "far_miss_6_plus"
    return "unexpected_within_horizon"


def _prepare_comparator(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"race", "driver", "suggestion_lap", "outcome_class", "exclusion_reason", "nearest_future_pit_distance"}
    missing = [c for c in required if c not in frame.columns]
    if missing:
        raise ValueError(f"comparator missing columns: {missing}")

    out = frame.copy()
    out["race"] = out["race"].astype(str)
    out["driver"] = out["driver"].astype(str)
    out["suggestion_lap"] = pd.to_numeric(out["suggestion_lap"], errors="coerce").fillna(-1).astype(int)
    out["outcome_class"] = out["outcome_class"].astype(str)
    out["exclusion_reason"] = out["exclusion_reason"].astype(str)
    out["nearest_future_pit_distance"] = pd.to_numeric(out["nearest_future_pit_distance"], errors="coerce")
    return out


def _collect_pool(
    frame: pd.DataFrame,
    model: str,
    horizon: int,
    pool_mode: str,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    if pool_mode == "mixed":
        excluded = frame[
            (frame["outcome_class"] == "EXCLUDED")
            & (frame["exclusion_reason"] == "NO_MATCH_WITHIN_HORIZON")
        ].copy()
        for _, row in excluded.iterrows():
            rows.append(
                {
                    "model": model,
                    "case_origin": "excluded_no_match",
                    "subtype_auto": _near_miss_subtype(row["nearest_future_pit_distance"], horizon),
                    "race": row["race"],
                    "driver": row["driver"],
                    "suggestion_lap": int(row["suggestion_lap"]),
                    "outcome_class": row["outcome_class"],
                    "exclusion_reason": row["exclusion_reason"],
                    "nearest_future_pit_distance": row["nearest_future_pit_distance"],
                    "failure_type_manual": "",
                    "context_source": "",
                    "notes": "",
                }
            )

    fps = frame[frame["outcome_class"] == "0"].copy()
    for _, row in fps.iterrows():
        rows.append(
            {
                "model": model,
                "case_origin": "scored_fp",
                "subtype_auto": "fp_scored",
                "race": row["race"],
                "driver": row["driver"],
                "suggestion_lap": int(row["suggestion_lap"]),
                "outcome_class": row["outcome_class"],
                "exclusion_reason": row["exclusion_reason"],
                "nearest_future_pit_distance": row["nearest_future_pit_distance"],
                "failure_type_manual": "",
                "context_source": "",
                "notes": "",
            }
        )

    return pd.DataFrame(rows, columns=COLUMNS)


def main() -> None:
    args = parse_args()
    if args.rows_per_model < 1:
        raise ValueError("--rows-per-model must be >= 1")

    reports_dir = args.reports_dir.resolve()
    suffix = args.suffix
    racewise_suffix = racewise_suffix_for(suffix)

    if args.output_csv is None:
        output_csv = reports_dir / "error_taxonomy_manual_template_stratified.csv"
    else:
        output_csv = args.output_csv

    sde_df = _prepare_comparator(
        _load_csv(_resolve_sde_comparator(reports_dir, suffix, racewise_suffix), "SDE comparator")
    )
    batch_suffix = suffix if args.batch_source == "pretrain" else racewise_suffix
    batch_df = _prepare_comparator(
        _load_csv(reports_dir / f"ml_comparator_{batch_suffix}.csv", f"Batch comparator ({args.batch_source})")
    )
    moa_df = _prepare_comparator(_load_csv(_resolve_moa_comparator(reports_dir, suffix), "MOA comparator"))

    pools = {
        "SDE": _collect_pool(sde_df, "SDE", args.horizon, args.pool_mode),
        "Batch": _collect_pool(batch_df, "Batch", args.horizon, args.pool_mode),
        "MOA": _collect_pool(moa_df, "MOA", args.horizon, args.pool_mode),
    }

    sampled_parts: list[pd.DataFrame] = []
    for model in ("SDE", "Batch", "MOA"):
        pool = pools[model]
        if len(pool) < args.rows_per_model:
            raise ValueError(
                f"insufficient pool size for {model}: requested={args.rows_per_model}, available={len(pool)}"
            )
        sampled = pool.sample(n=args.rows_per_model, random_state=args.seed, replace=False)
        sampled_parts.append(sampled)

    out = pd.concat(sampled_parts, ignore_index=True)
    out = out.sort_values(by=["model", "case_origin", "race", "driver", "suggestion_lap"]).reset_index(drop=True)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_csv, index=False)

    diag_rows = []
    for model, pool in pools.items():
        diag_rows.append(
            {
                "model": model,
                "pool_size": int(len(pool)),
                "requested_rows": int(args.rows_per_model),
                "sampled_rows": int((out["model"] == model).sum()),
                "excluded_no_match_in_pool": int((pool["case_origin"] == "excluded_no_match").sum()),
                "scored_fp_in_pool": int((pool["case_origin"] == "scored_fp").sum()),
            }
        )
    diag_df = pd.DataFrame(diag_rows)
    diag_path = output_csv.with_name(output_csv.stem + "_diagnostics.csv")
    diag_df.to_csv(diag_path, index=False)

    print("=== STRATIFIED TEMPLATE GENERATED ===")
    print(f"reports dir              : {reports_dir}")
    print(f"suffix                   : {suffix}")
    print(f"pool mode                : {args.pool_mode}")
    print(f"batch source             : {args.batch_source}")
    print(f"rows per model           : {args.rows_per_model}")
    print(f"seed                     : {args.seed}")
    print(f"output csv               : {output_csv}")
    print(f"diagnostics csv          : {diag_path}")
    for _, row in diag_df.iterrows():
        print(
            f"pool[{row['model']}]={int(row['pool_size'])}, "
            f"sampled={int(row['sampled_rows'])}, "
            f"excluded_no_match={int(row['excluded_no_match_in_pool'])}, "
            f"scored_fp={int(row['scored_fp_in_pool'])}"
        )


if __name__ == "__main__":
    main()
