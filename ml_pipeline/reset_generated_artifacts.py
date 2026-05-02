"""Hard-reset utility for generated artifacts (dry-run by default)."""

from __future__ import annotations

import argparse
from pathlib import Path
import shutil


DEFAULT_DATA_LAKE = Path("data_lake")
SINK_DIRS = [
    "pit_evals",
    "pit_suggestions",
    "tire_drops",
    "lift_coast",
    "drop_zones",
    "ml_features",
]


def _collect_paths(data_lake: Path) -> list[Path]:
    candidates: list[Path] = []

    # Sink contents and auxiliary runtime dirs
    for rel in SINK_DIRS + ["checkpoints", "debug_alerts"]:
        p = data_lake / rel
        if p.exists():
            for child in p.iterdir():
                candidates.append(child)

    # Consolidated JSONL artifacts (season/merged/single run)
    patterns = [
        "pit_evals_*.jsonl",
        "pit_suggestions_*.jsonl",
        "tire_drops_*.jsonl",
        "lift_coast_*.jsonl",
        "drop_zones_*.jsonl",
        "ml_features_*.jsonl",
    ]
    for pattern in patterns:
        candidates.extend(sorted(data_lake.glob(pattern)))

    # Prepared training datasets
    candidates.extend(sorted(data_lake.glob("ml_training_dataset_*.parquet")))
    candidates.extend(sorted(data_lake.glob("ml_training_dataset_*.csv")))

    # Model bundles and derived model outputs, preserving tools/moa.jar
    models_dir = data_lake / "models"
    if models_dir.exists():
        for child in models_dir.iterdir():
            candidates.append(child)

    # All generated reports content
    reports_dir = data_lake / "reports"
    if reports_dir.exists():
        for child in reports_dir.iterdir():
            candidates.append(child)

    # Replay manifests are generated artifacts
    manifests_dir = data_lake / "replay_manifests"
    if manifests_dir.exists():
        for child in manifests_dir.iterdir():
            candidates.append(child)

    # Nested accidental output root produced by bad relative paths
    nested_reports_root = data_lake / "data_lake"
    if nested_reports_root.exists():
        candidates.append(nested_reports_root)

    # Safety filter: never touch moa.jar
    protected = {(data_lake / "tools" / "moa.jar").resolve()}
    unique: list[Path] = []
    seen: set[Path] = set()
    for path in candidates:
        try:
            resolved = path.resolve()
        except FileNotFoundError:
            continue
        if resolved in protected:
            continue
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(path)

    return sorted(unique, key=lambda p: str(p))


def _delete_path(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="reset generated artifacts in data_lake")
    parser.add_argument("--data-lake", default=str(DEFAULT_DATA_LAKE), help="data_lake root")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="apply deletions (default is dry-run)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_lake = Path(args.data_lake)
    if not data_lake.exists():
        raise FileNotFoundError(f"data_lake not found: {data_lake}")

    targets = _collect_paths(data_lake)
    mode = "EXECUTE" if args.execute else "DRY_RUN"

    print(f"=== RESET GENERATED ARTIFACTS ({mode}) ===")
    print(f"data_lake root: {data_lake}")
    print(f"targets       : {len(targets)}")
    for path in targets:
        print(f"- {path}")

    if not args.execute:
        print("\nDry-run complete. Re-run with --execute to delete these paths.")
        return

    for path in targets:
        _delete_path(path)

    # Recreate expected directory skeleton
    for rel in SINK_DIRS + ["reports", "models", "replay_manifests", "checkpoints", "debug_alerts", "tools"]:
        (data_lake / rel).mkdir(parents=True, exist_ok=True)

    print("\nReset complete.")


if __name__ == "__main__":
    main()
