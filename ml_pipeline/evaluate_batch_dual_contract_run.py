"""CLI wrapper for one-run Batch dual-contract evaluation."""

from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ml_pipeline.lib.evaluate_batch_dual_contract_run import main


if __name__ == "__main__":
    main()
