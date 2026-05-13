"""CLI wrapper for SML/MOA Phase 2B dual-contract orchestration."""

from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ml_pipeline.lib.run_sml_phase2b_dual_contract import main


if __name__ == "__main__":
    main()
