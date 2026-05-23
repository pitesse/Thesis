"""CLI wrapper for Phase 2B presentation figure pack builder."""

from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ml_pipeline.lib.build_phase2b_presentation_figures import main


if __name__ == "__main__":
    main()
