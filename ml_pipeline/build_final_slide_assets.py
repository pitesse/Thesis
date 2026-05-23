#!/usr/bin/env python3
"""CLI wrapper for final slide asset generation."""

from __future__ import annotations

import sys
from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from ml_pipeline.lib.build_final_slide_assets import main as _main

    _main()


if __name__ == "__main__":
    main()
