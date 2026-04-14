"""Unified serving entrypoint for Kafka-based live ML inference."""

from __future__ import annotations

from lib.live_kafka_inference import main


if __name__ == "__main__":
    # thin wrapper keeps one stable user entrypoint even if lib internals are reorganized.
    main()
