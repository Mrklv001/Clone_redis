from __future__ import annotations
import time


def get_current_timestamp() -> float:
    """Get the current UNIX timestamp in milliseconds."""
    return time.time_ns() / 1e6
