"""
Server configuration for latzero-server.
"""

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ServerConfig:
    """Runtime configuration for the local LatZero server."""

    host: str = "127.0.0.1"
    port: int = 14130
    data_dir: Path = field(default_factory=lambda: Path.home() / ".cache" / "latzero")
    cleanup_interval: float = 0.5

    # --- Auto-scaling worker pool ---
    min_workers: int = 4
    max_workers: int = 128
    # Queue depth that triggers an immediate +2 worker scale-up
    scale_up_threshold: int = 50
    # Queue depth that must be sustained for scale_down_hold seconds to trigger -1 worker
    scale_down_threshold: int = 5
    scale_down_hold: float = 10.0
