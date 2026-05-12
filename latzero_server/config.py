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
