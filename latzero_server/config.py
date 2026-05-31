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

    # --- Cleanup / expiry ---
    # Increased from 0.5s — the expiry min-heap makes frequent full-scan unnecessary.
    cleanup_interval: float = 2.0

    # --- Auto-scaling worker pool ---
    min_workers: int = 4
    # Hard ceiling informed by empirical data: asyncio event-loop thrashes above
    # ~600 concurrent tasks on a single thread.  Observed: 568 workers → 14,657
    # TPS; 1280 workers → 4,516 TPS (3× regression).  512 gives headroom without
    # hitting the scheduler-overhead cliff.
    max_workers: int = 512
    # Queue depth that triggers a proportional scale-up
    scale_up_threshold: int = 50
    # Queue depth that must be sustained for scale_down_hold seconds to trigger -1 worker
    scale_down_threshold: int = 5
    scale_down_hold: float = 10.0
    # Controller wake-up interval (0.25s = 4 evaluations per second)
    controller_interval: float = 0.25
    # Max workers to add per proportional tick (keep steps moderate)
    max_step_up: int = 16
    # Workers to add in one emergency shot (queue > emergency_multiplier × threshold)
    burst_size: int = 32
    # Multiplier above threshold that triggers an emergency burst
    emergency_multiplier: float = 5.0

    # --- Process-level auto-scaling ---
    process_scale_up_threshold: int = 10      # total in-flight calls to trigger +1 replica
    process_scale_down_threshold: int = 3     # sustained in-flight to trigger -1 replica
    process_scale_cooldown: float = 5.0       # seconds between scale actions per process

    # --- Connection admission control ---
    # Maximum simultaneous TCP+WS connections. At ~150 KB overhead each, 5000 ≈ 750 MB.
    max_connections: int = 5000

    # --- Slow-consumer isolation ---
    # Per-connection write-buffer high-water mark: writes are skipped above this.
    connection_hwm_bytes: int = 256 * 1024       # 256 KB
    # Per-connection critical mark: client is forcibly disconnected above this.
    connection_critical_bytes: int = 1024 * 1024  # 1 MB

    # --- Async persistence ---
    # Time window (seconds) for batching pool snapshot writes.
    persistence_batch_window: float = 0.1
