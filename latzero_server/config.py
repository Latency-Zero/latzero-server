"""
Server configuration for latzero-server.
"""

from dataclasses import dataclass, field
import math
from pathlib import Path
from typing import List, Optional


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
    # A configurable task ceiling, not a throughput or concurrency guarantee.
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
    # Protective defaults must be measured for the deployed workload.
    max_connections: int = 5000

    # --- Slow-consumer isolation ---
    # TCP transport write-buffer high-water mark (writers drain, never skip).
    connection_hwm_bytes: int = 256 * 1024       # 256 KB
    # Per-connection critical mark: client is forcibly disconnected above this.
    connection_critical_bytes: int = 1024 * 1024  # 1 MB

    # --- Async persistence ---
    # Time window (seconds) for batching pool snapshot writes.
    persistence_batch_window: float = 0.1

    # --- Bounded admission / state ---
    max_frame_bytes: int = 1024 * 1024
    max_session_messages: int = 256
    max_session_bytes: int = 1024 * 1024
    max_queue_messages: int = 8192
    max_queue_bytes: int = 32 * 1024 * 1024
    control_reserve_messages: int = 32
    control_reserve_bytes: int = 64 * 1024
    max_outbox_messages: int = 256
    max_outbox_bytes: int = 1024 * 1024
    outbox_control_messages: int = 32
    outbox_control_bytes: int = 64 * 1024
    max_global_outbox_bytes: int = 64 * 1024 * 1024
    max_pools: int = 1024
    max_pool_bytes: int = 64 * 1024 * 1024
    max_buffers_per_pool: int = 4096
    max_subscriptions_per_pool: int = 16384
    max_processes_per_pool: int = 4096
    max_routes_per_pool: int = 4096
    max_routes_per_session: int = 256
    max_routes: int = 16384
    max_fanout_messages: int = 4096
    max_fanout_bytes: int = 32 * 1024 * 1024
    dispatch_slice: int = 16
    dispatch_slice_seconds: float = 0.002
    fanout_slice: int = 64
    cleanup_slice: int = 256
    write_timeout: float = 5.0
    shutdown_timeout: float = 5.0
    join_timeout: float = 10.0
    rpc_timeout: float = 30.0
    process_metrics_timeout: float = 15.0

    # None accepts clients without Origin, not arbitrary browser origins.
    websocket_enabled: bool = True
    websocket_port: Optional[int] = None
    websocket_origins: List[Optional[str]] = field(default_factory=lambda: [None])
    websocket_compression: Optional[str] = None
    websocket_max_queue: int = 16

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        self.data_dir = Path(self.data_dir)
        if not isinstance(self.host, str) or not self.host:
            raise ValueError("host must be a nonempty string")
        for name in ("port", "websocket_port"):
            value = getattr(self, name)
            if name == "websocket_port" and value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 65535:
                raise ValueError(f"{name} must be an integer port from 0 to 65535")
        if self.websocket_enabled and self.websocket_port is None and self.port == 65535:
            raise ValueError("websocket_port must be specified when TCP port is 65535")
        positive_counts = (
            "min_workers", "max_workers", "scale_up_threshold", "scale_down_threshold", "max_step_up", "burst_size",
            "max_connections", "connection_hwm_bytes", "connection_critical_bytes",
            "max_frame_bytes", "max_session_messages", "max_session_bytes",
            "max_queue_messages", "max_queue_bytes", "control_reserve_messages",
            "control_reserve_bytes", "max_outbox_messages", "max_outbox_bytes",
            "outbox_control_messages", "outbox_control_bytes", "max_global_outbox_bytes",
            "max_pools", "max_pool_bytes", "max_buffers_per_pool", "max_subscriptions_per_pool",
            "max_processes_per_pool", "max_routes_per_pool", "max_routes_per_session", "max_routes",
            "max_fanout_messages", "max_fanout_bytes", "dispatch_slice", "fanout_slice",
            "cleanup_slice", "websocket_max_queue",
        )
        for name in positive_counts:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.min_workers > self.max_workers:
            raise ValueError("min_workers must not exceed max_workers")
        if self.connection_hwm_bytes > self.connection_critical_bytes:
            raise ValueError("connection_hwm_bytes must not exceed connection_critical_bytes")
        for name in ("process_scale_down_threshold",):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        if (isinstance(self.process_scale_up_threshold, bool)
                or not isinstance(self.process_scale_up_threshold, int)
                or self.process_scale_up_threshold <= 0):
            raise ValueError("process_scale_up_threshold must be a positive integer")
        positive_times = (
            "cleanup_interval", "scale_down_hold", "controller_interval", "emergency_multiplier",
            "process_scale_cooldown", "persistence_batch_window", "dispatch_slice_seconds",
            "write_timeout", "shutdown_timeout", "join_timeout", "rpc_timeout", "process_metrics_timeout",
        )
        for name in positive_times:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if self.websocket_compression not in (None, "deflate"):
            raise ValueError("websocket_compression must be None or 'deflate'")
        if not isinstance(self.websocket_enabled, bool):
            raise ValueError("websocket_enabled must be a boolean")
        if not isinstance(self.websocket_origins, list) or any(
            value is not None and not isinstance(value, str) for value in self.websocket_origins
        ):
            raise ValueError("websocket_origins must be a list of explicit origins or None")
