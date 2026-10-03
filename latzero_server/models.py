"""
Data models for latzero-server runtime state.
"""

from dataclasses import dataclass, field
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Set


# ---------------------------------------------------------------------------
# Process auto-scaling models
# ---------------------------------------------------------------------------


@dataclass
class ProcessReplica:
    """A single replica of a scalable registered process."""

    client_id: str
    created_at: float
    in_flight: int = 0


@dataclass
class ProcessRegistration:
    """
    Rich registration metadata for a process (scalable or not).

    ``processes[process_id]`` maps to this object instead of a raw client_id.
    Replicas are local workers inside the owning client, not separate TCP
    connections.  The server tracks aggregate metrics reported by the client.
    """

    process_id: str                  # canonical "owner_client_id:process_name"
    process_name: str                # short name (e.g. "echo")
    owner_client_id: str             # the client that originally registered
    group_id: str                    # ties all registrations together
    scale: bool                      # eligible for auto-scaling?
    max_replicas: int                # upper limit (only meaningful when scale=True)
    created_at: float
    rr_index: int = 0                # cross-client round-robin for short-name calls
    last_scale_action: float = 0.0

    # Worker-backend configuration from registration
    worker_kind: str = "thread"      # "thread" | "process" | "adaptive"
    min_workers: int = 1
    max_workers: int = 10

    # Metrics periodically reported by the owning client
    worker_count: int = 0            # active local workers
    reported_queue_depth: int = 0    # backlog of calls waiting for a worker
    reported_avg_latency: float = 0.0
    reported_completed_count: int = 0
    last_metrics_at: float = 0.0

    # Retained for backward compatibility (can be empty in new mode)
    replicas: List[ProcessReplica] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Original models
# ---------------------------------------------------------------------------


@dataclass
class BufferEntry:
    """Pool-scoped buffer state."""

    value: Any
    updated_at: float
    updated_by: str
    persistent: bool = False
    ttl: Optional[float] = None
    version: int = 1
    expires_at: Optional[float] = None  # Monotonic runtime deadline, not persisted.
    size_bytes: int = 0

    def to_dict(self) -> dict:
        return {
            "value": self.value,
            "updated_at": self.updated_at,
            "updated_by": self.updated_by,
            "persistent": self.persistent,
            "ttl": self.ttl,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "BufferEntry":
        return cls(
            value=data.get("value"),
            updated_at=data.get("updated_at", 0.0),
            updated_by=data.get("updated_by", ""),
            persistent=data.get("persistent", False),
            ttl=data.get("ttl"),
            version=data.get("version", 1),
        )


@dataclass
class RouteEntry:
    """In-flight app call routing metadata."""

    request_id: str
    origin_client_id: str
    target_client_id: str
    response_client_id: str
    event: str
    created_at: float
    expires_at: Optional[float] = None
    process_registration_id: Optional[str] = None  # for in_flight tracking on scalable processes
    process_registration: Optional[ProcessRegistration] = None
    origin_request_id: Optional[str] = None
    parent_request_id: Optional[str] = None
    origin_session: Optional["ClientSession"] = None
    target_session: Optional["ClientSession"] = None
    response_session: Optional["ClientSession"] = None
    origin_generation: int = 0
    target_generation: int = 0
    response_generation: int = 0
    sent: bool = False


@dataclass
class PoolState:
    """All isolated runtime state for a server pool."""

    pool_id: str
    auth_required: bool = False
    auth_token_hash: Optional[str] = None
    buffers: Dict[str, BufferEntry] = field(default_factory=dict)
    subscriptions: Dict[str, Set[str]] = field(default_factory=dict)
    clients: Dict[str, "ClientSession"] = field(default_factory=dict)
    in_flight_requests: Dict[str, RouteEntry] = field(default_factory=dict)
    # Process pool: maps canonical "owner_client_id:process_name" → ProcessRegistration
    processes: Dict[str, "ProcessRegistration"] = field(default_factory=dict)
    buffer_bytes: int = 0
    subscription_count: int = 0

    def snapshot(self) -> dict:
        return {
            "pool_id": self.pool_id,
            "auth_required": self.auth_required,
            "auth_token_hash": self.auth_token_hash,
            "buffers": {
                key: entry.to_dict()
                for key, entry in self.buffers.items()
                if entry.persistent
            },
        }


@dataclass(eq=False)
class ClientSession:
    """Connection-scoped client session."""

    client_id: str
    writer: Any
    pool_id: Optional[str] = None
    closed: bool = False
    closing: bool = False
    generation: int = 0
    active_requests: Set[str] = field(default_factory=set)
    active_request_counts: Dict[str, int] = field(default_factory=dict)
    route_count: int = 0
    outbox: Deque[Any] = field(default_factory=deque)
    outbox_bytes: int = 0
    outbox_messages: int = 0
    outbox_ready: Any = None
    outbox_drained: Any = None
    writer_task: Any = None
    reader_task: Any = None
    joined: Any = None
    joined_once: bool = False
    close_task: Any = None

