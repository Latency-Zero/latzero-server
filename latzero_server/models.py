"""
Data models for latzero-server runtime state.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set


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
    For non-scaling registrations there is exactly one replica.
    """

    process_id: str           # canonical "owner_client_id:process_name"
    process_name: str         # short name (e.g. "echo")
    owner_client_id: str      # the client that originally registered
    group_id: str             # ties all replicas together
    scale: bool               # eligible for auto-scaling?
    max_replicas: int         # upper limit (only meaningful when scale=True)
    replicas: List[ProcessReplica]
    created_at: float
    rr_index: int = 0
    last_scale_action: float = 0.0


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


@dataclass
class ClientSession:
    """Connection-scoped client session."""

    client_id: str
    writer: Any
    pool_id: Optional[str] = None

