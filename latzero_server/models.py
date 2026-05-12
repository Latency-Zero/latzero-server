"""
Data models for latzero-server runtime state.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set


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
    # Process pool: maps "client_id:process_name" -> client_id
    processes: Dict[str, str] = field(default_factory=dict)

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

