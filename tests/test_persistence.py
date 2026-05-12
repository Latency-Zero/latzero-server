"""
Unit tests for latzero-server snapshot persistence.
"""

from latzero_server.models import BufferEntry, PoolState
from latzero_server.persistence import SnapshotStore


def test_snapshot_store_roundtrip(tmp_path):
    store = SnapshotStore(tmp_path)
    pool = PoolState(pool_id="demo", auth_required=True, auth_token_hash="hash123")
    pool.buffers["persisted"] = BufferEntry(
        value={"hello": "world"},
        updated_at=1.0,
        updated_by="client-a",
        persistent=True,
        ttl=None,
        version=2,
    )
    pool.buffers["ephemeral"] = BufferEntry(
        value={"temp": True},
        updated_at=2.0,
        updated_by="client-b",
        persistent=False,
        ttl=None,
        version=1,
    )

    store.save_pool(pool)
    loaded = store.load_pools()

    assert "demo" in loaded
    assert loaded["demo"]["auth_required"] is True
    assert "persisted" in loaded["demo"]["buffers"]
    assert "ephemeral" not in loaded["demo"]["buffers"]
