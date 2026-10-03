import asyncio
import json
from types import SimpleNamespace

import pytest

from latzero_server import server as server_module
from latzero_server.config import ServerConfig
from latzero_server.models import BufferEntry, PoolState
from latzero_server.persistence import SnapshotStore
from latzero_server.server import LatZeroServer


def test_restored_ttl_has_monotonic_deadline_and_drops_expired_values(tmp_path, monkeypatch):
    store = SnapshotStore(tmp_path)
    pool = PoolState(pool_id="restored", auth_required=True, auth_token_hash="token-hash")
    pool.buffers["live"] = BufferEntry(
        value={"safe": 42}, updated_at=100, updated_by="old-client",
        ttl=10.25, version=3, persistent=True,
    )
    pool.buffers["expired"] = BufferEntry(
        value="must-not-revive", updated_at=100, updated_by="old-client",
        ttl=2, persistent=True,
    )
    pool.buffers["permanent"] = BufferEntry(
        value="kept", updated_at=100, updated_by="old-client", persistent=True,
    )
    store.save_pool(pool)
    clock = SimpleNamespace(time=lambda: 105, monotonic=lambda: 1000)
    monkeypatch.setattr(server_module, "time", clock)
    server = LatZeroServer(ServerConfig(data_dir=tmp_path))
    restored = server._pools["restored"]
    assert set(restored.buffers) == {"live", "permanent"}
    assert restored.auth_token_hash == "token-hash"
    assert restored.buffers["live"].expires_at == 1005.25
    assert restored.buffers["permanent"].expires_at is None
    assert server._expiry_heap == [(1005.25, "restored", "live", 3)]

    async def expire_and_flush():
        server._store.start()
        # Runtime expiration is independent of wall-clock corrections.
        clock.time = lambda: 10
        clock.monotonic = lambda: 1005.25
        await server._expire_buffers()
        assert set(restored.buffers) == {"permanent"}
        assert restored.buffer_bytes == restored.buffers["permanent"].size_bytes
        await server._store.stop()

    asyncio.run(expire_and_flush())
    snapshot = SnapshotStore(tmp_path).load_pools()["restored"]
    assert set(snapshot["buffers"]) == {"permanent"}
    assert "must-not-revive" not in json.dumps(snapshot)


@pytest.mark.asyncio
async def test_refresh_compacts_stale_ttl_and_ephemeral_state_never_dirties_disk(daemon, monkeypatch):
    server, connect = daemon
    client = await connect()
    await client.join("owner")
    # Pool creation is already persisted/coalesced before observing hot writes.
    calls = []
    original_enqueue = server._store.enqueue
    monkeypatch.setattr(server._store, "enqueue", lambda pool: calls.append(pool.pool_id))
    for version in range(1, 101):
        reply = await client.request("set_buffer", {"key": "ttl", "value": version, "ttl": 3600})
        assert reply["payload"]["version"] == version
    assert not calls
    assert len(server._expiry_heap) <= 64
    await client.request("set_buffer", {"key": "ttl", "value": "persistent", "persistent": True})
    assert calls == ["test-pool"]
    await client.request("set_buffer", {"key": "ttl", "value": "ephemeral"})
    assert calls == ["test-pool", "test-pool"]
    await client.request("delete_buffer", {"key": "ttl"})
    assert len(calls) == 2
    monkeypatch.setattr(server._store, "enqueue", original_enqueue)


@pytest.mark.asyncio
async def test_cleanup_failure_is_visible_and_retried_without_dead_task(daemon, monkeypatch):
    server, _ = daemon
    # Restart just cleanup with an injected first failure and a bounded barrier.
    server._cleanup_task.cancel()
    await asyncio.gather(server._cleanup_task, return_exceptions=True)
    recovered = asyncio.Event()
    attempts = 0

    async def fault_once():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("injected-cleanup-fault")
        recovered.set()

    monkeypatch.setattr(server, "_expire_buffers", fault_once)
    server.config.cleanup_interval = 0.001
    server._cleanup_task = server._track(server._cleanup_loop(), "latzero-cleanup-test")
    await asyncio.wait_for(recovered.wait(), 1)
    assert not server._cleanup_task.done()
    assert server.get_dashboard_snapshot()["health"]["background_error"] == "injected-cleanup-fault"


@pytest.mark.asyncio
async def test_scaling_uses_fresh_metrics_and_normalized_worker_bounds(daemon, monkeypatch):
    server, connect = daemon
    client = await connect()
    await client.join("owner")
    await client.request("register_process", {
        "process_name": "work", "scale": True, "min_workers": 2,
        "max_workers": 3, "max_replicas": 20,
    })
    pool = server._pools["test-pool"]
    registration = pool.processes["owner:work"]
    assert registration.min_workers == 2 and registration.max_workers == 3
    assert registration.max_replicas == 3
    now = 1000
    monkeypatch.setattr(server_module, "time", SimpleNamespace(time=lambda: now, monotonic=lambda: now))
    sent = []

    async def capture(pool, client_id, message):
        sent.append(message)

    monkeypatch.setattr(server, "_send_to_client", capture)
    registration.worker_count = 2
    registration.reported_queue_depth = 100
    await server._check_process_scaling()
    assert not sent  # Unreported clients do not receive scaling commands.
    registration.last_metrics_at = now
    await server._check_process_scaling()
    assert sent[-1]["payload"]["action"] == "up"
    registration.last_scale_action = 0
    registration.worker_count = 3
    await server._check_process_scaling()
    assert len(sent) == 1
    registration.reported_queue_depth = 0
    await server._check_process_scaling()
    assert sent[-1]["payload"]["action"] == "down"
    registration.last_scale_action = 0
    registration.worker_count = 2
    await server._check_process_scaling()
    assert len(sent) == 2
    registration.worker_count = 3
    registration.last_metrics_at = now - server.config.process_metrics_timeout - 1
    await server._check_process_scaling()
    assert len(sent) == 2


@pytest.mark.asyncio
async def test_tcp_close_failure_does_not_skip_other_teardown_or_poison_retry(tmp_path, monkeypatch):
    server = LatZeroServer(ServerConfig(
        port=0, websocket_port=0, data_dir=tmp_path,
        min_workers=1, max_workers=2, write_timeout=0.1,
    ))
    await server.start()
    tcp = server._tcp_server
    persisted = PoolState(pool_id="flush-despite-close-fault", auth_required=True, auth_token_hash="hash")
    server._store.enqueue(persisted)

    async def fail_close():
        raise asyncio.TimeoutError("injected TCP close timeout")

    monkeypatch.setattr(tcp, "wait_closed", fail_close)
    try:
        with pytest.raises(asyncio.TimeoutError, match="injected TCP close timeout"):
            await server.stop()
        assert server._tcp_server is None and server._websocket_server is None
        assert not server._sessions and not server._background
        assert not server._store.health["running"]
        assert "flush-despite-close-fault" in SnapshotStore(tmp_path).load_pools()
        await server.stop()
        await server.start()
        assert server._accepting and server._tcp_server.is_serving()
    finally:
        await server.stop()
