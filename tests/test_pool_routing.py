import asyncio
import threading

import pytest
from websockets.legacy.client import connect as connect_ws

from latzero_server.config import ServerConfig
from latzero_server.server import LatZeroServer
from conftest import RawClient


class OtherOwner:
    configured = True

    def owns(self, pool):
        return pool == "owned"

    def redirect_payload(self, pool):
        return {"protocol": "pool_redirect_v1", "host": "127.0.0.1", "port": 15001,
                "ws_port": 15002, "pool": pool, "pod_index": 1, "pod_count": 2,
                "router_host": "127.0.0.1", "router_port": 15000, "router_ws_port": 15003,
                "cluster_id": "test-cluster"}


async def open_client(server, transport):
    if transport == "ws":
        port = server._websocket_server.sockets[0].getsockname()[1]
        return RawClient(websocket=await connect_ws(f"ws://127.0.0.1:{port}"))
    port = server._tcp_server.sockets[0].getsockname()[1]
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    return RawClient(reader, writer)


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("capable", [True, False])
async def test_wrong_owner_redirects_before_creating_state_and_closes(tmp_path, transport, capable):
    server = LatZeroServer(ServerConfig(port=0, websocket_port=0, data_dir=tmp_path), pool_routing=OtherOwner())
    await server.start()
    client = await open_client(server, transport)
    try:
        await client.request("hello", {"capabilities": ["pool_redirect_v1"] if capable else []})
        reply = await client.request("join_pool", {"client_id": "caller", "pool": "elsewhere"},
                                     response="redirect" if capable else "error")
        assert reply["client_id"] == "caller"
        assert reply["pool"] == reply["payload"]["pool"] == "elsewhere"
        assert reply["payload"]["pod_index"] == 1
        if not capable:
            assert reply["payload"]["code"] == "redirect_required"
        assert "elsewhere" not in server._pools
        if transport == "tcp":
            assert await asyncio.wait_for(client.reader.read(), 2) == b""
        else:
            await asyncio.wait_for(client.websocket.wait_closed(), 2)
        assert server._route_count == 0
    finally:
        await client.close()
        await server.stop()


@pytest.mark.asyncio
async def test_switch_redirect_detaches_old_identity_before_target_join(tmp_path):
    server = LatZeroServer(ServerConfig(port=0, websocket_enabled=False, data_dir=tmp_path), pool_routing=OtherOwner())
    await server.start()
    caller = await open_client(server, "tcp")
    other = await open_client(server, "tcp")
    try:
        await caller.request("hello", {"capabilities": ["pool_redirect_v1"]})
        await caller.request("join_pool", {"client_id": "caller", "pool": "owned"})
        caller.client_id, caller.pool = "caller", "owned"
        await other.join("peer", "owned")
        await caller.request("subscribe_buffer", {"key": "watched"})
        await caller.request("register_process", {"process_name": "work"})
        redirected = await caller.request("switch_pool", {"client_id": "caller", "pool": "elsewhere"}, response="redirect")
        assert redirected["payload"]["pool"] == "elsewhere"
        assert await asyncio.wait_for(caller.reader.read(), 2) == b""
        assert (await other.request("list_clients"))["payload"]["clients"] == ["peer"]
        assert not server._pools["owned"].subscriptions
        assert not server._pools["owned"].processes
        assert "elsewhere" not in server._pools
    finally:
        await asyncio.gather(caller.close(), other.close(), return_exceptions=True)
        await server.stop()


@pytest.mark.asyncio
async def test_directory_lock_failure_does_not_flush_unowned_snapshot_work(tmp_path):
    from latzero_server.directory_lock import DataDirectoryLock

    owner = DataDirectoryLock(tmp_path).acquire()
    server = LatZeroServer(ServerConfig(port=0, websocket_enabled=False, data_dir=tmp_path))
    try:
        with pytest.raises((OSError, RuntimeError)):
            await server.start()
        assert not server._storage_started
        assert server._store._saver_task is None
        assert server._tcp_server is None and not server._sessions
        await server.stop()
    finally:
        owner.release()


@pytest.mark.asyncio
async def test_preconstructed_daemon_recaptures_snapshot_under_owner_lock(tmp_path):
    from latzero_server.models import BufferEntry, PoolState
    from latzero_server.persistence import SnapshotStore

    server = LatZeroServer(ServerConfig(port=0, websocket_enabled=False, data_dir=tmp_path))
    pool = PoolState(pool_id="new")
    pool.buffers["saved"] = BufferEntry(value=42, updated_at=1, updated_by="old", persistent=True)
    await asyncio.get_running_loop().run_in_executor(None, SnapshotStore(tmp_path).save_pool, pool)
    assert "new" not in server._pools
    await server.start()
    try:
        assert server._pools["new"].buffers["saved"].value == 42
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_failed_snapshot_flush_keeps_directory_owned_until_successful_retry(tmp_path, monkeypatch):
    from latzero_server.directory_lock import DataDirectoryLock
    from latzero_server.models import PoolState

    server = LatZeroServer(ServerConfig(port=0, websocket_enabled=False, data_dir=tmp_path))
    await server.start()
    server._store._shutdown_max_retries = 1
    original = server._store._write_file

    def fail(snapshot):
        raise OSError("injected storage failure")

    monkeypatch.setattr(server._store, "_write_file", fail)
    server._store.enqueue(PoolState(pool_id="dirty"))
    with pytest.raises(Exception, match="injected storage failure"):
        await server.stop()
    retained_store = server._store
    assert server._directory_lock is not None
    with pytest.raises((OSError, RuntimeError)):
        DataDirectoryLock(tmp_path).acquire()
    assert server._store is retained_store
    monkeypatch.setattr(server._store, "_write_file", original)
    await server.stop()
    assert server._directory_lock is None
    lock = DataDirectoryLock(tmp_path).acquire()
    lock.release()


@pytest.mark.asyncio
async def test_restart_refuses_inflight_saver_before_replacing_owned_store(tmp_path, monkeypatch):
    from latzero_server.models import BufferEntry, PoolState

    server = LatZeroServer(ServerConfig(port=0, websocket_enabled=False, data_dir=tmp_path))
    await server.start()
    saver = server._store
    saver._shutdown_timeout = 0.1
    entered = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    original = saver._write_file

    def blocked(snapshot):
        loop.call_soon_threadsafe(entered.set)
        if not release.wait(3):
            raise RuntimeError("snapshot barrier was not released")
        return original(snapshot)

    monkeypatch.setattr(saver, "_write_file", blocked)
    pool = PoolState(pool_id="owned")
    pool.buffers["latest"] = BufferEntry(value="old", updated_at=1, updated_by="writer", persistent=True)
    server._pools[pool.pool_id] = pool
    saver.enqueue(pool)
    await asyncio.wait_for(entered.wait(), 1)
    try:
        with pytest.raises(Exception):
            await server.stop()
        assert server._directory_lock is not None and saver.health["in_flight"]
        with pytest.raises(RuntimeError, match="Previous snapshot writer"):
            await server.start()
        assert server._store is saver
        assert server._pools["owned"].buffers["latest"].value == "old"
    finally:
        release.set()
        await asyncio.wait_for(asyncio.shield(saver._saver_task), 3)
        saver._shutdown_timeout = 3
        await server.stop()
    assert server._directory_lock is None


@pytest.mark.asyncio
async def test_single_daemon_lock_blocks_second_daemon_without_altering_first(tmp_path):
    first = LatZeroServer(ServerConfig(port=0, websocket_enabled=False, data_dir=tmp_path))
    second = LatZeroServer(ServerConfig(port=0, websocket_enabled=False, data_dir=tmp_path))
    await first.start()
    try:
        with pytest.raises((OSError, RuntimeError)):
            await second.start()
        assert first._tcp_server.is_serving()
        assert first._directory_lock is not None
        assert second._directory_lock is None
    finally:
        await second.stop()
        await first.stop()
    await second.start()
    await second.stop()


@pytest.mark.asyncio
async def test_unconfigured_pod_never_accepts_internal_clients(tmp_path):
    from latzero_server.pods import PoolRouting

    server = LatZeroServer(ServerConfig(port=0, websocket_enabled=False, data_dir=tmp_path),
                           pool_routing=PoolRouting(0, 2))
    await server.start()
    try:
        client = await open_client(server, "tcp")
        try:
            error = await client.receive("error")
            assert error["payload"]["code"] == "server_busy"
            assert await asyncio.wait_for(client.reader.read(), 2) == b""
            assert not server._pools
        finally:
            await client.close()
    finally:
        await server.stop()


def test_snapshot_load_retains_only_exact_owned_pool_before_ttl_dirty_capture(tmp_path):
    from latzero_server.models import BufferEntry, PoolState
    from latzero_server.persistence import SnapshotStore

    store = SnapshotStore(tmp_path)
    for pool_id in ("owned", "elsewhere"):
        pool = PoolState(pool_id=pool_id)
        pool.buffers["expired"] = BufferEntry(value="stale", updated_at=1, updated_by="old", ttl=1, persistent=True)
        pool.buffers["latest"] = BufferEntry(value=pool_id, updated_at=1, updated_by="old", persistent=True)
        store.save_pool(pool)
    server = LatZeroServer(ServerConfig(data_dir=tmp_path), pool_routing=OtherOwner())
    assert set(server._pools) == {"owned"}
    assert set(server._pools["owned"].buffers) == {"latest"}
    assert set(server._store._dirty) == {"owned"}
    assert server._store._dirty["owned"].pool.buffers["latest"].value == "owned"


@pytest.mark.asyncio
async def test_restart_after_another_owner_uses_latest_disk_not_stale_memory(tmp_path):
    first = LatZeroServer(ServerConfig(port=0, websocket_enabled=False, data_dir=tmp_path))
    second = LatZeroServer(ServerConfig(port=0, websocket_enabled=False, data_dir=tmp_path))
    await first.start()
    one = await open_client(first, "tcp")
    await one.join("one", "owned")
    await one.request("set_buffer", {"key": "latest", "value": 1, "persistent": True})
    await one.request("set_buffer", {"key": "ephemeral", "value": "first-owner"})
    await one.close()
    await first.stop()
    await second.start()
    two = await open_client(second, "tcp")
    await two.join("two", "owned")
    await two.request("set_buffer", {"key": "latest", "value": 2, "persistent": True})
    await two.close()
    await second.stop()
    await first.start()
    try:
        assert first._pools["owned"].buffers["latest"].value == 2
        assert "ephemeral" not in first._pools["owned"].buffers
    finally:
        await first.stop()


@pytest.mark.asyncio
async def test_same_owner_restart_retains_existing_ephemeral_behavior(tmp_path):
    server = LatZeroServer(ServerConfig(port=0, websocket_enabled=False, data_dir=tmp_path))
    await server.start()
    client = await open_client(server, "tcp")
    await client.join("one", "owned")
    await client.request("set_buffer", {"key": "ephemeral", "value": 42})
    await client.close()
    await server.stop()
    await server.start()
    try:
        assert server._pools["owned"].buffers["ephemeral"].value == 42
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_failed_handover_snapshot_load_remains_required_on_retry(tmp_path):
    from latzero_server.models import BufferEntry, PoolState
    from latzero_server.persistence import SnapshotStore
    from latzero_server.directory_lock import DataDirectoryLock

    server = LatZeroServer(ServerConfig(port=0, websocket_enabled=False, data_dir=tmp_path))
    await server.start()
    await server.stop()
    next_owner = DataDirectoryLock(tmp_path).acquire()
    try:
        for pool_id in ("alpha", "beta"):
            pool = PoolState(pool_id=pool_id, auth_required=True, auth_token_hash=pool_id)
            pool.buffers["value"] = BufferEntry(value=pool_id, updated_at=1, updated_by="second", persistent=True)
            await asyncio.get_running_loop().run_in_executor(None, SnapshotStore(tmp_path).save_pool, pool)
    finally:
        next_owner.release()
    server.config.max_pools = 1
    for _ in range(2):
        with pytest.raises(ValueError, match="max_pools"):
            await server.start()
        assert not server._initial_storage_loaded
        assert not server._accepting and server._tcp_server is None
    server.config.max_pools = 2
    await server.start()
    try:
        assert set(server._pools) == {"alpha", "beta"}
        assert {pool_id: pool.auth_token_hash for pool_id, pool in server._pools.items()} == {"alpha": "alpha", "beta": "beta"}
        assert {pool_id: pool.buffers["value"].value for pool_id, pool in server._pools.items()} == {"alpha": "alpha", "beta": "beta"}
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_generated_join_presence_size_rejects_only_new_member(daemon):
    server, connect = daemon
    server.config.max_frame_bytes = 512
    first, second, third = await connect(), await connect(), await connect()
    first_id, second_id, third_id = "a" * 80, "b" * 80, "c" * 80
    await first.join(first_id, "p")
    await second.join(second_id, "p")
    await third.request("hello")
    rejected = await third.request("join_pool", {"client_id": third_id, "pool": "p"}, response="error")
    assert rejected["payload"]["code"] == "response_too_large"
    assert (await first.request("list_clients"))["payload"]["clients"] == [first_id, second_id]
    assert (await second.request("list_clients"))["payload"]["clients"] == [first_id, second_id]
    assert set(server._pools["p"].clients) == {first_id, second_id}
    assert not server._pools["p"].clients[first_id].closing


@pytest.mark.asyncio
async def test_rejected_owner_local_switch_preserves_old_membership_and_auth(daemon):
    server, connect = daemon
    source, first, second = await connect(), await connect(), await connect()
    source_id = "c" * 80
    await source.join(source_id, "old", auth_token="secret")
    await first.join("a" * 80, "p")
    await second.join("b" * 80, "p")
    server.config.max_frame_bytes = 512
    rejected = await source.request("switch_pool", {"client_id": source_id, "pool": "p"}, response="error")
    assert rejected["payload"]["code"] == "response_too_large"
    assert (await source.request("list_clients"))["payload"]["clients"] == [source_id]
    assert server._pools["old"].clients[source_id].pool_id == "old"
    assert source_id not in server._pools["p"].clients
