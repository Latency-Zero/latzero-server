import asyncio

import pytest
from websockets.legacy.client import connect as connect_ws

from latzero_server.config import ServerConfig
from latzero_server.server import LatZeroServer
from conftest import RawClient


class OtherOwner:
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
    SnapshotStore(tmp_path).save_pool(pool)
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
    assert server._directory_lock is not None
    with pytest.raises((OSError, RuntimeError)):
        DataDirectoryLock(tmp_path).acquire()
    monkeypatch.setattr(server._store, "_write_file", original)
    await server.stop()
    assert server._directory_lock is None
    lock = DataDirectoryLock(tmp_path).acquire()
    lock.release()


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
