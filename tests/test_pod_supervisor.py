import asyncio
import hashlib
import json
import os
from pathlib import Path
import sys

import pytest
from websockets.exceptions import ConnectionClosed
from websockets.legacy.client import connect

from latzero_server.config import ServerConfig
from latzero_server.directory_lock import DataDirectoryLock, DataDirectoryLockError
from latzero_server.pods import PoolRouting, PodSupervisor, pool_owner


def test_pool_owner_exact_hash_and_read_only_count():
    for pool in ("a", "pool with spaces", "a/b", "caf\u00e9", "cafe\u0301", "\u96ea"):
        for count in (1, 2, 4, 64):
            owner = int.from_bytes(hashlib.sha256(pool.encode("utf-8")).digest(), "big") % count
            assert pool_owner(pool, count) == owner
            assert sum(PoolRouting(index, count).owns(pool) for index in range(count)) == 1
    routing = PoolRouting(0, 2)
    with pytest.raises(AttributeError):
        routing.pod_count = 3
    with pytest.raises(AttributeError):
        routing.pod_index = 1


@pytest.mark.parametrize("count", [0, -1, 65, True, 2.0, None])
def test_invalid_pod_counts(count):
    with pytest.raises(ValueError):
        PoolRouting(0, count)


@pytest.mark.parametrize("index", [-1, 2, True, 1.0, None])
def test_invalid_pod_index(index):
    with pytest.raises(ValueError):
        PoolRouting(index, 2)


def test_configure_copies_endpoints_and_exact_redirect_contract():
    routing = PoolRouting(0, 2)
    with pytest.raises(RuntimeError):
        routing.redirect_payload("pool")
    endpoints = [{"host": "127.0.0.1", "port": 21001, "ws_port": 21002},
                 {"host": "127.0.0.1", "port": 21003, "ws_port": None}]
    routing.configure(endpoints, "127.0.0.1", 20001, None, "cluster")
    pool = "\u96ea/flat"
    owner = pool_owner(pool, 2)
    expected = dict(endpoints[owner])
    endpoints[owner]["port"] = 1
    payload = routing.redirect_payload(pool)
    assert payload == {"protocol": "pool_redirect_v1", "host": "127.0.0.1", "port": expected["port"],
                       "ws_port": expected["ws_port"], "pool": pool, "pod_index": owner, "pod_count": 2,
                       "router_host": "127.0.0.1", "router_port": 20001, "router_ws_port": None,
                       "cluster_id": "cluster"}
    payload["port"] = 2
    assert routing.redirect_payload(pool)["port"] == expected["port"]


@pytest.mark.parametrize("field,value", [("host", "localhost"), ("host", "0.0.0.0"), ("port", 0),
                                          ("port", True), ("port", 65536), ("ws_port", -1), ("ws_port", False)])
def test_configure_rejects_remote_or_invalid_endpoint(field, value):
    endpoint = {"host": "127.0.0.1", "port": 20001, "ws_port": None}
    endpoint[field] = value
    with pytest.raises(ValueError):
        PoolRouting(0, 1).configure([endpoint], "127.0.0.1", 20000, None, "cluster")


def test_lock_root_child_slots_and_retained_inode(tmp_path):
    root = DataDirectoryLock(tmp_path).acquire()
    inode = root.path.stat().st_ino
    child = DataDirectoryLock(tmp_path, slot=0).acquire()
    sibling = DataDirectoryLock(tmp_path, slot=63).acquire()
    try:
        with pytest.raises(DataDirectoryLockError):
            DataDirectoryLock(tmp_path).acquire()
        with pytest.raises(DataDirectoryLockError):
            DataDirectoryLock(tmp_path, slot=0).acquire()
        root.release()
        with pytest.raises(DataDirectoryLockError):
            DataDirectoryLock(tmp_path).acquire()
        child.release()
        with pytest.raises(DataDirectoryLockError):
            DataDirectoryLock(tmp_path).acquire()
        sibling.release()


def test_lock_root_epoch_ignores_children_and_failed_acquisitions(tmp_path):
    with DataDirectoryLock(tmp_path) as first:
        token = first.owner_token
        assert first.previous_owner_token is None and len(token) == 32
        with DataDirectoryLock(tmp_path, slot=1) as child:
            assert child.owner_token is None
        with pytest.raises(DataDirectoryLockError):
            DataDirectoryLock(tmp_path).acquire()
    with DataDirectoryLock(tmp_path) as second:
        assert second.previous_owner_token == token
        assert second.owner_token != token
        second_token = second.owner_token
    with DataDirectoryLock(tmp_path) as third:
        assert third.previous_owner_token == second_token
        with DataDirectoryLock(tmp_path) as restarted:
            assert restarted.acquired
            assert restarted.path.stat().st_ino == inode
            assert restarted.path.stat().st_size >= 65
        restarted.release()
    finally:
        root.release()
        child.release()
        sibling.release()


@pytest.mark.parametrize("slot", [-1, 64, True, 0.0, "0"])
def test_lock_slot_validation(tmp_path, slot):
    with pytest.raises(ValueError):
        DataDirectoryLock(tmp_path, slot=slot)


async def _json_line(reader, timeout=5):
    raw = await asyncio.wait_for(reader.readline(), timeout)
    assert raw
    return json.loads(raw)


async def _send(writer, message):
    writer.write((json.dumps(message) + "\n").encode("utf-8"))
    await asyncio.wait_for(writer.drain(), 5)


def _config(tmp_path, **options):
    values = {"data_dir": tmp_path, "port": 0, "websocket_port": 0, "min_workers": 1, "max_workers": 2,
              "join_timeout": 2.0, "write_timeout": 0.5, "shutdown_timeout": 0.5}
    values.update(options)
    return ServerConfig(**values)


@pytest.mark.asyncio
async def test_root_slot_contention_across_real_processes(tmp_path):
    process = await asyncio.create_subprocess_exec(sys.executable, str(Path(__file__).resolve()), "--lease", str(tmp_path), "0",
                                                   stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                                                   stderr=asyncio.subprocess.PIPE, env={**os.environ,
                                                       "PYTHONPATH": str(Path(__file__).resolve().parents[1])})
    try:
        assert (await _json_line(process.stdout))["locked"] is True
        with pytest.raises(DataDirectoryLockError):
            DataDirectoryLock(tmp_path).acquire()
        with pytest.raises(DataDirectoryLockError):
            DataDirectoryLock(tmp_path, slot=0).acquire()
        with DataDirectoryLock(tmp_path, slot=1):
            pass
    finally:
        process.stdin.close()
        await asyncio.wait_for(process.wait(), 5)
        assert not await process.stderr.read()
    with DataDirectoryLock(tmp_path):
        pass


@pytest.mark.asyncio
async def test_real_pods_router_readiness_redirect_and_restart(tmp_path):
    supervisor = PodSupervisor(_config(tmp_path), 2)
    try:
        await asyncio.gather(*(supervisor.start() for _ in range(3)))
        assert supervisor.tcp_port > 0 and supervisor.ws_port > 0
        assert len({child.pid for child in supervisor.children}) == 2
        assert all(child.pid != os.getpid() and child.status == "running" for child in supervisor.children)
        with pytest.raises(DataDirectoryLockError):
            DataDirectoryLock(tmp_path).acquire()
        reader, writer = await asyncio.open_connection("127.0.0.1", supervisor.tcp_port)
        try:
            await _send(writer, {"type": "hello", "request_id": "hello", "client_id": "client",
                                 "payload": {"capabilities": ["pool_redirect_v1"]}})
            assert (await _json_line(reader))["type"] == "ack"
            await _send(writer, {"type": "join_pool", "request_id": "join", "client_id": "client", "pool": "pool",
                                 "payload": {"client_id": "client", "pool": "pool", "auth_token": "auth"}})
            redirect = await _json_line(reader)
            assert redirect["type"] == "redirect" and redirect["request_id"] == "join"
            assert redirect["client_id"] == "client" and redirect["pool"] == "pool"
            child = supervisor.children[supervisor.pool_owner("pool")]
            assert redirect["payload"]["port"] == child.port
            assert redirect["payload"]["router_port"] == supervisor.tcp_port
            assert await asyncio.wait_for(reader.read(), 2) == b""
        finally:
            writer.close()
            await writer.wait_closed()
        first_pids = {child.pid for child in supervisor.children}
        await asyncio.gather(*(supervisor.stop() for _ in range(3)))
        assert all(child.process.returncode == 0 and child.status == "stopped" for child in supervisor.children)
        with DataDirectoryLock(tmp_path):
            pass
        await supervisor.start()
        assert not first_pids.intersection(child.pid for child in supervisor.children)
    finally:
        await supervisor.stop()


@pytest.mark.asyncio
async def test_forced_child_termination_reaps_runtime_and_seals_cluster(tmp_path, monkeypatch):
    import latzero_server.pods as module

    supervisor = PodSupervisor(_config(tmp_path, websocket_enabled=False), 2)
    await supervisor.start()
    child = supervisor.children[0]
    watcher = child._exit_task
    watcher.cancel()
    await asyncio.gather(watcher, return_exceptions=True)
    child.process.terminate()
    await asyncio.wait_for(child.process.wait(), 5)
    assert child.process.returncode != 0
    if child.process.runtime is not None:
        assert not child.process.runtime.alive()
    with pytest.raises(RuntimeError, match="did not stop cleanly"):
        await supervisor.stop()
    assert not supervisor._accepting
    assert all(record.process.returncode is not None for record in supervisor.children)
    assert supervisor._directory_lock is None
    assert not supervisor.get_dashboard_snapshot()["healthy"]
    with DataDirectoryLock(tmp_path):
        pass
    with pytest.raises(RuntimeError):
        await supervisor.serve_forever()


@pytest.mark.asyncio
async def test_stop_escalation_handles_actual_runtime_not_only_venv_launcher(tmp_path, monkeypatch):
    import latzero_server.pods as module

    monkeypatch.setattr(module, "_STOP_GRACE", 0.1)
    monkeypatch.setattr(module, "_REAP_TIMEOUT", 1.0)
    supervisor = PodSupervisor(_config(tmp_path, websocket_enabled=False), 2)
    await supervisor.start()
    actual = [child.process.runtime for child in supervisor.children]
    original = module._PodProcess.wait

    async def ignore_first_wait(process):
        if process.launcher.returncode is None:
            await asyncio.Future()
        return await original(process)

    # Suppress the STOP frame and EOF until escalation, without replacing the
    # production subprocess/handle termination implementation.
    async def no_stop(child, message, deadline=None):
        await asyncio.Future()

    monkeypatch.setattr(supervisor, "_send_control", no_stop)
    for child in supervisor.children:
        class NoClose:
            def close(self):
                pass
        child.process.launcher._original_stdin = child.process.launcher.stdin
        child.process.launcher.stdin = NoClose()
    try:
        with pytest.raises(RuntimeError, match="escalated True"):
            await asyncio.wait_for(supervisor.stop(), 5)
        assert all(child.process.returncode is not None for child in supervisor.children)
        assert all(handle is None or handle.handle is None or not handle.alive() for handle in actual)
        with DataDirectoryLock(tmp_path):
            pass
    finally:
        for child in supervisor.children:
            child.process.launcher.stdin = child.process.launcher._original_stdin
            child.process.stdin.close()


@pytest.mark.asyncio
async def test_parent_crash_child_slot_blocks_new_root_until_flush_exits(tmp_path):
    process = await asyncio.create_subprocess_exec(sys.executable, str(Path(__file__).resolve()), "--orphan-parent", str(tmp_path),
                                                   stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                                                   stderr=asyncio.subprocess.PIPE, env={**os.environ,
                                                       "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
                                                       "PYTHONIOENCODING": "utf-8"})
    parent_record = await _json_line(process.stdout)
    lease = parent_record["lease"]
    from latzero_server.pods import _WindowsProcess

    parent_handle = _WindowsProcess(parent_record["pid"], terminate=True) if os.name == "nt" else None
    child_handle = _WindowsProcess(lease["pid"], terminate=True) if os.name == "nt" else None
    try:
        await _send(process.stdin, {"operation": "exit"})
        await asyncio.wait_for(process.wait(), 5)
        with pytest.raises(DataDirectoryLockError):
            DataDirectoryLock(tmp_path).acquire()
        if child_handle is not None:
            child_handle.terminate()
            await asyncio.get_running_loop().run_in_executor(None, child_handle.kernel.WaitForSingleObject, child_handle.handle, 5000)
            assert not child_handle.alive()
        else:
            os.kill(lease["pid"], 15)
        with DataDirectoryLock(tmp_path):
            pass
    finally:
        process.stdin.close()
        if process.returncode is None:
            if parent_handle is not None:
                parent_handle.terminate()
            else:
                process.kill()
            await asyncio.wait_for(process.wait(), 5)
        for handle in (child_handle, parent_handle):
            if handle is not None:
                handle.terminate()
                handle.close()


@pytest.mark.asyncio
async def test_router_legacy_error_no_pool_or_auth_mutation(tmp_path):
    supervisor = PodSupervisor(_config(tmp_path, websocket_enabled=False), 2)
    try:
        await supervisor.start()
        reader, writer = await asyncio.open_connection("127.0.0.1", supervisor.tcp_port)
        try:
            await _send(writer, {"type": "hello", "request_id": "h"})
            assert (await _json_line(reader))["type"] == "ack"
            await _send(writer, {"type": "join_pool", "request_id": "j", "payload": {"client_id": "old", "pool": "private", "auth_token": "invalid"}})
            reply = await _json_line(reader)
            assert reply["type"] == "error" and reply["payload"]["code"] == "redirect_required"
            assert reply["payload"]["protocol"] == "pool_redirect_v1"
            assert reply["payload"]["ws_port"] is None and reply["payload"]["router_ws_port"] is None
            assert await asyncio.wait_for(reader.read(), 2) == b""
            assert not list(tmp_path.glob("pool-*.json"))
        finally:
            writer.close()
            await writer.wait_closed()
    finally:
        await supervisor.stop()


@pytest.mark.asyncio
async def test_router_shared_tcp_pre_websocket_connection_bound(tmp_path):
    supervisor = PodSupervisor(_config(tmp_path, max_connections=1), 2)
    raw_writer = None
    try:
        await supervisor.start()
        raw_reader, raw_writer = await asyncio.open_connection("127.0.0.1", supervisor.ws_port)
        other_reader, other_writer = await asyncio.open_connection("127.0.0.1", supervisor.ws_port)
        try:
            assert (await asyncio.wait_for(other_reader.read(), 2)).startswith(b"HTTP/1.1 503")
        finally:
            other_writer.close()
            await other_writer.wait_closed()
        tcp_reader, tcp_writer = await asyncio.open_connection("127.0.0.1", supervisor.tcp_port)
        try:
            reply = await _json_line(tcp_reader)
            assert reply["payload"]["code"] == "server_busy"
        finally:
            tcp_writer.close()
            await tcp_writer.wait_closed()
        assert len(supervisor._pending_websockets) == 1
        await supervisor.stop()
        assert await asyncio.wait_for(raw_reader.read(), 2) == b""
        assert not supervisor._readers and not supervisor._sessions
    finally:
        if raw_writer is not None:
            raw_writer.close()
            await raw_writer.wait_closed()
        await supervisor.stop()


@pytest.mark.asyncio
async def test_router_websocket_binary_error_and_join_redirect(tmp_path):
    supervisor = PodSupervisor(_config(tmp_path), 2)
    try:
        await supervisor.start()
        async with connect("ws://127.0.0.1:{}".format(supervisor.ws_port)) as websocket:
            await websocket.send(b"binary")
            assert json.loads(await websocket.recv())["payload"]["code"] == "protocol_error"
            await websocket.send(json.dumps({"type": "hello", "request_id": "h", "payload": {"capabilities": ["pool_redirect_v1"]}}))
            assert json.loads(await websocket.recv())["type"] == "ack"
            await websocket.send(json.dumps({"type": "switch_pool", "request_id": "switch", "payload": {"client_id": "ws", "pool": "pool"}}))
            redirect = json.loads(await websocket.recv())
            assert redirect["type"] == "redirect" and redirect["request_id"] == "switch"
            assert redirect["payload"]["ws_port"] == supervisor.children[supervisor.pool_owner("pool")].ws_port
            with pytest.raises(ConnectionClosed):
                await websocket.recv()
    finally:
        await supervisor.stop()


@pytest.mark.asyncio
async def test_delayed_child_refuses_dead_parent_before_snapshot_loader(tmp_path):
    process = await asyncio.create_subprocess_exec(sys.executable, "-m", "latzero_server.pods", "--child",
                                                   stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                                                   stderr=asyncio.subprocess.PIPE)
    from dataclasses import asdict

    config = asdict(_config(tmp_path, websocket_enabled=False))
    config["data_dir"] = str(tmp_path)
    await _send(process.stdin, {"config": config, "index": 0, "pods": 2, "parent_pid": 0xFFFFFFFE})
    process.stdin.close()
    messages = []
    while True:
        raw = await asyncio.wait_for(process.stdout.readline(), 5)
        if not raw:
            break
        messages.append(json.loads(raw))
    assert await asyncio.wait_for(process.wait(), 5) != 0
    assert not any(message.get("ready") for message in messages)
    assert not list(tmp_path.glob("pool-*.json"))
    with DataDirectoryLock(tmp_path):
        pass


if __name__ == "__main__" and sys.argv[1] == "--lease":
    with DataDirectoryLock(Path(sys.argv[2]), slot=int(sys.argv[3])):
        print(json.dumps({"locked": True, "pid": os.getpid()}), flush=True)
        sys.stdin.buffer.read()
elif __name__ == "__main__" and sys.argv[1] == "--orphan-parent":
    import subprocess

    root = DataDirectoryLock(Path(sys.argv[2])).acquire()
    lease_process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "--lease", sys.argv[2], "0"],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    lease = json.loads(lease_process.stdout.readline())
    print(json.dumps({"pid": os.getpid(), "lease": lease}), flush=True)
    sys.stdin.buffer.readline()
    # Keep the lease child's input write handle inherited by a detached
    # grandchild, simulating a slow surviving writer after root owner crash.
    os.set_handle_inheritable(lease_process.stdin.fileno(), True) if os.name == "nt" else None
    os._exit(0)
