import asyncio
import hashlib
import json
import os
from pathlib import Path
import sys
import weakref

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
        with DataDirectoryLock(tmp_path) as restarted:
            assert restarted.acquired
            assert restarted.path.stat().st_ino == inode
            assert restarted.path.stat().st_size >= 65
        restarted.release()
    finally:
        root.release()
        child.release()
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
async def test_forced_child_termination_reaps_runtime_and_seals_cluster(tmp_path):
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
    with pytest.raises(RuntimeError, match="Pod shutdown failed"):
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
    # Suppress the STOP frame and EOF until escalation, without replacing the
    # production subprocess/handle termination implementation.
    async def no_stop(child, message, deadline=None):
        raise asyncio.TimeoutError()

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
async def test_pre_ready_control_failure_discovers_and_reaps_runtime(tmp_path, monkeypatch):
    import latzero_server.pods as module

    monkeypatch.setattr(module, "_STOP_GRACE", 0.1)
    monkeypatch.setattr(module, "_REAP_TIMEOUT", 1.0)
    real_spawn = asyncio.create_subprocess_exec
    discovered = []
    real_discover = module._PodProcess.discover_runtimes

    async def spawn(*args, **options):
        return await real_spawn(sys.executable, str(Path(__file__).resolve()), "--pre-ready-child", **options)

    def discover(process):
        real_discover(process)
        if process.runtime is not None:
            discovered.append((process.launcher.pid, process.runtime))

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(module._PodProcess, "discover_runtimes", discover)
    supervisor = PodSupervisor(_config(tmp_path, websocket_enabled=False), 2)
    with pytest.raises(RuntimeError, match="Unexpected pod control output"):
        await asyncio.wait_for(supervisor.start(), 5)
    assert all(child.process is None or child.process.returncode is not None for child in supervisor.children)
    assert supervisor._directory_lock is None
    if os.name == "nt":
        assert discovered
        assert all(launcher != handle.pid for launcher, handle in discovered)
        assert all(handle.handle is None or not handle.alive() for _, handle in discovered)
    with DataDirectoryLock(tmp_path):
        pass


@pytest.mark.asyncio
async def test_public_ports_stay_inactive_until_every_child_configured(tmp_path, monkeypatch):
    supervisor = PodSupervisor(_config(tmp_path, websocket_enabled=False), 2)
    configuring = asyncio.Event()
    release = asyncio.Event()
    original = supervisor._send_control

    async def send_control(child, message, deadline=None):
        if message.get("operation") == "configure" and child.index == 1:
            configuring.set()
            await release.wait()
        await original(child, message, deadline)

    monkeypatch.setattr(supervisor, "_send_control", send_control)
    starting = asyncio.create_task(supervisor.start())
    try:
        await asyncio.wait_for(configuring.wait(), 5)
        assert not supervisor._accepting
        assert not supervisor._tcp_server.is_serving()
        assert all(child.port for child in supervisor.children)
        with pytest.raises((ConnectionRefusedError, OSError)):
            await asyncio.wait_for(asyncio.open_connection("127.0.0.1", supervisor.tcp_port), 2)
        release.set()
        await asyncio.wait_for(starting, 5)
        assert supervisor._accepting and supervisor._tcp_server.is_serving()
    finally:
        release.set()
        if not starting.done():
            starting.cancel()
        await asyncio.gather(starting, return_exceptions=True)
        await supervisor.stop()


@pytest.mark.asyncio
async def test_start_cancellation_reaps_partial_children_before_root_release(tmp_path, monkeypatch):
    supervisor = PodSupervisor(_config(tmp_path, websocket_enabled=False), 2)
    configuring = asyncio.Event()
    original = supervisor._send_control

    async def send_control(child, message, deadline=None):
        if message.get("operation") == "configure":
            configuring.set()
            await asyncio.Future()
        await original(child, message, deadline)

    monkeypatch.setattr(supervisor, "_send_control", send_control)
    starting = asyncio.create_task(supervisor.start())
    await asyncio.wait_for(configuring.wait(), 5)
    with pytest.raises(DataDirectoryLockError):
        DataDirectoryLock(tmp_path).acquire()
    starting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(starting, 5)
    assert not supervisor._accepting
    assert supervisor._directory_lock is None
    assert all(child.process.returncode == 0 for child in supervisor.children)
    assert not supervisor._spawns and not supervisor._creations
    with DataDirectoryLock(tmp_path):
        pass
    monkeypatch.setattr(supervisor, "_send_control", original)
    try:
        await supervisor.start()
    finally:
        await supervisor.stop()


@pytest.mark.asyncio
async def test_failed_partial_spawn_cannot_leave_sibling_spawn_after_cleanup(tmp_path, monkeypatch):
    supervisor = PodSupervisor(_config(tmp_path, websocket_enabled=False), 2)
    ready = asyncio.Event()
    original = supervisor._spawn
    cancelled = asyncio.Event()

    async def spawn(child, config, deadline):
        if child.index == 0:
            try:
                await original(child, config, deadline)
                ready.set()
                await asyncio.Future()
            finally:
                cancelled.set()
        else:
            await ready.wait()
            raise OSError("injected spawn failure")

    monkeypatch.setattr(supervisor, "_spawn", spawn)
    with pytest.raises(OSError, match="injected spawn failure"):
        await asyncio.wait_for(supervisor.start(), 5)
    assert cancelled.is_set()
    assert not supervisor._spawns and not supervisor._creations
    assert supervisor._directory_lock is None
    assert all(child.process is None or child.process.returncode is not None for child in supervisor.children)
    with DataDirectoryLock(tmp_path):
        pass


class _MemoryTransport:
    def __init__(self):
        self.closed = False

    def abort(self):
        self.closed = True

    def close(self):
        self.closed = True


class _MemoryWriter:
    def __init__(self, stalled=False):
        self.transport = _MemoryTransport()
        self.frames = []
        self.stalled = stalled

    def write(self, frame):
        self.frames.append(frame)

    async def drain(self):
        if self.stalled:
            await asyncio.Future()

    def close(self):
        self.transport.close()

    async def wait_closed(self):
        if self.stalled:
            await asyncio.Future()


@pytest.mark.asyncio
async def test_router_write_deadline_stalled_close_and_references_are_bounded(tmp_path):
    import gc

    supervisor = PodSupervisor(_config(tmp_path, write_timeout=0.05), 2)
    writer = _MemoryWriter(stalled=True)
    session = supervisor._new_session(writer, asyncio.get_running_loop().time() + 2)
    with pytest.raises(asyncio.TimeoutError):
        await supervisor._write(session, {"type": "ack", "request_id": "id", "payload": {}})
    assert len(writer.frames) == 1
    await asyncio.wait_for(supervisor._close_session(session), 1)
    assert writer.transport.closed
    assert supervisor._connection_count == 0 and not supervisor._sessions
    reference = weakref.ref(session)
    del session, writer
    gc.collect()
    assert reference() is None


@pytest.mark.asyncio
async def test_router_only_membership_requests_validated_before_redirect(tmp_path):
    supervisor = PodSupervisor(_config(tmp_path, max_frame_bytes=1024, max_session_messages=6), 2)
    supervisor._routing = PoolRouting(0, 2)
    supervisor._routing.configure([{"host": "127.0.0.1", "port": 1, "ws_port": None},
                                   {"host": "127.0.0.1", "port": 2, "ws_port": None}], "127.0.0.1", 3, None, "cluster")
    writer = _MemoryWriter()
    session = supervisor._new_session(writer, asyncio.get_running_loop().time() + 2)
    try:
        frames = [
            {"type": "hello", "client_id": "id", "request_id": "h", "payload": {"capabilities": ["pool_redirect_v1"]}},
            {"type": "set_buffer", "request_id": "set", "payload": {"key": "k", "value": 1}},
            {"type": "join_pool", "request_id": "alias", "client_id": "id", "pool": "outer", "payload": {"pool": "inner"}},
            {"type": "join_pool", "request_id": "identity", "client_id": "other", "pool": "pool", "payload": {}},
            {"type": "join_pool", "request_id": "auth", "client_id": "id", "pool": "pool", "payload": {"auth_token": 2}},
            {"type": "join_pool", "request_id": "join", "client_id": "id", "pool": "pool", "payload": {"auth_token": "wrong"}},
        ]
        for frame in frames:
            await supervisor._route_frame(session, json.dumps(frame))
        replies = [json.loads(frame) for frame in writer.frames]
        assert [reply["type"] for reply in replies] == ["ack", "error", "error", "error", "error", "redirect"]
        assert [reply["payload"].get("code") for reply in replies[1:5]] == ["not_joined", "protocol_error", "identity_change", "protocol_error"]
        assert replies[-1]["request_id"] == "join" and session.closing
        assert replies[-1]["payload"]["protocol"] == "pool_redirect_v1"
        assert not list(tmp_path.glob("pool-*.json"))
    finally:
        await supervisor._close_session(session)


@pytest.mark.asyncio
async def test_child_control_health_pool_id_output_stays_bounded():
    from types import SimpleNamespace
    from latzero_server.pods import _child_health, _control_frame, _CONTROL_LIMIT

    pools = {("pool-%d-" % index) + "\u96ea" * 160: None for index in range(1024)}
    server = SimpleNamespace(_pools=pools, _connection_count=0, _pending_websockets={}, _health_error=None,
                             _metrics={}, _store=SimpleNamespace(health={"healthy": True}))
    stats = _child_health(server, PoolRouting(0, 2))
    assert stats["pools"] == 1024 and stats["pool_ids_truncated"] is True
    assert len(_control_frame({"stats": stats})) <= _CONTROL_LIMIT
    assert 0 < len(stats["pool_ids"]) < 1024
    with pytest.raises(ValueError, match="64 KiB"):
        _control_frame({"data": "x" * _CONTROL_LIMIT})


@pytest.mark.asyncio
async def test_control_reader_cancels_blocked_raw_read_without_pipe_eof(monkeypatch):
    from types import SimpleNamespace
    from latzero_server.pods import _ControlReader

    read_fd, write_fd = os.pipe()
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(fileno=lambda: read_fd))
    reader = _ControlReader()
    try:
        assert await asyncio.get_running_loop().run_in_executor(None, reader.thread_ready.wait, 1)
        assert reader.thread.is_alive()
        await asyncio.wait_for(reader.wait_closed(), 2)
        assert not reader.thread.is_alive()
        assert reader.handle is None
        await reader.wait_closed()
    finally:
        reader.close()
        os.close(write_fd)
        await reader.wait_closed()
        os.close(read_fd)


@pytest.mark.asyncio
async def test_control_reader_split_coalesced_frames_and_pending_delivery_close(monkeypatch):
    from types import SimpleNamespace
    from latzero_server.pods import _ControlReader

    read_fd, write_fd = os.pipe()
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(fileno=lambda: read_fd))
    reader = _ControlReader()
    try:
        frames = [{"config": "\u96ea"}, {"operation": "configure"}, {"operation": "stats"}]
        raw = b"".join((json.dumps(frame, ensure_ascii=False) + "\n").encode("utf-8") for frame in frames)
        os.write(write_fd, raw[:13])
        os.write(write_fd, raw[13:])
        assert await asyncio.wait_for(reader.receive(), 2) == frames[0]
        assert await asyncio.wait_for(reader.receive(), 2) == frames[1]
        assert await asyncio.wait_for(reader.receive(), 2) == frames[2]
        os.write(write_fd, b"{}\n{}\n{}\n")
        await asyncio.wait_for(reader.wait_closed(), 2)
        assert not reader.thread.is_alive() and reader.queue.maxsize == 1
    finally:
        reader.close()
        os.close(write_fd)
        await reader.wait_closed()
        os.close(read_fd)


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
        reader, writer = await asyncio.open_connection("127.0.0.1", parent_record["drain_port"])
        assert await asyncio.wait_for(reader.readline(), 5) == b"draining\n"
        await _send(writer, {"operation": "release"})
        assert await asyncio.wait_for(reader.read(), 5) == b""
        writer.close()
        await writer.wait_closed()
        if child_handle is not None:
            await asyncio.get_running_loop().run_in_executor(None, child_handle.kernel.WaitForSingleObject, child_handle.handle, 5000)
            assert not child_handle.alive()
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
    from dataclasses import asdict

    root = DataDirectoryLock(Path(sys.argv[2])).acquire()
    lease_process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "--draining-pod"],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    drain = json.loads(lease_process.stdout.readline())
    config = asdict(_config(Path(sys.argv[2]), websocket_enabled=False))
    config["data_dir"] = sys.argv[2]
    lease_process.stdin.write((json.dumps({"config": config, "index": 0, "pods": 1, "parent_pid": os.getpid()}) + "\n").encode())
    lease_process.stdin.flush()
    lease = json.loads(lease_process.stdout.readline())
    lease_process.stdin.write((json.dumps({"operation": "configure", "endpoints": [{"host": "127.0.0.1", "port": lease["port"], "ws_port": None}],
                                         "router_host": "127.0.0.1", "router_port": lease["port"], "router_ws_port": None, "cluster_id": "fixture"}) + "\n").encode())
    lease_process.stdin.flush()
    assert json.loads(lease_process.stdout.readline())["configured"] is True
    print(json.dumps({"pid": os.getpid(), "lease": lease, "drain_port": drain["port"]}), flush=True)
    sys.stdin.buffer.readline()
    os._exit(0)
elif __name__ == "__main__" and sys.argv[1] == "--draining-pod":
    from latzero_server.pods import _child_run
    from latzero_server.server import LatZeroServer

    async def draining_child():
        release = asyncio.Event()
        draining = asyncio.Event()
        original = LatZeroServer.stop

        async def stop(server):
            draining.set()
            await asyncio.wait_for(release.wait(), 10)
            await original(server)

        async def allow_stop(reader, writer):
            try:
                await asyncio.wait_for(draining.wait(), 5)
                writer.write(b"draining\n")
                await writer.drain()
                await reader.readline()
                release.set()
            finally:
                writer.close()
                await writer.wait_closed()

        LatZeroServer.stop = stop
        listener = await asyncio.start_server(allow_stop, "127.0.0.1", 0)
        print(json.dumps({"port": listener.sockets[0].getsockname()[1]}), flush=True)
        resources = {"lock": None, "io_stopped": False}
        try:
            return await _child_run(resources)
        finally:
            listener.close()
            await listener.wait_closed()
            if resources["lock"] is not None:
                resources["lock"].release()

    raise SystemExit(asyncio.run(draining_child()))
elif __name__ == "__main__" and sys.argv[1] == "--pre-ready-child":
    sys.stdin.buffer.readline()
    print(json.dumps({"waiting": True, "pid": os.getpid()}), flush=True)
    asyncio.run(asyncio.Event().wait())
