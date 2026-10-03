"""Real sibling SDKs against isolated TCP/WS daemons; no transport mocks."""

import asyncio
import importlib
import importlib.machinery
import json
import queue
import shutil
import sys
import time
import types
from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path

import pytest

from latzero_server.config import ServerConfig
from latzero_server.server import LatZeroServer


ROOT = Path(__file__).resolve().parents[2]
HELPER = Path(__file__).with_name("interop_node.cjs")
CASES = [(fmt, api) for fmt in ("esm", "cjs") for api in ("default", "async")]


@pytest.fixture(params=CASES, ids=["%s-%s" % case for case in CASES])
def node_case(request):
    return request.param


class NodeSmoke:
    def __init__(self, process):
        self.process = process
        self.sequence = 0
        self.stderr = asyncio.create_task(process.stderr.read())

    async def receive(self):
        line = await asyncio.wait_for(self.process.stdout.readline(), 15)
        assert line, "Node smoke exited without a JSON result (exit=%r)" % self.process.returncode
        message = json.loads(line)
        assert message.get("ok"), "Node smoke failed: %s\n%s" % (
            message.get("error"), message.get("stack", ""),
        )
        return message

    async def command(self, operation, **fields):
        self.sequence += 1
        request = dict(fields, id=self.sequence, operation=operation)
        self.process.stdin.write((json.dumps(request) + "\n").encode("utf-8"))
        await asyncio.wait_for(self.process.stdin.drain(), 2)
        response = await self.receive()
        assert response["id"] == self.sequence, "Out-of-order interop helper response"
        return response["result"]

    async def close(self, failed):
        try:
            if self.process.returncode is None:
                if not failed:
                    await self.command("shutdown")
                else:
                    self.process.stdin.close()
                try:
                    await asyncio.wait_for(self.process.wait(), 5)
                except asyncio.TimeoutError:
                    self.process.kill()
                    await asyncio.wait_for(self.process.wait(), 5)
            stderr = await asyncio.wait_for(self.stderr, 2)
            if not failed:
                assert self.process.returncode == 0, stderr.decode("utf-8", errors="replace")
                assert not stderr, stderr.decode("utf-8", errors="replace")
        finally:
            if self.process.returncode is None:
                self.process.kill()
                await asyncio.wait_for(self.process.wait(), 5)
            self.process.stdin.close()
            if not self.stderr.done():
                self.stderr.cancel()
                await asyncio.gather(self.stderr, return_exceptions=True)


@asynccontextmanager
async def node_smoke(server, node_case=("esm", "default"), mode="sdk", origin=None):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Real-client interop requires Node with native WebSocket")
    for path in (ROOT / "node-client" / "index.js", ROOT / "node-client" / "index.cjs",
                 ROOT / "web-client" / "latzero-client.js"):
        if not path.is_file():
            pytest.skip("Real-client interop requires sibling SDK checkout: %s" % path)
    pool = "client-interop"
    fmt, api = node_case
    port = server._tcp_server.sockets[0].getsockname()[1]
    ws_port = server._websocket_server.sockets[0].getsockname()[1]
    options = dict(root=str(ROOT), port=port, wsPort=ws_port, pool=pool,
                   format=fmt, api=api, mode=mode, origin=origin)
    process = await asyncio.create_subprocess_exec(
        node, "--unhandled-rejections=strict", str(HELPER), json.dumps(options),
        cwd=str(HELPER.parents[1]), stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        limit=1024 * 1024,
    )
    smoke = NodeSmoke(process)
    failed = True
    try:
        ready = await smoke.receive()
        assert ready["ready"] is True
        yield smoke, ready, pool
        failed = False
    finally:
        await smoke.close(failed)


async def blocking(function, *args, **kwargs):
    # Keep the real daemon's event loop running while the synchronous SDK waits.
    future = asyncio.get_running_loop().run_in_executor(None, partial(function, *args, **kwargs))
    return await asyncio.wait_for(future, 8)


def assert_result(message, request_id, origin, target, kind, total):
    assert message["type"] == "app_result"
    assert message["request_id"] == request_id
    payload = message["payload"]
    assert payload["request_id"] == request_id
    assert payload["source_client_id"] == origin
    assert payload["target_client_id"] == target
    assert payload["event"] == ("calculate" if kind == "app" else target + ":compute")
    assert payload["error"] is None
    assert payload["value"] == {"owner": target, "kind": kind, "value": total}


@pytest.mark.asyncio
async def test_node_browser_real_daemon_rpc_and_opaque_ids(daemon, node_case):
    server, connect = daemon
    async with node_smoke(server, node_case) as (smoke, ready, pool):
        result = await smoke.command("rpc")
        assert len(result["receipts"]) == 16
        assert len(result["routed"]) == 12
        assert len(result["failures"]) == 4
        assert len(result["shortNames"]) == 6
        assert len(result["events"]) == 6
        assert all(item["id"] != item["hop"] for item in result["receipts"])
        assert len({item["id"] for item in result["receipts"]}) == 16

        raw = await connect()
        await raw.join("raw-origin", pool=pool)
        for target in ready["clients"]:
            request_id = "raw-origin-" + target
            await raw.request("call_process", {
                "process_id": target + ":compute", "data": {"a": 3, "b": 4}, "timeout": 1.5,
            }, request_id=request_id)
            response = await raw.receive("app_result", request_id)
            assert_result(response, request_id, "raw-origin", target, "process", 7)
        report = await smoke.command("report")
        assert report["errors"] == []
        for target in ready["clients"]:
            incoming = [message for message in report["wire"][target] if message["type"] == "call_app"
                        and message["payload"]["source_client_id"] == "raw-origin"]
            assert len(incoming) == 1
            assert incoming[0]["payload"]["event"] == target + ":compute"
            assert incoming[0]["request_id"] != "raw-origin-" + target
        assert not server._pools[pool].in_flight_requests

        # An old worker needs to echo only the opaque ID and {value,error}.
        legacy = await connect("ws")
        await legacy.join("legacy-worker", pool=pool)
        await legacy.request("register_process", {"process_name": "compute"})
        for origin in ("node-main", "browser-main"):
            for kind in ("app", "process"):
                call = asyncio.create_task(smoke.command(
                    "legacy_call", origin=origin, kind=kind, target="legacy-worker",
                ))
                try:
                    incoming = await legacy.receive("call_app")
                    assert incoming["payload"]["event"] == (
                        "calculate" if kind == "app" else "legacy-worker:compute"
                    )
                    assert not call.done(), "Native SDK settled before legacy callee completion"
                    await legacy.send({
                        "type": "app_result", "request_id": incoming["request_id"], "pool": None,
                        "payload": {"value": {"owner": "legacy-worker", "kind": kind, "value": 21}, "error": None},
                    })
                    response = await asyncio.wait_for(call, 8)
                    assert_result(response, response["request_id"], origin, "legacy-worker", kind, 21)
                    assert response["request_id"] != incoming["request_id"]
                finally:
                    if not call.done():
                        call.cancel()
                    await asyncio.gather(call, return_exceptions=True)
        assert not server._pools[pool].in_flight_requests


@pytest.mark.asyncio
async def test_node_browser_real_daemon_buffers_subscriptions_fractional_ttl(daemon, node_case):
    server, _ = daemon
    async with node_smoke(server, node_case) as (smoke, _, pool):
        assert await smoke.command("buffers") == {
            "writers": 3, "subscribers": 3, "updatesPerSubscriber": 4, "scalarValues": 4,
        }
        assert not server._pools[pool].subscriptions
        prepared = await smoke.command("prepare_ttl")
        for key, ttl, owner in (("node-ttl", 1.25, "node-main"), ("browser-ttl", 1.75, "browser-main")):
            assert prepared["entries"][key]["ttl"] == ttl
            assert prepared["entries"][key]["updated_by"] == owner
            assert server._pools[pool].buffers[key].ttl == ttl
        deadline = max(server._pools[pool].buffers[key].expires_at for key in prepared["entries"])
        expired = asyncio.Event()
        timer = asyncio.get_running_loop().call_later(max(0, deadline - time.monotonic()), expired.set)
        try:
            await asyncio.wait_for(expired.wait(), 4)
        finally:
            timer.cancel()
        assert await smoke.command("expire_ttl") == {"expired": ["node-ttl", "browser-ttl"]}
        assert not set(prepared["entries"]).intersection(server._pools[pool].buffers)
        assert (await smoke.command("report"))["errors"] == []


@pytest.fixture
def python_sdk():
    source = ROOT / "python-client" / "latzero"
    if not (source / "server_client.py").is_file():
        pytest.skip("Mixed-SDK smoke requires the sibling Python client checkout")
    # Load the actual stdlib-only daemon modules, not shared-memory package extras.
    name = "_latzero_interop_sdk"
    package = types.ModuleType(name)
    package.__path__ = [str(source)]
    package.__spec__ = importlib.machinery.ModuleSpec(name, loader=None, is_package=True)
    sys.modules[name] = package
    try:
        yield importlib.import_module(name + ".server_client").LatZero
    finally:
        for module in list(sys.modules):
            if module == name or module.startswith(name + "."):
                del sys.modules[module]


@pytest.mark.asyncio
async def test_python_node_browser_real_daemon_cross_sdk(daemon, node_case, python_sdk):
    server, _ = daemon
    client = None
    async with node_smoke(server, node_case) as (smoke, _, pool):
        try:
            port = server._tcp_server.sockets[0].getsockname()[1]
            client = await blocking(python_sdk, "latzero://python-main", pool,
                                    port=port, timeout=5, callback_workers=1)

            def calculate(a, b):
                return {"owner": "python-main", "kind": "app", "value": a + b}

            def compute(a, b):
                return {"owner": "python-main", "kind": "process", "value": a + b}

            client.on_event("calculate")(calculate)
            await blocking(client.process.register, compute, min_workers=1, max_workers=1)
            hooks = queue.Queue(maxsize=32)
            updates = queue.Queue(maxsize=16)
            client.on("on_app_result", hooks.put)
            client.on("on_buffer_update", updates.put)
            await blocking(client.subscribe_buffer, "mixed-shared")
            await blocking(client.set, "from-python", {"source": "python", "values": [None, 1.5]})
            mixed = await smoke.command("mixed", pythonId=client.client_id)
            assert len(mixed["results"]) == 8
            expected = {item["id"]: item for item in mixed["acceptances"]}
            assert len(expected) == 4
            for _ in range(4):
                payload = await blocking(hooks.get, timeout=4)
                route = expected.pop(payload["request_id"])
                assert payload["source_client_id"] == route["origin"]
                assert payload["target_client_id"] == route["target"]
                assert payload["response_to"] == "python-main"
                assert payload["error"] is None
                assert payload["value"] == {"owner": route["target"], "kind": route["kind"], "value": 9}
            assert not expected
            notifications = [await blocking(updates.get, timeout=4) for _ in range(2)]
            assert [item["entry"]["value"] for item in notifications] == [{"source": "node"}, {"source": "browser"}]
            assert await blocking(client.get, "mixed-shared") == {"source": "browser"}

            for target, recipient in (("node-main", "browser-main"), ("browser-main", "node-main")):
                for kind in ("app", "process"):
                    for response_to in (None, client.client_id):
                        options = dict(a=5, b=6, response_to=response_to)
                        if kind == "app":
                            result = await blocking(client.call_app, target, "calculate", **options)
                        else:
                            result = await blocking(client.process.call, target + ":compute", **options)
                        assert result == {"owner": target, "kind": kind, "value": 11}
                    options = dict(a=1, b=2, response_to=recipient)
                    if kind == "app":
                        acceptance = await blocking(client.call_app, target, "calculate", **options)
                    else:
                        acceptance = await blocking(client.process.call, target + ":compute", **options)
                    assert acceptance["queued"] is True
                    result = await smoke.command("hook", recipient=recipient, requestId=acceptance["request_id"])
                    assert_result(result, acceptance["request_id"], client.client_id, target, kind, 3)
                    assert result["payload"]["response_to"] == recipient
            assert (await smoke.command("report"))["errors"] == []
            assert not server._pools[pool].in_flight_requests
        finally:
            if client is not None:
                await blocking(client.disconnect)


@pytest.mark.asyncio
async def test_real_interop_helper_failure_closes_native_transports(daemon):
    server, _ = daemon
    sessions = []
    with pytest.raises(AssertionError, match="Unknown interop command"):
        async with node_smoke(server) as (smoke, _, pool):
            sessions = list(server._pools[pool].clients.values())
            assert len(sessions) == 3
            await smoke.command("invalid-test-operation")
    tasks = [session.writer_task for session in sessions if session.writer_task is not None]
    await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 5)
    assert all(session.closed and session.outbox_bytes == 0 and session.route_count == 0 for session in sessions)
    assert all(not state.clients and not state.in_flight_requests for state in server._pools.values())


@pytest.mark.asyncio
@pytest.mark.parametrize("allowed,origin,accepted", [
    ([None], None, True),
    ([None], "https://interop.example", False),
    ([None, "https://interop.example"], "https://interop.example", True),
    ([None, "https://interop.example"], "https://other.example", False),
    ([None], "null", False),
    ([None, "null"], "null", True),
], ids=["native-no-origin", "browser-default-denied", "browser-opt-in", "other-origin-denied",
        "file-null-default-denied", "file-null-opt-in"])
async def test_real_browser_script_native_websocket_origin_policy(tmp_path, allowed, origin, accepted):
    config = ServerConfig(port=0, websocket_port=0, data_dir=tmp_path,
                          min_workers=2, max_workers=4, websocket_origins=allowed)
    server = LatZeroServer(config)
    await asyncio.wait_for(server.start(), 5)
    try:
        async with node_smoke(server, mode="origin", origin=origin) as (_, ready, pool):
            outcome = ready["outcome"]
            assert outcome["connected"] is accepted
            if accepted:
                assert outcome["value"] == {"origin": origin}
                assert server._pools[pool].buffers["origin-probe"].value == {"origin": origin}
                session = server._pools[pool].clients["browser-main"]
                assert session.writer.request_headers.get("Origin") == origin
            else:
                assert outcome["code"] == "connection_lost"
                assert pool not in server._pools
    finally:
        await asyncio.wait_for(server.stop(), 10)
