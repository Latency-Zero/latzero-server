"""Raw-wire regressions for routing, admission, ordering, and session fences."""

import asyncio
import json
import math
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from websockets.legacy.client import connect as websocket_connect

from conftest import RawClient
from latzero_server import server as server_module
from latzero_server.config import ServerConfig
from latzero_server.server import LatZeroServer


pytestmark = pytest.mark.asyncio
TRANSPORTS = ["tcp", "ws"]


def envelope(client, msg_type, request_id, payload=None, pool=None):
    return {
        "type": msg_type,
        "request_id": request_id,
        "client_id": client.client_id,
        "pool": pool,
        "payload": payload if payload is not None else {},
    }


def assert_routes_released(server, *departed):
    assert server._route_count == 0
    assert all(not pool.in_flight_requests for pool in server._pools.values())
    for session in list(server._sessions.values()) + list(departed):
        assert session.route_count == 0
        assert not session.active_requests
        assert not session.active_request_counts
    for pool in server._pools.values():
        for registration in pool.processes.values():
            assert all(replica.in_flight == 0 for replica in registration.replicas)


def manual_clock(monkeypatch):
    clock = SimpleNamespace(now=time.monotonic(), wall=time.time())
    monkeypatch.setattr(server_module, "time", SimpleNamespace(
        monotonic=lambda: clock.now, time=lambda: clock.wall,
    ))
    return clock


async def send_raw(client, raw):
    if client.websocket is not None:
        await client.websocket.send(raw)
    else:
        client.writer.write(raw if isinstance(raw, bytes) else raw.encode("utf-8"))
        client.writer.write(b"\n")
        await client.writer.drain()


async def wire_message(client):
    if client.websocket is not None:
        raw = await asyncio.wait_for(client.websocket.recv(), 2)
    else:
        raw = await asyncio.wait_for(client.reader.readline(), 2)
        assert raw, "Connection closed before the expected wire frame"
    return json.loads(raw)


async def assert_connection_closed(client):
    if client.websocket is not None:
        await asyncio.wait_for(client.websocket.wait_closed(), 2)
    else:
        await asyncio.wait_for(client.reader.read(), 2)
        assert client.reader.at_eof()


async def departure_barrier(client, client_id):
    for _ in range(8):
        message = await client.receive("presence_update")
        if message["payload"]["client_id"] == client_id and message["payload"]["status"] == "left":
            return message
    pytest.fail("Disconnect presence barrier was missing")


async def drain_outboxes(server):
    await asyncio.wait_for(asyncio.gather(*(
        session.outbox_drained.wait() for session in server._sessions.values()
    )), 2)


@asynccontextmanager
async def server_with_limits(tmp_path, **options):
    defaults = dict(
        port=0, websocket_port=0, data_dir=tmp_path,
        min_workers=2, max_workers=4, cleanup_interval=3600,
        controller_interval=3600, shutdown_timeout=0.2,
    )
    defaults.update(options)
    server = LatZeroServer(ServerConfig(**defaults))
    clients = []

    async def connect(transport="tcp"):
        if transport == "ws":
            port = server._websocket_server.sockets[0].getsockname()[1]
            client = RawClient(websocket=await websocket_connect(f"ws://127.0.0.1:{port}"))
        else:
            port = server._tcp_server.sockets[0].getsockname()[1]
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            client = RawClient(reader=reader, writer=writer)
        clients.append(client)
        return client

    try:
        await server.start()
        yield server, connect
    finally:
        await asyncio.gather(*(client.close() for client in clients), return_exceptions=True)
        await asyncio.wait_for(server.stop(), 3)


class ControlledWriter:
    """A TCP writer whose drain is released by the test, not OS buffer timing."""

    def __init__(self):
        self.transport = self
        self.frames = []
        self.drain_entered = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()
        self.closed = asyncio.Event()
        self.buffer_size = 0

    def set_write_buffer_limits(self, **limits):
        pass

    def get_write_buffer_size(self):
        return self.buffer_size

    def is_closing(self):
        return self.closed.is_set()

    def write(self, encoded):
        if self.closed.is_set():
            raise ConnectionError("Controlled writer is closed")
        self.frames.append(json.loads(encoded))

    async def drain(self):
        self.drain_entered.set()
        await self.release.wait()

    def close(self):
        self.closed.set()
        self.release.set()

    async def wait_closed(self):
        await self.closed.wait()

    def abort(self):
        self.close()


def controlled_session(server, pool, client_id="blocked"):
    writer = ControlledWriter()
    session = server._new_session(writer)
    # A fake writer has no socket reader; never let teardown cancel this test.
    session.reader_task = None
    session.client_id = client_id
    session.pool_id = pool.pool_id
    session.joined_once = True
    session.joined.set()
    pool.clients[client_id] = session
    return session, writer


async def block_outbox(server, session, writer, messages=1):
    writer.release.clear()
    writer.drain_entered.clear()
    for index in range(messages):
        assert server._reserve_message(session, {
            "type": "emit_event", "request_id": "pressure" if index == 0 else f"pressure-{index}",
            "client_id": "test", "pool": session.pool_id,
            "payload": {"event": "pressure", "data": "x" * 512},
        }) is not None
    await asyncio.wait_for(writer.drain_entered.wait(), 2)


@pytest.mark.parametrize("transport", TRANSPORTS)
@pytest.mark.parametrize("response_to", [None, "origin", "observer"])
async def test_direct_self_response_and_third_party_app_routes(daemon, transport, response_to):
    server, connect = daemon
    origin = await connect(transport)
    target = await connect("ws" if transport == "tcp" else "tcp")
    observer = await connect(transport)
    await origin.join("origin")
    await target.join("target")
    await observer.join("observer")
    payload = {"target_client_id": "target", "event": "echo", "data": {"answer": 42}}
    if response_to is not None:
        payload["response_to"] = response_to
    accepted = await origin.request("call_app", payload, "public-origin")
    assert accepted["request_id"] == "public-origin"
    assert accepted["payload"]["queued"] is True
    assert accepted["payload"]["request_id"] == "public-origin"
    incoming = await target.receive("call_app")
    assert incoming["request_id"] != "public-origin"
    assert incoming["payload"]["event"] == "echo"
    assert incoming["payload"]["data"] == {"answer": 42}
    assert incoming["payload"]["source_client_id"] == "origin"
    assert incoming["payload"]["response_to"] == (response_to or "origin")
    completed = await target.request("app_result", {
        "value": {"answer": 42}, "error": None,
    }, incoming["request_id"])
    assert completed["payload"]["delivered"] is True
    recipient = observer if response_to == "observer" else origin
    result = await recipient.receive("app_result", "public-origin")
    assert result["pool"] == "test-pool"
    assert result["payload"]["request_id"] == "public-origin"
    assert result["payload"]["value"] == {"answer": 42}
    assert result["payload"]["error"] is None
    assert result["payload"]["target_client_id"] == "target"
    if recipient is observer:
        await origin.request("hello", request_id="origin-barrier")
        assert not any(m["type"] == "app_result" and m["request_id"] == "public-origin" for m in origin.pending)
    assert_routes_released(server)


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_application_error_keeps_origin_correlation_and_value_error_shape(daemon, transport):
    server, connect = daemon
    caller, callee = await connect(transport), await connect(transport)
    await caller.join("caller")
    await callee.join("callee")
    await caller.request("call_app", {"target_client_id": "callee", "event": "fail"}, "failed-call")
    incoming = await callee.receive("call_app")
    await callee.request("app_result", {"value": None, "error": "application failure"}, incoming["request_id"])
    result = await caller.receive("app_result", "failed-call")
    assert result["payload"]["request_id"] == "failed-call"
    assert result["payload"]["value"] is None
    assert result["payload"]["error"] == "application failure"
    assert_routes_released(server)


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_same_origin_id_from_different_callers_does_not_overwrite(daemon, transport):
    server, connect = daemon
    callers = [await connect(transport), await connect(transport)]
    callee = await connect(transport)
    for client, name in zip(callers, ["first", "second"]):
        await client.join(name)
    await callee.join("callee")
    for client in callers:
        await client.request("call_app", {
            "target_client_id": "callee", "event": "echo", "data": client.client_id,
        }, "shared-origin-id")
    incoming = [await callee.receive("call_app"), await callee.receive("call_app")]
    assert len({message["request_id"] for message in incoming}) == 2
    assert all(message["request_id"] != "shared-origin-id" for message in incoming)
    assert len(server._pools["test-pool"].in_flight_requests) == server._route_count == 2
    for message in reversed(incoming):
        source = message["payload"]["source_client_id"]
        await callee.request("app_result", {"value": source, "error": None}, message["request_id"])
    for client in callers:
        result = await client.receive("app_result", "shared-origin-id")
        assert result["payload"]["request_id"] == "shared-origin-id"
        assert result["payload"]["value"] == client.client_id
    assert_routes_released(server)


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_forged_callee_identity_cannot_consume_another_sessions_route(daemon, transport):
    server, connect = daemon
    caller, callee, attacker = [await connect(transport) for _ in range(3)]
    await caller.join("caller")
    await callee.join("callee")
    await attacker.join("attacker")
    await caller.request("call_app", {"target_client_id": "callee", "event": "echo"}, "protected")
    incoming = await callee.receive("call_app")
    pool = server._pools["test-pool"]
    route = pool.in_flight_requests[incoming["request_id"]]
    forged = envelope(attacker, "app_result", incoming["request_id"], {"value": "forged"})
    forged["client_id"] = "callee"
    await attacker.send(forged)
    rejected = await attacker.receive("error", incoming["request_id"])
    assert rejected["payload"]["code"] == "wrong_callee"
    assert pool.in_flight_requests[incoming["request_id"]] is route
    assert server._route_count == 1
    await callee.request("app_result", {"value": "legitimate", "error": None}, incoming["request_id"])
    assert (await caller.receive("app_result", "protected"))["payload"]["value"] == "legitimate"
    assert_routes_released(server)


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_duplicate_active_origin_rejects_without_execution_and_is_reusable(daemon, transport):
    server, connect = daemon
    caller, callee = await connect(transport), await connect(transport)
    await caller.join("caller")
    await callee.join("callee")
    payload = {"target_client_id": "callee", "event": "echo"}
    await caller.request("call_app", payload, "duplicate")
    incoming = await callee.receive("call_app")
    pool = server._pools["test-pool"]
    route = pool.in_flight_requests[incoming["request_id"]]
    rejected = await caller.request("call_app", payload, "duplicate", response="error")
    assert rejected["payload"]["code"] == "duplicate_request"
    assert pool.in_flight_requests == {incoming["request_id"]: route}
    await callee.request("hello", request_id="no-extra-call-barrier")
    assert not any(message["type"] == "call_app" for message in callee.pending)
    await callee.request("app_result", {"value": 1}, incoming["request_id"])
    assert (await caller.receive("app_result", "duplicate"))["payload"]["value"] == 1
    assert_routes_released(server)
    await caller.request("call_app", payload, "duplicate")
    repeated = await callee.receive("call_app")
    assert repeated["request_id"] != incoming["request_id"]
    await callee.request("app_result", {"value": 2}, repeated["request_id"])
    assert (await caller.receive("app_result", "duplicate"))["payload"]["value"] == 2
    assert_routes_released(server)


@pytest.mark.parametrize("transport", TRANSPORTS)
@pytest.mark.parametrize("recipient", ["missing", "foreign"])
async def test_ineligible_third_party_is_rejected_before_call_admission(daemon, transport, recipient):
    server, connect = daemon
    caller, callee, foreign = [await connect(transport) for _ in range(3)]
    await caller.join("caller")
    await callee.join("callee")
    await foreign.join("foreign", pool="foreign-pool")
    rejected = await caller.request("call_app", {
        "target_client_id": "callee", "event": "echo", "response_to": recipient,
    }, "invalid-recipient", response="error")
    assert rejected["payload"]["code"] != "route_not_found"
    assert_routes_released(server)
    await callee.request("hello", request_id="admission-barrier")
    assert not any(message["type"] == "call_app" for message in callee.pending)


@pytest.mark.parametrize("transport", TRANSPORTS)
@pytest.mark.parametrize("role", ["origin", "target", "response"])
@pytest.mark.parametrize("departure", ["disconnect", "switch"])
async def test_departure_terminally_fails_surviving_waiter(daemon, transport, role, departure):
    server, connect = daemon
    clients = {name: await connect(transport) for name in ["origin", "target", "response"]}
    for name, client in clients.items():
        await client.join(name)
    pool = server._pools["test-pool"]
    departing_session = pool.clients[role]
    await clients["origin"].request("call_app", {
        "target_client_id": "target", "event": "echo", "response_to": "response",
    }, "departing-route")
    incoming = await clients["target"].receive("call_app")
    old_generation = departing_session.generation
    if departure == "switch":
        await clients[role].request("switch_pool", {"client_id": role, "pool": "other-pool"})
        assert departing_session.generation > old_generation
        assert "other-pool" == departing_session.pool_id
    else:
        await clients[role].close()
    waiter = clients["origin"] if role == "response" else clients["response"]
    terminal = await waiter.receive("error", "departing-route")
    assert terminal["payload"]["request_id"] == "departing-route"
    assert terminal["payload"]["code"]
    assert terminal["payload"]["execution_uncertain"] is True
    assert role not in pool.clients
    assert_routes_released(server, departing_session)
    if role != "target":
        late = await clients["target"].request("app_result", {"value": "late"}, incoming["request_id"], response="error")
        assert late["payload"]["code"] == "route_not_found"
    await waiter.request("hello", request_id="terminal-once-barrier")
    assert not any(message["type"] in {"error", "app_result"} and message["request_id"] == "departing-route" for message in waiter.pending)


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_switch_back_cannot_revive_old_generation_route(daemon, transport):
    server, connect = daemon
    caller, callee = await connect(transport), await connect(transport)
    await caller.join("caller")
    await callee.join("callee")
    session = server._pools["test-pool"].clients["callee"]
    payload = {"target_client_id": "callee", "event": "echo"}
    await caller.request("call_app", payload, "generation-call")
    stale = await callee.receive("call_app")
    generation = session.generation
    await callee.request("switch_pool", {"client_id": "callee", "pool": "other-pool"})
    await caller.receive("error", "generation-call")
    await callee.request("switch_pool", {"client_id": "callee", "pool": "test-pool"})
    assert session.generation > generation
    await caller.request("call_app", payload, "generation-call")
    current = await callee.receive("call_app")
    assert current["request_id"] != stale["request_id"]
    rejected = await callee.request("app_result", {"value": "stale"}, stale["request_id"], response="error")
    assert rejected["payload"]["code"] == "route_not_found"
    assert current["request_id"] in server._pools["test-pool"].in_flight_requests
    await callee.request("app_result", {"value": "current"}, current["request_id"])
    assert (await caller.receive("app_result", "generation-call"))["payload"]["value"] == "current"
    assert_routes_released(server)


@pytest.mark.parametrize("transport", TRANSPORTS)
@pytest.mark.parametrize("call_type", ["call_app", "call_process"])
async def test_omitted_rpc_timeout_is_finite_and_expires_exactly_once(daemon, monkeypatch, transport, call_type):
    server, connect = daemon
    caller, callee = await connect(transport), await connect(transport)
    await caller.join("caller")
    await callee.join("callee")
    clock = manual_clock(monkeypatch)
    assert math.isfinite(server.config.rpc_timeout) and server.config.rpc_timeout > 0
    server.config.rpc_timeout = 5.0
    if call_type == "call_process":
        await callee.request("register_process", {"process_name": "echo"})
        payload = {"process_id": "callee:echo"}
    else:
        payload = {"target_client_id": "callee", "event": "echo"}
    await caller.request(call_type, payload, "default-deadline")
    incoming = await callee.receive("call_app")
    route = server._pools["test-pool"].in_flight_requests[incoming["request_id"]]
    assert route.expires_at is not None and math.isfinite(route.expires_at)
    assert route.expires_at == clock.now + server.config.rpc_timeout
    clock.now = route.expires_at
    await server._expire_routes()
    expired = await caller.receive("error", "default-deadline")
    assert expired["payload"]["code"] == "timeout"
    assert expired["payload"]["request_id"] == "default-deadline"
    assert expired["payload"]["execution_uncertain"] is True
    assert_routes_released(server)
    late = await callee.request("app_result", {"value": "too late"}, incoming["request_id"], response="error")
    assert late["payload"]["code"] == "route_not_found"
    await server._expire_routes()
    await caller.request("hello", request_id="expiry-once-barrier")
    assert not any(message["type"] in {"error", "app_result"} and message["request_id"] == "default-deadline" for message in caller.pending)


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_broadcast_child_ids_are_distinct_with_parent_correlation(daemon, transport):
    server, connect = daemon
    caller, first, second = [await connect(transport) for _ in range(3)]
    await caller.join("caller")
    await first.join("first")
    await second.join("second")
    for client in [first, second]:
        await client.request("register_process", {"process_name": "echo", "group_id": "shared-group"})
    accepted = await caller.request("broadcast_process", {"process_name": "echo", "data": 42}, "broadcast-parent")
    children = accepted["payload"]["request_ids"]
    assert set(accepted["payload"]["targets"]) == {"first:echo", "second:echo"}
    assert len(children) == len(set(children)) == 2
    assert "broadcast-parent" not in children
    invocations = [(client, await client.receive("call_app")) for client in [first, second]]
    assert {message["request_id"] for _, message in invocations} == set(children)
    for client, message in invocations:
        assert message["payload"]["event"] == f"{client.client_id}:echo"
    rejected = await caller.request("broadcast_process", {"process_name": "echo"}, "broadcast-parent", response="error")
    assert rejected["payload"]["code"] == "duplicate_request"
    caller_session = server._pools["test-pool"].clients["caller"]
    for index, (client, message) in enumerate(reversed(invocations)):
        await client.request("app_result", {"value": client.client_id, "error": None}, message["request_id"])
        result = await caller.receive("app_result", message["request_id"])
        assert result["request_id"] != "broadcast-parent"
        assert result["payload"]["request_id"] == message["request_id"]
        assert result["payload"]["parent_request_id"] == "broadcast-parent"
        assert result["payload"]["value"] == client.client_id
        assert ("broadcast-parent" in caller_session.active_requests) is (index == 0)
    assert_routes_released(server)


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_shared_group_owners_route_canonically_and_unregister_independently(daemon, transport):
    server, connect = daemon
    caller, first, second = [await connect(transport) for _ in range(3)]
    await caller.join("caller")
    await first.join("first")
    await second.join("second")
    owners = {"first:echo": first, "second:echo": second}
    for client in owners.values():
        registered = await client.request("register_process", {"process_name": "echo", "group_id": "legacy-group"})
        assert registered["payload"]["process_id"] == f"{client.client_id}:echo"
    pool = server._pools["test-pool"]
    assert pool.processes["first:echo"] is not pool.processes["second:echo"]
    assert {reg.owner_client_id for reg in pool.processes.values()} == {"first", "second"}
    assert set((await caller.request("list_processes"))["payload"]["processes"]) == set(owners)
    round_robin = []
    for index, process_id in enumerate(["first:echo", "second:echo", "echo", "echo"]):
        request_id = f"group-call-{index}"
        accepted = await caller.request("call_process", {"process_id": process_id}, request_id)
        selected = accepted["payload"]["process_id"]
        assert selected in owners
        if process_id != "echo":
            assert selected == process_id
        else:
            round_robin.append(selected)
        client = owners[selected]
        incoming = await client.receive("call_app")
        assert incoming["payload"]["event"] == selected
        assert pool.processes[selected].replicas[0].in_flight == 1
        await client.request("app_result", {"value": selected}, incoming["request_id"])
        assert (await caller.receive("app_result", request_id))["payload"]["value"] == selected
    assert set(round_robin) == set(owners)
    await caller.request("call_process", {"process_id": "first:echo"}, "first-pending")
    await first.receive("call_app")
    await caller.request("call_process", {"process_id": "second:echo"}, "second-pending")
    second_call = await second.receive("call_app")
    await first.request("unregister_process", {"process_name": "echo"})
    assert (await caller.receive("error", "first-pending"))["payload"]["code"] == "process_unregistered"
    assert "second:echo" in pool.processes and "first:echo" not in pool.processes
    await second.request("app_result", {"value": "still owned by second"}, second_call["request_id"])
    assert (await caller.receive("app_result", "second-pending"))["payload"]["value"] == "still owned by second"
    missing = await caller.request("call_process", {"process_id": "first:echo"}, response="error")
    assert missing["payload"]["code"] == "process_not_found"
    accepted = await caller.request("call_process", {"process_id": "echo"}, "remaining-owner")
    assert accepted["payload"]["process_id"] == "second:echo"
    incoming = await second.receive("call_app")
    assert incoming["payload"]["event"] == "second:echo"
    await second.request("app_result", {"value": "second"}, incoming["request_id"])
    await caller.receive("app_result", "remaining-owner")
    await second.close()
    await departure_barrier(caller, "second")
    assert (await caller.request("list_processes"))["payload"]["processes"] == {}
    assert_routes_released(server)


MALFORMED_MESSAGES = [
    "[]", "null", "42", '"text"', "{", '{"type": []}', '{"type": {}}',
    '{"type": null}', '{"type": 3}', '{"type": ""}',
    '{"type": "hello", "payload": []}', '{"type": "hello", "payload": true}',
    '{"type": "hello", "request_id": []}', '{"type": "hello", "client_id": {}}',
    '{"type": "hello", "pool": []}',
    '{"type": "set_buffer", "payload": {"key": "bad", "value": NaN}}',
    '{"type": "set_buffer", "payload": {"key": "bad", "value": Infinity}}',
    '{"type": "set_buffer", "payload": {"key": "bad", "value": 1e400}}',
]


@pytest.mark.parametrize("transport", TRANSPORTS)
@pytest.mark.parametrize("raw", MALFORMED_MESSAGES)
async def test_malformed_objects_types_payloads_and_numbers_are_protocol_errors(daemon, transport, raw):
    server, connect = daemon
    client = await connect(transport)
    await client.join("client")
    before = server._metrics["dispatched"]
    await send_raw(client, raw)
    rejected = await client.receive("error")
    assert rejected["payload"]["code"] == "protocol_error"
    assert not server._pools["test-pool"].buffers
    assert server._metrics["dispatched"] == before
    assert (await client.request("hello"))["payload"]["server"] == "latzero-server"


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_invalid_tcp_bytes_and_websocket_binary_frames_are_rejected(daemon, transport):
    server, connect = daemon
    client = await connect(transport)
    await client.join("client")
    raw = b"\xff\xfe" if transport == "tcp" else b'{"type":"set_buffer","payload":{"key":"binary","value":1}}'
    await send_raw(client, raw)
    rejected = await client.receive("error")
    assert rejected["payload"]["code"] == "protocol_error"
    assert not server._pools["test-pool"].buffers
    await client.request("hello", request_id="binary-rejection-barrier")


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_pool_mismatch_rejects_and_null_pool_uses_actual_session_identity(daemon, transport):
    server, connect = daemon
    client = await connect(transport)
    await client.join("actual")
    request = envelope(client, "set_buffer", "mismatched", {"key": "value", "value": 1}, pool="foreign-pool")
    request["client_id"] = "forged"
    await client.send(request)
    assert (await client.receive("error", "mismatched"))["payload"]["code"] == "pool_mismatch"
    assert not server._pools["test-pool"].buffers
    request.update(request_id="null-pool", pool=None)
    await client.send(request)
    await client.receive("ack", "null-pool")
    entry = (await client.request("get_buffer", {"key": "value"}))["payload"]["entry"]
    assert entry["value"] == 1 and entry["updated_by"] == "actual"
    await client.send({"type": "list_buffers", "request_id": "null-payload", "pool": None, "payload": None})
    assert (await client.receive("ack", "null-payload"))["payload"]["keys"] == ["value"]
    await client.send(envelope(client, "not-a-handler", "unknown-type"))
    assert (await client.receive("error", "unknown-type"))["payload"]["code"] == "unknown_message_type"


@pytest.mark.parametrize("transport", TRANSPORTS)
@pytest.mark.parametrize("ttl", [-1, True, "1", [], {}])
async def test_invalid_ttl_does_not_create_or_replace_buffer(daemon, transport, ttl):
    server, connect = daemon
    client = await connect(transport)
    await client.join("client")
    await client.request("set_buffer", {"key": "kept", "value": "original"})
    pool = server._pools["test-pool"]
    original = pool.buffers["kept"]
    bytes_before, heap_before = pool.buffer_bytes, list(server._expiry_heap)
    for key in ["kept", "new"]:
        rejected = await client.request("set_buffer", {"key": key, "value": "invalid", "ttl": ttl}, response="error")
        assert rejected["payload"]["code"] == "dispatch_error"
        assert pool.buffers == {"kept": original}
        assert pool.buffers["kept"] is original
        assert pool.buffer_bytes == bytes_before
        assert server._expiry_heap == heap_before
    read = await client.request("get_buffer", {"key": "kept"})
    assert read["payload"]["entry"]["value"] == "original"
    assert read["payload"]["entry"]["version"] == 1


@pytest.mark.parametrize("transport", TRANSPORTS)
@pytest.mark.parametrize("operation", ["get_buffer", "list_buffers"])
async def test_reads_and_lists_enforce_logical_ttl_expiry(tmp_path, monkeypatch, transport, operation):
    async with server_with_limits(tmp_path) as (server, connect):
        client = await connect(transport)
        await client.join("client")
        clock = manual_clock(monkeypatch)
        await client.request("set_buffer", {"key": "expired", "value": "old", "ttl": 5})
        await client.request("set_buffer", {"key": "live", "value": "kept"})
        pool = server._pools["test-pool"]
        assert pool.buffers["expired"].expires_at == clock.now + 5
        clock.now += 5
        assert "expired" in pool.buffers
        result = await client.request(operation, {"key": "expired"} if operation == "get_buffer" else {})
        if operation == "get_buffer":
            assert result["payload"]["exists"] is False
            assert result["payload"]["entry"] is None
        else:
            assert result["payload"]["keys"] == ["live"]
        assert "expired" not in pool.buffers
        await client.request("set_buffer", {"key": "expired", "value": "new", "ttl": 100})
        await server._expire_buffers()
        assert (await client.request("get_buffer", {"key": "expired"}))["payload"]["entry"]["value"] == "new"


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_zero_ttl_is_immediately_logically_expired(daemon, transport):
    _, connect = daemon
    client = await connect(transport)
    await client.join("client")
    await client.request("set_buffer", {"key": "zero", "value": "not visible", "ttl": 0})
    assert (await client.request("get_buffer", {"key": "zero"}))["payload"]["exists"] is False
    assert (await client.request("list_buffers"))["payload"]["keys"] == []


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_same_identity_rejoin_is_idempotent_and_different_identity_never_aliases(daemon, transport):
    server, connect = daemon
    client, target = await connect(transport), await connect(transport)
    await client.join("stable")
    await target.join("target")
    await client.request("register_process", {"process_name": "echo"})
    await client.request("subscribe_buffer", {"key": "watched"})
    await client.request("call_app", {"target_client_id": "target", "event": "echo"}, "rejoin-pending")
    invocation = await target.receive("call_app")
    pool = server._pools["test-pool"]
    session, registration = pool.clients["stable"], pool.processes["stable:echo"]
    generation = session.generation
    await client.request("join_pool", {"client_id": "stable", "pool": "test-pool"})
    assert session.generation == generation
    assert pool.clients["stable"] is session
    assert pool.processes["stable:echo"] is registration
    assert pool.subscription_count == 1
    assert pool.subscriptions == {"watched": {"stable"}}
    assert session.active_requests == {"rejoin-pending"}
    for destination in ["test-pool", "new-pool"]:
        rejected = await client.request("join_pool", {"client_id": "alias", "pool": destination}, response="error")
        assert rejected["payload"]["code"] == "identity_change"
        assert all("alias" not in candidate.clients for candidate in server._pools.values())
        assert pool.clients["stable"] is session
    await target.request("app_result", {"value": "kept"}, invocation["request_id"])
    await client.receive("app_result", "rejoin-pending")
    await client.request("leave_pool")
    rejected = await client.request("join_pool", {"client_id": "alias", "pool": "test-pool"}, response="error")
    assert rejected["payload"]["code"] == "identity_change"
    assert session.client_id == "stable"
    assert all("alias" not in candidate.clients for candidate in server._pools.values())
    assert_routes_released(server)


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_same_session_pipelined_join_set_get_and_notifications_are_ordered(daemon, transport):
    server, connect = daemon
    subscriber = await connect(transport)
    await subscriber.join("subscriber")
    await subscriber.request("subscribe_buffer", {"key": "ordered"})
    client = await connect(transport)
    messages = [
        envelope(client, "hello", "pipeline-hello"),
        envelope(client, "join_pool", "pipeline-join", {"client_id": "pipeline", "pool": "test-pool"}),
        envelope(client, "set_buffer", "pipeline-first", {"key": "ordered", "value": 1}),
        envelope(client, "set_buffer", "pipeline-second", {"key": "ordered", "value": 2}),
        envelope(client, "get_buffer", "pipeline-get", {"key": "ordered"}),
    ]
    if transport == "tcp":
        client.writer.write(("\n".join(json.dumps(message) for message in messages) + "\n").encode())
        await client.writer.drain()
    else:
        for message in messages:
            await client.send(message)
    acknowledgements = []
    for _ in range(len(messages) + 4):
        message = await wire_message(client)
        assert message["type"] != "error", message
        if message["type"] == "ack":
            acknowledgements.append(message)
        if len(acknowledgements) == len(messages):
            break
    assert [message["request_id"] for message in acknowledgements] == [message["request_id"] for message in messages]
    assert acknowledgements[2]["payload"]["version"] == 1
    assert acknowledgements[3]["payload"]["version"] == 2
    assert acknowledgements[4]["payload"]["entry"]["value"] == 2
    updates = [await subscriber.receive("buffer_update"), await subscriber.receive("buffer_update")]
    assert [update["payload"]["entry"]["version"] for update in updates] == [1, 2]
    assert [update["payload"]["entry"]["value"] for update in updates] == [1, 2]
    assert server._pools["test-pool"].clients["pipeline"].client_id == "pipeline"


async def test_membership_and_buffer_mutation_handlers_do_not_suspend(daemon):
    server, connect = daemon
    client = await connect()
    await client.join("stable")
    session = server._pools["test-pool"].clients["stable"]
    operations = [
        ("join_pool", {"client_id": "stable", "pool": "test-pool"}),
        ("set_buffer", {"key": "atomic", "value": 1}),
        ("get_buffer", {"key": "atomic"}),
        ("subscribe_buffer", {"key": "atomic"}),
        ("set_buffer", {"key": "atomic", "value": 2}),
        ("unsubscribe_buffer", {"key": "atomic"}),
        ("delete_buffer", {"key": "atomic"}),
        ("switch_pool", {"client_id": "stable", "pool": "other-pool"}),
        ("leave_pool", {}),
    ]
    for index, (msg_type, payload) in enumerate(operations):
        request = envelope(client, msg_type, f"no-suspend-{index}", payload)
        coroutine = server._dispatch_table[msg_type](session, request)
        try:
            with pytest.raises(StopIteration):
                coroutine.send(None)
        finally:
            coroutine.close()
        await client.receive("ack", request["request_id"])
    assert session.client_id == "stable" and session.pool_id is None


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_disconnect_discards_already_admitted_stale_session_mutations(daemon, monkeypatch, transport):
    server, connect = daemon
    client, observer = await connect(transport), await connect(transport)
    await client.join("departing")
    await observer.join("observer")
    pool = server._pools["test-pool"]
    session = pool.clients["departing"]
    entered, release, admitted = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original_hello, original_ingest = server._dispatch_table["hello"], server._ingest

    async def held_hello(current, message):
        if current is session:
            entered.set()
            await release.wait()
        await original_hello(current, message)

    async def observe_ingress(current, raw, size):
        await original_ingest(current, raw, size)
        if current is session and json.loads(raw).get("request_id") == "stale-mutation":
            admitted.set()

    monkeypatch.setitem(server._dispatch_table, "hello", held_hello)
    monkeypatch.setattr(server, "_ingest", observe_ingress)
    try:
        await client.send(envelope(client, "hello", "held-dispatch"))
        await asyncio.wait_for(entered.wait(), 2)
        await client.send(envelope(client, "set_buffer", "stale-mutation", {"key": "must-not-exist", "value": 1}))
        await asyncio.wait_for(admitted.wait(), 2)
        await client.close()
        await departure_barrier(observer, "departing")
        release.set()
        await asyncio.wait_for(server._worker_pool.join(), 2)
        assert session.closed or session.closing
        assert "departing" not in pool.clients
        assert "must-not-exist" not in pool.buffers
    finally:
        release.set()


@pytest.mark.parametrize("transport", TRANSPORTS)
@pytest.mark.parametrize("bound", ["count", "bytes"])
async def test_buffer_limits_reject_before_mutation_and_recover(daemon, transport, bound):
    server, connect = daemon
    client = await connect(transport)
    await client.join("client")
    pool = server._pools["test-pool"]
    if bound == "count":
        server.config.max_buffers_per_pool = 1
    else:
        server.config.max_pool_bytes = 1024
    await client.request("set_buffer", {"key": "kept", "value": "small"})
    original, size = pool.buffers["kept"], pool.buffer_bytes
    payload = {"key": "other", "value": "small"} if bound == "count" else {"key": "kept", "value": "x" * 1024}
    rejected = await client.request("set_buffer", payload, response="error")
    assert rejected["payload"]["code"] == "overloaded"
    assert pool.buffers == {"kept": original} and pool.buffers["kept"] is original
    assert pool.buffer_bytes == size <= server.config.max_pool_bytes
    await client.request("delete_buffer", {"key": "kept"})
    assert not pool.buffers and pool.buffer_bytes == 0
    await client.request("set_buffer", {"key": "other", "value": "small"})
    assert (await client.request("list_buffers"))["payload"]["keys"] == ["other"]


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_subscription_limit_is_idempotent_and_disconnect_releases_capacity(daemon, transport):
    server, connect = daemon
    first, second = await connect(transport), await connect(transport)
    await first.join("first")
    await second.join("second")
    server.config.max_subscriptions_per_pool = 1
    pool = server._pools["test-pool"]
    await first.request("subscribe_buffer", {"key": "watched"})
    await first.request("subscribe_buffer", {"key": "watched"})
    assert pool.subscription_count == 1
    for client, key in [(first, "other"), (second, "watched")]:
        rejected = await client.request("subscribe_buffer", {"key": key}, response="error")
        assert rejected["payload"]["code"] == "overloaded"
        assert pool.subscriptions == {"watched": {"first"}} and pool.subscription_count == 1
    await first.request("unsubscribe_buffer", {"key": "watched"})
    assert not pool.subscriptions and pool.subscription_count == 0
    await second.request("subscribe_buffer", {"key": "other"})
    await second.close()
    await departure_barrier(first, "second")
    assert not pool.subscriptions and pool.subscription_count == 0
    await first.request("subscribe_buffer", {"key": "recovered"})
    assert pool.subscription_count == 1


@pytest.mark.parametrize("transport", TRANSPORTS)
@pytest.mark.parametrize("bound", ["session", "pool", "global"])
async def test_route_limits_do_not_leak_routes_or_process_counters(daemon, transport, bound):
    server, connect = daemon
    first, second, callee = [await connect(transport) for _ in range(3)]
    await first.join("first")
    await second.join("second", pool="other-pool" if bound == "global" else "test-pool")
    await callee.join("callee")
    await callee.request("register_process", {"process_name": "echo"})
    if bound == "global":
        second_target = await connect(transport)
        await second_target.join("other-callee", pool="other-pool")
        await second_target.request("register_process", {"process_name": "echo"})
        second_process = "other-callee:echo"
        server.config.max_routes = 1
    else:
        second_target, second_process = callee, "callee:echo"
        if bound == "session":
            second = first
            server.config.max_routes_per_session = 1
        else:
            server.config.max_routes_per_pool = 1
    await first.request("call_process", {"process_id": "callee:echo"}, "admitted")
    incoming = await callee.receive("call_app")
    rejected = await second.request("call_process", {"process_id": second_process}, "rejected", response="error")
    assert rejected["payload"]["code"] == "overloaded"
    assert server._route_count == 1
    assert sum(len(pool.in_flight_requests) for pool in server._pools.values()) == 1
    assert server._pools["test-pool"].processes["callee:echo"].replicas[0].in_flight == 1
    await second_target.request("hello", request_id="limit-barrier")
    assert not any(message["type"] == "call_app" for message in second_target.pending)
    await callee.request("app_result", {"value": "first"}, incoming["request_id"])
    await first.receive("app_result", "admitted")
    assert_routes_released(server)
    await second.request("call_process", {"process_id": second_process}, "rejected")
    recovered = await second_target.receive("call_app")
    await second_target.request("app_result", {"value": "recovered"}, recovered["request_id"])
    assert (await second.receive("app_result", "rejected"))["payload"]["value"] == "recovered"
    assert_routes_released(server)


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_completion_control_ingress_is_admitted_when_regular_budget_is_full(tmp_path, monkeypatch, transport):
    async with server_with_limits(tmp_path, max_session_messages=2, max_queue_messages=2,
                                  control_reserve_messages=1, control_reserve_bytes=1024) as (server, connect):
        caller, callee = await connect(transport), await connect(transport)
        await caller.join("caller")
        await callee.join("callee")
        await caller.request("call_app", {"target_client_id": "callee", "event": "echo"}, "reserved-completion")
        incoming = await callee.receive("call_app")
        session = server._pools["test-pool"].clients["callee"]
        entered, release, completion_admitted = asyncio.Event(), asyncio.Event(), asyncio.Event()
        original_hello, original_submit = server._dispatch_table["hello"], server._worker_pool.submit
        admission = []

        async def held_hello(current, message):
            if current is session and message["request_id"] == "regular-active":
                entered.set()
                await release.wait()
            await original_hello(current, message)

        async def observe_submit(current, message, byte_size=None):
            accepted = await original_submit(current, message, byte_size)
            if current is session and message["type"] == "app_result":
                admission.append(accepted)
                completion_admitted.set()
            return accepted

        monkeypatch.setitem(server._dispatch_table, "hello", held_hello)
        monkeypatch.setattr(server._worker_pool, "submit", observe_submit)
        try:
            await callee.send(envelope(callee, "hello", "regular-active"))
            await asyncio.wait_for(entered.wait(), 2)
            await callee.send(envelope(callee, "hello", "regular-queued"))
            rejected = await callee.request("hello", request_id="regular-rejected", response="error")
            assert rejected["payload"]["code"] == "overloaded"
            await callee.send(envelope(callee, "app_result", incoming["request_id"], {"value": "completed under pressure"}))
            await asyncio.wait_for(completion_admitted.wait(), 2)
            assert admission == [True]
            assert server._worker_pool.stats.queue_depth == 3
            release.set()
            assert (await caller.receive("app_result", "reserved-completion"))["payload"]["value"] == "completed under pressure"
            assert (await callee.receive("ack", incoming["request_id"]))["payload"]["delivered"] is True
            await asyncio.wait_for(server._worker_pool.join(), 2)
            assert_routes_released(server)
        finally:
            release.set()


async def test_outbox_control_reserve_is_bounded_accounted_and_fifo(daemon):
    server, connect = daemon
    owner = await connect()
    await owner.join("owner")
    await drain_outboxes(server)
    server.config.max_outbox_messages = 1
    server.config.outbox_control_messages = 1
    session, writer = controlled_session(server, server._pools["test-pool"])
    try:
        await block_outbox(server, session, writer)
        assert session.outbox_messages == 1
        control = {"type": "error", "request_id": "control", "pool": "test-pool", "payload": {"code": "test"}}
        assert server._reserve_message(session, control, control=True) is not None
        assert session.outbox_messages == 2
        assert session.outbox_bytes <= server.config.max_outbox_bytes + server.config.outbox_control_bytes
        assert session.outbox_bytes <= server._outbox_bytes <= server.config.max_global_outbox_bytes
        writer.release.set()
        await asyncio.wait_for(session.outbox_drained.wait(), 2)
        assert [frame["request_id"] for frame in writer.frames] == ["pressure", "control"]
        assert session.outbox_messages == session.outbox_bytes == 0
        assert not session.outbox
    finally:
        writer.release.set()
        await server._close_session(session)


async def test_exhausted_control_reserve_closes_session_and_releases_accounting(daemon):
    server, connect = daemon
    owner = await connect()
    await owner.join("owner")
    await drain_outboxes(server)
    server.config.max_outbox_messages = 1
    server.config.outbox_control_messages = 1
    session, writer = controlled_session(server, server._pools["test-pool"])
    try:
        await block_outbox(server, session, writer)
        control = {"type": "error", "request_id": "reserved-control", "payload": {"code": "test"}}
        assert server._reserve_message(session, control, control=True) is not None
        assert session.outbox_messages == 2
        server.config.write_timeout = 0.05
        assert server._reserve_message(session, control, control=True) is None
        assert session.closing
        await asyncio.wait_for(writer.closed.wait(), 2)
        await server._close_session(session)
        assert session.closed
        assert session.outbox_messages == session.outbox_bytes == 0
        assert not session.outbox and id(session) not in server._sessions
        await drain_outboxes(server)
        assert server._outbox_bytes == 0
    finally:
        writer.release.set()
        await server._close_session(session)


@pytest.mark.parametrize("bound", ["messages", "bytes", "global"])
async def test_slow_outbox_rejects_call_without_false_acceptance_or_route_leak(daemon, bound):
    server, connect = daemon
    caller = await connect()
    await caller.join("caller")
    await drain_outboxes(server)
    pool = server._pools["test-pool"]
    session, writer = controlled_session(server, pool)
    try:
        await block_outbox(server, session, writer, messages=2)
        server.config.write_timeout = 0.05
        payload = {"target_client_id": "blocked", "event": "echo"}
        if bound == "messages":
            server.config.max_outbox_messages = 2
        elif bound == "bytes":
            server.config.max_outbox_bytes = session.outbox_bytes
        else:
            await asyncio.wait_for(pool.clients["caller"].outbox_drained.wait(), 2)
            server.config.max_global_outbox_bytes = server._outbox_bytes + 1024
            payload["data"] = "x" * 2048
        rejected = await caller.request("call_app", payload, "not-accepted", response="error")
        assert rejected["payload"]["code"] == "delivery_failed"
        await asyncio.wait_for(writer.closed.wait(), 2)
        assert session.closed or session.closing
        assert not any(frame["type"] == "call_app" for frame in writer.frames)
        await caller.request("hello", request_id="no-false-ack-barrier")
        assert not any(message["type"] == "ack" and message["request_id"] == "not-accepted" for message in caller.pending)
        assert_routes_released(server, session)
    finally:
        writer.release.set()
        await server._close_session(session)


async def test_rpc_result_uses_reserved_outbox_capacity_without_waiting_for_drain(daemon):
    server, connect = daemon
    caller, callee = await connect(), await connect()
    await caller.join("caller")
    await callee.join("callee")
    await drain_outboxes(server)
    pool = server._pools["test-pool"]
    response, writer = controlled_session(server, pool, "response")
    server.config.max_outbox_messages = 1
    server.config.outbox_control_messages = 1
    try:
        await block_outbox(server, response, writer)
        await caller.request("call_app", {"target_client_id": "callee", "event": "echo", "response_to": "response"}, "control-result")
        incoming = await callee.receive("call_app")
        completed = await callee.request("app_result", {"value": 42, "error": None}, incoming["request_id"])
        assert completed["payload"]["delivered"] is True
        assert response.outbox_messages == 2
        assert not any(frame["type"] == "app_result" for frame in writer.frames)
        assert_routes_released(server)
        writer.release.set()
        await asyncio.wait_for(response.outbox_drained.wait(), 2)
        result = next(frame for frame in writer.frames if frame["type"] == "app_result")
        assert result["request_id"] == result["payload"]["request_id"] == "control-result"
        assert result["payload"]["value"] == 42
    finally:
        writer.release.set()
        await server._close_session(response)


async def test_transport_above_high_water_mark_does_not_silently_skip_accepted_call(daemon):
    server, connect = daemon
    caller = await connect()
    await caller.join("caller")
    target, writer = controlled_session(server, server._pools["test-pool"])
    writer.buffer_size = server.config.connection_hwm_bytes + 1
    assert writer.buffer_size < server.config.connection_critical_bytes
    try:
        accepted = await caller.request("call_app", {"target_client_id": "blocked", "event": "echo"}, "above-hwm")
        assert accepted["payload"]["queued"] is True
        await asyncio.wait_for(target.outbox_drained.wait(), 2)
        incoming = next(frame for frame in writer.frames if frame["type"] == "call_app")
        await server._handle_app_result(target, {
            "type": "app_result", "request_id": incoming["request_id"], "pool": None,
            "payload": {"value": "not skipped", "error": None},
        })
        assert (await caller.receive("app_result", "above-hwm"))["payload"]["value"] == "not skipped"
        assert_routes_released(server)
    finally:
        await server._close_session(target)


async def test_transport_critical_limit_before_write_does_not_claim_uncertain_execution(daemon):
    server, connect = daemon
    caller = await connect()
    await caller.join("caller")
    target, writer = controlled_session(server, server._pools["test-pool"])
    writer.buffer_size = server.config.connection_critical_bytes + 1
    try:
        await caller.request("call_app", {"target_client_id": "blocked", "event": "must-not-execute"}, "critical-before-write")
        terminal = await caller.receive("error", "critical-before-write")
        assert not any(frame["type"] == "call_app" for frame in writer.frames)
        assert terminal["payload"]["request_id"] == "critical-before-write"
        assert terminal["payload"]["execution_uncertain"] is False
        assert_routes_released(server, target)
    finally:
        await server._close_session(target)


@pytest.mark.parametrize("termination", ["timeout", "origin_switch", "target_switch", "target_failure"])
async def test_terminated_route_queued_before_transmission_cannot_execute_later(daemon, termination):
    server, connect = daemon
    caller, response = await connect(), await connect()
    await caller.join("caller")
    await response.join("response")
    pool = server._pools["test-pool"]
    target, writer = controlled_session(server, pool)
    try:
        await block_outbox(server, target, writer)
        await caller.request("call_app", {"target_client_id": "blocked", "event": "must-not-execute", "response_to": "response"}, "queued-route")
        route = next(iter(pool.in_flight_requests.values()))
        assert not route.sent
        assert not any(frame["type"] == "call_app" for frame in writer.frames)
        if termination == "timeout":
            route.expires_at = time.monotonic() - 1
            await server._expire_routes()
        elif termination == "origin_switch":
            await caller.request("switch_pool", {"client_id": "caller", "pool": "other-pool"})
        elif termination == "target_failure":
            server._fail_session(target, "injected slow consumer failure")
        else:
            await server._handle_switch_pool(target, {
                "type": "switch_pool", "request_id": "target-switch", "pool": None,
                "payload": {"client_id": "blocked", "pool": "other-pool"},
            })
        terminal = await response.receive("error", "queued-route")
        assert terminal["payload"]["request_id"] == "queued-route"
        assert terminal["payload"]["execution_uncertain"] is False
        assert_routes_released(server)
        writer.release.set()
        await asyncio.wait_for(target.outbox_drained.wait(), 2)
        assert not any(frame["type"] == "call_app" and frame["request_id"] == route.request_id for frame in writer.frames)
    finally:
        writer.release.set()
        await server._close_session(target)


async def test_partial_event_delivery_reports_actual_acceptance_without_success_ack(daemon):
    server, connect = daemon
    sender, healthy = await connect(), await connect("ws")
    await sender.join("sender")
    await healthy.join("healthy")
    await drain_outboxes(server)
    pool = server._pools["test-pool"]
    blocked, writer = controlled_session(server, pool)
    server.config.max_outbox_messages = 2
    try:
        await block_outbox(server, blocked, writer, messages=2)
        server.config.write_timeout = 0.05
        result = await sender.request("emit_event", {"event": "once", "data": 42}, "partial-event", response="error")
        assert result["payload"]["code"] == "partial_delivery"
        assert result["payload"]["accepted"] == ["healthy"]
        assert result["payload"]["failed"] == ["blocked"]
        assert result["payload"].get("delivered") is not True
        event = await healthy.receive("emit_event", "partial-event")
        assert event["payload"]["data"] == 42
        await asyncio.wait_for(writer.closed.wait(), 2)
        assert not any(frame["type"] == "emit_event" and frame["request_id"] == "partial-event" for frame in writer.frames)
        await sender.request("hello", request_id="partial-ack-barrier")
        assert not any(message["type"] == "ack" and message["request_id"] == "partial-event" for message in sender.pending)
        await healthy.request("hello", request_id="one-event-barrier")
        assert not any(message["type"] == "emit_event" and message["request_id"] == "partial-event" for message in healthy.pending)
    finally:
        writer.release.set()
        await server._close_session(blocked)


async def test_buffer_commit_ack_survives_failed_notification_and_slow_subscriber_disconnects(daemon):
    server, connect = daemon
    owner, healthy = await connect(), await connect("ws")
    await owner.join("owner")
    await healthy.join("healthy")
    await healthy.request("subscribe_buffer", {"key": "committed"})
    await drain_outboxes(server)
    pool = server._pools["test-pool"]
    blocked, writer = controlled_session(server, pool)
    await server._handle_subscribe_buffer(blocked, {
        "type": "subscribe_buffer", "request_id": "fake-subscribe", "pool": None,
        "payload": {"key": "committed"},
    })
    await asyncio.wait_for(blocked.outbox_drained.wait(), 2)
    writer.drain_entered.clear()
    server.config.max_outbox_messages = 2
    try:
        await block_outbox(server, blocked, writer, messages=2)
        server.config.write_timeout = 0.05
        written = await owner.request("set_buffer", {"key": "committed", "value": 42}, "commit-with-failed-notification")
        assert written["payload"] == {"key": "committed", "version": 1}
        update = await healthy.receive("buffer_update")
        assert update["payload"]["entry"]["value"] == 42
        await asyncio.wait_for(writer.closed.wait(), 2)
        if blocked.close_task is not None:
            await asyncio.wait_for(asyncio.shield(blocked.close_task), 2)
        assert pool.buffers["committed"].value == 42
        assert "blocked" not in pool.clients
        assert pool.subscriptions == {"committed": {"healthy"}}
        assert pool.subscription_count == 1
        assert not any(frame["type"] == "buffer_update" for frame in writer.frames)
    finally:
        writer.release.set()
        await server._close_session(blocked)


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_oversized_response_becomes_small_correlated_error_without_closing_client(daemon, transport):
    server, connect = daemon
    client = await connect(transport)
    await client.join("client")
    await client.request("set_buffer", {"key": "large", "value": "x" * 768})
    await drain_outboxes(server)
    server.config.max_frame_bytes = 512
    rejected = await client.request("get_buffer", {"key": "large"}, "large-response", response="error")
    assert rejected["request_id"] == "large-response"
    assert rejected["payload"]["code"] == "response_too_large"
    assert len(json.dumps(rejected).encode("utf-8")) <= server.config.max_frame_bytes
    assert server._pools["test-pool"].buffers["large"].value == "x" * 768
    await client.request("set_buffer", {"key": "large", "value": "small"})
    assert (await client.request("get_buffer", {"key": "large"}))["payload"]["entry"]["value"] == "small"
    assert not any(message["type"] == "ack" and message["request_id"] == "large-response" for message in client.pending)


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_oversized_frames_are_rejected_and_connection_closes(daemon, transport):
    server, connect = daemon
    client = await connect(transport)
    await client.join("client")
    server.config.max_frame_bytes = 512
    await client.send(envelope(client, "hello", "oversized", {"padding": "x" * 768}))
    rejected = await client.receive("error")
    assert rejected["payload"]["code"] == "frame_too_large"
    await assert_connection_closed(client)


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_connection_limit_rejects_before_session_admission(daemon, transport):
    server, connect = daemon
    server.config.max_connections = 1
    admitted = await connect(transport)
    await admitted.join("admitted")
    if transport == "ws":
        from websockets.exceptions import InvalidStatusCode
        with pytest.raises(InvalidStatusCode) as rejected:
            await connect(transport)
        assert rejected.value.status_code == 503
    else:
        rejected = await connect(transport)
        error = await rejected.receive("error")
        assert error["payload"]["code"] == "server_busy"
        await assert_connection_closed(rejected)
    assert server._connection_count == len(server._sessions) == 1
    assert (await admitted.request("list_clients"))["payload"]["clients"] == ["admitted"]


@pytest.mark.parametrize("transport", TRANSPORTS)
@pytest.mark.parametrize("hello", [False, True])
async def test_join_deadline_closes_unjoined_connections_even_after_hello(daemon, monkeypatch, transport, hello):
    server, connect = daemon
    server.config.join_timeout = 0.05
    finished, start_deadline = asyncio.Event(), asyncio.Event()
    original_close, original_deadline = server._finish_close, server._join_deadline

    async def observe_close(session):
        await original_close(session)
        finished.set()

    async def gated_deadline(session):
        await start_deadline.wait()
        await original_deadline(session)

    monkeypatch.setattr(server, "_finish_close", observe_close)
    monkeypatch.setattr(server, "_join_deadline", gated_deadline)
    client = await connect(transport)
    try:
        if hello:
            await client.request("hello")
        start_deadline.set()
        error = await client.receive("error")
        assert error["payload"]["code"] == "join_timeout"
        await assert_connection_closed(client)
        await asyncio.wait_for(finished.wait(), 2)
        assert server._connection_count == 0 and not server._sessions
        assert not server._pools
    finally:
        start_deadline.set()


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_joined_session_satisfies_deadline_without_future_disconnect(daemon, transport):
    server, connect = daemon
    client = await connect(transport)
    await client.join("joined")
    session = server._pools["test-pool"].clients["joined"]
    await asyncio.wait_for(server._join_deadline(session), 2)
    await client.request("hello", request_id="joined-deadline-barrier")
    assert not session.closed and not session.closing
    assert not any(message["type"] == "error" for message in client.pending)


async def test_websocket_handshake_has_finite_deadline_before_session_creation(tmp_path):
    async with server_with_limits(tmp_path, join_timeout=0.05) as (server, _):
        port = server._websocket_server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            assert await asyncio.wait_for(reader.read(), 2) == b""
            assert server._connection_count == 0 and not server._sessions
        finally:
            writer.close()
            await writer.wait_closed()


async def test_partial_startup_failure_rolls_back_and_allows_idempotent_stop_restart(tmp_path, monkeypatch):
    server = LatZeroServer(ServerConfig(
        port=0, websocket_port=0, data_dir=tmp_path,
        min_workers=1, max_workers=2, shutdown_timeout=0.2,
    ))
    loop_handler = asyncio.get_running_loop().get_exception_handler()
    opened = []

    async def failing_websocket_start(*args, **kwargs):
        opened.append(server._tcp_server)
        assert opened[-1] is not None and opened[-1].is_serving()
        raise OSError("injected WebSocket bind failure")

    try:
        with monkeypatch.context() as patch:
            patch.setattr(server_module, "serve", failing_websocket_start)
            with pytest.raises(OSError, match="injected WebSocket bind failure"):
                await asyncio.wait_for(server.start(), 3)
        assert opened and not opened[0].is_serving()
        assert server._tcp_server is server._websocket_server is None
        assert server._cleanup_task is server._fanout_task is None
        assert not server._accepting and not server._sessions
        assert all(task.done() for task in server._background)
        assert server._worker_pool.stats.active_workers == 0
        assert server._store._saver_task is None or server._store._saver_task.done()
        assert asyncio.get_running_loop().get_exception_handler() is loop_handler
        await asyncio.wait_for(asyncio.gather(server.stop(), server.stop()), 3)
        await server.start()
        tcp_server, ws_server = server._tcp_server, server._websocket_server
        await server.start()
        assert server._tcp_server is tcp_server and server._websocket_server is ws_server
        port = tcp_server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        client = RawClient(reader=reader, writer=writer)
        try:
            await client.join("after-rollback")
            assert (await client.request("list_clients"))["payload"]["clients"] == ["after-rollback"]
        finally:
            await client.close()
    finally:
        await asyncio.wait_for(server.stop(), 3)
    assert server._connection_count == server._outbox_bytes == 0
    assert not server._sessions and not server._background


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_stop_fails_pending_routes_closes_peers_and_restart_has_no_stale_identity(daemon, transport):
    server, connect = daemon
    caller, callee = await connect(transport), await connect(transport)
    await caller.join("caller")
    await callee.join("callee")
    await caller.request("set_buffer", {"key": "retained", "value": 42})
    await caller.request("call_app", {"target_client_id": "callee", "event": "unfinished"}, "shutdown-route")
    await callee.receive("call_app")
    old_session = server._pools["test-pool"].clients["caller"]
    server.config.shutdown_timeout = 0.05
    stopping = asyncio.gather(server.stop(), server.stop())
    terminal = await caller.receive("error", "shutdown-route")
    assert terminal["payload"]["code"] == "server_stopping"
    assert terminal["payload"]["request_id"] == "shutdown-route"
    await asyncio.wait_for(stopping, 3)
    await assert_connection_closed(caller)
    await assert_connection_closed(callee)
    assert old_session.closed
    assert server._connection_count == server._outbox_bytes == 0
    assert not server._sessions and not server._background
    assert server._worker_pool.stats.active_workers == 0
    assert_routes_released(server, old_session)
    await server.stop()
    await server.start()
    await server.start()
    replacement = await connect(transport)
    await replacement.join("caller")
    assert server._pools["test-pool"].clients["caller"] is not old_session
    assert (await replacement.request("list_clients"))["payload"]["clients"] == ["caller"]
    assert (await replacement.request("get_buffer", {"key": "retained"}))["payload"]["entry"]["value"] == 42
