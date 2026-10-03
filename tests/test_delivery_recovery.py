import asyncio
import gc
import json
import weakref

import pytest

from test_hardening import controlled_session, drain_outboxes, server_with_limits
from latzero_server.models import PoolState


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_peer", ["target", "origin", "response"])
async def test_immediate_connection_failure_cancels_untransmitted_call(tmp_path, failed_peer):
    async with server_with_limits(tmp_path, write_timeout=0.1) as (server, _):
        pool = PoolState(pool_id="fenced")
        server._pools[pool.pool_id] = pool
        origin, origin_writer = controlled_session(server, pool, "origin")
        target, target_writer = controlled_session(server, pool, "target")
        response, response_writer = controlled_session(server, pool, "response")
        payload = {"data": 42, "response_to": "response"}
        request = {"type": "call_app", "request_id": "not-sent", "payload": payload}
        route = server._accept_call(pool, origin, target, "echo", payload, request)
        assert route is not None and not route.sent
        server._fail_session({"target": target, "origin": origin, "response": response}[failed_peer], "injected failure")
        close_task = {"target": target, "origin": origin, "response": response}[failed_peer].close_task
        await asyncio.wait_for(close_task, 1)
        await drain_outboxes(server)
        assert not any(frame["type"] == "call_app" for frame in target_writer.frames)
        assert not route.sent and server._route_count == 0
        assert not pool.in_flight_requests
        errors = [frame for frame in origin_writer.frames + response_writer.frames if frame["type"] == "error"]
        assert errors
        assert all(frame["payload"]["execution_uncertain"] is False for frame in errors)


@pytest.mark.asyncio
@pytest.mark.parametrize("closed_peer", ["origin", "response"])
async def test_underlying_transport_close_cancels_route_before_invocation(tmp_path, closed_peer):
    async with server_with_limits(tmp_path, write_timeout=0.1) as (server, _):
        pool = PoolState(pool_id="transport-fence")
        server._pools[pool.pool_id] = pool
        origin, origin_writer = controlled_session(server, pool, "origin")
        target, target_writer = controlled_session(server, pool, "target")
        response, response_writer = controlled_session(server, pool, "response")
        route = server._accept_call(pool, origin, target, "echo", {"response_to": "response"}, {"request_id": "unsent"})
        {"origin": origin_writer, "response": response_writer}[closed_peer].close()
        assert not {"origin": origin, "response": response}[closed_peer].closing
        await drain_outboxes(server)
        assert not any(frame["type"] == "call_app" for frame in target_writer.frames)
        assert not route.sent and not pool.in_flight_requests
        errors = [frame for frame in origin_writer.frames + response_writer.frames if frame["type"] == "error"]
        assert errors and all(frame["payload"]["execution_uncertain"] is False for frame in errors)


@pytest.mark.asyncio
async def test_closed_transport_does_not_dispatch_buffered_membership_or_mutation(tmp_path):
    async with server_with_limits(tmp_path) as (server, _):
        pool = PoolState(pool_id="owned")
        server._pools[pool.pool_id] = pool
        joined, writer = controlled_session(server, pool, "joined")
        writer.close()
        await server._dispatch(joined, {"type": "set_buffer", "payload": {"key": "not-written", "value": 42}})
        assert not pool.buffers
        pending, pending_writer = controlled_session(server, pool, "pending")
        pool.clients.pop("pending")
        pending.pool_id = None
        pending_writer.close()
        await server._dispatch(pending, {"type": "join_pool", "payload": {"client_id": "pending", "pool": "new"}})
        assert "new" not in server._pools


@pytest.mark.asyncio
async def test_oversized_acceptance_ack_rejects_origin_before_target_admission(tmp_path):
    async with server_with_limits(tmp_path, max_frame_bytes=1024) as (server, connect):
        origin = await connect()
        target = await connect()
        await origin.join("o", "p")
        await target.join("t", "p")
        request_id = "x" * 480
        reply = await origin.request("call_app", {"event": "echo", "target_client_id": "t"},
                                     request_id=request_id, response="error")
        assert reply["payload"]["code"] == "response_too_large"
        assert not server._pools["p"].in_flight_requests
        assert (await origin.request("list_clients"))["payload"]["clients"] == ["o", "t"]


@pytest.mark.asyncio
async def test_global_event_fanout_limit_rejects_producer_without_failing_quiet_peer(tmp_path):
    async with server_with_limits(tmp_path, max_fanout_messages=1) as (server, _):
        pool = PoolState(pool_id="isolated")
        server._pools[pool.pool_id] = pool
        first, _ = controlled_session(server, pool, "first")
        second, _ = controlled_session(server, pool, "second")
        event = {"type": "emit_event", "payload": {"event": "notice", "data": 42}}
        assert (await server._fanout([first], event))["accepted"] == ["first"]
        rejected = await server._fanout([second], event)
        assert rejected == {"accepted": [], "failed": ["second"]}
        assert not second.closing and second.outbox_messages == 0


@pytest.mark.asyncio
async def test_pending_websocket_handshakes_share_connection_budget_and_stop_closes_them(tmp_path):
    async with server_with_limits(tmp_path, max_connections=1) as (server, _):
        port = server._websocket_server.sockets[0].getsockname()[1]
        admitted_reader, admitted_writer = await asyncio.open_connection("127.0.0.1", port)
        rejected_reader, rejected_writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            rejected = await asyncio.wait_for(rejected_reader.read(), 1)
            assert rejected.startswith(b"HTTP/1.1 503")
            assert len(server._pending_websockets) == 1
            assert len(server._websocket_server.websockets) == 1
            assert server._connection_count == 0
            assert server.get_dashboard_snapshot()["pending_websocket_handshakes"] == 1
            await server.stop()
            assert await asyncio.wait_for(admitted_reader.read(), 1) == b""
            assert not server._pending_websockets
        finally:
            for writer in (admitted_writer, rejected_writer):
                writer.close()
                await writer.wait_closed()


@pytest.mark.asyncio
async def test_idle_fanout_releases_closed_session_frame_and_payload_references(tmp_path):
    async with server_with_limits(tmp_path) as (server, _):
        pool = PoolState(pool_id="fanout")
        server._pools[pool.pool_id] = pool
        session, writer = controlled_session(server, pool)
        session_ref = weakref.ref(session)
        message = {"type": "buffer_update", "pool": "fanout", "payload": {"value": "x" * 4096}}
        assert (await server._fanout([session], message))["accepted"] == [session.client_id]
        frame = session.outbox[0]
        frame_ref = weakref.ref(frame)
        await asyncio.wait_for(session.outbox_drained.wait(), 1)
        await server._close_session(session)
        # Allow done callbacks to remove the tracked close/writer tasks.
        await asyncio.sleep(0)
        assert not server._fanout_queue
        assert server._fanout_bytes == server._outbox_bytes == 0
        del session, writer, frame, message
        gc.collect()
        assert session_ref() is None
        assert frame_ref() is None


@pytest.mark.asyncio
async def test_finalized_route_drop_releases_idle_writer_references(tmp_path):
    async with server_with_limits(tmp_path) as (server, _):
        pool = PoolState(pool_id="released")
        server._pools[pool.pool_id] = pool
        origin, _ = controlled_session(server, pool, "origin")
        target, _ = controlled_session(server, pool, "target")
        request = {"request_id": "released-route"}
        route = server._accept_call(pool, origin, target, "echo", {}, request)
        route_ref = weakref.ref(route)
        server._finalize_route(pool, route, code="timeout", error="Injected pretransmission timeout")
        await drain_outboxes(server)
        del route
        gc.collect()
        assert route_ref() is None
        assert server._route_count == server._outbox_bytes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["call_app", "emit_event"])
async def test_valid_compact_request_cannot_disconnect_healthy_target_on_generated_oversize(tmp_path, kind):
    async with server_with_limits(tmp_path, max_frame_bytes=512) as (server, connect):
        origin = await connect()
        target = await connect()
        await origin.join("o", "p")
        await target.join("t", "p")
        message = {
            "type": kind, "request_id": "unicode", "pool": None,
            "payload": {"target_client_id": "t", "event": "echo", "data": "\u00e9" * 130},
        }
        encoded = (json.dumps(message, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
        assert len(encoded) < 512
        origin.writer.write(encoded)
        await origin.writer.drain()
        error = await origin.receive("error", "unicode")
        assert error["payload"]["code"] in {"response_too_large", "partial_delivery"}
        pool = server._pools["p"]
        assert not pool.clients["t"].closing
        assert (await target.request("list_clients"))["payload"]["clients"] == ["o", "t"]
        assert not pool.in_flight_requests
