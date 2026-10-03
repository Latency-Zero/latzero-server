import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["tcp", "ws"])
async def test_buffer_subscription_and_event_conformance(daemon, transport):
    server, connect = daemon
    owner = await connect(transport)
    subscriber = await connect(transport)
    await owner.join("owner")
    await subscriber.join("subscriber")
    await subscriber.request("subscribe_buffer", {"key": "answer"})
    written = await owner.request("set_buffer", {
        "key": "answer", "value": {"value": 42}, "persistent": True,
    })
    assert written["payload"] == {"key": "answer", "version": 1}
    update = await subscriber.receive("buffer_update")
    assert update["payload"]["entry"]["value"] == {"value": 42}
    read = await owner.request("get_buffer", {"key": "answer"})
    assert read["payload"]["entry"]["value"] == {"value": 42}
    listed = await owner.request("list_buffers", {"pattern": "ans"})
    assert listed["payload"]["keys"] == ["answer"]
    await owner.request("emit_event", {
        "event": "notice", "data": 7, "target_client_id": "subscriber",
    })
    event = await subscriber.receive("emit_event")
    assert event["payload"]["data"] == 7
    assert event["payload"]["source_client_id"] == "owner"
    await owner.request("delete_buffer", {"key": "answer"})
    assert (await subscriber.receive("buffer_update"))["payload"]["operation"] == "delete"
    await subscriber.request("unsubscribe_buffer", {"key": "answer"})
    assert not server._pools["test-pool"].subscriptions


@pytest.mark.asyncio
async def test_two_phase_self_call_and_registered_process(daemon):
    _, connect = daemon
    client = await connect()
    await client.join("self")
    await client.request("register_process", {"process_name": "echo"})
    listed = await client.request("list_processes")
    assert "self:echo" in listed["payload"]["processes"]
    await client.send({
        "type": "call_process", "request_id": "origin", "pool": None,
        "client_id": "self", "payload": {"process_id": "echo", "data": "hello"},
    })
    incoming = await client.receive("call_app")
    assert incoming["payload"]["event"] == "self:echo"
    assert (await client.receive("ack", "origin"))["payload"]["queued"] is True
    await client.send({
        "type": "app_result", "request_id": incoming["request_id"], "pool": None,
        "payload": {"value": "hello", "error": None},
    })
    result = await client.receive("app_result", "origin")
    assert result["payload"]["value"] == "hello"
    await client.request("unregister_process", {"process_name": "echo"})
    assert (await client.request("list_processes"))["payload"]["processes"] == {}


@pytest.mark.asyncio
async def test_auth_duplicate_membership_switch_and_leave(daemon):
    _, connect = daemon
    owner = await connect()
    other = await connect()
    await owner.join("owner", auth_token="secret")
    await other.request("join_pool", {
        "client_id": "other", "pool": "test-pool",
    }, response="error")
    duplicate = await other.request("join_pool", {
        "client_id": "owner", "pool": "test-pool", "auth_token": "secret",
    }, response="error")
    assert duplicate["payload"]["code"] == "duplicate_client"
    await other.join("other", auth_token="secret")
    clients = await owner.request("list_clients")
    assert clients["payload"]["clients"] == ["other", "owner"]
    await other.request("switch_pool", {"client_id": "other", "pool": "new-pool"})
    assert (await owner.request("list_clients"))["payload"]["clients"] == ["owner"]
    await other.request("leave_pool")
    error = await other.request("get_buffer", {"key": "anything"}, response="error")
    assert error["payload"]["code"] == "dispatch_error"
