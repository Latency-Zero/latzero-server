"""Production pod affinity, isolation, persistence and fail-closed integration."""

import asyncio
import hashlib
import json
import os
import signal
import socket
import time
from contextlib import suppress

import pytest
from websockets.exceptions import ConnectionClosed, InvalidStatusCode

from latzero_server.config import ServerConfig
from latzero_server.server import LatZeroServer
from pod_fixture import PodCluster, assert_listener_closed, pod_cluster, pools_for_owners, reference_owner


def canonical_path(root, pool):
    return root / ("pool-%s.json" % hashlib.sha256(pool.encode("utf-8")).hexdigest())


async def get_value(peer, key):
    return (await peer.ack("get_buffer", {"key": key}))["payload"]


async def complete_call(origin, worker, recipient, pool, kind, request_id, value):
    event = worker.client_id + ":compute" if kind == "process" else "calculate"
    payload = {"data": {"value": value}, "timeout": 5, "response_to": recipient.client_id}
    if kind == "process":
        payload["process_id"] = event
    else:
        payload.update(target_client_id=worker.client_id, event=event)
    acceptance = await origin.ack("call_process" if kind == "process" else "call_app", payload, request_id)
    assert acceptance["request_id"] == acceptance["payload"]["request_id"] == request_id
    assert acceptance["payload"]["queued"] is True
    incoming = await worker.receive("call_app")
    assert incoming["request_id"] != request_id
    assert incoming["pool"] == pool
    assert incoming["payload"] == {
        "event": event, "data": {"value": value}, "source_client_id": origin.client_id,
        "target_client_id": worker.client_id, "response_to": recipient.client_id,
    }
    await worker.send(worker.message("app_result", {"value": value, "error": None}, incoming["request_id"]))
    delivered = await worker.receive("ack", incoming["request_id"])
    assert delivered["payload"] == {"delivered": True}
    result = await recipient.receive("app_result", request_id)
    assert result == {
        "type": "app_result", "request_id": request_id, "client_id": worker.client_id,
        "pool": pool, "payload": {
            "request_id": request_id, "event": event, "source_client_id": origin.client_id,
            "target_client_id": worker.client_id, "response_to": recipient.client_id,
            "value": value, "error": None,
        },
    }
    if origin is not recipient:
        await origin.hello()
        assert not any(message["type"] == "app_result" and message.get("request_id") == request_id
                       for message in origin.received)
    return incoming


@pytest.mark.asyncio
async def test_same_pool_public_tcp_and_ws_connections_have_one_real_owner(tmp_path):
    async with pod_cluster(tmp_path, 4) as cluster:
        pool = "shared-affinity/\u00e9"
        owner = cluster.children[reference_owner(pool, 4)]
        peers = []
        for number, transport in enumerate(("tcp", "ws", "tcp", "ws")):
            peer, reply = await cluster.join("affinity-%d" % number, pool, transport)
            peers.append(peer)
            assert len(peer.redirects) == 1
            assert peer.port == (owner.ws_port if transport == "ws" else owner.port)
            assert reply["payload"]["clients"] == sorted("affinity-%d" % index for index in range(number + 1))
            assert len(peer.visited) == 2
            assert len(peer.join_messages) == 2
            assert all(message["payload"] == {"client_id": peer.client_id, "pool": pool, "auth_token": None}
                       for message in peer.join_messages)
            assert (await peer.ack("hello", {"capabilities": ["pool_redirect_v1"]}))["pool"] == pool
        for peer in peers:
            assert (await peer.ack("list_clients"))["payload"]["clients"] == sorted(item.client_id for item in peers)
        assert peers[0].writer.get_extra_info("sockname") != peers[2].writer.get_extra_info("sockname")
        assert len({child.pid for child in cluster.children}) == 4
        assert all(child.pid != os.getpid() and child.process.returncode is None for child in cluster.children)


@pytest.mark.asyncio
async def test_four_owner_pools_isolate_membership_buffers_subscriptions_and_routed_rpc(tmp_path):
    async with pod_cluster(tmp_path, 4) as cluster:
        pools = pools_for_owners(4, "isolated")
        groups = {}
        for index in range(4):
            pool = pools[index][0]
            origin, _ = await cluster.join("origin-%d" % index, pool, "tcp")
            worker, _ = await cluster.join("worker-%d" % index, pool, "ws")
            recipient, _ = await cluster.join("result-%d" % index, pool, "ws" if index % 2 else "tcp")
            groups[index] = (pool, origin, worker, recipient)
            assert origin.port == cluster.children[index].port
            assert worker.port == cluster.children[index].ws_port
            await recipient.ack("subscribe_buffer", {"key": "shared"})
            await worker.ack("register_process", {"process_name": "compute", "min_workers": 1, "max_workers": 1})

        for index, (pool, origin, worker, recipient) in groups.items():
            value = {"pool": pool, "index": index, "values": [None, False, 0, "", "\u00e9\u96ea"]}
            await origin.ack("set_buffer", {"key": "shared", "value": value})
            update = await recipient.receive("buffer_update", predicate=lambda message: message["payload"]["key"] == "shared")
            assert update["pool"] == pool
            assert update["payload"]["entry"]["value"] == value
            assert update["payload"]["entry"]["updated_by"] == origin.client_id
            for peer in (origin, worker, recipient):
                assert (await get_value(peer, "shared"))["entry"]["value"] == value
                assert (await peer.ack("list_clients"))["payload"]["clients"] == sorted(
                    item.client_id for item in (origin, worker, recipient))
                assert set((await peer.ack("list_processes"))["payload"]["processes"]) == {worker.client_id + ":compute"}
            for kind in ("app", "process"):
                await complete_call(origin, worker, recipient, pool, kind, "isolated-%d-%s" % (index, kind), value)
            other_worker = groups[(index + 1) % 4][2]
            missing_app = await origin.request("call_app", {"target_client_id": other_worker.client_id, "event": "calculate"})
            assert missing_app["type"] == "error" and missing_app["payload"]["code"] == "target_not_found"
            missing_process = await origin.request("call_process", {"process_id": other_worker.client_id + ":compute"})
            assert missing_process["type"] == "error" and missing_process["payload"]["code"] == "process_not_found"
            forbidden_recipient = await origin.request("call_app", {
                "target_client_id": worker.client_id, "event": "calculate", "response_to": other_worker.client_id,
            })
            assert forbidden_recipient["type"] == "error"
            await recipient.hello()
            assert [message["payload"]["entry"]["value"] for message in recipient.received
                    if message["type"] == "buffer_update"] == [value]


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["tcp", "ws"])
async def test_misdirected_direct_pod_join_redirects_before_auth_or_pool_state(tmp_path, transport):
    async with pod_cluster(tmp_path, 2, max_pools=1) as cluster:
        pools = pools_for_owners(2, "misdirected")
        wrong = cluster.children[0]
        target_pool, local_pool = pools[1][0], pools[0][0]
        peer = await cluster.open(transport, child=wrong)
        await peer.hello()
        request_id = "misdirected-join"
        reply = await peer.request("join_pool", {
            "client_id": "wrong-entry", "pool": target_pool, "auth_token": "must-not-create-auth",
        }, request_id)
        cluster.assert_redirect(reply, request_id, "wrong-entry", target_pool)
        await peer.expect_closed()
        owner_peer, joined = await cluster.join("legitimate", target_pool, transport)
        assert joined["payload"]["auth_required"] is False
        assert (await get_value(owner_peer, "not-mutated"))["exists"] is False
        local, _ = await cluster.join("local-owner", local_pool, transport)
        assert local.port == (wrong.ws_port if transport == "ws" else wrong.port)
        await local.ack("set_buffer", {"key": "proof", "value": "owned", "persistent": True})
        assert not canonical_path(tmp_path, target_pool).exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["tcp", "ws"])
async def test_noncapable_public_client_gets_redirect_required_error_then_close(tmp_path, transport):
    async with pod_cluster(tmp_path, 2) as cluster:
        peer = await cluster.open(transport)
        await peer.hello(capable=False)
        pool = "legacy-client-must-not-create"
        reply = await peer.request("join_pool", {"client_id": "legacy", "pool": pool, "auth_token": "wrong"}, "legacy-join")
        assert reply["type"] == "error"
        assert reply["request_id"] == "legacy-join" and reply["client_id"] == "legacy" and reply["pool"] == pool
        assert reply["payload"]["code"] == "redirect_required"
        assert isinstance(reply["payload"]["message"], str) and reply["payload"]["message"]
        assert {key: value for key, value in reply["payload"].items() if key not in {"code", "message"}} == cluster.redirect_payload(pool)
        await peer.expect_closed()
        capable, joined = await cluster.join("capable", pool, transport)
        assert joined["payload"]["auth_required"] is False
        assert (await capable.ack("list_clients"))["payload"]["clients"] == ["capable"]
        assert not canonical_path(tmp_path, pool).exists()


@pytest.mark.asyncio
async def test_cross_owner_switch_cleans_old_membership_subscriptions_processes_and_routes(tmp_path):
    async with pod_cluster(tmp_path, 2, max_subscriptions_per_pool=1, max_routes_per_pool=1,
                           max_routes_per_session=1, max_routes=1) as cluster:
        pools = pools_for_owners(2, "switch")
        old_pool, new_pool = pools[0][0], pools[1][0]
        mover, _ = await cluster.join("stable-identity", old_pool, "ws")
        old_peer, _ = await cluster.join("old-peer", old_pool)
        new_peer, _ = await cluster.join("new-peer", new_pool, auth_token="new-owner-token")
        await mover.ack("subscribe_buffer", {"key": "watched"})
        await mover.ack("register_process", {"process_name": "compute", "min_workers": 1, "max_workers": 1})
        await old_peer.ack("set_buffer", {"key": "old-only", "value": 1})
        await old_peer.ack("call_process", {
            "process_id": "stable-identity:compute", "data": {"hold": True}, "timeout": 20,
        }, "old-held-route")
        held = await mover.receive("call_app")
        assert held["payload"]["event"] == "stable-identity:compute"
        previous = mover
        mover, joined = await cluster.join("stable-identity", new_pool, "ws", "new-owner-token",
                                           peer=mover, operation="switch_pool")
        assert len(mover.redirects) == 1 and joined["payload"]["auth_required"] is True
        assert mover.port == cluster.children[1].ws_port and previous.port == cluster.children[0].ws_port
        departed = await old_peer.receive("error", "old-held-route")
        assert departed["payload"]["code"] == "peer_disconnected"
        assert departed["payload"]["execution_uncertain"] is True
        assert (await old_peer.ack("list_clients"))["payload"]["clients"] == ["old-peer"]
        assert (await old_peer.ack("list_processes"))["payload"]["processes"] == {}
        await old_peer.ack("subscribe_buffer", {"key": "watched"})
        await old_peer.ack("register_process", {"process_name": "compute", "min_workers": 1, "max_workers": 1})
        await complete_call(old_peer, old_peer, old_peer, old_pool, "process", "old-route-quota-recovered", {"old": True})
        assert (await get_value(mover, "old-only"))["exists"] is False
        assert (await mover.ack("list_processes"))["payload"]["processes"] == {}

        await mover.ack("subscribe_buffer", {"key": "watched"})
        await mover.ack("register_process", {"process_name": "compute", "min_workers": 1, "max_workers": 1})
        await new_peer.ack("call_process", {"process_id": "stable-identity:compute", "timeout": 20}, "same-owner-held")
        incoming = await mover.receive("call_app")
        rejoined = await mover.ack("join_pool", {"client_id": mover.client_id, "pool": new_pool,
                                                 "auth_token": "new-owner-token"}, "idempotent-join")
        assert rejoined["payload"]["clients"] == ["new-peer", "stable-identity"]
        assert set((await mover.ack("list_processes"))["payload"]["processes"]) == {"stable-identity:compute"}
        await mover.ack("subscribe_buffer", {"key": "watched"})
        await mover.send(mover.message("app_result", {"value": "retained-route", "error": None}, incoming["request_id"]))
        assert (await mover.receive("ack", incoming["request_id"]))["payload"]["delivered"] is True
        assert (await new_peer.receive("app_result", "same-owner-held"))["payload"]["value"] == "retained-route"
        await new_peer.ack("set_buffer", {"key": "watched", "value": "new-pool"})
        update = await mover.receive("buffer_update")
        assert update["pool"] == new_pool and update["payload"]["entry"]["value"] == "new-pool"
        await old_peer.ack("set_buffer", {"key": "watched", "value": "old-pool"})
        assert (await old_peer.receive("buffer_update"))["pool"] == old_pool
        await mover.hello()
        assert [message["pool"] for message in mover.received if message["type"] == "buffer_update"] == [new_pool]


@pytest.mark.asyncio
async def test_persistent_flat_hash_snapshots_auth_and_ttl_survive_two_to_four_pods(tmp_path):
    pools = ["A", "a", "a/b", "a_b", "\u00e9", "e\u0301", "../", "CON"]
    pools.extend(values[0] for values in pools_for_owners(4, "persisted-owner").values())
    token = "persisted-owner-secret"
    before = {}
    first_children = []
    async with pod_cluster(tmp_path, 2, persistence_batch_window=0.01) as first:
        first_children = list(first.children)
        peers = []
        for index, pool in enumerate(pools):
            peer, _ = await first.join("persist-%d" % index, pool, auth_token=token)
            peers.append(peer)
        await asyncio.gather(*(peer.ack("set_buffer", {
            "key": "permanent", "value": {"pool": pool, "values": [None, False, 0, "", "\u96ea"]}, "persistent": True,
        }) for pool, peer in zip(pools, peers)))
        for pool, peer in zip(pools, peers):
            await peer.ack("set_buffer", {"key": "ttl", "value": pool, "persistent": True, "ttl": 300})
            await peer.ack("set_buffer", {"key": "ephemeral", "value": "not-restored"})
            await peer.ack("set_buffer", {"key": "expired", "value": "not-restored", "persistent": True, "ttl": 0})
            assert (await get_value(peer, "expired"))["exists"] is False
            before[pool] = {key: (await get_value(peer, key))["entry"] for key in ("permanent", "ttl")}
    assert all(child.process.returncode == 0 for child in first_children)
    expected_paths = {canonical_path(tmp_path, pool) for pool in pools}
    assert set(tmp_path.glob("*.json")) == expected_paths
    assert not list(tmp_path.glob("**/*.tmp"))
    assert not list(tmp_path.glob("*/pool-*.json")), "Snapshots must stay flat, not pod-count partitioned"
    original_bytes = {path: path.read_bytes() for path in expected_paths}
    for pool in pools:
        snapshot = json.loads(canonical_path(tmp_path, pool).read_text(encoding="utf-8"))
        assert snapshot == {"pool_id": pool, "auth_required": True,
                            "auth_token_hash": hashlib.sha256(token.encode("utf-8")).hexdigest(), "buffers": before[pool]}
        assert token not in canonical_path(tmp_path, pool).read_text(encoding="utf-8")

    async with pod_cluster(tmp_path, 4, persistence_batch_window=0.01) as second:
        for index, pool in enumerate(pools):
            denied, rejection = await second.join("denied-%d" % index, pool, auth_token="not-the-token", expect="error")
            assert rejection["payload"]["code"] == "auth_failed"
            peer, joined = await second.join("restored-%d" % index, pool, "ws" if index % 2 else "tcp", token)
            assert joined["payload"]["auth_required"] is True
            owner = second.children[reference_owner(pool, 4)]
            assert peer.port == (owner.ws_port if peer.transport == "ws" else owner.port)
            for key in ("permanent", "ttl"):
                assert (await get_value(peer, key))["entry"] == before[pool][key]
            assert (await get_value(peer, "ephemeral"))["exists"] is False
            assert (await get_value(peer, "expired"))["exists"] is False
            assert (await peer.ack("list_clients"))["payload"]["clients"] == [peer.client_id]
            assert (await peer.ack("list_processes"))["payload"]["processes"] == {}
            await denied.close()
    assert {path: path.read_bytes() for path in expected_paths} == original_bytes
    assert set(tmp_path.glob("*.json")) == expected_paths


@pytest.mark.asyncio
async def test_legacy_backup_and_expired_restore_remain_safe_during_concurrent_pod_writes(tmp_path):
    legacy_pool = "legacy/shared"
    colliding_pool = "legacy_shared"
    old_value = {"old": True}
    legacy = tmp_path / "legacy_shared.json"
    snapshot = {
        "pool_id": legacy_pool, "auth_required": False, "auth_token_hash": None,
        "buffers": {
            "saved": {"value": old_value, "updated_at": 1, "updated_by": "legacy", "persistent": True, "ttl": None, "version": 1},
            "expired": {"value": "expired", "updated_at": 1, "updated_by": "legacy", "persistent": True, "ttl": 1, "version": 1},
        },
    }
    legacy.write_text(json.dumps(snapshot), encoding="utf-8")
    backup = legacy.read_bytes()
    async with pod_cluster(tmp_path, 4, persistence_batch_window=0.01) as cluster:
        old, _ = await cluster.join("legacy-reader", legacy_pool)
        assert (await get_value(old, "saved"))["entry"]["value"] == old_value
        assert (await get_value(old, "expired"))["exists"] is False
        collision, _ = await cluster.join("collision-owner", colliding_pool, "ws")
        writers = [old]
        for index in range(7):
            peer, _ = await cluster.join("concurrent-%d" % index, legacy_pool, "ws" if index % 2 else "tcp")
            writers.append(peer)
        replies = await asyncio.gather(*(peer.ack("set_buffer", {"key": "saved", "value": index, "persistent": True})
                                         for index, peer in enumerate(writers)))
        versions = [reply["payload"]["version"] for reply in replies]
        assert sorted(versions) == list(range(2, 10))
        final_writer = max(range(len(writers)), key=lambda index: versions[index])
        await collision.ack("set_buffer", {"key": "saved", "value": "independent", "persistent": True})
        assert (await get_value(old, "saved"))["entry"]["value"] == final_writer
        assert (await get_value(collision, "saved"))["entry"]["value"] == "independent"
    assert legacy.read_bytes() == backup
    saved = json.loads(canonical_path(tmp_path, legacy_pool).read_text(encoding="utf-8"))
    assert saved["buffers"]["saved"]["version"] == 9
    assert saved["buffers"]["saved"]["value"] == final_writer
    assert "expired" not in saved["buffers"]
    assert json.loads(canonical_path(tmp_path, colliding_pool).read_text(encoding="utf-8"))["buffers"]["saved"]["value"] == "independent"
    assert set(tmp_path.glob("*.json")) == {legacy, canonical_path(tmp_path, legacy_pool), canonical_path(tmp_path, colliding_pool)}
    assert not list(tmp_path.glob("**/*.tmp"))
    async with pod_cluster(tmp_path, 2) as restarted:
        for pool, expected in ((legacy_pool, final_writer), (colliding_pool, "independent")):
            peer, _ = await restarted.join("restored", pool)
            assert (await get_value(peer, "saved"))["entry"]["value"] == expected
            assert (await get_value(peer, "expired"))["exists"] is False
    assert legacy.read_bytes() == backup


@pytest.mark.asyncio
async def test_classic_daemon_and_second_supervisor_cannot_write_live_root(tmp_path):
    async with pod_cluster(tmp_path, 2) as owner:
        peer, _ = await owner.join("exclusive-owner", "exclusive-pool")
        await peer.ack("set_buffer", {"key": "persisted", "value": "owner", "persistent": True})
        classic = LatZeroServer(ServerConfig(port=0, websocket_port=0, data_dir=tmp_path))
        second = PodCluster(tmp_path, 4)
        try:
            with pytest.raises((OSError, RuntimeError)):
                await asyncio.wait_for(classic.start(), 5)
            assert classic._tcp_server is None and classic._websocket_server is None
            assert not classic._storage_started and not classic._sessions
            with pytest.raises((OSError, RuntimeError)):
                await second.start()
            assert all(child.process.returncode is not None for child in second.child_records)
            assert (await get_value(peer, "persisted"))["entry"]["value"] == "owner"
            await peer.ack("set_buffer", {"key": "persisted", "value": "still-owner", "persistent": True})
        finally:
            await classic.stop()
            await second.close()
    assert json.loads(canonical_path(tmp_path, "exclusive-pool").read_text(encoding="utf-8"))["buffers"]["persisted"]["value"] == "still-owner"
    async with pod_cluster(tmp_path, 2) as reopened:
        peer, _ = await reopened.join("later-owner", "exclusive-pool")
        assert (await get_value(peer, "persisted"))["entry"]["value"] == "still-owner"


@pytest.mark.asyncio
async def test_public_ws_bind_failure_rolls_back_and_reaps_all_started_pods(tmp_path):
    blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    blocker.bind(("127.0.0.1", 0))
    blocker.listen(1)
    cluster = PodCluster(tmp_path, 2, websocket_port=blocker.getsockname()[1])
    try:
        with pytest.raises((OSError, RuntimeError)):
            await cluster.start()
        children = list(cluster.children)
        assert all(child.process.returncode is not None for child in children)
        await assert_listener_closed(cluster.tcp_port)
        for child in children:
            await assert_listener_closed(child.port)
            await assert_listener_closed(child.ws_port)
    finally:
        blocker.close()
        await cluster.close()
    async with pod_cluster(tmp_path, 2) as reopened:
        peer, _ = await reopened.join("rollback-recovered", "rollback-pool")
        assert (await peer.ack("list_clients"))["payload"]["clients"] == ["rollback-recovered"]


@pytest.mark.asyncio
async def test_killed_owner_stops_serving_all_pods_without_remapping(tmp_path):
    cluster = PodCluster(tmp_path, 4)
    serve = None
    try:
        await cluster.start()
        pools = pools_for_owners(4, "crash")
        peers = []
        ports = [cluster.tcp_port, cluster.ws_port]
        for index, child in enumerate(cluster.children):
            peer, _ = await cluster.join("live-%d" % index, pools[index][0], "ws" if index % 2 else "tcp")
            peers.append(peer)
            ports.extend((child.port, child.ws_port))
        serve = asyncio.create_task(cluster.supervisor.serve_forever())
        await peers[3].hello()
        failed = cluster.children[1]
        os.kill(failed.pid, getattr(signal, "SIGKILL", signal.SIGTERM))
        await asyncio.wait_for(failed.process.wait(), 15)
        assert failed.process.returncode != 0
        try:
            await asyncio.wait_for(serve, 25)
        except RuntimeError as exc:
            assert str(exc)
        snapshot = cluster.snapshot()
        assert snapshot["healthy"] is False
        assert all(child.process.returncode is not None for child in cluster.child_records)
        for port in ports:
            await assert_listener_closed(port)
        for peer in peers:
            if peer.websocket is not None:
                await asyncio.wait_for(peer.websocket.wait_closed(), 5)
            else:
                await asyncio.wait_for(peer.reader.read(), 5)
        for index in range(4):
            assert cluster.supervisor.pool_owner(pools[index][0]) == index
    finally:
        if serve is not None and not serve.done():
            serve.cancel()
            await asyncio.gather(serve, return_exceptions=True)
        await cluster.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["tcp", "ws"])
async def test_router_control_errors_are_correlated_and_do_not_mutate_pool(tmp_path, transport):
    async with pod_cluster(tmp_path, 2, max_frame_bytes=1024) as cluster:
        peer = await cluster.open(transport)
        for kind, payload in (("unknown_control", {}), ("set_buffer", {"key": "must-not-write", "value": True}),
                              ("hello", {"capabilities": [1]}), ("join_pool", {"client_id": "invalid", "pool": []})):
            reply = await peer.request(kind, payload)
            assert reply["type"] == "error", reply
            assert isinstance(reply["payload"].get("code"), str) and reply["payload"]["code"]
        hello = await peer.hello()
        assert hello["payload"]["capabilities"] == ["pool_redirect_v1"]
        peer, _ = await cluster.join("valid-after-controls", "control-pool", transport, peer=peer)
        assert (await get_value(peer, "must-not-write"))["exists"] is False
        assert (await peer.ack("list_clients"))["payload"]["clients"] == ["valid-after-controls"]
        assert not list(tmp_path.glob("*.json"))


@pytest.mark.asyncio
async def test_router_tcp_split_coalesced_frames_and_pipelined_effect_after_redirect(tmp_path):
    async with pod_cluster(tmp_path, 2, max_frame_bytes=1024) as cluster:
        peer = await cluster.open()
        hello = peer.message("hello", {"capabilities": ["pool_redirect_v1"]}, "split-hello")
        second = peer.message("hello", {"capabilities": ["pool_redirect_v1"]}, "coalesced-hello")
        encoded = (json.dumps(hello) + "\n" + json.dumps(second) + "\n").encode("utf-8")
        peer.writer.write(encoded[:13])
        await asyncio.wait_for(peer.writer.drain(), 3)
        peer.writer.write(encoded[13:])
        await asyncio.wait_for(peer.writer.drain(), 3)
        assert (await peer.receive("ack", "coalesced-hello"))["payload"]["capabilities"] == ["pool_redirect_v1"]
        assert (await peer.receive("ack", "split-hello"))["request_id"] == "split-hello"
        pool = "pipeline-redirect"
        join = peer.message("join_pool", {"client_id": "pipelined", "pool": pool}, "pipeline-join")
        mutate = peer.message("set_buffer", {"key": "must-not-replay", "value": True, "persistent": True}, "pipeline-effect")
        peer.writer.write((json.dumps(join) + "\n" + json.dumps(mutate) + "\n").encode("utf-8"))
        await asyncio.wait_for(peer.writer.drain(), 3)
        reply = await peer.receive(("redirect", "error"), "pipeline-join")
        cluster.assert_redirect(reply, "pipeline-join", "pipelined", pool)
        await peer.expect_closed()
        owner, _ = await cluster.join("pipelined", pool)
        assert (await get_value(owner, "must-not-replay"))["exists"] is False
        assert not canonical_path(tmp_path, pool).exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["tcp", "ws"])
async def test_router_malformed_binary_and_frame_limits(tmp_path, transport):
    async with pod_cluster(tmp_path, 2, max_frame_bytes=1024) as cluster:
        peer = await cluster.open(transport)
        for raw in ("not-json", "[]", '{"type":"hello","payload":[]}'):
            if transport == "tcp":
                peer.writer.write((raw + "\n").encode("utf-8"))
                await asyncio.wait_for(peer.writer.drain(), 3)
            else:
                await peer.websocket.send(raw)
            error = await peer.receive("error")
            assert error["payload"]["code"] == "protocol_error"
        if transport == "ws":
            await peer.websocket.send(b'{"type":"hello"}')
            assert (await peer.receive("error"))["payload"]["code"] == "protocol_error"
        await peer.hello()
        oversized = "x" * 1025
        if transport == "tcp":
            peer.writer.write((oversized + "\n").encode("ascii"))
            await asyncio.wait_for(peer.writer.drain(), 3)
            assert (await peer.receive("error"))["payload"]["code"] == "frame_too_large"
            await peer.expect_closed()
        else:
            await peer.websocket.send(oversized)
            await asyncio.wait_for(peer.websocket.wait_closed(), 5)
            assert peer.websocket.close_code == 1009
        assert not list(tmp_path.glob("*.json"))


@pytest.mark.asyncio
async def test_router_connection_limit_includes_pending_ws_handshakes(tmp_path):
    async with pod_cluster(tmp_path, 2, max_connections=1) as cluster:
        admitted = await cluster.open()
        await admitted.hello()
        rejected = await cluster.open()
        error = await rejected.receive("error")
        assert error["payload"]["code"] == "server_busy"
        await rejected.expect_closed()
        with pytest.raises(InvalidStatusCode) as refused:
            await cluster.open("ws")
        assert refused.value.status_code == 503
        await admitted.hello()
        pool = "connection-slot"
        reply = await admitted.request("join_pool", {"client_id": "slot-release", "pool": pool}, "slot-redirect")
        cluster.assert_redirect(reply, "slot-redirect", "slot-release", pool)
        await admitted.expect_closed()
        await asyncio.sleep(0)
        handshake = await cluster.open("tcp", port=cluster.ws_port)
        rejected = await cluster.open()
        assert (await rejected.receive("error"))["payload"]["code"] == "server_busy"
        await rejected.expect_closed()
        await handshake.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["tcp", "ws"])
@pytest.mark.parametrize("send_hello", [False, True])
async def test_router_join_deadline_closes_unjoined_connections(tmp_path, transport, send_hello):
    async with pod_cluster(tmp_path, 2, join_timeout=0.3) as cluster:
        peer = await cluster.open(transport)
        if send_hello:
            await peer.hello()
        error = await peer.receive("error", timeout=4)
        assert error["payload"]["code"] == "join_timeout"
        await peer.expect_closed()
        assert not list(tmp_path.glob("*.json"))


@pytest.mark.asyncio
async def test_stopping_supervisor_reaps_children_and_closes_tcp_ws_ports(tmp_path):
    cluster = PodCluster(tmp_path, 2)
    try:
        await cluster.start()
        tcp, _ = await cluster.join("stop-tcp", "stop-pool")
        ws, _ = await cluster.join("stop-ws", "stop-pool", "ws")
        ports = [cluster.tcp_port, cluster.ws_port]
        for child in cluster.children:
            ports.extend((child.port, child.ws_port))
        await asyncio.wait_for(cluster.supervisor.stop(), 30)
        await tcp.expect_closed()
        await ws.expect_closed()
        assert all(child.process.returncode == 0 for child in cluster.child_records)
        for port in ports:
            await assert_listener_closed(port)
        await asyncio.wait_for(cluster.supervisor.stop(), 5)
    finally:
        await cluster.close()
