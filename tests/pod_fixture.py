"""Bounded raw transports for the real, separate-process pod supervisor."""

import asyncio
import hashlib
import json
from collections import deque
from contextlib import asynccontextmanager, suppress
from functools import partial

from websockets.exceptions import ConnectionClosed
from websockets.legacy.client import connect as connect_ws

from latzero_server.config import ServerConfig


def reference_owner(pool, pods):
    return int.from_bytes(hashlib.sha256(pool.encode("utf-8")).digest(), "big") % pods


def pools_for_owners(pods, prefix="integration", copies=1):
    result = {index: [] for index in range(pods)}
    for number in range(4096):
        pool = "%s-%d" % (prefix, number)
        bucket = result[reference_owner(pool, pods)]
        if len(bucket) < copies:
            bucket.append(pool)
        if all(len(values) == copies for values in result.values()):
            return result
    raise AssertionError("Bounded deterministic pool search did not cover every pod")


async def blocking(function, *args, **kwargs):
    future = asyncio.get_running_loop().run_in_executor(None, partial(function, *args, **kwargs))
    return await asyncio.wait_for(future, 10)


class PodPeer:
    def __init__(self, transport, host, port, reader=None, writer=None, websocket=None):
        self.transport = transport
        self.host = host
        self.port = port
        self.reader = reader
        self.writer = writer
        self.websocket = websocket
        self.client_id = None
        self.pool = None
        self.sequence = 0
        self.pending = deque()
        self.received = []
        self.sent = []
        self.redirects = []
        self.visited = []
        self.join_messages = []
        self.closed = False

    def message(self, kind, payload=None, request_id=None, pool=None, client_id=None):
        self.sequence += 1
        return {
            "type": kind,
            "request_id": request_id if request_id is not None else "pod-test-%d" % self.sequence,
            "client_id": self.client_id if client_id is None else client_id,
            "pool": pool,
            "payload": {} if payload is None else payload,
        }

    async def send(self, message):
        assert len(self.sent) < 1024, "Raw fixture send history exceeded its bound"
        self.sent.append(message)
        encoded = json.dumps(message, ensure_ascii=True, allow_nan=False, separators=(",", ":"))
        if self.websocket is not None:
            await asyncio.wait_for(self.websocket.send(encoded), 5)
        else:
            self.writer.write((encoded + "\n").encode("utf-8"))
            await asyncio.wait_for(self.writer.drain(), 5)

    async def read(self, deadline):
        remaining = deadline - asyncio.get_running_loop().time()
        assert remaining > 0, "Raw fixture response deadline expired"
        if self.websocket is not None:
            raw = await asyncio.wait_for(self.websocket.recv(), remaining)
            assert isinstance(raw, str), "Expected a text JSON response"
        else:
            raw = await asyncio.wait_for(self.reader.readline(), remaining)
            assert raw, "Pod connection closed before its expected response"
            assert raw.endswith(b"\n"), "TCP response was not a complete NDJSON frame"
        message = json.loads(raw)
        assert isinstance(message, dict), "Pod response was not a JSON object"
        assert len(self.received) < 1024, "Raw fixture receive history exceeded its bound"
        self.received.append(message)
        return message

    async def receive(self, kind, request_id=None, timeout=5, predicate=None):
        kinds = (kind,) if isinstance(kind, str) else tuple(kind)

        def matches(message):
            return (message.get("type") in kinds
                    and (request_id is None or message.get("request_id") == request_id)
                    and (predicate is None or predicate(message)))

        for message in list(self.pending):
            if matches(message):
                self.pending.remove(message)
                return message
        deadline = asyncio.get_running_loop().time() + timeout
        for _ in range(256):
            message = await self.read(deadline)
            if matches(message):
                return message
            assert len(self.pending) < 256, "Raw fixture unsolicited-message budget exceeded"
            self.pending.append(message)
        raise AssertionError("Expected response missing from bounded fixture reads")

    async def request(self, kind, payload=None, request_id=None, response=("ack", "error", "redirect"),
                      pool=None, client_id=None):
        message = self.message(kind, payload, request_id, pool, client_id)
        await self.send(message)
        return await self.receive(response, message["request_id"])

    async def ack(self, kind, payload=None, request_id=None, pool=None):
        reply = await self.request(kind, payload, request_id, pool=pool)
        assert reply["type"] == "ack", reply
        return reply

    async def hello(self, capable=True):
        return await self.ack("hello", {"capabilities": ["pool_redirect_v1"] if capable else []})

    async def expect_closed(self, timeout=5):
        if self.websocket is not None:
            await asyncio.wait_for(self.websocket.wait_closed(), timeout)
        else:
            assert await asyncio.wait_for(self.reader.read(), timeout) == b"", "Unexpected frames after redirect/close"

    async def close(self):
        if self.closed:
            return
        self.closed = True
        if self.websocket is not None:
            with suppress(ConnectionClosed, ConnectionError, OSError):
                await asyncio.wait_for(self.websocket.close(), 5)
        else:
            self.writer.close()
            with suppress(ConnectionError, OSError):
                await asyncio.wait_for(self.writer.wait_closed(), 5)


class PodCluster:
    def __init__(self, data_dir, pods=4, **options):
        from latzero_server.pods import PodSupervisor

        settings = dict(host="127.0.0.1", port=0, websocket_port=0, data_dir=data_dir,
                        min_workers=2, max_workers=4, shutdown_timeout=1, write_timeout=1)
        settings.update(options)
        self.config = ServerConfig(**settings)
        self.supervisor = PodSupervisor(self.config, pods, startup_timeout=30)
        self.count = pods
        self.clients = []
        self.child_records = []
        self.stats_waiters = {}
        self.expect_shutdown_error = False
        spawn = self.supervisor._spawn

        async def observed_spawn(child, config, deadline):
            await spawn(child, config, deadline)
            readline = child.process.stdout.readline

            async def observed_readline():
                raw = await readline()
                if raw:
                    message = json.loads(raw)
                    waiter = self.stats_waiters.get(child.index)
                    if isinstance(message.get("stats"), dict) and waiter is not None:
                        # Observe real control output; let the supervisor commit
                        # it before releasing the test's snapshot barrier.
                        def observed():
                            if not waiter.done():
                                waiter.set_result(message["stats"])

                        asyncio.get_running_loop().call_soon(observed)
                else:
                    child.process.stdout.readline = readline
                return raw

            child.process.stdout.readline = observed_readline

        self.supervisor._spawn = observed_spawn

    @property
    def tcp_port(self):
        return self.supervisor.tcp_port

    @property
    def ws_port(self):
        return self.supervisor.ws_port

    @property
    def children(self):
        return self.supervisor.children

    def snapshot(self):
        return self.supervisor.get_dashboard_snapshot()

    async def refresh_stats(self):
        async def refresh(child):
            if child._stats_pending:
                waiter = asyncio.get_running_loop().create_future()
                self.stats_waiters[child.index] = waiter
                await asyncio.wait_for(waiter, 5)
            waiter = asyncio.get_running_loop().create_future()
            self.stats_waiters[child.index] = waiter
            child._stats_pending = True
            child._stats_requested = asyncio.get_running_loop().time()
            try:
                await self.supervisor._send_control(child, {"operation": "stats"})
                result = await asyncio.wait_for(waiter, 5)
                assert result == child.stats
                return result
            finally:
                self.stats_waiters.pop(child.index, None)

        return await asyncio.gather(*(refresh(child) for child in self.children))

    async def start(self):
        try:
            await asyncio.wait_for(self.supervisor.start(), 40)
        finally:
            self.child_records = list(self.children)
        assert len(self.children) == self.count
        assert sorted(child.index for child in self.children) == list(range(self.count))
        assert 0 < self.tcp_port <= 65535
        if self.config.websocket_enabled:
            assert 0 < self.ws_port <= 65535
        return self

    async def open(self, transport="tcp", child=None, port=None):
        if port is None:
            if child is None:
                port = self.ws_port if transport == "ws" else self.tcp_port
            else:
                port = child.ws_port if transport == "ws" else child.port
        assert isinstance(port, int) and not isinstance(port, bool) and 0 < port <= 65535
        if transport == "ws":
            websocket = await connect_ws("ws://127.0.0.1:%d" % port, open_timeout=5, close_timeout=3)
            peer = PodPeer(transport, "127.0.0.1", port, websocket=websocket)
        else:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", port, limit=self.config.max_frame_bytes + 1), 5)
            peer = PodPeer(transport, "127.0.0.1", port, reader=reader, writer=writer)
        self.clients.append(peer)
        return peer

    def redirect_payload(self, pool):
        child = self.children[reference_owner(pool, self.count)]
        return {
            "protocol": "pool_redirect_v1", "host": "127.0.0.1", "port": child.port,
            "ws_port": child.ws_port, "pool": pool, "pod_index": child.index,
            "pod_count": self.count, "router_host": "127.0.0.1", "router_port": self.tcp_port,
            "router_ws_port": self.ws_port, "cluster_id": self.snapshot()["cluster_id"],
        }

    def assert_redirect(self, reply, request_id, client_id, pool):
        assert reply == {
            "type": "redirect", "request_id": request_id, "client_id": client_id, "pool": pool,
            "payload": self.redirect_payload(pool),
        }
        assert isinstance(reply["payload"]["cluster_id"], str) and reply["payload"]["cluster_id"]

    async def join(self, client_id, pool, transport="tcp", auth_token=None, peer=None,
                   operation="join_pool", expect="ack"):
        peer = peer or await self.open(transport)
        visited = {(peer.host, peer.port)}
        redirects = []
        join_messages = []
        for hop in range(5):
            if hop == 0 and operation == "switch_pool":
                assert peer.client_id == client_id
            else:
                peer.client_id = client_id
                hello = await peer.hello()
                assert hello["payload"]["server"] == "latzero-server"
                assert hello["client_id"] == client_id
            kind = operation if hop == 0 else "join_pool"
            message = peer.message(kind, {"client_id": client_id, "pool": pool, "auth_token": auth_token})
            join_messages.append(message)
            await peer.send(message)
            reply = await peer.receive(("ack", "error", "redirect"), message["request_id"])
            if reply["type"] != "redirect":
                assert reply["type"] == expect, reply
                if expect == "ack":
                    assert reply["client_id"] == client_id
                    assert reply["pool"] == reply["payload"]["pool"] == pool
                    peer.client_id, peer.pool = client_id, pool
                peer.redirects = redirects
                peer.visited = list(visited)
                peer.join_messages = join_messages
                return peer, reply
            self.assert_redirect(reply, message["request_id"], client_id, pool)
            assert hop < 4, "Redirect hop budget exceeded"
            redirects.append(reply)
            await peer.expect_closed()
            await peer.close()
            endpoint = (reply["payload"]["host"], reply["payload"]["ws_port" if transport == "ws" else "port"])
            assert endpoint not in visited, "Redirect endpoint cycle"
            visited.add(endpoint)
            peer = await self.open(transport, port=endpoint[1])
        raise AssertionError("Redirect hop budget exceeded")

    async def close(self):
        results = await asyncio.gather(*(peer.close() for peer in self.clients), return_exceptions=True)
        try:
            await asyncio.wait_for(self.supervisor.stop(), 40)
        except RuntimeError as exc:
            if not self.expect_shutdown_error:
                diagnostics = [{"index": child.index, "pid": child.pid,
                                "code": child.process.returncode if child.process is not None else None,
                                "stderr": "".join(child.stderr_tail)} for child in self.child_records]
                raise AssertionError("%s; child diagnostics: %r" % (exc, diagnostics)) from exc
            assert self.snapshot()["healthy"] is False
        assert all(child.process is None or child.process.returncode is not None for child in self.child_records), "Supervisor did not reap every child"
        for result in results:
            if isinstance(result, BaseException):
                raise result


@asynccontextmanager
async def pod_cluster(data_dir, pods=4, **options):
    cluster = PodCluster(data_dir, pods, **options)
    try:
        await cluster.start()
        yield cluster
    finally:
        await cluster.close()


async def assert_listener_closed(port):
    if port is None or port == 0:
        return
    with suppress(ConnectionResetError):
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port), 3)
        except OSError:
            return
        writer.close()
        await asyncio.wait_for(writer.wait_closed(), 3)
    raise AssertionError("Stopped listener still accepted TCP connections on port %d" % port)
