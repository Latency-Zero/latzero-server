import asyncio
import json
from collections import deque

import pytest
import pytest_asyncio
from websockets.legacy.client import connect

from latzero_server.config import ServerConfig
from latzero_server.server import LatZeroServer


@pytest.fixture(autouse=True)
def preserve_event_loop_policy(monkeypatch):
    # asyncio.run clears the policy slot; mixed sync/async tests must not orphan
    # the idle loop restored by pytest-asyncio between test runners.
    run = asyncio.run

    def preserving_run(coroutine, **options):
        policy = asyncio.get_event_loop_policy()
        try:
            previous = policy.get_event_loop()
        except RuntimeError:
            previous = None
        try:
            return run(coroutine, **options)
        finally:
            policy.set_event_loop(previous if previous is not None and not previous.is_closed() else None)

    monkeypatch.setattr(asyncio, "run", preserving_run)


class RawClient:
    def __init__(self, reader=None, writer=None, websocket=None):
        self.reader = reader
        self.writer = writer
        self.websocket = websocket
        self.pending = deque()
        self.sequence = 0
        self.client_id = None
        self.pool = None

    async def send(self, message):
        encoded = json.dumps(message, allow_nan=False)
        if self.websocket is not None:
            await self.websocket.send(encoded)
        else:
            self.writer.write((encoded + "\n").encode())
            await self.writer.drain()

    async def receive(self, msg_type, request_id=None):
        def matches(message):
            return message.get("type") == msg_type and (
                request_id is None or message.get("request_id") == request_id
            )

        for message in list(self.pending):
            if matches(message):
                self.pending.remove(message)
                return message
        for _ in range(100):
            if self.websocket is not None:
                raw = await asyncio.wait_for(self.websocket.recv(), 2)
            else:
                raw = await asyncio.wait_for(self.reader.readline(), 2)
                assert raw, "Connection closed before the expected response"
            message = json.loads(raw)
            if matches(message):
                return message
            self.pending.append(message)
        raise AssertionError("Expected response missing from bounded fixture reads")

    async def request(self, msg_type, payload=None, request_id=None, response="ack"):
        self.sequence += 1
        request_id = request_id or f"test-{self.sequence}"
        await self.send({
            "type": msg_type,
            "request_id": request_id,
            "client_id": self.client_id,
            "pool": None,
            "payload": payload or {},
        })
        return await self.receive(response, request_id)

    async def join(self, client_id, pool="test-pool", auth_token=None):
        await self.request("hello")
        reply = await self.request("join_pool", {
            "client_id": client_id, "pool": pool, "auth_token": auth_token,
        })
        self.client_id = client_id
        self.pool = pool
        return reply

    async def close(self):
        if self.websocket is not None:
            await self.websocket.close()
        else:
            self.writer.close()
            await self.writer.wait_closed()


@pytest_asyncio.fixture
async def daemon(tmp_path):
    config = ServerConfig(
        port=0, websocket_port=0, data_dir=tmp_path,
        min_workers=2, max_workers=4,
    )
    server = LatZeroServer(config)
    await server.start()
    clients = []

    async def client(transport="tcp"):
        if transport == "ws":
            port = server._websocket_server.sockets[0].getsockname()[1]
            websocket = await connect(f"ws://127.0.0.1:{port}")
            result = RawClient(websocket=websocket)
        else:
            port = server._tcp_server.sockets[0].getsockname()[1]
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            result = RawClient(reader=reader, writer=writer)
        clients.append(result)
        return result

    try:
        yield server, client
    finally:
        await asyncio.gather(*(c.close() for c in clients), return_exceptions=True)
        await asyncio.wait_for(server.stop(), 10)
