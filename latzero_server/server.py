"""
Async TCP and WebSocket server for LatZero pools, buffers, and routed app messaging.
"""

import asyncio
import hashlib
import json
import time
import uuid
from collections import deque
from contextlib import suppress
from typing import Any, Deque, Dict, Optional

import websockets
from websockets.server import WebSocketServerProtocol

from .config import ServerConfig
from .models import BufferEntry, ClientSession, PoolState, RouteEntry
from .persistence import SnapshotStore
from .protocol import decode_message, encode_message

try:
    import psutil
except ImportError:
    psutil = None


class LatZeroServer:
    """Local TCP and WebSocket server for LatZero server mode."""

    def __init__(self, config: Optional[ServerConfig] = None):
        self.config = config or ServerConfig()
        self._pools: Dict[str, PoolState] = {}
        self._tcp_server: Optional[asyncio.base_events.Server] = None
        self._websocket_server: Optional[websockets.WebSocketServer] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._store = SnapshotStore(self.config.data_dir)
        self._event_log: Deque[dict] = deque(maxlen=300)
        self._started_at = time.time()
        self._load_snapshots()

        # Metrics tracking
        self._msg_counter = 0
        self._last_tick = time.time()
        self._current_tps = 0.0
        self._request_latencies: Deque[float] = deque(maxlen=100)

    def _load_snapshots(self) -> None:
        for pool_id, snapshot in self._store.load_pools().items():
            pool = PoolState(
                pool_id=pool_id,
                auth_required=snapshot.get("auth_required", False),
                auth_token_hash=snapshot.get("auth_token_hash"),
            )
            for key, payload in snapshot.get("buffers", {}).items():
                pool.buffers[key] = BufferEntry.from_dict(payload)
            self._pools[pool_id] = pool

    async def start(self) -> None:
        """Start the TCP and WebSocket servers and background cleanup task."""
        if self._tcp_server is not None or self._websocket_server is not None:
            return
            
        # Start TCP server
        self._tcp_server = await asyncio.start_server(
            self._handle_connection,
            host=self.config.host,
            port=self.config.port,
        )
        
        # Start WebSocket server on port + 1
        ws_port = self.config.port + 1
        self._websocket_server = await websockets.serve(
            self._handle_websocket_connection,
            host=self.config.host,
            port=ws_port,
        )
        
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        self._record_event("info", "server_started", extra={
            "host": self.config.host, 
            "tcp_port": self.config.port,
            "websocket_port": ws_port
        })

    async def serve_forever(self) -> None:
        """Run the server until cancelled."""
        await self.start()
        assert self._tcp_server is not None and self._websocket_server is not None
        async with self._tcp_server:
            await self._tcp_server.serve_forever()

    async def stop(self) -> None:
        """Gracefully stop the server."""
        self._record_event("info", "server_stopping")
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._cleanup_task
            self._cleanup_task = None
        if self._tcp_server is not None:
            self._tcp_server.close()
            await self._tcp_server.wait_closed()
            self._tcp_server = None
        if self._websocket_server is not None:
            self._websocket_server.close()
            await self._websocket_server.wait_closed()
            self._websocket_server = None

    async def _cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.cleanup_interval)
            await self._expire_buffers()
            await self._expire_routes()

    async def _expire_buffers(self) -> None:
        now = time.time()
        for pool in self._pools.values():
            expired = [
                key for key, entry in pool.buffers.items()
                if entry.ttl is not None and (entry.updated_at + entry.ttl) <= now
            ]
            for key in expired:
                entry = pool.buffers.pop(key)
                await self._notify_buffer_update(pool, key, "expired", entry)
                self._store.save_pool(pool)
                self._record_event("warn", "buffer_expired", pool=pool.pool_id, extra={"key": key})

    async def _expire_routes(self) -> None:
        now = time.time()
        for pool in self._pools.values():
            expired = [
                request_id
                for request_id, route in pool.in_flight_requests.items()
                if route.expires_at is not None and route.expires_at <= now
            ]
            for request_id in expired:
                route = pool.in_flight_requests.pop(request_id)
                await self._send_to_client(
                    pool,
                    route.response_client_id,
                    {
                        "type": "error",
                        "request_id": request_id,
                        "client_id": route.target_client_id,
                        "pool": pool.pool_id,
                        "payload": {
                            "code": "timeout",
                            "message": f"Request '{request_id}' timed out",
                        },
                    },
                )

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        session = ClientSession(client_id="", writer=writer, pool_id=None)
        try:
            while not reader.at_eof():
                try:
                    raw = await reader.readline()
                    if not raw:
                        break
                    message = None
                    try:
                        message = decode_message(raw)
                        await self._dispatch(session, message)
                    except Exception as exc:
                        await self._send_message(
                            writer,
                            {
                                "type": "error",
                                "request_id": message.get("request_id") if isinstance(message, dict) else None,
                                "client_id": session.client_id,
                                "pool": session.pool_id,
                                "payload": {
                                    "code": "protocol_error",
                                    "message": str(exc),
                                },
                            },
                        )
                        self._record_event(
                            "error",
                            "protocol_error",
                            pool=session.pool_id,
                            client_id=session.client_id or None,
                            extra={"message": str(exc)},
                        )
                except (ConnectionResetError, ConnectionAbortedError, OSError):
                    # Client disconnected abruptly - this is normal behavior
                    break
        finally:
            await self._disconnect(session)
            writer.close()
            with suppress((ConnectionResetError, ConnectionAbortedError, OSError, Exception)):
                await writer.wait_closed()

    async def _handle_websocket_connection(self, websocket: WebSocketServerProtocol) -> None:
        """Handle WebSocket connections from web clients."""
        session = ClientSession(client_id="", writer=websocket, pool_id=None)
        try:
            async for message in websocket:
                try:
                    # WebSocket messages are already strings, parse as JSON
                    message_dict = json.loads(message)
                    await self._dispatch(session, message_dict)
                except json.JSONDecodeError as exc:
                    await self._send_error(websocket, None, "protocol_error", str(exc))
                    self._record_event(
                        "error",
                        "protocol_error",
                        pool=session.pool_id,
                        client_id=session.client_id or None,
                        extra={"message": str(exc)},
                    )
                except (ConnectionResetError, ConnectionAbortedError, OSError):
                    # Client disconnected abruptly - this is normal behavior
                    break
        finally:
            await self._disconnect(session)

    async def _dispatch(self, session: ClientSession, message: dict) -> None:
        self._msg_counter += 1
        msg_type = message.get("type")
        if msg_type == "hello":
            await self._ack(session.writer, message, {"server": "latzero-server"})
            return
        if msg_type == "join_pool":
            await self._handle_join_pool(session, message)
            return
        if msg_type == "switch_pool":
            await self._handle_switch_pool(session, message)
            return
        if msg_type == "leave_pool":
            await self._disconnect(session, keep_connection=True)
            await self._ack(session.writer, message, {"left_pool": True})
            return

        pool = self._require_pool(session, message)
        if msg_type == "set_buffer":
            await self._handle_set_buffer(session, pool, message)
        elif msg_type == "get_buffer":
            await self._handle_get_buffer(session, pool, message)
        elif msg_type == "delete_buffer":
            await self._handle_delete_buffer(session, pool, message)
        elif msg_type == "list_buffers":
            await self._handle_list_buffers(session, pool, message)
        elif msg_type == "subscribe_buffer":
            await self._handle_subscribe_buffer(session, pool, message)
        elif msg_type == "unsubscribe_buffer":
            await self._handle_unsubscribe_buffer(session, pool, message)
        elif msg_type == "call_app":
            await self._handle_call_app(session, pool, message)
        elif msg_type == "app_result":
            await self._handle_app_result(session, pool, message)
        elif msg_type == "emit_event":
            await self._handle_emit_event(session, pool, message)
        elif msg_type == "register_process":
            await self._handle_register_process(session, pool, message)
        elif msg_type == "unregister_process":
            await self._handle_unregister_process(session, pool, message)
        elif msg_type == "call_process":
            await self._handle_call_process(session, pool, message)
        elif msg_type == "broadcast_process":
            await self._handle_broadcast_process(session, pool, message)
        elif msg_type == "list_processes":
            await self._handle_list_processes(session, pool, message)
        else:
            raise ValueError(f"Unsupported message type: {msg_type}")

    def _require_pool(self, session: ClientSession, message: dict) -> PoolState:
        if not session.pool_id:
            raise ValueError("Client is not in a pool")
        pool = self._pools.get(session.pool_id)
        if pool is None:
            raise ValueError("Pool does not exist")
        return pool

    async def _handle_join_pool(self, session: ClientSession, message: dict) -> None:
        payload = message.get("payload") or {}
        client_id = payload.get("client_id") or message.get("client_id")
        pool_id = payload.get("pool") or message.get("pool")
        auth_token = payload.get("auth_token")

        if not client_id:
            raise ValueError("client_id is required")
        if not pool_id:
            raise ValueError("pool is required")

        pool = self._pools.get(pool_id)
        if pool is None:
            pool = PoolState(
                pool_id=pool_id,
                auth_required=bool(auth_token),
                auth_token_hash=self._hash_token(auth_token) if auth_token else None,
            )
            self._pools[pool_id] = pool
            self._store.save_pool(pool)
            self._record_event(
                "info",
                "pool_created",
                pool=pool_id,
                client_id=client_id,
                extra={"auth_required": pool.auth_required},
            )

        if pool.auth_required:
            if not auth_token or self._hash_token(auth_token) != pool.auth_token_hash:
                await self._send_error(session.writer, message, "auth_failed", "Pool authentication failed")
                self._record_event("warn", "auth_failed", pool=pool_id, client_id=client_id)
                return

        existing = pool.clients.get(client_id)
        if existing is not None and existing.writer is not session.writer:
            await self._send_error(session.writer, message, "duplicate_client", "Client ID is already connected")
            self._record_event("warn", "duplicate_client", pool=pool_id, client_id=client_id)
            return

        if session.pool_id and session.pool_id != pool_id:
            await self._disconnect(session, keep_connection=True)

        session.client_id = client_id
        session.pool_id = pool_id
        pool.clients[client_id] = session

        await self._ack(
            session.writer,
            message,
            {
                "pool": pool_id,
                "auth_required": pool.auth_required,
                "clients": sorted(pool.clients.keys()),
            },
        )
        await self._broadcast_presence(pool, client_id, "joined")
        self._record_event("info", "client_joined", pool=pool_id, client_id=client_id)

    async def _handle_switch_pool(self, session: ClientSession, message: dict) -> None:
        await self._handle_join_pool(session, message)

    async def _handle_set_buffer(self, session: ClientSession, pool: PoolState, message: dict) -> None:
        payload = message.get("payload") or {}
        key = payload.get("key")
        if not key:
            raise ValueError("key is required")
        now = time.time()
        existing = pool.buffers.get(key)
        version = 1 if existing is None else existing.version + 1
        entry = BufferEntry(
            value=payload.get("value"),
            updated_at=now,
            updated_by=session.client_id,
            persistent=bool(payload.get("persistent", False)),
            ttl=payload.get("ttl"),
            version=version,
        )
        pool.buffers[key] = entry
        self._store.save_pool(pool)
        await self._ack(session.writer, message, {"key": key, "version": version})
        await self._notify_buffer_update(pool, key, "set", entry)
        self._record_event(
            "info",
            "buffer_set",
            pool=pool.pool_id,
            client_id=session.client_id,
            extra={"key": key, "persistent": entry.persistent, "version": version},
        )

    async def _handle_get_buffer(self, session: ClientSession, pool: PoolState, message: dict) -> None:
        payload = message.get("payload") or {}
        key = payload.get("key")
        if not key:
            raise ValueError("key is required")
        entry = pool.buffers.get(key)
        await self._ack(
            session.writer,
            message,
            {
                "key": key,
                "exists": entry is not None,
                "entry": entry.to_dict() if entry else None,
            },
        )

    async def _handle_delete_buffer(self, session: ClientSession, pool: PoolState, message: dict) -> None:
        payload = message.get("payload") or {}
        key = payload.get("key")
        if not key:
            raise ValueError("key is required")
        entry = pool.buffers.pop(key, None)
        self._store.save_pool(pool)
        await self._ack(session.writer, message, {"key": key, "deleted": entry is not None})
        if entry is not None:
            await self._notify_buffer_update(pool, key, "delete", entry)
            self._record_event("info", "buffer_deleted", pool=pool.pool_id, client_id=session.client_id, extra={"key": key})

    async def _handle_list_buffers(self, session: ClientSession, pool: PoolState, message: dict) -> None:
        payload = message.get("payload") or {}
        pattern = payload.get("pattern")
        keys = sorted(pool.buffers.keys())
        if pattern:
            keys = [key for key in keys if key.startswith(pattern)]
        await self._ack(session.writer, message, {"keys": keys})

    async def _handle_subscribe_buffer(self, session: ClientSession, pool: PoolState, message: dict) -> None:
        payload = message.get("payload") or {}
        key = payload.get("key")
        if not key:
            raise ValueError("key is required")
        pool.subscriptions.setdefault(key, set()).add(session.client_id)
        await self._ack(session.writer, message, {"key": key, "subscribed": True})
        self._record_event("info", "buffer_subscribed", pool=pool.pool_id, client_id=session.client_id, extra={"key": key})

    async def _handle_unsubscribe_buffer(self, session: ClientSession, pool: PoolState, message: dict) -> None:
        payload = message.get("payload") or {}
        key = payload.get("key")
        if not key:
            raise ValueError("key is required")
        subscribers = pool.subscriptions.get(key, set())
        subscribers.discard(session.client_id)
        if not subscribers and key in pool.subscriptions:
            del pool.subscriptions[key]
        await self._ack(session.writer, message, {"key": key, "subscribed": False})
        self._record_event("info", "buffer_unsubscribed", pool=pool.pool_id, client_id=session.client_id, extra={"key": key})

    async def _handle_call_app(self, session: ClientSession, pool: PoolState, message: dict) -> None:
        payload = message.get("payload") or {}
        target_client_id = payload.get("target_client_id")
        event = payload.get("event")
        if not target_client_id or not event:
            raise ValueError("target_client_id and event are required")
        target = pool.clients.get(target_client_id)
        if target is None:
            await self._send_error(session.writer, message, "target_not_found", "Target client is not connected")
            self._record_event(
                "warn",
                "call_target_missing",
                pool=pool.pool_id,
                client_id=session.client_id,
                extra={"target_client_id": target_client_id, "event": event},
            )
            return

        request_id = message.get("request_id") or str(uuid.uuid4())
        timeout = payload.get("timeout")
        response_client_id = payload.get("response_to") or session.client_id
        route = RouteEntry(
            request_id=request_id,
            origin_client_id=session.client_id,
            target_client_id=target_client_id,
            response_client_id=response_client_id,
            event=event,
            created_at=time.time(),
            expires_at=(time.time() + timeout) if timeout else None,
        )
        pool.in_flight_requests[request_id] = route

        await self._send_message(
            target.writer,
            {
                "type": "call_app",
                "request_id": request_id,
                "client_id": session.client_id,
                "pool": pool.pool_id,
                "payload": {
                    "event": event,
                    "data": payload.get("data", {}),
                    "source_client_id": session.client_id,
                    "target_client_id": target_client_id,
                    "response_to": response_client_id,
                },
            },
        )
        await self._ack(session.writer, message, {"request_id": request_id, "queued": True})
        self._record_event(
            "info",
            "app_call_routed",
            pool=pool.pool_id,
            client_id=session.client_id,
            extra={
                "request_id": request_id,
                "target_client_id": target_client_id,
                "response_client_id": response_client_id,
                "event": event,
            },
        )

    async def _handle_app_result(self, session: ClientSession, pool: PoolState, message: dict) -> None:
        request_id = message.get("request_id")
        if not request_id:
            raise ValueError("request_id is required")
        route = pool.in_flight_requests.pop(request_id, None)
        if route is None:
            await self._send_error(session.writer, message, "route_not_found", "Request route no longer exists")
            self._record_event("warn", "route_not_found", pool=pool.pool_id, client_id=session.client_id, extra={"request_id": request_id})
            return
        await self._send_to_client(
            pool,
            route.response_client_id,
            {
                "type": "app_result",
                "request_id": request_id,
                "client_id": route.target_client_id,
                "pool": pool.pool_id,
                "payload": {
                    "event": route.event,
                    "source_client_id": route.origin_client_id,
                    "target_client_id": route.target_client_id,
                    "response_to": route.response_client_id,
                    "value": (message.get("payload") or {}).get("value"),
                    "error": (message.get("payload") or {}).get("error"),
                },
            },
        )
        latency = time.time() - route.created_at
        self._request_latencies.append(latency)
        await self._ack(session.writer, message, {"delivered": True})
        self._record_event(
            "info",
            "app_result_delivered",
            pool=pool.pool_id,
            client_id=session.client_id,
            extra={"request_id": request_id, "response_client_id": route.response_client_id},
        )

    async def _handle_emit_event(self, session: ClientSession, pool: PoolState, message: dict) -> None:
        payload = message.get("payload") or {}
        event = payload.get("event")
        if not event:
            raise ValueError("event is required")
        target_client_id = payload.get("target_client_id")
        envelope = {
            "type": "emit_event",
            "request_id": message.get("request_id"),
            "client_id": session.client_id,
            "pool": pool.pool_id,
            "payload": {
                "event": event,
                "data": payload.get("data", {}),
                "source_client_id": session.client_id,
                "target_client_id": target_client_id,
                "response_to": payload.get("response_to"),
            },
        }
        if target_client_id:
            target = pool.clients.get(target_client_id)
            if target is None:
                await self._send_error(session.writer, message, "target_not_found", "Target client is not connected")
                self._record_event(
                    "warn",
                    "emit_target_missing",
                    pool=pool.pool_id,
                    client_id=session.client_id,
                    extra={"target_client_id": target_client_id, "event": event},
                )
                return
            await self._send_message(target.writer, envelope)
        else:
            for client_id, target in pool.clients.items():
                if client_id == session.client_id:
                    continue
                await self._send_message(target.writer, envelope)
        await self._ack(session.writer, message, {"delivered": True})
        self._record_event(
            "info",
            "event_emitted",
            pool=pool.pool_id,
            client_id=session.client_id,
            extra={"event": event, "target_client_id": target_client_id},
        )

    async def _disconnect(self, session: ClientSession, keep_connection: bool = False) -> None:
        if not session.pool_id or not session.client_id:
            return
        pool = self._pools.get(session.pool_id)
        if pool is None:
            session.pool_id = None
            return
        pool.clients.pop(session.client_id, None)
        for key, subscribers in list(pool.subscriptions.items()):
            subscribers.discard(session.client_id)
            if not subscribers:
                del pool.subscriptions[key]
        for request_id, route in list(pool.in_flight_requests.items()):
            if session.client_id in {
                route.origin_client_id,
                route.target_client_id,
                route.response_client_id,
            }:
                del pool.in_flight_requests[request_id]
        # Unregister all processes owned by this client
        owned_processes = [pid for pid, cid in pool.processes.items() if cid == session.client_id]
        for pid in owned_processes:
            del pool.processes[pid]
            self._record_event("info", "process_unregistered", pool=pool.pool_id, client_id=session.client_id, extra={"process_id": pid, "reason": "client_disconnected"})
        await self._broadcast_presence(pool, session.client_id, "left")
        self._record_event("info", "client_left", pool=pool.pool_id, client_id=session.client_id)
        session.pool_id = None
        if not keep_connection:
            session.client_id = ""

    async def _handle_register_process(self, session: ClientSession, pool: PoolState, message: dict) -> None:
        payload = message.get("payload") or {}
        process_name = payload.get("process_name")
        if not process_name:
            raise ValueError("process_name is required")
        process_id = f"{session.client_id}:{process_name}"
        pool.processes[process_id] = session.client_id
        await self._ack(session.writer, message, {"process_id": process_id})
        self._record_event("info", "process_registered", pool=pool.pool_id, client_id=session.client_id, extra={"process_id": process_id})

    async def _handle_unregister_process(self, session: ClientSession, pool: PoolState, message: dict) -> None:
        payload = message.get("payload") or {}
        process_name = payload.get("process_name")
        if not process_name:
            raise ValueError("process_name is required")
        process_id = f"{session.client_id}:{process_name}"
        removed = process_id in pool.processes
        pool.processes.pop(process_id, None)
        await self._ack(session.writer, message, {"process_id": process_id, "removed": removed})
        if removed:
            self._record_event("info", "process_unregistered", pool=pool.pool_id, client_id=session.client_id, extra={"process_id": process_id})

    async def _handle_call_process(self, session: ClientSession, pool: PoolState, message: dict) -> None:
        payload = message.get("payload") or {}
        process_id = payload.get("process_id")
        if not process_id:
            raise ValueError("process_id is required")
        target_client_id = pool.processes.get(process_id)
        if target_client_id is None:
            await self._send_error(session.writer, message, "process_not_found", f"Process '{process_id}' is not registered")
            self._record_event("warn", "process_not_found", pool=pool.pool_id, client_id=session.client_id, extra={"process_id": process_id})
            return
        target = pool.clients.get(target_client_id)
        if target is None:
            await self._send_error(session.writer, message, "process_owner_offline", f"Process owner '{target_client_id}' is not connected")
            self._record_event("warn", "process_owner_offline", pool=pool.pool_id, client_id=session.client_id, extra={"process_id": process_id})
            return
        request_id = message.get("request_id") or str(uuid.uuid4())
        timeout = payload.get("timeout")
        response_client_id = payload.get("response_to") or session.client_id
        route = RouteEntry(
            request_id=request_id,
            origin_client_id=session.client_id,
            target_client_id=target_client_id,
            response_client_id=response_client_id,
            event=process_id,
            created_at=time.time(),
            expires_at=(time.time() + timeout) if timeout else None,
        )
        pool.in_flight_requests[request_id] = route
        await self._send_message(
            target.writer,
            {
                "type": "call_app",
                "request_id": request_id,
                "client_id": session.client_id,
                "pool": pool.pool_id,
                "payload": {
                    "event": process_id,
                    "data": payload.get("data", {}),
                    "source_client_id": session.client_id,
                    "target_client_id": target_client_id,
                    "response_to": response_client_id,
                },
            },
        )
        await self._ack(session.writer, message, {"process_id": process_id, "queued": True})
        self._record_event("info", "process_called", pool=pool.pool_id, client_id=session.client_id, extra={"process_id": process_id, "response_client_id": response_client_id})

    async def _handle_broadcast_process(self, session: ClientSession, pool: PoolState, message: dict) -> None:
        payload = message.get("payload") or {}
        process_name = payload.get("process_name")
        if not process_name:
            raise ValueError("process_name is required")
        suffix = f":{process_name}"
        matching = {pid: cid for pid, cid in pool.processes.items() if pid.endswith(suffix)}
        response_client_id = payload.get("response_to") or session.client_id
        timeout = payload.get("timeout")
        targets = []
        for process_id, target_client_id in matching.items():
            target = pool.clients.get(target_client_id)
            if target is None:
                continue
            req_id = str(uuid.uuid4())
            route = RouteEntry(
                request_id=req_id,
                origin_client_id=session.client_id,
                target_client_id=target_client_id,
                response_client_id=response_client_id,
                event=process_id,
                created_at=time.time(),
                expires_at=(time.time() + timeout) if timeout else None,
            )
            pool.in_flight_requests[req_id] = route
            await self._send_message(
                target.writer,
                {
                    "type": "call_app",
                    "request_id": req_id,
                    "client_id": session.client_id,
                    "pool": pool.pool_id,
                    "payload": {
                        "event": process_id,
                        "data": payload.get("data", {}),
                        "source_client_id": session.client_id,
                        "target_client_id": target_client_id,
                        "response_to": response_client_id,
                    },
                },
            )
            targets.append(process_id)
        await self._ack(session.writer, message, {"targets": targets})
        self._record_event("info", "process_broadcast", pool=pool.pool_id, client_id=session.client_id, extra={"process_name": process_name, "targets": targets})

    async def _handle_list_processes(self, session: ClientSession, pool: PoolState, message: dict) -> None:
        payload = message.get("payload") or {}
        pattern = payload.get("pattern")
        processes = dict(pool.processes)
        if pattern:
            prefix = f"{pattern}:"
            processes = {pid: cid for pid, cid in processes.items() if pid.startswith(prefix)}
        await self._ack(session.writer, message, {"processes": processes})

    async def _broadcast_presence(self, pool: PoolState, client_id: str, status: str) -> None:
        message = {
            "type": "presence_update",
            "request_id": None,
            "client_id": client_id,
            "pool": pool.pool_id,
            "payload": {
                "client_id": client_id,
                "status": status,
                "pool": pool.pool_id,
                "clients": sorted(pool.clients.keys()),
            },
        }
        for target in list(pool.clients.values()):
            with suppress(Exception):
                await self._send_message(target.writer, message)

    async def _notify_buffer_update(
        self,
        pool: PoolState,
        key: str,
        operation: str,
        entry: BufferEntry,
    ) -> None:
        subscribers = pool.subscriptions.get(key, set())
        if not subscribers:
            return
        message = {
            "type": "buffer_update",
            "request_id": None,
            "client_id": entry.updated_by,
            "pool": pool.pool_id,
            "payload": {
                "key": key,
                "operation": operation,
                "entry": entry.to_dict(),
            },
        }
        for client_id in list(subscribers):
            await self._send_to_client(pool, client_id, message)

    async def _send_to_client(self, pool: PoolState, client_id: str, message: dict) -> None:
        target = pool.clients.get(client_id)
        if target is None:
            return
        with suppress(Exception):
            await self._send_message(target.writer, message)

    async def _ack(self, writer: asyncio.StreamWriter, request: dict, payload: dict) -> None:
        await self._send_message(
            writer,
            {
                "type": "ack",
                "request_id": request.get("request_id"),
                "client_id": request.get("client_id"),
                "pool": request.get("pool"),
                "payload": payload,
            },
        )

    async def _send_error(self, writer: asyncio.StreamWriter, request: dict, code: str, message: str) -> None:
        await self._send_message(
            writer,
            {
                "type": "error",
                "request_id": request.get("request_id"),
                "client_id": request.get("client_id"),
                "pool": request.get("pool"),
                "payload": {
                    "code": code,
                    "message": message,
                },
            },
        )

    async def _send_message(self, writer, message: Dict[str, Any]) -> None:
        """Send message to either TCP or WebSocket connection."""
        if hasattr(writer, 'write'):  # TCP connection
            writer.write(encode_message(message))
            await writer.drain()
        else:  # WebSocket connection
            await writer.send(json.dumps(message))

    @staticmethod
    def _hash_token(token: Optional[str]) -> Optional[str]:
        if token is None:
            return None
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def _record_event(
        self,
        level: str,
        event: str,
        pool: Optional[str] = None,
        client_id: Optional[str] = None,
        extra: Optional[dict] = None,
    ) -> None:
        self._event_log.appendleft(
            {
                "time": time.time(),
                "level": level,
                "event": event,
                "pool": pool,
                "client_id": client_id,
                "extra": extra or {},
            }
        )

    def get_dashboard_snapshot(self) -> dict:
        """Return a serializable snapshot for the server TUI."""
        now = time.time()
        dt = now - self._last_tick
        if dt >= 1.0:
            self._current_tps = self._msg_counter / dt
            self._msg_counter = 0
            self._last_tick = now

        avg_latency = sum(self._request_latencies) / len(self._request_latencies) if self._request_latencies else 0.0
        memory_rss = psutil.Process().memory_info().rss if psutil else 0

        pools = []
        total_clients = 0
        total_buffers = 0
        total_subscriptions = 0
        total_routes = 0
        total_processes = 0

        for pool_id in sorted(self._pools.keys()):
            pool = self._pools[pool_id]
            client_ids = sorted(pool.clients.keys())
            total_clients += len(client_ids)
            total_buffers += len(pool.buffers)
            total_subscriptions += sum(len(subscribers) for subscribers in pool.subscriptions.values())
            total_routes += len(pool.in_flight_requests)
            total_processes += len(pool.processes)
            pools.append(
                {
                    "pool_id": pool_id,
                    "auth_required": pool.auth_required,
                    "clients": client_ids,
                    "buffers": [
                        {
                            "key": key,
                            **entry.to_dict(),
                            "subscriber_count": len(pool.subscriptions.get(key, set())),
                        }
                        for key, entry in sorted(pool.buffers.items())
                    ],
                    "subscriptions": {
                        key: sorted(subscribers)
                        for key, subscribers in sorted(pool.subscriptions.items())
                    },
                    "in_flight_requests": [
                        {
                            "request_id": route.request_id,
                            "origin_client_id": route.origin_client_id,
                            "target_client_id": route.target_client_id,
                            "response_client_id": route.response_client_id,
                            "event": route.event,
                            "created_at": route.created_at,
                            "expires_at": route.expires_at,
                        }
                        for route in pool.in_flight_requests.values()
                    ],
                    "processes": pool.processes,
                }
            )

        return {
            "host": self.config.host,
            "port": self.config.port,
            "data_dir": str(self.config.data_dir),
            "started_at": self._started_at,
            "pool_count": len(self._pools),
            "client_count": total_clients,
            "buffer_count": total_buffers,
            "subscription_count": total_subscriptions,
            "route_count": total_routes,
            "process_count": total_processes,
            "tps": self._current_tps,
            "avg_latency": avg_latency,
            "memory_rss": memory_rss,
            "pools": pools,
            "events": list(self._event_log),
        }
