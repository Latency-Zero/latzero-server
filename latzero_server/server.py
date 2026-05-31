"""
Async TCP and WebSocket server for LatZero pools, buffers, and routed app messaging.

Load-handling architecture
--------------------------
Connection handlers (one per client) are thin readers that do nothing but
parse the incoming line and enqueue it.  All dispatch logic runs inside the
AutoScalingWorkerPool (4–1280 asyncio Tasks) so the reader is never blocked
by business logic.

Key optimisations
-----------------
- Worker pool wired in: _handle_connection / _handle_websocket_connection
  submit messages to the pool instead of calling _dispatch inline.
- Connection admission control: asyncio.Semaphore(max_connections) + server_busy
  response protects against connection storms / OOM.
- Dispatch table: dict[str, handler] replaces the 14-branch if/elif chain.
- Fast internal IDs: itertools.count() counter instead of uuid4() on the hot path.
- Parallel broadcasts: asyncio.gather + encode-once for fan-out to N clients.
- Slow-consumer isolation: per-connection write-buffer HWM check; slow clients are
  skipped (HWM) or disconnected (critical).
- Async persistence: save_pool() → store.enqueue() (non-blocking put_nowait).
- Expiry min-heap: _expire_buffers no longer scans all buffers; it pops a heap.
- Snapshot caching: get_dashboard_snapshot() returns a cached dict for up to 1 s.
- Write coalescing: drain() skipped for small writes; asyncio batches naturally.
"""

import asyncio
import hashlib
import heapq
import itertools
import json
import time
import uuid
from collections import deque
from contextlib import suppress
from typing import Any, Callable, Coroutine, Deque, Dict, List, Optional, Tuple

import websockets
from websockets.server import WebSocketServerProtocol

from .config import ServerConfig
from .models import (
    BufferEntry,
    ClientSession,
    PoolState,
    ProcessReplica,
    ProcessRegistration,
    RouteEntry,
)
from .persistence import SnapshotStore
from .protocol import decode_message, encode_message
from .worker_pool import AutoScalingWorkerPool

try:
    import psutil
except ImportError:
    psutil = None


# ---------------------------------------------------------------------------
# Internal fast ID generator (NOT for client-visible request IDs)
# ---------------------------------------------------------------------------
_ID_COUNTER = itertools.count(1)


def _next_id() -> str:
    """Return a fast, unique server-internal routing ID."""
    return f"rq-{next(_ID_COUNTER)}"


class LatZeroServer:
    """Local TCP and WebSocket server for LatZero server mode."""

    def __init__(self, config: Optional[ServerConfig] = None):
        self.config = config or ServerConfig()
        self._pools: Dict[str, PoolState] = {}
        self._tcp_server: Optional[asyncio.base_events.Server] = None
        self._websocket_server: Optional[websockets.WebSocketServer] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._store = SnapshotStore(
            self.config.data_dir,
            batch_window=self.config.persistence_batch_window,
        )
        self._event_log: Deque[dict] = deque(maxlen=300)
        self._started_at = time.time()
        self._load_snapshots()

        # ── Admission control ────────────────────────────────────────────
        self._connection_semaphore = asyncio.Semaphore(self.config.max_connections)
        self._connection_count: int = 0

        # ── Worker pool ──────────────────────────────────────────────────
        self._worker_pool = AutoScalingWorkerPool(
            dispatch_fn=self._dispatch,
            min_workers=self.config.min_workers,
            max_workers=self.config.max_workers,
            scale_up_threshold=self.config.scale_up_threshold,
            scale_down_threshold=self.config.scale_down_threshold,
            scale_down_hold=self.config.scale_down_hold,
            controller_interval=self.config.controller_interval,
            max_step_up=self.config.max_step_up,
            burst_size=self.config.burst_size,
            emergency_multiplier=self.config.emergency_multiplier,
        )

        # ── Dispatch table ───────────────────────────────────────────────
        # Built once here; O(1) lookup per message instead of 14-branch if/elif.
        self._dispatch_table: Dict[str, Callable] = {
            "hello":              self._handle_hello,
            "join_pool":          self._handle_join_pool,
            "switch_pool":        self._handle_switch_pool,
            "leave_pool":         self._handle_leave_pool,
            "set_buffer":         self._handle_set_buffer,
            "get_buffer":         self._handle_get_buffer,
            "delete_buffer":      self._handle_delete_buffer,
            "list_buffers":       self._handle_list_buffers,
            "subscribe_buffer":   self._handle_subscribe_buffer,
            "unsubscribe_buffer": self._handle_unsubscribe_buffer,
            "call_app":           self._handle_call_app,
            "app_result":         self._handle_app_result,
            "emit_event":         self._handle_emit_event,
            "register_process":   self._handle_register_process,
            "unregister_process": self._handle_unregister_process,
            "call_process":       self._handle_call_process,
            "broadcast_process":  self._handle_broadcast_process,
            "list_processes":     self._handle_list_processes,
        }

        # ── Metrics ──────────────────────────────────────────────────────
        self._msg_counter = 0
        self._last_tick = time.time()
        self._current_tps = 0.0
        self._request_latencies: Deque[float] = deque(maxlen=200)

        # ── Expiry min-heap ──────────────────────────────────────────────
        # Each entry: (expires_at: float, pool_id: str, key: str)
        self._expiry_heap: List[Tuple[float, str, str]] = []

        # ── Snapshot cache ───────────────────────────────────────────────
        self._snapshot_cache: Optional[dict] = None
        self._snapshot_cache_at: float = 0.0
        self._snapshot_cache_ttl: float = 1.0  # seconds

    # ======================================================================
    # Startup / Shutdown
    # ======================================================================

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
        """Start TCP, WebSocket servers, worker pool, saver, and cleanup task."""
        if self._tcp_server is not None or self._websocket_server is not None:
            return

        # ── Windows ProactorEventLoop: suppress WinError 10054 ───────────
        # When a remote client closes abruptly, asyncio's proactor cleanup
        # calls sock.shutdown(SHUT_RDWR) on an already-dead socket and raises
        # WinError 10054 as an "unhandled exception in event loop".  We
        # install a custom handler that silently drops these expected errors.
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(self._loop_exception_handler)

        # Start async persistence saver
        self._store.start()

        # Start worker pool
        await self._worker_pool.start()

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
            "websocket_port": ws_port,
            "min_workers": self.config.min_workers,
            "max_workers": self.config.max_workers,
            "max_connections": self.config.max_connections,
        })

    @staticmethod
    def _loop_exception_handler(loop: asyncio.AbstractEventLoop, context: dict) -> None:
        """Custom asyncio exception handler.

        Suppresses expected Windows network errors that surface in the
        ProactorEventLoop's internal connection-lost callbacks:
          - WinError 10054  (connection reset by remote peer)
          - WinError 10053  (connection aborted by local software)
          - ConnectionResetError / ConnectionAbortedError

        All other exceptions are forwarded to the default handler.
        """
        exc = context.get("exception")
        if exc is None:
            loop.default_exception_handler(context)
            return

        # Check for Windows-specific error codes on OSError subclasses
        winerror = getattr(exc, "winerror", None)
        if winerror in (10054, 10053):   # WSAECONNRESET, WSAECONNABORTED
            return

        if isinstance(exc, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
            return

        loop.default_exception_handler(context)

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
        await self._worker_pool.stop()
        if self._tcp_server is not None:
            self._tcp_server.close()
            await self._tcp_server.wait_closed()
            self._tcp_server = None
        if self._websocket_server is not None:
            self._websocket_server.close()
            await self._websocket_server.wait_closed()
            self._websocket_server = None
        # Flush any pending persistence writes
        await self._store.stop()

    # ======================================================================
    # Cleanup loop
    # ======================================================================

    async def _cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.cleanup_interval)
            await self._expire_buffers()
            await self._expire_routes()
            await self._check_process_scaling()

    async def _expire_buffers(self) -> None:
        """O(log N) expiry using a min-heap instead of scanning all buffers."""
        now = time.time()
        while self._expiry_heap and self._expiry_heap[0][0] <= now:
            expires_at, pool_id, key = heapq.heappop(self._expiry_heap)
            pool = self._pools.get(pool_id)
            if pool is None:
                continue
            entry = pool.buffers.get(key)
            if entry is None:
                continue
            # Verify the TTL still matches (could have been updated/replaced)
            if entry.ttl is not None and (entry.updated_at + entry.ttl) <= now:
                pool.buffers.pop(key, None)
                await self._notify_buffer_update(pool, key, "expired", entry)
                self._store.enqueue(pool)
                self._record_event("warn", "buffer_expired", pool=pool_id, extra={"key": key})
            # else: the entry was refreshed; its new expiry is already on the heap

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

    async def _check_process_scaling(self) -> None:
        """
        Periodically evaluate scalable processes and send up/down commands.
        Called from _cleanup_loop.
        """
        now = time.time()
        cfg = self.config
        for pool in self._pools.values():
            for pid, reg in list(pool.processes.items()):
                if not reg.scale:
                    continue
                if now - reg.last_scale_action < cfg.process_scale_cooldown:
                    continue

                total_inflight = sum(r.in_flight for r in reg.replicas)
                n_replicas = len(reg.replicas)

                if total_inflight > cfg.process_scale_up_threshold and n_replicas < reg.max_replicas:
                    reg.last_scale_action = now
                    await self._send_to_client(
                        pool, reg.owner_client_id,
                        {
                            "type": "process_scale",
                            "request_id": _next_id(),
                            "client_id": reg.owner_client_id,
                            "pool": pool.pool_id,
                            "payload": {
                                "action": "up",
                                "process_name": reg.process_name,
                                "count": 1,
                                "group_id": reg.group_id,
                                "reason": f"high_load ({total_inflight} in-flight)",
                            },
                        },
                    )
                    self._record_event("info", "process_scale_up",
                        pool=pool.pool_id, client_id=reg.owner_client_id,
                        extra={"process_id": pid, "replicas": n_replicas + 1,
                               "total_inflight": total_inflight})

                elif total_inflight <= cfg.process_scale_down_threshold and n_replicas > 1:
                    reg.last_scale_action = now
                    replica_to_remove = reg.replicas[-1].client_id
                    await self._send_to_client(
                        pool, reg.owner_client_id,
                        {
                            "type": "process_scale",
                            "request_id": _next_id(),
                            "client_id": reg.owner_client_id,
                            "pool": pool.pool_id,
                            "payload": {
                                "action": "down",
                                "process_name": reg.process_name,
                                "count": 1,
                                "group_id": reg.group_id,
                                "replica_id": replica_to_remove,
                                "reason": f"low_load ({total_inflight} in-flight)",
                            },
                        },
                    )
                    self._record_event("info", "process_scale_down",
                        pool=pool.pool_id, client_id=reg.owner_client_id,
                        extra={"process_id": pid, "replicas": n_replicas - 1,
                               "total_inflight": total_inflight})

    # ======================================================================
    # Connection handlers — thin readers that enqueue, never dispatch inline
    # ======================================================================

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """TCP connection handler with admission control."""
        # ── Admission control ────────────────────────────────────────────
        if not self._connection_semaphore.locked():
            # Fast path: semaphore has capacity — acquire without blocking
            pass

        acquired = self._connection_semaphore._value > 0  # peek without side-effect
        if not acquired:
            # Server at capacity — reject immediately
            try:
                writer.write(encode_message({
                    "type": "error",
                    "request_id": None,
                    "client_id": None,
                    "pool": None,
                    "payload": {
                        "code": "server_busy",
                        "message": (
                            f"Server at connection limit "
                            f"({self.config.max_connections}). Try again shortly."
                        ),
                    },
                }))
                await writer.drain()
            except Exception:
                pass
            finally:
                writer.close()
            return

        async with self._connection_semaphore:
            self._connection_count += 1
            session = ClientSession(client_id="", writer=writer, pool_id=None)
            try:
                while not reader.at_eof():
                    try:
                        raw = await reader.readline()
                        if not raw:
                            break
                        try:
                            message = decode_message(raw)
                            # Submit to worker pool — non-blocking enqueue
                            await self._worker_pool.submit(session, message)
                        except Exception as exc:
                            await self._send_message(
                                writer,
                                {
                                    "type": "error",
                                    "request_id": None,
                                    "client_id": session.client_id,
                                    "pool": session.pool_id,
                                    "payload": {
                                        "code": "protocol_error",
                                        "message": str(exc),
                                    },
                                },
                            )
                            self._record_event(
                                "error", "protocol_error",
                                pool=session.pool_id,
                                client_id=session.client_id or None,
                                extra={"message": str(exc)},
                            )
                    except (ConnectionResetError, ConnectionAbortedError, OSError):
                        break
            finally:
                self._connection_count -= 1
                await self._disconnect(session)
                writer.close()
                with suppress((ConnectionResetError, ConnectionAbortedError, OSError, Exception)):
                    await writer.wait_closed()

    async def _handle_websocket_connection(self, websocket: WebSocketServerProtocol) -> None:
        """WebSocket connection handler with admission control."""
        acquired = self._connection_semaphore._value > 0
        if not acquired:
            try:
                await websocket.send(json.dumps({
                    "type": "error",
                    "request_id": None,
                    "client_id": None,
                    "pool": None,
                    "payload": {
                        "code": "server_busy",
                        "message": f"Server at connection limit ({self.config.max_connections}).",
                    },
                }))
            except Exception:
                pass
            finally:
                await websocket.close()
            return

        async with self._connection_semaphore:
            self._connection_count += 1
            session = ClientSession(client_id="", writer=websocket, pool_id=None)
            try:
                async for message in websocket:
                    try:
                        message_dict = json.loads(message)
                        await self._worker_pool.submit(session, message_dict)
                    except json.JSONDecodeError as exc:
                        await self._send_error(websocket, None, "protocol_error", str(exc))
                        self._record_event(
                            "error", "protocol_error",
                            pool=session.pool_id,
                            client_id=session.client_id or None,
                            extra={"message": str(exc)},
                        )
                    except (ConnectionResetError, ConnectionAbortedError, OSError):
                        break
            finally:
                self._connection_count -= 1
                await self._disconnect(session)

    # ======================================================================
    # Dispatch (runs inside worker pool tasks, NOT on the reader coroutine)
    # ======================================================================

    async def _dispatch(self, session: ClientSession, message: dict) -> None:
        self._msg_counter += 1
        msg_type = message.get("type")

        # ── O(1) table lookup ────────────────────────────────────────────
        handler = self._dispatch_table.get(msg_type)  # type: ignore[arg-type]
        if handler is None:
            await self._send_message(
                session.writer,
                {
                    "type": "error",
                    "request_id": message.get("request_id"),
                    "client_id": session.client_id,
                    "pool": session.pool_id,
                    "payload": {
                        "code": "unknown_message_type",
                        "message": f"Unsupported message type: {msg_type!r}",
                    },
                },
            )
            return

        try:
            await handler(session, message)
        except Exception as exc:
            await self._send_message(
                session.writer,
                {
                    "type": "error",
                    "request_id": message.get("request_id"),
                    "client_id": session.client_id,
                    "pool": session.pool_id,
                    "payload": {
                        "code": "dispatch_error",
                        "message": str(exc),
                    },
                },
            )
            self._record_event(
                "error", "dispatch_error",
                pool=session.pool_id,
                client_id=session.client_id or None,
                extra={"message": str(exc), "msg_type": msg_type},
            )

    # ======================================================================
    # Message handlers (pool-independent)
    # ======================================================================

    async def _handle_hello(self, session: ClientSession, message: dict) -> None:
        await self._ack(session.writer, message, {"server": "latzero-server"})

    async def _handle_leave_pool(self, session: ClientSession, message: dict) -> None:
        await self._disconnect(session, keep_connection=True)
        await self._ack(session.writer, message, {"left_pool": True})

    def _require_pool(self, session: ClientSession, message: dict) -> PoolState:
        if not session.pool_id:
            raise ValueError("Client is not in a pool")
        pool = self._pools.get(session.pool_id)
        if pool is None:
            raise ValueError("Pool does not exist")
        return pool

    # ── Pool handlers ──────────────────────────────────────────────────

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
            self._store.enqueue(pool)
            self._record_event(
                "info", "pool_created",
                pool=pool_id, client_id=client_id,
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

    # ── Buffer handlers ────────────────────────────────────────────────

    async def _handle_set_buffer(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        payload = message.get("payload") or {}
        key = payload.get("key")
        if not key:
            raise ValueError("key is required")
        now = time.time()
        existing = pool.buffers.get(key)
        version = 1 if existing is None else existing.version + 1
        ttl = payload.get("ttl")
        entry = BufferEntry(
            value=payload.get("value"),
            updated_at=now,
            updated_by=session.client_id,
            persistent=bool(payload.get("persistent", False)),
            ttl=ttl,
            version=version,
        )
        pool.buffers[key] = entry
        # Push onto expiry heap if TTL set (non-blocking)
        if ttl is not None:
            heapq.heappush(self._expiry_heap, (now + ttl, pool.pool_id, key))
        # Non-blocking enqueue instead of synchronous file write
        self._store.enqueue(pool)
        await self._ack(session.writer, message, {"key": key, "version": version})
        await self._notify_buffer_update(pool, key, "set", entry)
        self._record_event(
            "info", "buffer_set",
            pool=pool.pool_id, client_id=session.client_id,
            extra={"key": key, "persistent": entry.persistent, "version": version},
        )

    async def _handle_get_buffer(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
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

    async def _handle_delete_buffer(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        payload = message.get("payload") or {}
        key = payload.get("key")
        if not key:
            raise ValueError("key is required")
        entry = pool.buffers.pop(key, None)
        self._store.enqueue(pool)
        await self._ack(session.writer, message, {"key": key, "deleted": entry is not None})
        if entry is not None:
            await self._notify_buffer_update(pool, key, "delete", entry)
            self._record_event("info", "buffer_deleted", pool=pool.pool_id,
                               client_id=session.client_id, extra={"key": key})

    async def _handle_list_buffers(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        payload = message.get("payload") or {}
        pattern = payload.get("pattern")
        keys = sorted(pool.buffers.keys())
        if pattern:
            keys = [key for key in keys if key.startswith(pattern)]
        await self._ack(session.writer, message, {"keys": keys})

    async def _handle_subscribe_buffer(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        payload = message.get("payload") or {}
        key = payload.get("key")
        if not key:
            raise ValueError("key is required")
        pool.subscriptions.setdefault(key, set()).add(session.client_id)
        await self._ack(session.writer, message, {"key": key, "subscribed": True})
        self._record_event("info", "buffer_subscribed", pool=pool.pool_id,
                           client_id=session.client_id, extra={"key": key})

    async def _handle_unsubscribe_buffer(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        payload = message.get("payload") or {}
        key = payload.get("key")
        if not key:
            raise ValueError("key is required")
        subscribers = pool.subscriptions.get(key, set())
        subscribers.discard(session.client_id)
        if not subscribers and key in pool.subscriptions:
            del pool.subscriptions[key]
        await self._ack(session.writer, message, {"key": key, "subscribed": False})
        self._record_event("info", "buffer_unsubscribed", pool=pool.pool_id,
                           client_id=session.client_id, extra={"key": key})

    # ── App call handlers ──────────────────────────────────────────────

    async def _handle_call_app(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        payload = message.get("payload") or {}
        target_client_id = payload.get("target_client_id")
        event = payload.get("event")
        if not target_client_id or not event:
            raise ValueError("target_client_id and event are required")
        target = pool.clients.get(target_client_id)
        if target is None:
            await self._send_error(session.writer, message, "target_not_found", "Target client is not connected")
            self._record_event(
                "warn", "call_target_missing",
                pool=pool.pool_id, client_id=session.client_id,
                extra={"target_client_id": target_client_id, "event": event},
            )
            return

        # Prefer request_id from client message; fall back to fast internal ID
        request_id = message.get("request_id") or _next_id()
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
            "info", "app_call_routed",
            pool=pool.pool_id, client_id=session.client_id,
            extra={
                "request_id": request_id,
                "target_client_id": target_client_id,
                "response_client_id": response_client_id,
                "event": event,
            },
        )

    async def _handle_app_result(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        request_id = message.get("request_id")
        if not request_id:
            raise ValueError("request_id is required")
        route = pool.in_flight_requests.pop(request_id, None)
        if route is None:
            await self._send_error(session.writer, message, "route_not_found", "Request route no longer exists")
            self._record_event("warn", "route_not_found", pool=pool.pool_id,
                               client_id=session.client_id, extra={"request_id": request_id})
            return

        if route.process_registration_id:
            preg = pool.processes.get(route.process_registration_id)
            if preg:
                for rep in preg.replicas:
                    if rep.client_id == route.target_client_id:
                        rep.in_flight = max(0, rep.in_flight - 1)
                        break

        payload_data = message.get("payload") or {}
        result_msg = {
            "type": "app_result",
            "request_id": request_id,
            "client_id": route.target_client_id,
            "pool": pool.pool_id,
            "payload": {
                "event": route.event,
                "source_client_id": route.origin_client_id,
                "target_client_id": route.target_client_id,
                "response_to": route.response_client_id,
                "value": payload_data.get("value"),
                "error": payload_data.get("error"),
            },
        }
        ack_msg = {
            "type": "ack",
            "request_id": message.get("request_id"),
            "client_id": message.get("client_id"),
            "pool": message.get("pool"),
            "payload": {"delivered": True},
        }
        latency = time.time() - route.created_at
        self._request_latencies.append(latency)

        # Ack the target and route result to caller — two sync writes, zero yields.
        self._write_nowait(session.writer, ack_msg)
        target_session = pool.clients.get(route.response_client_id)
        if target_session is not None:
            self._write_nowait(target_session.writer, result_msg)

    async def _handle_emit_event(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
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
                    "warn", "emit_target_missing",
                    pool=pool.pool_id, client_id=session.client_id,
                    extra={"target_client_id": target_client_id, "event": event},
                )
                return
            await self._send_message(target.writer, envelope)
        else:
            # Fan-out to all other clients in pool — parallel
            recipients = [
                s for cid, s in list(pool.clients.items())
                if cid != session.client_id
            ]
            await self._fanout(recipients, envelope)
        await self._ack(session.writer, message, {"delivered": True})
        self._record_event(
            "info", "event_emitted",
            pool=pool.pool_id, client_id=session.client_id,
            extra={"event": event, "target_client_id": target_client_id},
        )

    # ── Process handlers ───────────────────────────────────────────────

    async def _handle_register_process(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        payload = message.get("payload") or {}
        process_name = payload.get("process_name")
        if not process_name:
            raise ValueError("process_name is required")

        scale = payload.get("scale", False)
        max_replicas = payload.get("max_replicas", 10)
        group_id = payload.get("group_id")

        if group_id:
            existing = None
            for reg in pool.processes.values():
                if reg.group_id == group_id and reg.process_name == process_name:
                    existing = reg
                    break
            if existing:
                existing.replicas.append(ProcessReplica(
                    client_id=session.client_id,
                    created_at=time.time(),
                ))
                process_id = f"{session.client_id}:{process_name}"
                pool.processes[process_id] = existing
                await self._ack(session.writer, message, {
                    "process_id": process_id,
                    "group_id": group_id,
                    "scale": False,
                })
                self._record_event("info", "process_replica_joined",
                    pool=pool.pool_id, client_id=session.client_id,
                    extra={"process_id": process_id, "group_id": group_id})
                return

        process_id = f"{session.client_id}:{process_name}"
        derived_group_id = group_id or f"{session.client_id}:{process_name}:{str(uuid.uuid4())[:8]}"
        reg = ProcessRegistration(
            process_id=process_id,
            process_name=process_name,
            owner_client_id=session.client_id,
            group_id=derived_group_id,
            scale=bool(scale),
            max_replicas=int(max_replicas),
            replicas=[ProcessReplica(
                client_id=session.client_id,
                created_at=time.time(),
            )],
            created_at=time.time(),
        )
        pool.processes[process_id] = reg

        ack_payload: dict = {"process_id": process_id, "group_id": derived_group_id}
        if scale:
            ack_payload["scale"] = True
            ack_payload["max_replicas"] = max_replicas
        await self._ack(session.writer, message, ack_payload)
        self._record_event("info", "process_registered",
            pool=pool.pool_id, client_id=session.client_id,
            extra={"process_id": process_id, "scale": scale, "group_id": derived_group_id})

    async def _handle_unregister_process(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        payload = message.get("payload") or {}
        process_name = payload.get("process_name")
        if not process_name:
            raise ValueError("process_name is required")
        process_id = f"{session.client_id}:{process_name}"
        reg = pool.processes.get(process_id)
        if reg is not None:
            if len(reg.replicas) > 1:
                reg.replicas = [r for r in reg.replicas if r.client_id != session.client_id]
            else:
                pool.processes.pop(process_id, None)
        else:
            pool.processes.pop(process_id, None)
        await self._ack(session.writer, message, {"process_id": process_id, "removed": True})
        self._record_event("info", "process_unregistered",
            pool=pool.pool_id, client_id=session.client_id,
            extra={"process_id": process_id})

    async def _handle_call_process(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        payload = message.get("payload") or {}
        process_id = payload.get("process_id")
        if not process_id:
            raise ValueError("process_id is required")
        reg = pool.processes.get(process_id)
        if reg is None:
            await self._send_error(session.writer, message, "process_not_found",
                                   f"Process '{process_id}' is not registered")
            self._record_event("warn", "process_not_found", pool=pool.pool_id,
                               client_id=session.client_id, extra={"process_id": process_id})
            return

        reps = reg.replicas
        if not reps:
            await self._send_error(session.writer, message, "process_no_replicas",
                                   f"Process '{process_id}' has no connected replicas")
            return

        idx = reg.rr_index % len(reps)
        reg.rr_index += 1
        target_replica = reps[idx]
        target_client_id = target_replica.client_id
        target = pool.clients.get(target_client_id)
        if target is None:
            # Stale replica — remove and retry once
            reg.replicas = [r for r in reps if r.client_id != target_client_id]
            reps = reg.replicas
            if not reps:
                await self._send_error(session.writer, message, "process_no_replicas",
                                       f"Process '{process_id}' has no connected replicas")
                return
            idx = 0
            target_replica = reps[0]
            target_client_id = target_replica.client_id
            target = pool.clients.get(target_client_id)
            if target is None:
                await self._send_error(session.writer, message, "process_owner_offline",
                                       f"No replicas online for process '{process_id}'")
                return

        target_replica.in_flight += 1
        request_id = message.get("request_id") or _next_id()
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
            process_registration_id=reg.process_id if reg.scale else None,
        )
        pool.in_flight_requests[request_id] = route

        # Two synchronous writes — zero event-loop yields, zero Task allocations.
        # drain() is skipped (see _write_nowait); the kernel handles delivery.
        self._write_nowait(target.writer, {
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
        })
        self._write_nowait(session.writer, {
            "type": "ack",
            "request_id": message.get("request_id"),
            "client_id": message.get("client_id"),
            "pool": message.get("pool"),
            "payload": {"process_id": process_id, "queued": True},
        })

    async def _handle_broadcast_process(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        payload = message.get("payload") or {}
        process_name = payload.get("process_name")
        if not process_name:
            raise ValueError("process_name is required")
        suffix = f":{process_name}"
        response_client_id = payload.get("response_to") or session.client_id
        timeout = payload.get("timeout")
        target_map: Dict[str, str] = {}
        for pid, reg in pool.processes.items():
            if pid.endswith(suffix):
                for rep in reg.replicas:
                    cid = rep.client_id
                    if cid not in target_map:
                        target_map[cid] = pid
        targets = []
        send_tasks = []
        for target_client_id, process_id in target_map.items():
            target = pool.clients.get(target_client_id)
            if target is None:
                continue
            req_id = _next_id()
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
            send_tasks.append(self._send_message(
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
            ))
            targets.append(process_id)
        if send_tasks:
            await asyncio.gather(*send_tasks, return_exceptions=True)
        await self._ack(session.writer, message, {"targets": targets})
        self._record_event("info", "process_broadcast", pool=pool.pool_id,
                           client_id=session.client_id,
                           extra={"process_name": process_name, "targets": targets})

    async def _handle_list_processes(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        payload = message.get("payload") or {}
        pattern = payload.get("pattern")
        processes: Dict[str, str] = {}
        for pid, reg in pool.processes.items():
            for rep in reg.replicas:
                rep_pid = f"{rep.client_id}:{reg.process_name}"
                if pattern:
                    prefix = f"{pattern}:"
                    if not rep_pid.startswith(prefix):
                        continue
                if rep_pid not in processes:
                    processes[rep_pid] = rep.client_id
        await self._ack(session.writer, message, {"processes": processes})

    # ======================================================================
    # Disconnect / cleanup
    # ======================================================================

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
        owned_pids = [pid for pid, reg in pool.processes.items() if reg.owner_client_id == session.client_id]
        for pid in owned_pids:
            del pool.processes[pid]
            self._record_event("info", "process_unregistered", pool=pool.pool_id,
                               client_id=session.client_id,
                               extra={"process_id": pid, "reason": "client_disconnected"})
        for pid, reg in list(pool.processes.items()):
            before = len(reg.replicas)
            reg.replicas = [r for r in reg.replicas if r.client_id != session.client_id]
            if reg.replicas and before != len(reg.replicas):
                self._record_event("info", "process_replica_removed", pool=pool.pool_id,
                                   client_id=session.client_id,
                                   extra={"process_id": pid, "reason": "client_disconnected"})
            if not reg.replicas:
                del pool.processes[pid]
        await self._broadcast_presence(pool, session.client_id, "left")
        self._record_event("info", "client_left", pool=pool.pool_id, client_id=session.client_id)
        session.pool_id = None
        if not keep_connection:
            session.client_id = ""

    # ======================================================================
    # Fan-out helpers — parallel, encode-once
    # ======================================================================

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
        recipients = list(pool.clients.values())
        await self._fanout(recipients, message)

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
        recipient_sessions = [
            pool.clients[cid]
            for cid in list(subscribers)
            if cid in pool.clients
        ]
        await self._fanout(recipient_sessions, message)

    async def _fanout(self, sessions: List[ClientSession], message: dict) -> None:
        """Send one message to many sessions in parallel.

        TCP sessions share a single pre-encoded bytes object.
        WebSocket sessions each get their own json.dumps call (WS API takes str).
        Failures in individual sends are silently suppressed.
        """
        if not sessions:
            return

        # Encode the JSON payload once for all TCP recipients
        encoded: Optional[bytes] = None
        json_str: Optional[str] = None

        tasks = []
        for session in sessions:
            writer = session.writer
            if hasattr(writer, "write"):
                # TCP — share encoded bytes
                if encoded is None:
                    encoded = encode_message(message)
                tasks.append(self._send_raw_tcp(writer, encoded))
            else:
                # WebSocket — share json string
                if json_str is None:
                    json_str = json.dumps(message)
                tasks.append(self._send_raw_ws(writer, json_str))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_raw_tcp(self, writer: asyncio.StreamWriter, encoded: bytes) -> None:
        """Write pre-encoded bytes to a TCP writer with slow-consumer protection."""
        try:
            transport = writer.transport
            # get_write_buffer_size() may not be available on all transport types
            try:
                buf_size = transport.get_write_buffer_size()
            except AttributeError:
                buf_size = 0

            if buf_size > self.config.connection_critical_bytes:
                # Critical threshold: schedule a graceful close and drop this write.
                # We deliberately avoid transport.abort() here — on Windows
                # ProactorEventLoop, abort() triggers WinError 10054 in the
                # proactor's _call_connection_lost callback.
                self._record_event("warn", "slow_consumer_disconnected",
                                   extra={"buf_size": buf_size})
                asyncio.create_task(self._close_writer(writer))
                return

            if buf_size > self.config.connection_hwm_bytes:
                # High-water mark: skip this message to avoid blocking
                self._record_event("warn", "slow_consumer_skipped",
                                   extra={"buf_size": buf_size})
                return

            writer.write(encoded)
            # Coalescing: only drain when buffer is near the kernel send-buffer
            # limit (64 KB). For localhost, the kernel drains naturally.
            try:
                if transport.get_write_buffer_size() > 65536:
                    await writer.drain()
            except AttributeError:
                pass  # transport doesn't expose buffer size — skip drain, kernel handles it
        except (ConnectionResetError, ConnectionAbortedError, OSError, Exception):
            pass

    @staticmethod
    async def _close_writer(writer: asyncio.StreamWriter) -> None:
        """Gracefully close a StreamWriter without triggering WinError 10054."""
        with suppress(Exception):
            writer.close()
        with suppress(Exception):
            await writer.wait_closed()

    async def _send_raw_ws(self, websocket: Any, json_str: str) -> None:
        """Send a pre-serialized JSON string to a WebSocket connection."""
        try:
            await websocket.send(json_str)
        except Exception:
            pass

    # ======================================================================
    # Low-level send helpers
    # ======================================================================

    def _write_nowait(self, writer: Any, message: Dict[str, Any]) -> None:
        """Synchronous fire-and-forget write — never yields to the event loop.

        For TCP, calls writer.write() (which buffers in the kernel) without
        ever awaiting drain().  The OS delivers the data asynchronously.
        This eliminates all context switches from the message-routing hot path.

        Slow consumers are detected via the write-buffer HWM and handled the
        same way as in _send_message (skip or graceful close).

        WebSocket connections still need an async send; those are scheduled as
        a fire-and-forget Task so this method stays synchronous.
        """
        try:
            if hasattr(writer, "write"):
                transport = writer.transport
                try:
                    buf_size = transport.get_write_buffer_size()
                except AttributeError:
                    buf_size = 0

                if buf_size > self.config.connection_critical_bytes:
                    asyncio.create_task(self._close_writer(writer))
                    return
                if buf_size > self.config.connection_hwm_bytes:
                    return

                writer.write(encode_message(message))
                # No drain() — the kernel buffers and delivers when ready.
                # asyncio's transport will flush on the next I/O poll iteration.
            else:
                # WebSocket: schedule async send as a background fire-and-forget task
                asyncio.create_task(writer.send(json.dumps(message)))
        except Exception:
            pass

    async def _send_to_client(self, pool: PoolState, client_id: str, message: dict) -> None:
        target = pool.clients.get(client_id)
        if target is None:
            return
        with suppress(Exception):
            await self._send_message(target.writer, message)

    async def _ack(self, writer: Any, request: dict, payload: dict) -> None:
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

    async def _send_error(self, writer: Any, request: Any, code: str, message: str) -> None:
        req = request or {}
        await self._send_message(
            writer,
            {
                "type": "error",
                "request_id": req.get("request_id") if isinstance(req, dict) else None,
                "client_id": req.get("client_id") if isinstance(req, dict) else None,
                "pool": req.get("pool") if isinstance(req, dict) else None,
                "payload": {
                    "code": code,
                    "message": message,
                },
            },
        )

    async def _send_message(self, writer: Any, message: Dict[str, Any]) -> bool:
        """Send message to either TCP or WebSocket connection.  Returns True on success."""
        try:
            if hasattr(writer, "write"):
                # TCP with slow-consumer write-buffer check
                transport = writer.transport
                try:
                    buf_size = transport.get_write_buffer_size()
                except AttributeError:
                    buf_size = 0

                if buf_size > self.config.connection_critical_bytes:
                    # Schedule graceful close (not abort — avoids WinError 10054)
                    self._record_event("warn", "slow_consumer_disconnected",
                                       extra={"buf_size": buf_size})
                    asyncio.create_task(self._close_writer(writer))
                    return False

                if buf_size > self.config.connection_hwm_bytes:
                    self._record_event("warn", "slow_consumer_skipped",
                                       extra={"buf_size": buf_size})
                    return False

                encoded = encode_message(message)
                writer.write(encoded)
                # Write coalescing: only drain near the kernel send-buffer limit (64 KB)
                try:
                    if transport.get_write_buffer_size() > 65536:
                        await writer.drain()
                except AttributeError:
                    pass  # skip drain — kernel handles flushing
                return True
            else:
                # WebSocket
                await writer.send(json.dumps(message))
                return True
        except (ConnectionResetError, ConnectionAbortedError, OSError, Exception):
            return False

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

    # ======================================================================
    # Dashboard snapshot (cached for up to 1 s to reduce CPU under load)
    # ======================================================================

    def get_dashboard_snapshot(self) -> dict:
        """Return a serializable snapshot for the server TUI (cached 1 s)."""
        now = time.time()
        if (
            self._snapshot_cache is not None
            and (now - self._snapshot_cache_at) < self._snapshot_cache_ttl
        ):
            return self._snapshot_cache

        # Recompute TPS
        dt = now - self._last_tick
        if dt >= 1.0:
            self._current_tps = self._msg_counter / dt
            self._msg_counter = 0
            self._last_tick = now

        avg_latency = (
            sum(self._request_latencies) / len(self._request_latencies)
            if self._request_latencies
            else 0.0
        )
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
            total_subscriptions += sum(len(s) for s in pool.subscriptions.values())
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
                    "processes": {
                        pid: {
                            "owner": reg.owner_client_id,
                            "name": reg.process_name,
                            "scale": reg.scale,
                            "replicas": [
                                {"client_id": r.client_id, "in_flight": r.in_flight}
                                for r in reg.replicas
                            ],
                        }
                        for pid, reg in pool.processes.items()
                    },
                }
            )

        # Pull live stats from the worker pool
        wp_stats = self._worker_pool.stats

        snapshot = {
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
            "connection_count": self._connection_count,
            "max_connections": self.config.max_connections,
            "tps": self._current_tps,
            "avg_latency": avg_latency,
            "memory_rss": memory_rss,
            # Worker pool live stats
            "worker_count": wp_stats.active_workers,
            "worker_min": wp_stats.min_workers,
            "worker_max": wp_stats.max_workers,
            "queue_depth": wp_stats.queue_depth,
            "worker_tps": wp_stats.messages_per_sec,
            "predicted_queue_depth": wp_stats.predicted_depth,
            "prediction_confidence": wp_stats.prediction_confidence,
            "scale_events": [
                {
                    "timestamp": ev.timestamp,
                    "direction": ev.direction,
                    "reason": ev.reason,
                    "old_count": ev.old_count,
                    "new_count": ev.new_count,
                }
                for ev in wp_stats.scale_events
            ],
            "pools": pools,
            "events": list(self._event_log),
        }

        self._snapshot_cache = snapshot
        self._snapshot_cache_at = now
        return snapshot
