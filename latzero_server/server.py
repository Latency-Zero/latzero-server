"""Bounded local TCP/WS broker with ordered dispatch and eventual snapshots."""

import asyncio
import hashlib
import heapq
import itertools
import json
import logging
import math
import time
import uuid
from collections import deque
from contextlib import suppress
from dataclasses import dataclass
from functools import partial
from typing import Any, Callable, Coroutine, Deque, Dict, List, Optional, Tuple

from websockets.exceptions import ConnectionClosed
from websockets.legacy.server import WebSocketServer, WebSocketServerProtocol, serve
from websockets.legacy.protocol import WebSocketCommonProtocol

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
from .protocol import decode_message, encode_message, validate_message
from .worker_pool import AutoScalingWorkerPool

try:
    import psutil
except ImportError:
    psutil = None


# ---------------------------------------------------------------------------
# Internal fast ID generator (NOT for client-visible request IDs)
# ---------------------------------------------------------------------------
_ID_COUNTER = itertools.count(1)
_LOGGER = logging.getLogger(__name__)


def _next_id() -> str:
    """Return a fast, unique server-internal routing ID."""
    return f"rq-{next(_ID_COUNTER)}"


@dataclass
class _OutboundFrame:
    encoded: bytes
    websocket_text: Optional[str] = None
    generation: Optional[int] = None
    route: Optional[RouteEntry] = None
    ready: bool = True
    released: bool = False


class _LimitedWebSocketProtocol(WebSocketServerProtocol):
    """Reserve the shared connection slot before spawning handshake work."""

    def __init__(self, daemon: "LatZeroServer", *args: Any, **options: Any):
        self.daemon = daemon
        super().__init__(*args, **options)

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        daemon = self.daemon
        if not daemon._accepting or daemon._connection_count + len(daemon._pending_websockets) >= daemon.config.max_connections:
            # Initialize transport/EOF handling but do not spawn handshake work
            # for a connection we cannot admit.
            WebSocketCommonProtocol.connection_made(self, transport)
            transport.write(b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
            transport.close()
            daemon._metrics["overload_rejections"] += 1
            return
        daemon._pending_websockets[id(self)] = self
        super().connection_made(transport)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.daemon._pending_websockets.pop(id(self), None)
        super().connection_lost(exc)


class LatZeroServer:
    """Local TCP and WebSocket server for LatZero server mode."""

    def __init__(self, config: Optional[ServerConfig] = None, *, pool_routing: Any = None):
        self.config = config or ServerConfig()
        self.config.validate()
        self._pool_routing = pool_routing
        self._directory_lock = None
        self._directory_owner_token = None
        self._storage_started = False
        self._initial_storage_loaded = pool_routing is not None
        self._storage_stop_attempted = False
        self._pools: Dict[str, PoolState] = {}
        self._tcp_server: Optional[asyncio.base_events.Server] = None
        self._websocket_server: Optional[WebSocketServer] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._store = SnapshotStore(
            self.config.data_dir,
            batch_window=self.config.persistence_batch_window,
            max_dirty_pools=self.config.max_pools,
        )
        self._event_log: Deque[dict] = deque(maxlen=300)
        self._started_at = time.time()
        self._connection_count: int = 0
        self._sessions: Dict[int, ClientSession] = {}
        self._pending_websockets: Dict[int, _LimitedWebSocketProtocol] = {}
        self._by_writer: Dict[int, ClientSession] = {}
        self._background: set = set()
        self._accepting = False
        self._stopping = False
        self._stop_task: Optional[asyncio.Task] = None
        self._lifecycle_lock = None
        self._outbox_bytes = 0
        self._route_count = 0
        self._fanout_queue: Deque[Any] = deque()
        self._fanout_bytes = 0
        self._fanout_ready: Optional[asyncio.Event] = None
        self._fanout_task: Optional[asyncio.Task] = None
        self._health_error: Optional[str] = None
        self._metrics = {
            "received": 0, "dispatched": 0, "accepted_calls": 0,
            "completed_calls": 0, "failed_calls": 0, "timeouts": 0,
            "overload_rejections": 0, "slow_disconnects": 0,
        }

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
            max_session_messages=self.config.max_session_messages,
            max_session_bytes=self.config.max_session_bytes,
            max_queue_messages=self.config.max_queue_messages,
            max_queue_bytes=self.config.max_queue_bytes,
            control_reserve_messages=self.config.control_reserve_messages,
            control_reserve_bytes=self.config.control_reserve_bytes,
            dispatch_slice=self.config.dispatch_slice,
            dispatch_slice_seconds=self.config.dispatch_slice_seconds,
            shutdown_timeout=self.config.shutdown_timeout,
            max_sessions=self.config.max_connections,
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
            "worker_metrics":     self._handle_worker_metrics,
            "list_clients":       self._handle_list_clients,
        }

        # ── Metrics ──────────────────────────────────────────────────────
        self._msg_counter = 0
        self._last_tick = time.monotonic()
        self._current_tps = 0.0
        self._request_latencies: Deque[float] = deque(maxlen=200)

        # ── Expiry min-heap ──────────────────────────────────────────────
        # Runtime deadlines are monotonic; persisted metadata remains wall-clock.
        self._expiry_heap: List[Tuple[float, str, str, int]] = []
        self._load_snapshots()

        # ── Snapshot cache ───────────────────────────────────────────────
        self._snapshot_cache: Optional[dict] = None
        self._snapshot_cache_at: float = 0.0
        self._snapshot_cache_ttl: float = 1.0  # seconds

    # ======================================================================
    # Startup / Shutdown
    # ======================================================================

    def _load_snapshots(self) -> None:
        wall_now, monotonic_now = time.time(), time.monotonic()
        for pool_id, snapshot in self._store.load_pools().items():
            if self._pool_routing is not None and not self._pool_routing.owns(pool_id):
                continue
            if len(self._pools) >= self.config.max_pools:
                raise ValueError("Restored snapshots exceed max_pools")
            pool = PoolState(
                pool_id=pool_id,
                auth_required=snapshot.get("auth_required", False),
                auth_token_hash=snapshot.get("auth_token_hash"),
            )
            removed_expired = False
            for key, payload in snapshot.get("buffers", {}).items():
                entry = BufferEntry.from_dict(payload)
                if entry.ttl is not None:
                    remaining = entry.updated_at + entry.ttl - wall_now
                    if remaining <= 0:
                        removed_expired = True
                        continue
                    entry.expires_at = monotonic_now + remaining
                    heapq.heappush(self._expiry_heap, (entry.expires_at, pool_id, key, entry.version))
                entry.size_bytes = self._buffer_size(key, entry)
                pool.buffer_bytes += entry.size_bytes
                pool.buffers[key] = entry
            if len(pool.buffers) > self.config.max_buffers_per_pool or pool.buffer_bytes > self.config.max_pool_bytes:
                raise ValueError(f"Restored pool '{pool_id}' exceeds configured state limits")
            self._pools[pool_id] = pool
            if removed_expired:
                self._store.enqueue(pool)

    async def start(self) -> None:
        if self._lifecycle_lock is None:
            self._lifecycle_lock = asyncio.Lock()
        async with self._lifecycle_lock:
            await self._start()

    async def _start(self) -> None:
        """Start TCP, WebSocket servers, worker pool, saver, and cleanup task."""
        if self._tcp_server is not None or self._websocket_server is not None:
            return

        self.config.validate()
        if self._storage_started and (self._store.health["running"] or self._store.health["in_flight"]):
            raise RuntimeError("Previous snapshot writer has not stopped; refusing unsafe restart")
        self._stopping = False
        self._stop_task = None
        ws_port = self.config.websocket_port
        if ws_port is None:
            ws_port = self.config.port + 1 if self.config.port else 0
        try:
            if self._pool_routing is None:
                from .directory_lock import DataDirectoryLock

                newly_acquired = self._directory_lock is None
                if newly_acquired:
                    self._directory_lock = DataDirectoryLock(self.config.data_dir).acquire()
                # Constructor inspection is read-only state. Re-capture under
                # ownership before opening listeners so a preconstructed daemon
                # cannot start from snapshots changed by a previous owner.
                ownership_changed = (newly_acquired and self._directory_owner_token is not None
                                     and self._directory_lock.previous_owner_token != self._directory_owner_token)
                self._directory_owner_token = self._directory_lock.owner_token
                if not self._initial_storage_loaded or ownership_changed:
                    self._pools.clear()
                    self._expiry_heap.clear()
                    self._store = SnapshotStore(self.config.data_dir,
                        batch_window=self.config.persistence_batch_window, max_dirty_pools=self.config.max_pools)
                    self._load_snapshots()
                    self._initial_storage_loaded = True
                elif self._store.health["running"] or self._store.health["in_flight"]:
                    raise RuntimeError("Previous snapshot writer has not stopped; refusing unsafe restart")
            self._store.start()
            self._storage_started = True
            self._storage_stop_attempted = False
            await self._worker_pool.start()
            self._fanout_ready = asyncio.Event()
            self._fanout_task = self._track(self._fanout_loop(), "latzero-fanout")
            self._accepting = self._pool_routing is None or bool(getattr(self._pool_routing, "configured", False))
            self._tcp_server = await asyncio.start_server(
                self._handle_connection, host=self.config.host, port=self.config.port,
                limit=self.config.max_frame_bytes + 1,
            )
            if self.config.websocket_enabled:
                self._websocket_server = await serve(
                    self._handle_websocket_connection, host=self.config.host, port=ws_port,
                    origins=self.config.websocket_origins,
                    compression=self.config.websocket_compression,
                    max_size=self.config.max_frame_bytes, max_queue=self.config.websocket_max_queue,
                    open_timeout=self.config.join_timeout, close_timeout=self.config.write_timeout,
                    write_limit=self.config.connection_hwm_bytes,
                    create_protocol=partial(_LimitedWebSocketProtocol, self),
                )
            self._cleanup_task = self._track(self._cleanup_loop(), "latzero-cleanup")
        except BaseException:
            await self._stop()
            raise
        self._record_event("info", "server_started", extra={
            "host": self.config.host,
            "tcp_port": self.config.port,
            "websocket_port": ws_port,
            "min_workers": self.config.min_workers,
            "max_workers": self.config.max_workers,
            "max_connections": self.config.max_connections,
        })

    def _track(self, coroutine: Coroutine, name: str) -> asyncio.Task:
        task = asyncio.create_task(coroutine, name=name)
        self._background.add(task)

        def completed(done: asyncio.Task) -> None:
            self._background.discard(done)
            if not done.cancelled() and done.exception() is not None:
                self._health_error = str(done.exception())
                _LOGGER.error("Background task %s failed", name, exc_info=(
                    type(done.exception()), done.exception(), done.exception().__traceback__,
                ))
                self._record_event("error", "background_task_failed", extra={"task": name, "error": self._health_error})

        task.add_done_callback(completed)
        return task

    async def serve_forever(self) -> None:
        """Run the server until cancelled."""
        await self.start()
        assert self._tcp_server is not None
        async with self._tcp_server:
            await self._tcp_server.serve_forever()

    async def stop(self) -> None:
        """Seal ingress, drain accepted work to a deadline, close, then flush."""
        if self._stop_task is not None and self._stop_task.done():
            if self._stop_task.cancelled() or self._stop_task.exception() is not None:
                self._stop_task = None
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self._stop_locked(), name="latzero-stop")
        await asyncio.shield(self._stop_task)

    async def _stop_locked(self) -> None:
        if self._lifecycle_lock is None:
            self._lifecycle_lock = asyncio.Lock()
        async with self._lifecycle_lock:
            await self._stop()

    async def _stop(self) -> None:
        self._stopping = True
        self._accepting = False
        self._record_event("info", "server_stopping")
        errors = []
        tcp_server = self._tcp_server
        if tcp_server is not None:
            try:
                tcp_server.close()
            except Exception as exc:
                errors.append(exc)
        if self._websocket_server is not None:
            # Stop accepting before closing established peers.
            try:
                self._websocket_server.server.close()
            except Exception as exc:
                errors.append(exc)
        for protocol in list(self._pending_websockets.values()):
            protocol.transport.close()
        deadline = time.monotonic() + self.config.shutdown_timeout
        try:
            await asyncio.wait_for(self._worker_pool.join(), max(0.001, deadline - time.monotonic()))
            while self._route_count and time.monotonic() < deadline:
                await asyncio.sleep(0.01)
        except asyncio.TimeoutError:
            self._record_event("warn", "shutdown_deadline")
        except Exception as exc:
            errors.append(exc)
        for pool in list(self._pools.values()):
            for route in list(pool.in_flight_requests.values()):
                try:
                    self._finalize_route(pool, route, code="server_stopping", error="Server is stopping")
                except Exception as exc:
                    errors.append(exc)
        readers = [s.reader_task for s in list(self._sessions.values()) if s.reader_task]
        for reader in readers:
            reader.cancel()
        if readers:
            await asyncio.gather(*readers, return_exceptions=True)
        try:
            await self._worker_pool.stop()
        except Exception as exc:
            errors.append(exc)
        sessions = list(self._sessions.values())
        if sessions:
            outcomes = await asyncio.gather(*(self._close_session(session) for session in sessions), return_exceptions=True)
            errors.extend(outcome for outcome in outcomes if isinstance(outcome, Exception))
        if tcp_server is not None:
            try:
                await asyncio.wait_for(tcp_server.wait_closed(), self.config.write_timeout)
            except Exception as exc:
                errors.append(exc)
            finally:
                self._tcp_server = None
        if self._websocket_server is not None:
            try:
                self._websocket_server.close()
                await asyncio.wait_for(self._websocket_server.wait_closed(), self.config.write_timeout)
            except Exception as exc:
                errors.append(exc)
            finally:
                self._websocket_server = None
        for task in list(self._background):
            task.cancel()
        if self._background:
            await asyncio.gather(*list(self._background), return_exceptions=True)
        self._cleanup_task = None
        self._fanout_task = None
        self._fanout_queue.clear()
        self._fanout_bytes = 0
        try:
            if self._storage_started:
                if (self._storage_stop_attempted
                        and not self._store.health["running"] and not self._store.health["in_flight"]):
                    self._store.start()
                self._storage_stop_attempted = True
                await self._store.stop()
        except Exception as exc:
            errors.append(exc)
        if (self._directory_lock is not None and not self._store.health["running"]
                and not self._store.health["in_flight"]
                and (not self._storage_started or not self._store.health["dirty_pools"])):
            self._directory_lock.release()
            self._directory_lock = None
        if errors:
            self._health_error = f"Shutdown failed: {errors[0]}"
            self._record_event("error", "shutdown_failed", extra={"errors": [str(exc) for exc in errors]})
            raise errors[0]

    # ======================================================================
    # Cleanup loop
    # ======================================================================

    async def _cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.cleanup_interval)
            try:
                await self._expire_buffers()
                await self._expire_routes()
                await self._check_process_scaling()
                now = time.monotonic()
                dt = now - self._last_tick
                if dt > 0:
                    self._current_tps = self._msg_counter / dt
                    self._msg_counter = 0
                    self._last_tick = now
            except Exception as exc:
                self._health_error = str(exc)
                _LOGGER.exception("Cleanup failed")
                self._record_event("error", "cleanup_failed", extra={"error": str(exc)})

    async def _expire_buffers(self) -> None:
        """O(log N) expiry using a min-heap instead of scanning all buffers."""
        now = time.monotonic()
        processed = 0
        while self._expiry_heap and self._expiry_heap[0][0] <= now:
            expires_at, pool_id, key, version = heapq.heappop(self._expiry_heap)
            pool = self._pools.get(pool_id)
            if pool is None:
                continue
            entry = pool.buffers.get(key)
            if entry and entry.version == version and entry.expires_at == expires_at:
                await self._expire_entry(pool, key)
            processed += 1
            if processed % self.config.cleanup_slice == 0:
                await asyncio.sleep(0)
                now = time.monotonic()
        self._compact_expiry_heap()

    async def _expire_entry(self, pool: PoolState, key: str) -> Optional[BufferEntry]:
        entry = pool.buffers.get(key)
        if entry is not None and entry.expires_at is not None and entry.expires_at <= time.monotonic():
            pool.buffers.pop(key)
            pool.buffer_bytes -= entry.size_bytes
            if entry.persistent:
                self._store.enqueue(pool)
            await self._notify_buffer_update(pool, key, "expired", entry)
            self._record_event("warn", "buffer_expired", pool=pool.pool_id, extra={"key": key})
            return None
        return entry

    def _compact_expiry_heap(self) -> None:
        buffer_count = sum(len(pool.buffers) for pool in self._pools.values())
        if len(self._expiry_heap) <= max(64, buffer_count * 2):
            return
        self._expiry_heap = [
            (entry.expires_at, pool.pool_id, key, entry.version)
            for pool in self._pools.values() for key, entry in pool.buffers.items()
            if entry.expires_at is not None
        ]
        heapq.heapify(self._expiry_heap)

    async def _expire_routes(self) -> None:
        now = time.monotonic()
        processed = 0
        for pool in list(self._pools.values()):
            for route in list(pool.in_flight_requests.values()):
                if route.expires_at is not None and route.expires_at <= now:
                    self._finalize_route(pool, route, code="timeout", error="Request timed out; accepted effects may have occurred")
                processed += 1
                if processed % self.config.cleanup_slice == 0:
                    await asyncio.sleep(0)
                    now = time.monotonic()

    async def _check_process_scaling(self) -> None:
        """
        Periodically evaluate scalable processes and send up/down commands.
        Called from _cleanup_loop.

        Only fresh, validated clients receive local scaling commands.
        """
        now = time.time()
        cfg = self.config
        for pool in list(self._pools.values()):
            for pid, reg in list(pool.processes.items()):
                if not reg.scale:
                    continue
                if not reg.last_metrics_at or now - reg.last_metrics_at > cfg.process_metrics_timeout:
                    continue
                if now - reg.last_scale_action < cfg.process_scale_cooldown:
                    continue

                total_inflight = sum(r.in_flight for r in reg.replicas)
                effective_load = max(total_inflight, reg.reported_queue_depth)
                n_replicas = reg.worker_count

                if effective_load > cfg.process_scale_up_threshold and n_replicas < reg.max_workers:
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
                                "worker_kind": reg.worker_kind,
                                "min_workers": reg.min_workers,
                                "max_workers": reg.max_workers,
                                "reason": (
                                    f"high_load (inflight={total_inflight}, "
                                    f"queue={reg.reported_queue_depth})"
                                ),
                            },
                        },
                    )
                    self._record_event("info", "process_scale_up",
                        pool=pool.pool_id, client_id=reg.owner_client_id,
                        extra={"process_id": pid, "workers": n_replicas + 1,
                               "total_inflight": total_inflight,
                               "queue_depth": reg.reported_queue_depth})

                elif effective_load <= cfg.process_scale_down_threshold and n_replicas > reg.min_workers:
                    reg.last_scale_action = now
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
                                "reason": (
                                    f"low_load (inflight={total_inflight}, "
                                    f"queue={reg.reported_queue_depth})"
                                ),
                            },
                        },
                    )
                    self._record_event("info", "process_scale_down",
                        pool=pool.pool_id, client_id=reg.owner_client_id,
                        extra={"process_id": pid, "workers": n_replicas - 1,
                               "total_inflight": total_inflight,
                               "queue_depth": reg.reported_queue_depth})

    # ======================================================================
    # Connection handlers — thin readers that enqueue, never dispatch inline
    # ======================================================================

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Thin bounded reader; no private semaphore state or unbounded waits."""
        if not self._accepting or self._connection_count + len(self._pending_websockets) >= self.config.max_connections:
            await self._reject_connection(writer)
            return
        session = self._new_session(writer)
        received = 0
        try:
            while not reader.at_eof() and not session.closing:
                try:
                    raw = await reader.readline()
                except ValueError:
                    await self._send_error(writer, None, "frame_too_large", "Frame exceeds configured maximum")
                    break
                if not raw:
                    break
                if len(raw.rstrip(b"\r\n")) > self.config.max_frame_bytes:
                    await self._send_error(writer, None, "frame_too_large", "Frame exceeds configured maximum")
                    break
                await self._ingest(session, raw, len(raw))
                received += 1
                if received % self.config.dispatch_slice == 0:
                    await asyncio.sleep(0)
        except (ConnectionError, OSError):
            _LOGGER.debug("TCP connection closed", exc_info=True)
        finally:
            await self._close_session(session)

    async def _handle_websocket_connection(self, websocket: WebSocketServerProtocol) -> None:
        self._pending_websockets.pop(id(websocket), None)
        if not self._accepting or self._connection_count >= self.config.max_connections:
            await self._reject_connection(websocket)
            return
        session = self._new_session(websocket)
        received = 0
        try:
            async for raw in websocket:
                if session.closing:
                    break
                if not isinstance(raw, str):
                    await self._send_error(websocket, None, "protocol_error", "WebSocket frames must be text JSON")
                    continue
                size = len(raw.encode("utf-8"))
                if size > self.config.max_frame_bytes:
                    await self._send_error(websocket, None, "frame_too_large", "Frame exceeds configured maximum")
                    break
                await self._ingest(session, raw, size)
                received += 1
                if received % self.config.dispatch_slice == 0:
                    await asyncio.sleep(0)
        except (ConnectionClosed, ConnectionError, OSError):
            _LOGGER.debug("WebSocket connection closed", exc_info=True)
        finally:
            await self._close_session(session)

    def _new_session(self, writer: Any) -> ClientSession:
        session = ClientSession(client_id="", writer=writer)
        session.outbox_ready = asyncio.Event()
        session.outbox_drained = asyncio.Event()
        session.outbox_drained.set()
        session.joined = asyncio.Event()
        session.reader_task = asyncio.current_task()
        self._sessions[id(session)] = session
        self._by_writer[id(writer)] = session
        self._connection_count += 1
        if hasattr(writer, "write"):
            writer.transport.set_write_buffer_limits(high=self.config.connection_hwm_bytes)
        session.writer_task = self._track(self._writer_loop(session), "latzero-writer")
        self._track(self._join_deadline(session), "latzero-join-deadline")
        return session

    async def _join_deadline(self, session: ClientSession) -> None:
        try:
            await asyncio.wait_for(session.joined.wait(), self.config.join_timeout)
        except asyncio.TimeoutError:
            if not session.closed:
                await self._send_error(session.writer, None, "join_timeout", "Join deadline exceeded")
                self._fail_session(session, "join_timeout")

    async def _reject_connection(self, writer: Any) -> None:
        encoded = encode_message({
            "type": "error", "request_id": None, "client_id": None, "pool": None,
            "payload": {"code": "server_busy", "message": "Server is stopping or at its connection limit"},
        })
        if hasattr(writer, "write"):
            try:
                writer.write(encoded)
                await asyncio.wait_for(writer.drain(), self.config.write_timeout)
            except (ConnectionError, OSError, asyncio.TimeoutError):
                pass
            finally:
                writer.close()
        else:
            with suppress(Exception):
                await asyncio.wait_for(writer.send(encoded[:-1].decode()), self.config.write_timeout)
            with suppress(Exception):
                await asyncio.wait_for(writer.close(), self.config.write_timeout)

    async def _ingest(self, session: ClientSession, raw: Any, size: int) -> None:
        try:
            message = decode_message(raw)
        except (ValueError, TypeError, RecursionError) as exc:
            await self._send_error(session.writer, None, "protocol_error", str(exc))
            return
        self._metrics["received"] += 1
        if (not self._accepting and message["type"] != "app_result") or not await self._worker_pool.submit(session, message, size):
            self._metrics["overload_rejections"] += 1
            await self._send_error(session.writer, message, "overloaded", "Dispatch capacity exceeded or server stopping")

    # ======================================================================
    # Dispatch (runs inside worker pool tasks, NOT on the reader coroutine)
    # ======================================================================

    async def _dispatch(self, session: ClientSession, message: dict) -> None:
        if session.closed or session.closing:
            return
        self._msg_counter += 1
        self._metrics["dispatched"] += 1
        try:
            validate_message(message)
        except (ValueError, TypeError) as exc:
            await self._send_error(session.writer, message, "protocol_error", str(exc))
            return
        msg_type = message["type"]
        if msg_type not in {"hello", "join_pool", "switch_pool"} and message.get("pool") not in (None, session.pool_id):
            await self._send_error(session.writer, message, "pool_mismatch", "Envelope pool does not match session membership")
            return

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
        except asyncio.QueueFull:
            self._metrics["overload_rejections"] += 1
            await self._send_error(session.writer, message, "overloaded", "State or persistence capacity exceeded")
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
        capabilities = (message.get("payload") or {}).get("capabilities", [])
        if not isinstance(capabilities, list) or any(not isinstance(item, str) for item in capabilities):
            raise ValueError("capabilities must be an array of strings")
        session.redirect_supported = "pool_redirect_v1" in capabilities
        await self._ack(session.writer, message, {
            "server": "latzero-server",
            **({"capabilities": ["pool_redirect_v1"]} if self._pool_routing is not None else {}),
        })

    async def _handle_leave_pool(self, session: ClientSession, message: dict) -> None:
        await self._disconnect(session, keep_connection=True)
        await self._ack(session.writer, message, {"left_pool": True})

    def _require_pool(self, session: ClientSession, message: dict) -> PoolState:
        if session.closed or session.closing or not session.pool_id:
            raise ValueError("Client is not in a pool")
        pool = self._pools.get(session.pool_id)
        if pool is None:
            raise ValueError("Pool does not exist")
        if pool.clients.get(session.client_id) is not session:
            raise ValueError("Session no longer owns its pool membership")
        return pool

    # ── Pool handlers ──────────────────────────────────────────────────

    async def _handle_join_pool(self, session: ClientSession, message: dict) -> None:
        payload = message.get("payload") or {}
        client_id = payload.get("client_id") or message.get("client_id")
        pool_id = payload.get("pool") or message.get("pool")
        auth_token = payload.get("auth_token")

        self._identifier(client_id, "client_id")
        self._identifier(pool_id, "pool")
        if auth_token is not None and not isinstance(auth_token, str):
            raise ValueError("auth_token must be a string or null")
        if session.client_id and session.client_id != client_id:
            await self._send_error(session.writer, message, "identity_change", "Client identity is stable for the connection")
            return

        if self._pool_routing is not None and not self._pool_routing.owns(pool_id):
            payload = self._pool_routing.redirect_payload(pool_id)
            self._write_nowait(session.writer, {
                "type": "redirect" if session.redirect_supported else "error",
                "request_id": message.get("request_id"), "client_id": client_id, "pool": pool_id,
                "payload": payload if session.redirect_supported else {
                    "code": "redirect_required", "message": "This pool is owned by another pod; use a redirect-capable SDK",
                    **payload,
                },
            })
            session.closing = True
            self._worker_pool.invalidate(session)
            if session.close_task is None:
                session.close_task = self._track(self._finish_close(session), "latzero-redirect-close")
            return

        pool = self._pools.get(pool_id)
        if pool is None:
            if len(self._pools) >= self.config.max_pools:
                raise asyncio.QueueFull()
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
        if existing is not None and existing is not session:
            await self._send_error(session.writer, message, "duplicate_client", "Client ID is already connected")
            self._record_event("warn", "duplicate_client", pool=pool_id, client_id=client_id)
            return

        if existing is session and session.pool_id == pool_id:
            await self._ack(session.writer, message, {
                "pool": pool_id, "auth_required": pool.auth_required, "clients": sorted(pool.clients),
            })
            return
        if session.pool_id and session.pool_id != pool_id:
            await self._disconnect(session, keep_connection=True)

        session.client_id = client_id
        session.pool_id = pool_id
        pool.clients[client_id] = session
        session.joined_once = True
        if session.joined is not None:
            session.joined.set()

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
        self._identifier(key, "key")
        now = time.time()
        ttl = payload.get("ttl")
        if ttl is not None:
            self._finite_number(ttl, "ttl", allow_zero=True)
            if not math.isfinite(now + ttl):
                raise ValueError("ttl deadline must be finite")
        if not isinstance(payload.get("persistent", False), bool):
            raise ValueError("persistent must be a boolean")
        existing = await self._expire_entry(pool, key)
        version = 1 if existing is None else existing.version + 1
        entry = BufferEntry(
            value=payload.get("value"),
            updated_at=now,
            updated_by=session.client_id,
            persistent=bool(payload.get("persistent", False)),
            ttl=ttl,
            version=version,
            expires_at=time.monotonic() + ttl if ttl is not None else None,
        )
        entry.size_bytes = self._buffer_size(key, entry)
        if (existing is None and len(pool.buffers) >= self.config.max_buffers_per_pool
                or pool.buffer_bytes - (existing.size_bytes if existing else 0) + entry.size_bytes > self.config.max_pool_bytes):
            raise asyncio.QueueFull()
        pool.buffers[key] = entry
        pool.buffer_bytes += entry.size_bytes - (existing.size_bytes if existing else 0)
        # Push onto expiry heap if TTL set (non-blocking)
        if ttl is not None:
            heapq.heappush(self._expiry_heap, (entry.expires_at, pool.pool_id, key, entry.version))
        self._compact_expiry_heap()
        if entry.persistent or existing is not None and existing.persistent:
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
        self._identifier(key, "key")
        entry = await self._expire_entry(pool, key)
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
        self._identifier(key, "key")
        entry = pool.buffers.pop(key, None)
        if entry is not None:
            pool.buffer_bytes -= entry.size_bytes
            if entry.persistent:
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
        if pattern is not None and not isinstance(pattern, str):
            raise ValueError("pattern must be a string")
        for key in list(pool.buffers):
            await self._expire_entry(pool, key)
        keys = sorted(pool.buffers.keys())
        if pattern:
            keys = [key for key in keys if key.startswith(pattern)]
        await self._ack(session.writer, message, {"keys": keys})

    async def _handle_subscribe_buffer(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        payload = message.get("payload") or {}
        key = payload.get("key")
        self._identifier(key, "key")
        subscribers = pool.subscriptions.get(key, set())
        if session.client_id not in subscribers:
            if pool.subscription_count >= self.config.max_subscriptions_per_pool:
                raise asyncio.QueueFull()
            pool.subscription_count += 1
        pool.subscriptions.setdefault(key, set()).add(session.client_id)
        await self._ack(session.writer, message, {"key": key, "subscribed": True})
        self._record_event("info", "buffer_subscribed", pool=pool.pool_id,
                           client_id=session.client_id, extra={"key": key})

    async def _handle_unsubscribe_buffer(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        payload = message.get("payload") or {}
        key = payload.get("key")
        self._identifier(key, "key")
        subscribers = pool.subscriptions.get(key, set())
        if session.client_id in subscribers:
            pool.subscription_count -= 1
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
        self._identifier(target_client_id, "target_client_id")
        self._identifier(event, "event")
        target = pool.clients.get(target_client_id)
        if target is None or target.closed or target.closing:
            await self._send_error(session.writer, message, "target_not_found", "Target client is not connected")
            self._record_event(
                "warn", "call_target_missing",
                pool=pool.pool_id, client_id=session.client_id,
                extra={"target_client_id": target_client_id, "event": event},
            )
            return

        self._accept_call(pool, session, target, event, payload, message)

    def _accept_call(
        self, pool: PoolState, origin: ClientSession, target: ClientSession,
        event: str, payload: dict, request: dict,
        registration: Optional[ProcessRegistration] = None,
        parent_request_id: Optional[str] = None, acknowledge: bool = True,
    ) -> Optional[RouteEntry]:
        origin_id = parent_request_id or request.get("request_id") or _next_id()
        self._identifier(origin_id, "request_id")
        if parent_request_id is None and origin_id in origin.active_requests:
            self._write_nowait(origin.writer, self._error_envelope(origin, request, "duplicate_request", "Request ID is already active"))
            return None
        timeout = payload.get("timeout")
        if timeout is None:
            timeout = self.config.rpc_timeout
        self._finite_number(timeout, "timeout")
        response_id = payload.get("response_to") or origin.client_id
        self._identifier(response_id, "response_to")
        response = pool.clients.get(response_id)
        if response is None or response.closed or response.closing:
            raise ValueError("response_to must name a connected member of this pool")
        if (len(pool.in_flight_requests) >= self.config.max_routes_per_pool
                or origin.route_count >= self.config.max_routes_per_session
                or self._route_count >= self.config.max_routes):
            if acknowledge:
                self._write_nowait(origin.writer, self._error_envelope(origin, request, "overloaded", "Outstanding route limit exceeded"))
            self._metrics["overload_rejections"] += 1
            return None
        hop_id = _next_id()
        route = RouteEntry(
            request_id=hop_id, origin_request_id=hop_id if parent_request_id else origin_id,
            parent_request_id=parent_request_id,
            origin_client_id=origin.client_id, target_client_id=target.client_id,
            response_client_id=response.client_id, event=event, created_at=time.time(),
            expires_at=time.monotonic() + timeout,
            process_registration_id=registration.process_id if registration else None,
            process_registration=registration,
            origin_session=origin, target_session=target, response_session=response,
            origin_generation=origin.generation, target_generation=target.generation,
            response_generation=response.generation,
        )
        invocation = {
            "type": "call_app", "request_id": hop_id, "client_id": origin.client_id,
            "pool": pool.pool_id, "payload": {
                "event": event, "data": payload.get("data", {}),
                "source_client_id": origin.client_id, "target_client_id": target.client_id,
                "response_to": response.client_id,
            },
        }
        encoded_invocation = encode_message(invocation)
        if len(encoded_invocation) - 1 > self.config.max_frame_bytes:
            if acknowledge:
                self._write_nowait(origin.writer, self._error_envelope(origin, request, "response_too_large", "Generated invocation exceeds configured frame maximum"))
            return None
        ack_frame = None
        if acknowledge:
            ack_frame = self._reserve_message(origin, {
                "type": "ack", "request_id": request.get("request_id"),
                "client_id": origin.client_id, "pool": pool.pool_id,
                "payload": {"queued": True, "request_id": origin_id,
                            **({"process_id": event} if registration else {})},
            }, control=True)
            if ack_frame is None:
                self._write_nowait(origin.writer, self._error_envelope(origin, request, "response_too_large", "Acceptance response exceeds configured frame maximum"))
                return None
        frame = self._reserve_encoded(target, encoded_invocation, route=route, generation=target.generation)
        if frame is None:
            if ack_frame is not None:
                self._remove_frame(origin, ack_frame)
                self._write_nowait(origin.writer, self._error_envelope(origin, request, "delivery_failed", "Target delivery path rejected the call"))
            return None
        pool.in_flight_requests[hop_id] = route
        self._route_count += 1
        origin.route_count += 1
        origin.active_requests.add(origin_id)
        origin.active_request_counts[origin_id] = origin.active_request_counts.get(origin_id, 0) + 1
        if registration:
            for replica in registration.replicas:
                if replica.client_id == target.client_id:
                    replica.in_flight += 1
                    break
        self._metrics["accepted_calls"] += 1
        return route

    @staticmethod
    def _session_matches(session: Optional[ClientSession], generation: int, pool: PoolState) -> bool:
        return (session is not None and not session.closed and not session.closing
                and session.generation == generation and session.pool_id == pool.pool_id
                and pool.clients.get(session.client_id) is session)

    def _finalize_route(
        self, pool: PoolState, route: RouteEntry, result: Optional[dict] = None,
        code: Optional[str] = None, error: Optional[str] = None,
    ) -> bool:
        if pool.in_flight_requests.get(route.request_id) is not route:
            return False
        pool.in_flight_requests.pop(route.request_id)
        self._route_count -= 1
        origin = route.origin_session
        active_id = route.parent_request_id or route.origin_request_id
        if origin is not None:
            origin.route_count = max(0, origin.route_count - 1)
            remaining = origin.active_request_counts.get(active_id, 1) - 1
            if remaining:
                origin.active_request_counts[active_id] = remaining
            else:
                origin.active_request_counts.pop(active_id, None)
                origin.active_requests.discard(active_id)
        if route.process_registration is not None:
            for replica in route.process_registration.replicas:
                if replica.client_id == route.target_client_id:
                    replica.in_flight = max(0, replica.in_flight - 1)
                    break
        if code == "timeout":
            self._metrics["timeouts"] += 1
        if code is not None or result is not None and result.get("error") is not None:
            self._metrics["failed_calls"] += 1
        else:
            self._metrics["completed_calls"] += 1
        self._request_latencies.append(time.time() - route.created_at)
        destination = route.response_session
        if not self._session_matches(destination, route.response_generation, pool):
            if self._session_matches(origin, route.origin_generation, pool):
                destination = origin
                code, error = "response_disconnected", "Response recipient disconnected"
            else:
                return False
        public_id = route.origin_request_id
        payload = {
            "request_id": public_id, "event": route.event,
            "source_client_id": route.origin_client_id, "target_client_id": route.target_client_id,
            "response_to": route.response_client_id,
        }
        if route.parent_request_id is not None:
            payload["parent_request_id"] = route.parent_request_id
        if code is not None:
            payload.update({"code": code, "message": error, "execution_uncertain": route.sent})
        else:
            payload.update({"value": result.get("value"), "error": result.get("error")})
        return self._write_nowait(destination.writer, {
            "type": "error" if code else "app_result", "request_id": public_id,
            "client_id": route.target_client_id, "pool": pool.pool_id, "payload": payload,
        })

    async def _handle_app_result(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        request_id = message.get("request_id")
        if not request_id:
            raise ValueError("request_id is required")
        route = pool.in_flight_requests.get(request_id)
        if route is None:
            await self._send_error(session.writer, message, "route_not_found", "Request route no longer exists")
            self._record_event("warn", "route_not_found", pool=pool.pool_id,
                               client_id=session.client_id, extra={"request_id": request_id})
            return

        if route.target_session is not session or route.target_generation != session.generation:
            await self._send_error(session.writer, message, "wrong_callee", "Only the designated callee may complete this route")
            return
        if route.expires_at <= time.monotonic():
            self._finalize_route(pool, route, code="timeout", error="Result arrived after its deadline")
            await self._send_error(session.writer, message, "route_expired", "Result arrived after its deadline")
            return
        delivered = self._finalize_route(pool, route, result=message.get("payload") or {})
        await self._ack(session.writer, message, {"delivered": delivered})

    async def _handle_emit_event(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        payload = message.get("payload") or {}
        event = payload.get("event")
        self._identifier(event, "event")
        target_client_id = payload.get("target_client_id")
        if target_client_id is not None:
            self._identifier(target_client_id, "target_client_id")
        if payload.get("response_to") is not None:
            self._identifier(payload["response_to"], "response_to")
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
        if len(encode_message(envelope)) - 1 > self.config.max_frame_bytes:
            await self._send_error(session.writer, message, "response_too_large", "Generated event exceeds configured frame maximum")
            return
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
            accepted = await self._send_message(target.writer, envelope)
            delivery = {"accepted": [target_client_id] if accepted else [], "failed": [] if accepted else [target_client_id]}
        else:
            # Fan-out to all other clients in pool — parallel
            recipients = [
                s for cid, s in list(pool.clients.items())
                if cid != session.client_id
            ]
            delivery = await self._fanout(recipients, envelope)
        if delivery["failed"]:
            self._write_nowait(session.writer, {
                **self._error_envelope(session, message, "partial_delivery", "Event was not accepted by every destination; do not automatically retry"),
                "payload": {"code": "partial_delivery", "message": "Partial event delivery", **delivery},
            })
        else:
            await self._ack(session.writer, message, {"delivered": True, **delivery})
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
        self._identifier(process_name, "process_name")

        scale = payload.get("scale", False)
        max_replicas = payload.get("max_replicas", 10)
        group_id = payload.get("group_id")
        worker_kind = payload.get("worker_kind", "thread")
        min_workers = payload.get("min_workers", 1)
        max_workers = payload.get("max_workers", max_replicas)
        for value, name in ((min_workers, "min_workers"), (max_workers, "max_workers"), (max_replicas, "max_replicas")):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if min_workers > max_workers:
            raise ValueError("min_workers must not exceed max_workers")
        if not isinstance(scale, bool) or worker_kind not in {"thread", "process", "adaptive"}:
            raise ValueError("Invalid scale or worker_kind")
        if group_id is not None:
            self._identifier(group_id, "group_id")

        process_id = f"{session.client_id}:{process_name}"
        if process_id not in pool.processes and len(pool.processes) >= self.config.max_processes_per_pool:
            raise asyncio.QueueFull()
        derived_group_id = group_id or f"{session.client_id}:{process_name}:{str(uuid.uuid4())[:8]}"
        reg = ProcessRegistration(
            process_id=process_id,
            process_name=process_name,
            owner_client_id=session.client_id,
            group_id=derived_group_id,
            scale=bool(scale),
            max_replicas=max_workers,
            worker_kind=worker_kind,
            min_workers=min_workers,
            max_workers=max_workers,
            worker_count=min_workers,
            replicas=[ProcessReplica(
                client_id=session.client_id,
                created_at=time.time(),
            )],
            created_at=time.time(),
        )
        pool.processes[process_id] = reg

        ack_payload: dict = {
            "process_id": process_id,
            "group_id": derived_group_id,
            "worker_kind": worker_kind,
            "min_workers": min_workers,
            "max_workers": max_workers,
        }
        if scale:
            ack_payload["scale"] = True
            ack_payload["max_replicas"] = max_replicas
        await self._ack(session.writer, message, ack_payload)
        self._record_event("info", "process_registered",
            pool=pool.pool_id, client_id=session.client_id,
            extra={"process_id": process_id, "scale": scale, "group_id": derived_group_id,
                   "worker_kind": worker_kind, "min_workers": min_workers, "max_workers": max_workers})

    async def _handle_unregister_process(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        payload = message.get("payload") or {}
        process_name = payload.get("process_name")
        self._identifier(process_name, "process_name")
        process_id = f"{session.client_id}:{process_name}"
        removed = pool.processes.pop(process_id, None)
        for route in list(pool.in_flight_requests.values()):
            if route.process_registration is removed and removed is not None:
                self._finalize_route(pool, route, code="process_unregistered", error="Process unregistered before completion")
        await self._ack(session.writer, message, {"process_id": process_id, "removed": True})
        self._record_event("info", "process_unregistered",
            pool=pool.pool_id, client_id=session.client_id,
            extra={"process_id": process_id})

    async def _handle_call_process(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        payload = message.get("payload") or {}
        process_id = payload.get("process_id")
        self._identifier(process_id, "process_id", max_bytes=1025)

        # Short-name routing: when process_id has no ':', treat it as a short
        # process name and do cross-client round-robin across all registrations.
        if ":" not in process_id:
            candidates = [
                reg for reg in pool.processes.values()
                if reg.process_name == process_id and reg.replicas
            ]
            if not candidates:
                await self._send_error(session.writer, message, "process_not_found",
                                       f"No process named '{process_id}' is registered in this pool")
                return
            idx = (candidates[0].rr_index if len(candidates) == 1
                   else sum(r.rr_index for r in candidates)) % len(candidates)
            reg = candidates[idx]
            reg.rr_index += 1
            target_client_id = reg.owner_client_id
            process_id = reg.process_id
        else:
            reg = pool.processes.get(process_id)
            if reg is None:
                await self._send_error(session.writer, message, "process_not_found",
                                       f"Process '{process_id}' is not registered")
                self._record_event("warn", "process_not_found", pool=pool.pool_id,
                                   client_id=session.client_id, extra={"process_id": process_id})
                return
            target_client_id = reg.owner_client_id

        target = pool.clients.get(target_client_id)
        if target is None:
            await self._send_error(session.writer, message, "process_owner_offline",
                                   f"Owner '{target_client_id}' for process '{process_id}' is not connected")
            return

        self._accept_call(pool, session, target, process_id, payload, message, registration=reg)

    async def _handle_broadcast_process(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        payload = message.get("payload") or {}
        process_name = payload.get("process_name")
        self._identifier(process_name, "process_name")
        parent_id = message.get("request_id") or _next_id()
        if parent_id in session.active_requests:
            await self._send_error(session.writer, message, "duplicate_request", "Request ID is already active")
            return
        targets = []
        failed = []
        children = []
        for process_id, registration in list(pool.processes.items()):
            if registration.process_name != process_name:
                continue
            target = pool.clients.get(registration.owner_client_id)
            if target is None:
                continue
            route = self._accept_call(pool, session, target, process_id, payload, message,
                                      registration=registration, parent_request_id=parent_id, acknowledge=False)
            if route is not None:
                targets.append(process_id)
                children.append(route.origin_request_id)
            else:
                failed.append(process_id)
        if failed:
            self._write_nowait(session.writer, {
                **self._error_envelope(session, message, "partial_delivery", "Some broadcast calls were not accepted"),
                "payload": {"code": "partial_delivery", "message": "Partial process broadcast admission",
                            "accepted": targets, "failed": failed, "request_ids": children},
            })
        else:
            await self._ack(session.writer, message, {"targets": targets, "request_ids": children})
        self._record_event("info", "process_broadcast", pool=pool.pool_id,
                           client_id=session.client_id,
                           extra={"process_name": process_name, "targets": targets})

    async def _handle_list_processes(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        payload = message.get("payload") or {}
        pattern = payload.get("pattern")
        if pattern is not None and not isinstance(pattern, str):
            raise ValueError("pattern must be a string")
        processes: Dict[str, dict] = {}
        for pid, reg in pool.processes.items():
            for rep in reg.replicas:
                rep_pid = f"{rep.client_id}:{reg.process_name}"
                if pattern:
                    prefix = f"{pattern}:"
                    if not rep_pid.startswith(prefix):
                        continue
                if rep_pid not in processes:
                    processes[rep_pid] = {
                        "client_id": rep.client_id,
                        "process_name": reg.process_name,
                        "worker_kind": reg.worker_kind,
                        "worker_count": reg.worker_count or len(reg.replicas),
                        "queue_depth": reg.reported_queue_depth,
                        "avg_latency": reg.reported_avg_latency,
                        "completed_count": reg.reported_completed_count,
                    }
        await self._ack(session.writer, message, {"processes": processes})

    async def _handle_list_clients(self, session: ClientSession, message: dict) -> None:
        pool = self._require_pool(session, message)
        await self._ack(session.writer, message, {"clients": sorted(pool.clients.keys())})

    async def _handle_worker_metrics(self, session: ClientSession, message: dict) -> None:
        """
        Receive periodic worker metrics from a client.

        Payload format::
            {
                "metrics": [
                    {
                        "process_name": str,
                        "active_workers": int,
                        "queue_depth": int,
                        "avg_latency": float,
                        "completed_count": int,
                    },
                    ...
                ]
            }
        """
        pool = self._require_pool(session, message)
        payload = message.get("payload") or {}
        metrics_list = payload.get("metrics", [])
        if not isinstance(metrics_list, list):
            raise ValueError("metrics must be an array")
        for metric in metrics_list:
            if not isinstance(metric, dict):
                raise ValueError("Each metric must be an object")
            self._identifier(metric.get("process_name"), "process_name")
            for name in ("active_workers", "queue_depth", "completed_count"):
                value = metric.get(name, 0)
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    raise ValueError(f"{name} must be a nonnegative integer")
            self._finite_number(metric.get("avg_latency", 0), "avg_latency", allow_zero=True)
        now = time.time()
        for m in metrics_list:
            pname = m.get("process_name")
            if not pname:
                continue
            pid = f"{session.client_id}:{pname}"
            reg = pool.processes.get(pid)
            if reg is None:
                continue
            reg.worker_count = m.get("active_workers", reg.worker_count)
            reg.reported_queue_depth = m.get("queue_depth", 0)
            reg.reported_avg_latency = m.get("avg_latency", 0.0)
            reg.reported_completed_count = m.get("completed_count", reg.reported_completed_count)
            reg.last_metrics_at = now
        await self._ack(session.writer, message, {"received": len(metrics_list)})

    # ======================================================================
    # Disconnect / cleanup
    # ======================================================================

    async def _disconnect(self, session: ClientSession, keep_connection: bool = False) -> None:
        old_generation = session.generation
        session.generation += 1
        pool = self._pools.get(session.pool_id)
        session.pool_id = None
        if pool is None or pool.clients.get(session.client_id) is not session:
            return
        pool.clients.pop(session.client_id)
        for key, subscribers in list(pool.subscriptions.items()):
            if session.client_id in subscribers:
                subscribers.remove(session.client_id)
                pool.subscription_count -= 1
            if not subscribers:
                pool.subscriptions.pop(key, None)
        for route in list(pool.in_flight_requests.values()):
            if (route.origin_session is session and route.origin_generation == old_generation
                    or route.target_session is session and route.target_generation == old_generation
                    or route.response_session is session and route.response_generation == old_generation):
                self._finalize_route(pool, route, code="peer_disconnected", error="A routed session left its pool before completion")
        for pid, registration in list(pool.processes.items()):
            if registration.owner_client_id == session.client_id:
                pool.processes.pop(pid, None)
        await self._broadcast_presence(pool, session.client_id, "left")
        self._record_event("info", "client_left", pool=pool.pool_id, client_id=session.client_id)

    def _fail_session(self, session: ClientSession, reason: str) -> None:
        if session.closing or session.closed:
            return
        session.closing = True
        self._worker_pool.invalidate(session)
        self._metrics["slow_disconnects"] += 1
        self._record_event("warn", "connection_failed", pool=session.pool_id,
                           client_id=session.client_id, extra={"reason": reason})
        session.close_task = self._track(self._finish_close(session), "latzero-close")

    async def _close_session(self, session: ClientSession) -> None:
        if session.close_task is None:
            session.closing = True
            self._worker_pool.invalidate(session)
            session.close_task = self._track(self._finish_close(session), "latzero-close")
        if session.close_task is not asyncio.current_task():
            await asyncio.shield(session.close_task)

    async def _finish_close(self, session: ClientSession) -> None:
        try:
            session.closed = True
            if session.joined is not None:
                session.joined.set()
            await self._disconnect(session)
            if (session.outbox_drained is not None and session.writer_task is not None
                    and not session.writer_task.done() and session.writer_task is not asyncio.current_task()):
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(session.outbox_drained.wait(), self.config.write_timeout)
            if session.writer_task is not None and session.writer_task is not asyncio.current_task():
                session.writer_task.cancel()
                await asyncio.gather(session.writer_task, return_exceptions=True)
            while session.outbox:
                self._release_frame(session, session.outbox.popleft())
            writer = session.writer
            if hasattr(writer, "write"):
                writer.close()
                with suppress(Exception):
                    await asyncio.wait_for(writer.wait_closed(), self.config.write_timeout)
            else:
                with suppress(Exception):
                    await asyncio.wait_for(writer.close(), self.config.write_timeout)
            reader = session.reader_task
            if reader is not None and reader is not asyncio.current_task() and not reader.done():
                reader.cancel()
        finally:
            if self._sessions.pop(id(session), None) is not None:
                self._connection_count -= 1
            self._by_writer.pop(id(session.writer), None)

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

    async def _fanout(self, sessions: List[ClientSession], message: dict) -> dict:
        """Atomically reserve ordered placeholders, then release them fairly."""
        delivery = {"accepted": [], "failed": []}
        if not sessions:
            return delivery
        encoded = encode_message(message)
        websocket_text = encoded[:-1].decode("utf-8") if any(not hasattr(s.writer, "write") for s in sessions) else None
        size = len(encoded) + 64 * len(sessions)
        if len(encoded) - 1 > self.config.max_frame_bytes and message.get("type") == "emit_event":
            delivery["failed"] = [session.client_id for session in sessions]
            return delivery
        if (len(encoded) - 1 > self.config.max_frame_bytes
                or len(self._fanout_queue) >= self.config.max_fanout_messages
                or self._fanout_bytes + size > self.config.max_fanout_bytes):
            for session in sessions:
                delivery["failed"].append(session.client_id)
                if message.get("type") != "emit_event":
                    self._fail_session(session, "fanout_capacity")
            return delivery
        reserved = []
        for session in sessions:
            frame = self._reserve_encoded(session, encoded, generation=session.generation, ready=False,
                                          websocket_text=websocket_text)
            if frame is None:
                delivery["failed"].append(session.client_id)
            else:
                reserved.append((session, frame))
                delivery["accepted"].append(session.client_id)
        if reserved:
            self._fanout_queue.append((reserved, size))
            self._fanout_bytes += size
            self._fanout_ready.set()
        return delivery

    async def _fanout_loop(self) -> None:
        while True:
            await self._fanout_ready.wait()
            while self._fanout_queue:
                reserved, size = self._fanout_queue[0]
                for index, (session, frame) in enumerate(reserved, 1):
                    frame.ready = True
                    session.outbox_ready.set()
                    if index % self.config.fanout_slice == 0:
                        await asyncio.sleep(0)
                self._fanout_queue.popleft()
                self._fanout_bytes -= size
                reserved = session = frame = None
                await asyncio.sleep(0)
            self._fanout_ready.clear()

    # ======================================================================
    # Low-level send helpers
    # ======================================================================

    def _reserve_message(self, session: ClientSession, message: dict, **options: Any) -> Optional[_OutboundFrame]:
        encoded = encode_message(message)
        if len(encoded) - 1 > self.config.max_frame_bytes:
            return None
        return self._reserve_encoded(session, encoded, **options)

    def _reserve_encoded(
        self, session: ClientSession, encoded: bytes, control: bool = False,
        generation: Optional[int] = None, route: Optional[RouteEntry] = None, ready: bool = True,
        websocket_text: Optional[str] = None,
    ) -> Optional[_OutboundFrame]:
        if session.closed or session.closing:
            return None
        cfg = self.config
        messages = cfg.max_outbox_messages + (cfg.outbox_control_messages if control else 0)
        byte_limit = cfg.max_outbox_bytes + (cfg.outbox_control_bytes if control else 0)
        global_limit = cfg.max_global_outbox_bytes + (cfg.outbox_control_bytes if control else 0)
        if (session.outbox_messages >= messages or session.outbox_bytes + len(encoded) > byte_limit
                or self._outbox_bytes + len(encoded) > global_limit):
            self._fail_session(session, "egress_capacity")
            return None
        frame = _OutboundFrame(encoded=encoded, websocket_text=websocket_text, generation=generation, route=route, ready=ready)
        session.outbox.append(frame)
        session.outbox_bytes += len(encoded)
        session.outbox_messages += 1
        self._outbox_bytes += len(encoded)
        session.outbox_drained.clear()
        if ready:
            session.outbox_ready.set()
        return frame

    def _release_frame(self, session: ClientSession, frame: _OutboundFrame) -> None:
        if frame.released:
            return
        frame.released = True
        session.outbox_bytes -= len(frame.encoded)
        session.outbox_messages -= 1
        self._outbox_bytes -= len(frame.encoded)
        if session.outbox_messages == 0:
            session.outbox_drained.set()

    def _remove_frame(self, session: ClientSession, frame: _OutboundFrame) -> None:
        session.outbox.remove(frame)
        self._release_frame(session, frame)

    async def _writer_loop(self, session: ClientSession) -> None:
        current = None
        processed = 0
        try:
            while True:
                await session.outbox_ready.wait()
                while session.outbox:
                    if not session.outbox[0].ready:
                        break
                    current = session.outbox.popleft()
                    if current.generation is not None and current.generation != session.generation:
                        self._release_frame(session, current)
                        current = None
                        continue
                    route = current.route
                    if route is not None:
                        pool = self._pools.get(session.pool_id)
                        if pool is None or pool.in_flight_requests.get(route.request_id) is not route:
                            self._release_frame(session, current)
                            current = route = pool = None
                            continue
                        if not all((
                            self._session_matches(route.origin_session, route.origin_generation, pool),
                            self._session_matches(route.target_session, route.target_generation, pool),
                            self._session_matches(route.response_session, route.response_generation, pool),
                        )):
                            self._finalize_route(pool, route, code="peer_disconnected", error="A routed session closed before transmission")
                            self._release_frame(session, current)
                            current = route = pool = None
                            continue
                        if route.expires_at <= time.monotonic():
                            self._finalize_route(pool, route, code="timeout", error="Call expired before transmission")
                            self._release_frame(session, current)
                            current = route = pool = None
                            continue
                    if hasattr(session.writer, "write"):
                        if session.writer.transport.get_write_buffer_size() > self.config.connection_critical_bytes:
                            raise ConnectionError("Transport write buffer exceeded critical limit")
                        if route is not None:
                            route.sent = True
                        session.writer.write(current.encoded)
                        await asyncio.wait_for(session.writer.drain(), self.config.write_timeout)
                    else:
                        if route is not None:
                            route.sent = True
                        text = current.websocket_text or current.encoded[:-1].decode("utf-8")
                        await asyncio.wait_for(session.writer.send(text), self.config.write_timeout)
                        text = None
                    self._release_frame(session, current)
                    current = None
                    route = pool = None
                    processed += 1
                    if processed % self.config.dispatch_slice == 0:
                        await asyncio.sleep(0)
                session.outbox_ready.clear()
        except (ConnectionClosed, ConnectionError, OSError, asyncio.TimeoutError) as exc:
            self._fail_session(session, str(exc))
        except Exception as exc:
            _LOGGER.exception("Connection writer failed")
            self._fail_session(session, str(exc))
        finally:
            if current is not None:
                self._release_frame(session, current)

    def _write_nowait(self, writer: Any, message: Dict[str, Any]) -> bool:
        session = self._by_writer.get(id(writer))
        if session is None:
            return False
        encoded = encode_message(message)
        if len(encoded) - 1 > self.config.max_frame_bytes:
            small = self._error_envelope(session, message, "response_too_large", "Response exceeds configured frame maximum")
            safe = encode_message(small)
            if len(safe) - 1 > self.config.max_frame_bytes or self._reserve_encoded(session, safe, control=True) is None:
                self._fail_session(session, "response_too_large")
            return False
        msg_type = message.get("type")
        return self._reserve_encoded(
            session, encoded, control=msg_type in {"ack", "error", "redirect", "app_result", "process_scale"},
            generation=session.generation if msg_type in {"call_app", "emit_event", "app_result", "process_scale", "buffer_update", "presence_update"} else None,
        ) is not None

    async def _send_to_client(self, pool: PoolState, client_id: str, message: dict) -> None:
        target = pool.clients.get(client_id)
        if target is None:
            return
        with suppress(Exception):
            await self._send_message(target.writer, message)

    async def _ack(self, writer: Any, request: dict, payload: dict) -> None:
        session = self._by_writer.get(id(writer))
        await self._send_message(
            writer,
            {
                "type": "ack",
                "request_id": request.get("request_id"),
                "client_id": session.client_id if session else request.get("client_id"),
                "pool": session.pool_id if session else request.get("pool"),
                "payload": payload,
            },
        )

    async def _send_error(self, writer: Any, request: Any, code: str, message: str) -> None:
        session = self._by_writer.get(id(writer))
        if session is not None:
            self._write_nowait(writer, self._error_envelope(session, request, code, message))

    @staticmethod
    def _error_envelope(session: ClientSession, request: Any, code: str, message: str) -> dict:
        return {"type": "error", "request_id": request.get("request_id") if isinstance(request, dict) else None,
                "client_id": session.client_id, "pool": session.pool_id,
                "payload": {"code": code, "message": message}}

    async def _send_message(self, writer: Any, message: Dict[str, Any]) -> bool:
        """True means bounded delivery-path admission, not remote observation."""
        return self._write_nowait(writer, message)

    @staticmethod
    def _identifier(value: Any, name: str, max_bytes: int = 512) -> None:
        if not isinstance(value, str) or not value or len(value.encode("utf-8")) > max_bytes:
            raise ValueError(f"{name} must be a nonempty string up to {max_bytes} UTF-8 bytes")

    @staticmethod
    def _finite_number(value: Any, name: str, allow_zero: bool = False) -> None:
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or value < 0 or not allow_zero and value == 0):
            raise ValueError(f"{name} must be a finite {'nonnegative' if allow_zero else 'positive'} number")

    @staticmethod
    def _buffer_size(key: str, entry: BufferEntry) -> int:
        return len(encode_message({key: entry.to_dict()})) + 512

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
            "pending_websocket_handshakes": len(self._pending_websockets),
            "max_connections": self.config.max_connections,
            "tps": self._current_tps,
            "avg_latency": avg_latency,
            "memory_rss": memory_rss,
            "metrics": dict(self._metrics),
            "egress_bytes": self._outbox_bytes,
            "fanout_bytes": self._fanout_bytes,
            "queue_bytes": wp_stats.queue_bytes,
            "health": {"background_error": self._health_error, "persistence": self._store.health},
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
