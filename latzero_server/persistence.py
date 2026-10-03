"""Eventual, coalesced pool snapshots, not a write-ahead durability guarantee.

The event loop owns dirty pools and captures their snapshots. Only deep-owned
snapshot dictionaries reach the executor; one saver serializes all writes.
"""

import asyncio
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
import logging
import math
from pathlib import Path
import tempfile
import time
from typing import Any, Dict, Optional, Tuple

from .models import PoolState


logger = logging.getLogger(__name__)


class SnapshotStoreError(RuntimeError):
    """A snapshot barrier could not finish; pending state has been retained."""


@dataclass
class _DirtyPool:
    pool: PoolState
    version: int = 1
    failures: int = 0
    retry_at: float = 0.0
    retry_delay: float = 0.0
    error: Optional[str] = None


class SnapshotStore:
    """Persists pool metadata and persistent buffers to JSON files.

    ``enqueue`` is non-blocking and raises ``asyncio.QueueFull`` when a new
    dirty pool would exceed the limit. Updates to an already dirty pool still
    coalesce. ``stop`` is a flush barrier and rejects new enqueues. On timeout
    it leaves an active executor write alone, reports failure, and prevents a
    second writer from starting until that saver has finished.

    Synchronous ``save_pool`` and ``delete_pool`` are for use outside a running
    event loop, with no active saver. Legacy files are preserved as backups.
    """

    def __init__(
        self,
        data_dir: Path,
        batch_window: float = 0.1,
        max_dirty_pools: int = 1024,
        retry_initial_delay: float = 0.1,
        retry_max_delay: float = 5.0,
        shutdown_timeout: float = 10.0,
        shutdown_max_retries: int = 3,
    ):
        for name, value in (
            ("batch_window", batch_window),
            ("retry_initial_delay", retry_initial_delay),
            ("retry_max_delay", retry_max_delay),
            ("shutdown_timeout", shutdown_timeout),
        ):
            if (
                type(value) not in (int, float)
                or not math.isfinite(value)
                or value < 0
                or (name != "batch_window" and value == 0)
            ):
                raise ValueError("{} must be a finite valid duration".format(name))
        if retry_max_delay < retry_initial_delay:
            raise ValueError("retry_max_delay must be >= retry_initial_delay")
        for name, value in (
            ("max_dirty_pools", max_dirty_pools),
            ("shutdown_max_retries", shutdown_max_retries),
        ):
            if type(value) is not int or value < 1:
                raise ValueError("{} must be a positive integer".format(name))

        self._data_dir = Path(data_dir)
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._batch_window = batch_window
        self._max_dirty_pools = max_dirty_pools
        self._retry_initial_delay = retry_initial_delay
        self._retry_max_delay = retry_max_delay
        self._shutdown_timeout = shutdown_timeout
        self._shutdown_max_retries = shutdown_max_retries
        self._dirty: Dict[str, _DirtyPool] = {}
        self._wakeup: Optional[asyncio.Event] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._saver_task: Optional[asyncio.Task] = None
        self._stopping = False
        self._shutdown_deadline: Optional[float] = None
        self._shutdown_failures = 0
        self._in_flight_pool: Optional[str] = None
        self._terminal_error: Optional[BaseException] = None
        self._last_error: Optional[str] = None
        self._last_error_at: Optional[float] = None
        self._last_success_at: Optional[float] = None
        self._failures = 0
        self._retries = 0
        self._rejected = 0
        self._load_errors = 0
        self._load_error: Optional[str] = None
        self._writes = 0
        self._skipped_writes = 0
        self._snapshot_copy_seconds = 0.0
        self._serialization_seconds = 0.0
        self._write_seconds = 0.0
        self._failed_write_seconds = 0.0

    @property
    def health(self) -> Dict[str, Any]:
        """JSON-safe health and cumulative costs; success is not an fsync ACK."""
        errors = [entry.error for entry in self._dirty.values() if entry.error]
        error = (
            "{}: {}".format(type(self._terminal_error).__name__, self._terminal_error)
            if self._terminal_error is not None else None
        )
        error = error or self._load_error
        return {
            "healthy": error is None and not errors,
            "running": self._saver_task is not None and not self._saver_task.done(),
            "stopping": self._stopping,
            "dirty_pools": len(self._dirty),
            "max_dirty_pools": self._max_dirty_pools,
            "in_flight": self._in_flight_pool is not None,
            "in_flight_pool": self._in_flight_pool,
            "retrying_pools": len(errors),
            "error": error or (errors[0] if errors else None),
            "last_error": self._last_error,
            "last_error_at": self._last_error_at,
            "last_success_at": self._last_success_at,
            "failures": self._failures,
            "retries": self._retries,
            "rejected_pools": self._rejected,
            "load_errors": self._load_errors,
            "writes": self._writes,
            "skipped_writes": self._skipped_writes,
            "snapshot_copy_seconds": self._snapshot_copy_seconds,
            "serialization_seconds": self._serialization_seconds,
            "write_seconds": self._write_seconds,
            "failed_write_seconds": self._failed_write_seconds,
        }

    def start(self) -> None:
        """Start (or explicitly retry) the saver inside the owning event loop."""
        loop = asyncio.get_running_loop()
        if self._saver_task is not None and not self._saver_task.done():
            if self._stopping or self._loop is not loop:
                raise RuntimeError("Snapshot saver is still stopping or on another loop")
            return
        self._loop = loop
        self._wakeup = asyncio.Event()
        self._stopping = False
        self._shutdown_deadline = None
        self._shutdown_failures = 0
        self._terminal_error = None
        for entry in self._dirty.values():
            entry.retry_at = 0.0
        self._saver_task = loop.create_task(
            self._saver_loop(), name="latzero-snapshot-saver"
        )
        self._saver_task.add_done_callback(self._saver_finished)

    async def stop(self) -> None:
        """Await the single saver, including captured work and executor I/O."""
        if self._saver_task is None:
            if not self._dirty:
                self._stopping = True
                return
            self.start()
        task = self._saver_task
        if task.done():
            if task.cancelled():
                raise SnapshotStoreError("Snapshot saver was cancelled")
            task.result()
            if self._terminal_error is not None:
                raise self._terminal_error
            return
        if self._loop is not asyncio.get_running_loop():
            raise RuntimeError("Snapshot stop must run on the owning event loop")
        if not self._stopping:
            self._stopping = True
            self._shutdown_deadline = self._loop.time() + self._shutdown_timeout
            for entry in self._dirty.values():
                entry.retry_at = 0.0
        self._wakeup.set()
        remaining = max(0.0, self._shutdown_deadline - self._loop.time())
        try:
            await asyncio.wait_for(asyncio.shield(task), remaining)
        except asyncio.TimeoutError as exc:
            error = asyncio.TimeoutError(
                "Snapshot shutdown timed out; pending state and active I/O retained"
            )
            self._terminal_error = error
            self._record_error(error)
            self._wakeup.set()
            raise error from exc
        if self._terminal_error is not None:
            raise self._terminal_error

    def enqueue(self, pool: PoolState) -> None:
        """Mark persistent state dirty; same-pool updates consume no new slots."""
        if self._stopping:
            raise RuntimeError("Snapshot store is stopping; new writes are rejected")
        if self._saver_task is not None and not self._saver_task.done():
            if self._loop is not asyncio.get_running_loop():
                raise RuntimeError("Snapshot enqueue must run on the owning event loop")
        if not isinstance(pool.pool_id, str) or not pool.pool_id:
            raise ValueError("pool_id must be a nonempty string")
        pool.pool_id.encode("utf-8")
        entry = self._dirty.get(pool.pool_id)
        if entry is None:
            if len(self._dirty) >= self._max_dirty_pools:
                self._rejected += 1
                raise asyncio.QueueFull("Snapshot dirty-pool limit reached")
            self._dirty[pool.pool_id] = _DirtyPool(pool)
        else:
            entry.pool = pool
            entry.version += 1
        if self._wakeup is not None:
            self._wakeup.set()

    def _record_error(self, exc: BaseException) -> None:
        self._last_error = "{}: {}".format(type(exc).__name__, exc)
        self._last_error_at = time.time()
        logger.warning("Snapshot persistence failure: %s", self._last_error)

    def _saver_finished(self, task: asyncio.Task) -> None:
        # Retrieve terminal exceptions even when shutdown was not awaited.
        if not task.cancelled():
            task.exception()

    def _check_shutdown(self) -> None:
        if not self._stopping:
            return
        if self._terminal_error is not None:
            raise self._terminal_error
        if self._shutdown_failures >= self._shutdown_max_retries:
            raise SnapshotStoreError(
                "Snapshot shutdown failed after {} attempts: {}".format(
                    self._shutdown_failures, self._last_error
                )
            )
        if self._loop.time() >= self._shutdown_deadline:
            raise asyncio.TimeoutError("Snapshot shutdown deadline exceeded")

    async def _wait_for_wakeup(self, delay: Optional[float] = None) -> None:
        self._wakeup.clear()
        if delay is None:
            await self._wakeup.wait()
        elif delay > 0:
            try:
                await asyncio.wait_for(self._wakeup.wait(), delay)
            except asyncio.TimeoutError:
                pass

    async def _saver_loop(self) -> None:
        try:
            while self._dirty or not self._stopping:
                self._check_shutdown()
                if not self._dirty:
                    await self._wait_for_wakeup()
                    continue
                if not self._stopping and self._batch_window:
                    await self._wait_for_wakeup(self._batch_window)
                # One turn per pool keeps a hot pool from starving its peers.
                for pool_id in list(self._dirty):
                    self._check_shutdown()
                    entry = self._dirty.get(pool_id)
                    if entry is None or entry.retry_at > self._loop.time():
                        continue
                    await self._save_dirty(pool_id, entry)
                    del entry
                if self._dirty:
                    delay = min(e.retry_at for e in self._dirty.values()) - self._loop.time()
                    if self._stopping:
                        delay = min(delay, self._shutdown_deadline - self._loop.time())
                    await self._wait_for_wakeup(delay)
        except asyncio.CancelledError:
            self._terminal_error = SnapshotStoreError("Snapshot saver was cancelled")
            self._record_error(self._terminal_error)
            raise
        except Exception as exc:
            self._terminal_error = exc
            self._record_error(exc)
            raise

    def _capture_snapshot(self, pool: PoolState) -> dict:
        started = time.perf_counter()
        try:
            snapshot = deepcopy(pool.snapshot())
            self._validate_snapshot(snapshot)
            if snapshot["pool_id"] != pool.pool_id:
                raise ValueError("Snapshot pool identity changed during capture")
            return snapshot
        finally:
            self._snapshot_copy_seconds += time.perf_counter() - started

    def _record_write(self, result: Tuple[bool, float, float]) -> None:
        written, serialization_seconds, write_seconds = result
        self._writes += int(written)
        self._skipped_writes += int(not written)
        self._serialization_seconds += serialization_seconds
        self._write_seconds += write_seconds
        self._last_success_at = time.time()

    async def _save_dirty(self, pool_id: str, entry: _DirtyPool) -> None:
        version = entry.version
        if entry.failures:
            self._retries += 1
        io_started = None
        try:
            snapshot = self._capture_snapshot(entry.pool)
            self._in_flight_pool = pool_id
            io_started = time.perf_counter()
            future = self._loop.run_in_executor(None, self._write_file, snapshot)
            try:
                result = await asyncio.shield(future)
            except asyncio.CancelledError:
                # A cancelled waiter must not release the writer while its
                # thread still owns the file. Normal stop never cancels us.
                await asyncio.shield(future)
                raise
            self._record_write(result)
        except Exception as exc:
            if io_started is not None:
                self._failed_write_seconds += time.perf_counter() - io_started
            self._failures += 1
            self._shutdown_failures += int(self._stopping)
            entry.failures += 1
            entry.error = "{}: {}".format(type(exc).__name__, exc)
            entry.retry_delay = (
                self._retry_initial_delay if entry.failures == 1
                else min(self._retry_max_delay, entry.retry_delay * 2)
            )
            entry.retry_at = self._loop.time() + entry.retry_delay
            self._record_error(exc)
        else:
            entry.failures = 0
            entry.error = None
            entry.retry_at = 0.0
            entry.retry_delay = 0.0
            if self._dirty.get(pool_id) is entry and entry.version == version:
                del self._dirty[pool_id]
        finally:
            self._in_flight_pool = None

    def _path_for_pool(self, pool_id: str) -> Path:
        if not isinstance(pool_id, str) or not pool_id:
            raise ValueError("pool_id must be a nonempty string")
        digest = hashlib.sha256(pool_id.encode("utf-8")).hexdigest()
        return self._data_dir / "pool-{}.json".format(digest)

    def _legacy_path_for_pool(self, pool_id: str) -> Path:
        safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in pool_id)
        return self._data_dir / "{}.json".format(safe)

    @staticmethod
    def _validate_snapshot(data: Any) -> None:
        if not isinstance(data, dict):
            raise ValueError("Snapshot must be an object")
        pool_id = data.get("pool_id")
        if not isinstance(pool_id, str) or not pool_id:
            raise ValueError("Snapshot pool_id must be a nonempty string")
        pool_id.encode("utf-8")
        if type(data.get("auth_required")) is not bool:
            raise ValueError("Snapshot auth_required must be boolean")
        token_hash = data.get("auth_token_hash")
        if token_hash is not None and (not isinstance(token_hash, str) or not token_hash):
            raise ValueError("Snapshot auth_token_hash must be null or a nonempty string")
        if data["auth_required"] and token_hash is None:
            raise ValueError("Authenticated snapshot is missing its token hash")
        buffers = data.get("buffers")
        if not isinstance(buffers, dict):
            raise ValueError("Snapshot buffers must be an object")
        for key, entry in buffers.items():
            if not isinstance(key, str) or not key or not isinstance(entry, dict):
                raise ValueError("Snapshot buffer keys and entries are invalid")
            if "value" not in entry or entry.get("persistent") is not True:
                raise ValueError("Snapshot must contain only persistent buffer entries")
            if not isinstance(entry.get("updated_by"), str):
                raise ValueError("Snapshot buffer updated_by must be a string")
            if type(entry.get("version")) is not int or entry["version"] < 1:
                raise ValueError("Snapshot buffer version must be a positive integer")
            updated_at = entry.get("updated_at")
            ttl = entry.get("ttl")
            for value in (updated_at,) if ttl is None else (updated_at, ttl):
                if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                    raise ValueError("Snapshot TTL metadata must be finite and nonnegative")
            if ttl is not None and not math.isfinite(updated_at + ttl):
                raise ValueError("Snapshot expiry must be finite")
        pending = [data]
        seen = set()
        while pending:
            value = pending.pop()
            if type(value) in (dict, list, tuple):
                if id(value) in seen:
                    continue
                seen.add(id(value))
                if isinstance(value, dict):
                    if any(not isinstance(key, str) for key in value):
                        raise ValueError("Snapshot JSON object keys must be strings")
                    pending.extend(value.values())
                else:
                    pending.extend(value)
            elif type(value) is float:
                if not math.isfinite(value):
                    raise ValueError("Snapshot JSON numbers must be finite")
            elif value is not None and type(value) not in (str, int, bool):
                raise ValueError("Snapshot values must be JSON serializable")

    @staticmethod
    def _unique_object(pairs: list) -> dict:
        data = {}
        for key, value in pairs:
            if key in data:
                raise ValueError("Snapshot contains duplicate JSON keys")
            data[key] = value
        return data

    def _read_snapshot(self, path: Path) -> Tuple[dict, bytes]:
        raw = path.read_bytes()
        data = json.loads(raw.decode("utf-8"), object_pairs_hook=self._unique_object)
        self._validate_snapshot(data)
        return data, raw

    def _has_verified_legacy(self, pool_id: str) -> bool:
        path = self._legacy_path_for_pool(pool_id)
        if not path.exists():
            return False
        try:
            data, _ = self._read_snapshot(path)
            return data["pool_id"] == pool_id
        except (ValueError, UnicodeError, TypeError):
            return False

    def _write_file(self, snapshot: dict) -> Tuple[bool, float, float]:
        """Blocking I/O for a deep-owned snapshot, never a live PoolState."""
        started = time.perf_counter()
        raw = json.dumps(snapshot, separators=(",", ":"), allow_nan=False).encode("utf-8")
        serialization_seconds = time.perf_counter() - started
        started = time.perf_counter()
        path = self._path_for_pool(snapshot["pool_id"])
        if path.exists():
            existing, existing_raw = self._read_snapshot(path)
            if existing["pool_id"] != snapshot["pool_id"]:
                raise ValueError("Canonical snapshot belongs to a different pool")
            if existing_raw == raw:
                return False, serialization_seconds, time.perf_counter() - started
        elif not snapshot["buffers"] and not snapshot["auth_required"]:
            if not self._has_verified_legacy(snapshot["pool_id"]):
                return False, serialization_seconds, time.perf_counter() - started
        # An empty canonical snapshot is a tombstone: preserved legacy backups
        # must not resurrect removed buffers or deleted authentication metadata.
        tmp = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", dir=str(self._data_dir),
                prefix=".{}-".format(path.stem), suffix=".tmp", delete=False,
            ) as handle:
                tmp = Path(handle.name)
                handle.write(raw)
            tmp.replace(path)
        except Exception:
            if tmp is not None:
                try:
                    tmp.unlink()
                except FileNotFoundError:
                    pass
                except OSError:
                    logger.warning("Could not remove failed snapshot temp file %s", tmp)
            raise
        return True, serialization_seconds, time.perf_counter() - started

    def load_pools(self) -> Dict[str, dict]:
        """Load verified JSON; canonical files take precedence over legacy ones."""
        self._load_error = None
        legacy: Dict[str, dict] = {}
        canonical: Dict[str, dict] = {}
        for path in sorted(self._data_dir.glob("*.json")):
            try:
                data, _ = self._read_snapshot(path)
                pool_id = data["pool_id"]
                if path.name.lower() == self._path_for_pool(pool_id).name.lower():
                    canonical[pool_id] = data
                elif path.name.lower() == self._legacy_path_for_pool(pool_id).name.lower():
                    legacy[pool_id] = data
                else:
                    raise ValueError("Snapshot filename does not match its pool identity")
            except Exception as exc:
                self._load_errors += 1
                self._record_error(exc)
                self._load_error = self._last_error
                logger.warning("Ignoring invalid snapshot %s", path)
        legacy.update(canonical)
        return {
            pool_id: data for pool_id, data in legacy.items()
            if data["buffers"] or data["auth_required"]
        }

    def _check_synchronous_write(self) -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise RuntimeError("Use enqueue instead of synchronous snapshot writes on the loop")
        if self._saver_task is not None and not self._saver_task.done():
            raise RuntimeError("Snapshot saver still owns pending I/O")

    def save_pool(self, pool: PoolState) -> None:
        """Capture and save synchronously outside the event loop."""
        self._check_synchronous_write()
        try:
            snapshot = self._capture_snapshot(pool)
            self._record_write(self._write_file(snapshot))
        except Exception as exc:
            self._failures += 1
            self._terminal_error = exc
            self._record_error(exc)
            raise
        self._terminal_error = None
        self._dirty.pop(pool.pool_id, None)

    def delete_pool(self, pool_id: str) -> None:
        """Suppress persisted state, retaining canonical/legacy backup safety."""
        self.save_pool(PoolState(pool_id=pool_id))
