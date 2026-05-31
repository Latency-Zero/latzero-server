"""
Snapshot persistence for latzero-server pools.

Write-ahead async saver
-----------------------
Hot path (set_buffer / delete_buffer) calls ``enqueue(pool)`` which is
non-blocking (``put_nowait``).  A background ``_saver_loop`` task drains
the queue, coalesces multiple writes for the same pool_id into one, then
runs the actual JSON serialization + file write in a thread-pool executor
via ``asyncio.to_thread`` — so the event loop is *never* blocked by disk I/O.
"""

import asyncio
import json
from pathlib import Path
from typing import Dict, Optional

from .models import PoolState


class SnapshotStore:
    """Persists pool metadata and persistent buffers to JSON files.

    Usage
    -----
    Call ``start()`` once after the event loop is running.
    Call ``enqueue(pool)`` for every write (non-blocking).
    Call ``stop()`` on shutdown to flush all pending writes.
    """

    def __init__(self, data_dir: Path, batch_window: float = 0.1):
        self._data_dir = Path(data_dir)
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._batch_window = batch_window

        # Unbounded queue — readers (connection handlers) never block.
        self._queue: asyncio.Queue = asyncio.Queue()
        self._saver_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background saver task.  Must be called inside the event loop."""
        if self._saver_task is None or self._saver_task.done():
            self._saver_task = asyncio.create_task(
                self._saver_loop(), name="latzero-snapshot-saver"
            )

    async def stop(self) -> None:
        """Flush all pending writes and stop the saver task."""
        # Drain synchronously first (flush remaining items)
        await self._flush_remaining()
        if self._saver_task is not None and not self._saver_task.done():
            self._saver_task.cancel()
            try:
                await self._saver_task
            except asyncio.CancelledError:
                pass
            self._saver_task = None

    # ------------------------------------------------------------------
    # Non-blocking write enqueue (called from hot path)
    # ------------------------------------------------------------------

    def enqueue(self, pool: PoolState) -> None:
        """Schedule a pool snapshot to be persisted.  Returns immediately."""
        self._queue.put_nowait(pool)

    # ------------------------------------------------------------------
    # Background saver loop
    # ------------------------------------------------------------------

    async def _saver_loop(self) -> None:
        """Batch and persist pool snapshots off the event loop."""
        while True:
            # Wait for at least one item
            pool = await self._queue.get()
            batch: Dict[str, PoolState] = {pool.pool_id: pool}

            # Drain any additional items that arrived in the same window
            await asyncio.sleep(self._batch_window)
            while not self._queue.empty():
                try:
                    p = self._queue.get_nowait()
                    # Later entry for same pool overrides earlier one
                    batch[p.pool_id] = p
                except asyncio.QueueEmpty:
                    break

            # Write all unique pools off the event loop
            for p in batch.values():
                await asyncio.to_thread(self._write_file, p)

    async def _flush_remaining(self) -> None:
        """Synchronously write everything still in the queue (called at shutdown)."""
        batch: Dict[str, PoolState] = {}
        while not self._queue.empty():
            try:
                p = self._queue.get_nowait()
                batch[p.pool_id] = p
            except asyncio.QueueEmpty:
                break
        for p in batch.values():
            await asyncio.to_thread(self._write_file, p)

    # ------------------------------------------------------------------
    # Internal file I/O (runs in thread-pool executor)
    # ------------------------------------------------------------------

    def _path_for_pool(self, pool_id: str) -> Path:
        safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in pool_id)
        return self._data_dir / f"{safe}.json"

    def _write_file(self, pool: PoolState) -> None:
        """Serialize and write one pool snapshot (blocking — run via to_thread)."""
        path = self._path_for_pool(pool.pool_id)
        snapshot = pool.snapshot()
        if not snapshot["buffers"] and not snapshot["auth_required"]:
            if path.exists():
                path.unlink()
            return
        tmp = path.with_suffix(".tmp")
        try:
            with tmp.open("w", encoding="utf-8") as handle:
                json.dump(snapshot, handle, separators=(",", ":"))
            tmp.replace(path)   # atomic on POSIX; best-effort on Windows
        except Exception:
            if tmp.exists():
                tmp.unlink(missing_ok=True)
            raise

    # ------------------------------------------------------------------
    # Startup loader (synchronous — called before event loop tasks run)
    # ------------------------------------------------------------------

    def load_pools(self) -> Dict[str, dict]:
        """Load all persisted pool snapshots (synchronous, called at startup)."""
        loaded: Dict[str, dict] = {}
        for path in self._data_dir.glob("*.json"):
            try:
                with path.open("r", encoding="utf-8") as handle:
                    data = json.load(handle)
                pool_id = data.get("pool_id")
                if pool_id:
                    loaded[pool_id] = data
            except Exception:
                continue
        return loaded

    def save_pool(self, pool: PoolState) -> None:
        """Synchronous save — used only at startup/teardown outside the event loop."""
        self._write_file(pool)

    def delete_pool(self, pool_id: str) -> None:
        """Remove a persisted pool snapshot."""
        path = self._path_for_pool(pool_id)
        if path.exists():
            path.unlink()
