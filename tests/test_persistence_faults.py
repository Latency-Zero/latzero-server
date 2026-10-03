"""Deterministic saver, shutdown, and storage-fault barriers."""

import asyncio
from copy import deepcopy
import json
from pathlib import Path
import threading
import time
from types import SimpleNamespace
import weakref

import pytest

from latzero_server.models import BufferEntry, PoolState
from latzero_server import persistence
from latzero_server.persistence import SnapshotStore, SnapshotStoreError


def make_pool(pool_id="demo", value=1):
    pool = PoolState(pool_id=pool_id)
    pool.buffers["value"] = BufferEntry(
        value={"items": [value]}, updated_at=1.0, updated_by="client",
        persistent=True, ttl=None, version=1,
    )
    return pool


async def wait_event(event):
    await asyncio.wait_for(event.wait(), 3)


async def turn():
    future = asyncio.get_running_loop().create_future()
    asyncio.get_running_loop().call_soon(future.set_result, None)
    await future


class WriteBarrier:
    def __init__(self, store, loop, block_count=1):
        self.write = store._write_file
        self.loop = loop
        self.started = [asyncio.Event() for _ in range(block_count)]
        self.release = [threading.Event() for _ in range(block_count)]
        self.snapshots = []
        self.thread_ids = []
        self.active = 0
        self.max_active = 0

    def __call__(self, snapshot):
        assert isinstance(snapshot, dict)
        index = len(self.snapshots)
        self.snapshots.append(deepcopy(snapshot))
        self.thread_ids.append(threading.get_ident())
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if index < len(self.started):
                self.loop.call_soon_threadsafe(self.started[index].set)
                if not self.release[index].wait(5):
                    raise AssertionError("Test did not release writer barrier")
            return self.write(snapshot)
        finally:
            self.active -= 1

    def release_all(self):
        for event in self.release:
            event.set()


@pytest.mark.asyncio
async def test_in_flight_stop_waits_for_captured_snapshot(tmp_path, monkeypatch):
    store = SnapshotStore(tmp_path, batch_window=0)
    pool = make_pool()
    expected = deepcopy(pool.snapshot())
    loop = asyncio.get_running_loop()
    barrier = WriteBarrier(store, loop)
    capture_thread_ids = []
    snapshot = pool.snapshot

    def snapshot_on_loop():
        capture_thread_ids.append(threading.get_ident())
        return snapshot()

    monkeypatch.setattr(pool, "snapshot", snapshot_on_loop)
    monkeypatch.setattr(store, "_write_file", barrier)
    store.start()
    store.enqueue(pool)
    stopping = None
    try:
        await wait_event(barrier.started[0])
        pool.buffers["value"].value["items"].append("live mutation")
        pool.buffers.clear()
        stopping = asyncio.create_task(store.stop())
        await turn()
        assert not stopping.done()
        assert store.health["in_flight"]
        assert store.health["dirty_pools"] == 1
        assert len(barrier.snapshots) == 1
        barrier.release_all()
        await asyncio.wait_for(stopping, 3)
        assert json.loads(store._path_for_pool("demo").read_text()) == expected
        assert capture_thread_ids == [threading.get_ident()]
        assert all(thread_id != threading.get_ident() for thread_id in barrier.thread_ids)
        assert not store.health["running"]
        assert store.health["dirty_pools"] == 0
        assert barrier.max_active == 1
    finally:
        barrier.release_all()
        if stopping is None:
            await store.stop()
        elif not stopping.done():
            await stopping


@pytest.mark.asyncio
async def test_dirty_during_save_is_flushed_after_old_version(tmp_path, monkeypatch):
    store = SnapshotStore(tmp_path, batch_window=0)
    pool = make_pool()
    barrier = WriteBarrier(store, asyncio.get_running_loop(), block_count=2)
    monkeypatch.setattr(store, "_write_file", barrier)
    store.start()
    store.enqueue(pool)
    stopping = None
    try:
        await wait_event(barrier.started[0])
        pool.buffers["value"].value["items"].append(2)
        pool.buffers["value"].version += 1
        for _ in range(100):
            store.enqueue(pool)
        stopping = asyncio.create_task(store.stop())
        await turn()
        barrier.release[0].set()
        await wait_event(barrier.started[1])
        assert not stopping.done()
        assert len(barrier.snapshots) == 2
        assert barrier.snapshots[0]["buffers"]["value"]["value"] == {"items": [1]}
        assert barrier.snapshots[1]["buffers"]["value"]["value"] == {"items": [1, 2]}
        barrier.release[1].set()
        await asyncio.wait_for(stopping, 3)
        assert SnapshotStore(tmp_path).load_pools() == {"demo": pool.snapshot()}
        assert store.health["writes"] == 2
        assert barrier.max_active == 1
    finally:
        barrier.release_all()
        if stopping is None:
            await store.stop()
        elif not stopping.done():
            await stopping


@pytest.mark.asyncio
async def test_stop_during_batch_window_flushes_without_waiting_for_window(tmp_path):
    store = SnapshotStore(tmp_path, batch_window=60)
    pool = make_pool()
    store.start()
    store.enqueue(pool)
    await turn()
    await asyncio.wait_for(store.stop(), 3)
    assert SnapshotStore(tmp_path).load_pools() == {"demo": pool.snapshot()}


@pytest.mark.asyncio
async def test_dirty_map_is_bounded_including_in_flight_slot(tmp_path, monkeypatch):
    store = SnapshotStore(tmp_path, batch_window=0, max_dirty_pools=1)
    pool = make_pool()
    barrier = WriteBarrier(store, asyncio.get_running_loop())
    monkeypatch.setattr(store, "_write_file", barrier)
    store.start()
    store.enqueue(pool)
    try:
        await wait_event(barrier.started[0])
        for value in range(50):
            pool.buffers["value"].value = {"items": [value]}
            store.enqueue(pool)
        with pytest.raises(asyncio.QueueFull):
            store.enqueue(make_pool("other"))
        assert len(store._dirty) == 1
        assert store.health["dirty_pools"] == 1
        assert store.health["rejected_pools"] == 1
        barrier.release_all()
        await store.stop()
        assert store.load_pools() == {"demo": pool.snapshot()}
    finally:
        barrier.release_all()
        if store.health["running"]:
            await store.stop()


@pytest.mark.asyncio
async def test_stop_flushes_enqueues_before_start_and_is_idempotent(tmp_path):
    store = SnapshotStore(tmp_path, batch_window=0)
    pool = make_pool()
    store.enqueue(pool)
    await store.stop()
    await store.stop()
    assert store.load_pools() == {"demo": pool.snapshot()}
    with pytest.raises(RuntimeError, match="stopping"):
        store.enqueue(pool)
    store.start()
    pool.buffers["value"].value = {"items": [2]}
    store.enqueue(pool)
    await store.stop()
    assert store.load_pools() == {"demo": pool.snapshot()}


@pytest.mark.asyncio
async def test_exception_health_bounded_retry_and_recovery(tmp_path, monkeypatch):
    store = SnapshotStore(
        tmp_path, batch_window=0, retry_initial_delay=0.01,
        retry_max_delay=0.02, shutdown_timeout=3,
    )
    pool = make_pool()
    write = store._write_file
    waits = []
    failed = asyncio.Event()
    permit_retries = asyncio.Event()
    write_started = asyncio.Event()
    release_write = threading.Event()
    loop = asyncio.get_running_loop()
    clock = SimpleNamespace(monotonic=lambda: 1000.0, perf_counter=time.perf_counter, time=time.time)
    monkeypatch.setattr(persistence, "time", clock)
    wait_for_wakeup = store._wait_for_wakeup
    calls = []

    def flaky_write(snapshot):
        calls.append(deepcopy(snapshot))
        if len(calls) <= 3:
            raise OSError("injected storage failure")
        loop.call_soon_threadsafe(write_started.set)
        if not release_write.wait(5):
            raise AssertionError("Recovery writer was not released")
        return write(snapshot)

    async def observe_retry(delay=None):
        if delay is not None and delay > 0 and store.health["failures"]:
            entry = store._dirty[pool.pool_id]
            waits.append(entry.retry_delay)
            failed.set()
            await permit_retries.wait()
            entry.retry_at = 0.0
            return
        await wait_for_wakeup(delay)

    monkeypatch.setattr(store, "_write_file", flaky_write)
    monkeypatch.setattr(store, "_wait_for_wakeup", observe_retry)
    store.start()
    store.enqueue(pool)
    try:
        await wait_event(failed)
        assert not store.health["healthy"]
        assert "injected storage failure" in store.health["last_error"]
        assert store.health["running"]
        assert store.health["retrying_pools"] == 1
        assert store.health["dirty_pools"] == 1
        permit_retries.set()
        await wait_event(write_started)
        assert len(calls) == 4
        assert waits == [0.01, 0.02, 0.02]
        assert store.health["retries"] == 3
        release_write.set()
        await store.stop()
        assert store.health["healthy"]
        assert store.health["failures"] == 3
        assert store.health["dirty_pools"] == 0
        assert store.health["serialization_seconds"] >= 0
        assert store.health["write_seconds"] >= 0
        assert store.health["snapshot_copy_seconds"] > 0
        assert store.load_pools() == {"demo": pool.snapshot()}
        json.dumps(store.health, allow_nan=False)
    finally:
        permit_retries.set()
        release_write.set()
        if store.health["running"]:
            await store.stop()


@pytest.mark.asyncio
async def test_failing_pool_does_not_starve_other_pool(tmp_path, monkeypatch):
    store = SnapshotStore(
        tmp_path, batch_window=0, retry_initial_delay=0.01,
        retry_max_delay=0.02, shutdown_max_retries=3,
    )
    write = store._write_file
    good = make_pool("good")
    bad = make_pool("bad")

    def selective_failure(snapshot):
        if snapshot["pool_id"] == "bad":
            raise OSError("bad pool")
        return write(snapshot)

    monkeypatch.setattr(store, "_write_file", selective_failure)
    store.enqueue(bad)
    store.enqueue(good)
    with pytest.raises(SnapshotStoreError, match="after 3 attempts"):
        await asyncio.wait_for(store.stop(), 3)
    assert store.load_pools() == {"good": good.snapshot()}
    assert store.health["dirty_pools"] == 1
    assert not store.health["healthy"]


@pytest.mark.asyncio
async def test_snapshot_capture_failure_is_retried_and_reported(tmp_path, monkeypatch):
    store = SnapshotStore(tmp_path, batch_window=0, retry_initial_delay=0.01)
    pool = make_pool()
    snapshot = pool.snapshot
    calls = []

    def flaky_snapshot():
        calls.append(threading.get_ident())
        if len(calls) == 1:
            raise ValueError("injected copy failure")
        return snapshot()

    monkeypatch.setattr(pool, "snapshot", flaky_snapshot)
    store.enqueue(pool)
    await store.stop()
    assert calls == [threading.get_ident(), threading.get_ident()]
    assert store.health["failures"] == 1
    assert store.health["retries"] == 1
    assert "injected copy failure" in store.health["last_error"]
    assert store.health["healthy"]
    assert store.load_pools() == {"demo": snapshot()}


@pytest.mark.asyncio
async def test_permanent_failure_stop_is_bounded_and_retained_for_restart(tmp_path, monkeypatch):
    store = SnapshotStore(
        tmp_path, batch_window=0, retry_initial_delay=0.01,
        retry_max_delay=0.01, shutdown_timeout=3, shutdown_max_retries=2,
    )
    pool = make_pool()
    write = store._write_file
    calls = []

    def fail(snapshot):
        calls.append(snapshot)
        raise OSError("disk unavailable")

    monkeypatch.setattr(store, "_write_file", fail)
    store.enqueue(pool)
    with pytest.raises(SnapshotStoreError, match="after 2 attempts"):
        await asyncio.wait_for(store.stop(), 3)
    assert len(calls) == 2
    assert store.health["dirty_pools"] == 1
    assert not store.health["healthy"]
    assert not store.health["running"]
    with pytest.raises(SnapshotStoreError):
        await store.stop()
    monkeypatch.setattr(store, "_write_file", write)
    store.start()
    await store.stop()
    assert store.health["healthy"]
    assert store.health["dirty_pools"] == 0
    assert store.load_pools() == {"demo": pool.snapshot()}


@pytest.mark.asyncio
async def test_timeout_does_not_cancel_active_io_or_allow_second_writer(tmp_path, monkeypatch):
    store = SnapshotStore(tmp_path, batch_window=0, shutdown_timeout=0.05)
    pool = make_pool()
    barrier = WriteBarrier(store, asyncio.get_running_loop())
    monkeypatch.setattr(store, "_write_file", barrier)
    store.start()
    store.enqueue(pool)
    try:
        await wait_event(barrier.started[0])
        pool.buffers["value"].value["items"].append(2)
        store.enqueue(pool)
        with pytest.raises(asyncio.TimeoutError, match="retained"):
            await store.stop()
        assert store.health["in_flight"]
        assert store.health["running"]
        assert not store.health["healthy"]
        assert not store._saver_task.cancelled()
        with pytest.raises(RuntimeError):
            store.start()
        with pytest.raises(RuntimeError):
            store.enqueue(pool)
        with pytest.raises(RuntimeError):
            store.save_pool(pool)
        with pytest.raises(RuntimeError, match="owns pending I/O"):
            await asyncio.get_running_loop().run_in_executor(None, store.save_pool, pool)
        assert len(barrier.snapshots) == 1
        barrier.release_all()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(store._saver_task), 3)
        assert store.health["dirty_pools"] == 1
        assert not store.health["in_flight"]
        # The fault phase used a tiny injected deadline; recovery includes real
        # thread startup and filesystem work and needs a bounded realistic wait.
        store._shutdown_timeout = 3
        store.start()
        await store.stop()
        assert store.load_pools() == {"demo": pool.snapshot()}
        assert barrier.max_active == 1
    finally:
        barrier.release_all()
        if store.health["running"]:
            try:
                await asyncio.wait_for(asyncio.shield(store._saver_task), 3)
            except asyncio.TimeoutError:
                pass


@pytest.mark.asyncio
async def test_cancelling_stop_waiter_does_not_cancel_saver(tmp_path, monkeypatch):
    store = SnapshotStore(tmp_path, batch_window=0)
    pool = make_pool()
    barrier = WriteBarrier(store, asyncio.get_running_loop())
    monkeypatch.setattr(store, "_write_file", barrier)
    store.start()
    store.enqueue(pool)
    stopping = None
    try:
        await wait_event(barrier.started[0])
        stopping = asyncio.create_task(store.stop())
        await turn()
        stopping.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stopping
        assert store.health["in_flight"]
        assert not store._saver_task.cancelled()
        barrier.release_all()
        await store.stop()
        assert store.load_pools() == {"demo": pool.snapshot()}
        assert barrier.max_active == 1
    finally:
        barrier.release_all()
        if store.health["running"]:
            await store.stop()


@pytest.mark.asyncio
async def test_synchronous_write_is_rejected_on_event_loop(tmp_path):
    store = SnapshotStore(tmp_path)
    with pytest.raises(RuntimeError, match="enqueue"):
        store.save_pool(make_pool())
    with pytest.raises(RuntimeError, match="enqueue"):
        store.delete_pool("demo")
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_idle_saver_releases_saved_live_pool(tmp_path, monkeypatch):
    store = SnapshotStore(tmp_path, batch_window=0)
    pool = make_pool()
    pool_ref = weakref.ref(pool)
    idle = asyncio.Event()
    wait_for_wakeup = store._wait_for_wakeup

    async def observe_idle(delay=None):
        if delay is None and store.health["writes"]:
            idle.set()
        await wait_for_wakeup(delay)

    monkeypatch.setattr(store, "_wait_for_wakeup", observe_idle)
    store.start()
    store.enqueue(pool)
    try:
        await wait_event(idle)
        del pool
        assert pool_ref() is None
        assert store.health["dirty_pools"] == 0
    finally:
        await store.stop()


@pytest.mark.asyncio
async def test_executor_submission_error_is_retried(tmp_path, monkeypatch):
    store = SnapshotStore(tmp_path, batch_window=0, retry_initial_delay=0.01)
    pool = make_pool()
    loop = asyncio.get_running_loop()
    submit = loop.run_in_executor
    attempts = []

    def flaky_submit(executor, function, *args):
        attempts.append(args[0])
        if len(attempts) == 1:
            raise RuntimeError("injected executor failure")
        return submit(executor, function, *args)

    monkeypatch.setattr(loop, "run_in_executor", flaky_submit)
    store.enqueue(pool)
    await store.stop()
    assert len(attempts) == 2
    assert all(isinstance(snapshot, dict) for snapshot in attempts)
    assert store.health["failures"] == 1
    assert store.health["retries"] == 1
    assert store.health["healthy"]
    assert store.load_pools() == {"demo": pool.snapshot()}


@pytest.mark.asyncio
async def test_serialization_error_is_retried(tmp_path, monkeypatch):
    store = SnapshotStore(tmp_path, batch_window=0, retry_initial_delay=0.01)
    pool = make_pool()
    serialize = json.dumps
    attempts = []

    def flaky_serialize(snapshot, *args, **kwargs):
        attempts.append(snapshot)
        if len(attempts) == 1:
            raise ValueError("injected serialization failure")
        return serialize(snapshot, *args, **kwargs)

    monkeypatch.setattr("latzero_server.persistence.json.dumps", flaky_serialize)
    store.enqueue(pool)
    await store.stop()
    assert len(attempts) == 2
    assert store.health["failures"] == 1
    assert store.health["retries"] == 1
    assert "serialization failure" in store.health["last_error"]
    assert store.health["healthy"]
    assert not list(tmp_path.glob("*.tmp"))
    assert store.load_pools() == {"demo": pool.snapshot()}


def test_replace_failure_preserves_existing_file_and_cleans_unique_temp(tmp_path, monkeypatch):
    store = SnapshotStore(tmp_path)
    pool = make_pool()
    store.save_pool(pool)
    path = store._path_for_pool("demo")
    original = path.read_bytes()
    pool.buffers["value"].value["items"].append(2)
    replace = Path.replace
    temps = []

    def failing_replace(temp, target):
        if Path(target) == path:
            temps.append(temp)
            raise OSError("injected atomic replace failure")
        return replace(temp, target)

    monkeypatch.setattr(Path, "replace", failing_replace)
    for _ in range(2):
        with pytest.raises(OSError, match="replace failure"):
            store.save_pool(pool)
        assert path.read_bytes() == original
        assert not list(tmp_path.glob("*.tmp"))
    assert len({temp.name for temp in temps}) == 2
    assert not store.health["healthy"]
    monkeypatch.setattr(Path, "replace", replace)
    store.save_pool(pool)
    assert store.health["healthy"]
    assert store.load_pools() == {"demo": pool.snapshot()}
