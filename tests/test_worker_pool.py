"""Deterministic ordering, resource-bound, and lifecycle tests for dispatch."""

import asyncio
import gc
import json
import logging
import weakref
from types import SimpleNamespace

import pytest

from latzero_server import worker_pool
from latzero_server.worker_pool import AutoScalingWorkerPool, LoadPredictor


def session(name="client"):
    return SimpleNamespace(
        name=name,
        closed=False,
        closing=False,
        generation=0,
        pool_id=None,
        active_requests=set(),
    )


def make_pool(dispatch, **options):
    limits = dict(
        min_workers=1,
        max_workers=4,
        controller_interval=3600,
        max_session_messages=16,
        max_session_bytes=1024,
        max_queue_messages=64,
        max_queue_bytes=4096,
        control_reserve_messages=2,
        control_reserve_bytes=64,
        dispatch_slice=2,
        dispatch_slice_seconds=1,
        shutdown_timeout=0.05,
        max_sessions=8,
    )
    limits.update(options)
    return AutoScalingWorkerPool(dispatch, **limits)


def test_fifo_with_await_and_no_same_session_overlap():
    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()
        starts, finishes = [], []
        active = 0
        peak = 0

        async def dispatch(client, message):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            starts.append(message["seq"])
            try:
                if message["seq"] == 0:
                    entered.set()
                    await release.wait()
                await asyncio.sleep(0)
                finishes.append(message["seq"])
            finally:
                active -= 1

        pool = make_pool(dispatch, min_workers=4)
        client = session()
        await pool.start()
        try:
            for seq in range(12):
                assert await pool.submit(client, {"seq": seq}, byte_size=2)
            await asyncio.wait_for(entered.wait(), 1)
            assert starts == [0]
            assert pool.stats.queue_depth == 12
            assert pool.stats.queue_bytes == 24
            assert pool.stats.active_dispatches == 1
            barrier = asyncio.create_task(pool.join())
            await asyncio.sleep(0)
            assert not barrier.done()
            release.set()
            await asyncio.wait_for(barrier, 1)
            assert starts == finishes == list(range(12))
            assert peak == 1
            assert pool.stats.queue_depth == pool.stats.queue_bytes == 0
            assert not pool._mailboxes
            assert not pool._ready
        finally:
            release.set()
            await pool.stop()

    asyncio.run(scenario())


def test_quiet_session_runs_after_bounded_hot_slice():
    async def scenario():
        order = []

        async def dispatch(client, message):
            order.append((client.name, message["seq"]))

        pool = make_pool(dispatch, max_workers=1)
        hot, quiet = session("hot"), session("quiet")
        await pool.start()
        try:
            for seq in range(8):
                assert await pool.submit(hot, {"seq": seq}, byte_size=1)
            assert await pool.submit(quiet, {"seq": 0}, byte_size=1)
            await asyncio.wait_for(pool.join(), 1)
            assert order[:3] == [("hot", 0), ("hot", 1), ("quiet", 0)]
            assert [seq for name, seq in order if name == "hot"] == list(range(8))
        finally:
            await pool.stop()

    asyncio.run(scenario())


def test_hot_slice_yields_to_other_event_loop_work():
    async def scenario():
        processed, observed = [], []

        async def dispatch(client, message):
            processed.append(message["seq"])
            if message["seq"] == 0:
                asyncio.get_running_loop().call_soon(
                    lambda: observed.append(len(processed))
                )

        pool = make_pool(dispatch, max_workers=1)
        client = session()
        await pool.start()
        try:
            for seq in range(8):
                assert await pool.submit(client, {"seq": seq}, 1)
            await asyncio.wait_for(pool.join(), 1)
            assert observed == [2]
            assert processed == list(range(8))
        finally:
            await pool.stop()

    asyncio.run(scenario())


def test_stalled_hot_session_does_not_occupy_other_workers():
    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()
        quiet_done = asyncio.Event()
        hot_active = 0

        async def dispatch(client, message):
            nonlocal hot_active
            if client.name == "quiet":
                quiet_done.set()
                return
            hot_active += 1
            try:
                assert hot_active == 1
                entered.set()
                await release.wait()
            finally:
                hot_active -= 1

        pool = make_pool(dispatch, min_workers=3, max_workers=3)
        hot, quiet = session("hot"), session("quiet")
        await pool.start()
        try:
            for seq in range(6):
                assert await pool.submit(hot, {"seq": seq}, byte_size=1)
            await asyncio.wait_for(entered.wait(), 1)
            assert await pool.submit(quiet, {}, byte_size=1)
            await asyncio.wait_for(quiet_done.wait(), 1)
            assert hot_active == 1
            assert pool.stats.active_dispatches == 1
            release.set()
            await asyncio.wait_for(pool.join(), 1)
            assert pool.stats.dispatch_failures == 0
        finally:
            release.set()
            await pool.stop()

    asyncio.run(scenario())


def test_time_slice_and_timing_metrics(monkeypatch):
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(
        worker_pool,
        "time",
        SimpleNamespace(monotonic=lambda: clock.now, time=lambda: clock.now),
    )

    async def scenario():
        order = []

        async def dispatch(client, message):
            order.append(client.name)
            clock.now += 0.5

        pool = make_pool(
            dispatch, max_workers=1, dispatch_slice=16, dispatch_slice_seconds=0.5
        )
        hot, quiet = session("hot"), session("quiet")
        await pool.start()
        try:
            for _ in range(4):
                assert await pool.submit(hot, {}, byte_size=1)
            assert await pool.submit(quiet, {}, byte_size=1)
            await asyncio.wait_for(pool.join(), 1)
            assert order == ["hot", "quiet", "hot", "hot", "hot"]
            assert pool.stats.service_seconds == 2.5
            assert pool.stats.max_service_seconds == 0.5
            assert pool.stats.queue_wait_seconds == 5.0
            assert pool.stats.max_queue_wait_seconds == 2.0
        finally:
            await pool.stop()

    asyncio.run(scenario())


def test_submit_does_not_yield_and_estimates_utf8_bytes():
    async def scenario():
        async def dispatch(client, message):
            pass

        message = {"payload": "\u00e9"}
        size = len(json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        pool = make_pool(
            dispatch,
            max_session_messages=1,
            max_session_bytes=size,
            max_queue_messages=1,
            max_queue_bytes=size,
        )
        client = session()
        await pool.start()
        try:
            for expected in (True, False):
                submission = pool.submit(client, message)
                try:
                    with pytest.raises(StopIteration) as result:
                        submission.send(None)
                    assert result.value.value is expected
                finally:
                    submission.close()
            assert pool.stats.queue_bytes == size
            assert pool.stats.rejected_messages == 1
            await asyncio.wait_for(pool.join(), 1)
        finally:
            await pool.stop()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "options, first_size, second_size",
    [
        ({"max_session_messages": 1}, 1, 1),
        ({"max_session_bytes": 3}, 3, 1),
        ({"max_queue_messages": 1}, 1, 1),
        ({"max_queue_bytes": 3}, 3, 1),
    ],
)
def test_limits_include_active_dispatch(options, first_size, second_size):
    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()

        async def dispatch(client, message):
            entered.set()
            await release.wait()

        pool = make_pool(dispatch, **options)
        client = session()
        await pool.start()
        try:
            assert await pool.submit(client, {}, byte_size=first_size)
            await asyncio.wait_for(entered.wait(), 1)
            assert not await pool.submit(client, {}, byte_size=second_size)
            assert pool.stats.queue_depth == 1
            assert pool.stats.queue_bytes == first_size
            release.set()
            await asyncio.wait_for(pool.join(), 1)
            assert await pool.submit(client, {}, byte_size=second_size)
            await asyncio.wait_for(pool.join(), 1)
        finally:
            release.set()
            await pool.stop()

    asyncio.run(scenario())


def test_global_limits_across_sessions():
    async def scenario():
        async def dispatch(client, message):
            pass

        pool = make_pool(dispatch, max_queue_messages=2, max_queue_bytes=4)
        first, second, third = session("first"), session("second"), session("third")
        await pool.start()
        try:
            assert await pool.submit(first, {}, byte_size=3)
            assert not await pool.submit(second, {}, byte_size=2)
            assert id(second) not in pool._mailboxes
            assert await pool.submit(second, {}, byte_size=1)
            assert not await pool.submit(third, {}, byte_size=0)
            assert pool.stats.queue_depth == 2
            assert pool.stats.queue_bytes == 4
            await asyncio.wait_for(pool.join(), 1)
            assert not pool._mailboxes
        finally:
            await pool.stop()

    asyncio.run(scenario())


def test_app_result_reserve_is_bounded_and_preserves_fifo():
    async def scenario():
        dispatched = []

        async def dispatch(client, message):
            dispatched.append(message["seq"])

        pool = make_pool(
            dispatch,
            max_session_messages=2,
            max_session_bytes=4,
            max_queue_messages=2,
            max_queue_bytes=4,
            control_reserve_messages=1,
            control_reserve_bytes=2,
        )
        client = session()
        await pool.start()
        try:
            for seq in range(2):
                assert await pool.submit(client, {"type": "set_buffer", "seq": seq}, 2)
            assert not await pool.submit(client, {"type": "hello"}, 1)
            assert not await pool.submit(client, {"type": "app_result", "seq": 2}, 3)
            assert await pool.submit(client, {"type": "app_result", "seq": 2}, 2)
            assert not await pool.submit(client, {"type": "app_result", "seq": 3}, 0)
            assert not await pool.submit(session("other"), {"type": "app_result"}, 0)
            assert pool.stats.queue_depth == 3
            assert pool.stats.queue_bytes == 6
            await asyncio.wait_for(pool.join(), 1)
            assert dispatched == [0, 1, 2]
            assert pool.stats.rejected_messages == 4
        finally:
            await pool.stop()

    asyncio.run(scenario())


def test_control_can_enter_from_another_session_when_regular_queue_is_full():
    async def scenario():
        dispatched = []

        async def dispatch(client, message):
            dispatched.append(client.name)

        pool = make_pool(
            dispatch,
            max_queue_messages=1,
            max_queue_bytes=2,
            control_reserve_messages=1,
            control_reserve_bytes=2,
        )
        await pool.start()
        try:
            assert await pool.submit(session("request"), {"type": "call_app"}, 2)
            assert not await pool.submit(session("ordinary"), {"type": "hello"}, 1)
            assert await pool.submit(session("result"), {"type": "app_result"}, 2)
            await asyncio.wait_for(pool.join(), 1)
            assert dispatched == ["request", "result"]
        finally:
            await pool.stop()

    asyncio.run(scenario())


@pytest.mark.parametrize("state", ["closed", "closing"])
def test_closed_sessions_are_discarded_without_explicit_invalidation(state):
    async def scenario():
        dispatched = []

        async def dispatch(client, message):
            dispatched.append(message)

        pool = make_pool(dispatch)
        client = session()
        await pool.start()
        try:
            for _ in range(3):
                assert await pool.submit(client, {}, 2)
            setattr(client, state, True)
            assert not await pool.submit(client, {"type": "app_result"}, 1)
            await asyncio.wait_for(pool.join(), 1)
            assert not dispatched
            assert pool.stats.discarded_messages == 3
            assert pool.stats.queue_depth == pool.stats.queue_bytes == 0
            assert not pool._mailboxes
        finally:
            await pool.stop()

    asyncio.run(scenario())


def test_invalidation_purges_queued_but_keeps_active_accounted():
    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()
        dispatched = []

        async def dispatch(client, message):
            dispatched.append(message["seq"])
            entered.set()
            await release.wait()

        pool = make_pool(dispatch)
        client = session()
        await pool.start()
        try:
            for seq in range(3):
                assert await pool.submit(client, {"seq": seq}, 2)
            await asyncio.wait_for(entered.wait(), 1)
            client.closed = True
            pool.invalidate(client)
            pool.invalidate(client)
            assert pool.stats.queue_depth == 1
            assert pool.stats.queue_bytes == 2
            assert pool.stats.discarded_messages == 2
            assert not pool._ready
            barrier = asyncio.create_task(pool.join())
            await asyncio.sleep(0)
            assert not barrier.done()
            release.set()
            await asyncio.wait_for(barrier, 1)
            assert dispatched == [0]
            assert not pool._mailboxes
            assert pool.stats.queue_depth == pool.stats.queue_bytes == 0
        finally:
            release.set()
            await pool.stop()

    asyncio.run(scenario())


def test_idle_mailbox_and_ready_tokens_are_removed_on_invalidation():
    async def scenario():
        async def dispatch(client, message):
            pass

        pool = make_pool(dispatch, max_sessions=1)
        await pool.start()
        try:
            for _ in range(100):
                client = session()
                assert await pool.submit(client, {}, 1)
                client.closed = True
                pool.invalidate(client)
                assert not pool._mailboxes
                assert not pool._ready
            assert pool.stats.discarded_messages == 100
            assert await pool.submit(session(), {}, 1)
            assert not await pool.submit(session(), {}, 1)
            await asyncio.wait_for(pool.join(), 1)
            assert await pool.submit(session(), {}, 1)
            await asyncio.wait_for(pool.join(), 1)
            assert not pool._mailboxes
        finally:
            await pool.stop()

    asyncio.run(scenario())


@pytest.mark.asyncio
async def test_idle_workers_release_session_and_message_references():
    class TrackedSession(SimpleNamespace):
        pass

    class TrackedValue:
        pass

    async def scenario():
        async def dispatch(client, message):
            pass

        pool = make_pool(dispatch)
        client = TrackedSession(**vars(session()))
        value = TrackedValue()
        message = {"payload": value}
        client_ref, value_ref = weakref.ref(client), weakref.ref(value)
        await pool.start()
        try:
            assert await pool.submit(client, message, 1)
            await asyncio.wait_for(pool.join(), 1)
            del client, value, message
            gc.collect()
            assert client_ref() is None
            assert value_ref() is None
            assert not pool._mailboxes
        finally:
            await pool.stop()

    await scenario()


def test_fifo_pool_transitions_do_not_drop_pipelined_messages():
    async def scenario():
        dispatched = []

        async def dispatch(client, message):
            kind = message["type"]
            if kind in ("join_pool", "switch_pool"):
                client.pool_id = message["pool"]
                client.generation += 1
                await asyncio.sleep(0)
            dispatched.append((kind, client.pool_id, client.generation))

        pool = make_pool(dispatch, min_workers=3)
        client = session()
        await pool.start()
        try:
            messages = [
                {"type": "join_pool", "pool": "first"},
                {"type": "set_buffer", "pool": "first"},
                {"type": "switch_pool", "pool": "second"},
                {"type": "get_buffer", "pool": None},
            ]
            for message in messages:
                assert await pool.submit(client, message, 1)
            assert all(item.generation == 0 for item in pool._mailboxes[id(client)].messages)
            await asyncio.wait_for(pool.join(), 1)
            assert dispatched == [
                ("join_pool", "first", 1),
                ("set_buffer", "first", 1),
                ("switch_pool", "second", 2),
                ("get_buffer", "second", 2),
            ]
            assert pool.stats.discarded_messages == 0
        finally:
            await pool.stop()

    asyncio.run(scenario())


def test_dispatch_failures_are_logged_and_worker_continues(caplog):
    async def scenario():
        dispatched = []

        async def dispatch(client, message):
            dispatched.append(message["seq"])
            if message["seq"] == 0:
                raise RuntimeError("handler failed")

        pool = make_pool(dispatch)
        client = session()
        await pool.start()
        try:
            for seq in range(2):
                assert await pool.submit(client, {"seq": seq}, 1)
            await asyncio.wait_for(pool.join(), 1)
            assert dispatched == [0, 1]
            assert pool.stats.messages_processed == 2
            assert pool.stats.dispatch_failures == 1
            assert pool.stats.active_workers == 1
            assert pool.stats.queue_depth == pool.stats.queue_bytes == 0
        finally:
            await pool.stop()

    with caplog.at_level(logging.ERROR, logger=worker_pool.__name__):
        asyncio.run(scenario())
    assert "Message dispatch failed" in caplog.text
    assert "handler failed" in caplog.text


def test_shutdown_cancels_stalled_handler_and_supports_restart():
    async def scenario():
        entered = asyncio.Event()
        cancelled = asyncio.Event()
        never = asyncio.Event()
        dispatched = []

        async def dispatch(client, message):
            if message.get("stall"):
                entered.set()
                try:
                    await never.wait()
                finally:
                    cancelled.set()
            else:
                dispatched.append(message)

        pool = make_pool(dispatch, shutdown_timeout=0.01)
        client = session()
        assert not await pool.submit(client, {}, 1)
        await pool.stop()
        await pool.start()
        await pool.start()
        try:
            assert pool.stats.active_workers == 1
            assert await pool.submit(client, {"stall": True}, 2)
            assert await pool.submit(client, {"queued": True}, 2)
            await asyncio.wait_for(entered.wait(), 1)
            await asyncio.wait_for(asyncio.gather(pool.stop(), pool.stop()), 1)
            assert cancelled.is_set()
            assert not dispatched
            assert not await pool.submit(client, {}, 1)
            assert pool.stats.active_workers == 0
            assert pool.stats.queue_depth == pool.stats.queue_bytes == 0
            assert not pool._mailboxes
            assert not pool._ready
            await asyncio.wait_for(pool.join(), 1)
            await pool.stop()
            await pool.start()
            assert await pool.submit(client, {"restarted": True}, 1)
            await asyncio.wait_for(pool.join(), 1)
            assert dispatched == [{"restarted": True}]
        finally:
            never.set()
            await pool.stop()

    asyncio.run(scenario())


def test_shutdown_is_finite_when_handler_suppresses_cancellation():
    async def scenario():
        entered = asyncio.Event()
        cancellation_caught = asyncio.Event()
        release = asyncio.Event()
        never = asyncio.Event()
        worker_exited = asyncio.Event()
        dispatched = []

        async def dispatch(client, message):
            if message.get("stall"):
                entered.set()
                try:
                    await never.wait()
                except asyncio.CancelledError:
                    cancellation_caught.set()
                    await release.wait()
            else:
                dispatched.append(message)

        pool = make_pool(dispatch, max_workers=1, shutdown_timeout=0)
        client = session()
        await pool.start()
        pool._workers[0].add_done_callback(lambda task: worker_exited.set())
        try:
            assert await pool.submit(client, {"stall": True}, 2)
            assert await pool.submit(client, {"queued": True}, 2)
            await asyncio.wait_for(entered.wait(), 1)
            await asyncio.wait_for(pool.stop(), 1)
            await asyncio.wait_for(cancellation_caught.wait(), 1)
            assert pool.stats.active_workers == 1
            assert pool.stats.queue_depth == 1
            assert pool.stats.queue_bytes == 2
            assert pool.stats.discarded_messages == 1
            assert not pool._ready
            with pytest.raises(RuntimeError, match="have not stopped"):
                await pool.start()
            release.set()
            await asyncio.wait_for(worker_exited.wait(), 1)
            await asyncio.wait_for(pool.join(), 1)
            assert pool.stats.queue_depth == pool.stats.queue_bytes == 0
            assert not pool._mailboxes
            assert not dispatched
            await pool.start()
            assert await pool.submit(client, {"restarted": True}, 1)
            await asyncio.wait_for(pool.join(), 1)
            assert dispatched == [{"restarted": True}]
        finally:
            release.set()
            await pool.stop()

    asyncio.run(scenario())


def test_cancelled_stop_waiter_does_not_cancel_shutdown_or_race_restart():
    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()
        dispatched = []

        async def dispatch(client, message):
            entered.set()
            await release.wait()
            dispatched.append(message)

        pool = make_pool(dispatch, shutdown_timeout=1)
        client = session()
        await pool.start()
        try:
            assert await pool.submit(client, {"first": True}, 1)
            await asyncio.wait_for(entered.wait(), 1)
            stopping = asyncio.create_task(pool.stop())
            await asyncio.sleep(0)
            stopping.cancel()
            with pytest.raises(asyncio.CancelledError):
                await stopping
            assert pool._stop_task is not None
            assert not pool._stop_task.done()
            assert not await pool.submit(client, {}, 1)
            restarting = asyncio.create_task(pool.start())
            await asyncio.sleep(0)
            assert not restarting.done()
            release.set()
            await asyncio.wait_for(restarting, 1)
            assert pool.stats.active_workers == 1
            assert await pool.submit(client, {"second": True}, 1)
            await asyncio.wait_for(pool.join(), 1)
            assert dispatched == [{"first": True}, {"second": True}]
        finally:
            release.set()
            await pool.stop()

    asyncio.run(scenario())


def test_stop_seals_admission_then_drains_accepted_messages():
    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()
        dispatched = []

        async def dispatch(client, message):
            if message["seq"] == 0:
                entered.set()
                await release.wait()
            dispatched.append(message["seq"])

        pool = make_pool(dispatch, shutdown_timeout=1)
        client = session()
        await pool.start()
        try:
            for seq in range(3):
                assert await pool.submit(client, {"seq": seq}, 1)
            await asyncio.wait_for(entered.wait(), 1)
            stopping = asyncio.create_task(pool.stop())
            await asyncio.sleep(0)
            assert not await pool.submit(client, {"seq": 3}, 1)
            assert not await pool.submit(client, {"type": "app_result"}, 1)
            release.set()
            await asyncio.wait_for(stopping, 1)
            assert dispatched == [0, 1, 2]
            assert pool.stats.discarded_messages == 0
        finally:
            release.set()
            await pool.stop()

    asyncio.run(scenario())


def test_repeated_retirements_respect_minimum_without_cancelling_active_work():
    async def scenario():
        all_entered = asyncio.Event()
        release = asyncio.Event()
        retired = asyncio.Event()
        entered = 0
        completed = []
        exited = 0

        async def dispatch(client, message):
            nonlocal entered
            entered += 1
            if entered == 5:
                all_entered.set()
            await release.wait()
            completed.append(client.name)

        def worker_exited(task):
            nonlocal exited
            exited += 1
            if exited == 3:
                retired.set()

        pool = make_pool(dispatch, min_workers=2, max_workers=5)
        await pool.start()
        try:
            pool._scale_up(100, "test maximum")
            assert pool.stats.active_workers == 5
            for task in pool._workers:
                task.add_done_callback(worker_exited)
            for index in range(5):
                assert await pool.submit(session(str(index)), {}, 1)
            await asyncio.wait_for(all_entered.wait(), 1)
            for _ in range(20):
                pool._scale_down(100, "test repeated retirement")
            assert pool.stats.pending_retirements == 3
            assert pool.stats.active_workers == 5
            assert not completed
            release.set()
            await asyncio.wait_for(retired.wait(), 1)
            await asyncio.wait_for(pool.join(), 1)
            assert sorted(completed) == ["0", "1", "2", "3", "4"]
            assert pool.stats.active_workers == 2
            assert pool.stats.pending_retirements == 0
            for _ in range(20):
                pool._scale_down(100, "already at minimum")
            assert pool.stats.active_workers == 2
            assert pool.stats.pending_retirements == 0
        finally:
            release.set()
            await pool.stop()

    asyncio.run(scenario())


def test_scale_up_can_revoke_pending_retirements():
    async def scenario():
        async def dispatch(client, message):
            pass

        pool = make_pool(dispatch, min_workers=2, max_workers=5)
        await pool.start()
        try:
            pool._scale_up(3, "expand")
            pool._scale_down(3, "retire")
            assert pool.stats.pending_retirements == 3
            pool._scale_up(100, "restore")
            assert pool.stats.active_workers == 5
            assert pool.stats.pending_retirements == 0
            assert len(pool.stats.scale_events) == 3
            assert pool.stats.scale_events[0].new_count == 5
        finally:
            await pool.stop()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "options",
    [
        {"min_workers": 0},
        {"min_workers": 3, "max_workers": 2},
        {"max_workers": True},
        {"scale_up_threshold": 0},
        {"scale_down_threshold": 0},
        {"controller_interval": 0},
        {"scale_down_hold": -1},
        {"max_step_up": 0},
        {"burst_size": 0},
        {"emergency_multiplier": float("inf")},
        {"max_session_messages": 0},
        {"max_session_bytes": -1},
        {"max_queue_messages": 1.5},
        {"max_queue_bytes": 0},
        {"control_reserve_messages": -1},
        {"control_reserve_bytes": True},
        {"dispatch_slice": 0},
        {"dispatch_slice_seconds": float("nan")},
        {"shutdown_timeout": -1},
        {"max_sessions": 0},
    ],
)
def test_invalid_configuration_is_rejected(options):
    async def dispatch(client, message):
        pass

    with pytest.raises(ValueError):
        make_pool(dispatch, **options)


@pytest.mark.parametrize("byte_size", [-1, True, 1.5, "1"])
def test_invalid_byte_size_is_rejected(byte_size):
    async def scenario():
        async def dispatch(client, message):
            pass

        pool = make_pool(dispatch)
        await pool.start()
        try:
            assert not await pool.submit(session(), {}, byte_size)
            assert pool.stats.rejected_messages == 1
            assert not pool._mailboxes
        finally:
            await pool.stop()

    asyncio.run(scenario())


def test_predictor_interface_is_preserved():
    predictor = LoadPredictor()
    assert predictor.predict() == (0.0, 0.0)
    predictor.record(2.0)
    assert predictor.predict() == (2.0, 0.0)
