"""
Auto-scaling asyncio worker pool for LatZero message dispatch.

Readers admit messages without yielding into bounded per-session FIFOs.
A ready-session FIFO assigns one worker per session, with bounded dispatch
slices so hot sessions cannot monopolize the event loop. Pending budgets
include the message currently being dispatched.

Scaling algorithm
-----------------
The controller runs every 0.25 s (4×/s) and applies THREE layers:

  1. Emergency burst  — queue > 5× threshold  → jump immediately to
                        min(current + burst_size, max_workers)
  2. Proportional    — queue between threshold and 5×  → add workers
                        proportional to queue/threshold, capped by
                        max_step_up per tick.
  3. Predictive      — linear regression on 15-s rolling window predicts
                        depth 5 s ahead.  If forecast > threshold, pre-scale
                        proportionally (confidence >= 0.3 required, not 0.6).

Scale-down: queue < scale_down_threshold sustained for scale_down_hold
            seconds → drain one worker at a time (conservative).
"""

import asyncio
import json
import logging
import math
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Deque, Dict, List, Optional, Tuple


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@dataclass
class ScaleEvent:
    """Record of a single auto-scaling decision."""
    timestamp: float
    direction: str          # "up" or "down"
    reason: str             # human-readable rationale
    old_count: int
    new_count: int


@dataclass
class WorkerPoolStats:
    """Live metrics snapshot exposed to the TUI and dashboard."""
    active_workers: int = 0
    max_workers: int = 1280
    min_workers: int = 4
    queue_depth: int = 0
    messages_processed: int = 0
    messages_per_sec: float = 0.0
    scale_events: List[ScaleEvent] = field(default_factory=list)
    predicted_depth: float = 0.0
    prediction_confidence: float = 0.0   # 0.0–1.0
    low_depth_since: Optional[float] = None
    queue_bytes: int = 0
    active_dispatches: int = 0
    pending_retirements: int = 0
    rejected_messages: int = 0
    discarded_messages: int = 0
    dispatch_failures: int = 0
    queue_wait_seconds: float = 0.0       # cumulative, for dispatched messages
    service_seconds: float = 0.0          # cumulative, including failed dispatches
    max_queue_wait_seconds: float = 0.0
    max_service_seconds: float = 0.0


# ---------------------------------------------------------------------------
# Load predictor
# ---------------------------------------------------------------------------

class LoadPredictor:
    """
    Lightweight linear regression on queue-depth history.

    Maintains a rolling window of (timestamp, queue_depth) samples.
    Provides a forecast of the queue depth N seconds in the future,
    along with a confidence score based on r².

    Tuned for responsiveness: 15-second window, 5-second horizon,
    minimum confidence threshold dropped to 0.3 so it activates quickly
    under bursty load rather than waiting for a smooth trend.
    """

    def __init__(self, window_seconds: float = 15.0, forecast_horizon: float = 5.0):
        self._window = window_seconds
        self._horizon = forecast_horizon
        self._samples: Deque[Tuple[float, float]] = deque()

    def record(self, depth: float) -> None:
        """Add a new sample for the current timestamp."""
        now = time.monotonic()
        self._samples.append((now, depth))
        # Prune samples older than the window
        cutoff = now - self._window
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.popleft()

    def predict(self) -> Tuple[float, float]:
        """
        Return (predicted_depth_at_horizon, confidence).

        Uses ordinary least squares on (time, depth) pairs.
        confidence = r² of the fit, clamped to [0, 1].
        If fewer than 3 samples, returns (current_depth, 0.0).
        """
        if len(self._samples) < 3:
            current = self._samples[-1][1] if self._samples else 0.0
            return max(0.0, current), 0.0

        xs = [s[0] for s in self._samples]
        ys = [s[1] for s in self._samples]
        n = len(xs)

        # Mean
        x_mean = sum(xs) / n
        y_mean = sum(ys) / n

        # Slope and intercept
        ss_xx = sum((x - x_mean) ** 2 for x in xs)
        ss_xy = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))

        if ss_xx == 0:
            return max(0.0, y_mean), 0.0

        slope = ss_xy / ss_xx
        intercept = y_mean - slope * x_mean

        # Forecast
        future_x = (xs[-1] if xs else time.monotonic()) + self._horizon
        predicted = slope * future_x + intercept

        # r² confidence
        ss_res = sum((y - (slope * x + intercept)) ** 2 for x, y in zip(xs, ys))
        ss_tot = sum((y - y_mean) ** 2 for y in ys)
        r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
        confidence = max(0.0, min(1.0, r2))

        return max(0.0, predicted), confidence


# ---------------------------------------------------------------------------
# Worker Pool
# ---------------------------------------------------------------------------

@dataclass
class _QueuedMessage:
    message: dict
    byte_size: int
    generation: int
    submitted_at: float


@dataclass
class _SessionMailbox:
    session: Any
    messages: Deque[_QueuedMessage] = field(default_factory=deque)
    pending_messages: int = 0
    pending_bytes: int = 0
    active: bool = False


class AutoScalingWorkerPool:
    """
    Bounded per-session FIFOs drained by a self-scaling pool of worker Tasks.

    Workers are ephemeral asyncio Tasks — zero process spawn overhead.
    Readers never wait for queue capacity: submission accepts or rejects
    synchronously. Only app_result messages can use the additive control
    reserve, without bypassing their session's FIFO.

    Scaling layers (applied every controller_interval seconds):
      1. Emergency burst  — depth > 5 × threshold → add burst_size workers
      2. Proportional     — depth > threshold → add ceil(depth/threshold) workers
      3. Predictive       — forecast > threshold (conf >= 0.3) → pre-scale proportionally
      4. Gradual drain    — depth < down_threshold sustained → -1 worker
    """

    def __init__(
        self,
        dispatch_fn: Callable[..., Coroutine],
        min_workers: int = 4,
        max_workers: int = 1280,
        scale_up_threshold: int = 50,
        scale_down_threshold: int = 5,
        scale_down_hold: float = 10.0,
        # How often the controller wakes up (4×/s for fast reaction)
        controller_interval: float = 0.25,
        # Max workers to add per tick in the proportional layer
        max_step_up: int = 32,
        # Workers to add in one shot when depth > emergency_multiplier × threshold
        burst_size: int = 64,
        emergency_multiplier: float = 5.0,
        max_session_messages: int = 256,
        max_session_bytes: int = 1024 * 1024,
        max_queue_messages: int = 8192,
        max_queue_bytes: int = 32 * 1024 * 1024,
        control_reserve_messages: int = 32,
        control_reserve_bytes: int = 64 * 1024,
        dispatch_slice: int = 16,
        dispatch_slice_seconds: float = 0.002,
        shutdown_timeout: float = 5.0,
        max_sessions: int = 5000,
    ):
        for name, value in (
            ("min_workers", min_workers),
            ("max_workers", max_workers),
            ("scale_up_threshold", scale_up_threshold),
            ("scale_down_threshold", scale_down_threshold),
            ("max_step_up", max_step_up),
            ("burst_size", burst_size),
            ("max_session_messages", max_session_messages),
            ("max_session_bytes", max_session_bytes),
            ("max_queue_messages", max_queue_messages),
            ("max_queue_bytes", max_queue_bytes),
            ("dispatch_slice", dispatch_slice),
            ("max_sessions", max_sessions),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if min_workers > max_workers:
            raise ValueError("min_workers must not exceed max_workers")
        for name, value in (
            ("control_reserve_messages", control_reserve_messages),
            ("control_reserve_bytes", control_reserve_bytes),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        for name, value, allow_zero in (
            ("controller_interval", controller_interval, False),
            ("emergency_multiplier", emergency_multiplier, False),
            ("dispatch_slice_seconds", dispatch_slice_seconds, False),
            ("scale_down_hold", scale_down_hold, True),
            ("shutdown_timeout", shutdown_timeout, True),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value < 0
                or (value == 0 and not allow_zero)
            ):
                qualifier = "nonnegative" if allow_zero else "positive"
                raise ValueError(f"{name} must be finite and {qualifier}")

        self._dispatch_fn = dispatch_fn
        self._min_workers = min_workers
        self._max_workers = max_workers
        self._scale_up_threshold = scale_up_threshold
        self._scale_down_threshold = scale_down_threshold
        self._scale_down_hold = scale_down_hold
        self._controller_interval = controller_interval
        self._max_step_up = max_step_up
        self._burst_size = burst_size
        self._emergency_multiplier = emergency_multiplier
        self._max_session_messages = max_session_messages
        self._max_session_bytes = max_session_bytes
        self._max_queue_messages = max_queue_messages
        self._max_queue_bytes = max_queue_bytes
        self._control_reserve_messages = control_reserve_messages
        self._control_reserve_bytes = control_reserve_bytes
        self._dispatch_slice = dispatch_slice
        self._dispatch_slice_seconds = dispatch_slice_seconds
        self._shutdown_timeout = shutdown_timeout
        self._max_sessions = max_sessions

        self._mailboxes: Dict[int, _SessionMailbox] = {}
        # Ordered keys allow FIFO scheduling and O(1) removal on disconnect,
        # without accumulating stale ready tokens or idle session references.
        self._ready = OrderedDict()
        # Python 3.8 synchronization primitives bind when created. Allocate
        # them in start(), not in a constructor used outside asyncio.run().
        self._work_available = None
        self._drained = None
        self._pending_messages = 0
        self._pending_bytes = 0
        self._pending_retirements = 0
        self._running = False
        self._aborting = True
        self._stop_task: Optional[asyncio.Task] = None

        # Active worker tasks
        self._workers: List[asyncio.Task] = []

        # Background controller task
        self._controller_task: Optional[asyncio.Task] = None

        # Metrics
        self._stats = WorkerPoolStats(
            min_workers=min_workers,
            max_workers=max_workers,
        )
        self._msg_counter = 0
        self._last_tps_tick = time.monotonic()
        self._predictor = LoadPredictor()
        self._scale_events: Deque[ScaleEvent] = deque(maxlen=50)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the minimum number of workers and the scaling controller."""
        if self._stop_task is not None:
            stopping = self._stop_task
            await asyncio.shield(stopping)
            if self._stop_task is stopping:
                self._stop_task = None
        if self._running:
            return
        self._workers[:] = [task for task in self._workers if not task.done()]
        if self._workers:
            raise RuntimeError("Previous dispatch workers have not stopped")
        self._work_available = asyncio.Event()
        self._drained = asyncio.Event()
        self._drained.set()
        self._running = True
        self._aborting = False
        self._pending_retirements = 0
        self._stats.low_depth_since = None
        self._msg_counter = 0
        self._last_tps_tick = time.monotonic()
        for _ in range(self._min_workers):
            self._spawn_worker()
        self._controller_task = asyncio.create_task(
            self._scale_controller(), name="latzero-scale-controller"
        )
        self._update_stats()

    async def stop(self) -> None:
        """Seal admission, drain to a deadline, then cancel remaining workers."""
        if self._stop_task is None:
            self._running = False
            self._pending_retirements = 0
            self._stop_task = asyncio.create_task(
                self._shutdown(), name="latzero-worker-shutdown"
            )
        stopping = self._stop_task
        try:
            await asyncio.shield(stopping)
        finally:
            if stopping.done() and self._stop_task is stopping:
                self._stop_task = None

    async def _shutdown(self) -> None:
        deadline = time.monotonic() + self._shutdown_timeout
        if self._controller_task is not None:
            self._controller_task.cancel()
            try:
                await self._controller_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Worker scaling controller failed during shutdown")
            self._controller_task = None

        if self._pending_messages:
            try:
                await asyncio.wait_for(
                    self.join(), timeout=max(0.0, deadline - time.monotonic())
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Worker shutdown timed out with %d pending messages",
                    self._pending_messages,
                )

        self._aborting = True
        for mailbox in list(self._mailboxes.values()):
            self.invalidate(mailbox.session)
        workers = [task for task in self._workers if not task.done()]
        for task in workers:
            task.cancel()
        if workers:
            # asyncio.wait, unlike wait_for(gather(...)), remains bounded even
            # when an application handler suppresses cancellation.
            await asyncio.wait(
                workers, timeout=max(0.0, deadline - time.monotonic())
            )
        if self._work_available is not None:
            self._work_available.clear()
        self._workers[:] = [task for task in self._workers if not task.done()]
        if self._workers:
            logger.error(
                "%d dispatch workers still running after cancellation; restart requires their exit",
                len(self._workers),
            )
        self._update_stats()

    async def join(self) -> None:
        """Wait until all admitted messages have finished or been discarded."""
        while self._pending_messages:
            await self._drained.wait()

    # ------------------------------------------------------------------
    # Message submission (called by reader coroutines)
    # ------------------------------------------------------------------

    async def submit(
        self, session: Any, message: dict, byte_size: Optional[int] = None
    ) -> bool:
        """Admit without yielding; False means no message was enqueued.

        byte_size is the ingress frame's byte length. When omitted, compact
        UTF-8 JSON length is estimated without retaining an encoded copy.
        Budgets cover queued and active messages, not decoded-object overhead.
        """
        if (
            not self._running
            or getattr(session, "closed", False)
            or getattr(session, "closing", False)
            or not isinstance(message, dict)
        ):
            return self._reject_submission()

        key = id(session)
        mailbox = self._mailboxes.get(key)
        if mailbox is None and len(self._mailboxes) >= self._max_sessions:
            return self._reject_submission()
        control = message.get("type") == "app_result"
        reserve_messages = self._control_reserve_messages if control else 0
        reserve_bytes = self._control_reserve_bytes if control else 0
        session_messages = mailbox.pending_messages if mailbox is not None else 0
        session_bytes = mailbox.pending_bytes if mailbox is not None else 0
        if (
            session_messages + 1 > self._max_session_messages + reserve_messages
            or self._pending_messages + 1 > self._max_queue_messages + reserve_messages
        ):
            return self._reject_submission()
        available_bytes = min(
            self._max_session_bytes + reserve_bytes - session_bytes,
            self._max_queue_bytes + reserve_bytes - self._pending_bytes,
        )
        if byte_size is None:
            byte_size = 0
            try:
                encoder = json.JSONEncoder(ensure_ascii=False, separators=(",", ":"))
                for chunk in encoder.iterencode(message):
                    byte_size += len(chunk.encode("utf-8"))
                    if byte_size > available_bytes:
                        return self._reject_submission()
            except (TypeError, ValueError, UnicodeError, RecursionError):
                return self._reject_submission()
        if (
            isinstance(byte_size, bool)
            or not isinstance(byte_size, int)
            or byte_size < 0
            or byte_size > available_bytes
        ):
            return self._reject_submission()

        if mailbox is None:
            mailbox = _SessionMailbox(session=session)
            self._mailboxes[key] = mailbox
        mailbox.messages.append(
            _QueuedMessage(
                message=message,
                byte_size=byte_size,
                generation=getattr(session, "generation", 0),
                submitted_at=time.monotonic(),
            )
        )
        mailbox.pending_messages += 1
        mailbox.pending_bytes += byte_size
        self._pending_messages += 1
        self._pending_bytes += byte_size
        self._drained.clear()
        if not mailbox.active:
            self._ready[key] = None
            self._work_available.set()
        return True

    def _reject_submission(self) -> bool:
        self._stats.rejected_messages += 1
        return False

    def invalidate(self, session: Any) -> None:
        """Purge queued work on disconnect; an active handler finishes separately.

        Mark the session closed/closing before calling this. Pool membership
        transitions must not invalidate following pipelined messages.
        """
        key = id(session)
        mailbox = self._mailboxes.get(key)
        if mailbox is None or mailbox.session is not session:
            return
        while mailbox.messages:
            self._complete_message(mailbox, mailbox.messages.popleft(), discarded=True)
        self._ready.pop(key, None)
        if not mailbox.active:
            self._mailboxes.pop(key, None)
        if not self._ready and not self._pending_retirements:
            if self._work_available is not None:
                self._work_available.clear()

    def _complete_message(
        self, mailbox: _SessionMailbox, item: _QueuedMessage, discarded: bool = False
    ) -> None:
        mailbox.pending_messages -= 1
        mailbox.pending_bytes -= item.byte_size
        self._pending_messages -= 1
        self._pending_bytes -= item.byte_size
        if discarded:
            self._stats.discarded_messages += 1
        if not self._pending_messages:
            self._drained.set()

    # ------------------------------------------------------------------
    # Worker
    # ------------------------------------------------------------------

    def _spawn_worker(self) -> asyncio.Task:
        task = asyncio.create_task(self._worker_loop(), name="latzero-worker")
        self._workers.append(task)
        task.add_done_callback(self._worker_done)
        return task

    def _worker_done(self, task: asyncio.Task) -> None:
        if task in self._workers:
            self._workers.remove(task)
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                logger.error(
                    "Dispatch worker exited unexpectedly",
                    exc_info=(type(error), error, error.__traceback__),
                )
        live = self._worker_count()
        self._pending_retirements = min(
            self._pending_retirements, max(0, live - self._min_workers)
        )
        if self._running:
            for _ in range(max(0, self._min_workers - live)):
                self._spawn_worker()

    def _worker_count(self) -> int:
        return sum(not task.done() for task in self._workers)

    async def _worker_loop(self) -> None:
        """Claim a ready session, dispatch one bounded slice, then yield."""
        while not self._aborting:
            if self._pending_retirements:
                if self._worker_count() > self._min_workers:
                    self._pending_retirements -= 1
                    # Remove synchronously with claiming retirement, rather
                    # than leaving an exiting worker available to retire twice.
                    self._workers.remove(asyncio.current_task())
                    return
                self._pending_retirements = 0
            if not self._ready:
                self._work_available.clear()
                await self._work_available.wait()
                continue

            key, _ = self._ready.popitem(last=False)
            mailbox = self._mailboxes[key]
            mailbox.active = True
            slice_started = time.monotonic()
            processed = 0
            try:
                while mailbox.messages:
                    session = mailbox.session
                    if getattr(session, "closed", False) or getattr(session, "closing", False):
                        self.invalidate(session)
                        break
                    item = mailbox.messages.popleft()
                    # A FIFO join/switch may advance generation. Do not drop
                    # later pipelined frames solely for that change: dispatch_fn
                    # must validate any explicit pool against current membership.
                    started = time.monotonic()
                    wait = started - item.submitted_at
                    self._stats.queue_wait_seconds += wait
                    self._stats.max_queue_wait_seconds = max(
                        self._stats.max_queue_wait_seconds, wait
                    )
                    self._msg_counter += 1
                    self._stats.messages_processed += 1
                    self._stats.active_dispatches += 1
                    cancelled = False
                    try:
                        await self._dispatch_fn(session, item.message)
                    except asyncio.CancelledError:
                        cancelled = True
                        raise
                    except Exception:
                        self._stats.dispatch_failures += 1
                        logger.exception(
                            "Message dispatch failed for session %s (type=%r)",
                            key,
                            item.message.get("type"),
                        )
                    finally:
                        service = time.monotonic() - started
                        self._stats.service_seconds += service
                        self._stats.max_service_seconds = max(
                            self._stats.max_service_seconds, service
                        )
                        self._stats.active_dispatches -= 1
                        self._complete_message(mailbox, item, discarded=cancelled)
                    processed += 1
                    if (
                        processed >= self._dispatch_slice
                        or time.monotonic() - slice_started >= self._dispatch_slice_seconds
                    ):
                        break
            finally:
                mailbox.active = False
                if mailbox.messages:
                    self._ready[key] = None
                    self._work_available.set()
                else:
                    self._mailboxes.pop(key, None)
                # Idle worker frames must not retain finished sessions/payloads.
                mailbox = session = item = None
            await asyncio.sleep(0)

    # ------------------------------------------------------------------
    # Auto-scaling controller  (runs every controller_interval seconds)
    # ------------------------------------------------------------------

    async def _scale_controller(self) -> None:
        """Inspect queue depth every tick and apply the three-layer scaling policy."""
        while True:
            await asyncio.sleep(self._controller_interval)
            self._update_stats()

            depth = self._pending_messages
            self._predictor.record(float(depth))
            predicted, confidence = self._predictor.predict()
            self._stats.predicted_depth = predicted
            self._stats.prediction_confidence = confidence

            n = self._worker_count() - self._pending_retirements

            # ── Layer 1: Emergency burst ─────────────────────────────────
            # Queue is critically deep (> N× threshold). Spawn a large batch
            # of workers immediately to catch up, instead of trickling +2.
            emergency_level = self._scale_up_threshold * self._emergency_multiplier
            if depth > emergency_level and n < self._max_workers:
                to_add = min(self._burst_size, self._max_workers - n)
                self._scale_up(
                    to_add,
                    reason=(
                        f"emergency: queue {depth} > "
                        f"{emergency_level:.0f} ({self._emergency_multiplier:.0f}× threshold)"
                    ),
                )
                continue  # Re-evaluate on next tick

            # ── Layer 2: Proportional reactive scale-up ──────────────────
            # Add workers proportional to how overloaded the queue is.
            # ratio=1  → at threshold      → add 1
            # ratio=2  → 2× threshold      → add 2
            # ratio=5  → 5× threshold      → add max_step_up
            if depth > self._scale_up_threshold and n < self._max_workers:
                ratio = depth / self._scale_up_threshold
                step = min(int(ratio), self._max_step_up)
                step = max(step, 2)  # Always at least 2 (same as before for small overload)
                self._scale_up(
                    step,
                    reason=(
                        f"proportional: queue {depth} "
                        f"({ratio:.1f}× threshold) → +{step}"
                    ),
                )
                continue

            # ── Layer 3: Predictive pre-scale ────────────────────────────
            # Forecast says depth will exceed threshold within the horizon.
            # Lower confidence threshold (0.3) to activate quickly under
            # bursty load, not just during smooth steady-state ramps.
            if (
                confidence >= 0.3
                and predicted > self._scale_up_threshold
                and n < self._max_workers
            ):
                ratio = predicted / self._scale_up_threshold
                step = min(max(int(ratio), 1), self._max_step_up)
                self._scale_up(
                    step,
                    reason=(
                        f"predictive: forecast {predicted:.0f} > "
                        f"{self._scale_up_threshold} (conf {confidence:.2f}) → +{step}"
                    ),
                )
                continue

            # ── Layer 4: Gradual drain ───────────────────────────────────
            if depth < self._scale_down_threshold and n > self._min_workers:
                now = time.monotonic()
                if self._stats.low_depth_since is None:
                    self._stats.low_depth_since = now
                elif (now - self._stats.low_depth_since) >= self._scale_down_hold:
                    self._scale_down(
                        1,
                        reason=(
                            f"idle: queue {depth} < {self._scale_down_threshold} "
                            f"for {self._scale_down_hold:.0f}s"
                        ),
                    )
                    self._stats.low_depth_since = None
            else:
                self._stats.low_depth_since = None

    # ------------------------------------------------------------------
    # Scaling helpers
    # ------------------------------------------------------------------

    def _scale_up(self, count: int, reason: str) -> None:
        if not self._running:
            return
        live = self._worker_count()
        old = live - self._pending_retirements
        restored = min(max(0, count), self._pending_retirements)
        self._pending_retirements -= restored
        to_add = min(max(0, count - restored), self._max_workers - live)
        for _ in range(to_add):
            self._spawn_worker()
        new = self._worker_count() - self._pending_retirements
        if new > old:
            ev = ScaleEvent(
                timestamp=time.time(),
                direction="up",
                reason=reason,
                old_count=old,
                new_count=new,
            )
            self._scale_events.appendleft(ev)
            self._stats.scale_events = list(self._scale_events)

    def _scale_down(self, count: int, reason: str) -> None:
        if not self._running:
            return
        old = self._worker_count() - self._pending_retirements
        to_remove = min(max(0, count), old - self._min_workers)
        if to_remove <= 0:
            return
        self._pending_retirements += to_remove
        self._work_available.set()
        new_expected = old - to_remove
        ev = ScaleEvent(
            timestamp=time.time(),
            direction="down",
            reason=reason,
            old_count=old,
            new_count=new_expected,
        )
        self._scale_events.appendleft(ev)
        self._stats.scale_events = list(self._scale_events)

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def _update_stats(self) -> None:
        now = time.monotonic()
        dt = now - self._last_tps_tick
        if dt >= 1.0:
            self._stats.messages_per_sec = self._msg_counter / dt
            self._msg_counter = 0
            self._last_tps_tick = now

        self._stats.active_workers = self._worker_count()
        self._stats.queue_depth = self._pending_messages
        self._stats.queue_bytes = self._pending_bytes
        self._stats.pending_retirements = self._pending_retirements
        self._stats.scale_events = list(self._scale_events)

    @property
    def stats(self) -> WorkerPoolStats:
        """Return a current stats snapshot (updates lazily on controller tick)."""
        self._stats.active_workers = self._worker_count()
        self._stats.queue_depth = self._pending_messages
        self._stats.queue_bytes = self._pending_bytes
        self._stats.pending_retirements = self._pending_retirements
        return self._stats
