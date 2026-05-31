"""
Auto-scaling asyncio worker pool for LatZero message dispatch.

Architecture:
    [Reader 1] ──┐
    [Reader 2] ──┤    ┌───────────────────┐    ┌──────────────────┐
    [Reader N] ──┼──→ │  Dispatch Queue   │ ──→│  Worker Pool     │
                 └──── │  (asyncio.Queue)  │    │  4–1280 Tasks     │
                       └───────────────────┘    └──────────────────┘
                                                        ↑
                                               Auto-Scaling Controller
                                               (proportional + predictive)

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
import time
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Deque, List, Optional, Tuple
from collections import deque


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

# Sentinel pushed onto the queue to signal a worker to exit cleanly
_STOP_SENTINEL = object()


class AutoScalingWorkerPool:
    """
    Shared asyncio.Queue fed by reader coroutines and drained by a
    self-scaling pool of worker Tasks.

    Workers are ephemeral asyncio Tasks — zero process spawn overhead.
    Readers are never blocked by dispatch; they enqueue and move on.

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
    ):
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

        # The shared message queue (unbounded — readers never block)
        self._queue: asyncio.Queue[Any] = asyncio.Queue()

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
        for _ in range(self._min_workers):
            self._spawn_worker()
        self._controller_task = asyncio.create_task(
            self._scale_controller(), name="latzero-scale-controller"
        )
        self._update_stats()

    async def stop(self) -> None:
        """
        Gracefully shut down: cancel controller, then signal each worker
        with the stop sentinel and wait for all to finish.
        """
        if self._controller_task is not None:
            self._controller_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._controller_task
            self._controller_task = None

        # Send one sentinel per live worker so each exits after its current msg
        for _ in self._workers:
            await self._queue.put(_STOP_SENTINEL)

        if self._workers:
            with suppress(Exception):
                await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()

    # ------------------------------------------------------------------
    # Message submission (called by reader coroutines)
    # ------------------------------------------------------------------

    async def submit(self, session: Any, message: dict) -> None:
        """Enqueue a (session, message) pair for dispatch. Non-blocking."""
        await self._queue.put((session, message))

    # ------------------------------------------------------------------
    # Worker
    # ------------------------------------------------------------------

    def _spawn_worker(self) -> asyncio.Task:
        task = asyncio.create_task(self._worker_loop(), name="latzero-worker")
        self._workers.append(task)
        # Remove from list when done (normal exit or exception)
        task.add_done_callback(lambda t: self._workers.remove(t) if t in self._workers else None)
        return task

    async def _worker_loop(self) -> None:
        """Pull messages from the queue and dispatch them."""
        while True:
            item = await self._queue.get()
            try:
                if item is _STOP_SENTINEL:
                    return  # Clean exit
                session, message = item
                self._msg_counter += 1
                self._stats.messages_processed += 1
                try:
                    await self._dispatch_fn(session, message)
                except Exception:
                    # Errors are handled inside _dispatch_fn; swallow here
                    # so a buggy handler never kills the worker.
                    pass
            finally:
                self._queue.task_done()

    # ------------------------------------------------------------------
    # Auto-scaling controller  (runs every controller_interval seconds)
    # ------------------------------------------------------------------

    async def _scale_controller(self) -> None:
        """Inspect queue depth every tick and apply the three-layer scaling policy."""
        while True:
            await asyncio.sleep(self._controller_interval)
            self._update_stats()

            depth = self._queue.qsize()
            self._predictor.record(float(depth))
            predicted, confidence = self._predictor.predict()
            self._stats.predicted_depth = predicted
            self._stats.prediction_confidence = confidence

            n = len(self._workers)

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
        old = len(self._workers)
        to_add = min(count, self._max_workers - old)
        for _ in range(to_add):
            self._spawn_worker()
        new = len(self._workers)
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
        old = len(self._workers)
        to_remove = min(count, old - self._min_workers)
        if to_remove <= 0:
            return
        # Put sentinels — the next idle worker(s) will pick them up and exit
        for _ in range(to_remove):
            self._queue.put_nowait(_STOP_SENTINEL)
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

        self._stats.active_workers = len(self._workers)
        self._stats.queue_depth = self._queue.qsize()
        self._stats.scale_events = list(self._scale_events)

    @property
    def stats(self) -> WorkerPoolStats:
        """Return a current stats snapshot (updates lazily on controller tick)."""
        self._stats.active_workers = len(self._workers)
        self._stats.queue_depth = self._queue.qsize()
        return self._stats
