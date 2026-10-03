"""Bounded raw TCP measurement, not a capacity or improvement benchmark.

From a checkout (no editable install needed):
    python examples/benchmark_daemon.py --mode both --duration 1 --clients 2 \
        --workers 2 --rate 100

One daemon and its raw clients share an event loop/process. Resources and loop
lag therefore include the load generator. JSON goes to stdout; data and both
optional listeners use a temporary directory and OS-assigned ports. RPC means
terminal completion, not acceptance ACK; fanout means observed notifications.
Rate applies only to scheduled-open mode. Exit 1 means an unclean envelope
(explicit rejection, failure, timeout, or a validation problem), not capacity.
Payload bytes specify ASCII padding; unique tokens and envelopes are extra.
"""

import argparse
import asyncio
from collections import Counter, deque
from dataclasses import asdict, dataclass
from importlib import metadata
import itertools
import json
import math
from pathlib import Path
import platform
import sys
import tempfile
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from latzero_server.config import ServerConfig
from latzero_server.persistence import SnapshotStore
from latzero_server.server import LatZeroServer

try:
    import psutil
except ImportError:
    psutil = None


# Harness safety ceilings, not daemon operating-envelope recommendations.
LIMITS = {"duration_seconds": 60, "warmup_seconds": 30, "clients": 32,
          "outstanding_per_client": 16, "in_flight": 128, "workers": 64,
          "payload_bytes": 65536, "fanout": 16, "rate": 10000,
          "operations_per_phase": 10000, "effects_per_run": 500000,
          "sweep_runs": 32, "deadline_seconds": 10, "setup_seconds": 30,
          "shutdown_seconds": 15}
KINDS = ("set_get", "app_rpc", "process_rpc", "fanout")
clock = time.perf_counter


async def sleep_until(target: float) -> None:
    # asyncio's Windows clock may wake a timer early; retain the exact schedule.
    while clock() < target:
        await asyncio.sleep(target - clock())


def percentile(values: List[float], percent: float) -> Optional[float]:
    """Linear interpolation at (n - 1) * percent / 100; empty means null."""
    if not 0 <= percent <= 100:
        raise ValueError("percentile must be between 0 and 100")
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * percent / 100
    low, high = math.floor(index), math.ceil(index)
    return ordered[low] + (ordered[high] - ordered[low]) * (index - low)


def distribution(values: List[float]) -> dict:
    return {"count": len(values), "p50": percentile(values, 50),
            "p95": percentile(values, 95), "p99": percentile(values, 99),
            "p99.9": percentile(values, 99.9), "max": max(values) if values else None}


@dataclass(frozen=True)
class Settings:
    mode: str = "closed"
    duration: float = 1.0
    clients: int = 2
    outstanding: int = 1
    workers: int = 2
    rate: float = 100.0
    payload_bytes: int = 32
    fanout: int = 2
    persistence_fraction: float = 0.25
    workload: str = "mixed"
    warmup: float = 0.0
    timeout: float = 2.0
    max_operations: int = 10000
    sample_interval: float = 0.02
    handler_delay: float = 0.0
    websocket_enabled: bool = False

    def validate(self) -> None:
        if self.mode not in ("closed", "open") or self.workload not in ("mixed", "set_get", "rpc", "fanout"):
            raise ValueError("unsupported mode or workload")
        for name, ceiling in (("clients", 32), ("outstanding", 16), ("workers", 64),
                              ("max_operations", 10000)):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= ceiling:
                raise ValueError("{} must be an integer in [1, {}]".format(name, ceiling))
        for name, ceiling in (("payload_bytes", 65536), ("fanout", 16)):
            value = getattr(self, name)
            if type(value) is not int or not 0 <= value <= ceiling:
                raise ValueError("{} must be an integer in [0, {}]".format(name, ceiling))
        for name, ceiling in (("duration", 60), ("timeout", 10), ("rate", 10000)):
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= ceiling:
                raise ValueError("{} must be finite in (0, {}]".format(name, ceiling))
        for name, ceiling in (("warmup", 30), ("handler_delay", self.timeout), ("persistence_fraction", 1)):
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= ceiling:
                raise ValueError("{} must be finite in [0, {}]".format(name, ceiling))
        if (type(self.sample_interval) not in (int, float) or not math.isfinite(self.sample_interval)
                or not 0.01 <= self.sample_interval <= 1):
            raise ValueError("sample_interval must be finite in [0.01, 1]")
        if self.clients * self.outstanding > LIMITS["in_flight"]:
            raise ValueError("clients * outstanding exceeds harness in-flight ceiling")
        effect_bound = (self.clients + 2 * self.fanout + 1) * self.max_operations * (2 if self.warmup else 1)
        if effect_bound > LIMITS["effects_per_run"]:
            raise ValueError("configured exact effect history exceeds harness ceiling; lower max_operations")


class ResponseTracker:
    """Exact, capped typed correlation history, including late/duplicate replies."""

    def __init__(self, limit: int):
        self.limit = limit
        self.entries: Dict[str, dict] = {}
        self.counts = Counter()

    def begin(self, request_id: str, terminal: str) -> None:
        if request_id in self.entries:
            raise ValueError("request ID reused: " + request_id)
        if len(self.entries) >= self.limit:
            raise RuntimeError("response history ceiling reached")
        self.entries[request_id] = {"terminal": terminal, "seen": set(),
                                    "done": False, "sent": False, "expired": False}

    def receive(self, message: dict) -> bool:
        kind, request_id = message.get("type"), message.get("request_id")
        if kind not in ("ack", "error", "app_result"):
            return False
        entry = self.entries.get(request_id)
        if entry is None:
            self.counts["unexpected_responses"] += 1
            return False
        terminal = kind == "error" or kind == entry["terminal"]
        if kind in entry["seen"] or terminal and entry["done"]:
            self.counts["duplicate_responses"] += 1
            return False
        entry["seen"].add(kind)
        if kind == "app_result":
            correlation = (message.get("payload") or {}).get("request_id")
            if correlation is not None and correlation != request_id:
                self.counts["mismatched_correlations"] += 1
        if terminal:
            entry["done"] = True
            if entry["expired"]:
                self.counts["late_responses"] += 1
        return terminal

    def summary(self) -> dict:
        result = {name: self.counts[name] for name in (
            "unexpected_responses", "duplicate_responses", "mismatched_correlations", "late_responses")}
        result.update({"requests": len(self.entries),
                       "frames_sent": sum(e["sent"] for e in self.entries.values()),
                       "missing_responses": sum(e["sent"] and not e["done"] for e in self.entries.values()),
                       "missing_acceptance_acks": sum(e["terminal"] == "app_result" and
                           "app_result" in e["seen"] and "ack" not in e["seen"] for e in self.entries.values())})
        return result


class EffectLedger:
    """Retain all expected unique values within a declared finite run budget."""

    def __init__(self, limit: int = LIMITS["effects_per_run"]):
        self.limit = limit
        self.expected: Dict[Tuple[str, str, str], tuple] = {}
        self.seen = Counter()
        self.counts = Counter()
        self.outcomes: Dict[str, str] = {}
        self.changed = asyncio.Event()

    def expect(self, recipient: str, kind: str, value: dict, detail: str) -> tuple:
        key = (recipient, kind, value["token"])
        if key in self.expected or len(self.expected) >= self.limit:
            raise RuntimeError("duplicate expectation or effect history ceiling reached")
        self.expected[key] = (value, detail)
        return key

    def observe(self, recipient: str, kind: str, value: Any, detail: str) -> None:
        token = value.get("token") if isinstance(value, dict) else None
        key = (recipient, kind, token)
        expected = self.expected.get(key)
        if expected is None:
            self.counts["unexpected_effects"] += 1
        elif expected != (value, detail):
            self.counts["mismatched_effects"] += 1
        else:
            self.seen[key] += 1
            if self.seen[key] > 1:
                self.counts["duplicate_effects"] += 1
        self.changed.set()

    async def wait(self, keys: List[tuple], deadline: float) -> None:
        while any(not self.seen[key] for key in keys):
            self.changed.clear()
            await asyncio.wait_for(self.changed.wait(), max(0, deadline - clock()))

    def summary(self) -> dict:
        missing = Counter(self.outcomes.get(key[2], "unfinished")
                          for key in self.expected if not self.seen[key])
        return {"expected": len(self.expected), "observed": sum(bool(self.seen[k]) for k in self.expected),
                "missing_effects": sum(missing.values()), "missing_by_outcome": dict(missing),
                **{name: self.counts[name] for name in (
                    "unexpected_effects", "mismatched_effects", "duplicate_effects")}}


class ProtocolFailure(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class RawPeer:
    """One reader and serialized deadline-bounded writes; no task per push."""

    def __init__(self, reader, writer, client_id: str, ledger: EffectLedger, history_limit: int):
        self.reader, self.writer, self.client_id = reader, writer, client_id
        self.ledger = ledger
        self.tracker = ResponseTracker(history_limit)
        self.pending: Dict[str, asyncio.Future] = {}
        self.write_lock = asyncio.Lock()
        self.sequence = 0
        self.calls = None
        self.errors: List[str] = []
        self.closing = False
        self.reader_task = asyncio.create_task(self._read(), name="benchmark-reader")

    async def _read(self) -> None:
        try:
            while True:
                raw = await self.reader.readline()
                if not raw:
                    raise ConnectionError("peer EOF")
                message = json.loads(raw)
                if not isinstance(message, dict):
                    raise ValueError("non-object response")
                kind, payload = message.get("type"), message.get("payload") or {}
                if kind == "call_app":
                    self.ledger.observe(self.client_id, "rpc", payload.get("data"), payload.get("event"))
                    if self.calls is None:
                        raise ValueError("RPC arrived at a non-callee")
                    self.calls.put_nowait(message)
                elif kind == "buffer_update":
                    entry = payload.get("entry") or {}
                    self.ledger.observe(self.client_id, "buffer", entry.get("value"), payload.get("key"))
                elif kind == "emit_event":
                    self.ledger.observe(self.client_id, "event", payload.get("data"), payload.get("event"))
                elif self.tracker.receive(message):
                    future = self.pending.get(message.get("request_id"))
                    if future is not None and not future.done():
                        future.set_result(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not self.closing and len(self.errors) < 8:
                self.errors.append("{}: {}".format(type(exc).__name__, exc))
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(ConnectionError(str(exc)))

    async def request(self, kind: str, payload: dict, deadline: float,
                      request_id: Optional[str] = None, terminal: str = "ack") -> dict:
        self.sequence += 1
        request_id = request_id or "{}-control-{}".format(self.client_id, self.sequence)
        self.tracker.begin(request_id, terminal)
        future = asyncio.get_running_loop().create_future()
        self.pending[request_id] = future
        encoded = (json.dumps({"type": kind, "request_id": request_id, "pool": None,
                              "client_id": self.client_id, "payload": payload},
                             allow_nan=False, separators=(",", ":")) + "\n").encode("utf-8")

        async def exchange() -> dict:
            async with self.write_lock:
                if clock() >= deadline:
                    raise asyncio.TimeoutError()
                self.writer.write(encoded)
                self.tracker.entries[request_id]["sent"] = True
                await self.writer.drain()
            return await future

        try:
            reply = await asyncio.wait_for(exchange(), max(0, deadline - clock()))
            if clock() > deadline:
                raise asyncio.TimeoutError()
            if reply["type"] == "error":
                raise ProtocolFailure((reply.get("payload") or {}).get("code", "unknown_error"))
            return reply
        except asyncio.TimeoutError:
            self.tracker.entries[request_id]["expired"] = True
            raise
        finally:
            self.pending.pop(request_id, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()

    async def close(self) -> None:
        self.closing = True
        self.writer.close()
        try:
            await asyncio.wait_for(self.writer.wait_closed(), 2)
        finally:
            self.reader_task.cancel()
            await asyncio.gather(self.reader_task, return_exceptions=True)


class BoundedAdmission:
    """Admission reserves an idle fixed lane, never queues another operation."""

    def __init__(self, slots: int):
        if slots < 1:
            raise ValueError("slots must be positive")
        self.slots = slots
        self.free = deque(range(slots))
        self.busy = set()
        self.peak = 0
        self.rejected = 0

    def take(self) -> Optional[int]:
        if not self.free:
            self.rejected += 1
            return None
        slot = self.free.popleft()
        self.busy.add(slot)
        self.peak = max(self.peak, len(self.busy))
        return slot

    def release(self, slot: int) -> None:
        if slot not in self.busy:
            raise ValueError("lane was not reserved")
        self.busy.remove(slot)
        self.free.append(slot)


def scheduled_count(duration: float, rate: float, limit: int) -> int:
    return min(limit, math.ceil(duration * rate))


class Workload:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.ledger = EffectLedger()
        self.peers: List[RawPeer] = []
        self.clients: List[RawPeer] = []
        self.subscribers: List[RawPeer] = []
        self.callee = None
        self.responder_task = None
        self.state: Dict[str, tuple] = {}
        self.state_mismatches = 0
        self.buffer_writes = 0
        self.persistent_writes = 0
        self.padding = "x" * settings.payload_bytes

    async def connect(self, port: int, client_id: str) -> RawPeer:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(
            "127.0.0.1", port, limit=1024 * 1024 + 1), self.settings.timeout)
        history_limit = 4 * self.settings.max_operations + 2 * self.settings.clients * self.settings.outstanding + 16
        peer = RawPeer(reader, writer, client_id, self.ledger, history_limit)
        self.peers.append(peer)
        await peer.request("hello", {}, clock() + self.settings.timeout)
        await peer.request("join_pool", {"pool": "benchmark", "client_id": client_id},
                           clock() + self.settings.timeout)
        return peer

    async def setup(self, port: int) -> None:
        cfg = self.settings
        self.callee = await self.connect(port, "callee")
        self.callee.calls = asyncio.Queue(maxsize=cfg.clients * cfg.outstanding)
        await self.callee.request("register_process", {"process_name": "echo", "scale": False,
            "min_workers": 1, "max_workers": 1}, clock() + cfg.timeout)
        self.responder_task = asyncio.create_task(self._respond(), name="benchmark-callee")
        for index in range(cfg.clients):
            self.clients.append(await self.connect(port, "client-{}".format(index)))
        for index in range(cfg.fanout):
            subscriber = await self.connect(port, "subscriber-{}".format(index))
            self.subscribers.append(subscriber)
            for lane in range(cfg.clients * cfg.outstanding):
                await subscriber.request("subscribe_buffer", {"key": "fanout-{}".format(lane)},
                                         clock() + cfg.timeout)

    async def _respond(self) -> None:
        try:
            while True:
                message = await self.callee.calls.get()
                try:
                    if self.settings.handler_delay:
                        await asyncio.sleep(self.settings.handler_delay)
                    reply = await self.callee.request("app_result", {
                        "value": message["payload"]["data"], "error": None},
                        clock() + self.settings.timeout, request_id=message["request_id"])
                    if reply["payload"].get("delivered") is not True:
                        raise ValueError("callee reply was not admitted")
                except Exception as exc:
                    if len(self.callee.errors) < 8:
                        self.callee.errors.append("callee: {}: {}".format(type(exc).__name__, exc))
                finally:
                    self.callee.calls.task_done()
        except asyncio.CancelledError:
            raise

    def kind(self, sequence: int) -> str:
        if self.settings.workload == "rpc":
            return ("app_rpc", "process_rpc")[sequence % 2]
        if self.settings.workload == "mixed":
            return KINDS[sequence % len(KINDS)]
        return self.settings.workload

    async def execute(self, lane: int, sequence: int, phase: str, scheduled: float) -> str:
        cfg = self.settings
        peer = self.clients[lane // cfg.outstanding]
        token = "{}-{}".format(phase, sequence)
        value = {"token": token, "padding": self.padding}
        deadline = scheduled + cfg.timeout
        kind = self.kind(sequence)
        effects = []
        if kind in ("set_get", "fanout"):
            # Select by write ordinal, not mixed-method ordinal (which biases it).
            ordinal = self.buffer_writes
            self.buffer_writes += 1
            persistent = math.floor((ordinal + 1) * cfg.persistence_fraction) > math.floor(ordinal * cfg.persistence_fraction)
            self.persistent_writes += persistent
            key = "{}-{}".format("fanout" if kind == "fanout" else "state", lane)
            if kind == "fanout":
                effects = [self.ledger.expect(p.client_id, "buffer", value, key) for p in self.subscribers]
            written = await peer.request("set_buffer", {"key": key, "value": value, "persistent": persistent},
                                         deadline, token + "-set")
            version = written["payload"].get("version")
            expected_version = self.state.get(key, (None, False, 0))[2] + 1
            if written["payload"].get("key") != key or version != expected_version:
                self.state_mismatches += 1
                raise ValueError("set ACK key/version mismatch (possible duplicate effect)")
            self.state[key] = (value, persistent, version)
            state_effect = self.ledger.expect(peer.client_id, "state", value, key)
            self.ledger.observe(peer.client_id, "state", value, key)
            effects.append(state_effect)
            if kind == "set_get":
                read = await peer.request("get_buffer", {"key": key}, deadline, token + "-get")
                entry = read["payload"].get("entry") or {}
                if (read["payload"].get("key") != key or not read["payload"].get("exists")
                        or entry.get("value") != value or entry.get("version") != version
                        or entry.get("persistent") != persistent):
                    self.state_mismatches += 1
                    raise ValueError("get did not return the unique committed value/version")
            else:
                effects.extend(self.ledger.expect(p.client_id, "event", value, "benchmark-event")
                               for p in self.peers if p is not peer)
                await peer.request("emit_event", {"event": "benchmark-event", "data": value},
                                   deadline, token + "-event")
        else:
            event = "echo" if kind == "app_rpc" else "callee:echo"
            effects = [self.ledger.expect("callee", "rpc", value, event)]
            payload = {"data": value, "timeout": max(0.001, deadline - clock())}
            if kind == "app_rpc":
                payload.update({"target_client_id": "callee", "event": event})
            else:
                payload["process_id"] = "callee:echo"
            result = await peer.request("call_app" if kind == "app_rpc" else "call_process",
                                        payload, deadline, token + "-rpc", terminal="app_result")
            if result["payload"].get("value") != value or result["payload"].get("error") is not None:
                raise ValueError("RPC did not return the unique expected value")
        await self.ledger.wait(effects, deadline)
        if clock() > deadline:
            raise asyncio.TimeoutError()
        return kind

    async def drain(self) -> None:
        # A list_clients reply is a per-session FIFO barrier behind earlier replies.
        await asyncio.wait_for(self.callee.calls.join(), self.settings.timeout + self.settings.handler_delay)
        for peer in self.peers:
            await peer.request("list_clients", {}, clock() + self.settings.timeout)

    async def close(self) -> None:
        if self.responder_task is not None:
            self.responder_task.cancel()
            await asyncio.gather(self.responder_task, return_exceptions=True)
        await asyncio.gather(*(p.close() for p in self.peers), return_exceptions=True)


class Monitor:
    def __init__(self, server: LatZeroServer, interval: float):
        self.server, self.interval = server, interval
        self.peaks: Dict[str, Any] = {}
        self.lag: List[float] = []
        self.samples = 0
        self.missed_ticks = 0
        self.process = psutil.Process() if psutil else None
        self.resource_error = None

    def sample(self) -> None:
        server, wp = self.server, self.server._worker_pool.stats
        values = {"ingress_messages": wp.queue_depth, "ingress_bytes": wp.queue_bytes,
                  "egress_messages": sum(s.outbox_messages for s in server._sessions.values()),
                  "egress_bytes": server._outbox_bytes, "routes": server._route_count,
                  "fanout_messages": len(server._fanout_queue), "fanout_bytes": server._fanout_bytes,
                  "connections": server._connection_count, "workers": wp.active_workers,
                  "buffer_bytes": sum(p.buffer_bytes for p in server._pools.values()),
                  "tasks": len(asyncio.all_tasks()), "threads": threading.active_count(),
                  "rss_bytes": None, "handles_or_fds": None,
                  "dirty_pools": server._store.health["dirty_pools"]}
        if self.process:
            try:
                values["rss_bytes"] = self.process.memory_info().rss
                values["threads"] = self.process.num_threads()
                method = self.process.num_handles if hasattr(self.process, "num_handles") else self.process.num_fds
                values["handles_or_fds"] = method()
            except (psutil.Error, OSError) as exc:
                self.resource_error = str(exc)
        for name, value in values.items():
            previous = self.peaks.get(name)
            self.peaks[name] = max(previous or 0, value) if value is not None else previous
        self.samples += 1

    async def run(self) -> None:
        target = clock()
        while True:
            await sleep_until(target)
            now = clock()
            lag = max(0, now - target)
            if len(self.lag) < 10000:
                self.lag.append(lag)
            self.sample()
            skipped = math.floor(lag / self.interval)
            self.missed_ticks += skipped
            target += (skipped + 1) * self.interval


async def run_phase(workload: Workload, duration: float, phase: str) -> dict:
    cfg = workload.settings
    slots = cfg.clients * cfg.outstanding
    admission = BoundedAdmission(slots)
    queues = [asyncio.Queue(maxsize=1) for _ in range(slots)]
    counts = Counter()
    errors = Counter()
    latency: Dict[str, List[float]] = {name: [] for name in ("success", "fail", "timeout", "rejected")}
    per_method: Dict[str, List[float]] = {name: [] for name in KINDS}
    schedule_delay = []
    started = clock()
    end = started + duration
    wp_before = asdict(workload.server._worker_pool.stats) if hasattr(workload, "server") else None
    active = 0
    active_peak = 0

    async def execute(lane: int, sequence: int, issue: float) -> None:
        nonlocal active, active_peak
        status = "success"
        method = workload.kind(sequence)
        counts["admitted"] += 1
        active += 1
        active_peak = max(active_peak, active)
        schedule_delay.append(max(0, clock() - issue))
        try:
            await workload.execute(lane, sequence, phase, issue)
        except asyncio.TimeoutError:
            status = "timeout"
            errors["request_deadline"] += 1
        except ProtocolFailure as exc:
            status = "timeout" if exc.code in ("timeout", "route_expired") else (
                "rejected" if exc.code in ("overloaded", "delivery_failed") else "fail")
            errors[exc.code] += 1
        except Exception as exc:
            status = "fail"
            errors[type(exc).__name__] += 1
        elapsed = max(0, clock() - issue)
        active -= 1
        counts[status] += 1
        latency[status].append(elapsed)
        if status == "success":
            per_method[method].append(elapsed)
        workload.ledger.outcomes["{}-{}".format(phase, sequence)] = status

    async def open_worker(lane: int) -> None:
        while True:
            ticket = await queues[lane].get()
            if ticket is None:
                return
            try:
                await execute(lane, *ticket)
            finally:
                admission.release(lane)

    async def closed_worker(lane: int) -> None:
        while clock() < end and counts["offered"] < cfg.max_operations:
            sequence = counts["offered"]
            counts["offered"] += 1
            await execute(lane, sequence, clock())

    if cfg.mode == "open":
        tasks = [asyncio.create_task(open_worker(lane), name="benchmark-lane") for lane in range(slots)]
        try:
            for sequence in range(scheduled_count(duration, cfg.rate, cfg.max_operations)):
                issue = started + sequence / cfg.rate
                await sleep_until(issue)
                counts["offered"] += 1
                if clock() >= issue + cfg.timeout:
                    counts["timeout"] += 1
                    counts["scheduler_expired"] += 1
                    latency["timeout"].append(clock() - issue)
                    continue
                lane = admission.take()
                if lane is None:
                    counts["rejected"] += 1
                    counts["scheduler_rejected"] += 1
                    latency["rejected"].append(clock() - issue)
                else:
                    queues[lane].put_nowait((sequence, issue))
            for queue in queues:
                await queue.put(None)
            await asyncio.wait_for(asyncio.gather(*tasks), cfg.timeout + 1)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    else:
        tasks = [asyncio.create_task(closed_worker(lane), name="benchmark-lane") for lane in range(slots)]
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), duration + cfg.timeout + 1)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    elapsed = max(clock() - started, 1e-9)
    worker_metrics = None
    if wp_before is not None:
        wp = workload.server._worker_pool.stats
        dispatched = wp.messages_processed - wp_before["messages_processed"]
        worker_metrics = {"dispatched_messages": dispatched,
            "queue_wait_total_seconds": wp.queue_wait_seconds - wp_before["queue_wait_seconds"],
            "service_total_seconds": wp.service_seconds - wp_before["service_seconds"],
            "queue_wait_max_seconds_run_to_date": wp.max_queue_wait_seconds,
            "service_max_seconds_run_to_date": wp.max_service_seconds,
            "egress_wait_seconds": None}
        worker_metrics["timer_source"] = "daemon time.monotonic; potentially coarse on Windows"
        worker_metrics["timer_resolution_seconds"] = time.get_clock_info("monotonic").resolution
        worker_metrics["queue_wait_mean_seconds"] = worker_metrics["queue_wait_total_seconds"] / dispatched if dispatched else None
        worker_metrics["service_mean_seconds"] = worker_metrics["service_total_seconds"] / dispatched if dispatched else None
    planned = math.ceil(duration * cfg.rate) if cfg.mode == "open" else None
    return {"phase": phase, "requested_issue_seconds": duration, "elapsed_including_completions_seconds": elapsed,
            "operation_limit_reached": counts["offered"] >= cfg.max_operations,
            "unoffered_due_to_operation_limit": max(0, planned - cfg.max_operations) if planned else 0,
            "counts": {name: counts[name] for name in ("offered", "admitted", "success", "fail", "timeout",
                "rejected", "scheduler_rejected", "scheduler_expired")},
            "errors": dict(errors), "successful_completed_operations_per_second": counts["success"] / elapsed,
            "latency_seconds": {name: distribution(values) for name, values in latency.items()},
            "successful_per_method_latency_seconds": {name: distribution(values) for name, values in per_method.items()},
            "schedule_delay_seconds": distribution(schedule_delay), "worker_metrics": worker_metrics,
            "fixed_lane_tasks": slots, "in_flight_peak": max(admission.peak, active_peak),
            "additional_backlog_limit": 0, "reserved_lane_mailbox_capacity": slots if cfg.mode == "open" else 0,
            "terminal_count_matches_offers": counts["offered"] == sum(counts[name] for name in ("success", "fail", "timeout", "rejected")),
            "latency_origin": "scheduled_issue" if cfg.mode == "open" else "issue"}


async def run_one(settings: Settings, repetition: int = 1) -> dict:
    settings.validate()
    workload = Workload(settings)
    monitor_task = None
    phases = []
    shutdown_error = None
    run_error = None
    port = None
    websocket_port = None
    with tempfile.TemporaryDirectory(prefix="latzero-benchmark-") as data_dir:
        config = ServerConfig(host="127.0.0.1", port=0, data_dir=Path(data_dir),
            min_workers=settings.workers, max_workers=settings.workers,
            websocket_enabled=settings.websocket_enabled, websocket_port=0,
            rpc_timeout=settings.timeout, write_timeout=2, shutdown_timeout=2)
        server = LatZeroServer(config)
        workload.server = server
        monitor = Monitor(server, settings.sample_interval)
        try:
            await asyncio.wait_for(server.start(), 5)
            port = server._tcp_server.sockets[0].getsockname()[1]
            if server._websocket_server is not None:
                websocket_port = server._websocket_server.sockets[0].getsockname()[1]
            monitor.sample()
            monitor_task = asyncio.create_task(monitor.run(), name="benchmark-monitor")
            await asyncio.wait_for(workload.setup(port), LIMITS["setup_seconds"])
            if settings.warmup:
                phases.append(await run_phase(workload, settings.warmup, "warmup"))
                await asyncio.wait_for(workload.drain(), settings.timeout + settings.handler_delay + 5)
            phases.append(await run_phase(workload, settings.duration, "measure"))
            await asyncio.wait_for(workload.drain(), settings.timeout + settings.handler_delay + 5)
        except Exception as exc:
            run_error = "{}: {}".format(type(exc).__name__, exc)
        finally:
            monitor.sample()
            metrics = dict(server._metrics)
            health_before_stop = {"background_error": server._health_error, "persistence": server._store.health}
            responses = Counter()
            for peer in workload.peers:
                responses.update(peer.tracker.summary())
            if monitor_task is not None:
                monitor_task.cancel()
                await asyncio.gather(monitor_task, return_exceptions=True)
            await workload.close()
            try:
                await asyncio.wait_for(server.stop(), LIMITS["shutdown_seconds"])
            except Exception as exc:
                shutdown_error = "{}: {}".format(type(exc).__name__, exc)
        restored = SnapshotStore(Path(data_dir)).load_pools().get("benchmark", {}).get("buffers", {})
        persistence = Counter()
        for key, (value, persistent, version) in workload.state.items():
            entry = restored.get(key)
            if persistent:
                persistence["expected_persistent"] += 1
                if entry is None:
                    persistence["missing"] += 1
                elif entry.get("value") != value or entry.get("version") != version:
                    persistence["mismatched"] += 1
            else:
                persistence["expected_ephemeral"] += 1
                if entry is not None:
                    persistence["ephemeral_in_snapshot"] += 1
        persistence_result = {name: persistence[name] for name in ("expected_persistent", "expected_ephemeral",
            "missing", "mismatched", "ephemeral_in_snapshot")}
        final_health = {"background_error": server._health_error, "persistence": server._store.health}
        effects = workload.ledger.summary()
        endpoint_errors = {p.client_id: p.errors for p in workload.peers if p.errors}
        validation = (bool(phases) and phases[-1]["phase"] == "measure" and not run_error
            and all(p["terminal_count_matches_offers"] for p in phases)
            and not endpoint_errors and not shutdown_error and not workload.state_mismatches
            and not any(responses[name] for name in ("missing_responses", "missing_acceptance_acks",
                "duplicate_responses", "unexpected_responses", "mismatched_correlations"))
            and not any(effects[name] for name in ("missing_effects", "duplicate_effects", "mismatched_effects", "unexpected_effects"))
            and not any(persistence_result[name] for name in ("missing", "mismatched", "ephemeral_in_snapshot"))
            and final_health["background_error"] is None and final_health["persistence"]["healthy"])
        after_stop = {"ingress_messages": server._worker_pool.stats.queue_depth,
            "ingress_bytes": server._worker_pool.stats.queue_bytes, "egress_bytes": server._outbox_bytes,
            "fanout_messages": len(server._fanout_queue), "fanout_bytes": server._fanout_bytes,
            "routes": server._route_count, "connections": server._connection_count,
            "workers": server._worker_pool.stats.active_workers,
            "tasks": len(asyncio.all_tasks()), "threads": threading.active_count()}
        validation = validation and not any(after_stop[name] for name in (
            "ingress_messages", "ingress_bytes", "egress_bytes", "fanout_messages", "fanout_bytes", "routes", "connections", "workers"))
        clean = validation and not any(p["counts"][name] for p in phases for name in ("fail", "timeout", "rejected"))
        result = {"settings": asdict(settings), "repetition": repetition, "tui": False,
            "server_config": {key: str(value) if isinstance(value, Path) else value for key, value in asdict(config).items()},
            "bound_tcp_port": port, "bound_websocket_port": websocket_port,
            "phases": phases, "responses": dict(responses), "effects": effects,
            "state_value_or_version_mismatches": workload.state_mismatches, "endpoint_errors": endpoint_errors,
            "buffer_write_attempts": workload.buffer_writes, "persistent_write_attempts": workload.persistent_writes,
            "daemon_metrics_setup_warmup_measure_and_drain": metrics,
            "sampled_peaks_setup_warmup_measure_and_drain": monitor.peaks,
            "loop_lag_seconds": distribution(monitor.lag), "sample_count": monitor.samples,
            "missed_sample_ticks": monitor.missed_ticks, "resource_error": monitor.resource_error,
            "resource_scope": "daemon and generator in one process; peaks are sampled, not exact high-water marks",
            "thread_metric_source": "psutil OS threads" if monitor.process else "threading.active_count Python threads only",
            "health_before_stop": health_before_stop, "health_after_stop": final_health,
            "graceful_snapshot_verification": persistence_result, "shutdown_error": shutdown_error,
            "run_error": run_error, "post_stop_resources": after_stop,
            "validation_passed": validation, "clean_envelope": clean}
    result["temporary_data_removed"] = True
    return result


def dependency_version(name: str) -> Optional[str]:
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return None


def environment() -> dict:
    return {"os": platform.platform(), "architecture": platform.machine(),
            "python": platform.python_version(), "python_implementation": platform.python_implementation(),
            "python_executable": sys.executable, "websockets": dependency_version("websockets"),
            "latzero_server": dependency_version("latzero-server"), "psutil": dependency_version("psutil"),
            "latency_clock": "time.perf_counter (monotonic)",
            "latency_clock_resolution_seconds": time.get_clock_info("perf_counter").resolution,
            "command": [sys.executable] + getattr(sys, "orig_argv", [sys.executable] + sys.argv)[1:]}


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", default="both", help="closed, open, both, or comma list")
    parser.add_argument("--workload", default="mixed", help="mixed, set_get, rpc, fanout, or comma list")
    for name, default in (("clients", "2"), ("outstanding", "1"), ("workers", "2"),
                          ("rate", "100"), ("payload-bytes", "32"), ("fanout", "2"),
                          ("persistence-fraction", "0.25")):
        parser.add_argument("--" + name, default=default, help="value or comma-separated sweep")
    parser.add_argument("--duration", type=float, default=1)
    parser.add_argument("--warmup", type=float, default=0)
    parser.add_argument("--timeout", type=float, default=2, help="end-to-end seconds from issue (scheduled issue in open mode)")
    parser.add_argument("--max-operations", type=int, default=10000, help="finite per-phase history/operation ceiling")
    parser.add_argument("--sample-interval", type=float, default=0.02)
    parser.add_argument("--handler-delay", type=float, default=0, help="controlled I/O delay in the single raw callee")
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--websocket-enabled", action="store_true", help="also start an unused isolated WS listener on port 0")
    args = parser.parse_args(argv)
    try:
        modes = list(dict.fromkeys(mode for item in args.mode.split(",")
                                 for mode in (("closed", "open") if item == "both" else (item,))))
        dimensions = {"mode": modes, "workload": args.workload.split(",")}
        for name in ("clients", "outstanding", "workers", "rate", "payload_bytes", "fanout", "persistence_fraction"):
            convert = float if name in ("rate", "persistence_fraction") else int
            dimensions[name] = [convert(item) for item in getattr(args, name).split(",")]
        run_count = math.prod(len(values) for values in dimensions.values()) * args.repeat
        if not 1 <= args.repeat <= 32 or not 1 <= run_count <= LIMITS["sweep_runs"]:
            raise ValueError("sweep including repeats must contain 1..32 runs")
        settings = [Settings(**dict(zip(dimensions, values)), duration=args.duration, warmup=args.warmup,
            timeout=args.timeout, max_operations=args.max_operations, sample_interval=args.sample_interval,
            handler_delay=args.handler_delay, websocket_enabled=args.websocket_enabled)
            for values in itertools.product(*dimensions.values())]
        for item in settings:
            item.validate()
    except ValueError as exc:
        parser.error(str(exc))

    async def run() -> list:
        results = []
        for item in settings:
            for repetition in range(1, args.repeat + 1):
                results.append(await run_one(item, repetition))
        return results

    results = asyncio.run(run())
    print(json.dumps({"schema_version": 1, "purpose": "bounded harness smoke/measurement, not capacity or improvement evidence",
        "baseline": "No pre-change performance baseline was measured.", "environment": environment(),
        "harness_safety_ceilings": LIMITS,
        "payload_semantics": "ASCII padding bytes; unique token and protocol envelope are additional",
        "scope": ["raw TCP NDJSON", "set/get with unique values and ephemeral/persistent replacements",
                  "two-phase app/process echo RPC", "subscription updates and event broadcast observed at recipients"],
        "remaining_gates": ["independent-process generator and pre-change baseline", "hot/quiet pool fairness",
            "CPU handler cost, churn, slow peers, recovery and soak", "actual SDK/WS load and runtime matrix",
            "exact high-water metrics, egress wait and persistence lag instrumentation", "measured operating envelope"],
        "runs": results}, indent=2, allow_nan=False))
    return 0 if all(result["clean_envelope"] for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
