import asyncio
from dataclasses import replace
import json
import math
from pathlib import Path

import pytest

from examples.benchmark_daemon import (
    BoundedAdmission,
    EffectLedger,
    ResponseTracker,
    Settings,
    percentile,
    run_one,
    run_phase,
    scheduled_count,
)


def test_percentiles_are_explicit_deterministic_interpolations():
    assert percentile([], 99.9) is None
    assert percentile([4], 99.9) == 4
    values = [4, 1, 3, 2]
    assert percentile(values, 50) == 2.5
    assert percentile(values, 95) == pytest.approx(3.85)
    assert percentile(values, 99) == pytest.approx(3.97)
    assert percentile(values, 99.9) == pytest.approx(3.997)
    assert values == [4, 1, 3, 2]
    with pytest.raises(ValueError):
        percentile(values, 101)


@pytest.mark.parametrize("result_first", [True, False])
def test_rpc_typed_correlation_both_orders_and_duplicates(result_first):
    tracker = ResponseTracker(2)
    tracker.begin("origin", "app_result")
    tracker.entries["origin"]["sent"] = True
    assert tracker.receive({"type": "call_app", "request_id": "origin"}) is False
    ack = {"type": "ack", "request_id": "origin"}
    result = {"type": "app_result", "request_id": "origin", "payload": {"request_id": "origin"}}
    for message in ([result, ack] if result_first else [ack, result]):
        assert tracker.receive(message) is (message is result)
    assert not tracker.receive(result)
    assert tracker.summary()["duplicate_responses"] == 1
    assert tracker.summary()["missing_responses"] == 0
    assert tracker.summary()["missing_acceptance_acks"] == 0
    with pytest.raises(ValueError):
        tracker.begin("origin", "ack")


def test_correlation_missing_mismatched_unknown_late_and_capped():
    tracker = ResponseTracker(2)
    tracker.begin("ack-only", "app_result")
    tracker.entries["ack-only"]["sent"] = True
    assert not tracker.receive({"type": "ack", "request_id": "ack-only"})
    assert tracker.summary()["missing_responses"] == 1
    tracker.entries["ack-only"]["expired"] = True
    assert tracker.receive({"type": "app_result", "request_id": "ack-only", "payload": {"request_id": "wrong"}})
    assert tracker.summary()["mismatched_correlations"] == 1
    assert tracker.summary()["late_responses"] == 1
    assert not tracker.receive({"type": "ack", "request_id": "unknown"})
    assert tracker.summary()["unexpected_responses"] == 1
    tracker.begin("unsent", "ack")
    assert tracker.summary()["missing_responses"] == 0
    with pytest.raises(RuntimeError):
        tracker.begin("excess", "ack")


def test_terminal_error_does_not_require_acceptance_ack():
    tracker = ResponseTracker(1)
    tracker.begin("request", "app_result")
    tracker.entries["request"]["sent"] = True
    assert tracker.receive({"type": "error", "request_id": "request"})
    assert tracker.summary()["missing_responses"] == 0
    assert tracker.summary()["missing_acceptance_acks"] == 0
    assert not tracker.receive({"type": "app_result", "request_id": "request"})
    assert tracker.summary()["duplicate_responses"] == 1


@pytest.mark.asyncio
async def test_effect_ledger_checks_unique_value_and_recipient_without_silence():
    ledger = EffectLedger(limit=2)
    value = {"token": "unique", "padding": "xxx"}
    key = ledger.expect("recipient", "event", value, "event")
    ledger.observe("wrong-recipient", "event", value, "event")
    ledger.observe("recipient", "event", {"token": "unique", "padding": "wrong"}, "event")
    ledger.outcomes["unique"] = "timeout"
    assert ledger.summary()["missing_by_outcome"] == {"timeout": 1}
    with pytest.raises(asyncio.TimeoutError):
        await ledger.wait([key], 0)
    ledger.observe("recipient", "event", value, "event")
    await asyncio.wait_for(ledger.wait([key], 0), 1)
    ledger.observe("recipient", "event", value, "event")
    result = ledger.summary()
    assert result["unexpected_effects"] == 1
    assert result["mismatched_effects"] == 1
    assert result["duplicate_effects"] == 1
    assert result["missing_effects"] == 0
    with pytest.raises(RuntimeError):
        ledger.expect("recipient", "event", value, "event")
    ledger.expect("other", "event", value, "event")
    with pytest.raises(RuntimeError):
        ledger.expect("excess", "event", value, "event")


def test_scheduled_offers_and_fixed_lane_overload_are_bounded():
    assert scheduled_count(1, 100, 10000) == 100
    assert scheduled_count(0.025, 100, 10000) == 3
    assert scheduled_count(60, 10000, 7) == 7
    admission = BoundedAdmission(2)
    first, second = admission.take(), admission.take()
    for _ in range(1000):
        assert admission.take() is None
    assert admission.peak == 2
    assert admission.rejected == 1000
    assert len(admission.busy) == 2
    assert len(admission.free) == 0
    admission.release(first)
    assert admission.take() == first
    admission.release(second)
    with pytest.raises(ValueError):
        admission.release(second)


@pytest.mark.asyncio
async def test_open_scheduler_counts_overload_and_latency_from_scheduled_issue(monkeypatch):
    # Gate two fixed lanes while all ten offers arrive; no per-offer task can grow.
    from examples import benchmark_daemon as benchmark

    gate = asyncio.Event()
    ticks = [0.0]
    observed = []

    async def advance(target):
        ticks[0] = target
        await asyncio.sleep(0)
        if target >= 0.9:
            gate.set()

    class ControlledWorkload:
        settings = Settings(mode="open", clients=2, rate=10, duration=1, max_operations=10)
        ledger = EffectLedger(10)

        def kind(self, sequence):
            return "set_get"

        async def execute(self, lane, sequence, phase, scheduled):
            observed.append((lane, sequence, scheduled))
            await gate.wait()

    monkeypatch.setattr(benchmark, "clock", lambda: ticks[0])
    monkeypatch.setattr(benchmark, "sleep_until", advance)
    result = await asyncio.wait_for(run_phase(ControlledWorkload(), 1, "measure"), 2)
    assert result["counts"] == {"offered": 10, "admitted": 2, "success": 2, "fail": 0,
                                "timeout": 0, "rejected": 8, "scheduler_rejected": 8, "scheduler_expired": 0}
    assert observed == [(0, 0, 0), (1, 1, 0.1)]
    assert result["fixed_lane_tasks"] == result["in_flight_peak"] == 2
    assert result["terminal_count_matches_offers"]
    assert result["latency_seconds"]["success"]["p50"] == pytest.approx(0.85)


@pytest.mark.asyncio
async def test_scheduler_expiration_is_not_silently_omitted(monkeypatch):
    from examples import benchmark_daemon as benchmark

    ticks = [0.0]

    async def delayed(target):
        ticks[0] = target + 0.5

    class ExpiredWorkload:
        settings = Settings(mode="open", clients=1, rate=10, duration=1, timeout=0.1, max_operations=10)
        ledger = EffectLedger(10)

        def kind(self, sequence):
            return "set_get"

        async def execute(self, *args):
            raise AssertionError("expired scheduled work must not be issued")

    monkeypatch.setattr(benchmark, "clock", lambda: ticks[0])
    monkeypatch.setattr(benchmark, "sleep_until", delayed)
    result = await asyncio.wait_for(run_phase(ExpiredWorkload(), 1, "measure"), 2)
    assert result["counts"]["offered"] == result["counts"]["timeout"] == result["counts"]["scheduler_expired"] == 10
    assert result["counts"]["admitted"] == 0
    assert result["latency_seconds"]["timeout"]["p50"] == 0.5


@pytest.mark.parametrize("change", [
    {"duration": float("inf")}, {"rate": float("nan")}, {"clients": 0},
    {"outstanding": 17}, {"clients": 32, "outstanding": 16}, {"workers": True},
    {"payload_bytes": 65537}, {"fanout": 17}, {"persistence_fraction": -0.1},
    {"warmup": 31}, {"timeout": 0}, {"max_operations": 10001},
    {"sample_interval": 0.001}, {"clients": 32, "fanout": 16},
])
def test_settings_reject_unsafe_or_nonfinite_envelopes(change):
    with pytest.raises(ValueError):
        replace(Settings(), **change).validate()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["closed", "open"])
async def test_small_temp_daemon_integration_has_exact_rpc_and_fanout(mode):
    settings = Settings(mode=mode, duration=1, clients=2, workers=2, rate=8,
                        max_operations=8, fanout=2, persistence_fraction=0.5)
    result = await asyncio.wait_for(run_one(settings), 12)
    measured = result["phases"][-1]
    assert result["validation_passed"], result
    assert result["clean_envelope"], result
    assert measured["counts"]["offered"] == 8
    assert measured["counts"]["admitted"] == measured["counts"]["success"] == 8
    assert all(measured["successful_per_method_latency_seconds"][name]["count"] == 2
               for name in ("set_get", "app_rpc", "process_rpc", "fanout"))
    assert all(math.isfinite(measured["latency_seconds"]["success"][name])
               for name in ("p50", "p95", "p99", "p99.9"))
    assert measured["fixed_lane_tasks"] == 2
    assert measured["additional_backlog_limit"] == 0
    assert measured["reserved_lane_mailbox_capacity"] == (2 if mode == "open" else 0)
    assert measured["latency_origin"] == ("scheduled_issue" if mode == "open" else "issue")
    assert result["responses"]["missing_responses"] == 0
    assert result["responses"]["duplicate_responses"] == 0
    assert result["effects"]["missing_effects"] == 0
    assert result["effects"]["duplicate_effects"] == 0
    assert result["effects"]["expected"] > 4
    assert result["server_config"]["port"] == 0
    assert result["server_config"]["websocket_enabled"] is False
    assert result["bound_tcp_port"] > 0
    assert result["tui"] is False
    assert result["temporary_data_removed"]
    assert not Path(result["server_config"]["data_dir"]).exists()
    assert result["health_after_stop"]["persistence"]["healthy"]
    assert result["health_after_stop"]["persistence"]["dirty_pools"] == 0
    assert result["sampled_peaks_setup_warmup_measure_and_drain"]["workers"] == 2
    assert result["sampled_peaks_setup_warmup_measure_and_drain"]["connections"] == 5
    assert result["buffer_write_attempts"] == 4
    assert result["persistent_write_attempts"] == 2
    assert all(result["post_stop_resources"][name] == 0 for name in (
        "ingress_messages", "ingress_bytes", "egress_bytes", "fanout_messages", "fanout_bytes", "routes", "connections", "workers"))


@pytest.mark.asyncio
async def test_cli_comma_sweep_repeats_exact_settings_without_real_load(monkeypatch, capsys):
    from examples import benchmark_daemon as benchmark

    async def record(settings, repetition):
        return {"settings": benchmark.asdict(settings), "repetition": repetition, "clean_envelope": True}

    monkeypatch.setattr(benchmark, "run_one", record)
    monkeypatch.setattr(benchmark, "environment", lambda: {"os": "deterministic-cli-test"})
    loop = asyncio.get_running_loop()
    code = await asyncio.wait_for(loop.run_in_executor(None, lambda: benchmark.main([
        "--mode", "both", "--clients", "1,2", "--workers", "2", "--rate", "8",
        "--repeat", "2", "--duration", "0.1", "--max-operations", "4"])), 3)
    assert code == 0
    document = json.loads(capsys.readouterr().out)
    runs = document["runs"]
    assert len(runs) == 8
    assert [(r["settings"]["mode"], r["settings"]["clients"], r["repetition"]) for r in runs] == [
        (mode, clients, repetition) for mode in ("closed", "open") for clients in (1, 2) for repetition in (1, 2)]
    assert all(r["settings"]["duration"] == 0.1 and r["settings"]["rate"] == 8 for r in runs)
    assert all(runs[i]["settings"] == runs[i + 1]["settings"] for i in range(0, 8, 2))
    assert document["baseline"] == "No pre-change performance baseline was measured."
