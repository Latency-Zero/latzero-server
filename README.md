# latzero-server

`latzero-server` is the local TCP daemon for LatZero server mode.

It listens on `127.0.0.1:14130` by default, manages named pools, routes
app-to-app calls, tracks subscriptions, and persists selected buffers to disk.

## Quick Start

Python 3.8 or newer is supported. Install the daemon in an isolated environment:

```bash
python -m pip install .
```

The default command runs the daemon without loading optional dashboard packages:

```bash
latzero-server
```

`--headless` explicitly selects the same mode. Both modes retain startup output
and error diagnostics, including on Windows; use normal service-manager or shell
output redirection when running unattended. Stop with Ctrl+C.

Install the optional TUI dependencies and select the interactive dashboard:

```bash
python -m pip install ".[tui]"
latzero-server --tui
```

`--tui` and `--headless` are mutually exclusive. A missing TUI dependency is
reported before the daemon opens listeners. The exported
`from latzero_server import ServerDashboard` remains available with the TUI extra.

WebSocket clients without an Origin header are allowed by default, while browser
origins must be explicitly configured. Allow an exact browser origin with
`--ws-origin http://localhost:8080`; repeat the flag for multiple origins. A
file-page demo sends the literal origin `null` and requires `--ws-origin null`.
This string is distinct from a missing Origin header and is not a blanket trust
policy. Origin checks do not protect against a malicious same-user native
process. Use `--ws-port` to select a separate port or `--no-ws` to disable WS.

## Stabilization Notes

This release corrects earlier CLI behavior: bare `latzero-server` no longer
launches the TUI, `--tui` now selects it explicitly, and Windows headless mode no
longer detaches the console or discards stdout/stderr. Automation that needs a
dashboard must add `--tui`.

The daemon coordinates local clients; it is not a durable job queue or a
multi-daemon shared-state service. Acceptance ACKs do not guarantee application
completion or crash-safe persistence. Snapshot persistence is eventual; keep
backups before storage migration and do not assume acknowledged writes survive
a crash. Live notifications are not a durable replay log. Capacity and latency
claims require measured workloads and recorded runtime/configuration settings.

Server dispatch now uses bounded per-connection FIFOs with one active handler per
session. TCP and WS each use one ordered writer, finite write deadlines, and
reserved control-response capacity. Overload is an explicit protocol error or
connection failure, never a skipped notification followed by a success ACK.
The shared connection budget includes pending WS handshakes. A new WS
connection beyond that budget receives HTTP 503 before the WS upgrade; TCP
connections receive `server_busy`. Pending handshakes have a finite opening
deadline and are counted in dashboard health.
`emit_event` can partially accept a broadcast; a `partial_delivery` error lists
accepted and failed destinations. Do not automatically retry it. Buffer-write
ACKs confirm state commitment, not delivery of every subscription notification;
disconnected subscribers must explicitly reconnect and re-read state.

Direct and self RPCs complete with `app_result` or a terminal `error`, not the
acceptance ACK. Explicit third-party calls return acceptance metadata to the
caller and publish results to the designated recipient. Public result envelopes
retain the caller's ID and add `payload.request_id`; incoming worker hop IDs are
opaque and must be echoed unchanged. Only the actual designated callee can
complete a route. Broadcast child results have independent IDs and additive
`parent_request_id` metadata. Client identities are stable for a connection;
same-identity rejoin is idempotent and pool switching removes old registrations.

Wire TTLs and RPC timeouts are fractional seconds. RPCs without a deadline use
the finite `ServerConfig.rpc_timeout` default (30 seconds). JavaScript's
documented `autoClean` and API timeout values are milliseconds and are now
converted to seconds; code compensating for the old unit bug must be updated.
Accepted calls that time out or disconnect may already have produced effects.
No effectful request is automatically replayed.

Persistent snapshots use exact-identity SHA-256 filenames and unique temporary
files. Verified legacy snapshots remain readable and are preserved as backups;
canonical snapshots take precedence, including empty tombstones that prevent
old values from reappearing. Snapshot copies are owned by the event loop before
executor I/O. Graceful stop includes dirty and already-captured writes, storage
failures are reported with bounded retries, and restored TTLs are scheduled and
checked on reads. This still provides no WAL, fsync ACK, or crash-loss guarantee.

Protective limits are configurable through `ServerConfig`, independently of
dispatch-worker tuning. Defaults are not a measured operating envelope:

| Resource | Default |
| --- | --- |
| TCP/WS application frame | 1 MiB |
| Session queued/active ingress | 256 messages / 1 MiB |
| Global queued/active ingress | 8192 messages / 32 MiB |
| Session egress | 256 messages / 1 MiB |
| Global egress | 64 MiB |
| Ingress/egress control reserve | 32 messages / 64 KiB |
| Pool state estimate | 64 MiB |
| Pools / buffers per pool | 1024 / 4096 |
| Subscriptions / processes per pool | 16384 / 4096 |
| Routes per session / pool / daemon | 256 / 4096 / 16384 |
| Ordered fanout backlog | 4096 updates / 32 MiB |
| Write / shutdown / initial join deadline | 5 / 5 / 10 seconds |

Ingress byte accounting includes wire frames, not exact decoded Python-object
memory. Buffer-state accounting adds conservative metadata overhead. Measure
RSS and object overhead for the selected payloads; these counters alone are not
a memory-capacity guarantee. Persistence has a separate bounded flush deadline
and cannot preempt a stuck filesystem operation; a timed-out writer stays owned
and blocks unsafe restart. User CPU-bound callbacks cannot be preempted by
asyncio or Python threads.

## Isolated Validation

Run server correctness, transport, concurrency, storage, and measurement tests:

```bash
python -m pytest tests
```

Network fixtures use separate OS-assigned TCP/WS ports and temporary snapshot
directories. `tests/test_client_interop.py` also exercises the sibling Node
ESM/CJS, browser script, and Python daemon SDKs against the real daemon. Those
checks require sibling checkouts and a Node runtime with native WebSocket;
otherwise they are explicitly skipped. The browser script runs under a Node VM
using real WS connections, not in a browser engine, so real-browser validation
remains a release gate. The browser repository has its own deterministic mock
suite; demos are not a substitute for automated tests.

Start a small raw TCP measurement from the server checkout:

```bash
python examples/benchmark_daemon.py --mode both --duration 1 --clients 2 --workers 2 --rate 100
```

The harness bounds its own task/history/admission budgets, starts a temporary
daemon, and reports terminal completions, scheduled open-loop latency,
rejections, missing/duplicate effects, snapshot fidelity, and sampled resource
peaks as JSON. Most workload settings accept comma-separated sweeps; `--repeat`
and `--warmup` record reproducible repetitions. An exit code of 1 reports errors
or a non-clean envelope, including explicit load-generator admission rejections.
`validation_passed` separates correlation/effect correctness from that clean
envelope. One run shares a process/event loop with its load generator, so its
throughput and resources are not independent daemon capacity measurements.
Without optional `psutil`, RSS and native handle counts are null.

No pre-change performance baseline or product SLO was measured. Long soak,
hot/quiet-pool fairness, churn, slow peers, CPU costs, independent-process
generators, real SDK/WS load, exact high-water instrumentation, and minimum
runtime execution remain rollout gates. Python 3.8 syntax and minimum-compatible
APIs are preserved; grammar checks are not proof of running on Python 3.8.

## Standalone Builds

Prepare build dependencies explicitly in an isolated environment. `build.py`
does not install or upgrade packages:

```bash
python -m pip install ".[tui]" pyinstaller
python build.py
python build.py --onedir
```

For reproducible release builds, select reviewed dependency version pins and
record the Python version and `python -m pip freeze` output. Python 3.8 remains
the runtime minimum; use dependency versions that support the selected Python.

Build paths are anchored to the checkout, not the current working directory.
Each build owns unique temporary icon/spec/work paths, removed even on failure.
`--clean` removes only `build/` and `dist/`, never the checked-in spec or an icon
beside the source PNG. Console output is always retained; `--console` is accepted
but is equivalent to the default. The checked-in spec is also relocatable:

```bash
python -m PyInstaller latzero-server.spec
```

Builds bundle both PNG resources, README/project attribution, and metadata with
the installed dependencies' license material. A project `LICENSE` or
`LICENSE.txt` is bundled when present; currently the project's MIT declaration
and author attribution are in `pyproject.toml`. Verify release license material
before distribution.

Validate the resulting executable from a directory outside the checkout:
check `--help`, run default/`--headless` with a temporary `--data-dir` and unused
port, and verify `--tui` and its logo in an interactive terminal. Use temporary
data for smoke tests rather than the default snapshot cache. Signing and a
multi-platform release policy remain separate release decisions.

Run focused startup and packaging regressions with
`python -m pytest tests/test_packaging.py`. These tests use isolated imports, daemon/build doubles,
and temporary paths; executable and interactive-dashboard smoke tests are
separate release checks.

## Features

- Named open or auth-guarded pools
- One connected pool per client session
- JSON-over-TCP protocol
- Targeted app calls and third-party response routing
- Explicit buffer subscriptions
- Optional per-buffer persistence via JSON snapshots

## Protocol Overview

Each line on the socket is one JSON object with:

- `type`
- `request_id`
- `client_id`
- `pool`
- `payload`

Important message types:

- `join_pool`
- `switch_pool`
- `set_buffer`
- `get_buffer`
- `subscribe_buffer`
- `call_app`
- `app_result`
- `emit_event`
- `presence_update`
- `buffer_update`
- `ack`
- `error`

## Dashboard Controls

When you launch with `--tui`:

- `tab` / `shift+tab`: switch panes
- `j` / `k` or arrow keys: move selection
- `p`, `c`, `b`, `e`: jump to pools, clients, buffers, events
- `r`: refresh
- `q`: quit

## Client Mode

Use the `LatZero` client from `python-client`:

```python
from latzero import LatZero

client = LatZero("latzero://client-1", pool="demo")
client.set("buffer", {"hello": "world"}, persistent=True)
```
