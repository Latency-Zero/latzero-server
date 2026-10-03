"""Local process pods with stable pool ownership and redirect-only ingress."""

import asyncio
import hashlib
import json
import math
import os
import sys
import threading
import uuid
from collections import deque
from contextlib import suppress
from dataclasses import asdict, dataclass, field
from functools import partial
from pathlib import Path
from typing import Any, Dict, List, Optional

from websockets.exceptions import ConnectionClosed
from websockets.legacy.server import serve

from .config import ServerConfig
from .directory_lock import DataDirectoryLock
from .protocol import decode_message, encode_message


_HOST = "127.0.0.1"
_PROTOCOL = "pool_redirect_v1"
_CONTROL_LIMIT = 64 * 1024
_STOP_GRACE = 20.0
_REAP_TIMEOUT = 2.0


def _pod_count(pods: int) -> int:
    if type(pods) is not int or not 1 <= pods <= 64:
        raise ValueError("pods must be an integer from 1 to 64")
    return pods


def _identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 512:
        raise ValueError("{} must be a nonempty string up to 512 UTF-8 bytes".format(name))
    return value


def _port(value: Any, name: str, optional: bool = False) -> Optional[int]:
    if optional and value is None:
        return None
    if type(value) is not int or not 1 <= value <= 65535:
        raise ValueError("{} must be an integer port from 1 to 65535".format(name))
    return value


def pool_owner(pool: str, pods: int) -> int:
    """Hash the exact, unnormalized UTF-8 pool identifier."""
    _pod_count(pods)
    if not isinstance(pool, str) or not pool:
        raise ValueError("pool must be a nonempty string")
    return int.from_bytes(hashlib.sha256(pool.encode("utf-8")).digest(), "big") % pods


class PoolRouting:
    """Immutable ownership count with a copied, loopback-only endpoint table."""

    __slots__ = ("_pod_index", "_pod_count", "_table", "_router", "_cluster_id")

    def __init__(self, pod_index: int, pod_count: int):
        self._pod_count = _pod_count(pod_count)
        if type(pod_index) is not int or not 0 <= pod_index < pod_count:
            raise ValueError("pod_index must be from 0 to pod_count - 1")
        self._pod_index = pod_index
        self._table = None
        self._router = None
        self._cluster_id = None

    @property
    def pod_index(self) -> int:
        return self._pod_index

    @property
    def pod_count(self) -> int:
        return self._pod_count

    @property
    def configured(self) -> bool:
        return self._table is not None

    def owns(self, pool: str) -> bool:
        return pool_owner(pool, self.pod_count) == self.pod_index

    def configure(self, endpoints: List[dict], router_host: str, router_port: int,
                  router_ws_port: Optional[int], cluster_id: str) -> None:
        if not isinstance(endpoints, list) or len(endpoints) != self.pod_count:
            raise ValueError("endpoints must contain exactly pod_count entries")
        table = []
        for endpoint in endpoints:
            if not isinstance(endpoint, dict) or endpoint.get("host") != _HOST:
                raise ValueError("Pod endpoints must use 127.0.0.1")
            table.append((_port(endpoint.get("port"), "port"), _port(endpoint.get("ws_port"), "ws_port", True)))
        if router_host != _HOST:
            raise ValueError("router_host must be 127.0.0.1")
        router = (_port(router_port, "router_port"), _port(router_ws_port, "router_ws_port", True))
        _identifier(cluster_id, "cluster_id")
        self._table = tuple(table)
        self._router = router
        self._cluster_id = cluster_id

    def redirect_payload(self, pool: str) -> dict:
        if self._table is None:
            raise RuntimeError("Pod routing has not been configured")
        owner = pool_owner(pool, self.pod_count)
        port, ws_port = self._table[owner]
        return {
            "protocol": _PROTOCOL, "host": _HOST, "port": port, "ws_port": ws_port,
            "pool": pool, "pod_index": owner, "pod_count": self.pod_count,
            "router_host": _HOST, "router_port": self._router[0],
            "router_ws_port": self._router[1], "cluster_id": self._cluster_id,
        }


@dataclass
class PodChild:
    index: int
    process: Any = None
    pid: Optional[int] = None
    port: Optional[int] = None
    ws_port: Optional[int] = None
    status: str = "starting"
    error: Optional[str] = None
    stats: dict = field(default_factory=dict)
    stderr_tail: Any = field(default_factory=lambda: deque(maxlen=8), repr=False)
    stderr_bytes: int = 0
    _ready: Any = field(default=None, repr=False)
    _configured: Any = field(default=None, repr=False)
    _stopped: Any = field(default=None, repr=False)
    _stdout_task: Any = field(default=None, repr=False)
    _stderr_task: Any = field(default=None, repr=False)
    _exit_task: Any = field(default=None, repr=False)
    _creation_task: Any = field(default=None, repr=False)
    _control_lock: Any = field(default=None, repr=False)
    _stats_pending: bool = field(default=False, repr=False)
    _stats_requested: Any = field(default=None, repr=False)
    _stdout_eof: bool = field(default=False, repr=False)
    _stderr_eof: bool = field(default=False, repr=False)


@dataclass
class _RouterSession:
    writer: Any
    deadline: float
    client_id: Optional[str] = None
    redirect_supported: bool = False
    frames: int = 0
    closing: bool = False
    reader_task: Any = None
    close_task: Any = None


def _control_frame(message: dict) -> bytes:
    frame = encode_message(message)
    if len(frame) > _CONTROL_LIMIT:
        raise ValueError("Pod control frame exceeds 64 KiB")
    return frame


class _WindowsProcess:
    def __init__(self, pid: int, terminate: bool = False):
        import ctypes
        from ctypes import wintypes

        self.pid = pid
        self.kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        self.kernel.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
        self.kernel.OpenProcess.restype = wintypes.HANDLE
        self.kernel.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
        self.kernel.WaitForSingleObject.restype = wintypes.DWORD
        self.kernel.GetExitCodeProcess.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
        self.kernel.GetExitCodeProcess.restype = wintypes.BOOL
        self.kernel.TerminateProcess.argtypes = (wintypes.HANDLE, wintypes.UINT)
        self.kernel.TerminateProcess.restype = wintypes.BOOL
        self.kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
        self.kernel.CloseHandle.restype = wintypes.BOOL
        self.handle = self.kernel.OpenProcess(0x00100000 | 0x1000 | int(terminate), False, pid)
        if not self.handle:
            raise OSError(ctypes.get_last_error(), "Cannot observe process {}".format(pid))

    def alive(self) -> bool:
        result = self.kernel.WaitForSingleObject(self.handle, 0)
        if result not in (0, 258):
            raise OSError("Cannot determine process liveness")
        return result == 258

    @property
    def returncode(self) -> Optional[int]:
        if self.alive():
            return None
        from ctypes import byref
        from ctypes.wintypes import DWORD

        code = DWORD()
        if not self.kernel.GetExitCodeProcess(self.handle, byref(code)):
            raise OSError("Cannot determine process exit code")
        return int(code.value)

    def terminate(self) -> None:
        if self.alive() and not self.kernel.TerminateProcess(self.handle, 1):
            raise OSError("Cannot terminate process {}".format(self.pid))

    def close(self) -> None:
        if self.handle is not None:
            self.kernel.CloseHandle(self.handle)
            self.handle = None


def _windows_parents() -> Dict[int, int]:
    import ctypes
    from ctypes import wintypes

    class ProcessEntry(ctypes.Structure):
        _fields_ = [("dwSize", wintypes.DWORD), ("cntUsage", wintypes.DWORD), ("pid", wintypes.DWORD),
                    ("heap", ctypes.c_size_t), ("module", wintypes.DWORD), ("threads", wintypes.DWORD),
                    ("parent", wintypes.DWORD), ("priority", wintypes.LONG), ("flags", wintypes.DWORD),
                    ("exe", wintypes.WCHAR * 260)]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateToolhelp32Snapshot.argtypes = (wintypes.DWORD, wintypes.DWORD)
    kernel.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    for name in ("Process32FirstW", "Process32NextW"):
        function = getattr(kernel, name)
        function.argtypes = (wintypes.HANDLE, ctypes.POINTER(ProcessEntry))
        function.restype = wintypes.BOOL
    kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel.CloseHandle.restype = wintypes.BOOL
    snapshot = kernel.CreateToolhelp32Snapshot(2, 0)
    if snapshot == wintypes.HANDLE(-1).value:
        raise OSError("Cannot inspect pod process ancestry")
    parents = {}
    try:
        entry = ProcessEntry()
        entry.dwSize = ctypes.sizeof(entry)
        present = kernel.Process32FirstW(snapshot, ctypes.byref(entry))
        while present and len(parents) < 65536:
            parents[int(entry.pid)] = int(entry.parent)
            present = kernel.Process32NextW(snapshot, ctypes.byref(entry))
    finally:
        kernel.CloseHandle(snapshot)
    return parents


def _windows_descendant(pid: int, ancestor: int) -> bool:
    parents = _windows_parents()
    for _ in range(16):
        if pid == ancestor:
            return True
        pid = parents.get(pid)
        if pid is None:
            break
    return False


class _PodProcess:
    """Keep the venv launcher and actual Windows runtime as one child lease."""

    def __init__(self, process: Any):
        self.launcher = process
        self.runtime = None
        self._extra_runtimes = {}
        self._pid = process.pid
        self._runtime_code = None

    @property
    def pid(self) -> int:
        return self._pid

    @property
    def returncode(self) -> Optional[int]:
        if any(handle.alive() for handle in self._extra_runtimes.values()):
            return None
        if self._runtime_code is not None:
            return (self._runtime_code or self.launcher.returncode) if self.launcher.returncode is not None else None
        if self.runtime is not None:
            code = self.runtime.returncode
            if code is None or self.launcher.returncode is None:
                return None
            return code or self.launcher.returncode
        return self.launcher.returncode

    def attach_runtime(self, pid: int) -> None:
        if type(pid) is not int or not 0 < pid <= 0xFFFFFFFF:
            raise ValueError("Invalid pod runtime PID")
        if pid != self.launcher.pid and (os.name != "nt" or not _windows_descendant(pid, self.launcher.pid)):
            raise ValueError("Pod runtime is not a child of its launcher")
        if os.name == "nt" and (self.runtime is None or self.runtime.pid != pid):
            if self.runtime is not None:
                self._extra_runtimes[self.runtime.pid] = self.runtime
            self.runtime = self._extra_runtimes.pop(pid, None) or _WindowsProcess(pid, terminate=True)
        self._pid = pid

    def discover_runtimes(self) -> None:
        if os.name != "nt":
            return
        parents = _windows_parents()
        descendants = {self.launcher.pid}
        for _ in range(16):
            found = {pid for pid, parent in parents.items() if parent in descendants}
            if found.issubset(descendants):
                break
            descendants.update(found)
        descendants.discard(self.launcher.pid)
        for pid in descendants:
            if self.runtime is not None and pid == self.runtime.pid or pid in self._extra_runtimes:
                continue
            try:
                handle = _WindowsProcess(pid, terminate=True)
            except OSError:
                if pid in _windows_parents():
                    raise
                continue
            if self.runtime is None:
                self.runtime = handle
                self._pid = pid
            else:
                self._extra_runtimes[pid] = handle

    def __getattr__(self, name: str) -> Any:
        return getattr(self.launcher, name)

    async def wait(self) -> int:
        await self.launcher.wait()
        self.discover_runtimes()
        while (self.runtime is not None and self.runtime.alive()
               or any(handle.alive() for handle in self._extra_runtimes.values())):
            await asyncio.sleep(0.05)
        return self.returncode

    def terminate(self) -> None:
        self.discover_runtimes()
        if self.runtime is not None:
            self.runtime.terminate()
        for handle in self._extra_runtimes.values():
            handle.terminate()
        if self.launcher.returncode is None:
            self.launcher.terminate()

    def kill(self) -> None:
        self.discover_runtimes()
        if self.runtime is not None:
            self.runtime.terminate()
        for handle in self._extra_runtimes.values():
            handle.terminate()
        if self.launcher.returncode is None:
            self.launcher.kill()

    def release_handle(self) -> None:
        if self.runtime is not None and self.returncode is not None:
            self._runtime_code = self.runtime.returncode
            self.runtime.close()
            self.runtime = None
        if self.returncode is not None:
            for handle in self._extra_runtimes.values():
                handle.close()
            self._extra_runtimes.clear()


class PodSupervisor:
    """Own N real brokers and a bounded, stateless public redirect router."""

    def __init__(self, config: ServerConfig, pods: int, startup_timeout: float = 30.0):
        config.validate()
        if config.host != _HOST:
            raise ValueError("Process pods only support host 127.0.0.1")
        self._pod_count = _pod_count(pods)
        if type(startup_timeout) not in (int, float) or not math.isfinite(startup_timeout) or startup_timeout <= 0:
            raise ValueError("startup_timeout must be finite and positive")
        self.config = config
        self.startup_timeout = float(startup_timeout)
        self.children: List[PodChild] = []
        self.tcp_port: Optional[int] = None
        self.ws_port: Optional[int] = None
        self.cluster_id: Optional[str] = None
        self._routing = None
        self._tcp_server = None
        self._websocket_server = None
        self._directory_lock = None
        self._lifecycle_lock = None
        self._stop_task = None
        self._failure_task = None
        self._health_task = None
        self._closed = None
        self._started = False
        self._stopping = False
        self._accepting = False
        self._health_error = None
        self._sessions: Dict[int, _RouterSession] = {}
        self._pending_websockets: Dict[int, Any] = {}
        self._connection_count = 0
        self._readers = set()
        self._creations = set()
        self._spawns = set()
        self._metrics = {"received": 0, "redirects": 0, "overload_rejections": 0}

    @property
    def pods(self) -> int:
        return self._pod_count

    def pool_owner(self, pool: str) -> int:
        return pool_owner(pool, self.pods)

    async def start(self) -> None:
        if self._lifecycle_lock is None:
            self._lifecycle_lock = asyncio.Lock()
        async with self._lifecycle_lock:
            if self._started and self._accepting:
                return
            if self._stop_task is not None and not self._stop_task.done():
                raise RuntimeError("Previous pods are still stopping")
            if self._spawns or self._creations or self._directory_lock is not None or any(
                child.process is not None and (child.process.returncode is None or not child._stdout_eof or not child._stderr_eof)
                for child in self.children
            ):
                raise RuntimeError("Previous pods have not been reaped")
            if self._readers or self._sessions:
                raise RuntimeError("Previous router sessions are still closing")
            self.config.validate()
            if self.config.host != _HOST:
                raise ValueError("Process pods only support host 127.0.0.1")
            child_config = asdict(self.config)
            child_config.update(host=_HOST, port=0, websocket_port=0, data_dir=str(self.config.data_dir.resolve()))
            _control_frame({"config": child_config, "index": 0, "pods": self.pods, "parent_pid": os.getpid()})
            self._stopping = False
            self._stop_task = None
            self._health_error = None
            self._closed = asyncio.Event()
            self.cluster_id = uuid.uuid4().hex
            self.children = []
            deadline = asyncio.get_running_loop().time() + self.startup_timeout
            try:
                self._directory_lock = DataDirectoryLock(self.config.data_dir).acquire()
                await asyncio.wait_for(self._bind_router(), self._remaining(deadline))
                loop = asyncio.get_running_loop()
                for index in range(self.pods):
                    child = PodChild(index=index, _ready=loop.create_future(), _configured=loop.create_future(),
                                     _stopped=loop.create_future(), _control_lock=asyncio.Lock())
                    # A startup failure can leave another child's future unused.
                    for future in (child._ready, child._configured):
                        future.add_done_callback(lambda done: None if done.cancelled() else done.exception())
                    self.children.append(child)
                for child in self.children:
                    task = asyncio.create_task(self._spawn(child, child_config, deadline), name="latzero-pod-spawn-{}".format(child.index))
                    self._spawns.add(task)

                    def spawn_done(done):
                        self._spawns.discard(done)
                        if not done.cancelled():
                            done.exception()

                    task.add_done_callback(spawn_done)
                await asyncio.wait_for(asyncio.gather(*list(self._spawns)), self._remaining(deadline))
                await asyncio.wait_for(asyncio.gather(*(asyncio.shield(child._ready) for child in self.children)),
                                       self._remaining(deadline))
                endpoints = [{"host": _HOST, "port": child.port, "ws_port": child.ws_port} for child in self.children]
                self._routing = PoolRouting(0, self.pods)
                self._routing.configure(endpoints, _HOST, self.tcp_port, self.ws_port, self.cluster_id)
                frame = {"operation": "configure", "endpoints": endpoints, "router_host": _HOST,
                         "router_port": self.tcp_port, "router_ws_port": self.ws_port, "cluster_id": self.cluster_id}
                await asyncio.wait_for(asyncio.gather(*(self._send_control(child, frame, deadline) for child in self.children)),
                                       self._remaining(deadline))
                await asyncio.wait_for(asyncio.gather(*(asyncio.shield(child._configured) for child in self.children)),
                                       self._remaining(deadline))
                if self._health_error or any(child.process.returncode is not None for child in self.children):
                    raise RuntimeError(self._health_error or "A pod exited during startup")
                self._accepting = True
                await asyncio.wait_for(self._tcp_server.start_serving(), self._remaining(deadline))
                if self._websocket_server is not None:
                    await asyncio.wait_for(self._websocket_server.start_serving(), self._remaining(deadline))
                self._started = True
                for child in self.children:
                    child.status = "running"
                self._health_task = asyncio.create_task(self._health_loop(), name="latzero-pod-health")
            except BaseException as exc:
                self._health_error = "Pod startup failed: {}".format(exc)[:2048]
                self._stopping = True
                for task in list(self._spawns):
                    task.cancel()
                if self._spawns:
                    await asyncio.wait(list(self._spawns), timeout=1.0)
                try:
                    await self._stop()
                except Exception as cleanup_error:
                    self._health_error += "; cleanup failed: {}".format(cleanup_error)[:1024]
                raise

    @staticmethod
    def _remaining(deadline: float) -> float:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise asyncio.TimeoutError("Pod lifecycle deadline exceeded")
        return remaining

    async def _bind_router(self) -> None:
        self._tcp_server = await asyncio.start_server(self._accept_tcp, _HOST, self.config.port,
                                                      limit=self.config.max_frame_bytes + 1, start_serving=False)
        self.tcp_port = self._tcp_server.sockets[0].getsockname()[1]
        self.ws_port = None
        if self.config.websocket_enabled:
            # Lazy import avoids a cycle with the broker's routing integration.
            from .server import _LimitedWebSocketProtocol

            class RouterProtocol(_LimitedWebSocketProtocol):
                def connection_made(protocol, transport):
                    protocol.router_deadline = asyncio.get_running_loop().time() + self.config.join_timeout
                    super(RouterProtocol, protocol).connection_made(transport)

            ws_port = self.config.websocket_port
            if ws_port is None:
                ws_port = self.config.port + 1 if self.config.port else 0
            self._websocket_server = await serve(
                self._handle_websocket, _HOST, ws_port, start_serving=False,
                origins=self.config.websocket_origins, compression=self.config.websocket_compression,
                max_size=self.config.max_frame_bytes, max_queue=self.config.websocket_max_queue,
                open_timeout=self.config.join_timeout, close_timeout=self.config.write_timeout,
                write_limit=self.config.connection_hwm_bytes, create_protocol=partial(RouterProtocol, self),
            )
            self.ws_port = self._websocket_server.sockets[0].getsockname()[1]

    async def _spawn(self, child: PodChild, config: dict, deadline: float) -> None:
        if self._stopping:
            return
        command = [sys.executable, "--pod-child"] if getattr(sys, "frozen", False) else [
            sys.executable, "-m", "latzero_server.pods", "--child",
        ]
        env = os.environ.copy()
        env.update(PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8")
        package_root = str(Path(__file__).resolve().parent.parent)
        env["PYTHONPATH"] = package_root + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
        creation = asyncio.create_task(asyncio.create_subprocess_exec(
            *command, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE, limit=_CONTROL_LIMIT + 1, env=env,
        ))
        child._creation_task = creation
        self._creations.add(creation)

        def created(task):
            self._creations.discard(task)
            if task.cancelled() or task.exception() is not None:
                return
            child.process = _PodProcess(task.result())
            child.pid = child.process.pid
            child._stdout_task = asyncio.create_task(self._read_stdout(child), name="latzero-pod-stdout-{}".format(child.index))
            child._stderr_task = asyncio.create_task(self._read_stderr(child), name="latzero-pod-stderr-{}".format(child.index))
            child._exit_task = asyncio.create_task(self._watch_child(child), name="latzero-pod-exit-{}".format(child.index))
            if self._stopping:
                child.process.stdin.close()

        creation.add_done_callback(created)
        await asyncio.shield(creation)
        if self._stopping:
            return
        await self._send_control(child, {"config": config, "index": child.index, "pods": self.pods, "parent_pid": os.getpid()}, deadline)

    async def _send_control(self, child: PodChild, message: dict, deadline: Optional[float] = None) -> None:
        frame = _control_frame(message)
        limit = self.config.write_timeout if deadline is None else min(self.config.write_timeout, self._remaining(deadline))

        async def send_frame():
            async with child._control_lock:
                child.process.stdin.write(frame)
                await child.process.stdin.drain()

        await asyncio.wait_for(send_frame(), limit)

    async def _read_stdout(self, child: PodChild) -> None:
        try:
            while True:
                raw = await child.process.stdout.readline()
                if not raw:
                    child._stdout_eof = True
                    if not self._stopping:
                        self._child_failure(child, "Pod {} control output closed unexpectedly".format(child.index))
                    return
                if len(raw) > _CONTROL_LIMIT or not raw.endswith(b"\n"):
                    raise ValueError("Invalid or oversized pod control output")
                message = json.loads(raw)
                if not isinstance(message, dict):
                    raise ValueError("Pod control output must be an object")
                if message.get("ready") is True and child.port is None:
                    if message.get("index") != child.index or type(message.get("index")) is not int:
                        raise ValueError("Pod readiness identity mismatch")
                    child.process.attach_runtime(message.get("pid"))
                    child.pid = message["pid"]
                    child.port = _port(message.get("port"), "port")
                    child.ws_port = _port(message.get("ws_port"), "ws_port", True)
                    if (child.ws_port is not None) != self.config.websocket_enabled:
                        raise ValueError("Pod WebSocket readiness mismatch")
                    child.status = "ready"
                    if not child._ready.done():
                        child._ready.set_result(message)
                elif message.get("configured") is True and child._ready.done() and not child._configured.done():
                    child._configured.set_result(message)
                elif message.get("stopped") is True and not child._stopped.done():
                    child._stopped.set_result(message)
                elif isinstance(message.get("stats"), dict) and child._configured.done():
                    child.stats = message["stats"]
                    child._stats_pending = False
                    child._stats_requested = None
                    if child.stats.get("health_error") or child.stats.get("persistence", {}).get("healthy") is False:
                        self._child_failure(child, "Pod {} reported unhealthy state".format(child.index))
                elif isinstance(message.get("error"), str):
                    raise RuntimeError(message["error"][:2048])
                else:
                    raise ValueError("Unexpected pod control output")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._child_failure(child, "Pod {} control failed: {}".format(child.index, exc))
            # Even a broken control producer must not block its own exit pipe.
            with suppress(ConnectionError, OSError):
                while await child.process.stdout.read(4096):
                    pass
                child._stdout_eof = True

    async def _read_stderr(self, child: PodChild) -> None:
        try:
            while True:
                raw = await child.process.stderr.read(4096)
                if not raw:
                    child._stderr_eof = True
                    return
                child.stderr_bytes += len(raw)
                child.stderr_tail.append(raw.decode("utf-8", errors="replace"))
        except (ConnectionError, OSError):
            return

    async def _watch_child(self, child: PodChild) -> int:
        code = await child.process.launcher.wait()
        if not self._stopping:
            self._child_failure(child, "Pod {} exited unexpectedly (code {})".format(child.index, code))
        elif child.status != "failed":
            child.status = "stopped" if code == 0 else "failed"
        return code

    def _child_failure(self, child: PodChild, error: str) -> None:
        child.status = "failed"
        child.error = error[:2048]
        for future in (child._ready, child._configured):
            if not future.done():
                future.set_exception(RuntimeError(child.error))
        if self._stopping:
            return
        self._health_error = self._health_error or child.error
        try:
            self._seal_ingress()
        except Exception as exc:
            self._health_error += "; router seal failed: {}".format(exc)[:512]
        if self._started and (self._failure_task is None or self._failure_task.done()):
            self._failure_task = asyncio.create_task(self.stop(), name="latzero-pod-failure-stop")
            self._failure_task.add_done_callback(lambda done: None if done.cancelled() else done.exception())

    async def _health_loop(self) -> None:
        while True:
            await asyncio.sleep(1.0)

            async def request(child):
                if not child._stats_pending and child.process.returncode is None:
                    child._stats_pending = True
                    child._stats_requested = asyncio.get_running_loop().time()
                    try:
                        await self._send_control(child, {"operation": "stats"})
                    except (ConnectionError, OSError, asyncio.TimeoutError) as exc:
                        self._child_failure(child, "Pod {} health control failed: {}".format(child.index, exc))
                elif child._stats_requested is not None and asyncio.get_running_loop().time() - child._stats_requested > self.config.write_timeout:
                    self._child_failure(child, "Pod {} health reply deadline exceeded".format(child.index))
                if child.process.runtime is not None and not child.process.runtime.alive():
                    self._child_failure(child, "Pod {} runtime exited unexpectedly".format(child.index))

            await asyncio.gather(*(request(child) for child in self.children))

    def _accept_tcp(self, reader: Any, writer: Any) -> None:
        if not self._accepting or self._connection_count + len(self._pending_websockets) >= self.config.max_connections:
            self._metrics["overload_rejections"] += 1
            frame = encode_message(self._error(None, None, "server_busy", "Router is stopping or at its connection limit"))
            if len(frame) - 1 <= self.config.max_frame_bytes:
                writer.write(frame)
            writer.close()
            return
        writer.transport.set_write_buffer_limits(high=self.config.connection_hwm_bytes)
        session = self._new_session(writer, asyncio.get_running_loop().time() + self.config.join_timeout)
        session.reader_task = asyncio.create_task(self._handle_tcp(session, reader), name="latzero-router-tcp")
        self._track_reader(session.reader_task)

    def _new_session(self, writer: Any, deadline: float) -> _RouterSession:
        session = _RouterSession(writer, deadline)
        self._sessions[id(session)] = session
        self._connection_count += 1
        return session

    def _track_reader(self, task: Any) -> None:
        self._readers.add(task)

        def done(completed):
            self._readers.discard(completed)
            if not completed.cancelled():
                completed.exception()

        task.add_done_callback(done)

    async def _handle_tcp(self, session: _RouterSession, reader: Any) -> None:
        try:
            while not session.closing:
                try:
                    raw = await asyncio.wait_for(reader.readline(), self._remaining(session.deadline))
                except ValueError:
                    await self._write(session, self._error(session, None, "frame_too_large", "Frame exceeds configured maximum"))
                    break
                if not raw:
                    break
                if len(raw.rstrip(b"\r\n")) > self.config.max_frame_bytes:
                    await self._write(session, self._error(session, None, "frame_too_large", "Frame exceeds configured maximum"))
                    break
                if not raw.endswith(b"\n"):
                    await self._write(session, self._error(session, None, "protocol_error", "TCP JSON frames require a newline"))
                    break
                await self._route_frame(session, raw)
        except asyncio.TimeoutError:
            with suppress(ConnectionError, OSError, asyncio.TimeoutError):
                await self._write(session, self._error(session, None, "join_timeout", "Join deadline exceeded"), join_deadline=False)
        except (ConnectionError, OSError):
            pass
        finally:
            await self._close_session(session)

    async def _handle_websocket(self, websocket: Any) -> None:
        self._pending_websockets.pop(id(websocket), None)
        if not self._accepting or self._connection_count + len(self._pending_websockets) >= self.config.max_connections:
            websocket.transport.close()
            return
        deadline = getattr(websocket, "router_deadline", asyncio.get_running_loop().time() + self.config.join_timeout)
        session = self._new_session(websocket, deadline)
        session.reader_task = asyncio.current_task()
        self._track_reader(session.reader_task)
        try:
            while not session.closing:
                raw = await asyncio.wait_for(websocket.recv(), self._remaining(session.deadline))
                if not isinstance(raw, str):
                    session.frames += 1
                    await self._write(session, self._error(session, None, "protocol_error", "WebSocket frames must be text JSON"))
                    if session.frames >= self.config.max_session_messages:
                        break
                    continue
                if len(raw.encode("utf-8")) > self.config.max_frame_bytes:
                    await self._write(session, self._error(session, None, "frame_too_large", "Frame exceeds configured maximum"))
                    break
                await self._route_frame(session, raw)
        except asyncio.TimeoutError:
            with suppress(ConnectionClosed, ConnectionError, OSError, asyncio.TimeoutError):
                await self._write(session, self._error(session, None, "join_timeout", "Join deadline exceeded"), join_deadline=False)
        except (ConnectionClosed, ConnectionError, OSError):
            pass
        finally:
            await self._close_session(session)

    @staticmethod
    def _identity(message: dict, name: str, required: bool = False) -> Optional[str]:
        payload = message.get("payload") or {}
        inner, outer = payload.get(name), message.get(name)
        if inner is not None and outer is not None and inner != outer:
            raise ValueError("{} does not match the envelope".format(name))
        value = inner if inner is not None else outer
        return _identifier(value, name) if required or value is not None else None

    async def _route_frame(self, session: _RouterSession, raw: Any) -> None:
        session.frames += 1
        self._metrics["received"] += 1
        if session.frames > self.config.max_session_messages:
            await self._write(session, self._error(session, None, "overloaded", "Router frame budget exceeded"))
            session.closing = True
            return
        message = None
        try:
            message = decode_message(raw)
            request_id = message.get("request_id")
            if request_id is not None and len(request_id.encode("utf-8")) > 512:
                raise ValueError("request_id exceeds 512 UTF-8 bytes")
            payload = message.get("payload") or {}
            client_id = self._identity(message, "client_id", message["type"] in ("join_pool", "switch_pool"))
            pool = self._identity(message, "pool", message["type"] in ("join_pool", "switch_pool"))
            if payload.get("auth_token") is not None and not isinstance(payload["auth_token"], str):
                raise ValueError("auth_token must be a string or null")
            if session.client_id and client_id is not None and session.client_id != client_id:
                await self._write(session, self._error(session, message, "identity_change", "Client identity is stable for the connection"))
                return
            if message["type"] == "hello":
                capabilities = payload.get("capabilities", [])
                if not isinstance(capabilities, list) or any(not isinstance(item, str) for item in capabilities):
                    raise ValueError("capabilities must be a list of strings")
                session.redirect_supported = _PROTOCOL in capabilities
                if client_id is not None:
                    session.client_id = client_id
                await self._write(session, {"type": "ack", "request_id": request_id, "client_id": session.client_id,
                                            "pool": None, "payload": {"server": "latzero-server"}})
            elif message["type"] in ("join_pool", "switch_pool"):
                session.client_id = client_id
                redirect = self._routing.redirect_payload(pool)
                if session.redirect_supported:
                    response = {"type": "redirect", "request_id": request_id, "client_id": client_id,
                                "pool": pool, "payload": redirect}
                else:
                    response = self._error(session, message, "redirect_required", "This pool requires a pool_redirect_v1 client")
                    response["pool"] = pool
                    response["payload"].update(redirect)
                await self._write(session, response)
                self._metrics["redirects"] += 1
                session.closing = True
            else:
                await self._write(session, self._error(session, message, "not_joined", "Join a pool before sending operations"))
        except (ValueError, TypeError, RecursionError) as exc:
            await self._write(session, self._error(session, message, "protocol_error", str(exc)[:256]))

    @staticmethod
    def _error(session: Optional[_RouterSession], message: Optional[dict], code: str, text: str) -> dict:
        return {"type": "error", "request_id": message.get("request_id") if message else None,
                "client_id": session.client_id if session else None, "pool": None,
                "payload": {"code": code, "message": text}}

    async def _write(self, session: _RouterSession, message: dict, join_deadline: bool = True) -> None:
        frame = encode_message(message)
        if len(frame) - 1 > self.config.max_frame_bytes:
            message = self._error(session, message, "response_too_large", "Router response exceeds frame limit")
            frame = encode_message(message)
            if len(frame) - 1 > self.config.max_frame_bytes:
                frame = encode_message(self._error(None, None, "response_too_large", "Response exceeds frame limit"))
            if len(frame) - 1 > self.config.max_frame_bytes:
                session.closing = True
                return
        timeout = self.config.write_timeout
        if join_deadline:
            timeout = min(timeout, self._remaining(session.deadline))
        writer = session.writer
        if hasattr(writer, "write"):
            writer.write(frame)
            await asyncio.wait_for(writer.drain(), timeout)
        else:
            await asyncio.wait_for(writer.send(frame[:-1].decode("utf-8")), timeout)

    async def _close_session(self, session: _RouterSession) -> None:
        if session.close_task is None:
            session.closing = True
            session.close_task = asyncio.create_task(self._finish_session(session), name="latzero-router-close")
        await asyncio.shield(session.close_task)

    async def _finish_session(self, session: _RouterSession) -> None:
        writer = session.writer
        try:
            if hasattr(writer, "write"):
                writer.close()
                await asyncio.wait_for(writer.wait_closed(), self.config.write_timeout)
            else:
                await asyncio.wait_for(writer.close(), self.config.write_timeout)
        except (ConnectionClosed, ConnectionError, OSError, asyncio.TimeoutError):
            if getattr(writer, "transport", None) is not None:
                writer.transport.abort()
        finally:
            if self._sessions.pop(id(session), None) is not None:
                self._connection_count -= 1

    def _seal_ingress(self) -> None:
        self._accepting = False
        if self._tcp_server is not None:
            self._tcp_server.close()
        if self._websocket_server is not None:
            self._websocket_server.server.close()
        for protocol in list(self._pending_websockets.values()):
            if getattr(protocol, "transport", None) is not None:
                protocol.transport.close()

    async def serve_forever(self) -> None:
        if not self._started:
            if self._health_error:
                raise RuntimeError(self._health_error)
            await self.start()
        await self._closed.wait()
        if self._health_error:
            raise RuntimeError(self._health_error)

    async def stop(self) -> None:
        if self._stop_task is not None and self._stop_task.done() and (
            self._stop_task.cancelled() or self._stop_task.exception() is not None
        ):
            self._stop_task = None
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self._stop_locked(), name="latzero-pods-stop")
        await asyncio.shield(self._stop_task)

    async def _stop_locked(self) -> None:
        if self._lifecycle_lock is None:
            self._lifecycle_lock = asyncio.Lock()
        async with self._lifecycle_lock:
            await self._stop()

    async def _stop(self) -> None:
        self._stopping = True
        errors = []
        try:
            self._seal_ingress()
        except Exception as exc:
            errors.append(str(exc))
        if self._health_task is not None:
            self._health_task.cancel()
        grace = asyncio.get_running_loop().time() + _STOP_GRACE
        outcomes = await asyncio.gather(self._stop_router(grace), *(self._stop_child(child, grace) for child in self.children),
                                        return_exceptions=True)
        errors.extend(str(outcome) for outcome in outcomes if isinstance(outcome, BaseException))
        if self._health_task is not None:
            done, pending = await asyncio.wait([self._health_task], timeout=max(0.0, min(
                1.0, grace + 2 * _REAP_TIMEOUT - asyncio.get_running_loop().time())))
            if pending:
                errors.append("Pod health task did not stop")
            for task in done:
                if not task.cancelled() and task.exception() is not None:
                    errors.append(str(task.exception()))
            self._health_task = None
        reaped = not self._spawns and not self._creations and all(child.process is None or child.process.returncode is not None
                                           and child._stdout_eof and child._stderr_eof for child in self.children)
        if reaped and self._directory_lock is not None:
            try:
                self._directory_lock.release()
                self._directory_lock = None
            except Exception as exc:
                errors.append(str(exc))
        elif not reaped:
            errors.append("Unreaped pods retain the directory lease")
        if reaped:
            for child in self.children:
                if child.process is not None:
                    try:
                        child.process.release_handle()
                    except Exception as exc:
                        errors.append(str(exc))
        self._started = False
        if self._closed is not None:
            self._closed.set()
        if errors:
            self._health_error = self._health_error or "Pod shutdown failed: {}".format("; ".join(errors))[:2048]
            raise RuntimeError("Pod shutdown failed: {}".format("; ".join(errors))[:4096])

    async def _stop_router(self, deadline: float) -> None:
        errors = []
        for reader in list(self._readers):
            reader.cancel()
        closers = [asyncio.create_task(self._close_session(session)) for session in list(self._sessions.values())]
        if closers:
            done, pending = await asyncio.wait(closers, timeout=min(self.config.write_timeout + 0.5, self._remaining(deadline)))
            for task in done:
                if not task.cancelled() and task.exception() is not None:
                    errors.append(str(task.exception()))
            if pending:
                for session in list(self._sessions.values()):
                    if getattr(session.writer, "transport", None) is not None:
                        session.writer.transport.abort()
                    if session.close_task is not None:
                        session.close_task.cancel()
                for task in pending:
                    task.cancel()
                errors.append("Router connections exceeded close deadline")
        if self._readers:
            _, pending = await asyncio.wait(list(self._readers), timeout=min(1.0, self._remaining(deadline)))
            if pending:
                errors.append("Router readers exceeded close deadline")
        tcp, websocket = self._tcp_server, self._websocket_server
        self._tcp_server = self._websocket_server = None
        for listener in (tcp, websocket):
            if listener is None:
                continue
            try:
                listener.close()
                await asyncio.wait_for(listener.wait_closed(), min(self.config.write_timeout, self._remaining(deadline)))
            except Exception as exc:
                errors.append(str(exc))
                if listener is websocket:
                    for protocol in list(websocket.websockets):
                        protocol.transport.abort()
                        protocol.handler_task.cancel()
        self._pending_websockets.clear()
        if errors:
            raise RuntimeError("Router shutdown failed: {}".format("; ".join(errors)))

    async def _stop_child(self, child: PodChild, grace: float) -> None:
        if child._creation_task is not None and not child._creation_task.done():
            await asyncio.wait_for(asyncio.shield(child._creation_task), self._remaining(grace))
        process = child.process
        if process is None:
            child.status = "stopped"
            return
        escalated = False
        if process.returncode is None:
            if child.status != "failed":
                child.status = "stopping"
            try:
                await self._send_control(child, {"operation": "stop"}, grace)
            except (ConnectionError, OSError, asyncio.TimeoutError):
                pass
            finally:
                if process.stdin is not None:
                    process.stdin.close()
            try:
                await asyncio.wait_for(process.wait(), self._remaining(grace))
            except asyncio.TimeoutError:
                escalated = True
                with suppress(ProcessLookupError, OSError):
                    process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), self._remaining(grace + _REAP_TIMEOUT))
                except asyncio.TimeoutError:
                    with suppress(ProcessLookupError, OSError):
                        process.kill()
                    await asyncio.wait_for(process.wait(), self._remaining(grace + 2 * _REAP_TIMEOUT))
        elif process.stdin is not None:
            process.stdin.close()
        tasks = [task for task in (child._stdout_task, child._stderr_task, child._exit_task) if task is not None]
        if tasks:
            done, pending = await asyncio.wait(tasks, timeout=min(1.0, self._remaining(grace + 2 * _REAP_TIMEOUT)))
            for task in pending:
                task.cancel()
            for task in done:
                if not task.cancelled():
                    task.exception()
        stopped = child._stopped.result() if child._stopped.done() and not child._stopped.cancelled() else None
        if escalated or process.returncode != 0 or not stopped or stopped.get("ok") is not True or not child._stdout_eof or not child._stderr_eof:
            child.status = "failed"
            error = "Pod {} did not stop cleanly (code {}, escalated {}, acknowledgement {})".format(
                child.index, process.returncode, escalated, stopped,
            )
            child.error = child.error or error[:2048]
            raise RuntimeError(child.error)
        if child.status != "failed":
            child.status = "stopped"

    def get_dashboard_snapshot(self) -> dict:
        children = [{"index": child.index, "pid": child.pid, "port": child.port, "ws_port": child.ws_port,
                     "status": child.status, "returncode": child.process.returncode if child.process else None,
                     "error": child.error, "stats": child.stats, "stderr_bytes": child.stderr_bytes}
                    for child in self.children]
        return {"mode": "pods", "pod_count": self.pods, "host": _HOST, "tcp_port": self.tcp_port,
                "ws_port": self.ws_port, "cluster_id": self.cluster_id, "accepting": self._accepting,
                "healthy": self._health_error is None and all(child.status != "failed" for child in self.children),
                "health_error": self._health_error, "connections": self._connection_count,
                "handshakes": len(self._pending_websockets), "metrics": dict(self._metrics), "children": children}


class _ControlReader:
    """One bounded daemon reader, including on Windows console/pipe handles."""

    def __init__(self):
        self.loop = asyncio.get_running_loop()
        self.queue = asyncio.Queue(maxsize=1)
        self.closed = threading.Event()
        self.eof = threading.Event()
        self.pending = None
        self.thread = threading.Thread(target=self._read, name="latzero-pod-stdin", daemon=True)
        self.thread.start()

    def _read(self):
        while not self.closed.is_set():
            try:
                raw = sys.stdin.buffer.readline(_CONTROL_LIMIT + 1)
                if not raw:
                    self.eof.set()
                self.pending = asyncio.run_coroutine_threadsafe(self.queue.put(raw), self.loop)
                self.pending.result()
                if not raw or len(raw) > _CONTROL_LIMIT or not raw.endswith(b"\n"):
                    return
            except Exception:
                return

    async def receive(self) -> Optional[dict]:
        raw = await self.queue.get()
        if not raw:
            return None
        if len(raw) > _CONTROL_LIMIT or not raw.endswith(b"\n"):
            raise ValueError("Invalid or oversized pod control input")
        message = json.loads(raw)
        if not isinstance(message, dict):
            raise ValueError("Pod control input must be an object")
        return message

    def close(self):
        self.closed.set()
        if self.pending is not None:
            self.pending.cancel()


class _ParentProcess:
    """Hold the Windows process object, rather than probing with os.kill(0)."""

    def __init__(self, pid: int):
        if type(pid) is not int or not 0 < pid <= 0xFFFFFFFF:
            raise ValueError("parent_pid must be a positive process ID")
        self.pid = pid
        self.handle = None
        if os.name == "nt":
            if not _windows_descendant(os.getpid(), pid):
                raise RuntimeError("Configured parent is not an ancestor of this pod")
            self.handle = _WindowsProcess(pid)
        if not self.alive():
            self.close()
            raise RuntimeError("Parent process is gone; refusing snapshot writes")

    def alive(self) -> bool:
        if os.name == "nt":
            return self.handle.alive()
        if os.getppid() != self.pid:
            return False
        try:
            os.kill(self.pid, 0)
            return True
        except OSError:
            return False

    def close(self):
        if self.handle is not None:
            self.handle.close()
            self.handle = None


def _child_output(message: dict) -> None:
    sys.stdout.buffer.write(_control_frame(message))
    sys.stdout.buffer.flush()


def _child_health(server: Any, routing: PoolRouting) -> dict:
    pool_ids = []
    size = 0
    for pool_id in server._pools:
        size += len(encode_message({"pool": pool_id}))
        if size > _CONTROL_LIMIT // 2:
            break
        pool_ids.append(pool_id)
    return {"pod_index": routing.pod_index, "pid": os.getpid(), "pools": len(server._pools),
            "pool_ids": pool_ids, "pool_ids_truncated": len(pool_ids) != len(server._pools),
            "connections": server._connection_count, "handshakes": len(server._pending_websockets),
            "health_error": server._health_error, "metrics": dict(server._metrics), "persistence": server._store.health}


async def _child_run(resources: dict) -> int:
    reader = _ControlReader()
    server = None
    parent = None
    parent_task = None
    control_task = None
    error = None
    index = None
    try:
        initial = await asyncio.wait_for(reader.receive(), 30.0)
        if initial is None:
            return 0
        routing = PoolRouting(initial.get("index"), initial.get("pods"))
        index = routing.pod_index
        config_data = initial.get("config")
        if not isinstance(config_data, dict):
            raise ValueError("Initial pod frame requires a config object")
        config = ServerConfig(**config_data)
        if config.host != _HOST or config.port != 0 or config.websocket_port != 0:
            raise ValueError("Child listeners must use 127.0.0.1 and ephemeral ports")
        resources["lock"] = DataDirectoryLock(config.data_dir, slot=index).acquire()
        parent = _ParentProcess(initial.get("parent_pid"))
        # Claim the writer slot and observe the parent before constructing the
        # snapshot loader. A delayed orphan must never begin writing new state.
        from .server import LatZeroServer

        if not parent.alive() or reader.eof.is_set():
            raise RuntimeError("Parent process is gone; refusing snapshot writes")
        server = LatZeroServer(config, pool_routing=routing)
        await server.start()
        server._accepting = False
        tcp_port = server._tcp_server.sockets[0].getsockname()[1]
        ws_port = server._websocket_server.sockets[0].getsockname()[1] if server._websocket_server else None
        _child_output({"ready": True, "index": index, "pid": os.getpid(), "port": tcp_port, "ws_port": ws_port})
        configured = False

        async def wait_parent():
            while parent.alive():
                await asyncio.sleep(0.25)

        parent_task = asyncio.create_task(wait_parent(), name="latzero-pod-parent")
        configure_deadline = asyncio.get_running_loop().time() + 30.0
        while True:
            control_task = asyncio.create_task(reader.receive(), name="latzero-pod-control")
            done, _ = await asyncio.wait([control_task, parent_task], timeout=None if configured else max(
                0.0, configure_deadline - asyncio.get_running_loop().time()), return_when=asyncio.FIRST_COMPLETED)
            if parent_task in done:
                break
            if not done:
                raise asyncio.TimeoutError("Pod routing configuration deadline exceeded")
            message = control_task.result()
            control_task = None
            if message is None or message.get("operation") == "stop":
                break
            if message.get("operation") == "configure" and not configured:
                routing.configure(message.get("endpoints"), message.get("router_host"), message.get("router_port"),
                                  message.get("router_ws_port"), message.get("cluster_id"))
                if not parent.alive():
                    break
                configured = True
                server._accepting = True
                _child_output({"configured": True, "index": index})
            elif message.get("operation") == "stats" and configured:
                _child_output({"stats": _child_health(server, routing), "index": index})
            else:
                raise ValueError("Unexpected pod control operation")
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        error = "{}: {}".format(type(exc).__name__, exc)[:2048]
        with suppress(BrokenPipeError, OSError):
            _child_output({"error": error, "index": index})
    finally:
        for task in (control_task, parent_task):
            if task is not None:
                task.cancel()
        tasks = [task for task in (control_task, parent_task) if task is not None]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        reader.close()
        if server is not None:
            server._accepting = False
            try:
                await server.stop()
            except Exception as exc:
                error = error or "Shutdown failed: {}".format(exc)[:2048]
        resources["io_stopped"] = server is None or (not server._store.health["running"]
            and not server._store.health["in_flight"] and not server._store.health["dirty_pools"])
        if parent is not None:
            parent.close()
        if index is not None:
            with suppress(BrokenPipeError, OSError):
                _child_output({"stopped": True, "ok": error is None, "index": index, "error": error})
    return 1 if error else 0


def child_main() -> int:
    """Hidden frozen-CLI dispatch and ``python -m ...pods --child`` entrypoint."""
    resources = {"lock": None, "io_stopped": False}
    completed = False
    try:
        code = asyncio.run(_child_run(resources))
        completed = True
        return code
    except KeyboardInterrupt:
        return 0
    finally:
        # A timed-out executor write can outlive server.stop(). Keep its slot
        # until process exit (or parent escalation), not merely until the ACK.
        if completed and resources["io_stopped"] and resources["lock"] is not None:
            resources["lock"].release()


if __name__ == "__main__":
    if sys.argv[1:] != ["--child"]:
        raise SystemExit("This module is an internal pod worker; use latzero-server --pods N")
    raise SystemExit(child_main())
