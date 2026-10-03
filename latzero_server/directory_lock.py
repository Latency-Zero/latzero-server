"""Persistent, nonblocking writer leases for a flat snapshot directory."""

import errno
import os
import threading
import uuid
from pathlib import Path
from typing import Dict, Optional


_MAX_SLOTS = 64
_REGISTRY_PID = os.getpid()
_REGISTRY: Dict[tuple, dict] = {}
_PATHS: Dict[str, tuple] = {}
_REGISTRY_LOCK = threading.RLock()


class DataDirectoryLockError(OSError):
    """Another daemon or a draining pod still owns the directory."""


def _lock_range(fd: int, offset: int, length: int, acquire: bool) -> None:
    if os.name == "nt":
        import msvcrt

        os.lseek(fd, offset, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_NBLCK if acquire else msvcrt.LK_UNLCK, length)
    else:
        import fcntl

        flags = fcntl.LOCK_EX | fcntl.LOCK_NB if acquire else fcntl.LOCK_UN
        fcntl.lockf(fd, flags, length, offset, os.SEEK_SET)


def _release_scan(fd: int, count: int) -> None:
    # Windows requires unlock boundaries to match each LockFile range.
    for offset in range(count, 0, -1):
        _lock_range(fd, offset, 1, False)


class DataDirectoryLock:
    """Lock byte zero for a parent/classic daemon, or byte ``slot + 1``.

    A root lease checks all 64 child slots before succeeding. Child leases
    deliberately coexist with their parent, and outlive it while flushing.
    The lock file is never unlinked. On POSIX, leases share one descriptor:
    closing *any* descriptor for the inode would otherwise release all of
    this process's record locks, including leases held by another object.
    """

    def __init__(self, path: Path, slot: Optional[int] = None):
        if slot is not None and (type(slot) is not int or not 0 <= slot < _MAX_SLOTS):
            raise ValueError("slot must be an integer from 0 to 63 or None")
        self.data_dir = Path(path)
        self.path = self.data_dir / ".latzero.lock"
        self.slot = slot
        self._entry = None
        self._pid = None
        self.previous_owner_token = None
        self.owner_token = None

    @property
    def acquired(self) -> bool:
        return self._entry is not None and self._pid == os.getpid()

    def acquire(self) -> "DataDirectoryLock":
        global _REGISTRY_PID
        with _REGISTRY_LOCK:
            if self.acquired:
                return self
            if _REGISTRY_PID != os.getpid():
                # Record locks aren't inherited by forked POSIX children.
                for entry in _REGISTRY.values():
                    for fd in entry["fds"]:
                        os.close(fd)
                _REGISTRY.clear()
                _PATHS.clear()
                _REGISTRY_PID = os.getpid()
            self.data_dir.mkdir(parents=True, exist_ok=True)
            key = os.path.normcase(str(self.path.resolve()))
            identity = _PATHS.get(key)
            entry = _REGISTRY.get(identity)
            if entry is None:
                fd = os.open(str(self.path), os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0), 0o600)
                try:
                    stat = os.fstat(fd)
                    identity = (stat.st_dev, stat.st_ino)
                    entry = _REGISTRY.get(identity)
                    if entry is None:
                        if stat.st_size < _MAX_SLOTS + 1:
                            # Extending mustn't truncate a root token written
                            # since this descriptor's initial size check.
                            os.lseek(fd, _MAX_SLOTS, os.SEEK_SET)
                            if os.write(fd, b"\x00") != 1:
                                raise OSError("Incomplete lock file initialization")
                        entry = {"fd": fd, "fds": [fd], "held": set(), "paths": set()}
                        _REGISTRY[identity] = entry
                    else:
                        # Retain alias descriptors until all leases are gone.
                        entry["fds"].append(fd)
                except BaseException:
                    os.close(fd)
                    raise
                _PATHS[key] = identity
                entry["paths"].add(key)
            offset = 0 if self.slot is None else self.slot + 1
            locked = False
            scanned = 0
            try:
                if offset in entry["held"]:
                    raise OSError(errno.EBUSY, "Writer lease is already held in this process")
                _lock_range(entry["fd"], offset, 1, True)
                locked = True
                if self.slot is None:
                    for child_offset in range(1, _MAX_SLOTS + 1):
                        if child_offset in entry["held"]:
                            raise OSError(errno.EBUSY, "A pod writer is still active")
                        _lock_range(entry["fd"], child_offset, 1, True)
                        scanned += 1
                    _release_scan(entry["fd"], scanned)
                    scanned = 0
                    os.lseek(entry["fd"], _MAX_SLOTS + 1, os.SEEK_SET)
                    previous = os.read(entry["fd"], 32)
                    self.previous_owner_token = previous.decode("ascii") if len(previous) == 32 and all(
                        value in b"0123456789abcdef" for value in previous) else None
                    self.owner_token = uuid.uuid4().hex
                    os.lseek(entry["fd"], _MAX_SLOTS + 1, os.SEEK_SET)
                    if os.write(entry["fd"], self.owner_token.encode("ascii")) != 32:
                        raise OSError("Incomplete root ownership marker write")
                entry["held"].add(offset)
                self._entry = entry
                self._pid = os.getpid()
                return self
            except OSError as exc:
                if scanned:
                    _release_scan(entry["fd"], scanned)
                if locked:
                    _lock_range(entry["fd"], offset, 1, False)
                self._discard_unused(identity, entry)
                raise DataDirectoryLockError(
                    errno.EBUSY, "Snapshot directory is already in use or still draining: {} ({})".format(self.data_dir, exc)
                ) from exc
            except BaseException:
                if scanned:
                    _release_scan(entry["fd"], scanned)
                if locked:
                    _lock_range(entry["fd"], offset, 1, False)
                self._discard_unused(identity, entry)
                raise

    @staticmethod
    def _discard_unused(identity: tuple, entry: dict) -> None:
        if entry["held"]:
            return
        for fd in entry["fds"]:
            os.close(fd)
        _REGISTRY.pop(identity, None)
        for key in entry["paths"]:
            _PATHS.pop(key, None)

    def release(self) -> None:
        with _REGISTRY_LOCK:
            if not self.acquired:
                self._entry = None
                return
            entry = self._entry
            offset = 0 if self.slot is None else self.slot + 1
            _lock_range(entry["fd"], offset, 1, False)
            entry["held"].remove(offset)
            self._entry = None
            self._pid = None
            stat = os.fstat(entry["fd"])
            self._discard_unused((stat.st_dev, stat.st_ino), entry)

    def __enter__(self) -> "DataDirectoryLock":
        return self.acquire()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.release()
