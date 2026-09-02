"""
modules/run_lock.py

Per-calculation singleton lock so that at most one seekrflow process operates
on a given model root directory at a time.

Two concurrent ``flow.py run`` processes on the same root directory both write
``.seekrflow_job_status.json`` from independent in-memory state and both submit
and monitor jobs, which produces oscillating statuses and duplicate scheduler
jobs. The lock makes that impossible.

The lock is an ``fcntl.flock`` on ``<root>/.seekrflow_run.lock`` held for the
lifetime of the process, so the kernel releases it even if the process is
SIGKILLed or its parent dies.
"""

from __future__ import annotations

import fcntl
import json
import os
import sys
import time
import typing


RUN_LOCK_FILENAME = ".seekrflow_run.lock"

# Distinct exit code so batch.py can report "already running" instead of
# treating a refused duplicate as a crashed child.
DUPLICATE_RUN_EXIT_CODE = 3


class RunLockBusyError(RuntimeError):
    """Another live seekrflow process holds the lock for this root directory."""

    def __init__(self, path: str, owner: dict | None):
        self.path = path
        self.owner = owner or {}
        owner_pid = self.owner.get("pid", "unknown")
        owner_instruction = self.owner.get("instruction", "unknown")
        super().__init__(
            f"another seekrflow process (PID {owner_pid}, instruction "
            f"{owner_instruction!r}) already holds {path}"
        )


def lock_file_path(root_directory: str) -> str:
    return os.path.join(str(root_directory), RUN_LOCK_FILENAME)


def read_owner(root_directory: str) -> dict | None:
    """
    Read the recorded owner metadata, if any. Does not indicate liveness.
    """
    path = lock_file_path(root_directory)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    return data


def is_locked(root_directory: str) -> bool:
    """
    True if a live process currently holds the lock for this root directory.
    """
    path = lock_file_path(root_directory)
    if not os.path.exists(path):
        return False
    try:
        fd = os.open(path, os.O_RDWR)
    except OSError:
        return False
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return True
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(fd)


class RunLock:
    """
    Held ``flock`` on a root directory. Released on ``release()`` or exit.
    """

    def __init__(self, path: str, fd: int):
        self.path = path
        self._fd: int | None = fd

    def release(self) -> None:
        fd = self._fd
        if fd is None:
            return
        self._fd = None
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        try:
            os.close(fd)
        except OSError:
            pass

    def __enter__(self) -> "RunLock":
        return self

    def __exit__(self, *_exc_info) -> None:
        self.release()


# Keeps acquired locks referenced for the process lifetime so the descriptors
# are not closed by garbage collection.
_held_locks: list[RunLock] = []


def acquire(
        root_directory: str,
        instruction: str,
        argv: typing.Sequence[str] | None = None,
        ) -> RunLock:
    """
    Take the singleton lock for ``root_directory``.

    Raises ``RunLockBusyError`` if another live process holds it.
    """
    os.makedirs(str(root_directory), exist_ok=True)
    path = lock_file_path(root_directory)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as e:
        os.close(fd)
        raise RunLockBusyError(path, read_owner(root_directory)) from e
    payload = {
        "pid": os.getpid(),
        "instruction": instruction,
        "argv": list(argv) if argv is not None else list(sys.argv),
        "started_at": time.time(),
        "hostname": os.uname().nodename,
    }
    try:
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, json.dumps(payload, indent=2).encode())
        os.fsync(fd)
    except OSError:
        # Metadata is advisory; the lock itself is what matters.
        pass
    lock = RunLock(path, fd)
    _held_locks.append(lock)
    return lock
