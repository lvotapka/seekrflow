"""
modules/process_registry.py

Per-user registry of running seekrflow processes.

Monitors are long-lived, are often started from directories the user later
forgets about, and survive their terminal. Without a single place to look, they
accumulate silently: six standalone runs once polled the same Globus endpoint
for weeks after their shells died. Every run records itself here so
``seekrflow ps`` and ``seekrflow stop`` can see and stop them all.

The registry is advisory. Liveness always comes from the PID and the per-root
run lock, never from this file, so a stale entry cannot mislead.
"""

from __future__ import annotations

import fcntl
import json
import os
import sys
import time
import typing

import seekrflow.modules.run_lock as run_lock


REGISTRY_FILENAME = "processes.json"
USER_STATE_DIRNAME = ".seekrflow"


def user_state_directory() -> str:
    return os.path.join(os.path.expanduser("~"), USER_STATE_DIRNAME)


def registry_path() -> str:
    return os.path.join(user_state_directory(), REGISTRY_FILENAME)


def pid_alive(pid: int | None) -> bool:
    try:
        pid = int(pid)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return not _is_zombie(pid)


def _is_zombie(pid: int) -> bool:
    try:
        with open(f"/proc/{pid}/stat", "rb") as f:
            fields = f.read().rsplit(b")", 1)[-1].split()
    except (OSError, IndexError):
        return False
    return bool(fields) and fields[0] == b"Z"


def _safe_cwd() -> str:
    """
    The cwd, or "" if it has been deleted underneath us. Recorded for display
    only, so it must never stop a process from registering itself.
    """
    try:
        return os.getcwd()
    except OSError:
        return ""


def _safe_abspath(path: str) -> str:
    """abspath without needing a valid cwd for already-absolute paths."""
    if os.path.isabs(path):
        return os.path.normpath(path)
    try:
        return os.path.abspath(path)
    except OSError:
        return path


def _read_unlocked(path: str) -> dict[str, dict]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    processes = data.get("processes")
    if not isinstance(processes, dict):
        return {}
    return {
        key: entry for key, entry in processes.items()
        if isinstance(entry, dict)
    }


def _write_unlocked(path: str, processes: dict[str, dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"processes": processes}, f, indent=2, default=str)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _with_lock(path: str, fn: typing.Callable[[], typing.Any]) -> typing.Any:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    lock_path = path + ".lock"
    try:
        lock_f = open(lock_path, "a+")
    except OSError:
        return fn()
    try:
        fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
        try:
            return fn()
        finally:
            fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)
    finally:
        lock_f.close()


def register(
        root_directory: str,
        instruction: str,
        work_directory: str | None = None,
        name: str | None = None,
        batch_directory: str | None = None,
        ) -> None:
    """
    Record this process. Best effort: never raises.
    """
    path = registry_path()
    key = str(os.getpid())

    def _body() -> None:
        processes = _read_unlocked(path)
        processes[key] = {
            "pid": os.getpid(),
            "name": name or "",
            "instruction": instruction,
            "root_directory": _safe_abspath(str(root_directory)),
            "work_directory": (
                _safe_abspath(str(work_directory)) if work_directory else ""),
            "batch_directory": batch_directory or "",
            "argv": list(sys.argv),
            "cwd": _safe_cwd(),
            "started_at": time.time(),
            "hostname": os.uname().nodename,
        }
        _write_unlocked(path, processes)

    try:
        _with_lock(path, _body)
    except OSError as e:
        print(f"[registry] could not record this process: {e}")


def unregister(pid: int | None = None) -> None:
    """
    Drop one process's entry. Best effort: never raises.
    """
    path = registry_path()
    key = str(pid if pid is not None else os.getpid())

    def _body() -> None:
        processes = _read_unlocked(path)
        if processes.pop(key, None) is not None:
            _write_unlocked(path, processes)

    try:
        _with_lock(path, _body)
    except OSError:
        pass


def read_all() -> dict[str, dict]:
    """Every recorded entry, live or not."""
    return _read_unlocked(registry_path())


def prune() -> dict[str, dict]:
    """Drop entries whose process is gone; return the survivors."""
    path = registry_path()

    def _body() -> dict[str, dict]:
        processes = _read_unlocked(path)
        alive = {
            key: entry for key, entry in processes.items()
            if pid_alive(entry.get("pid"))
        }
        if alive != processes:
            _write_unlocked(path, alive)
        return alive

    try:
        return _with_lock(path, _body)
    except OSError:
        return read_all()


def live_processes() -> list[dict]:
    """
    Live seekrflow processes, newest last, annotated with lock ownership.
    """
    entries = []
    for entry in prune().values():
        entry = dict(entry)
        root = entry.get("root_directory") or ""
        entry["holds_run_lock"] = bool(
            root and run_lock.is_locked(root))
        entries.append(entry)
    entries.sort(key=lambda e: e.get("started_at") or 0.0)
    return entries


def find_unregistered_lock_owners(
        roots: typing.Iterable[str]) -> list[dict]:
    """
    Run-lock owners missing from the registry (e.g. started by older code).
    """
    known = {
        str(entry.get("pid")) for entry in read_all().values()
    }
    owners = []
    for root in roots:
        if not run_lock.is_locked(root):
            continue
        owner = run_lock.read_owner(root) or {}
        pid = owner.get("pid")
        if pid is None or str(pid) in known:
            continue
        owners.append({
            "pid": pid,
            "root_directory": _safe_abspath(str(root)),
            "instruction": owner.get("instruction", ""),
            "argv": owner.get("argv") or [],
            "started_at": owner.get("started_at") or 0.0,
            "holds_run_lock": True,
            "unregistered": True,
        })
    return owners
