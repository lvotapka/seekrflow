"""
Serialize Globus Compute *submits* and cap concurrent status polls.

The holder lock is held only while registering/submitting a Compute task, not
while waiting for ``future.result``. Status polls are also tracked in
``in_flight`` so two workers are not flooded with duplicate squeue tasks.

A batch sets ``SEEKR_GLOBUS_LOCK_FILE`` to scope the lock to that batch;
every other run falls back to a per-user lock under ``~/.seekrflow/``.
Set ``SEEKR_GLOBUS_LOCK_DISABLE=1`` to opt out entirely.

Priority when the holder lock is free: cancel > submit > focused status >
status, then oldest waiter.
"""

from __future__ import annotations

import fcntl
import json
import os
import threading
import time
import typing


LOCK_ENV = "SEEKR_GLOBUS_LOCK_FILE"
DISABLE_ENV = "SEEKR_GLOBUS_LOCK_DISABLE"
GLOBUS_LOCK_FILENAME = ".seekrflow_globus_lock.json"
USER_STATE_DIRNAME = ".seekrflow"
ACQUIRE_POLL_SECONDS = 0.5

KIND_PRIORITY = {
    "cancel": 0,
    "submit": 1,
    "status_focused": 2,
    "status": 3,
}

# Matches a typical 2-worker Globus Compute endpoint. Submit/cancel are not
# counted against this cap.
STATUS_IN_FLIGHT_CAP = 2
STATUS_IN_FLIGHT_KINDS = frozenset({"status", "status_focused"})

IN_FLIGHT_SUBMIT = "submit"
IN_FLIGHT_DUPLICATE = "duplicate"
IN_FLIGHT_AT_CAPACITY = "at_capacity"

# The per-user lock file only needs its dead holder/waiter reaping once per
# process; the path itself is re-read every call so it always reflects the
# current environment.
_user_lock_initialized = False


def lock_file_path(batch_directory: str) -> str:
    return os.path.join(batch_directory, GLOBUS_LOCK_FILENAME)


def user_state_directory() -> str:
    return os.path.join(os.path.expanduser("~"), USER_STATE_DIRNAME)


def user_lock_file_path() -> str:
    return os.path.join(user_state_directory(), GLOBUS_LOCK_FILENAME)


def resolve_lock_file_path() -> str | None:
    """
    The lock this process should use: batch-scoped if set, else per-user.

    Returns None only when locking is explicitly disabled.
    """
    global _user_lock_initialized
    if str(os.environ.get(DISABLE_ENV, "")).strip().lower() in {
            "1", "true", "yes"}:
        return None
    path = os.environ.get(LOCK_ENV) or None
    if path is not None:
        return path
    path = user_lock_file_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if not _user_lock_initialized:
            init_lock(path)
            _user_lock_initialized = True
    except OSError as e:
        print(f"[globus-lock] per-user lock unavailable ({e}); "
              "proceeding without serialization")
        return None
    return path


def _normalize_kind(kind: str) -> str:
    if kind in KIND_PRIORITY:
        return kind
    return "status"


def is_status_kind(kind: str) -> bool:
    return _normalize_kind(kind) in STATUS_IN_FLIGHT_KINDS


def _empty_lock_data() -> dict:
    return {"holder": None, "waiters": [], "in_flight": []}


def _pid_alive(pid: int) -> bool:
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
    return True


def _read_unlocked(path: str) -> dict:
    empty = _empty_lock_data()
    if not os.path.exists(path):
        return empty
    with open(path, "r") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            return empty
    if not isinstance(data, dict):
        return empty
    waiters = data.get("waiters") or []
    if not isinstance(waiters, list):
        waiters = []
    holder = data.get("holder")
    if holder is not None and not isinstance(holder, dict):
        holder = None
    in_flight = data.get("in_flight") or []
    if not isinstance(in_flight, list):
        in_flight = []
    return {"holder": holder, "waiters": waiters, "in_flight": in_flight}


def _write_unlocked(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _with_lock(path: str, fn: typing.Callable[[], typing.Any]) -> typing.Any:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    lock_path = path + ".lock"
    with open(lock_path, "a+") as lock_f:
        fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
        try:
            return fn()
        finally:
            fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)


def _entry_pid(entry: dict | None) -> int:
    if not isinstance(entry, dict):
        return -1
    try:
        return int(entry.get("pid", -1))
    except (TypeError, ValueError):
        return -1


def _entry_tid(entry: dict | None) -> int:
    if not isinstance(entry, dict):
        return -1
    try:
        return int(entry.get("tid", -1))
    except (TypeError, ValueError):
        return -1


def _same_owner(entry: dict | None, pid: int, tid: int) -> bool:
    return _entry_pid(entry) == pid and _entry_tid(entry) == tid


def _reap_dead_waiters(waiters: list) -> list[dict]:
    alive = []
    for waiter in waiters:
        if not isinstance(waiter, dict):
            continue
        if _pid_alive(_entry_pid(waiter)):
            alive.append(waiter)
    return alive


def _reap_holder(holder: dict | None) -> dict | None:
    if holder is None:
        return None
    if not _pid_alive(_entry_pid(holder)):
        return None
    return holder


def _reap_in_flight(entries: list) -> list[dict]:
    alive = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if not entry.get("key"):
            continue
        if _pid_alive(_entry_pid(entry)):
            alive.append(entry)
    return alive


def _normalize_lock_fields(data: dict) -> dict:
    data["holder"] = _reap_holder(data.get("holder"))
    data["waiters"] = _reap_dead_waiters(data.get("waiters") or [])
    data["in_flight"] = _reap_in_flight(data.get("in_flight") or [])
    return data


def _waiter_sort_key(waiter: dict) -> tuple:
    kind = _normalize_kind(str(waiter.get("kind") or "status"))
    try:
        ts = float(waiter.get("ts") or 0.0)
    except (TypeError, ValueError):
        ts = 0.0
    return (KIND_PRIORITY[kind], ts)


def _upsert_waiter(
        waiters: list[dict],
        pid: int,
        tid: int,
        kind: str,
        ) -> list[dict]:
    for waiter in waiters:
        if _same_owner(waiter, pid, tid):
            waiter["kind"] = kind
            return waiters
    waiters.append({
        "pid": pid,
        "tid": tid,
        "kind": kind,
        "ts": time.time(),
    })
    return waiters


def init_lock(path: str) -> None:
    """Create the lock file if needed; reap dead holder/waiters/in_flight."""

    def _body() -> None:
        data = _normalize_lock_fields(_read_unlocked(path))
        _write_unlocked(path, data)

    _with_lock(path, _body)


def try_acquire(
        path: str | None,
        kind: str,
        pid: int | None = None,
        tid: int | None = None,
        ) -> bool:
    """
    Try to become the sole Globus-call holder.

    ``tid`` distinguishes threads in the same child process (asyncio
    thread-pool submits). Same-thread re-acquire is allowed.
    """
    if not path:
        return True
    kind = _normalize_kind(kind)
    pid = os.getpid() if pid is None else int(pid)
    tid = threading.get_ident() if tid is None else int(tid)

    def _body() -> bool:
        data = _normalize_lock_fields(_read_unlocked(path))
        holder = data.get("holder")
        waiters = data.get("waiters") or []
        if _same_owner(holder, pid, tid):
            depth = int(holder.get("depth") or 1)
            holder["depth"] = depth + 1
            holder["kind"] = kind
            data["holder"] = holder
            data["waiters"] = waiters
            _write_unlocked(path, data)
            return True
        waiters = _upsert_waiter(waiters, pid, tid, kind)
        if holder is not None:
            data["holder"] = holder
            data["waiters"] = waiters
            _write_unlocked(path, data)
            return False
        waiters.sort(key=_waiter_sort_key)
        winner = waiters[0]
        if not _same_owner(winner, pid, tid):
            data["holder"] = None
            data["waiters"] = waiters
            _write_unlocked(path, data)
            return False
        waiters = waiters[1:]
        data["holder"] = {
            "pid": pid,
            "tid": tid,
            "kind": kind,
            "ts": time.time(),
            "depth": 1,
        }
        data["waiters"] = waiters
        _write_unlocked(path, data)
        return True

    return bool(_with_lock(path, _body))


def acquire_blocking(
        path: str | None,
        kind: str,
        pid: int | None = None,
        tid: int | None = None,
        poll_seconds: float = ACQUIRE_POLL_SECONDS,
        ) -> bool:
    """Block until this caller is the holder. No-op when ``path`` is empty."""
    if not path:
        return True
    if poll_seconds <= 0:
        poll_seconds = ACQUIRE_POLL_SECONDS
    while True:
        if try_acquire(path, kind, pid=pid, tid=tid):
            return True
        time.sleep(poll_seconds)


def release(
        path: str | None,
        pid: int | None = None,
        tid: int | None = None,
        ) -> None:
    """Release the lock if this pid/tid holds it. Best-effort."""
    if not path:
        return
    pid = os.getpid() if pid is None else int(pid)
    tid = threading.get_ident() if tid is None else int(tid)

    def _body() -> None:
        data = _normalize_lock_fields(_read_unlocked(path))
        holder = data.get("holder")
        waiters = data.get("waiters") or []
        if _same_owner(holder, pid, tid):
            depth = int(holder.get("depth") or 1) - 1
            if depth > 0:
                holder["depth"] = depth
            else:
                holder = None
        data["holder"] = holder
        data["waiters"] = waiters
        _write_unlocked(path, data)

    try:
        _with_lock(path, _body)
    except OSError:
        pass


def try_begin_in_flight(
        path: str | None,
        kind: str,
        key: str,
        pid: int | None = None,
        ) -> str:
    """
    Record a status poll about to be submitted, or refuse it.

    Returns ``IN_FLIGHT_SUBMIT``, ``IN_FLIGHT_DUPLICATE``, or
    ``IN_FLIGHT_AT_CAPACITY``. Submit/cancel kinds always return submit and
    are not recorded. ``path`` None (lock disabled) always submits.
    """
    if not path:
        return IN_FLIGHT_SUBMIT
    kind = _normalize_kind(kind)
    if kind not in STATUS_IN_FLIGHT_KINDS:
        return IN_FLIGHT_SUBMIT
    key = str(key or "")
    if not key:
        return IN_FLIGHT_SUBMIT
    pid = os.getpid() if pid is None else int(pid)

    def _body() -> str:
        data = _normalize_lock_fields(_read_unlocked(path))
        in_flight = data.get("in_flight") or []
        for entry in in_flight:
            if str(entry.get("key") or "") == key:
                data["in_flight"] = in_flight
                _write_unlocked(path, data)
                return IN_FLIGHT_DUPLICATE
        status_count = sum(
            1 for entry in in_flight
            if _normalize_kind(str(entry.get("kind") or "status"))
            in STATUS_IN_FLIGHT_KINDS
        )
        if status_count >= STATUS_IN_FLIGHT_CAP:
            data["in_flight"] = in_flight
            _write_unlocked(path, data)
            return IN_FLIGHT_AT_CAPACITY
        in_flight.append({
            "pid": pid,
            "kind": kind,
            "key": key,
            "ts": time.time(),
        })
        data["in_flight"] = in_flight
        _write_unlocked(path, data)
        return IN_FLIGHT_SUBMIT

    return str(_with_lock(path, _body))


def end_in_flight(
        path: str | None,
        key: str,
        pid: int | None = None,
        ) -> None:
    """Drop this process's in-flight entry for ``key``. Best-effort."""
    if not path:
        return
    key = str(key or "")
    if not key:
        return
    pid = os.getpid() if pid is None else int(pid)

    def _body() -> None:
        data = _normalize_lock_fields(_read_unlocked(path))
        data["in_flight"] = [
            entry for entry in (data.get("in_flight") or [])
            if not (
                str(entry.get("key") or "") == key
                and _entry_pid(entry) == pid
            )
        ]
        _write_unlocked(path, data)

    try:
        _with_lock(path, _body)
    except OSError:
        pass
