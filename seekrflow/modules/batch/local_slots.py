"""
Batch-scoped pool of local-stage execution slots.

Used so at most ``max_concurrent_local_runs`` local stages run across a
batch at once. Standalone flow.py never touches this module unless a
``--local-slot-file`` is explicitly provided.
"""

from __future__ import annotations

import fcntl
import json
import os
import time
import typing


LOCAL_SLOTS_FILENAME = ".seekrflow_local_slots.json"


def slot_file_path(batch_directory: str) -> str:
    return os.path.join(batch_directory, LOCAL_SLOTS_FILENAME)


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Process exists but we cannot signal it.
        return True
    except OSError:
        return False
    return True


def _read_unlocked(path: str) -> dict:
    if not os.path.exists(path):
        return {"max_slots": 1, "holders": []}
    with open(path, "r") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            return {"max_slots": 1, "holders": []}
    if not isinstance(data, dict):
        return {"max_slots": 1, "holders": []}
    holders = data.get("holders") or []
    if not isinstance(holders, list):
        holders = []
    max_slots = data.get("max_slots", 1)
    try:
        max_slots = int(max_slots)
    except (TypeError, ValueError):
        max_slots = 1
    if max_slots < 1:
        max_slots = 1
    return {"max_slots": max_slots, "holders": holders}


def _write_unlocked(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _with_lock(path: str, fn: typing.Callable[[], typing.Any]) -> typing.Any:
    """
    Serialize pool mutations with an exclusive flock on a sibling lock file.
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    lock_path = path + ".lock"
    with open(lock_path, "a+") as lock_f:
        fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
        try:
            return fn()
        finally:
            fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)


def _reap_dead_holders(holders: list[dict]) -> list[dict]:
    alive = []
    for holder in holders:
        if not isinstance(holder, dict):
            continue
        try:
            pid = int(holder.get("pid", -1))
        except (TypeError, ValueError):
            continue
        if _pid_alive(pid):
            alive.append(holder)
    return alive


def init_pool(path: str, max_slots: int) -> None:
    """
    Create or update the slot pool. Keeps live holders; sets max_slots.
    """
    if max_slots < 1:
        raise ValueError(f"max_slots must be >= 1, got {max_slots}")

    def _body() -> None:
        data = _read_unlocked(path)
        data["holders"] = _reap_dead_holders(data.get("holders") or [])
        data["max_slots"] = int(max_slots)
        _write_unlocked(path, data)

    _with_lock(path, _body)


def try_acquire(
        path: str,
        work_directory: str,
        stage: str,
        pid: int,
        ) -> bool:
    """
    Try to claim a local-stage slot. Returns True if acquired (or already held
    by this work_directory+stage+pid).
    """
    work_directory = os.path.abspath(work_directory)
    stage = str(stage)
    pid = int(pid)

    def _body() -> bool:
        data = _read_unlocked(path)
        holders = _reap_dead_holders(data.get("holders") or [])
        max_slots = int(data.get("max_slots", 1))
        for holder in holders:
            if (holder.get("work_directory") == work_directory
                    and holder.get("stage") == stage
                    and int(holder.get("pid", -1)) == pid):
                data["holders"] = holders
                _write_unlocked(path, data)
                return True
        if len(holders) >= max_slots:
            data["holders"] = holders
            _write_unlocked(path, data)
            return False
        holders.append({
            "work_directory": work_directory,
            "stage": stage,
            "pid": pid,
            "acquired_at": time.time(),
        })
        data["holders"] = holders
        data["max_slots"] = max_slots
        _write_unlocked(path, data)
        return True

    return bool(_with_lock(path, _body))


def release(
        path: str,
        work_directory: str,
        stage: str,
        pid: int | None = None,
        ) -> None:
    """
    Release a held slot. Best-effort; ignores missing entries.
    """
    work_directory = os.path.abspath(work_directory)
    stage = str(stage)

    def _body() -> None:
        data = _read_unlocked(path)
        holders = _reap_dead_holders(data.get("holders") or [])
        kept = []
        for holder in holders:
            if holder.get("work_directory") != work_directory:
                kept.append(holder)
                continue
            if holder.get("stage") != stage:
                kept.append(holder)
                continue
            if pid is not None and int(holder.get("pid", -1)) != int(pid):
                kept.append(holder)
                continue
            # Drop matching holder.
            continue
        data["holders"] = kept
        _write_unlocked(path, data)

    try:
        _with_lock(path, _body)
    except OSError:
        pass


def update_holder_pid(
        path: str,
        work_directory: str,
        stage: str,
        old_pid: int,
        new_pid: int,
        ) -> bool:
    """
    Point an existing holder at a different PID (e.g. the MD worker process).
    """
    work_directory = os.path.abspath(work_directory)
    stage = str(stage)
    old_pid = int(old_pid)
    new_pid = int(new_pid)

    def _body() -> bool:
        data = _read_unlocked(path)
        holders = _reap_dead_holders(data.get("holders") or [])
        updated = False
        for holder in holders:
            if (holder.get("work_directory") == work_directory
                    and holder.get("stage") == stage
                    and int(holder.get("pid", -1)) == old_pid):
                holder["pid"] = new_pid
                updated = True
                break
        data["holders"] = holders
        _write_unlocked(path, data)
        return updated

    return bool(_with_lock(path, _body))
