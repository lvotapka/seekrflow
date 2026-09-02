"""
Batch child process registry and lifecycle control.

The coordinator spawns one headless ``flow.py run`` child per system in its own
session, so children survive the parent. They are tracked here so that:

* exiting the monitor detaches them cleanly instead of orphaning them,
* a later ``batch run`` adopts a live child instead of spawning a duplicate,
* ``batch status`` / ``batch stop`` can work without a live parent.

"Detach" asks a child to write a final status snapshot and exit while leaving
its submitted scheduler jobs running; a later run reattaches to those jobs from
the job ids in the child's status file. "Stop" (``cancel_jobs=True``) signals
the child so its shutdown path cancels those jobs first.
"""

from __future__ import annotations

import fcntl
import json
import os
import signal
import time
import typing

import seekrflow.modules.batch.commands as batch_commands
import seekrflow.modules.batch.structures as batch_structures
import seekrflow.modules.run_lock as run_lock


CHILD_REGISTRY_FILENAME = ".seekrflow_batch_children.json"

# How long to let children finish a detach before escalating to signals.
DETACH_WAIT_SECONDS = 90.0
# How long to wait after SIGTERM before SIGKILL.
TERM_WAIT_SECONDS = 20.0
_POLL_SECONDS = 0.5


def registry_path(batch_directory: str) -> str:
    return os.path.join(batch_directory, CHILD_REGISTRY_FILENAME)


def _is_zombie(pid: int) -> bool:
    """
    True for an exited-but-unreaped process, which still answers signal 0.
    """
    try:
        with open(f"/proc/{pid}/stat", "rb") as f:
            fields = f.read().rsplit(b")", 1)[-1].split()
    except (OSError, IndexError):
        return False
    return bool(fields) and fields[0] == b"Z"


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
    children = data.get("children")
    if not isinstance(children, dict):
        return {}
    return {
        name: entry for name, entry in children.items()
        if isinstance(entry, dict)
    }


def _write_unlocked(path: str, children: dict[str, dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"children": children}, f, indent=2, default=str)
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


def read_registry(batch_directory: str) -> dict[str, dict]:
    """All recorded children, live or not."""
    return _read_unlocked(registry_path(batch_directory))


def live_children(batch_directory: str) -> dict[str, dict]:
    """Recorded children whose PIDs are still alive."""
    return {
        name: entry
        for name, entry in read_registry(batch_directory).items()
        if pid_alive(entry.get("pid"))
    }


def register_child(
        batch_directory: str,
        name: str,
        pid: int,
        work_directory: str,
        log_path: str = "",
        json_path: str = "",
        ) -> None:
    """Record one spawned child, replacing any previous entry for ``name``."""
    path = registry_path(batch_directory)
    try:
        pgid = os.getpgid(pid)
    except OSError:
        pgid = pid

    def _body() -> None:
        children = _read_unlocked(path)
        children[name] = {
            "pid": int(pid),
            "pgid": int(pgid),
            "work_directory": os.path.abspath(work_directory),
            "log_path": log_path,
            "json_path": json_path,
            "started_at": time.time(),
        }
        _write_unlocked(path, children)

    _with_lock(path, _body)


def prune_registry(batch_directory: str) -> dict[str, dict]:
    """Drop dead entries; return the surviving ones."""
    path = registry_path(batch_directory)

    def _body() -> dict[str, dict]:
        children = _read_unlocked(path)
        alive = {
            name: entry for name, entry in children.items()
            if pid_alive(entry.get("pid"))
        }
        if alive != children:
            _write_unlocked(path, alive)
        return alive

    return _with_lock(path, _body)


def find_live_owner(
        work_directory: str,
        root_name: str = "root",
        ) -> dict | None:
    """
    Owner metadata if a live seekrflow process holds this system's run lock.
    """
    root_directory = os.path.join(work_directory, root_name)
    if not run_lock.is_locked(root_directory):
        return None
    return run_lock.read_owner(root_directory) or {}


def root_directory_name(work_directory: str) -> str:
    """Resolve the root subdirectory name from the materialized config."""
    seekrflow_json = os.path.join(
        work_directory, batch_structures.SEEKRFLOW_JSON_NAME)
    try:
        with open(seekrflow_json, "r") as f:
            cfg = json.load(f)
    except (OSError, json.JSONDecodeError):
        return "root"
    return cfg.get("root_directory") or "root"


def remaining_job_ids(work_directory: str) -> list[str]:
    """Scheduler job ids recorded in a system's status file."""
    status = batch_structures.read_child_status(work_directory)
    if not status:
        return []
    job_ids: list[str] = []
    for info in (status.get("stages") or {}).values():
        if not isinstance(info, dict):
            continue
        for job_id in info.get("job_ids") or []:
            if job_id and str(job_id) not in job_ids:
                job_ids.append(str(job_id))
    return job_ids


def request_detach(work_directory: str) -> None:
    """Ask a child to write final status and exit, leaving its jobs running."""
    run_dir = os.path.join(work_directory, "run")
    batch_commands.append_detach_command(run_dir)


def _signal_child(entry: dict, sig: int) -> None:
    """Signal a child's whole process group (children are session leaders)."""
    pid = entry.get("pid")
    if not pid_alive(pid):
        return
    pid = int(pid)
    pgid = entry.get("pgid")
    try:
        pgid = int(pgid) if pgid else os.getpgid(pid)
    except OSError:
        pgid = None
    try:
        if pgid and pgid != os.getpgid(os.getpid()):
            os.killpg(pgid, sig)
        else:
            os.kill(pid, sig)
    except (ProcessLookupError, PermissionError, OSError) as e:
        print(f"[batch] signal {sig} to PID {pid} failed: {e}")


def _wait_for_exit(
        entries: dict[str, dict],
        timeout: float,
        reap: typing.Callable[[], None] | None = None,
        ) -> dict[str, dict]:
    """Poll until entries exit or timeout; return the ones still alive."""
    deadline = time.monotonic() + max(0.0, timeout)
    remaining = dict(entries)
    while remaining and time.monotonic() < deadline:
        if reap is not None:
            reap()
        remaining = {
            name: entry for name, entry in remaining.items()
            if pid_alive(entry.get("pid"))
        }
        if not remaining:
            break
        time.sleep(_POLL_SECONDS)
    if reap is not None:
        reap()
    return {
        name: entry for name, entry in remaining.items()
        if pid_alive(entry.get("pid"))
    }


def stop_children(
        batch_directory: str,
        entries: dict[str, dict] | None = None,
        cancel_jobs: bool = False,
        detach_timeout: float = DETACH_WAIT_SECONDS,
        term_timeout: float = TERM_WAIT_SECONDS,
        reap: typing.Callable[[], None] | None = None,
        ) -> dict[str, list[str]]:
    """
    Bring batch children down.

    With ``cancel_jobs`` false (the default) children are asked to detach, so
    scheduler jobs keep running and can be reattached later. With it true they
    are signalled so their shutdown path cancels those jobs.

    Returns ``{"detached": [...], "signalled": [...], "killed": [...]}``.
    """
    if entries is None:
        entries = live_children(batch_directory)
    entries = {
        name: entry for name, entry in entries.items()
        if pid_alive(entry.get("pid"))
    }
    result: dict[str, list[str]] = {
        "detached": [], "signalled": [], "killed": []}
    if not entries:
        prune_registry(batch_directory)
        return result

    if not cancel_jobs:
        for name, entry in entries.items():
            work_directory = entry.get("work_directory")
            if not work_directory:
                continue
            try:
                request_detach(work_directory)
            except OSError as e:
                print(f"[batch] could not write detach command for {name}: {e}")
        print(
            f"[batch] asked {len(entries)} child(ren) to detach; waiting up to "
            f"{detach_timeout:.0f}s (submitted jobs keep running)")
        remaining = _wait_for_exit(entries, detach_timeout, reap=reap)
        result["detached"] = [n for n in entries if n not in remaining]
    else:
        remaining = dict(entries)

    if remaining:
        verb = "stopping" if cancel_jobs else "did not detach in time; stopping"
        print(f"[batch] {len(remaining)} child(ren) {verb}: SIGTERM")
        for entry in remaining.values():
            _signal_child(entry, signal.SIGTERM)
        result["signalled"] = list(remaining)
        remaining = _wait_for_exit(remaining, term_timeout, reap=reap)

    if remaining:
        print(f"[batch] {len(remaining)} child(ren) ignored SIGTERM: SIGKILL")
        for entry in remaining.values():
            _signal_child(entry, signal.SIGKILL)
        result["killed"] = list(remaining)
        _wait_for_exit(remaining, 5.0, reap=reap)

    prune_registry(batch_directory)
    return result


def report_remaining_jobs(entries: dict[str, dict]) -> None:
    """Print scheduler jobs left running after a detach."""
    lines = []
    for name, entry in entries.items():
        work_directory = entry.get("work_directory")
        if not work_directory:
            continue
        job_ids = remaining_job_ids(work_directory)
        if job_ids:
            lines.append(f"    {name}: {', '.join(job_ids)}")
    if not lines:
        return
    print(
        "[batch] scheduler jobs left running (a later 'batch run' will "
        "reattach to them):")
    for line in lines:
        print(line)
