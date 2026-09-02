"""
manage.py

See and stop every seekrflow process belonging to this user, regardless of
which directory or batch started it.

    seekrflow ps
    seekrflow stop --all
    seekrflow stop --root /path/to/work/root
    seekrflow stop --older-than 7d --cancel-jobs

``stop`` detaches by default: each run writes a final status snapshot and exits
while its submitted scheduler jobs keep running, so a later run reattaches to
them from the job ids in that snapshot. ``--cancel-jobs`` instead signals the
run so its shutdown path cancels those jobs first.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sys
import time

import seekrflow.modules.process_registry as process_registry
import seekrflow.modules.batch.commands as batch_commands
import seekrflow.modules.batch.structures as batch_structures


DETACH_WAIT_SECONDS = 60.0
TERM_WAIT_SECONDS = 20.0
_POLL_SECONDS = 0.5

# Duplicated from seekr_run rather than imported: this CLI must start instantly
# and without the simulation stack installed.
STATUS_FILE_NAME = ".seekrflow_job_status.json"

_DURATION_UNITS = {
    "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0, "w": 604800.0}


def parse_duration(text: str) -> float:
    """
    Parse '30m', '2h', '7d' (bare numbers are seconds) into seconds.
    """
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([smhdw]?)\s*", text.lower())
    if not match:
        raise argparse.ArgumentTypeError(
            f"invalid duration {text!r}; use e.g. 30m, 6h, 7d")
    value, unit = match.groups()
    return float(value) * _DURATION_UNITS[unit or "s"]


def format_age(started_at: float) -> str:
    if not started_at:
        return "?"
    seconds = max(0.0, time.time() - float(started_at))
    if seconds < 3600:
        return f"{seconds / 60:.0f}m"
    if seconds < 86400:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def status_file_path(entry: dict) -> str | None:
    root = entry.get("root_directory")
    if not root:
        return None
    return os.path.join(str(root), STATUS_FILE_NAME)


def read_run_status(entry: dict) -> dict | None:
    """
    A run's status snapshot, by root directory, falling back to work layout.
    """
    path = status_file_path(entry)
    if path and os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return None
    work_directory = entry.get("work_directory") or ""
    if not work_directory:
        return None
    return batch_structures.read_child_status(work_directory)


def stage_summary(entry: dict) -> tuple[str, str]:
    """
    ``(stage/state, job ids)`` from a run's status file, for display.
    """
    status = read_run_status(entry)
    if not status:
        return "-", ""
    stages = status.get("stages") or {}
    job_ids: list[str] = []
    active = "-"
    for stage_name, info in stages.items():
        if not isinstance(info, dict):
            continue
        for job_id in info.get("job_ids") or []:
            if job_id and str(job_id) not in job_ids:
                job_ids.append(str(job_id))
        if info.get("state") == "started" and active == "-":
            active = f"{stage_name}/started"
    if active == "-" and stages:
        name, info = next(iter(stages.items()))
        active = f"{name}/{(info or {}).get('state', '?')}"
    return active, ", ".join(job_ids)


def collect(root: str | None = None, older_than: float | None = None
            ) -> list[dict]:
    """
    Live seekrflow processes matching the given filters.
    """
    entries = process_registry.live_processes()
    roots = {
        entry.get("root_directory") for entry in process_registry.read_all()
        .values() if entry.get("root_directory")
    }
    if root:
        roots.add(os.path.abspath(root))
    entries.extend(process_registry.find_unregistered_lock_owners(
        r for r in roots if r))
    if root:
        target = os.path.abspath(root)
        entries = [
            e for e in entries
            if e.get("root_directory") == target
            or e.get("work_directory") == target
        ]
    if older_than is not None:
        cutoff = time.time() - older_than
        entries = [
            e for e in entries if (e.get("started_at") or 0.0) < cutoff]
    return entries


def print_table(entries: list[dict]) -> None:
    from rich.console import Console
    from rich.table import Table

    table = Table(title="Seekrflow processes")
    table.add_column("PID", no_wrap=True)
    table.add_column("Name", no_wrap=True, overflow="ellipsis")
    table.add_column("Instr", no_wrap=True)
    table.add_column("Age", justify="right", no_wrap=True)
    table.add_column("Lock", no_wrap=True)
    table.add_column("Stage/State", no_wrap=True, overflow="ellipsis")
    table.add_column("Jobs", overflow="fold")
    table.add_column("Root", overflow="fold")

    for entry in entries:
        stage, jobs = stage_summary(entry)
        name = entry.get("name") or "-"
        if entry.get("unregistered"):
            name = f"{name} (unregistered)"
        table.add_row(
            str(entry.get("pid", "?")),
            name,
            entry.get("instruction") or "-",
            format_age(entry.get("started_at") or 0.0),
            "held" if entry.get("holds_run_lock") else "-",
            stage,
            jobs,
            entry.get("root_directory") or "-",
        )
    Console().print(table)


def request_detach(entry: dict) -> bool:
    """
    Ask a run to detach via its command file. False if that is not possible.
    """
    work_directory = entry.get("work_directory")
    if not work_directory or not os.path.isdir(work_directory):
        return False
    if read_run_status(entry) is None:
        # Nothing is writing status here, so nothing will read the command
        # file either; go straight to signalling rather than waiting it out.
        return False
    run_dir = os.path.join(work_directory, "run")
    try:
        os.makedirs(run_dir, exist_ok=True)
        batch_commands.append_detach_command(run_dir)
    except OSError as e:
        print(f"[stop] could not write detach command for PID "
              f"{entry.get('pid')}: {e}")
        return False
    return True


def signal_process(entry: dict, sig: int) -> None:
    pid = entry.get("pid")
    if not process_registry.pid_alive(pid):
        return
    pid = int(pid)
    try:
        pgid = os.getpgid(pid)
    except OSError:
        pgid = None
    try:
        # Session leaders (batch children) get the whole group so their
        # workers go too; never signal our own group.
        if pgid and pgid == pid and pgid != os.getpgid(os.getpid()):
            os.killpg(pgid, sig)
        else:
            os.kill(pid, sig)
    except (ProcessLookupError, PermissionError, OSError) as e:
        print(f"[stop] signal {sig} to PID {pid} failed: {e}")


def wait_for_exit(entries: list[dict], timeout: float) -> list[dict]:
    deadline = time.monotonic() + max(0.0, timeout)
    remaining = list(entries)
    while remaining and time.monotonic() < deadline:
        remaining = [
            e for e in remaining
            if process_registry.pid_alive(e.get("pid"))]
        if not remaining:
            break
        time.sleep(_POLL_SECONDS)
    return [
        e for e in remaining if process_registry.pid_alive(e.get("pid"))]


def stop_entries(
        entries: list[dict],
        cancel_jobs: bool = False,
        detach_timeout: float = DETACH_WAIT_SECONDS,
        term_timeout: float = TERM_WAIT_SECONDS,
        ) -> int:
    if not entries:
        print("[stop] nothing to stop.")
        return 0
    remaining = list(entries)
    if not cancel_jobs:
        asked = [e for e in remaining if request_detach(e)]
        skipped = len(remaining) - len(asked)
        if skipped:
            print(f"[stop] {skipped} process(es) are not writing status and "
                  "cannot be asked to detach; signalling those directly")
        if asked:
            print(f"[stop] asked {len(asked)} process(es) to detach; waiting "
                  f"up to {detach_timeout:.0f}s (submitted jobs keep running)")
            remaining = wait_for_exit(asked, detach_timeout) + [
                e for e in remaining if e not in asked]
        for entry in entries:
            if entry not in remaining:
                print(f"[stop] detached: PID {entry.get('pid')} "
                      f"{entry.get('name') or ''}".rstrip())

    if remaining:
        print(f"[stop] sending SIGTERM to {len(remaining)} process(es)")
        for entry in remaining:
            signal_process(entry, signal.SIGTERM)
        remaining = wait_for_exit(remaining, term_timeout)

    if remaining:
        print(f"[stop] SIGKILL for {len(remaining)} unresponsive process(es)")
        for entry in remaining:
            signal_process(entry, signal.SIGKILL)
        remaining = wait_for_exit(remaining, 5.0)

    process_registry.prune()
    if remaining:
        pids = ", ".join(str(e.get("pid")) for e in remaining)
        print(f"[stop] still alive after SIGKILL: {pids}")
        return 1
    print("[stop] all targeted processes stopped.")
    return 0


def main(argv: list[str] | None = None) -> int:
    argparser = argparse.ArgumentParser(
        prog="seekrflow",
        description=(
            "Inspect and stop seekrflow processes belonging to this user. "
            "Use 'seekrflow ps' to see what is running anywhere on this "
            "machine, and 'seekrflow stop' to bring runs down cleanly."),
    )
    sub = argparser.add_subparsers(dest="command", required=True)

    ps = sub.add_parser("ps", help="List live seekrflow processes.")
    ps.add_argument(
        "--root", dest="root", metavar="DIR", default=None,
        help="Only show the run owning this model root (or work) directory.")
    ps.add_argument(
        "--older-than", dest="older_than", metavar="DURATION",
        type=parse_duration, default=None,
        help="Only show runs started longer ago than this (e.g. 6h, 7d).")

    stop = sub.add_parser(
        "stop", help="Detach (default) or stop live seekrflow processes.")
    target = stop.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--all", dest="all", action="store_true",
        help="Target every live seekrflow process for this user.")
    target.add_argument(
        "--root", dest="root", metavar="DIR", default=None,
        help="Target the run owning this model root (or work) directory.")
    target.add_argument(
        "--pid", dest="pid", metavar="PID", type=int, default=None,
        help="Target one process by PID.")
    stop.add_argument(
        "--older-than", dest="older_than", metavar="DURATION",
        type=parse_duration, default=None,
        help="Further restrict to runs started longer ago than this.")
    stop.add_argument(
        "--cancel-jobs", dest="cancel_jobs", action="store_true", default=False,
        help="Also cancel the scheduler jobs these runs submitted. Without "
        "it, runs detach and their jobs keep running.")
    stop.add_argument(
        "--dry-run", dest="dry_run", action="store_true", default=False,
        help="Show what would be stopped and exit.")

    args = argparser.parse_args(argv)

    if args.command == "ps":
        entries = collect(root=args.root, older_than=args.older_than)
        if not entries:
            print("No live seekrflow processes.")
            return 0
        print_table(entries)
        print(f"registry: {process_registry.registry_path()}")
        return 0

    entries = collect(root=args.root, older_than=args.older_than)
    if args.pid is not None:
        entries = [e for e in entries if int(e.get("pid", -1)) == args.pid]
        if not entries and process_registry.pid_alive(args.pid):
            # Not in the registry (older code); we can still signal it.
            entries = [{"pid": args.pid, "name": "", "work_directory": ""}]
    if args.dry_run:
        if not entries:
            print("Nothing would be stopped.")
            return 0
        print_table(entries)
        print(
            "[dry-run] would "
            f"{'stop and cancel jobs for' if args.cancel_jobs else 'detach'} "
            f"{len(entries)} process(es)")
        return 0
    return stop_entries(entries, cancel_jobs=args.cancel_jobs)


if __name__ == "__main__":
    sys.exit(main())
