"""
Batch coordinator: materialize configs, stage-gated child processes, UI.
"""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import sys
import typing
from concurrent.futures import ThreadPoolExecutor, as_completed

import seekrflow.modules.batch.structures as batch_structures
import seekrflow.modules.batch.commands as batch_commands
import seekrflow.modules.batch.children as batch_children
import seekrflow.modules.batch.ui as batch_ui
import seekrflow.modules.run_lock as run_lock


FLOW_MODULE = "seekrflow.flow"


def _tail_log(path: str, n_lines: int = 20) -> str:
    if not path or not os.path.exists(path):
        return ""
    try:
        with open(path, "r", errors="replace") as f:
            lines = f.readlines()
        return "".join(lines[-n_lines:]).strip()
    except OSError:
        return ""


def _child_log_path(work_directory: str, stage: str) -> str:
    logs = os.path.join(work_directory, "logs")
    os.makedirs(logs, exist_ok=True)
    return os.path.join(logs, f"batch_child_{stage}.log")


def spawn_flow_child(
        instruction: str,
        seekrflow_json: str,
        log_path: str,
        poll_interval: float | None = None,
        skip_checks: bool = False,
        local_slot_file: str | None = None,
        globus_lock_file: str | None = None,
        extra_args: list[str] | None = None,
        ) -> subprocess.Popen:
    cmd = [
        sys.executable, "-m", FLOW_MODULE,
        instruction,
        "-i", seekrflow_json,
        "--no-keystrokes",
        "--batch-child",
    ]
    if poll_interval is not None:
        cmd.extend(["--poll-interval", str(poll_interval)])
    if skip_checks:
        cmd.append("--skip_checks")
    if local_slot_file is not None:
        cmd.extend(["--local-slot-file", local_slot_file])
    if extra_args:
        cmd.extend(extra_args)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    log_f = open(log_path, "w")
    env = os.environ.copy()
    if globus_lock_file is not None:
        env["SEEKR_GLOBUS_LOCK_FILE"] = globus_lock_file
    proc = subprocess.Popen(
        cmd,
        stdout=log_f,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        env=env,
    )
    proc._batch_log_file = log_f  # type: ignore[attr-defined]
    return proc


def _close_proc_log(proc) -> None:
    log_f = getattr(proc, "_batch_log_file", None)
    if log_f is not None:
        try:
            log_f.close()
        except Exception:
            pass


def run_stage_for_system(
        name: str,
        seekrflow_json: str,
        work_directory: str,
        instruction: str,
        poll_interval: float | None = None,
        skip_checks: bool = False,
        globus_lock_file: str | None = None,
        extra_args: list[str] | None = None,
        log_stage: str | None = None,
        ) -> tuple[str, int, str, str]:
    """
    Run one flow instruction for one system. Returns
    (name, returncode, log_path, error_snippet).
    """
    log_path = _child_log_path(work_directory, log_stage or instruction)
    proc = spawn_flow_child(
        instruction, seekrflow_json, log_path,
        poll_interval=poll_interval, skip_checks=skip_checks,
        globus_lock_file=globus_lock_file,
        extra_args=extra_args)
    returncode = proc.wait()
    _close_proc_log(proc)
    err = ""
    if returncode != 0:
        err = _tail_log(log_path)
    return name, returncode, log_path, err


def run_gated_stage(
        batch: batch_structures.Batch,
        json_paths: dict[str, str],
        instruction: str,
        rows: list[batch_ui.SystemRow],
        concurrency: int = 1,
        poll_interval: float | None = None,
        skip_checks: bool = False,
        extra_args: list[str] | None = None,
        log_stage: str | None = None,
        ) -> bool:
    """
    Run instruction for all active systems. Returns True if all succeeded.
    """
    import seekrflow.modules.batch.globus_lock as globus_lock

    display_stage = log_stage or instruction
    row_by_name = {r.name: r for r in rows}
    globus_path = globus_lock.lock_file_path(batch.batch_directory_resolved())
    globus_lock.init_lock(globus_path)
    work_items = []
    for name, json_path in json_paths.items():
        row = row_by_name[name]
        row.batch_stage = display_stage
        row.state = "pending"
        row.log_path = _child_log_path(row.work_directory, display_stage)
        work_items.append((name, json_path, row.work_directory))

    failures: list[str] = []
    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        futures = [
            pool.submit(
                run_stage_for_system,
                name, json_path, work_dir, instruction,
                poll_interval, skip_checks, globus_path,
                extra_args, display_stage if log_stage else None,
            )
            for name, json_path, work_dir in work_items
        ]
        for fut in as_completed(futures):
            name, rc, log_path, err = fut.result()
            row = row_by_name[name]
            row.log_path = log_path
            if rc == 0:
                row.state = "completed"
                row.last_error = ""
                print(f"[batch] {display_stage} OK: {name}")
            elif rc == run_lock.DUPLICATE_RUN_EXIT_CODE:
                row.state = "duplicate"
                row.last_error = (
                    "another seekrflow process already owns this work "
                    "directory")
                failures.append(name)
                extra = ""
                if extra_args and "-T" in extra_args:
                    extra = (
                        " Stop the live run first "
                        "(batch.py stop -i <batch.json>).")
                print(
                    f"[batch] {display_stage} REFUSED: {name} is already owned "
                    f"by another live seekrflow process.{extra} See {log_path}")
            else:
                row.state = "failed"
                row.last_error = err.splitlines()[-1] if err else f"exit {rc}"
                failures.append(name)
                print(
                    f"[batch] {display_stage} FAILED: {name} "
                    f"(exit={rc}). See {log_path}")
                if err:
                    print(err[-500:])

    batch_structures.write_batch_status(
        batch.batch_directory_resolved(),
        {
            "stage": display_stage,
            "failures": failures,
            "systems": {
                r.name: {
                    "state": r.state,
                    "last_error": r.last_error,
                    "log_path": r.log_path,
                    "skipped": r.skipped,
                }
                for r in rows
            },
        },
    )
    return len(failures) == 0


def run_all_children_with_ui(
        batch: batch_structures.Batch,
        json_paths: dict[str, str],
        rows: list[batch_ui.SystemRow],
        skip_checks: bool = False,
        ) -> bool:
    """
    Spawn run children for all systems immediately, drive the summary/detail
    UI, and wait until all exit.

    A system already owned by a live child (see ``batch/children.py``) is
    adopted for monitoring rather than started a second time; two children on
    one work directory would corrupt each other's status file and submit
    duplicate scheduler jobs.

    Local-stage concurrency is enforced inside each child via the shared
    ``max_concurrent_local_runs`` slot pool (not by delaying child spawn).
    Globus Compute calls are serialized via ``SEEKR_GLOBUS_LOCK_FILE``.

    Leaving the monitor detaches the children: they write a final status
    snapshot and exit while their scheduler jobs keep running, and a later
    ``batch run`` reattaches from the job ids in that snapshot.

    A clean run (not a resume) requires wiping each system's
    ``work_*/root/.seekrflow_job_status.json`` (or the work directories).
    Restoring ``stop``/``wait`` from that file is intentional for resume.
    """
    import seekrflow.modules.batch.local_slots as local_slots
    import seekrflow.modules.batch.globus_lock as globus_lock

    row_by_name = {r.name: r for r in rows}
    procs: dict[str, typing.Any] = {}
    entries: dict[str, dict] = {}
    adopted: set[str] = set()
    finalized: set[str] = set()
    focused: str | None = None
    # Set while bringing children down, so a clean exit reads as "detached"
    # (jobs still running) rather than "completed".
    detaching = {"value": False}
    max_local = batch.max_concurrent_local_runs
    batch_dir = batch.batch_directory_resolved()
    slot_path = local_slots.slot_file_path(batch_dir)
    local_slots.init_pool(slot_path, max_local)
    globus_path = globus_lock.lock_file_path(batch_dir)
    globus_lock.init_lock(globus_path)
    print(
        f"[batch] local stage slot pool: {slot_path} "
        f"(max_concurrent_local_runs={max_local})")
    print(f"[batch] globus compute lock: {globus_path}")
    registered = batch_children.prune_registry(batch_dir)

    for name, json_path in json_paths.items():
        row = row_by_name[name]
        row.batch_stage = "run"
        row.state = "pending"
        row.log_path = _child_log_path(row.work_directory, "run")
        run_dir = os.path.join(row.work_directory, "run")
        os.makedirs(run_dir, exist_ok=True)
        existing = adopt_existing_child(batch_dir, name, row, registered)
        if existing is not None:
            entries[name] = existing
            adopted.add(name)
            continue
        # Stale commands from a previous session must not be replayed by the
        # child, which starts reading this file from the beginning.
        batch_commands.archive_batch_commands(run_dir)
        print(f"[batch] starting run child: {name}")
        proc = spawn_flow_child(
            "run", json_path, row.log_path,
            poll_interval=batch.background_poll_interval,
            skip_checks=skip_checks,
            local_slot_file=slot_path,
            globus_lock_file=globus_path,
        )
        procs[name] = proc
        entries[name] = {
            "pid": proc.pid,
            "pgid": proc.pid,
            "work_directory": row.work_directory,
            "log_path": row.log_path,
            "json_path": json_path,
        }
        batch_children.register_child(
            batch_dir, name, proc.pid, row.work_directory,
            log_path=row.log_path, json_path=json_path)
        row.child_pid = proc.pid

    def child_alive(name: str) -> bool:
        proc = procs.get(name)
        if proc is not None:
            return proc.poll() is None
        entry = entries.get(name)
        if entry is None:
            return False
        return batch_children.pid_alive(entry.get("pid"))

    def set_focus(name: str | None) -> None:
        nonlocal focused
        if name == focused:
            return
        focused = name
        for row in rows:
            if row.skipped or row.name not in entries:
                continue
            run_dir = os.path.join(row.work_directory, "run")
            seconds = (
                batch.focused_poll_interval
                if name is not None and row.name == name
                else batch.background_poll_interval
            )
            batch_commands.append_poll_interval_command(run_dir, seconds)

    def _finalize_proc(name: str) -> None:
        if name in finalized:
            return
        if child_alive(name):
            return
        finalized.add(name)
        row = row_by_name[name]
        proc = procs.get(name)
        if proc is None:
            # Adopted child: no exit status available, so trust its snapshot.
            if row.state not in {"completed", "failed"}:
                row.state = "detached"
            return
        _close_proc_log(proc)
        if proc.returncode == 0:
            if row.state not in {"completed", "failed"}:
                # A detached child exits cleanly with work still outstanding.
                row.state = "detached" if detaching["value"] else "completed"
        elif proc.returncode == run_lock.DUPLICATE_RUN_EXIT_CODE:
            row.state = "duplicate"
            row.last_error = (
                "another seekrflow process already owns this work directory")
        elif proc.returncode < 0 and detaching["value"]:
            row.state = "failed"
            row.last_error = (
                f"did not detach; terminated with signal "
                f"{-proc.returncode}")
        else:
            row.state = "failed"
            tail = _tail_log(row.log_path)
            row.last_error = (
                tail.splitlines()[-1] if tail else f"exit {proc.returncode}")

    def reap() -> None:
        for name in list(entries.keys()):
            if not child_alive(name):
                _finalize_proc(name)

    def refresh_rows() -> None:
        reap()
        for row in rows:
            if row.skipped:
                continue
            if row.name in entries:
                row.refresh_from_status_file()
        # Selected system (summary or detail) gets focused_poll_interval;
        # others stay on background_poll_interval.
        if rows:
            sel = rows[ui.selected]
            if not sel.skipped and sel.name in entries:
                set_focus(sel.name)
            elif focused is not None:
                set_focus(None)

    def should_stop() -> bool:
        if not entries:
            return True
        return not any(child_alive(name) for name in entries)

    def on_command(line: str, target) -> None:
        batch_ui.fanout_command_to_systems(line, target)

    ui = batch_ui.BatchUI(
        rows=rows,
        on_command=on_command,
        refresh_rows=refresh_rows,
        stale_after_s=2.0 * batch.background_poll_interval,
    )
    set_focus(None)
    if adopted:
        print(
            f"[batch] adopted {len(adopted)} already-running child(ren): "
            f"{', '.join(sorted(adopted))}")
    print("[batch] run stage started; opening monitor UI...")
    with monitor_shutdown_signals(ui):
        try:
            ui.run(should_stop=should_stop, poll_seconds=1.0)
        finally:
            if getattr(ui, "detach_requested", False):
                print(
                    "[batch] detach command received. Please wait for all "
                    "processes to detach.")
            detaching["value"] = True
            detach_children(batch_dir, entries, reap=reap)

    reap()
    failures = [
        name for name in entries if row_by_name[name].state in
        {"failed", "duplicate"}
    ]

    batch_structures.write_batch_status(
        batch_dir,
        {
            "stage": "run",
            "failures": failures,
            "max_concurrent_local_runs": max_local,
            "local_slot_file": slot_path,
            "systems": {
                r.name: {
                    "state": r.state,
                    "last_error": r.last_error,
                    "log_path": r.log_path,
                    "skipped": r.skipped,
                }
                for r in rows
            },
        },
    )
    return len(failures) == 0


@contextlib.contextmanager
def monitor_shutdown_signals(ui: batch_ui.BatchUI):
    """
    Ask the monitor to close on SIGTERM/SIGHUP so children still get detached.

    Without this, terminating the coordinator (or closing its terminal) would
    leave the children running unsupervised, and the next ``batch run`` would
    find their work directories locked.
    """
    def _request_stop(signum, _frame):
        print(f"\n[batch] received signal {signum}; closing monitor...")
        ui._stop = True

    previous: dict[int, typing.Any] = {}
    for signum in (signal.SIGTERM, signal.SIGHUP):
        try:
            previous[signum] = signal.signal(signum, _request_stop)
        except (ValueError, OSError, AttributeError):
            # Not the main thread, or the signal does not exist here.
            pass
    try:
        yield
    finally:
        for signum, handler in previous.items():
            try:
                signal.signal(signum, handler)
            except (ValueError, OSError):
                pass


def adopt_existing_child(
        batch_dir: str,
        name: str,
        row: batch_ui.SystemRow,
        registered: dict[str, dict],
        ) -> dict | None:
    """
    Registry/run-lock entry for a live child already owning this system.

    Returns None when the system is free to start.
    """
    entry = registered.get(name)
    if entry is not None and batch_children.pid_alive(entry.get("pid")):
        row.child_pid = int(entry["pid"])
        row.log_path = entry.get("log_path") or row.log_path
        print(
            f"[batch] {name}: adopting live child PID {entry['pid']} "
            "instead of starting a second one")
        return dict(entry)
    root_name = batch_children.root_directory_name(row.work_directory)
    owner = batch_children.find_live_owner(row.work_directory, root_name)
    if owner is None:
        return None
    owner_pid = owner.get("pid")
    print(
        f"[batch] {name}: work directory is locked by live PID {owner_pid}; "
        "monitoring it instead of starting a second child")
    if batch_children.pid_alive(owner_pid):
        row.child_pid = int(owner_pid)
        adopted_entry = {
            "pid": int(owner_pid),
            "pgid": int(owner_pid),
            "work_directory": row.work_directory,
            "log_path": row.log_path,
            "json_path": "",
        }
        batch_children.register_child(
            batch_dir, name, int(owner_pid), row.work_directory,
            log_path=row.log_path)
        return adopted_entry
    return None


def detach_children(
        batch_dir: str,
        entries: dict[str, dict],
        reap: typing.Callable[[], None] | None = None,
        ) -> None:
    """
    Bring down every still-running child without cancelling its jobs.
    """
    live = {
        name: entry for name, entry in entries.items()
        if batch_children.pid_alive(entry.get("pid"))
    }
    if not live:
        batch_children.prune_registry(batch_dir)
        return
    batch_children.stop_children(
        batch_dir, live, cancel_jobs=False, reap=reap)
    batch_children.report_remaining_jobs(live)


def run_analyses(
        batch: batch_structures.Batch,
        rows: list[batch_ui.SystemRow],
        ) -> None:
    system_work_dirs = [
        (r.name, r.work_directory)
        for r in rows
        if not r.skipped
    ]
    if not batch.batch_analyses:
        print("[batch] no batch_analyses configured; skipping analyze.")
        return
    for analyzer in batch.batch_analyses:
        print(f"[batch] running analysis: {analyzer.type}")
        analyzer.run(batch, system_work_dirs)


def report_batch_status(batch: batch_structures.Batch) -> int:
    """
    Print one row per system: child liveness, lock owner, and stage status.
    """
    from rich.console import Console
    from rich.table import Table

    batch_dir = batch.batch_directory_resolved()
    registry = batch_children.read_registry(batch_dir)
    table = Table(title=f"Seekrflow Batch Status: {batch_dir}")
    table.add_column("System", no_wrap=True, overflow="ellipsis")
    table.add_column("Child", no_wrap=True)
    table.add_column("Stage", no_wrap=True, overflow="ellipsis")
    table.add_column("State", no_wrap=True)
    table.add_column("Prog", justify="right", no_wrap=True)
    table.add_column("Sem", no_wrap=True)
    table.add_column("Manager", no_wrap=True)
    table.add_column("Jobs", overflow="fold")

    live_count = 0
    for row in make_rows(batch):
        if row.skipped:
            table.add_row(row.name, "skipped", "-", "-", "-", "-", "-", "")
            continue
        entry = registry.get(row.name) or {}
        pid = entry.get("pid")
        alive = batch_children.pid_alive(pid)
        if alive:
            live_count += 1
            child_text = f"{int(pid)}"
            row.child_pid = int(pid)
        else:
            root_name = batch_children.root_directory_name(row.work_directory)
            owner = batch_children.find_live_owner(row.work_directory, root_name)
            if owner:
                live_count += 1
                child_text = f"{owner.get('pid', '?')}*"
                try:
                    row.child_pid = int(owner.get("pid"))
                except (TypeError, ValueError):
                    row.child_pid = None
            else:
                child_text = "-"
        row.refresh_from_status_file()
        job_ids = batch_children.remaining_job_ids(row.work_directory)
        table.add_row(
            row.name,
            child_text,
            row.batch_stage,
            row.state,
            row.progress,
            str(row.semaphore),
            row.manager_status,
            ", ".join(job_ids),
        )

    Console().print(table)
    print(
        f"[batch] {live_count} live child(ren); "
        f"'*' marks a run-lock owner not in the child registry.")
    print(f"[batch] child registry: {batch_children.registry_path(batch_dir)}")
    return 0


def stop_batch(
        batch: batch_structures.Batch,
        cancel_jobs: bool = False,
        ) -> int:
    """
    Detach (default) or stop all live children of this batch.
    """
    batch_dir = batch.batch_directory_resolved()
    live = batch_children.live_children(batch_dir)
    # Include lock owners that never made it into the registry.
    for row in make_rows(batch):
        if row.skipped or row.name in live:
            continue
        root_name = batch_children.root_directory_name(row.work_directory)
        owner = batch_children.find_live_owner(row.work_directory, root_name)
        if not owner or not batch_children.pid_alive(owner.get("pid")):
            continue
        live[row.name] = {
            "pid": int(owner["pid"]),
            "pgid": int(owner["pid"]),
            "work_directory": row.work_directory,
        }
    if not live:
        print("[batch] no live children found.")
        return 0
    if cancel_jobs:
        print(
            f"[batch] stopping {len(live)} child(ren) and cancelling their "
            "scheduler jobs.")
    else:
        print(
            f"[batch] detaching {len(live)} child(ren); scheduler jobs keep "
            "running.")
    result = batch_children.stop_children(
        batch_dir, live, cancel_jobs=cancel_jobs)
    for action, names in result.items():
        if names:
            print(f"[batch] {action}: {', '.join(sorted(names))}")
    if not cancel_jobs:
        batch_children.report_remaining_jobs(live)
    return 0


def make_rows(batch: batch_structures.Batch) -> list[batch_ui.SystemRow]:
    rows = []
    for system in batch.systems:
        work = batch.system_work_directory(system.name)
        rows.append(
            batch_ui.SystemRow(
                name=system.name,
                work_directory=work,
                skipped=system.skip,
            )
        )
    return rows


def run_batch(
        batch: batch_structures.Batch,
        instruction: str,
        skip_checks: bool = False,
        transfer_from_remote_only: str | None = None,
        ) -> int:
    """
    Top-level batch orchestration. Returns process exit code (0 = success).
    """
    assert instruction in {
        "any", "parameterize", "prepare", "run", "analyze",
    }, f"Invalid batch instruction: {instruction}"

    batch_dir = batch.batch_directory_resolved()
    os.makedirs(batch_dir, exist_ok=True)
    rows = make_rows(batch)
    active = batch_structures.active_systems(batch)
    if not active and instruction != "analyze":
        print("[batch] no active (non-skipped) systems.")
        return 1

    print(f"[batch] materializing {len(active)} system(s) under {batch_dir}")
    json_paths = batch_structures.materialize_all(batch)

    needs_param = batch.needs_parameterize()
    do_param = instruction in ("parameterize", "any") and needs_param
    do_prepare = instruction in ("prepare", "any")
    do_run = instruction in ("run", "any")
    do_analyze = instruction in ("analyze", "any")

    if instruction == "parameterize" and not needs_param:
        print("[batch] template has no parameterizer; nothing to do.")
        return 0

    if instruction == "any" and not needs_param:
        print("[batch] skipping parameterize (no parameterizer in template).")

    if do_param:
        ok = run_gated_stage(
            batch, json_paths, "parameterize", rows,
            concurrency=batch.prepare_concurrency,
            skip_checks=skip_checks,
        )
        if not ok:
            print(
                "[batch] parameterize failed for one or more systems; "
                "blocking further stages. Set skip: true on failed systems "
                "to proceed without them.")
            return 1

    if do_prepare:
        ok = run_gated_stage(
            batch, json_paths, "prepare", rows,
            concurrency=batch.prepare_concurrency,
            skip_checks=skip_checks,
        )
        if not ok:
            print(
                "[batch] prepare failed for one or more systems; "
                "blocking run. Set skip: true on failed systems to proceed "
                "without them.")
            return 1

    if do_run:
        if transfer_from_remote_only:
            print(
                f"[batch] transfer-only from remote "
                f"(stage={transfer_from_remote_only}); "
                "not starting monitors or submitting jobs.")
            print(
                "[batch] if a system is owned by a live child, "
                "stop that run first (batch.py stop).")
            ok = run_gated_stage(
                batch, json_paths, "run", rows,
                concurrency=1,
                skip_checks=skip_checks,
                extra_args=["-T", transfer_from_remote_only],
                log_stage="transfer",
            )
        else:
            # For run-only, still require prior prepare success unless status
            # says OK. Re-materialized systems are assumed ready if user
            # asked for run.
            ok = run_all_children_with_ui(
                batch, json_paths, rows, skip_checks=skip_checks)
        if not ok:
            print("[batch] one or more run children failed.")
            return 1

    if do_analyze:
        run_analyses(batch, rows)

    print("[batch] done.")
    return 0
