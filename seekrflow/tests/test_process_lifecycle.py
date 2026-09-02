"""
Tests for seekrflow process lifecycle safety: the per-user process registry,
the ``seekrflow ps``/``stop`` CLI, the orphan watchdog, and the guarantee that
a stop signal always ends a monitor.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time

import pytest

import seekrflow.manage as manage
import seekrflow.modules.process_registry as process_registry
import seekrflow.modules.run_lock as run_lock
import seekrflow.modules.batch.globus_lock as globus_lock


@pytest.fixture
def user_home(tmp_path, monkeypatch):
    """Isolate ~/.seekrflow so tests never touch the real registry."""
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setattr(os.path, "expanduser", lambda p: p.replace(
        "~", str(home), 1) if p.startswith("~") else p)
    # Earlier tests may leave the process in a deleted directory; registration
    # must not depend on the cwd, but neither should these tests.
    try:
        os.getcwd()
    except OSError:
        os.chdir(tmp_path)
    return home


def _registry_entry(tmp_path, pid, name="demo", age_seconds=0.0):
    work = tmp_path / f"work_{name}"
    root = work / "root"
    (work / "run").mkdir(parents=True, exist_ok=True)
    root.mkdir(parents=True, exist_ok=True)
    return {
        "pid": pid,
        "name": name,
        "instruction": "run",
        "root_directory": str(root),
        "work_directory": str(work),
        "started_at": time.time() - age_seconds,
    }


def test_registry_records_and_prunes(user_home, tmp_path):
    entry = _registry_entry(tmp_path, os.getpid())
    process_registry.register(
        entry["root_directory"], "run",
        work_directory=entry["work_directory"], name="demo")
    recorded = process_registry.read_all()
    assert str(os.getpid()) in recorded
    assert recorded[str(os.getpid())]["name"] == "demo"
    assert recorded[str(os.getpid())]["root_directory"] == \
        entry["root_directory"]

    # A dead PID is dropped, a live one is kept.
    path = process_registry.registry_path()
    data = json.loads(open(path).read())
    data["processes"]["999999999"] = _registry_entry(
        tmp_path, 999999999, name="dead")
    with open(path, "w") as f:
        json.dump(data, f)
    survivors = process_registry.prune()
    assert str(os.getpid()) in survivors
    assert "999999999" not in survivors

    process_registry.unregister()
    assert str(os.getpid()) not in process_registry.read_all()


def test_registry_reports_run_lock_ownership(user_home, tmp_path):
    entry = _registry_entry(tmp_path, os.getpid())
    process_registry.register(
        entry["root_directory"], "run",
        work_directory=entry["work_directory"], name="demo")
    lock = run_lock.acquire(entry["root_directory"], "run")
    try:
        live = process_registry.live_processes()
        assert len(live) == 1
        assert live[0]["holds_run_lock"] is True
    finally:
        lock.release()
    assert process_registry.live_processes()[0]["holds_run_lock"] is False


def test_find_unregistered_lock_owners(user_home, tmp_path):
    """A run started by older code shows up via its run lock."""
    root = tmp_path / "orphan_root"
    lock = run_lock.acquire(str(root), "run")
    try:
        owners = process_registry.find_unregistered_lock_owners([str(root)])
        assert len(owners) == 1
        assert owners[0]["pid"] == os.getpid()
        assert owners[0]["unregistered"] is True

        # Once registered, it is no longer reported as unregistered.
        process_registry.register(str(root), "run")
        assert process_registry.find_unregistered_lock_owners([str(root)]) == []
    finally:
        lock.release()
        process_registry.unregister()


def test_parse_duration_and_age_formatting():
    assert manage.parse_duration("30") == 30.0
    assert manage.parse_duration("30m") == 1800.0
    assert manage.parse_duration("6h") == 21600.0
    assert manage.parse_duration("7d") == 604800.0
    assert manage.parse_duration("2w") == 1209600.0
    with pytest.raises(Exception):
        manage.parse_duration("soon")
    assert manage.format_age(time.time() - 120).endswith("m")
    assert manage.format_age(time.time() - 7200).endswith("h")
    assert manage.format_age(time.time() - 3 * 86400).endswith("d")


def test_collect_filters_by_root_and_age(user_home, tmp_path, monkeypatch):
    old = _registry_entry(tmp_path, os.getpid(), name="old", age_seconds=86400 * 8)
    new = _registry_entry(tmp_path, os.getpid(), name="new", age_seconds=60)
    monkeypatch.setattr(
        process_registry, "live_processes", lambda: [old, new])
    monkeypatch.setattr(
        process_registry, "read_all", lambda: {})
    monkeypatch.setattr(
        process_registry, "find_unregistered_lock_owners", lambda roots: [])

    assert {e["name"] for e in manage.collect()} == {"old", "new"}
    assert [e["name"] for e in manage.collect(
        older_than=manage.parse_duration("7d"))] == ["old"]
    assert [e["name"] for e in manage.collect(
        root=old["root_directory"])] == ["old"]


def test_stop_skips_detach_wait_without_status_file(tmp_path, monkeypatch):
    """A target that writes no status is signalled immediately."""
    entry = _registry_entry(tmp_path, os.getpid(), name="nostatus")
    assert manage.request_detach(entry) is False

    # With a status file present, the detach command is written instead.
    status = os.path.join(entry["root_directory"], manage.STATUS_FILE_NAME)
    with open(status, "w") as f:
        json.dump({"pid": os.getpid(), "stages": {}}, f)
    assert manage.request_detach(entry) is True
    commands = os.path.join(entry["work_directory"], "run",
                            "batch_commands.jsonl")
    assert json.loads(open(commands).read().strip()) == {"cmd": "detach"}


def test_stop_escalates_to_signals(tmp_path, monkeypatch):
    entry = _registry_entry(tmp_path, 4242, name="stubborn")
    sent = []
    alive = {"value": True}

    monkeypatch.setattr(
        manage.process_registry, "pid_alive", lambda pid: alive["value"])
    monkeypatch.setattr(manage, "request_detach", lambda e: False)
    monkeypatch.setattr(manage.process_registry, "prune", lambda: {})

    def fake_signal(e, sig):
        sent.append(sig)
        if sig == signal.SIGKILL:
            alive["value"] = False

    monkeypatch.setattr(manage, "signal_process", fake_signal)
    rc = manage.stop_entries(
        [entry], detach_timeout=0.05, term_timeout=0.05)
    assert sent == [signal.SIGTERM, signal.SIGKILL]
    assert rc == 0


def test_stage_summary_reads_root_status(tmp_path):
    entry = _registry_entry(tmp_path, os.getpid(), name="withjobs")
    with open(os.path.join(
            entry["root_directory"], manage.STATUS_FILE_NAME), "w") as f:
        json.dump({
            "pid": os.getpid(),
            "stages": {
                "sampling": {"state": "started", "job_ids": ["123", "456"]},
                "ramd": {"state": "unstarted", "job_ids": []},
            },
        }, f)
    stage, jobs = manage.stage_summary(entry)
    assert stage == "sampling/started"
    assert jobs == "123, 456"


def test_globus_lock_defaults_to_per_user(user_home, monkeypatch):
    monkeypatch.delenv(globus_lock.LOCK_ENV, raising=False)
    monkeypatch.delenv(globus_lock.DISABLE_ENV, raising=False)
    monkeypatch.setattr(globus_lock, "_user_lock_initialized", False)

    path = globus_lock.resolve_lock_file_path()
    assert path == globus_lock.user_lock_file_path()
    assert path.startswith(str(user_home))
    assert os.path.exists(path)

    # A batch-scoped lock wins when set, and is honored even after the
    # per-user path was already resolved once in this process.
    monkeypatch.setenv(globus_lock.LOCK_ENV, "/tmp/batch_lock.json")
    assert globus_lock.resolve_lock_file_path() == "/tmp/batch_lock.json"
    monkeypatch.delenv(globus_lock.LOCK_ENV)
    assert globus_lock.resolve_lock_file_path() == \
        globus_lock.user_lock_file_path()

    # And it can be disabled outright.
    monkeypatch.setenv(globus_lock.DISABLE_ENV, "1")
    assert globus_lock.resolve_lock_file_path() is None


def test_orphan_watchdog_detaches_after_grace(monkeypatch):
    from seekrflow.modules import seekr_run

    monkeypatch.setattr(seekr_run, "ORPHAN_GRACE_SECONDS", 0.05)
    monkeypatch.setattr(seekr_run, "ORPHAN_CHECK_INTERVAL", 0.01)

    class FakePipeline:
        orphan_detach = True
        _orphaned_since = None

        def __init__(self):
            self.detached = False

        def _orphan_check(self):
            return True

        def write_status_snapshot(self):
            pass

        def detach(self):
            self.detached = True

        _orphan_watchdog_loop = seekr_run.SeekrPipeline._orphan_watchdog_loop

    pipeline = FakePipeline()
    asyncio.run(asyncio.wait_for(
        pipeline._orphan_watchdog_loop(asyncio.Event()), timeout=5))
    assert pipeline.detached is True
    assert pipeline._orphaned_since is not None


def test_orphan_watchdog_respects_unattended(monkeypatch):
    from seekrflow.modules import seekr_run

    monkeypatch.setattr(seekr_run, "ORPHAN_GRACE_SECONDS", 0.01)
    monkeypatch.setattr(seekr_run, "ORPHAN_CHECK_INTERVAL", 0.01)

    class FakePipeline:
        orphan_detach = False
        _orphaned_since = None

        def __init__(self):
            self.detached = False

        def _orphan_check(self):
            return True

        def write_status_snapshot(self):
            pass

        def detach(self):
            self.detached = True

        _orphan_watchdog_loop = seekr_run.SeekrPipeline._orphan_watchdog_loop

    pipeline = FakePipeline()
    asyncio.run(asyncio.wait_for(
        pipeline._orphan_watchdog_loop(asyncio.Event()), timeout=2))
    assert pipeline.detached is False


def test_orphan_watchdog_recovers_when_supervised(monkeypatch):
    from seekrflow.modules import seekr_run

    monkeypatch.setattr(seekr_run, "ORPHAN_GRACE_SECONDS", 3600.0)
    monkeypatch.setattr(seekr_run, "ORPHAN_CHECK_INTERVAL", 0.01)

    states = iter([True, True, False])

    class FakePipeline:
        orphan_detach = True
        _orphaned_since = None

        def __init__(self):
            self.detached = False
            self.stop_event = asyncio.Event()

        def _orphan_check(self):
            try:
                return next(states)
            except StopIteration:
                self.stop_event.set()
                return False

        def write_status_snapshot(self):
            pass

        def detach(self):
            self.detached = True

        _orphan_watchdog_loop = seekr_run.SeekrPipeline._orphan_watchdog_loop

    pipeline = FakePipeline()
    asyncio.run(asyncio.wait_for(
        pipeline._orphan_watchdog_loop(pipeline.stop_event), timeout=5))
    assert pipeline.detached is False
    assert pipeline._orphaned_since is None


def test_stop_signal_kills_wedged_event_loop(tmp_path):
    """
    The failure that left six monitors needing SIGKILL: a blocked event loop
    never runs asyncio's signal handler. The low-level backstop must still
    bring the process down on a single SIGTERM.
    """
    script = tmp_path / "wedged.py"
    script.write_text(
        "import asyncio, os, signal, time\n"
        "HARD = 2.0\n"
        "armed = {'v': False}\n"
        "def force_exit(signum, frame):\n"
        "    os._exit(7)\n"
        "def arm(signum, frame):\n"
        "    if armed['v']:\n"
        "        os._exit(7)\n"
        "    armed['v'] = True\n"
        "    signal.setitimer(signal.ITIMER_REAL, HARD)\n"
        "async def main():\n"
        "    loop = asyncio.get_running_loop()\n"
        "    loop.add_signal_handler(signal.SIGTERM, lambda: None)\n"
        "    signal.signal(signal.SIGALRM, force_exit)\n"
        "    signal.signal(signal.SIGTERM, arm)\n"
        "    print('ready', flush=True)\n"
        "    time.sleep(60)\n"
        "asyncio.run(main())\n"
    )
    proc = subprocess.Popen(
        [sys.executable, str(script)], stdout=subprocess.PIPE, text=True)
    try:
        assert proc.stdout.readline().strip() == "ready"
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=15) == 7
    finally:
        if proc.poll() is None:
            proc.kill()


def test_shutdown_does_not_persist_stop_semaphores():
    """
    A cancelling shutdown must not leave every stage stopped for the next run.

    ``kill()`` used to set ``semaphore='stop'`` unconditionally, and that value
    is restored on restart, so one Ctrl-C silently disabled all 60 stages of a
    20-system batch.
    """
    from seekrflow.modules import seekr_run

    class FakeStage:
        def __init__(self):
            self.semaphore = "go"
            self.stopped_for_shutdown = False
            self.holds_local_slot = False
            self.resource_name = "local"
            self.resource = None
            self.process = None

        release_local_slot = seekr_run.StageWorkflow.release_local_slot
        kill = seekr_run.StageWorkflow.kill

    shutdown_stage = FakeStage()
    shutdown_stage.kill(persist_stop=False)
    assert shutdown_stage.semaphore == "go"
    assert shutdown_stage.stopped_for_shutdown is True

    # An explicit user stop still persists, since that intent should survive.
    user_stage = FakeStage()
    user_stage.kill()
    assert user_stage.semaphore == "stop"


def test_hard_exit_backstop_is_installed_after_loop_handlers(monkeypatch):
    """
    Order matters: add_signal_handler installs its own handler, so ours has to
    come second or it gets replaced.
    """
    from seekrflow.modules import seekr_run
    import inspect

    source = inspect.getsource(seekr_run.SeekrPipeline.run_workflows)
    add_index = source.index("add_signal_handler")
    backstop_index = source.index("_install_hard_exit_backstop")
    assert add_index < backstop_index
