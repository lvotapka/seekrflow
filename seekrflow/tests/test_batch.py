"""
Tests for seekrflow batch orchestration helpers.
"""

from __future__ import annotations

import json
import os
import pathlib
import signal
import subprocess
import sys
import time

import pytest

import seekrflow.modules.batch.structures as batch_structures
import seekrflow.modules.batch.commands as batch_commands
import seekrflow.modules.batch.analysis as batch_analysis
import seekrflow.modules.batch.children as batch_children
import seekrflow.modules.batch.coordinator as batch_coordinator
import seekrflow.modules.batch.ui as batch_ui
import seekrflow.modules.run_lock as run_lock


def test_deep_merge_dicts_and_index_aligned_lists():
    base = {
        "a": 1,
        "nested": {"x": 1, "y": 2},
        "lst": [1, 2],
        "scale_settings": [
            {
                "type": "molecular_dynamics",
                "hydrogen_mass": 3.0,
                "system": {"solvated_pdb": "a.pdb", "keep": True},
            },
            {"type": "other", "value": 1},
        ],
    }
    override = {
        "nested": {"y": 9, "z": 3},
        "lst": [7],
        "b": 2,
        "scale_settings": [
            {
                "system": {"solvated_pdb": "b.pdb"},
            },
        ],
    }
    merged = batch_structures.deep_merge(base, override)
    assert merged["a"] == 1
    assert merged["b"] == 2
    assert merged["nested"] == {"x": 1, "y": 9, "z": 3}
    # Non-dict list elements: index 0 replaced; trailing base kept.
    assert merged["lst"] == [7, 2]
    # Dict list elements: deep-merge index 0; keep trailing base entry.
    assert merged["scale_settings"][0]["type"] == "molecular_dynamics"
    assert merged["scale_settings"][0]["hydrogen_mass"] == 3.0
    assert merged["scale_settings"][0]["system"] == {
        "solvated_pdb": "b.pdb",
        "keep": True,
    }
    assert merged["scale_settings"][1] == {"type": "other", "value": 1}
    # base unchanged
    assert base["nested"]["y"] == 2
    assert base["scale_settings"][0]["system"]["solvated_pdb"] == "a.pdb"


def test_deep_merge_list_append_extra_override_elements():
    base = [{"a": 1}]
    override = [{"a": 2}, {"b": 3}]
    merged = batch_structures.deep_merge(base, override)
    assert merged == [{"a": 2}, {"b": 3}]


def test_absolutize_existing_paths(tmp_path):
    rel = "data/file.txt"
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "file.txt").write_text("ok")
    obj = {"path": rel, "missing": "nope.txt", "n": 1}
    out = batch_structures.absolutize_existing_paths(obj, str(tmp_path))
    assert os.path.isabs(out["path"])
    assert out["path"].endswith("file.txt")
    assert out["missing"] == "nope.txt"
    assert out["n"] == 1


def test_load_batch_and_materialize(tmp_path):
    template = {
        "name": "tmpl",
        "structure_version": "1.5",
        "workflow": {
            "type": "workflow",
            "structure_version": "1.1",
            "components": {"members": [], "group_selectors": []},
            "cv_specs": [],
            "anchor_spec": {"type": "uniform", "n_anchors": 2},
            "procedure": {
                "name": "p",
                "type": "composite",
                "procedures": [],
            },
            "scale_settings": [],
            "plugins": [],
        },
        "physical_attributes": {
            "temperature": 298.15,
            "pressure": None,
            "ionic_strength": 0.0,
            "random_seed": 1,
        },
        "work_directory": "work",
        "root_directory": None,
        "parameterizer": None,
        "run_settings": {"resources": [], "placements": []},
    }
    template_path = tmp_path / "template.json"
    template_path.write_text(json.dumps(template))
    batch_json = {
        "batch_directory": "my_batch",
        "template": "template.json",
        "systems": [
            {"name": "sys_a", "overrides": {}},
            {
                "name": "sys_b",
                "skip": True,
                "overrides": {"physical_attributes": {"temperature": 310.0}},
            },
            {
                "name": "sys_c",
                "overrides": {"physical_attributes": {"temperature": 300.0}},
            },
        ],
        "batch_analyses": [
            {"type": "collect_batch_analysis", "run_seekr_analyze": False},
        ],
    }
    batch_path = tmp_path / "batch.json"
    batch_path.write_text(json.dumps(batch_json))

    batch = batch_structures.load_batch(str(batch_path))
    assert batch.batch_directory == "my_batch"
    assert not batch.needs_parameterize()
    assert len(batch_structures.active_systems(batch)) == 2
    assert len(batch.batch_analyses) == 1
    assert batch.batch_analyses[0].type == "collect_batch_analysis"

    paths = batch_structures.materialize_all(batch)
    assert set(paths.keys()) == {"sys_a", "sys_c"}
    work_a = batch.system_work_directory("sys_a")
    assert os.path.isdir(work_a)
    assert os.path.exists(paths["sys_a"])
    with open(paths["sys_a"]) as f:
        cfg = json.load(f)
    assert cfg["name"] == "sys_a"
    assert cfg["work_directory"] == work_a
    assert cfg["physical_attributes"]["temperature"] == 298.15

    with open(paths["sys_c"]) as f:
        cfg_c = json.load(f)
    assert cfg_c["physical_attributes"]["temperature"] == 300.0


def test_batch_commands_roundtrip(tmp_path):
    run_dir = tmp_path / "run"
    batch_commands.append_semaphore_command(str(run_dir), "wait", "MMVT")
    batch_commands.append_poll_interval_command(str(run_dir), 12.5)
    path = batch_commands.batch_commands_path(str(run_dir))
    cmds, offset = batch_commands.read_new_commands(str(path), 0)
    assert len(cmds) == 2
    assert cmds[0]["cmd"] == "semaphore"
    assert cmds[0]["value"] == "wait"
    assert cmds[1]["seconds"] == 12.5
    cmds2, offset2 = batch_commands.read_new_commands(str(path), offset)
    assert cmds2 == []
    assert offset2 == offset


def test_collect_batch_analysis(tmp_path):
    batch_dir = tmp_path / "batch"
    batch_dir.mkdir()
    work = batch_dir / "work_s1"
    root = work / "root"
    root.mkdir(parents=True)
    # No model.json → missing_model
    batch = batch_structures.Batch(
        batch_directory=str(batch_dir),
        template={},
        systems=[],
    )
    analyzer = batch_analysis.Collect_batch_analysis(run_seekr_analyze=False)
    analyzer.run(batch, [("s1", str(work))])
    out = batch_dir / "analysis" / "collect_results.json"
    assert out.exists()
    rows = json.loads(out.read_text())
    assert rows[0]["analyze_status"] == "missing_model"


def test_stage_gate_failure_blocks(monkeypatch, tmp_path):
    """Coordinator run_gated_stage reports failure and writes status."""
    batch_dir = tmp_path / "b"
    batch_dir.mkdir()
    work = batch_dir / "work_x"
    work.mkdir()
    (work / "logs").mkdir()
    json_path = work / "seekrflow.json"
    json_path.write_text("{}")

    batch = batch_structures.Batch(
        batch_directory=str(batch_dir),
        template={},
        systems=[batch_structures.Batch_system(name="x")],
    )
    batch._source_dir = str(tmp_path)
    rows = [
        batch_ui.SystemRow(name="x", work_directory=str(work)),
    ]

    def fake_run(*args, **kwargs):
        return "x", 1, str(work / "logs" / "batch_child_prepare.log"), "boom"

    monkeypatch.setattr(
        batch_coordinator, "run_stage_for_system", fake_run)
    ok = batch_coordinator.run_gated_stage(
        batch, {"x": str(json_path)}, "prepare", rows, concurrency=1)
    assert ok is False
    assert rows[0].state == "failed"
    status = json.loads(
        (batch_dir / batch_structures.BATCH_STATUS_FILENAME).read_text())
    assert status["failures"] == ["x"]


def test_local_slots_acquire_release_and_max(tmp_path, monkeypatch):
    import seekrflow.modules.batch.local_slots as local_slots

    monkeypatch.setattr(local_slots, "_pid_alive", lambda pid: True)
    path = str(tmp_path / local_slots.LOCAL_SLOTS_FILENAME)
    local_slots.init_pool(path, max_slots=1)
    assert local_slots.try_acquire(path, "/w1", "mmvt", pid=111)
    assert not local_slots.try_acquire(path, "/w2", "mmvt", pid=222)
    # Same holder can re-acquire.
    assert local_slots.try_acquire(path, "/w1", "mmvt", pid=111)
    local_slots.release(path, "/w1", "mmvt", pid=None)
    assert local_slots.try_acquire(path, "/w2", "mmvt", pid=222)


def test_local_slots_reap_dead_pid(tmp_path, monkeypatch):
    import seekrflow.modules.batch.local_slots as local_slots

    path = str(tmp_path / local_slots.LOCAL_SLOTS_FILENAME)
    local_slots.init_pool(path, max_slots=1)
    assert local_slots.try_acquire(path, "/w1", "mmvt", pid=999999)
    # Force holder PID to look dead.
    monkeypatch.setattr(local_slots, "_pid_alive", lambda pid: False)
    assert local_slots.try_acquire(path, "/w2", "mmvt", pid=12345)


def test_local_slots_update_holder_pid(tmp_path, monkeypatch):
    import seekrflow.modules.batch.local_slots as local_slots

    monkeypatch.setattr(local_slots, "_pid_alive", lambda pid: True)
    path = str(tmp_path / local_slots.LOCAL_SLOTS_FILENAME)
    local_slots.init_pool(path, max_slots=1)
    assert local_slots.try_acquire(path, "/w1", "mmvt", pid=111)
    assert local_slots.update_holder_pid(path, "/w1", "mmvt", 111, 222)
    # Release by work+stage should clear regardless of pid.
    local_slots.release(path, "/w1", "mmvt", pid=None)
    assert local_slots.try_acquire(path, "/w2", "mmvt", pid=333)


def test_run_spawns_all_children_with_slot_file(monkeypatch, tmp_path):
    """All run children start immediately and receive --local-slot-file."""
    batch_dir = tmp_path / "batch"
    batch_dir.mkdir()
    json_paths = {}
    rows = []
    for name in ("a", "b"):
        work = batch_dir / f"work_{name}"
        work.mkdir()
        (work / "logs").mkdir()
        path = work / "seekrflow.json"
        path.write_text("{}")
        json_paths[name] = str(path)
        rows.append(batch_ui.SystemRow(name=name, work_directory=str(work)))

    batch = batch_structures.Batch(
        batch_directory=str(batch_dir),
        template={},
        systems=[batch_structures.Batch_system(name=n) for n in json_paths],
        max_concurrent_local_runs=1,
    )

    class FakeProc:
        def __init__(self):
            self.returncode = 0
            # Sentinel: never a live process, so cleanup treats it as exited.
            self.pid = -1

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

        def terminate(self):
            pass

    spawn_kwargs = []

    def fake_spawn(*args, **kwargs):
        spawn_kwargs.append(kwargs)
        return FakeProc()

    class FakeUI:
        def __init__(self, rows, on_command, refresh_rows=None, **kwargs):
            self.detail = False
            self.selected = 0
            assert all(r.state == "pending" for r in rows)

        def run(self, should_stop, poll_seconds=1.0):
            assert len(spawn_kwargs) == 2
            assert should_stop()

    monkeypatch.setattr(batch_coordinator, "spawn_flow_child", fake_spawn)
    monkeypatch.setattr(batch_ui, "BatchUI", FakeUI)

    ok = batch_coordinator.run_all_children_with_ui(
        batch, json_paths, rows)
    assert ok is True
    assert len(spawn_kwargs) == 2
    for kw in spawn_kwargs:
        assert kw.get("local_slot_file")
        assert os.path.exists(kw["local_slot_file"])
        assert kw.get("globus_lock_file")
        assert os.path.exists(kw["globus_lock_file"])


def test_run_adopts_live_child_instead_of_duplicating(monkeypatch, tmp_path):
    """A system already owned by a live child must not get a second one."""
    batch_dir = tmp_path / "batch"
    batch_dir.mkdir()
    json_paths = {}
    rows = []
    for name in ("a", "b"):
        work = batch_dir / f"work_{name}"
        (work / "logs").mkdir(parents=True)
        path = work / "seekrflow.json"
        path.write_text("{}")
        json_paths[name] = str(path)
        rows.append(batch_ui.SystemRow(name=name, work_directory=str(work)))

    batch = batch_structures.Batch(
        batch_directory=str(batch_dir),
        template={},
        systems=[batch_structures.Batch_system(name=n) for n in json_paths],
        max_concurrent_local_runs=1,
    )
    # Pretend 'a' is already being run by this process.
    batch_children.register_child(
        str(batch_dir), "a", os.getpid(), str(batch_dir / "work_a"))

    spawned = []

    class FakeProc:
        def __init__(self):
            self.returncode = 0
            self.pid = -1

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    def fake_spawn(*args, **kwargs):
        spawned.append(args[1])
        return FakeProc()

    class FakeUI:
        def __init__(self, rows, on_command, refresh_rows=None, **kwargs):
            self.detail = False
            self.selected = 0

        def run(self, should_stop, poll_seconds=1.0):
            # 'a' is alive (this process), so the batch is not done.
            assert should_stop() is False

    monkeypatch.setattr(batch_coordinator, "spawn_flow_child", fake_spawn)
    monkeypatch.setattr(batch_ui, "BatchUI", FakeUI)
    # Never signal the adopted PID: in this test that PID is pytest itself.
    monkeypatch.setattr(
        batch_coordinator, "detach_children",
        lambda batch_dir, entries, reap=None: None)

    batch_coordinator.run_all_children_with_ui(batch, json_paths, rows)

    assert spawned == [json_paths["b"]]
    row_by_name = {r.name: r for r in rows}
    assert row_by_name["a"].child_pid == os.getpid()
    assert row_by_name["b"].child_pid == -1


def test_host_guest_example_batch_loads():
    example = pathlib.Path(__file__).resolve().parents[1] / "examples" / \
        "host_guest" / "batch_host_guest.json"
    if not example.exists():
        pytest.skip("example missing")
    batch = batch_structures.load_batch(str(example))
    assert len(batch.systems) == 3
    assert batch.systems[0].name == "1_butanol"
    assert batch.max_concurrent_local_runs == 1
    assert batch.batch_analyses[0].type == "collect_batch_analysis"
    # Materialize without running children
    paths = batch_structures.materialize_all(batch)
    assert len(paths) == 3
    for name, path in paths.items():
        with open(path) as f:
            cfg = json.load(f)
        assert cfg["name"] == name
        assert cfg["work_directory"].endswith(f"work_{name}")
        md = cfg["workflow"]["scale_settings"][0]
        # Path-only overrides must still inherit template MD knobs.
        assert md["type"] == "molecular_dynamics"
        assert md["hydrogen_mass"] == 3.0
        assert md["platform_type"] == "cuda"
        pt = md["system"]["parameters_topology"]["prmtop_filename"]
        assert os.path.isabs(pt)
        assert os.path.exists(pt)
    with open(paths["1_propanol"]) as f:
        propanol = json.load(f)
    assert propanol["workflow"]["scale_settings"][0]["system"][
        "solvated_pdb"].endswith("BCD_1-propanol.pdb")


def test_summary_refresh_shows_queued_and_error(tmp_path):
    """Summary mirrors flow.py: separate State/Manager; Error only if failed."""
    work = tmp_path / "work_x"
    root = work / "root"
    root.mkdir(parents=True)
    status = {
        "stages": {
            "mmvt": {
                "state": "started",
                "semaphore": "go",
                "progress": 0.25,
                "manager_status": "queued",
                "resource_name": "cluster",
                "status_polled_at": time.time(),
                "last_error": "",
                "raw_status": {
                    "success": False,
                    "error": "No module named globus_sdk",
                    "stage_status": {
                        "notes": "Model file not found: /tmp/model.json",
                    },
                    "manager_status": {"error": "squeue down"},
                },
            }
        }
    }
    (root / ".seekrflow_job_status.json").write_text(json.dumps(status))
    row = batch_ui.SystemRow(name="x", work_directory=str(work))
    row.refresh_from_status_file()
    assert row.state == "queued"
    assert row.manager_status == "queued"
    assert row.semaphore == "go"
    assert row.progress == "25%"
    # Transient probe noise must not appear in Error.
    assert row.last_error == ""

    status["stages"]["mmvt"]["last_error"] = "scheduler idle with incomplete"
    status["stages"]["mmvt"]["state"] = "failed"
    status["stages"]["mmvt"]["manager_status"] = "idle"
    status["stages"]["mmvt"]["semaphore"] = "wait"
    (root / ".seekrflow_job_status.json").write_text(json.dumps(status))
    row.refresh_from_status_file()
    assert row.state == "failed"
    assert row.semaphore == "wait"
    assert "scheduler idle" in row.last_error


def test_display_state_and_stage_error_helpers():
    assert batch_ui._stage_error_text(
        {"state": "started", "last_error": "should ignore"}) == ""
    assert batch_ui._stage_error_text(
        {"state": "failed", "transfer_error": "xfer boom"}) == "xfer boom"
    assert batch_ui._stage_error_text(
        {
            "state": "failed",
            "raw_status": {"manager_status": {"error": "squeue down"}},
        }
    ) == ""
    name, info = batch_ui._pick_summary_stage({
        "a": {"state": "completed", "manager_status": "idle", "progress": 1.0},
        "b": {"state": "started", "manager_status": "queued", "progress": 0.1},
        "c": {"state": "unstarted", "manager_status": "idle", "progress": 0.0},
    })
    assert name == "b"
    assert info["manager_status"] == "queued"
    name, info = batch_ui._pick_summary_stage({
        "b": {"state": "started", "manager_status": "running", "progress": 0.5},
        "c": {
            "state": "completed",
            "manager_status": "idle",
            "progress": 1.0,
            "transfer_status": "running",
            "transfer_direction": "back",
        },
    })
    assert name == "c"
    name, info = batch_ui._pick_summary_stage({
        "sampling": {
            "state": "completed",
            "manager_status": "queued",
            "resource_name": "cluster",
            "status_polled_at": time.time(),
            "progress": 1.0,
        },
        "ramd": {
            "state": "started",
            "manager_status": "idle",
            "resource_name": "cluster",
            "status_polled_at": time.time(),
            "progress": 0.07,
        },
    })
    assert name == "sampling"
    assert info["manager_status"] == "queued"
    name, info = batch_ui._pick_summary_stage({
        "sampling": {
            "state": "completed",
            "manager_status": "queued",
            "resource_name": "cluster",
            "status_polled_at": time.time(),
            "progress": 1.0,
        },
        "ramd": {
            "state": "started",
            "manager_status": "running",
            "resource_name": "cluster",
            "status_polled_at": time.time(),
            "progress": 0.07,
        },
    })
    assert name == "ramd"
    assert info["manager_status"] == "running"


def test_summary_row_follows_status_file_without_hysteresis(tmp_path):
    work = tmp_path / "work"
    root = work / "root"
    root.mkdir(parents=True)
    path = root / ".seekrflow_job_status.json"
    row = batch_ui.SystemRow(name="x", work_directory=str(work))
    assert row.state == "pending"

    path.write_text(json.dumps({
        "stages": {
            "mmvt": {
                "state": "started",
                "semaphore": "go",
                "progress": 0.2,
                "manager_status": "queued",
                "resource_name": "cluster",
                "last_error": "",
            }
        }
    }))
    row.refresh_from_status_file()
    assert row.state == "pending"
    assert row.manager_status == "pending"
    assert row.last_error == ""

    path.write_text(json.dumps({
        "stages": {
            "mmvt": {
                "state": "started",
                "semaphore": "go",
                "progress": 0.2,
                "manager_status": "queued",
                "resource_name": "cluster",
                "status_polled_at": time.time(),
                "last_error": "",
            }
        }
    }))
    row.refresh_from_status_file()
    assert row.state == "queued"
    assert row.manager_status == "queued"
    assert row.last_error == ""


def test_summary_row_shows_fused_host_queue_over_idle_member(tmp_path):
    work = tmp_path / "work"
    root = work / "root"
    root.mkdir(parents=True)
    now = time.time()
    (root / ".seekrflow_job_status.json").write_text(json.dumps({
        "stages": {
            "ramd_procedure_sampling": {
                "state": "completed",
                "semaphore": "go",
                "progress": 1.0,
                "manager_status": "queued",
                "resource_name": "cluster",
                "status_polled_at": now,
                "job_ids": ["21267447"],
            },
            "ramd_procedure_ramd": {
                "state": "started",
                "semaphore": "go",
                "progress": 0.07,
                "manager_status": "idle",
                "resource_name": "cluster",
                "status_polled_at": now,
                "job_ids": [],
            },
        }
    }))
    row = batch_ui.SystemRow(name="x", work_directory=str(work))
    row.refresh_from_status_file()
    assert row.batch_stage == "ramd_procedure_sampling"
    assert row.state == "queued"
    assert row.manager_status == "queued"


def test_error_column_reserved_width():
    table = batch_ui.build_summary_table(
        [batch_ui.SystemRow(name="sys", work_directory="/tmp")],
        selected_index=0,
    )
    error_col = table.columns[-1]
    assert error_col.header == "Error"
    assert error_col.min_width == 24
    state_col = next(c for c in table.columns if c.header == "State")
    assert state_col.width == 14


def test_globus_status_kind_follows_poll_interval(monkeypatch):
    from seekrflow.modules import seekr_run
    monkeypatch.setattr(seekr_run, "_runtime_polling_interval", 5.0)
    assert seekr_run._globus_status_kind() == "status_focused"
    monkeypatch.setattr(seekr_run, "_runtime_polling_interval", 300.0)
    assert seekr_run._globus_status_kind() == "status"


def test_run_lock_refuses_second_holder(tmp_path):
    """One live process per root directory; the lock frees on release."""
    root = tmp_path / "root"
    lock = run_lock.acquire(str(root), "run")
    assert run_lock.read_owner(str(root))["pid"] == os.getpid()

    script = (
        "import sys, seekrflow.modules.run_lock as rl\n"
        "try:\n"
        "    rl.acquire(sys.argv[1], 'run')\n"
        "except rl.RunLockBusyError:\n"
        "    sys.exit(rl.DUPLICATE_RUN_EXIT_CODE)\n"
        "sys.exit(0)\n"
    )
    busy = subprocess.run(
        [sys.executable, "-c", script, str(root)],
        capture_output=True, text=True)
    assert busy.returncode == run_lock.DUPLICATE_RUN_EXIT_CODE

    lock.release()
    free = subprocess.run(
        [sys.executable, "-c", script, str(root)],
        capture_output=True, text=True)
    assert free.returncode == 0


def test_stop_children_detaches_then_escalates(tmp_path, monkeypatch):
    """Detach first; SIGTERM then SIGKILL only for children that ignore it."""
    batch_dir = tmp_path / "batch"
    work = batch_dir / "work_a"
    (work / "run").mkdir(parents=True)
    batch_children.register_child(
        str(batch_dir), "a", os.getpid(), str(work))

    signalled = []
    alive = {"value": True}

    def fake_signal(entry, sig):
        signalled.append(sig)
        if sig == signal.SIGKILL:
            alive["value"] = False

    monkeypatch.setattr(batch_children, "_signal_child", fake_signal)
    monkeypatch.setattr(
        batch_children, "pid_alive", lambda pid: alive["value"])

    result = batch_children.stop_children(
        str(batch_dir), cancel_jobs=False,
        detach_timeout=0.05, term_timeout=0.05)

    commands = (work / "run" / "batch_commands.jsonl").read_text()
    assert json.loads(commands.strip()) == {"cmd": "detach"}
    assert signalled == [signal.SIGTERM, signal.SIGKILL]
    assert result["detached"] == []
    assert result["killed"] == ["a"]


def test_stop_children_skips_detach_when_cancelling_jobs(
        tmp_path, monkeypatch):
    batch_dir = tmp_path / "batch"
    work = batch_dir / "work_a"
    (work / "run").mkdir(parents=True)
    batch_children.register_child(
        str(batch_dir), "a", os.getpid(), str(work))

    signalled = []
    alive = {"value": True}

    def fake_signal(entry, sig):
        signalled.append(sig)
        alive["value"] = False

    monkeypatch.setattr(batch_children, "_signal_child", fake_signal)
    monkeypatch.setattr(
        batch_children, "pid_alive", lambda pid: alive["value"])

    batch_children.stop_children(
        str(batch_dir), cancel_jobs=True,
        detach_timeout=0.05, term_timeout=0.05)

    # No detach command: the child must cancel its jobs on the way out.
    assert not (work / "run" / "batch_commands.jsonl").exists()
    assert signalled == [signal.SIGTERM]


def test_registry_prunes_dead_children(tmp_path):
    batch_dir = tmp_path / "batch"
    work = batch_dir / "work_a"
    work.mkdir(parents=True)
    batch_children.register_child(
        str(batch_dir), "alive", os.getpid(), str(work))
    batch_children.register_child(
        str(batch_dir), "dead", -1, str(work))
    assert set(batch_children.read_registry(str(batch_dir))) == {
        "alive", "dead"}
    assert set(batch_children.prune_registry(str(batch_dir))) == {"alive"}


def test_foreign_status_writer_shows_stale(tmp_path):
    """A status file owned by another PID must not read as live progress."""
    work = tmp_path / "work_x"
    root = work / "root"
    root.mkdir(parents=True)
    (root / ".seekrflow_job_status.json").write_text(json.dumps({
        "pid": os.getpid(),
        "stages": {
            "mmvt": {
                "state": "started",
                "semaphore": "go",
                "progress": 0.5,
                "manager_status": "running",
                "resource_name": "cluster",
                "status_polled_at": time.time(),
            }
        },
    }))
    row = batch_ui.SystemRow(name="x", work_directory=str(work))
    row.child_pid = os.getpid() + 100000
    row.refresh_from_status_file()
    assert row.state == "stale"
    assert str(os.getpid()) in row.last_error

    # Matching PID reads normally.
    row.child_pid = os.getpid()
    row.refresh_from_status_file()
    assert row.state == "running"
    assert row.manager_status == "running"


def test_archive_batch_commands_drops_prior_session(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    batch_commands.append_semaphore_command(str(run_dir), "wait")
    path = batch_commands.batch_commands_path(str(run_dir))
    assert os.path.exists(path)

    archive = batch_commands.archive_batch_commands(str(run_dir))
    assert archive is not None
    assert "wait" in pathlib.Path(archive).read_text()
    assert not os.path.exists(path)

    # A child starting at offset 0 now sees no stale commands.
    cmds, offset = batch_commands.read_new_commands(path, 0)
    assert cmds == []
    assert offset == 0
    # Nothing to archive the second time.
    assert batch_commands.archive_batch_commands(str(run_dir)) is None


def test_globus_transfer_task_must_succeed():
    """A FAILED task raises instead of being reported as complete."""
    import seekrflow.modules.transfer.globus as transfer_globus

    class FakeTransferClient:
        def __init__(self, docs):
            self.docs = docs
            self.index = 0
            self.cancelled = []

        def task_wait(self, task_id, timeout=0, polling_interval=0):
            doc = self.docs[min(self.index, len(self.docs) - 1)]
            return str(doc.get("status", "")).upper() in {
                "SUCCEEDED", "FAILED"}

        def get_task(self, task_id):
            doc = self.docs[min(self.index, len(self.docs) - 1)]
            self.index += 1
            return doc

        def cancel_task(self, task_id):
            self.cancelled.append(task_id)

    succeeded = transfer_globus.await_transfer_task(
        FakeTransferClient(
            [{"status": "ACTIVE", "nice_status": "OK"},
             {"status": "SUCCEEDED"}]),
        "task-ok", "label", timeout=1.0, poll_interval=0.001)
    assert succeeded["status"] == "SUCCEEDED"

    with pytest.raises(transfer_globus.GlobusTransferError):
        transfer_globus.await_transfer_task(
            FakeTransferClient([{"status": "FAILED",
                                 "nice_status": "ENDPOINT_ERROR"}]),
            "task-bad", "label", timeout=1.0, poll_interval=0.001)

    # A task stuck erroring is cancelled so it cannot race a retry.
    stuck = FakeTransferClient(
        [{"status": "ACTIVE", "nice_status": "CONNECT_FAILED"}])
    with pytest.raises(transfer_globus.GlobusTransferError):
        transfer_globus.await_transfer_task(
            stuck, "task-stuck", "label", timeout=1.0, poll_interval=0.001)
    assert stuck.cancelled == ["task-stuck"]

    assert transfer_globus.classify_task_document(
        {"status": "FAILED", "nice_status": "PERMISSION_DENIED"})[0] == "fatal"
    assert transfer_globus.classify_task_document(
        {"status": "FAILED", "nice_status": "FILE_NOT_FOUND"})[0] == "retry"
    assert transfer_globus.classify_task_document(
        {"status": "INACTIVE"})[0] == "fatal"


def test_globus_transfer_verifies_destination_contents():
    import seekrflow.modules.transfer.globus as transfer_globus
    import seekrflow.modules.transfer.base as transfer_base

    class FakeLs:
        def __init__(self, names):
            self.names = names

        def operation_ls(self, collection_id, path=None):
            return [{"name": name} for name in self.names]

    transfer_globus.verify_destination_contents(
        FakeLs(["model.json"]), "c", "/remote", ["model.json"])

    with pytest.raises(transfer_globus.GlobusTransferRetryableError) as excinfo:
        transfer_globus.verify_destination_contents(
            FakeLs(["other.txt"]), "c", "/remote", ["model.json"])
    assert transfer_base.is_retryable_transfer_error(excinfo.value)
    assert not transfer_base.is_retryable_transfer_error(RuntimeError("nope"))


def test_endpoint_capacity_is_gated_only_for_submit(monkeypatch, tmp_path):
    from seekrflow.modules.remote_interfaces import globus_compute_sdk

    monkeypatch.setenv(
        "SEEKR_GLOBUS_LOCK_FILE", str(tmp_path / "globus_lock.json"))
    busy = {
        "status": "online",
        "details": {
            "idle_workers": 0, "total_workers": 2, "pending_tasks": 19},
    }
    assert globus_compute_sdk.endpoint_is_busy(busy)
    assert globus_compute_sdk.CAPACITY_GATED_KINDS == frozenset({"submit"})
    for kind in ("status", "status_focused", "cancel"):
        assert kind not in globus_compute_sdk.CAPACITY_GATED_KINDS

    # A busy endpoint is retryable capacity pressure, not a broken payload.
    assert globus_compute_sdk.is_retryable_globus_error(
        globus_compute_sdk.GlobusRetryableError(
            "globus-client: endpoint 'delta' busy (0 idle workers, "
            "total_workers=2, pending_tasks=19)"))
    assert not globus_compute_sdk.is_retryable_globus_error(
        RuntimeError("globus-task: sbatch: error: invalid partition"))
    assert globus_compute_sdk.decide_globus_submit_action(
        endpoint_status=busy, attempt=1, max_attempts=12) == "retry"
    assert globus_compute_sdk.decide_globus_submit_action(
        endpoint_status=busy, attempt=12,
        max_attempts=12) == "fail_exhausted"


def test_capacity_submit_failure_stays_queued_not_waiting():
    """Busy endpoints must not park a stage behind a wait semaphore."""
    from seekrflow.modules import seekr_run
    from seekrflow.modules.remote_interfaces import globus_compute_sdk

    busy = globus_compute_sdk.GlobusRetryableError(
        "globus-client: endpoint 'delta' busy (0 idle workers, "
        "total_workers=2, pending_tasks=19)")
    assert seekr_run.submit_failure_is_capacity(busy)
    assert not seekr_run.submit_failure_is_capacity(
        globus_compute_sdk.GlobusEndpointOfflineError("endpoint is OFFLINE"))
    assert not seekr_run.submit_failure_is_capacity(
        RuntimeError("Remote submit failed: sbatch: invalid account"))


def test_queued_stage_is_not_monitored():
    """A stage waiting on capacity has no job to monitor; it must relaunch."""
    import asyncio
    from types import SimpleNamespace
    from seekrflow.modules import seekr_run
    from seekrflow.modules import structures as flow_structures

    sw = seekr_run.StageWorkflow(
        model=SimpleNamespace(directory="/work"),
        seekrflow=SimpleNamespace(name="run1"),
        stage=SimpleNamespace(name="mmvt", index=1),
        workflow_engine=object(),
        resource_name="cluster",
        resource=flow_structures.Resource_remote_slurm(name="cluster"),
    )
    sw.peer_workflows = {}
    sw.semaphore = "go"
    sw.state = "queued"
    sw.task = object()

    asyncio.run(sw._monitor_stage_loop())

    # Semaphore untouched (no manual intervention needed) and the task handle
    # is cleared so the scheduler loop can re-launch the stage.
    assert sw.semaphore == "go"
    assert sw.state == "queued"
    assert sw.task is None


def test_queued_stage_with_job_ids_is_monitored(monkeypatch):
    """Queued with tracked jobs must poll, not treat as nothing-to-watch."""
    import asyncio
    from types import SimpleNamespace
    from seekrflow.modules import seekr_run
    from seekrflow.modules import structures as flow_structures

    sw = seekr_run.StageWorkflow(
        model=SimpleNamespace(directory="/work"),
        seekrflow=SimpleNamespace(name="run1"),
        stage=SimpleNamespace(name="mmvt", index=1),
        workflow_engine=object(),
        resource_name="cluster",
        resource=flow_structures.Resource_remote_slurm(name="cluster"),
    )
    sw.peer_workflows = {}
    sw.semaphore = "go"
    sw.state = "queued"
    sw.job_ids.add("12345")
    sw.task = object()
    calls = []

    def fake_status(*args, **kwargs):
        calls.append(1)
        sw.detached_requested = True
        return {
            "success": True,
            "stage_status": {
                "state": "started",
                "progress": 0.2,
                "finished": False,
            },
            "manager_status": {"jobs": [{"JobID": "12345"}]},
        }

    async def noop_sleep(*args, **kwargs):
        return

    monkeypatch.setattr(seekr_run.workload_remote, "status_remote", fake_status)
    monkeypatch.setattr(seekr_run, "_sleep_polling_interval", noop_sleep)

    asyncio.run(sw._monitor_stage_loop())

    assert calls, "queued+job_ids must poll status instead of exiting"
    assert sw.state != "queued"


def test_queued_without_job_ids_does_not_reattach():
    from types import SimpleNamespace
    from seekrflow.modules import seekr_run
    from seekrflow.modules import structures as flow_structures

    sw = seekr_run.StageWorkflow(
        model=SimpleNamespace(directory="/work"),
        seekrflow=SimpleNamespace(name="run1"),
        stage=SimpleNamespace(name="mmvt", index=1),
        workflow_engine=object(),
        resource_name="cluster",
        resource=flow_structures.Resource_remote_slurm(name="cluster"),
    )
    sw.state = "queued"
    assert not seekr_run.should_reattach_queued_jobs(sw)
    sw.job_ids.add("999")
    assert seekr_run.should_reattach_queued_jobs(sw)
    sw.state = "started"
    assert seekr_run.should_reattach_started_jobs(sw)
    sw.job_ids.clear()
    assert not seekr_run.should_reattach_started_jobs(sw)


def test_ui_refreshes_after_children_stop():
    """Last-child exit must still paint the final status snapshot."""
    refreshes = []
    checks = {"n": 0}

    def should_stop():
        checks["n"] += 1
        return checks["n"] > 1

    ui = batch_ui.BatchUI(
        rows=[],
        on_command=lambda *a: None,
        refresh_rows=lambda: refreshes.append(1),
    )
    ui._run_loop(should_stop, poll_seconds=0)
    assert len(refreshes) >= 2


def test_endpoint_status_cache_is_shared(monkeypatch, tmp_path):
    from seekrflow.modules.remote_interfaces import globus_compute_sdk

    monkeypatch.setenv(
        "SEEKR_GLOBUS_LOCK_FILE", str(tmp_path / "globus_lock.json"))
    status = {"status": "online", "details": {"idle_workers": 1}}

    class CountingClient:
        def __init__(self):
            self.calls = 0

        def get_endpoint_status(self, endpoint):
            self.calls += 1
            return status

    client = CountingClient()
    for _ in range(4):
        assert globus_compute_sdk.get_endpoint_status_cached(
            client, "delta") == status
    assert client.calls == 1

    # An expired entry is refetched.
    monkeypatch.setattr(
        globus_compute_sdk, "ENDPOINT_STATUS_CACHE_TTL_S", -1.0)
    globus_compute_sdk.get_endpoint_status_cached(client, "delta")
    assert client.calls == 2


def _make_batch_ui(tmp_path):
    row = batch_ui.SystemRow("sys1", str(tmp_path / "work1"))
    ui = batch_ui.BatchUI(rows=[row], on_command=lambda line, t: None)
    return ui, row


def test_summary_q_requests_detach_all(tmp_path):
    ui, _row = _make_batch_ui(tmp_path)
    ui._handle_char("q")
    assert ui.detach_requested is False
    assert ui._stop is False
    assert ui._input_buffer == "q"
    ui._handle_char("\n")
    assert ui.detach_requested is True
    assert ui._stop is True
    assert "wait" in ui._status_message.lower()
    assert "detach" in ui._status_message.lower()


def test_detail_q_does_not_detach(tmp_path):
    ui, _row = _make_batch_ui(tmp_path)
    ui.detail = True
    ui._handle_char("q")
    assert ui._input_buffer == "q"
    ui._handle_char("\n")
    assert ui.detach_requested is False
    assert ui._stop is False
    assert "Esc" in ui._status_message
    assert "Summary" in ui._status_message


def test_dispatch_q_summary_detaches_all_detail_does_not(tmp_path):
    ui, _row = _make_batch_ui(tmp_path)
    ui._dispatch_line("q")
    assert ui.detach_requested is True
    assert ui._stop is True

    ui2, _row2 = _make_batch_ui(tmp_path)
    ui2.detail = True
    ui2._dispatch_line("detach")
    assert ui2.detach_requested is False
    assert ui2._stop is False
    assert "Esc" in ui2._status_message


def test_dispatch_transfer_echoes_above_table(tmp_path):
    ui, row = _make_batch_ui(tmp_path)
    captured = []
    ui.on_command = lambda line, t: captured.append((line, t))
    ui._dispatch_line("t mmvt")
    assert "transfer requested" in ui._status_message
    assert "mmvt" in ui._status_message
    assert "1 system" in ui._status_message
    assert captured == [("t mmvt", [row])]

    ui2, row2 = _make_batch_ui(tmp_path)
    ui2.detail = True
    ui2._dispatch_line("t mmvt")
    assert "transfer requested: mmvt (sys1)" == ui2._status_message

    ui3, _row3 = _make_batch_ui(tmp_path)
    ui3._dispatch_line("t")
    assert "transfer requested: all" in ui3._status_message

    ui4, _row4 = _make_batch_ui(tmp_path)
    ui4._dispatch_line("nope")
    assert ui4._status_message == "unknown command: 'nope'"


def test_fanout_q_does_not_write_detach_command(tmp_path):
    work = tmp_path / "work1"
    run_dir = work / "run"
    run_dir.mkdir(parents=True)
    row = batch_ui.SystemRow("sys1", str(work))
    batch_ui.fanout_command_to_systems("q", row)
    batch_ui.fanout_command_to_systems("detach", row)
    cmd_path = batch_commands.batch_commands_path(str(run_dir))
    assert not os.path.exists(cmd_path)


def test_help_and_footer_q_is_summary_only():
    summary = batch_ui._help_text(False)
    detail = batch_ui._help_text(True)
    assert "g/w/s/q all systems" in summary
    assert "q" not in detail
    row = batch_ui.SystemRow("sys1", "/work")
    view = batch_ui.build_detail_view(row)
    footer = "\n".join(str(part) for part in view.renderables)
    assert "g/w/s: this system" in footer
    assert "g/w/s/q" not in footer


def test_coordinator_prints_detach_wait_message(monkeypatch, tmp_path, capsys):
    batch_dir = tmp_path / "batch"
    batch_dir.mkdir()
    json_paths = {}
    rows = []
    for name in ("a",):
        work = batch_dir / f"work_{name}"
        (work / "logs").mkdir(parents=True)
        path = work / "seekrflow.json"
        path.write_text("{}")
        json_paths[name] = str(path)
        rows.append(batch_ui.SystemRow(name=name, work_directory=str(work)))

    batch = batch_structures.Batch(
        batch_directory=str(batch_dir),
        template={},
        systems=[batch_structures.Batch_system(name=n) for n in json_paths],
    )

    class FakeProc:
        def __init__(self):
            self.returncode = 0
            self.pid = -1

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

        def terminate(self):
            pass

    class FakeUI:
        def __init__(self, rows, on_command, refresh_rows=None, **kwargs):
            self.detail = False
            self.selected = 0
            self.detach_requested = False

        def run(self, should_stop, poll_seconds=1.0):
            self.detach_requested = True

    monkeypatch.setattr(
        batch_coordinator, "spawn_flow_child", lambda *a, **k: FakeProc())
    monkeypatch.setattr(batch_ui, "BatchUI", FakeUI)

    batch_coordinator.run_all_children_with_ui(batch, json_paths, rows)
    out = capsys.readouterr().out
    assert "detach command received" in out
    assert "wait for all processes to detach" in out


def test_display_maps_transfer_direction_and_pending():
    assert batch_ui._display_state_and_manager({
        "state": "started",
        "manager_status": "idle",
        "transfer_status": "running",
        "transfer_direction": "out",
    }) == ("transferring", "gathering")
    assert batch_ui._display_state_and_manager({
        "state": "completed",
        "manager_status": "idle",
        "transfer_status": "running",
        "transfer_direction": "back",
    }) == ("transferring", "pulling")
    assert batch_ui._display_state_and_manager({
        "state": "started",
        "manager_status": "running",
        "resource_name": "cluster",
        "job_ids": ["99"],
    }) == ("pending", "pending")
    assert batch_ui._display_state_and_manager({
        "state": "started",
        "manager_status": "running",
        "resource_name": "cluster",
        "job_ids": ["99"],
        "status_polled_at": None,
    }) == ("pending", "pending")
    assert batch_ui._display_state_and_manager({
        "state": "started",
        "manager_status": "queued",
        "resource_name": "cluster",
        "job_ids": [],
        "status_polled_at": time.time(),
    }) == ("queued", "queued")
    assert batch_ui._display_state_and_manager({
        "state": "started",
        "manager_status": "running",
        "resource_name": "cluster",
        "job_ids": ["99"],
        "status_polled_at": time.time(),
    }) == ("running", "running")
    assert batch_ui._display_state_and_manager({
        "state": "completed",
        "manager_status": "idle",
        "resource_name": "cluster",
        "job_ids": [],
    }) == ("completed", "idle")
    assert batch_ui._display_state_and_manager({
        "state": "queued",
        "manager_status": "idle",
        "resource_name": "cluster",
    }) == ("queued", "idle")
    assert batch_ui._display_state_and_manager({
        "state": "started",
        "manager_status": "running",
        "resource_name": "local",
    }) == ("started", "running")
    assert batch_ui._display_state_and_manager({
        "state": "completed",
        "manager_status": "queued",
        "resource_name": "cluster",
        "status_polled_at": time.time(),
    }) == ("queued", "queued")
    assert batch_ui._display_state_and_manager({
        "state": "completed",
        "manager_status": "running",
        "resource_name": "cluster",
        "status_polled_at": time.time(),
    }) == ("running", "running")
    assert batch_ui._display_state_and_manager({
        "state": "failed",
        "manager_status": "running",
        "resource_name": "cluster",
        "status_polled_at": time.time(),
    }) == ("failed", "running")


def test_stale_status_poll_is_dimmed():
    row = batch_ui.SystemRow("x", "/work")
    row.state = "started"
    row.manager_status = "running"
    row.status_polled_at = time.time() - 1000.0
    assert batch_ui._row_status_is_stale(row, stale_after_s=600.0)
    assert batch_ui._style_with_dim("yellow", True) == "dim yellow"
    row.status_polled_at = time.time()
    assert not batch_ui._row_status_is_stale(row, stale_after_s=600.0)


def test_patch_completed_remote_transfer_updates_status_file(tmp_path):
    from seekrflow.modules import seekr_run

    root = tmp_path / "root"
    root.mkdir()
    seekr_run.write_job_status(str(root), {
        "stages": {
            "mmvt": {
                "state": "completed",
                "resource_name": "cluster",
                "transfer_status": "idle",
            },
            "local_prep": {
                "state": "completed",
                "resource_name": "local",
                "transfer_status": "idle",
            },
            "running": {
                "state": "started",
                "resource_name": "cluster",
                "transfer_status": "idle",
            },
        }
    })
    seekr_run.patch_completed_remote_transfer(
        str(root), transfer_status="running", transfer_direction="back")
    data = seekr_run.load_job_status(str(root))
    assert data["stages"]["mmvt"]["transfer_status"] == "running"
    assert data["stages"]["mmvt"]["transfer_direction"] == "back"
    assert data["stages"]["mmvt"]["status_polled_at"]
    assert data["stages"]["local_prep"]["transfer_status"] == "idle"
    assert data["stages"]["running"]["transfer_status"] == "idle"


def test_status_message_renders_above_table(tmp_path):
    ui, _row = _make_batch_ui(tmp_path)
    ui._status_message = "transfer requested: mmvt (1 system)"
    group = ui._render()
    assert "transfer requested" in str(group.renderables[0])


def test_spawn_flow_child_appends_transfer_args(monkeypatch, tmp_path):
    recorded = {}

    class FakePopen:
        def __init__(self, cmd, **kwargs):
            recorded["cmd"] = cmd
            self._batch_log_file = None

    monkeypatch.setattr(batch_coordinator.subprocess, "Popen", FakePopen)
    log = tmp_path / "logs" / "x.log"
    batch_coordinator.spawn_flow_child(
        "run", "/tmp/sf.json", str(log), extra_args=["-T", "mmvt"])
    assert recorded["cmd"][-2:] == ["-T", "mmvt"]
    assert "--batch-child" in recorded["cmd"]


def test_run_batch_transfer_only_skips_ui(monkeypatch, tmp_path):
    batch_dir = tmp_path / "batch"
    batch_dir.mkdir()
    batch = batch_structures.Batch(
        batch_directory=str(batch_dir),
        template={},
        systems=[batch_structures.Batch_system(name="x")],
    )
    batch._source_dir = str(tmp_path)
    gated = {}

    def fake_gated(*args, **kwargs):
        gated["kwargs"] = kwargs
        gated["args"] = args
        return True

    def boom(*a, **k):
        raise AssertionError("live UI must not open for -T")

    monkeypatch.setattr(batch_coordinator, "run_gated_stage", fake_gated)
    monkeypatch.setattr(batch_coordinator, "run_all_children_with_ui", boom)
    monkeypatch.setattr(
        batch_structures, "materialize_all", lambda b: {"x": "/tmp/x.json"})
    rc = batch_coordinator.run_batch(
        batch, "run", transfer_from_remote_only="mmvt")
    assert rc == 0
    assert gated["kwargs"]["extra_args"] == ["-T", "mmvt"]
    assert gated["kwargs"]["log_stage"] == "transfer"
    assert gated["kwargs"]["concurrency"] == 1
    assert gated["args"][2] == "run"


def test_batch_cli_rejects_transfer_flag_without_run(capsys):
    import seekrflow.batch as batch_cli

    rc = batch_cli.main(["prepare", "-i", "x.json", "-T", "mmvt"])
    assert rc == 2
    assert "-T" in capsys.readouterr().err

