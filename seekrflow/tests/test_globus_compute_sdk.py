"""Tests for Globus Compute remote interface helpers."""

import os

from seekrflow.modules.remote_interfaces import globus_compute_sdk as gcs


def test_warn_endpoint_dedupes_same_key(capsys):
    gcs._last_endpoint_warn.clear()
    name = "ep-dedupe-test"
    gcs._warn_endpoint(name, "online_idle0", "WARN A")
    gcs._warn_endpoint(name, "online_idle0", "WARN B")
    out = capsys.readouterr().out
    assert out.count("WARN A") == 1
    assert "WARN B" not in out


def test_warn_endpoint_prints_on_key_change(capsys):
    gcs._last_endpoint_warn.clear()
    name = "ep-key-change"
    gcs._warn_endpoint(name, "online_idle0", "WARN IDLE")
    gcs._warn_endpoint(name, "state_offline", "WARN OFF")
    out = capsys.readouterr().out
    assert "WARN IDLE" in out
    assert "WARN OFF" in out


def test_warn_endpoint_reprints_after_interval(capsys, monkeypatch):
    gcs._last_endpoint_warn.clear()
    name = "ep-interval"
    clock = {"t": 1000.0}
    monkeypatch.setattr(gcs.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(gcs, "_ENDPOINT_WARN_INTERVAL_S", 10.0)

    gcs._warn_endpoint(name, "online_idle0", "WARN 1")
    clock["t"] = 1005.0
    gcs._warn_endpoint(name, "online_idle0", "WARN 2")
    clock["t"] = 1011.0
    gcs._warn_endpoint(name, "online_idle0", "WARN 3")

    out = capsys.readouterr().out
    assert "WARN 1" in out
    assert "WARN 2" not in out
    assert "WARN 3" in out


def test_globus_result_timeout_constant():
    assert gcs.GLOBUS_RESULT_TIMEOUT_S == 1200.0


def test_status_contention_error_helper():
    assert gcs.is_status_contention_error(
        gcs.GlobusRetryableError(
            "globus-client: status already in flight for ep|/r|mmvt"))
    assert gcs.is_status_contention_error(
        gcs.GlobusRetryableError(
            "globus-client: status poll cap reached (2 in-flight "
            "status tasks)"))
    assert not gcs.is_status_contention_error(
        RuntimeError("sbatch: invalid account"))


def test_decide_offline_endpoint_does_not_retry():
    status = {"status": "offline", "details": {"idle_workers": 0}}
    assert gcs.decide_globus_submit_action(endpoint_status=status) == (
        "fail_offline")
    assert gcs.decide_globus_submit_action(
        error=gcs.GlobusEndpointOfflineError(
            "globus-client: endpoint 'x' is STOPPED")
    ) == "fail_offline"
    assert gcs.decide_globus_submit_action(
        error="globus-client: endpoint 'x' is disconnected",
        attempt=1,
    ) == "fail_offline"


def test_decide_busy_retries_then_exhausts():
    busy = {"status": "online", "details": {"idle_workers": 0}}
    assert gcs.decide_globus_submit_action(
        endpoint_status=busy, attempt=1) == "retry"
    assert gcs.decide_globus_submit_action(
        endpoint_status=busy, attempt=12) == "fail_exhausted"
    assert gcs.decide_globus_submit_action(
        error=gcs.GlobusRetryableError(
            "globus-client: endpoint 'x' busy (0 idle workers)"),
        attempt=3,
    ) == "retry"


def test_decide_timeout_and_globus_sdk_flake_retry():
    assert gcs.decide_globus_submit_action(
        error=TimeoutError("globus-task: exceeded 1200s"),
        attempt=1,
    ) == "retry"
    assert gcs.decide_globus_submit_action(
        error=TimeoutError("globus-task: exceeded 1200s"),
        attempt=12,
    ) == "fail_exhausted"
    assert gcs.decide_globus_submit_action(
        error=ModuleNotFoundError("No module named 'globus_sdk'"),
        attempt=1,
    ) == "retry"
    assert gcs.decide_globus_submit_action(
        error="globus-task: No module named 'globus_sdk'",
        attempt=11,
    ) == "retry"


def test_decide_payload_error_is_fatal():
    assert gcs.decide_globus_submit_action(
        error="unknown stage 'mmvt_not_real'",
        attempt=1,
    ) == "fail_fatal"
    assert gcs.decide_globus_submit_action(
        error="sbatch: error: Batch job submission failed",
        attempt=1,
    ) == "fail_fatal"


def test_run_globus_submit_offline_no_retry():
    calls = {"n": 0}

    def submit_fn():
        calls["n"] += 1
        raise gcs.GlobusEndpointOfflineError(
            "globus-client: endpoint 'x' is OFFLINE")

    sleeps = []
    try:
        gcs.run_globus_submit_with_retries(
            submit_fn, sleep_fn=sleeps.append, max_attempts=12)
    except gcs.GlobusEndpointOfflineError:
        pass
    else:
        raise AssertionError("expected GlobusEndpointOfflineError")
    assert calls["n"] == 1
    assert sleeps == []


def test_run_globus_submit_busy_retries_then_fails():
    calls = {"n": 0}

    def submit_fn():
        calls["n"] += 1
        raise gcs.GlobusRetryableError(
            "globus-client: endpoint 'x' busy (0 idle workers)")

    sleeps = []
    try:
        gcs.run_globus_submit_with_retries(
            submit_fn, sleep_fn=sleeps.append, max_attempts=12)
    except gcs.GlobusRetryableError:
        pass
    else:
        raise AssertionError("expected GlobusRetryableError")
    assert calls["n"] == 12
    assert len(sleeps) == 11


def test_run_globus_submit_timeout_retries_twelve_then_fails():
    calls = {"n": 0}

    def submit_fn():
        calls["n"] += 1
        raise TimeoutError("globus-task: exceeded 1200s")

    sleeps = []
    try:
        gcs.run_globus_submit_with_retries(
            submit_fn, sleep_fn=sleeps.append, sleep_s=30.0, max_attempts=12)
    except TimeoutError:
        pass
    else:
        raise AssertionError("expected TimeoutError")
    assert calls["n"] == 12
    assert sleeps == [30.0] * 11


def test_run_globus_submit_globus_sdk_flake_retries_then_fails():
    calls = {"n": 0}

    def submit_fn():
        calls["n"] += 1
        raise ModuleNotFoundError("No module named 'globus_sdk'")

    sleeps = []
    try:
        gcs.run_globus_submit_with_retries(
            submit_fn, sleep_fn=sleeps.append, max_attempts=12)
    except ModuleNotFoundError:
        pass
    else:
        raise AssertionError("expected ModuleNotFoundError")
    assert calls["n"] == 12
    assert len(sleeps) == 11


def test_run_globus_submit_payload_fails_immediately():
    calls = {"n": 0}

    def submit_fn():
        calls["n"] += 1
        return {"success": False, "error": "unknown stage 'nope'"}

    sleeps = []
    try:
        gcs.run_globus_submit_with_retries(
            submit_fn, sleep_fn=sleeps.append, max_attempts=12)
    except RuntimeError as e:
        assert "unknown stage" in str(e)
    else:
        raise AssertionError("expected RuntimeError")
    assert calls["n"] == 1
    assert sleeps == []


def test_with_globus_lock_falls_back_to_per_user(monkeypatch, tmp_path):
    """
    Standalone runs serialize too: without a batch lock they use the per-user
    one, so unsupervised runs cannot stampede an endpoint.
    """
    import seekrflow.modules.batch.globus_lock as globus_lock

    monkeypatch.delenv("SEEKR_GLOBUS_LOCK_FILE", raising=False)
    monkeypatch.delenv("SEEKR_GLOBUS_LOCK_DISABLE", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(
        os.path, "expanduser",
        lambda p: p.replace("~", str(tmp_path), 1)
        if p.startswith("~") else p)
    acquired = []

    def fake_acquire(path, kind, **kwargs):
        acquired.append((path, kind))
        return True

    monkeypatch.setattr(globus_lock, "acquire_blocking", fake_acquire)
    monkeypatch.setattr(
        globus_lock, "release", lambda path, **kw: acquired.append("rel"))
    assert gcs._with_batch_globus_lock("submit", lambda: 7) == 7
    assert acquired[0] == (globus_lock.user_lock_file_path(), "submit")
    assert acquired[-1] == "rel"


def test_with_globus_lock_can_be_disabled(monkeypatch):
    import seekrflow.modules.batch.globus_lock as globus_lock

    monkeypatch.delenv("SEEKR_GLOBUS_LOCK_FILE", raising=False)
    monkeypatch.setenv("SEEKR_GLOBUS_LOCK_DISABLE", "1")
    acquired = []

    monkeypatch.setattr(
        globus_lock, "acquire_blocking",
        lambda path, kind, **kw: acquired.append((path, kind)) or True)
    monkeypatch.setattr(
        globus_lock, "release", lambda path, **kw: acquired.append("rel"))
    assert gcs._with_batch_globus_lock("submit", lambda: 7) == 7
    assert acquired[0] == (None, "submit")


def test_with_batch_globus_lock_uses_env_path(monkeypatch, tmp_path):
    import seekrflow.modules.batch.globus_lock as globus_lock

    lock_path = str(tmp_path / ".seekrflow_globus_lock.json")
    monkeypatch.setenv("SEEKR_GLOBUS_LOCK_FILE", lock_path)
    seen = []

    def fake_acquire(path, kind, **kwargs):
        seen.append(("acq", path, kind))
        return True

    def fake_release(path, **kwargs):
        seen.append(("rel", path))

    monkeypatch.setattr(globus_lock, "acquire_blocking", fake_acquire)
    monkeypatch.setattr(globus_lock, "release", fake_release)
    assert gcs._with_batch_globus_lock("cancel", lambda: "ok") == "ok"
    assert seen == [
        ("acq", lock_path, "cancel"),
        ("rel", lock_path),
    ]


def test_lock_released_before_future_result(monkeypatch):
    """The Compute lock must not be held while waiting on future.result."""
    import seekrflow.modules.batch.globus_lock as globus_lock

    events = []
    monkeypatch.setattr(
        globus_lock, "resolve_lock_file_path", lambda: "/tmp/lock")
    monkeypatch.setattr(
        globus_lock, "acquire_blocking",
        lambda path, kind, **k: events.append("acquire") or True)
    monkeypatch.setattr(
        globus_lock, "release",
        lambda path, **k: events.append("release"))
    monkeypatch.setattr(
        globus_lock, "is_status_kind",
        lambda kind: kind in {"status", "status_focused"})
    monkeypatch.setattr(
        globus_lock, "try_begin_in_flight",
        lambda path, kind, key, **k: events.append("begin") or (
            globus_lock.IN_FLIGHT_SUBMIT))
    monkeypatch.setattr(
        globus_lock, "end_in_flight",
        lambda path, key, **k: events.append("end"))

    def submit_fn():
        events.append("submit")
        return "future"

    def wait_fn(fut):
        events.append("wait")
        assert fut == "future"
        return {"ok": True}

    assert gcs._acquire_submit_release_wait(
        "status", "ep|/root|mmvt", submit_fn, wait_fn) == {"ok": True}
    assert events == [
        "acquire", "begin", "submit", "release", "wait", "end"]


def test_duplicate_status_submit_is_skipped(monkeypatch, tmp_path):
    import seekrflow.modules.batch.globus_lock as globus_lock

    path = str(tmp_path / "lock.json")
    monkeypatch.setenv("SEEKR_GLOBUS_LOCK_FILE", path)
    monkeypatch.setattr(globus_lock, "_pid_alive", lambda pid: True)
    globus_lock.init_lock(path)
    key = "ep|/root|mmvt"
    assert globus_lock.try_begin_in_flight(
        path, "status", key) == globus_lock.IN_FLIGHT_SUBMIT
    called = []
    try:
        gcs._acquire_submit_release_wait(
            "status",
            key,
            lambda: called.append("submit") or "f",
            lambda fut: called.append("wait") or fut,
        )
    except gcs.GlobusRetryableError as e:
        assert "already in flight" in str(e)
        assert gcs.is_retryable_globus_error(e)
    else:
        raise AssertionError("expected GlobusRetryableError")
    assert called == []


def test_inflight_key_uses_endpoint_root_stage():
    assert gcs.inflight_key(
        "abc-ep", ["/remote/root", "mmvt", False]
    ) == "abc-ep|/remote/root|mmvt"


def test_submit_remote_workflow_waits_after_lock_release(monkeypatch, tmp_path):
    """Executor submit happens under the lock; future.result does not."""
    import sys
    import types
    import seekrflow.modules.batch.globus_lock as globus_lock

    events = []
    holding = {"on": False}

    class FakeFuture:
        def result(self, timeout=None):
            events.append(("result", holding["on"]))
            return {"success": True}

    class FakeExecutor:
        def __init__(self, endpoint):
            events.append("executor_init")

        def register_function(self, *a, **k):
            return "fid"

        def submit_to_registered_function(self, **k):
            events.append(("submit", holding["on"]))
            return FakeFuture()

        def shutdown(self, wait=False, **k):
            events.append(("shutdown", wait))

    class FakeClient:
        def get_endpoint_status(self, endpoint):
            return {
                "status": "online",
                "details": {"idle_workers": 1, "total_workers": 2},
            }

    fake_mod = types.ModuleType("globus_compute_sdk")
    fake_mod.Client = FakeClient
    fake_mod.Executor = FakeExecutor
    fake_ser = types.ModuleType("globus_compute_sdk.serialize")

    class ComputeSerializer:
        def __init__(self, strategy_code=None):
            pass

    class CombinedCode:
        pass

    fake_ser.ComputeSerializer = ComputeSerializer
    fake_ser.CombinedCode = CombinedCode
    monkeypatch.setitem(sys.modules, "globus_compute_sdk", fake_mod)
    monkeypatch.setitem(sys.modules, "globus_compute_sdk.serialize", fake_ser)

    lock_path = str(tmp_path / "lock.json")
    monkeypatch.setenv("SEEKR_GLOBUS_LOCK_FILE", lock_path)
    monkeypatch.delenv("SEEKR_GLOBUS_LOCK_DISABLE", raising=False)
    globus_lock.init_lock(lock_path)

    orig_acq = globus_lock.acquire_blocking
    orig_rel = globus_lock.release

    def acq(path, kind, **kw):
        holding["on"] = True
        events.append("acquire")
        return orig_acq(path, kind, **kw)

    def rel(path, **kw):
        events.append("release")
        holding["on"] = False
        return orig_rel(path, **kw)

    monkeypatch.setattr(globus_lock, "acquire_blocking", acq)
    monkeypatch.setattr(globus_lock, "release", rel)

    result = gcs.submit_remote_workflow_with_globus_compute(
        "delta",
        lambda args: None,
        "endpoint-id",
        ["/remote/root", "mmvt"],
        silent=True,
        kind="status",
    )
    assert result == {"success": True}
    assert ("submit", True) in events
    assert ("result", False) in events
    assert events.index("release") < events.index(("result", False))

