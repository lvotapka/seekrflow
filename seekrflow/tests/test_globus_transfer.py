"""Tests for Globus transfer filters, error classification, and wait vs queued."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import seekrflow.modules.transfer.base as transfer_base
import seekrflow.modules.transfer.globus as transfer_globus
from seekrflow.modules.transfer.aws_s3 import LOCAL_RUNNER_DIRNAMES
from seekrflow.modules.run_lock import RUN_LOCK_FILENAME


class FakeTransferData:
    def __init__(self, source, destination, **kwargs):
        self.source = source
        self.destination = destination
        self.kwargs = kwargs
        self.filters = []
        self.items = []

    def add_filter_rule(self, **kwargs):
        self.filters.append(kwargs)

    def add_item(self, *args, **kwargs):
        self.items.append((args, kwargs))


class FakeSdkError(Exception):
    """Duck-typed globus_sdk-like error."""

    def __init__(
            self,
            *args,
            http_status=None,
            code=None,
            message=None,
            method=None,
            url=None,
            ):
        super().__init__(*args)
        self.http_status = http_status
        self.code = code
        self.message = message
        self.method = method
        self.url = url


FakeSdkError.__module__ = "globus_sdk.exc"


def test_build_transfer_data_excludes_volatile_files_and_skips_source_errors():
    tdata = transfer_globus.build_transfer_data(
        "src-id", "dst-id", "label", TransferData=FakeTransferData)
    assert tdata.kwargs["skip_source_errors"] is True
    assert tdata.kwargs["verify_checksum"] is True
    names = [rule["name"] for rule in tdata.filters]
    assert all(rule["method"] == "exclude" for rule in tdata.filters)
    assert transfer_globus.STATUS_FILE_NAME in names
    assert transfer_globus.STATUS_TMP_GLOB in names
    assert RUN_LOCK_FILENAME in names
    for dirname in LOCAL_RUNNER_DIRNAMES:
        assert dirname in names


def test_file_not_found_and_checksum_are_retryable_not_fatal():
    assert transfer_globus.classify_task_document(
        {"status": "FAILED", "nice_status": "FILE_NOT_FOUND"})[0] == "retry"
    assert transfer_globus.classify_task_document(
        {"status": "FAILED", "nice_status": "CHECKSUM_MISMATCH"})[0] == "retry"
    assert transfer_globus.classify_task_document(
        {"status": "ACTIVE", "nice_status": "FILE_NOT_FOUND"})[0] == "retry"
    assert transfer_globus.classify_task_document(
        {"status": "FAILED", "nice_status": "PERMISSION_DENIED"})[0] == "fatal"
    assert "FILE_NOT_FOUND" not in transfer_globus.FATAL_NICE_STATUSES


def test_sdk_http_zero_formats_cleanly_and_is_retryable():
    error = FakeSdkError(0, http_status=0)
    formatted = transfer_globus.format_transfer_exception(error)
    assert "0" != formatted
    assert "HTTP 0" in formatted
    wrapped = transfer_globus.wrap_transfer_exception(error)
    assert isinstance(wrapped, transfer_globus.GlobusTransferRetryableError)
    assert transfer_base.is_retryable_transfer_error(wrapped)
    assert transfer_base.is_retryable_transfer_error(error)


def test_sdk_post_tuple_formats_cleanly_and_is_retryable():
    url = "https://transfer.api.globus.org/v0.10/transfer"
    error = FakeSdkError("POST", url)
    formatted = transfer_globus.format_transfer_exception(error)
    assert "POST" in formatted
    assert "transfer.api.globus.org" in formatted
    assert formatted.startswith("globus-transfer:")
    wrapped = transfer_globus.wrap_transfer_exception(error)
    assert isinstance(wrapped, transfer_globus.GlobusTransferRetryableError)
    assert transfer_base.is_retryable_transfer_error(error)


def test_sdk_429_is_retryable_401_is_fatal():
    too_many = FakeSdkError(
        http_status=429, code="RateLimitExceeded", method="POST",
        url="https://transfer.api.globus.org/v0.10/transfer",
        message="Too Many Requests")
    wrapped_429 = transfer_globus.wrap_transfer_exception(too_many)
    assert isinstance(wrapped_429, transfer_globus.GlobusTransferRetryableError)
    assert "HTTP 429" in str(wrapped_429)
    assert "RateLimitExceeded" in str(wrapped_429)

    denied = FakeSdkError(
        http_status=401, code="AUTHENTICATION_FAILED", method="POST",
        url="https://transfer.api.globus.org/v0.10/transfer")
    wrapped_401 = transfer_globus.wrap_transfer_exception(denied)
    assert isinstance(wrapped_401, transfer_globus.GlobusTransferError)
    assert not isinstance(wrapped_401, transfer_globus.GlobusTransferRetryableError)
    assert not transfer_base.is_retryable_transfer_error(wrapped_401)


def test_checksum_message_is_retryable():
    error = FakeSdkError("Checksum mismatch on .seekrflow_job_status.json")
    wrapped = transfer_globus.wrap_transfer_exception(error)
    assert isinstance(wrapped, transfer_globus.GlobusTransferRetryableError)


def _stage_workflow():
    from seekrflow.modules import seekr_run
    from seekrflow.modules import structures as flow_structures

    return seekr_run.StageWorkflow(
        model=SimpleNamespace(directory="/work"),
        seekrflow=SimpleNamespace(name="run1"),
        stage=SimpleNamespace(name="mmvt", index=1),
        workflow_engine=object(),
        resource_name="cluster",
        resource=flow_structures.Resource_remote_slurm(name="cluster"),
    )


def test_retryable_transfer_error_stays_queued_not_waiting():
    sw = _stage_workflow()
    sw.semaphore = "go"
    sw.state = "started"
    sw.apply_transfer_error(
        transfer_globus.GlobusTransferRetryableError(
            "globus-transfer: CHECKSUM_MISMATCH"))
    assert sw.state == "queued"
    assert sw.semaphore == "go"
    assert sw.transfer_status == "failed"
    assert "retried" in sw.last_error


def test_permission_denied_transfer_sets_wait():
    sw = _stage_workflow()
    sw.semaphore = "go"
    sw.state = "started"
    sw.apply_transfer_error(
        transfer_globus.GlobusTransferError(
            "globus-transfer: task t1 failed: PERMISSION_DENIED"))
    assert sw.state == "failed"
    assert sw.semaphore == "wait"
    assert sw.last_error.startswith("transfer failed:")


def test_unwrapped_sdk_error_stays_queued():
    """Safety net: a leaked SDK error must not park the stage on wait."""
    sw = _stage_workflow()
    sw.semaphore = "go"
    sw.apply_transfer_error(FakeSdkError(0, http_status=0))
    assert sw.state == "queued"
    assert sw.semaphore == "go"


def test_bare_zero_exception_is_retryable_and_stays_queued():
    """The real SDK failure: args==(0,) and no http_status attribute."""
    error = Exception(0)
    assert transfer_globus.http_status_from_error(error) == 0
    wrapped = transfer_globus.wrap_transfer_exception(error)
    assert isinstance(wrapped, transfer_globus.GlobusTransferRetryableError)
    assert "HTTP 0" in str(wrapped)
    assert transfer_base.is_retryable_transfer_error(error)
    assert transfer_base.is_retryable_transfer_error(
        transfer_globus.GlobusTransferError("globus-transfer: HTTP 0"))

    sw = _stage_workflow()
    sw.semaphore = "go"
    sw.apply_transfer_error(Exception(0))
    assert sw.state == "queued"
    assert sw.semaphore == "go"


def test_http0_globus_transfer_error_stays_queued():
    sw = _stage_workflow()
    sw.semaphore = "go"
    sw.apply_transfer_error(
        transfer_globus.GlobusTransferError("globus-transfer: HTTP 0"))
    assert sw.state == "queued"
    assert sw.semaphore == "go"


def test_transfer_relaunch_cap_sets_wait():
    sw = _stage_workflow()
    sw.semaphore = "go"
    err = transfer_globus.GlobusTransferRetryableError(
        "globus-transfer: HTTP 0")
    cap = transfer_globus.GLOBUS_TRANSFER_MAX_ATTEMPTS
    for _ in range(cap - 1):
        sw.apply_transfer_error(err)
        assert sw.state == "queued"
        assert sw.semaphore == "go"
    sw.apply_transfer_error(err)
    assert sw.state == "failed"
    assert sw.semaphore == "wait"
    assert "could not be confirmed" in sw.last_error
    assert "may already be remote" in sw.last_error


def test_skips_submit_when_destination_already_has_model(tmp_path, monkeypatch):
    local = tmp_path / "local"
    local.mkdir()
    (local / "model.json").write_text("{}")
    submits = []

    class Client:
        def add_app_data_access_scope(self, collection_id):
            return None

        def operation_ls(self, collection_id, path=None):
            return [{"name": "model.json"}]

        def submit_transfer(self, tdata):
            submits.append(tdata)
            return {"task_id": "should-not-happen"}

    monkeypatch.setattr(
        transfer_globus, "get_transfer_client", lambda: Client())
    monkeypatch.setattr(
        transfer_globus, "build_transfer_data",
        lambda *a, **k: FakeTransferData("s", "d"))
    transfer_globus.transfer_files_with_globus(
        "sys", str(local), "/remote", "local-id", "remote-id",
        retry_sleep=0.0)
    assert submits == []


def test_get_task_http0_succeeds_if_model_json_present(tmp_path, monkeypatch):
    local = tmp_path / "local"
    local.mkdir()
    (local / "model.json").write_text("{}")
    monkeypatch.setattr(transfer_globus, "GLOBUS_TRANSFER_MAX_ERROR_POLLS", 1)
    monkeypatch.setattr(transfer_globus, "GLOBUS_TRANSFER_POLL_INTERVAL_S", 0.001)
    ls_calls = {"n": 0}
    submits = []
    cancelled = []

    class Client:
        def add_app_data_access_scope(self, collection_id):
            return None

        def operation_ls(self, collection_id, path=None):
            ls_calls["n"] += 1
            if ls_calls["n"] == 1:
                return []
            return [{"name": "model.json"}]

        def submit_transfer(self, tdata):
            submits.append(1)
            return {"task_id": "t1"}

        def task_wait(self, task_id, timeout=0, polling_interval=0):
            return True

        def get_task(self, task_id):
            raise Exception(0)

        def cancel_task(self, task_id):
            cancelled.append(task_id)

    monkeypatch.setattr(
        transfer_globus, "get_transfer_client", lambda: Client())
    monkeypatch.setattr(
        transfer_globus, "build_transfer_data",
        lambda *a, **k: FakeTransferData("s", "d"))
    transfer_globus.transfer_files_with_globus(
        "sys", str(local), "/remote", "local-id", "remote-id",
        max_attempts=2, retry_sleep=0.0)
    assert submits == [1]
    assert cancelled == []


def test_http0_after_submit_does_not_submit_again(tmp_path, monkeypatch):
    local = tmp_path / "local"
    local.mkdir()
    (local / "model.json").write_text("{}")
    monkeypatch.setattr(transfer_globus, "GLOBUS_TRANSFER_MAX_ERROR_POLLS", 1)
    monkeypatch.setattr(transfer_globus, "GLOBUS_TRANSFER_POLL_INTERVAL_S", 0.001)
    submits = []

    class Client:
        def add_app_data_access_scope(self, collection_id):
            return None

        def operation_ls(self, collection_id, path=None):
            return []

        def submit_transfer(self, tdata):
            submits.append(1)
            return {"task_id": "t1"}

        def task_wait(self, task_id, timeout=0, polling_interval=0):
            return True

        def get_task(self, task_id):
            raise Exception(0)

        def cancel_task(self, task_id):
            return None

    monkeypatch.setattr(
        transfer_globus, "get_transfer_client", lambda: Client())
    monkeypatch.setattr(
        transfer_globus, "build_transfer_data",
        lambda *a, **k: FakeTransferData("s", "d"))
    with pytest.raises(transfer_globus.GlobusTransferRetryableError):
        transfer_globus.transfer_files_with_globus(
            "sys", str(local), "/remote", "local-id", "remote-id",
            max_attempts=3, retry_sleep=0.0)
    assert submits == [1]


def test_exhausted_sdk_retry_raises_classified_error(monkeypatch):
    """After inner retries, a raw SDK error is re-raised as retryable."""

    class BoomClient:
        def add_app_data_access_scope(self, collection_id):
            return None

        def submit_transfer(self, tdata):
            raise FakeSdkError(0, http_status=0)

    monkeypatch.setattr(
        transfer_globus, "get_transfer_client", lambda: BoomClient())
    monkeypatch.setattr(
        transfer_globus, "build_transfer_data",
        lambda *a, **k: FakeTransferData("s", "d"))

    with pytest.raises(transfer_globus.GlobusTransferRetryableError) as excinfo:
        transfer_globus.transfer_files_with_globus(
            "sys", "/local", "/remote", "local-id", "remote-id",
            max_attempts=2, retry_sleep=0.0)
    assert "HTTP 0" in str(excinfo.value)
    assert transfer_base.is_retryable_transfer_error(excinfo.value)
