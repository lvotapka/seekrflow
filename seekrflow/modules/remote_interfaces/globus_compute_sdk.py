"""
modules/remote_interfaces/globus_compute_sdk.py

Provide workflow submission with globus compute SDK.
"""

from __future__ import annotations

import fcntl
import json
import os
import time
import typing

# Soft wait for a submitted Globus Compute future. On expiry, raise TimeoutError
# so callers can retry on the next poll without treating it as a hard failure.
GLOBUS_RESULT_TIMEOUT_S = 1200.0

# Endpoint health is shared across batch children through a small cache file so
# that N children do not each hit the API for the same answer.
ENDPOINT_STATUS_CACHE_FILENAME = ".seekrflow_endpoint_status.json"
ENDPOINT_STATUS_CACHE_TTL_S = 30.0

# Call kinds that must not be blocked by a busy endpoint: status keeps the
# state machine honest and cancel must always get through.
CAPACITY_GATED_KINDS = frozenset({"submit"})

# Batch submit retries (same count/spacing as idle-incomplete grace).
GLOBUS_SUBMIT_MAX_ATTEMPTS = 12
GLOBUS_SUBMIT_RETRY_SLEEP_S = 30.0

OFFLINE_ENDPOINT_STATES = frozenset({"offline", "disconnected", "stopped"})

# Dedupe endpoint health warnings: same (endpoint, warn_key) at most once per
# this many seconds; a different warn_key prints immediately.
_ENDPOINT_WARN_INTERVAL_S = 300.0
_last_endpoint_warn: dict[str, tuple[str, float]] = {}


class GlobusEndpointOfflineError(RuntimeError):
    """Endpoint is offline/disconnected/stopped; do not retry."""


class GlobusRetryableError(RuntimeError):
    """Busy, timeout, or globus_sdk import flake; retry then fail."""


def _warn_endpoint(name: str, warn_key: str, message: str) -> None:
    now = time.monotonic()
    previous = _last_endpoint_warn.get(name)
    if (
            previous is not None
            and previous[0] == warn_key
            and (now - previous[1]) < _ENDPOINT_WARN_INTERVAL_S
    ):
        return
    _last_endpoint_warn[name] = (warn_key, now)
    print(message)


def _batch_globus_lock_enabled() -> bool:
    """
    True when this process shares a lock with sibling batch children.

    Only a batch has many processes racing one endpoint, so this gates the
    capacity check. The serialization lock itself is always on (per-user by
    default) since it cannot starve anyone.
    """
    return bool(os.environ.get("SEEKR_GLOBUS_LOCK_FILE"))


def _with_batch_globus_lock(kind: str, fn: typing.Callable[[], typing.Any]):
    """
    Serialize one short Globus Compute call, batch-wide or user-wide.

    Do not wrap ``future.result`` in this helper: that holds the lock for the
    entire remote round-trip. Use ``_acquire_submit_release_wait`` instead.
    """
    import seekrflow.modules.batch.globus_lock as globus_lock
    path = globus_lock.resolve_lock_file_path()
    globus_lock.acquire_blocking(path, kind)
    try:
        return fn()
    finally:
        globus_lock.release(path)


def inflight_key(endpoint: str, args: typing.Sequence | None) -> str:
    """Stable (endpoint, remote_root, stage) key for status-poll dedupe."""
    dest = ""
    stage = ""
    if args:
        dest = str(args[0]) if len(args) > 0 else ""
        if len(args) > 1:
            stage = str(args[1])
    return f"{endpoint}|{dest}|{stage}"


def _shutdown_executor(gcx: typing.Any) -> None:
    """Drop the local Executor without waiting on or cancelling the task."""
    shutdown = getattr(gcx, "shutdown", None)
    if not callable(shutdown):
        return
    for kwargs in (
            {"wait": False, "cancel_futures": False},
            {"wait": False},
            {},
    ):
        try:
            shutdown(**kwargs)
            return
        except TypeError:
            continue
        except Exception:
            return


def _acquire_submit_release_wait(
        kind: str,
        poll_key: str,
        submit_fn: typing.Callable[[], typing.Any],
        wait_fn: typing.Callable[[typing.Any], typing.Any],
        ) -> typing.Any:
    """
    Hold the Globus lock only around ``submit_fn``; ``wait_fn`` runs after.

    Status kinds also occupy an ``in_flight`` slot so a second poll for the
    same endpoint/root/stage is skipped while the first is outstanding.
    """
    import seekrflow.modules.batch.globus_lock as globus_lock
    path = globus_lock.resolve_lock_file_path()
    globus_lock.acquire_blocking(path, kind)
    token = None
    try:
        if globus_lock.is_status_kind(kind):
            action = globus_lock.try_begin_in_flight(path, kind, poll_key)
            if action == globus_lock.IN_FLIGHT_DUPLICATE:
                raise GlobusRetryableError(
                    f"globus-client: status already in flight for {poll_key}"
                )
            if action == globus_lock.IN_FLIGHT_AT_CAPACITY:
                raise GlobusRetryableError(
                    f"globus-client: status poll cap reached "
                    f"({globus_lock.STATUS_IN_FLIGHT_CAP} in-flight "
                    f"status tasks)"
                )
            token = poll_key
        submitted = submit_fn()
    except BaseException:
        if token is not None:
            globus_lock.end_in_flight(path, token)
        raise
    finally:
        globus_lock.release(path)
    try:
        return wait_fn(submitted)
    finally:
        if token is not None:
            globus_lock.end_in_flight(path, token)


def _endpoint_status_cache_path() -> str | None:
    """
    Cache file beside whichever Globus lock this process uses.
    """
    import seekrflow.modules.batch.globus_lock as globus_lock
    lock_path = globus_lock.resolve_lock_file_path()
    if not lock_path:
        return None
    return os.path.join(
        os.path.dirname(os.path.abspath(lock_path)),
        ENDPOINT_STATUS_CACHE_FILENAME,
    )


def _read_cached_endpoint_status(
        endpoint: str,
        ttl: float | None = None,
        ) -> dict | None:
    if ttl is None:
        ttl = ENDPOINT_STATUS_CACHE_TTL_S
    path = _endpoint_status_cache_path()
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            try:
                data = json.load(f)
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    entry = data.get(endpoint)
    if not isinstance(entry, dict):
        return None
    try:
        fetched_at = float(entry.get("fetched_at", 0.0))
    except (TypeError, ValueError):
        return None
    if (time.time() - fetched_at) > ttl:
        return None
    status = entry.get("status")
    return status if isinstance(status, dict) else None


def _write_cached_endpoint_status(endpoint: str, status: dict) -> None:
    path = _endpoint_status_cache_path()
    if not path:
        return
    try:
        with open(path, "a+") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                f.seek(0)
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    data = {}
                if not isinstance(data, dict):
                    data = {}
                data[endpoint] = {
                    "fetched_at": time.time(),
                    "status": status,
                }
                f.seek(0)
                f.truncate()
                json.dump(data, f, indent=2, default=str)
                f.flush()
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except OSError:
        pass


def get_endpoint_status_cached(client: typing.Any, endpoint: str) -> dict:
    """
    Endpoint status, reusing a recent batch-shared answer when available.
    """
    cached = _read_cached_endpoint_status(endpoint)
    if cached is not None:
        return cached
    status = client.get_endpoint_status(endpoint)
    if isinstance(status, dict):
        _write_cached_endpoint_status(endpoint, status)
        return status
    return status


def endpoint_is_offline(status: dict | None) -> bool:
    if not status:
        return False
    s = (status.get("status") or "").lower()
    return s in OFFLINE_ENDPOINT_STATES


def endpoint_is_busy(status: dict | None) -> bool:
    if not status:
        return False
    s = (status.get("status") or "").lower()
    details = status.get("details") or {}
    idle = details.get("idle_workers")
    return s == "online" and idle == 0


def is_retryable_globus_error(error: BaseException | str) -> bool:
    """Busy / timeout / globus_sdk import flake — not a payload/sbatch error."""
    if isinstance(error, GlobusEndpointOfflineError):
        return False
    if isinstance(error, TimeoutError):
        return True
    if isinstance(error, GlobusRetryableError):
        return True
    text = str(error).lower()
    if "globus_sdk" in text:
        return True
    if isinstance(error, ModuleNotFoundError) and "globus" in text:
        return True
    tagged = "globus-client:" in text or "globus-task:" in text
    if tagged:
        if any(
                token in text
                for token in (
                    "timeout",
                    "busy",
                    "idle worker",
                    "no module named",
                    "already in flight",
                    "status poll cap",
                )
        ):
            return True
    return False


def is_status_contention_error(error: BaseException | str) -> bool:
    """True when a status submit was skipped because a poll is already in flight."""
    text = str(error).lower()
    return "already in flight" in text or "status poll cap" in text


def is_offline_globus_error(error: BaseException | str) -> bool:
    if isinstance(error, GlobusEndpointOfflineError):
        return True
    text = str(error).lower()
    if "globus-client:" not in text and "endpoint" not in text:
        return False
    return any(state in text for state in OFFLINE_ENDPOINT_STATES)


def decide_globus_submit_action(
        endpoint_status: dict | None = None,
        error: BaseException | str | None = None,
        attempt: int = 1,
        max_attempts: int = GLOBUS_SUBMIT_MAX_ATTEMPTS,
        ) -> str:
    """
    Classify a Globus submit attempt.

    Returns one of:
      ``submit`` — proceed (or accept a successful result)
      ``retry`` — sleep and try again
      ``fail_offline`` — endpoint down; no retry
      ``fail_fatal`` — payload / unknown error; no retry
      ``fail_exhausted`` — retryable but attempts used up
    """
    if endpoint_status is not None and error is None:
        if endpoint_is_offline(endpoint_status):
            return "fail_offline"
        if endpoint_is_busy(endpoint_status):
            if attempt >= max_attempts:
                return "fail_exhausted"
            return "retry"
        return "submit"
    if error is not None:
        if is_offline_globus_error(error):
            return "fail_offline"
        if is_retryable_globus_error(error):
            if attempt >= max_attempts:
                return "fail_exhausted"
            return "retry"
        return "fail_fatal"
    return "submit"


def run_globus_submit_with_retries(
        submit_fn: typing.Callable[[], dict],
        *,
        max_attempts: int = GLOBUS_SUBMIT_MAX_ATTEMPTS,
        sleep_s: float = GLOBUS_SUBMIT_RETRY_SLEEP_S,
        sleep_fn: typing.Callable[[float], None] = time.sleep,
        on_retry: typing.Callable[[int, str], None] | None = None,
        ) -> dict:
    """
    Call ``submit_fn`` until success or a non-retryable / exhausted failure.

    Sleeps ``sleep_s`` between attempts only (not an extra result timeout).
    """
    last_error: BaseException | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            result = submit_fn()
        except Exception as e:
            last_error = e
            action = decide_globus_submit_action(
                error=e, attempt=attempt, max_attempts=max_attempts)
            if action == "retry":
                if on_retry is not None:
                    on_retry(attempt, str(e))
                sleep_fn(sleep_s)
                continue
            raise
        if isinstance(result, dict) and result.get("success") is False:
            err = result.get("error") or "remote submit failed"
            action = decide_globus_submit_action(
                error=err, attempt=attempt, max_attempts=max_attempts)
            if action == "retry":
                if on_retry is not None:
                    on_retry(attempt, str(err))
                sleep_fn(sleep_s)
                continue
            raise RuntimeError(err)
        return result
    if last_error is not None:
        raise last_error
    raise RuntimeError("Globus submit retries exhausted")


def _tagged_exception(exc: BaseException, side: str) -> BaseException:
    """Prefix globus-client: vs globus-task: and classify retryability."""
    text = str(exc)
    msg = f"globus-{side}: {text}"
    if isinstance(exc, TimeoutError):
        return TimeoutError(msg)
    if is_offline_globus_error(exc) or (
            "endpoint" in text.lower()
            and any(s in text.lower() for s in OFFLINE_ENDPOINT_STATES)
    ):
        return GlobusEndpointOfflineError(msg)
    if is_retryable_globus_error(exc):
        return GlobusRetryableError(msg)
    return RuntimeError(msg)

# TODO: remove 'kinds' and consider whether to keep lock functionality.
def submit_remote_workflow_with_globus_compute(
        name: str,
        workflow: typing.Any,
        endpoint: str,
        args: tuple,
        silent: bool = False,
        kind: str = "submit",
        ) -> dict:
    poll_key = inflight_key(endpoint, args)

    def _submit() -> tuple:
        try:
            from globus_compute_sdk import Client, Executor
            from globus_compute_sdk.serialize import (
                ComputeSerializer, CombinedCode)
        except ModuleNotFoundError as e:
            if "globus_sdk" in str(e):
                raise GlobusRetryableError(f"globus-client: {e}") from e
            raise
        try:
            c = Client()
            status = get_endpoint_status_cached(c, endpoint)
        except Exception as e:
            raise _tagged_exception(e, "client") from e

        s = (status.get("status") or "unknown").lower()
        d = status.get("details") or {}
        idle = d.get("idle_workers")
        total = d.get("total_workers")
        pending = d.get("pending_tasks") or d.get("outstanding_tasks")

        if not silent:
            if s == "online":
                if total is not None and idle == 0:
                    _warn_endpoint(
                        name,
                        "online_idle0",
                        f"WARNING: Globus endpoint '{name}' is ONLINE but currently "
                        f"has 0 idle workers (total_workers={total}, "
                        f"pending_tasks={pending or 0}). Your task will queue until "
                        f"workers become available.",
                    )
            elif s in OFFLINE_ENDPOINT_STATES:
                _warn_endpoint(
                    name,
                    f"state_{s}",
                    f"WARNING: Globus endpoint '{name}' is {s.upper()}. Start it "
                    f"on the host: `globus-compute-endpoint start <ENDPOINT_NAME>`.",
                )
            elif s in {"initializing", "starting"}:
                _warn_endpoint(
                    name,
                    f"state_{s}",
                    f"Globus endpoint '{name}' is STARTING; tasks will queue until "
                    f"workers connect.",
                )
            else:
                _warn_endpoint(
                    name,
                    f"state_{s}",
                    f"WARNING: Globus endpoint '{name}' is in an UNKNOWN state. "
                    f"Jobs will not run.",
                )

        # Batch children: do not pile new work onto a down or fully-busy
        # endpoint. Standalone flow.py (no lock file) still submits and lets
        # the endpoint queue the task, matching prior behavior.
        #
        # Status and cancel are never capacity-gated: blocking status probes
        # starves the state machine of the truth it needs, and a blocked
        # cancel leaves jobs running.
        if _batch_globus_lock_enabled() and kind in CAPACITY_GATED_KINDS:
            if endpoint_is_offline(status):
                raise GlobusEndpointOfflineError(
                    f"globus-client: endpoint {name!r} is {s.upper()}"
                )
            if endpoint_is_busy(status):
                raise GlobusRetryableError(
                    f"globus-client: endpoint {name!r} busy "
                    f"(0 idle workers, total_workers={total}, "
                    f"pending_tasks={pending or 0})"
                )

        # Do not use Executor as a context manager: __exit__ typically
        # shutdown(wait=True), which would wait for the remote result (or
        # cancel the task) while we still want the future to outlive the lock.
        gcx = Executor(endpoint)
        try:
            gcx.serializer = ComputeSerializer(strategy_code=CombinedCode())
            function_id = gcx.register_function(workflow, description="run")
            future = gcx.submit_to_registered_function(
                function_id=function_id, args=(args,))
        except Exception:
            _shutdown_executor(gcx)
            raise
        return gcx, future

    def _wait(submitted: tuple) -> dict:
        gcx, future = submitted
        try:
            try:
                return future.result(timeout=GLOBUS_RESULT_TIMEOUT_S)
            except TimeoutError as e:
                raise TimeoutError(
                    f"globus-task: Globus Compute task on endpoint {name!r} "
                    f"exceeded {GLOBUS_RESULT_TIMEOUT_S:.0f}s waiting for a "
                    f"result; will retry"
                ) from e
        except TimeoutError:
            raise
        except (GlobusEndpointOfflineError, GlobusRetryableError):
            raise
        except Exception as e:
            raise _tagged_exception(e, "task") from e
        finally:
            _shutdown_executor(gcx)

    return _acquire_submit_release_wait(kind, poll_key, _submit, _wait)
