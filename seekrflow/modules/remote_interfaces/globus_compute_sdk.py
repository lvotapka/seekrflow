"""
modules/remote_interfaces/globus_compute_sdk.py

Provide workflow submission with globus compute SDK.
"""

from __future__ import annotations

import time
import typing
import threading

# Soft wait for a submitted Globus Compute future. On expiry, raise TimeoutError
# so callers can retry on the next poll without treating it as a hard failure.
GLOBUS_RESULT_TIMEOUT_S = 120.0

# Dedupe endpoint health warnings: same (endpoint, warn_key) at most once per
# this many seconds; a different warn_key prints immediately.
_ENDPOINT_WARN_INTERVAL_S = 300.0
_last_endpoint_warn: dict[str, tuple[str, float]] = {}

# One Client per process, and registered function IDs keyed by
# (endpoint, workload). Submits run from worker threads, so guard both.
_client = None
_function_ids: dict[tuple[str, str], str] = {}
_globus_lock = threading.Lock()

def _get_client():
    """
    If the Globus client has already been created, return it, otherwise
    make a new Globus client.
    """
    global _client
    from globus_compute_sdk import Client
    with _globus_lock:
        if _client is None:
            _client = Client()
        return _client

def _workload_key(workload: typing.Any) -> str:
    """
    Generate a key from the workload manager class module and qualname.
    """
    return f"{workload.__module__}.{workload.__qualname__}"

def _get_function_id(
        gcx, 
        endpoint: str, 
        workload: typing.Any
        ) -> str:
    """
    Register ``workload`` once per process and endpoint, then reuse the ID.
    """
    key = (endpoint, _workload_key(workload))
    with _globus_lock:
        function_id = _function_ids.get(key)
    if function_id is None:
        function_id = gcx.register_function(workload, description="run")
        with _globus_lock:
            _function_ids[key] = function_id
    return function_id

def _forget_function_id(endpoint: str, workload: typing.Any) -> None:
    with _globus_lock:
        _function_ids.pop((endpoint, _workload_key(workload)), None)

def _warn_endpoint(
        name: str, 
        warn_key: str, 
        message: str) -> None:
    """
    Make a warning once, but don't keep repeating it over and over.
    """
    now = time.monotonic()
    previous = _last_endpoint_warn.get(name)
    if (
            previous is not None
            and previous[0] == warn_key
            and (now - previous[1]) < _ENDPOINT_WARN_INTERVAL_S
    ):
        return
    _last_endpoint_warn[name] = (warn_key, now)
    # TODO: figure out how to log this sort of warning - or print to GUI
    print(message)


def submit_remote_workload_with_globus_compute(
        resource_name: str,
        workload: typing.Any,
        endpoint: str,
        manager_payload: dict,
        silent: bool = False,
        ) -> dict:
    from globus_compute_sdk import Executor
    from globus_compute_sdk.serialize import ComputeSerializer, CombinedCode
    c = _get_client()
    status = c.get_endpoint_status(endpoint)
    s = (status.get("status") or "unknown").lower()
    d = status.get("details") or {}
    idle = d.get("idle_workers")
    total = d.get("total_workers")
    pending = d.get("pending_tasks") or d.get("outstanding_tasks")
    if not silent:
        if s == "online":
            if total is not None and idle == 0:
                _warn_endpoint(
                    resource_name,
                    "online_idle0",
                    f"WARNING: Globus endpoint '{resource_name}' is ONLINE but currently "
                    f"has 0 idle workers (total_workers={total}, "
                    f"pending_tasks={pending or 0}). Your task will queue until "
                    f"workers become available.",
                )
        elif s in {"offline", "disconnected", "stopped"}:
            _warn_endpoint(
                resource_name,
                f"state_{s}",
                f"WARNING: Globus endpoint '{resource_name}' is {s.upper()}. Start it "
                f"on the host: `globus-compute-endpoint start <ENDPOINT_NAME>`.",
            )
        elif s in {"initializing", "starting"}:
            _warn_endpoint(
                resource_name,
                f"state_{s}",
                f"Globus endpoint '{resource_name}' is STARTING; tasks will queue until "
                f"workers connect.",
            )
        else:
            _warn_endpoint(
                resource_name,
                f"state_{s}",
                f"WARNING: Globus endpoint '{resource_name}' is in an UNKNOWN state. "
                f"Jobs will not run.",
            )
    
    args = [manager_payload]
    def submit_once() -> dict:
        gcx = Executor(endpoint, client=c)
        try:
            gcx.serializer = ComputeSerializer(strategy_code=CombinedCode())
            function_id = _get_function_id(gcx, endpoint, workload)
            future = gcx.submit_to_registered_function(
                function_id=function_id, args=(args,))
            try:
                return future.result(timeout=GLOBUS_RESULT_TIMEOUT_S)
            except TimeoutError as e:
                raise TimeoutError(
                    f"Globus Compute task on endpoint {resource_name!r} "
                    f"exceeded {GLOBUS_RESULT_TIMEOUT_S:.0f}s waiting for a "
                    f"result; will retry on the next poll"
                ) from e
        except Exception as error:
            # A stale or deleted function ID: re-register on the next try.
            if getattr(error, "http_status", None) in (403, 404):
                _forget_function_id(endpoint, workload)
            raise
        finally:
            gcx.shutdown(wait=False, cancel_futures=True)

    return submit_once()
