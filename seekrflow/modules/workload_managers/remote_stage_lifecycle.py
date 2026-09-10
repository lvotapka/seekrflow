"""
Pure helpers for remote stage launch probing and scheduler naming.

Kept separate from seekr_run so unit tests do not require radical.asyncflow.
"""
from __future__ import annotations


# TODO: if this is used, spread to other places where job names are needed/assigned.
def remote_scheduler_job_name(seekrflow_name: str, stage_name: str) -> str:
    """Workload-manager job name for a stage launch."""
    return f"{seekrflow_name}_{stage_name}"

# TODO: remove or perhaps revise - status result not the same anymore
def remote_model_missing(status: dict | None) -> bool:
    """
    True when the remote workdir has no model.json.

    That payload is not an authoritative stage state (the copy may still
    be in flight), but it is a reliable signal that outbound transfer
    never landed — leftover job ids must not skip the copy forever.
    """
    if not isinstance(status, dict):
        return False
    stage_status = status.get("stage_status") or {}
    if stage_status.get("model_xml_found") is False:
        return True
    notes = str(stage_status.get("notes") or status.get("error") or "")
    return "model file not found" in notes.lower()


# TODO: status returns will be different now - remove?
def scheduler_queue_uncertain(status: dict | None) -> bool:
    """
    True when squeue/qstat was not a clean empty-or-live reading.

    A missing state file, a failed squeue, or a missing payload must not
    be treated as "no jobs" — that is how resume submits duplicates.
    """
    if status is None:
        return True
    mgr = status.get("manager_status")
    if not isinstance(mgr, dict):
        return True
    jobs = mgr.get("jobs") or []
    if jobs:
        return False
    return bool(str(mgr.get("error") or "").strip())


# TODO: status returns will be different now - remove?
def jobs_from_status(status: dict | None) -> list:
    if not isinstance(status, dict):
        return []
    mgr = status.get("manager_status")
    if not isinstance(mgr, dict):
        return []
    return list(mgr.get("jobs") or [])


def classify_remote_probe_status(
        status: dict | None,
        tracked_job_ids: set[str] | frozenset[str] | None = None,
        resume: bool = False,
        ) -> str:
    """
    Classify a remote status payload for fresh-launch probing.

    Returns one of ``completed``, ``reattach``, ``submit``, or ``defer``.

    Live jobs reattach. A clean empty squeue may submit even with leftover
    job ids (those jobs were cancelled or finished). An uncertain squeue
    (failed probe, manager error) never submits: leftover ids / resume
    reattach, and a first launch defers. Waiting does not eventually
    sbatch on top of a job we simply failed to see.
    """
    conservative = bool(tracked_job_ids) or resume
    jobs = jobs_from_status(status)
    if jobs:
        return "reattach"
    if status is None or scheduler_queue_uncertain(status):
        if conservative:
            return "reattach"
        return "defer"
    stage_status = status.get("stage_status") or {}
    missing_model = remote_model_missing(status)
    if stage_status.get("state") == "completed" and not missing_model:
        return "completed"
    return "submit"


def force_overwrite_skips_launch_probe(force_overwrite: bool) -> bool:
    """
    When True, skip remote/cloud launch probing so a force-rerun can submit.

    Probe would otherwise treat a prior completed status as skip-submit and
    never reach cancel/reset + seekr ``force_overwrite``.
    """
    return bool(force_overwrite)


def owns_scheduler_job(fusion_host: str | None) -> bool:
    """True when this stage may own a scheduler submission (not a fused member)."""
    return fusion_host is None


def remote_cancel_needed(
        status: dict | None,
        *,
        local_state: str,
        tracked_job_ids: set[str] | frozenset[str],
        ) -> bool:
    """
    Whether shutdown should attempt scheduler cancellation for this stage.

    Cancel only when the fresh status shows jobs in the queue, or when the
    status probe failed and we still believe a job may be active locally.
    Completed/failed/idle stages with an empty queue are skipped even if
    ``tracked_job_ids`` contains stale ids.
    """
    jobs = ((status or {}).get("manager_status") or {}).get("jobs") or []
    if jobs:
        return True
    if status is None:
        return local_state == "started" or bool(tracked_job_ids)
    return False
