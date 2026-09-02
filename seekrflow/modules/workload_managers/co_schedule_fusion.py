"""
Pure logic for co_schedule_with job-script fusion.
"""
from __future__ import annotations

import typing

from seekrflow.modules.workload_managers import remote_stage_lifecycle

if typing.TYPE_CHECKING:
    from seekrflow.modules.structures import Resource_remote_base

def combine_fused_commands(command_strings: list[str]) -> str:
    """Join per-stage remote commands, short-circuiting on failure."""
    fused_commands = " && ".join(f"({cmd})" for cmd in command_strings)
    return fused_commands


def _resource_supports_fusion(
        resource: typing.Any,
        ) -> bool:
    return resource is not None and resource.type in (
        "slurm_remote", "pbs_remote", "aws_cloud")


def populate_fusion_map(
        stage_workflows: list,
        benchmark_stage: str | None = None,
        ) -> None:
    """
    Wire co-scheduled stages into host fused_before / fused_after lists.
    Remote-only; local co_schedule_with is a no-op.
    """
    # stage_workflow_index_map: key - stage name, value - stage_workflow index
    stage_workflow_index_map = {}
    # upstream_stage_map: key - stage name, value - upstream stage name
    upstream_stage_map = {}
    # host_map: key - stage_name, value - host stage name
    host_map = {}

    for i, sw in enumerate(stage_workflows):
        if sw.resolved_execution is not None:
            sw.co_schedule_with = sw.resolved_execution.co_schedule_with
        else:
            sw.co_schedule_with = None
        stage_workflow_index_map[sw.stage.name] = i

    for sw in stage_workflows:
        co = sw.co_schedule_with
        if not co:
            continue
        #if sw.benchmark_mode or sw.stage.name == benchmark_stage:
        #    continue
        if sw.resource_name == "local":
            continue
        if not _resource_supports_fusion(sw.resource):
            continue
        #if sw.fused_before or sw.fused_after:
        #    raise ValueError(
        #        f"Stage {sw.stage.name!r} cannot be co-scheduled because it "
        #        f"already hosts fused stages.")

        if co == "predecessor":
            upstream_idx = getattr(sw.stage, "input_stage_index", 0)
            if upstream_idx <= 0:
                continue
            upstream_sw = next(
                (candidate for candidate in stage_workflows
                 if candidate.stage.index == upstream_idx),
                None,
            )
            if upstream_sw is None:
                continue
            upstream_stage_map[sw.stage.name] = upstream_sw.stage.name

        else:
            fused_idx = stage_workflows.index(sw)
            upstream_sw = next(
                (candidate for candidate in stage_workflows
                 if getattr(candidate.stage, "input_stage_index", 0) - 1
                 == fused_idx),
                None,
            )
            if upstream_sw is None:
                continue
            upstream_stage_map[sw.stage.name] = upstream_sw.stage.name

        for stage_name in upstream_stage_map:
            visited_indices_backstop = set()
            starting_index = stage_workflow_index_map[stage_name]
            visited_indices_backstop.add(starting_index)
            cur_upstream_name = upstream_stage_map[stage_name]
            counter = 0
            while cur_upstream_name is not None:
                host_map[stage_name] = cur_upstream_name
                next_index = stage_workflow_index_map[cur_upstream_name]
                next_name = stage_workflows[next_index].stage.name
                cur_upstream_name = upstream_stage_map.get(next_name, None)
                if next_index in visited_indices_backstop:
                    raise Exception("Infinite loop detected. Is there a cycle in the "\
                                    "co-schedule definition?")
                visited_indices_backstop.add(next_index)

    for stage_name, host_name in host_map.items():
        stage_index = stage_workflow_index_map[stage_name]
        sw = stage_workflows[stage_index]
        sw.fusion_host = host_name
        co = sw.co_schedule_with
        host_index = stage_workflow_index_map[host_name]
        host_sw = stage_workflows[host_index]
        if co == "predecessor":
            host_sw.fused_after.append(sw.stage.name)
        else:
            host_sw.fused_before.append(sw.stage.name)
        
def fusion_dependencies_satisfied(
        stage_workflow,
        stage_by_name: dict,
        ) -> bool:
    """Fused stages wait for the host job to be submitted, not host completion."""
    host_name = stage_workflow.fusion_host
    if host_name is None:
        return True
    host_sw = stage_by_name.get(host_name)
    if host_sw is None:
        return False
    if host_sw.state in ("started", "completed"):
        return True
    return host_sw.manager_status in {
        "running", "queued", "running/queued",
    }


def skips_remote_submit(stage_workflow) -> bool:
    return (
        stage_workflow.fusion_host is not None
        and stage_workflow.resource_name != "local"
    )


def is_fusion_host(stage_workflow) -> bool:
    """Remote stage that owns a fused job (submits the combined command)."""
    return (
        stage_workflow.fusion_host is None
        and stage_workflow.resource_name != "local"
        and bool(stage_workflow.fused_before or stage_workflow.fused_after)
    )


def is_fusion_member(stage_workflow) -> bool:
    """Remote stage fused into a host job (monitor-only submit)."""
    return (
        stage_workflow.fusion_host is not None
        and stage_workflow.resource_name != "local"
    )


def is_in_fused_set(stage_workflow) -> bool:
    return is_fusion_host(stage_workflow) or is_fusion_member(stage_workflow)


def fusion_host_name(stage_workflow) -> str | None:
    """Name of the root host for a fused set, or None if standalone."""
    if is_fusion_member(stage_workflow):
        return stage_workflow.fusion_host
    if is_fusion_host(stage_workflow):
        return stage_workflow.stage.name
    return None


POLLED_SCHEDULER_STATUSES = frozenset({
    "queued", "running", "running/queued",
})


def copy_host_scheduler_onto_member(member_sw, host_sw) -> None:
    """
    Fused members have no SLURM/PBS state file of their own. Copy the host
    job's queued/running label so monitors and the batch table describe the
    job that is actually on the scheduler.
    """
    if host_sw is None:
        return
    host_mgr = getattr(host_sw, "manager_status", None)
    if host_mgr not in POLLED_SCHEDULER_STATUSES:
        return
    member_jobs = getattr(member_sw, "job_ids", None) or set()
    member_mgr = getattr(member_sw, "manager_status", None)
    if member_jobs or member_mgr in POLLED_SCHEDULER_STATUSES:
        return
    member_sw.manager_status = host_mgr
    if getattr(member_sw, "status_polled_at", None) is None:
        host_polled = getattr(host_sw, "status_polled_at", None)
        if host_polled is not None:
            member_sw.status_polled_at = host_polled


def apply_host_scheduler_status_to_fused_members(
        stages: dict,
        stage_workflows: list,
        ) -> None:
    """
    Mutate pipeline snapshot dicts so fused members show the host squeue
    label when they have no jobs of their own.
    """
    for sw in stage_workflows:
        if not is_fusion_member(sw):
            continue
        host_name = sw.fusion_host
        host_info = stages.get(host_name) if host_name else None
        member_info = stages.get(sw.stage.name)
        if not isinstance(host_info, dict) or not isinstance(member_info, dict):
            continue
        host_mgr = str(host_info.get("manager_status") or "")
        if host_mgr not in POLLED_SCHEDULER_STATUSES:
            continue
        member_jobs = member_info.get("job_ids") or []
        member_mgr = str(member_info.get("manager_status") or "")
        if member_jobs or member_mgr in POLLED_SCHEDULER_STATUSES:
            continue
        member_info["manager_status"] = host_mgr
        if not member_info.get("status_polled_at"):
            host_polled = host_info.get("status_polled_at")
            if host_polled:
                member_info["status_polled_at"] = host_polled


def fused_set_members(
        host_stage_workflow: typing.Any,
        ) -> list[str]:
    return (
        list(host_stage_workflow.fused_before)
        + [host_stage_workflow.stage.name]
        + list(host_stage_workflow.fused_after)
    )


def _stage_counts_for_progress(sw: typing.Any) -> bool:
    """Exclude cheap logistic / co-scheduled stages from set progress."""
    if getattr(sw.stage, "scale_type", None) == "logistic":
        return False
    if sw.co_schedule_with is not None:
        return False
    return True


def fused_set_completed(
        host_stage_workflow_name: str,
        stage_by_name: dict,
        ) -> bool:
    host_stage_workflow = stage_by_name[host_stage_workflow_name]
    for stage_name in fused_set_members(host_stage_workflow):
        sw = stage_by_name[stage_name]
        if sw.state != "completed":
            return False
    return True


def fused_set_progress(
        host_stage_workflow_name: str,
        stage_by_name: dict,
        ) -> float:
    total_progress = 0.0
    normalize = 0.0
    host_stage_workflow = stage_by_name[host_stage_workflow_name]
    for stage_name in fused_set_members(host_stage_workflow):
        sw = stage_by_name[stage_name]
        if _stage_counts_for_progress(sw):
            total_progress += sw.progress
            normalize += 1.0
    if normalize == 0.0:
        return 0.0
    return total_progress / normalize


def fused_set_completion_fraction(
        host_stage_workflow_name: str,
        stage_by_name: dict,
        ) -> float:
    """
    Fraction of fused-set members in state ``completed``.

    Includes logistic / ``co_schedule_with`` members, unlike
    ``fused_set_progress``. Idle-incomplete resubmit uses this so a finished
    host with an unfinished lumped tail still counts as incomplete work.
    """
    host_stage_workflow = stage_by_name[host_stage_workflow_name]
    members = fused_set_members(host_stage_workflow)
    if not members:
        return 0.0
    done = 0
    for stage_name in members:
        sw = stage_by_name.get(stage_name)
        if sw is not None and sw.state == "completed":
            done += 1
    return done / float(len(members))


def mark_fused_set_completed(
        host_stage_workflow: typing.Any,
        stage_by_name: dict,
        ) -> None:
    """Mark every member of a fused set completed (probe short-circuit)."""
    for stage_name in fused_set_members(host_stage_workflow):
        sw = stage_by_name[stage_name]
        sw.state = "completed"
        sw.progress = 1.0


def classify_fused_probe_status(
        member_statuses: dict[str, dict | None],
        host_stage_name: str,
        tracked_job_ids: set[str] | frozenset[str] | None = None,
        resume: bool = False,
        ) -> str:
    """
    Classify a one-shot probe over all fused-set members.

    Returns ``completed``, ``reattach``, ``submit``, or ``defer``.

    Live jobs on any member reattach. Fused members often have no SLURM
    state file, so only the **host** queue reading can be uncertain.
    A failed or erroring host probe never submits (reattach if leftover
    ids / resume, else defer). A clean empty host squeue may submit so
    cancelled jobs can be replaced.
    """
    conservative = bool(tracked_job_ids) or resume
    any_jobs = False
    all_completed = True
    for status in member_statuses.values():
        jobs = remote_stage_lifecycle.jobs_from_status(status)
        if jobs:
            any_jobs = True
        if status is None:
            all_completed = False
            continue
        stage_status = status.get("stage_status") or {}
        if stage_status.get("state") != "completed":
            all_completed = False
    if any_jobs:
        return "reattach"
    host_status = member_statuses.get(host_stage_name)
    if (host_status is None
            or remote_stage_lifecycle.scheduler_queue_uncertain(host_status)):
        if conservative:
            return "reattach"
        return "defer"
    if all_completed and member_statuses:
        return "completed"
    return "submit"