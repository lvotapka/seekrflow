"""
modules/workload_managers/remote.py

An intermediate module between the seekrflow runner and the remote workload
managers (e.g., SLURM). This module handles common functionality needed for
remote workload management.
"""

import os
import typing
import pathlib

import seekrflow.modules.structures as structures
import seekrflow.modules.workload_managers.local as workload_local
import seekrflow.modules.workload_managers.slurm_pbs as workload_slurm_pbs
import seekrflow.modules.workload_managers.aws as workload_aws
import seekrflow.modules.workload_managers.dispatch_lowering as dispatch_lowering
#import seekrflow.modules.workload_managers.remote_stage_lifecycle as remote_stage_lifecycle
import seekrflow.modules.remote_interfaces.globus_compute_sdk as remote_globus
import seekrflow.modules.remote_interfaces.ssh as remote_ssh
import seekrflow.modules.remote_interfaces.local_shell as remote_local_shell

# TODO: keep as-is
def resource_kind(resource: structures.Resource_base | None) -> str:
    """Return ``local``, ``remote``, or ``cloud`` for a resource object."""
    if resource is None:
        return "local"
    if resource.type in ("slurm_remote", "pbs_remote"):
        return "remote"
    if resource.type in ("aws_cloud", ):
        return "cloud"
    return "local"

# TODO: keep as-is
def resolve_model_directory(
        seekrflow: structures.Seekrflow,
        resource: structures.Resource_base,
        ) -> str:
    """
    Return the model root used by remote SLURM/PBS workflows or local_shell.
    """
    if resource.remote_interface.type == "local_shell":
        return str(seekrflow.get_root_directory())
    return os.path.join(resource.remote_working_directory, seekrflow.name)

def _get_stage_resource_name(
        seekrflow: structures.Seekrflow,
        stage_name: str,
        ) -> str:
    """
    Resolve the configured resource name for a stage via placement policies.
    """
    return seekrflow.run_settings.resolve_stage_execution(
        stage_name, seekrflow.workflow.procedure).resource_name

# TODO: this is going to change because what has returned will be different
# from before.
def _normalize_stage_status(
        status_result: dict,
        ) -> dict:
    """
    Normalize stage status payload to align with local status contract:
    finished(bool), state(str), progress(float), notes(str).
    """
    stage_status = dict(status_result.get("stage_status") or {})
    if len(stage_status) == 0:
        stage_status = {
            "finished": False,
            "state": "unknown",
            "progress": 0.0,
            "notes": "",
        }

    finished = bool(stage_status.get("finished", False))
    notes = str(stage_status.get("notes", ""))

    progress = stage_status.get("progress", 0.0)
    
    state = stage_status.get("state")
    if state is None:
        if finished:
            state = "completed"
        else:
            manager_status = status_result.get("manager_status") or {}
            jobs = manager_status.get("jobs") or []
            if len(jobs) > 0:
                state = "started"
            else:
                state = "unstarted"

    stage_status["finished"] = finished
    stage_status["state"] = state
    stage_status["progress"] = progress
    stage_status["notes"] = notes
    return stage_status

def submit_workload(
        resource: structures.Resource_remote_base,
        workload: typing.Any,
        payload: dict,
        silent: bool = False,
        ) -> dict:
    """
    Submit a workflow to a remote resource.
    """
    # TODO: figure out what to put into 'args'
    if resource.remote_interface.type == "globus_compute_sdk":
        endpoint = resource.remote_interface.endpoint_id
        result = remote_globus\
            .submit_remote_workload_with_globus_compute(
                resource.name, workload, endpoint, payload, silent)
    elif resource.remote_interface.type == "ssh":
        hostname = resource.remote_interface.hostname
        username = resource.remote_interface.username
        password = resource.remote_interface.password
        port = resource.remote_interface.port
        private_key_filename = resource.remote_interface.private_key_filename
        private_key_passphrase = resource.remote_interface.private_key_passphrase
        result = remote_ssh\
            .submit_remote_workload_with_ssh(
                resource.name, workload, payload,  
                hostname, username, password, port,
                private_key_filename, private_key_passphrase)
    # TODO: add ORBIT
    elif resource.remote_interface.type == "local_shell":
        result = remote_local_shell\
            .submit_remote_workflow_with_local_shell(
                resource.name, workload, payload,
                python_executable=resource.remote_interface.python_executable,
                silent=silent)
    else:
        raise NotImplementedError(
            f"Remote interface type not implemented: "\
            f"{resource.remote_interface.type}")
    
    return result

# NOTE: used in fetch_unit_counts
def run_python_snippet_workload(args):
    """Run a python snippet on the remote worker; return raw stdout/stderr.
    Args: [root_dir, snippet, worker_init]. Stdlib only (no seekrflow)."""
    import shlex, pathlib, subprocess
    root_dir, snippet = args[0], args[1]
    worker_init = args[2] if len(args) > 2 else ""
    init = (worker_init or "").strip()
    cmd = (f"{init}; python -c {shlex.quote(snippet)}" if init
           else f"python -c {shlex.quote(snippet)}")
    try:
        proc = subprocess.run(["bash", "-lc", cmd],
                              cwd=str(pathlib.Path(root_dir)),
                              capture_output=True, text=True, timeout=1200) #120)
        return {"success": True, "error": None, "stdout": proc.stdout,
                "stderr": proc.stderr, "returncode": proc.returncode}
    except Exception as e:
        return {"success": False, "error": str(e), "stdout": "", "stderr": ""}

# TODO: this function is used to obtain numbers of swarms, and maybe anchors,
# after a previous generating stage has finished. Could this be replaced by 
# Stage Info objects pulled before the stage needing them? Don't think so.
def fetch_unit_counts(
        seekrflow: structures.Seekrflow,
        launching_stage: typing.Any,
        resource: structures.Resource_remote_base,
        silent: bool = False,
        ) -> dispatch_lowering.StageUnitCounts:
    """
    Query seekr ``info`` on a remote worker for launch-time unit enumeration.
    """
    if resource.type not in ("slurm_remote", "pbs_remote"):
        raise NotImplementedError(
            f"Resource type {resource.type} is not implemented.")

    launching_stage_index = getattr(
        launching_stage, "index", launching_stage.name)
    snippet = dispatch_lowering.build_info_fetch_snippet(
        "model.json",
        launching_stage.name,
        launching_stage_index,
    )
    extra_args = [
        snippet,
        getattr(resource, "worker_init", "") or "",
    ]

    result = submit_workload(
        resource,
        run_python_snippet_workload,
        silent=silent,
        kind="submit",
    )
    if not result.get("success"):
        raise RuntimeError(
            f"Remote unit-count fetch failed: {result.get('error')}")
    return dispatch_lowering.parse_info_fetch_output(
        result.get("stdout", ""))

# TODO: add swarm indices/anchor indices or get from dispatch?
def submit_job(
        seekrflow: structures.Seekrflow,
        resource: typing.Any,
        stage_names: list[str],
        job_specs: list[dict],
        resolved_execution: structures.Resolved_execution | None = None,
        silent: bool = False,
        ) -> dict:
    """
    Run the stage on a local, remote, or cloud resource.
    """
    destination_path = resolve_model_directory(seekrflow, resource)
    job_name = seekrflow.name + "_" + stage_names[0]
    kind = resource_kind(resource)
    if resolved_execution is not None:
        cpus = (
            resolved_execution.cpus
            if resolved_execution.cpus is not None
            else resource.cpus_per_task)
        memory_mb = (
            resolved_execution.memory_mb
            if resolved_execution.memory_mb is not None
            else resource.memory_per_node)
        time_limit = (
            resolved_execution.time_limit
            if resolved_execution.time_limit is not None
            else resource.time_limit)
    else:
        cpus = resource.cpus_per_task
        memory_mb = resource.memory_per_node
        time_limit = resource.time_limit
    # Pass the contents of job.py and structures.py to the remote worker.
    _JOB_DIR = pathlib.Path(seekrflow.__file__).resolve().parent / "modules" / "job"
    job_py_source = (_JOB_DIR / "job.py").read_text()
    structures_py_source = (_JOB_DIR / "structures.py").read_text()

    manager_payload = {
        "root_dir": destination_path,
        "resource_payload": None,
        "job_specs": job_specs,
        "runner_files": {
            "job.py": job_py_source,
            "structures.py": structures_py_source,
        }
    }
        
    if resource.type == "local":
        run_workload = workload_local.local_run_workload
        resource_payload = {
            "cpus": cpus,
        }
    elif resource.type == "slurm_remote":
        resource_payload = {
            "scheduler": "slurm",
            "partition_queue": resource.partition,
            "account": resource.account,
            "constraint": resource.constraint,
            "job_name": job_name,
            "cpus": cpus,
            "memory_mb": memory_mb,
            "time_limit": time_limit,
            "scheduler_options": resource.scheduler_options,
            "worker_init": resource.worker_init,
        }
        run_workload = workload_slurm_pbs.slurm_pbs_run_workload
    elif resource.type == "pbs_remote":
        resource_payload = {
            "scheduler": "pbs",
            "partition_queue": resource.queue,
            "account": resource.account,
            "constraint": resource.constraint,
            "job_name": job_name,
            "cpus": cpus,
            "memory_mb": memory_mb,
            "time_limit": time_limit,
            "scheduler_options": resource.scheduler_options,
            "worker_init": resource.worker_init,
        }
        run_workload = workload_slurm_pbs.slurm_pbs_run_workload
    elif resource.type == "aws_cloud":
    else:
        raise NotImplementedError(
            f"Resource type {resource.type} is not implemented.")
    
    manager_payload["resource_payload"] = resource_payload
    result = submit_workload(
        resource, run_workload, manager_payload, silent)
    return result

def status(
        resource: structures.Resource_base | None,
        manager_payload: dict,
        silent: bool = False,
        ) -> dict:
    """
    Generalized remote/cloud stage status query for any stage whose producing
    procedure is configured in run_settings.placements.
    """
    
    if resource is None:
        raise NotImplementedError("Local status is not implemented.")
    elif resource.type in ("slurm_remote", "pbs_remote"):
        status_workflow = workload_slurm_pbs.slurm_pbs_status_workload
        manager_payload["scheduler"] = resource.type
    else:
        raise NotImplementedError(f"Resource type {resource.type} not implemented")
    result = submit_workload(resource, status_workflow, payload=manager_payload, 
                             silent=silent)

    # Normalize stage_status regardless of success so callers always have
    # manager_status / job list available (e.g. for display while seekr module
    # is unavailable in the status-check environment).
    # TODO: remove?
    result["stage_status"] = _normalize_stage_status(result)
    return result

def submit_cancel_workload(
        resource: structures.Resource_base,
        manager_payload: dict,
        silent: bool = False
    ) -> None:
    """
    Cancel a remote or cloud stage job.
    """
    if resource.type in ("slurm_remote", "pbs_remote"):
        cancel_workload = workload_slurm_pbs.slurm_pbs_cancel_workload
    
    submit_workload(
        resource, cancel_workload, payload=manager_payload, silent=silent, kind="cancel")
    return

# TODO: why don't we use cancel and reset all the time?
#  answer: we don't use the one above, but we need a way
#  to obtain the job id or name to cancel from the stage name(s)
#  and the internal id. 
def cancel_and_reset_stage(
        resource: structures.Resource_base,
        manager_payload: dict,
        silent: bool = False,
        model_directory: str | None = None, # TODO: see if really needed by AWS
        #monitor_stage_names: list[str] | None = None, # TODO: remove in favor of stage-names
        ) -> None:
    """
    Cancel a stage's remote/cloud job and reset its scheduler/runner bookkeeping.
    """
    assert len(stage_names) > 0
    resource_name = _get_stage_resource_name(seekrflow, stage_names[0])
    if resource_name == "local":
        raise ValueError(f"Stage {stage_names[0]} is configured to run locally, not remotely")

    print(f"Canceling and resetting remote stage: {stage_names[0]} on resource {resource_name}")
    resource = seekrflow.run_settings.get_resource_by_name(resource_name)
    if resource.type == "aws_cloud":
        if model_directory is None:
            raise ValueError(
                "model_directory is required to reset an aws_cloud stage")
        result = workload_aws.cancel_and_reset_aws_stage(
            seekrflow,
            stage_names[0], # TODO: fix the host stage paradigm for AWS
            resource,
            model_directory,
            monitor_stage_names=stage_names,
        )
        if result.get("canceled_job"):
            print(f"  Canceled job: {result['canceled_job']}")
        cleared = result.get("cleared_s3_keys") or []
        if cleared:
            print(f"  Cleared {len(cleared)} S3 monitor object(s) for force-rerun")
        print(f"  Cleared runner state for {stage_names[0]} stage")
        return
    elif resource.type == "slurm_remote":
        # TODO: lump SLURM with PBS? (DRY)
        cancel_reset_workflow = workload_slurm.slurm_remote_force_rerun_workflow
    elif resource.type == "pbs_remote":
        cancel_reset_workflow = workload_pbs.pbs_remote_force_rerun_workflow
    else:
        raise NotImplementedError(f"Resource type {resource.type} not implemented")
    
    remove_json_files = False # because a previous stage might not be force-overwritten
    #  and it's state needs to be preserved.
    result = submit_workload(
        resource_name, cancel_reset_workflow,
        extra_args=[job_id, job_name, remove_json_files], silent=silent)

    if result.get("canceled_job"):
        print(f"  Canceled job: {result['canceled_job']}")

    print(f"  Cleared runner state for job name: {job_name}.")
    return
