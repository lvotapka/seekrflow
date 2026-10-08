"""
modules/client/status_and_cancel.py:

Report a quick status of detached jobs and/or stop them.
"""

import json

import seekrflow.modules.workload_managers.remote_local as workload_remote_local


def _detached_records(session):
    """Yield (systemrun, snapshot record) from the last output file."""
    try:
        with open(session.output_file) as f:
            session_statuses = json.load(f)
    except (OSError, json.JSONDecodeError) as error:
        print(f"[detached] cannot read {session.output_file}: {error}")
        return
    by_name = {systemrun.name: systemrun for systemrun in session.systemrun_objects}
    for system_name, system_status in session_statuses.items():
        systemrun = by_name.get(system_name)
        if systemrun is None:
            continue
        for record in (system_status.get("stage_workflows") or {}).values():
            yield systemrun, record

def _resource_for_record(systemrun, record):
    resource_name = record.get("resource_name") or "local"
    return systemrun.seekrflow.run_settings.get_resource_by_name(resource_name)

def report_detached_status(session) -> None:
    """One live status call per resource for jobs recorded in the output file."""
    payloads = {}
    resources = {}
    pending = []
    for systemrun, record in _detached_records(session):
        names = ", ".join((record.get("stages") or {}))
        if not record.get("job_id"):
            print(f"{systemrun.name} [{names}]: never submitted")
            continue
        resource = _resource_for_record(systemrun, record)
        resource_name = resource.name
        resources[resource_name] = resource
        system_name = systemrun.seekrflow.name
        payload = payloads.setdefault(resource_name, {"system_payloads": {}})
        system_payload = payload["system_payloads"].setdefault(system_name, {})
        if "jobs" not in system_payload:
            system_payload["root_dir"] = workload_remote_local.resolve_model_directory(
                systemrun.seekrflow, resource)
            system_payload["jobs"] = []
        job = {
            "internal_id": record.get("internal_id"),
            "job_id": record.get("job_id"),
            "job_name": record.get("job_name"),
            "stage_indices": [
                stage.get("index") for stage in (record.get("stages") or {}).values()],
            "array_indices": record.get("array_indices") or [],
        }
        system_payload["jobs"].append(job)
        pending.append((system_name, names, job["job_id"]))
    found = {}
    for resource_name, payload in payloads.items():
        try:
            result = workload_remote_local.status(
                resources[resource_name], payload, silent=True)
        except Exception as error:
            print(f"[status] {resource_name}: {error}")
            continue
        for system_name, jobs_by_id in (result.get("payload") or {}).items():
            found.setdefault(system_name, {}).update(jobs_by_id or {})
    for system_name, names, job_id in pending:
        job = (found.get(system_name) or {}).get(job_id)
        if job is None:
            job = (found.get(system_name) or {}).get(str(job_id))
        if not job:
            print(f"{system_name} [{names}]: no scheduler record for {job_id}")
            continue
        states = {
            entry.get("state")
            for entry in (job.get("manager_dicts_by_array_index") or {}).values()}
        print(f"{system_name} [{names}]: {', '.join(sorted(states)) or 'idle'}")
        for stage_index, stage in (job.get("stage_dicts_by_stage_index") or {}).items():
            print(
                f"  stage {stage_index}: {stage.get('state')} "
                f"finished={bool(stage.get('finished'))}")

def _cancel_payload(seekrflow, resource, job_id, job_name) -> dict:
    return {
        "remove_json_files": False,
        "system_payloads": {
            seekrflow.name: {
                "root_dir": workload_remote_local.resolve_model_directory(
                    seekrflow, resource),
                "jobs": [{"job_id": job_id, "job_name": job_name}],
            },
        },
    }

def stop_detached_jobs(session) -> None:
    """Cancel every job id recorded in the output file. Leave that file unchanged."""
    for systemrun, record in _detached_records(session):
        job_id = record.get("job_id")
        names = ", ".join((record.get("stages") or {}))
        if not job_id:
            print(f"{systemrun.name} [{names}]: never submitted")
            continue
        resource = _resource_for_record(systemrun, record)
        try:
            workload_remote_local.submit_cancel_workload(
                resource,
                _cancel_payload(
                    systemrun.seekrflow, resource, job_id, record.get("job_name")),
                silent=True,
            )
        except Exception as error:
            print(f"[stop] {systemrun.name} [{names}]: {error}")
            continue
        print(f"{systemrun.name} [{names}]: cancelled {job_id}")