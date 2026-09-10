"""
SLURM workload manager functions for remote job submission, status checking, 
and cancellation.
"""

# TODO: this will be totally rewritten, because we will not want to execute much
# code on the endpoint worker, and this will actually be simply retrieving file
# contents or something that is being actively written by a subprocess within 
# the submitted job as the simulations run, and that is running status.py
# periodically.

# NOTE: notice that this was changed from 'workflow' to 'workload'
# TODO: need to handle the job outputs for multi-anchor,multi-swarm jobs,
#  not just output files, but also how to track the progress of the job.
# TODO: combine with PBS to make it a single module. (DRY)
def slurm_remote_run_workload(args):
    """
    Submit a remote workload to SLURM.
    """
    import time
    import json
    import shlex
    import pathlib
    import subprocess
    from typing import List

    root_dir = args[0]
    partition = args[1]
    account = args[2]
    constraint = args[3]
    job_name = args[4]
    cpus_per_task = args[5]
    memory_mb_per_node = args[6]
    time_limit = args[7]
    scheduler_options = args[8]
    worker_init = args[9]
    command_string = args[10]
    array_indices = args[11]
    stage_indices = args[12] # The indices of the bundle of stages in this job
    anchor_indices = args[13]
    swarm_indices = args[14]
    benchmark_mode = args[15] # TODO: needed?
    if array_indices is None:
        n_tasks = 1
    else:
        n_tasks = len(array_indices)
    LOG_DIR = "logs"
    model_filename = root_dir_path / "model.json"
    STAGE_STATE_INTERVAL = 10

    # Define helper functions for this workload
    def run(cmd: List[str], check: bool = True) -> tuple:
        """Run command, return (rc, stdout, stderr)"""
        out = subprocess.run(cmd, stdout=subprocess.PIPE, 
                           stderr=subprocess.PIPE, text=True)
        if check and out.returncode != 0:
            raise RuntimeError(f"Command failed: ({out.returncode}): "
                             f"{' '.join(cmd)}\nSTDERR:\n{out.stderr}")
        return out.returncode, out.stdout.strip(), out.stderr.strip()

    def get_slurm_dir(root_dir: pathlib.Path) -> pathlib.Path:
        return root_dir / ".slurm_runner"

    def get_stage_dir(root_dir: pathlib.Path) -> pathlib.Path:
        return root_dir / ".stage_states"

    def get_internal_id():
        """
        Since job IDs aren't assigned until the job is submitted, we need a way to
        choose an internal, seekrflow-specific ID to use for Stage and Slurm Info
        and State files. This can just be where the slurm runner directory
        has its content file names split by underscore and the ID is after the first
        underscore or something.
        """
        slurm_runner_dir = get_slurm_dir(root_dir)
        if not slurm_runner_dir.exists():
            return 0
        files = sorted(
            slurm_runner_dir.glob("*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if files:
            ids = [file.stem.split("_")[-1] for file in files]
            return max(int(id) for id in ids) + 1
        return 0

    def collapse_indices(
            idxs: List[int]
            ) -> str:
        """
        Collapse [0,1,2,5,6,9] -> '0-2,5-6,9'
        """
        if not idxs: return ""
        idxs = sorted(set(idxs))
        ranges = []
        start = prev = idxs[0]
        for x in idxs[1:]:
            if x == prev + 1:
                prev = x
            else:
                ranges.append(f"{start}-{prev}" if start != prev else str(start))
                start = prev = x
        ranges.append(f"{start}-{prev}" if start != prev else str(start))
        return ",".join(ranges)

    # 1. perform preliminary checks: make sure model.json exists, fill out StageInfo 
    #    for each stage being submitted. Write out a preliminary SlurmInfo, 
    #    SlurmState, StageInfo, and StateState files.
    root_dir_path = pathlib.Path(root_dir)
    # NOTE: keeping asserts on worker - will be propagated back to local
    assert root_dir_path.exists(), f"Root directory does not exist: {root_dir_path}"
    assert root_dir_path.is_dir(), f"Root directory is not a directory: {root_dir_path}"
    assert model_filename.exists(), f"Model file does not exist: {model_filename}"
    slurm_dir_path = get_slurm_dir(root_dir_path)
    stage_dir_path = get_stage_dir(root_dir_path)
    internal_id = get_internal_id()
    
    log_dir_path = root_dir_path / LOG_DIR
    slurm_info_basename = f"slurm_info_{internal_id}.json"
    slurm_state_basename = f"slurm_state_{internal_id}.json"
    stage_info_filenames = []
    stage_state_filenames = []
    for stage_index in stage_indices:
        stage_info_filename = stage_dir_path / f"stage_info_{internal_id}_{stage_index}.json"
        stage_state_filename = stage_dir_path / f"stage_state_{internal_id}_{stage_index}.json"
        stage_info_filenames.append(stage_info_filename.name)
        stage_state_filenames.append(stage_state_filename.name)

    # Write out preliminary SlurmInfo, SlurmState, StageInfo, and StageState files
    starting_slurm_info_dict = {
        "internal_id": internal_id,
        "log_dir": LOG_DIR,
        "worker_init": worker_init,
        "partition": partition,
        "time_limit": time_limit,
        "account": account,
        "scheduler_options": scheduler_options,
        "constraint": constraint,
        "cpus_per_task": cpus_per_task,
        "memory_mb": memory_mb_per_node,
        "n_tasks": n_tasks,
        "array_indices": array_indices,
        "command_string": command_string,
        "benchmark_mode": benchmark_mode,
        "filename": slurm_info_basename,
        "slurm_state_filename": slurm_state_basename,
        "stage_info_filenames": stage_info_filenames,
        "stage_state_filenames": stage_state_filenames,
    }
    starting_slurm_info_json = json.dumps(starting_slurm_info_dict)
    starting_slurm_state_dict = {
        "internal_id": internal_id,
        "state": "pending",
        "filename": slurm_state_basename,
        "slurm_info_filename": slurm_info_basename,
    }
    starting_slurm_state_json = json.dumps(starting_slurm_state_dict)

    # Execute command to initialize slurm info and state files.
    slurm_job_command = f"python {slurm_dir_path}/slurm_job_monitor.py "\
        f"-r {root_dir} -I {internal_id} -i {starting_slurm_info_json} "\
        f"-s {starting_slurm_state_json}"
    run(["bash", "-lc", slurm_job_command])

    starting_stage_info_dicts = []
    starting_stage_state_dicts = []
    for i, stage_index in enumerate(stage_indices):
        starting_stage_info_dict = {
            "internal_id": internal_id,
            "stage_index": stage_index,
            "anchor_indices": anchor_indices,
            "swarm_indices": swarm_indices,
            "filename": stage_info_filenames[i],
            "slurm_info_filename": slurm_info_basename,
            "stage_state_filename": stage_state_filenames[i],
        }
        starting_stage_info_json = json.dumps(starting_stage_info_dict)
        starting_stage_state_dict = {
            "internal_id": internal_id,
            "state": "unknown",
            "finished": False,
            "progress": None,
            "notes": None,
            "filename": stage_state_filenames[i],
            "stage_info_filename": stage_info_filenames[i],
        }
        starting_stage_state_json = json.dumps(starting_stage_state_dict)
        stage_info_command = f"python {stage_dir_path}/remote_status_monitor.py "\
            f"-r {root_dir} -I {internal_id} -S {stage_index} "\
            f"-i {starting_stage_info_json} -s {starting_stage_state_json}"
        run(["bash", "-lc", stage_info_command])

    # 2. submit the job to SLURM
    if array_indices is None:
        stdout_basename = f"{job_name}_{internal_id}.out"
        stderr_basename = f"{job_name}_{internal_id}.err"
        slurm_args = ["sbatch",
                    "-J", job_name,
                    "-p", partition,
                    "-t", time_limit,
                    "-o", f"{log_dir_path}/{stdout_basename}",
                    "-e", f"{log_dir_path}/{stderr_basename}",
                    "-D", f"{root_dir_path}",
                    ]
        stdout_basenames = [stdout_basename]
        stderr_basenames = [stderr_basename]
    else:
        stdout_basename_slurm = f"{job_name}_{internal_id}_%a.out"
        stderr_basename_slurm = f"{job_name}_{internal_id}_%a.err"
        slurm_args = ["sbatch",
                    "-J", job_name,
                    "-p", partition,
                    "-t", time_limit,
                    "--array", collapse_indices(array_indices),
                    "-o", f"{log_dir_path}/{stdout_basename_slurm}",
                    "-e", f"{log_dir_path}/{stderr_basename_slurm}",
                    "-D", f"{root_dir_path}",
                    ]
        stdout_basenames = []
        stderr_basenames = []
        for array_index in array_indices:
            stdout_basename = f"{job_name}_{internal_id}_{array_index}.out"
            stderr_basename = f"{job_name}_{internal_id}_{array_index}.err"
            stdout_basenames.append(stdout_basename)
            stderr_basenames.append(stderr_basename)
    
    if scheduler_options: slurm_args += [scheduler_options]
    slurm_args += ["--cpus-per-task", str(cpus_per_task)]
    if memory_mb_per_node: slurm_args += ["--mem", str(memory_mb_per_node)]
    if account: slurm_args += ["--account", account]
    if constraint: slurm_args += ["--constraint", constraint]
    
    
    loop_str = ""
    for stage_index in stage_indices:
        loop_str += f"python {stage_dir_path}/remote_status_monitor.py "
        loop_str += f"-r {shlex.quote(str(root_dir))} -I {internal_id} "
        loop_str += f"-S {stage_index} || true; "

    before_cmd = (
    "(while true; do "
    f"{loop_str}"
    f"sleep {STAGE_STATE_INTERVAL}; "
    "done) >/dev/null 2>&1 & MONITOR_PID=$!")

    completed_json = json.dumps({"state": "completed", "finished": True})
    error_json = json.dumps({"state": "error", "finished": False})
    monitor = f"python {stage_dir_path}/remote_status_monitor.py"
    r = shlex.quote(str(root_dir))
    s_ok = shlex.quote(completed_json)
    s_bad = shlex.quote(error_json)
    finalize_bits = []
    for stage_index in stage_indices:
        finalize_bits.append(
            f"if [ \"$rc\" -eq 0 ]; then "
            f"{monitor} -r {r} -I {internal_id} -S {stage_index} -s {s_ok}; "
            f"else "
            f"{monitor} -r {r} -I {internal_id} -S {stage_index} -s {s_bad}; "
            f"fi"
        )
    after_cmd = (
        "kill $MONITOR_PID 2>/dev/null || true; "
        + "; ".join(finalize_bits)
    )
    
    # We will actually be including an entire asyncflow within the wrapper command,
    # so we don't need to do anything here.
    before_cmd = ""
    after_cmd = ""

    wrap_cmd = command_string
    full = shlex.quote(f"{before_cmd}; {wrap_cmd}; rc=$?; {after_cmd}; exit $rc") \
        if not worker_init else shlex.quote(
           f"{worker_init}; {before_cmd}; {wrap_cmd}; rc=$?; {after_cmd}; exit $rc")
    slurm_args += ["--wrap", full]
    out = run(["bash", "-lc", " ".join(x for x in slurm_args)])
    stdout = out[1]
    job_id = stdout.strip().split()[-1]

    slurm_queued_info_dict = {
        "job_id": job_id,
        "job_name": job_name,
        "job_stdout_filenames": stdout_basenames,
        "job_stderr_filenames": stderr_basenames,
        "submitted_at": time.time(),
    }
    slurm_queued_info_json = json.dumps(slurm_queued_info_dict)
    slurm_queued_state_dict = {
        "state": "queued",
    }
    slurm_queued_state_json = json.dumps(slurm_queued_state_dict)
    slurm_queued_info_command = f"python {slurm_dir_path}/slurm_job_monitor.py "\
        f"-r {root_dir} -I {internal_id} -i {slurm_queued_info_json} "\
        f"-s {slurm_queued_state_json}"
    run(["bash", "-lc", slurm_queued_info_command])
    return {"success": True, "error": None, "internal_id": internal_id}


def slurm_remote_status_workflow(args):
    """
    SLURM status workflow.

    Read the Slurm/Stage Info/State files and return them.
    """
    import json
    import shlex
    import pathlib
    import subprocess
    from dataclasses import dataclass, asdict
    from typing import Dict, List, Tuple, Optional

    def run(cmd: List[str], check: bool = True) -> tuple:
        """Run command, return (rc, stdout, stderr)"""
        out = subprocess.run(cmd, stdout=subprocess.PIPE, 
                           stderr=subprocess.PIPE, text=True)
        if check and out.returncode != 0:
            raise RuntimeError(f"Command failed: ({out.returncode}): "
                             f"{' '.join(cmd)}\nSTDERR:\n{out.stderr}")
        return out.returncode, out.stdout.strip(), out.stderr.strip()

    def parse_json_file_to_dict(path: pathlib.Path) -> dict:
        with open(path, "r") as f:
            return json.load(f)

    root_dir = pathlib.Path(args[0])
    internal_id = args[1]
    stage_indices = args[2]

    slurm_dir_path = root_dir / ".slurm_runner"

    # Call slurm_job_monitor.py to get the slurm info and state
    slurm_info_command = f"python {slurm_dir_path}/slurm_job_monitor.py "\
        f"-r {shlex.quote(str(root_dir))} -I {internal_id}"
    rc, stdout, stderr = run(slurm_info_command, shell=True)
    if rc != 0:
        return {"success": False, "error": f"Failed to obtain slurm info/state: {stderr}"}

    root_dir_path = pathlib.Path(root_dir)
    assert root_dir_path.exists(), f"Root directory does not exist: {root_dir_path}"
    assert root_dir_path.is_dir(), f"Root directory is not a directory: {root_dir_path}"
    slurm_dir_path = root_dir_path / ".slurm_runner"
    stage_dir_path = root_dir_path / ".stage_states"

    slurm_info_path = slurm_dir_path / f"slurm_info_{internal_id}.json"
    slurm_state_path = slurm_dir_path / f"slurm_state_{internal_id}.json"
    
    stage_info_dicts = []
    stage_state_dicts = []
    for stage_index in stage_indices:
        stage_info_filename = stage_dir_path / f"stage_info_{internal_id}_{stage_index}.json"
        stage_state_filename = stage_dir_path / f"stage_state_{internal_id}_{stage_index}.json"
        stage_info_dicts.append(parse_json_file_to_dict(stage_info_filename))
        stage_state_dicts.append(parse_json_file_to_dict(stage_state_filename))

    result = {
        "success": True,
        "error": None,
        "slurm_info": parse_json_file_to_dict(slurm_info_path),
        "slurm_state": parse_json_file_to_dict(slurm_state_path),
        "stage_info_dicts": stage_info_dicts,
        "stage_state_dicts": stage_state_dicts,
    }
    return result

def slurm_remote_cancel_workflow(args):
    """
    Cancel SLURM job(s) remotely by id and/or scheduler job name.

    Args: [root_dir, job_id, job_name?, remove_json_files?]
    """
    import shlex
    import pathlib
    import subprocess
    from typing import List

    root_dir = args[0]
    job_id = args[1] if len(args) > 0 else None
    job_name = args[2] if len(args) > 1 else None
    remove_json_files = args[3] if len(args) > 2 else False

    root_dir_path = pathlib.Path(root_dir)

    def run(cmd: List[str], check: bool = True) -> tuple:
        """Run command, return (rc, stdout, stderr)"""
        out = subprocess.run(cmd, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE, text=True)
        if check and out.returncode != 0:
            raise RuntimeError(f"Command failed: ({out.returncode}): "
                             f"{' '.join(cmd)}\nSTDERR:\n{out.stderr}")
        return out.returncode, out.stdout.strip(), out.stderr.strip()

    def cleanup_stage_state_files(root_dir_path: pathlib.Path) -> None:
        slurm_dir = root_dir / ".slurm_runner"
        stage_dir = root_dir / ".stage_states"
        if stage_dir.exists():
            for filepath in stage_dir.glob("*.json"):
                try:
                    filepath.unlink()
                except Exception as e:
                    pass
        if slurm_dir.exists():
            for filepath in slurm_dir.glob("*.json"):
                try:
                    filepath.unlink()
                except Exception as e:
                    pass

    if not job_id and not job_name:
        return {"success": False, "error": "job_id or job_name required"}

    results = []
    if job_id:
        results.append(run(
            ["bash", "-lc", f"scancel {shlex.quote(str(job_id))}"],
            check=False))
    if job_name:
        results.append(run(
            ["bash", "-lc", f"scancel --name={shlex.quote(str(job_name))}"],
            check=False))
    result = {
        "success": True,
        "error": None,
        "jobid": job_id,
        "job_name": job_name,
        "cancel output:": results,
    }

    if remove_json_files:
        try:
            cleanup_stage_state_files(root_dir_path)
            result["success"] = True
        except Exception as e:
            result["error"] = f"Failed to clear runner state: {e}"
            result["success"] = False

    return result