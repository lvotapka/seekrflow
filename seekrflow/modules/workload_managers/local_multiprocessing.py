"""
modules/workload_managers/local_multiprocessing.py

Local multiprocessing workload manager for seekrflow.
"""

import os
import glob
import time
import json
import signal
import multiprocessing
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Tuple

# ============================================================================
# Multiprocessing Info/State Management
# ============================================================================

@dataclass
class LocalProcessInfo:
    """Unchanging nformation for a locally running multiprocessing process."""
    stage_name: str
    pid: int
    started_at: float
    root_dir: str
    output_file: str
    anchor: str
    swarm_id: int | None
    device_index: str | None
    force_overwrite: bool
    benchmark_mode: bool

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
    
    @staticmethod
    def from_dict(data: dict) -> "LocalProcessInfo":
        """Create from dictionary loaded from JSON."""
        return LocalProcessInfo(**data)
    
    @staticmethod
    def load(path: str) -> "LocalProcessInfo":
        """Load state from file."""
        with open(path, "r") as f:
            data = json.load(f)
        return LocalProcessInfo.from_dict(data)
    
    def save(self, path: str) -> None:
        """Save state to file."""
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=4)

@dataclass
class LocalProcessState:
    """Transient information for a locally running multiprocessing process."""
    stage_state: str  # "unstarted", "started", "completed", "error", "unknown"
    manager_state: str  # 'pending', 'running', 'idle', 'failed', 'cancelled'
    finished: bool
    progress: float
    ended_at: Optional[float]  # Set when process completes, fails, or is killed
    error: Optional[str]
    traceback: Optional[str]
    notes: Optional[str]
    #TODO: replace this variable with fields in the info/state objects
    #progress_info: Optional[Dict] = None # Stage-specific progress information
    imports_successful: bool
    model_loaded: bool

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
    
    @staticmethod
    def from_dict(data: dict) -> "LocalProcessState":
        """Create from dictionary loaded from JSON."""
        return LocalProcessState(**data)
    
    @staticmethod
    def load(path: str) -> "LocalProcessState":
        """Load state from file."""
        with open(path, "r") as f:
            data = json.load(f)
        return LocalProcessState.from_dict(data)
    
    def save(self, path: str) -> None:
        """Save state to file."""
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=4)


def get_local_state_dir(root_dir: str) -> str:
    """
    Get the directory for local multiprocessing state files.
    Located next to model.json in the root directory.
    """
    state_dir = os.path.join(
        root_dir, ".multiprocessing")
    os.makedirs(state_dir, exist_ok=True)
    return state_dir

def get_local_info_and_state_files(
        root_dir: str, 
        stage_name: str, 
        pid: int,
        anchor: str = "any",
        swarm_id: int | None = None
        ) -> Tuple[str, str]:
    """
    Get the state file path for a local process.
    Includes PID in filename to avoid conflicts.
    """
    state_dir = get_local_state_dir(root_dir)
    anchor_str = ""
    if anchor != "any":
        anchor_str = f"anchor_{anchor}_"

    swarm_id_str = ""
    if swarm_id is not None:
        swarm_id_str = f"swarm_{swarm_id}_"

    info_name = os.path.join(
        state_dir, f"{stage_name}_{anchor_str}{swarm_id_str}info_{pid}.json")

    state_name = os.path.join(
        state_dir, f"{stage_name}_{anchor_str}{swarm_id_str}state_{pid}.json")

    return info_name, state_name


def check_pid_exists(pid: int) -> bool:
    """
    Check if a process with given PID is still running.
    
    Uses os.kill with signal 0, which doesn't actually send a signal
    but checks if the process exists.
    """
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except PermissionError:
        return True
    except OSError:
        return False

def find_latest_info_and_state_files(
        root_dir: str, 
        stage_name: str,
        anchor: str = "any",
        swarm_id: int | None = None
        ) -> Optional[Tuple[str, str]]:
    """
    Find the most recent state file for a given stage.
    Returns (None, None) if no state files exist for this stage.
    """
    state_dir = get_local_state_dir(root_dir)
    anchor_str = ""
    if anchor != "any":
        anchor_str = f"anchor_{anchor}_"
    swarm_id_str = ""
    if swarm_id is not None:
        swarm_id_str = f"swarm_{swarm_id}_"
    pattern = f"{stage_name}_{anchor_str}{swarm_id_str}state_*.json"
    state_glob = os.path.join(state_dir, pattern)
    state_files = list(glob.glob(state_glob))
    if not state_files:
        return None, None
    state_files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    state_file = state_files[0]
    info_file = state_file.replace("state_", "info_")
    if not os.path.exists(info_file):
        return None, None
    return info_file, state_file

def check_for_existing_local_process(
        root_dir: str, 
        stage_name: str,
        anchor: str = "any",
        swarm_id: int | None = None
        ) -> Optional[LocalProcessInfo]:
    """
    Check if there's a process from a previous run still active.
    Returns the process info file if found and running, None otherwise.
    The info file is found because the PID is needed sometimes.
    
    This is used for reattachment after detaching with 'd' command,
    and for monitoring.
    """
    info_file, state_file = find_latest_info_and_state_files(
        root_dir, stage_name, anchor, swarm_id)
    #info_file, state_file = get_local_info_and_state_files(
    #    root_dir, stage_name, anchor, swarm_id)
    if not info_file or not os.path.exists(info_file) or not state_file \
            or not os.path.exists(state_file):
        return None
    
    try:
        state = LocalProcessState.load(state_file)
        info = LocalProcessInfo.load(info_file)
        
        # Only consider it if it's marked as running
        if state.manager_state in ["running", "pending"]:
            # Verify the PID actually exists
            if check_pid_exists(info.pid):
                print(f"Found existing {stage_name} process from previous "\
                      f"session (PID: {info.pid})")
                return info
            else:
                # Process died without updating state file
                print(f"Found stale {stage_name} state file - " \
                      f"process {info.pid} no longer exists")
                state.manager_state = "failed"
                state.ended_at = time.time()
                state.error = "Process terminated unexpectedly"
                state.save(state_file)
                return None
        
    except Exception as e:
        print(f"Warning: Could not load state file for {stage_name}: {e}")
    
    return None

def status_local(
        root_dir: str,
        stage_index: int,
        stage_name: str,
        stage_process: multiprocessing.Process | None,
        anchor: str = "any",
        swarm_id: int | None = None,
        ) -> dict: 
    """
    Check if the stage has finished locally. This is done by:
    1. Checking the process info/state files
    2. Checking if the process is still alive
    3. Reading the results XML files to see how many steps have elapsed
    """
    import os
    import sys
    import traceback
    
    # Check for state file
    info_file, state_file = find_latest_info_and_state_files(
        root_dir, stage_name, anchor, swarm_id)
    if not info_file or not state_file:
        return {
            "success": False,
            "error": "No local process info/state files found",
            "process_info": None,
            "process_state": None,
        }
        
    process_info = None
    process_state = None
    try:
        process_info = LocalProcessInfo.load(info_file)
    except Exception as e:
        print(f"Warning: Could not load local process info file: {e}")
        return {
            "success": False,
            "error": f"Could not load local process info file: {e}",
            "process_info": None,
            "process_state": None,
        }
    try:
        process_state = LocalProcessState.load(state_file)
    except Exception as e:
        print(f"Warning: Could not load local process state file: {e}")
        return {
            "success": False,
            "error": f"Could not load local process state file: {e}",
            "process_info": None,
            "process_state": None,
        }
    
    benchmark_mode = bool(process_info.benchmark_mode)
    
    # TODO: perhaps extract this from the state file instead?
    if anchor == "any":
        partitioned_arg = None
    else:
        partitioned_arg = anchor

    current_run_alive = False
    if stage_process is not None and stage_process.is_alive():
        current_run_alive = True
    elif process_state is not None \
            and process_state.manager_state == "running" \
            and check_pid_exists(process_info.pid):
        current_run_alive = True

    if benchmark_mode:
        # status.py doesn't know benchmark-mode criteria. For benchmark runs,
        # derive state from the process-state lifecycle and local process liveness.
        running_now = False
        if stage_process is not None and stage_process.is_alive():
            running_now = True
        elif process_state is not None \
                and process_state.manager_state == "running" \
                and check_pid_exists(process_info.pid):
            running_now = True

        if process_state is not None and process_state.stage_state == "completed":
            process_state.finished = True
        elif running_now:
            process_state.finished = False
            process_state.progress = 0.0
        else:
            process_state.finished = False
            process_state.progress = 0.0
    else:
        # Check actual simulation progress from model files
        model_filename = os.path.join(root_dir, "model.json")

        try:
            # Import here so child process gets fresh imports
            import seekr.modules.structures as structures
            import seekr.status as seekr_status

            # Only care about stage progress, not analysis statistics
            model = structures.load_model(model_filename)
            instruction = "progress"
            message_dict = seekr_status.status(
                model,
                instruction,
                stage_arg=stage_index,
                # seekr.status/run.extract_anchor_index expects "any" or a
                # concrete anchor identifier; passing None raises TypeError.
                anchor_arg=anchor,
                swarm_id=swarm_id,
                print_json=True,
            )
            progress_by_stage = message_dict.get("progress", {})
            stage_progress = progress_by_stage.get(stage_index)
            if stage_progress is None:
                # Be tolerant of key-type differences from status() output.
                stage_progress = progress_by_stage.get(str(stage_index), {})

        except Exception as e:
            # Update state to failed (process itself updates this)
            print(f"\nStage {stage_index} status check failed: {e}")
            return {
                "success": False,
                "error": f"Stage {stage_index} status check failed: {e}",
                "process_info": process_info.to_dict(),
                "process_state": process_state.to_dict(),
            }

        if stage_progress.get("finished", False):
            process_state.finished = True
            process_state.progress = 1.0
        else:
            progress_map = stage_progress.get("progress", {})
            if partitioned_arg is None:
                # "any" means stage-level view. seekr.status stores progress
                # per-partition (e.g., per-anchor) rather than under key None,
                # so aggregate all partition progress values.
                progress_values = []
                if isinstance(progress_map, dict):
                    for _, per_partition in progress_map.items():
                        if isinstance(per_partition, dict):
                            value = per_partition.get("progress")
                            if isinstance(value, (int, float)):
                                progress_values.append(float(value))
                if progress_values:
                    progress = sum(progress_values) / len(progress_values)
                else:
                    progress = 0.0
                partitioned_status = {}
            else:
                partitioned_status = progress_map.get(partitioned_arg, {})
                # Empty dict = no progress data written yet (run just started, or
                # the partitioned_arg isn't present in seekr's output yet). Treat
                # that as 0.0 progress rather than an invariant violation.
                if partitioned_status and "progress" not in partitioned_status:
                    raise RuntimeError(
                        f"seekr status output for stage {stage_index} "
                        f"(stage_name={stage_name!r}, partition={partitioned_arg!r}) "
                        f"is missing the required 'progress' key. Got keys: "
                        f"{sorted(partitioned_status.keys())}")
                progress = partitioned_status.get("progress", 0.0)

            if process_state.stage_state not in ["completed", "error"] \
                    and process_state.manager_state not in ["cancelled", "failed", "idle"]:
                if progress > 0.0 or current_run_alive:
                    process_state.stage_state = "started"
                else:
                    process_state.stage_state = "unstarted"
                process_state.finished = False
                process_state.progress = progress
            
    # seekr.status() above may still show a
    # previous run as "finished" until the freshly-spawned child clears it
    # via force_overwrite. The orchestrator's own Process handle is the
    # authoritative source for whether the CURRENT run is still active:
    # if it is alive, the stage cannot possibly be completed. The state
    # file is unreliable here because, just after a force_rerun, the
    # latest-by-mtime file is the OLD "killed" record from the prior run
    # (the newly-spawned child hasn't written its state file yet due to
    # "spawn" import overhead).

    if current_run_alive and process_state.stage_state == "completed":
        process_state.finished = False
        process_state.stage_state = "started"
    elif current_run_alive and process_state.stage_state == "unstarted":
        # During partitioned runs, seekr.status can briefly return no progress
        # data even though the process is actively producing outputs.
        process_state.stage_state = "started"

    if (not current_run_alive) and (process_state.finished) \
            and (process_state.manager_state == "idle"):
        process_state.stage_state = "completed"

    # If the child process finished cleanly (seekr_run.run returned without
    # raising), trust that terminal state only for benchmark runs. In BD
    # benchmark mode, do_run_instruction_bd intentionally wipes BD outputs
    # afterwards, so seekr.status can report finished=False/progress=0.0 and
    # the progress-derived state would otherwise pin to "unstarted" forever.
    # NOTE: Should now be unnecessary since we will be directly passing the 
    # contents of the process state file to the orchestrator.
    #if benchmark_mode \
    #        and not current_run_alive \
    #        and process_state is not None \
    #        and process_state.stage_state == "completed" \
    #        and stage_status["state"] != "completed":
    #    process_state.finished = True
    #    process_state.stage_state = "completed"
    #    # Preserve the underlying progress number so the UI can still show
    #    # what seekr currently sees (e.g. 0.0 after benchmark cleanup).
    
    # Check if process is running and build job object with integrated state
    #if stage_process is not None and stage_process.is_alive():
    #    job = {
    #        "JobID": f"local_pid_{stage_process.pid}",
    #        "State": "RUNNING",
    #        "PID": stage_process.pid
    #    }
    #    # Integrate process state info into job object
    #    if process_state:
    #        job["process_status"] = process_state.status
    #        job["started_at"] = process_state.started_at
    #        job["output_file"] = process_state.progress_info.get(
    #            "output_file", "N/A") if process_state.progress_info \
    #            else "N/A"
    #    stage_manager_status["jobs"].append(job)
    if process_state:
        # Check state file
        if process_state.manager_state in ["running", "pending"]:
            # Process claims to be running, verify
            if not check_pid_exists(process_info.pid):
                # Process died without updating state file
                process_state.manager_state = "failed"
                process_state.notes = f"Process {process_info.pid} "\
                    "exited unexpectedly"
                process_state.error = "Process crashed"
                if process_state.ended_at is None:
                    process_state.ended_at = time.time()
                process_state.finished = False

        elif process_state.manager_state in ["failed", "cancelled"]:
            process_state.notes = f"Process {process_state.manager_state}: "\
                f"{process_state.error}"
            if process_state.ended_at is None:
                process_state.ended_at = time.time()
            process_state.finished = False
    
    process_state.save(state_file)
    results = {
        "success": True,
        "error": None,
        "process_info": process_info.to_dict(),
        "process_state": process_state.to_dict()
    }
    
    return results
     
# ============================================================================
# Local Execution Function
# ============================================================================

def run_locally(
        root_dir: str,
        stage_name: str,
        anchor: str = "any",
        swarm_id: int | None = None,
        device_index: str | None = None,
        force_overwrite: bool = False,
        benchmark_mode: bool = False,
        #restart_attempts: int = 0,
        ):
    """
    Run seekr stage locally using multiprocessing.
    Writes info/state files with PID for monitoring and reattachment, as well
    as other information.
    Redirects stdout/stderr to {stage}_run.out.
    
    This function is executed in a separate process.
    """
    # TODO: handle benchmark mode?

    import os
    import sys
    import time
    import signal
    import pathlib
    import traceback
    
    # Make this process a process group leader so all subprocesses 
    # (like openmm / nam_simulation) are in the same group and can 
    # be killed together
    os.setpgrp()
    
    pid = os.getpid()
    # TODO: check if this is a file or a dir
    info_file, state_file = get_local_info_and_state_files(
        root_dir, stage_name, pid, anchor, swarm_id)
    anchor_str = ""
    if anchor != "any":
        anchor_str = f"anchor_{anchor}_"

    swarm_id_str = ""
    if swarm_id is not None:
        swarm_id_str = f"swarm_{swarm_id}_"

    root_dir_path = pathlib.Path(root_dir)
    log_dir_path = root_dir_path / "logs"
    log_dir_path.mkdir(exist_ok=True)
    output_file_path = log_dir_path / f"{stage_name}_{anchor_str}{swarm_id_str}run_{pid}.out"
    model_filename_path = root_dir_path / "model.json"
    
    # Create initial state BEFORE redirecting output (so any errors 
    # are visible)
    started_at = time.time()
    process_info = LocalProcessInfo(
        stage_name=stage_name,
        pid=pid,
        started_at=started_at,
        root_dir=root_dir,
        output_file=str(output_file_path),
        anchor=anchor,
        swarm_id=swarm_id,
        device_index=device_index,
        force_overwrite=force_overwrite,
        benchmark_mode=benchmark_mode,
    )
    process_info.save(info_file)
    process_state = LocalProcessState(
        stage_state="unstarted",
        manager_state="pending",
        finished=False,
        progress=0.0,
        ended_at=None,
        error=None,
        traceback=None,
        notes=None,
        imports_successful=False,
        model_loaded=False,
    )
    process_state.save(state_file)
    
    # NOW redirect output at OS level using dup2 (so subprocesses via 
    # os.system() are also redirected)
    try:
        output_fd = os.open(
            str(output_file_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        os.dup2(output_fd, 1)  # Redirect stdout (fd 1)
        os.dup2(output_fd, 2)  # Redirect stderr (fd 2)
        os.close(output_fd)
        
        # Also redirect at Python level for consistency
        # Line buffered, append mode
        sys.stdout = open(str(output_file_path), 'a', buffering=1) 
        sys.stderr = sys.stdout
    except Exception as e:
        print(f"FATAL: Cannot redirect output to {output_file_path}: {e}", 
              file=sys.stderr)
        raise
    
    print(f"{stage_name} stage started at {time.ctime(started_at)}")
    print(f"PID: {pid}")
    print(f"Model file: {model_filename_path}")
    print(f"State file: {state_file}")
    print(f"Anchor(s): {anchor}")
    print(f"Device index: {device_index}")
    print(f"Force overwrite: {force_overwrite}")
    print(f"Benchmark mode: {benchmark_mode}")
    print(f"Swarm ID: {swarm_id}")
    print("-" * 60)
    sys.stdout.flush()
    
    # Handle termination signals to update state before exit
    def signal_handler(signum, frame):
        print(f"\nReceived signal {signum}, updating state to 'cancelled'...")
        sys.stdout.flush()
        process_state.manager_state = "cancelled"
        if process_state.ended_at is None:
            process_state.ended_at = time.time()
        process_state.error = f"Process killed by signal {signum}"
        process_state.save(state_file)
        sys.exit(128 + signum)  # Standard exit code for signals
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Import here so child process gets fresh imports
        import seekr.modules.structures as structures
        import seekr.run as seekr_run
        
        # Update state - imports successful
        process_state.imports_successful = True
        process_state.save(state_file)
        print("Imports successful")
        sys.stdout.flush()
        
        # Load model
        model = structures.load_model(str(model_filename_path))
        process_state.model_loaded = True
        process_state.manager_state = "running"
        process_state.save(state_file)
        print("Model loaded")
        sys.stdout.flush()
        
        # Run stage
        print(f"Starting {stage_name}...")
        sys.stdout.flush()
        seekr_run.run(
            model, stage_name, anchor, device_index, force_overwrite, 
            swarm_id, benchmark=benchmark_mode)
        
        # Update state to completed (process itself updates this)
        process_state.stage_state = "completed"
        process_state.manager_state = "idle"
        process_state.ended_at = time.time()
        process_state.finished = True
        process_state.save(state_file)
        print(f"\n{stage_name} completed at {time.ctime()}")
        sys.stdout.flush()
        
    except Exception as e:
        # Update state to failed (process itself updates this)
        process_state.manager_state = "failed"
        process_state.stage_state = "error"
        process_state.error = str(e)
        process_state.traceback = traceback.format_exc()
        process_state.ended_at = time.time()
        process_state.save(state_file)
        print(f"\n{stage_name} failed: {e}")
        traceback.print_exc()
        sys.stdout.flush()
        raise  # Re-raise so process exits with error code

    return

def kill_existing_local_stage_processes(
        stage_name: str,
        root_dir: str,
        ) -> None:
    """
    Kill any running local processes for the given stage, identified via
    state files in the .multiprocessing directory. Safe to invoke repeatedly.
    """
    state_dir = get_local_state_dir(root_dir)
    pattern = f"{stage_name}*state_*.json"
    for state_file in glob.glob(os.path.join(state_dir, pattern)):
        info_file = state_file.replace("state_", "info_")
        try:
            process_info = LocalProcessInfo.load(info_file)
        except Exception as e:
            print(f"  Warning: Could not load info file {info_file}: {e}")
            continue
        try:
            process_state = LocalProcessState.load(state_file)
        except Exception as e:
            print(f"  Warning: Could not load state file {state_file}: {e}")
            continue
        if process_state.manager_state not in ["running", "pending"] \
                or not check_pid_exists(process_info.pid):
            continue
        pid = process_info.pid
        print(f"  Killing existing {stage_name} process (PID: {pid})...")
        try:
            os.killpg(pid, signal.SIGTERM)
            time.sleep(2)
            if check_pid_exists(pid):
                os.killpg(pid, signal.SIGKILL)
                time.sleep(1)
            process_state.manager_state = "cancelled"
            process_state.ended_at = time.time()
            process_state.error = "Process killed for force re-run"
            process_state.save(state_file)
        except (ProcessLookupError, PermissionError) as e:
            print(f"  Warning: Could not kill process: {e}")
    return

def check_and_raise_if_process_failed(
        stage_name: str, 
        stage_process: multiprocessing.Process | None, 
        root_dir: str,
        anchor: str = "any",
        swarm_id: int | None = None
        ) -> None:
    """
    Check if a local process has failed by examining state files.
    Raises an exception if failure detected.
    """
    if stage_process is not None and not stage_process.is_alive():
        # Check exit code
        if hasattr(stage_process, 'exitcode') \
                and stage_process.exitcode is not None \
                and stage_process.exitcode != 0:
            info_file, state_file = find_latest_info_and_state_files(
                root_dir, stage_name, anchor, swarm_id)
            if info_file and state_file:
                try:
                    process_info = LocalProcessInfo.load(info_file)
                    process_state = LocalProcessState.load(state_file)
                    # Trust the state file: if the child reported success,
                    # ignore a noisy nonzero exit code from interpreter
                    # shutdown (common with fork + asyncio/threads).
                    if process_state.stage_state in ["completed"] \
                            or process_state.manager_state in ["cancelled"]:
                        return
                    error_text = process_state.error if process_state.error is not None \
                        else f"process exited with code " \
                             f"{stage_process.exitcode} while state was " \
                             f"still '{process_state.stage_state}'"
                    error_msg = f"Local {stage_name.upper()} stage failed. "\
                                f"Error: {error_text}"
                    if process_state.traceback:
                        error_msg += f"\nTraceback:\n{process_state.traceback}"
                    raise Exception(error_msg)
                except json.JSONDecodeError:
                    raise Exception(f"Local {stage_name.upper()} stage failed "\
                                    f"with exit code {stage_process.exitcode}")
            else:
                raise Exception(f"Local {stage_name.upper()} stage failed "\
                                f"with exit code {stage_process.exitcode}")
