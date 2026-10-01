"""
modules/client/run.py

Handle client run processes.
"""

import os
import time
import json
import fcntl
import signal
import typing
import asyncio
import datetime
import multiprocessing
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor

import attrs
import seekr.modules.structures as seekr_structures
from radical.asyncflow import WorkflowEngine, LocalExecutionBackend
import seekr.modules.structures as seekr_structures
import seekr.modules.scales.base as scales_base

import seekrflow.modules.structures as structures
import seekrflow.modules.transfer.base as transfer_base
import seekrflow.modules.client.structures as client_structures
import seekrflow.modules.client.validation as client_validation
import seekrflow.modules.workload_managers.remote_local as workload_remote_local
import seekrflow.modules.job.structures as job_structures

MAIN_LOOP_INTERVAL = 5.0 # seconds
STATUS_WRITE_INTERVAL = 5.0 # seconds
CONTROL_FILE_READ_INTERVAL = 5.0 # seconds
STATUS_SCHEMA_VERSION = 2
SHUTDOWN_CANCEL_TIMEOUT = 30.0
ENGINE_SHUTDOWN_TIMEOUT = 15.0
REMOTE_STATUS_WRITE_INTERVAL = 15.0 # seconds

# TODO: track consecutive empty job checks to avoid premature submission?
        #   formerly: MAX_EMPTY_CHECKS_BEFORE_RESUBMIT = 10
        # TODO: track quick failures (jobs that start running but fail within N cycles)
        #   formerly: MIN_RUNNING_TIME_BEFORE_IDLE = 2 * MAIN_LOOP_INTERVAL

async def _run_blocking(fn: typing.Callable[..., typing.Any], *args, **kwargs):
    """
    Run a blocking callable off the asyncio event loop (thread pool).
    Used for Globus / remote status and submit calls so client stays 
    responsive while endpoints are slow.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, lambda: fn(*args, **kwargs))

def run_units_from_info(
        dimensions: list[dict],
        number_of_anchors: int,
        number_of_swarms: int,
    ) -> list[job_structures.RunUnit]:
    """
    Generate run units grid from dimensions and numbers of anchors and swarms.
    """
    # Validate first
    if "anchor" in dimensions:
        if number_of_anchors <= 0:
            raise ValueError(
                "Dispatch dimension 'anchor' requires a positive num_anchors."
                f"got {number_of_anchors}.")
    if "swarm" in dimensions:
        if number_of_swarms <= 1:
            raise ValueError(
                f"Dispatch dimension 'swarm' requires num_swarms > 1, "
                f"got {number_of_swarms}.")
    # Assign the grid of run units
    if len(dimensions) == 0:
        return [job_structures.RunUnit(anchor="any", swarm_id=None)]
    
    if dimensions == ["anchor"]:
        return [
            job_structures.RunUnit(anchor=anchor, swarm_id=None)
            for anchor in range(number_of_anchors)
        ]

    if dimensions == ["swarm"]:
        return [
            job_structures.RunUnit(anchor="any", swarm_id=swarm)
            for swarm in range(number_of_swarms)
        ]

    if dimensions == ["anchor", "swarm"]:
        units: list[job_structures.RunUnit] = []
        for anchor in range(number_of_anchors):
            for swarm in range(number_of_swarms):
                units.append(job_structures.RunUnit(anchor=anchor, swarm_id=swarm))
        return units

    raise ValueError(f"Unsupported dispatch dimensions: {dimensions!r}.")
    
# TODO: move to a client_stage_workflow.py module?
@attrs.define
class StageWorkflow:
    """
    A workflow for a single stage of SEEKR3.
    """
    # Constructor args (no defaults → required, in this order)
    model: seekr_structures.Seekr_model = attrs.field(repr=False)
    seekrflow: structures.Seekrflow = attrs.field(repr=False)
    stage_list: List[scales_base.Base_stage] = attrs.field(repr=False)
    workflow_engine: WorkflowEngine = attrs.field(repr=False)
    #resource_name: str = attrs.field(default="local")
    resource: structures.Resource_base | None = attrs.field(repr=False)
    resolved_execution: structures.Resolved_execution | None = attrs.field(
        default=None, repr=False)
    force_overwrite: Dict[str, bool] = attrs.field(factory=dict)
    benchmark_mode: Dict[str, bool] = attrs.field(factory=dict)
    
    # Derived / mutable state
    dependency_indices: list[int] = attrs.field(factory=list)
    dependency_tasks: list = attrs.field(factory=list)
    task: typing.Any = attrs.field(default=None)
    process: multiprocessing.Process | None = attrs.field(
        default=None, repr=False)
    stage_state: Dict[str, str] = attrs.field(
        factory=dict)
    #    default="unknown", validator=attrs.validators.in_(
    #        {'unstarted', 'started', 'completed', 'error', 'unknown'}))
    # TODO: get these filled out from the state
    progress: Dict[str, dict[str, float]] = attrs.field(factory=dict)
    elapsed_times: Dict[str, float] = attrs.field(factory=dict)
    number_of_steps_completed: Dict[str, int] = attrs.field(factory=dict)
    timestep_values: Dict[str, float] = attrs.field(factory=dict)

    manager_status: str = attrs.field(
        default="idle", validator=attrs.validators.in_(
            {"pending", "queued", "running", "queued/running", "idle", "failed", 
            "cancelled", "pushing", "pulling"}))
    semaphore: str = attrs.field(
        default="go", validator=attrs.validators.in_(
            {"go", "wait", "stop"}))
    #job_ids: List[str] = attrs.field(factory=list)
    #job_names: List[str] = attrs.field(factory=list)
    internal_id: Optional[int] = attrs.field(default=None)
    job_id: Optional[str] = attrs.field(default=None)
    job_name: Optional[str] = attrs.field(default=None)
    array_indices: Optional[List[int]] = attrs.field(default=None)

    # TODO: move transfer-related attributes to a separate class?
    transfer_requested: bool = attrs.field(default=False)
    transfer_status: str = attrs.field(default="idle")
    transfer_direction: str | None = attrs.field(default=None)
    transfer_error: str | None = attrs.field(default=None)
    transfer_relaunch_count: int = attrs.field(default=0)
    transfer_from: str | None = attrs.field(default=None)
    last_error: str | None = attrs.field(default=None)
    #last_raw_status: dict | None = attrs.field(default=None, repr=False)
    running_start_time: float | None = attrs.field(default=None)
    status_polled_at: float | None = attrs.field(default=None)
    #co_schedule_with: str | None = attrs.field(default=None)
    #fusion_host: str | None = attrs.field(default=None)
    #fused_before: list[str] = attrs.field(factory=list)
    #fused_after: list[str] = attrs.field(factory=list)
    #peer_workflows: dict[str, "StageWorkflow"] = attrs.field(
    #    factory=dict, repr=False)
    #holds_local_slot: bool = attrs.field(default=False)
    #local_slot_file: str | None = attrs.field(default=None, repr=False)
    telemetry_poll_interval: float | None = attrs.field(default=None)
    detached_requested: bool = attrs.field(default=False)
    control_file_semaphore: str | None = attrs.field(default=None, repr=False)
    
    # TODO: work on this function and see whether it's even necessary
    async def probe_launch(self) -> tuple[str, dict|None]:
        """
        Probe for any running jobs for our stages and return the action and status.
        """
        if self.resource is None: # Local resource
            return "submit", None
        stage_indices = [stage.index for stage in self.stage_list]
        # TODO: figure out what to do if internal_id is None
        if self.internal_id is None:
            # A fresh run probably
            return "submit", None
        # TODO: construct a manager_payload
        try:
            resulting_payload = workload_remote_local.status(
                resource=self.resource,
                manager_payload=manager_payload,
                silent=True
            )
        except Exception as e:
            print(
                f"[remote-probe] fused set member {stage_indices[0]}: "
                f"probe failed ({e}); not submitting until state "
                f"can be read"
            )
            if self.job_id is not None:
                return "reattach", None
            return "defer", None

        # See if any jobs exist in the manager status
        self.job_id = member_statuses.get("manager_state", {}).get("last_known_jobs", None)
        if self.job_id is not None:
            return "reattach", None
        completed = True
        for stage_state_dict in member_statuses.get("stage_state_dicts", {}):
            if not stage_state_dict.get("finished", False):
                completed = False
                break
        if completed:
            return "completed", member_statuses
        return "submit", member_statuses
        
    def _outbound_transfer_resources(self):
        """
        Source/destination resources for the outbound copy, or (None, None).
        """
        if self.transfer_from is None:
            dep_index = getattr(self.stage_list[0], "input_stage_index", 0)
            if self.resource.name != "local" and dep_index <= 0:
                return None, self.resource
            return None, None
        src_resource = self.systemrun.seekrflow.run_settings.get_resource_by_name(
            self.transfer_from)
        return src_resource, self.resource

    def _compute_submit_time_limit_override(self) -> str | None:
        """
        Determine the time string in "HH:MM:SS" for the time limit override.
        """
        stage_names = [stage.name for stage in self.stage_list]
        # If time policy is fixed or not set, return None, which means no override.
        if self.resolved_execution is None:
            return None
        policy = self.resolved_execution.time_policy
        if isinstance(policy, structures.Time_policy_fixed):
            return None
        else:
            raise NotImplementedError(f"Time policy {policy} not yet implemented.")
        # TODO: need to figure out how to implement the time limit override
        # for the adaptive time policy.
        

    def move_files(
            self,
            backwards: bool = False
            ) -> None:
        """
        Copy this system's files to/from the remote workdir.
        """
        if backwards:
            steps = [(self.resource, True)] if self.resource is not None else []
        else:
            src, dst = self._outbound_transfer_resources()
            steps = []
            if src is not None:
                steps.append((src, True))
            if dst is not None:
                steps.append((dst, False))
        if not steps:
            self.transfer_status = "skipped"
            self.transfer_error = None
            return
        self.transfer_status = "running"
        self.transfer_direction = "in" if backwards else "out"
        self.manager_status = "pulling" if backwards else "pushing"
        try:
            for resource, pull in steps:
                transfer_base.transfer_files_to_from_remote_resource(
                    self.seekrflow.name, resource, self.model.directory,
                    backwards=pull)
        except Exception as e:
            self.transfer_error = str(e)
            self.control_file_semaphore = "wait"
            self.semaphore = "wait"
            self.manager_status = "failed"
            for stage in self.stage_list:
                self.stage_state[stage.name] = "error"
            self.last_error = str(e)
            return

        self.transfer_status = "completed"
        self.transfer_direction = None
        self.manager_status = "idle"
        self.transfer_relaunch_count = 0
        
    async def create_tasks(self) -> None:
        """
        Register tasks for running this stage_workflow.
        """
        # Transfer files if dependent stage resource is different
        @self.workflow_engine.function_task
        async def transfer_files(*args):
            if self.transfer_from is None:
                # For the first remote stage (no dependency), seed remote workdir
                # from local so model/config files exist before submission.
                dep_index = getattr(self.stage_list[0], "input_stage_index", 0)
                if self.resource.name != "local" and dep_index <= 0:
                    pass
                else:
                    self.transfer_status = "skipped"
                    self.transfer_error = None
                    return
            self.move_files(backwards=False)

        dep_index = getattr(self.stage_list[0], "input_stage_index", 0)
        should_transfer = bool(self.transfer_from) or (
            self.resource.name != "local" and dep_index <= 0
        )
        if should_transfer:
            self.dependency_tasks.append(transfer_files(*self.dependency_tasks))

        # Run stage
        @self.workflow_engine.function_task
        async def run_stage(*args):
            stage_names = [stage.name for stage in self.stage_list]
            # TODO: idea goal: don't even distinguish between local and remote here
            # let the 'remote' interface combined with the workload manager handle it.
            # For instance, local would be the 'local_shell' interface combined with the
            # multiprocessing workload manager.
            """
            if self.resource_name == "local":
                force_overwrite_now = self.force_overwrite
                if self.force_overwrite:
                    workload_local_mp.kill_existing_local_stage_processes(
                        self.stage.name, self.model.directory)
                    # Force-rerun is a one-shot request.
                    self.force_overwrite = False
                existing_state = workload_local_mp\
                    .check_for_existing_local_processes(
                        self.model.directory, self.stage.name)
                if existing_state and not force_overwrite_now:
                    # Note: We can't truly "reattach" to a multiprocessing.Process object,
                    # but we can track the PID and monitor/kill it via the state file
                    print(f"  Reattached to {self.stage.name} process "
                          f"(PID: {existing_state.pid})")
                else:
                    # TODO: construct job_spec and run it locally
                    self.process = multiprocessing.Process(
                        target=workload_local_mp.run_locally,
                        args=(self.model.directory, self.stage.name,),
                        kwargs={
                            "force_overwrite": force_overwrite_now,
                            "benchmark_mode": self.benchmark_mode,
                        },
                    )
                    self.process.start()
            """

            #else: # remote
            if self.resource is None:
                raise Exception(
                    f"Remote resource config missing for {self.resource.name!r}")
            try:
                remote_or_cloud = workload_remote_local.resource_kind(
                    self.resource)
                destination_path = None
                destination_model_filename = None
                if remote_or_cloud == "remote":
                    destination_path = (
                        workload_remote_local.resolve_model_directory(
                            self.seekrflow, self.resource))
                    destination_model_filename = os.path.join(
                        destination_path, "model.json")

                # Apply force overwrite to these stages
                for stage_name, force_now in self.force_overwrite.items():
                    if force_now:
                        workload_remote_local.cancel_and_reset_stage(
                            self.seekrflow,
                            stage_names,
                            self.job_id,
                            self.job_name,
                            model_directory=self.model.directory,
                        )
                        for stage_name2 in stage_names:
                            self.force_overwrite[stage_name2] = False
                        # If any stage is force-overwritten, kill the entire job.
                        break

                # prepare stage specs, run units, and job spec
                stage_specs = []
                for stage in self.stage_list:
                    stage_spec = job_structures.StageSpec(
                        stage_index=stage.index,
                        stage_name=stage.name,
                        force_overwrite=self.force_overwrite[stage.name],
                        benchmark=self.benchmark_mode[stage.name],
                    )
                    stage_specs.append(stage_spec)

                # TODO: how are we going to handle run units?
                dispatch = (
                    self.resolved_execution.dispatch \
                        if self.resolved_execution is not None else None)
                dimensions = dispatch.dimensions if dispatch is not None else None
                group_size = dispatch.group_size if dispatch is not None else None
                concurrency = dispatch.concurrency if dispatch is not None else None
                
                number_of_anchors = 100 # TODO: assign from previous stage
                number_of_swarms = 100 # TODO: assign from previous stage
                
                units_grid = run_units_from_info(dimensions, number_of_anchors, number_of_swarms)
                size = group_size or len(units_grid)
                array_size = 0
                job_specs = []
                self.array_indices = []
                for array_index, start in enumerate(range(0, len(units_grid), size)):
                    units_chunk = units_grid[start:start + size]
                    # one JobSpec: same stage_specs, run_units=chunk,
                    # concurrency=min(concurrency, len(chunk)), array_index=array_index
                    job_spec = job_structures.JobSpec(
                        internal_id=0, # Assign on the remote
                        remote_root_dir=destination_path,
                        status_write_interval=REMOTE_STATUS_WRITE_INTERVAL,
                        telemetry_poll_interval=self.telemetry_poll_interval,
                        stage_specs=stage_specs,
                        run_units=units_chunk,
                        concurrency=min(concurrency, len(units_chunk)),
                        array_index=array_index,
                    )
                    self.array_indices.append(array_index)
                    job_specs.append(job_spec)
                    array_size += 1

                # Determine effective walltime:
                #   - benchmark mode: short fixed cap
                #   - else: adaptive/fixed time_policy estimate
                """ # TODO: implement time limit overrides eventually
                time_limit_override = None
                anchor_times_for_submit = None
                if self.benchmark_mode:
                    time_limit_override = (
                        base.BENCHMARK_REMOTE_TIME_LIMIT)
                    print(
                        f"[seekr-time] stage {self.stage.name}: "
                        f"benchmark mode -> requesting "
                        f"{time_limit_override}"
                    )
                else:
                    time_limit_override = self._compute_submit_time_limit_override()
                    if time_limit_override is not None:
                        print(
                            f"[seekr-time] stage {self.stage.name}: "
                            f"requesting {time_limit_override}"
                        )
                """
                # Retrieve existing jobs, if any
                # TODO: revisit this
                if (not any(self.force_overwrite.values())) \
                        and (self.job_id is not None or any(self.progress.values() > 0)):
                    # If our stages have been run (or running), and no force overwrite,
                    pre_action, pre_status = (
                        await self.probe_launch())
                    if pre_action != "submit":
                        print(
                            f"[remote-submit] stage {self.stage_list[0].name}: "
                            f"aborting sbatch; probe={pre_action} "
                            "(live jobs or inconclusive squeue)"
                        )
                        if pre_action == "completed":
                            #self.stage_state = "completed" # TODO: set all members to completed
                            self.progress = 1.0
                            return
                        self.job_id = pre_status.get("manager_state", {}).get("last_known_jobs", None)
                        return "reattach", pre_status

                # Finally, submit the job
                run_result = await _run_blocking(
                    workload_remote_local.submit_job,
                    self.seekrflow,
                    self.resource,
                    stage_names,
                    job_specs,
                    self.resolved_execution,
                )
                if not isinstance(run_result, dict):
                    raise RuntimeError(
                        f"Remote submit returned invalid payload: {run_result!r}"
                    )
                if run_result.get("success") is False:
                    raise RuntimeError(
                        f"Remote submit failed for stage {self.stage_list[0].name}: "
                        f"{run_result.get('error')}"
                    )
                if "success" not in run_result:
                    raise RuntimeError(
                        f"Remote submit returned payload without success flag: {run_result!r}"
                    )
                internal_id = run_result["internal_id"]
                job_id = run_result["job_id"]
                job_name = run_result["job_name"]
                for job_spec in job_specs:
                    job_spec.internal_id = internal_id
                self.internal_id = internal_id
                self.job_id = job_id
                self.job_name = job_name
                    
            except Exception as e:
                for stage_name in stage_names:
                    self.stage_state[stage_name] = "error"
                self.last_error = f"remote submit failed: {e}"
                # TODO: update control file here to set semaphore=wait
                self.control_file_semaphore = "wait"
                self.semaphore = "wait"
                self.manager_status = "failed"
                print(
                    f"[remote-submit] stage {stage_names[0]} submit failed; "
                    f"setting semaphore=wait. error={e}"
                )
                raise
            return

        self.task = run_stage(*self.dependency_tasks)
        await asyncio.sleep(0)

        @self.workflow_engine.function_task
        async def monitor_stage(task):
            await self._monitor_stage_loop()

        self.task = monitor_stage(self.task)
        await asyncio.sleep(0) # let monitor_stage's async_wrapper register
        return
    
    async def _monitor_stage_loop(self) -> None:
        """
        Monitor for transfer requests.
        """
        while True:
            if self.detached_requested:
                break
            if self.semaphore == "stop":
                break
            if self.transfer_requested:
                self.transfer_requested = False
                await _run_blocking(self.move_files, backwards=True)

            await asyncio.sleep(STATUS_WRITE_INTERVAL)

    def status_snapshot(self) -> dict:
        """
        Return the JSON-serializable status snapshot of this stage's current
        state.
        """
        process_info = None
        if self.process is not None:
            process_info = {
                "pid": self.process.pid,
                "alive": self.process.is_alive(),
                "exitcode": self.process.exitcode,
            }
        stages = {}
        for stage in self.stage_list:
            stages[stage.name] = {
                "name": stage.name,
                "index": stage.index,
                "stage_state": self.stage_state.get(stage.name, "unknown"),
                "force_overwrite": self.force_overwrite[stage.name],
                "benchmark_mode": self.benchmark_mode[stage.name],
                "semaphore": self.semaphore,
                "progress": self.progress.get(stage.name, {}),
            }
        return {
            "resource_name": self.resource.name if self.resource is not None else "local",
            "manager_status": self.manager_status,
            "dependency_indices": self.dependency_indices,
            "transfer_status": self.transfer_status,
            "transfer_direction": self.transfer_direction,
            "transfer_error": self.transfer_error,
            "transfer_relaunch_count": self.transfer_relaunch_count,
            "status_polled_at": self.status_polled_at,
            "running_start_time": self.running_start_time,
            "process": process_info,
            "internal_id": self.internal_id,
            "job_id": self.job_id,
            "job_name": self.job_name,
            "array_indices": self.array_indices,
            "stages": stages,
        }

    def kill(self) -> None:
        """
        Stop this stage's monitor loop and terminate any running job.
        Idempotent; safe to call on stages that never started.
        """
        self.semaphore = "stop"
        self.manager_status = "idle"
        # TODO: cancel local processes and remote jobs - handle this in a 
        #  more general way that works for both local and remote.
        return

    def has_running_task_or_process(self) -> bool:
        """
        Check if this stage has any running tasks or processes.
        """
        # TODO: see if the process is actually still running.
        return self.task is not None \
            or self.process is not None

    def is_terminal(self) -> bool:
        """
        Check if this stage is terminal.
        """
        if self.semaphore == "stop" and not self.has_running_task_or_process():
            return True
        if all(stage_state == "completed" for stage_state in self.stage_state.values()):
            return True
        return False

def _link_to_previous(prev_resolved, cur_resolved) -> bool:
    """
    Check if the current stage is linked to the previous stage.
    """
    if prev_resolved is None or cur_resolved is None:
        return False
    if cur_resolved.co_schedule_with == "predecessor":
        return True
    if prev_resolved.co_schedule_with == "successor":
        return True
    return False

@attrs.define
class SeekrPipeline:
    """
    The entire set of workflows for a seekr run.
    """
    # Constructor args (no defaults → required, in this order)
    systemrun: client_structures.SystemRun = attrs.field(repr=False)
    workflow_engine: WorkflowEngine = attrs.field(repr=False)
    
    # Derived / mutable state
    # TODO: store stage_names and tasks within the stage workflows?
    #   Make obtaining them a method?
    #stage_names: list[str] = attrs.field(factory=list)
    stage_workflows: list[StageWorkflow] \
        = attrs.field(factory=list, repr=False)
    stage_tasks: list = attrs.field(factory=list, repr=False)
    task_id_to_stage: dict = attrs.field(factory=dict, repr=False)
    telemetry: typing.Any = attrs.field(default=None, repr=False)
    #force_rerun_stages: set[str] = attrs.field(factory=set)
    #semaphore_overrides: dict[str, str] = attrs.field(factory=dict)
    #benchmark_stage: str | None = attrs.field(default=None)
    #keystrokes_enabled: bool = attrs.field(default=True)
    #batch_child_mode: bool = attrs.field(default=False)
    #local_slot_file: str | None = attrs.field(default=None)
    #orphan_detach: bool = attrs.field(default=True)
    #_input_buffer: str = attrs.field(default="", repr=False)
    _live_display: typing.Any = attrs.field(default=None, repr=False)
    detached_requested: bool = attrs.field(default=False, repr=False)
    shutting_down: bool = attrs.field(default=False, repr=False)
    _shutdown_task: asyncio.Task | None = attrs.field(
        default=None, repr=False)
    _stop_event: asyncio.Event | None = attrs.field(
        default=None, repr=False)
    #_batch_commands_offset: int = attrs.field(default=0, repr=False)
    #_foreign_writer_warned: bool = attrs.field(default=False, repr=False)
    #_hard_exit_armed: bool = attrs.field(default=False, repr=False)
    #_orphaned_since: float | None = attrs.field(default=None, repr=False)
    
    def __attrs_post_init__(
            self, 
            ) -> None:
        # Assign stage lists by resolved.co_schedule_with fields
        new_stage_lists:List[List[scales_base.Base_stage]] = []
        new_resolved_lists:List[List[structures.Resolved_execution]] = []
        current: List[scales_base.Base_stage] = []
        current_resolved: List[structures.Resolved_execution] = []
        for stage in self.systemrun.model.stages:
            try:
                resolved = client_validation.resolve_stage_execution(
                    self.systemrun.seekrflow.run_settings, stage.name, 
                    self.systemrun.seekrflow.workflow.procedure)
            except ValueError:
                resolved = None
            if current and _link_to_previous(prev_resolved, resolved):
                current.append(stage)
                current_resolved.append(resolved)
            else:
                if current:
                    new_stage_lists.append(current)
                    new_resolved_lists.append(current_resolved)
                current = [stage]
                current_resolved = [resolved]
            prev_resolved = resolved
        if current:
            new_stage_lists.append(current)
            new_resolved_lists.append(current_resolved)

        for stage_list, resolved_list \
                in zip(
                    new_stage_lists, new_resolved_lists):
            self.add_stage_list(stage_list, resolved_list)
            
    
    def add_stage_list(
            self, 
            stage_list: List[scales_base.Base_stage],
            resolved_list: List[structures.Resolved_execution]
            ) -> None:
        """
        Add a seekr stage to the pipeline.
        """
        group_resource_name = None
        resource = None
        for resolved in resolved_list:
            resource_name = "local" if resolved is None else resolved.resource_name
            if resource_name == "local":
                resource = None
            else:
                resource = self.systemrun.seekrflow.run_settings.get_resource_by_name(resource_name)
            if group_resource_name is None:
                group_resource_name = resource_name
            elif group_resource_name != resource_name:
                raise ValueError(
                    f"All stages in a group must have the same resource: "\
                    f"{group_resource_name} != {resource_name}")
        stage_workflow = StageWorkflow(
            self.systemrun.model, self.systemrun.seekrflow, stage_list, self.workflow_engine, resource)
        #TODO: handle telemetry_poll_interval
        if resolved is not None:
            stage_workflow.resolved_execution = resolved
            if resolved.resource is not None:
                stage_workflow.resource = resolved.resource
        stage_workflow_semaphore = None
        for stage in stage_list:
            stage_workflow.force_overwrite[stage.name] \
                = stage.name in self.systemrun.force_rerun_stages
            stage_workflow.benchmark_mode[stage.name] \
                = (stage.name == self.systemrun.benchmark_stage)
            stage_semaphore = self.systemrun.semaphore_dict.get(stage.name, "go")
            if stage_workflow_semaphore is None:
                stage_workflow_semaphore = stage_semaphore
            elif stage_workflow_semaphore != stage_semaphore:
                    raise ValueError(
                        f"All stages in a group must have the same semaphore: "\
                        f"{stage_workflow_semaphore} != {stage_semaphore}")
        if stage_workflow_semaphore is None:
            stage_workflow_semaphore = "go"
        stage_workflow.semaphore = stage_workflow_semaphore
        self.stage_workflows.append(stage_workflow)
    
    def _on_signal(self) -> None:
        """ Signal-safe callback: schedule async shutdown. """
        if self.shutting_down:
            print("[shutdown] forcing immediate exit")
            os._exit(1)
        if self._shutdown_task is not None and not self._shutdown_task.done():
            print("[shutdown] forcing immediate exit")
            os._exit(1)
        print("\n\n=== Received interrupt signal ===")
        self._shutdown_task = asyncio.create_task(self.shutdown())

    def _cancel_all_stages(self) -> None:
        """
        Cancel all stages in the pipeline.
        """
        for stage_workflow in self.stage_workflows:
            stage_workflow.kill()
        return

    async def shutdown(self) -> None:
        """
        Kill all stages and shut down the workflow engine.
        """
        if self.shutting_down:
            return
        self.shutting_down = True
        timed_out = False
        loop = asyncio.get_running_loop()
        try:
            await asyncio.wait_for(
                loop.run_in_executor(None, self._cancel_all_stages),
                timeout=SHUTDOWN_CANCEL_TIMEOUT,
            )
        except asyncio.TimeoutError:
            timed_out = True
            print(
                f"[shutdown] remote job cancellation timed out after "
                f"{SHUTDOWN_CANCEL_TIMEOUT}s (endpoint unreachable?); "
                f"abandoning remote cancels and exiting")
        if self._stop_event is not None:
            self._stop_event.set()
        # Capture terminal state before tearing down the engine.
        #self.write_status_snapshot()
        try:
            await asyncio.wait_for(
                self.workflow_engine.shutdown(),
                timeout=ENGINE_SHUTDOWN_TIMEOUT,
            )
        except asyncio.TimeoutError:
            timed_out = True
            print(
                f"[shutdown] workflow engine shutdown timed out after "
                f"{ENGINE_SHUTDOWN_TIMEOUT}s; forcing exit")
        if timed_out:
            os._exit(1)
        return

    async def detach_shutdown(self) -> None:
        """
        Soft shutdown for detach: stop local monitoring/UI loops and close
        the workflow engine, but do not kill running stage processes.
        """
        if self.shutting_down:
            return
        self.shutting_down = True
        if self._stop_event is not None:
            self._stop_event.set()
        #self.write_status_snapshot()
        await self.workflow_engine.shutdown()
        return

    def status_snapshot(self) -> dict:
        """
        Get the current status snapshot of the pipeline.
        """
        root = self.systemrun.model.directory
        now = time.time()
        stage_workflows = {
            sw_index: sw.status_snapshot() \
                for sw_index, sw in enumerate(self.stage_workflows)
        }
        return {
            "schema_version": STATUS_SCHEMA_VERSION,
            "timestamp": now,
            "timestamp_iso": datetime.datetime.fromtimestamp(
                now, tz=datetime.timezone.utc).isoformat(),
            "pid": os.getpid(),
            "work_directory": self.systemrun.seekrflow.work_directory,
            "root_directory": root,
            "shutting_down": self.shutting_down,
            "detached_requested": self.detached_requested,
            "stage_workflows": stage_workflows,
        }

    async def run_workflows(self) -> None:
        """
        Run the workflows for the entire pipeline.
        """
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._on_signal)
        try:
            await self._run_workflows_body()
        finally:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)

    async def _run_workflows_body(self):
        """
        Run the workflows for the entire pipeline.
        This involves:
        - Check for detach or stop conditions
        - For each stage workflow
            - Check for remaining dependency tasks
            - Launch the create_tasks

        """
        #stage_by_name = {sw.stages.name: sw for sw in self.stage_workflows}
        stage_workflow_by_name = {}
        for stage_workflow in self.stage_workflows:
            for stage in stage_workflow.stage_list:
                stage_workflow_by_name[stage.name] = stage_workflow

        def stages_dependencies_satisfied(stage_workflow: StageWorkflow) -> bool:
            """
            Check if all stages in this stage_workflow have their dependencies satisfied.
            """
            for stage in stage_workflow.stage_list:
                dep_index = stage.input_stage_index
                if dep_index > 0:
                    dep_stage = self.systemrun.model.stages[dep_index - 1]
                    sw = stage_workflow_by_name[dep_stage.name]
                    if sw.stage_state[dep_stage.name] != "completed":
                        return False
            return True

        # TODO: these become session-level, not system level
        stop_event = asyncio.Event()
        self._stop_event = stop_event
        try:
            while True:
                if self.detached_requested or stop_event.is_set():
                    break
                all_terminal = True
                for stage_workflow in self.stage_workflows:
                    # If we need to detach or kill the client
                    if self.detached_requested or stop_event.is_set():
                        all_terminal = True
                        break
                    
                    # If all stage semaphores in this stage_workflow are stopped
                    if stage_workflow.semaphore == "stop":
                        stage_workflow.kill()
                        continue

                    # If stage dependencies are not satisfied
                    if not stages_dependencies_satisfied(stage_workflow):
                        all_terminal = False
                        continue

                    # If stage_workflow.stage_state is "completed" for all stages
                    if all(stage_workflow.stage_state[stage.name] == "completed" \
                            for stage in stage_workflow.stage_list):
                        continue

                    # If a stage in the workflow process or task is already running
                    if stage_workflow.has_running_task_or_process():
                        all_terminal = False
                        continue

                    prior_state = stage_workflow.manager_status
                    if prior_state in {
                           "idle", "failed", "unstarted", "unknown", "queued", "started"} \
                            and stage_workflow.semaphore == "go":
                        # Nothing is currently running:
                        # (Re)launch stage when permitted by semaphore and deps.
                        stage_workflow.dependency_indices = []
                        stage_workflow.dependency_tasks = []
                        stage_workflow.tasks = {stage.name:None for stage \
                            in stage_workflow.stage_list}
                        stage_workflow.processes = None
                        if prior_state != "failed":
                            stage_workflow.manager_status = "idle"

                        # Transfer from remote if necessary
                        upstream_idx = getattr(
                            stage_workflow.stage, "input_stage_index", 0)
                        stage_workflow.transfer_from = None
                        if upstream_idx and upstream_idx > 0:
                            upstream_sw = next(
                                (sw for sw in self.stage_workflows
                                 if sw.stage.index == upstream_idx),
                                None,
                            )
                            if (upstream_sw is not None
                                    and upstream_sw.resource_name
                                    != stage_workflow.resource_name):
                                stage_workflow.transfer_from = \
                                    upstream_sw.resource_name

                        # Handle remote jobs
                        #   Launch a probe to determine what to do with the remote job
                        #      
                        #   Handle force-overwrite for the probe
                        #   Skip the probe if force_overwrite is True
                        #   If not skipping the probe:
                        #     decide whether to resume a probe
                        #   Depending on what the probe returns:
                        #     if "completed", set stage_workflows.state to "completed"
                        #       and full progress.
                        #     if "defer", set stage_workflow to "queued" and continue
                        #     if "reattach", get the manager status from probe
                        #       update status_polled_at, create monitor-only task,
                        #       and add it to launched_tasks and to self.stage_tasks
                        # Handle local jobs
                        #   check for any existing local processes.
                        #   based on this, set starting states and managers.

                        await stage_workflow.create_tasks()
                        if len(stage_workflow.tasks) > 0:
                            self.stage_tasks += stage_workflow.tasks.values()
                        all_terminal = False
                        continue

                    if not stage_workflow.is_terminal():
                        all_terminal = False

                if all_terminal:
                    break

                await asyncio.sleep(MAIN_LOOP_INTERVAL)

        finally:
            stop_event.set()

        if (not self.detached_requested) and len(self.stage_tasks) > 0:
            await asyncio.gather(*self.stage_tasks, return_exceptions=True)

        if self.detached_requested:
            await self.detach_shutdown()
        elif self._shutdown_task is not None:
            await self._shutdown_task
        else:
            await self.shutdown()

async def assign_backend_workflow_engine_to_session(
        session: client_structures.RunSession,
    ) -> None:
    """
    Assign a backend and workflow engine to the session.
    """
    backend = await LocalExecutionBackend(ThreadPoolExecutor())
    workflow_engine = await WorkflowEngine.create(backend=backend)
    session.backend = backend
    session.workflow_engine = workflow_engine
    return

async def monitor_session_pipelines(
        pipelines: list[SeekrPipeline]
    ) -> None:
    """
    Monitor the session pipelines.
    """
    stop = False
    while not stop:
        # Make status payload for all pipelines in the session
        ran_nothing = True
        status_payloads_by_resource = {}
        resource_dict = {}
        # Initialize the dicts first
        for pipeline in pipelines:
            system_name = pipeline.systemrun.name
            if pipeline.shutting_down:
                continue
            ran_nothing = False
            for sw in pipeline.stage_workflows:
                if sw.internal_id is None:
                    continue
                if sw.resource is None:
                    resource_name = "local"
                    root_directory = sw.model.directory
                else:
                    resource_name = sw.resource.name
                    root_directory = os.path.join(
                        sw.resource.remote_working_directory, sw.seekrflow.name)

                if resource_name not in resource_dict:
                    resource_dict[resource_name] = sw.resource
                if resource_name not in status_payloads_by_resource:
                    status_payloads_by_resource[resource_name] = {}
                if system_name not in status_payloads_by_resource[resource_name]:
                    status_payloads_by_resource[resource_name][system_name] = {}
                    status_payloads_by_resource[resource_name][system_name]["root_dir"] \
                        = root_directory
                    status_payloads_by_resource[resource_name][system_name]["jobs"] = []

                job_dict = {}
                job_dict["internal_id"] = sw.internal_id
                job_dict["job_id"] = sw.job_id
                job_dict["job_name"] = sw.job_name
                job_dict["stage_indices"] = [stage.index for stage in sw.stage_list]
                job_dict["array_indices"] = sw.array_indices
                status_payloads_by_resource[resource_name][system_name]["jobs"]\
                    .append(job_dict)

        for resource_name, payload in status_payloads_by_resource.items():
            resource = resource_dict[resource_name]
            status = None
            try:
                status = await _run_blocking(
                    workload_remote_local.status,
                    resource,
                    manager_payload=payload,
                )
            except Exception as e:
                print(
                    f"[remote-status] {resource_name}: "
                    f"status call raised an exception: {e}"
                )
                await asyncio.sleep(STATUS_WRITE_INTERVAL)
                continue
            if status is not None:
                return_payload = status["payload"]
                for pipeline in pipelines:
                    system_name = pipeline.systemrun.name
                    system_payload = return_payload.get(system_name, None)
                    if system_payload is None:
                        continue
                    for sw in pipeline.stage_workflows:
                        job_dict = system_payload.get(sw.job_id, None)
                        if job_dict is None:
                            continue
                        slurm_dicts_by_array_index = job_dict["slurm_dicts_by_array_index"]
                        slurm_state_set = set()
                        for array_index, slurm_dict in slurm_dicts_by_array_index.items():
                            slurm_state_set.add(slurm_dict["state"])
                            #slurm_last_known_elapsed = slurm_dict["last_known_elapsed"]
                            #slurm_last_known_jobs = slurm_dict["last_known_jobs"]
                        if len(slurm_state_set) == 0:
                            sw.manager_status = "idle"
                        else:
                            if "running" in slurm_state_set and (("queued" in slurm_state_set) or ("pending" in slurm_state_set)):
                                sw.manager_status = "queued/running"
                            elif "running" in slurm_state_set:
                                sw.manager_status = "running"
                            elif ("queued" in slurm_state_set) or ("pending" in slurm_state_set):
                                sw.manager_status = "queued"
                            else:
                                sw.manager_status = "idle"

                        stage_dicts_by_stage_index = job_dict["stage_dicts_by_stage_index"]
                        for stage in sw.stage_list:
                            stage_index = stage.index
                            stage_dict = stage_dicts_by_stage_index.get(stage_index, None)
                            if stage_dict is None:
                                continue
                            stage_state = stage_dict["state"]
                            stage_finished = stage_dict["finished"]
                            stage_anchor_swarm_progress_list \
                                = stage_dict["stage_anchor_swarm_progress_list"]
                            sw.stage_state[stage.name] = stage_state
                            sw.progress[stage.name] = {}
                            if stage_anchor_swarm_progress_list is not None:
                                for key in stage_anchor_swarm_progress_list:
                                    key_stage_index, key_anchor, key_swarm_id, progress = key
                                    progress_key = f"anchor_{key_anchor}_swarm_{key_swarm_id}"
                                    if key_stage_index == stage_index:
                                        sw.progress[stage.name][progress_key] = progress
        if ran_nothing:
            stop = True
            return
        await asyncio.sleep(STATUS_WRITE_INTERVAL)
        
    return

async def status_writer_loop(
        output_file: str,
        pipelines: list[SeekrPipeline],
        ) -> None:
    """
    Periodically write the status snapshot until stop_event is set.
    """
    stop = False
    while not stop:
        session_statuses = {}
        all_shutting_down = True
        for pipeline in pipelines:
            pipeline_status = pipeline.status_snapshot()
            session_statuses[pipeline.systemrun.name] = pipeline_status
            if not pipeline_status.get("shutting_down", False):
                all_shutting_down = False
        if all_shutting_down:
            stop = True
            
        output_file_tmp = output_file + ".tmp"
        with open(output_file_tmp, "w") as f:
            json.dump(session_statuses, f)
        os.rename(output_file_tmp, output_file)
        
        await asyncio.sleep(STATUS_WRITE_INTERVAL)

async def control_file_reader_loop(
        control_file: str,
        pipelines: list[SeekrPipeline],
    ) -> None:
    """
    Read the control file and update the pipelines.
    """
    stop = False
    lock_path = control_file + ".lock"
    while not stop:
        all_shutting_down = True
        made_changes = False
        with open(lock_path, "a+") as lockf:
            # Need to apply a lock to prevent the control file from being read 
            # and written to concurrently by the GUI program and the client.
            fcntl.flock(lockf, fcntl.LOCK_EX)
            try:
                with open(control_file, "r") as f:
                    control_dict = json.load(f)
                pending = []
                for pipeline in pipelines:
                    if pipeline.shutting_down:
                        continue
                    all_shutting_down = False
                    system_name = pipeline.systemrun.name
                    system_control = control_dict[system_name]
                    detach = system_control["detach"]
                    if detach:
                        pipeline.detached_requested = True
                    for sw in pipeline.stage_workflows:
                        stage_workflow_control = system_control["stage_workflows"][sw.stage_list[0].name]
                        #sw.semaphore = stage_workflow_control["semaphore"]
                        transfer = stage_workflow_control["transfer"]
                        if transfer and not sw.transfer_requested and sw.transfer_status != "running":
                            sw.transfer_requested = True
                        if sw.control_file_semaphore is not None:
                            system_control["stage_workflows"][sw.stage_list[0].name]["semaphore"] \
                                = sw.control_file_semaphore
                            sw.semaphore = sw.control_file_semaphore
                            pending.append((sw, sw.control_file_semaphore))
                            made_changes = True
                        else:
                            sw.semaphore = stage_workflow_control["semaphore"]
                    
                    control_dict[system_name] = system_control
                
                if made_changes:
                    tmp_file = control_file + ".tmp"
                    with open(tmp_file, "w") as f:
                        json.dump(control_dict, f, indent=2)
                    os.rename(tmp_file, control_file)
                    for sw, value in pending:
                        if sw.control_file_semaphore == value:
                            sw.control_file_semaphore = None
            finally:
                fcntl.flock(lockf, fcntl.LOCK_UN)

        if all_shutting_down:
            stop = True
        await asyncio.sleep(CONTROL_FILE_READ_INTERVAL)
    return

def initialize_control_file(
        control_file: str,
        pipelines: list[SeekrPipeline],
    ) -> None:
    """
    Initialize the control file to have all "go" semaphores and no detach 
    or transfer upon client start.
    """
    control_dict = {}
    for pipeline in pipelines:
        system_name = pipeline.systemrun.name
        control_dict[system_name] = {"detach": False, "stage_workflows": {}}
        for sw in pipeline.stage_workflows:
            stage_workflow_control = {
                "semaphore": sw.semaphore,
                "transfer": False,
            }
            control_dict[system_name]["stage_workflows"][sw.stage_list[0].name] \
                = stage_workflow_control
    lock_path = control_file + ".lock"
    with open(lock_path, "a+") as lockf:
        fcntl.flock(lockf, fcntl.LOCK_EX)
        try:
            tmp_file = control_file + ".tmp"
            with open(tmp_file, "w") as f:
                json.dump(control_dict, f, indent=2)
            os.rename(tmp_file, control_file)
        finally:
            fcntl.flock(lockf, fcntl.LOCK_UN)
    return

async def launch_session_pipelines(
        session: client_structures.RunSession,
        ) -> None:
    """
    Launch a session pipeline.
    """
    await assign_backend_workflow_engine_to_session(session)
    pipelines = [
        SeekrPipeline(systemrun=systemrun, workflow_engine=session.workflow_engine)
        for systemrun in session.systemrun_objects
    ]
    initialize_control_file(session.control_file, pipelines)
    try:
        await asyncio.gather(
            control_file_reader_loop(session.control_file, pipelines),
            status_writer_loop(session.output_file, pipelines),
            monitor_session_pipelines(pipelines),
            *(pipeline.run_workflows() for pipeline in pipelines),
        )
    finally:
        await session.workflow_engine.shutdown()
    return