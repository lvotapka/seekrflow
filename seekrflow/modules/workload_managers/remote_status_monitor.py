"""
modules/workload_managers/remote_status_monitor.py

Pass this script into a slurm/pbs/remote job to monitor the seekr 
calculation's progress. 
"""

import json
import pathlib
import argparse
import subprocess
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict

@dataclass
class StageInfo:
    internal_id: int
    stage_index: int
    anchor_indices: Optional[List[int]]
    swarm_indices: Optional[List[str]]
    filename: str
    slurm_info_filename: str
    stage_state_filename: str

    def save(
            self, 
            root_dir_path: pathlib.Path | None = None, 
            path: pathlib.Path | None = None) -> None:
        if path is None:
            if self.filename is None:
                raise ValueError("StageInfo filename is required.")
            self.filename = pathlib.Path(self.filename).name
            path_basename = pathlib.Path(self.filename)
            path = root_dir_path / ".stage_states" / path_basename
        else:
            path_basename = path.name
            self.filename = str(path_basename)
        # Creates root_dir/.stage_states; fails if root_dir is missing
        path.parent.mkdir(exist_ok=True)
        with open(path, "w") as f:
            json.dump(asdict(self), f, indent=2)

    @staticmethod
    def load(path: pathlib.Path) -> "StageInfo":
        with open(path, "r") as f:
            return StageInfo(**json.load(f))

@dataclass
class StageState:
    internal_id: int
    state: str #'unknown', 'unstarted', 'started', 'completed', 'error'
    finished: bool
    progress: Optional[float] = None
    stage_anchor_swarm_progress_dict: Optional[Dict[Tuple[int, int, int], float]] = None
    notes: Optional[str] = None
    filename: Optional[str] = None
    stage_info_filename: Optional[str] = None

    def save(
            self, 
            root_dir_path: pathlib.Path | None = None, 
            path: pathlib.Path | None = None) -> None:
        if path is None:
            if self.filename is None:
                raise ValueError("StageState filename is required.")
            self.filename = pathlib.Path(self.filename).name
            path_basename = pathlib.Path(self.filename)
            path = root_dir_path / ".stage_states" / path_basename
        else:
            path_basename = path.name
            self.filename = str(path_basename)
        # Creates root_dir/.stage_states; fails if root_dir is missing
        path.parent.mkdir(exist_ok=True)
        with open(path, "w") as f:
            json.dump(asdict(self), f, indent=2)

    @staticmethod
    def load(path: pathlib.Path) -> "StageState":
        with open(path, "r") as f:
            return StageState(**json.load(f))

@dataclass
class StageBundleState:
    internal_id: int
    filename: str
    stage_info_filenames: List[str]
    stage_state_filenames: List[str]
    state: str #'unknown', 'unstarted', 'started', 'completed', 'error'
    finished: bool
    progress: Optional[float] = None

@dataclass
class TelemetryState:
    task_status: Dict[str, str] # taskid -> status
    cpu_usage: Dict[str, float] # taskid -> cpu usage
    memory_usage: Dict[str, float] # taskid -> memory usage
    # TODO: more here?
    filename: Optional[str] = None

def run(cmd: List[str], check: bool = True) -> tuple:
    out = subprocess.run(cmd, stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE, text=True)
    if check and out.returncode != 0:
        raise RuntimeError(f"Command failed: ({{out.returncode}}): "
                            f"{{' '.join(cmd)}}\nSTDERR:\n{{out.stderr}}")
    return out.returncode, out.stdout.strip(), out.stderr.strip()

def main():
    parser = argparse.ArgumentParser(description="Monitor the SLURM job.")
    parser.add_argument(
        "-r", "--root-dir", type=str, required=True,
        help="The root directory of the seekr calculation.")
    parser.add_argument(
        "-I", "--internal-id", type=int, required=True, default=None,
        help="The internal ID of the seekr calculation.")
    parser.add_argument(
        "-S", "--stage-index", type=int, required=True, default=None,
        help="The stage index to monitor.")
    parser.add_argument(
        "-i", "--info-args", type=str, required=False, default=None,
        help="A JSON string of the arguments to be updated within the slurm info file.")
    parser.add_argument(
        "-s", "--state-args", type=str, required=False, default=None,
        help="A JSON string of the arguments to be updated within the slurm state file.")
    args = parser.parse_args()
    root_dir = args.root_dir
    internal_id = args.internal_id
    stage_index = args.stage_index
    info_args = args.info_args
    state_args = args.state_args

    root_dir_path = pathlib.Path(root_dir)
    model_filename = root_dir_path / "model.json"
    assert model_filename.exists(), f"Model file does not exist: {model_filename}"
    stage_info_basename = f"stage_info_{internal_id}_{stage_index}.json"
    stage_state_basename = f"stage_state_{internal_id}_{stage_index}.json"

    stage_info_path = root_dir_path / ".stage_states" / stage_info_basename
    stage_state_path = root_dir_path / ".stage_states" / stage_state_basename
    if stage_info_path.exists():
        stage_info = StageInfo.load(stage_info_path)
        if info_args is not None:
            stage_info.update(**json.loads(info_args))
    else:
        assert info_args is not None, "Stage info file does not exist and info args are not provided."
        stage_info = StageInfo(**json.loads(info_args))
    
    if stage_state_path.exists():
        stage_state = StageState.load(stage_state_path)
        if state_args is not None:
            stage_state.update(**json.loads(state_args))
    else:
        assert state_args is not None, "Stage state file does not exist and state args are not provided."
        stage_state = StageState(**json.loads(state_args))

    anchor_indices = stage_info.anchor_indices
    swarm_indices = stage_info.swarm_indices
    
    stage_error_notes = ""
    try:
        import seekr.modules.structures as structures
        import seekr.status as seekr_status
        model = structures.load_model(str(model_filename))
        instruction = "progress"
        arg_pairs = []
        if (anchor_indices is None) and (swarm_indices is None):
            arg_pairs = [("any", None)]
        elif anchor_indices is not None:
            if swarm_indices is None:
                for anchor in anchor_indices:
                    arg_pairs.append((anchor, None))
            else:
                for anchor in anchor_indices:
                    for swarm_id in swarm_indices:
                        arg_pairs.append((anchor, swarm_id))
        else:
            # Then anchor_indices is None, but swarm_indices is not
            for swarm_id in swarm_indices:
                arg_pairs.append(("any", swarm_id))

        stage_progress = 0.0
        num_progress_entries = 0
        stage_anchor_swarm_progress_dict = {{}}
        for arg_pair in arg_pairs:
            anchor, swarm_id = arg_pair
            message_dict = seekr_status.status(
                model,
                instruction,
                stage_arg=stage_info.stage_index,
                anchor_arg=anchor,
                swarm_id=swarm_id,
                print_json=True,
            )
            # NOTE: change from 'progress' to 'state'
            stage_state_dict = message_dict.get("progress", {}).get(
                stage_info.stage_index, {})
            stage_finished = stage_state_dict.get("finished", False)
            stage_anchor_dict = stage_state_dict.get("progress", {}) # TODO: will be "anchors"
            for anchor_id, anchor_state_dict in stage_anchor_dict.items():
                for swarm_id, swarm_state_dict in anchor_state_dict.get("swarms", {}).items():
                    swarm_progress = swarm_state_dict.get("progress", 0.0)
                    num_progress_entries += 1
                    stage_progress += swarm_progress
                    stage_anchor_swarm_key = (stage_info.stage_index, anchor_id, swarm_id)
                    stage_anchor_swarm_progress_dict[stage_anchor_swarm_key] = swarm_progress

        if num_progress_entries > 0:
            stage_progress = stage_progress / num_progress_entries
        else:
            stage_progress = 0.0
        stage_state.progress = stage_progress
        stage_state.finished = stage_finished
        if stage_finished:
            stage_state.state = "completed"
        elif stage_progress > 0.0:
            stage_state.state = "started"
        else:
            stage_state.state = "unstarted"

        stage_state.stage_anchor_swarm_progress_dict = stage_anchor_swarm_progress_dict
        stage_state.save()
    except Exception as e:
        stage_error_notes += f"Error in stage {{stage_info.stage_index}}: {{e}}\n"
    #if stage_error_notes:
    #    error_notes += f"Stage {{stage_info.stage_index}} error notes: {{stage_error_notes}}\n"
    stage_state.notes = stage_error_notes

    stage_info.save()
    stage_state.save()

if __name__ == '__main__':
    main()