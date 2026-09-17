"""
client.py

Orchestrate seekrflow single or batch runs by launching workflows,
monitoring progress and statuses, perform transfers, produce output
for reporting, and take input from semaphores or other commands to
direct the workflows.
"""

import os
import argparse
from typing import List, Dict

import seekrflow.modules.structures as structures
import seekrflow.modules.client.structures as client_structures
import seekrflow.modules.client.start as client_start

def client(
        session: client_structures.RunSession,
        output_json: str,
        transfer_from_remote_only: str,
        force_rerun: List[str],
        benchmark: str,
        semaphore_dict: Dict[str, str]) -> None:
    print("mark10")
    for seekrflow in session.seekrflow_objects:
        client_start.validate_run_settings(seekrflow)

def main():
    parser = argparse.ArgumentParser(
        description="Run a single or batch of seekrflow workflows")
    parser.add_argument(
        "-i", "--input-json", dest="input_json", metavar="INPUT_JSON",
        help="Input JSON file containing the job specification")
    parser.add_argument(
        "-m", "--model_filename", dest="model_filename",
        metavar="MODEL_FILENAME", type=str, default="",
        help="Path to the model file to use for the simulation. This activates "
        "the so-called 'hotshot mode', where an existing model.xml file can be "
        "run easily without needing to prepare a full seekrflow structure. Note "
        "that if this is provided, the seekrflow JSON file must not be provided.")
    parser.add_argument(
        "-n", "--name", dest="name",
        metavar="NAME", type=str, default="",
        help="Name for the simulation or calculation. This is particularly "
        "useful in hotshot mode to give a more informative name to the run - "
        "otherwise, it will be named 'hotshot_<inode>_<device>'.")
    parser.add_argument(
        "-o", "--output-json", dest="output_json", metavar="OUTPUT_JSON",
        help="Output JSON file containing internal states of jobs")
    parser.add_argument(
        "-T", "--transfer-from-remote-only", dest="transfer_from_remote_only", 
        metavar="STAGE", type=str, default=None,
        help="Pull files from remote for STAGE (or 'all') to the local system"
        "without starting monitors or submitting jobs. Stop a live batch "
        "run first if a child still owns the work directory.")
    parser.add_argument(
        "-f", "--force_rerun", dest="force_rerun", metavar="STAGE",
        type=str, nargs="*", default=None,
        help="If set, force re-run of stages. Provide a space-separated list "
        "of stage names (e.g. -f bd mmvt) to force-rerun only those stages, "
        "or pass -f with no arguments to force-rerun all stages. Any running "
        "processes for the affected stages will be killed and the stages "
        "restarted with force_overwrite=True.")
    parser.add_argument(
        "-b", "--benchmark", dest="benchmark",
        metavar="STAGE", type=str, default=None,
        help="If set, run the named stage in benchmark mode - a quick run on "
        "the resource to get approximate timings. At most, a single "
        "stage can be benchmarked per invocation, since dependent stages "
        "would not produce the outputs needed downstream. Default: None.")
    parser.add_argument(
        "--semaphore", dest="semaphore",
        metavar="STAGE_CONTROL", type=str, default=None,
        help="Control stage execution. Format: 'stage:value,stage:value,...' "
        "where stage is any configured stage name and value is go/wait/stop. "
        "go: normal operation (default), wait: don't submit new jobs but let "
        "running jobs finish, stop: don't submit new jobs AND kill any running "
        "jobs. Example: --semaphore stage1:stop,stage2:wait.")
    #parser.add_argument(
    #    "-O", "--output-socket", dest="output_socket", metavar="OUTPUT_SOCKET",
    #    help="Output socket number for internal state updates"
    #)
    args = vars(parser.parse_args())
    input_json = args.get("input_json")
    model_filename = args.get("model_filename")
    name = args.get("name")
    output_json = args.get("output_json")
    transfer_from_remote_only = args.get("transfer_from_remote_only")
    force_rerun = args.get("force_rerun")
    benchmark = args.get("benchmark")

    semaphore_dict = {}
    if args["semaphore"]:
        for item in args["semaphore"].split(","):
            parts = item.strip().split(":")
            if len(parts) != 2:
                raise ValueError(
                    f"Invalid semaphore format: {item}. Expected 'stage:value'")
            stage, value = parts[0].strip(), parts[1].strip()
            if value not in ["go", "wait", "stop"]:
                raise ValueError(
                    f"Invalid semaphore value: {value}. Must be go, wait, or stop")
            semaphore_dict[stage] = value

    if force_rerun is not None:
        stages_to_check = force_rerun if force_rerun \
            else list(semaphore_dict.keys())
        for stage in stages_to_check:
            if semaphore_dict.get(stage) == "stop":
                raise ValueError(
                    f"Conflicting options: --force-rerun {stage} and "
                    f"--semaphore {stage}:stop. "
                    "Cannot force rerun a stopped stage.")

    if benchmark is not None:
        stages_to_check = benchmark if benchmark \
            else list(semaphore_dict.keys())
        for stage in stages_to_check:
            if semaphore_dict.get(stage) == "stop":
                raise ValueError(
                    f"Conflicting options: --benchmark {stage} and "
                    f"--semaphore {stage}:stop. "
                    "Cannot benchmark a stopped stage.")

    hotshot_mode = False
    if input_json == "":
        seekrflow = structures.Seekrflow()
        assert model_filename != "", \
            "Model filename must be provided in hotshot mode."
        hotshot_mode = True
        assert os.path.exists(model_filename), \
            f"Model file {model_filename} does not exist."
        if name != "":
            seekrflow.name = name
        else:
            model_dirname = os.path.dirname(model_filename)
            if model_dirname == "":
                model_dirname = os.path.abspath(os.curdir)
            st = os.stat(model_dirname)
            seekrflow.name = "hotshot_" + str(st.st_dev) + "_" + str(st.st_ino)
        print("Running in hotshot mode with seekrflow name:", seekrflow.name)
        seekrflow.work_directory = None
        seekrflow.root_directory = os.path.dirname(
            os.path.abspath(model_filename))
        structures.try_to_load_resources_json(seekrflow)
        session = client_start.make_session_from_single_seekrflow(seekrflow)

    else:
        assert model_filename == "", \
            "If input JSON file is provided, model filename must not be provided."
        assert os.path.exists(input_json), \
            f"Input JSON file {input_json} does not exist."
        
        # TODO: load either the seekrflow JSON or the batch JSON

        if client_start.input_is_batch_file(input_json):
            session = client_start.make_session_from_batch_file(input_json)
        else:
            seekrflow = structures.load_seekrflow(input_json)
            if name != "":
                seekrflow.name = name
            work_dir = os.path.abspath(
                os.path.expanduser(seekrflow.work_directory))
            seekrflow.make_work_directory(work_dir)
            session = client_start.make_session_from_single_seekrflow(seekrflow)
        
    client(session, output_json,transfer_from_remote_only, force_rerun, 
           benchmark, semaphore_dict)

if __name__ == "__main__":
    main()