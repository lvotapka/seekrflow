"""
prepare.py

Prepare a batch of seekrflow workflows.
"""

import os
import argparse

import seekrflow.modules.structures as structures
import seekrflow.modules.seekr_input as seekr_input
import seekrflow.modules.client.structures as client_structures
import seekrflow.modules.client.start as client_start

def prepare(
        batch: client_structures.Batch,
        force_targets: list[str] | None = None,
        skip_checks: bool = False) -> None:
    """
    Prepare a batch of seekrflow workflows.
    """
    curdir = os.getcwd()
    if batch.batch_directory is not None:
        os.chdir(batch.batch_directory)
    for seekrflow, batch_system in zip(batch.create_seekrflow_objects(), batch.systems):
        force_overwrite = (
            force_targets is not None
            and ("*" in force_targets or batch_system.name in force_targets)
        )
        if not batch_system.skip:
            seekr_input.prepare_model(
                seekrflow, force_overwrite=force_overwrite,
                skip_checks=skip_checks)
    
    os.chdir(curdir)
    return

def main():
    parser = argparse.ArgumentParser(
        description="Prepare a batch of seekrflow workflows")
    parser.add_argument(
        "input_json", metavar="INPUT_JSON", type=str,
        help="Input JSON file containing the job specification.")
    parser.add_argument(
        "-f", "--force_overwrite", dest="force_overwrite", metavar="SPEC",
        type=str, nargs="*", default=None,
        help="Force-rerun the given system targets. Each SPEC is "
        "'SYSTEM'. Pass -f with no SPEC to force-rerun everything. "
        "Examples: -f system1 | -f ")
    parser.add_argument(
        "-s", "--skip_checks", dest="skip_checks", default=False,
        help="By default, pre-simulation checks will be run after the "
        "preparation is complete, and if the checks fail, the SEEKR "
        "model will not be saved. This argument bypasses those "
        "checks and allows the model to be generated anyways.",
        action="store_true")

    args = vars(parser.parse_args())
    input_json = args.get("input_json")
    force_overwrite = args.get("force_overwrite")
    skip_checks = args.get("skip_checks")

    force_targets = None
    if force_overwrite is not None:
        if not force_overwrite:
            force_targets = [("*")]
        else:
            force_targets = [t for t in force_overwrite]

    if client_start.input_is_batch_file(input_json):
        batch = client_structures.Batch.load_batch_file(input_json)
    else:
        seekrflow = structures.load_seekrflow(input_json)
        work_dir = os.path.abspath(
            os.path.expanduser(seekrflow.work_directory))
        seekrflow.make_work_directory(work_dir)
        batch = client_start.make_batch_from_single_seekrflow(seekrflow)
    prepare(batch, force_targets=force_targets, skip_checks=skip_checks)

if __name__ == "__main__":
    main()