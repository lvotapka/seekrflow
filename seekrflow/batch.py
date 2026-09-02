"""
batch.py

Coordinate parameterization, preparation, running, and analysis of many
seekrflow systems from a single batch JSON definition.
"""

from __future__ import annotations

import argparse
import sys

import seekrflow.modules.batch.structures as batch_structures
import seekrflow.modules.batch.coordinator as batch_coordinator


def main(argv: list[str] | None = None) -> int:
    argparser = argparse.ArgumentParser(
        description=(
            "Run a batch of seekrflow systems from one JSON definition. "
            "Systems are materialized under batch_directory/work_<name>/ "
            "and executed as headless flow.py children with a shared monitor."
        )
    )
    argparser.add_argument(
        "instruction", metavar="INSTRUCTION", type=str,
        help=(
            "Stage to perform: 'any' (parameterize if needed, then prepare, "
            "run, analyze), 'parameterize', 'prepare', 'run', or 'analyze'. "
            "Also 'status' to report on live batch children, and 'stop' to "
            "detach them (or, with --cancel-jobs, cancel their jobs too)."
        ),
    )
    argparser.add_argument(
        "-i", "--input_json", dest="input_json",
        metavar="BATCH_JSON", type=str, required=True,
        help="Path to the batch JSON file.",
    )
    argparser.add_argument(
        "-s", "--skip_checks", dest="skip_checks", default=False,
        action="store_true",
        help="Pass --skip_checks through to child prepare steps.",
    )
    argparser.add_argument(
        "--cancel-jobs", dest="cancel_jobs", default=False,
        action="store_true",
        help=(
            "Only with 'stop': also cancel the scheduler jobs the children "
            "submitted. Without it, children detach and their jobs keep "
            "running for a later 'run' to reattach to."
        ),
    )
    argparser.add_argument(
        "-T", "--transfer_from_remote_only", dest="transfer_from_remote_only",
        metavar="STAGE", type=str, default=None,
        help=(
            "Only with 'run': pull files from remote for STAGE (or 'all') "
            "without starting monitors or submitting jobs. Uses each "
            "system's flow.py -T. Stop a live batch run first if a child "
            "still owns the work directory."
        ),
    )
    args = argparser.parse_args(argv)
    instruction = args.instruction
    stage_instructions = ("any", "parameterize", "prepare", "run", "analyze")
    if instruction not in stage_instructions + ("status", "stop"):
        print(
            f"Invalid instruction {instruction!r}. "
            "Options: any, parameterize, prepare, run, analyze, status, stop.",
            file=sys.stderr,
        )
        return 2
    if args.cancel_jobs and instruction != "stop":
        print("--cancel-jobs is only valid with 'stop'.", file=sys.stderr)
        return 2
    if (args.transfer_from_remote_only is not None
            and instruction != "run"):
        print(
            "-T / --transfer_from_remote_only is only valid with 'run'.",
            file=sys.stderr,
        )
        return 2
    batch = batch_structures.load_batch(args.input_json)
    if instruction == "status":
        return batch_coordinator.report_batch_status(batch)
    if instruction == "stop":
        return batch_coordinator.stop_batch(
            batch, cancel_jobs=args.cancel_jobs)
    return batch_coordinator.run_batch(
        batch, instruction, skip_checks=args.skip_checks,
        transfer_from_remote_only=args.transfer_from_remote_only)


if __name__ == "__main__":
    raise SystemExit(main())
