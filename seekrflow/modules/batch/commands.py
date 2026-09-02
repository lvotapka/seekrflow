"""
Batch command-file helpers for child processes and future GUI/daemon control.
"""

from __future__ import annotations

import json
import os
import time
import typing


BATCH_COMMANDS_FILENAME = "batch_commands.jsonl"


def batch_commands_path(run_directory: str) -> str:
    return os.path.join(run_directory, BATCH_COMMANDS_FILENAME)


def archive_batch_commands(run_directory: str) -> str | None:
    """
    Move any existing command file aside so a new session starts empty.

    Children read this file from the beginning, so without rotation a restarted
    child would replay every command of every prior session — including stale
    ``wait``/``stop`` semaphore fanouts. Returns the archive path, or None when
    there was nothing to archive.
    """
    path = batch_commands_path(run_directory)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return None
    stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime())
    archive = os.path.join(
        run_directory, f"batch_commands.{stamp}.jsonl")
    suffix = 1
    while os.path.exists(archive):
        archive = os.path.join(
            run_directory, f"batch_commands.{stamp}-{suffix}.jsonl")
        suffix += 1
    try:
        os.replace(path, archive)
    except OSError as e:
        print(f"[batch] could not archive {path}: {e}")
        return None
    return archive


def append_batch_command(run_directory: str, command: dict) -> str:
    """
    Append one JSON command line for a child to consume.
    Creates the run directory if needed.
    """
    os.makedirs(run_directory, exist_ok=True)
    path = batch_commands_path(run_directory)
    with open(path, "a") as f:
        f.write(json.dumps(command) + "\n")
    return path


def append_semaphore_command(
        run_directory: str,
        value: str,
        stage: str | None = None,
        ) -> str:
    return append_batch_command(
        run_directory,
        {"cmd": "semaphore", "value": value, "stage": stage},
    )


def append_detach_command(run_directory: str) -> str:
    return append_batch_command(run_directory, {"cmd": "detach"})


def append_transfer_command(
        run_directory: str,
        stage: str | None = None,
        ) -> str:
    return append_batch_command(
        run_directory,
        {"cmd": "transfer", "stage": stage},
    )


def append_poll_interval_command(
        run_directory: str,
        seconds: float,
        ) -> str:
    return append_batch_command(
        run_directory,
        {"cmd": "set_poll_interval", "seconds": float(seconds)},
    )


def read_new_commands(
        path: str,
        offset: int,
        ) -> tuple[list[dict], int]:
    """
    Read newly appended JSONL commands since byte offset.

    Returns (commands, new_offset). Incomplete trailing lines are left for
    the next read (offset is not advanced past them).
    """
    if not os.path.exists(path):
        return [], offset
    commands: list[dict] = []
    with open(path, "r") as f:
        f.seek(offset)
        while True:
            pos = f.tell()
            line = f.readline()
            if not line:
                break
            if not line.endswith("\n"):
                # Incomplete line; wait for more data.
                return commands, pos
            line = line.strip()
            if not line:
                offset = f.tell()
                continue
            try:
                commands.append(json.loads(line))
            except json.JSONDecodeError:
                # Skip malformed lines but advance past them.
                pass
            offset = f.tell()
    return commands, offset
