"""
modules/remote_interfaces/local_shell.py

Provide workflow submission on the local machine (e.g. an HPC login node)
via subprocess, mirroring the SSH python -c transport without Fabric.
"""

from __future__ import annotations

import subprocess
import typing

import seekrflow.modules.remote_interfaces.base as remote_interface_base

def submit_remote_workflow_with_local_shell(
        resource_name: str,
        workflow: typing.Any,
        manager_payload: dict,
        python_executable: str = "python3",
        silent: bool = False,
        ) -> dict:
    """
    Run 'workflow' locally with the same serialization/parse path as SSH.
    """
    # TODO: convert manager_payload to args
    args = [manager_payload]
    del silent  # reserved for future quiet logging parity with Globus
    cmd = remote_interface_base.build_python_c_command(
        workflow, args, python_executable=python_executable)
    completed = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
    )
    stdout = completed.stdout or ""
    stderr = completed.stderr or ""
    if completed.returncode != 0:
        stderr_tail = "\n".join(stderr.splitlines()[-10:])
        stdout_tail = "\n".join(stdout.splitlines()[-10:])
        raise RuntimeError(
            f"Local shell workflow {resource_name!r} exited with code "
            f"{completed.returncode}. stdout_tail={stdout_tail!r}. "
            f"stderr_tail={stderr_tail!r}"
        )
    return remote_interface_base.parse_workflow_stdout(
        stdout, stderr, transport_label="local_shell")
