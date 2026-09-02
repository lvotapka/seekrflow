"""
tools/probe_globus_endpoint.py

Diagnostic script: sends a probe workflow to a Globus Compute endpoint and
reports what the endpoint worker actually sees -- Python executable, env
vars, hostname, user, sys.path, and the results of trying to import seekr
and a few of its submodules.

Use this when the launch path works (because the SLURM job activates the
env) but the status path fails with "No module named 'seekr...'" -- the
status workflow runs directly in the endpoint worker process, so this
script tells you exactly what that worker's interpreter sees.

Usage:
    python -m seekrflow.tools.probe_globus_endpoint <endpoint_id>
    # or
    python -m seekrflow.tools.probe_globus_endpoint --seekrflow path/to/seekrflow.json --resource my_pbs_cluster
"""

import sys
import json
import argparse


def probe_workflow(args):
    """
    Runs in the Globus Compute endpoint worker. Returns a dict describing
    the worker's Python environment. Pure stdlib + best-effort imports so
    it never fails to return something useful.
    """
    import os
    import sys
    import socket
    import platform
    import traceback

    report = {
        "executable": sys.executable,
        "version": sys.version,
        "platform": platform.platform(),
        "hostname": socket.gethostname(),
        "cwd": os.getcwd(),
        "pid": os.getpid(),
        "user": os.environ.get("USER") or os.environ.get("LOGNAME") or "?",
        "home": os.environ.get("HOME", "?"),
        "path_env": os.environ.get("PATH", ""),
        "pythonpath_env": os.environ.get("PYTHONPATH", ""),
        "conda_prefix": os.environ.get("CONDA_PREFIX", ""),
        "conda_default_env": os.environ.get("CONDA_DEFAULT_ENV", ""),
        "mamba_root_prefix": os.environ.get("MAMBA_ROOT_PREFIX", ""),
        "virtual_env": os.environ.get("VIRTUAL_ENV", ""),
        "sys_path": list(sys.path),
        "imports": {},
    }

    modules_to_try = [
        "seekr",
        "seekr.modules",
        "seekr.modules.structures",
        "seekr.status",
        "seekr.run",
        "seekr2",
        "seekr2.modules.common_base",
        "openmm",
        "globus_compute_sdk",
    ]
    for mod_name in modules_to_try:
        entry = {}
        try:
            mod = __import__(mod_name, fromlist=["*"])
            entry["ok"] = True
            entry["file"] = getattr(mod, "__file__", None)
            entry["version"] = getattr(mod, "__version__", None)
        except Exception as e:
            entry["ok"] = False
            entry["error"] = f"{type(e).__name__}: {e}"
            entry["traceback"] = traceback.format_exc()
        report["imports"][mod_name] = entry

    # Try to locate any seekr install on disk via the executable's prefix.
    try:
        import importlib.util
        spec = importlib.util.find_spec("seekr")
        report["find_spec_seekr"] = {
            "origin": getattr(spec, "origin", None) if spec else None,
            "submodule_search_locations": list(spec.submodule_search_locations)
                if spec and spec.submodule_search_locations else None,
        }
    except Exception as e:
        report["find_spec_seekr"] = {"error": f"{type(e).__name__}: {e}"}

    return report


def _resolve_endpoint_id(cli_args) -> str:
    if cli_args.endpoint_id:
        return cli_args.endpoint_id
    if not cli_args.seekrflow or not cli_args.resource:
        raise SystemExit(
            "Provide either <endpoint_id> as positional arg or "
            "both --seekrflow and --resource."
        )
    import seekrflow.modules.structures as structures
    sf = structures.load_seekrflow(cli_args.seekrflow)
    resource = sf.run_settings.get_resource_by_name(cli_args.resource)
    iface = resource.remote_interface
    if getattr(iface, "type", None) != "globus_compute_sdk":
        raise SystemExit(
            f"Resource {cli_args.resource!r} uses interface "
            f"{getattr(iface, 'type', None)!r}, not globus_compute_sdk."
        )
    return iface.endpoint_id


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "endpoint_id",
        nargs="?",
        help="Globus Compute endpoint UUID. If omitted, use --seekrflow + --resource.",
    )
    parser.add_argument(
        "--seekrflow",
        help="Path to seekrflow.json (alternative to passing endpoint_id directly).",
    )
    parser.add_argument(
        "--resource",
        help="Resource name in seekrflow.json whose globus endpoint to probe.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print raw JSON report instead of a formatted summary.",
    )
    cli_args = parser.parse_args()

    endpoint_id = _resolve_endpoint_id(cli_args)
    print(f"[probe] submitting probe workflow to endpoint {endpoint_id} ...")

    from globus_compute_sdk import Executor
    from globus_compute_sdk.serialize import ComputeSerializer, CombinedCode

    with Executor(endpoint_id) as gcx:
        gcx.serializer = ComputeSerializer(strategy_code=CombinedCode())
        function_id = gcx.register_function(
            probe_workflow, description="seekrflow endpoint env probe")
        future = gcx.submit_to_registered_function(
            function_id=function_id, args=(None,))
        report = future.result()

    if cli_args.json:
        print(json.dumps(report, indent=2, default=str))
        return

    # Human-friendly summary.
    print()
    print("=" * 72)
    print(f"Endpoint worker environment report")
    print("=" * 72)
    print(f"hostname            : {report['hostname']}")
    print(f"user                : {report['user']}")
    print(f"pid                 : {report['pid']}")
    print(f"cwd                 : {report['cwd']}")
    print(f"executable          : {report['executable']}")
    print(f"python version      : {report['version'].splitlines()[0]}")
    print(f"platform            : {report['platform']}")
    print(f"HOME                : {report['home']}")
    print(f"CONDA_PREFIX        : {report['conda_prefix'] or '(unset)'}")
    print(f"CONDA_DEFAULT_ENV   : {report['conda_default_env'] or '(unset)'}")
    print(f"MAMBA_ROOT_PREFIX   : {report['mamba_root_prefix'] or '(unset)'}")
    print(f"VIRTUAL_ENV         : {report['virtual_env'] or '(unset)'}")
    print(f"PYTHONPATH          : {report['pythonpath_env'] or '(unset)'}")
    print()
    print("PATH:")
    for entry in report["path_env"].split(":"):
        print(f"  {entry}")
    print()
    print("sys.path:")
    for entry in report["sys_path"]:
        print(f"  {entry}")
    print()
    print("Import attempts:")
    for mod_name, entry in report["imports"].items():
        if entry.get("ok"):
            ver = entry.get("version") or "?"
            print(f"  OK   {mod_name:<32} version={ver}")
            print(f"       file = {entry.get('file')}")
        else:
            print(f"  FAIL {mod_name:<32} {entry.get('error')}")
    print()
    print(f"find_spec('seekr'): {report.get('find_spec_seekr')}")


if __name__ == "__main__":
    main()
