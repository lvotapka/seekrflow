"""
Pluggable batch analysis objects.

Concrete analyzers are selected by a ``type`` field in the batch JSON and
run in order after all systems complete (or on the ``analyze`` instruction).
"""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import typing

from attrs import define, field, validators


@define
class Batch_analysis:
    """
    Base batch analysis plugin.
    """
    type: typing.Literal["base"] = "base"

    def run(
            self,
            batch: typing.Any,
            system_work_dirs: list[tuple[str, str]],
            ) -> None:
        """
        Run analysis over finished systems.

        Parameters
        ----------
        batch:
            The loaded Batch object.
        system_work_dirs:
            List of (system_name, absolute_work_directory) for non-skipped
            systems.
        """
        raise NotImplementedError(
            f"Batch analysis type {self.type!r} has no run() implementation")


@define
class Collect_batch_analysis(Batch_analysis):
    """
    Stub collector: optionally invoke seekr analyze per system and write a
    simple aggregate CSV/JSON under batch_directory/<output_subdirectory>/.
    """
    type: typing.Literal["collect_batch_analysis"] = "collect_batch_analysis"
    output_subdirectory: str = field(
        default="analysis",
        validator=validators.instance_of(str),
    )
    run_seekr_analyze: bool = field(default=True)
    seekr_analyze_module: str = field(
        default="seekr.analyze",
        validator=validators.instance_of(str),
    )

    def run(
            self,
            batch: typing.Any,
            system_work_dirs: list[tuple[str, str]],
            ) -> None:
        out_dir = os.path.join(
            os.path.abspath(batch.batch_directory),
            self.output_subdirectory)
        os.makedirs(out_dir, exist_ok=True)
        rows: list[dict] = []
        for name, work_dir in system_work_dirs:
            root = os.path.join(work_dir, "root")
            model_json = os.path.join(root, "model.json")
            row: dict = {
                "name": name,
                "work_directory": work_dir,
                "model_json": model_json if os.path.exists(model_json) else "",
                "analyze_status": "skipped",
                "analyze_output": "",
            }
            if not os.path.exists(model_json):
                row["analyze_status"] = "missing_model"
                rows.append(row)
                continue
            if self.run_seekr_analyze:
                log_path = os.path.join(out_dir, f"{name}_analyze.out")
                try:
                    proc = subprocess.run(
                        [sys.executable, "-m", self.seekr_analyze_module,
                         model_json],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    with open(log_path, "w") as f:
                        f.write(proc.stdout or "")
                        if proc.stderr:
                            f.write("\n--- stderr ---\n")
                            f.write(proc.stderr)
                    row["analyze_output"] = log_path
                    row["analyze_status"] = (
                        "ok" if proc.returncode == 0 else "failed")
                    if proc.returncode != 0:
                        row["analyze_returncode"] = proc.returncode
                except FileNotFoundError as e:
                    row["analyze_status"] = "analyze_unavailable"
                    row["analyze_error"] = str(e)
                except Exception as e:
                    row["analyze_status"] = "error"
                    row["analyze_error"] = str(e)
            else:
                row["analyze_status"] = "collect_only"
            rows.append(row)

        json_path = os.path.join(out_dir, "collect_results.json")
        with open(json_path, "w") as f:
            json.dump(rows, f, indent=4)
        csv_path = os.path.join(out_dir, "collect_results.csv")
        fieldnames = [
            "name", "work_directory", "model_json", "analyze_status",
            "analyze_output",
        ]
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        print(f"[batch analysis] wrote {json_path} and {csv_path}")


@define
class Mmvt_batch_analysis(Batch_analysis):
    """
    Placeholder for MMVT comparison/ranking analyses (implement later).
    """
    type: typing.Literal["mmvt_batch_analysis"] = "mmvt_batch_analysis"

    def run(
            self,
            batch: typing.Any,
            system_work_dirs: list[tuple[str, str]],
            ) -> None:
        raise NotImplementedError(
            "mmvt_batch_analysis is not implemented yet; "
            "use collect_batch_analysis or provide a custom subclass.")


@define
class Ramd_batch_analysis(Batch_analysis):
    """
    Placeholder for RAMD ranking/comparison analyses (implement later).
    """
    type: typing.Literal["ramd_batch_analysis"] = "ramd_batch_analysis"

    def run(
            self,
            batch: typing.Any,
            system_work_dirs: list[tuple[str, str]],
            ) -> None:
        raise NotImplementedError(
            "ramd_batch_analysis is not implemented yet; "
            "use collect_batch_analysis or provide a custom subclass.")
