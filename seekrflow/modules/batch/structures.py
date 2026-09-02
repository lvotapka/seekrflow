"""
Data structures and helpers for seekrflow batch definitions.
"""

from __future__ import annotations

import copy
import json
import os
import typing

from attrs import define, field, validators
import cattrs
from cattrs.strategies import include_subclasses

import seekrflow.modules.structures as seekrflow_structures
import seekrflow.modules.batch.analysis as batch_analysis


BATCH_STATUS_FILENAME = ".seekrflow_batch_status.json"
SEEKRFLOW_JSON_NAME = "seekrflow.json"


def deep_merge(base: typing.Any, override: typing.Any) -> typing.Any:
    """
    Deep-merge override onto a deep copy of base.

    Dicts merge recursively. Lists merge element-by-index: dict elements
    are deep-merged; other element types are replaced. Extra override
    elements are appended; trailing base elements (omitted indices) are kept.
    Scalars and other types are replaced by override.
    """
    if override is None:
        return copy.deepcopy(base)
    if isinstance(base, dict) and isinstance(override, dict):
        result = copy.deepcopy(base)
        for key, value in override.items():
            if key in result:
                result[key] = deep_merge(result[key], value)
            else:
                result[key] = copy.deepcopy(value)
        return result
    if isinstance(base, list) and isinstance(override, list):
        result: list = []
        shared = min(len(base), len(override))
        for i in range(shared):
            if isinstance(base[i], dict) and isinstance(override[i], dict):
                result.append(deep_merge(base[i], override[i]))
            else:
                result.append(copy.deepcopy(override[i]))
        if len(override) > shared:
            result.extend(copy.deepcopy(override[shared:]))
        elif len(base) > shared:
            result.extend(copy.deepcopy(base[shared:]))
        return result
    return copy.deepcopy(override)


def absolutize_existing_paths(
        obj: typing.Any,
        base_dir: str,
        ) -> typing.Any:
    """
    Recursively turn relative path strings that exist under base_dir into
    absolute paths so children can chdir into work directories safely.
    """
    if isinstance(obj, dict):
        return {
            key: absolutize_existing_paths(value, base_dir)
            for key, value in obj.items()
        }
    if isinstance(obj, list):
        return [absolutize_existing_paths(value, base_dir) for value in obj]
    if isinstance(obj, str) and obj and not os.path.isabs(obj):
        candidate = os.path.normpath(os.path.join(base_dir, obj))
        if os.path.exists(candidate):
            return os.path.abspath(candidate)
    return obj


@define
class Batch_system:
    """
    One system entry in a batch.
    """
    name: str = field(validator=validators.instance_of(str))
    skip: bool = field(default=False, validator=validators.instance_of(bool))
    overrides: dict = field(factory=dict, validator=validators.instance_of(dict))


@define
class Batch:
    """
    Top-level batch configuration.
    """
    batch_directory: str = field(validator=validators.instance_of(str))
    # Path string or inline seekrflow dict (validated at materialize time).
    template: typing.Any = field()
    systems: list[Batch_system] = field(factory=list)
    prepare_concurrency: int = field(
        default=1,
        validator=validators.and_(
            validators.instance_of(int),
            validators.ge(1),
        ),
    )
    # Cap concurrent *local stages* across the batch (see local_slots.py).
    # Remote/cloud stages are never counted against this limit.
    max_concurrent_local_runs: int = field(
        default=1,
        validator=validators.and_(
            validators.instance_of(int),
            validators.ge(1),
        ),
    )
    background_poll_interval: float = field(
        default=300.0,
        validator=validators.and_(
            validators.instance_of(float),
            validators.gt(0.0),
        ),
    )
    focused_poll_interval: float = field(
        default=5.0,
        validator=validators.and_(
            validators.instance_of(float),
            validators.gt(0.0),
        ),
    )
    batch_analyses: list[batch_analysis.Batch_analysis] = field(
        factory=list,
    )
    # Populated after load; not part of JSON input.
    _source_path: str | None = field(default=None, eq=False, repr=False)
    _source_dir: str | None = field(default=None, eq=False, repr=False)

    def needs_parameterize(self) -> bool:
        """
        True if the template (after a no-override merge) has a parameterizer.
        """
        template_dict = self._load_template_dict()
        return template_dict.get("parameterizer") is not None

    def _load_template_dict(self) -> dict:
        if isinstance(self.template, dict):
            return copy.deepcopy(self.template)
        if not isinstance(self.template, str):
            raise TypeError(
                f"batch template must be a path string or dict, got "
                f"{type(self.template)}")
        template_path = self.template
        if not os.path.isabs(template_path) and self._source_dir:
            template_path = os.path.join(self._source_dir, template_path)
        with open(template_path, "r") as f:
            return json.load(f)

    def template_path_resolved(self) -> str | None:
        if isinstance(self.template, dict):
            return None
        if not isinstance(self.template, str):
            return None
        template_path = self.template
        if not os.path.isabs(template_path) and self._source_dir:
            template_path = os.path.join(self._source_dir, template_path)
        return os.path.abspath(template_path)

    def batch_directory_resolved(self) -> str:
        path = self.batch_directory
        if not os.path.isabs(path) and self._source_dir:
            path = os.path.join(self._source_dir, path)
        return os.path.abspath(path)

    def system_work_directory(self, system_name: str) -> str:
        return os.path.join(
            self.batch_directory_resolved(), f"work_{system_name}")


def _register_batch_analysis_subclasses(converter: cattrs.Converter) -> None:
    include_subclasses(batch_analysis.Batch_analysis, converter)


def load_batch(filename: str) -> Batch:
    """
    Load a Batch object from a JSON file.
    """
    filename = os.path.abspath(filename)
    with open(filename, "r") as f:
        data = json.load(f)
    converter = cattrs.Converter()
    _register_batch_analysis_subclasses(converter)
    batch: Batch = converter.structure(data, Batch)
    batch._source_path = filename
    batch._source_dir = os.path.dirname(filename)
    names = [s.name for s in batch.systems]
    if len(names) != len(set(names)):
        raise ValueError(f"Duplicate system names in batch: {names}")
    return batch


def active_systems(batch: Batch) -> list[Batch_system]:
    """Return systems that are not skipped."""
    return [s for s in batch.systems if not s.skip]


def materialize_system(
        batch: Batch,
        system: Batch_system,
        ) -> tuple[str, str]:
    """
    Deep-merge template + overrides, write seekrflow.json under
    batch_directory/work_{name}/, and return (work_directory, json_path).
    """
    template_dict = batch._load_template_dict()
    template_path = batch.template_path_resolved()
    path_base = (
        os.path.dirname(template_path) if template_path
        else (batch._source_dir or os.getcwd())
    )
    merged = deep_merge(template_dict, system.overrides or {})
    merged = absolutize_existing_paths(merged, path_base)
    work_dir = batch.system_work_directory(system.name)
    os.makedirs(work_dir, exist_ok=True)
    os.makedirs(os.path.join(work_dir, "logs"), exist_ok=True)
    merged["name"] = system.name
    merged["work_directory"] = work_dir
    json_path = os.path.join(work_dir, SEEKRFLOW_JSON_NAME)
    with open(json_path, "w") as f:
        json.dump(merged, f, indent=4)
    # Validate against Seekrflow schema early.
    seekrflow_structures.load_seekrflow(json_path)
    return work_dir, json_path


def materialize_all(batch: Batch) -> dict[str, str]:
    """
    Materialize all non-skipped systems. Returns {name: seekrflow_json_path}.
    """
    result: dict[str, str] = {}
    for system in active_systems(batch):
        _, json_path = materialize_system(batch, system)
        result[system.name] = json_path
    return result


def write_batch_status(
        batch_directory: str,
        payload: dict,
        ) -> str:
    """Atomically write batch-level status JSON."""
    os.makedirs(batch_directory, exist_ok=True)
    target = os.path.join(batch_directory, BATCH_STATUS_FILENAME)
    tmp = target + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f, indent=4, default=str)
    os.replace(tmp, target)
    return target


def read_child_status(work_directory: str) -> dict | None:
    """Read a child's .seekrflow_job_status.json if present."""
    candidates = [
        os.path.join(work_directory, "root", ".seekrflow_job_status.json"),
    ]
    # Also honor a non-default root directory name from materialized JSON.
    seekrflow_json = os.path.join(work_directory, SEEKRFLOW_JSON_NAME)
    if os.path.exists(seekrflow_json):
        try:
            with open(seekrflow_json, "r") as f:
                cfg = json.load(f)
            root_name = cfg.get("root_directory") or "root"
            candidates.insert(
                0,
                os.path.join(
                    work_directory, root_name, ".seekrflow_job_status.json"),
            )
        except (OSError, json.JSONDecodeError):
            pass
    seen: set[str] = set()
    for path in candidates:
        if path in seen:
            continue
        seen.add(path)
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    return json.load(f)
            except (OSError, json.JSONDecodeError):
                return None
    return None
