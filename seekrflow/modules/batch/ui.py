"""
Rich UI for the batch coordinator: summary table + per-system detail.
"""

from __future__ import annotations

import os
import termios
import time
import tty
import typing

from rich.console import Console, Group
from rich.live import Live
from rich.table import Table
from rich.text import Text

import seekrflow.modules.batch.structures as batch_structures
import seekrflow.modules.batch.commands as batch_commands

_CONSOLE = Console()

_COMMAND_VERBS = {
    "t": "transfer",
    "transfer": "transfer",
    "g": "go",
    "go": "go",
    "w": "wait",
    "wait": "wait",
    "s": "stop",
    "stop": "stop",
}


def command_status_message(
        line: str,
        targets: list[SystemRow],
        *,
        detail: bool,
        ) -> str:
    """One-line echo for a dispatched live-table command."""
    parts = line.split()
    if not parts:
        return ""
    cmd = parts[0].lower()
    if cmd in ("h", "?"):
        return ""
    if cmd not in _COMMAND_VERBS:
        return f"unknown command: {cmd!r}"
    verb = _COMMAND_VERBS[cmd]
    stage = parts[1] if len(parts) > 1 else "all"
    if detail and targets:
        return f"{verb} requested: {stage} ({targets[0].name})"
    n = len(targets)
    noun = "system" if n == 1 else "systems"
    return f"{verb} requested: {stage} ({n} {noun})"



def _stage_error_text(info: dict) -> str:
    """
    Sticky failure text only — never surface transient probe notes from
    raw_status (those flash every few seconds on remote status hiccups).
    """
    state = str(info.get("state") or "")
    if state != "failed":
        return ""
    for key in ("last_error", "transfer_error"):
        val = info.get(key)
        if val:
            return str(val)
    return ""


_POLLED_SCHEDULER_STATUSES = {"queued", "running", "running/queued"}
_TERMINAL_DISPLAY_STATES = {"completed", "failed", "detached", "skipped"}
_OPERATOR_TERMINAL_STATES = {"failed", "detached", "skipped"}


def _summary_stage_is_active(info: dict) -> bool:
    if info.get("transfer_status") == "running":
        return True
    if str(info.get("state") or "") != "completed":
        return True
    display_state, _display_mgr = _display_state_and_manager(info)
    return display_state in _POLLED_SCHEDULER_STATUSES


def _pick_summary_stage(stages: dict) -> tuple[str, dict]:
    """
    Choose one stage for the summary row, preferring active scheduler work
    so the row does not jump between stages on transient probe noise.
    """
    items = list(stages.items())
    if not items:
        raise ValueError("stages is empty")
    active = [(n, i) for n, i in items if _summary_stage_is_active(i)]
    if not active:
        name = items[-1][0]
        return name, items[-1][1]

    def rank(item: tuple[str, dict]) -> tuple:
        _name, info = item
        state = str(info.get("state") or "")
        display_state, _display_mgr = _display_state_and_manager(info)
        try:
            prog = -float(info.get("progress") or 0.0)
        except (TypeError, ValueError):
            prog = 0.0
        if info.get("transfer_status") == "running":
            return (0, 0, prog)
        if display_state in _POLLED_SCHEDULER_STATUSES:
            # Prefer a still-running fused member over a completed host
            # that is only still queued/running because the combined job
            # has not drained yet.
            drain = 1 if state == "completed" else 0
            return (1, drain, prog)
        if state in {"started", "queued"}:
            return (2, 0, prog)
        if state == "failed":
            return (3, 0, prog)
        return (4, 0, prog)

    return min(active, key=rank)


class SystemRow:
    """Mutable per-system status for the summary table."""

    def __init__(self, name: str, work_directory: str, skipped: bool = False):
        self.name = name
        self.work_directory = work_directory
        self.skipped = skipped
        self.batch_stage = "-"  # parameterize|prepare|run|analyze|...
        self.state = "skipped" if skipped else "pending"
        self.semaphore = "-"
        self.progress = "-"
        self.last_error = ""
        self.log_path = ""
        self.child_status: dict | None = None
        self.manager_status = "-"
        # PID of the child this row is meant to reflect, when known.
        self.child_pid: int | None = None
        self.status_polled_at: float | None = None

    def status_writer_pid(self) -> int | None:
        status = self.child_status
        if not isinstance(status, dict):
            return None
        try:
            return int(status.get("pid", -1))
        except (TypeError, ValueError):
            return None

    def refresh_from_status_file(self) -> None:
        status = batch_structures.read_child_status(self.work_directory)
        self.child_status = status
        if status is None:
            return
        writer_pid = self.status_writer_pid()
        if (self.child_pid is not None and writer_pid is not None
                and writer_pid > 0 and writer_pid != self.child_pid):
            # Another process is writing this system's status file, so its
            # contents describe someone else's run. Surface that rather than
            # rendering two interleaved snapshots as flapping states.
            self.state = "stale"
            self.manager_status = "-"
            self.last_error = (
                f"status file written by foreign PID {writer_pid} "
                f"(expected {self.child_pid})")
            return
        stages = status.get("stages") or {}
        if not stages:
            return
        stage_name, info = _pick_summary_stage(stages)
        self.batch_stage = stage_name
        self.state, self.manager_status = _display_state_and_manager(info)
        self.semaphore = info.get("semaphore", "-")
        try:
            self.progress = f"{float(info.get('progress', 0.0)):.0%}"
        except (TypeError, ValueError):
            self.progress = str(info.get("progress", "-"))
        self.last_error = _stage_error_text(info)
        try:
            polled = info.get("status_polled_at")
            self.status_polled_at = float(polled) if polled else None
        except (TypeError, ValueError):
            self.status_polled_at = None


DEFAULT_STATUS_STALE_AFTER_S = 600.0  # 2 × default background_poll_interval


def _row_status_is_stale(
        row: SystemRow,
        stale_after_s: float = DEFAULT_STATUS_STALE_AFTER_S,
        ) -> bool:
    if row.status_polled_at is None or stale_after_s <= 0:
        return False
    return (time.time() - row.status_polled_at) > stale_after_s


def _has_polled_scheduler_status(info: dict) -> bool:
    """True when this snapshot includes a scheduler poll timestamp."""
    polled = info.get("status_polled_at")
    if polled is None or polled == "":
        return False
    try:
        return float(polled) > 0.0
    except (TypeError, ValueError):
        return False


def _display_state_and_manager(info: dict) -> tuple[str, str]:
    """Map snapshot fields to the State / Manager cells."""
    transfer = info.get("transfer_status")
    direction = info.get("transfer_direction")
    state = str(info.get("state") or "-")
    manager = str(info.get("manager_status") or "-")
    if transfer == "running":
        if direction == "back":
            return "transferring", "pulling"
        return "transferring", "gathering"
    # Operator terminals stay visible even if squeue still has a leftover job.
    if state in _OPERATOR_TERMINAL_STATES:
        return state, manager
    resource = str(info.get("resource_name") or "local")
    if resource != "local" and (
            _has_polled_scheduler_status(info)
            and manager in _POLLED_SCHEDULER_STATUSES):
        return manager, manager
    if state in _TERMINAL_DISPLAY_STATES:
        return state, manager
    # Capacity wait is a workflow state, not a leftover SLURM label.
    if state == "queued":
        return "queued", manager
    if resource == "local":
        return state, manager
    return "pending", "pending"


def _style_with_dim(style: str, dim: bool) -> str:
    if not dim:
        return style
    if not style or style == "white":
        return "dim"
    if style.startswith("dim"):
        return style
    return f"dim {style}"


def _state_text(state: str, dim: bool = False) -> Text:
    colors = {
        "completed": "green",
        "failed": "red",
        "started": "yellow",
        "running": "yellow",
        "transferring": "cyan",
        "queued": "cyan",
        "running/queued": "cyan",
        "pending": "dim",
        "skipped": "dim",
        "error": "red",
        "unstarted": "dim",
        "idle": "dim",
        "detached": "blue",
        "duplicate": "red",
        "stale": "magenta",
    }
    return Text(state, style=_style_with_dim(colors.get(state, "white"), dim))


def _manager_text(manager: str, dim: bool = False) -> Text:
    colors = {
        "running": "green",
        "queued": "cyan",
        "running/queued": "blue",
        "transfer": "cyan",
        "gathering": "cyan",
        "pulling": "cyan",
        "pending": "dim",
        "idle": "dim",
        "unknown": "dim",
    }
    return Text(
        manager, style=_style_with_dim(colors.get(manager, "white"), dim))


def build_summary_table(
        rows: list[SystemRow],
        selected_index: int,
        stale_after_s: float = DEFAULT_STATUS_STALE_AFTER_S,
        ) -> Table:
    # Fixed widths so Rich does not resize the whole table when Error
    # text appears/disappears (that resize looked like "jumping").
    table = Table(title="Seekrflow Batch", expand=True)
    table.add_column("", width=2, no_wrap=True)
    table.add_column("System", min_width=24, no_wrap=True, overflow="ellipsis")
    table.add_column("Stage", min_width=18, no_wrap=True, overflow="ellipsis")
    table.add_column("State", width=14, no_wrap=True)
    table.add_column("Progress", width=8, justify="right", no_wrap=True)
    table.add_column("Sem", width=5, no_wrap=True)
    table.add_column("Manager", width=14, no_wrap=True)
    table.add_column(
        "Error", min_width=24, no_wrap=True, overflow="ellipsis")
    for i, row in enumerate(rows):
        marker = ">" if i == selected_index else " "
        err = (row.last_error or "")[:50]
        stale = _row_status_is_stale(row, stale_after_s)
        table.add_row(
            marker,
            row.name,
            row.batch_stage,
            _state_text(row.state, dim=stale),
            row.progress,
            row.semaphore,
            _manager_text(row.manager_status, dim=stale),
            Text(err, style="red") if err else Text(""),
        )
    return table


def build_detail_view(row: SystemRow) -> Group:
    table = Table(title=f"System: {row.name}", expand=True)
    table.add_column("Stage")
    table.add_column("State")
    table.add_column("Progress")
    table.add_column("Semaphore")
    table.add_column("Manager")
    table.add_column("Resource")
    table.add_column("Error")
    status = row.child_status or batch_structures.read_child_status(
        row.work_directory)
    stages = (status or {}).get("stages") or {}
    if not stages:
        table.add_row(
            "-", row.state, row.progress, row.semaphore,
            row.manager_status, "-",
            (row.last_error or "")[:40])
    else:
        for name, info in stages.items():
            try:
                prog = f"{float(info.get('progress', 0.0)):.0%}"
            except (TypeError, ValueError):
                prog = str(info.get("progress", "-"))
            display_state, display_manager = _display_state_and_manager(info)
            err = _stage_error_text(info)[:40]
            table.add_row(
                name,
                _state_text(display_state),
                prog,
                str(info.get("semaphore", "-")),
                _manager_text(display_manager),
                str(info.get("resource_name", "-")),
                Text(err, style="red") if err else Text(""),
            )
    footer = Text(
        f"Log: {row.log_path or '(none)'}   "
        f"Work: {row.work_directory}\n"
        "Esc: summary   g/w/s: this system   h: help",
        style="dim",
    )
    if row.state == "stale":
        warning = Text(
            f"WARNING: {row.last_error}. The stages below describe that "
            "other process, not this batch's child.",
            style="bold magenta",
        )
        return Group(warning, table, footer)
    return Group(table, footer)


DETACH_WAIT_MESSAGE = (
    "Detach command received. Please wait for all processes to detach.")
DETACH_SUMMARY_ONLY_MESSAGE = (
    "q detaches all systems; press Esc to return to Summary first")


def _help_text(detail: bool) -> str:
    if detail:
        return (
            "Detail commands: g/w/s [stage], t transfer, "
            "Esc summary, h help"
        )
    return (
        "Summary: ↑/↓ select, Enter detail, g/w/s/q all systems, h help"
    )


class BatchUI:
    """
    Interactive batch monitor. Returns when stop_callback says so, or the
    user types q+Enter on Summary to detach every child (jobs keep running).
    """

    def __init__(
            self,
            rows: list[SystemRow],
            on_command: typing.Callable[[str, list[SystemRow] | SystemRow], None],
            refresh_rows: typing.Callable[[], None] | None = None,
            stale_after_s: float = DEFAULT_STATUS_STALE_AFTER_S,
            ):
        self.rows = rows
        self.on_command = on_command
        self.refresh_rows = refresh_rows
        self.stale_after_s = stale_after_s
        self.selected = 0
        self.detail = False
        self._input_buffer = ""
        self._stop = False
        self.detach_requested = False
        self._status_message = ""

    def _visible_rows(self) -> list[SystemRow]:
        return self.rows

    def _render(self) -> Group:
        rows = self._visible_rows()
        if not rows:
            return Group(Text("No systems in batch.", style="red"))
        self.selected = max(0, min(self.selected, len(rows) - 1))
        if self.detail:
            body = build_detail_view(rows[self.selected])
        else:
            body = build_summary_table(
                rows, self.selected, stale_after_s=self.stale_after_s)
        parts: list = []
        if self._status_message:
            parts.append(Text(self._status_message, style="bold"))
        parts.append(body)
        parts.append(
            Text(
                _help_text(self.detail) + f"\n> {self._input_buffer}",
                style="dim italic",
            ),
        )
        return Group(*parts)

    def _request_detach_all(self) -> None:
        """
        Summary-only: close the monitor so the coordinator detaches every child.
        """
        if self.detail:
            self._status_message = DETACH_SUMMARY_ONLY_MESSAGE
            return
        if self.detach_requested:
            self._status_message = DETACH_WAIT_MESSAGE
            self._stop = True
            return
        self.detach_requested = True
        self._status_message = DETACH_WAIT_MESSAGE
        self._stop = True

    def _dispatch_line(self, line: str) -> None:
        line = line.strip()
        if not line:
            return
        cmd = line.split()[0].lower()
        if cmd in ("q", "detach"):
            self._request_detach_all()
            return
        rows = self._visible_rows()
        if not rows:
            return
        if self.detail:
            targets = [rows[self.selected]]
        else:
            targets = [r for r in rows if not r.skipped]
        echo = command_status_message(line, targets, detail=self.detail)
        if echo:
            self._status_message = echo
        if cmd in ("h", "?"):
            self.on_command(line, targets)
            return
        if cmd not in _COMMAND_VERBS:
            return
        self.on_command(line, targets)

    def _handle_char(self, char: str) -> None:
        if char in ("\x1b",):  # Esc alone starts escape; handled in reader
            return
        if char in ("\n", "\r"):
            if self._input_buffer.strip() == "" and not self.detail:
                self.detail = True
            else:
                line = self._input_buffer
                self._input_buffer = ""
                self._dispatch_line(line)
            return
        if char in ("\x7f", "\b"):
            self._input_buffer = self._input_buffer[:-1]
            return
        if char == "\x03":
            self._stop = True
            return
        if char.isprintable():
            self._input_buffer += char

    def run(self, should_stop: typing.Callable[[], bool], poll_seconds: float = 1.0) -> None:
        """
        Blocking UI loop using Rich Live + cbreak stdin.

        Ctrl-C is absorbed here rather than propagating: the caller must reach
        its cleanup path so that spawned children are detached instead of
        orphaned.
        """
        try:
            self._run_loop(should_stop, poll_seconds)
        except KeyboardInterrupt:
            self._stop = True
            _CONSOLE.print("\n[batch-ui] interrupted; closing monitor...")

    def _run_loop(
            self,
            should_stop: typing.Callable[[], bool],
            poll_seconds: float = 1.0,
            ) -> None:
        import select
        import sys
        import time

        try:
            if not sys.stdin.isatty():
                while not should_stop() and not self._stop:
                    if self.refresh_rows:
                        self.refresh_rows()
                    time.sleep(poll_seconds)
                if self.refresh_rows:
                    self.refresh_rows()
                return
        except KeyboardInterrupt:
            raise
        except Exception:
            pass

        fd = sys.stdin.fileno()
        old_attrs = None
        try:
            old_attrs = termios.tcgetattr(fd)
            tty.setcbreak(fd)
        except Exception:
            old_attrs = None

        try:
            with Live(
                self._render(),
                console=_CONSOLE,
                auto_refresh=False,
                redirect_stdout=True,
                redirect_stderr=True,
            ) as live:
                while not should_stop() and not self._stop:
                    if self.refresh_rows:
                        self.refresh_rows()
                    live.update(self._render())
                    live.refresh()
                    ready, _, _ = select.select([fd], [], [], poll_seconds)
                    if not ready:
                        continue
                    data = os.read(fd, 1024)
                    if not data:
                        break
                    text = data.decode(errors="ignore")
                    i = 0
                    while i < len(text):
                        ch = text[i]
                        if ch == "\x1b":
                            if i + 2 < len(text) and text[i + 1] == "[":
                                code = text[i + 2]
                                if code == "A":
                                    self.selected = max(0, self.selected - 1)
                                elif code == "B":
                                    self.selected = min(
                                        len(self._visible_rows()) - 1,
                                        self.selected + 1)
                                elif code == "C":
                                    self.detail = True
                                elif code == "D":
                                    self.detail = False
                                i += 3
                                continue
                            self.detail = False
                            self._input_buffer = ""
                            i += 1
                            continue
                        self._handle_char(ch)
                        i += 1
                    if self._stop and self.detach_requested:
                        live.update(self._render())
                        live.refresh()
                if self.refresh_rows:
                    self.refresh_rows()
                live.update(self._render())
                live.refresh()
        finally:
            if old_attrs is not None:
                try:
                    termios.tcsetattr(fd, termios.TCSADRAIN, old_attrs)
                except Exception:
                    pass


def fanout_command_to_systems(
        line: str,
        targets: list[SystemRow] | SystemRow,
        ) -> None:
    """
    Translate a keystroke command line into batch_commands.jsonl writes.
    """
    if isinstance(targets, SystemRow):
        targets = [targets]
    line = line.strip()
    if not line:
        return
    parts = line.split()
    cmd = parts[0].lower()
    if cmd in ("q", "detach"):
        # Detach is Summary-only and handled by BatchUI, not per-system.
        return
    stage = parts[1] if len(parts) > 1 else None
    for row in targets:
        if row.skipped:
            continue
        run_dir = os.path.join(row.work_directory, "run")
        if cmd in ("g", "go"):
            batch_commands.append_semaphore_command(run_dir, "go", stage)
        elif cmd in ("w", "wait"):
            batch_commands.append_semaphore_command(run_dir, "wait", stage)
        elif cmd in ("s", "stop"):
            batch_commands.append_semaphore_command(run_dir, "stop", stage)
        elif cmd in ("t", "transfer"):
            batch_commands.append_transfer_command(run_dir, stage)
        elif cmd in ("h", "?"):
            print(_help_text(False))
        else:
            print(f"[batch-ui] unknown command: {cmd!r}")
