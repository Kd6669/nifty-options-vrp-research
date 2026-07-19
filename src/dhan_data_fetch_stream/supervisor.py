"""Durable, secret-free supervision for the rolling-options backfill.

The supervisor deliberately launches the existing CLI without credential
arguments.  Dhan credentials are inherited by the child process from the
environment and are used only to redact captured output before it is written.
"""

from __future__ import annotations

import argparse
from collections import Counter, deque
from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import threading
import time
from typing import Any, Iterable, Mapping, Sequence, TextIO

from .acquisition import DATA_REQUESTS_PER_DAY, DATA_REQUESTS_PER_SECOND, plan_rolling_options


TERMINAL_STATUSES = frozenset({"completed", "completed_empty"})
NO_RESTART_STATUSES = frozenset(
    {"credential_blocked", "daily_budget_exhausted", "rate_limited", "invalid_response"}
)
AUTH_CODES = frozenset({"DH-901", "401", "807", "808", "809", "810"})
QUOTA_CODES = frozenset({"DH-904", "429", "805", "daily_budget"})
SENSITIVE_ARG_RE = re.compile(r"(?:access[-_]?token|client[-_]?secret|password|authorization)", re.I)
JWT_RE = re.compile(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b")
CODE_RE = re.compile(r"\b(?:DH-901|DH-904|429|401|805|807|808|809|810)\b", re.I)
FATAL_TEXT_RE = re.compile(r"\b(?:schema|integrity|checksum|hash mismatch|corrupt(?:ed|ion)?)\b", re.I)


@dataclass(frozen=True)
class SupervisorConfig:
    root: Path
    status_dir: Path
    start_date: date
    end_date: date
    expiry_codes: tuple[int, ...] = (1,)
    expiry_flags: tuple[str, ...] = ("WEEK", "MONTH")
    option_types: tuple[str, ...] = ("CALL", "PUT")
    moneyness_width: int = 10
    workers: int = 5
    requests_per_second: float = 5.0
    daily_budget: int = 100_000
    max_retries: int = 4
    timeout_seconds: float = 30.0
    poll_seconds: float = 10.0
    stall_seconds: float = 180.0
    max_restarts: int = 3
    restart_backoff_seconds: float = 30.0
    expected_cells: int = 8_820

    def validate(self) -> None:
        if not 1 <= self.workers <= 5:
            raise ValueError("workers must be in 1..5")
        if not 0 < self.requests_per_second <= DATA_REQUESTS_PER_SECOND:
            raise ValueError(f"requests_per_second must be in (0, {DATA_REQUESTS_PER_SECOND}]")
        if not 0 < self.daily_budget <= DATA_REQUESTS_PER_DAY:
            raise ValueError(f"daily_budget must be in 1..{DATA_REQUESTS_PER_DAY}")
        if self.poll_seconds <= 0 or self.stall_seconds <= self.poll_seconds:
            raise ValueError("stall_seconds must exceed the positive poll_seconds")
        if self.max_restarts < 0 or self.restart_backoff_seconds < 0:
            raise ValueError("restart limits and backoff must be non-negative")
        planned = len(self.planned_cells())
        if planned != self.expected_cells:
            raise ValueError(f"planned cell count {planned} does not match expected {self.expected_cells}")

    def planned_cells(self) -> list[Any]:
        return plan_rolling_options(
            start_date=self.start_date,
            end_date=self.end_date,
            expiry_codes=self.expiry_codes,
            expiry_flags=self.expiry_flags,
            option_types=self.option_types,
            moneyness_width=self.moneyness_width,
        )

    def child_command(self) -> list[str]:
        command = [
            sys.executable,
            "-m",
            "dhan_data_fetch_stream.cli",
            "backfill-rolling-options",
            "--root",
            str(self.root),
            "--start-date",
            self.start_date.isoformat(),
            "--end-date",
            self.end_date.isoformat(),
            "--expiry-codes",
            ",".join(str(value) for value in self.expiry_codes),
            "--expiry-flags",
            ",".join(self.expiry_flags),
            "--option-types",
            ",".join(self.option_types),
            "--moneyness-width",
            str(self.moneyness_width),
            "--workers",
            str(self.workers),
            "--requests-per-second",
            str(self.requests_per_second),
            "--daily-budget",
            str(self.daily_budget),
            "--max-retries",
            str(self.max_retries),
            "--timeout-seconds",
            str(self.timeout_seconds),
        ]
        assert_secret_free_command(command)
        return command


class OutputCapture:
    """Redact child output before durable storage and retain error summaries."""

    def __init__(self, status_dir: Path, secrets: Iterable[str]) -> None:
        self.status_dir = status_dir
        self.secrets = tuple(value for value in secrets if value)
        self.code_counts: Counter[str] = Counter()
        self.stderr_tail: deque[str] = deque(maxlen=20)
        self._lock = threading.Lock()
        status_dir.mkdir(parents=True, exist_ok=True)

    def start(self, process: subprocess.Popen[str]) -> list[threading.Thread]:
        threads = [
            threading.Thread(
                target=self._drain,
                args=(process.stdout, self.status_dir / "child_stdout.log", False),
                daemon=True,
            ),
            threading.Thread(
                target=self._drain,
                args=(process.stderr, self.status_dir / "child_stderr.log", True),
                daemon=True,
            ),
        ]
        for thread in threads:
            thread.start()
        return threads

    def summary(self) -> dict[str, Any]:
        with self._lock:
            return {"code_counts": dict(self.code_counts), "stderr_tail": list(self.stderr_tail)}

    def reset_summary(self) -> None:
        """Begin a fresh error window while preserving append-only log files."""
        with self._lock:
            self.code_counts.clear()
            self.stderr_tail.clear()

    def _drain(self, stream: TextIO | None, path: Path, is_stderr: bool) -> None:
        if stream is None:
            return
        with path.open("a", encoding="utf-8", newline="\n") as handle:
            for raw_line in iter(stream.readline, ""):
                line = redact_text(raw_line.rstrip("\r\n"), self.secrets)
                with self._lock:
                    for code in CODE_RE.findall(line):
                        self.code_counts[code.upper()] += 1
                    if is_stderr and line:
                        self.stderr_tail.append(line[:1_000])
                handle.write(line + "\n")
                handle.flush()
        stream.close()


def assert_secret_free_command(command: Sequence[str]) -> None:
    for arg in command:
        if SENSITIVE_ARG_RE.search(str(arg)) or JWT_RE.search(str(arg)):
            raise ValueError("credential-bearing command arguments are forbidden; use inherited environment only")


def redact_text(text: str, secrets: Iterable[str]) -> str:
    result = JWT_RE.sub("[REDACTED]", str(text))
    for secret in secrets:
        if secret:
            result = result.replace(secret, "[REDACTED]")
    result = re.sub(
        r"(?i)((?:access[-_]?token|authorization|password)\s*[:=]\s*)\S+",
        r"\1[REDACTED]",
        result,
    )
    return result


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(path.suffix + ".partial")
    with partial.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, default=str)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(partial, path)


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(path.suffix + ".partial")
    with partial.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(partial, path)


def append_event(path: Path, event: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(event, sort_keys=True, default=str) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def read_manifest_snapshot(config: SupervisorConfig, *, now: float | None = None) -> dict[str, Any]:
    now = time.time() if now is None else now
    planned = config.planned_cells()
    expected = {cell.request_id: (index, cell) for index, cell in enumerate(planned)}
    manifests: dict[str, Mapping[str, Any]] = {}
    parse_errors: list[str] = []
    unplanned: list[str] = []
    manifest_dir = config.root / "manifests" / "requests"
    for path in manifest_dir.glob("*.json") if manifest_dir.is_dir() else ():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            parse_errors.append(f"{path.name}: {type(exc).__name__}")
            continue
        if payload.get("dataset") != "options" or payload.get("endpoint") != "/charts/rollingoption":
            continue
        request_id = str(payload.get("request_id") or path.stem)
        if request_id not in expected:
            unplanned.append(request_id)
            continue
        manifests[request_id] = payload

    statuses = Counter(str(item.get("status", "unknown")) for item in manifests.values())
    completed = statuses["completed"]
    completed_empty = statuses["completed_empty"]
    accounted = completed + completed_empty
    failed = sum(count for status, count in statuses.items() if status not in TERMINAL_STATUSES)
    rows = sum(
        int(item.get("rows", 0) or 0)
        for item in manifests.values()
        if item.get("status") in TERMINAL_STATUSES
    )
    completed_items = [item for item in manifests.values() if item.get("status") in TERMINAL_STATUSES]
    latest_item = max(completed_items, key=lambda item: str(item.get("completed_at_utc", "")), default=None)
    latest_completed = None if latest_item is None else latest_item.get("completed_at_utc")
    latest_data_timestamp = max(
        (str(item["max_timestamp_ist"]) for item in completed_items if item.get("max_timestamp_ist")),
        default=None,
    )
    prefix = 0
    while prefix < len(planned) and manifests.get(planned[prefix].request_id, {}).get("status") in TERMINAL_STATUSES:
        prefix += 1
    frontier_cell = None if prefix == len(planned) else planned[prefix]
    latest_age = None
    if latest_completed:
        try:
            latest_age = max(0.0, now - datetime.fromisoformat(str(latest_completed)).timestamp())
        except ValueError:
            parse_errors.append("latest completed_at_utc is not ISO-8601")
    all_partials = sorted(config.root.rglob("*.partial"))
    partials = [str(path) for path in all_partials if not _is_quarantined_partial(config.root, path)]
    quarantined_partials = [str(path) for path in all_partials if _is_quarantined_partial(config.root, path)]
    latest_failure_by_status: dict[str, str] = {}
    for item in manifests.values():
        status = str(item.get("status", "unknown"))
        if status in TERMINAL_STATUSES or not item.get("completed_at_utc"):
            continue
        completed_at = str(item["completed_at_utc"])
        latest_failure_by_status[status] = max(latest_failure_by_status.get(status, ""), completed_at)
    return {
        "expected_cells": config.expected_cells,
        "manifest_files": len(manifests),
        "completed": completed,
        "completed_empty": completed_empty,
        "accounted": accounted,
        "remaining": max(0, config.expected_cells - accounted),
        "failed": failed,
        "status_counts": dict(sorted(statuses.items())),
        "retained_rows": rows,
        "latest_completed_at_utc": latest_completed,
        "latest_completed_age_seconds": latest_age,
        "latest_data_timestamp_ist": latest_data_timestamp,
        "latest_completed_request": _request_summary(latest_item),
        "frontier": {
            "completed_prefix_cells": prefix,
            "next_request": None if frontier_cell is None else _cell_summary(frontier_cell),
        },
        "manifest_parse_errors": parse_errors,
        "unplanned_request_ids": sorted(unplanned),
        "partial_files": partials,
        "quarantined_partial_files": quarantined_partials,
        "latest_failure_at_utc_by_status": latest_failure_by_status,
    }


def read_daily_budget(root: Path) -> dict[str, Any]:
    today = datetime.now(timezone.utc).date().isoformat()
    path = root / "manifests" / f"daily_budget_{today}.json"
    payload: dict[str, Any] = {"utc_date": today, "used": 0, "limit": DATA_REQUESTS_PER_DAY}
    if path.is_file():
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(value, Mapping):
                payload.update(value)
        except (OSError, json.JSONDecodeError):
            payload["read_error"] = True
    used = int(payload.get("used", 0) or 0)
    limit = int(payload.get("limit", DATA_REQUESTS_PER_DAY) or DATA_REQUESTS_PER_DAY)
    payload["remaining"] = max(0, limit - used)
    return payload


def disk_status(root: Path) -> dict[str, Any]:
    probe = root
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    usage = shutil.disk_usage(probe)
    return {"path": str(probe), "free_bytes": usage.free, "free_gib": round(usage.free / 2**30, 2)}


def process_tree(supervisor_pid: int, child_pid: int | None, child_running: bool) -> list[dict[str, Any]]:
    tree = [{"pid": supervisor_pid, "parent_pid": os.getppid(), "role": "supervisor", "running": True}]
    if child_pid is not None:
        tree.append(
            {"pid": child_pid, "parent_pid": supervisor_pid, "role": "acquisition", "running": child_running}
        )
    return tree


def fatal_blockers(
    snapshot: Mapping[str, Any],
    errors: Mapping[str, Any],
    *,
    not_before_utc: str | None = None,
) -> list[str]:
    blockers: set[str] = set()
    for status in snapshot.get("status_counts", {}):
        latest = snapshot.get("latest_failure_at_utc_by_status", {}).get(status)
        is_current = not_before_utc is None or (latest is not None and str(latest) >= not_before_utc)
        if status in NO_RESTART_STATUSES and is_current:
            blockers.add(f"manifest_status:{status}")
    if snapshot.get("manifest_parse_errors"):
        blockers.add("integrity:manifest_parse_error")
    for code in errors.get("code_counts", {}):
        normalized = str(code).upper()
        if normalized in AUTH_CODES:
            blockers.add(f"authentication:{normalized}")
        if normalized in QUOTA_CODES:
            blockers.add(f"quota_or_rate_limit:{normalized}")
    if any(FATAL_TEXT_RE.search(line) for line in errors.get("stderr_tail", ())):
        blockers.add("schema_or_integrity_error")
    return sorted(blockers)


def terminal_audit(config: SupervisorConfig) -> dict[str, Any]:
    snapshot = read_manifest_snapshot(config)
    planned_ids = {cell.request_id for cell in config.planned_cells()}
    integrity_errors: list[dict[str, str]] = []
    canonical_paths: set[Path] = set()
    manifest_dir = config.root / "manifests" / "requests"
    for request_id in sorted(planned_ids):
        path = manifest_dir / f"{request_id}.json"
        if not path.is_file():
            integrity_errors.append({"request_id": request_id, "reason": "manifest_missing"})
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            integrity_errors.append({"request_id": request_id, "reason": "manifest_unreadable"})
            continue
        if payload.get("request_id") != request_id or payload.get("payload_sha256") != request_id:
            integrity_errors.append({"request_id": request_id, "reason": "manifest_identity_conflict"})
        status = payload.get("status")
        if status not in TERMINAL_STATUSES:
            integrity_errors.append({"request_id": request_id, "reason": f"non_terminal_status:{status}"})
            continue
        for path_key, hash_key, required in (
            ("bronze_path", "bronze_sha256", True),
            ("silver_path", "silver_sha256", status == "completed"),
        ):
            raw_path = payload.get(path_key)
            expected_hash = payload.get(hash_key)
            if not raw_path:
                if required:
                    integrity_errors.append({"request_id": request_id, "reason": f"{path_key}_missing"})
                continue
            artifact = _resolve_artifact_path(config.root, str(raw_path))
            canonical_paths.add(artifact)
            if not artifact.is_file():
                integrity_errors.append({"request_id": request_id, "reason": f"{path_key}_not_found"})
            elif not expected_hash or _sha256(artifact) != expected_hash:
                integrity_errors.append({"request_id": request_id, "reason": f"{path_key}_hash_mismatch"})

    all_partials = sorted(config.root.rglob("*.partial"))
    partials = [path for path in all_partials if not _is_quarantined_partial(config.root, path)]
    quarantined_partials = [str(path) for path in all_partials if _is_quarantined_partial(config.root, path)]
    partial_conflicts = []
    orphan_partials = []
    for partial in partials:
        canonical = Path(str(partial)[: -len(".partial")])
        item = {"partial": str(partial), "canonical": str(canonical)}
        if canonical.exists() or canonical in canonical_paths:
            partial_conflicts.append(item)
        else:
            orphan_partials.append(item)
    passed = (
        snapshot["accounted"] == config.expected_cells
        and snapshot["failed"] == 0
        and not integrity_errors
        and not partial_conflicts
        and not orphan_partials
        and not snapshot["unplanned_request_ids"]
    )
    return {
        "audit_version": "1.0.0",
        "audited_at_utc": _utc_now(),
        "passed": passed,
        "expected_cells": config.expected_cells,
        "completed": snapshot["completed"],
        "completed_empty": snapshot["completed_empty"],
        "accounted": snapshot["accounted"],
        "failed": snapshot["failed"],
        "retained_rows": snapshot["retained_rows"],
        "integrity_errors": integrity_errors,
        "partial_canonical_conflicts": partial_conflicts,
        "orphan_partials": orphan_partials,
        "quarantined_partials": quarantined_partials,
        "unplanned_request_ids": snapshot["unplanned_request_ids"],
    }


class RollingOptionsSupervisor:
    def __init__(self, config: SupervisorConfig) -> None:
        config.validate()
        self.config = config
        self.command = config.child_command()
        self.command_display = subprocess.list2cmdline(self.command)
        self.events_path = config.status_dir / "events.jsonl"
        self.status_json = config.status_dir / "status.json"
        self.status_md = config.status_dir / "STATUS.md"
        self.audit_json = config.status_dir / "terminal_audit.json"
        self.lock_path = config.status_dir / "supervisor.lock.json"
        self.started_at = time.time()
        self.restart_count = 0
        self.samples: deque[tuple[float, int]] = deque()
        self.capture = OutputCapture(
            config.status_dir,
            (os.environ.get("DHAN_ACCESS_TOKEN", ""), os.environ.get("DHAN_TOKEN", "")),
        )
        self.process: subprocess.Popen[str] | None = None
        self.threads: list[threading.Thread] = []
        self.child_started_at_utc: str | None = None

    def run(self) -> int:
        self._acquire_lock()
        try:
            self._emit("supervisor_started", command=self.command_display)
            while True:
                if self.process is None:
                    self._launch_child()
                assert self.process is not None
                snapshot = read_manifest_snapshot(self.config)
                errors = self.capture.summary()
                child_running = self.process.poll() is None
                status = self._build_status(snapshot, errors, child_running)
                self._write_status(status)
                if child_running:
                    time.sleep(self.config.poll_seconds)
                    continue

                exit_code = int(self.process.returncode or 0)
                for thread in self.threads:
                    thread.join(timeout=2)
                snapshot = read_manifest_snapshot(self.config)
                errors = self.capture.summary()
                blockers = fatal_blockers(snapshot, errors, not_before_utc=self.child_started_at_utc)
                if snapshot["accounted"] == self.config.expected_cells and snapshot["failed"] == 0:
                    audit = terminal_audit(self.config)
                    atomic_write_json(self.audit_json, audit)
                    state = "completed" if audit["passed"] else "blocked"
                    self._emit("terminal_audit", state=state, audit=audit)
                    self._write_status(self._build_status(snapshot, errors, False, state=state, audit=audit))
                    return 0 if audit["passed"] else 2
                if blockers:
                    self._emit("restart_suppressed", exit_code=exit_code, blockers=blockers)
                    self._write_status(
                        self._build_status(snapshot, errors, False, state="blocked", blockers=blockers)
                    )
                    return 3
                if self.restart_count >= self.config.max_restarts:
                    blockers = ["bounded_restart_limit_exhausted"]
                    self._emit("restart_suppressed", exit_code=exit_code, blockers=blockers)
                    self._write_status(
                        self._build_status(snapshot, errors, False, state="blocked", blockers=blockers)
                    )
                    return 4
                self.restart_count += 1
                backoff = self.config.restart_backoff_seconds * self.restart_count
                self._emit(
                    "unexpected_child_exit",
                    exit_code=exit_code,
                    restart_number=self.restart_count,
                    backoff_seconds=backoff,
                    resume_boundary_accounted=snapshot["accounted"],
                )
                time.sleep(backoff)
                self.process = None
        finally:
            self.lock_path.unlink(missing_ok=True)

    def _launch_child(self) -> None:
        self.capture.reset_summary()
        self.child_started_at_utc = _utc_now()
        self.process = subprocess.Popen(
            self.command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        self.threads = self.capture.start(self.process)
        self._emit(
            "child_started",
            child_pid=self.process.pid,
            restart_count=self.restart_count,
            command=self.command_display,
        )

    def _build_status(
        self,
        snapshot: Mapping[str, Any],
        errors: Mapping[str, Any],
        child_running: bool,
        *,
        state: str | None = None,
        blockers: Sequence[str] = (),
        audit: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = time.time()
        accounted = int(snapshot["accounted"])
        self.samples.append((now, accounted))
        while self.samples and now - self.samples[0][0] > 900:
            self.samples.popleft()
        rate = 0.0
        if len(self.samples) > 1 and self.samples[-1][0] > self.samples[0][0]:
            rate = (self.samples[-1][1] - self.samples[0][1]) / (self.samples[-1][0] - self.samples[0][0])
        remaining = int(snapshot["remaining"])
        eta = None if rate <= 0 else remaining / rate
        latest_age = snapshot.get("latest_completed_age_seconds")
        stalled = bool(
            child_running
            and remaining > 0
            and (latest_age is None or float(latest_age) >= self.config.stall_seconds)
            and now - self.started_at >= self.config.stall_seconds
        )
        child_pid = None if self.process is None else self.process.pid
        derived_state = state or ("stalled" if stalled else "running" if child_running else "child_exited")
        return {
            "status_version": "1.0.0",
            "updated_at_utc": _utc_now(),
            "state": derived_state,
            "supervisor_pid": os.getpid(),
            "child_pid": child_pid,
            "process_tree": process_tree(os.getpid(), child_pid, child_running),
            "command": self.command_display,
            "credentials_in_command": False,
            "credentials_persisted": False,
            "restart_count": self.restart_count,
            "max_restarts": self.config.max_restarts,
            "blockers": list(blockers),
            "manifest": dict(snapshot),
            "progress": {
                "rate_cells_per_second_15m": round(rate, 6),
                "rate_cells_per_minute_15m": round(rate * 60, 3),
                "eta_seconds": None if eta is None else round(eta, 1),
                "eta_at_utc": None
                if eta is None
                else datetime.fromtimestamp(now + eta, tz=timezone.utc).isoformat(),
            },
            "errors": dict(errors),
            "disk": disk_status(self.config.root),
            "stall": {"detected": stalled, "threshold_seconds": self.config.stall_seconds},
            "rate_limit": {
                "selected_requests_per_second": self.config.requests_per_second,
                "official_engine_cap": DATA_REQUESTS_PER_SECOND,
                "workers": self.config.workers,
                "worker_cap": 5,
                "within_limits": True,
            },
            "daily_budget": read_daily_budget(self.config.root),
            "terminal_audit": None if audit is None else dict(audit),
        }

    def _write_status(self, status: Mapping[str, Any]) -> None:
        atomic_write_json(self.status_json, status)
        atomic_write_text(self.status_md, render_status_markdown(status))
        manifest = status["manifest"]
        append_event(
            self.events_path,
            {
                "event_version": "1.0.0",
                "timestamp_utc": status["updated_at_utc"],
                "event": "status_sample",
                "state": status["state"],
                "supervisor_pid": status["supervisor_pid"],
                "child_pid": status["child_pid"],
                "completed": manifest["completed"],
                "completed_empty": manifest["completed_empty"],
                "failed": manifest["failed"],
                "accounted": manifest["accounted"],
                "retained_rows": manifest["retained_rows"],
                "latest_completed_at_utc": manifest["latest_completed_at_utc"],
                "frontier_completed_prefix_cells": manifest["frontier"]["completed_prefix_cells"],
                "rate_cells_per_minute_15m": status["progress"]["rate_cells_per_minute_15m"],
                "eta_at_utc": status["progress"]["eta_at_utc"],
                "stalled": status["stall"]["detected"],
                "error_code_counts": status["errors"]["code_counts"],
                "disk_free_bytes": status["disk"]["free_bytes"],
            },
        )

    def _emit(self, event_type: str, **values: Any) -> None:
        append_event(
            self.events_path,
            {"event_version": "1.0.0", "timestamp_utc": _utc_now(), "event": event_type, **values},
        )

    def _acquire_lock(self) -> None:
        self.config.status_dir.mkdir(parents=True, exist_ok=True)
        if self.lock_path.is_file():
            try:
                existing = json.loads(self.lock_path.read_text(encoding="utf-8"))
                pid = int(existing.get("supervisor_pid", 0))
            except (OSError, ValueError, json.JSONDecodeError):
                pid = 0
            if pid and _pid_running(pid):
                raise RuntimeError(f"supervisor already running with PID {pid}")
            self.lock_path.unlink(missing_ok=True)
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        fd = os.open(self.lock_path, flags)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump({"supervisor_pid": os.getpid(), "started_at_utc": _utc_now()}, handle)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())


def render_status_markdown(status: Mapping[str, Any]) -> str:
    manifest = status["manifest"]
    progress = status["progress"]
    disk = status["disk"]
    budget = status["daily_budget"]
    errors = status["errors"]
    return (
        "# Rolling Options Supervisor\n\n"
        f"- State: `{status['state']}`\n"
        f"- Updated UTC: `{status['updated_at_utc']}`\n"
        f"- Supervisor PID / child PID: `{status['supervisor_pid']}` / `{status['child_pid']}`\n"
        f"- Command (secret-free): `{status['command']}`\n"
        f"- Accounted: **{manifest['accounted']}/{manifest['expected_cells']}** "
        f"({manifest['completed']} completed, {manifest['completed_empty']} empty, {manifest['failed']} failed)\n"
        f"- Retained rows: **{manifest['retained_rows']}**\n"
        f"- Latest completed UTC: `{manifest['latest_completed_at_utc']}`\n"
        f"- Latest data timestamp IST: `{manifest['latest_data_timestamp_ist']}`\n"
        f"- Frontier prefix: **{manifest['frontier']['completed_prefix_cells']}** cells\n"
        f"- Rate: **{progress['rate_cells_per_minute_15m']} cells/min**; ETA UTC: `{progress['eta_at_utc']}`\n"
        f"- Stalled: **{status['stall']['detected']}** (threshold {status['stall']['threshold_seconds']}s)\n"
        f"- Error codes: `{json.dumps(errors['code_counts'], sort_keys=True)}`\n"
        f"- Daily budget: **{budget['used']}/{budget['limit']}**, remaining {budget['remaining']}\n"
        f"- Disk free: **{disk['free_gib']} GiB** on `{disk['path']}`\n"
        f"- Restart count: **{status['restart_count']}/{status['max_restarts']}**\n"
        f"- Partial files: **{len(manifest['partial_files'])}**\n"
        f"- Blockers: `{json.dumps(status['blockers'])}`\n"
    )


def _request_summary(item: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if item is None:
        return None
    return {
        "request_id": item.get("request_id"),
        "completed_at_utc": item.get("completed_at_utc"),
        "status": item.get("status"),
        "rows": item.get("rows"),
        "payload": {
            key: item.get("payload", {}).get(key)
            for key in ("fromDate", "toDate", "expiryFlag", "expiryCode", "strike", "drvOptionType")
        },
    }


def _cell_summary(cell: Any) -> dict[str, Any]:
    return {
        "request_id": cell.request_id,
        "from_date": cell.payload.get("fromDate"),
        "to_date": cell.payload.get("toDate"),
        "expiry_flag": cell.payload.get("expiryFlag"),
        "expiry_code": cell.payload.get("expiryCode"),
        "strike": cell.payload.get("strike"),
        "option_type": cell.payload.get("drvOptionType"),
    }


def _resolve_artifact_path(root: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    candidates = [Path.cwd() / path, root.parent.parent / path, root / path]
    return next((candidate for candidate in candidates if candidate.exists()), candidates[0])


def _is_quarantined_partial(root: Path, path: Path) -> bool:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return False
    parts = tuple(part.lower() for part in relative.parts)
    return len(parts) >= 2 and parts[0] == "exceptions" and parts[1] == "orphan_partials"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        handle = ctypes.windll.kernel32.OpenProcess(0x00100000, False, pid)
        if not handle:
            return False
        try:
            return ctypes.windll.kernel32.WaitForSingleObject(handle, 0) == 0x102
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="reports/dhan_phase2_backfill_20210101_20260715")
    parser.add_argument("--status-dir")
    parser.add_argument("--start-date", default="2021-01-01")
    parser.add_argument("--end-date", default="2026-07-15")
    parser.add_argument("--expiry-codes", default="1")
    parser.add_argument("--expiry-flags", default="WEEK,MONTH")
    parser.add_argument("--option-types", default="CALL,PUT")
    parser.add_argument("--moneyness-width", type=int, default=10)
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--requests-per-second", type=float, default=5.0)
    parser.add_argument("--daily-budget", type=int, default=100_000)
    parser.add_argument("--max-retries", type=int, default=4)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--poll-seconds", type=float, default=10.0)
    parser.add_argument("--stall-seconds", type=float, default=180.0)
    parser.add_argument("--max-restarts", type=int, default=3)
    parser.add_argument("--restart-backoff-seconds", type=float, default=30.0)
    parser.add_argument("--expected-cells", type=int, default=8_820)
    parser.add_argument(
        "--audit-only",
        action="store_true",
        help="Verify terminal manifests and artifact hashes without launching acquisition or requiring credentials",
    )
    return parser


def config_from_args(args: argparse.Namespace) -> SupervisorConfig:
    root = Path(args.root)
    status_dir = Path(args.status_dir) if args.status_dir else root / "supervisor"
    return SupervisorConfig(
        root=root,
        status_dir=status_dir,
        start_date=date.fromisoformat(args.start_date),
        end_date=date.fromisoformat(args.end_date),
        expiry_codes=tuple(int(value.strip()) for value in args.expiry_codes.split(",") if value.strip()),
        expiry_flags=tuple(value.strip().upper() for value in args.expiry_flags.split(",") if value.strip()),
        option_types=tuple(value.strip().upper() for value in args.option_types.split(",") if value.strip()),
        moneyness_width=args.moneyness_width,
        workers=args.workers,
        requests_per_second=args.requests_per_second,
        daily_budget=args.daily_budget,
        max_retries=args.max_retries,
        timeout_seconds=args.timeout_seconds,
        poll_seconds=args.poll_seconds,
        stall_seconds=args.stall_seconds,
        max_restarts=args.max_restarts,
        restart_backoff_seconds=args.restart_backoff_seconds,
        expected_cells=args.expected_cells,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = config_from_args(args)
    if args.audit_only:
        config.validate()
        audit = terminal_audit(config)
        config.status_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_json(config.status_dir / "terminal_audit.json", audit)
        print(json.dumps(audit, sort_keys=True))
        return 0 if audit["passed"] else 1
    return RollingOptionsSupervisor(config).run()


if __name__ == "__main__":
    raise SystemExit(main())
