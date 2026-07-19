"""Conservative Windows orchestration for a completed SPAN downloader run.

The orchestration layer deliberately owns no download or extraction logic.  It
waits for explicitly named downloader processes, proves that no other process
can append to the same download journal, launches at most one repair from the
current checkout, and then delegates extraction to the existing resumable CLI
before running the pilot auditor and fail-closed Phase 1 finalizer.

Runtime evidence is written outside the append-only source journals.  Raw
archives and manifests are inputs only, except when the delegated repair or the
already-running follower/extractor append through their normal code paths.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
import calendar
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any, Callable, Iterable, Mapping, Sequence

from .backfill import extraction_compaction_lock
from .backfill_downloader import DOWNLOADED_STATES, SLOT_SPECS
from .availability import load_availability_events
from .corrupt_recovery import validate_corrupt_recovery_report
from .manifest_exports import read_stable_jsonl_prefix
from .phase1_finalizer import PINNED_CELLS, PINNED_END, PINNED_START


SCHEMA_VERSION = "span-phase1-postrun/v3"
REPAIR_CONCURRENCY = 1
REPAIR_QUEUE_SIZE = 2
REPAIR_MAX_ATTEMPTS = 4
REPAIR_INCOMPLETE_PASSES = 2
REPAIR_TIMEOUT_SECONDS = 600.0
REPAIR_SESSION_REFRESH_REQUESTS = 20
CORRUPT_RECOVERY_TIMEOUT_SECONDS = 600.0
CORRUPT_RECOVERY_MAX_ATTEMPTS = 3
EXTRACT_BATCH_ROWS = 50_000
EXTRACT_PARSE_WORKERS = 2
FOLLOWER_RETIREMENT_TIMEOUT_SECONDS = 300.0
PROCESS_SNAPSHOT_MAX_ATTEMPTS = 5
PROCESS_SNAPSHOT_RETRY_SECONDS = 2.0
PROCESS_SNAPSHOT_TIMEOUT_SECONDS = 30.0
_SAFE_PREFIX = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_MANIFEST_ARGUMENT = re.compile(
    r"--download-manifest(?:=|\s+)(?:\"([^\"]+)\"|'([^']+)'|(\S+))",
    re.IGNORECASE,
)
_SECRET_ARGUMENT = re.compile(
    r"(?i)(--(?:access[-_]?token|token|secret|password|api[-_]?key))(?:=|\s+)(\S+)"
)


@dataclass(frozen=True)
class ProcessRecord:
    pid: int
    parent_pid: int | None
    name: str
    command_line: str
    creation_date: str | None = None


@dataclass(frozen=True)
class MatrixSummary:
    expected_cells: int
    accounted_cells: int
    terminal_cells: int
    nonterminal_cells: int
    out_of_range_cells: int
    latest_state_counts: Mapping[str, int]
    source_event_count: int
    source_prefix_sha256: str
    ignored_trailing_bytes: int

    @property
    def fully_terminal(self) -> bool:
        return (
            self.expected_cells == PINNED_CELLS
            and self.accounted_cells == self.expected_cells
            and self.terminal_cells == self.expected_cells
            and self.nonterminal_cells == 0
            and self.out_of_range_cells == 0
            and self.ignored_trailing_bytes == 0
        )


@dataclass(frozen=True)
class ExtractionGap:
    downloaded_sources: int
    extracted_sources: int
    missing_sources: tuple[str, ...]

    @property
    def caught_up(self) -> bool:
        return not self.missing_sources


@dataclass(frozen=True)
class DownloadManifestSnapshot:
    canonical_path: str
    snapshot_path: str
    sha256: str
    size_bytes: int
    event_count: int


class FollowerRetirementError(RuntimeError):
    """Follower retirement failed with durable, status-safe evidence."""

    def __init__(self, message: str, evidence: Mapping[str, Any]) -> None:
        super().__init__(message)
        self.evidence = dict(evidence)


@dataclass(frozen=True)
class PostrunConfig:
    repo_root: Path
    run_root: Path
    wait_for_pids: tuple[int, ...]
    follower_pids: tuple[int, ...]
    log_prefix: str
    availability_manifest: Path
    availability_import: Path
    provenance_root: Path
    benchmark_artifacts: tuple[Path, ...]
    pilot_output_root: Path | None = None
    skip_generic_repair: bool = False
    skip_follower_catchup: bool = False
    retire_followers_before_full_extract: bool = False
    follower_retirement_timeout_seconds: float = FOLLOWER_RETIREMENT_TIMEOUT_SECONDS
    follower_timeout_seconds: float = 21_600.0
    evidence_timeout_seconds: float = 21_600.0
    quiescence_seconds: float = 120.0
    poll_seconds: float = 15.0
    test_result: str | None = None

    def validated(self) -> "PostrunConfig":
        repo = self.repo_root.resolve()
        run = self.run_root.resolve()
        if not repo.is_dir():
            raise FileNotFoundError(f"repository does not exist: {repo}")
        if not run.is_dir():
            raise FileNotFoundError(f"run root does not exist: {run}")
        if not _SAFE_PREFIX.fullmatch(self.log_prefix):
            raise ValueError("log_prefix must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}")
        wait_pids = _positive_unique_pids(self.wait_for_pids, "wait_for_pids")
        follower_pids = (
            ()
            if self.skip_follower_catchup and not self.follower_pids
            else _positive_unique_pids(self.follower_pids, "follower_pids")
        )
        if self.skip_follower_catchup:
            if follower_pids:
                raise ValueError(
                    "skip_follower_catchup requires follower_pids to be empty"
                )
            if self.retire_followers_before_full_extract:
                raise ValueError(
                    "skip_follower_catchup cannot request follower retirement"
                )
        if self.follower_timeout_seconds <= 0:
            raise ValueError("follower_timeout_seconds must be > 0")
        if self.follower_retirement_timeout_seconds <= 0:
            raise ValueError("follower_retirement_timeout_seconds must be > 0")
        if self.evidence_timeout_seconds <= 0:
            raise ValueError("evidence_timeout_seconds must be > 0")
        if self.quiescence_seconds <= 0:
            raise ValueError("quiescence_seconds must be > 0")
        if self.poll_seconds <= 0:
            raise ValueError("poll_seconds must be > 0")
        if self.quiescence_seconds < self.poll_seconds:
            raise ValueError("quiescence_seconds must be >= poll_seconds")
        python = repo / ".venv" / "Scripts" / "python.exe"
        if not python.is_file():
            raise FileNotFoundError(
                f"checkout virtualenv Python does not exist: {python}"
            )
        manifest = run / "manifests" / "download.jsonl"
        if not manifest.is_file():
            raise FileNotFoundError(f"download manifest does not exist: {manifest}")
        availability = self.availability_manifest.resolve()
        if not availability.is_file():
            raise FileNotFoundError(
                f"availability manifest does not exist: {availability}"
            )
        availability_import = self.availability_import.resolve()
        if not availability_import.is_file():
            raise FileNotFoundError(
                f"availability import does not exist: {availability_import}"
            )
        provenance_root = self.provenance_root.resolve()
        if not provenance_root.is_dir():
            raise FileNotFoundError(
                f"availability provenance root does not exist: {provenance_root}"
            )
        benchmarks = tuple(path.resolve() for path in self.benchmark_artifacts)
        if not benchmarks:
            raise ValueError("at least one benchmark artifact is required")
        return PostrunConfig(
            repo_root=repo,
            run_root=run,
            wait_for_pids=wait_pids,
            follower_pids=follower_pids,
            log_prefix=self.log_prefix,
            availability_manifest=availability,
            availability_import=availability_import,
            provenance_root=provenance_root,
            benchmark_artifacts=benchmarks,
            pilot_output_root=(
                self.pilot_output_root.resolve()
                if self.pilot_output_root
                else run / "reports" / "required_pilots"
            ),
            skip_generic_repair=bool(self.skip_generic_repair),
            skip_follower_catchup=bool(self.skip_follower_catchup),
            retire_followers_before_full_extract=bool(
                self.retire_followers_before_full_extract
            ),
            follower_retirement_timeout_seconds=float(
                self.follower_retirement_timeout_seconds
            ),
            follower_timeout_seconds=float(self.follower_timeout_seconds),
            evidence_timeout_seconds=float(self.evidence_timeout_seconds),
            quiescence_seconds=float(self.quiescence_seconds),
            poll_seconds=float(self.poll_seconds),
            test_result=self.test_result,
        )


def summarize_download_matrix(
    manifest: str | Path,
    *,
    start_date: date = PINNED_START,
    end_date: date = PINNED_END,
) -> MatrixSummary:
    """Return the exact latest-cell state of one stable download journal."""
    snapshot = read_stable_jsonl_prefix(manifest)
    latest: dict[tuple[str, str], Mapping[str, Any]] = {}
    out_of_range = 0
    slots = {slot for slot, _suffix in SLOT_SPECS}
    for _line_number, event in snapshot.events:
        day_text = str(event.get("trading_date", ""))
        slot = str(event.get("slot", ""))
        try:
            day = date.fromisoformat(day_text)
        except ValueError:
            out_of_range += 1
            continue
        if slot not in slots or not (start_date <= day <= end_date):
            out_of_range += 1
            continue
        latest[(day_text, slot)] = event
    expected = ((end_date - start_date).days + 1) * len(SLOT_SPECS)
    terminal = sum(event.get("terminal") is True for event in latest.values())
    states: dict[str, int] = {}
    for event in latest.values():
        state = str(event.get("state", "<missing>"))
        states[state] = states.get(state, 0) + 1
    return MatrixSummary(
        expected_cells=expected,
        accounted_cells=len(latest),
        terminal_cells=terminal,
        nonterminal_cells=len(latest) - terminal,
        out_of_range_cells=out_of_range,
        latest_state_counts=dict(sorted(states.items())),
        source_event_count=snapshot.event_count,
        source_prefix_sha256=snapshot.prefix_sha256,
        ignored_trailing_bytes=snapshot.ignored_trailing_bytes,
    )


def extraction_gap(
    download_manifest: str | Path, extraction_manifest: str | Path
) -> ExtractionGap:
    """Compare latest downloaded source hashes with successful extraction events."""
    return _extraction_gap(download_manifest, extraction_manifest, eligible_months=None)


def eligible_terminal_extraction_gap(
    download_manifest: str | Path,
    extraction_manifest: str | Path,
    *,
    current_day: date | None = None,
) -> ExtractionGap:
    """Return the gap for closed months the continuously running follower can select."""
    download = read_stable_jsonl_prefix(download_manifest)
    if download.ignored_trailing_bytes:
        raise ValueError("download journal has an unterminated tail")
    eligible = _eligible_terminal_months(
        (event for _line, event in download.events),
        current_day=current_day or datetime.now().astimezone().date(),
    )
    return _extraction_gap(
        download_manifest, extraction_manifest, eligible_months=eligible
    )


def _eligible_terminal_months(
    events: Iterable[Mapping[str, Any]], *, current_day: date
) -> frozenset[tuple[int, int]]:
    latest: dict[tuple[date, str], Mapping[str, Any]] = {}
    for event in events:
        try:
            day = date.fromisoformat(str(event.get("trading_date", "")))
        except ValueError:
            continue
        slot = str(event.get("slot", ""))
        if slot:
            latest[(day, slot)] = event
    current_month = (current_day.year, current_day.month)
    result: set[tuple[int, int]] = set()
    for year, month in sorted({(day.year, day.month) for day, _slot in latest}):
        if (year, month) >= current_month:
            continue
        last_day = calendar.monthrange(year, month)[1]
        if all(
            latest.get((date(year, month, day_number), slot), {}).get("terminal")
            is True
            for day_number in range(1, last_day + 1)
            for slot, _suffix in SLOT_SPECS
        ):
            result.add((year, month))
    return frozenset(result)


def _extraction_gap(
    download_manifest: str | Path,
    extraction_manifest: str | Path,
    *,
    eligible_months: frozenset[tuple[int, int]] | None,
) -> ExtractionGap:
    download = read_stable_jsonl_prefix(download_manifest)
    extraction = read_stable_jsonl_prefix(extraction_manifest)
    if download.ignored_trailing_bytes or extraction.ignored_trailing_bytes:
        raise ValueError("download/extraction journal has an unterminated tail")
    latest_download: dict[tuple[str, str], Mapping[str, Any]] = {}
    for _line, event in download.events:
        key = (str(event.get("trading_date", "")), str(event.get("slot", "")))
        if all(key):
            latest_download[key] = event
    latest_extraction: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    for _line, event in extraction.events:
        key = (
            str(event.get("date", event.get("trading_date", ""))),
            str(event.get("slot", "")),
            str(event.get("source_sha256", "")),
        )
        if all(key):
            latest_extraction[key] = event
    expected = {
        (day, slot, str(event.get("sha256")))
        for (day, slot), event in latest_download.items()
        if event.get("state") in DOWNLOADED_STATES
        and isinstance(event.get("sha256"), str)
        and event.get("sha256")
        and (eligible_months is None or _event_month(day) in eligible_months)
    }
    successful = {
        key
        for key, event in latest_extraction.items()
        if event.get("event") in {"fragment_created", "fragment_already_valid"}
    }
    missing = sorted(expected - successful)
    return ExtractionGap(
        downloaded_sources=len(expected),
        extracted_sources=len(expected & successful),
        missing_sources=tuple("|".join(item) for item in missing),
    )


def _event_month(day_text: str) -> tuple[int, int]:
    day = date.fromisoformat(day_text)
    return day.year, day.month


def process_targets_manifest(
    process: ProcessRecord, manifest: str | Path, repo_root: str | Path
) -> bool:
    """Identify a downloader writer for exactly one manifest path."""
    command = process.command_line or ""
    normalized_command = command.lower().replace("/", "\\")
    if "span-backfill" not in normalized_command or not re.search(
        r"span-backfill(?:\.exe)?\s+download(?:\s|$)", normalized_command
    ):
        return False
    match = _MANIFEST_ARGUMENT.search(command)
    if match is None:
        return False
    raw = next(value for value in match.groups() if value is not None).strip("\"'")
    candidate = Path(raw)
    if not candidate.is_absolute():
        candidate = Path(repo_root) / candidate
    return os.path.normcase(str(candidate.resolve())) == os.path.normcase(
        str(Path(manifest).resolve())
    )


def find_manifest_writers(
    processes: Iterable[ProcessRecord], manifest: str | Path, repo_root: str | Path
) -> tuple[ProcessRecord, ...]:
    return tuple(
        sorted(
            (
                process
                for process in processes
                if process_targets_manifest(process, manifest, repo_root)
            ),
            key=lambda item: item.pid,
        )
    )


def process_tree(
    processes: Iterable[ProcessRecord], root_pid: int
) -> tuple[ProcessRecord, ...]:
    """Return a deterministic root/descendant snapshot from one inventory."""
    records = {item.pid: item for item in processes}
    selected: set[int] = {root_pid} if root_pid in records else set()
    changed = True
    while changed:
        changed = False
        for item in records.values():
            if item.parent_pid in selected and item.pid not in selected:
                selected.add(item.pid)
                changed = True
    return tuple(records[pid] for pid in sorted(selected))


def process_targets_follower(
    process: ProcessRecord, manifest: str | Path, repo_root: str | Path
) -> bool:
    """Identify the completed-month follower bound to this download journal."""
    command = process.command_line or ""
    if "follow_span_completed_months.py" not in command.lower():
        return False
    match = _MANIFEST_ARGUMENT.search(command)
    if match is None:
        return False
    raw = next(value for value in match.groups() if value is not None).strip("\"'")
    candidate = Path(raw)
    if not candidate.is_absolute():
        candidate = Path(repo_root) / candidate
    return os.path.normcase(str(candidate.resolve())) == os.path.normcase(
        str(Path(manifest).resolve())
    )


def _is_direct_system_console_descendant(
    process: ProcessRecord, explicit_followers: Mapping[int, ProcessRecord]
) -> bool:
    """Allow only Windows' console host directly owned by an explicit follower."""
    if (
        process.parent_pid not in explicit_followers
        or process.name.casefold() != "conhost.exe"
        or not process.creation_date
    ):
        return False
    command = process.command_line.strip()
    if command.startswith("\\??\\"):
        command = command[4:]
    if command.startswith('"'):
        closing_quote = command.find('"', 1)
        if closing_quote < 0:
            return False
        executable = command[1:closing_quote]
    else:
        executable = command.split(maxsplit=1)[0] if command else ""
    expected = (
        Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32" / "conhost.exe"
    )
    return os.path.normcase(os.path.normpath(executable)) == os.path.normcase(
        os.path.normpath(str(expected))
    )


def _approved_console_descendants(
    captured_pids: Iterable[int],
    explicit_followers: Mapping[int, ProcessRecord],
    observed: Mapping[int, ProcessRecord],
) -> dict[int, ProcessRecord]:
    return {
        pid: observed[pid]
        for pid in captured_pids
        if pid not in explicit_followers
        and _is_direct_system_console_descendant(observed[pid], explicit_followers)
    }


def retire_followers_at_boundary(
    config: PostrunConfig,
    manifest: str | Path,
    initial_followers: Mapping[int, ProcessRecord | None],
    journal: str | Path,
    *,
    process_inventory: Callable[[], Sequence[ProcessRecord]] | None = None,
    terminate_tree: Callable[[int, float], Mapping[str, Any]] | None = None,
    lock_factory: Callable[..., Any] | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> Mapping[str, Any]:
    """Retire only the exact validated follower trees at a locked idle boundary."""
    cfg = config.validated()
    if not cfg.retire_followers_before_full_extract:
        raise ValueError("follower retirement requires explicit opt-in")
    inventory = process_inventory or list_windows_processes
    terminator = terminate_tree or _terminate_follower_tree
    acquire = lock_factory or extraction_compaction_lock
    manifest_path = Path(manifest).resolve()
    extraction_manifest = cfg.run_root / "manifests" / "extraction.jsonl"
    journal_path = Path(journal)
    timeout_seconds = cfg.follower_retirement_timeout_seconds
    evidence: dict[str, Any] = {
        "enabled": True,
        "manifest": str(manifest_path),
        "extraction_lock": str(
            extraction_manifest.parent / ".span-extract-compact.lock"
        ),
        "timeout_seconds": timeout_seconds,
        "initial_explicit_pids": list(cfg.follower_pids),
        "started_at_utc": _utc_now(),
        "outcome": "RUNNING",
    }
    _append_event(
        journal_path,
        "follower_retirement_started",
        explicit_pids=list(cfg.follower_pids),
        manifest=str(manifest_path),
        timeout_seconds=timeout_seconds,
    )
    try:
        with acquire(
            extraction_manifest,
            timeout_seconds=timeout_seconds,
            poll_seconds=min(0.25, timeout_seconds),
        ):
            observed = {item.pid: item for item in inventory()}
            active = _validated_retirement_followers(
                cfg, manifest_path, initial_followers, observed
            )
            roots = _validated_follower_roots(active, observed)
            captured_pids = sorted(
                {
                    item.pid
                    for root in roots
                    for item in process_tree(observed.values(), root.pid)
                }
            )
            console_descendants = _approved_console_descendants(
                captured_pids, active, observed
            )
            unapproved_descendants = sorted(
                set(captured_pids) - set(active) - set(console_descendants)
            )
            if unapproved_descendants:
                raise RuntimeError(
                    "follower tree contains non-explicit descendants; refusing termination: "
                    + ",".join(str(pid) for pid in unapproved_descendants)
                )
            evidence["validated_processes"] = [
                _process_evidence(active[pid]) for pid in sorted(active)
            ]
            evidence["validated_console_descendants"] = [
                _process_evidence(console_descendants[pid])
                for pid in sorted(console_descendants)
            ]
            evidence["validated_roots"] = [root.pid for root in roots]
            evidence["captured_tree_pids"] = captured_pids
            _append_event(
                journal_path,
                "follower_retirement_validated",
                roots=[root.pid for root in roots],
                process_tree=evidence["validated_processes"],
            )

            deadline = clock() + timeout_seconds
            attempts: list[Mapping[str, Any]] = []
            for root in roots:
                # Re-inventory immediately before each irreversible termination.
                immediate = {item.pid: item for item in inventory()}
                current_active = _validated_retirement_followers(
                    cfg, manifest_path, initial_followers, immediate
                )
                current_root = current_active.get(root.pid)
                if current_root is None:
                    attempts.append(
                        {"root_pid": root.pid, "state": "already_exited_before_signal"}
                    )
                    continue
                immediate_tree = process_tree(immediate.values(), root.pid)
                immediate_tree_pids = {item.pid for item in immediate_tree}
                immediate_console = _approved_console_descendants(
                    immediate_tree_pids, current_active, immediate
                )
                immediate_unapproved = sorted(
                    immediate_tree_pids - set(current_active) - set(immediate_console)
                )
                if immediate_unapproved:
                    raise RuntimeError(
                        "follower tree contains non-explicit descendants immediately "
                        "before termination; refusing termination: "
                        + ",".join(str(pid) for pid in immediate_unapproved)
                    )
                for pid, process in immediate_console.items():
                    original = console_descendants.get(pid)
                    if original is None:
                        raise RuntimeError(
                            "new console descendant appeared immediately before "
                            f"termination; refusing termination: {pid}"
                        )
                    if process != original:
                        raise RuntimeError(
                            "console descendant identity changed immediately before "
                            f"termination; refusing termination: {pid}"
                        )
                remaining = deadline - clock()
                if remaining <= 0:
                    raise RuntimeError(
                        "follower retirement deadline expired before signal"
                    )
                result = dict(terminator(root.pid, remaining))
                result.setdefault("root_pid", root.pid)
                attempts.append(result)
            evidence["termination_attempts"] = attempts

            while True:
                remaining_inventory = {item.pid: item for item in inventory()}
                explicit_survivors = sorted(
                    pid for pid in cfg.follower_pids if pid in remaining_inventory
                )
                same_manifest_survivors = sorted(
                    item.pid
                    for item in remaining_inventory.values()
                    if process_targets_follower(item, manifest_path, cfg.repo_root)
                )
                tree_survivors = sorted(
                    pid for pid in captured_pids if pid in remaining_inventory
                )
                if not (
                    explicit_survivors or same_manifest_survivors or tree_survivors
                ):
                    break
                if clock() >= deadline:
                    raise RuntimeError(
                        "follower retirement timed out; survivors explicit="
                        f"{explicit_survivors}, same_manifest={same_manifest_survivors}, "
                        f"captured_tree={tree_survivors}"
                    )
                sleep(min(cfg.poll_seconds, max(0.01, deadline - clock())))

            evidence["outcome"] = "RETIRED"
            evidence["finished_at_utc"] = _utc_now()
            evidence["remaining_explicit_pids"] = []
            evidence["remaining_same_manifest_follower_pids"] = []
            _append_event(
                journal_path,
                "follower_retirement_completed",
                roots=[root.pid for root in roots],
                captured_tree_pids=captured_pids,
                termination_attempts=attempts,
            )
            return evidence
    except Exception as exc:
        evidence["outcome"] = "FAIL"
        evidence["finished_at_utc"] = _utc_now()
        evidence["error"] = f"{type(exc).__name__}: {exc}"
        _append_event(
            journal_path,
            "follower_retirement_failed",
            error=evidence["error"],
        )
        raise FollowerRetirementError(str(exc), evidence) from exc


def _validated_retirement_followers(
    cfg: PostrunConfig,
    manifest: Path,
    initial_followers: Mapping[int, ProcessRecord | None],
    observed: Mapping[int, ProcessRecord],
) -> dict[int, ProcessRecord]:
    same_manifest = {
        item.pid
        for item in observed.values()
        if process_targets_follower(item, manifest, cfg.repo_root)
    }
    unexpected = sorted(same_manifest - set(cfg.follower_pids))
    if unexpected:
        raise RuntimeError(
            "unlisted same-manifest follower exists; refusing termination: "
            + ",".join(str(pid) for pid in unexpected)
        )
    validated: dict[int, ProcessRecord] = {}
    for pid in cfg.follower_pids:
        current = observed.get(pid)
        initial = initial_followers.get(pid)
        if current is None:
            continue
        if initial is None:
            raise RuntimeError(
                f"explicit follower PID {pid} appeared after initial validation"
            )
        if not initial.creation_date or not current.creation_date:
            raise RuntimeError(
                f"explicit follower PID {pid} lacks creation-date evidence"
            )
        if current.creation_date != initial.creation_date:
            raise RuntimeError(f"explicit follower PID {pid} was reused")
        if current.name != initial.name or current.command_line != initial.command_line:
            raise RuntimeError(f"explicit follower PID {pid} command identity changed")
        if not process_targets_follower(current, manifest, cfg.repo_root):
            raise RuntimeError(
                f"explicit follower PID {pid} is no longer bound to {manifest}"
            )
        validated[pid] = current
    return validated


def _validated_follower_roots(
    active: Mapping[int, ProcessRecord], observed: Mapping[int, ProcessRecord]
) -> tuple[ProcessRecord, ...]:
    active_pids = set(active)
    roots = tuple(
        sorted(
            (item for item in active.values() if item.parent_pid not in active_pids),
            key=lambda item: item.pid,
        )
    )
    covered = {
        item.pid
        for root in roots
        for item in process_tree(observed.values(), root.pid)
        if item.pid in active_pids
    }
    if covered != active_pids:
        raise RuntimeError("explicit follower process tree could not be rooted safely")
    return roots


def _process_evidence(process: ProcessRecord) -> dict[str, Any]:
    return {
        "pid": process.pid,
        "parent_pid": process.parent_pid,
        "name": process.name,
        "creation_date": process.creation_date,
        "command_line": redact_command_line(process.command_line),
    }


def _terminate_follower_tree(
    root_pid: int, timeout_seconds: float
) -> Mapping[str, Any]:
    completed = subprocess.run(
        ["taskkill.exe", "/PID", str(root_pid), "/T", "/F"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=max(0.01, timeout_seconds),
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    return {
        "root_pid": root_pid,
        "state": "termination_requested",
        "return_code": completed.returncode,
    }


def decide_repair(
    matrix: MatrixSummary,
    active_writers: Sequence[ProcessRecord],
    *,
    prior_launch_recorded: bool,
    skip_generic_repair: bool = False,
) -> str:
    """Pure fail-closed launch decision used by runtime and tests."""
    if active_writers:
        return "REFUSE_ACTIVE_MANIFEST_WRITER"
    if matrix.fully_terminal:
        return "SKIP_MATRIX_FULLY_TERMINAL"
    if skip_generic_repair:
        return "SKIP_GENERIC_REPAIR_EXPLICIT_REARM"
    if prior_launch_recorded:
        return "REFUSE_PRIOR_REPAIR_LAUNCH"
    return "LAUNCH_ONE_REPAIR"


def build_repair_command(config: PostrunConfig) -> tuple[str, ...]:
    cfg = config.validated()
    return (
        str(cfg.repo_root / ".venv" / "Scripts" / "python.exe"),
        "-u",
        "-m",
        "nifty_span.cli",
        "span-backfill",
        "download",
        "--start-date",
        PINNED_START.isoformat(),
        "--end-date",
        PINNED_END.isoformat(),
        "--raw-root",
        str(cfg.run_root / "raw"),
        "--download-manifest",
        str(cfg.run_root / "manifests" / "download.jsonl"),
        "--download-concurrency",
        str(REPAIR_CONCURRENCY),
        "--queue-size",
        str(REPAIR_QUEUE_SIZE),
        "--max-attempts",
        str(REPAIR_MAX_ATTEMPTS),
        "--retry-incomplete-passes",
        str(REPAIR_INCOMPLETE_PASSES),
        "--timeout-seconds",
        str(int(REPAIR_TIMEOUT_SECONDS)),
        "--session-refresh-requests",
        str(REPAIR_SESSION_REFRESH_REQUESTS),
        "--json",
    )


def build_corrupt_recovery_command(config: PostrunConfig) -> tuple[str, ...]:
    """Build the fixed, secret-free recovery command run after follower retirement."""

    cfg = config.validated()
    return (
        str(cfg.repo_root / ".venv" / "Scripts" / "python.exe"),
        "-u",
        "-m",
        "nifty_span.cli",
        "span-backfill",
        "recover-corrupt",
        "--start-date",
        PINNED_START.isoformat(),
        "--end-date",
        PINNED_END.isoformat(),
        "--raw-root",
        str(cfg.run_root / "raw"),
        "--download-manifest",
        str(cfg.run_root / "manifests" / "download.jsonl"),
        "--availability-manifest",
        str(cfg.availability_manifest),
        "--report-root",
        str(cfg.run_root / "reports" / "corrupt_recovery"),
        "--corrupt-timeout-seconds",
        str(int(CORRUPT_RECOVERY_TIMEOUT_SECONDS)),
        "--corrupt-max-attempts",
        str(CORRUPT_RECOVERY_MAX_ATTEMPTS),
        "--json",
    )


def build_availability_classification_command(
    config: PostrunConfig,
) -> tuple[str, ...]:
    """Build the exact-range official availability import command."""

    cfg = config.validated()
    return (
        str(cfg.repo_root / ".venv" / "Scripts" / "python.exe"),
        "-u",
        "-m",
        "nifty_span.cli",
        "span-backfill",
        "classify",
        "--start-date",
        PINNED_START.isoformat(),
        "--end-date",
        PINNED_END.isoformat(),
        "--raw-root",
        str(cfg.run_root / "raw"),
        "--download-manifest",
        str(cfg.run_root / "manifests" / "download.jsonl"),
        "--availability-manifest",
        str(cfg.availability_manifest),
        "--availability-import",
        str(cfg.availability_import),
        "--provenance-root",
        str(cfg.provenance_root),
        "--json",
    )


def validate_availability_classification_result(
    config: PostrunConfig, *, stdout_path: str | Path, exit_code: int
) -> dict[str, Any]:
    """Validate classifier JSON/exit semantics and all retained source evidence."""

    cfg = config.validated()
    path = Path(stdout_path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"availability classifier JSON is unreadable: {exc}"
        ) from exc
    if not isinstance(payload, Mapping):
        raise RuntimeError("availability classifier result is not an object")
    expected = {
        "start_date": PINNED_START.isoformat(),
        "end_date": PINNED_END.isoformat(),
        "availability_manifest": str(cfg.availability_manifest),
        "provenance_root": str(cfg.provenance_root),
    }
    for field, value in expected.items():
        if payload.get(field) != value:
            raise RuntimeError(
                f"availability classifier {field}={payload.get(field)!r}, expected {value!r}"
            )
    counts: dict[str, int] = {}
    for field in (
        "imported_dates",
        "classified_missing_cells",
        "unresolved_missing_cells",
        "source_boundary_cells",
        "retained_sources",
    ):
        value = payload.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise RuntimeError(
                f"availability classifier has invalid {field}: {value!r}"
            )
        counts[field] = value
    expected_exit = 0 if counts["unresolved_missing_cells"] == 0 else 1
    if exit_code != expected_exit:
        raise RuntimeError(
            f"availability classifier exit {exit_code} disagrees with "
            f"unresolved_missing_cells={counts['unresolved_missing_cells']}"
        )
    # Loading re-verifies every retained source URL, artifact path, size, and hash.
    latest = load_availability_events(cfg.availability_manifest)
    accepted_latest = sum(
        event.get("classification_outcome") == "accepted_absence"
        for event in latest.values()
    )
    boundary_latest = sum(
        event.get("classification_outcome") == "source_boundary"
        and event.get("source_availability_boundary_proven") is True
        for event in latest.values()
    )
    if accepted_latest < counts["classified_missing_cells"]:
        raise RuntimeError(
            "availability classifier claims more accepted absences than the "
            "validated manifest contains"
        )
    if boundary_latest < counts["source_boundary_cells"]:
        raise RuntimeError(
            "availability classifier claims more source boundaries than the "
            "validated manifest contains"
        )
    return {
        **counts,
        "latest_accepted_absence_events": accepted_latest,
        "latest_source_boundary_events": boundary_latest,
        "latest_validated_events": len(latest),
        "result_json": str(path.resolve()),
    }


def validate_corrupt_recovery_artifact(
    config: PostrunConfig,
    report_root: str | Path,
    *,
    files_before: Iterable[str],
    exit_code: int,
) -> dict[str, Any]:
    """Require one fresh successful classifier report matching its exit code."""

    root = Path(report_root)
    prior = {str(Path(value).resolve()) for value in files_before}
    fresh = sorted(
        path
        for path in root.glob("span_corrupt_recovery_*.json")
        if str(path.resolve()) not in prior
    )
    if len(fresh) != 1:
        raise RuntimeError(
            f"corrupt recovery must publish exactly one fresh JSON report; found {len(fresh)}"
        )
    path = fresh[0].resolve()
    cfg = config.validated()
    evidence = validate_corrupt_recovery_report(
        path,
        start_date=PINNED_START,
        end_date=PINNED_END,
        raw_root=cfg.run_root / "raw",
        download_manifest=cfg.run_root / "manifests" / "download.jsonl",
        availability_manifest=cfg.availability_manifest,
    )
    unresolved = int(evidence["unresolved_cells"])
    unresolved_corrupt = int(evidence["unresolved_corrupt_cells"])
    unresolved_missing = int(evidence["unresolved_missing_cells"])
    ok = evidence["ok"] is True
    expected_exit = 0 if unresolved == 0 else 1
    if exit_code != expected_exit or ok != (unresolved == 0):
        raise RuntimeError("corrupt/static recovery exit and report disagree")
    if unresolved_corrupt != 0:
        raise RuntimeError("corrupt/static recovery left corrupt cells unresolved")
    if unresolved_missing < 0 or unresolved_missing > unresolved:
        raise RuntimeError("corrupt/static recovery has invalid missing-cell counts")
    return evidence


def publish_download_manifest_snapshot(
    canonical_manifest: str | Path, snapshot_root: str | Path
) -> DownloadManifestSnapshot:
    """Publish one immutable content-addressed copy of a complete canonical journal."""
    canonical = Path(canonical_manifest).resolve()
    observed = read_stable_jsonl_prefix(canonical)
    if observed.ignored_trailing_bytes:
        raise ValueError(f"download journal has an unterminated tail: {canonical}")
    content = canonical.read_bytes()
    digest = sha256(content).hexdigest()
    if len(content) != observed.prefix_bytes or digest != observed.prefix_sha256:
        raise RuntimeError(f"download journal changed while snapshotting: {canonical}")

    root = Path(snapshot_root).resolve()
    destination = root / f"{digest}.jsonl"
    if destination.exists():
        if (
            destination.stat().st_size != len(content)
            or _sha256_file(destination) != digest
        ):
            raise RuntimeError(
                f"existing download snapshot failed integrity: {destination}"
            )
    else:
        _atomic_write_bytes(destination, content)

    frozen = read_stable_jsonl_prefix(destination)
    if (
        frozen.ignored_trailing_bytes
        or frozen.prefix_sha256 != digest
        or frozen.prefix_bytes != len(content)
        or frozen.event_count != observed.event_count
    ):
        raise RuntimeError(
            f"published download snapshot failed integrity: {destination}"
        )
    return DownloadManifestSnapshot(
        canonical_path=str(canonical),
        snapshot_path=str(destination),
        sha256=digest,
        size_bytes=len(content),
        event_count=observed.event_count,
    )


def verify_canonical_manifest_unchanged(
    snapshot: DownloadManifestSnapshot,
    processes: Iterable[ProcessRecord],
    repo_root: str | Path,
) -> None:
    """Fail if a writer exists or either side of the frozen boundary changed."""
    canonical = Path(snapshot.canonical_path)
    frozen_path = Path(snapshot.snapshot_path)
    writers = find_manifest_writers(processes, canonical, repo_root)
    if writers:
        raise RuntimeError(
            "download manifest writer active at frozen extraction boundary: "
            + ",".join(str(item.pid) for item in writers)
        )
    frozen = read_stable_jsonl_prefix(frozen_path)
    current = read_stable_jsonl_prefix(canonical)
    for label, observed in (("snapshot", frozen), ("canonical", current)):
        if observed.ignored_trailing_bytes:
            raise RuntimeError(f"{label} download journal has an unterminated tail")
        if (
            observed.prefix_sha256 != snapshot.sha256
            or observed.prefix_bytes != snapshot.size_bytes
            or observed.event_count != snapshot.event_count
        ):
            raise RuntimeError(
                f"{label} download journal changed across frozen boundary"
            )


def build_extract_command(
    config: PostrunConfig, snapshot: DownloadManifestSnapshot
) -> tuple[str, ...]:
    """Build the one explicit resumable full-range extraction command."""
    cfg = config.validated()
    return (
        str(cfg.repo_root / ".venv" / "Scripts" / "python.exe"),
        "-u",
        "-m",
        "nifty_span.cli",
        "span-backfill",
        "extract",
        "--start-date",
        PINNED_START.isoformat(),
        "--end-date",
        PINNED_END.isoformat(),
        "--raw-root",
        str(cfg.run_root / "raw"),
        "--download-manifest",
        snapshot.snapshot_path,
        "--availability-manifest",
        str(cfg.availability_manifest),
        "--fragment-root",
        str(cfg.run_root / "fragments"),
        "--extraction-manifest",
        str(cfg.run_root / "manifests" / "extraction.jsonl"),
        "--parquet-root",
        str(cfg.run_root / "compacted"),
        "--quarantine-root",
        str(cfg.run_root / "exceptions" / "duplicate_conflicts"),
        "--report-root",
        str(cfg.run_root / "reports"),
        "--symbols",
        "NIFTY",
        "--batch-rows",
        str(EXTRACT_BATCH_ROWS),
        "--parse-workers",
        str(EXTRACT_PARSE_WORKERS),
        "--json",
    )


def validate_post_extract_boundary(
    snapshot: DownloadManifestSnapshot,
    extraction_manifest: str | Path,
    *,
    extract_exit_code: int,
    processes: Iterable[ProcessRecord],
    repo_root: str | Path,
) -> ExtractionGap:
    """Require a successful extractor, unchanged source journal, and zero full gap."""
    if extract_exit_code != 0:
        raise RuntimeError(f"full-range extraction exited {extract_exit_code}")
    verify_canonical_manifest_unchanged(snapshot, processes, repo_root)
    gap = extraction_gap(snapshot.snapshot_path, extraction_manifest)
    if not gap.caught_up:
        raise RuntimeError(
            f"full-range extraction gap remains for {len(gap.missing_sources)} source hashes"
        )
    return gap


def build_pilot_command(config: PostrunConfig) -> tuple[str, ...]:
    cfg = config.validated()
    return (
        str(cfg.repo_root / ".venv" / "Scripts" / "python.exe"),
        str(cfg.repo_root / "scripts" / "audit_span_required_pilots.py"),
        "--run-root",
        str(cfg.run_root),
        "--output-root",
        str(cfg.pilot_output_root),
        "--json",
    )


def build_finalizer_command(
    config: PostrunConfig,
    *,
    commit_sha: str,
    pilot_artifact: str | Path,
    recovery_artifact: str | Path,
) -> tuple[str, ...]:
    cfg = config.validated()
    command: list[str] = [
        str(cfg.repo_root / ".venv" / "Scripts" / "python.exe"),
        str(cfg.repo_root / "scripts" / "finalize_span_phase1.py"),
        "--run-root",
        str(cfg.run_root),
        "--start-date",
        PINNED_START.isoformat(),
        "--end-date",
        PINNED_END.isoformat(),
        "--availability-manifest",
        str(cfg.availability_manifest),
    ]
    for artifact in cfg.benchmark_artifacts:
        command.extend(("--benchmark-artifact", str(artifact)))
    command.extend(("--pilot-artifact", str(Path(pilot_artifact).resolve())))
    command.extend(("--recovery-artifact", str(Path(recovery_artifact).resolve())))
    command.extend(("--commit-sha", commit_sha))
    if cfg.test_result:
        command.extend(("--test-result", cfg.test_result))
    command.extend(("--tool-version", f"python={sys.version.split()[0]}"))
    return tuple(command)


def classify_orchestration_outcome(
    *,
    matrix_full: bool,
    catchup_complete: bool,
    pilot_status: str | None,
    finalizer_outcome: str | None,
    blocked_matrix_ready: bool = False,
) -> str:
    """Never promote WAITING, FAIL, or BLOCKED evidence to success."""
    if finalizer_outcome == "BLOCKED_SOURCE":
        if blocked_matrix_ready and catchup_complete:
            return "BLOCKED_SOURCE"
        return "WAITING" if not catchup_complete else "FAIL"
    if pilot_status == "WAITING" or not catchup_complete:
        return "WAITING"
    if (
        matrix_full
        and catchup_complete
        and pilot_status == "PASS"
        and finalizer_outcome == "PASS_READY"
    ):
        return "PASS_READY"
    return "FAIL"


def post_repair_matrix_status(matrix: MatrixSummary) -> str:
    """Describe the matrix without short-circuiting source-boundary finalization."""
    if matrix.fully_terminal:
        return "FULLY_TERMINAL"
    return "STABLE_INCOMPLETE_CONTINUE_TO_FINALIZER"


def validated_subprocess_outcome(
    kind: str, value: str | None, exit_code: int
) -> str | None:
    """Reject a freshly written artifact when its process exit contract disagrees."""
    expected = {
        "pilot": {"PASS": 0, "FAIL": 1, "WAITING": 2},
        "finalizer": {"PASS_READY": 0, "FAIL_INCOMPLETE": 1, "BLOCKED_SOURCE": 1},
    }
    if kind not in expected or value not in expected[kind]:
        return None
    return value if exit_code == expected[kind][value] else None


def apply_benchmark_wait(outcome: str, *, evidence_complete: bool) -> str:
    """Let a proven source block outrank missing benchmark evidence."""
    if outcome == "BLOCKED_SOURCE":
        return outcome
    return outcome if evidence_complete else "WAITING"


def missing_benchmark_artifacts(config: PostrunConfig) -> tuple[str, ...]:
    cfg = config.validated()
    return tuple(str(path) for path in cfg.benchmark_artifacts if not path.is_file())


def redact_command(argv: Sequence[str]) -> list[str]:
    """Return report-safe argv even if a future caller adds a secret flag."""
    result: list[str] = []
    redact_next = False
    for item in argv:
        if redact_next:
            result.append("<redacted>")
            redact_next = False
            continue
        if re.fullmatch(
            r"(?i)--(?:access[-_]?token|token|secret|password|api[-_]?key)", item
        ):
            result.append(item)
            redact_next = True
        elif re.match(
            r"(?i)--(?:access[-_]?token|token|secret|password|api[-_]?key)=", item
        ):
            result.append(item.split("=", 1)[0] + "=<redacted>")
        else:
            result.append(item)
    return result


def run_postrun(config: PostrunConfig) -> Mapping[str, Any]:
    """Execute the conservative post-run state machine on Windows."""
    cfg = config.validated()
    if os.name != "nt":
        raise RuntimeError("SPAN post-run orchestration is supported only on Windows")
    paths = _runtime_paths(cfg)
    paths["report_root"].mkdir(parents=True, exist_ok=True)
    paths["log_root"].mkdir(parents=True, exist_ok=True)
    lock_fd = _acquire_lock(paths["lock"])
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "started_at_utc": _utc_now(),
        "outcome": "RUNNING",
        "config": {
            "repo_root": str(cfg.repo_root),
            "run_root": str(cfg.run_root),
            "wait_for_pids": list(cfg.wait_for_pids),
            "follower_pids": list(cfg.follower_pids),
            "skip_generic_repair": cfg.skip_generic_repair,
            "skip_follower_catchup": cfg.skip_follower_catchup,
            "retire_followers_before_full_extract": (
                cfg.retire_followers_before_full_extract
            ),
            "follower_retirement_timeout_seconds": (
                cfg.follower_retirement_timeout_seconds
            ),
            "log_prefix": cfg.log_prefix,
            "evidence_timeout_seconds": cfg.evidence_timeout_seconds,
            "repair": {
                "download_concurrency": REPAIR_CONCURRENCY,
                "queue_size": REPAIR_QUEUE_SIZE,
                "max_attempts": REPAIR_MAX_ATTEMPTS,
                "retry_incomplete_passes": REPAIR_INCOMPLETE_PASSES,
                "timeout_seconds": REPAIR_TIMEOUT_SECONDS,
            },
            "corrupt_recovery": {
                "concurrency": 1,
                "max_attempts": CORRUPT_RECOVERY_MAX_ATTEMPTS,
                "timeout_seconds": CORRUPT_RECOVERY_TIMEOUT_SECONDS,
                "source": "exact_official_static_archive_url",
            },
            "availability_classification": {
                "import": str(cfg.availability_import),
                "provenance_root": str(cfg.provenance_root),
                "range": [PINNED_START.isoformat(), PINNED_END.isoformat()],
                "source": "reviewed_official_availability_import",
            },
            "full_range_extraction": {
                "batch_rows": EXTRACT_BATCH_ROWS,
                "parse_workers": EXTRACT_PARSE_WORKERS,
                "source": "immutable_content_addressed_download_snapshot",
            },
        },
        "artifacts": {
            name: str(path) for name, path in paths.items() if name != "lock"
        },
        "checkout": _checkout_evidence(cfg.repo_root),
    }
    try:
        _publish_status(paths, payload)
        manifest = cfg.run_root / "manifests" / "download.jsonl"
        initial_followers = _validate_and_wait_exact_downloaders(
            cfg, manifest, payload, paths
        )
        processes = list_windows_processes()
        writers = find_manifest_writers(processes, manifest, cfg.repo_root)
        prior = _journal_has_event(paths["journal"], "repair_launched")
        before = summarize_download_matrix(manifest)
        decision = decide_repair(
            before,
            writers,
            prior_launch_recorded=prior,
            skip_generic_repair=cfg.skip_generic_repair,
        )
        payload["matrix_before_repair"] = asdict(before)
        payload["repair_decision"] = decision
        _append_event(
            paths["journal"],
            "repair_decision",
            decision=decision,
            explicit_skip_generic_repair=cfg.skip_generic_repair,
            wait_for_pids=list(cfg.wait_for_pids),
            matrix_source_prefix_sha256=before.source_prefix_sha256,
            matrix_terminal_cells=before.terminal_cells,
            matrix_nonterminal_cells=before.nonterminal_cells,
        )
        if decision.startswith("REFUSE_"):
            payload["outcome"] = "FAIL"
            payload["reason"] = decision
            return payload
        if decision == "LAUNCH_ONE_REPAIR":
            repair = _run_logged_process(
                build_repair_command(cfg),
                cwd=cfg.repo_root,
                stdout_path=paths["repair_stdout"],
                stderr_path=paths["repair_stderr"],
                journal=paths["journal"],
                event_prefix="repair",
            )
            payload["repair"] = repair
            payload["repair"]["nonzero_requires_finalizer"] = repair["exit_code"] != 0
        lingering_writers = find_manifest_writers(
            list_windows_processes(), manifest, cfg.repo_root
        )
        if lingering_writers:
            payload["outcome"] = "FAIL"
            payload["reason"] = "manifest writer remained after repair process exit"
            payload["lingering_writer_pids"] = [item.pid for item in lingering_writers]
            return payload
        after = summarize_download_matrix(manifest)
        payload["matrix_after_repair"] = asdict(after)
        payload["post_repair_matrix_status"] = post_repair_matrix_status(after)

        if cfg.skip_follower_catchup:
            catchup = {
                "complete": True,
                "outcome": "SKIPPED_EXPLICIT_NO_FOLLOWER",
                "reason": (
                    "no follower process is configured; immutable full-range snapshot "
                    "extraction remains mandatory later in this run"
                ),
                "full_range_extraction_required": True,
            }
            _append_event(
                paths["journal"],
                "follower_catchup_skipped",
                decision="SKIP_FOLLOWER_CATCHUP_EXPLICIT_NO_FOLLOWER",
                explicit_skip_follower_catchup=True,
                follower_pids=[],
                matrix_source_prefix_sha256=after.source_prefix_sha256,
                full_range_extraction_required=True,
            )
        else:
            catchup = _wait_for_follower_catchup(cfg, paths)
        payload["eligible_follower_catchup"] = catchup
        if not catchup["complete"]:
            payload["outcome"] = "WAITING"
            payload["reason"] = catchup["reason"]
            return payload

        if cfg.skip_follower_catchup:
            payload["follower_retirement"] = {
                "enabled": False,
                "outcome": "SKIPPED_EXPLICIT_NO_FOLLOWER",
                "reason": "no follower process was configured or eligible for retirement",
            }
        elif cfg.retire_followers_before_full_extract:
            payload["follower_retirement"] = retire_followers_at_boundary(
                cfg,
                manifest,
                initial_followers,
                paths["journal"],
            )
        else:
            payload["follower_retirement"] = {
                "enabled": False,
                "outcome": "SKIPPED_NOT_OPTED_IN",
            }

        availability_before = _file_version(cfg.availability_manifest)
        availability_classification = _run_logged_process(
            build_availability_classification_command(cfg),
            cwd=cfg.repo_root,
            stdout_path=paths["availability_classification_stdout"],
            stderr_path=paths["availability_classification_stderr"],
            journal=paths["journal"],
            event_prefix="availability_classification",
        )
        availability_after = _file_version(cfg.availability_manifest)
        payload["availability_classification"] = {
            **availability_classification,
            "manifest_before": availability_before,
            "manifest_after": availability_after,
        }
        payload["availability_classification"].update(
            validate_availability_classification_result(
                cfg,
                stdout_path=paths["availability_classification_stdout"],
                exit_code=int(availability_classification["exit_code"]),
            )
        )

        corrupt_report_root = paths["corrupt_report_root"]
        corrupt_report_root.mkdir(parents=True, exist_ok=True)
        corrupt_before = tuple(
            str(path.resolve())
            for path in corrupt_report_root.glob("span_corrupt_recovery_*.json")
        )
        corrupt_recovery = _run_logged_process(
            build_corrupt_recovery_command(cfg),
            cwd=cfg.repo_root,
            stdout_path=paths["corrupt_recovery_stdout"],
            stderr_path=paths["corrupt_recovery_stderr"],
            journal=paths["journal"],
            event_prefix="corrupt_recovery",
        )
        payload["corrupt_recovery"] = corrupt_recovery
        corrupt_evidence = validate_corrupt_recovery_artifact(
            cfg,
            corrupt_report_root,
            files_before=corrupt_before,
            exit_code=int(corrupt_recovery["exit_code"]),
        )
        payload["corrupt_recovery"].update(corrupt_evidence)
        after = summarize_download_matrix(manifest)
        payload["matrix_after_corrupt_recovery"] = asdict(after)
        payload["post_corrupt_recovery_matrix_status"] = post_repair_matrix_status(
            after
        )

        snapshot = publish_download_manifest_snapshot(manifest, paths["snapshot_root"])
        payload["download_manifest_snapshot"] = asdict(snapshot)
        verify_canonical_manifest_unchanged(
            snapshot, list_windows_processes(), cfg.repo_root
        )
        extraction = _run_logged_process(
            build_extract_command(cfg, snapshot),
            cwd=cfg.repo_root,
            stdout_path=paths["extract_stdout"],
            stderr_path=paths["extract_stderr"],
            journal=paths["journal"],
            event_prefix="extract",
        )
        payload["full_range_extraction"] = extraction
        full_gap = validate_post_extract_boundary(
            snapshot,
            cfg.run_root / "manifests" / "extraction.jsonl",
            extract_exit_code=int(extraction["exit_code"]),
            processes=list_windows_processes(),
            repo_root=cfg.repo_root,
        )
        payload["full_range_extraction_gap"] = asdict(full_gap)
        if not _journals_quiescent_now(cfg, paths):
            payload["outcome"] = "WAITING"
            payload["reason"] = "journals changed after frozen full-range extraction"
            return payload

        pilot_artifact = Path(cfg.pilot_output_root) / "span_required_pilots.json"
        pilot_before = _file_version(pilot_artifact)
        pilot = _run_logged_process(
            build_pilot_command(cfg),
            cwd=cfg.repo_root,
            stdout_path=paths["pilot_stdout"],
            stderr_path=paths["pilot_stderr"],
            journal=paths["journal"],
            event_prefix="pilot",
        )
        pilot_after = _file_version(pilot_artifact)
        raw_pilot_status = (
            _read_json_field(pilot_artifact, "overall_status")
            if pilot_after is not None and pilot_after != pilot_before
            else None
        )
        pilot_status = validated_subprocess_outcome(
            "pilot", raw_pilot_status, int(pilot["exit_code"])
        )
        payload["pilot"] = {
            **pilot,
            "artifact": str(pilot_artifact),
            "status": pilot_status,
        }

        evidence_wait = _wait_for_benchmark_artifacts(cfg)
        payload["benchmark_evidence_wait"] = evidence_wait

        # Re-prove quiescence immediately before invoking the finalizer.  The
        # finalizer independently fingerprints all journals before and after.
        if not _journals_quiescent_now(cfg, paths):
            payload["outcome"] = "WAITING"
            payload["reason"] = "journals changed after the pilot audit"
            return payload
        commit_sha = _git_head(cfg.repo_root)
        final_artifact = (
            cfg.run_root / "reports" / "final" / "SPAN_PHASE1_COMPLETION.json"
        )
        final_before = _file_version(final_artifact)
        finalizer = _run_logged_process(
            build_finalizer_command(
                cfg,
                commit_sha=commit_sha,
                pilot_artifact=pilot_artifact,
                recovery_artifact=corrupt_evidence["artifact"],
            ),
            cwd=cfg.repo_root,
            stdout_path=paths["finalizer_stdout"],
            stderr_path=paths["finalizer_stderr"],
            journal=paths["journal"],
            event_prefix="finalizer",
        )
        final_after = _file_version(final_artifact)
        raw_finalizer_outcome = (
            _read_json_field(final_artifact, "outcome")
            if final_after is not None and final_after != final_before
            else None
        )
        blocked_matrix_ready = (
            _read_json_value(final_artifact, "blocked_matrix_ready") is True
            if final_after is not None and final_after != final_before
            else False
        )
        finalizer_outcome = validated_subprocess_outcome(
            "finalizer", raw_finalizer_outcome, int(finalizer["exit_code"])
        )
        payload["finalizer"] = {
            **finalizer,
            "artifact": str(final_artifact),
            "outcome": finalizer_outcome,
            "blocked_matrix_ready": blocked_matrix_ready,
        }
        payload["outcome"] = classify_orchestration_outcome(
            matrix_full=after.fully_terminal,
            catchup_complete=True,
            pilot_status=pilot_status,
            finalizer_outcome=finalizer_outcome,
            blocked_matrix_ready=blocked_matrix_ready,
        )
        payload["outcome"] = apply_benchmark_wait(
            str(payload["outcome"]), evidence_complete=bool(evidence_wait["complete"])
        )
        if not evidence_wait["complete"] and payload["outcome"] == "WAITING":
            payload["reason"] = evidence_wait["reason"]
        if payload["outcome"] != "PASS_READY":
            payload.setdefault(
                "reason",
                "acceptance evidence did not pass; inspect pilot and finalizer artifacts",
            )
        return payload
    except Exception as exc:  # noqa: BLE001 - all failures become durable evidence.
        if isinstance(exc, FollowerRetirementError):
            payload["follower_retirement"] = exc.evidence
        payload["outcome"] = "FAIL"
        payload["reason"] = f"{type(exc).__name__}: {exc}"
        _append_event(paths["journal"], "orchestrator_failed", error=payload["reason"])
        return payload
    finally:
        payload["finished_at_utc"] = _utc_now()
        try:
            _publish_status(paths, payload)
        finally:
            os.close(lock_fd)
            paths["lock"].unlink(missing_ok=True)


def list_windows_processes() -> tuple[ProcessRecord, ...]:
    script = (
        "Get-CimInstance Win32_Process | "
        "Select-Object ProcessId,ParentProcessId,Name,CommandLine,CreationDate | "
        "ConvertTo-Json -Compress"
    )
    last_error: BaseException | None = None
    for attempt in range(1, PROCESS_SNAPSHOT_MAX_ATTEMPTS + 1):
        try:
            completed = subprocess.run(
                ["powershell.exe", "-NoLogo", "-NoProfile", "-Command", script],
                check=True,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=PROCESS_SNAPSHOT_TIMEOUT_SECONDS,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            raw = completed.stdout.strip()
            if not raw:
                raise ValueError("Windows process inventory was empty")
            values = json.loads(raw)
            if isinstance(values, Mapping):
                values = [values]
            if not isinstance(values, list) or not values:
                raise ValueError("Windows process inventory contained no records")
            return tuple(
                ProcessRecord(
                    pid=int(item["ProcessId"]),
                    parent_pid=(
                        int(item["ParentProcessId"])
                        if item.get("ParentProcessId")
                        else None
                    ),
                    name=str(item.get("Name") or ""),
                    command_line=str(item.get("CommandLine") or ""),
                    creation_date=(
                        str(item["CreationDate"]) if item.get("CreationDate") else None
                    ),
                )
                for item in values
            )
        except (
            json.JSONDecodeError,
            KeyError,
            OSError,
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            TypeError,
            ValueError,
        ) as exc:
            last_error = exc
            if attempt < PROCESS_SNAPSHOT_MAX_ATTEMPTS:
                time.sleep(PROCESS_SNAPSHOT_RETRY_SECONDS)

    raise RuntimeError(
        "Windows process inventory failed after "
        f"{PROCESS_SNAPSHOT_MAX_ATTEMPTS} attempts: "
        f"{type(last_error).__name__}: {last_error}"
    ) from last_error


def _validate_and_wait_exact_downloaders(
    cfg: PostrunConfig,
    manifest: Path,
    payload: dict[str, Any],
    paths: Mapping[str, Path],
) -> Mapping[int, ProcessRecord | None]:
    initial = {item.pid: item for item in list_windows_processes()}
    observations: list[dict[str, Any]] = []
    for pid in cfg.wait_for_pids:
        process = initial.get(pid)
        if process is None:
            observations.append({"pid": pid, "state": "already_exited"})
            continue
        if not process_targets_manifest(process, manifest, cfg.repo_root):
            raise RuntimeError(
                f"explicit wait PID {pid} is not the expected downloader for {manifest}"
            )
        observations.append(
            {
                "pid": pid,
                "parent_pid": process.parent_pid,
                "creation_date": process.creation_date,
                "state": "validated_waiting",
                "command_line": redact_command_line(process.command_line),
            }
        )
    payload["explicit_downloader_pids"] = observations
    follower_observations: list[dict[str, Any]] = []
    initial_followers: dict[int, ProcessRecord | None] = {}
    for pid in cfg.follower_pids:
        process = initial.get(pid)
        initial_followers[pid] = process
        if process is None:
            follower_observations.append({"pid": pid, "state": "already_exited"})
            continue
        if not process_targets_follower(process, manifest, cfg.repo_root):
            raise RuntimeError(
                f"explicit follower PID {pid} is not bound to {manifest}"
            )
        follower_observations.append(
            {
                "pid": pid,
                "parent_pid": process.parent_pid,
                "creation_date": process.creation_date,
                "state": "validated_existing_follower",
                "command_line": redact_command_line(process.command_line),
            }
        )
    payload["explicit_follower_pids"] = follower_observations
    _append_event(
        paths["journal"], "waiting_for_exact_pids", pids=list(cfg.wait_for_pids)
    )
    remaining = set(cfg.wait_for_pids) & set(initial)
    while remaining:
        time.sleep(cfg.poll_seconds)
        current = {item.pid: item for item in list_windows_processes()}
        for pid in tuple(remaining):
            if pid not in current:
                remaining.remove(pid)
            else:
                original = initial[pid]
                observed = current[pid]
                if (
                    original.creation_date
                    and observed.creation_date != original.creation_date
                ):
                    raise RuntimeError(f"PID {pid} was reused while waiting")
    _append_event(paths["journal"], "exact_pid_boundary_reached")
    return initial_followers


def _wait_for_follower_catchup(
    cfg: PostrunConfig, paths: Mapping[str, Path]
) -> dict[str, Any]:
    deadline = time.monotonic() + cfg.follower_timeout_seconds
    stable_since: float | None = None
    previous: tuple[tuple[str, int, int, str], ...] | None = None
    latest_gap: ExtractionGap | None = None
    while time.monotonic() < deadline:
        processes = {item.pid: item for item in list_windows_processes()}
        writers = find_manifest_writers(
            processes.values(),
            cfg.run_root / "manifests" / "download.jsonl",
            cfg.repo_root,
        )
        if writers:
            return {
                "complete": False,
                "reason": "a downloader writer reappeared while waiting for follower",
                "writer_pids": [item.pid for item in writers],
            }
        try:
            fingerprints = _journal_fingerprints(cfg)
            latest_gap = eligible_terminal_extraction_gap(
                cfg.run_root / "manifests" / "download.jsonl",
                cfg.run_root / "manifests" / "extraction.jsonl",
            )
        except (FileNotFoundError, ValueError):
            stable_since = None
            previous = None
            time.sleep(cfg.poll_seconds)
            continue
        if fingerprints == previous:
            stable_since = stable_since or time.monotonic()
        else:
            previous = fingerprints
            stable_since = time.monotonic()
        if (
            latest_gap.caught_up
            and stable_since is not None
            and time.monotonic() - stable_since >= cfg.quiescence_seconds
        ):
            return {
                "complete": True,
                "reason": (
                    "all follower-eligible terminal months are extracted and journals "
                    "are quiescent"
                ),
                "gap": asdict(latest_gap),
                "journal_fingerprints": fingerprints,
            }
        if cfg.follower_pids and not any(pid in processes for pid in cfg.follower_pids):
            return {
                "complete": False,
                "reason": "all explicit follower PIDs exited before catch-up",
            }
        time.sleep(cfg.poll_seconds)
    return {
        "complete": False,
        "reason": "bounded follower catch-up timeout expired",
        "gap": asdict(latest_gap) if latest_gap else None,
    }


def _wait_for_benchmark_artifacts(cfg: PostrunConfig) -> dict[str, Any]:
    deadline = time.monotonic() + cfg.evidence_timeout_seconds
    while True:
        missing = tuple(
            str(path) for path in cfg.benchmark_artifacts if not path.is_file()
        )
        if not missing:
            return {
                "complete": True,
                "reason": "all declared benchmark artifacts exist",
                "artifacts": [str(path) for path in cfg.benchmark_artifacts],
            }
        if time.monotonic() >= deadline:
            return {
                "complete": False,
                "reason": "bounded benchmark-evidence wait expired",
                "missing": list(missing),
            }
        time.sleep(cfg.poll_seconds)


def _journals_quiescent_now(cfg: PostrunConfig, paths: Mapping[str, Path]) -> bool:
    before = _journal_fingerprints(cfg)
    time.sleep(cfg.quiescence_seconds)
    processes = list_windows_processes()
    writers = find_manifest_writers(
        processes, cfg.run_root / "manifests" / "download.jsonl", cfg.repo_root
    )
    return not writers and before == _journal_fingerprints(cfg)


def _journal_fingerprints(cfg: PostrunConfig) -> tuple[tuple[str, int, int, str], ...]:
    paths = (
        cfg.run_root / "manifests" / "download.jsonl",
        cfg.run_root / "manifests" / "extraction.jsonl",
        cfg.availability_manifest,
    )
    result = []
    for path in paths:
        snapshot = read_stable_jsonl_prefix(path)
        if snapshot.ignored_trailing_bytes:
            raise ValueError(f"journal has an unterminated tail: {path}")
        stat = path.stat()
        result.append((str(path), stat.st_size, stat.st_mtime_ns, _sha256_file(path)))
    return tuple(result)


def _run_logged_process(
    argv: Sequence[str],
    *,
    cwd: Path,
    stdout_path: Path,
    stderr_path: Path,
    journal: Path,
    event_prefix: str,
) -> dict[str, Any]:
    safe_command = redact_command(argv)
    started = _utc_now()
    with (
        stdout_path.open("ab", buffering=0) as stdout,
        stderr_path.open("ab", buffering=0) as stderr,
    ):
        process = subprocess.Popen(
            list(argv),
            cwd=cwd,
            stdout=stdout,
            stderr=stderr,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        time.sleep(0.25)
        observed_tree = process_tree(list_windows_processes(), process.pid)
        tree_evidence = [
            {
                "pid": item.pid,
                "parent_pid": item.parent_pid,
                "name": item.name,
                "creation_date": item.creation_date,
                "command_line": redact_command_line(item.command_line),
            }
            for item in observed_tree
        ]
        _append_event(
            journal,
            f"{event_prefix}_launched",
            pid=process.pid,
            command=safe_command,
            started_at_utc=started,
            stdout=str(stdout_path),
            stderr=str(stderr_path),
            process_tree=tree_evidence,
        )
        exit_code = process.wait()
    finished = _utc_now()
    _append_event(
        journal,
        f"{event_prefix}_exited",
        pid=process.pid,
        exit_code=exit_code,
        finished_at_utc=finished,
    )
    return {
        "pid": process.pid,
        "process_tree": tree_evidence,
        "command": safe_command,
        "command_line": subprocess.list2cmdline(safe_command),
        "started_at_utc": started,
        "finished_at_utc": finished,
        "exit_code": exit_code,
        "stdout": str(stdout_path),
        "stderr": str(stderr_path),
    }


def _runtime_paths(cfg: PostrunConfig) -> dict[str, Path]:
    report_root = cfg.run_root / "reports" / "postrun"
    log_root = cfg.run_root / "logs"
    prefix = cfg.log_prefix
    return {
        "report_root": report_root,
        "log_root": log_root,
        "status_json": report_root / f"{prefix}.status.json",
        "status_markdown": report_root / f"{prefix}.status.md",
        "journal": report_root / f"{prefix}.events.jsonl",
        "lock": report_root / f"{prefix}.lock",
        "snapshot_root": report_root / "download_snapshots",
        "corrupt_report_root": cfg.run_root / "reports" / "corrupt_recovery",
        "repair_stdout": log_root / f"{prefix}.repair.stdout.log",
        "repair_stderr": log_root / f"{prefix}.repair.stderr.log",
        "availability_classification_stdout": log_root
        / f"{prefix}.availability-classification.stdout.log",
        "availability_classification_stderr": log_root
        / f"{prefix}.availability-classification.stderr.log",
        "corrupt_recovery_stdout": log_root / f"{prefix}.corrupt-recovery.stdout.log",
        "corrupt_recovery_stderr": log_root / f"{prefix}.corrupt-recovery.stderr.log",
        "extract_stdout": log_root / f"{prefix}.extract.stdout.log",
        "extract_stderr": log_root / f"{prefix}.extract.stderr.log",
        "pilot_stdout": log_root / f"{prefix}.pilot.stdout.log",
        "pilot_stderr": log_root / f"{prefix}.pilot.stderr.log",
        "finalizer_stdout": log_root / f"{prefix}.finalizer.stdout.log",
        "finalizer_stderr": log_root / f"{prefix}.finalizer.stderr.log",
    }


def _publish_status(paths: Mapping[str, Path], payload: Mapping[str, Any]) -> None:
    _atomic_write(
        paths["status_json"],
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
    )
    lines = [
        "# SPAN Phase 1 post-run orchestration",
        "",
        f"Outcome: **{payload.get('outcome', 'UNKNOWN')}**",
        "",
        f"- Started UTC: `{payload.get('started_at_utc', '')}`",
        f"- Finished UTC: `{payload.get('finished_at_utc', '')}`",
        f"- Reason: `{payload.get('reason', '')}`",
        f"- Status JSON: `{paths['status_json']}`",
        f"- Event journal: `{paths['journal']}`",
        "",
        "`PASS_READY` is emitted only when the matrix, follower catch-up, required",
        "pilots, and fail-closed finalizer all pass. `BLOCKED_SOURCE`, `WAITING`,",
        "and `FAIL` are terminal report states, never aliases for success.",
    ]
    _atomic_write(paths["status_markdown"], "\n".join(lines) + "\n")


def _append_event(path: Path, event: str, **values: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"event": event, "recorded_at_utc": _utc_now(), **values}
    encoded = (
        json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()
    with path.open("ab", buffering=0) as stream:
        stream.write(encoded)
        os.fsync(stream.fileno())


def _journal_has_event(path: Path, event: str) -> bool:
    if not path.is_file():
        return False
    snapshot = read_stable_jsonl_prefix(path)
    if snapshot.ignored_trailing_bytes:
        raise ValueError(f"orchestration journal has an unterminated tail: {path}")
    return any(item.get("event") == event for _line, item in snapshot.events)


def _acquire_lock(path: Path) -> int:
    try:
        return os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise RuntimeError(f"orchestration lock already exists: {path}") from exc


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.{os.getpid()}.partial")
    with partial.open("w", encoding="utf-8", newline="\n") as stream:
        stream.write(text)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(partial, path)


def _atomic_write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.{os.getpid()}.partial")
    try:
        with partial.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(partial, path)
    finally:
        partial.unlink(missing_ok=True)


def _read_json_field(path: Path, field: str) -> str | None:
    value = _read_json_value(path, field)
    return str(value) if value is not None else None


def _read_json_value(path: Path, field: str) -> Any:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload.get(field) if isinstance(payload, Mapping) else None


def _file_version(path: Path) -> tuple[int, int, str] | None:
    if not path.is_file():
        return None
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns, _sha256_file(path)


def _checkout_evidence(repo: Path) -> dict[str, Any]:
    head = _git_head(repo)
    completed = subprocess.run(
        ["git", "status", "--short"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    dirty_paths = [line for line in completed.stdout.splitlines() if line.strip()]
    return {"commit_sha": head, "dirty": bool(dirty_paths), "status_short": dirty_paths}


def _git_head(repo: Path) -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return completed.stdout.strip()


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def redact_command_line(command_line: str) -> str:
    return _SECRET_ARGUMENT.sub(
        lambda match: f"{match.group(1)} <redacted>", command_line
    )


def _positive_unique_pids(values: Sequence[int], name: str) -> tuple[int, ...]:
    result = tuple(dict.fromkeys(int(value) for value in values))
    if not result or any(value <= 0 for value in result):
        raise ValueError(f"{name} must contain at least one positive PID")
    return result


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")
