"""Bounded exact-static recovery for corrupt and unresolved NSE SPAN slots.

This command is intentionally a post-download operation.  It refuses to run
while a downloader targets the same journal, freezes the reports-API evidence
into an immutable snapshot, and requests each unresolved corrupt slot from its
exact official static URL.  Valid archives pass through the downloader's
existing immutable validation/publication path.  Invalid bytes never enter the
canonical raw tree.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any
import asyncio
import inspect
import json
import os
import random
import re
import subprocess
import uuid
import zipfile
import zlib

from .availability import (
    CORRUPT_ARCHIVE_EVENT_SCHEMA,
    REPEATED_STATIC_404_BASIS,
    REPEATED_STATIC_BOUNDARY_EVENT,
    REPEATED_STATIC_BOUNDARY_EVENT_SCHEMA,
    REPEATED_STATIC_CORRUPT_BASIS,
    load_availability_events,
)
from .backfill_downloader import (
    DOWNLOADED_STATES,
    MISSING_STATES,
    SLOT_BY_SUFFIX,
    SLOT_SPECS,
    ZIP_CONTENT_TYPES,
    ArchiveResourceLimits,
    ClientFactory,
    ClientLike,
    SleepFn,
    _ArchiveError,
    _InnerArchive,
    _curl_client_factory,
    _persist_inner_archive,
    _validate_inner_archive,
)
from .durable_jsonl import append_jsonl_record
from .manifest_exports import StableJsonlPrefix, read_stable_jsonl_prefix

STATIC_ARCHIVE_ROOT = "https://nsearchives.nseindia.com/archives/nsccl/span"
RECOVERY_REPORT_SCHEMA = "span-corrupt-static-recovery/v1"
BUNDLE_VALIDATION_BLOCKED_STATE = "bundle_validation_blocked"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_ZIP_MAGICS = (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")
_SAFE_RESPONSE_HEADERS = frozenset(
    {"content-length", "content-type", "etag", "last-modified"}
)


@dataclass(frozen=True)
class ProcessRecord:
    pid: int
    parent_pid: int | None
    name: str
    command_line: str
    creation_date: str | None = None


ProcessProvider = Callable[[], Iterable[ProcessRecord]]


@dataclass(frozen=True)
class CorruptRecoveryConfig:
    """Conservative fixed-concurrency recovery settings."""

    max_attempts: int = 3
    timeout_seconds: float = 600.0
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 30.0
    jitter_seconds: float = 0.5
    include_missing_targets: bool = True
    archive_limits: ArchiveResourceLimits = field(default_factory=ArchiveResourceLimits)

    def validated(self) -> "CorruptRecoveryConfig":
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be > 0")
        if self.backoff_base_seconds < 0:
            raise ValueError("backoff_base_seconds must be >= 0")
        if self.backoff_max_seconds < self.backoff_base_seconds:
            raise ValueError("backoff_max_seconds must be >= backoff_base_seconds")
        if self.jitter_seconds < 0:
            raise ValueError("jitter_seconds must be >= 0")
        self.archive_limits.validated()
        return self


@dataclass(frozen=True)
class CorruptRecoveryCell:
    trading_date: str
    slot: str
    suffix: str
    source_state: str
    source_event_id: str | None
    source_line_number: int
    static_url: str
    disposition: str
    network_attempts: int
    static_status: int | None = None
    static_sha256: str | None = None
    static_size_bytes: int | None = None
    expected_sha256: str | None = None
    expected_size_bytes: int | None = None
    canonical_path: str | None = None
    availability_event_id: str | None = None
    validation_state: str | None = None
    error: str | None = None
    evidence_basis: str | None = None


@dataclass(frozen=True)
class CorruptRecoveryReport:
    schema_version: str
    run_id: str
    started_at_utc: str
    finished_at_utc: str
    start_date: str
    end_date: str
    concurrency: int
    timeout_seconds: float
    max_attempts: int
    source_manifest: str
    source_snapshot: str
    source_snapshot_sha256: str
    source_snapshot_size_bytes: int
    source_snapshot_events: int
    raw_root: str
    availability_manifest: str
    selected_cells: int
    network_calls: int
    recovered_cells: int
    classified_source_corrupt_cells: int
    already_classified_cells: int
    unresolved_cells: int
    unresolved_corrupt_cells: int
    unresolved_missing_cells: int
    ok: bool
    json_report: str
    markdown_report: str
    cells: tuple[CorruptRecoveryCell, ...]
    classified_source_absent_cells: int = 0

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["cells"] = [asdict(cell) for cell in self.cells]
        return payload


@dataclass(frozen=True)
class _Target:
    day: date
    slot: str
    suffix: str
    line_number: int
    event: Mapping[str, Any]
    expected_sha256: str | None
    expected_size_bytes: int | None
    evidence_sufficient: bool

    @property
    def url(self) -> str:
        return static_archive_url(self.day, self.suffix)


@dataclass(frozen=True)
class _Snapshot:
    path: Path
    sha256: str
    size_bytes: int
    event_count: int
    stable: StableJsonlPrefix


@dataclass(frozen=True)
class _TargetSelection:
    targets: tuple[_Target, ...]
    invalid: tuple[CorruptRecoveryCell, ...]


def build_corrupt_recovery_command(
    *,
    python_executable: str | Path,
    start_date: date,
    end_date: date,
    raw_root: str | Path,
    download_manifest: str | Path,
    availability_manifest: str | Path,
    report_root: str | Path,
    timeout_seconds: float = 600.0,
    max_attempts: int = 3,
    corrupt_only: bool = False,
) -> tuple[str, ...]:
    """Build the secret-free postrun command without inspecting the filesystem."""

    CorruptRecoveryConfig(
        timeout_seconds=timeout_seconds, max_attempts=max_attempts
    ).validated()
    if start_date > end_date:
        raise ValueError("start_date must be <= end_date")
    command = (
        str(python_executable),
        "-u",
        "-m",
        "nifty_span.cli",
        "span-backfill",
        "recover-corrupt",
        "--start-date",
        start_date.isoformat(),
        "--end-date",
        end_date.isoformat(),
        "--raw-root",
        str(raw_root),
        "--download-manifest",
        str(download_manifest),
        "--availability-manifest",
        str(availability_manifest),
        "--report-root",
        str(report_root),
        "--corrupt-timeout-seconds",
        _number_text(timeout_seconds),
        "--corrupt-max-attempts",
        str(max_attempts),
        "--json",
    )
    return command + (("--corrupt-only",) if corrupt_only else ())


def recover_corrupt_span_cells(
    *,
    start_date: date,
    end_date: date,
    raw_root: str | Path,
    download_manifest: str | Path,
    availability_manifest: str | Path,
    report_root: str | Path,
    config: CorruptRecoveryConfig | None = None,
    client_factory: ClientFactory | None = None,
    process_provider: ProcessProvider | None = None,
    sleep: SleepFn = asyncio.sleep,
    random_fn: Callable[[], float] = random.random,
) -> CorruptRecoveryReport:
    """Synchronously run the fixed-concurrency static recovery classifier."""

    return asyncio.run(
        recover_corrupt_span_cells_async(
            start_date=start_date,
            end_date=end_date,
            raw_root=raw_root,
            download_manifest=download_manifest,
            availability_manifest=availability_manifest,
            report_root=report_root,
            config=config,
            client_factory=client_factory,
            process_provider=process_provider,
            sleep=sleep,
            random_fn=random_fn,
        )
    )


def validate_corrupt_recovery_report(
    report_path: str | Path,
    *,
    start_date: date,
    end_date: date,
    raw_root: str | Path,
    download_manifest: str | Path,
    availability_manifest: str | Path,
) -> dict[str, Any]:
    """Independently validate and fingerprint one recovery evidence bundle."""

    path = Path(report_path).resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise RuntimeError("corrupt recovery report root is not an object")
    if payload.get("schema_version") != RECOVERY_REPORT_SCHEMA:
        raise RuntimeError("corrupt recovery report schema is unsupported")
    expected_paths = {
        "raw_root": Path(raw_root).resolve(),
        "source_manifest": Path(download_manifest).resolve(),
        "availability_manifest": Path(availability_manifest).resolve(),
        "json_report": path,
    }
    for field_name, expected in expected_paths.items():
        if Path(str(payload.get(field_name, ""))).resolve() != expected:
            raise RuntimeError(
                f"corrupt recovery {field_name} is not bound to this run"
            )
    if (
        payload.get("start_date") != start_date.isoformat()
        or payload.get("end_date") != end_date.isoformat()
    ):
        raise RuntimeError("corrupt recovery range is not the pinned run range")

    snapshot = Path(str(payload.get("source_snapshot", ""))).resolve()
    try:
        snapshot.relative_to(path.parent / "manifest_snapshots")
    except ValueError as exc:
        raise RuntimeError(
            "corrupt recovery snapshot is outside its evidence root"
        ) from exc
    if not snapshot.is_file():
        raise RuntimeError("corrupt recovery source snapshot is absent")
    stable = read_stable_jsonl_prefix(snapshot)
    if stable.ignored_trailing_bytes:
        raise RuntimeError("corrupt recovery source snapshot has an incomplete tail")
    snapshot_sha = _file_sha256(snapshot)
    if (
        payload.get("source_snapshot_sha256") != snapshot_sha
        or _report_int(payload, "source_snapshot_size_bytes") != snapshot.stat().st_size
        or _report_int(payload, "source_snapshot_events") != stable.event_count
    ):
        raise RuntimeError("corrupt recovery source snapshot fingerprint disagrees")

    cells = payload.get("cells")
    if not isinstance(cells, list) or not all(
        isinstance(cell, Mapping) for cell in cells
    ):
        raise RuntimeError("corrupt recovery cells are not a list of objects")
    suffix_by_slot = dict(SLOT_SPECS)
    seen: set[tuple[str, str]] = set()
    availability_latest: dict[tuple[str, str], Mapping[str, Any]] | None = None
    recovered = classified = classified_absent = already = unresolved = 0
    unresolved_corrupt = unresolved_missing = network_calls = 0
    for raw_cell in cells:
        cell = dict(raw_cell)
        day_text = str(cell.get("trading_date", ""))
        slot = str(cell.get("slot", ""))
        try:
            day = date.fromisoformat(day_text)
        except ValueError as exc:
            raise RuntimeError("corrupt recovery cell date is invalid") from exc
        if day < start_date or day > end_date or slot not in suffix_by_slot:
            raise RuntimeError("corrupt recovery cell is outside the requested matrix")
        key = (day_text, slot)
        if key in seen:
            raise RuntimeError("corrupt recovery contains duplicate date/slot cells")
        seen.add(key)
        suffix = str(cell.get("suffix", ""))
        if suffix != suffix_by_slot[slot]:
            raise RuntimeError("corrupt recovery slot/suffix mapping is invalid")
        expected_url = static_archive_url(day, suffix)
        if cell.get("static_url") != expected_url:
            raise RuntimeError("corrupt recovery cell has an unexpected static URL")
        attempts = _report_int(cell, "network_attempts")
        if attempts < 0:
            raise RuntimeError("corrupt recovery cell has negative network attempts")
        network_calls += attempts
        status = cell.get("static_status")
        if status is not None and (
            type(status) is not int or status < 100 or status > 599
        ):
            raise RuntimeError("corrupt recovery cell has an invalid HTTP status")
        body_sha = cell.get("static_sha256")
        body_size = cell.get("static_size_bytes")
        if body_sha is not None and not _SHA256.fullmatch(str(body_sha)):
            raise RuntimeError("corrupt recovery cell has an invalid body SHA-256")
        if body_size is not None and (type(body_size) is not int or body_size < 0):
            raise RuntimeError("corrupt recovery cell has an invalid body size")
        disposition = str(cell.get("disposition", ""))
        source_state = str(cell.get("source_state", ""))
        if disposition in DOWNLOADED_STATES:
            recovered += 1
            canonical = Path(str(cell.get("canonical_path", ""))).resolve()
            try:
                canonical.relative_to(Path(raw_root).resolve())
            except ValueError as exc:
                raise RuntimeError(
                    "recovered canonical path is outside raw root"
                ) from exc
            if (
                not canonical.is_file()
                or body_sha != _file_sha256(canonical)
                or body_size != canonical.stat().st_size
            ):
                raise RuntimeError("recovered canonical archive fingerprint disagrees")
        elif disposition == "official_source_corrupt_archive":
            classified += 1
            if availability_latest is None:
                availability_latest = load_availability_events(
                    Path(availability_manifest)
                )
            basis = cell.get("evidence_basis")
            exact_payload = basis is None and (
                body_sha == cell.get("expected_sha256")
                and body_size == cell.get("expected_size_bytes")
            )
            repeated_static = basis == REPEATED_STATIC_CORRUPT_BASIS and attempts == 3
            boundary_event = availability_latest.get(key)
            if (
                source_state != "corrupt_inner_zip"
                or status != 200
                or not (exact_payload or repeated_static)
                or not cell.get("availability_event_id")
                or not cell.get("validation_state")
                or boundary_event is None
                or boundary_event.get("event_id") != cell.get("availability_event_id")
            ):
                raise RuntimeError("corrupt boundary cell evidence is inconsistent")
        elif disposition == "official_source_archive_absent":
            classified_absent += 1
            if availability_latest is None:
                availability_latest = load_availability_events(
                    Path(availability_manifest)
                )
            boundary_event = availability_latest.get(key)
            if (
                source_state not in MISSING_STATES
                or status != 404
                or attempts != 3
                or cell.get("evidence_basis") != REPEATED_STATIC_404_BASIS
                or not cell.get("availability_event_id")
                or cell.get("validation_state") != "http_404"
                or boundary_event is None
                or boundary_event.get("event_id") != cell.get("availability_event_id")
            ):
                raise RuntimeError(
                    "static-absence boundary cell evidence is inconsistent"
                )
        elif disposition == "already_classified":
            already += 1
            if not cell.get("availability_event_id"):
                raise RuntimeError(
                    "already-classified cell lacks availability evidence"
                )
        else:
            unresolved += 1
            if source_state == "corrupt_inner_zip":
                unresolved_corrupt += 1
            if source_state in MISSING_STATES | {BUNDLE_VALIDATION_BLOCKED_STATE}:
                unresolved_missing += 1

    recomputed = {
        "selected_cells": len(cells),
        "network_calls": network_calls,
        "recovered_cells": recovered,
        "classified_source_corrupt_cells": classified,
        "classified_source_absent_cells": classified_absent,
        "already_classified_cells": already,
        "unresolved_cells": unresolved,
        "unresolved_corrupt_cells": unresolved_corrupt,
        "unresolved_missing_cells": unresolved_missing,
    }
    for field_name, expected in recomputed.items():
        if field_name == "classified_source_absent_cells" and field_name not in payload:
            if expected == 0:
                continue
        if _report_int(payload, field_name) != expected:
            raise RuntimeError(f"corrupt recovery {field_name} disagrees with cells")
    if type(payload.get("ok")) is not bool or payload.get("ok") != (unresolved == 0):
        raise RuntimeError("corrupt recovery ok flag disagrees with cells")

    markdown = Path(str(payload.get("markdown_report", ""))).resolve()
    if (
        markdown.parent != path.parent
        or not markdown.is_file()
        or markdown.stat().st_size == 0
    ):
        raise RuntimeError("corrupt recovery Markdown evidence is absent or misplaced")
    markdown_text = markdown.read_text(encoding="utf-8")
    if any(str(cell["static_url"]) not in markdown_text for cell in cells):
        raise RuntimeError("corrupt recovery Markdown omits attempted static endpoints")
    decision = (
        "Alternative-source decision required:"
        if unresolved_missing
        else "No alternative-source decision is required"
    )
    if decision not in markdown_text:
        raise RuntimeError("corrupt recovery Markdown omits the source decision")
    return {
        "artifact": str(path),
        "artifact_sha256": _file_sha256(path),
        "artifact_size_bytes": path.stat().st_size,
        "markdown": str(markdown),
        "markdown_sha256": _file_sha256(markdown),
        "markdown_size_bytes": markdown.stat().st_size,
        "source_snapshot": str(snapshot),
        "source_snapshot_sha256": snapshot_sha,
        "source_snapshot_size_bytes": snapshot.stat().st_size,
        "run_id": str(payload.get("run_id", "")),
        **recomputed,
        "ok": unresolved == 0,
    }


async def recover_corrupt_span_cells_async(
    *,
    start_date: date,
    end_date: date,
    raw_root: str | Path,
    download_manifest: str | Path,
    availability_manifest: str | Path,
    report_root: str | Path,
    config: CorruptRecoveryConfig | None = None,
    client_factory: ClientFactory | None = None,
    process_provider: ProcessProvider | None = None,
    sleep: SleepFn = asyncio.sleep,
    random_fn: Callable[[], float] = random.random,
) -> CorruptRecoveryReport:
    if start_date > end_date:
        raise ValueError("start_date must be <= end_date")
    cfg = (config or CorruptRecoveryConfig()).validated()
    raw = Path(raw_root).resolve()
    manifest = Path(download_manifest).resolve()
    availability = Path(availability_manifest).resolve()
    reports = Path(report_root).resolve()
    processes = process_provider or list_windows_processes
    if not manifest.is_file():
        raise FileNotFoundError(manifest)
    reports.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex
    started = _utc_now()
    lock_path = manifest.parent / f".{manifest.name}.corrupt-recovery.lock"
    lock_fd = _acquire_lock(lock_path, run_id)
    try:
        _assert_no_writer(processes, manifest)
        snapshot = _freeze_manifest(manifest, reports / "manifest_snapshots")
        _assert_no_writer(processes, manifest)
        _assert_manifest_fingerprint(manifest, snapshot.stable)
        prior_availability = load_availability_events(availability)
        selection = _select_targets(
            snapshot.stable,
            start_date,
            end_date,
            prior_availability,
            include_missing_targets=cfg.include_missing_targets,
        )
        targets = selection.targets
        expected_manifest = snapshot.stable
        client: ClientLike | None = None
        entered = False
        cells: list[CorruptRecoveryCell] = list(selection.invalid)
        network_calls = 0
        try:
            for target in targets:
                _assert_no_writer(processes, manifest)
                _assert_manifest_fingerprint(manifest, expected_manifest)
                existing = _recover_existing_canonical(
                    target=target,
                    raw_root=raw,
                    manifest=manifest,
                    snapshot=snapshot,
                    config=cfg,
                )
                if existing is not None:
                    result, appended_download = existing
                    cells.append(result)
                    if appended_download:
                        expected_manifest = read_stable_jsonl_prefix(manifest)
                        if expected_manifest.ignored_trailing_bytes:
                            raise RuntimeError(
                                "download manifest has an unterminated appended tail"
                            )
                    continue
                prior = prior_availability.get((target.day.isoformat(), target.slot))
                if _same_boundary(prior, target):
                    cells.append(
                        _result(
                            target,
                            disposition="already_classified",
                            network_attempts=0,
                            availability_event_id=str(prior.get("event_id") or "")
                            or None,
                        )
                    )
                    continue
                if client is None:
                    client, entered = await _create_client(
                        client_factory or _curl_client_factory
                    )
                (
                    result,
                    appended_download,
                    appended_availability,
                    calls,
                ) = await _recover_target(
                    target=target,
                    raw_root=raw,
                    manifest=manifest,
                    availability_manifest=availability,
                    snapshot=snapshot,
                    config=cfg,
                    client=client,
                    sleep=sleep,
                    random_fn=random_fn,
                )
                network_calls += calls
                cells.append(result)
                if appended_download:
                    expected_manifest = read_stable_jsonl_prefix(manifest)
                    if expected_manifest.ignored_trailing_bytes:
                        raise RuntimeError(
                            "download manifest has an unterminated appended tail"
                        )
                if appended_availability:
                    prior_availability = load_availability_events(availability)
            _assert_no_writer(processes, manifest)
            _assert_manifest_fingerprint(manifest, expected_manifest)
        finally:
            if client is not None:
                await _close_client(client, entered)

        recovered = sum(cell.disposition in DOWNLOADED_STATES for cell in cells)
        classified = sum(
            cell.disposition == "official_source_corrupt_archive" for cell in cells
        )
        classified_absent = sum(
            cell.disposition == "official_source_archive_absent" for cell in cells
        )
        already = sum(cell.disposition == "already_classified" for cell in cells)
        unresolved = len(cells) - recovered - classified - classified_absent - already
        unresolved_corrupt = sum(
            cell.source_state == "corrupt_inner_zip"
            and cell.disposition
            not in DOWNLOADED_STATES
            | {"official_source_corrupt_archive", "already_classified"}
            for cell in cells
        )
        unresolved_missing = sum(
            cell.source_state in MISSING_STATES | {BUNDLE_VALIDATION_BLOCKED_STATE}
            and cell.disposition
            not in DOWNLOADED_STATES
            | {"official_source_archive_absent", "already_classified"}
            for cell in cells
        )
        json_path = reports / f"span_corrupt_recovery_{run_id}.json"
        markdown_path = reports / f"SPAN_CORRUPT_RECOVERY_{run_id}.md"
        report = CorruptRecoveryReport(
            schema_version=RECOVERY_REPORT_SCHEMA,
            run_id=run_id,
            started_at_utc=started,
            finished_at_utc=_utc_now(),
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            concurrency=1,
            timeout_seconds=cfg.timeout_seconds,
            max_attempts=cfg.max_attempts,
            source_manifest=str(manifest),
            source_snapshot=str(snapshot.path),
            source_snapshot_sha256=snapshot.sha256,
            source_snapshot_size_bytes=snapshot.size_bytes,
            source_snapshot_events=snapshot.event_count,
            raw_root=str(raw),
            availability_manifest=str(availability),
            selected_cells=len(targets) + len(selection.invalid),
            network_calls=network_calls,
            recovered_cells=recovered,
            classified_source_corrupt_cells=classified,
            already_classified_cells=already,
            unresolved_cells=unresolved,
            unresolved_corrupt_cells=unresolved_corrupt,
            unresolved_missing_cells=unresolved_missing,
            ok=unresolved == 0,
            json_report=str(json_path),
            markdown_report=str(markdown_path),
            cells=tuple(cells),
            classified_source_absent_cells=classified_absent,
        )
        _atomic_write(json_path, _json_bytes(report.to_dict()))
        _atomic_write(markdown_path, _markdown(report).encode("utf-8"))
        return report
    finally:
        _release_lock(lock_fd)


async def _recover_target(
    *,
    target: _Target,
    raw_root: Path,
    manifest: Path,
    availability_manifest: Path,
    snapshot: _Snapshot,
    config: CorruptRecoveryConfig,
    client: ClientLike,
    sleep: SleepFn,
    random_fn: Callable[[], float],
) -> tuple[CorruptRecoveryCell, bool, bool, int]:
    calls = 0
    last: dict[str, Any] = {}
    observations: list[dict[str, Any]] = []
    for attempt in range(1, config.max_attempts + 1):
        calls += 1
        try:
            response = await client.get(target.url, timeout=config.timeout_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - transports expose different exceptions.
            last = {
                "disposition": "unresolved_transport_error",
                "error": f"transport error ({type(exc).__name__})",
            }
            if attempt < config.max_attempts:
                await sleep(_backoff(config, attempt, random_fn()))
                continue
            return (
                _result(target, network_attempts=attempt, **last),
                False,
                False,
                calls,
            )

        status = int(getattr(response, "status_code", 0) or 0)
        content = bytes(getattr(response, "content", b"") or b"")
        headers = _safe_headers(getattr(response, "headers", {}) or {})
        digest = sha256(content).hexdigest()
        content_type = headers.get("content-type", "").split(";", 1)[0].strip().lower()
        metadata = {
            "static_status": status,
            "static_sha256": digest,
            "static_size_bytes": len(content),
        }
        if status in {403, 429} or 500 <= status <= 599:
            observations.append(
                _static_observation(
                    target=target,
                    attempt=attempt,
                    status=status,
                    headers=headers,
                    digest=digest,
                    size_bytes=len(content),
                    zip_magic_ok=content.startswith(_ZIP_MAGICS),
                    validation_state=f"http_{status}",
                )
            )
            last = {
                **metadata,
                "disposition": "unresolved_http_error",
                "error": f"official static endpoint returned HTTP {status}",
            }
            if attempt < config.max_attempts:
                await sleep(_backoff(config, attempt, random_fn()))
                continue
            return (
                _result(target, network_attempts=attempt, **last),
                False,
                False,
                calls,
            )
        if status != 200:
            observations.append(
                _static_observation(
                    target=target,
                    attempt=attempt,
                    status=status,
                    headers=headers,
                    digest=digest,
                    size_bytes=len(content),
                    zip_magic_ok=False,
                    validation_state=f"http_{status}",
                )
            )
            if (
                status == 404
                and str(target.event.get("state", "")) in MISSING_STATES
                and attempt == 3
                and _identical_observations(
                    observations,
                    status=404,
                    validation_state="http_404",
                    zip_magic_ok=False,
                )
            ):
                availability_event = _repeated_static_boundary_event(
                    target=target,
                    snapshot=snapshot,
                    basis=REPEATED_STATIC_404_BASIS,
                    observations=observations,
                )
                append_jsonl_record(availability_manifest, availability_event)
                return (
                    _result(
                        target,
                        disposition="official_source_archive_absent",
                        network_attempts=attempt,
                        static_status=status,
                        static_sha256=digest,
                        static_size_bytes=len(content),
                        availability_event_id=availability_event["event_id"],
                        validation_state="http_404",
                        evidence_basis=REPEATED_STATIC_404_BASIS,
                        error=(
                            "three identical exact-static HTTP 404 observations prove "
                            "a retrieval boundary, not historical nonpublication"
                        ),
                    ),
                    False,
                    True,
                    calls,
                )
            if (
                status == 404
                and str(target.event.get("state", "")) in MISSING_STATES
                and attempt < min(config.max_attempts, 3)
            ):
                await sleep(_backoff(config, attempt, random_fn()))
                continue
            return (
                _result(
                    target,
                    disposition="unresolved_http_error",
                    network_attempts=attempt,
                    error=f"official static endpoint returned HTTP {status}",
                    **metadata,
                ),
                False,
                False,
                calls,
            )
        content_length = _optional_int(headers.get("content-length"))
        preliminary_error = _preliminary_response_error(
            content,
            content_type=content_type,
            content_length=content_length,
            limits=config.archive_limits,
        )
        if preliminary_error is not None:
            observations.append(
                _static_observation(
                    target=target,
                    attempt=attempt,
                    status=status,
                    headers=headers,
                    digest=digest,
                    size_bytes=len(content),
                    zip_magic_ok=content.startswith(_ZIP_MAGICS),
                    validation_state=preliminary_error[0],
                )
            )
            last = {
                **metadata,
                "disposition": "unresolved_invalid_response",
                "validation_state": preliminary_error[0],
                "error": preliminary_error[1],
            }
            if attempt < config.max_attempts:
                await sleep(_backoff(config, attempt, random_fn()))
                continue
            return (
                _result(target, network_attempts=attempt, **last),
                False,
                False,
                calls,
            )

        filename = f"nsccl.{target.day:%Y%m%d}.{target.suffix}.zip"
        synthetic = zipfile.ZipInfo(filename=filename)
        synthetic.CRC = zlib.crc32(content) & 0xFFFFFFFF
        synthetic.compress_size = len(content)
        synthetic.file_size = len(content)
        try:
            archive = _validate_inner_archive(
                target.day,
                target.suffix,
                synthetic,
                content,
                config.archive_limits,
            )
        except _ArchiveError as exc:
            observations.append(
                _static_observation(
                    target=target,
                    attempt=attempt,
                    status=status,
                    headers=headers,
                    digest=digest,
                    size_bytes=len(content),
                    zip_magic_ok=True,
                    validation_state=exc.state,
                )
            )
            exact_match = (
                target.evidence_sufficient
                and digest == target.expected_sha256
                and len(content) == target.expected_size_bytes
            )
            if exact_match and exc.state == "corrupt_inner_zip":
                availability_event = _corrupt_availability_event(
                    target=target,
                    snapshot=snapshot,
                    attempt=attempt,
                    status=status,
                    headers=headers,
                    digest=digest,
                    size_bytes=len(content),
                    validation=exc,
                )
                append_jsonl_record(availability_manifest, availability_event)
                return (
                    _result(
                        target,
                        disposition="official_source_corrupt_archive",
                        network_attempts=attempt,
                        static_status=status,
                        static_sha256=digest,
                        static_size_bytes=len(content),
                        availability_event_id=availability_event["event_id"],
                        validation_state=exc.state,
                        error=str(exc),
                    ),
                    False,
                    True,
                    calls,
                )
            if (
                not target.evidence_sufficient
                and str(target.event.get("state", "")) == "corrupt_inner_zip"
                and attempt == 3
                and _identical_observations(
                    observations,
                    status=200,
                    validation_state="corrupt_inner_zip",
                    zip_magic_ok=True,
                )
            ):
                availability_event = _repeated_static_boundary_event(
                    target=target,
                    snapshot=snapshot,
                    basis=REPEATED_STATIC_CORRUPT_BASIS,
                    observations=observations,
                )
                append_jsonl_record(availability_manifest, availability_event)
                return (
                    _result(
                        target,
                        disposition="official_source_corrupt_archive",
                        network_attempts=attempt,
                        static_status=status,
                        static_sha256=digest,
                        static_size_bytes=len(content),
                        availability_event_id=availability_event["event_id"],
                        validation_state=exc.state,
                        evidence_basis=REPEATED_STATIC_CORRUPT_BASIS,
                        error=(
                            "three identical exact-static corrupt archives prove a "
                            "retrieval boundary without reports/static payload equality"
                        ),
                    ),
                    False,
                    True,
                    calls,
                )
            exact_non_corrupt_state = exact_match and exc.state != "corrupt_inner_zip"
            last = {
                **metadata,
                "disposition": (
                    "unresolved_non_corrupt_validation_state"
                    if exact_non_corrupt_state
                    else (
                        "unresolved_evidence_mismatch"
                        if target.evidence_sufficient
                        else "unresolved_insufficient_reports_api_evidence"
                    )
                ),
                "validation_state": exc.state,
                "error": (
                    "exact payload equality cannot prove source corruption for this validation state"
                    if exact_non_corrupt_state
                    else (
                        "static invalid payload does not exactly match reports-API rejected-inner evidence"
                        if target.evidence_sufficient
                        else "reports-API rejected-inner evidence is incomplete"
                    )
                ),
            }
            if attempt < config.max_attempts:
                await sleep(_backoff(config, attempt, random_fn()))
                continue
            return (
                _result(target, network_attempts=attempt, **last),
                False,
                False,
                calls,
            )

        try:
            saved_state, canonical = _persist_inner_archive(
                raw_root, target.day, archive, config.archive_limits
            )
        except _ArchiveError as exc:
            return (
                _result(
                    target,
                    disposition="unresolved_canonical_publish_error",
                    network_attempts=attempt,
                    static_status=status,
                    static_sha256=digest,
                    static_size_bytes=len(content),
                    validation_state=exc.state,
                    error=str(exc),
                ),
                False,
                False,
                calls,
            )
        event = _downloaded_event(
            target=target,
            archive=archive,
            raw_root=raw_root,
            canonical=canonical,
            saved_state=saved_state,
            attempt=attempt,
            status=status,
            headers=headers,
            snapshot=snapshot,
        )
        append_jsonl_record(manifest, event)
        return (
            _result(
                target,
                disposition=saved_state,
                network_attempts=attempt,
                static_status=status,
                static_sha256=digest,
                static_size_bytes=len(content),
                canonical_path=str(canonical),
            ),
            True,
            False,
            calls,
        )
    raise AssertionError("bounded recovery loop returned unexpectedly")


def _select_targets(
    snapshot: StableJsonlPrefix,
    start_date: date,
    end_date: date,
    availability_latest: Mapping[tuple[str, str], Mapping[str, Any]],
    *,
    include_missing_targets: bool = True,
) -> _TargetSelection:
    latest: dict[tuple[str, str], tuple[int, Mapping[str, Any]]] = {}
    for line_number, event in snapshot.events:
        key = (str(event.get("trading_date", "")), str(event.get("slot", "")))
        latest[key] = (line_number, event)
    corrupt_bundle_signatures: set[tuple[str, str, int, str]] = set()
    for (day_text, _slot), (_line_number, event) in latest.items():
        if (
            str(event.get("state", "")) == "corrupt_inner_zip"
            and event.get("terminal") is False
        ):
            signature = _bundle_signature(day_text, event)
            if signature is not None:
                corrupt_bundle_signatures.add(signature)
    targets: list[_Target] = []
    invalid: list[CorruptRecoveryCell] = []
    suffix_by_slot = dict(SLOT_SPECS)
    for (day_text, slot), (line_number, event) in sorted(latest.items()):
        state = str(event.get("state", ""))
        availability = availability_latest.get((day_text, slot))
        classified = (
            str(availability.get("classification_outcome", ""))
            if availability is not None
            else ""
        )
        corrupt_target = (
            state == "corrupt_inner_zip"
            and event.get("terminal") is False
            and classified not in {"accepted_absence", "source_boundary"}
        )
        blocked_companion = state == BUNDLE_VALIDATION_BLOCKED_STATE
        missing_target = (
            include_missing_targets
            and state in MISSING_STATES
            and classified
            not in {
                "accepted_absence",
                "source_boundary",
            }
        )
        if not corrupt_target and not blocked_companion and not missing_target:
            continue
        suffix = str(event.get("suffix", ""))
        try:
            day = date.fromisoformat(day_text)
        except ValueError:
            invalid.append(
                _invalid_manifest_cell(
                    event=event,
                    line_number=line_number,
                    day_text=day_text,
                    slot=slot,
                    suffix=suffix,
                    error="latest recoverable event has an invalid trading_date",
                )
            )
            continue
        if not (start_date <= day <= end_date):
            continue
        if blocked_companion:
            signature = _bundle_signature(day_text, event)
            if signature is None or signature not in corrupt_bundle_signatures:
                invalid.append(
                    _invalid_manifest_cell(
                        event=event,
                        line_number=line_number,
                        day_text=day_text,
                        slot=slot,
                        suffix=suffix,
                        error=(
                            "bundle_validation_blocked cell lacks an exact latest "
                            "corrupt-bundle companion with matching run, attempt, "
                            "and response SHA-256"
                        ),
                    )
                )
                continue
        if slot not in suffix_by_slot or suffix_by_slot[slot] != suffix:
            invalid.append(
                _invalid_manifest_cell(
                    event=event,
                    line_number=line_number,
                    day_text=day_text,
                    slot=slot,
                    suffix=suffix,
                    error="latest recoverable event has an invalid slot/suffix pair",
                )
            )
            continue
        rejected = event.get("rejected_inner")
        expected_hash = (
            str(rejected.get("sha256", "")).lower()
            if isinstance(rejected, Mapping)
            else ""
        )
        expected_size = (
            _optional_int(rejected.get("size_bytes"))
            if isinstance(rejected, Mapping)
            else None
        )
        expected_name = f"nsccl.{day:%Y%m%d}.{suffix}.zip"
        outer = event.get("outer_member")
        evidence_sufficient = all(
            (
                isinstance(event.get("event_id"), str) and bool(event.get("event_id")),
                event.get("http_status") == 200,
                bool(_SHA256.fullmatch(expected_hash)),
                expected_size is not None and expected_size > 0,
                isinstance(outer, Mapping)
                and str(outer.get("name", "")).lower() == expected_name,
                suffix in event.get("returned_suffixes", ()),
            )
        )
        targets.append(
            _Target(
                day=day,
                slot=slot,
                suffix=suffix,
                line_number=line_number,
                event=event,
                expected_sha256=expected_hash or None,
                expected_size_bytes=expected_size,
                evidence_sufficient=evidence_sufficient,
            )
        )
    return _TargetSelection(tuple(targets), tuple(invalid))


def _bundle_signature(
    day_text: str, event: Mapping[str, Any]
) -> tuple[str, str, int, str] | None:
    """Bind blocked companions to the exact corrupt reports-API bundle.

    Recovery still requests each official static slot URL, but a blocked slot is
    eligible only as part of the same date/run/attempt/response bundle as a
    latest corrupt slot.  This prevents unrelated blocked cells from becoming
    independent recovery downloads.
    """

    run_id = event.get("run_id")
    attempt = event.get("attempt")
    response = event.get("response")
    body_sha = response.get("body_sha256") if isinstance(response, Mapping) else None
    if (
        not isinstance(run_id, str)
        or not run_id
        or type(attempt) is not int
        or attempt < 1
        or not isinstance(body_sha, str)
        or not _SHA256.fullmatch(body_sha.lower())
    ):
        return None
    return day_text, run_id, attempt, body_sha.lower()


def _invalid_manifest_cell(
    *,
    event: Mapping[str, Any],
    line_number: int,
    day_text: str,
    slot: str,
    suffix: str,
    error: str,
) -> CorruptRecoveryCell:
    rejected = event.get("rejected_inner")
    expected_hash = (
        str(rejected.get("sha256", "")).lower()
        if isinstance(rejected, Mapping)
        else None
    )
    expected_size = (
        _optional_int(rejected.get("size_bytes"))
        if isinstance(rejected, Mapping)
        else None
    )
    return CorruptRecoveryCell(
        trading_date=day_text or "<missing>",
        slot=slot or "<missing>",
        suffix=suffix or "<missing>",
        source_state=str(event.get("state", "")),
        source_event_id=str(event.get("event_id") or "") or None,
        source_line_number=line_number,
        static_url="",
        disposition="unresolved_manifest_schema_error",
        network_attempts=0,
        expected_sha256=expected_hash,
        expected_size_bytes=expected_size,
        validation_state="manifest_schema_error",
        error=error,
    )


def _recover_existing_canonical(
    *,
    target: _Target,
    raw_root: Path,
    manifest: Path,
    snapshot: _Snapshot,
    config: CorruptRecoveryConfig,
) -> tuple[CorruptRecoveryCell, bool] | None:
    """Resume a crash after atomic publication but before journal append.

    The canonical filename, full ZIP/CRC/member/date/suffix contract, and the
    immutable no-overwrite path are revalidated.  No network request is needed.
    An existing invalid path remains unresolved and is never replaced.
    """

    canonical = (
        raw_root
        / f"{target.day:%Y}"
        / f"{target.day:%m}"
        / f"{target.day:%d}"
        / f"nsccl.{target.day:%Y%m%d}.{target.suffix}.zip"
    )
    if not canonical.exists():
        return None
    try:
        if not canonical.is_file():
            raise _ArchiveError(
                "local_file_invalid",
                f"canonical path is not a regular file: {canonical}",
                suffix=target.suffix,
            )
        content = canonical.read_bytes()
        filename = canonical.name
        synthetic = zipfile.ZipInfo(filename=filename)
        synthetic.CRC = zlib.crc32(content) & 0xFFFFFFFF
        synthetic.compress_size = len(content)
        synthetic.file_size = len(content)
        archive = _validate_inner_archive(
            target.day,
            target.suffix,
            synthetic,
            content,
            config.archive_limits,
        )
        saved_state, persisted = _persist_inner_archive(
            raw_root, target.day, archive, config.archive_limits
        )
    except (OSError, _ArchiveError) as exc:
        state = exc.state if isinstance(exc, _ArchiveError) else "local_file_io_error"
        return (
            _result(
                target,
                disposition="unresolved_existing_canonical_invalid",
                network_attempts=0,
                canonical_path=str(canonical),
                validation_state=state,
                error=f"existing canonical archive failed revalidation ({type(exc).__name__})",
            ),
            False,
        )
    if saved_state != "downloaded_existing":
        raise RuntimeError(
            f"existing canonical resume unexpectedly returned state {saved_state!r}"
        )
    event = _downloaded_event(
        target=target,
        archive=archive,
        raw_root=raw_root,
        canonical=persisted,
        saved_state=saved_state,
        attempt=0,
        status=None,
        headers={},
        snapshot=snapshot,
        recovery_mode="existing_canonical_revalidated",
    )
    append_jsonl_record(manifest, event)
    return (
        _result(
            target,
            disposition=saved_state,
            network_attempts=0,
            static_sha256=archive.sha256,
            static_size_bytes=archive.size_bytes,
            canonical_path=str(persisted),
        ),
        True,
    )


def _downloaded_event(
    *,
    target: _Target,
    archive: _InnerArchive,
    raw_root: Path,
    canonical: Path,
    saved_state: str,
    attempt: int,
    status: int | None,
    headers: Mapping[str, str],
    snapshot: _Snapshot,
    recovery_mode: str = "official_static_request",
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "schema_version": 1,
        "event_id": uuid.uuid4().hex,
        "run_id": f"corrupt-static-recovery-{uuid.uuid4().hex}",
        "observed_at_utc": _utc_now(),
        "trading_date": target.day.isoformat(),
        "slot": target.slot,
        "suffix": target.suffix,
        "state": saved_state,
        "terminal": True,
        "attempt": attempt,
        "path": str(canonical.relative_to(raw_root)),
        "sha256": archive.sha256,
        "size_bytes": archive.size_bytes,
        "recovery_mode": recovery_mode,
        "inner_spn": {
            "name": archive.spn_name,
            "crc32": archive.spn_crc32,
            "compressed_size": archive.spn_compressed_size,
            "uncompressed_size": archive.spn_uncompressed_size,
        },
        "zip_crc_ok": True,
        "members": [archive.spn_name],
        "returned_suffixes": [target.suffix],
        "recovery_provenance": _reports_api_reference(target, snapshot),
    }
    if status is None:
        event["existing_canonical"] = {
            "name": archive.filename,
            "crc32": archive.outer_crc32,
            "revalidated_without_network": True,
        }
    else:
        event["http_status"] = status
        event["static_archive"] = {
            "url": target.url,
            "name": archive.filename,
            "crc32": archive.outer_crc32,
        }
        event["response"] = {
            "body_sha256": archive.sha256,
            "body_size_bytes": archive.size_bytes,
            "content_type": headers.get("content-type"),
            "safe_headers": dict(headers),
        }
    return event


def _corrupt_availability_event(
    *,
    target: _Target,
    snapshot: _Snapshot,
    attempt: int,
    status: int,
    headers: Mapping[str, str],
    digest: str,
    size_bytes: int,
    validation: _ArchiveError,
) -> dict[str, Any]:
    return {
        "schema_version": CORRUPT_ARCHIVE_EVENT_SCHEMA,
        "event": "official_source_corrupt_archive",
        "event_id": uuid.uuid4().hex,
        "run_id": f"corrupt-static-recovery-{uuid.uuid4().hex}",
        "observed_at_utc": _utc_now(),
        "trading_date": target.day.isoformat(),
        "slot": target.slot,
        "suffix": target.suffix,
        "download_state": "corrupt_inner_zip",
        "market_state": "trading_source_boundary",
        "calendar_classification": "official_source_corrupt_archive",
        "classification_outcome": "source_boundary",
        "source_availability_boundary_proven": True,
        "raw_persisted": False,
        "canonical_archive_path": None,
        "exact_payload_match": True,
        "reports_api_evidence": _reports_api_reference(target, snapshot),
        "static_archive_evidence": {
            "url": target.url,
            "attempt": attempt,
            "http_status": status,
            "content_type": headers.get("content-type"),
            "safe_headers": dict(headers),
            "body_sha256": digest,
            "body_size_bytes": size_bytes,
            "zip_magic_ok": True,
            "validation_state": validation.state,
            "validation_error": str(validation),
        },
    }


def _static_observation(
    *,
    target: _Target,
    attempt: int,
    status: int,
    headers: Mapping[str, str],
    digest: str,
    size_bytes: int,
    zip_magic_ok: bool,
    validation_state: str,
) -> dict[str, Any]:
    return {
        "attempt": attempt,
        "observed_at_utc": _utc_now(),
        "url": target.url,
        "http_status": status,
        "content_type": headers.get("content-type"),
        "safe_headers": dict(headers),
        "body_sha256": digest,
        "body_size_bytes": size_bytes,
        "body_retained": False,
        "zip_magic_ok": zip_magic_ok,
        "validation_state": validation_state,
    }


def _identical_observations(
    observations: list[Mapping[str, Any]],
    *,
    status: int,
    validation_state: str,
    zip_magic_ok: bool,
) -> bool:
    if len(observations) != 3:
        return False
    fingerprints = {
        (
            observation.get("http_status"),
            observation.get("body_sha256"),
            observation.get("body_size_bytes"),
            observation.get("validation_state"),
            observation.get("zip_magic_ok"),
        )
        for observation in observations
    }
    return (
        len(fingerprints) == 1
        and next(iter(fingerprints))[:1] == (status,)
        and all(
            observation.get("attempt") == attempt
            and observation.get("validation_state") == validation_state
            and observation.get("zip_magic_ok") is zip_magic_ok
            for attempt, observation in enumerate(observations, start=1)
        )
    )


def _repeated_static_boundary_event(
    *,
    target: _Target,
    snapshot: _Snapshot,
    basis: str,
    observations: list[Mapping[str, Any]],
) -> dict[str, Any]:
    if basis not in {REPEATED_STATIC_CORRUPT_BASIS, REPEATED_STATIC_404_BASIS}:
        raise ValueError(f"unsupported repeated-static evidence basis {basis!r}")
    if len(observations) != 3:
        raise ValueError("repeated-static boundary requires exactly three observations")
    return {
        "schema_version": REPEATED_STATIC_BOUNDARY_EVENT_SCHEMA,
        "event": REPEATED_STATIC_BOUNDARY_EVENT,
        "event_id": uuid.uuid4().hex,
        "run_id": f"corrupt-static-recovery-{uuid.uuid4().hex}",
        "observed_at_utc": _utc_now(),
        "trading_date": target.day.isoformat(),
        "slot": target.slot,
        "suffix": target.suffix,
        "download_state": str(target.event.get("state", "")),
        "market_state": "trading_source_boundary",
        "calendar_classification": "official_source_repeated_static_boundary",
        "classification_outcome": "source_boundary",
        "source_availability_boundary_proven": True,
        "evidence_basis": basis,
        "exact_payload_match": False,
        "historical_nonpublication_proven": False,
        "raw_persisted": False,
        "canonical_archive_path": None,
        "reports_api_evidence": _reports_api_reference(target, snapshot),
        "static_archive_observations": [dict(item) for item in observations],
    }


def _reports_api_reference(target: _Target, snapshot: _Snapshot) -> dict[str, Any]:
    return {
        "manifest_snapshot_path": str(snapshot.path),
        "manifest_snapshot_sha256": snapshot.sha256,
        "manifest_snapshot_size_bytes": snapshot.size_bytes,
        "manifest_event_line": target.line_number,
        "manifest_event_id": target.event.get("event_id"),
        "manifest_run_id": target.event.get("run_id"),
        "rejected_inner": {
            "sha256": target.expected_sha256,
            "size_bytes": target.expected_size_bytes,
        },
    }


def _report_int(payload: Mapping[str, Any], field_name: str) -> int:
    value = payload.get(field_name)
    if type(value) is not int:
        raise RuntimeError(f"corrupt recovery {field_name} must be an integer")
    return value


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _same_boundary(event: Mapping[str, Any] | None, target: _Target) -> bool:
    if not event:
        return False
    reports = event.get("reports_api_evidence")
    if not isinstance(reports, Mapping):
        return False
    if event.get("event") == REPEATED_STATIC_BOUNDARY_EVENT:
        observations = event.get("static_archive_observations")
        basis = event.get("evidence_basis")
        expected_basis = (
            REPEATED_STATIC_CORRUPT_BASIS
            if str(target.event.get("state", "")) == "corrupt_inner_zip"
            else REPEATED_STATIC_404_BASIS
        )
        return bool(
            event.get("source_availability_boundary_proven")
            and event.get("download_state") == target.event.get("state")
            and reports.get("manifest_event_id") == target.event.get("event_id")
            and reports.get("manifest_run_id") == target.event.get("run_id")
            and basis == expected_basis
            and isinstance(observations, list)
            and len(observations) == 3
        )
    if event.get("event") != "official_source_corrupt_archive":
        return False
    rejected = reports.get("rejected_inner") if isinstance(reports, Mapping) else None
    return bool(
        event.get("source_availability_boundary_proven")
        and isinstance(rejected, Mapping)
        and rejected.get("sha256") == target.expected_sha256
        and rejected.get("size_bytes") == target.expected_size_bytes
        and reports.get("manifest_event_id") == target.event.get("event_id")
    )


def _freeze_manifest(manifest: Path, snapshot_root: Path) -> _Snapshot:
    first = read_stable_jsonl_prefix(manifest)
    if first.ignored_trailing_bytes:
        raise RuntimeError("download manifest has an unterminated tail")
    content = manifest.read_bytes()
    second = read_stable_jsonl_prefix(manifest)
    if not _same_fingerprint(first, second) or len(content) != first.prefix_bytes:
        raise RuntimeError(
            "download manifest changed while freezing reports-API evidence"
        )
    digest = sha256(content).hexdigest()
    if digest != first.prefix_sha256:
        raise RuntimeError("download manifest bytes differ from stable-prefix hash")
    snapshot_root.mkdir(parents=True, exist_ok=True)
    path = snapshot_root / f"{digest}.jsonl"
    if path.exists():
        if path.read_bytes() != content:
            raise RuntimeError(f"existing immutable snapshot conflicts: {path}")
    else:
        _atomic_write(path, content, no_replace=True)
    frozen = read_stable_jsonl_prefix(path)
    if frozen.ignored_trailing_bytes or not _same_fingerprint(first, frozen):
        raise RuntimeError(
            "published reports-API manifest snapshot failed verification"
        )
    return _Snapshot(path, digest, len(content), frozen.event_count, frozen)


def _assert_manifest_fingerprint(path: Path, expected: StableJsonlPrefix) -> None:
    current = read_stable_jsonl_prefix(path)
    if current.ignored_trailing_bytes or not _same_fingerprint(current, expected):
        raise RuntimeError(
            "download manifest changed outside corrupt-recovery ownership"
        )


def _same_fingerprint(left: StableJsonlPrefix, right: StableJsonlPrefix) -> bool:
    return (
        left.prefix_sha256 == right.prefix_sha256
        and left.prefix_bytes == right.prefix_bytes
        and left.event_count == right.event_count
    )


def _assert_no_writer(process_provider: ProcessProvider, manifest: Path) -> None:
    writers = find_manifest_writers(process_provider(), manifest, Path.cwd())
    if writers:
        raise RuntimeError(
            "refusing corrupt recovery while a downloader targets this manifest: "
            + ",".join(str(item.pid) for item in writers)
        )


def find_manifest_writers(
    processes: Iterable[ProcessRecord], manifest: str | Path, repo_root: str | Path
) -> tuple[ProcessRecord, ...]:
    """Return only downloader processes targeting this exact resolved journal."""

    target = os.path.normcase(str(Path(manifest).resolve()))
    result: list[ProcessRecord] = []
    argument = re.compile(
        r"--download-manifest(?:=|\s+)(?:\"([^\"]+)\"|'([^']+)'|(\S+))",
        re.IGNORECASE,
    )
    for process in processes:
        command = process.command_line or ""
        normalized = command.lower().replace("/", "\\")
        if "span-backfill" not in normalized or not re.search(
            r"span-backfill(?:\.exe)?\s+download(?:\s|$)", normalized
        ):
            continue
        match = argument.search(command)
        if match is None:
            continue
        raw = next(value for value in match.groups() if value is not None).strip("\"'")
        candidate = Path(raw)
        if not candidate.is_absolute():
            candidate = Path(repo_root) / candidate
        if os.path.normcase(str(candidate.resolve())) == target:
            result.append(process)
    return tuple(sorted(result, key=lambda item: item.pid))


def list_windows_processes() -> tuple[ProcessRecord, ...]:
    """Return a process snapshot without importing the postrun orchestrator."""

    if os.name != "nt":
        return ()
    script = (
        "Get-CimInstance Win32_Process | "
        "Select-Object ProcessId,ParentProcessId,Name,CommandLine,CreationDate | "
        "ConvertTo-Json -Compress"
    )
    completed = subprocess.run(
        ["powershell.exe", "-NoLogo", "-NoProfile", "-Command", script],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    raw = completed.stdout.strip()
    if not raw:
        return ()
    values = json.loads(raw)
    if isinstance(values, Mapping):
        values = [values]
    return tuple(
        ProcessRecord(
            pid=int(item["ProcessId"]),
            parent_pid=(
                int(item["ParentProcessId"]) if item.get("ParentProcessId") else None
            ),
            name=str(item.get("Name") or ""),
            command_line=str(item.get("CommandLine") or ""),
            creation_date=(
                str(item["CreationDate"]) if item.get("CreationDate") else None
            ),
        )
        for item in values
    )


def _acquire_lock(path: str | Path, run_id: str) -> int:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        if os.name == "nt":
            import msvcrt

            if os.path.getsize(path) == 0:
                os.write(descriptor, b"\0")
                os.fsync(descriptor)
            os.lseek(descriptor, 0, os.SEEK_SET)
            msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        os.close(descriptor)
        raise RuntimeError(f"corrupt recovery lock is already held: {path}") from exc
    payload = _json_bytes(
        {"pid": os.getpid(), "run_id": run_id, "created_at_utc": _utc_now()}
    )
    os.ftruncate(descriptor, 0)
    os.lseek(descriptor, 0, os.SEEK_SET)
    os.write(descriptor, payload)
    os.fsync(descriptor)
    return descriptor


def _release_lock(descriptor: int) -> None:
    try:
        os.lseek(descriptor, 0, os.SEEK_SET)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


async def _create_client(factory: ClientFactory) -> tuple[ClientLike, bool]:
    candidate = factory(0)
    if inspect.isawaitable(candidate):
        candidate = await candidate
    enter = getattr(candidate, "__aenter__", None)
    if callable(enter):
        return await enter(), True
    return candidate, False


async def _close_client(client: ClientLike, entered: bool) -> None:
    if entered:
        exit_fn = getattr(client, "__aexit__", None)
        if callable(exit_fn):
            await exit_fn(None, None, None)
            return
    close = getattr(client, "aclose", None) or getattr(client, "close", None)
    if callable(close):
        result = close()
        if inspect.isawaitable(result):
            await result


def _preliminary_response_error(
    content: bytes,
    *,
    content_type: str,
    content_length: int | None,
    limits: ArchiveResourceLimits,
) -> tuple[str, str] | None:
    if len(content) > limits.max_response_bytes:
        return (
            "response_resource_limit_exceeded",
            "static response exceeds configured byte limit",
        )
    if content_type not in ZIP_CONTENT_TYPES:
        return (
            "invalid_content_type",
            "static response content type is not an accepted ZIP type",
        )
    if content_length is not None and content_length != len(content):
        return (
            "content_length_mismatch",
            "static response body length differs from Content-Length",
        )
    if not content.startswith(_ZIP_MAGICS):
        return "invalid_zip_magic", "static response does not start with ZIP magic"
    return None


def _safe_headers(raw: Mapping[str, Any]) -> dict[str, str]:
    return {
        str(key).lower(): str(value)
        for key, value in raw.items()
        if str(key).lower() in _SAFE_RESPONSE_HEADERS
    }


def _backoff(config: CorruptRecoveryConfig, attempt: int, sample: float) -> float:
    base = min(
        config.backoff_max_seconds, config.backoff_base_seconds * (2 ** (attempt - 1))
    )
    bounded = min(1.0, max(0.0, float(sample)))
    return base + config.jitter_seconds * bounded


def static_archive_url(day: date, suffix: str) -> str:
    if suffix not in SLOT_BY_SUFFIX:
        raise ValueError(f"unsupported SPAN suffix {suffix!r}")
    return f"{STATIC_ARCHIVE_ROOT}/nsccl.{day:%Y%m%d}.{suffix}.zip"


def _result(
    target: _Target,
    *,
    disposition: str,
    network_attempts: int,
    **fields: Any,
) -> CorruptRecoveryCell:
    return CorruptRecoveryCell(
        trading_date=target.day.isoformat(),
        slot=target.slot,
        suffix=target.suffix,
        source_state=str(target.event.get("state", "")),
        source_event_id=str(target.event.get("event_id") or "") or None,
        source_line_number=target.line_number,
        static_url=target.url,
        disposition=disposition,
        network_attempts=network_attempts,
        expected_sha256=target.expected_sha256,
        expected_size_bytes=target.expected_size_bytes,
        **fields,
    )


def _markdown(report: CorruptRecoveryReport) -> str:
    status = "PASS" if report.ok else "UNRESOLVED"
    lines = [
        "# NSE SPAN exact-static recovery and source evidence",
        "",
        f"- Outcome: `{status}`",
        f"- Run ID: `{report.run_id}`",
        f"- Range: `{report.start_date}` through `{report.end_date}`",
        f"- Concurrency: `{report.concurrency}`",
        f"- Timeout: `{report.timeout_seconds}` seconds",
        f"- Maximum attempts: `{report.max_attempts}`",
        f"- Immutable reports-API snapshot SHA-256: `{report.source_snapshot_sha256}`",
        f"- Selected cells: `{report.selected_cells}`",
        f"- Recovered: `{report.recovered_cells}`",
        f"- Official-source corrupt boundaries: `{report.classified_source_corrupt_cells}`",
        f"- Official-source static-absence boundaries: `{report.classified_source_absent_cells}`",
        f"- Already classified: `{report.already_classified_cells}`",
        f"- Unresolved: `{report.unresolved_cells}`",
        f"- Unresolved corrupt cells: `{report.unresolved_corrupt_cells}`",
        f"- Unresolved missing-slot cells: `{report.unresolved_missing_cells}`",
        "",
        "| Date | Slot | Source state | Static URL | Disposition | HTTP | SHA-256 | Bytes |",
        "|---|---|---|---|---|---:|---|---:|",
    ]
    for cell in report.cells:
        lines.append(
            "| "
            + " | ".join(
                (
                    cell.trading_date,
                    cell.slot,
                    cell.source_state,
                    cell.static_url,
                    cell.disposition,
                    str(cell.static_status or ""),
                    cell.static_sha256 or "",
                    str(cell.static_size_bytes or ""),
                )
            )
            + " |"
        )
    lines.extend(
        (
            "",
            "Invalid static response bytes are represented only by full SHA-256 and byte-size evidence; they are not retained in canonical raw storage.",
            "Repeated-static boundaries do not claim reports/static payload equality or historical nonpublication; they preserve an explicit BLOCKED_SOURCE disposition.",
            (
                "Alternative-source decision required: authorize a specific non-NSE historical archive source for the unresolved date/slot cells, or retain them as explicit unresolved official-source gaps."
                if report.unresolved_missing_cells
                else "No alternative-source decision is required by this recovery run."
            ),
            "",
        )
    )
    return "\n".join(lines)


def _atomic_write(path: Path, content: bytes, *, no_replace: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    try:
        with partial.open("xb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        if no_replace:
            try:
                os.link(partial, path)
            except FileExistsError:
                if path.read_bytes() != content:
                    raise RuntimeError(
                        f"concurrent immutable artifact conflicts: {path}"
                    )
        else:
            os.replace(partial, path)
    finally:
        partial.unlink(missing_ok=True)


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode("utf-8")


def _optional_int(value: Any) -> int | None:
    try:
        return None if value is None else int(value)
    except (TypeError, ValueError):
        return None


def _number_text(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else str(value)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")
