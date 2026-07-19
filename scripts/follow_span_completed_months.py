"""Follow a live SPAN download manifest and finalize only completed closed months.

The downloader owns the append-only JSONL input.  This follower never writes to
that file: it reads a stable prefix, validates the current date/slot cells, and
publishes an immutable content-addressed snapshot before invoking extraction.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo
import argparse
import calendar
import json
import logging
import os
import sys
import threading
import time
import uuid

from nifty_span.span.backfill import extract_and_compact_span_range
from nifty_span.span.backfill_audit import audit_span_backfill
from nifty_span.span.availability import (
    import_and_classify_availability,
    load_availability_events,
)
from nifty_span.span.backfill_downloader import MISSING_STATES, SLOT_SPECS


LOGGER = logging.getLogger("span-completed-month-follower")
SCRIPT_STATE_VERSION = 3
IST = ZoneInfo("Asia/Kolkata")
SLOT_SUFFIX = dict(SLOT_SPECS)
FINGERPRINT_FIELDS = (
    "trading_date",
    "slot",
    "suffix",
    "state",
    "terminal",
    "path",
    "sha256",
    "size_bytes",
    "http_status",
    "source_availability_boundary_proven",
)
_STATE_CLOCK_LOCK = threading.Lock()
_STATE_CLOCK_LAST = 0


class ManifestNotStableError(RuntimeError):
    """The captured append-only prefix ended in a partial JSONL record."""


class ManifestValidationError(RuntimeError):
    """A complete manifest record violates the follower input contract."""


class FollowerFatalError(RuntimeError):
    """Extraction or strict audit failed and polling must stop."""


@dataclass(frozen=True)
class FollowerConfig:
    download_manifest: Path
    raw_root: Path
    fragment_root: Path
    extraction_manifest: Path
    compacted_root: Path
    quarantine_root: Path
    report_root: Path
    state_root: Path
    availability_manifest: Path | None = None
    availability_import: Path | None = None
    provenance_root: Path | None = None
    symbols: tuple[str, ...] = ("NIFTY",)
    batch_rows: int = 50_000
    parse_workers: int = 4
    poll_seconds: float = 30.0

    def validated(self) -> FollowerConfig:
        if self.batch_rows < 1:
            raise ValueError("batch_rows must be >= 1")
        if self.parse_workers < 1:
            raise ValueError("parse_workers must be >= 1")
        if self.poll_seconds <= 0:
            raise ValueError("poll_seconds must be > 0")
        symbols = tuple(sorted({item.strip().upper() for item in self.symbols if item.strip()}))
        if not symbols:
            raise ValueError("at least one symbol is required")
        refresh_values = (self.availability_import, self.provenance_root)
        if any(value is not None for value in refresh_values) and not all(
            value is not None for value in refresh_values
        ):
            raise ValueError("availability_import and provenance_root must be supplied together")
        if any(value is not None for value in refresh_values) and self.availability_manifest is None:
            raise ValueError(
                "availability_import and provenance_root require availability_manifest"
            )
        return FollowerConfig(
            download_manifest=self.download_manifest.resolve(),
            raw_root=self.raw_root.resolve(),
            fragment_root=self.fragment_root.resolve(),
            extraction_manifest=self.extraction_manifest.resolve(),
            compacted_root=self.compacted_root.resolve(),
            quarantine_root=self.quarantine_root.resolve(),
            report_root=self.report_root.resolve(),
            state_root=self.state_root.resolve(),
            availability_manifest=(
                self.availability_manifest.resolve() if self.availability_manifest else None
            ),
            availability_import=(
                self.availability_import.resolve() if self.availability_import else None
            ),
            provenance_root=(self.provenance_root.resolve() if self.provenance_root else None),
            symbols=symbols,
            batch_rows=self.batch_rows,
            parse_workers=self.parse_workers,
            poll_seconds=self.poll_seconds,
        )


@dataclass(frozen=True)
class CycleReport:
    discovered_months: int
    eligible_months: int
    processed_months: int
    skipped_months: int
    incomplete_months: int
    blocked_months: int
    snapshot_path: str | None
    snapshot_sha256: str | None
    outcomes: tuple[tuple[str, str], ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["outcomes"] = dict(self.outcomes)
        return payload


Extractor = Callable[..., Any]
Auditor = Callable[..., Any]
AvailabilityClassifier = Callable[..., Any]


@dataclass(frozen=True)
class _AvailabilityJournal:
    path: Path | None
    identity: str
    import_sha256: str | None
    source_set_sha256: str | None


def follow_once(
    config: FollowerConfig,
    *,
    today: date | None = None,
    extractor: Extractor = extract_and_compact_span_range,
    auditor: Auditor = audit_span_backfill,
    availability_classifier: AvailabilityClassifier = import_and_classify_availability,
) -> CycleReport:
    """Run one non-mutating manifest observation and closed-month follow cycle."""

    cfg = config.validated()
    current_day = today or datetime.now(IST).date()
    manifest_bytes = _stable_read(cfg.download_manifest)
    events = _parse_download_manifest(manifest_bytes, cfg.download_manifest)
    latest = _latest_cells(events)
    discovered = sorted({(day.year, day.month) for day, _slot in latest})
    eligible = _eligible_months(latest, current_day)
    if not eligible:
        return CycleReport(
            discovered_months=len(discovered),
            eligible_months=0,
            processed_months=0,
            skipped_months=0,
            incomplete_months=0,
            blocked_months=0,
            snapshot_path=None,
            snapshot_sha256=None,
            outcomes=(),
        )

    snapshot_path, snapshot_digest = _publish_snapshot(cfg.state_root, manifest_bytes)
    config_digest = _config_fingerprint(cfg)
    processed = skipped = incomplete = blocked = 0
    outcomes: list[tuple[str, str]] = []

    for year, month in eligible:
        month_key = f"{year:04d}-{month:02d}"
        start_date = date(year, month, 1)
        end_date = date(year, month, calendar.monthrange(year, month)[1])
        input_digest = _month_fingerprint(latest, year, month)
        journal: _AvailabilityJournal | None = None
        try:
            journal = _effective_availability_journal(
                cfg.availability_manifest, cfg.availability_import
            )
            missing_availability = _missing_availability_cells(
                latest,
                year,
                month,
                journal.path,
            )
        except Exception as exc:
            _append_state(
                cfg.state_root,
                month_key,
                {
                    "event": "fatal_availability",
                    "month": month_key,
                    "input_fingerprint": input_digest,
                    "config_fingerprint": config_digest,
                    "manifest_snapshot": str(snapshot_path),
                    "manifest_snapshot_sha256": snapshot_digest,
                    "error_type": type(exc).__name__,
                    **(_journal_state(journal) if journal is not None else {}),
                },
            )
            raise FollowerFatalError(
                f"availability integrity check failed for {month_key}: {type(exc).__name__}"
            ) from exc
        if missing_availability and cfg.availability_import is not None:
            try:
                attempt_fingerprint, import_digest = _classification_attempt_fingerprint(
                    input_fingerprint=input_digest,
                    journal=journal,
                    uncovered_cells=missing_availability,
                )
                prior_attempt = _latest_matching_state(
                    cfg.state_root,
                    month_key,
                    {"classification_attempt_fingerprint": attempt_fingerprint},
                )
                if prior_attempt is None:
                    classification = availability_classifier(
                        start_date=start_date,
                        end_date=end_date,
                        import_path=cfg.availability_import,
                        download_manifest=snapshot_path,
                        availability_manifest=journal.path,
                        provenance_root=cfg.provenance_root,
                    )
                    remaining = _missing_availability_cells(
                        latest,
                        year,
                        month,
                        journal.path,
                    )
                    post_journal = _effective_availability_journal(
                        cfg.availability_manifest, cfg.availability_import
                    )
                    if post_journal.identity != journal.identity:
                        raise ManifestValidationError(
                            f"reviewed availability import changed during classification: "
                            f"{cfg.availability_import}"
                        )
                    # Classification appends to the content-addressed availability journal.
                    # Carry the refreshed identity into this cycle's reusable-state key;
                    # otherwise the next poll sees a different identity and repeats extraction
                    # and audit once before it can recognize the durable incomplete result.
                    journal = post_journal
                    remaining_attempt_fingerprint, _ = _classification_attempt_fingerprint(
                        input_fingerprint=input_digest,
                        journal=journal,
                        uncovered_cells=remaining,
                    )
                    _append_state(
                        cfg.state_root,
                        month_key,
                        {
                            "event": "availability_classification_attempted",
                            "month": month_key,
                            "input_fingerprint": input_digest,
                            "config_fingerprint": config_digest,
                            "classification_attempt_fingerprint": (
                                remaining_attempt_fingerprint
                            ),
                            "requested_attempt_fingerprint": attempt_fingerprint,
                            "availability_import_sha256": import_digest,
                            **_journal_state(journal),
                            "requested_cells": [
                                {"trading_date": trading_date, "slot": slot}
                                for trading_date, slot in sorted(missing_availability)
                            ],
                            "uncovered_cells": [
                                {"trading_date": trading_date, "slot": slot}
                                for trading_date, slot in sorted(remaining)
                            ],
                            "manifest_snapshot": str(snapshot_path),
                            "manifest_snapshot_sha256": snapshot_digest,
                            "classified_missing_cells": int(
                                getattr(classification, "classified_missing_cells", 0)
                            ),
                            "unresolved_missing_cells": int(
                                getattr(classification, "unresolved_missing_cells", 0)
                            ),
                            "source_boundary_cells": int(
                                getattr(classification, "source_boundary_cells", 0)
                            ),
                        },
                    )
                    LOGGER.info(
                        "month=%s action=classify_availability requested_cells=%d "
                        "remaining_cells=%d classified=%d unresolved=%d source_boundary=%d",
                        month_key,
                        len(missing_availability),
                        len(remaining),
                        int(getattr(classification, "classified_missing_cells", 0)),
                        int(getattr(classification, "unresolved_missing_cells", 0)),
                        int(getattr(classification, "source_boundary_cells", 0)),
                    )
                else:
                    LOGGER.info(
                        "month=%s action=skip_availability_classification "
                        "reason=matching_durable_attempt uncovered_cells=%d",
                        month_key,
                        len(missing_availability),
                    )
            except Exception as exc:
                _append_state(
                    cfg.state_root,
                    month_key,
                    {
                        "event": "fatal_availability",
                        "month": month_key,
                        "input_fingerprint": input_digest,
                        "config_fingerprint": config_digest,
                        "manifest_snapshot": str(snapshot_path),
                        "manifest_snapshot_sha256": snapshot_digest,
                        "error_type": type(exc).__name__,
                        **_journal_state(journal),
                    },
                )
                raise FollowerFatalError(
                    f"availability classification failed for {month_key}: {type(exc).__name__}"
                ) from exc
        availability_digest = _availability_fingerprint(journal.path, year, month)
        fingerprints = {
            "input_fingerprint": input_digest,
            "config_fingerprint": config_digest,
            "availability_fingerprint": availability_digest,
            "availability_journal_identity": journal.identity,
        }
        prior = _latest_matching_state(cfg.state_root, month_key, fingerprints)
        if prior is not None and _state_is_reusable(prior):
            skipped += 1
            prior_outcome = str(prior.get("outcome", "PASS_READY"))
            outcomes.append((month_key, prior_outcome))
            LOGGER.info("month=%s action=skip outcome=%s", month_key, prior_outcome)
            continue

        common_state = {
            "month": month_key,
            **fingerprints,
            "manifest_snapshot": str(snapshot_path),
            "manifest_snapshot_sha256": snapshot_digest,
            **_journal_state(journal),
        }
        _append_state(cfg.state_root, month_key, {"event": "started", **common_state})
        LOGGER.info(
            "month=%s action=extract parse_workers=%d", month_key, cfg.parse_workers
        )
        try:
            extraction = extractor(
                start_date=start_date,
                end_date=end_date,
                raw_root=cfg.raw_root,
                download_manifest=snapshot_path,
                fragment_root=cfg.fragment_root,
                extraction_manifest=cfg.extraction_manifest,
                compacted_root=cfg.compacted_root,
                quarantine_root=cfg.quarantine_root,
                symbols=cfg.symbols,
                batch_rows=cfg.batch_rows,
                parse_workers=cfg.parse_workers,
            )
        except Exception as exc:
            _append_state(
                cfg.state_root,
                month_key,
                {"event": "fatal_extraction", **common_state, "error_type": type(exc).__name__},
            )
            raise FollowerFatalError(f"extraction failed for {month_key}: {type(exc).__name__}") from exc
        if not bool(getattr(extraction, "ok", False)):
            _append_state(
                cfg.state_root,
                month_key,
                {"event": "fatal_extraction", **common_state, "reason": "report_not_ok"},
            )
            raise FollowerFatalError(f"extraction report failed for {month_key}")

        try:
            audit = auditor(
                start_date=start_date,
                end_date=end_date,
                raw_root=cfg.raw_root,
                download_manifest=snapshot_path,
                extraction_manifest=cfg.extraction_manifest,
                fragment_root=cfg.fragment_root,
                compacted_root=cfg.compacted_root,
                report_root=cfg.report_root / f"{year:04d}_{month:02d}",
                availability_manifest=journal.path,
            )
        except Exception as exc:
            _append_state(
                cfg.state_root,
                month_key,
                {"event": "fatal_integrity", **common_state, "error_type": type(exc).__name__},
            )
            raise FollowerFatalError(f"strict audit failed for {month_key}: {type(exc).__name__}") from exc

        outcome = str(getattr(audit, "outcome", ""))
        audit_counts = _safe_audit_counts(audit)
        outcomes.append((month_key, outcome))
        if outcome == "FAIL_INTEGRITY":
            _append_state(
                cfg.state_root,
                month_key,
                {"event": "fatal_integrity", "outcome": outcome, **common_state, **audit_counts},
            )
            raise FollowerFatalError(f"strict audit reported FAIL_INTEGRITY for {month_key}")
        if outcome == "FAIL_INCOMPLETE":
            incomplete += 1
            processed += 1
            _append_state(
                cfg.state_root,
                month_key,
                {"event": "audit_incomplete", "outcome": outcome, **common_state, **audit_counts},
            )
            LOGGER.info("month=%s outcome=%s", month_key, outcome)
            continue
        if outcome == "BLOCKED_SOURCE":
            blocked += 1
            processed += 1
            _append_state(
                cfg.state_root,
                month_key,
                {"event": "blocked_source", "outcome": outcome, **common_state, **audit_counts},
            )
            LOGGER.info("month=%s outcome=%s", month_key, outcome)
            continue
        if outcome != "PASS_READY":
            _append_state(
                cfg.state_root,
                month_key,
                {"event": "fatal_integrity", "outcome": outcome, **common_state, **audit_counts},
            )
            raise FollowerFatalError(f"strict audit returned unknown outcome {outcome!r} for {month_key}")

        outputs = _validated_outputs(audit)
        if int(getattr(audit, "downloaded_cells", 0)) > 0 and not outputs:
            _append_state(
                cfg.state_root,
                month_key,
                {
                    "event": "fatal_integrity",
                    "outcome": "FAIL_INTEGRITY",
                    "reason": "downloaded_cells_without_compacted_output",
                    **common_state,
                    **audit_counts,
                },
            )
            raise FollowerFatalError(f"PASS_READY audit has no compacted output for {month_key}")
        processed += 1
        _append_state(
            cfg.state_root,
            month_key,
            {
                "event": "completed",
                "outcome": outcome,
                "outputs": outputs,
                **common_state,
                **audit_counts,
            },
        )
        LOGGER.info("month=%s outcome=%s outputs=%d", month_key, outcome, len(outputs))

    return CycleReport(
        discovered_months=len(discovered),
        eligible_months=len(eligible),
        processed_months=processed,
        skipped_months=skipped,
        incomplete_months=incomplete,
        blocked_months=blocked,
        snapshot_path=str(snapshot_path),
        snapshot_sha256=snapshot_digest,
        outcomes=tuple(outcomes),
    )


def _stable_read(path: Path) -> bytes:
    if not path.exists():
        return b""
    with path.open("rb") as stream:
        captured_size = os.fstat(stream.fileno()).st_size
        content = stream.read(captured_size)
    if len(content) != captured_size:
        raise ManifestNotStableError(f"could not read the captured prefix of {path}")
    if content and not content.endswith(b"\n"):
        raise ManifestNotStableError(f"captured manifest prefix ends in a partial line: {path}")
    return content


def _parse_jsonl(content: bytes, path: Path) -> list[Mapping[str, Any]]:
    events: list[Mapping[str, Any]] = []
    for line_number, raw in enumerate(content.splitlines(), start=1):
        if not raw.strip():
            continue
        try:
            event = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ManifestValidationError(f"invalid JSON in {path} line {line_number}") from exc
        if not isinstance(event, dict):
            raise ManifestValidationError(f"non-object JSON in {path} line {line_number}")
        events.append(event)
    return events


def _parse_download_manifest(content: bytes, path: Path) -> list[Mapping[str, Any]]:
    events = _parse_jsonl(content, path)
    for line_number, event in enumerate(events, start=1):
        if "slot" not in event:
            continue
        try:
            date.fromisoformat(str(event["trading_date"]))
        except (KeyError, ValueError) as exc:
            raise ManifestValidationError(
                f"invalid trading_date in cell event {line_number} of {path}"
            ) from exc
        slot = str(event["slot"])
        if slot not in SLOT_SUFFIX:
            raise ManifestValidationError(f"invalid slot in cell event {line_number} of {path}")
        if str(event.get("suffix", "")) != SLOT_SUFFIX[slot]:
            raise ManifestValidationError(f"slot/suffix mismatch in cell event {line_number} of {path}")
        if not isinstance(event.get("terminal"), bool):
            raise ManifestValidationError(f"non-boolean terminal in cell event {line_number} of {path}")
    return events


def _latest_cells(
    events: Sequence[Mapping[str, Any]],
) -> dict[tuple[date, str], Mapping[str, Any]]:
    latest: dict[tuple[date, str], Mapping[str, Any]] = {}
    for event in events:
        if "slot" not in event:
            continue
        latest[(date.fromisoformat(str(event["trading_date"])), str(event["slot"]))] = event
    return latest


def _eligible_months(
    latest: Mapping[tuple[date, str], Mapping[str, Any]], current_day: date
) -> list[tuple[int, int]]:
    current_month = (current_day.year, current_day.month)
    months = sorted({(day.year, day.month) for day, _slot in latest})
    eligible: list[tuple[int, int]] = []
    for year, month in months:
        if (year, month) >= current_month:
            continue
        last_day = calendar.monthrange(year, month)[1]
        complete = True
        for day_number in range(1, last_day + 1):
            day = date(year, month, day_number)
            for slot, _suffix in SLOT_SPECS:
                event = latest.get((day, slot))
                if event is None or event.get("terminal") is not True:
                    complete = False
                    break
            if not complete:
                break
        if complete:
            eligible.append((year, month))
    return eligible


def _publish_snapshot(state_root: Path, content: bytes) -> tuple[Path, str]:
    digest = sha256(content).hexdigest()
    directory = state_root / "snapshots"
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / f"{digest}.jsonl"
    if destination.exists():
        if sha256(destination.read_bytes()).hexdigest() != digest:
            raise FollowerFatalError(f"existing manifest snapshot failed integrity: {destination}")
        return destination, digest

    partial = directory / f".{digest}.{uuid.uuid4().hex}.partial"
    try:
        with partial.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(partial, destination)
        except FileExistsError:
            if sha256(destination.read_bytes()).hexdigest() != digest:
                raise FollowerFatalError(
                    f"concurrent manifest snapshot failed integrity: {destination}"
                )
        _fsync_directory(directory)
    finally:
        partial.unlink(missing_ok=True)
    return destination, digest


def _month_fingerprint(
    latest: Mapping[tuple[date, str], Mapping[str, Any]], year: int, month: int
) -> str:
    cells = []
    for (day, slot), event in sorted(latest.items(), key=lambda item: item[0]):
        if (day.year, day.month) != (year, month):
            continue
        normalized = {field: event.get(field) for field in FINGERPRINT_FIELDS}
        normalized["trading_date"] = day.isoformat()
        normalized["slot"] = slot
        cells.append(normalized)
    return _json_digest(cells)


def _effective_availability_journal(
    base_manifest: Path | None, availability_import: Path | None
) -> _AvailabilityJournal:
    if availability_import is None:
        identity = _json_digest(
            {
                "mode": "base_availability_manifest",
                "path": str(base_manifest) if base_manifest is not None else None,
            }
        )
        return _AvailabilityJournal(base_manifest, identity, None, None)
    if base_manifest is None:
        raise ManifestValidationError(
            "availability import requires a base availability manifest path"
        )

    content = _stable_file_read(availability_import)
    import_digest = sha256(content).hexdigest()
    try:
        payload = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ManifestValidationError(
            f"invalid reviewed availability import: {availability_import}"
        ) from exc
    if not isinstance(payload, dict):
        raise ManifestValidationError("reviewed availability import must be a JSON object")
    raw_sources = payload.get("sources")
    if not isinstance(raw_sources, list) or not raw_sources:
        raise ManifestValidationError("reviewed availability import requires sources")

    source_digests: list[dict[str, str]] = []
    all_hashes_embedded = True
    for index, source in enumerate(raw_sources):
        if not isinstance(source, dict):
            raise ManifestValidationError(f"reviewed import source {index} is not an object")
        source_id = str(source.get("id", "")).strip()
        if not source_id:
            raise ManifestValidationError(f"reviewed import source {index} has no id")
        declared = str(source.get("sha256", "")).strip().lower()
        if len(declared) == 64 and all(character in "0123456789abcdef" for character in declared):
            digest = declared
        else:
            all_hashes_embedded = False
            raw_path = Path(str(source.get("path", "")))
            source_path = (
                raw_path if raw_path.is_absolute() else availability_import.parent / raw_path
            ).resolve()
            if not source_path.is_file():
                raise ManifestValidationError(
                    f"reviewed import source artifact is absent: {source_path}"
                )
            digest = _sha256_file(source_path)
        source_digests.append({"source_id": source_id, "sha256": digest})
    source_set_digest = _json_digest(sorted(source_digests, key=lambda item: item["source_id"]))
    source_identity = None if all_hashes_embedded else source_set_digest
    journal_identity = (
        import_digest
        if source_identity is None
        else f"{import_digest}.sources-{source_identity}"
    )
    suffix = base_manifest.suffix or ".jsonl"
    stem = base_manifest.stem if base_manifest.suffix else base_manifest.name
    effective_path = base_manifest.with_name(f"{stem}.{journal_identity}{suffix}")
    return _AvailabilityJournal(
        effective_path,
        journal_identity,
        import_digest,
        source_identity,
    )


def _journal_state(journal: _AvailabilityJournal) -> dict[str, str | None]:
    return {
        "effective_availability_manifest": (
            str(journal.path) if journal.path is not None else None
        ),
        "availability_journal_identity": journal.identity,
        "availability_import_sha256": journal.import_sha256,
        "availability_source_set_sha256": journal.source_set_sha256,
    }


def _stable_file_read(path: Path) -> bytes:
    if not path.is_file():
        raise ManifestValidationError(f"required reviewed import does not exist: {path}")
    with path.open("rb") as stream:
        before = os.fstat(stream.fileno())
        content = stream.read(before.st_size)
        after = os.fstat(stream.fileno())
    if (
        len(content) != before.st_size
        or before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
    ):
        raise ManifestNotStableError(f"reviewed import changed while being read: {path}")
    return content


def _missing_availability_cells(
    latest: Mapping[tuple[date, str], Mapping[str, Any]],
    year: int,
    month: int,
    availability_manifest: Path | None,
) -> set[tuple[str, str]]:
    required = {
        (day.isoformat(), slot)
        for (day, slot), event in latest.items()
        if (day.year, day.month) == (year, month)
        and event.get("terminal") is True
        and str(event.get("state", "")) in MISSING_STATES
    }
    if not required or availability_manifest is None:
        return required
    available = load_availability_events(availability_manifest)
    return required - set(available)


def _classification_attempt_fingerprint(
    *,
    input_fingerprint: str,
    journal: _AvailabilityJournal,
    uncovered_cells: set[tuple[str, str]],
) -> tuple[str, str]:
    if journal.import_sha256 is None:
        raise ManifestValidationError("classification attempt requires reviewed import identity")
    import_digest = journal.import_sha256
    attempt_digest = _json_digest(
        {
            "state_version": SCRIPT_STATE_VERSION,
            "input_fingerprint": input_fingerprint,
            "availability_import_sha256": import_digest,
            "availability_journal_identity": journal.identity,
            "uncovered_cells": [
                {"trading_date": trading_date, "slot": slot}
                for trading_date, slot in sorted(uncovered_cells)
            ],
        }
    )
    return attempt_digest, import_digest


def _availability_fingerprint(path: Path | None, year: int, month: int) -> str:
    if path is None:
        return _json_digest({"availability_manifest": None})
    content = _stable_read(path)
    events = _parse_jsonl(content, path)
    latest: dict[tuple[str, str], Mapping[str, Any]] = {}
    for event in events:
        try:
            event_day = date.fromisoformat(str(event["trading_date"]))
            slot = str(event["slot"])
        except (KeyError, ValueError) as exc:
            raise ManifestValidationError(f"invalid availability event in {path}") from exc
        if slot not in SLOT_SUFFIX:
            raise ManifestValidationError(f"invalid availability slot in {path}")
        if (event_day.year, event_day.month) == (year, month):
            latest[(event_day.isoformat(), slot)] = event
    normalized = [latest[key] for key in sorted(latest)]
    return _json_digest(normalized)


def _config_fingerprint(config: FollowerConfig) -> str:
    return _json_digest(
        {
            "state_version": SCRIPT_STATE_VERSION,
            "symbols": config.symbols,
            "batch_rows": config.batch_rows,
            "parse_workers": config.parse_workers,
        }
    )


def _json_digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return sha256(encoded).hexdigest()


def _append_state(state_root: Path, month: str, payload: Mapping[str, Any]) -> Path:
    directory = state_root / "events" / month
    directory.mkdir(parents=True, exist_ok=True)
    now = datetime.now(UTC)
    # Windows wall-clock timestamps can repeat for consecutive state writes.  Include a
    # strictly increasing process-local monotonic component so lexicographic replay preserves
    # causal order instead of falling back to random UUID ordering inside the same clock tick.
    stem = (
        f"{now.strftime('%Y%m%dT%H%M%S.%fZ')}."
        f"{_next_state_clock():020d}.{uuid.uuid4().hex}"
    )
    destination = directory / f"{stem}.json"
    partial = directory / f".{stem}.partial"
    event = {"recorded_at_utc": now.isoformat().replace("+00:00", "Z"), **payload}
    encoded = (json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n").encode()
    with partial.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(partial, destination)
    _fsync_directory(directory)
    return destination


def _next_state_clock() -> int:
    global _STATE_CLOCK_LAST
    with _STATE_CLOCK_LOCK:
        _STATE_CLOCK_LAST = max(time.monotonic_ns(), _STATE_CLOCK_LAST + 1)
        return _STATE_CLOCK_LAST


def _latest_matching_state(
    state_root: Path, month: str, fingerprints: Mapping[str, str]
) -> Mapping[str, Any] | None:
    directory = state_root / "events" / month
    if not directory.is_dir():
        return None
    matching: Mapping[str, Any] | None = None
    for path in sorted(directory.glob("*.json")):
        try:
            event = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise FollowerFatalError(f"invalid follower state record: {path}") from exc
        if not isinstance(event, dict):
            raise FollowerFatalError(f"invalid follower state record: {path}")
        if all(event.get(key) == value for key, value in fingerprints.items()):
            matching = event
    return matching


def _state_is_reusable(event: Mapping[str, Any]) -> bool:
    kind = event.get("event")
    if kind in {"audit_incomplete", "blocked_source"}:
        return True
    if kind != "completed" or event.get("outcome") != "PASS_READY":
        return False
    outputs = event.get("outputs")
    if not isinstance(outputs, list):
        return False
    for output in outputs:
        if not isinstance(output, dict):
            return False
        path = Path(str(output.get("path", "")))
        expected = str(output.get("sha256", ""))
        if not path.is_file() or not expected or _sha256_file(path) != expected:
            return False
    return True


def _validated_outputs(audit: Any) -> list[dict[str, str]]:
    outputs: list[dict[str, str]] = []
    for month in getattr(audit, "months", ()):
        raw_path = str(getattr(month, "compacted_path", ""))
        declared = getattr(month, "sha256", None)
        if not raw_path or not declared:
            continue
        path = Path(raw_path).resolve()
        actual = _sha256_file(path) if path.is_file() else None
        if actual != str(declared):
            raise FollowerFatalError(f"audited compacted output failed integrity: {path}")
        outputs.append({"path": str(path), "sha256": actual})
    return outputs


def _safe_audit_counts(audit: Any) -> dict[str, int]:
    names = (
        "expected_cells",
        "terminal_cells",
        "downloaded_cells",
        "unavailable_cells",
        "failed_or_incomplete_cells",
        "raw_integrity_failures",
        "downloaded_without_valid_extraction",
        "duplicate_natural_keys",
    )
    return {name: int(getattr(audit, name, 0)) for name in names}


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--download-manifest", type=Path, required=True)
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--fragment-root", type=Path, required=True)
    parser.add_argument("--extraction-manifest", type=Path, required=True)
    parser.add_argument("--parquet-root", type=Path, required=True)
    parser.add_argument("--quarantine-root", type=Path, required=True)
    parser.add_argument("--report-root", type=Path, required=True)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--availability-manifest", type=Path)
    parser.add_argument(
        "--availability-import",
        type=Path,
        help="reviewed span-availability-import/v1 JSON used to classify missing cells",
    )
    parser.add_argument(
        "--provenance-root",
        type=Path,
        help="immutable retained-source root used with --availability-import",
    )
    parser.add_argument("--symbols", nargs="+", default=["NIFTY"])
    parser.add_argument("--batch-rows", type=int, default=50_000)
    parser.add_argument("--parse-workers", type=int, default=4)
    parser.add_argument("--poll-seconds", type=float, default=30.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def _config_from_args(args: argparse.Namespace) -> FollowerConfig:
    return FollowerConfig(
        download_manifest=args.download_manifest,
        raw_root=args.raw_root,
        fragment_root=args.fragment_root,
        extraction_manifest=args.extraction_manifest,
        compacted_root=args.parquet_root,
        quarantine_root=args.quarantine_root,
        report_root=args.report_root,
        state_root=args.state_root,
        availability_manifest=args.availability_manifest,
        availability_import=args.availability_import,
        provenance_root=args.provenance_root,
        symbols=tuple(args.symbols),
        batch_rows=args.batch_rows,
        parse_workers=args.parse_workers,
        poll_seconds=args.poll_seconds,
    )


def _emit(report: CycleReport, as_json: bool) -> None:
    if as_json:
        print(json.dumps(report.to_dict(), sort_keys=True))
    else:
        LOGGER.info(
            "cycle eligible=%d processed=%d skipped=%d incomplete=%d blocked=%d",
            report.eligible_months,
            report.processed_months,
            report.skipped_months,
            report.incomplete_months,
            report.blocked_months,
        )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        config = _config_from_args(args).validated()
    except ValueError as exc:
        LOGGER.error("invalid configuration: %s", exc)
        return 2

    if args.once:
        try:
            _emit(follow_once(config), args.json)
            return 0
        except (ManifestNotStableError, ManifestValidationError) as exc:
            LOGGER.warning("manifest unavailable for a safe cycle: %s", exc)
            return 2
        except FollowerFatalError as exc:
            LOGGER.error("follower stopped: %s", exc)
            return 1

    while True:
        try:
            _emit(follow_once(config), args.json)
        except ManifestNotStableError as exc:
            LOGGER.warning("manifest prefix not stable; retrying: %s", exc)
        except (ManifestValidationError, FollowerFatalError) as exc:
            LOGGER.error("follower stopped: %s", exc)
            return 1
        try:
            time.sleep(config.poll_seconds)
        except KeyboardInterrupt:
            LOGGER.info("follower stopped by operator")
            return 0


if __name__ == "__main__":
    sys.exit(main())
