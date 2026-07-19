"""Conservative, provenance-backed classification of missing NSE SPAN cells.

The NSE holiday API is a useful current-year source, but it is not a complete
historical calendar of special sessions or SPAN publication schedules.  This
module therefore imports explicit human-reviewed classifications whose source
bytes are retained and hashed.  It never derives closure from weekday alone.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse
import json
import os
import shutil
import uuid

from .backfill_downloader import MISSING_STATES, SLOT_SPECS
from .durable_jsonl import append_jsonl_records


IMPORT_SCHEMA = "span-availability-import/v1"
EVENT_SCHEMA = "span-availability-event/v1"
CORRUPT_ARCHIVE_EVENT_SCHEMA = "span-source-corruption-availability/v1"
REPEATED_STATIC_BOUNDARY_EVENT_SCHEMA = "span-repeated-static-boundary/v1"
REPEATED_STATIC_BOUNDARY_EVENT = "official_source_repeated_static_boundary"
REPEATED_STATIC_CORRUPT_BASIS = "repeated_http200_corrupt_inner_zip"
REPEATED_STATIC_404_BASIS = "repeated_http404"
OFFICIAL_HOSTS = frozenset(
    {
        "nseindia.com",
        "www.nseindia.com",
        "nsearchives.nseindia.com",
        "nseclearing.in",
        "www.nseclearing.in",
        "archive.nseclearing.in",
    }
)
MARKET_STATES = frozenset(
    {
        "closed",
        "regular_trading_day",
        "special_trading_session",
        "trading_source_boundary",
    }
)
CLOSED_CLASSIFICATIONS = frozenset(
    {"official_holiday", "official_non_trading_day", "official_weekend"}
)
WEEKDAY_NAMES = {
    "Monday": 0,
    "Tuesday": 1,
    "Wednesday": 2,
    "Thursday": 3,
    "Friday": 4,
    "Saturday": 5,
    "Sunday": 6,
}


@dataclass(frozen=True)
class AvailabilityClassificationReport:
    start_date: str
    end_date: str
    imported_dates: int
    classified_missing_cells: int
    unresolved_missing_cells: int
    source_boundary_cells: int
    retained_sources: int
    availability_manifest: str
    provenance_root: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def import_and_classify_availability(
    *,
    start_date: date,
    end_date: date,
    import_path: str | Path,
    download_manifest: str | Path,
    availability_manifest: str | Path,
    provenance_root: str | Path,
) -> AvailabilityClassificationReport:
    """Import reviewed official evidence and classify only missing raw cells.

    ``closed`` dates can account for absent SPAN slots.  A regular or special
    trading session never can.  An explicitly evidenced source boundary is
    retained as a blocker rather than being treated as an acceptable absence.
    """

    if start_date > end_date:
        raise ValueError(f"start date {start_date} must be <= end date {end_date}")
    source_path = Path(import_path).resolve()
    payload = _read_object(source_path)
    if payload.get("schema_version") != IMPORT_SCHEMA:
        raise ValueError(
            f"availability import must use schema_version {IMPORT_SCHEMA!r}"
        )

    output_manifest = Path(availability_manifest).resolve()
    artifacts_root = Path(provenance_root).resolve()
    if output_manifest.exists():
        load_availability_events(output_manifest)
    retained = _retain_sources(
        payload.get("sources"),
        source_path.parent,
        artifacts_root,
        output_manifest.parent,
    )
    date_entries = _validate_dates(payload.get("dates"), retained, start_date, end_date)
    date_entries = _expand_weekly_rules(
        payload.get("weekly_rules"), retained, start_date, end_date, date_entries
    )
    downloads = _latest_download_events(Path(download_manifest))

    events: list[dict[str, Any]] = []
    classified = unresolved = boundaries = 0
    observed = datetime.now(UTC).isoformat()
    for (trading_date, slot), download in sorted(downloads.items()):
        day = date.fromisoformat(trading_date)
        if day < start_date or day > end_date:
            continue
        if str(download.get("state", "")) not in MISSING_STATES:
            continue
        entry = date_entries.get(trading_date)
        if entry is None:
            unresolved += 1
            continue
        market_state = str(entry["market_state"])
        if market_state == "closed":
            classification = str(entry["classification"])
            outcome = "accepted_absence"
            classified += 1
        elif market_state == "trading_source_boundary":
            classification = "official_trading_day_source_boundary"
            outcome = "source_boundary"
            boundaries += 1
        else:
            # A weekend special session and an ordinary trading day are both
            # expected-source days.  Missing archives remain unresolved.
            classification = f"official_{market_state}"
            outcome = "unresolved"
            unresolved += 1
        events.append(
            {
                "schema_version": EVENT_SCHEMA,
                "event": "availability_classification",
                "trading_date": trading_date,
                "slot": slot,
                "download_state": str(download.get("state", "")),
                "market_state": market_state,
                "calendar_classification": classification,
                "classification_outcome": outcome,
                "source_availability_boundary_proven": outcome == "source_boundary",
                "reason": str(entry["reason"]),
                "sources": [retained[item] for item in entry["source_ids"]],
                "observed_at_utc": observed,
            }
        )
    _append_jsonl(output_manifest, events)
    return AvailabilityClassificationReport(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        imported_dates=len(date_entries),
        classified_missing_cells=classified,
        unresolved_missing_cells=unresolved,
        source_boundary_cells=boundaries,
        retained_sources=len(retained),
        availability_manifest=str(output_manifest),
        provenance_root=str(artifacts_root),
    )


def load_availability_events(
    path: str | Path,
) -> dict[tuple[str, str], Mapping[str, Any]]:
    """Load latest events and re-verify every retained official source artifact."""

    manifest = Path(path).resolve()
    latest: dict[tuple[str, str], Mapping[str, Any]] = {}
    for line_number, event in _jsonl(manifest):
        _validate_event(event, manifest.parent, line_number)
        key = (str(event["trading_date"]), str(event["slot"]))
        latest[key] = event
    return latest


def _retain_sources(
    raw_sources: Any,
    import_parent: Path,
    provenance_root: Path,
    manifest_parent: Path,
) -> dict[str, dict[str, str]]:
    if not isinstance(raw_sources, list) or not raw_sources:
        raise ValueError("availability import requires a non-empty sources list")
    provenance_root.mkdir(parents=True, exist_ok=True)
    retained: dict[str, dict[str, str]] = {}
    for index, item in enumerate(raw_sources):
        if not isinstance(item, dict):
            raise ValueError(f"sources[{index}] must be an object")
        source_id = str(item.get("id", "")).strip()
        if not source_id or source_id in retained:
            raise ValueError(f"sources[{index}] has an empty or duplicate id")
        url = str(item.get("url", "")).strip()
        _validate_official_url(url)
        fetched_at = _utc_timestamp(str(item.get("fetched_at_utc", "")))
        local = Path(str(item.get("path", "")))
        if not local.is_absolute():
            local = import_parent / local
        local = local.resolve()
        if not local.is_file():
            raise ValueError(f"source {source_id!r} artifact does not exist: {local}")
        digest = _sha256_file(local)
        declared = str(item.get("sha256", "")).lower()
        if declared and declared != digest:
            raise ValueError(
                f"source {source_id!r} SHA-256 differs from declared value"
            )
        suffix = local.suffix.lower() if local.suffix else ".bin"
        target = provenance_root / f"{digest}{suffix}"
        _publish_immutable(local, target, digest)
        retained[source_id] = {
            "source_id": source_id,
            "source_url": url,
            "source_sha256": digest,
            "source_fetched_at_utc": fetched_at,
            "source_artifact_path": os.path.relpath(target, manifest_parent).replace(
                "\\", "/"
            ),
        }
    return retained


def _validate_dates(
    raw_dates: Any,
    sources: Mapping[str, Mapping[str, str]],
    start_date: date,
    end_date: date,
) -> dict[str, dict[str, Any]]:
    if not isinstance(raw_dates, list):
        raise ValueError("availability import dates must be a list")
    result: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(raw_dates):
        if not isinstance(item, dict):
            raise ValueError(f"dates[{index}] must be an object")
        trading_date = date.fromisoformat(str(item.get("date", "")))
        if trading_date < start_date or trading_date > end_date:
            continue
        key = trading_date.isoformat()
        if key in result:
            raise ValueError(f"conflicting duplicate availability entry for {key}")
        market_state = str(item.get("market_state", ""))
        if market_state not in MARKET_STATES:
            raise ValueError(
                f"dates[{index}] has unsupported market_state {market_state!r}"
            )
        classification = str(item.get("classification", ""))
        if market_state == "closed" and classification not in CLOSED_CLASSIFICATIONS:
            raise ValueError(
                f"closed date {key} requires classification in {sorted(CLOSED_CLASSIFICATIONS)!r}"
            )
        if market_state != "closed" and classification:
            raise ValueError(
                f"non-closed date {key} must not supply an accepted classification"
            )
        reason = str(item.get("reason", "")).strip()
        if not reason:
            raise ValueError(f"date {key} requires a review reason")
        source_ids = item.get("source_ids")
        if not isinstance(source_ids, list) or not source_ids:
            raise ValueError(f"date {key} requires at least one source_id")
        normalized_ids = tuple(str(value) for value in source_ids)
        unknown = set(normalized_ids) - set(sources)
        if unknown:
            raise ValueError(
                f"date {key} references unknown sources {sorted(unknown)!r}"
            )
        result[key] = {
            "market_state": market_state,
            "classification": classification,
            "reason": reason,
            "source_ids": normalized_ids,
        }
    return result


def _expand_weekly_rules(
    raw_rules: Any,
    sources: Mapping[str, Mapping[str, str]],
    start_date: date,
    end_date: date,
    explicit_dates: Mapping[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Expand reviewed source-backed weekly contracts.

    Explicit date entries always override a recurring contract. This is what
    keeps an NSE-notified Saturday/Sunday live session from being accepted as
    a normal weekend absence.
    """

    result = dict(explicit_dates)
    if raw_rules is None:
        return result
    if not isinstance(raw_rules, list):
        raise ValueError("availability import weekly_rules must be a list")
    generated_by: dict[str, str] = {}
    for index, item in enumerate(raw_rules):
        if not isinstance(item, dict):
            raise ValueError(f"weekly_rules[{index}] must be an object")
        rule_id = str(item.get("id", "")).strip()
        if not rule_id:
            raise ValueError(f"weekly_rules[{index}] requires a non-empty id")
        rule_start = date.fromisoformat(str(item.get("date_from", "")))
        rule_end = date.fromisoformat(str(item.get("date_to", "")))
        if rule_start > rule_end:
            raise ValueError(f"weekly rule {rule_id!r} has date_from after date_to")
        weekdays = item.get("weekdays")
        if not isinstance(weekdays, list) or not weekdays:
            raise ValueError(f"weekly rule {rule_id!r} requires weekdays")
        unknown_weekdays = set(str(value) for value in weekdays) - set(WEEKDAY_NAMES)
        if unknown_weekdays:
            raise ValueError(
                f"weekly rule {rule_id!r} has unsupported weekdays {sorted(unknown_weekdays)!r}"
            )
        weekday_numbers = {WEEKDAY_NAMES[str(value)] for value in weekdays}
        market_state = str(item.get("market_state", ""))
        classification = str(item.get("classification", ""))
        if market_state != "closed" or classification != "official_weekend":
            raise ValueError(
                f"weekly rule {rule_id!r} must classify closed official_weekend dates"
            )
        reason = str(item.get("reason", "")).strip()
        if not reason:
            raise ValueError(f"weekly rule {rule_id!r} requires a review reason")
        source_ids = item.get("source_ids")
        if not isinstance(source_ids, list) or not source_ids:
            raise ValueError(f"weekly rule {rule_id!r} requires at least one source_id")
        normalized_ids = tuple(str(value) for value in source_ids)
        unknown_sources = set(normalized_ids) - set(sources)
        if unknown_sources:
            raise ValueError(
                f"weekly rule {rule_id!r} references unknown sources {sorted(unknown_sources)!r}"
            )

        cursor = max(start_date, rule_start)
        last = min(end_date, rule_end)
        while cursor <= last:
            key = cursor.isoformat()
            if cursor.weekday() in weekday_numbers and key not in explicit_dates:
                if key in generated_by:
                    raise ValueError(
                        f"weekly rules {generated_by[key]!r} and {rule_id!r} overlap on {key}"
                    )
                generated_by[key] = rule_id
                result[key] = {
                    "market_state": market_state,
                    "classification": classification,
                    "reason": reason,
                    "source_ids": normalized_ids,
                }
            cursor += timedelta(days=1)
    return result


def _validate_event(
    event: Mapping[str, Any], manifest_parent: Path, line_number: int
) -> None:
    where = f"availability manifest line {line_number}"
    if event.get("schema_version") == CORRUPT_ARCHIVE_EVENT_SCHEMA:
        _validate_corrupt_archive_event(event, manifest_parent, where)
        return
    if event.get("schema_version") == REPEATED_STATIC_BOUNDARY_EVENT_SCHEMA:
        _validate_repeated_static_boundary_event(event, manifest_parent, where)
        return
    if (
        event.get("schema_version") != EVENT_SCHEMA
        or event.get("event") != "availability_classification"
    ):
        raise ValueError(f"{where} has an unsupported schema or event")
    date.fromisoformat(str(event.get("trading_date", "")))
    if str(event.get("slot", "")) not in {slot for slot, _ in SLOT_SPECS}:
        raise ValueError(f"{where} has an invalid slot")
    if str(event.get("download_state", "")) not in MISSING_STATES:
        raise ValueError(f"{where} does not classify a missing download state")
    market_state = str(event.get("market_state", ""))
    outcome = str(event.get("classification_outcome", ""))
    classification = str(event.get("calendar_classification", ""))
    if market_state == "closed":
        if (
            outcome != "accepted_absence"
            or classification not in CLOSED_CLASSIFICATIONS
        ):
            raise ValueError(f"{where} has an invalid closed-date outcome")
    elif market_state == "trading_source_boundary":
        if outcome != "source_boundary" or not event.get(
            "source_availability_boundary_proven"
        ):
            raise ValueError(f"{where} has an invalid source-boundary outcome")
    elif market_state in {"regular_trading_day", "special_trading_session"}:
        if outcome != "unresolved":
            raise ValueError(f"{where} attempts to accept absence on a trading session")
    else:
        raise ValueError(f"{where} has an invalid market_state")
    raw_sources = event.get("sources")
    if not isinstance(raw_sources, list) or not raw_sources:
        raise ValueError(f"{where} lacks source provenance")
    for source in raw_sources:
        if not isinstance(source, dict):
            raise ValueError(f"{where} has malformed source provenance")
        if not str(source.get("source_id", "")).strip():
            raise ValueError(f"{where} source provenance lacks source_id")
        _validate_official_url(str(source.get("source_url", "")))
        _utc_timestamp(str(source.get("source_fetched_at_utc", "")))
        digest = str(source.get("source_sha256", ""))
        raw_path = Path(str(source.get("source_artifact_path", "")))
        artifact = raw_path if raw_path.is_absolute() else (manifest_parent / raw_path)
        artifact = artifact.resolve()
        if (
            not artifact.is_file()
            or len(digest) != 64
            or _sha256_file(artifact) != digest
        ):
            raise ValueError(
                f"{where} source artifact is absent or has the wrong SHA-256"
            )


def _validate_corrupt_archive_event(
    event: Mapping[str, Any], manifest_parent: Path, where: str
) -> None:
    """Validate a byte-equality source boundary without retaining invalid bytes."""

    if event.get("event") != "official_source_corrupt_archive":
        raise ValueError(f"{where} has an unsupported corruption event")
    trading_date = date.fromisoformat(str(event.get("trading_date", "")))
    slot = str(event.get("slot", ""))
    suffix = str(event.get("suffix", ""))
    if (slot, suffix) not in SLOT_SPECS:
        raise ValueError(f"{where} has an invalid slot/suffix pair")
    if event.get("download_state") != "corrupt_inner_zip":
        raise ValueError(f"{where} does not classify corrupt_inner_zip evidence")
    if (
        event.get("market_state") != "trading_source_boundary"
        or event.get("calendar_classification") != "official_source_corrupt_archive"
        or event.get("classification_outcome") != "source_boundary"
        or event.get("source_availability_boundary_proven") is not True
    ):
        raise ValueError(f"{where} has an invalid official-source corruption outcome")
    if (
        event.get("exact_payload_match") is not True
        or event.get("raw_persisted") is not False
        or event.get("canonical_archive_path") is not None
    ):
        raise ValueError(
            f"{where} does not prove canonical exclusion and exact equality"
        )
    _utc_timestamp(str(event.get("observed_at_utc", "")))
    if not str(event.get("event_id", "")).strip():
        raise ValueError(f"{where} lacks event_id")
    _reject_secret_fields(event, where)

    reports = event.get("reports_api_evidence")
    static = event.get("static_archive_evidence")
    if not isinstance(reports, Mapping) or not isinstance(static, Mapping):
        raise ValueError(f"{where} lacks reports/static evidence")
    expected_name = f"nsccl.{trading_date:%Y%m%d}.{suffix}.zip"
    expected_url = (
        f"https://nsearchives.nseindia.com/archives/nsccl/span/{expected_name}"
    )
    if static.get("url") != expected_url:
        raise ValueError(f"{where} does not use the exact official static URL")
    _validate_official_url(str(static.get("url", "")))
    if static.get("http_status") != 200 or static.get("zip_magic_ok") is not True:
        raise ValueError(f"{where} lacks a complete HTTP 200 ZIP-magic response")
    if static.get("validation_state") != "corrupt_inner_zip":
        raise ValueError(f"{where} does not prove an invalid corrupt ZIP archive")
    static_hash = str(static.get("body_sha256", ""))
    static_size = _positive_int(static.get("body_size_bytes"))
    rejected = reports.get("rejected_inner")
    if not isinstance(rejected, Mapping):
        raise ValueError(f"{where} lacks rejected-inner reports-API evidence")
    rejected_hash = str(rejected.get("sha256", ""))
    rejected_size = _positive_int(rejected.get("size_bytes"))
    if (
        len(static_hash) != 64
        or static_hash != rejected_hash
        or static_size is None
        or static_size <= 0
        or static_size != rejected_size
    ):
        raise ValueError(f"{where} static and reports-API rejected bytes do not match")

    snapshot_value = str(reports.get("manifest_snapshot_path", ""))
    snapshot_path = Path(snapshot_value)
    if not snapshot_path.is_absolute():
        snapshot_path = manifest_parent / snapshot_path
    snapshot_path = snapshot_path.resolve()
    snapshot_hash = str(reports.get("manifest_snapshot_sha256", ""))
    snapshot_size = _positive_int(reports.get("manifest_snapshot_size_bytes"))
    if (
        not snapshot_path.is_file()
        or len(snapshot_hash) != 64
        or _sha256_file(snapshot_path) != snapshot_hash
        or snapshot_size != snapshot_path.stat().st_size
    ):
        raise ValueError(f"{where} reports-API manifest snapshot failed integrity")
    line_number = _positive_int(reports.get("manifest_event_line"))
    if line_number is None:
        raise ValueError(f"{where} lacks reports-API manifest line provenance")
    source_events = dict(_jsonl(snapshot_path))
    source = source_events.get(line_number)
    if not isinstance(source, Mapping):
        raise ValueError(f"{where} reports-API manifest event is absent")
    if (
        source.get("trading_date") != trading_date.isoformat()
        or source.get("slot") != slot
        or source.get("suffix") != suffix
        or source.get("state") != "corrupt_inner_zip"
        or source.get("terminal") is not False
        or source.get("event_id") != reports.get("manifest_event_id")
        or source.get("run_id") != reports.get("manifest_run_id")
    ):
        raise ValueError(f"{where} reports-API manifest event identity differs")
    source_rejected = source.get("rejected_inner")
    if not isinstance(source_rejected, Mapping) or (
        source_rejected.get("sha256") != rejected_hash
        or _positive_int(source_rejected.get("size_bytes")) != rejected_size
    ):
        raise ValueError(f"{where} reports-API rejected-inner evidence differs")


def _validate_repeated_static_boundary_event(
    event: Mapping[str, Any], manifest_parent: Path, where: str
) -> None:
    """Validate a repeated exact-static source boundary without retaining bytes."""

    if event.get("event") != REPEATED_STATIC_BOUNDARY_EVENT:
        raise ValueError(f"{where} has an unsupported repeated-static event")
    trading_date = date.fromisoformat(str(event.get("trading_date", "")))
    slot = str(event.get("slot", ""))
    suffix = str(event.get("suffix", ""))
    if (slot, suffix) not in SLOT_SPECS:
        raise ValueError(f"{where} has an invalid slot/suffix pair")
    basis = str(event.get("evidence_basis", ""))
    download_state = str(event.get("download_state", ""))
    if basis == REPEATED_STATIC_CORRUPT_BASIS:
        if download_state != "corrupt_inner_zip":
            raise ValueError(f"{where} does not bind corrupt_inner_zip evidence")
        expected_status = 200
        expected_validation = "corrupt_inner_zip"
        expected_zip_magic = True
    elif basis == REPEATED_STATIC_404_BASIS:
        if download_state not in MISSING_STATES:
            raise ValueError(f"{where} does not bind a missing download state")
        expected_status = 404
        expected_validation = "http_404"
        expected_zip_magic = False
    else:
        raise ValueError(f"{where} has an unsupported repeated-static evidence basis")
    if (
        event.get("market_state") != "trading_source_boundary"
        or event.get("calendar_classification")
        != "official_source_repeated_static_boundary"
        or event.get("classification_outcome") != "source_boundary"
        or event.get("source_availability_boundary_proven") is not True
        or event.get("exact_payload_match") is not False
        or event.get("historical_nonpublication_proven") is not False
        or event.get("raw_persisted") is not False
        or event.get("canonical_archive_path") is not None
    ):
        raise ValueError(f"{where} has an invalid repeated-static boundary outcome")
    _utc_timestamp(str(event.get("observed_at_utc", "")))
    if not str(event.get("event_id", "")).strip():
        raise ValueError(f"{where} lacks event_id")
    _reject_secret_fields(event, where)

    expected_name = f"nsccl.{trading_date:%Y%m%d}.{suffix}.zip"
    expected_url = (
        f"https://nsearchives.nseindia.com/archives/nsccl/span/{expected_name}"
    )
    observations = event.get("static_archive_observations")
    if not isinstance(observations, list) or len(observations) != 3:
        raise ValueError(f"{where} requires exactly three static observations")
    fingerprints: set[tuple[int, str, int, str, bool]] = set()
    for attempt, observation in enumerate(observations, start=1):
        if not isinstance(observation, Mapping):
            raise ValueError(f"{where} has a malformed static observation")
        if (
            observation.get("attempt") != attempt
            or observation.get("url") != expected_url
        ):
            raise ValueError(f"{where} static observation identity differs")
        _validate_official_url(str(observation.get("url", "")))
        _utc_timestamp(str(observation.get("observed_at_utc", "")))
        status = observation.get("http_status")
        digest = str(observation.get("body_sha256", ""))
        size = _positive_int(observation.get("body_size_bytes"))
        validation_state = str(observation.get("validation_state", ""))
        zip_magic_ok = observation.get("zip_magic_ok")
        if (
            status != expected_status
            or not _is_sha256(digest)
            or size is None
            or validation_state != expected_validation
            or zip_magic_ok is not expected_zip_magic
            or observation.get("body_retained") is not False
        ):
            raise ValueError(f"{where} static observation does not prove its basis")
        if basis == REPEATED_STATIC_CORRUPT_BASIS and size <= 0:
            raise ValueError(f"{where} corrupt static observation has no body")
        fingerprints.add((status, digest, size, validation_state, bool(zip_magic_ok)))
    if len(fingerprints) != 1:
        raise ValueError(f"{where} repeated static observations are not identical")

    reports = event.get("reports_api_evidence")
    if not isinstance(reports, Mapping):
        raise ValueError(f"{where} lacks reports-API evidence")
    source = _validate_snapshot_event_reference(reports, manifest_parent, where)
    if (
        source.get("trading_date") != trading_date.isoformat()
        or source.get("slot") != slot
        or source.get("suffix") != suffix
        or source.get("state") != download_state
        or source.get("event_id") != reports.get("manifest_event_id")
        or source.get("run_id") != reports.get("manifest_run_id")
    ):
        raise ValueError(f"{where} reports-API manifest event identity differs")
    terminal = source.get("terminal")
    if basis == REPEATED_STATIC_CORRUPT_BASIS and terminal is not False:
        raise ValueError(f"{where} corrupt source event is not nonterminal")
    if basis == REPEATED_STATIC_404_BASIS and terminal is not True:
        raise ValueError(f"{where} missing source event is not terminal")


def _validate_snapshot_event_reference(
    reports: Mapping[str, Any], manifest_parent: Path, where: str
) -> Mapping[str, Any]:
    snapshot_value = str(reports.get("manifest_snapshot_path", ""))
    snapshot_path = Path(snapshot_value)
    if not snapshot_path.is_absolute():
        snapshot_path = manifest_parent / snapshot_path
    snapshot_path = snapshot_path.resolve()
    snapshot_hash = str(reports.get("manifest_snapshot_sha256", ""))
    snapshot_size = _positive_int(reports.get("manifest_snapshot_size_bytes"))
    if (
        not snapshot_path.is_file()
        or not _is_sha256(snapshot_hash)
        or _sha256_file(snapshot_path) != snapshot_hash
        or snapshot_size != snapshot_path.stat().st_size
    ):
        raise ValueError(f"{where} reports-API manifest snapshot failed integrity")
    line_number = _positive_int(reports.get("manifest_event_line"))
    if line_number is None or line_number < 1:
        raise ValueError(f"{where} lacks reports-API manifest line provenance")
    source = dict(_jsonl(snapshot_path)).get(line_number)
    if not isinstance(source, Mapping):
        raise ValueError(f"{where} reports-API manifest event is absent")
    return source


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )


def _reject_secret_fields(value: Any, where: str) -> None:
    blocked = {
        "authorization",
        "cookie",
        "set_cookie",
        "token",
        "secret",
        "password",
        "api_key",
    }
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = str(key).lower().replace("-", "_")
            if normalized in blocked or normalized.endswith("_token"):
                raise ValueError(
                    f"{where} contains forbidden secret-bearing field {key!r}"
                )
            _reject_secret_fields(child, where)
    elif isinstance(value, list):
        for child in value:
            _reject_secret_fields(child, where)


def _positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _latest_download_events(path: Path) -> dict[tuple[str, str], Mapping[str, Any]]:
    latest: dict[tuple[str, str], Mapping[str, Any]] = {}
    for _, event in _jsonl(path):
        trading_date = str(event.get("trading_date", ""))
        slot = str(event.get("slot", ""))
        if trading_date and slot:
            latest[(trading_date, slot)] = event
    return latest


def _jsonl(path: Path) -> Iterable[tuple[int, Mapping[str, Any]]]:
    if not path.is_file():
        return ()
    result: list[tuple[int, Mapping[str, Any]]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"invalid JSON at {path}:{line_number}: {exc}"
                ) from exc
            if not isinstance(event, dict):
                raise ValueError(
                    f"manifest event is not an object at {path}:{line_number}"
                )
            result.append((line_number, event))
    return result


def _read_object(path: Path) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid availability import JSON at {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("availability import root must be an object")
    return payload


def _validate_official_url(url: str) -> None:
    parsed = urlparse(url)
    if (
        parsed.scheme != "https"
        or (parsed.hostname or "").lower() not in OFFICIAL_HOSTS
    ):
        raise ValueError(
            f"source URL is not an approved official NSE/NSE Clearing HTTPS host: {url!r}"
        )


def _utc_timestamp(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid fetched_at_utc timestamp {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"fetched_at_utc must be timezone-aware: {value!r}")
    return parsed.astimezone(UTC).isoformat()


def _publish_immutable(source: Path, target: Path, digest: str) -> None:
    if target.exists():
        if not target.is_file() or _sha256_file(target) != digest:
            raise ValueError(
                f"existing provenance artifact conflicts with {digest}: {target}"
            )
        return
    partial = target.with_name(f".{target.name}.{uuid.uuid4().hex}.partial")
    try:
        shutil.copyfile(source, partial)
        if _sha256_file(partial) != digest:
            raise ValueError(f"source changed while retaining provenance: {source}")
        with partial.open("r+b") as handle:
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(partial, target)
        except FileExistsError:
            if _sha256_file(target) != digest:
                raise ValueError(
                    f"concurrent provenance artifact conflicts with {digest}: {target}"
                )
    finally:
        partial.unlink(missing_ok=True)


def _append_jsonl(path: Path, events: Iterable[Mapping[str, Any]]) -> None:
    append_jsonl_records(path, events)


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
