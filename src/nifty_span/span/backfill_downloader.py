"""Resumable, bounded and integrity-first NSE SPAN archive downloader.

The NSE reports endpoint returns an outer ZIP whose members are the immutable
per-slot ZIP files consumed by the SPAN extractor.  This module deliberately
does not extract either ZIP layer: it validates the response in memory and
atomically persists the exact inner ZIP bytes.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
from hashlib import sha256
from pathlib import Path, PurePosixPath
from typing import Any, Protocol
import asyncio
import inspect
import io
import json
import os
import random
import re
import stat
import uuid
import zipfile

from .downloader import API_URL, ARCHIVES_PAYLOAD, HEADERS, HOME_URL
from .durable_jsonl import JsonlAppendRetryPolicy, append_jsonl_record_async


SLOT_SPECS = (
    ("BOD", "i1"),
    ("ID1", "i2"),
    ("ID2", "i3"),
    ("ID3", "i4"),
    ("ID4", "i5"),
    ("EOD", "s"),
)
SLOT_BY_SUFFIX = {suffix: slot for slot, suffix in SLOT_SPECS}
MAX_SAFE_CONCURRENCY = 8
MISSING_STATES = frozenset({"not_returned_http_404", "slot_not_returned"})
DOWNLOADED_STATES = frozenset({"downloaded", "downloaded_existing"})
REPAIR_ORDERS = frozenset({"chronological", "unseen-first"})
DETERMINISTIC_CORRUPT_STATES = frozenset(
    {"corrupt_inner_zip", "bundle_validation_blocked"}
)
TRANSPORT_ERROR_STATES = frozenset({"transport_error", "retrying_transport_error"})
DOWNLOAD_MANIFEST_APPEND_RETRY_POLICY = JsonlAppendRetryPolicy(
    max_attempts=95,
    initial_delay_seconds=0.1,
    max_delay_seconds=2.0,
    jitter_ratio=0.2,
)
ZIP_CONTENT_TYPES = frozenset(
    {
        "application/zip",
        "application/x-zip-compressed",
        "application/octet-stream",
        "binary/octet-stream",
    }
)
RETRYABLE_CONTENT_STATES = frozenset(
    {
        "corrupt_inner_zip",
        "corrupt_outer_zip",
        "invalid_content_type",
        "invalid_zip_magic",
        "missing_spn",
        "missing_spn_member",
    }
)
_OUTER_RE = re.compile(r"^nsccl\.(\d{8})\.(i[1-5]|s)\.zip$", re.IGNORECASE)
_ZIP_MAGICS = (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")


class ResponseLike(Protocol):
    status_code: int
    content: bytes
    headers: Mapping[str, Any]


class ClientLike(Protocol):
    async def get(self, url: str, **kwargs: Any) -> ResponseLike: ...


ClientFactory = Callable[[int], ClientLike | Awaitable[ClientLike]]
SleepFn = Callable[[float], Awaitable[None]]


@dataclass(frozen=True)
class BackfillConfig:
    max_concurrent: int = 4
    queue_size: int | None = None
    max_attempts: int = 5
    retry_incomplete_passes: int = 1
    timeout_seconds: float = 180.0
    warm_timeout_seconds: float = 20.0
    session_refresh_requests: int = 100
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 60.0
    jitter_seconds: float = 0.5
    adaptive_restore_successes: int = 20
    reprobe_missing: bool = False
    repair_order: str = "chronological"
    unsafe_allow_high_concurrency: bool = False
    archive_limits: ArchiveResourceLimits = field(
        default_factory=lambda: ArchiveResourceLimits()
    )

    def validated(self) -> BackfillConfig:
        if self.max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1")
        if (
            self.max_concurrent > MAX_SAFE_CONCURRENCY
            and not self.unsafe_allow_high_concurrency
        ):
            raise ValueError(
                f"max_concurrent must be <= {MAX_SAFE_CONCURRENCY} for normal NSE operation; "
                "set unsafe_allow_high_concurrency=True only for an explicitly controlled benchmark"
            )
        if self.queue_size is not None and self.queue_size < 1:
            raise ValueError("queue_size must be >= 1")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.retry_incomplete_passes < 0:
            raise ValueError("retry_incomplete_passes must be >= 0")
        if self.session_refresh_requests < 1:
            raise ValueError("session_refresh_requests must be >= 1")
        if self.adaptive_restore_successes < 1:
            raise ValueError("adaptive_restore_successes must be >= 1")
        if self.repair_order not in REPAIR_ORDERS:
            raise ValueError(
                f"repair_order must be one of {sorted(REPAIR_ORDERS)}, got {self.repair_order!r}"
            )
        self.archive_limits.validated()
        return self


@dataclass(frozen=True)
class ArchiveResourceLimits:
    """Conservative bounds checked before ZIP members are decompressed."""

    max_response_bytes: int = 512 * 1024 * 1024
    max_outer_members: int = 16
    max_outer_member_compressed_bytes: int = 256 * 1024 * 1024
    max_outer_member_uncompressed_bytes: int = 256 * 1024 * 1024
    max_outer_total_compressed_bytes: int = 1024 * 1024 * 1024
    max_outer_total_uncompressed_bytes: int = 1024 * 1024 * 1024
    max_inner_archive_bytes: int = 256 * 1024 * 1024
    max_inner_members: int = 4
    max_inner_member_compressed_bytes: int = 256 * 1024 * 1024
    max_inner_member_uncompressed_bytes: int = 512 * 1024 * 1024
    max_inner_total_compressed_bytes: int = 256 * 1024 * 1024
    max_inner_total_uncompressed_bytes: int = 512 * 1024 * 1024
    max_compression_ratio: float = 100.0

    def validated(self) -> ArchiveResourceLimits:
        for name, value in asdict(self).items():
            if value <= 0:
                raise ValueError(f"archive limit {name} must be > 0")
        return self


@dataclass(frozen=True)
class BackfillCellResult:
    trading_date: str
    slot: str
    suffix: str
    state: str
    terminal: bool
    attempt: int
    path: str | None = None
    sha256: str | None = None
    size_bytes: int | None = None
    error: str | None = None


@dataclass(frozen=True)
class SpanBackfillReport:
    start_date: str
    end_date: str
    requested_dates: int
    total_cells: int
    network_calls: int
    skipped_completed_dates: int
    completed_dates: int
    incomplete_dates: int
    downloaded_slots: int
    missing_slots: int
    failed_slots: int
    throttle_events: int
    initial_concurrency: int
    final_concurrency: int
    minimum_concurrency: int
    max_queue_depth: int
    manifest_path: str
    cells: tuple[BackfillCellResult, ...]
    configured_retry_passes: int = 0
    executed_retry_passes: int = 0
    retried_dates: int = 0
    retry_network_calls: int = 0
    repair_order: str = "chronological"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["cells"] = [asdict(cell) for cell in self.cells]
        return payload


@dataclass(frozen=True)
class _InnerArchive:
    suffix: str
    slot: str
    filename: str
    content: bytes
    sha256: str
    size_bytes: int
    outer_crc32: str
    spn_name: str
    spn_crc32: str
    spn_compressed_size: int
    spn_uncompressed_size: int


@dataclass(frozen=True)
class _InvalidInnerArchive:
    """Audit evidence for one named outer member that is not safe to persist."""

    suffix: str
    slot: str
    filename: str
    state: str
    error: str
    sha256: str
    size_bytes: int
    outer_crc32: str
    outer_compressed_size: int
    outer_uncompressed_size: int


@dataclass(frozen=True)
class _OuterArchiveValidation:
    """Structurally valid outer bundle, split by independently validated slot."""

    valid: tuple[_InnerArchive, ...]
    invalid: tuple[_InvalidInnerArchive, ...]
    returned_suffixes: tuple[str, ...]


class _ArchiveError(ValueError):
    def __init__(self, state: str, message: str, *, suffix: str | None = None) -> None:
        super().__init__(message)
        self.state = state
        self.suffix = suffix


class _Manifest:
    def __init__(self, path: Path, run_id: str) -> None:
        self.path = path
        self.run_id = run_id
        self.latest: dict[tuple[str, str], dict[str, Any]] = {}
        self.recovered_tail_path: Path | None = None
        self._lock = asyncio.Lock()
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        raw = self.path.read_bytes()
        lines = raw.splitlines(keepends=True)
        valid_prefix_bytes = 0
        for line_number, line in enumerate(lines, start=1):
            try:
                text = line.decode("utf-8").strip()
                event = json.loads(text) if text else None
                if event is not None and not isinstance(event, dict):
                    raise ValueError("manifest event must be a JSON object")
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                is_final_unterminated = line_number == len(lines) and not raw.endswith(
                    (b"\n", b"\r")
                )
                if is_final_unterminated:
                    self._quarantine_and_remove_truncated_tail(raw[valid_prefix_bytes:])
                    return
                raise ValueError(
                    f"invalid manifest JSON at {self.path}:{line_number}: {exc}"
                ) from exc
            valid_prefix_bytes += len(line)
            if event is None:
                continue
            trading_date = str(event.get("trading_date", ""))
            slot = str(event.get("slot", ""))
            if trading_date and slot:
                self.latest[(trading_date, slot)] = event

    def _quarantine_and_remove_truncated_tail(self, corrupt_tail: bytes) -> None:
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        quarantine = self.path.with_name(f"{self.path.name}.corrupt-tail.{stamp}.bin")
        with quarantine.open("xb") as handle:
            handle.write(corrupt_tail)
            handle.flush()
            os.fsync(handle.fileno())
        prefix_size = self.path.stat().st_size - len(corrupt_tail)
        prefix = self.path.read_bytes()[:prefix_size]
        partial = self.path.with_name(
            f".{self.path.name}.{uuid.uuid4().hex}.recovery.partial"
        )
        try:
            with partial.open("xb") as handle:
                handle.write(prefix)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(partial, self.path)
            _fsync_directory(self.path.parent)
        finally:
            partial.unlink(missing_ok=True)
        self.recovered_tail_path = quarantine

    async def append(self, event: dict[str, Any]) -> dict[str, Any]:
        record = {
            "schema_version": 1,
            "event_id": uuid.uuid4().hex,
            "run_id": self.run_id,
            "observed_at_utc": _utc_now(),
            **event,
        }
        async with self._lock:
            await append_jsonl_record_async(
                self.path,
                record,
                policy=DOWNLOAD_MANIFEST_APPEND_RETRY_POLICY,
            )
            key = (str(record["trading_date"]), str(record["slot"]))
            self.latest[key] = record
        return record


class _AdaptiveLimiter:
    def __init__(self, configured: int, restore_after: int) -> None:
        self.configured = configured
        self.limit = configured
        self.minimum = configured
        self.active = 0
        self.throttle_events = 0
        self._successes = 0
        self._restore_after = restore_after
        self._condition = asyncio.Condition()

    @asynccontextmanager
    async def permit(self):
        async with self._condition:
            await self._condition.wait_for(lambda: self.active < self.limit)
            self.active += 1
        try:
            yield
        finally:
            async with self._condition:
                self.active -= 1
                self._condition.notify_all()

    async def throttled(self) -> None:
        async with self._condition:
            self.throttle_events += 1
            self._successes = 0
            self.limit = max(1, self.limit - 1)
            self.minimum = min(self.minimum, self.limit)
            self._condition.notify_all()

    async def successful(self) -> None:
        async with self._condition:
            self._successes += 1
            if self.limit < self.configured and self._successes >= self._restore_after:
                self.limit += 1
                self._successes = 0
                self._condition.notify_all()


class _WorkerClient:
    def __init__(
        self,
        worker_id: int,
        factory: ClientFactory,
        refresh_requests: int,
        warm_timeout: float,
    ) -> None:
        self.worker_id = worker_id
        self.factory = factory
        self.refresh_requests = refresh_requests
        self.warm_timeout = warm_timeout
        self.client: ClientLike | None = None
        self.requests = 0
        self._entered = False

    async def request(self, **kwargs: Any) -> ResponseLike:
        if self.client is None or self.requests >= self.refresh_requests:
            await self.refresh()
        assert self.client is not None
        self.requests += 1
        return await self.client.get(API_URL, **kwargs)

    async def refresh(self) -> None:
        await self.close()
        candidate = self.factory(self.worker_id)
        if inspect.isawaitable(candidate):
            candidate = await candidate
        enter = getattr(candidate, "__aenter__", None)
        if callable(enter):
            candidate = await enter()
            self._entered = True
        self.client = candidate
        self.requests = 0
        try:
            await self.client.get(HOME_URL, timeout=self.warm_timeout)
        except BaseException:
            await self.close()
            raise

    async def close(self) -> None:
        if self.client is None:
            return
        client, entered = self.client, self._entered
        self.client = None
        self._entered = False
        if entered:
            exit_fn = getattr(client, "__aexit__", None)
            if callable(exit_fn):
                await exit_fn(None, None, None)
                return
        close_fn = getattr(client, "aclose", None) or getattr(client, "close", None)
        if callable(close_fn):
            result = close_fn()
            if inspect.isawaitable(result):
                await result


@dataclass
class _Metrics:
    network_calls: int = 0
    skipped_completed_dates: int = 0
    max_queue_depth: int = 0


def download_span_backfill(
    *,
    start_date: date,
    end_date: date,
    output_root: str | Path,
    manifest_path: str | Path | None = None,
    config: BackfillConfig | None = None,
    client_factory: ClientFactory | None = None,
    sleep: SleepFn = asyncio.sleep,
    random_fn: Callable[[], float] = random.random,
) -> SpanBackfillReport:
    """Run the async backfill from synchronous code."""

    return asyncio.run(
        download_span_backfill_async(
            start_date=start_date,
            end_date=end_date,
            output_root=output_root,
            manifest_path=manifest_path,
            config=config,
            client_factory=client_factory,
            sleep=sleep,
            random_fn=random_fn,
        )
    )


async def download_span_backfill_async(
    *,
    start_date: date,
    end_date: date,
    output_root: str | Path,
    manifest_path: str | Path | None = None,
    config: BackfillConfig | None = None,
    client_factory: ClientFactory | None = None,
    sleep: SleepFn = asyncio.sleep,
    random_fn: Callable[[], float] = random.random,
) -> SpanBackfillReport:
    """Download every date using a bounded producer-consumer queue."""

    if start_date > end_date:
        raise ValueError(f"start date {start_date} must be <= end date {end_date}")
    effective = (config or BackfillConfig()).validated()
    root = Path(output_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    manifest_file = (
        Path(manifest_path).resolve()
        if manifest_path is not None
        else root / "span_backfill_manifest.jsonl"
    )
    factory = client_factory or _curl_client_factory
    run_id = uuid.uuid4().hex
    manifest = _Manifest(manifest_file, run_id)
    limiter = _AdaptiveLimiter(
        effective.max_concurrent, effective.adaptive_restore_successes
    )
    metrics = _Metrics()
    queue_size = effective.queue_size or max(1, effective.max_concurrent * 2)
    executed_retry_passes = 0
    retried_dates: set[date] = set()
    retry_network_calls = 0

    async def run_pass(days: Sequence[date], pass_number: int) -> None:
        queue: asyncio.Queue[date | None] = asyncio.Queue(maxsize=queue_size)

        async def producer() -> None:
            for day in days:
                await queue.put(day)
                metrics.max_queue_depth = max(metrics.max_queue_depth, queue.qsize())
            for _ in range(effective.max_concurrent):
                await queue.put(None)

        async def worker(worker_id: int) -> None:
            client = _WorkerClient(
                worker_id,
                factory,
                effective.session_refresh_requests,
                effective.warm_timeout_seconds,
            )
            try:
                while True:
                    day = await queue.get()
                    try:
                        if day is None:
                            return
                        await _process_day(
                            day=day,
                            root=root,
                            manifest=manifest,
                            client=client,
                            limiter=limiter,
                            metrics=metrics,
                            config=effective,
                            sleep=sleep,
                            random_fn=random_fn,
                        )
                    finally:
                        queue.task_done()
            finally:
                await client.close()

        producer_task = asyncio.create_task(
            producer(), name=f"span-date-producer-{pass_number}"
        )
        workers = [
            asyncio.create_task(
                worker(index), name=f"span-worker-{pass_number}-{index}"
            )
            for index in range(effective.max_concurrent)
        ]
        try:
            await asyncio.gather(producer_task, *workers)
        except BaseException:
            producer_task.cancel()
            for task in workers:
                task.cancel()
            await asyncio.gather(producer_task, *workers, return_exceptions=True)
            raise

    all_dates = tuple(
        start_date + timedelta(days=offset)
        for offset in range((end_date - start_date).days + 1)
    )
    initial_dates = (
        _unseen_first_manifest_dates(
            manifest,
            start_date,
            end_date,
            reprobe_missing=effective.reprobe_missing,
            include_completed=True,
        )
        if effective.repair_order == "unseen-first"
        else all_dates
    )
    await run_pass(initial_dates, 0)
    for pass_number in range(1, effective.retry_incomplete_passes + 1):
        if effective.repair_order == "unseen-first":
            incomplete_dates = _unseen_first_manifest_dates(
                manifest,
                start_date,
                end_date,
                reprobe_missing=effective.reprobe_missing,
                include_completed=False,
            )
        else:
            incomplete_dates = _incomplete_manifest_dates(
                manifest,
                start_date,
                end_date,
                reprobe_missing=effective.reprobe_missing,
            )
        if not incomplete_dates:
            break
        calls_before = metrics.network_calls
        await run_pass(incomplete_dates, pass_number)
        executed_retry_passes += 1
        retried_dates.update(incomplete_dates)
        retry_network_calls += metrics.network_calls - calls_before

    cells = _report_cells(manifest, start_date, end_date)
    date_states: dict[str, list[BackfillCellResult]] = {}
    for cell in cells:
        date_states.setdefault(cell.trading_date, []).append(cell)
    completed_dates = sum(
        len(items) == len(SLOT_SPECS) and all(item.terminal for item in items)
        for items in date_states.values()
    )
    downloaded = sum(cell.state in DOWNLOADED_STATES for cell in cells)
    missing = sum(cell.state in MISSING_STATES for cell in cells)
    failed = len(cells) - downloaded - missing
    requested_dates = (end_date - start_date).days + 1
    return SpanBackfillReport(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        requested_dates=requested_dates,
        total_cells=requested_dates * len(SLOT_SPECS),
        network_calls=metrics.network_calls,
        skipped_completed_dates=metrics.skipped_completed_dates,
        completed_dates=completed_dates,
        incomplete_dates=requested_dates - completed_dates,
        downloaded_slots=downloaded,
        missing_slots=missing,
        failed_slots=failed,
        throttle_events=limiter.throttle_events,
        initial_concurrency=effective.max_concurrent,
        final_concurrency=limiter.limit,
        minimum_concurrency=limiter.minimum,
        max_queue_depth=metrics.max_queue_depth,
        manifest_path=str(manifest_file),
        cells=cells,
        configured_retry_passes=effective.retry_incomplete_passes,
        executed_retry_passes=executed_retry_passes,
        retried_dates=len(retried_dates),
        retry_network_calls=retry_network_calls,
        repair_order=effective.repair_order,
    )


def _unseen_first_manifest_dates(
    manifest: _Manifest,
    start_date: date,
    end_date: date,
    *,
    reprobe_missing: bool,
    include_completed: bool,
) -> tuple[date, ...]:
    """Order resumable work without re-requesting deterministic corrupt bundles.

    Never-observed or partially observed dates lead, followed by the bounded
    transport queue, other generic nonterminal dates, and finally terminal
    dates whose immutable local files still need the normal resume validation.
    """

    unseen: list[date] = []
    transport: list[date] = []
    generic: list[date] = []
    completed: list[date] = []
    day = start_date
    while day <= end_date:
        trading_date = day.isoformat()
        events = tuple(
            manifest.latest.get((trading_date, slot)) for slot, _ in SLOT_SPECS
        )
        states = {str(event.get("state", "")) for event in events if event is not None}
        if states & DETERMINISTIC_CORRUPT_STATES:
            day += timedelta(days=1)
            continue
        if any(event is None for event in events):
            unseen.append(day)
        elif states & TRANSPORT_ERROR_STATES:
            transport.append(day)
        elif any(
            not bool(event.get("terminal")) for event in events if event is not None
        ):
            generic.append(day)
        elif reprobe_missing and states & MISSING_STATES:
            generic.append(day)
        elif include_completed:
            completed.append(day)
        day += timedelta(days=1)
    return tuple((*unseen, *transport, *generic, *completed))


def _incomplete_manifest_dates(
    manifest: _Manifest,
    start_date: date,
    end_date: date,
    *,
    reprobe_missing: bool,
) -> tuple[date, ...]:
    incomplete: list[date] = []
    day = start_date
    while day <= end_date:
        for slot, _suffix in SLOT_SPECS:
            event = manifest.latest.get((day.isoformat(), slot))
            if event is None or not bool(event.get("terminal")):
                incomplete.append(day)
                break
            if reprobe_missing and str(event.get("state", "")) in MISSING_STATES:
                incomplete.append(day)
                break
        day += timedelta(days=1)
    return tuple(incomplete)


async def _process_day(
    *,
    day: date,
    root: Path,
    manifest: _Manifest,
    client: _WorkerClient,
    limiter: _AdaptiveLimiter,
    metrics: _Metrics,
    config: BackfillConfig,
    sleep: SleepFn,
    random_fn: Callable[[], float],
) -> None:
    trading_date = day.isoformat()
    valid_downloaded: set[str] = set()
    complete = True
    for slot, suffix in SLOT_SPECS:
        current = manifest.latest.get((trading_date, slot))
        if current is None:
            complete = False
            continue
        state = str(current.get("state", ""))
        if state in DOWNLOADED_STATES:
            if _download_event_matches_file(
                current,
                day,
                suffix,
                root,
                config.archive_limits,
            ):
                valid_downloaded.add(suffix)
            else:
                complete = False
                await manifest.append(
                    _cell_event(
                        day,
                        slot,
                        suffix,
                        "local_file_invalid",
                        terminal=False,
                        attempt=int(current.get("attempt", 0) or 0),
                        error="manifest hash/integrity does not match the local immutable archive",
                    )
                )
        elif bool(current.get("terminal")):
            if config.reprobe_missing and state in MISSING_STATES:
                complete = False
            continue
        else:
            complete = False
    if complete:
        metrics.skipped_completed_dates += 1
        return

    for attempt in range(1, config.max_attempts + 1):
        try:
            async with limiter.permit():
                metrics.network_calls += 1
                response = await client.request(
                    params={
                        "archives": ARCHIVES_PAYLOAD,
                        "date": day.strftime("%d-%b-%Y"),
                        "type": "Archives",
                    },
                    timeout=config.timeout_seconds,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - transport implementations vary.
            transient = _is_transient_exception(exc)
            if transient and attempt < config.max_attempts:
                await _append_all_unresolved(
                    manifest,
                    day,
                    valid_downloaded,
                    state="retrying_transport_error",
                    terminal=False,
                    attempt=attempt,
                    error=f"{type(exc).__name__}: {exc}",
                )
                await sleep(_backoff(config, attempt, None, random_fn))
                continue
            await _append_all_unresolved(
                manifest,
                day,
                valid_downloaded,
                state="transport_error",
                terminal=False,
                attempt=attempt,
                error=f"{type(exc).__name__}: {exc}",
            )
            return

        status = int(getattr(response, "status_code", 0) or 0)
        headers = _headers(response)
        if status == 404:
            await _append_all_unresolved(
                manifest,
                day,
                valid_downloaded,
                state="not_returned_http_404",
                terminal=True,
                attempt=attempt,
                http_status=status,
            )
            await limiter.successful()
            return
        if status in {403, 429} or 500 <= status <= 599:
            if status in {403, 429}:
                await limiter.throttled()
            if attempt < config.max_attempts:
                await _append_all_unresolved(
                    manifest,
                    day,
                    valid_downloaded,
                    state=f"retrying_http_{status}",
                    terminal=False,
                    attempt=attempt,
                    http_status=status,
                )
                retry_after = _retry_after_seconds(headers.get("retry-after"))
                await sleep(_backoff(config, attempt, retry_after, random_fn))
                continue
            await _append_all_unresolved(
                manifest,
                day,
                valid_downloaded,
                state=f"http_{status}_exhausted",
                terminal=False,
                attempt=attempt,
                http_status=status,
            )
            return
        if status != 200:
            await _append_all_unresolved(
                manifest,
                day,
                valid_downloaded,
                state="http_error",
                terminal=False,
                attempt=attempt,
                error=f"unexpected HTTP status {status}",
                http_status=status,
            )
            return

        content = bytes(getattr(response, "content", b"") or b"")
        content_type = headers.get("content-type", "").split(";", 1)[0].strip().lower()
        declared_length = _positive_int(headers.get("content-length"))
        if len(content) > config.archive_limits.max_response_bytes or (
            declared_length is not None
            and declared_length > config.archive_limits.max_response_bytes
        ):
            await _append_all_unresolved(
                manifest,
                day,
                valid_downloaded,
                state="response_resource_limit_exceeded",
                terminal=False,
                attempt=attempt,
                error=(
                    f"response body/declared length {len(content)}/{declared_length} exceeds "
                    f"{config.archive_limits.max_response_bytes} bytes"
                ),
                http_status=status,
                response=_response_metadata(content, content_type),
            )
            return
        if content_type and content_type not in ZIP_CONTENT_TYPES:
            response_metadata = _response_metadata(content, content_type)
            state = (
                "retrying_invalid_content_type"
                if attempt < config.max_attempts
                else "invalid_content_type"
            )
            await _append_all_unresolved(
                manifest,
                day,
                valid_downloaded,
                state=state,
                terminal=False,
                attempt=attempt,
                error=f"expected ZIP content type, received {content_type!r}",
                http_status=status,
                response=response_metadata,
            )
            if attempt < config.max_attempts:
                await sleep(_backoff(config, attempt, None, random_fn))
                continue
            return
        if not content.startswith(_ZIP_MAGICS):
            response_metadata = _response_metadata(content, content_type)
            state = (
                "retrying_invalid_zip_magic"
                if attempt < config.max_attempts
                else "invalid_zip_magic"
            )
            await _append_all_unresolved(
                manifest,
                day,
                valid_downloaded,
                state=state,
                terminal=False,
                attempt=attempt,
                error="HTTP 200 response does not start with ZIP magic",
                http_status=status,
                response=response_metadata,
            )
            if attempt < config.max_attempts:
                await sleep(_backoff(config, attempt, None, random_fn))
                continue
            return
        response_metadata = _response_metadata(content, content_type)
        try:
            validation = _validate_outer_archive(day, content, config.archive_limits)
        except _ArchiveError as exc:
            retryable = exc.state in RETRYABLE_CONTENT_STATES
            state = (
                f"retrying_{exc.state}"
                if retryable and attempt < config.max_attempts
                else exc.state
            )
            await _append_all_unresolved(
                manifest,
                day,
                valid_downloaded,
                state=state,
                terminal=False,
                attempt=attempt,
                error=str(exc),
                http_status=status,
                only_suffix=exc.suffix,
                response=response_metadata,
            )
            if retryable and attempt < config.max_attempts:
                await sleep(_backoff(config, attempt, None, random_fn))
                continue
            return

        returned = set(validation.returned_suffixes)
        archive_by_suffix = {archive.suffix: archive for archive in validation.valid}
        invalid_by_suffix = {archive.suffix: archive for archive in validation.invalid}
        retry_invalid = False
        for slot, suffix in SLOT_SPECS:
            invalid = invalid_by_suffix.get(suffix)
            if invalid is not None:
                retryable = invalid.state in RETRYABLE_CONTENT_STATES
                retry_invalid = retry_invalid or (
                    retryable and attempt < config.max_attempts
                )
                state = (
                    f"retrying_{invalid.state}"
                    if retryable and attempt < config.max_attempts
                    else invalid.state
                )
                await manifest.append(
                    _cell_event(
                        day,
                        slot,
                        suffix,
                        state,
                        terminal=False,
                        attempt=attempt,
                        error=invalid.error,
                        http_status=status,
                        outer_member={
                            "name": invalid.filename,
                            "crc32": invalid.outer_crc32,
                            "compressed_size": invalid.outer_compressed_size,
                            "uncompressed_size": invalid.outer_uncompressed_size,
                        },
                        rejected_inner={
                            "sha256": invalid.sha256,
                            "size_bytes": invalid.size_bytes,
                        },
                        returned_suffixes=sorted(returned),
                        response=response_metadata,
                    )
                )
                continue
            archive = archive_by_suffix.get(suffix)
            if archive is None:
                current = manifest.latest.get((trading_date, slot))
                already_missing = (
                    bool(current and current.get("terminal"))
                    and str(current.get("state", "")) in MISSING_STATES
                )
                if suffix not in valid_downloaded and not already_missing:
                    await manifest.append(
                        _cell_event(
                            day,
                            slot,
                            suffix,
                            "slot_not_returned",
                            terminal=True,
                            attempt=attempt,
                            http_status=status,
                            returned_suffixes=sorted(returned),
                            response=response_metadata,
                        )
                    )
                continue
            if suffix in valid_downloaded:
                continue
            try:
                saved_state, path = _persist_inner_archive(
                    root,
                    day,
                    archive,
                    config.archive_limits,
                )
            except _ArchiveError as exc:
                await manifest.append(
                    _cell_event(
                        day,
                        slot,
                        suffix,
                        exc.state,
                        terminal=False,
                        attempt=attempt,
                        error=str(exc),
                        http_status=status,
                        sha256=archive.sha256,
                        size_bytes=archive.size_bytes,
                        response=response_metadata,
                    )
                )
                continue
            valid_downloaded.add(suffix)
            await manifest.append(
                _cell_event(
                    day,
                    slot,
                    suffix,
                    saved_state,
                    terminal=True,
                    attempt=attempt,
                    http_status=status,
                    path=str(path.relative_to(root)),
                    sha256=archive.sha256,
                    size_bytes=archive.size_bytes,
                    outer_member={
                        "name": archive.filename,
                        "crc32": archive.outer_crc32,
                    },
                    inner_spn={
                        "name": archive.spn_name,
                        "crc32": archive.spn_crc32,
                        "compressed_size": archive.spn_compressed_size,
                        "uncompressed_size": archive.spn_uncompressed_size,
                    },
                    zip_crc_ok=True,
                    members=[archive.spn_name],
                    returned_suffixes=sorted(returned),
                    response=response_metadata,
                )
            )
        if retry_invalid:
            await sleep(_backoff(config, attempt, None, random_fn))
            continue
        await limiter.successful()
        return


def _validate_outer_archive(
    day: date,
    content: bytes,
    limits: ArchiveResourceLimits | None = None,
) -> _OuterArchiveValidation:
    effective_limits = (limits or ArchiveResourceLimits()).validated()
    if len(content) > effective_limits.max_response_bytes:
        raise _ArchiveError(
            "response_resource_limit_exceeded",
            f"outer response has {len(content)} bytes, limit is {effective_limits.max_response_bytes}",
        )
    try:
        outer = zipfile.ZipFile(io.BytesIO(content))
    except (zipfile.BadZipFile, OSError) as exc:
        raise _ArchiveError(
            "corrupt_outer_zip", f"outer archive cannot be opened: {exc}"
        ) from exc
    with outer:
        infos = outer.infolist()
        if not infos:
            raise _ArchiveError("corrupt_outer_zip", "outer archive has no members")
        _validate_zip_resources(infos, effective_limits, layer="outer")
        bad_member = outer.testzip()
        if bad_member is not None:
            raise _ArchiveError(
                "corrupt_outer_zip", f"outer CRC failure in {bad_member!r}"
            )
        parsed: dict[str, zipfile.ZipInfo] = {}
        expected_date = day.strftime("%Y%m%d")
        for info in infos:
            name = info.filename
            if info.is_dir() or not _safe_basename(name):
                raise _ArchiveError(
                    "unsafe_outer_member", f"unsafe outer member name {name!r}"
                )
            match = _OUTER_RE.fullmatch(name)
            if match is None:
                raise _ArchiveError(
                    "filename_mismatch", f"unexpected outer member name {name!r}"
                )
            member_date, suffix = match.group(1), match.group(2).lower()
            if member_date != expected_date:
                raise _ArchiveError(
                    "filename_mismatch",
                    f"outer member {name!r} is for {member_date}, expected {expected_date}",
                )
            if suffix in parsed:
                raise _ArchiveError(
                    "duplicate_slot", f"duplicate outer member for suffix {suffix}"
                )
            parsed[suffix] = info

        validated: list[_InnerArchive] = []
        invalid: list[_InvalidInnerArchive] = []
        for suffix, info in parsed.items():
            inner_bytes = outer.read(info)
            try:
                validated.append(
                    _validate_inner_archive(
                        day,
                        suffix,
                        info,
                        inner_bytes,
                        effective_limits,
                    )
                )
            except _ArchiveError as exc:
                invalid.append(
                    _InvalidInnerArchive(
                        suffix=suffix,
                        slot=SLOT_BY_SUFFIX[suffix],
                        filename=info.filename,
                        state=exc.state,
                        error=str(exc),
                        sha256=sha256(inner_bytes).hexdigest(),
                        size_bytes=len(inner_bytes),
                        outer_crc32=f"{info.CRC:08x}",
                        outer_compressed_size=info.compress_size,
                        outer_uncompressed_size=info.file_size,
                    )
                )
        return _OuterArchiveValidation(
            valid=tuple(validated),
            invalid=tuple(invalid),
            returned_suffixes=tuple(parsed),
        )


def _validate_inner_archive(
    day: date,
    suffix: str,
    outer_info: zipfile.ZipInfo,
    content: bytes,
    limits: ArchiveResourceLimits | None = None,
) -> _InnerArchive:
    effective_limits = (limits or ArchiveResourceLimits()).validated()
    if len(content) > effective_limits.max_inner_archive_bytes:
        raise _ArchiveError(
            "zip_resource_limit_exceeded",
            f"inner archive has {len(content)} bytes, limit is {effective_limits.max_inner_archive_bytes}",
            suffix=suffix,
        )
    if not content.startswith(_ZIP_MAGICS):
        raise _ArchiveError(
            "corrupt_inner_zip",
            "inner member does not start with ZIP magic",
            suffix=suffix,
        )
    try:
        inner = zipfile.ZipFile(io.BytesIO(content))
    except (zipfile.BadZipFile, OSError) as exc:
        raise _ArchiveError(
            "corrupt_inner_zip", f"inner ZIP cannot be opened: {exc}", suffix=suffix
        ) from exc
    with inner:
        all_infos = inner.infolist()
        _validate_zip_resources(
            all_infos, effective_limits, layer="inner", suffix=suffix
        )
        bad_member = inner.testzip()
        if bad_member is not None:
            raise _ArchiveError(
                "corrupt_inner_zip",
                f"inner CRC failure in {bad_member!r}",
                suffix=suffix,
            )
        infos = [info for info in all_infos if not info.is_dir()]
        for info in infos:
            if not _safe_basename(info.filename):
                raise _ArchiveError(
                    "unsafe_inner_member",
                    f"unsafe inner member name {info.filename!r}",
                    suffix=suffix,
                )
        spn_infos = [info for info in infos if info.filename.lower().endswith(".spn")]
        if not spn_infos:
            raise _ArchiveError(
                "missing_spn", "inner ZIP contains no .spn member", suffix=suffix
            )
        if len(spn_infos) != 1 or len(infos) != 1:
            raise _ArchiveError(
                "filename_mismatch",
                "inner ZIP must contain exactly one .spn member",
                suffix=suffix,
            )
        spn = spn_infos[0]
        inner_suffix = f"i0{suffix[1]}" if suffix.startswith("i") else "s"
        expected = f"nsccl.{day:%Y%m%d}.{inner_suffix}.spn"
        if spn.filename.lower() != expected:
            raise _ArchiveError(
                "filename_mismatch",
                f"inner member {spn.filename!r} does not match {expected!r}",
                suffix=suffix,
            )
        return _InnerArchive(
            suffix=suffix,
            slot=SLOT_BY_SUFFIX[suffix],
            filename=outer_info.filename,
            content=content,
            sha256=sha256(content).hexdigest(),
            size_bytes=len(content),
            outer_crc32=f"{outer_info.CRC:08x}",
            spn_name=spn.filename,
            spn_crc32=f"{spn.CRC:08x}",
            spn_compressed_size=spn.compress_size,
            spn_uncompressed_size=spn.file_size,
        )


def _persist_inner_archive(
    root: Path,
    day: date,
    archive: _InnerArchive,
    limits: ArchiveResourceLimits | None = None,
) -> tuple[str, Path]:
    effective_limits = (limits or ArchiveResourceLimits()).validated()
    day_dir = root / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}"
    day_dir.mkdir(parents=True, exist_ok=True)
    destination = day_dir / archive.filename.lower()
    if destination.exists():
        _validate_existing_destination(destination, day, archive, effective_limits)
        return "downloaded_existing", destination

    partial = day_dir / f".{archive.filename}.{uuid.uuid4().hex}.partial"
    try:
        with partial.open("xb") as handle:
            handle.write(archive.content)
            handle.flush()
            os.fsync(handle.fileno())
        _validate_saved_inner(
            partial, day, archive.suffix, archive.sha256, effective_limits
        )
        try:
            # partial and destination share a directory/volume.  Hard-link creation
            # is an atomic create-if-absent operation, unlike os.replace(), which
            # can overwrite a file published by a concurrent process.
            os.link(partial, destination)
        except FileExistsError:
            _validate_existing_destination(destination, day, archive, effective_limits)
            return "downloaded_existing", destination
        except OSError as exc:
            raise _ArchiveError(
                "atomic_publish_error",
                f"atomic no-overwrite publication failed for {destination}: {exc}",
                suffix=archive.suffix,
            ) from exc
        partial.unlink()
        _fsync_directory(day_dir)
        try:
            _validate_saved_inner(
                destination,
                day,
                archive.suffix,
                archive.sha256,
                effective_limits,
            )
        except BaseException:
            # This process created the hard link, so removing an invalid result
            # cannot delete a pre-existing immutable archive.
            destination.unlink(missing_ok=True)
            raise
        return "downloaded", destination
    finally:
        partial.unlink(missing_ok=True)


def _validate_existing_destination(
    destination: Path,
    day: date,
    archive: _InnerArchive,
    limits: ArchiveResourceLimits,
) -> None:
    existing_size = destination.stat().st_size
    if existing_size > limits.max_inner_archive_bytes:
        raise _ArchiveError(
            "zip_resource_limit_exceeded",
            f"existing inner archive has {existing_size} bytes, limit is "
            f"{limits.max_inner_archive_bytes}",
            suffix=archive.suffix,
        )
    existing_hash = _file_sha256(destination)
    if existing_hash != archive.sha256:
        raise _ArchiveError(
            "immutable_hash_conflict",
            f"refusing to overwrite {destination}; existing SHA-256 {existing_hash} "
            f"differs from returned {archive.sha256}",
            suffix=archive.suffix,
        )
    _validate_saved_inner(destination, day, archive.suffix, archive.sha256, limits)


def _validate_saved_inner(
    path: Path,
    day: date,
    suffix: str,
    expected_hash: str,
    limits: ArchiveResourceLimits | None = None,
) -> None:
    effective_limits = (limits or ArchiveResourceLimits()).validated()
    file_size = path.stat().st_size
    if file_size > effective_limits.max_inner_archive_bytes:
        raise _ArchiveError(
            "zip_resource_limit_exceeded",
            f"saved inner archive has {file_size} bytes, limit is "
            f"{effective_limits.max_inner_archive_bytes}",
            suffix=suffix,
        )
    content = path.read_bytes()
    if sha256(content).hexdigest() != expected_hash:
        raise _ArchiveError(
            "local_file_invalid", f"SHA-256 validation failed for {path}", suffix=suffix
        )
    synthetic = zipfile.ZipInfo(filename=f"nsccl.{day:%Y%m%d}.{suffix}.zip")
    synthetic.CRC = 0
    _validate_inner_archive(day, suffix, synthetic, content, effective_limits)


def _download_event_matches_file(
    event: Mapping[str, Any],
    day: date,
    suffix: str,
    root: Path,
    limits: ArchiveResourceLimits,
) -> bool:
    expected_hash = str(event.get("sha256", ""))
    relative = str(event.get("path", ""))
    if not expected_hash or not relative:
        return False
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError:
        return False
    if not path.is_file():
        return False
    if event.get("size_bytes") is not None and path.stat().st_size != int(
        event["size_bytes"]
    ):
        return False
    try:
        _validate_saved_inner(path, day, suffix, expected_hash, limits)
    except (OSError, ValueError, zipfile.BadZipFile):
        return False
    return True


async def _append_all_unresolved(
    manifest: _Manifest,
    day: date,
    valid_downloaded: set[str],
    *,
    state: str,
    terminal: bool,
    attempt: int,
    error: str | None = None,
    http_status: int | None = None,
    only_suffix: str | None = None,
    **fields: Any,
) -> None:
    for slot, suffix in SLOT_SPECS:
        if suffix in valid_downloaded:
            continue
        event_state = (
            state
            if only_suffix is None or suffix == only_suffix
            else "bundle_validation_blocked"
        )
        await manifest.append(
            _cell_event(
                day,
                slot,
                suffix,
                event_state,
                terminal=terminal,
                attempt=attempt,
                error=error,
                http_status=http_status,
                **fields,
            )
        )


def _cell_event(
    day: date,
    slot: str,
    suffix: str,
    state: str,
    *,
    terminal: bool,
    attempt: int,
    **fields: Any,
) -> dict[str, Any]:
    return {
        "trading_date": day.isoformat(),
        "slot": slot,
        "suffix": suffix,
        "state": state,
        "terminal": terminal,
        "attempt": attempt,
        **{key: value for key, value in fields.items() if value is not None},
    }


def _report_cells(
    manifest: _Manifest, start: date, end: date
) -> tuple[BackfillCellResult, ...]:
    results: list[BackfillCellResult] = []
    day = start
    while day <= end:
        for slot, suffix in SLOT_SPECS:
            event = manifest.latest.get((day.isoformat(), slot), {})
            results.append(
                BackfillCellResult(
                    trading_date=day.isoformat(),
                    slot=slot,
                    suffix=suffix,
                    state=str(event.get("state", "manifest_cell_missing")),
                    terminal=bool(event.get("terminal", False)),
                    attempt=int(event.get("attempt", 0) or 0),
                    path=event.get("path"),
                    sha256=event.get("sha256"),
                    size_bytes=event.get("size_bytes"),
                    error=event.get("error"),
                )
            )
        day += timedelta(days=1)
    return tuple(results)


def _safe_basename(name: str) -> bool:
    if not name or "\\" in name or name in {".", ".."}:
        return False
    pure = PurePosixPath(name)
    return pure.name == name and len(pure.parts) == 1


def _validate_zip_resources(
    infos: list[zipfile.ZipInfo],
    limits: ArchiveResourceLimits,
    *,
    layer: str,
    suffix: str | None = None,
) -> None:
    if layer == "outer":
        max_members = limits.max_outer_members
        max_member_compressed = limits.max_outer_member_compressed_bytes
        max_member_uncompressed = limits.max_outer_member_uncompressed_bytes
        max_total_compressed = limits.max_outer_total_compressed_bytes
        max_total_uncompressed = limits.max_outer_total_uncompressed_bytes
    elif layer == "inner":
        max_members = limits.max_inner_members
        max_member_compressed = limits.max_inner_member_compressed_bytes
        max_member_uncompressed = limits.max_inner_member_uncompressed_bytes
        max_total_compressed = limits.max_inner_total_compressed_bytes
        max_total_uncompressed = limits.max_inner_total_uncompressed_bytes
    else:  # pragma: no cover - internal contract.
        raise ValueError(f"unknown ZIP layer {layer!r}")

    if len(infos) > max_members:
        raise _ArchiveError(
            "zip_resource_limit_exceeded",
            f"{layer} ZIP has {len(infos)} members, limit is {max_members}",
            suffix=suffix,
        )
    seen_names: set[str] = set()
    total_compressed = 0
    total_uncompressed = 0
    for info in infos:
        normalized_name = info.filename.casefold()
        if normalized_name in seen_names:
            raise _ArchiveError(
                "duplicate_member_name",
                f"duplicate {layer} ZIP member name {info.filename!r}",
                suffix=suffix,
            )
        seen_names.add(normalized_name)
        if info.flag_bits & 0x1:
            raise _ArchiveError(
                "encrypted_zip_member",
                f"encrypted {layer} ZIP member {info.filename!r} is not accepted",
                suffix=suffix,
            )
        unix_mode = (info.external_attr >> 16) & 0o170000
        if unix_mode == stat.S_IFLNK:
            raise _ArchiveError(
                "symlink_zip_member",
                f"symlink {layer} ZIP member {info.filename!r} is not accepted",
                suffix=suffix,
            )
        compressed = int(info.compress_size)
        uncompressed = int(info.file_size)
        total_compressed += compressed
        total_uncompressed += uncompressed
        if compressed > max_member_compressed or uncompressed > max_member_uncompressed:
            raise _ArchiveError(
                "zip_resource_limit_exceeded",
                f"{layer} member {info.filename!r} size {compressed}/{uncompressed} exceeds "
                f"compressed/uncompressed limits {max_member_compressed}/{max_member_uncompressed}",
                suffix=suffix,
            )
        ratio = (
            float("inf")
            if compressed == 0 and uncompressed
            else uncompressed / max(1, compressed)
        )
        if ratio > limits.max_compression_ratio:
            raise _ArchiveError(
                "zip_resource_limit_exceeded",
                f"{layer} member {info.filename!r} compression ratio {ratio:.2f} exceeds "
                f"{limits.max_compression_ratio:.2f}",
                suffix=suffix,
            )
    if (
        total_compressed > max_total_compressed
        or total_uncompressed > max_total_uncompressed
    ):
        raise _ArchiveError(
            "zip_resource_limit_exceeded",
            f"{layer} ZIP total size {total_compressed}/{total_uncompressed} exceeds "
            f"compressed/uncompressed limits {max_total_compressed}/{max_total_uncompressed}",
            suffix=suffix,
        )
    total_ratio = (
        float("inf")
        if total_compressed == 0 and total_uncompressed
        else total_uncompressed / max(1, total_compressed)
    )
    if total_ratio > limits.max_compression_ratio:
        raise _ArchiveError(
            "zip_resource_limit_exceeded",
            f"{layer} ZIP total compression ratio {total_ratio:.2f} exceeds "
            f"{limits.max_compression_ratio:.2f}",
            suffix=suffix,
        )


def _headers(response: ResponseLike) -> dict[str, str]:
    raw = getattr(response, "headers", {}) or {}
    return {str(key).lower(): str(value) for key, value in raw.items()}


def _response_metadata(content: bytes, content_type: str) -> dict[str, Any]:
    """Record the generated outer wrapper for audit, not durable identity."""

    return {
        "body_sha256": sha256(content).hexdigest(),
        "body_size_bytes": len(content),
        "content_type": content_type or None,
    }


def _positive_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _is_transient_exception(exc: Exception) -> bool:
    if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
        return True
    text = f"{type(exc).__name__} {exc}".lower()
    return any(
        token in text
        for token in ("timeout", "timed out", "reset", "connection", "curl")
    )


def _retry_after_seconds(value: str | None) -> float | None:
    if value is None:
        return None
    text = value.strip()
    try:
        return max(0.0, float(text))
    except ValueError:
        try:
            moment = parsedate_to_datetime(text)
            if moment.tzinfo is None:
                moment = moment.replace(tzinfo=UTC)
            return max(0.0, (moment - datetime.now(UTC)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None


def _backoff(
    config: BackfillConfig,
    attempt: int,
    retry_after: float | None,
    random_fn: Callable[[], float],
) -> float:
    exponential = min(
        config.backoff_max_seconds,
        config.backoff_base_seconds * (2 ** max(0, attempt - 1)),
    )
    jitter = max(0.0, random_fn()) * config.jitter_seconds
    return max(exponential + jitter, retry_after or 0.0)


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":  # Windows does not permit opening directories this way.
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _curl_client_factory(_worker_id: int) -> ClientLike:
    try:
        from curl_cffi.requests import AsyncSession  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - production dependency.
        raise RuntimeError("curl_cffi is required for NSE SPAN downloads") from exc
    return AsyncSession(impersonate="chrome124", headers=HEADERS)
