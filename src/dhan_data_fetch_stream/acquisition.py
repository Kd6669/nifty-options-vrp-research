"""Resumable, credential-safe Dhan historical acquisition primitives.

The module keeps authentication in memory, hashes only non-secret payloads, and
writes one atomic manifest per request.  Historical rolling options remain
labelled as a rolling moneyness surface; current FUTIDX identities are never
treated as an archive of expired contracts.
"""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import io
import json
import os
from pathlib import Path
import random
import re
import threading
import time
from typing import Any, Callable, Iterable, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from .core import DhanCredentials


IST = ZoneInfo("Asia/Kolkata")
DHAN_API_BASE_URL = "https://api.dhan.co/v2"
DHAN_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
DHAN_DETAILED_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
INDIA_VIX_SECURITY_ID = "21"
DATA_REQUESTS_PER_SECOND = 5.0
DATA_REQUESTS_PER_DAY = 100_000
OPTION_CHAIN_KEY_INTERVAL_SECONDS = 3.0
ROLLING_MAX_DAYS = 30
INTRADAY_MAX_DAYS = 90
SCHEMA_VERSION = "1.2.0"
RETRYABLE_HTTP = {403, 408, 425, 429, 500, 502, 503, 504}
_JWT_RE = re.compile(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b")


@dataclass(frozen=True)
class RequestCell:
    dataset: str
    endpoint: str
    payload: Mapping[str, Any]
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def request_id(self) -> str:
        canonical = json.dumps(
            {"endpoint": self.endpoint, "payload": dict(self.payload)},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()


@dataclass(frozen=True)
class RequestOutcome:
    request_id: str
    dataset: str
    status: str
    attempts: int
    rows: int
    min_timestamp_ist: str | None
    max_timestamp_ist: str | None
    bronze_path: str | None
    bronze_sha256: str | None
    silver_path: str | None
    silver_sha256: str | None
    manifest_path: str
    error_class: str | None = None
    error_code: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class FutureIdentity:
    security_id: str
    trading_symbol: str
    display_symbol: str
    expiry_text: str
    lot_size: int


@dataclass(frozen=True)
class CurrentInstrumentSnapshot:
    fetched_at_utc: str
    source_url: str
    source_sha256: str
    nifty_index_security_id: str
    futures: tuple[FutureIdentity, ...]


@dataclass(frozen=True)
class RebuildStats:
    root: str
    manifests_seen: int
    rebuilt: int
    rows: int
    quality_exception_rows: int
    failures: int


class DhanRequestError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        code: str | None = None,
        retry_after: float | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.retry_after = retry_after
        self.retryable = retryable


class RateLimiter:
    """Thread-safe fixed-spacing limiter."""

    def __init__(self, requests_per_second: float = DATA_REQUESTS_PER_SECOND) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        self._interval = 1.0 / float(requests_per_second)
        self._next = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delay = max(0.0, self._next - now)
            self._next = max(now, self._next) + self._interval
        if delay:
            time.sleep(delay)


class KeyedIntervalLimiter:
    def __init__(self, interval_seconds: float = OPTION_CHAIN_KEY_INTERVAL_SECONDS) -> None:
        self._interval = float(interval_seconds)
        self._next_by_key: dict[str, float] = {}
        self._lock = threading.Lock()

    def wait(self, key: str) -> None:
        with self._lock:
            now = time.monotonic()
            ready = self._next_by_key.get(key, 0.0)
            delay = max(0.0, ready - now)
            self._next_by_key[key] = max(now, ready) + self._interval
        if delay:
            time.sleep(delay)


class DailyBudget:
    def __init__(self, root: str | Path, limit: int = DATA_REQUESTS_PER_DAY) -> None:
        if limit <= 0 or limit > DATA_REQUESTS_PER_DAY:
            raise ValueError(f"daily request limit must be in 1..{DATA_REQUESTS_PER_DAY}")
        self.root = Path(root)
        self.limit = int(limit)
        self._lock = threading.Lock()

    def reserve(self) -> int:
        with self._lock:
            today = datetime.now(timezone.utc).date().isoformat()
            path = self.root / "manifests" / f"daily_budget_{today}.json"
            payload: dict[str, Any] = {"utc_date": today, "used": 0, "limit": self.limit}
            if path.is_file():
                payload.update(json.loads(path.read_text(encoding="utf-8")))
            used = int(payload.get("used", 0))
            if used >= self.limit:
                raise DhanRequestError("daily_request_budget_exhausted", code="daily_budget", retryable=False)
            payload.update({"used": used + 1, "limit": self.limit, "updated_at_utc": _utc_now()})
            _atomic_write_json(path, payload)
            return used + 1


class DhanTransport:
    def __init__(
        self,
        credentials: DhanCredentials,
        *,
        base_url: str = DHAN_API_BASE_URL,
        timeout_seconds: float = 30.0,
    ) -> None:
        self.credentials = credentials
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = float(timeout_seconds)

    def post(self, endpoint: str, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "access-token": self.credentials.access_token,
        }
        if endpoint.startswith("/optionchain"):
            headers["client-id"] = self.credentials.client_id
        request = Request(
            f"{self.base_url}/{endpoint.lstrip('/')}",
            data=json.dumps(dict(payload), separators=(",", ":")).encode("utf-8"),
            method="POST",
            headers=headers,
        )
        try:
            with urlopen(request, timeout=max(1.0, self.timeout_seconds)) as response:
                raw = response.read().decode("utf-8", errors="replace")
        except HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace") if exc.fp is not None else ""
            code, message = _provider_error(raw)
            retry_after = _parse_retry_after(exc.headers.get("Retry-After"))
            raise DhanRequestError(
                redact_secret_text(message or f"Dhan API HTTP {exc.code}", (self.credentials.access_token,)),
                status=exc.code,
                code=code,
                retry_after=retry_after,
                retryable=exc.code in RETRYABLE_HTTP,
            ) from None
        except URLError as exc:
            raise DhanRequestError(
                redact_secret_text(str(exc.reason), (self.credentials.access_token,)),
                code="network_error",
                retryable=True,
            ) from None
        data = json.loads(raw)
        if not isinstance(data, Mapping):
            raise DhanRequestError("response_is_not_a_json_object", code="invalid_response")
        return data


class AcquisitionEngine:
    def __init__(
        self,
        *,
        root: str | Path,
        transport: DhanTransport,
        daily_budget: int = DATA_REQUESTS_PER_DAY,
        requests_per_second: float = DATA_REQUESTS_PER_SECOND,
        max_retries: int = 4,
        workers: int = 1,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.root = Path(root)
        self.transport = transport
        self.budget = DailyBudget(self.root, daily_budget)
        self.rate = RateLimiter(min(float(requests_per_second), DATA_REQUESTS_PER_SECOND))
        self.option_chain_rate = KeyedIntervalLimiter()
        self.max_retries = max(1, int(max_retries))
        self.workers = max(1, min(int(workers), 5))
        self.sleep = sleep

    def run(
        self,
        cells: Sequence[RequestCell],
        *,
        resume: bool = True,
        stop_on_credential_blocked: bool = True,
    ) -> list[RequestOutcome]:
        quarantine_orphan_partials(self.root)
        if self.workers == 1:
            outcomes: list[RequestOutcome] = []
            for cell in cells:
                outcome = self._run_one(cell, resume=resume)
                outcomes.append(outcome)
                if stop_on_credential_blocked and outcome.status == "credential_blocked":
                    break
            return outcomes
        from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait

        # Keep only ``workers`` cells in flight. Submitting the entire plan up
        # front makes a bad credential fan out thousands of failure manifests
        # before the first result can stop the run.
        indexed = iter(enumerate(cells))
        outcomes: list[tuple[int, RequestOutcome]] = []
        pending: dict[Future[RequestOutcome], int] = {}
        abort = False
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            for _ in range(self.workers):
                try:
                    index, cell = next(indexed)
                except StopIteration:
                    break
                pending[pool.submit(self._run_one, cell, resume=resume)] = index
            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    index = pending.pop(future)
                    outcome = future.result()
                    outcomes.append((index, outcome))
                    if stop_on_credential_blocked and outcome.status == "credential_blocked":
                        abort = True
                if abort:
                    for future in list(pending):
                        if future.cancel():
                            pending.pop(future)
                    continue
                for _ in range(len(done)):
                    try:
                        index, cell = next(indexed)
                    except StopIteration:
                        break
                    pending[pool.submit(self._run_one, cell, resume=resume)] = index
        return [outcome for _, outcome in sorted(outcomes, key=lambda item: item[0])]

    def _run_one(self, cell: RequestCell, *, resume: bool) -> RequestOutcome:
        manifest_path = self.root / "manifests" / "requests" / f"{cell.request_id}.json"
        if resume:
            resumed = _resume_outcome(manifest_path)
            if resumed is not None:
                return resumed
        cached_bronze = _cached_bronze(manifest_path) if resume else None
        started = _utc_now()
        attempts = 0
        response: Mapping[str, Any] | None = None if cached_bronze is None else cached_bronze[0]
        last_error: DhanRequestError | None = None
        for attempt in range(1, self.max_retries + 1) if response is None else ():
            attempts = attempt
            try:
                self.budget.reserve()
                self.rate.wait()
                if cell.endpoint.startswith("/optionchain"):
                    self.option_chain_rate.wait(cell.request_id)
                response = self.transport.post(cell.endpoint, cell.payload)
                break
            except DhanRequestError as exc:
                last_error = exc
                if not exc.retryable or attempt >= self.max_retries:
                    break
                backoff = exc.retry_after
                if backoff is None:
                    backoff = min(60.0, (2 ** (attempt - 1)) + random.uniform(0.0, 0.5))
                self.sleep(max(0.0, backoff))
        if response is None:
            assert last_error is not None
            error_message = redact_secret_text(
                str(last_error),
                (self.transport.credentials.access_token, self.transport.credentials.client_id),
            )
            payload = _manifest_base(cell, started, attempts)
            payload.update(
                {
                    "status": _failure_status(last_error),
                    "completed_at_utc": _utc_now(),
                    "rows": 0,
                    "error_class": type(last_error).__name__,
                    "error_code": last_error.code,
                    "http_status": last_error.status,
                    "error_message": error_message,
                }
            )
            _atomic_write_json(manifest_path, payload)
            exception_path = self.root / "exceptions" / "requests" / f"{cell.request_id}.json"
            _atomic_write_json(exception_path, payload)
            return RequestOutcome(
                cell.request_id,
                cell.dataset,
                str(payload["status"]),
                attempts,
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                str(manifest_path),
                type(last_error).__name__,
                last_error.code,
                error_message,
            )

        if cached_bronze is None:
            request_month = _cell_partition_month(cell)
            bronze_path = (
                self.root
                / "bronze"
                / cell.dataset
                / f"year={request_month.year:04d}"
                / f"month={request_month.month:02d}"
                / f"{cell.request_id}.json"
            )
            _atomic_write_json(bronze_path, response)
            bronze_sha = sha256_file(bronze_path)
        else:
            _, bronze_path, bronze_sha = cached_bronze
        try:
            rows = normalize_response(cell, response)
        except DhanRequestError as exc:
            payload = _manifest_base(cell, started, attempts)
            payload.update(
                {
                    "status": "invalid_response",
                    "completed_at_utc": _utc_now(),
                    "rows": 0,
                    "bronze_path": str(bronze_path),
                    "bronze_sha256": bronze_sha,
                    "error_class": type(exc).__name__,
                    "error_code": exc.code,
                    "error_message": redact_secret_text(
                        str(exc),
                        (self.transport.credentials.access_token, self.transport.credentials.client_id),
                    ),
                }
            )
            _atomic_write_json(manifest_path, payload)
            exception_path = self.root / "exceptions" / "responses" / f"{cell.request_id}.json"
            _atomic_write_json(exception_path, payload)
            return RequestOutcome(
                cell.request_id,
                cell.dataset,
                "invalid_response",
                attempts,
                0,
                None,
                None,
                str(bronze_path),
                bronze_sha,
                None,
                None,
                str(manifest_path),
                type(exc).__name__,
                exc.code,
                str(payload["error_message"]),
            )
        provider_rows = _provider_row_count(cell, response)
        window_dropped = max(0, provider_rows - len(rows))
        rows, quality_exceptions = partition_normalized_rows(cell, rows)
        min_ts, max_ts = _timestamp_bounds(rows)
        silver_path: Path | None = None
        silver_sha: str | None = None
        if rows:
            partition_date = _partition_date(rows, cell)
            silver_path = (
                self.root
                / "silver"
                / cell.dataset
                / f"year={partition_date.year:04d}"
                / f"month={partition_date.month:02d}"
                / f"{cell.request_id}.parquet"
            )
            _atomic_write_parquet(silver_path, rows, dataset=cell.dataset)
            silver_sha = sha256_file(silver_path)
        payload = _manifest_base(cell, started, attempts)
        payload.update(
            {
                "status": "completed" if rows else "completed_empty",
                "completed_at_utc": _utc_now(),
                "provider_rows": provider_rows,
                "rows": len(rows),
                "provider_rows_dropped_by_request_window": window_dropped,
                "quality_exception_rows": len(quality_exceptions),
                "min_timestamp_ist": min_ts,
                "max_timestamp_ist": max_ts,
                "bronze_path": str(bronze_path),
                "bronze_sha256": bronze_sha,
                "silver_path": None if silver_path is None else str(silver_path),
                "silver_sha256": silver_sha,
            }
        )
        _atomic_write_json(manifest_path, payload)
        if quality_exceptions:
            exception_path = self.root / "exceptions" / "quality" / f"{cell.request_id}.parquet"
            _atomic_write_untyped_parquet(exception_path, quality_exceptions, dataset=f"{cell.dataset}_quality_exceptions")
            payload["quality_exception_path"] = str(exception_path)
            payload["quality_exception_sha256"] = sha256_file(exception_path)
            _atomic_write_json(manifest_path, payload)
        if window_dropped:
            _atomic_write_json(
                self.root / "exceptions" / "request_window" / f"{cell.request_id}.json",
                {
                    "request_id": cell.request_id,
                    "dataset": cell.dataset,
                    "status": "provider_returned_rows_outside_half_open_request_window",
                    "provider_rows": provider_rows,
                    "normalized_rows": len(rows),
                    "dropped_rows": window_dropped,
                    "fromDate": cell.payload.get("fromDate"),
                    "toDate": cell.payload.get("toDate"),
                },
            )
        return RequestOutcome(
            cell.request_id,
            cell.dataset,
            str(payload["status"]),
            attempts,
            len(rows),
            min_ts,
            max_ts,
            str(bronze_path),
            bronze_sha,
            None if silver_path is None else str(silver_path),
            silver_sha,
            str(manifest_path),
        )


def plan_rolling_options(
    *,
    start_date: date,
    end_date: date,
    expiry_flags: Sequence[str] = ("WEEK", "MONTH"),
    expiry_codes: Sequence[int],
    moneyness_width: int = 10,
    option_types: Sequence[str] = ("CALL", "PUT"),
) -> list[RequestCell]:
    if not expiry_codes:
        raise ValueError("expiry_codes are required because Dhan's official mappings conflict")
    if moneyness_width < 0 or moneyness_width > 10:
        raise ValueError("moneyness_width must be between 0 and 10")
    labels = ["ATM"] + [f"ATM-{n}" for n in range(1, moneyness_width + 1)] + [
        f"ATM+{n}" for n in range(1, moneyness_width + 1)
    ]
    cells: list[RequestCell] = []
    for chunk_start, chunk_to in partitioned_date_chunks(start_date, end_date, ROLLING_MAX_DAYS):
        for flag in expiry_flags:
            normalized_flag = str(flag).upper()
            if normalized_flag not in {"WEEK", "MONTH"}:
                raise ValueError("expiry flags must be WEEK or MONTH")
            for code in expiry_codes:
                for label in labels:
                    for side in option_types:
                        normalized_side = str(side).upper()
                        payload = {
                            "exchangeSegment": "NSE_FNO",
                            "interval": "1",
                            "securityId": 13,
                            "instrument": "OPTIDX",
                            "expiryFlag": normalized_flag,
                            "expiryCode": int(code),
                            "strike": label,
                            "drvOptionType": normalized_side,
                            "requiredData": ["open", "high", "low", "close", "iv", "volume", "strike", "oi", "spot"],
                            "fromDate": chunk_start.isoformat(),
                            "toDate": chunk_to.isoformat(),
                        }
                        cells.append(
                            RequestCell(
                                "options",
                                "/charts/rollingoption",
                                payload,
                                {
                                    "underlying": "NIFTY",
                                    "surface": "rolling_moneyness_not_absolute_full_chain",
                                    "expiry_resolution_status": "not_returned_by_endpoint",
                                },
                            )
                        )
    return cells


def plan_spot(start_date: date, end_date: date) -> list[RequestCell]:
    return [
        RequestCell(
            "spot",
            "/charts/intraday",
            {
                "securityId": "13",
                "exchangeSegment": "IDX_I",
                "instrument": "INDEX",
                "interval": "1",
                "oi": False,
                "fromDate": f"{start.isoformat()} 09:15:00",
                "toDate": f"{to.isoformat()} 00:00:00",
            },
            {"underlying": "NIFTY", "to_date_semantics": "probe_required"},
        )
        for start, to in partitioned_date_chunks(start_date, end_date, INTRADAY_MAX_DAYS)
    ]


def plan_india_vix(start_date: date, end_date: date) -> list[RequestCell]:
    """Plan independent 1-minute INDIA VIX index cells on calendar-month boundaries."""
    return [
        RequestCell(
            "india_vix",
            "/charts/intraday",
            {
                "securityId": INDIA_VIX_SECURITY_ID,
                "exchangeSegment": "IDX_I",
                "instrument": "INDEX",
                "interval": "1",
                "oi": False,
                "fromDate": f"{start.isoformat()} 09:15:00",
                "toDate": f"{to.isoformat()} 00:00:00",
            },
            {
                "underlying": "INDIA VIX",
                "series_label": "INDIA VIX",
                "official_master_url": DHAN_DETAILED_MASTER_URL,
                "official_master_identity": {
                    "exchange": "NSE",
                    "segment": "I",
                    "security_id": INDIA_VIX_SECURITY_ID,
                    "instrument": "INDEX",
                    "underlying_security_id": INDIA_VIX_SECURITY_ID,
                    "underlying_symbol": "INDIA VIX",
                    "symbol_name": "INDIA VIX",
                    "display_name": "India VIX",
                },
                "to_date_semantics": "probe_required",
            },
        )
        for start, to in partitioned_date_chunks(start_date, end_date, INTRADAY_MAX_DAYS)
    ]


def plan_current_futures(
    start_date: date,
    end_date: date,
    snapshot: CurrentInstrumentSnapshot,
) -> list[RequestCell]:
    cells: list[RequestCell] = []
    for identity in snapshot.futures:
        for start, to in partitioned_date_chunks(start_date, end_date, INTRADAY_MAX_DAYS):
            cells.append(
                RequestCell(
                    "futures",
                    "/charts/intraday",
                    {
                        "securityId": identity.security_id,
                        "exchangeSegment": "NSE_FNO",
                        "instrument": "FUTIDX",
                        "interval": "1",
                        "oi": True,
                        "fromDate": f"{start.isoformat()} 09:15:00",
                        "toDate": f"{to.isoformat()} 00:00:00",
                    },
                    {
                        "underlying": "NIFTY",
                        "futures_expiry_text": identity.expiry_text,
                        "series_label": identity.trading_symbol,
                        "master_sha256": snapshot.source_sha256,
                        "coverage_constraint": "current_active_contract_only",
                    },
                )
            )
    return cells


def feasibility_cells(*, recent_start: date, recent_end: date, boundary_dates: Sequence[date]) -> list[RequestCell]:
    cells: list[RequestCell] = [
        RequestCell(
            "probe",
            "/optionchain/expirylist",
            {"UnderlyingScrip": 13, "UnderlyingSeg": "IDX_I"},
            {"probe": "active_expiry_list"},
        )
    ]
    cells.extend(plan_spot(recent_start, recent_end))
    for sentinel in boundary_dates:
        cells.extend(plan_spot(sentinel, sentinel))
    for flag in ("WEEK", "MONTH"):
        for code in (0, 1, 2, 3):
            payload = {
                "exchangeSegment": "NSE_FNO",
                "interval": "1",
                "securityId": 13,
                "instrument": "OPTIDX",
                "expiryFlag": flag,
                "expiryCode": code,
                "strike": "ATM",
                "drvOptionType": "CALL",
                "requiredData": ["open", "high", "low", "close", "iv", "volume", "strike", "oi", "spot"],
                "fromDate": recent_start.isoformat(),
                "toDate": (recent_end + timedelta(days=1)).isoformat(),
            }
            cells.append(RequestCell("options", "/charts/rollingoption", payload, {"probe": "expiry_code_matrix"}))
    return cells


def date_chunks(start_date: date, end_date: date, max_days: int) -> list[tuple[date, date]]:
    if end_date < start_date:
        raise ValueError("end_date must not precede start_date")
    if max_days <= 0:
        raise ValueError("max_days must be positive")
    end_exclusive = end_date + timedelta(days=1)
    cursor = start_date
    chunks: list[tuple[date, date]] = []
    while cursor < end_exclusive:
        to_date = min(cursor + timedelta(days=max_days), end_exclusive)
        chunks.append((cursor, to_date))
        cursor = to_date
    return chunks


def partitioned_date_chunks(start_date: date, end_date: date, max_days: int) -> list[tuple[date, date]]:
    """Half-open chunks capped by request limit and calendar-month boundary."""
    if end_date < start_date:
        raise ValueError("end_date must not precede start_date")
    if max_days <= 0:
        raise ValueError("max_days must be positive")
    end_exclusive = end_date + timedelta(days=1)
    cursor = start_date
    chunks: list[tuple[date, date]] = []
    while cursor < end_exclusive:
        next_month = date(cursor.year + (cursor.month == 12), 1 if cursor.month == 12 else cursor.month + 1, 1)
        to_date = min(cursor + timedelta(days=max_days), next_month, end_exclusive)
        chunks.append((cursor, to_date))
        cursor = to_date
    return chunks


def fetch_current_instrument_snapshot(
    *,
    url: str = DHAN_MASTER_URL,
    timeout_seconds: float = 30.0,
) -> CurrentInstrumentSnapshot:
    with urlopen(url, timeout=max(1.0, float(timeout_seconds))) as response:
        raw = response.read()
    text = raw.decode("utf-8-sig", errors="replace")
    return parse_current_instrument_snapshot(text, source_url=url, source_sha256=hashlib.sha256(raw).hexdigest())


def parse_current_instrument_snapshot(
    text: str,
    *,
    source_url: str = DHAN_MASTER_URL,
    source_sha256: str | None = None,
) -> CurrentInstrumentSnapshot:
    index_id: str | None = None
    futures: list[FutureIdentity] = []
    for row in csv.DictReader(io.StringIO(text)):
        instrument = str(row.get("SEM_INSTRUMENT_NAME", "")).upper()
        segment = str(row.get("SEM_SEGMENT", "")).upper()
        trading_symbol = str(row.get("SEM_TRADING_SYMBOL", "")).strip()
        if instrument == "INDEX" and segment == "I" and trading_symbol == "NIFTY":
            index_id = str(row.get("SEM_SMST_SECURITY_ID", "")).strip()
        if (
            instrument == "FUTIDX"
            and str(row.get("SEM_EXM_EXCH_ID", "")).upper() == "NSE"
            and re.fullmatch(r"NIFTY-[A-Za-z]{3}\d{4}-FUT", trading_symbol)
        ):
            futures.append(
                FutureIdentity(
                    security_id=str(row.get("SEM_SMST_SECURITY_ID", "")).strip(),
                    trading_symbol=trading_symbol,
                    display_symbol=str(row.get("SEM_CUSTOM_SYMBOL", "")).strip(),
                    expiry_text=str(row.get("SEM_EXPIRY_DATE", "")).strip(),
                    lot_size=int(float(row.get("SEM_LOT_UNITS", 0))),
                )
            )
    if index_id != "13":
        raise ValueError(f"official master did not resolve exact NIFTY INDEX security ID 13: {index_id!r}")
    futures.sort(key=lambda item: item.expiry_text)
    if not futures:
        raise ValueError("official master returned no active NIFTY FUTIDX rows")
    return CurrentInstrumentSnapshot(
        _utc_now(),
        source_url,
        source_sha256 or hashlib.sha256(text.encode("utf-8")).hexdigest(),
        index_id,
        tuple(futures[:3]),
    )


def normalize_response(cell: RequestCell, response: Mapping[str, Any]) -> list[dict[str, Any]]:
    if cell.dataset == "probe":
        return []
    if cell.dataset == "options":
        root = response.get("data", response)
        if not isinstance(root, Mapping):
            raise DhanRequestError("rolling response data must be an object", code="invalid_parallel_arrays")
        side_key = "ce" if str(cell.payload.get("drvOptionType", "")).upper() == "CALL" else "pe"
        arrays = root.get(side_key)
        if arrays is None:
            return []
        if not isinstance(arrays, Mapping):
            raise DhanRequestError("rolling side payload must be an object", code="invalid_parallel_arrays")
        required = ("timestamp", "open", "high", "low", "close", "iv", "volume", "strike", "oi", "spot")
        length = validate_parallel_arrays(arrays, required)
        rows: list[dict[str, Any]] = []
        for index in range(length):
            ts = datetime.fromtimestamp(float(arrays["timestamp"][index]), tz=IST)
            if not _inside_request_window(ts, cell):
                continue
            rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "request_id": cell.request_id,
                    "provider": "dhan_rollingoption",
                    "timestamp_ist": ts,
                    "trade_date": ts.date(),
                    "session_status": _regular_session_status(ts),
                    "underlying": "NIFTY",
                    "expiry_date": None,
                    "expiry_flag": str(cell.payload["expiryFlag"]),
                    "expiry_code": int(cell.payload["expiryCode"]),
                    "moneyness_label": str(cell.payload["strike"]),
                    "strike": _decimal_strike(arrays["strike"][index]),
                    "option_type": str(cell.payload["drvOptionType"]),
                    "open": _float_or_none(arrays["open"][index]),
                    "high": _float_or_none(arrays["high"][index]),
                    "low": _float_or_none(arrays["low"][index]),
                    "close": _float_or_none(arrays["close"][index]),
                    "provider_iv_raw": _float_or_none(arrays["iv"][index]),
                    "provider_iv_unit": "provider_raw_unverified",
                    "volume": _int_or_none(arrays["volume"][index]),
                    "open_interest": _int_or_none(arrays["oi"][index]),
                    "provider_spot": _float_or_none(arrays["spot"][index]),
                    "expiry_resolution_status": "not_returned_by_rolling_endpoint",
                }
            )
        return rows
    required = ("timestamp", "open", "high", "low", "close", "volume")
    length = validate_parallel_arrays(response, required, optional=("open_interest",))
    rows = []
    for index in range(length):
        ts = datetime.fromtimestamp(float(response["timestamp"][index]), tz=IST)
        if not _inside_request_window(ts, cell):
            continue
        common = {
            "schema_version": SCHEMA_VERSION,
            "request_id": cell.request_id,
            "provider": "dhan_intraday",
            "timestamp_ist": ts,
            "trade_date": ts.date(),
            "session_status": _regular_session_status(ts),
            "underlying": str(cell.metadata.get("underlying") or "NIFTY"),
            "open": _float_or_none(response["open"][index]),
            "high": _float_or_none(response["high"][index]),
            "low": _float_or_none(response["low"][index]),
            "close": _float_or_none(response["close"][index]),
            "volume": _int_or_none(response["volume"][index]),
        }
        if cell.dataset in {"spot", "india_vix"}:
            common.update({"security_id": str(cell.payload["securityId"]), "open_interest": None})
        elif cell.dataset == "futures":
            oi = response.get("open_interest")
            common.update(
                {
                    "security_id": str(cell.payload["securityId"]),
                    "futures_expiry_text": cell.metadata.get("futures_expiry_text"),
                    "series_label": cell.metadata.get("series_label"),
                    "open_interest": None if not isinstance(oi, list) else _int_or_none(oi[index]),
                }
            )
        rows.append(common)
    return rows


def validate_parallel_arrays(
    payload: Mapping[str, Any],
    required: Sequence[str],
    *,
    optional: Sequence[str] = (),
) -> int:
    lengths: dict[str, int] = {}
    for name in required:
        value = payload.get(name)
        if not isinstance(value, list):
            raise DhanRequestError(f"parallel array missing: {name}", code="invalid_parallel_arrays")
        lengths[name] = len(value)
    for name in optional:
        value = payload.get(name)
        if value is not None:
            if not isinstance(value, list):
                raise DhanRequestError(f"parallel array is not a list: {name}", code="invalid_parallel_arrays")
            lengths[name] = len(value)
    if len(set(lengths.values())) > 1:
        raise DhanRequestError(
            f"parallel array length mismatch: {json.dumps(lengths, sort_keys=True)}",
            code="invalid_parallel_arrays",
        )
    return next(iter(lengths.values()), 0)


def validate_normalized_rows(cell: RequestCell, rows: Sequence[Mapping[str, Any]]) -> None:
    _, exceptions = partition_normalized_rows(cell, rows)
    if exceptions:
        first = exceptions[0]
        raise DhanRequestError(str(first["failure_reason"]), code=str(first["failure_code"]))


def partition_normalized_rows(
    cell: RequestCell,
    rows: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    keys: set[tuple[Any, ...]] = set()
    valid: list[dict[str, Any]] = []
    exceptions: list[dict[str, Any]] = []
    for row in rows:
        failure_code: str | None = None
        failure_reason: str | None = None
        ohlc = tuple(row.get(name) for name in ("open", "high", "low", "close"))
        if all(value is not None for value in ohlc):
            open_, high, low, close = (float(value) for value in ohlc)
            if low > min(open_, close) or max(open_, close) > high:
                failure_code = "ohlc_invariant"
                failure_reason = "OHLC invariant failed"
        for name in ("volume", "open_interest"):
            value = row.get(name)
            if value is not None and int(value) < 0:
                failure_code = "negative_volume_or_oi"
                failure_reason = f"negative {name}"
                break
        if cell.dataset == "options":
            key = (
                row.get("timestamp_ist"), row.get("underlying"), row.get("expiry_flag"),
                row.get("expiry_code"), row.get("moneyness_label"), row.get("strike"), row.get("option_type"),
            )
        elif cell.dataset == "futures":
            key = (row.get("timestamp_ist"), row.get("security_id"))
        elif cell.dataset in {"spot", "india_vix"}:
            key = (row.get("timestamp_ist"), row.get("security_id"))
        else:
            key = (row.get("timestamp_ist"), row.get("underlying"))
        if key in keys:
            failure_code = "duplicate_natural_key"
            failure_reason = "duplicate natural key inside response"
        else:
            keys.add(key)
        if failure_code is None:
            valid.append(dict(row))
        else:
            exception = dict(row)
            exception.update(
                {
                    "failure_code": failure_code,
                    "failure_reason": failure_reason,
                    "quarantined_at_utc": _utc_now(),
                }
            )
            exceptions.append(exception)
    return valid, exceptions


def redact_secret_text(text: str, secrets: Iterable[str] = ()) -> str:
    redacted = str(text)
    for secret in secrets:
        if secret:
            redacted = redacted.replace(str(secret), "<redacted>")
    redacted = re.sub(
        r"(?i)(authorization)(\s*[:=]\s*[\"']?\s*Bearer\s+)[A-Za-z0-9._~-]+",
        r"\1\2<redacted>",
        redacted,
    )
    redacted = re.sub(
        r"(?i)(access[-_ ]?token)(\s*[\"']?\s*[:=]\s*[\"']?)[^\s,}\"']+",
        r"\1\2<redacted>",
        redacted,
    )
    return _JWT_RE.sub("<redacted-jwt>", redacted)


def sha256_file(path: str | Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def rebuild_silver_from_bronze(root: str | Path) -> RebuildStats:
    """Rebuild every Dhan silver partition from immutable bronze without network or credentials."""
    root_path = Path(root)
    manifests = sorted((root_path / "manifests" / "requests").glob("*.json"))
    rebuilt = rows_written = quality_count = failures = 0
    for manifest_path in manifests:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        bronze_value = manifest.get("bronze_path")
        expected_hash = manifest.get("bronze_sha256")
        if not bronze_value or not expected_hash:
            continue
        bronze_path = Path(str(bronze_value))
        if not bronze_path.is_file() or sha256_file(bronze_path) != expected_hash:
            failures += 1
            continue
        cell = RequestCell(
            str(manifest["dataset"]),
            str(manifest["endpoint"]),
            dict(manifest["payload"]),
            dict(manifest.get("metadata") or {}),
        )
        response = json.loads(bronze_path.read_text(encoding="utf-8"))
        try:
            normalized = normalize_response(cell, response)
        except DhanRequestError:
            failures += 1
            continue
        provider_rows = _provider_row_count(cell, response)
        window_dropped = max(0, provider_rows - len(normalized))
        valid, quality_exceptions = partition_normalized_rows(cell, normalized)
        silver_path: Path | None = None
        silver_sha: str | None = None
        if valid:
            partition_date = _partition_date(valid, cell)
            silver_path = (
                root_path
                / "silver"
                / cell.dataset
                / f"year={partition_date.year:04d}"
                / f"month={partition_date.month:02d}"
                / f"{cell.request_id}.parquet"
            )
            _atomic_write_parquet(silver_path, valid, dataset=cell.dataset)
            silver_sha = sha256_file(silver_path)
        exception_path: Path | None = None
        exception_sha: str | None = None
        if quality_exceptions:
            exception_path = root_path / "exceptions" / "quality" / f"{cell.request_id}.parquet"
            _atomic_write_untyped_parquet(
                exception_path,
                quality_exceptions,
                dataset=f"{cell.dataset}_quality_exceptions",
            )
            exception_sha = sha256_file(exception_path)
        min_ts, max_ts = _timestamp_bounds(valid)
        manifest.update(
            {
                "normalizer_version": SCHEMA_VERSION,
                "status": "completed" if valid else "completed_empty",
                "rebuilt_offline_at_utc": _utc_now(),
                "provider_rows": provider_rows,
                "provider_rows_dropped_by_request_window": window_dropped,
                "quality_exception_rows": len(quality_exceptions),
                "quality_exception_path": None if exception_path is None else str(exception_path),
                "quality_exception_sha256": exception_sha,
                "rows": len(valid),
                "min_timestamp_ist": min_ts,
                "max_timestamp_ist": max_ts,
                "silver_path": None if silver_path is None else str(silver_path),
                "silver_sha256": silver_sha,
            }
        )
        _atomic_write_json(manifest_path, manifest)
        rebuilt += 1
        rows_written += len(valid)
        quality_count += len(quality_exceptions)
    return RebuildStats(
        str(root_path),
        len(manifests),
        rebuilt,
        rows_written,
        quality_count,
        failures,
    )


def _manifest_base(cell: RequestCell, started: str, attempts: int) -> dict[str, Any]:
    return {
        "manifest_version": "1.0.0",
        "normalizer_version": SCHEMA_VERSION,
        "request_id": cell.request_id,
        "payload_sha256": cell.request_id,
        "dataset": cell.dataset,
        "endpoint": cell.endpoint,
        "payload": dict(cell.payload),
        "metadata": dict(cell.metadata),
        "started_at_utc": started,
        "attempts": attempts,
        "credentials_persisted": False,
    }


def _resume_outcome(path: Path) -> RequestOutcome | None:
    if not path.is_file():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("normalizer_version") != SCHEMA_VERSION:
        return None
    if payload.get("status") not in {"completed", "completed_empty"}:
        return None
    for path_key, hash_key in (("bronze_path", "bronze_sha256"), ("silver_path", "silver_sha256")):
        artifact = payload.get(path_key)
        expected = payload.get(hash_key)
        if artifact is not None and (not Path(artifact).is_file() or sha256_file(artifact) != expected):
            return None
    return RequestOutcome(
        str(payload["request_id"]),
        str(payload["dataset"]),
        "already_valid",
        int(payload.get("attempts", 0)),
        int(payload.get("rows", 0)),
        payload.get("min_timestamp_ist"),
        payload.get("max_timestamp_ist"),
        payload.get("bronze_path"),
        payload.get("bronze_sha256"),
        payload.get("silver_path"),
        payload.get("silver_sha256"),
        str(path),
    )


def _cached_bronze(path: Path) -> tuple[Mapping[str, Any], Path, str] | None:
    if not path.is_file():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    artifact = payload.get("bronze_path")
    expected = payload.get("bronze_sha256")
    if not artifact or not expected:
        return None
    bronze_path = Path(str(artifact))
    if not bronze_path.is_file() or sha256_file(bronze_path) != expected:
        return None
    response = json.loads(bronze_path.read_text(encoding="utf-8"))
    return (response, bronze_path, str(expected)) if isinstance(response, Mapping) else None


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(path.suffix + ".partial")
    with partial.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, default=_json_default)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(partial, path)


def _atomic_write_parquet(path: Path, rows: list[dict[str, Any]], *, dataset: str) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(path.suffix + ".partial")
    table = pa.Table.from_pylist(rows, schema=_parquet_schema(pa, dataset))
    table = table.replace_schema_metadata(
        {b"schema_version": SCHEMA_VERSION.encode(), b"dataset": dataset.encode()}
    )
    pq.write_table(table, partial)
    with partial.open("r+b") as handle:
        os.fsync(handle.fileno())
    os.replace(partial, path)


def _atomic_write_untyped_parquet(path: Path, rows: list[dict[str, Any]], *, dataset: str) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(path.suffix + ".partial")
    table = pa.Table.from_pylist(rows).replace_schema_metadata(
        {b"schema_version": SCHEMA_VERSION.encode(), b"dataset": dataset.encode()}
    )
    pq.write_table(table, partial)
    with partial.open("r+b") as handle:
        os.fsync(handle.fileno())
    os.replace(partial, path)


def quarantine_orphan_partials(root: str | Path) -> list[dict[str, Any]]:
    """Move interrupted acquisition writes aside before a manifest-safe resume.

    Only acquisition artifact trees are inspected. A ``*.partial`` sibling is
    never authoritative and is never promoted. Existing canonical siblings are
    left untouched and hashed so a conflict remains auditable.
    """
    root_path = Path(root)
    candidates: list[Path] = []
    for relative in ("bronze", "silver", "exceptions/quality"):
        base = root_path / relative
        if base.is_dir():
            candidates.extend(path for path in base.rglob("*.partial") if path.is_file())
    if not candidates:
        return []
    captured_at = _utc_now()
    run_id = captured_at.replace(":", "").replace("-", "").replace("+", "_").replace(".", "_")
    quarantine_root = root_path / "exceptions" / "orphan_partials" / run_id
    records: list[dict[str, Any]] = []
    for source in sorted(candidates):
        relative_source = source.relative_to(root_path)
        canonical = Path(str(source)[: -len(".partial")])
        record = {
            "captured_at_utc": captured_at,
            "status": "noncanonical_interrupted_partial",
            "source_path": str(source),
            "source_relative_path": relative_source.as_posix(),
            "partial_sha256": sha256_file(source),
            "partial_bytes": source.stat().st_size,
            "canonical_path": str(canonical),
            "canonical_exists": canonical.is_file(),
            "canonical_sha256": sha256_file(canonical) if canonical.is_file() else None,
        }
        # Flatten the destination to stay below legacy Windows path limits;
        # the full original relative path remains in the audit record.
        destination = quarantine_root / f"{record['partial_sha256'][:16]}__{source.name}"
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.replace(source, destination)
        record["quarantine_path"] = str(destination)
        records.append(record)
    _atomic_write_json(
        root_path / "manifests" / "orphan_partials" / f"{run_id}.json",
        {
            "captured_at_utc": captured_at,
            "status": "quarantined_noncanonical_partials",
            "count": len(records),
            "records": records,
        },
    )
    return records


def _parquet_schema(pa: Any, dataset: str) -> Any:
    common = [
        pa.field("schema_version", pa.string(), nullable=False),
        pa.field("request_id", pa.string(), nullable=False),
        pa.field("provider", pa.string(), nullable=False),
        pa.field("timestamp_ist", pa.timestamp("us", tz="Asia/Kolkata"), nullable=False),
        pa.field("trade_date", pa.date32(), nullable=False),
        pa.field("session_status", pa.string(), nullable=False),
        pa.field("underlying", pa.string(), nullable=False),
    ]
    ohlc = [
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.int64()),
    ]
    if dataset == "options":
        return pa.schema(
            common
            + [
                pa.field("expiry_date", pa.date32()),
                pa.field("expiry_flag", pa.string(), nullable=False),
                pa.field("expiry_code", pa.int32(), nullable=False),
                pa.field("moneyness_label", pa.string(), nullable=False),
                pa.field("strike", pa.decimal128(18, 4), nullable=False),
                pa.field("option_type", pa.string(), nullable=False),
            ]
            + ohlc
            + [
                pa.field("provider_iv_raw", pa.float64()),
                pa.field("provider_iv_unit", pa.string(), nullable=False),
                pa.field("open_interest", pa.int64()),
                pa.field("provider_spot", pa.float64()),
                pa.field("expiry_resolution_status", pa.string(), nullable=False),
            ]
        )
    extra = [pa.field("security_id", pa.string(), nullable=False)]
    if dataset == "futures":
        extra += [
            pa.field("futures_expiry_text", pa.string()),
            pa.field("series_label", pa.string()),
        ]
    if dataset in {"spot", "india_vix", "futures"}:
        return pa.schema(common + ohlc + extra + [pa.field("open_interest", pa.int64())])
    raise ValueError(f"unsupported acquisition dataset schema: {dataset}")


def _timestamp_bounds(rows: Sequence[Mapping[str, Any]]) -> tuple[str | None, str | None]:
    values = [row.get("timestamp_ist") for row in rows if isinstance(row.get("timestamp_ist"), datetime)]
    if not values:
        return None, None
    return min(values).isoformat(), max(values).isoformat()


def _provider_row_count(cell: RequestCell, response: Mapping[str, Any]) -> int:
    if cell.dataset == "probe":
        return 0
    if cell.dataset == "options":
        root = response.get("data", response)
        if not isinstance(root, Mapping):
            return 0
        side_key = "ce" if str(cell.payload.get("drvOptionType", "")).upper() == "CALL" else "pe"
        arrays = root.get(side_key)
        return len(arrays.get("timestamp", [])) if isinstance(arrays, Mapping) else 0
    timestamps = response.get("timestamp")
    return len(timestamps) if isinstance(timestamps, list) else 0


def _inside_request_window(timestamp_ist: datetime, cell: RequestCell) -> bool:
    raw_from = cell.payload.get("fromDate")
    raw_to = cell.payload.get("toDate")
    if not raw_from or not raw_to:
        return True
    start = _parse_provider_boundary(raw_from)
    end = _parse_provider_boundary(raw_to)
    return start <= timestamp_ist < end


def _parse_provider_boundary(value: Any) -> datetime:
    text = str(value)
    if len(text) == 10:
        text += " 00:00:00"
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)


def _regular_session_status(timestamp_ist: datetime) -> str:
    hhmm = (timestamp_ist.hour, timestamp_ist.minute)
    return "regular_session" if (9, 15) <= hhmm <= (15, 30) else "outside_regular_session"


def _partition_date(rows: Sequence[Mapping[str, Any]], cell: RequestCell) -> date:
    first = rows[0].get("trade_date")
    if isinstance(first, date):
        return first
    raw = str(cell.payload.get("fromDate", ""))[:10]
    return date.fromisoformat(raw)


def _cell_partition_month(cell: RequestCell) -> date:
    raw = str(cell.payload.get("fromDate", ""))[:10]
    if raw:
        return date.fromisoformat(raw)
    return datetime.now(IST).date()


def _provider_error(raw: str) -> tuple[str | None, str]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None, raw[:500]
    if not isinstance(payload, Mapping):
        return None, str(payload)[:500]
    return (
        str(payload.get("errorCode") or payload.get("code") or "") or None,
        str(payload.get("errorMessage") or payload.get("message") or payload)[:500],
    )


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


def _failure_status(error: DhanRequestError) -> str:
    if error.status == 401 or error.code in {"DH-901", "807", "808", "809", "810"}:
        return "credential_blocked"
    if error.status == 429 or error.code in {"DH-904", "805"}:
        return "rate_limited"
    if error.code == "daily_budget":
        return "daily_budget_exhausted"
    return "failed"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _decimal_strike(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.0001"))


def _float_or_none(value: Any) -> float | None:
    return None if value is None or value == "" else float(value)


def _int_or_none(value: Any) -> int | None:
    return None if value is None or value == "" else int(float(value))


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "__dataclass_fields__"):
        return asdict(value)
    raise TypeError(f"not JSON serializable: {type(value).__name__}")
