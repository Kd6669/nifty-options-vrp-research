from __future__ import annotations

import base64
import csv
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import io
import json
import os
from pathlib import Path
import re
import time
from threading import Thread
from typing import Any, Callable, Iterable, Mapping
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


DHAN_API_BASE_URL = "https://api.dhan.co/v2"
DHAN_SCRIP_MASTER_CSV_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
DEFAULT_DHAN_UNDERLYING_SCRIP = 13
DEFAULT_DHAN_UNDERLYING_SEGMENT = "IDX_I"
DEFAULT_DHAN_REST_STREAM = "stream:dhan_rest_nifty_options"
DEFAULT_DHAN_TBT_STREAM = "stream:dhan_tbt_nifty_options"
IST = ZoneInfo("Asia/Kolkata")

PACKET_PARQUET_COLUMNS = (
    "capture_ts",
    "stream",
    "stream_id",
    "packet_kind",
    "provider",
    "source",
    "index",
    "expiry",
    "exchange",
    "symbol",
    "instrument_token",
    "security_id",
    "exchange_segment",
    "option_type",
    "strike",
    "ltp",
    "bid",
    "ask",
    "bid_size",
    "ask_size",
    "oi",
    "volume",
    "spot",
    "india_vix",
    "received_ts",
    "quote_ts",
    "dhan_packet_type",
    "payload_json",
)

_DHAN_EXCHANGE_SEGMENT_BY_ID = {
    0: "IDX_I",
    1: "NSE_EQ",
    2: "NSE_FNO",
    3: "NSE_CURRENCY",
    4: "BSE_EQ",
    5: "MCX_COMM",
    7: "BSE_CURRENCY",
    8: "BSE_FNO",
}


class DhanHttpError(RuntimeError):
    pass


@dataclass(frozen=True)
class DhanCredentials:
    client_id: str
    access_token: str

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "DhanCredentials":
        env = os.environ if environ is None else environ
        token = str(env.get("DHAN_ACCESS_TOKEN") or env.get("DHAN_TOKEN") or "").strip()
        if not token:
            raise ValueError("DHAN_ACCESS_TOKEN is required")
        client_id = str(env.get("DHAN_CLIENT_ID") or "").strip() or (_client_id_from_jwt(token) or "")
        if not client_id:
            raise ValueError("DHAN_CLIENT_ID is required when it cannot be inferred from DHAN_ACCESS_TOKEN")
        return cls(client_id=client_id, access_token=token)


@dataclass(frozen=True)
class InstrumentIdentity:
    index: str
    expiry: str
    strike: float
    option_type: str
    instrument_token: str
    symbol: str
    lot_size: int
    exchange_symbol: str | None = None
    exchange: str | None = None
    segment: str | None = None
    exchange_token: str | None = None

    @property
    def key(self) -> tuple[str, str, float, str]:
        return (self.index.upper(), self.expiry, float(self.strike), self.option_type.upper())


@dataclass(frozen=True)
class InstrumentMaster:
    instruments: dict[tuple[str, str, float, str], InstrumentIdentity]

    @classmethod
    def from_rows(cls, rows: Iterable[InstrumentIdentity]) -> "InstrumentMaster":
        instruments: dict[tuple[str, str, float, str], InstrumentIdentity] = {}
        for identity in rows:
            if identity.key in instruments:
                raise ValueError(f"duplicate instrument identity: {identity.key}")
            instruments[identity.key] = identity
        return cls(instruments=instruments)

    def lookup(self, *, index: str, expiry: str, strike: float, option_type: str) -> InstrumentIdentity:
        key = (index.upper(), str(expiry), float(strike), option_type.upper())
        return self.instruments[key]

    def maybe_lookup(self, *, index: str, expiry: str, strike: float, option_type: str) -> InstrumentIdentity | None:
        key = (index.upper(), str(expiry), float(strike), option_type.upper())
        return self.instruments.get(key)


@dataclass(frozen=True)
class DhanInstrumentCsvLoader:
    url: str = DHAN_SCRIP_MASTER_CSV_URL
    timeout_seconds: float = 20.0

    def load(self, *, indices: tuple[str, ...] = ("NIFTY",), expiries: tuple[str, ...] = ()) -> InstrumentMaster:
        with urlopen(self.url, timeout=self.timeout_seconds) as response:
            text = response.read().decode("utf-8", errors="replace")
        return self.load_from_text(text, indices=indices, expiries=expiries)

    def load_from_text(
        self,
        text: str,
        *,
        indices: tuple[str, ...] = ("NIFTY",),
        expiries: tuple[str, ...] = (),
    ) -> InstrumentMaster:
        wanted_indices = {item.upper() for item in indices}
        wanted_expiries = {str(item) for item in expiries}
        rows: list[InstrumentIdentity] = []
        for raw in csv.DictReader(io.StringIO(text)):
            identity = _identity_from_dhan_csv_row(raw)
            if identity is None:
                continue
            if identity.index.upper() not in wanted_indices:
                continue
            if wanted_expiries and identity.expiry not in wanted_expiries:
                continue
            rows.append(identity)
        return InstrumentMaster.from_rows(rows)


@dataclass(frozen=True)
class DhanOptionChainClient:
    credentials: DhanCredentials
    base_url: str = DHAN_API_BASE_URL

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "DhanOptionChainClient":
        return cls(credentials=DhanCredentials.from_env(environ))

    def expiry_list(
        self,
        *,
        underlying_scrip: int = DEFAULT_DHAN_UNDERLYING_SCRIP,
        underlying_segment: str = DEFAULT_DHAN_UNDERLYING_SEGMENT,
        timeout_seconds: float = 10.0,
    ) -> tuple[str, ...]:
        response = self._post_json(
            "/optionchain/expirylist",
            {"UnderlyingScrip": int(underlying_scrip), "UnderlyingSeg": str(underlying_segment)},
            timeout_seconds=timeout_seconds,
        )
        data = response.get("data")
        if not isinstance(data, list):
            raise ValueError("Dhan expiry-list response missing data[]")
        return tuple(str(item) for item in data if str(item).strip())

    def option_chain(
        self,
        *,
        expiry: str,
        underlying_scrip: int = DEFAULT_DHAN_UNDERLYING_SCRIP,
        underlying_segment: str = DEFAULT_DHAN_UNDERLYING_SEGMENT,
        timeout_seconds: float = 10.0,
    ) -> Mapping[str, Any]:
        return self._post_json(
            "/optionchain",
            {"UnderlyingScrip": int(underlying_scrip), "UnderlyingSeg": str(underlying_segment), "Expiry": str(expiry)},
            timeout_seconds=timeout_seconds,
        )

    def _post_json(self, path: str, payload: Mapping[str, Any], *, timeout_seconds: float) -> Mapping[str, Any]:
        return _post_dhan_json(
            credentials=self.credentials,
            base_url=self.base_url,
            path=path,
            payload=payload,
            timeout_seconds=timeout_seconds,
            include_client_id=True,
        )


@dataclass(frozen=True)
class DhanHistoricalClient:
    credentials: DhanCredentials
    base_url: str = DHAN_API_BASE_URL

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "DhanHistoricalClient":
        return cls(credentials=DhanCredentials.from_env(environ))

    def intraday(
        self,
        *,
        security_id: str,
        exchange_segment: str = "NSE_FNO",
        instrument: str = "OPTIDX",
        interval: str = "1",
        oi: bool = True,
        from_date: str,
        to_date: str,
        timeout_seconds: float = 15.0,
    ) -> Mapping[str, Any]:
        return _post_dhan_json(
            credentials=self.credentials,
            base_url=self.base_url,
            path="/charts/intraday",
            payload={
                "securityId": str(security_id),
                "exchangeSegment": str(exchange_segment),
                "instrument": str(instrument),
                "interval": str(interval),
                "oi": bool(oi),
                "fromDate": str(from_date),
                "toDate": str(to_date),
            },
            timeout_seconds=timeout_seconds,
            include_client_id=False,
        )


@dataclass(frozen=True)
class DhanRestCaptureStats:
    iterations: int
    packets: int
    rows: int
    redis_stream: str
    expiry: str
    spot: float | None
    parquet_rows: int
    parquet_files: tuple[str, ...]
    manifest_path: str | None


@dataclass(frozen=True)
class DhanTbtCaptureStats:
    packets: int
    option_instruments: int
    redis_stream: str
    expiry: str
    spot: float | None
    feed_mode: str
    status: str
    no_update_iterations: int
    parquet_rows: int
    parquet_files: tuple[str, ...]
    manifest_path: str | None
    last_error: str | None = None


@dataclass(frozen=True)
class DhanIntradayFullChainStats:
    output_dir: str
    parquet_path: str
    manifest_path: str
    errors_path: str
    index: str
    expiry: str
    trading_date: str
    from_date: str
    to_date: str
    instruments: int
    completed_instruments: int
    failed_instruments: int
    rows: int


@dataclass(frozen=True)
class RedisParquetExportStats:
    redis_stream: str
    output_dir: str
    prefix: str
    packets: int
    parquet_rows: int
    parquet_files: tuple[str, ...]
    last_stream_id: str | None


@dataclass
class PacketParquetWriter:
    output_dir: Path
    prefix: str
    flush_rows: int = 1_000
    rows: list[dict[str, Any]] = field(default_factory=list)
    written_files: list[Path] = field(default_factory=list)
    total_rows: int = 0
    _part_index: int = 0

    def __post_init__(self) -> None:
        self.output_dir = Path(self.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if self.flush_rows <= 0:
            raise ValueError("flush_rows must be positive")

    def append_packet(
        self,
        *,
        payload: Mapping[str, Any],
        packet_kind: str,
        stream: str | None = None,
        stream_id: str | None = None,
        capture_ts: str | None = None,
    ) -> None:
        self.rows.append(
            packet_capture_row(
                payload,
                packet_kind=packet_kind,
                stream=stream,
                stream_id=stream_id,
                capture_ts=capture_ts,
            )
        )
        if len(self.rows) >= self.flush_rows:
            self.flush()

    def flush(self) -> Path | None:
        if not self.rows:
            return None
        path = self.output_dir / f"{self.prefix}_part_{self._part_index:06d}.parquet"
        self._part_index += 1
        rows = self.rows
        self.rows = []
        _write_rows_parquet(rows, path)
        self.total_rows += len(rows)
        self.written_files.append(path)
        return path

    def close(self) -> None:
        self.flush()

    def manifest_fragment(self) -> dict[str, Any]:
        return {
            "prefix": self.prefix,
            "rows": self.total_rows + len(self.rows),
            "flush_rows": self.flush_rows,
            "files": [str(path) for path in self.written_files],
        }


def fetch_dhan_intraday_full_chain(
    *,
    client: DhanHistoricalClient,
    output_dir: str | Path,
    trading_date: str,
    expiry: str,
    index: str = "NIFTY",
    exchange: str = "NSE",
    exchange_segment: str = "NSE_FNO",
    instrument: str = "OPTIDX",
    interval: str = "1",
    oi: bool = True,
    from_date: str | None = None,
    to_date: str | None = None,
    sleep_seconds: float = 0.75,
    timeout_seconds: float = 15.0,
    max_retries: int = 4,
    limit: int = 0,
    instrument_master: InstrumentMaster | None = None,
) -> DhanIntradayFullChainStats:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    resolved_from = from_date or f"{trading_date} 09:15:00"
    resolved_to = to_date or _default_intraday_to_date(trading_date)
    master = instrument_master or DhanInstrumentCsvLoader().load(indices=(index,), expiries=(expiry,))
    instruments = list(dhan_expiry_instruments(master, index=index, expiry=expiry, exchange=exchange))
    if limit > 0:
        instruments = instruments[: int(limit)]
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    completed = 0
    errors_path = out / "dhan_intraday_1m_errors.jsonl"
    if errors_path.exists():
        errors_path.unlink()
    for number, identity in enumerate(instruments, start=1):
        response = None
        error_text = None
        for attempt in range(max(1, int(max_retries))):
            try:
                response = client.intraday(
                    security_id=identity.instrument_token,
                    exchange_segment=exchange_segment,
                    instrument=instrument,
                    interval=interval,
                    oi=oi,
                    from_date=resolved_from,
                    to_date=resolved_to,
                    timeout_seconds=timeout_seconds,
                )
                break
            except Exception as exc:  # noqa: BLE001 - record per-security API failures and continue.
                error_text = f"{type(exc).__name__}: {exc}"
                if attempt + 1 >= max(1, int(max_retries)):
                    break
                backoff = max(float(sleep_seconds), min(60.0, 5.0 * (attempt + 1)))
                if "429" in error_text or "Too many requests" in error_text:
                    backoff = max(backoff, 30.0)
                time.sleep(backoff)
        if response is None:
            error = _intraday_error(identity, error_text or "unknown error", number=number)
            errors.append(error)
            _append_jsonl(errors_path, error)
            time.sleep(max(0.0, float(sleep_seconds)))
            continue
        instrument_rows = dhan_intraday_rows(
            response,
            identity=identity,
            trading_date=trading_date,
            exchange_segment=exchange_segment,
            instrument=instrument,
            interval=interval,
        )
        rows.extend(instrument_rows)
        completed += 1
        time.sleep(max(0.0, float(sleep_seconds)))
    parquet_path = out / "dhan_intraday_1m_full_chain.parquet"
    _write_rows_parquet(rows, parquet_path)
    manifest_path = out / "dhan_intraday_1m_manifest.json"
    manifest = {
        "capture": "dhan_intraday_1m_full_chain",
        "provider": "dhan",
        "index": index,
        "expiry": expiry,
        "trading_date": trading_date,
        "from_date": resolved_from,
        "to_date": resolved_to,
        "exchange_segment": exchange_segment,
        "instrument": instrument,
        "interval": interval,
        "oi": oi,
        "instruments": len(instruments),
        "completed_instruments": completed,
        "failed_instruments": len(errors),
        "rows": len(rows),
        "parquet_path": str(parquet_path),
        "errors_path": str(errors_path),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    manifest_path.write_text(json.dumps(_jsonable(manifest), indent=2, sort_keys=True), encoding="utf-8")
    return DhanIntradayFullChainStats(
        output_dir=str(out),
        parquet_path=str(parquet_path),
        manifest_path=str(manifest_path),
        errors_path=str(errors_path),
        index=index,
        expiry=expiry,
        trading_date=trading_date,
        from_date=resolved_from,
        to_date=resolved_to,
        instruments=len(instruments),
        completed_instruments=completed,
        failed_instruments=len(errors),
        rows=len(rows),
    )


def capture_dhan_rest_option_chain_to_redis(
    *,
    client: DhanOptionChainClient,
    redis_url: str,
    redis_stream: str = DEFAULT_DHAN_REST_STREAM,
    index: str = "NIFTY",
    expiry: str | None = None,
    exchange: str = "NSE",
    underlying_scrip: int = DEFAULT_DHAN_UNDERLYING_SCRIP,
    underlying_segment: str = DEFAULT_DHAN_UNDERLYING_SEGMENT,
    interval_seconds: float = 3.0,
    iterations: int = 0,
    maxlen: int | None = 100_000,
    parquet_dir: str | Path | None = None,
    parquet_prefix: str = "dhan_rest_poll_packets",
    parquet_flush_rows: int = 1_000,
    timeout_seconds: float = 10.0,
) -> DhanRestCaptureStats:
    if interval_seconds < 3.0:
        raise ValueError("Dhan option-chain API is rate-limited; interval_seconds must be at least 3.0")
    resolved_expiry = expiry or nearest_dhan_expiry(
        client.expiry_list(underlying_scrip=underlying_scrip, underlying_segment=underlying_segment, timeout_seconds=timeout_seconds)
    )
    master = DhanInstrumentCsvLoader().load(indices=(index,), expiries=(resolved_expiry,))
    redis_client = redis_client_from_url(redis_url)
    writer = None if parquet_dir is None else PacketParquetWriter(Path(parquet_dir), prefix=parquet_prefix, flush_rows=parquet_flush_rows)
    count = 0
    packets = 0
    rows_total = 0
    latest_spot: float | None = None
    manifest_path: str | None = None
    try:
        while iterations <= 0 or count < iterations:
            received_ts = datetime.now(timezone.utc).isoformat()
            response = client.option_chain(
                expiry=resolved_expiry,
                underlying_scrip=underlying_scrip,
                underlying_segment=underlying_segment,
                timeout_seconds=timeout_seconds,
            )
            latest_spot = dhan_option_chain_spot(response)
            if latest_spot is not None:
                spot_payload = _dhan_underlying_payload(
                    spot=latest_spot,
                    index=index,
                    expiry=resolved_expiry,
                    exchange=exchange,
                    received_ts=received_ts,
                )
                stream_id = publish_payload_with_client(redis_client, stream=redis_stream, packet_kind="quote", payload=spot_payload, maxlen=maxlen)
                packets += 1
                if writer is not None:
                    writer.append_packet(payload=spot_payload, packet_kind="quote", stream=redis_stream, stream_id=stream_id)
            row_payloads = dhan_option_chain_rows(
                response,
                index=index,
                expiry=resolved_expiry,
                exchange=exchange,
                instrument_master=master,
                received_ts=received_ts,
            )
            for row_payload in row_payloads:
                stream_id = publish_payload_with_client(redis_client, stream=redis_stream, packet_kind="quote", payload=row_payload, maxlen=maxlen)
                packets += 1
                if writer is not None:
                    writer.append_packet(payload=row_payload, packet_kind="quote", stream=redis_stream, stream_id=stream_id)
            rows_total += len(row_payloads)
            count += 1
            if iterations <= 0 or count < iterations:
                time.sleep(interval_seconds)
    finally:
        if writer is not None:
            writer.close()
            manifest_path = str(
                write_capture_manifest(
                    Path(parquet_dir) / f"{parquet_prefix}_manifest.json",
                    {
                        "capture": "dhan_rest_option_chain",
                        "provider": "dhan",
                        "redis_stream": redis_stream,
                        "index": index,
                        "expiry": resolved_expiry,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "parquet": writer.manifest_fragment(),
                    },
                )
            )
    return DhanRestCaptureStats(
        iterations=count,
        packets=packets,
        rows=rows_total,
        redis_stream=redis_stream,
        expiry=resolved_expiry,
        spot=latest_spot,
        parquet_rows=0 if writer is None else writer.total_rows,
        parquet_files=() if writer is None else tuple(str(path) for path in writer.written_files),
        manifest_path=manifest_path,
    )


def capture_dhan_tbt_to_redis(
    *,
    credentials: DhanCredentials,
    redis_url: str,
    redis_stream: str = DEFAULT_DHAN_TBT_STREAM,
    index: str = "NIFTY",
    expiry: str,
    exchange: str = "NSE",
    exchange_segment: str = "NSE_FNO",
    spot: float | None = None,
    ring_width_steps: int = 12,
    strike_step: float = 50.0,
    full_chain: bool = False,
    feed_mode: str = "full",
    iterations: int = 0,
    max_no_update_seconds: float | None = 300.0,
    startup_timeout_seconds: float | None = 30.0,
    maxlen: int | None = 100_000,
    parquet_dir: str | Path | None = None,
    parquet_prefix: str = "dhan_tbt_feed_packets",
    parquet_flush_rows: int = 1_000,
) -> DhanTbtCaptureStats:
    try:
        from dhanhq import DhanContext, MarketFeed  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - depends on operator environment.
        raise RuntimeError("dhanhq package is required for Dhan TBT capture") from exc

    master = DhanInstrumentCsvLoader().load(indices=(index,), expiries=(expiry,))
    resolved_spot = float(spot) if spot is not None else None
    if full_chain:
        instruments = dhan_expiry_instruments(master, index=index, expiry=expiry, exchange=exchange)
    else:
        if resolved_spot is None:
            option_client = DhanOptionChainClient(credentials)
            resolved_spot = _startup_dhan_spot(option_client, expiry=expiry, timeout_seconds=startup_timeout_seconds)
        instruments = option_ring_instruments(
            master,
            index=index,
            expiry=expiry,
            exchange=exchange,
            spot=resolved_spot,
            ring_width_steps=ring_width_steps,
            strike_step=strike_step,
        )
    if not instruments:
        raise ValueError(f"no Dhan option instruments resolved for {index} expiry={expiry}")
    subscription_type = _market_feed_subscription_type(MarketFeed, feed_mode)
    exchange_code = _market_feed_exchange_code(MarketFeed, exchange_segment)
    subscription = [(exchange_code, identity.instrument_token, subscription_type) for identity in instruments]
    identity_by_security = {str(identity.instrument_token): identity for identity in instruments}
    redis_client = redis_client_from_url(redis_url)
    writer = None if parquet_dir is None else PacketParquetWriter(Path(parquet_dir), prefix=parquet_prefix, flush_rows=parquet_flush_rows)
    feed = None
    packets = 0
    no_update_iterations = 0
    status = "running"
    last_error: str | None = None
    manifest_path: str | None = None
    try:
        context = DhanContext(credentials.client_id, credentials.access_token)
        feed = MarketFeed(context, subscription, "v2")
        _call_with_timeout(feed.run_forever, timeout_seconds=startup_timeout_seconds, label="dhan_feed_connect")
        while iterations <= 0 or packets < iterations:
            received_ts = datetime.now(timezone.utc).isoformat()
            try:
                raw_packet = _call_with_timeout(feed.get_data, timeout_seconds=max_no_update_seconds, label="dhan_feed_get_data")
            except TimeoutError as exc:
                no_update_iterations += 1
                status = "no_update"
                last_error = str(exc)
                break
            if not isinstance(raw_packet, Mapping):
                continue
            payload = dhan_feed_packet_payload(
                raw_packet,
                identity_by_security=identity_by_security,
                index=index,
                expiry=expiry,
                exchange=exchange,
                received_ts=received_ts,
            )
            if payload is None:
                continue
            packet_kind = _packet_kind_from_dhan_type(str(payload.get("dhan_packet_type") or payload.get("type") or ""))
            stream_id = publish_payload_with_client(redis_client, stream=redis_stream, packet_kind=packet_kind, payload=payload, maxlen=maxlen)
            packets += 1
            status = "running"
            if writer is not None:
                writer.append_packet(payload=payload, packet_kind=packet_kind, stream=redis_stream, stream_id=stream_id)
        if status == "running":
            status = "stopped" if iterations > 0 else "running"
    except Exception as exc:
        status = "failed"
        last_error = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        if feed is not None:
            _close_dhan_feed(feed)
        if writer is not None:
            writer.close()
            manifest_path = str(
                write_capture_manifest(
                    Path(parquet_dir) / f"{parquet_prefix}_manifest.json",
                    {
                        "capture": "dhan_tbt_market_feed",
                        "provider": "dhan",
                        "redis_stream": redis_stream,
                        "index": index,
                        "expiry": expiry,
                        "feed_mode": feed_mode,
                        "full_chain": full_chain,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "option_instruments": len(instruments),
                        "parquet": writer.manifest_fragment(),
                    },
                )
            )
    return DhanTbtCaptureStats(
        packets=packets,
        option_instruments=len(instruments),
        redis_stream=redis_stream,
        expiry=expiry,
        spot=resolved_spot,
        feed_mode=feed_mode,
        status=status,
        no_update_iterations=no_update_iterations,
        parquet_rows=0 if writer is None else writer.total_rows,
        parquet_files=() if writer is None else tuple(str(path) for path in writer.written_files),
        manifest_path=manifest_path,
        last_error=last_error,
    )


def export_redis_stream_to_parquet(
    *,
    redis_url: str,
    stream: str,
    output_dir: str | Path,
    prefix: str = "redis_packets",
    start_id: str = "0-0",
    max_packets: int = 0,
    block_ms: int = 1_000,
    count: int = 500,
    idle_timeout_seconds: float | None = None,
    flush_rows: int = 1_000,
) -> RedisParquetExportStats:
    client = redis_client_from_url(redis_url)
    writer = PacketParquetWriter(Path(output_dir), prefix=prefix, flush_rows=flush_rows)
    packets = 0
    last_id = start_id
    last_stream_id: str | None = None
    idle_started = time.monotonic()
    try:
        while max_packets <= 0 or packets < max_packets:
            rows = client.xread({stream: last_id}, block=max(1, int(block_ms)), count=max(1, int(count)))
            if not rows:
                if idle_timeout_seconds is not None and time.monotonic() - idle_started >= float(idle_timeout_seconds):
                    break
                continue
            idle_started = time.monotonic()
            for _stream_name, entries in rows:
                for stream_id, fields in entries:
                    packet = decode_redis_stream_packet(stream_id, fields)
                    writer.append_packet(payload=packet["payload"], packet_kind=packet["packet_kind"], stream=stream, stream_id=packet["stream_id"])
                    packets += 1
                    last_id = packet["stream_id"]
                    last_stream_id = packet["stream_id"]
                    if max_packets > 0 and packets >= max_packets:
                        break
                if max_packets > 0 and packets >= max_packets:
                    break
    finally:
        writer.close()
    return RedisParquetExportStats(
        redis_stream=stream,
        output_dir=str(Path(output_dir)),
        prefix=prefix,
        packets=packets,
        parquet_rows=writer.total_rows,
        parquet_files=tuple(str(path) for path in writer.written_files),
        last_stream_id=last_stream_id,
    )


def dhan_option_chain_spot(response: Mapping[str, Any]) -> float | None:
    payload = _dhan_payload(response)
    return _float_first(payload, "last_price", "underlying_ltp", "spot")


def dhan_option_chain_rows(
    response: Mapping[str, Any],
    *,
    index: str,
    expiry: str,
    exchange: str = "NSE",
    instrument_master: InstrumentMaster | None = None,
    received_ts: str | None = None,
) -> tuple[dict[str, Any], ...]:
    payload = _dhan_payload(response)
    option_chain = payload.get("oc")
    if not isinstance(option_chain, Mapping):
        raise ValueError("Dhan option-chain response missing data.oc")
    rows: list[dict[str, Any]] = []
    for strike_key, strike_payload in option_chain.items():
        if not isinstance(strike_payload, Mapping):
            continue
        try:
            strike = float(strike_key)
        except (TypeError, ValueError):
            continue
        for side_key, option_type in (("ce", "CE"), ("pe", "PE")):
            contract = strike_payload.get(side_key) or strike_payload.get(option_type)
            if not isinstance(contract, Mapping):
                continue
            identity = None if instrument_master is None else instrument_master.maybe_lookup(
                index=index,
                expiry=expiry,
                strike=strike,
                option_type=option_type,
            )
            rows.append(
                _dhan_contract_payload(
                    contract,
                    identity=identity,
                    index=index,
                    expiry=expiry,
                    exchange=exchange,
                    strike=strike,
                    option_type=option_type,
                    received_ts=received_ts,
                )
            )
    return tuple(rows)


def dhan_intraday_rows(
    response: Mapping[str, Any],
    *,
    identity: InstrumentIdentity,
    trading_date: str,
    exchange_segment: str,
    instrument: str,
    interval: str,
) -> list[dict[str, Any]]:
    timestamps = response.get("timestamp")
    if not isinstance(timestamps, list):
        return []
    rows: list[dict[str, Any]] = []
    for idx, _timestamp in enumerate(timestamps):
        epoch = _int_at(response, "timestamp", idx)
        timestamp_utc = None if epoch is None else datetime.fromtimestamp(epoch, timezone.utc)
        timestamp_ist = None if timestamp_utc is None else timestamp_utc.astimezone(IST)
        rows.append(
            {
                "provider": "dhan_historical",
                "index": identity.index,
                "expiry": identity.expiry,
                "exchange": identity.exchange,
                "exchange_segment": exchange_segment,
                "instrument": instrument,
                "interval": str(interval),
                "security_id": identity.instrument_token,
                "instrument_token": identity.instrument_token,
                "symbol": identity.symbol,
                "exchange_symbol": identity.exchange_symbol,
                "option_type": identity.option_type,
                "strike": identity.strike,
                "lot_size": identity.lot_size,
                "trade_date": trading_date,
                "timestamp_epoch": epoch,
                "timestamp_utc": None if timestamp_utc is None else timestamp_utc.isoformat(),
                "timestamp_ist": None if timestamp_ist is None else timestamp_ist.isoformat(),
                "open": _float_at(response, "open", idx),
                "high": _float_at(response, "high", idx),
                "low": _float_at(response, "low", idx),
                "close": _float_at(response, "close", idx),
                "volume": _int_at(response, "volume", idx),
                "open_interest": _int_at(response, "open_interest", idx),
            }
        )
    return rows


def dhan_feed_packet_payload(
    raw_packet: Mapping[str, Any],
    *,
    identity_by_security: Mapping[str, InstrumentIdentity],
    index: str,
    expiry: str,
    exchange: str,
    received_ts: str,
) -> dict[str, Any] | None:
    security_id = _text_first(raw_packet, "security_id", "SecurityId")
    if security_id is None:
        return None
    identity = identity_by_security.get(str(security_id))
    payload = dict(raw_packet)
    depth = raw_packet.get("depth")
    bid, bid_size, ask, ask_size = _top_of_depth(depth)
    if bid is not None:
        payload["bid"] = bid
        payload["bid_size"] = bid_size
    if ask is not None:
        payload["ask"] = ask
        payload["ask_size"] = ask_size
    exchange_segment = raw_packet.get("exchange_segment")
    if isinstance(exchange_segment, int):
        payload["exchange_segment_name"] = _DHAN_EXCHANGE_SEGMENT_BY_ID.get(exchange_segment)
    payload.update(
        {
            "provider": "dhan_feed",
            "packet_kind": _packet_kind_from_dhan_type(str(raw_packet.get("type") or "")),
            "quote_source": "dhan_marketfeed",
            "exchange": exchange,
            "index": index,
            "expiry": expiry,
            "security_id": security_id,
            "instrument_token": security_id if identity is None else identity.instrument_token,
            "symbol": None if identity is None else identity.symbol,
            "exchange_symbol": None if identity is None else identity.exchange_symbol,
            "option_type": None if identity is None else identity.option_type,
            "strike": None if identity is None else identity.strike,
            "lot_size": None if identity is None else identity.lot_size,
            "ltp": _float_first(raw_packet, "ltp", "LTP", "last_price"),
            "oi": _int_first(raw_packet, "oi", "OI"),
            "volume": _int_first(raw_packet, "volume"),
            "quote_ts": _text_first(raw_packet, "LTT", "quote_ts"),
            "received_ts": received_ts,
            "dhan_packet_type": _text_first(raw_packet, "type"),
        }
    )
    return payload


def dhan_expiry_instruments(master: InstrumentMaster, *, index: str, expiry: str, exchange: str) -> tuple[InstrumentIdentity, ...]:
    return tuple(
        sorted(
            (
                identity
                for identity in master.instruments.values()
                if identity.index.upper() == index.upper()
                and str(identity.exchange or "").upper() == exchange.upper()
                and identity.expiry == expiry
            ),
            key=lambda item: (item.strike, item.option_type, item.symbol),
        )
    )


def option_ring_instruments(
    master: InstrumentMaster,
    *,
    index: str,
    expiry: str,
    exchange: str,
    spot: float,
    ring_width_steps: int,
    strike_step: float = 50.0,
) -> tuple[InstrumentIdentity, ...]:
    atm = round(float(spot) / strike_step) * strike_step
    min_strike = atm - max(0, int(ring_width_steps)) * strike_step
    max_strike = atm + max(0, int(ring_width_steps)) * strike_step
    return tuple(
        sorted(
            (
                identity
                for identity in master.instruments.values()
                if identity.index.upper() == index.upper()
                and str(identity.exchange or "").upper() == exchange.upper()
                and identity.expiry == expiry
                and min_strike <= identity.strike <= max_strike
            ),
            key=lambda item: (item.strike, item.option_type),
        )
    )


def nearest_dhan_expiry(expiries: Iterable[str], *, today: date | None = None) -> str:
    base = today or date.today()
    future = sorted(expiry for expiry in expiries if _date_or_none(expiry) is not None and _date_or_none(expiry) >= base)
    if not future:
        raise ValueError("Dhan expiry list did not contain a future expiry")
    return future[0]


def packet_capture_row(
    payload: Mapping[str, Any],
    *,
    packet_kind: str,
    stream: str | None = None,
    stream_id: str | None = None,
    capture_ts: str | None = None,
) -> dict[str, Any]:
    raw = dict(payload)
    row: dict[str, Any] = {column: None for column in PACKET_PARQUET_COLUMNS}
    row.update(
        {
            "capture_ts": capture_ts or datetime.now(timezone.utc).isoformat(),
            "stream": stream,
            "stream_id": stream_id,
            "packet_kind": str(raw.get("packet_kind") or packet_kind),
            "provider": _text_first(raw, "provider", "broker", "packet_provider"),
            "source": _text_first(raw, "source", "quote_source", "spot_source", "vix_source"),
            "index": _text_first(raw, "index", "underlying"),
            "expiry": _text_first(raw, "expiry"),
            "exchange": _text_first(raw, "exchange"),
            "symbol": _text_first(raw, "symbol", "trading_symbol", "exchange_symbol"),
            "instrument_token": _text_first(raw, "instrument_token", "token", "exchange_token"),
            "security_id": _text_first(raw, "security_id", "SecurityId"),
            "exchange_segment": _text_first(raw, "exchange_segment", "ExchangeSegment"),
            "option_type": _text_first(raw, "option_type", "right"),
            "strike": _float_first(raw, "strike", "strike_price"),
            "ltp": _float_first(raw, "ltp", "LTP", "last_price"),
            "bid": _float_first(raw, "bid", "bid_price", "top_bid_price"),
            "ask": _float_first(raw, "ask", "ask_price", "top_ask_price"),
            "bid_size": _int_first(raw, "bid_size", "bid_quantity", "top_bid_quantity"),
            "ask_size": _int_first(raw, "ask_size", "ask_quantity", "top_ask_quantity"),
            "oi": _int_first(raw, "oi", "OI", "open_interest"),
            "volume": _int_first(raw, "volume"),
            "spot": _float_first(raw, "spot", "underlying_ltp"),
            "india_vix": _float_first(raw, "india_vix", "vix"),
            "received_ts": _text_first(raw, "received_ts"),
            "quote_ts": _text_first(raw, "quote_ts", "LTT"),
            "dhan_packet_type": _text_first(raw, "dhan_packet_type", "type"),
            "payload_json": json.dumps(_jsonable(raw), separators=(",", ":"), sort_keys=True),
        }
    )
    return row


def redis_client_from_url(redis_url: str) -> Any:
    try:
        import redis  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - depends on operator environment.
        raise RuntimeError("redis package is required") from exc
    return redis.Redis.from_url(redis_url, decode_responses=True)


def publish_payload_with_client(
    client: Any,
    *,
    stream: str,
    payload: Mapping[str, Any],
    packet_kind: str = "quote",
    maxlen: int | None = 100_000,
) -> str:
    fields = {
        "packet_kind": packet_kind,
        "payload": json.dumps(_jsonable(dict(payload)), separators=(",", ":"), sort_keys=True),
    }
    kwargs: dict[str, Any] = {}
    if maxlen is not None and maxlen > 0:
        kwargs["maxlen"] = int(maxlen)
        kwargs["approximate"] = True
    try:
        return str(client.xadd(stream, fields, **kwargs))
    except Exception as exc:
        if "unknown command" not in str(exc).lower():
            raise
        client.rpush(stream, json.dumps(fields, separators=(",", ":"), sort_keys=True))
        if maxlen is not None and maxlen > 0:
            client.ltrim(stream, -int(maxlen), -1)
        return f"list:{client.llen(stream)}"


def decode_redis_stream_packet(stream_id: Any, fields: Mapping[Any, Any]) -> dict[str, Any]:
    decoded = {_decode_text(key): _decode_value(value) for key, value in fields.items()}
    payload = decoded.get("payload")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            payload = {"raw_payload": payload}
    if not isinstance(payload, Mapping):
        payload = dict(decoded)
    packet_kind = str(decoded.get("packet_kind") or payload.get("packet_kind") or "quote").lower()
    return {"stream_id": _decode_text(stream_id), "payload": dict(payload), "packet_kind": packet_kind}


def write_capture_manifest(path: str | Path, payload: Mapping[str, Any]) -> Path:
    manifest_path = Path(path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(_jsonable(dict(payload)), indent=2, sort_keys=True), encoding="utf-8")
    return manifest_path


def _post_dhan_json(
    *,
    credentials: DhanCredentials,
    base_url: str,
    path: str,
    payload: Mapping[str, Any],
    timeout_seconds: float,
    include_client_id: bool,
) -> Mapping[str, Any]:
    url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
    body = json.dumps(dict(payload), separators=(",", ":")).encode("utf-8")
    headers = {"Accept": "application/json", "Content-Type": "application/json", "access-token": credentials.access_token}
    if include_client_id:
        headers["client-id"] = credentials.client_id
    request = Request(url, data=body, method="POST", headers=headers)
    try:
        with urlopen(request, timeout=max(1.0, float(timeout_seconds))) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace") if exc.fp is not None else str(exc)
        raise DhanHttpError(f"Dhan API HTTP {exc.code}: {_redact_token_text(detail)}") from exc
    except URLError as exc:
        raise DhanHttpError(f"Dhan API request failed: {exc.reason}") from exc
    data = json.loads(raw)
    if not isinstance(data, Mapping):
        raise ValueError("Dhan API response must be a JSON object")
    return data


def _identity_from_dhan_csv_row(raw: Mapping[str, str]) -> InstrumentIdentity | None:
    if str(raw.get("SEM_INSTRUMENT_NAME", "")).upper() != "OPTIDX":
        return None
    option_type = str(raw.get("SEM_OPTION_TYPE", "")).upper()
    if option_type not in {"CE", "PE"}:
        return None
    exchange = str(raw.get("SEM_EXM_EXCH_ID", "")).upper()
    if exchange != "NSE":
        return None
    trading_symbol = str(raw.get("SEM_TRADING_SYMBOL", "")).strip()
    index = _index_from_trading_symbol(trading_symbol)
    expiry = _expiry_date_text(raw.get("SEM_EXPIRY_DATE", ""))
    security_id = str(raw.get("SEM_SMST_SECURITY_ID", "")).strip()
    strike = raw.get("SEM_STRIKE_PRICE")
    lot_size = raw.get("SEM_LOT_UNITS")
    if not (index and expiry and security_id and strike and lot_size):
        return None
    return InstrumentIdentity(
        index=index,
        expiry=expiry,
        strike=float(strike),
        option_type=option_type,
        instrument_token=security_id,
        symbol=trading_symbol,
        lot_size=int(float(lot_size)),
        exchange_symbol=f"{exchange}_{trading_symbol}",
        exchange=exchange,
        segment="NSE_FNO",
        exchange_token=security_id,
    )


def _dhan_contract_payload(
    contract: Mapping[str, Any],
    *,
    identity: InstrumentIdentity | None,
    index: str,
    expiry: str,
    exchange: str,
    strike: float,
    option_type: str,
    received_ts: str | None,
) -> dict[str, Any]:
    greeks = contract.get("greeks") if isinstance(contract.get("greeks"), Mapping) else {}
    security_id = _text_first(contract, "security_id")
    return {
        "provider": "dhan_rest",
        "packet_kind": "quote",
        "quote_source": "dhan_option_chain",
        "exchange": exchange,
        "index": index,
        "expiry": expiry,
        "security_id": security_id,
        "instrument_token": security_id if identity is None else identity.instrument_token,
        "symbol": None if identity is None else identity.symbol,
        "exchange_symbol": None if identity is None else identity.exchange_symbol,
        "option_type": option_type,
        "strike": strike,
        "bid": _float_first(contract, "top_bid_price", "bid", "bid_price"),
        "ask": _float_first(contract, "top_ask_price", "ask", "ask_price"),
        "bid_size": _int_first(contract, "top_bid_quantity", "bid_quantity"),
        "ask_size": _int_first(contract, "top_ask_quantity", "ask_quantity"),
        "ltp": _float_first(contract, "last_price", "ltp"),
        "avg_price": _float_first(contract, "average_price", "avg_price"),
        "iv": _float_first(contract, "implied_volatility", "iv"),
        "delta": _float_first(greeks, "delta"),
        "gamma": _float_first(greeks, "gamma"),
        "theta": _float_first(greeks, "theta"),
        "vega": _float_first(greeks, "vega"),
        "oi": _int_first(contract, "oi", "open_interest"),
        "previous_oi": _int_first(contract, "previous_oi"),
        "previous_volume": _int_first(contract, "previous_volume"),
        "previous_close_price": _float_first(contract, "previous_close_price"),
        "volume": _int_first(contract, "volume"),
        "lot_size": None if identity is None else identity.lot_size,
        "received_ts": received_ts,
        "oi_unit_source": "dhan_option_chain_raw_oi",
    }


def _dhan_underlying_payload(*, spot: float, index: str, expiry: str, exchange: str, received_ts: str) -> dict[str, Any]:
    return {
        "packet_kind": "underlying",
        "provider": "dhan_rest",
        "spot_source": "dhan_option_chain",
        "exchange": exchange,
        "index": index,
        "expiry": expiry,
        "symbol": index,
        "instrument_token": f"{index}-SPOT",
        "ltp": float(spot),
        "spot": float(spot),
        "underlying_ltp": float(spot),
        "received_ts": received_ts,
    }


def _startup_dhan_spot(client: DhanOptionChainClient, *, expiry: str, timeout_seconds: float | None) -> float:
    response = client.option_chain(expiry=expiry, timeout_seconds=10.0 if timeout_seconds is None else timeout_seconds)
    spot = dhan_option_chain_spot(response)
    if spot is None:
        raise ValueError("startup Dhan option-chain response did not contain data.last_price; pass --spot explicitly")
    return float(spot)


def _market_feed_subscription_type(market_feed_cls: Any, feed_mode: str) -> int:
    mapping = {
        "ticker": getattr(market_feed_cls, "Ticker", 15),
        "quote": getattr(market_feed_cls, "Quote", 17),
        "full": getattr(market_feed_cls, "Full", 21),
    }
    try:
        return int(mapping[feed_mode.strip().lower()])
    except KeyError as exc:
        raise ValueError("feed_mode must be one of: ticker, quote, full") from exc


def _market_feed_exchange_code(market_feed_cls: Any, exchange_segment: str) -> int:
    mapping = {
        "IDX_I": getattr(market_feed_cls, "IDX", 0),
        "NSE_EQ": getattr(market_feed_cls, "NSE", 1),
        "NSE": getattr(market_feed_cls, "NSE", 1),
        "NSE_FNO": getattr(market_feed_cls, "NSE_FNO", 2),
        "BSE_EQ": getattr(market_feed_cls, "BSE", 4),
        "BSE": getattr(market_feed_cls, "BSE", 4),
        "BSE_FNO": getattr(market_feed_cls, "BSE_FNO", 8),
    }
    try:
        return int(mapping[exchange_segment.strip().upper()])
    except KeyError as exc:
        raise ValueError(f"unsupported Dhan MarketFeed exchange segment: {exchange_segment}") from exc


def _call_with_timeout(call: Callable[[], Any], *, timeout_seconds: float | None, label: str) -> Any:
    if timeout_seconds is None:
        return call()
    result: list[Any] = []
    errors: list[BaseException] = []

    def target() -> None:
        try:
            result.append(call())
        except BaseException as exc:  # noqa: BLE001 - re-raised in caller thread.
            errors.append(exc)

    thread = Thread(target=target, daemon=True, name=f"dhan-data-{label}")
    thread.start()
    thread.join(timeout=max(0.1, float(timeout_seconds)))
    if thread.is_alive():
        raise TimeoutError(f"DHAN_TIMEOUT:{label}:{timeout_seconds:.1f}s")
    if errors:
        raise errors[0]
    return result[0] if result else None


def _close_dhan_feed(feed: Any) -> None:
    close = getattr(feed, "close_connection", None)
    if callable(close):
        try:
            close()
            return
        except Exception:
            return
    disconnect = getattr(feed, "disconnect", None)
    if callable(disconnect):
        try:
            disconnect()
        except Exception:
            return


def _top_of_depth(depth: Any) -> tuple[float | None, int | None, float | None, int | None]:
    if not isinstance(depth, list):
        return None, None, None, None
    for item in depth:
        if not isinstance(item, Mapping):
            continue
        bid = _float_first(item, "bid_price", "bid")
        ask = _float_first(item, "ask_price", "ask")
        bid_size = _int_first(item, "bid_quantity", "bid_size")
        ask_size = _int_first(item, "ask_quantity", "ask_size")
        if bid is not None or ask is not None:
            return bid, bid_size, ask, ask_size
    return None, None, None, None


def _packet_kind_from_dhan_type(packet_type: str) -> str:
    if "ticker" in packet_type.lower():
        return "tick"
    return "quote"


def _dhan_payload(response: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = response.get("data", response.get("payload", response))
    if not isinstance(payload, Mapping):
        raise ValueError("Dhan response data must be an object")
    return payload


def _write_rows_parquet(rows: list[dict[str, Any]], path: Path) -> None:
    try:
        import pyarrow as pa  # type: ignore[import-not-found]
        import pyarrow.parquet as pq  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - depends on operator environment.
        raise RuntimeError("pyarrow is required to write parquet") from exc
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(_jsonable(dict(payload)), separators=(",", ":"), sort_keys=True) + "\n")


def _intraday_error(identity: InstrumentIdentity, error_text: str, *, number: int) -> dict[str, Any]:
    return {
        "number": number,
        "security_id": identity.instrument_token,
        "symbol": identity.symbol,
        "expiry": identity.expiry,
        "strike": identity.strike,
        "option_type": identity.option_type,
        "error": error_text,
        "at": datetime.now(timezone.utc).isoformat(),
    }


def _default_intraday_to_date(trading_date: str) -> str:
    today_ist = datetime.now(IST).date()
    requested = date.fromisoformat(trading_date)
    if requested == today_ist:
        now_ist = datetime.now(IST).replace(second=0, microsecond=0)
        market_close = datetime.combine(requested, datetime.strptime("15:30:00", "%H:%M:%S").time(), tzinfo=IST)
        return min(now_ist, market_close).strftime("%Y-%m-%d %H:%M:%S")
    return f"{trading_date} 15:30:00"


def _index_from_trading_symbol(symbol: str) -> str | None:
    if "-" not in symbol:
        return None
    index = symbol.split("-", 1)[0].strip().upper()
    return index or None


def _expiry_date_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text.split(" ", 1)[0]


def _client_id_from_jwt(token: str) -> str | None:
    parts = token.split(".")
    if len(parts) < 2:
        return None
    payload = parts[1]
    payload += "=" * (-len(payload) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload.encode("ascii"))
        data = json.loads(decoded.decode("utf-8"))
    except Exception:
        return None
    value = data.get("dhanClientId") if isinstance(data, Mapping) else None
    return None if value in (None, "") else str(value)


def _date_or_none(value: str) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _text_first(raw: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = raw.get(key)
        if value not in (None, ""):
            text = str(value).strip()
            return text or None
    return None


def _float_first(raw: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = raw.get(key)
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    return None


def _int_first(raw: Mapping[str, Any], *keys: str) -> int | None:
    value = _float_first(raw, *keys)
    return None if value is None else int(value)


def _float_at(response: Mapping[str, Any], key: str, idx: int) -> float | None:
    value = _item_at(response, key, idx)
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_at(response: Mapping[str, Any], key: str, idx: int) -> int | None:
    value = _float_at(response, key, idx)
    return None if value is None else int(value)


def _item_at(response: Mapping[str, Any], key: str, idx: int) -> Any:
    values = response.get(key)
    if not isinstance(values, list) or idx >= len(values):
        return None
    return values[idx]


def _decode_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _decode_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _redact_token_text(text: str) -> str:
    redacted = str(text)
    redacted = re.sub(
        r"(?i)(authorization)(\s*[:=]\s*[\"']?\s*Bearer\s+)([A-Za-z0-9._~-]+)",
        r"\1\2<redacted>",
        redacted,
    )
    redacted = re.sub(
        r"(?i)(access[-_ ]?token)(\s*[\"']?\s*[:=]\s*[\"']?)([^\s,}\"']+)",
        r"\1\2<redacted>",
        redacted,
    )
    redacted = re.sub(
        r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b",
        "<redacted-jwt>",
        redacted,
    )
    return redacted


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return str(value)
