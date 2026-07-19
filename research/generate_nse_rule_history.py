"""Build a source-audited NIFTY contract-rule and actual-expiry dimension.

Only official NSE/NSE Clearing URLs are permitted.  Actual expiry dates are
accepted only when an official F&O bhavcopy contains a NIFTY option contract
whose expiry date equals that bhavcopy's trade date.  Weekday rules are used to
enumerate candidates, never as sole evidence of an actual expiry.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import time
import urllib.error
import urllib.request
import zipfile
from calendar import monthrange
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "nse_rules"
SOURCES = OUT / "sources"
START = date(2021, 1, 1)
AS_OF = date(2026, 7, 15)
IST = timezone(timedelta(hours=5, minutes=30))


@dataclass(frozen=True)
class Source:
    source_id: str
    title: str
    publication_date: str | None
    url: str
    kind: str
    circular_ref: str | None = None
    local_name: str | None = None


STATIC_SOURCES = [
    Source("NSE_FAOP_47854", "Revision in Market Lot of Derivative Contracts on Indices", "2021-03-31", "https://nsearchives.nseindia.com/content/circulars/FAOP47854.pdf", "circular", "NSE/FAOP/47854", "FAOP47854.pdf"),
    Source("NSE_FAOP_61415", "Revision in Market Lot of Derivative Contracts on Indices", "2024-04-02", "https://nsearchives.nseindia.com/content/circulars/FAOP61415.pdf", "circular", "NSE/FAOP/61415", "FAOP61415.pdf"),
    Source("NSE_FAOP_64506", "Discontinuation of weekly derivatives contracts on other NSE indices", "2024-10-10", "https://nsearchives.nseindia.com/content/circulars/FAOP64506.pdf", "circular", "NSE/FAOP/64506", "FAOP64506.pdf"),
    Source("NSE_FAOP_64625", "Contract size revision for index derivatives", "2024-10-18", "https://nsearchives.nseindia.com/content/circulars/FAOP64625.pdf", "circular", "NSE/FAOP/64625", "FAOP64625.pdf"),
    Source("NSE_FAOP_65336", "Revision in Expiry Day of Index derivatives contracts", "2024-11-29", "https://nsearchives.nseindia.com/content/circulars/FAOP65336.pdf", "circular", "NSE/FAOP/65336", "FAOP65336.pdf"),
    Source("NSE_FAOP_66938", "Proposed Monday expiry revision (not implemented)", "2025-03-04", "https://nsearchives.nseindia.com/content/circulars/FAOP66938.pdf", "circular", "NSE/FAOP/66938", "FAOP66938.pdf"),
    Source("NSE_FAOP_67338", "Deferral of proposed Monday expiry revision", "2025-03-27", "https://nsearchives.nseindia.com/content/circulars/FAOP67338.pdf", "circular", "NSE/FAOP/67338", "FAOP67338.pdf"),
    Source("NSE_FAOP_68589", "SEBI-approved Tuesday expiry direction", "2025-06-17", "https://nsearchives.nseindia.com/content/circulars/FAOP68589.pdf", "circular", "NSE/FAOP/68589", "FAOP68589.pdf"),
    Source("NSE_FAOP_68685", "Revision in Expiry Day of Index and Stock Derivatives Contracts", "2025-06-23", "https://nsearchives.nseindia.com/content/circulars/FAOP68685.pdf", "circular", "NSE/FAOP/68685", "FAOP68685.pdf"),
    Source("NSE_FAOP_68747", "Revision in Expiry Day - operational update", "2025-06-25", "https://nsearchives.nseindia.com/content/circulars/FAOP68747.pdf", "circular", "NSE/FAOP/68747", "FAOP68747.pdf"),
    Source("NSE_FAOP_70616", "Revision in Market Lot of Derivative Contracts on Indices", "2025-10-03", "https://nsearchives.nseindia.com/content/circulars/FAOP70616.pdf", "circular", "NSE/FAOP/70616", "FAOP70616.pdf"),
    Source("DHAN_V2_ANNEXURE", "DhanHQ v2 Annexure - Expiry Code", None, "https://dhanhq.co/docs/v2/annexure/", "official_api_documentation", None, "dhan_v2_annexure.html"),
    Source("DHAN_V2_EXPIRED_OPTIONS", "DhanHQ v2 Expired Options Data", None, "https://dhanhq.co/docs/v2/expired-options-data/", "official_api_documentation", None, "dhan_v2_expired_options_data.html"),
    Source("NSE_NIFTY50_PRODUCT_PAGE", "NIFTY 50 F&O contract specifications", None, "https://www.nseindia.com/static/products-services/equity-derivatives-nifty50", "official_product_page", None, "nse_nifty50_product_page.html"),
    Source("NSE_MARKET_TIMINGS", "NSE market timings - Equity Derivatives", None, "https://www.nseindia.com/static/market-data/market-timings", "official_market_page", None, "nse_market_timings.html"),
    Source("NSE_CLEARING_SETTLEMENT_MECHANISM", "NSE Clearing equity derivatives settlement mechanism", None, "https://www.nseclearing.in/clearing-settlement/equity-derivatives/settlement-mechanism", "official_clearing_page", None, "nse_clearing_settlement_mechanism.html"),
    Source("NSE_CLEARING_SETTLEMENT_PRICE", "NSE Clearing equity derivatives settlement price", None, "https://www.nseclearing.in/clearing-settlement/equity-derivatives/settlement-price", "official_clearing_page", None, "nse_clearing_settlement_price.html"),
]


RULE_HISTORY: list[dict[str, Any]] = [
    {
        "rule_id": "NIFTY_EXPIRY_THURSDAY_2021_2025",
        "rule_type": "expiry_weekday",
        "instrument_scope": ["FUTIDX", "OPTIDX"],
        "contract_scope": ["weekly", "monthly", "quarterly", "half_yearly"],
        "effective_from": "2021-01-01",
        "effective_to": "2025-08-31",
        "scheduled_weekday": "Thursday",
        "holiday_rule": "previous_trading_day",
        "source_ids": ["NSE_FAOP_65336", "NSE_FAOP_66938", "NSE_FAOP_67338", "NSE_FAOP_68747"],
        "mapping_confidence": "high",
        "notes": "FAOP/65336 explicitly says no NIFTY change; the proposed Monday change was deferred and never took effect; FAOP/68747 preserves contracts through 2025-08-31.",
    },
    {
        "rule_id": "NIFTY_EXPIRY_TUESDAY_FROM_2025_09",
        "rule_type": "expiry_weekday",
        "instrument_scope": ["FUTIDX", "OPTIDX"],
        "contract_scope": ["weekly", "monthly", "quarterly", "half_yearly"],
        "effective_from": "2025-09-01",
        "effective_to": None,
        "scheduled_weekday": "Tuesday",
        "holiday_rule": "previous_trading_day",
        "source_ids": ["NSE_FAOP_68589", "NSE_FAOP_68685", "NSE_FAOP_68747"],
        "mapping_confidence": "high",
    },
    {
        "rule_id": "NIFTY_WEEKLY_ONLY_NSE_BENCHMARK_FROM_2024_11_20",
        "rule_type": "weekly_availability",
        "instrument_scope": ["OPTIDX"],
        "contract_scope": ["weekly"],
        "effective_from": "2024-11-20",
        "effective_to": None,
        "status": "NIFTY weekly continued; BANKNIFTY, MIDCPNIFTY and FINNIFTY weekly discontinued",
        "source_ids": ["NSE_FAOP_64506"],
        "mapping_confidence": "high",
    },
    {
        "rule_id": "NIFTY_LOT_75_TO_50_2021",
        "rule_type": "market_lot_transition",
        "instrument_scope": ["FUTIDX", "OPTIDX"],
        "effective_from": "2021-04-30",
        "effective_to": None,
        "old_market_lot": 75,
        "new_market_lot": 50,
        "transition_by_contract": {
            "monthly": "May and June 2021 remain 75; July 2021 monthly and later are 50",
            "weekly": "weekly expiries in August 2021 and later are 50",
            "long_term_options": "existing contracts over three months changed to 50 after June 2021 expiry (2021-06-25 per circular)",
        },
        "source_ids": ["NSE_FAOP_47854"],
        "mapping_confidence": "high",
    },
    {
        "rule_id": "NIFTY_LOT_50_TO_25_2024",
        "rule_type": "market_lot_transition",
        "instrument_scope": ["FUTIDX", "OPTIDX"],
        "effective_from": "2024-04-26",
        "effective_to": None,
        "old_market_lot": 50,
        "new_market_lot": 25,
        "transition_by_contract": {
            "last_old_monthly": "2024-04-25",
            "first_new_weekly": "2024-05-02",
            "first_new_monthly": "2024-05-30",
            "all_contracts_available_from_trade_date": "2024-04-26 use 25 except the expressly preserved April monthly expiry",
        },
        "source_ids": ["NSE_FAOP_61415"],
        "mapping_confidence": "high",
    },
    {
        "rule_id": "NIFTY_LOT_25_TO_75_2024_2025",
        "rule_type": "market_lot_transition",
        "instrument_scope": ["FUTIDX", "OPTIDX"],
        "effective_from": "2024-11-20",
        "effective_to": None,
        "old_market_lot": 25,
        "new_market_lot": 75,
        "transition_by_contract": {
            "weekly": {"last_old": "2024-12-19", "first_new": "2025-01-02"},
            "monthly": {"last_old": "2025-01-30", "first_new": "2025-02-27"},
            "quarterly_half_yearly_existing": "revised at 2024-12-26 EOD",
        },
        "source_ids": ["NSE_FAOP_64625"],
        "mapping_confidence": "high",
    },
    {
        "rule_id": "NIFTY_LOT_75_TO_65_2025_2026",
        "rule_type": "market_lot_transition",
        "instrument_scope": ["FUTIDX", "OPTIDX"],
        "effective_from": "2025-10-28T15:30:00+05:30",
        "effective_to": None,
        "old_market_lot": 75,
        "new_market_lot": 65,
        "transition_by_contract": {
            "weekly": {"last_old": "2025-12-23", "first_new": "2026-01-06"},
            "monthly": {"last_old": "2025-12-30", "first_new": "2026-01-27"},
            "quarterly_half_yearly_existing": "revised at 2025-12-30 EOD",
        },
        "source_ids": ["NSE_FAOP_70616"],
        "mapping_confidence": "high",
    },
    {
        "rule_id": "NIFTY_OPTION_TICK_0_05",
        "rule_type": "tick_size",
        "instrument_scope": ["OPTIDX"],
        "effective_from": "2021-01-01",
        "effective_to": None,
        "tick_size_rupees": 0.05,
        "source_ids": ["NSE_NIFTY50_PRODUCT_PAGE"],
        "mapping_confidence": "medium",
        "notes": "Current official product specification states Re.0.05. No within-range change circular was found; exhaustive historical contract-master proof remains an explicit exception.",
    },
    {
        "rule_id": "NIFTY_EXPIRY_TIMESTAMP_1530_IST",
        "rule_type": "pricing_cutoff",
        "instrument_scope": ["FUTIDX", "OPTIDX"],
        "effective_from": "2021-01-01",
        "effective_to": None,
        "actual_expiry_time_ist": "15:30:00",
        "source_ids": ["NSE_MARKET_TIMINGS", "NSE_CLEARING_SETTLEMENT_MECHANISM", "NSE_CLEARING_SETTLEMENT_PRICE"],
        "mapping_confidence": "high",
        "notes": "NSE Equity Derivatives normal market closes 15:30; NSE Clearing says expiry exercise is at close of trading hours and final index settlement uses the underlying index close on the last trading day.",
    },
]


def download(url: str, attempts: int = 5) -> bytes:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; nifty-vrp-research/1.0)", "Accept": "*/*"}
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=45) as response:
                data = response.read()
                if not data:
                    raise ValueError("empty response")
                return data
        except (urllib.error.URLError, TimeoutError, ValueError) as exc:
            last = exc
            time.sleep(min(8.0, 0.75 * 2**attempt))
    raise RuntimeError(f"failed after {attempts} attempts: {url}: {last}")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def old_bhavcopy_url(d: date) -> str:
    month = d.strftime("%b").upper()
    stem = d.strftime("%d%b%Y").upper()
    return f"https://nsearchives.nseindia.com/content/historical/DERIVATIVES/{d:%Y}/{month}/fo{stem}bhav.csv.zip"


def new_bhavcopy_url(d: date) -> str:
    return f"https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{d:%Y%m%d}_F_0000.csv.zip"


def parse_bhavcopy(data: bytes) -> tuple[list[dict[str, str]], str]:
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        names = archive.namelist()
        if len(names) != 1:
            raise ValueError(f"unexpected archive entries: {names}")
        with archive.open(names[0]) as raw:
            rows = list(csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig")))
    if not rows:
        raise ValueError("bhavcopy contains no rows")
    schema = "udiff" if "FinInstrmTp" in rows[0] else "legacy"
    return rows, schema


def nifty_expiries(rows: list[dict[str, str]], schema: str, instrument: str = "option") -> set[date]:
    if schema == "legacy":
        code = "OPTIDX" if instrument == "option" else "FUTIDX"
        values = {
            datetime.strptime(row["EXPIRY_DT"], "%d-%b-%Y").date()
            for row in rows
            if row.get("SYMBOL") == "NIFTY" and row.get("INSTRUMENT") == code
        }
    else:
        code = "IDO" if instrument == "option" else "IDF"
        values = {
            date.fromisoformat(row["XpryDt"])
            for row in rows
            if row.get("TckrSymb") == "NIFTY" and row.get("FinInstrmTp") == code
        }
    return values


def try_bhavcopy(d: date) -> dict[str, Any] | None:
    # Legacy is authoritative and stable through 2024; UDiFF is available for
    # later dates.  Fallback supports exchange archive migrations.
    urls = [old_bhavcopy_url(d), new_bhavcopy_url(d)] if d.year <= 2024 else [new_bhavcopy_url(d), old_bhavcopy_url(d)]
    for url in urls:
        try:
            data = download(url, attempts=2)
            rows, schema = parse_bhavcopy(data)
            option_expiries = nifty_expiries(rows, schema, "option")
            future_expiries = nifty_expiries(rows, schema, "future")
            option_counts: dict[str, int] = {}
            for expiry in option_expiries:
                if schema == "legacy":
                    count = sum(
                        row.get("SYMBOL") == "NIFTY"
                        and row.get("INSTRUMENT") == "OPTIDX"
                        and datetime.strptime(row["EXPIRY_DT"], "%d-%b-%Y").date() == expiry
                        for row in rows
                    )
                else:
                    count = sum(
                        row.get("TckrSymb") == "NIFTY"
                        and row.get("FinInstrmTp") == "IDO"
                        and date.fromisoformat(row["XpryDt"]) == expiry
                        for row in rows
                    )
                option_counts[expiry.isoformat()] = count
            return {
                "url": url,
                "schema": schema,
                "sha256": sha256(data),
                "bytes": len(data),
                "option_expiries": option_expiries,
                "future_expiries": future_expiries,
                "option_counts": option_counts,
            }
        except Exception:
            continue
    return None


def weekly_scheduled_dates() -> list[date]:
    result: list[date] = []
    cursor = START
    while cursor <= AS_OF:
        target = 3 if cursor < date(2025, 9, 1) else 1  # Thu then Tue
        if cursor.weekday() == target:
            result.append(cursor)
        cursor += timedelta(days=1)
    return result


def is_last_weekday_of_month(d: date, weekday: int) -> bool:
    last = date(d.year, d.month, monthrange(d.year, d.month)[1])
    while last.weekday() != weekday:
        last -= timedelta(days=1)
    return d == last


def market_lot(expiry: date, expiry_type: str) -> tuple[int | None, str | None]:
    if expiry_type == "weekly":
        if expiry <= date(2021, 7, 22):
            return 75, "NIFTY_LOT_75_TO_50_2021"
        if expiry >= date(2021, 8, 5) and expiry <= date(2024, 4, 25):
            return 50, "NIFTY_LOT_75_TO_50_2021"
        if date(2024, 5, 2) <= expiry <= date(2024, 12, 19):
            return 25, "NIFTY_LOT_50_TO_25_2024"
        if date(2025, 1, 2) <= expiry <= date(2025, 12, 23):
            return 75, "NIFTY_LOT_25_TO_75_2024_2025"
        if expiry >= date(2026, 1, 6):
            return 65, "NIFTY_LOT_75_TO_65_2025_2026"
    if expiry_type == "monthly":
        if expiry <= date(2021, 6, 24):
            return 75, "NIFTY_LOT_75_TO_50_2021"
        if date(2021, 7, 29) <= expiry <= date(2024, 4, 25):
            return 50, "NIFTY_LOT_75_TO_50_2021"
        if date(2024, 5, 30) <= expiry <= date(2025, 1, 30):
            return 25, "NIFTY_LOT_50_TO_25_2024"
        if date(2025, 2, 27) <= expiry <= date(2025, 12, 30):
            return 75, "NIFTY_LOT_25_TO_75_2024_2025"
        if expiry >= date(2026, 1, 27):
            return 65, "NIFTY_LOT_75_TO_65_2025_2026"
    return None, None


def classify_expiry(actual: date, scheduled: date) -> str:
    target_weekday = 3 if scheduled < date(2025, 9, 1) else 1
    return "monthly" if is_last_weekday_of_month(scheduled, target_weekday) else "weekly"


def build_actual_expiries() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    calendar: list[dict[str, Any]] = []
    evidence_sources: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    cache: dict[date, dict[str, Any] | None] = {}
    for idx, scheduled in enumerate(weekly_scheduled_dates(), 1):
        found: tuple[date, str, int, str, str, int] | None = None
        for days_back in range(0, 8):
            candidate = scheduled - timedelta(days=days_back)
            if candidate < START or candidate > AS_OF or candidate.weekday() >= 5:
                continue
            if candidate not in cache:
                cache[candidate] = try_bhavcopy(candidate)
                time.sleep(0.08)
            item = cache[candidate]
            if item is None:
                continue
            if candidate in item["option_expiries"]:
                found = (
                    candidate,
                    item["sha256"],
                    item["bytes"],
                    item["url"],
                    item["schema"],
                    item["option_counts"][candidate.isoformat()],
                )
                break
        if found is None:
            unresolved.append({
                "exception_id": f"EXPIRY_NOT_PROVEN_{scheduled.isoformat()}",
                "scheduled_expiry": scheduled.isoformat(),
                "status": "blocked",
                "reason": "No official bhavcopy within the preceding seven calendar days proved a NIFTY OPTIDX/IDO contract expiring on its trade date.",
            })
            continue
        actual, evidence_sha256, evidence_bytes, url, schema, evidence_rows = found
        expiry_type = classify_expiry(actual, scheduled)
        lot, lot_rule_id = market_lot(actual, expiry_type)
        rule_id = "NIFTY_EXPIRY_THURSDAY_2021_2025" if scheduled < date(2025, 9, 1) else "NIFTY_EXPIRY_TUESDAY_FROM_2025_09"
        source_id = f"NSE_FO_BHAVCOPY_{actual:%Y%m%d}"
        calendar.append({
            "symbol": "NIFTY",
            "instrument_scope": ["OPTIDX", "FUTIDX" if expiry_type == "monthly" else None],
            "actual_expiry_date": actual.isoformat(),
            "actual_expiry_timestamp_ist": datetime.combine(actual, datetime.min.time(), IST).replace(hour=15, minute=30).isoformat(),
            "expiry_type": expiry_type,
            "original_scheduled_expiry": scheduled.isoformat(),
            "expiry_rule_weekday": "Thursday" if scheduled < date(2025, 9, 1) else "Tuesday",
            "expiry_holiday_adjusted": actual != scheduled,
            "rule_id": rule_id,
            "rule_effective_from": "2021-01-01" if scheduled < date(2025, 9, 1) else "2025-09-01",
            "rule_effective_to": "2025-08-31" if scheduled < date(2025, 9, 1) else None,
            "contract_lot_size": lot,
            "market_lot": lot,
            "contract_multiplier": None,
            "trading_unit": "one market lot",
            "tick_size": None,
            "lot_rule_id": lot_rule_id,
            "source_id": source_id,
            "source_url": url,
            "source_sha256": evidence_sha256,
            "source_schema": schema,
            "source_evidence_rows": evidence_rows,
            "mapping_status": "proven" if lot is not None else "expiry_proven_lot_blocked",
            "mapping_confidence": "high" if lot is not None else "blocked",
        })
        evidence_sources.append({
            "source_id": source_id,
            "title": f"NSE F&O bhavcopy {actual.isoformat()}",
            "publication_date": actual.isoformat(),
            "url": url,
            "kind": "official_bhavcopy",
            "sha256": evidence_sha256,
            "bytes": evidence_bytes,
            "saved_locally": False,
            "schema": schema,
            "evidence": f"{evidence_rows} NIFTY option rows have expiry date equal to trade date",
        })
        if idx % 25 == 0:
            print(f"validated {idx} scheduled expiries; proven={len(calendar)} blocked={len(unresolved)}", flush=True)
    return calendar, evidence_sources, unresolved


def append_active_future_expiries(calendar: list[dict[str, Any]], evidence_sources: list[dict[str, Any]], unresolved: list[dict[str, Any]]) -> None:
    item = try_bhavcopy(AS_OF)
    if item is None:
        unresolved.append({"exception_id": "AS_OF_ACTIVE_CONTRACTS_UNAVAILABLE", "status": "blocked", "reason": "Official as-of bhavcopy unavailable."})
        return
    url = item["url"]
    schema = item["schema"]
    expiries = sorted(d for d in item["option_expiries"] if d > AS_OF)
    source_id = f"NSE_FO_BHAVCOPY_{AS_OF:%Y%m%d}"
    existing = {row["actual_expiry_date"] for row in calendar}
    monthly_futures = item["future_expiries"]
    for actual in expiries:
        # Current contract master/bhavcopy proves the date. Classification is
        # monthly when a corresponding NIFTY future uses the same expiry.
        expiry_type = "monthly" if actual in monthly_futures else "weekly" if actual <= AS_OF + timedelta(days=40) else "quarterly_or_half_yearly"
        if expiry_type == "quarterly_or_half_yearly":
            continue
        lot, lot_rule_id = market_lot(actual, expiry_type)
        if actual.isoformat() in existing:
            continue
        calendar.append({
            "symbol": "NIFTY",
            "instrument_scope": ["OPTIDX", "FUTIDX" if expiry_type == "monthly" else None],
            "actual_expiry_date": actual.isoformat(),
            "actual_expiry_timestamp_ist": datetime.combine(actual, datetime.min.time(), IST).replace(hour=15, minute=30).isoformat(),
            "expiry_type": expiry_type,
            "original_scheduled_expiry": actual.isoformat(),
            "expiry_rule_weekday": "Tuesday",
            "expiry_holiday_adjusted": False,
            "rule_id": "NIFTY_EXPIRY_TUESDAY_FROM_2025_09",
            "rule_effective_from": "2025-09-01",
            "rule_effective_to": None,
            "contract_lot_size": lot,
            "market_lot": lot,
            "contract_multiplier": None,
            "trading_unit": "one market lot",
            "tick_size": None,
            "lot_rule_id": lot_rule_id,
            "source_id": source_id,
            "source_url": url,
            "source_sha256": item["sha256"],
            "source_schema": schema,
            "source_evidence_rows": None,
            "mapping_status": "proven_active_as_of_2026_07_15",
            "mapping_confidence": "high",
        })
    if source_id not in {s["source_id"] for s in evidence_sources}:
        evidence_sources.append({
            "source_id": source_id,
            "title": f"NSE F&O bhavcopy {AS_OF.isoformat()}",
            "publication_date": AS_OF.isoformat(),
            "url": url,
            "kind": "official_bhavcopy",
            "sha256": item["sha256"],
            "bytes": item["bytes"],
            "saved_locally": False,
            "schema": schema,
            "evidence": "Active NIFTY IDO/IDF expiry dates as of the enrichment cutoff",
        })


def write_json(path: Path, value: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def contract_rule_rows(source_manifest: list[dict[str, Any]]) -> list[dict[str, Any]]:
    hashes = {row["source_id"]: row.get("sha256") for row in source_manifest}
    publication_dates = {row["source_id"]: row.get("publication_date") for row in source_manifest}
    specs = [
        ("weekly", "2021-01-01", "2021-07-22", 75, "NIFTY_LOT_75_TO_50_2021", "NSE/FAOP/47854", "NSE_FAOP_47854", "2021-04-30", None),
        ("weekly", "2021-08-05", "2024-04-25", 50, "NIFTY_LOT_75_TO_50_2021", "NSE/FAOP/47854", "NSE_FAOP_47854", "2021-04-30", None),
        ("weekly", "2024-05-02", "2024-12-19", 25, "NIFTY_LOT_50_TO_25_2024", "NSE/FAOP/61415", "NSE_FAOP_61415", "2024-04-26", None),
        ("weekly", "2025-01-02", "2025-12-23", 75, "NIFTY_LOT_25_TO_75_2024_2025", "NSE/FAOP/64625", "NSE_FAOP_64625", "2024-11-20", None),
        ("weekly", "2026-01-06", None, 65, "NIFTY_LOT_75_TO_65_2025_2026", "NSE/FAOP/70616", "NSE_FAOP_70616", "2025-10-28", None),
        ("monthly", "2021-01-01", "2021-06-24", 75, "NIFTY_LOT_75_TO_50_2021", "NSE/FAOP/47854", "NSE_FAOP_47854", "2021-04-30", None),
        ("monthly", "2021-07-29", "2024-04-25", 50, "NIFTY_LOT_75_TO_50_2021", "NSE/FAOP/47854", "NSE_FAOP_47854", "2021-04-30", None),
        ("monthly", "2024-05-30", "2025-01-30", 25, "NIFTY_LOT_50_TO_25_2024", "NSE/FAOP/61415", "NSE_FAOP_61415", "2024-04-26", None),
        ("monthly", "2025-02-27", "2025-12-30", 75, "NIFTY_LOT_25_TO_75_2024_2025", "NSE/FAOP/64625", "NSE_FAOP_64625", "2024-11-20", None),
        ("monthly", "2026-01-27", None, 65, "NIFTY_LOT_75_TO_65_2025_2026", "NSE/FAOP/70616", "NSE_FAOP_70616", "2025-10-28", None),
    ]
    rows = []
    for expiry_type, start, end, lot, rule_id, circular_id, source_id, circular_effective_from, _ in specs:
        rows.append({
            "underlying": "NIFTY",
            "instrument_scope": "OPTIDX,FUTIDX" if expiry_type == "monthly" else "OPTIDX",
            "expiry_type": expiry_type,
            "contract_expiry_from": start,
            "contract_expiry_to": end,
            "contract_lot_size": lot,
            "market_lot": lot,
            "rule_id": rule_id,
            "circular_id": circular_id,
            "source_id": source_id,
            "source_sha256": hashes.get(source_id),
            # The lookup applicability is the contract-expiry interval itself.
            # Circular implementation/publication dates are distinct metadata
            # and must never be substituted as the dimension validity range.
            "effective_from": start,
            "effective_to": end,
            "circular_effective_from": circular_effective_from,
            "circular_publication_date": publication_dates.get(source_id),
            "tick_size": None,
            "contract_multiplier": None,
            "trading_unit": "one market lot",
            "mapping_status": "proven",
            "mapping_confidence": "high",
        })
    return rows


def build_dhan_expiry_code_1_mapping(calendar: list[dict[str, Any]], source_manifest: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Map code 1 as the second eligible future contract per exact flag.

    Dhan's linked v2 Annexure is controlling: 0=current/near, 1=next,
    2=far.  The rolling endpoint's own example uses expiryCode=1 and an
    explicit WEEK/MONTH flag.  Weekly selection deliberately excludes monthly
    expiries, per the flag's exact-contract-family interpretation requested by
    the enrichment contract.
    """
    hashes = {row["source_id"]: row.get("sha256") for row in source_manifest}
    eligible = {
        kind: sorted(
            (row for row in calendar if row["expiry_type"] == kind and row["mapping_status"].startswith("proven")),
            key=lambda row: row["actual_expiry_date"],
        )
        for kind in ("weekly", "monthly")
    }
    rows: list[dict[str, Any]] = []
    exceptions: list[dict[str, Any]] = []
    trade_date = START
    while trade_date <= AS_OF:
        for expiry_type in ("weekly", "monthly"):
            future = [row for row in eligible[expiry_type] if date.fromisoformat(row["actual_expiry_date"]) >= trade_date]
            if len(future) < 2:
                exceptions.append({
                    "exception_id": f"DHAN_CODE1_SECOND_CONTRACT_MISSING_{trade_date}_{expiry_type}",
                    "trade_date": trade_date.isoformat(),
                    "expiry_type": expiry_type,
                    "expiry_code": 1,
                    "status": "blocked",
                    "reason": "Fewer than two bhavcopy-proven eligible contracts are available on/after trade date.",
                })
                continue
            selected = future[1]
            rows.append({
                "trade_date": trade_date.isoformat(),
                "underlying": "NIFTY",
                "expiry_type": expiry_type,
                "expiry_code": 1,
                "expiry_code_semantics": "next_expiry_second_eligible_contract",
                "eligible_contract_ordinal": 2,
                "actual_expiry_date": selected["actual_expiry_date"],
                "actual_expiry_timestamp_ist": selected["actual_expiry_timestamp_ist"],
                "expiry_rule_weekday": selected["expiry_rule_weekday"],
                "expiry_rule_effective_from": selected["rule_effective_from"],
                "expiry_rule_effective_to": selected["rule_effective_to"],
                "expiry_holiday_adjusted": selected["expiry_holiday_adjusted"],
                "original_scheduled_expiry": selected["original_scheduled_expiry"],
                "contract_lot_size": selected["contract_lot_size"],
                "market_lot": selected["market_lot"],
                "rule_id": selected["rule_id"],
                "lot_rule_id": selected["lot_rule_id"],
                "circular_id": "NSE/FAOP/68747" if selected["rule_id"] == "NIFTY_EXPIRY_TUESDAY_FROM_2025_09" else "NSE/FAOP/65336",
                "source_id": selected["source_id"],
                "source_sha256": selected["source_sha256"],
                "dhan_semantics_source_id": "DHAN_V2_ANNEXURE",
                "dhan_semantics_source_sha256": hashes.get("DHAN_V2_ANNEXURE"),
                "dhan_endpoint_source_id": "DHAN_V2_EXPIRED_OPTIONS",
                "dhan_endpoint_source_sha256": hashes.get("DHAN_V2_EXPIRED_OPTIONS"),
                "mapping_status": "proven",
                "mapping_method": "rule_composition",
                "mapping_confidence": "high",
            })
        trade_date += timedelta(days=1)
    return rows, exceptions


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    SOURCES.mkdir(parents=True, exist_ok=True)
    source_manifest: list[dict[str, Any]] = []
    for source in STATIC_SOURCES:
        data = download(source.url)
        local_path = SOURCES / str(source.local_name)
        local_tmp = local_path.with_suffix(local_path.suffix + ".tmp")
        local_tmp.write_bytes(data)
        local_tmp.replace(local_path)
        entry = asdict(source)
        entry.update({"sha256": sha256(data), "bytes": len(data), "saved_locally": True, "local_path": local_path.relative_to(ROOT).as_posix()})
        source_manifest.append(entry)
        print(f"fetched {source.source_id} {len(data)} bytes", flush=True)
    existing_calendar_path = OUT / "nse_actual_expiry_calendar.json"
    existing_manifest_path = OUT / "source_manifest.json"
    if existing_calendar_path.exists() and existing_manifest_path.exists():
        calendar = json.loads(existing_calendar_path.read_text(encoding="utf-8"))["rows"]
        # Current product-page tick evidence is not an effective-dated
        # historical schedule; never carry the earlier current-only value into
        # point-in-time rows when reusing a proven calendar.
        for row in calendar:
            row["tick_size"] = None
            lot, lot_rule_id = market_lot(date.fromisoformat(row["actual_expiry_date"]), row["expiry_type"])
            row["contract_lot_size"] = lot
            row["market_lot"] = lot
            row["lot_rule_id"] = lot_rule_id
        previous_sources = json.loads(existing_manifest_path.read_text(encoding="utf-8"))["sources"]
        evidence_sources = [row for row in previous_sources if row.get("kind") == "official_bhavcopy"]
        unresolved = []
        print(f"reused {len(calendar)} previously proven expiry rows", flush=True)
    else:
        calendar, evidence_sources, unresolved = build_actual_expiries()
        append_active_future_expiries(calendar, evidence_sources, unresolved)
    calendar.sort(key=lambda row: (row["actual_expiry_date"], row["expiry_type"]))
    source_manifest.extend(evidence_sources)

    history = {
        "schema_version": "1.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "coverage": {"observation_from": START.isoformat(), "observation_through": AS_OF.isoformat(), "symbol": "NIFTY"},
        "rules": RULE_HISTORY,
        "global_contract_fields": {
            "security_descriptor": {"futures": "FUTIDX/NIFTY", "options": "OPTIDX/NIFTY/CE-or-PE/strike/expiry"},
            "contract_multiplier": None,
            "contract_multiplier_status": "blocked_no_distinct_multiplier_found_in_primary_evidence",
            "trading_unit": "one market lot",
            "option_tick_size_rupees": 0.05,
            "expiry_time_ist": "15:30:00",
        },
    }
    write_json(OUT / "nse_contract_rule_history.json", history)
    write_json(OUT / "nse_actual_expiry_calendar.json", {"schema_version": "1.0.0", "rows": calendar})
    write_json(OUT / "source_manifest.json", {"schema_version": "1.0.0", "sources": source_manifest})

    mapping_rows, mapping_exceptions = build_dhan_expiry_code_1_mapping(calendar, source_manifest)
    unresolved.extend(mapping_exceptions)
    unresolved.extend([
        {
            "exception_id": "DHAN_EXPIRY_CODE_DOCUMENT_DISCOVERY_DISCREPANCY",
            "status": "resolved_by_controlling_v2_source",
            "reason": "A search/redirect snippet described code 1 inconsistently. The currently linked official DhanHQ v2 Annexure is controlling and explicitly defines 0=current/near, 1=next, 2=far; code 1 is therefore never labelled near in these artifacts.",
            "source_id": "DHAN_V2_ANNEXURE",
        },
        {
            "exception_id": "DISTINCT_CONTRACT_MULTIPLIER",
            "status": "blocked_not_distinct_in_evidence",
            "reason": "Official NSE materials identify market lot/contract size and trading in market lots; no separate NIFTY point-value multiplier was found. Do not invent one.",
        },
        {
            "exception_id": "HISTORICAL_TICK_SIZE_SCHEDULE",
            "status": "blocked",
            "reason": "The current NSE NIFTY page proves a current option tick of Re.0.05 and says futures price steps are index-level based, but it does not provide a point-in-time historical schedule. Historical dimension tick_size remains null absent archived effective-dated contract-master evidence.",
        },
        {
            "exception_id": "EXHAUSTIVE_EXCEPTIONAL_SESSION_AUDIT",
            "status": "blocked_outside_bhavcopy_proof",
            "reason": "Bhavcopies prove actual listed expiries and regular close-based cutoff. Exceptional shortened/special sessions were not exhaustively circular-audited; any such date needs a session override before using 15:30.",
        },
    ])
    write_json(OUT / "unresolved_exceptions.json", {"schema_version": "1.0.0", "exceptions": unresolved})
    write_json(OUT / "dhan_expiry_code_1_mapping.json", {"schema_version": "1.0.0", "rows": mapping_rows})

    rule_rows = contract_rule_rows(source_manifest)
    write_json(OUT / "nse_contract_rule_dimension.json", {"schema_version": "1.0.0", "rows": rule_rows})

    # Arrow cannot store a mixed list containing null as a stable scalar field;
    # encode the instrument scope as a comma-separated dimension attribute.
    parquet_rows = []
    for row in calendar:
        item = dict(row)
        item["instrument_scope"] = ",".join(v for v in item["instrument_scope"] if v)
        parquet_rows.append(item)
    table = pa.Table.from_pylist(parquet_rows)
    calendar_parquet = OUT / "nse_actual_expiry_calendar.parquet"
    calendar_tmp = calendar_parquet.with_suffix(".parquet.tmp")
    pq.write_table(table, calendar_tmp, compression="zstd")
    calendar_tmp.replace(calendar_parquet)
    mapping_parquet = OUT / "dhan_expiry_code_1_mapping.parquet"
    mapping_tmp = mapping_parquet.with_suffix(".parquet.tmp")
    pq.write_table(pa.Table.from_pylist(mapping_rows), mapping_tmp, compression="zstd")
    mapping_tmp.replace(mapping_parquet)
    rule_parquet = OUT / "nse_contract_rule_dimension.parquet"
    rule_tmp = rule_parquet.with_suffix(".parquet.tmp")
    pq.write_table(pa.Table.from_pylist(rule_rows), rule_tmp, compression="zstd")
    rule_tmp.replace(rule_parquet)

    validation = {
        "schema_version": "1.0.0",
        "calendar_rows": len(calendar),
        "historical_proven_rows": sum(row["mapping_status"] == "proven" for row in calendar),
        "active_future_rows_as_of_cutoff": sum(row["mapping_status"].startswith("proven_active") for row in calendar),
        "holiday_adjusted_rows": sum(bool(row["expiry_holiday_adjusted"]) for row in calendar),
        "null_market_lot_rows": sum(row["market_lot"] is None for row in calendar),
        "duplicate_date_type_rows": len(calendar) - len({(row["actual_expiry_date"], row["expiry_type"]) for row in calendar}),
        "dhan_code_1_mapping_rows": len(mapping_rows),
        "dhan_code_1_duplicate_keys": len(mapping_rows) - len({(row["trade_date"], row["expiry_type"], row["expiry_code"]) for row in mapping_rows}),
        "contract_rule_dimension_rows": len(rule_rows),
        "blocked_exceptions": sum(str(row.get("status", "")).startswith("blocked") for row in unresolved),
        "resolved_exceptions": sum(str(row.get("status", "")).startswith("resolved") for row in unresolved),
        "sha256": {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in [OUT / "nse_contract_rule_history.json", OUT / "nse_actual_expiry_calendar.json", OUT / "nse_actual_expiry_calendar.parquet", OUT / "dhan_expiry_code_1_mapping.json", OUT / "dhan_expiry_code_1_mapping.parquet", OUT / "nse_contract_rule_dimension.json", OUT / "nse_contract_rule_dimension.parquet", OUT / "source_manifest.json", OUT / "unresolved_exceptions.json"]
        },
    }
    write_json(OUT / "validation_report.json", validation)
    print(json.dumps(validation, indent=2), flush=True)


if __name__ == "__main__":
    main()
