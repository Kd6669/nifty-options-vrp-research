#!/usr/bin/env python3
"""Bounded, read-only NSE SPAN archive availability probe.

The probe never extracts or mutates raw SPAN data. It validates the generated
outer response ZIP and each returned inner SPAN ZIP entirely in memory.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import re
import sys
import tempfile
import time
from typing import Any, Mapping, Sequence
import xml.etree.ElementTree as ET
import zipfile


# Resolve this checkout's package even when another robs_live is installed.
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPOSITORY_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from nifty_span.span.downloader import API_URL, ARCHIVES_PAYLOAD, HEADERS, HOME_URL  # noqa: E402


SUFFIX_TO_SLOT = {
    "i1": "BOD",
    "i2": "ID1",
    "i3": "ID2",
    "i4": "ID3",
    "i5": "ID4",
    "s": "EOD",
}
EXPECTED_SUFFIXES = tuple(SUFFIX_TO_SLOT)
OUTER_NAME_RE = re.compile(r"^nsccl\.(?P<date>\d{8})\.(?P<suffix>i[1-5]|s)\.zip$")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def request_params(trading_date: date) -> dict[str, str]:
    return {
        "archives": ARCHIVES_PAYLOAD,
        "date": trading_date.strftime("%d-%b-%Y"),
        "type": "Archives",
    }


def inspect_response(
    *,
    trading_date: date,
    status_code: int,
    headers: Mapping[str, Any],
    body: bytes,
    requested_utc: str,
    finished_utc: str,
    elapsed_seconds: float,
    preview_bytes: int = 500,
) -> dict[str, Any]:
    """Return deterministic validation evidence for one NSE response."""
    expected_date_tag = trading_date.strftime("%Y%m%d")
    normalized_headers = {str(key).lower(): str(value) for key, value in headers.items()}
    result: dict[str, Any] = {
        "trading_date": trading_date.isoformat(),
        "requested_utc": requested_utc,
        "finished_utc": finished_utc,
        "elapsed_seconds": round(float(elapsed_seconds), 6),
        "request": {"method": "GET", "url": API_URL, "params": request_params(trading_date)},
        "response": {
            "status": int(status_code),
            "content_type": normalized_headers.get("content-type"),
            "content_disposition": normalized_headers.get("content-disposition"),
            "content_length_header": normalized_headers.get("content-length"),
            "bytes": len(body),
            "sha256": hashlib.sha256(body).hexdigest(),
            "magic_hex": body[:16].hex(),
            "is_zip": zipfile.is_zipfile(io.BytesIO(body)),
        },
        "outer_zip": None,
        "returned_suffixes": [],
        "missing_suffixes": list(EXPECTED_SUFFIXES),
        "returned_slots": [],
        "missing_slots": [SUFFIX_TO_SLOT[suffix] for suffix in EXPECTED_SUFFIXES],
        "valid_suffixes": [],
        "invalid_suffixes": [],
        "validation_errors": [],
    }

    if not result["response"]["is_zip"]:
        preview = body[: max(0, int(preview_bytes))]
        result["response"]["preview_bytes"] = len(preview)
        result["response"]["preview_utf8"] = preview.decode("utf-8", "replace")
        return result

    outer_evidence: dict[str, Any] = {"valid": True, "testzip_bad_member": None, "members": []}
    result["outer_zip"] = outer_evidence
    returned_suffixes: list[str] = []
    valid_suffixes: list[str] = []
    invalid_suffixes: list[str] = []
    errors: list[str] = result["validation_errors"]

    try:
        with zipfile.ZipFile(io.BytesIO(body)) as outer:
            try:
                bad_member = outer.testzip()
                outer_evidence["testzip_bad_member"] = bad_member
                if bad_member is not None:
                    outer_evidence["valid"] = False
                    errors.append(f"outer ZIP CRC failure: {bad_member}")
            except Exception as exc:  # pragma: no cover - defensive around stdlib implementation.
                outer_evidence["valid"] = False
                outer_evidence["testzip_error"] = _error_text(exc)
                errors.append(f"outer ZIP integrity error: {_error_text(exc)}")

            seen_suffixes: set[str] = set()
            for info in outer.infolist():
                member_evidence = _inspect_outer_member(
                    outer=outer,
                    info=info,
                    expected_date_tag=expected_date_tag,
                )
                outer_evidence["members"].append(member_evidence)
                suffix = member_evidence.get("suffix")
                if suffix in SUFFIX_TO_SLOT and member_evidence["name_valid"] and member_evidence["date_matches"]:
                    if suffix not in seen_suffixes:
                        returned_suffixes.append(suffix)
                        seen_suffixes.add(suffix)
                    else:
                        errors.append(f"duplicate returned suffix {suffix}: {info.filename}")
                for error in member_evidence["validation_errors"]:
                    errors.append(f"{info.filename}: {error}")
                if suffix in SUFFIX_TO_SLOT and member_evidence["name_valid"] and member_evidence["date_matches"]:
                    target = valid_suffixes if member_evidence["valid"] else invalid_suffixes
                    if suffix not in target:
                        target.append(suffix)
    except Exception as exc:
        outer_evidence["valid"] = False
        outer_evidence["open_error"] = _error_text(exc)
        errors.append(f"outer ZIP open error: {_error_text(exc)}")

    outer_evidence["valid"] = bool(outer_evidence["valid"] and not errors)
    result["returned_suffixes"] = _canonical_suffixes(returned_suffixes)
    result["missing_suffixes"] = [suffix for suffix in EXPECTED_SUFFIXES if suffix not in returned_suffixes]
    result["returned_slots"] = [SUFFIX_TO_SLOT[suffix] for suffix in result["returned_suffixes"]]
    result["missing_slots"] = [SUFFIX_TO_SLOT[suffix] for suffix in result["missing_suffixes"]]
    result["valid_suffixes"] = _canonical_suffixes(valid_suffixes)
    result["invalid_suffixes"] = _canonical_suffixes(invalid_suffixes)
    return result


def _inspect_outer_member(
    *,
    outer: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    expected_date_tag: str,
) -> dict[str, Any]:
    safe_basename = _is_safe_basename(info.filename)
    name_match = OUTER_NAME_RE.fullmatch(info.filename) if safe_basename else None
    suffix = name_match.group("suffix") if name_match else None
    date_matches = bool(name_match and name_match.group("date") == expected_date_tag)
    evidence: dict[str, Any] = {
        "name": info.filename,
        "compressed_bytes": info.compress_size,
        "uncompressed_bytes": info.file_size,
        "crc32": f"{info.CRC:08x}",
        "safe_basename": safe_basename,
        "name_valid": name_match is not None,
        "date_matches": date_matches,
        "suffix": suffix,
        "slot": SUFFIX_TO_SLOT.get(suffix) if suffix else None,
        "valid": False,
        "validation_errors": [],
        "inner_zip": None,
    }
    errors: list[str] = evidence["validation_errors"]
    if not safe_basename:
        errors.append("outer member is not a safe basename")
    elif name_match is None:
        errors.append("outer member name does not match nsccl.YYYYMMDD.(i1..i5|s).zip")
    elif not date_matches:
        errors.append(f"outer member date does not match requested {expected_date_tag}")

    try:
        inner_bytes = outer.read(info)
    except Exception as exc:
        errors.append(f"outer member read error: {_error_text(exc)}")
        return evidence

    inner_evidence: dict[str, Any] = {
        "bytes": len(inner_bytes),
        "sha256": hashlib.sha256(inner_bytes).hexdigest(),
        "magic_hex": inner_bytes[:8].hex(),
        "is_zip": zipfile.is_zipfile(io.BytesIO(inner_bytes)),
        "testzip_bad_member": None,
        "spn_members": [],
        "valid": False,
    }
    evidence["inner_zip"] = inner_evidence
    if not inner_evidence["is_zip"]:
        errors.append("inner member is not a ZIP")
        return evidence

    try:
        with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner:
            try:
                bad_member = inner.testzip()
                inner_evidence["testzip_bad_member"] = bad_member
                if bad_member is not None:
                    errors.append(f"inner ZIP CRC failure: {bad_member}")
            except Exception as exc:  # pragma: no cover - defensive around stdlib implementation.
                inner_evidence["testzip_error"] = _error_text(exc)
                errors.append(f"inner ZIP integrity error: {_error_text(exc)}")

            spn_infos = [candidate for candidate in inner.infolist() if candidate.filename.lower().endswith(".spn")]
            if not spn_infos:
                errors.append("inner ZIP has no .spn member")
            expected_spn = _expected_spn_name(expected_date_tag, suffix) if suffix else None
            for spn_info in spn_infos:
                spn_evidence = _inspect_spn_member(inner, spn_info, expected_spn)
                inner_evidence["spn_members"].append(spn_evidence)
                for error in spn_evidence["validation_errors"]:
                    errors.append(f"{spn_info.filename}: {error}")
            if len(spn_infos) > 1:
                errors.append(f"inner ZIP has {len(spn_infos)} .spn members; expected exactly one")
    except Exception as exc:
        inner_evidence["open_error"] = _error_text(exc)
        errors.append(f"inner ZIP open error: {_error_text(exc)}")

    spn_members = inner_evidence["spn_members"]
    inner_evidence["valid"] = (
        inner_evidence["testzip_bad_member"] is None
        and len(spn_members) == 1
        and bool(spn_members[0]["valid"])
        and not any(error.startswith("inner ZIP") for error in errors)
    )
    evidence["valid"] = bool(
        evidence["name_valid"]
        and evidence["date_matches"]
        and inner_evidence["valid"]
        and not errors
    )
    return evidence


def _inspect_spn_member(
    inner: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    expected_name: str | None,
) -> dict[str, Any]:
    safe_basename = _is_safe_basename(info.filename)
    evidence: dict[str, Any] = {
        "name": info.filename,
        "compressed_bytes": info.compress_size,
        "uncompressed_bytes": info.file_size,
        "crc32": f"{info.CRC:08x}",
        "safe_basename": safe_basename,
        "expected_name": expected_name,
        "name_matches_expected": bool(expected_name and info.filename.lower() == expected_name.lower()),
        "valid": False,
        "validation_errors": [],
    }
    errors: list[str] = evidence["validation_errors"]
    if not safe_basename:
        errors.append(".spn member is not a safe basename")
    if expected_name and not evidence["name_matches_expected"]:
        errors.append(f".spn name does not match expected {expected_name}")
    try:
        raw = inner.read(info)
        evidence["sha256"] = hashlib.sha256(raw).hexdigest()
        evidence.update(_parse_nifty_counts(raw))
    except Exception as exc:
        evidence["parse_error"] = _error_text(exc)
        errors.append(f".spn read/XML error: {_error_text(exc)}")
    evidence["valid"] = bool(not errors and evidence.get("xml_root") == "spanFile")
    return evidence


def _parse_nifty_counts(raw: bytes) -> dict[str, Any]:
    root = ET.fromstring(raw)
    counts = {"FUT": 0, "CE": 0, "PE": 0}
    fut_pf_count = 0
    oop_pf_count = 0
    for fut_pf in root.iter("futPf"):
        if (fut_pf.findtext("pfCode") or "").strip().upper() != "NIFTY":
            continue
        fut_pf_count += 1
        counts["FUT"] += len(fut_pf.findall("fut"))
    for oop_pf in root.iter("oopPf"):
        if (oop_pf.findtext("pfCode") or "").strip().upper() != "NIFTY":
            continue
        oop_pf_count += 1
        for series in oop_pf.findall("series"):
            for option in series.findall("opt"):
                raw_type = (option.findtext("o") or "").strip().upper()
                counts["CE" if raw_type.startswith("C") else "PE"] += 1
    return {
        "xml_root": str(root.tag),
        "nifty_fut_pf": fut_pf_count,
        "nifty_oop_pf": oop_pf_count,
        "nifty_counts": counts,
        "nifty_total_rows": sum(counts.values()),
    }


def _expected_spn_name(date_tag: str, suffix: str | None) -> str | None:
    if suffix is None:
        return None
    inner_suffix = f"i0{suffix[1]}" if suffix.startswith("i") else suffix
    return f"nsccl.{date_tag}.{inner_suffix}.spn"


def _is_safe_basename(name: str) -> bool:
    if not name or name in {".", ".."}:
        return False
    return (
        PurePosixPath(name).name == name
        and PureWindowsPath(name).name == name
        and "/" not in name
        and "\\" not in name
    )


def _canonical_suffixes(values: Sequence[str]) -> list[str]:
    found = set(values)
    return [suffix for suffix in EXPECTED_SUFFIXES if suffix in found]


def _error_text(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def run_probe(
    *,
    trading_dates: Sequence[date],
    delay_seconds: float,
    timeout_seconds: float,
    preview_bytes: int,
) -> dict[str, Any]:
    try:
        from curl_cffi.requests import Session  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - dependency belongs to runtime.
        raise RuntimeError("curl_cffi is required for NSE SPAN probes") from exc

    run_requested = utc_now()
    run_started_perf = time.perf_counter()
    warmup_requested = utc_now()
    warmup_started_perf = time.perf_counter()
    with Session(impersonate="chrome124", headers=HEADERS) as session:
        warmup_response = session.get(HOME_URL, timeout=timeout_seconds)
        warmup_finished = utc_now()
        warmup = {
            "requested_utc": iso_utc(warmup_requested),
            "finished_utc": iso_utc(warmup_finished),
            "elapsed_seconds": round(time.perf_counter() - warmup_started_perf, 6),
            "status": int(getattr(warmup_response, "status_code", 0) or 0),
            "content_type": getattr(warmup_response, "headers", {}).get("content-type"),
            "bytes": len(bytes(getattr(warmup_response, "content", b"") or b"")),
        }
        probes: list[dict[str, Any]] = []
        for index, trading_date in enumerate(trading_dates):
            if index:
                time.sleep(delay_seconds)
            requested = utc_now()
            started_perf = time.perf_counter()
            try:
                response = session.get(API_URL, params=request_params(trading_date), timeout=timeout_seconds)
                finished = utc_now()
                probes.append(
                    inspect_response(
                        trading_date=trading_date,
                        status_code=int(getattr(response, "status_code", 0) or 0),
                        headers=getattr(response, "headers", {}),
                        body=bytes(getattr(response, "content", b"") or b""),
                        requested_utc=iso_utc(requested),
                        finished_utc=iso_utc(finished),
                        elapsed_seconds=time.perf_counter() - started_perf,
                        preview_bytes=preview_bytes,
                    )
                )
            except Exception as exc:  # Per-date failure must not erase other evidence.
                finished = utc_now()
                probes.append(
                    {
                        "trading_date": trading_date.isoformat(),
                        "requested_utc": iso_utc(requested),
                        "finished_utc": iso_utc(finished),
                        "elapsed_seconds": round(time.perf_counter() - started_perf, 6),
                        "request": {"method": "GET", "url": API_URL, "params": request_params(trading_date)},
                        "request_error": _error_text(exc),
                        "returned_suffixes": [],
                        "missing_suffixes": list(EXPECTED_SUFFIXES),
                        "returned_slots": [],
                        "missing_slots": [SUFFIX_TO_SLOT[suffix] for suffix in EXPECTED_SUFFIXES],
                    }
                )

    run_finished = utc_now()
    return {
        "schema_version": 1,
        "requested_utc": iso_utc(run_requested),
        "finished_utc": iso_utc(run_finished),
        "elapsed_seconds": round(time.perf_counter() - run_started_perf, 6),
        "configuration": {
            "dates": [item.isoformat() for item in trading_dates],
            "delay_seconds": delay_seconds,
            "timeout_seconds": timeout_seconds,
            "preview_bytes": preview_bytes,
            "sequential_shared_session": True,
            "raw_data_mutation": False,
        },
        "request_contract": {
            "api_url": API_URL,
            "home_url": HOME_URL,
            "archives_payload": ARCHIVES_PAYLOAD,
            "archives_payload_sha256": hashlib.sha256(ARCHIVES_PAYLOAD.encode()).hexdigest(),
            "headers": dict(HEADERS),
        },
        "warmup": warmup,
        "probes": probes,
    }


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    target = path.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            prefix=f".{target.name}.",
            suffix=".tmp",
            dir=target.parent,
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            json.dump(payload, handle, indent=2, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    except Exception:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        raise


def parse_iso_dates(values: Sequence[str]) -> list[date]:
    dates: list[date] = []
    seen: set[date] = set()
    for raw in values:
        try:
            parsed = date.fromisoformat(raw)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"invalid ISO date {raw!r}; expected YYYY-MM-DD") from exc
        if parsed not in seen:
            dates.append(parsed)
            seen.add(parsed)
    return dates


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", dest="dates", nargs="+", required=True, metavar="YYYY-MM-DD", help="one or more dates to probe sequentially")
    parser.add_argument("--delay-seconds", type=float, default=2.5, help="delay between date requests (default: 2.5)")
    parser.add_argument("--timeout-seconds", type=float, default=240.0, help="per-request timeout (default: 240)")
    parser.add_argument("--preview-bytes", type=int, default=500, help="maximum non-ZIP response preview bytes (default: 500)")
    parser.add_argument("--output-json", type=Path, help="optional atomically-written JSON evidence path")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.delay_seconds < 0:
        parser.error("--delay-seconds must be >= 0")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be > 0")
    if args.preview_bytes < 0:
        parser.error("--preview-bytes must be >= 0")
    try:
        trading_dates = parse_iso_dates(args.dates)
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))
    payload = run_probe(
        trading_dates=trading_dates,
        delay_seconds=float(args.delay_seconds),
        timeout_seconds=float(args.timeout_seconds),
        preview_bytes=int(args.preview_bytes),
    )
    if args.output_json is not None:
        write_json_atomic(args.output_json, payload)
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
