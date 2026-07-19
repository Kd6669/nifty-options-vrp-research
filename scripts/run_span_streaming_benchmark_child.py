from __future__ import annotations

import argparse
from datetime import date
from hashlib import sha256
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import sys
import traceback

from nifty_span.span.backfill import extract_and_compact_span_range


SUFFIX_TO_SLOT = {
    "i1": "BOD",
    "i2": "ID1",
    "i3": "ID2",
    "i4": "ID3",
    "i5": "ID4",
    "s": "EOD",
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("raw_dir", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("symbols_json")
    parser.add_argument("workers", type=int)
    parser.add_argument("result_path", type=Path)
    parser.add_argument("date_filter", nargs="?", default="")
    args = parser.parse_args(argv)
    payload: dict[str, object] = {
        "runtime": {
            "sys_executable": sys.executable,
            "python_version": sys.version,
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "package_versions": {
                name: _version(name) for name in ("pandas", "pyarrow", "psutil")
            },
        }
    }
    failure: BaseException | None = None
    try:
        payload["extractor_report"] = _run(args)
    except BaseException as exc:  # noqa: BLE001 - child must persist failure evidence.
        failure = exc
        payload["error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }
    finally:
        args.result_path.parent.mkdir(parents=True, exist_ok=True)
        partial = args.result_path.with_suffix(args.result_path.suffix + ".partial")
        partial.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        os.replace(partial, args.result_path)
    if failure is not None:
        raise failure
    return 0


def _run(args: argparse.Namespace) -> dict[str, object]:
    raw_dir = args.raw_dir.resolve()
    output_dir = args.output_dir.resolve()
    paths = sorted(raw_dir.rglob("*.zip"))
    if args.date_filter:
        paths = [path for path in paths if f".{args.date_filter}." in path.name]
    if not paths:
        raise ValueError("no matching benchmark archives")
    events = []
    days = []
    for path in paths:
        match = re.fullmatch(r"nsccl\.(\d{8})\.(i[1-5]|s)\.zip", path.name, re.IGNORECASE)
        if match is None:
            raise ValueError(f"unexpected benchmark archive name: {path}")
        tag, suffix = match.group(1), match.group(2).lower()
        day = date(int(tag[:4]), int(tag[4:6]), int(tag[6:8]))
        days.append(day)
        events.append(
            {
                "observed_at_utc": "2026-07-15T00:00:00+00:00",
                "trading_date": day.isoformat(),
                "slot": SUFFIX_TO_SLOT[suffix],
                "suffix": suffix,
                "state": "downloaded",
                "terminal": True,
                "path": path.relative_to(raw_dir).as_posix(),
                "sha256": _sha256(path),
                "size_bytes": path.stat().st_size,
            }
        )
    manifest = output_dir.parent / "download_manifest.jsonl"
    manifest.write_text(
        "".join(json.dumps(event, sort_keys=True) + "\n" for event in events),
        encoding="utf-8",
    )
    result = extract_and_compact_span_range(
        start_date=min(days),
        end_date=max(days),
        raw_root=raw_dir,
        download_manifest=manifest,
        fragment_root=output_dir.parent / "fragments",
        extraction_manifest=output_dir.parent / "extraction_manifest.jsonl",
        compacted_root=output_dir,
        quarantine_root=output_dir.parent / "quarantine",
        symbols=tuple(json.loads(args.symbols_json)),
        parse_workers=args.workers,
    )
    return {
        "raw_zip_count": result.extraction.manifest_archive_count,
        "processed_zip_count": (
            result.extraction.manifest_archive_count - result.extraction.failed_archive_count
        ),
        "failed_zip_count": result.extraction.failed_archive_count,
        "row_count": sum(month.output_row_count for month in result.compacted_months),
        "parquet_dir": str(output_dir),
        "symbols": json.loads(args.symbols_json),
        "created_fragment_count": result.extraction.created_fragment_count,
        "skipped_fragment_count": result.extraction.skipped_fragment_count,
    }


def _version(distribution: str) -> str | None:
    try:
        return importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        return None


def _sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
