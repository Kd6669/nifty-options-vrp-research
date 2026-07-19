from __future__ import annotations

import argparse
from collections.abc import Iterable
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import tempfile
import time
from typing import Any
import zipfile


LEGACY_FIELDS = (
    "date",
    "time_slot",
    "symbol",
    "instrument",
    "expiry",
    "strike",
    "price",
    "delta",
    "implied_vol",
    "price_scan_range",
    "vol_scan_range",
    "cvf",
    *(f"s{i}" for i in range(1, 17)),
    "composite_delta",
)
NATURAL_KEY_FIELDS = ("date", "time_slot", "symbol", "instrument", "expiry", "strike")
LOG_CAPTURE_LIMIT_BYTES = 256 * 1024


CHILD_CODE = r"""
import importlib.metadata
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import sys
import traceback

from nifty_span.span.extractor import extract_span_archives
from nifty_span.span.backfill import extract_and_compact_span_range


def version(distribution):
    try:
        return importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        return None


result_path = Path(sys.argv[5])
payload = {
    "runtime": {
        "sys_executable": sys.executable,
        "python_version": sys.version,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "package_versions": {
            "pandas": version("pandas"),
            "pyarrow": version("pyarrow"),
            "psutil": version("psutil"),
        },
    }
}
failure = None
try:
    implementation = sys.argv[6]
    if implementation == "legacy":
        date_filter = sys.argv[7]
        trading_date = None
        if date_filter:
            trading_date = __import__("datetime").datetime.strptime(
                date_filter, "%Y%m%d"
            ).date()
        report = extract_span_archives(
            span_data_dir=sys.argv[1],
            parquet_dir=sys.argv[2],
            symbols_filter=tuple(json.loads(sys.argv[3])),
            trading_date=trading_date,
            max_workers=int(sys.argv[4]),
        )
        payload["extractor_report"] = report.__dict__
    else:
        raw_dir = Path(sys.argv[1]).resolve()
        output_dir = Path(sys.argv[2]).resolve()
        date_filter = sys.argv[7]
        zip_paths = sorted(raw_dir.rglob("*.zip"))
        if date_filter:
            zip_paths = [path for path in zip_paths if f".{date_filter}." in path.name]
        suffix_to_slot = {"i1": "BOD", "i2": "ID1", "i3": "ID2", "i4": "ID3", "i5": "ID4", "s": "EOD"}
        events = []
        days = []
        for path in zip_paths:
            match = re.fullmatch(r"nsccl\.(\d{8})\.(i[1-5]|s)\.zip", path.name, re.IGNORECASE)
            if match is None:
                raise ValueError(f"unexpected benchmark archive name: {path}")
            tag, suffix = match.group(1), match.group(2).lower()
            day = __import__("datetime").date(int(tag[:4]), int(tag[4:6]), int(tag[6:8]))
            days.append(day)
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            events.append({
                "observed_at_utc": "2026-07-15T00:00:00+00:00",
                "trading_date": day.isoformat(),
                "slot": suffix_to_slot[suffix],
                "suffix": suffix,
                "state": "downloaded",
                "terminal": True,
                "path": path.relative_to(raw_dir).as_posix(),
                "sha256": digest,
                "size_bytes": path.stat().st_size,
            })
        manifest = output_dir.parent / "download_manifest.jsonl"
        manifest.write_text("".join(json.dumps(event, sort_keys=True) + "\n" for event in events), encoding="utf-8")
        result = extract_and_compact_span_range(
            start_date=min(days),
            end_date=max(days),
            raw_root=raw_dir,
            download_manifest=manifest,
            fragment_root=output_dir.parent / "fragments",
            extraction_manifest=output_dir.parent / "extraction_manifest.jsonl",
            compacted_root=output_dir,
            quarantine_root=output_dir.parent / "quarantine",
            symbols=tuple(json.loads(sys.argv[3])),
            parse_workers=int(sys.argv[4]),
        )
        payload["extractor_report"] = {
            "raw_zip_count": result.extraction.manifest_archive_count,
            "processed_zip_count": result.extraction.manifest_archive_count - result.extraction.failed_archive_count,
            "failed_zip_count": result.extraction.failed_archive_count,
            "row_count": sum(month.output_row_count for month in result.compacted_months),
            "parquet_dir": str(output_dir),
            "symbols": json.loads(sys.argv[3]),
            "created_fragment_count": result.extraction.created_fragment_count,
            "skipped_fragment_count": result.extraction.skipped_fragment_count,
        }
except BaseException as exc:
    failure = exc
    payload["error"] = {
        "type": type(exc).__name__,
        "message": str(exc),
        "traceback": traceback.format_exc(),
    }
finally:
    result_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = result_path.with_suffix(result_path.suffix + ".partial")
    temporary_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    os.replace(temporary_path, result_path)
if failure is not None:
    raise failure
"""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _archive_evidence(path: Path) -> dict[str, Any]:
    try:
        with zipfile.ZipFile(path) as archive:
            corrupt_member = archive.testzip()
            members = archive.namelist()
        zip_error = None
    except (OSError, zipfile.BadZipFile) as exc:
        corrupt_member = None
        members = []
        zip_error = f"{type(exc).__name__}: {exc}"
    return {
        "path": str(path.resolve()),
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
        "zip_testzip": corrupt_member,
        "zip_error": zip_error,
        "members": members,
    }


def _input_manifest(zip_paths: Iterable[Path]) -> list[dict[str, Any]]:
    return [_archive_evidence(path) for path in zip_paths]


def _manifest_identity(manifest: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "path": item["path"],
            "bytes": item["bytes"],
            "sha256": item["sha256"],
            "zip_testzip": item["zip_testzip"],
            "zip_error": item["zip_error"],
            "members": item["members"],
        }
        for item in manifest
    ]


def _assert_valid_input_manifest(manifest: list[dict[str, Any]]) -> None:
    invalid = [
        item
        for item in manifest
        if item["zip_error"] is not None or item["zip_testzip"] is not None or not item["members"]
    ]
    if invalid:
        details = "; ".join(
            f"{item['path']}: zip_error={item['zip_error']!r}, "
            f"corrupt_member={item['zip_testzip']!r}, members={len(item['members'])}"
            for item in invalid
        )
        raise SystemExit(f"input ZIP prevalidation failed: {details}")


def _canonical_scalar(value: Any) -> list[Any]:
    import pandas as pd  # type: ignore[import-not-found]

    if value is None or value is pd.NA:
        return ["null", None]
    try:
        if bool(pd.isna(value)):
            return ["null", None]
    except (TypeError, ValueError):
        pass
    if isinstance(value, bool):
        return ["bool", value]
    if isinstance(value, int):
        return ["int", str(value)]
    if isinstance(value, float):
        number = float(value)
        if math.isnan(number):
            return ["float", "nan"]
        if math.isinf(number):
            return ["float", "+inf" if number > 0 else "-inf"]
        return ["float", number.hex()]
    if hasattr(value, "isoformat"):
        return ["date", value.isoformat()]
    return ["string", str(value)]


def _dimension_counts(frame: Any, field: str) -> list[dict[str, Any]]:
    grouped = frame.groupby(field, dropna=False).size().reset_index(name="rows")
    return [
        {field: _canonical_scalar(row[0])[1], "rows": int(row[1])}
        for row in grouped.itertuples(index=False, name=None)
    ]


def _semantic_output_evidence(output_dir: Path) -> dict[str, Any]:
    import pyarrow as pa  # type: ignore[import-not-found]
    import pyarrow.parquet as pq  # type: ignore[import-not-found]

    paths = sorted(output_dir.rglob("*.parquet"))
    if not paths:
        raise ValueError(f"no Parquet outputs found under {output_dir}")

    file_evidence: list[dict[str, Any]] = []
    tables = []
    schema_variants: set[str] = set()
    for path in paths:
        parquet = pq.ParquetFile(path)
        schema_text = str(parquet.schema_arrow)
        schema_variants.add(schema_text)
        file_evidence.append(
            {
                "path": str(path.relative_to(output_dir)),
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
                "rows": parquet.metadata.num_rows,
                "row_groups": parquet.metadata.num_row_groups,
                "schema": schema_text,
            }
        )
        tables.append(pq.read_table(path))

    table = pa.concat_tables(tables, promote_options="default")
    missing_fields = sorted(set(LEGACY_FIELDS).difference(table.column_names))
    if missing_fields:
        raise ValueError(f"output is missing legacy fields: {missing_fields}")
    frame = table.select(LEGACY_FIELDS).to_pandas()
    if frame.empty:
        raise ValueError("Parquet outputs contain zero rows")

    grouped_keys = frame.groupby(list(NATURAL_KEY_FIELDS), dropna=False).size()
    duplicate_groups = grouped_keys[grouped_keys > 1]
    key_positions = tuple(LEGACY_FIELDS.index(field) for field in NATURAL_KEY_FIELDS)
    canonical_rows: list[tuple[str, str]] = []
    for row in frame.itertuples(index=False, name=None):
        normalized = [_canonical_scalar(value) for value in row]
        key = json.dumps(
            [normalized[position] for position in key_positions],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        rendered = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
        canonical_rows.append((key, rendered))
    canonical_rows.sort(key=lambda item: (item[0], item[1]))
    digest = hashlib.sha256()
    for _, rendered in canonical_rows:
        digest.update(rendered.encode("utf-8"))
        digest.update(b"\n")

    return {
        "files": file_evidence,
        "row_count": len(frame),
        "legacy_fields": list(LEGACY_FIELDS),
        "natural_key_fields": list(NATURAL_KEY_FIELDS),
        "canonical_semantic_sha256": digest.hexdigest(),
        "duplicate_key_count": int(len(duplicate_groups)),
        "duplicate_row_count": int(duplicate_groups.sum()) if len(duplicate_groups) else 0,
        "schema": str(table.select(LEGACY_FIELDS).schema),
        "schema_variant_count": len(schema_variants),
        "counts": {
            "date": _dimension_counts(frame, "date"),
            "time_slot": _dimension_counts(frame, "time_slot"),
            "instrument": _dimension_counts(frame, "instrument"),
        },
    }


def _private_memory_bytes(process: Any) -> int:
    try:
        details = process.memory_full_info()
        for attribute in ("uss", "private"):
            value = getattr(details, attribute, None)
            if value is not None:
                return int(value)
    except Exception:  # psutil errors vary by operating system and process lifetime.
        pass
    try:
        details = process.memory_info()
        for attribute in ("private", "private_bytes"):
            value = getattr(details, attribute, None)
            if value is not None:
                return int(value)
    except Exception:
        pass
    return 0


def _read_log(path: Path) -> dict[str, Any]:
    data = path.read_bytes() if path.exists() else b""
    truncated = len(data) > LOG_CAPTURE_LIMIT_BYTES
    if truncated:
        data = data[-LOG_CAPTURE_LIMIT_BYTES:]
    return {
        "text": data.decode("utf-8", errors="replace"),
        "truncated_to_last_bytes": LOG_CAPTURE_LIMIT_BYTES if truncated else None,
    }


def _run_once(
    *,
    repo_root: Path,
    raw_dir: Path,
    output_dir: Path,
    control_dir: Path,
    symbols: tuple[str, ...],
    workers: int,
    label: str,
    expected_archives: int,
    implementation: str,
    date_filter: str | None,
) -> dict[str, Any]:
    try:
        import psutil  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError(
            "psutil is required for process-tree memory measurement; install with "
            "`uv sync --extra benchmark` or `uv run --extra benchmark ...`"
        ) from exc

    control_dir.mkdir(parents=True, exist_ok=True)
    result_path = control_dir / "child_result.json"
    stdout_path = control_dir / "stdout.log"
    stderr_path = control_dir / "stderr.log"
    environment = os.environ.copy()
    source_dir = repo_root / "src"
    existing_pythonpath = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        f"{source_dir}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else str(source_dir)
    )
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    if implementation == "streaming":
        command = [
            sys.executable,
            str(repo_root / "scripts" / "run_span_streaming_benchmark_child.py"),
            str(raw_dir),
            str(output_dir),
            json.dumps(symbols),
            str(workers),
            str(result_path),
            date_filter or "",
        ]
    else:
        command = [
            sys.executable,
            "-c",
            CHILD_CODE,
            str(raw_dir),
            str(output_dir),
            json.dumps(symbols),
            str(workers),
            str(result_path),
            implementation,
            date_filter or "",
        ]
    started = time.perf_counter()
    with stdout_path.open("wb") as stdout_handle, stderr_path.open("wb") as stderr_handle:
        process = subprocess.Popen(  # noqa: S603 - executable and arguments are controlled locally.
            command,
            cwd=repo_root,
            env=environment,
            stdout=stdout_handle,
            stderr=stderr_handle,
        )
        root_process = psutil.Process(process.pid)
        peak_aggregate_rss_estimate = 0
        peak_private_memory_estimate = 0
        samples = 0
        while process.poll() is None:
            processes = [root_process]
            try:
                processes.extend(root_process.children(recursive=True))
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                pass
            aggregate_rss = 0
            private_memory = 0
            for observed_process in processes:
                try:
                    aggregate_rss += int(observed_process.memory_info().rss)
                except (psutil.AccessDenied, psutil.NoSuchProcess):
                    continue
                private_memory += _private_memory_bytes(observed_process)
            peak_aggregate_rss_estimate = max(peak_aggregate_rss_estimate, aggregate_rss)
            peak_private_memory_estimate = max(peak_private_memory_estimate, private_memory)
            samples += 1
            time.sleep(0.02)
        process.wait()
    wall_seconds = time.perf_counter() - started

    child_result: dict[str, Any] | None = None
    child_result_error: str | None = None
    if result_path.exists():
        try:
            child_result = json.loads(result_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            child_result_error = f"{type(exc).__name__}: {exc}"
    else:
        child_result_error = "child result file was not created"

    semantic_output: dict[str, Any] | None = None
    semantic_output_error: str | None = None
    try:
        semantic_output = _semantic_output_evidence(output_dir)
    except Exception as exc:
        semantic_output_error = f"{type(exc).__name__}: {exc}"

    run: dict[str, Any] = {
        "label": label,
        "exit_code": process.returncode,
        "wall_seconds": round(wall_seconds, 6),
        "rss_sample_interval_seconds": 0.02,
        "memory_samples": samples,
        "peak_aggregate_process_tree_rss_estimate_bytes": peak_aggregate_rss_estimate,
        "peak_process_tree_private_memory_estimate_bytes": peak_private_memory_estimate,
        "memory_method": {
            "aggregate_rss": "sum of process-tree RSS; shared pages may be counted repeatedly",
            "private": "sum of process-tree USS/private bytes when exposed by psutil",
        },
        "child_result": child_result,
        "child_result_error": child_result_error,
        "stdout": _read_log(stdout_path),
        "stderr": _read_log(stderr_path),
        "semantic_output": semantic_output,
        "semantic_output_error": semantic_output_error,
    }
    validation_errors: list[str] = []
    if process.returncode != 0:
        validation_errors.append(f"child exit code was {process.returncode}")
    if child_result_error:
        validation_errors.append(child_result_error)
    report = child_result.get("extractor_report") if isinstance(child_result, dict) else None
    if not isinstance(report, dict):
        validation_errors.append("extractor report is missing")
    else:
        if int(report.get("failed_zip_count", -1)) != 0:
            validation_errors.append(f"failed_zip_count was {report.get('failed_zip_count')!r}")
        if int(report.get("raw_zip_count", -1)) != expected_archives:
            validation_errors.append(
                f"raw_zip_count was {report.get('raw_zip_count')!r}; expected {expected_archives}"
            )
        if int(report.get("processed_zip_count", -1)) != expected_archives:
            validation_errors.append(
                f"processed_zip_count was {report.get('processed_zip_count')!r}; "
                f"expected {expected_archives}"
            )
        if int(report.get("row_count", 0)) <= 0:
            validation_errors.append(f"extractor row_count was {report.get('row_count')!r}")
    if semantic_output_error:
        validation_errors.append(f"semantic output inspection failed: {semantic_output_error}")
    elif semantic_output is None or int(semantic_output.get("row_count", 0)) <= 0:
        validation_errors.append("semantic output is empty")
    else:
        if int(semantic_output.get("duplicate_key_count", -1)) != 0:
            validation_errors.append(
                f"semantic output duplicate_key_count was "
                f"{semantic_output.get('duplicate_key_count')!r}"
            )
        if isinstance(report, dict) and int(semantic_output.get("row_count", -1)) != int(
            report.get("row_count", -2)
        ):
            validation_errors.append(
                "semantic output row_count does not match extractor report: "
                f"{semantic_output.get('row_count')!r} != {report.get('row_count')!r}"
            )
    run["valid"] = not validation_errors
    run["validation_errors"] = validation_errors
    return run


def _summary(runs: list[dict[str, Any]]) -> dict[str, Any]:
    def describe(field: str) -> dict[str, Any]:
        values = [float(run[field]) for run in runs]
        return {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "mean": statistics.fmean(values),
            "median": statistics.median(values),
            "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
        }

    return {
        "valid_runs": sum(bool(run["valid"]) for run in runs),
        "invalid_runs": sum(not bool(run["valid"]) for run in runs),
        "wall_seconds": describe("wall_seconds"),
        "peak_aggregate_process_tree_rss_estimate_bytes": describe(
            "peak_aggregate_process_tree_rss_estimate_bytes"
        ),
        "peak_process_tree_private_memory_estimate_bytes": describe(
            "peak_process_tree_private_memory_estimate_bytes"
        ),
    }


def _git_metadata(repo_root: Path) -> dict[str, Any]:
    def git(*arguments: str) -> str:
        result = subprocess.run(
            ["git", *arguments],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    status = git("status", "--porcelain=v1", "--untracked-files=all")
    diff = subprocess.run(
        ["git", "diff", "--binary", "HEAD"],
        cwd=repo_root,
        check=True,
        capture_output=True,
    ).stdout
    return {
        "head": git("rev-parse", "HEAD"),
        "branch": git("branch", "--show-current"),
        "dirty": bool(status),
        "status_porcelain": status.splitlines(),
        "tracked_diff_sha256": hashlib.sha256(diff).hexdigest(),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark SPAN ZIP-to-Parquet extraction with prevalidated immutable inputs, "
            "independent fresh-output trials, incremental reruns, process-tree memory "
            "sampling, and canonical semantic output verification."
        )
    )
    parser.add_argument("--raw-dir", required=True, type=Path, help="Root containing SPAN ZIPs.")
    parser.add_argument(
        "--implementation",
        choices=("legacy", "streaming"),
        default="legacy",
        help="Extractor implementation to benchmark (default: legacy).",
    )
    parser.add_argument("--date", help="Optional YYYYMMDD archive-date filter for a controlled fixture subset.")
    parser.add_argument("--symbols", nargs="+", default=["NIFTY"], help="Symbols to extract.")
    parser.add_argument("--workers", type=int, default=4, help="Extractor worker process count.")
    parser.add_argument(
        "--warmup-runs",
        type=int,
        default=1,
        help="Untimed-for-summary fresh-output warmups (default: 1; may be 0).",
    )
    parser.add_argument(
        "--fresh-runs",
        type=int,
        default=3,
        help="Independent measured fresh-output runs (minimum: 3; default: 3).",
    )
    parser.add_argument(
        "--rerun-runs",
        type=int,
        default=1,
        help="Measured unchanged reruns against the final fresh output (minimum: 1; default: 1).",
    )
    parser.add_argument("--output-json", type=Path, help="Write the complete evidence payload here.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.workers < 1:
        raise SystemExit("--workers must be at least 1")
    if args.warmup_runs < 0:
        raise SystemExit("--warmup-runs must be at least 0")
    if args.fresh_runs < 3:
        raise SystemExit("--fresh-runs must be at least 3")
    if args.rerun_runs < 1:
        raise SystemExit("--rerun-runs must be at least 1")

    raw_dir = args.raw_dir.resolve()
    if not raw_dir.is_dir():
        raise SystemExit(f"raw directory does not exist: {raw_dir}")
    zip_paths = sorted(raw_dir.rglob("*.zip"))
    if args.date:
        if not (len(args.date) == 8 and args.date.isdigit()):
            raise SystemExit("--date must be YYYYMMDD")
        zip_paths = [path for path in zip_paths if f".{args.date}." in path.name]
    if not zip_paths:
        raise SystemExit(f"raw directory contains no ZIP files: {raw_dir}")
    input_manifest_before = _input_manifest(zip_paths)
    _assert_valid_input_manifest(input_manifest_before)

    repo_root = Path(__file__).resolve().parents[1]
    symbols = tuple(str(symbol).upper() for symbol in args.symbols)
    started_at_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    warmup_runs: list[dict[str, Any]] = []
    fresh_runs: list[dict[str, Any]] = []
    rerun_runs: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="span_extraction_benchmark_") as temporary:
        temporary_root = Path(temporary)
        for index in range(args.warmup_runs):
            warmup_runs.append(
                _run_once(
                    repo_root=repo_root,
                    raw_dir=raw_dir,
                    output_dir=temporary_root / f"warmup_{index + 1}" / "parquet",
                    control_dir=temporary_root / f"warmup_{index + 1}" / "control",
                    symbols=symbols,
                    workers=args.workers,
                    label=f"warmup_fresh_{index + 1}",
                    expected_archives=len(zip_paths),
                    implementation=args.implementation,
                    date_filter=args.date,
                )
            )
        final_output_dir: Path | None = None
        for index in range(args.fresh_runs):
            final_output_dir = temporary_root / f"fresh_{index + 1}" / "parquet"
            fresh_runs.append(
                _run_once(
                    repo_root=repo_root,
                    raw_dir=raw_dir,
                    output_dir=final_output_dir,
                    control_dir=temporary_root / f"fresh_{index + 1}" / "control",
                    symbols=symbols,
                    workers=args.workers,
                    label=f"fresh_output_{index + 1}",
                    expected_archives=len(zip_paths),
                    implementation=args.implementation,
                    date_filter=args.date,
                )
            )
        assert final_output_dir is not None
        for index in range(args.rerun_runs):
            rerun_runs.append(
                _run_once(
                    repo_root=repo_root,
                    raw_dir=raw_dir,
                    output_dir=final_output_dir,
                    control_dir=temporary_root / f"rerun_{index + 1}" / "control",
                    symbols=symbols,
                    workers=args.workers,
                    label=f"unchanged_rerun_{index + 1}",
                    expected_archives=len(zip_paths),
                    implementation=args.implementation,
                    date_filter=args.date,
                )
            )

    zip_paths_after = sorted(raw_dir.rglob("*.zip"))
    if args.date:
        zip_paths_after = [path for path in zip_paths_after if f".{args.date}." in path.name]
    input_manifest_after = _input_manifest(zip_paths_after)
    inputs_unchanged = _manifest_identity(input_manifest_before) == _manifest_identity(
        input_manifest_after
    )
    all_runs = [*warmup_runs, *fresh_runs, *rerun_runs]
    semantic_digests = {
        run["semantic_output"]["canonical_semantic_sha256"]
        for run in all_runs
        if isinstance(run.get("semantic_output"), dict)
        and run["semantic_output"].get("canonical_semantic_sha256")
    }
    semantic_outputs_identical = len(semantic_digests) == 1
    payload = {
        "schema_version": 2,
        "started_at_utc": started_at_utc,
        "completed_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "repository": _git_metadata(repo_root),
        "harness": {
            "path": str(Path(__file__).resolve()),
            "sha256": _sha256(Path(__file__).resolve()),
        },
        "implementation": {
            "name": args.implementation,
            "extractor_path": str(
                (
                    repo_root
                    / "src"
                    / "robs_live"
                    / "span"
                    / ("extractor.py" if args.implementation == "legacy" else "streaming_extractor.py")
                ).resolve()
            ),
            "extractor_sha256": _sha256(
                repo_root
                / "src"
                / "robs_live"
                / "span"
                / ("extractor.py" if args.implementation == "legacy" else "streaming_extractor.py")
            ),
        },
        "platform": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "parent_sys_executable": sys.executable,
            "parent_python_version": sys.version,
        },
        "raw_dir": str(raw_dir),
        "symbols": list(symbols),
        "date_filter": args.date,
        "workers": args.workers,
        "configuration": {
            "warmup_runs": args.warmup_runs,
            "fresh_runs": args.fresh_runs,
            "rerun_runs": args.rerun_runs,
        },
        "input_validation": {
            "prevalidated": True,
            "inputs_unchanged_after_runs": inputs_unchanged,
            "before": input_manifest_before,
            "after": input_manifest_after,
        },
        "warmup_runs": warmup_runs,
        "fresh_runs": fresh_runs,
        "rerun_runs": rerun_runs,
        "summaries": {
            "fresh_runs": _summary(fresh_runs),
            "rerun_runs": _summary(rerun_runs),
            "semantic_outputs_identical": semantic_outputs_identical,
            "semantic_digest_variants": sorted(semantic_digests),
        },
    }
    rendered = json.dumps(payload, indent=2, sort_keys=True)
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(f"{rendered}\n", encoding="utf-8")
    print(rendered)
    return 0 if (
        inputs_unchanged
        and semantic_outputs_identical
        and all(bool(run["valid"]) for run in all_runs)
    ) else 1


if __name__ == "__main__":
    raise SystemExit(main())
