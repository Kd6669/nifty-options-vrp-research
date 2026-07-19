"""Partition-incremental driver for the mandatory pre-BSM enrichment layer."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, Mapping
import uuid

from .enrichment import (
    ENRICHMENT_VERSION,
    enrich_options_pre_bsm,
    load_dimension_rows,
    sha256_file,
    write_enriched_partitions,
)


@dataclass(frozen=True)
class PreBsmRunStats:
    options_files_planned: int
    options_files_processed: int
    options_files_resumed: int
    input_option_rows: int
    canonical_rows: int
    option_exception_rows: int
    duplicate_right_rows: int
    ready_rows: int
    blocked_rows: int
    output_root: str
    status_path: str
    bsm_executed: bool = False


def run_pre_bsm_incremental(
    *,
    options_root: str | Path,
    spot_root: str | Path,
    vix_root: str | Path,
    contract_rules: str | Path,
    actual_expiries: str | Path,
    output_root: str | Path,
    pilot_files: int = 0,
    acquisition_terminally_accounted: bool = False,
    resume: bool = True,
) -> PreBsmRunStats:
    """Enrich each immutable option request file into idempotent Parquet parts.

    Right-side data are scanned only for the option file's date window and are
    cached by window. A per-input manifest is the resume boundary; it is reused
    only when the source hash and every output hash still validate.
    """
    import pyarrow as pa
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq

    options_path = Path(options_root).resolve()
    output_path = Path(output_root).resolve()
    files = sorted(options_path.rglob("*.parquet"))
    if pilot_files < 0:
        raise ValueError("pilot_files must be non-negative")
    if pilot_files:
        files = files[:pilot_files]
    if not files:
        raise ValueError(f"no option Parquet files found under {options_path}")

    rule_rows = load_dimension_rows(contract_rules)
    expiry_rows = load_dimension_rows(actual_expiries)
    contract_rules_hash = sha256_file(contract_rules)
    actual_expiries_hash = sha256_file(actual_expiries)
    spot_dataset = ds.dataset(Path(spot_root).resolve(), format="parquet", partitioning="hive")
    vix_dataset = ds.dataset(Path(vix_root).resolve(), format="parquet", partitioning="hive")
    right_cache: dict[tuple[date, date], tuple[list[dict[str, Any]], list[dict[str, Any]]]] = {}
    status_path = (
        output_path
        / "enriched_options"
        / f"version={ENRICHMENT_VERSION}"
        / "manifests"
        / "pre_bsm_run_status.json"
    )
    source_manifest_dir = status_path.parent / "right_input_manifests"
    spot_lineage = _write_source_manifest(Path(spot_root).resolve(), source_manifest_dir / "nifty_spot.json")
    vix_lineage = _write_source_manifest(Path(vix_root).resolve(), source_manifest_dir / "india_vix.json")
    expected_lineage_hashes = {
        "spot": spot_lineage["manifest_sha256"],
        "india_vix": vix_lineage["manifest_sha256"],
        "contract_rules": contract_rules_hash,
        "actual_expiries": actual_expiries_hash,
    }
    totals = {
        "processed": 0,
        "resumed": 0,
        "input": 0,
        "canonical": 0,
        "exceptions": 0,
        "duplicates": 0,
        "ready": 0,
        "blocked": 0,
    }

    for index, option_file in enumerate(files, 1):
        source_hash = sha256_file(option_file)
        request_id = option_file.stem
        physical_id = request_id[:20]
        manifest_path = status_path.parent / f"{physical_id}.json"
        resumed_manifest = (
            _validated_completed_manifest(
                manifest_path,
                source_hash,
                expected_lineage_hashes,
                acquisition_terminally_accounted=acquisition_terminally_accounted,
            )
            if resume
            else None
        )
        if resumed_manifest is not None:
            totals["resumed"] += 1
            totals["input"] += int(
                resumed_manifest.get("input_lineage", {}).get("options", {}).get("row_count", 0)
            )
            totals["canonical"] += int(resumed_manifest.get("canonical_row_count", 0))
            totals["exceptions"] += int(resumed_manifest.get("exception_row_count", 0))
            totals["duplicates"] += int(resumed_manifest.get("duplicate_right_row_count", 0))
            coverage = resumed_manifest.get("coverage", {})
            totals["ready"] += int(coverage.get("ready_rows", 0))
            totals["blocked"] += int(coverage.get("blocked_rows", 0))
            _write_status(status_path, files, index, totals, acquisition_terminally_accounted)
            continue

        option_table = pq.read_table(option_file)
        option_rows = option_table.to_pylist()
        trade_dates = [row["trade_date"] for row in option_rows if isinstance(row.get("trade_date"), date)]
        if not trade_dates:
            raise ValueError(f"option input has no typed trade_date: {option_file}")
        window = (min(trade_dates), max(trade_dates))
        if window not in right_cache:
            right_cache[window] = (
                _read_window(spot_dataset, window, pa=pa, ds=ds),
                _read_window(vix_dataset, window, pa=pa, ds=ds),
            )
        spot_rows, vix_rows = right_cache[window]
        batch = enrich_options_pre_bsm(
            option_rows,
            spot_rows,
            vix_rows,
            rule_rows,
            expiry_rows,
            acquisition_terminally_accounted=acquisition_terminally_accounted,
        )
        input_lineage = {
            "options": {
                "path": str(option_file),
                "sha256": source_hash,
                "row_count": len(option_rows),
                "request_id": request_id,
            },
            "spot": _right_lineage(spot_rows, spot_lineage, window),
            "india_vix": _right_lineage(vix_rows, vix_lineage, window),
            "contract_rules": {
                "path": str(Path(contract_rules).resolve()),
                "sha256": contract_rules_hash,
            },
            "actual_expiries": {
                "path": str(Path(actual_expiries).resolve()),
                "sha256": actual_expiries_hash,
            },
            "acquisition_terminally_accounted": acquisition_terminally_accounted,
        }
        written = write_enriched_partitions(
            batch,
            output_path,
            # Keep atomic sibling paths below legacy Windows path limits while
            # retaining the full request identity in the manifest name/lineage.
            part_id=physical_id,
            manifest_id=physical_id,
            input_lineage=input_lineage,
        )
        if len(batch.rows) + len(batch.exceptions) != len(option_rows):
            raise RuntimeError(f"pre-BSM row conservation failed for {option_file}")
        if Path(written.manifest_path) != manifest_path:
            raise RuntimeError("unexpected enrichment manifest path")

        totals["processed"] += 1
        totals["input"] += len(option_rows)
        totals["canonical"] += len(batch.rows)
        totals["exceptions"] += len(batch.exceptions)
        totals["duplicates"] += len(batch.duplicate_right_rows)
        totals["ready"] += int(batch.coverage["ready_rows"])
        totals["blocked"] += int(batch.coverage["blocked_rows"])
        _write_status(status_path, files, index, totals, acquisition_terminally_accounted)

    return PreBsmRunStats(
        options_files_planned=len(files),
        options_files_processed=totals["processed"],
        options_files_resumed=totals["resumed"],
        input_option_rows=totals["input"],
        canonical_rows=totals["canonical"],
        option_exception_rows=totals["exceptions"],
        duplicate_right_rows=totals["duplicates"],
        ready_rows=totals["ready"],
        blocked_rows=totals["blocked"],
        output_root=str(output_path),
        status_path=str(status_path),
    )


def _read_window(dataset: Any, window: tuple[date, date], *, pa: Any, ds: Any) -> list[dict[str, Any]]:
    start, end = window
    predicate = (ds.field("trade_date") >= pa.scalar(start)) & (ds.field("trade_date") <= pa.scalar(end))
    return dataset.to_table(filter=predicate).to_pylist()


def _right_lineage(
    rows: list[Mapping[str, Any]], source_manifest: Mapping[str, Any], window: tuple[date, date]
) -> dict[str, Any]:
    return {
        "root": source_manifest["root"],
        "source_manifest_path": source_manifest["manifest_path"],
        "source_manifest_sha256": source_manifest["manifest_sha256"],
        "trade_date_from": window[0].isoformat(),
        "trade_date_to": window[1].isoformat(),
        "row_count": len(rows),
    }


def _write_source_manifest(root: Path, path: Path) -> dict[str, Any]:
    files = sorted(root.rglob("*.parquet"))
    payload = {
        "manifest_version": "1.0.0",
        "root": str(root),
        "files": [
            {
                "path": str(file),
                "relative_path": file.relative_to(root).as_posix(),
                "bytes": file.stat().st_size,
                "sha256": sha256_file(file),
            }
            for file in files
        ],
    }
    _atomic_json(path, payload)
    return {
        "root": str(root),
        "manifest_path": str(path),
        "manifest_sha256": sha256_file(path),
        "file_count": len(files),
    }


def _validated_completed_manifest(
    path: Path,
    source_hash: str,
    expected_lineage_hashes: Mapping[str, str],
    *,
    acquisition_terminally_accounted: bool,
) -> Mapping[str, Any] | None:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("input_lineage", {}).get("options", {}).get("sha256") != source_hash:
            return None
        lineage = payload.get("input_lineage", {})
        if bool(lineage.get("acquisition_terminally_accounted")) != acquisition_terminally_accounted:
            return None
        actual_hashes = {
            "spot": lineage.get("spot", {}).get("source_manifest_sha256"),
            "india_vix": lineage.get("india_vix", {}).get("source_manifest_sha256"),
            "contract_rules": lineage.get("contract_rules", {}).get("sha256"),
            "actual_expiries": lineage.get("actual_expiries", {}).get("sha256"),
        }
        if actual_hashes != dict(expected_lineage_hashes):
            return None
        artifacts = list(payload.get("partitions", ())) + list(payload.get("exception_artifacts", ()))
        if not all(
            Path(item["path"]).is_file() and sha256_file(item["path"]) == item.get("sha256")
            for item in artifacts
        ):
            return None
        return payload
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


def _write_status(
    path: Path,
    files: list[Path],
    current_index: int,
    totals: Mapping[str, int],
    acquisition_terminally_accounted: bool,
) -> None:
    payload = {
        "status_version": "1.0.0",
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "bsm_executed": False,
        "bsm_gate": "BLOCKED" if not acquisition_terminally_accounted else "ROW_LEVEL_AUDIT_REQUIRED",
        "options_files_total": len(files),
        "options_files_seen": current_index,
        **dict(totals),
    }
    _atomic_json(path, payload)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f"{path.name}.{uuid.uuid4().hex}.partial")
    try:
        with partial.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, default=str)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(partial, path)
    finally:
        partial.unlink(missing_ok=True)


def stats_payload(stats: PreBsmRunStats) -> dict[str, Any]:
    return asdict(stats)
