"""Input and artifact validation for the hypothesis evidence pipeline."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from .config import ResearchConfig
from .contracts import ArtifactSet, STAGE_ORDER


def sha256(path: Path, *, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def inspect_artifact(path: Path, *, include_hash: bool = False) -> dict[str, Any]:
    record: dict[str, Any] = {
        "path": str(path),
        "exists": path.exists(),
    }
    if not path.exists():
        return record
    record["bytes"] = path.stat().st_size
    if path.suffix == ".parquet":
        metadata = pq.ParquetFile(path).metadata
        record["rows"] = metadata.num_rows
        record["row_groups"] = metadata.num_row_groups
    elif path.suffix == ".json":
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            record["json_type"] = type(payload).__name__
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            record["json_error"] = str(error)
    if include_hash:
        record["sha256"] = sha256(path)
    return record


def validate_inputs(config: ResearchConfig) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    checks.append(
        {
            "name": "repo_root",
            "path": str(config.repo_root),
            "ok": (config.repo_root / "pyproject.toml").is_file(),
        }
    )
    checks.append(
        {
            "name": "gold_root",
            "path": str(config.gold_root),
            "ok": config.gold_root.is_dir()
            and next(config.gold_root.rglob("*.parquet"), None) is not None,
        }
    )
    checks.append(
        {
            "name": "expiry_calendar",
            "path": str(config.expiry_calendar),
            "ok": config.expiry_calendar.is_file(),
        }
    )
    checks.append(
        {
            "name": "dataset_audit",
            "path": str(config.dataset_audit),
            "ok": config.dataset_audit.is_file(),
        }
    )

    dataset_release = None
    audited_root = None
    audit_matches_root = False
    if config.dataset_audit.is_file():
        audit = json.loads(config.dataset_audit.read_text(encoding="utf-8"))
        dataset_release = audit.get("dataset_release")
        audited_root_raw = audit.get("dataset_root_local")
        if audited_root_raw:
            audited_root = str(Path(audited_root_raw).resolve())
            audit_matches_root = Path(audited_root).resolve() == config.gold_root.resolve()
    checks.append(
        {
            "name": "dataset_audit_matches_gold_root",
            "path": str(config.dataset_audit),
            "ok": audit_matches_root,
            "audited_root": audited_root,
        }
    )
    return {
        "ok": all(bool(check["ok"]) for check in checks),
        "dataset_release": dataset_release,
        "checks": checks,
    }


def validate_outputs(
    artifacts: ArtifactSet,
    *,
    stages: tuple[str, ...] = STAGE_ORDER,
    include_hash: bool = False,
) -> dict[str, Any]:
    records = []
    for stage in stages:
        outputs = [
            inspect_artifact(path, include_hash=include_hash)
            for path in artifacts.stage_outputs(stage)
        ]
        records.append(
            {
                "stage": stage,
                "ok": all(record["exists"] and "json_error" not in record for record in outputs),
                "outputs": outputs,
            }
        )
    return {
        "ok": all(record["ok"] for record in records),
        "stages": records,
    }
