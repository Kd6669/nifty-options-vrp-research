"""Configuration contract for the Phase 2 evidence pipeline."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "phase2-hypothesis-evidence/v1"


def _expand(value: str) -> str:
    return os.path.expanduser(os.path.expandvars(value))


def _resolve(root: Path, value: str) -> Path:
    candidate = Path(_expand(value))
    if not candidate.is_absolute():
        candidate = root / candidate
    return candidate.resolve()


@dataclass(frozen=True)
class ResearchConfig:
    """All external paths and analysis conventions needed by the pipeline."""

    repo_root: Path
    gold_root: Path
    expiry_calendar: Path
    dataset_audit: Path
    output_dir: Path
    session_mode: str = "observed"
    entry_offset_source: str = "computed"
    schema_version: str = SCHEMA_VERSION

    @classmethod
    def from_json(cls, path: Path) -> ResearchConfig:
        config_path = path.resolve()
        raw: dict[str, Any] = json.loads(config_path.read_text(encoding="utf-8"))
        schema_version = str(raw.get("schema_version", ""))
        if schema_version != SCHEMA_VERSION:
            raise ValueError(
                f"unsupported config schema {schema_version!r}; expected {SCHEMA_VERSION!r}"
            )

        configured_root = str(raw.get("repo_root", "."))
        repo_root = _resolve(config_path.parent, configured_root)
        required = ("gold_root", "expiry_calendar", "dataset_audit", "output_dir")
        missing = [key for key in required if not raw.get(key)]
        if missing:
            raise ValueError(f"missing required config keys: {', '.join(missing)}")

        session_mode = str(raw.get("session_mode", "observed"))
        entry_offset_source = str(raw.get("entry_offset_source", "computed"))
        if session_mode not in {"standard", "observed"}:
            raise ValueError("session_mode must be 'standard' or 'observed'")
        if entry_offset_source not in {"provider", "computed"}:
            raise ValueError("entry_offset_source must be 'provider' or 'computed'")

        return cls(
            repo_root=repo_root,
            gold_root=_resolve(repo_root, str(raw["gold_root"])),
            expiry_calendar=_resolve(repo_root, str(raw["expiry_calendar"])),
            dataset_audit=_resolve(repo_root, str(raw["dataset_audit"])),
            output_dir=_resolve(repo_root, str(raw["output_dir"])),
            session_mode=session_mode,
            entry_offset_source=entry_offset_source,
            schema_version=schema_version,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "repo_root": str(self.repo_root),
            "gold_root": str(self.gold_root),
            "expiry_calendar": str(self.expiry_calendar),
            "dataset_audit": str(self.dataset_audit),
            "output_dir": str(self.output_dir),
            "session_mode": self.session_mode,
            "entry_offset_source": self.entry_offset_source,
        }
