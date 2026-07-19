from __future__ import annotations

from dataclasses import replace
from datetime import date
import json
from pathlib import Path

import pytest

from dhan_data_fetch_stream.supervisor import (
    OutputCapture,
    SupervisorConfig,
    append_event,
    assert_secret_free_command,
    atomic_write_json,
    atomic_write_text,
    fatal_blockers,
    read_manifest_snapshot,
    redact_text,
    render_status_markdown,
    terminal_audit,
    main,
)


def _config(tmp_path: Path, **updates: object) -> SupervisorConfig:
    base = SupervisorConfig(
        root=tmp_path / "data",
        status_dir=tmp_path / "status",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 30),
        expiry_codes=(1,),
        expiry_flags=("WEEK",),
        option_types=("CALL",),
        moneyness_width=0,
        expected_cells=1,
        poll_seconds=1,
        stall_seconds=2,
    )
    return replace(base, **updates)


def _write_manifest(config: SupervisorConfig, *, status: str, rows: int = 0) -> tuple[Path, dict]:
    cell = config.planned_cells()[0]
    bronze = config.root / "bronze" / "options" / f"{cell.request_id}.json"
    bronze.parent.mkdir(parents=True, exist_ok=True)
    bronze.write_text("{}", encoding="utf-8")
    import hashlib

    bronze_hash = hashlib.sha256(b"{}").hexdigest()
    payload = {
        "request_id": cell.request_id,
        "payload_sha256": cell.request_id,
        "dataset": "options",
        "endpoint": "/charts/rollingoption",
        "status": status,
        "rows": rows,
        "completed_at_utc": "2026-01-31T00:00:00+00:00",
        "max_timestamp_ist": "2026-01-30T15:29:00+05:30" if rows else None,
        "payload": dict(cell.payload),
        "bronze_path": str(bronze),
        "bronze_sha256": bronze_hash,
        "silver_path": None,
        "silver_sha256": None,
    }
    path = config.root / "manifests" / "requests" / f"{cell.request_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path, payload


def test_config_builds_exact_bounded_secret_free_resume_command(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config.validate()
    command = config.child_command()
    assert "--no-resume" not in command
    assert command[command.index("--workers") + 1] == "5"
    assert command[command.index("--requests-per-second") + 1] == "5.0"
    assert "DHAN_ACCESS_TOKEN" not in " ".join(command)
    with pytest.raises(ValueError, match="credential-bearing"):
        assert_secret_free_command(["python", "--access-token", "secret"])


def test_config_refuses_limits_above_engine_caps(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="workers"):
        _config(tmp_path, workers=6).validate()
    with pytest.raises(ValueError, match="requests_per_second"):
        _config(tmp_path, requests_per_second=5.1).validate()
    with pytest.raises(ValueError, match="daily_budget"):
        _config(tmp_path, daily_budget=100_001).validate()


def test_manifest_snapshot_reports_counts_rows_timestamp_and_frontier(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _write_manifest(config, status="completed_empty")
    snapshot = read_manifest_snapshot(config, now=1_800_000_000)
    assert snapshot["completed"] == 0
    assert snapshot["completed_empty"] == 1
    assert snapshot["accounted"] == 1
    assert snapshot["failed"] == 0
    assert snapshot["retained_rows"] == 0
    assert snapshot["frontier"]["completed_prefix_cells"] == 1
    assert snapshot["latest_completed_at_utc"] == "2026-01-31T00:00:00+00:00"


def test_fatal_blockers_prevent_auth_quota_schema_and_integrity_restarts() -> None:
    snapshot = {
        "status_counts": {"credential_blocked": 1, "invalid_response": 1},
        "manifest_parse_errors": ["bad.json"],
    }
    errors = {
        "code_counts": {"DH-901": 1, "DH-904": 2, "429": 3},
        "stderr_tail": ["schema mismatch"],
    }
    blockers = fatal_blockers(snapshot, errors)
    assert "manifest_status:credential_blocked" in blockers
    assert "manifest_status:invalid_response" in blockers
    assert "authentication:DH-901" in blockers
    assert "quota_or_rate_limit:DH-904" in blockers
    assert "quota_or_rate_limit:429" in blockers
    assert "integrity:manifest_parse_error" in blockers
    assert "schema_or_integrity_error" in blockers


def test_fatal_blockers_ignore_stale_auth_manifest_from_prior_child() -> None:
    snapshot = {
        "status_counts": {"credential_blocked": 1},
        "latest_failure_at_utc_by_status": {"credential_blocked": "2026-01-01T00:00:00+00:00"},
        "manifest_parse_errors": [],
    }
    assert fatal_blockers(snapshot, {"code_counts": {}, "stderr_tail": []}, not_before_utc="2026-01-02T00:00:00+00:00") == []
    blockers = fatal_blockers(
        snapshot,
        {"code_counts": {}, "stderr_tail": []},
        not_before_utc="2025-12-31T00:00:00+00:00",
    )
    assert blockers == ["manifest_status:credential_blocked"]


def test_redaction_removes_inherited_secret_and_jwt_before_storage(tmp_path: Path) -> None:
    jwt = "ey" + "JhbGciOiJIUzI1NiJ9." + "eyJzdWIiOiJ4In0." + "signature"
    assert "secret-value" not in redact_text("token=secret-value", ["secret-value"])
    assert jwt not in redact_text(f"failure {jwt}", [])
    capture = OutputCapture(tmp_path, ["secret-value"])
    assert capture.summary() == {"code_counts": {}, "stderr_tail": []}


def test_terminal_audit_passes_empty_cell_and_rejects_orphan_partial(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _write_manifest(config, status="completed_empty")
    audit = terminal_audit(config)
    assert audit["passed"] is True
    orphan = config.root / "bronze" / "options" / "interrupted.json.partial"
    orphan.write_text("incomplete", encoding="utf-8")
    audit = terminal_audit(config)
    assert audit["passed"] is False
    assert audit["orphan_partials"][0]["partial"] == str(orphan)


def test_audit_only_cli_writes_terminal_artifact_without_credentials(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config = _config(tmp_path)
    _write_manifest(config, status="completed_empty")
    result = main(
        [
            "--root",
            str(config.root),
            "--status-dir",
            str(config.status_dir),
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-30",
            "--expiry-codes",
            "1",
            "--expiry-flags",
            "WEEK",
            "--option-types",
            "CALL",
            "--moneyness-width",
            "0",
            "--expected-cells",
            "1",
            "--audit-only",
        ]
    )
    assert result == 0
    assert json.loads(capsys.readouterr().out)["passed"] is True
    assert json.loads((config.status_dir / "terminal_audit.json").read_text(encoding="utf-8"))["passed"] is True


def test_terminal_audit_rejects_canonical_partial_conflict(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _, payload = _write_manifest(config, status="completed_empty")
    partial = Path(payload["bronze_path"] + ".partial")
    partial.write_text("incomplete", encoding="utf-8")
    audit = terminal_audit(config)
    assert audit["passed"] is False
    assert audit["partial_canonical_conflicts"][0]["partial"] == str(partial)


def test_quarantined_partial_is_reported_but_not_an_active_orphan(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _write_manifest(config, status="completed_empty")
    quarantined = config.root / "exceptions" / "orphan_partials" / "run" / "old.json.partial"
    quarantined.parent.mkdir(parents=True, exist_ok=True)
    quarantined.write_text("incomplete", encoding="utf-8")
    snapshot = read_manifest_snapshot(config)
    assert snapshot["partial_files"] == []
    assert snapshot["quarantined_partial_files"] == [str(quarantined)]
    audit = terminal_audit(config)
    assert audit["passed"] is True
    assert audit["quarantined_partials"] == [str(quarantined)]


def test_markdown_contains_operator_critical_fields() -> None:
    status = {
        "state": "running",
        "updated_at_utc": "now",
        "supervisor_pid": 10,
        "child_pid": 11,
        "command": "python safe",
        "restart_count": 0,
        "max_restarts": 3,
        "blockers": [],
        "manifest": {
            "accounted": 5,
            "expected_cells": 10,
            "completed": 4,
            "completed_empty": 1,
            "failed": 0,
            "retained_rows": 99,
            "latest_completed_at_utc": "now",
            "latest_data_timestamp_ist": "then",
            "frontier": {"completed_prefix_cells": 5},
            "partial_files": [],
        },
        "progress": {"rate_cells_per_minute_15m": 2, "eta_at_utc": "later"},
        "stall": {"detected": False, "threshold_seconds": 180},
        "errors": {"code_counts": {"DH-904": 1}},
        "daily_budget": {"used": 5, "limit": 100_000, "remaining": 99_995},
        "disk": {"free_gib": 50, "path": "C:/"},
    }
    text = render_status_markdown(status)
    assert "5/10" in text
    assert "DH-904" in text
    assert "99" in text


def test_status_writes_are_atomic_and_event_log_is_append_only(tmp_path: Path) -> None:
    json_path = tmp_path / "status.json"
    md_path = tmp_path / "STATUS.md"
    event_path = tmp_path / "events.jsonl"
    atomic_write_json(json_path, {"state": "running"})
    atomic_write_text(md_path, "running\n")
    append_event(event_path, {"event": "one"})
    append_event(event_path, {"event": "two"})
    assert json.loads(json_path.read_text(encoding="utf-8"))["state"] == "running"
    assert md_path.read_text(encoding="utf-8") == "running\n"
    assert not list(tmp_path.glob("*.partial"))
    assert [json.loads(line)["event"] for line in event_path.read_text().splitlines()] == ["one", "two"]
