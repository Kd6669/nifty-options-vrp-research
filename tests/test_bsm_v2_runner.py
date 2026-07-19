from __future__ import annotations

from datetime import date, datetime
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from dhan_data_fetch_stream.bsm import bsm_price
from dhan_data_fetch_stream.bsm_v2_runner import (
    run_bsm_v2_month,
    run_bsm_v2_root,
    sha256_file,
)
from dhan_data_fetch_stream.v2_terminal_audit import audit_bsm_v2


IST = ZoneInfo("Asia/Kolkata")


def _input_table(*, block_put_quality: bool = False) -> pa.Table:
    time_years = 7.0 / 365.0
    rows = []
    for minute, option_type in [(17, "PUT"), (16, "CALL")]:
        close = bsm_price(25_010.0, 25_000.0, 0.2, time_years, option_type)
        rows.append(
            {
                "timestamp_ist": datetime(2021, 1, 4, 9, minute, tzinfo=IST),
                "trade_date": date(2021, 1, 4),
                "session_status": "regular_session",
                "underlying": "NIFTY",
                "expiry_flag": "WEEK",
                "expiry_code": 1,
                "moneyness_label": "ATM",
                "strike": 25_000.0,
                "option_type": option_type,
                "close": close,
                "provider_iv_raw": 0.2,
                "provider_iv_unit": "decimal",
                "provider_spot": 25_001.0,
                "independent_nifty_spot": 25_010.0,
                "nifty_spot_join_status": "matched",
                # VIX is contextual and unavailable in this source period.
                "india_vix": None,
                "india_vix_join_status": "source_unavailable",
                "actual_expiry_timestamp_ist": datetime(2021, 1, 11, 15, 30, tzinfo=IST),
                "expiry_mapping_status": "resolved",
                "contract_rule_status": "resolved",
                "contract_lot_size": 75.0,
                "time_to_expiry_status": "valid",
                "t_years_act365": time_years,
                "bsm_gate_status": "BLOCKED" if block_put_quality and option_type == "PUT" else "READY",
                "bsm_gate_failure_reason": (
                    "proven_severe_provider_payload_corruption"
                    if block_put_quality and option_type == "PUT"
                    else None
                ),
                "quality_severe_anomaly": block_put_quality and option_type == "PUT",
                "proven_severe_payload_corruption": block_put_quality and option_type == "PUT",
                "quality_patch_version": "2.1.0",
                "source_request_id": f"request-{minute}",
            }
        )
    return pa.Table.from_pylist(rows)


def _write_accepted_month(
    root: Path, *, duplicate_groups: int = 0, block_put_quality: bool = False
) -> Path:
    version_root = root / "pre_bsm" / "version=2.1.0"
    source_dir = version_root / "year=2021" / "month=01"
    source_dir.mkdir(parents=True)
    source = source_dir / "pre_bsm.parquet"
    pq.write_table(_input_table(block_put_quality=block_put_quality), source)
    manifest_dir = version_root / "manifests"
    manifest_dir.mkdir(parents=True)
    manifest = {
        "manifest_version": "2.1.0",
        "patch_version": "2.1.0",
        "audit_schema": "dhan_pre_bsm_quality_patch_month",
        "audit_schema_version": "1.0.0",
        "status": "published",
        "month": "2021-01",
        "bsm_executed": False,
        "config": {"acquisition_terminally_accounted": True},
        "artifacts": [
            {
                "path": str(source.resolve()),
                "bytes": source.stat().st_size,
                "sha256": sha256_file(source),
                "row_count": 2,
            },
            {
                "path": str((source_dir / "source_exceptions.parquet").resolve()),
                "sha256": "evidence-only",
                "row_count": 0,
            },
        ],
        "audit": {
            "input_rows": 2,
            "output_rows": 2,
            "canonical_regular_rows": 2,
            "source_exception_rows": 0,
            "future_join_violations": 0,
            "asof_tolerance_violations": 0,
            "primary_key_duplicate_groups": duplicate_groups,
            "primary_key_duplicate_excess_rows": duplicate_groups,
            "parquet_metadata_rows": 2,
            "orphan_partial_count": 0,
            "severe_anomaly_eligible_rows": 0,
            "proven_severe_payload_rows": int(block_put_quality),
        },
    }
    month_manifest = manifest_dir / "month=2021-01.json"
    month_manifest.write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    terminal = {
        "schema": "dhan_pre_bsm_quality_patch_terminal_audit",
        "schema_version": "1.0.0",
        "patch_version": "2.1.0",
        "status": "PASS",
        "months": 67,
        "expected_rows": 43_018_677,
        "bsm_launch_authorized": True,
        "manifest_sha256_by_month": {"2021-01": sha256_file(month_manifest)},
    }
    (manifest_dir / "quality_patch_terminal_audit.json").write_text(
        json.dumps(terminal), encoding="utf-8"
    )
    return source


def test_month_runner_preserves_rows_and_vix_missing_does_not_block_bsm() -> None:
    with TemporaryDirectory() as temp:
        root = Path(temp)
        source = _write_accepted_month(root)

        result = run_bsm_v2_month(source, root / "bsm", month="2021-01", row_group_size=1)

        assert not result.resumed
        assert result.input_rows == result.output_rows == 2
        assert result.status_counts == {"ok": 2}
        output = pq.ParquetFile(result.output_path).read()
        assert output["timestamp_ist"].to_pylist() == sorted(output["timestamp_ist"].to_pylist())
        assert output["india_vix"].null_count == 2
        assert output["bsm_solver_converged"].to_pylist() == [True, True]
        assert output["bsm_price_input_field"].to_pylist() == ["close", "close"]
        assert output["provider_spot"].to_pylist() == [25_001.0, 25_001.0]
        assert output["provider_iv_raw"].to_pylist() == [0.2, 0.2]
        manifest = json.loads(Path(result.manifest_path).read_text(encoding="utf-8"))
        assert manifest["row_conservation"] is True
        assert manifest["parquet_metadata_rows"] == 2
        assert manifest["primary_key_duplicate_rows"] == 0
        assert manifest["provider_iv_delta_decimal_quantiles"]["max"] < 1.0e-10
        assert manifest["output_sha256"] == sha256_file(Path(result.output_path))
        assert not list((root / "bsm").rglob("*.partial"))


def test_month_runner_resumes_exact_lineage_without_rewrite() -> None:
    with TemporaryDirectory() as temp:
        root = Path(temp)
        source = _write_accepted_month(root)
        first = run_bsm_v2_month(source, root / "bsm", month="2021-01")
        first_mtime = Path(first.output_path).stat().st_mtime_ns

        second = run_bsm_v2_month(source, root / "bsm", month="2021-01")

        assert second.resumed
        assert second.output_sha256 == first.output_sha256
        assert Path(second.output_path).stat().st_mtime_ns == first_mtime


def test_month_runner_quarantines_corrupt_publication_before_regeneration() -> None:
    with TemporaryDirectory() as temp:
        root = Path(temp)
        source = _write_accepted_month(root)
        first = run_bsm_v2_month(source, root / "bsm", month="2021-01")
        Path(first.output_path).write_bytes(b"corrupt")

        repaired = run_bsm_v2_month(source, root / "bsm", month="2021-01")

        assert not repaired.resumed
        assert repaired.output_rows == 2
        assert pq.ParquetFile(repaired.output_path).metadata.num_rows == 2
        quarantine = list(
            (root / "bsm" / "version=2.1.0" / "exceptions" / "stale_or_corrupt").rglob(
                "*.parquet"
            )
        )
        assert len(quarantine) == 1
        assert quarantine[0].read_bytes() == b"corrupt"
        assert not list((root / "bsm").rglob("*.partial"))


def test_full_root_runner_discovers_months_and_publishes_atomic_status() -> None:
    with TemporaryDirectory() as temp:
        root = Path(temp)
        source = _write_accepted_month(root)
        # These adjacent evidence files must never be treated as BSM inputs.
        source_dir = source.parent
        pq.write_table(pa.table({"reason": ["audit-only"]}), source_dir / "source_exceptions.parquet")
        pq.write_table(pa.table({"duplicate_count": [2]}), source_dir / "primary_key_duplicates.parquet")

        stats = run_bsm_v2_root(root / "pre_bsm", root / "bsm")

        assert stats.months_total == 1
        assert stats.months_processed == 1
        assert stats.months_resumed == 0
        assert stats.rows_total == 2
        status = json.loads(Path(stats.status_path).read_text(encoding="utf-8"))
        assert status["state"] == "complete"
        assert status["months_completed"] == status["months_total"] == 1
        assert status["rows_completed"] == 2
        assert status["pid"] > 0
        assert status["started_at_utc"]
        assert status["orphan_partial_count"] == 0
        assert status["orphan_partial_paths"] == []
        assert Path(stats.status_markdown_path).is_file()
        assert not list((root / "bsm").rglob("*.partial"))


def test_runner_rejects_pre_bsm_month_that_fails_acceptance_gate() -> None:
    with TemporaryDirectory() as temp:
        root = Path(temp)
        source = _write_accepted_month(root, duplicate_groups=1)

        try:
            run_bsm_v2_month(source, root / "bsm", month="2021-01")
        except ValueError as exc:
            assert "primary_key_duplicate_groups_nonzero_or_missing" in str(exc)
        else:  # pragma: no cover - protects the critical acceptance boundary.
            raise AssertionError("BSM accepted a pre-BSM month with duplicate primary keys")


def test_quality_blocked_row_never_enters_solver_eligible_population() -> None:
    with TemporaryDirectory() as temp:
        root = Path(temp)
        source = _write_accepted_month(root, block_put_quality=True)

        result = run_bsm_v2_month(source, root / "bsm", month="2021-01")

        output = pq.read_table(result.output_path)
        put_index = output["option_type"].to_pylist().index("PUT")
        assert output["bsm_solver_converged"][put_index].as_py() is False
        assert output["bsm_failure_reason"][put_index].as_py() == "pre_bsm_quality_gate_blocked"
        assert output["bsm_iv_close"][put_index].as_py() is None
        assert output["bsm_delta"][put_index].as_py() is None
        manifest = json.loads(Path(result.manifest_path).read_text(encoding="utf-8"))
        assert manifest["ready_input_rows"] == 1
        assert manifest["blocked_input_rows"] == 1
        assert manifest["quality_severe_input_rows"] == 1
        assert manifest["proven_severe_input_rows"] == 1
        assert manifest["quality_severe_solved_rows"] == 0
        assert manifest["blocked_rows_with_finite_bsm_values"] == 0


def test_terminal_audit_preserves_blocked_rows_and_rejects_no_numerical_invariants() -> None:
    with TemporaryDirectory() as temp:
        root = Path(temp)
        source = _write_accepted_month(root, block_put_quality=True)
        run_bsm_v2_month(source, root / "bsm", month="2021-01")

        audit = audit_bsm_v2(
            root / "bsm" / "version=2.1.0",
            expected_rows=2,
            expected_months=1,
            expected_ready_rows=1,
            expected_blocked_rows=1,
        )

        assert audit["status"] == "PASS"
        assert audit["solver_metrics"]["quality_severe_input_rows"] == 1
        assert audit["solver_metrics"]["quality_severe_solved_rows"] == 0
        assert audit["numerical_audit"]["blocked_solved_violation_rows"] == 0
        assert audit["numerical_audit"]["severe_solved_violation_rows"] == 0
