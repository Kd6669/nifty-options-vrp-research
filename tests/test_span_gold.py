from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dhan_data_fetch_stream.span_gold import SpanGoldConfig, run_span_gold, sha256_file


def test_span_gold_conserves_rows_audits_unmatched_and_resumes() -> None:
    with TemporaryDirectory() as temp:
        paths = _fixture(Path(temp))
        stats = run_span_gold(
            **paths,
            config=SpanGoldConfig(threads=1, memory_limit="1GB", row_group_size=2),
        )
        assert stats.terminal_status == "PILOT_PASS"
        assert (stats.rows_total, stats.matched_rows, stats.unmatched_rows) == (2, 1, 1)
        output = Path(stats.output_root) / "gold/year=2021/month=01/part-000.parquet"
        rows = pq.read_table(output).to_pylist()
        assert [row["span_enrichment_status"] for row in rows] == [
            "matched",
            "unmatched",
        ]
        assert rows[0]["span_price"] == 123.0
        assert rows[0]["span_join_policy"] == "BOD_CONSERVATIVE_UNKNOWN_EFFECTIVE_TIME"
        assert rows[1]["span_unmatched_reason"] == "contract_not_in_bod_span"
        assert rows[1]["span_price"] is None
        before = sha256_file(output)

        resumed = run_span_gold(
            **paths,
            config=SpanGoldConfig(threads=1, memory_limit="1GB", row_group_size=2),
        )
        assert resumed.months_resumed == 1
        assert resumed.months_processed == 0
        assert sha256_file(output) == before


def test_span_gold_rejects_bod_with_known_effective_time() -> None:
    with TemporaryDirectory() as temp:
        paths = _fixture(Path(temp), known_effective=True)
        with pytest.raises(ValueError, match="requires unknown/null effective time"):
            run_span_gold(
                **paths,
                config=SpanGoldConfig(threads=1, memory_limit="1GB", row_group_size=2),
            )


def test_span_gold_rejects_tampered_matrix() -> None:
    with TemporaryDirectory() as temp:
        paths = _fixture(Path(temp))
        Path(paths["span_matrix"]).write_bytes(b"tampered")
        with pytest.raises(ValueError, match="matrix hash mismatch"):
            run_span_gold(
                **paths,
                config=SpanGoldConfig(threads=1, memory_limit="1GB", row_group_size=2),
            )


def test_span_gold_adopts_fully_validated_crash_pair() -> None:
    with TemporaryDirectory() as temp:
        paths = _fixture(Path(temp))
        stats = run_span_gold(
            **paths,
            config=SpanGoldConfig(threads=1, memory_limit="1GB", row_group_size=2),
        )
        root = Path(stats.output_root)
        output = root / "gold/year=2021/month=01/part-000.parquet"
        before = sha256_file(output)
        month_manifest = root / "manifests/months/month=2021-01.json"
        month_manifest.unlink()

        adopted = run_span_gold(
            **paths,
            config=SpanGoldConfig(threads=1, memory_limit="1GB", row_group_size=2),
        )
        manifest = json.loads(month_manifest.read_text(encoding="utf-8"))
        assert adopted.months_resumed == 1
        assert manifest["crash_pair_adopted"] is True
        assert sha256_file(output) == before


def test_span_gold_rejects_crash_pair_with_wrong_gold_lineage() -> None:
    with TemporaryDirectory() as temp:
        paths = _fixture(Path(temp))
        stats = run_span_gold(
            **paths,
            config=SpanGoldConfig(threads=1, memory_limit="1GB", row_group_size=2),
        )
        root = Path(stats.output_root)
        output = root / "gold/year=2021/month=01/part-000.parquet"
        table = pq.read_table(output)
        index = table.schema.get_field_index("span_gold_lineage_sha256")
        table = table.set_column(
            index, "span_gold_lineage_sha256", pa.array(["0" * 64] * 2)
        )
        pq.write_table(table, output)
        month_manifest = root / "manifests/months/month=2021-01.json"
        month_manifest.unlink()

        rerun = run_span_gold(
            **paths,
            config=SpanGoldConfig(threads=1, memory_limit="1GB", row_group_size=2),
        )
        manifest = json.loads(month_manifest.read_text(encoding="utf-8"))
        assert rerun.months_processed == 1
        assert "crash_pair_adopted" not in manifest
        rows = pq.read_table(output, columns=["span_gold_lineage_sha256"]).to_pylist()
        assert {row["span_gold_lineage_sha256"] for row in rows} == {
            manifest["lineage_sha256"]
        }
        assert any((root / "quarantine").rglob("*.parquet"))


def test_span_gold_rejects_summary_count_disagreement() -> None:
    with TemporaryDirectory() as temp:
        paths = _fixture(Path(temp))
        completion = Path(paths["span_completion"])
        payload = json.loads(completion.read_text(encoding="utf-8"))
        summary = Path(payload["artifacts"]["audit"]["summary_json"]["path"])
        summary_payload = json.loads(summary.read_text(encoding="utf-8"))
        summary_payload["terminal_cells"] -= 1
        _write_json(summary, summary_payload)
        payload["artifacts"]["audit"]["summary_json"]["sha256"] = sha256_file(summary)
        _write_json(completion, payload)
        with pytest.raises(ValueError, match="summary contract mismatch"):
            run_span_gold(
                **paths,
                config=SpanGoldConfig(threads=1, memory_limit="1GB", row_group_size=2),
            )


def test_span_gold_rejects_non_calendar_matrix_date() -> None:
    with TemporaryDirectory() as temp:
        paths = _fixture(Path(temp))
        matrix = Path(paths["span_matrix"])
        table = pq.read_table(matrix)
        rows = table.to_pylist()
        for row in rows:
            if row["trading_date"] == "2021-02-01":
                row["trading_date"] = "2021-02-01x"
        pq.write_table(pa.Table.from_pylist(rows), matrix)
        completion = Path(paths["span_completion"])
        payload = json.loads(completion.read_text(encoding="utf-8"))
        payload["artifacts"]["audit"]["matrix_parquet"]["sha256"] = sha256_file(matrix)
        _write_json(completion, payload)
        with pytest.raises(ValueError, match="invalid ISO trading dates"):
            run_span_gold(
                **paths,
                config=SpanGoldConfig(threads=1, memory_limit="1GB", row_group_size=2),
            )


def test_span_gold_fails_terminal_acceptance_with_orphan_partial() -> None:
    with TemporaryDirectory() as temp:
        paths = _fixture(Path(temp))
        root = Path(paths["output_root"]) / "version=1.3.0"
        orphan = root / "gold/year=2020/month=12/.part-000.parquet.dead.partial"
        orphan.parent.mkdir(parents=True, exist_ok=True)
        orphan.write_bytes(b"stale")

        with pytest.raises(RuntimeError, match="orphan_partials_present"):
            run_span_gold(
                **paths,
                config=SpanGoldConfig(threads=1, memory_limit="1GB", row_group_size=2),
            )
        audit_path = root / "manifests/span_gold_terminal_audit.json"
        audit = json.loads(audit_path.read_text(encoding="utf-8"))
        assert audit["status"] == "FAIL"
        assert audit["errors"] == ["orphan_partials_present"]
        assert audit["orphan_partial_paths"] == [str(orphan)]


def test_span_gold_rejects_missing_acceptance_checks() -> None:
    with TemporaryDirectory() as temp:
        paths = _fixture(Path(temp))
        completion = Path(paths["span_completion"])
        payload = json.loads(completion.read_text(encoding="utf-8"))
        payload["blocked_matrix_checks"] = {}
        completion.write_text(json.dumps(payload), encoding="utf-8")
        with pytest.raises(ValueError, match="checks missing"):
            run_span_gold(
                **paths,
                config=SpanGoldConfig(threads=1, memory_limit="1GB", row_group_size=2),
            )


def _fixture(root: Path, *, known_effective: bool = False) -> dict[str, object]:
    bsm_root = root / "bsm"
    bsm_table = pa.table(
        {
            "timestamp_ist": pa.array(
                [
                    datetime(2021, 1, 4, 3, 45, tzinfo=timezone.utc),
                    datetime(2021, 1, 4, 3, 46, tzinfo=timezone.utc),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "trade_date": pa.array([date(2021, 1, 4)] * 2, type=pa.date32()),
            "underlying": ["NIFTY", "NIFTY"],
            "actual_expiry_date": pa.array([date(2021, 1, 28)] * 2, type=pa.date32()),
            "expiry_flag": ["MONTH", "MONTH"],
            "expiry_code": pa.array([1, 1], type=pa.int32()),
            "moneyness_label": ["ATM", "ATM+1"],
            "strike": pa.array([14000.0, 14050.0], type=pa.float64()),
            "option_type": ["CALL", "CALL"],
            "request_id": ["a", "b"],
            "bsm_status": ["ok", "ok"],
        }
    )
    months = _months()
    bsm_month_audits = []
    for month in months:
        year, number = month.split("-")
        bsm_path = bsm_root / f"year={year}/month={number}/part-000.parquet"
        bsm_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(bsm_table, bsm_path)
        bsm_month_audits.append(
            {"month": month, "rows": 2, "output_sha256": sha256_file(bsm_path)}
        )
    bsm_audit = bsm_root / "manifests/bsm_v2_terminal_audit.json"
    _write_json(
        bsm_audit,
        {
            "schema": "dhan_bsm_v2_terminal_audit",
            "status": "PASS",
            "span_enriched": False,
            "months": 67,
            "expected_rows": 134,
            "output_rows": 134,
            "months_audited": bsm_month_audits,
        },
    )

    span_root = root / "span"
    span_root.mkdir()
    effective = (
        datetime(2021, 1, 4, 3, 30, tzinfo=timezone.utc) if known_effective else None
    )
    span_table = pa.table(
        {
            "date": pa.array([date(2021, 1, 4)], type=pa.date32()),
            "time_slot": ["BOD"],
            "symbol": ["NIFTY"],
            "instrument": ["CE"],
            "expiry": pa.array([date(2021, 1, 28)], type=pa.date32()),
            "strike": [14000.0],
            "price": [123.0],
            "source_sha256": ["a" * 64],
            "span_effective_ts_ist": pa.array(
                [effective], type=pa.timestamp("us", tz="UTC")
            ),
            "effective_time_source": [
                "official_metadata" if known_effective else "unknown"
            ],
        }
    )
    span_months = []
    for index, month in enumerate(months):
        year, number = month.split("-")
        month_path = span_root / f"{year}_{number}.parquet"
        pq.write_table(span_table, month_path)
        row_count = 1 if index < 66 else 24_870_123 - 66
        span_months.append(
            {
                "year": int(year),
                "month": int(number),
                "compacted_path": str(month_path.resolve()),
                "exists": True,
                "issue": None,
                "row_count": row_count,
                "sha256": sha256_file(month_path),
            }
        )
    matrix = root / "span_date_slot_matrix.parquet"
    matrix_rows = []
    current = date(2021, 1, 1)
    end = date(2026, 7, 15)
    slots = ("BOD", "ID1", "ID2", "ID3", "ID4", "EOD")
    while current <= end:
        for slot in slots:
            matrix_rows.append(
                {
                    "trading_date": current.isoformat(),
                    "slot": slot,
                    "accounted": True,
                    "source_boundary_proven": len(matrix_rows) == 0,
                }
            )
        current += timedelta(days=1)
    pq.write_table(
        pa.Table.from_pylist(matrix_rows),
        matrix,
    )
    summary = root / "span_backfill_summary.json"
    _write_json(
        summary,
        {
            "start_date": "2021-01-01",
            "end_date": "2026-07-15",
            "requested_dates": 2022,
            "expected_cells": 12132,
            "accounted_cells": 12132,
            "resolved_or_blocked_cells": 12132,
            "terminal_cells": 12131,
            "source_boundary_cells": 1,
            "compacted_months": 67,
            "compacted_rows": 24870123,
            "compacted_unique": True,
            "duplicate_natural_keys": 0,
            "raw_integrity_ok": True,
            "raw_integrity_failures": 0,
            "extraction_complete": True,
            "downloaded_without_valid_extraction": 0,
            "unresolved_missing_cells": 0,
            "unresolved_non_boundary_cells": 0,
            "outcome": "BLOCKED_SOURCE",
            "months": span_months,
        },
    )
    completion = root / "SPAN_PHASE1_COMPLETION.json"
    _write_json(
        completion,
        {
            "finalizer_schema_version": "span-phase1-finalizer-v1",
            "outcome": "BLOCKED_SOURCE",
            "pinned_contract": {
                "start_date": "2021-01-01",
                "end_date": "2026-07-15",
                "dates": 2022,
                "cells": 12132,
                "range_matches": True,
            },
            "blocked_matrix_ready": True,
            "blocked_matrix_checks": {
                "accounted_cells_exact": True,
                "blocked_matrix_complete": True,
                "compacted_unique": True,
                "downloaded_extraction_complete": True,
                "expected_cells_exact": True,
                "raw_integrity_ok": True,
                "requested_dates_exact": True,
                "source_boundary_cells_positive": True,
                "unresolved_missing_zero": True,
                "unresolved_non_boundary_zero": True,
            },
            "source_stability": {"stable": True},
            "artifact_checks": {
                "all_audit_artifacts_nonempty": True,
                "all_export_artifacts_nonempty": True,
                "availability_export_hash_matches_source": True,
                "download_json_hash_matches_export": True,
                "download_parquet_hash_matches_export": True,
                "extraction_json_hash_matches_export": True,
                "extraction_parquet_hash_matches_export": True,
            },
            "audit": {
                "source_boundary_cells": 1,
                "terminal_cells": 12131,
                "requested_dates": 2022,
                "expected_cells": 12132,
                "accounted_cells": 12132,
                "resolved_or_blocked_cells": 12132,
                "compacted_months": 67,
                "compacted_rows": 24870123,
                "earliest_proven_download_date": "2021-01-01",
                "latest_proven_download_date": "2026-07-15",
                "unresolved_missing_cells": 0,
                "unresolved_non_boundary_cells": 0,
            },
            "artifacts": {
                "audit": {
                    "matrix_parquet": {
                        "path": str(matrix.resolve()),
                        "sha256": sha256_file(matrix),
                    },
                    "summary_json": {
                        "path": str(summary.resolve()),
                        "sha256": sha256_file(summary),
                    },
                }
            },
        },
    )
    return {
        "bsm_root": str(bsm_root),
        "bsm_terminal_audit": str(bsm_audit),
        "span_compacted_root": str(span_root),
        "span_completion": str(completion),
        "span_matrix": str(matrix),
        "output_root": str(root / "out"),
        "months": ("2021-01",),
    }


def _months() -> list[str]:
    return [
        f"{year:04d}-{month:02d}"
        for year in range(2021, 2027)
        for month in range(1, 13)
        if (year, month) <= (2026, 7)
    ]


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
