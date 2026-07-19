from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import dhan_data_fetch_stream.span_release as release_module
from dhan_data_fetch_stream.span_release import SpanReleaseConfig, run_span_release
from dhan_data_fetch_stream.span_gold import sha256_file


def test_final_span_release_widens_slots_preserves_rows_and_resumes(
    tmp_path: Path, monkeypatch
) -> None:
    paths = _fixture(tmp_path, monkeypatch)
    stats = run_span_release(
        **paths,
        months=("2021-01",),
        config=SpanReleaseConfig(threads=1, memory_limit="1GB", row_group_size=2),
    )
    assert stats.terminal_status == "PILOT_PASS"
    assert (stats.rows_total, stats.bod_matched_rows, stats.bod_unmatched_rows) == (
        2,
        1,
        1,
    )
    bod_path = Path(stats.bod_output_root) / "gold/year=2021/month=01/part-000.parquet"
    six_path = (
        Path(stats.six_slot_output_root) / "gold/year=2021/month=01/part-000.parquet"
    )
    bod = pq.read_table(bod_path).to_pylist()
    six = pq.read_table(six_path).to_pylist()
    assert [row["span_join_status"] for row in bod] == [
        "matched",
        "unmatched_contract",
    ]
    assert six[0]["span_bod_price"] == 100.0
    assert six[0]["span_eod_price"] == 105.0
    assert six[0]["span_unmatched_slot_count"] == 0
    assert six[1]["span_unmatched_slot_count"] == 6
    assert all(
        six[1][f"span_{slot.lower()}_join_status"] == "unmatched_contract"
        for slot in release_module.SLOTS
    )
    assert all(row["bsm_status"] in {"ok", "blocked"} for row in six)
    assert not list(tmp_path.rglob("*.partial"))
    before = (sha256_file(bod_path), sha256_file(six_path))

    resumed = run_span_release(
        **paths,
        months=("2021-01",),
        config=SpanReleaseConfig(threads=1, memory_limit="1GB", row_group_size=2),
    )
    assert resumed.months_resumed == 1
    assert (sha256_file(bod_path), sha256_file(six_path)) == before


def _fixture(tmp_path: Path, monkeypatch) -> dict[str, str]:
    months = [
        f"{year:04d}-{month:02d}"
        for year in range(2021, 2027)
        for month in range(1, 13)
        if (year, month) <= (2026, 7)
    ]
    bsm_root = tmp_path / "bsm"
    bsm = pa.table(
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
            "strike": [14000.0, 14050.0],
            "option_type": ["CALL", "CALL"],
            "request_id": ["a", "b"],
            "bsm_status": ["ok", "blocked"],
        }
    )
    bsm_audits = []
    for month in months:
        year, number = month.split("-")
        path = bsm_root / f"year={year}/month={number}/part-000.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(bsm, path)
        bsm_audits.append(
            {
                "month": month,
                "rows": 2,
                "output_sha256": sha256_file(path),
                "status_counts": {"ok": 1, "blocked": 1},
            }
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
            "months_audited": bsm_audits,
        },
    )

    span_root = tmp_path / "span"
    inventory = []
    span_rows = []
    for index, slot in enumerate(release_module.SLOTS):
        row = {
            "date": date(2021, 1, 4),
            "time_slot": slot,
            "symbol": "NIFTY",
            "instrument": "CE",
            "expiry": date(2021, 1, 28),
            "strike": 14000.0,
            "price": 100.0 + index,
            "delta": 0.5,
            "implied_vol": 0.2,
            "price_scan_range": 1.0,
            "vol_scan_range": 1.0,
            "cvf": 1.0,
            "composite_delta": 0.5,
            "source_file": f"{slot}.zip",
            "source_sha256": f"{index + 1:064x}",
            "source_member": "member.spn",
            "effective_time_source": "unknown",
            "span_effective_ts_ist": None,
        }
        row.update({f"s{scenario}": float(scenario) for scenario in range(1, 17)})
        span_rows.append(row)
    span_table = pa.Table.from_pylist(span_rows)
    for month in months:
        year, number = month.split("-")
        path = span_root / f"{year}_{number}.parquet"
        span_root.mkdir(parents=True, exist_ok=True)
        pq.write_table(span_table, path)
        inventory.append(
            {
                "month": month,
                "natural_key_duplicates": 0,
                "path": str(path.resolve()),
                "row_count": len(span_rows),
                "sha256": sha256_file(path),
                "size_bytes": path.stat().st_size,
            }
        )

    gap_rows = []
    start = date(1990, 1, 1)
    for index in range(3993):
        corrupt = index < 52
        gap_rows.append(
            {
                "trading_date": (start + timedelta(days=index)).isoformat(),
                "slot": release_module.SLOTS[index % 6],
                "classification_outcome": (
                    "source_boundary" if index < 93 else "accepted_absence"
                ),
                "source_boundary_category": "fixture" if index < 93 else None,
                "source_boundary_proven": index < 93,
                "evidence_basis": "fixture",
                "evidence_event_id": f"event-{index}",
                "availability_event_sha256": f"{index + 1:064x}",
                "gap_category": (
                    "repeated_corrupt_http_200" if corrupt else "ordinary_unavailable"
                ),
                "safe_downstream_status": "NO_SPAN_OBSERVATION_DO_NOT_FILL",
                "final_download_state": (
                    "corrupt_inner_zip" if corrupt else "not_returned_http_404"
                ),
            }
        )
    final_root = tmp_path / "final"
    gap_path = final_root / "span_source_gap_manifest.parquet"
    gap_path.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(gap_rows), gap_path)
    gap_sha = sha256_file(gap_path)
    release_path = final_root / "SPAN_PHASE1_RELEASE_MANIFEST.json"
    release_payload = {
        "schema_version": "span-phase1-release/v1",
        "release_status": "ACCEPTED_WITH_SOURCE_GAPS",
        "technical_audit_outcome": "BLOCKED_SOURCE",
        "repository_commit_sha": release_module.EXPECTED_SPAN_COMMIT,
        "counts": {
            "monthly_parquets": 67,
            "compacted_rows": 24_870_123,
            "natural_key_duplicates": 0,
            "expected_cells": 12_132,
            "accounted_cells": 12_132,
            "source_gap_manifest_rows": 3_993,
            "accepted_unavailable_cells": 3_941,
            "repeated_corrupt_source_cells": 52,
            "source_boundary_cells": 93,
            "orphan_partial_files": 0,
            "unresolved_non_source_boundary_cells": 0,
        },
        "integrity_contract": {
            "corrupt_archives_remain_corrupt": True,
            "no_backfill": True,
            "no_fake_rows": True,
            "no_forward_fill": True,
            "no_interpolation": True,
            "technical_outcome_preserved": True,
        },
        "source_gap_artifacts": {
            "parquet": {"path": str(gap_path.resolve()), "sha256": gap_sha}
        },
        "monthly_inventory": inventory,
    }
    _write_json(release_path, release_payload)
    release_sha = sha256_file(release_path)
    handoff_path = final_root / "DHAN_SPAN_HANDOFF.json"
    _write_json(
        handoff_path,
        {
            "schema_version": "dhan-span-handoff/v1",
            "release_status": "ACCEPTED_WITH_SOURCE_GAPS",
            "technical_audit_outcome": "BLOCKED_SOURCE",
            "release_manifest": {
                "path": str(release_path.resolve()),
                "sha256": release_sha,
            },
            "source_gap_manifest": {
                "parquet": {"path": str(gap_path.resolve()), "sha256": gap_sha}
            },
            "monthly_inventory": inventory,
        },
    )
    monkeypatch.setattr(release_module, "EXPECTED_GAP_SHA256", gap_sha)
    monkeypatch.setattr(release_module, "EXPECTED_RELEASE_SHA256", release_sha)
    monkeypatch.setattr(
        release_module, "EXPECTED_HANDOFF_SHA256", sha256_file(handoff_path)
    )
    return {
        "bsm_root": str(bsm_root),
        "bsm_terminal_audit": str(bsm_audit),
        "span_compacted_root": str(span_root),
        "span_release_manifest": str(release_path),
        "span_handoff": str(handoff_path),
        "span_source_gap_manifest": str(gap_path),
        "bod_output_root": str(tmp_path / "bod"),
        "six_slot_output_root": str(tmp_path / "six"),
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
