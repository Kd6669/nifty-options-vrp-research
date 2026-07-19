from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import tempfile

import pyarrow.parquet as pq
import pytest

from dhan_data_fetch_stream.acquisition import (
    AcquisitionEngine,
    CurrentInstrumentSnapshot,
    DhanRequestError,
    FutureIdentity,
    RequestCell,
    date_chunks,
    normalize_response,
    parse_current_instrument_snapshot,
    partitioned_date_chunks,
    plan_current_futures,
    plan_india_vix,
    quarantine_orphan_partials,
    plan_rolling_options,
    plan_spot,
    redact_secret_text,
    rebuild_silver_from_bronze,
    sha256_file,
    validate_parallel_arrays,
    validate_normalized_rows,
)
from dhan_data_fetch_stream.core import DhanCredentials
from dhan_data_fetch_stream.cli import build_parser


class FakeTransport:
    def __init__(self, response: dict[str, object] | None = None, error: Exception | None = None) -> None:
        self.credentials = DhanCredentials("client", "super-secret-token")
        self.response = response
        self.error = error
        self.calls = 0

    def post(self, endpoint: str, payload: dict[str, object]) -> dict[str, object]:
        self.calls += 1
        if self.error:
            raise self.error
        assert self.response is not None
        return self.response


def test_date_chunks_have_noninclusive_bounds_and_limits() -> None:
    rolling = date_chunks(date(2026, 1, 1), date(2026, 2, 15), 30)
    assert rolling == [(date(2026, 1, 1), date(2026, 1, 31)), (date(2026, 1, 31), date(2026, 2, 16))]
    assert all((end - start).days <= 30 for start, end in rolling)
    monthly = partitioned_date_chunks(date(2026, 1, 1), date(2026, 2, 15), 30)
    assert monthly == [
        (date(2026, 1, 1), date(2026, 1, 31)),
        (date(2026, 1, 31), date(2026, 2, 1)),
        (date(2026, 2, 1), date(2026, 2, 16)),
    ]


def test_rolling_plan_requires_explicit_expiry_codes_and_labels_surface_honestly() -> None:
    with pytest.raises(ValueError, match="expiry_codes"):
        plan_rolling_options(start_date=date(2026, 1, 1), end_date=date(2026, 1, 2), expiry_codes=())

    cells = plan_rolling_options(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 2),
        expiry_codes=(1,),
        expiry_flags=("WEEK",),
        moneyness_width=1,
    )

    assert len(cells) == 6
    assert {cell.payload["strike"] for cell in cells} == {"ATM", "ATM-1", "ATM+1"}
    assert all(cell.metadata["surface"] == "rolling_moneyness_not_absolute_full_chain" for cell in cells)
    assert all("token" not in json.dumps(cell.payload).lower() for cell in cells)


def test_spot_and_current_future_plans_are_bounded_and_do_not_infer_expired_ids() -> None:
    spot_cells = plan_spot(date(2025, 1, 1), date(2025, 7, 1))
    assert len(spot_cells) == 7
    assert all(
        cell.payload["fromDate"][:7]
        == (date.fromisoformat(cell.payload["toDate"][:10]) - date.resolution).isoformat()[:7]
        for cell in spot_cells
    )
    snapshot = CurrentInstrumentSnapshot(
        "2026-07-15T00:00:00+00:00",
        "master",
        "a" * 64,
        "13",
        (FutureIdentity("61093", "NIFTY-Jul2026-FUT", "NIFTY JUL FUT", "2026-07-28 14:30:00", 65),),
    )
    cells = plan_current_futures(date(2021, 1, 1), date(2021, 1, 2), snapshot)
    assert [cell.payload["securityId"] for cell in cells] == ["61093"]
    assert cells[0].metadata["coverage_constraint"] == "current_active_contract_only"


def test_india_vix_plan_is_independent_monthly_and_uses_official_master_identity() -> None:
    cells = plan_india_vix(date(2025, 1, 1), date(2025, 7, 1))

    assert len(cells) == 7
    assert {cell.dataset for cell in cells} == {"india_vix"}
    assert all(cell.payload["securityId"] == "21" for cell in cells)
    assert all(cell.payload["exchangeSegment"] == "IDX_I" for cell in cells)
    assert all(cell.payload["instrument"] == "INDEX" for cell in cells)
    assert all(cell.metadata["underlying"] == "INDIA VIX" for cell in cells)
    assert all(cell.metadata["official_master_identity"]["security_id"] == "21" for cell in cells)
    assert all(
        (date.fromisoformat(cell.payload["toDate"][:10]) - date.fromisoformat(cell.payload["fromDate"][:10])).days
        <= 90
        for cell in cells
    )
    assert all(
        cell.payload["fromDate"][:7]
        == (date.fromisoformat(cell.payload["toDate"][:10]) - date.resolution).isoformat()[:7]
        for cell in cells
    )


def test_india_vix_normalization_is_typed_and_keeps_independent_identity() -> None:
    cell = plan_india_vix(date(2026, 1, 1), date(2026, 1, 1))[0]
    response = {
        "timestamp": [1767249000],
        "open": [14.25],
        "high": [14.5],
        "low": [14.0],
        "close": [14.4],
        "volume": [0],
    }

    rows = normalize_response(cell, response)

    assert rows[0]["underlying"] == "INDIA VIX"
    assert rows[0]["security_id"] == "21"
    assert rows[0]["open_interest"] is None
    assert isinstance(rows[0]["close"], float)


def test_india_vix_cli_and_engine_keep_separate_typed_resumable_quality_partitions() -> None:
    args = build_parser().parse_args(
        ["backfill-india-vix", "--start-date", "2026-01-01", "--end-date", "2026-01-01"]
    )
    assert args.command == "backfill-india-vix"

    response = {
        "timestamp": [1767249000, 1767249060],
        "open": [14.25, 14.4],
        "high": [14.5, 14.3],
        "low": [14.0, 14.0],
        "close": [14.4, 14.2],
        "volume": [0, 0],
    }
    cell = plan_india_vix(date(2026, 1, 1), date(2026, 1, 1))[0]
    transport = FakeTransport(response)
    with tempfile.TemporaryDirectory() as tmp:
        engine = AcquisitionEngine(
            root=tmp,
            transport=transport,
            max_retries=1,
            requests_per_second=100,
            sleep=lambda _: None,
        )
        first = engine.run([cell])[0]
        resumed = engine.run([cell])[0]
        manifest = json.loads(Path(first.manifest_path).read_text(encoding="utf-8"))
        silver = pq.read_table(first.silver_path)
        quarantine = pq.read_table(manifest["quality_exception_path"])

    assert first.status == "completed"
    assert resumed.status == "already_valid"
    assert transport.calls == 1
    assert first.rows == 1
    assert "bronze\\india_vix\\" in first.bronze_path
    assert "silver\\india_vix\\" in first.silver_path
    assert manifest["dataset"] == "india_vix"
    assert manifest["metadata"]["official_master_identity"]["security_id"] == "21"
    assert manifest["quality_exception_rows"] == 1
    assert silver.schema.metadata[b"dataset"] == b"india_vix"
    assert silver.schema.field("close").type == silver.schema.field("open").type
    assert silver.to_pylist()[0]["underlying"] == "INDIA VIX"
    assert quarantine.to_pylist()[0]["failure_code"] == "ohlc_invariant"


def test_parallel_array_mismatch_is_rejected() -> None:
    with pytest.raises(DhanRequestError, match="length mismatch"):
        validate_parallel_arrays({"timestamp": [1, 2], "open": [1]}, ("timestamp", "open"))


def test_normalized_duplicate_and_ohlc_invariants_are_rejected() -> None:
    cell = RequestCell("spot", "/charts/intraday", {})
    row = {
        "timestamp_ist": "same", "underlying": "NIFTY", "open": 100, "high": 90,
        "low": 80, "close": 95, "volume": 1, "open_interest": None,
    }
    with pytest.raises(DhanRequestError, match="OHLC"):
        validate_normalized_rows(cell, [row])
    valid = dict(row, high=110)
    with pytest.raises(DhanRequestError, match="duplicate natural key"):
        validate_normalized_rows(cell, [valid, valid])


def test_rolling_normalization_preserves_request_label_and_returned_strike() -> None:
    cell = plan_rolling_options(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 1),
        expiry_codes=(1,),
        expiry_flags=("WEEK",),
        moneyness_width=0,
        option_types=("CALL",),
    )[0]
    response = {
        "data": {
            "ce": {
                "timestamp": [1767249000],
                "open": [100],
                "high": [110],
                "low": [90],
                "close": [105],
                "iv": [12.5],
                "volume": [10],
                "strike": [25000],
                "oi": [20],
                "spot": [24995],
            }
        }
    }

    rows = normalize_response(cell, response)

    assert len(rows) == 1
    assert str(rows[0]["strike"]) == "25000.0000"
    assert rows[0]["moneyness_label"] == "ATM"
    assert rows[0]["expiry_date"] is None
    assert rows[0]["provider_iv_raw"] == 12.5
    assert rows[0]["session_status"] == "regular_session"


def test_rolling_normalization_enforces_half_open_to_date_when_provider_overshoots() -> None:
    cell = plan_rolling_options(
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
        expiry_codes=(1,),
        expiry_flags=("WEEK",),
        moneyness_width=0,
        option_types=("CALL",),
    )[0]
    def epoch(text: str) -> float:
        from datetime import datetime
        from zoneinfo import ZoneInfo
        return datetime.fromisoformat(text).replace(tzinfo=ZoneInfo("Asia/Kolkata")).timestamp()
    response = {
        "data": {
            "ce": {
                "timestamp": [epoch("2026-07-14T09:15:00"), epoch("2026-07-15T09:15:00")],
                "open": [100, 101], "high": [110, 111], "low": [90, 91], "close": [105, 106],
                "iv": [12.5, 12.6], "volume": [10, 11], "strike": [25000, 25100],
                "oi": [20, 21], "spot": [24995, 25095],
            }
        }
    }

    rows = normalize_response(cell, response)

    assert len(rows) == 1
    assert rows[0]["trade_date"] == date(2026, 7, 14)


def test_master_parser_selects_exact_nifty_not_niftynext() -> None:
    text = "\n".join(
        [
            "SEM_EXM_EXCH_ID,SEM_SEGMENT,SEM_SMST_SECURITY_ID,SEM_INSTRUMENT_NAME,SEM_TRADING_SYMBOL,SEM_CUSTOM_SYMBOL,SEM_EXPIRY_DATE,SEM_LOT_UNITS",
            "NSE,I,13,INDEX,NIFTY,Nifty 50,,1",
            "NSE,D,1,FUTIDX,NIFTYNXT50-Jul2026-FUT,NIFTYNXT50 JUL FUT,2026-07-28 14:30:00,25",
            "NSE,D,61093,FUTIDX,NIFTY-Jul2026-FUT,NIFTY JUL FUT,2026-07-28 14:30:00,65",
        ]
    )

    snapshot = parse_current_instrument_snapshot(text)

    assert snapshot.nifty_index_security_id == "13"
    assert [future.security_id for future in snapshot.futures] == ["61093"]


def test_engine_writes_atomic_hashed_artifacts_and_resumes_without_network() -> None:
    response = {
        "timestamp": [1767249000],
        "open": [100],
        "high": [110],
        "low": [90],
        "close": [105],
        "volume": [10],
    }
    cell = plan_spot(date(2026, 1, 1), date(2026, 1, 1))[0]
    transport = FakeTransport(response)
    with tempfile.TemporaryDirectory() as tmp:
        engine = AcquisitionEngine(root=tmp, transport=transport, max_retries=1, requests_per_second=100, sleep=lambda _: None)
        first = engine.run([cell])[0]
        second = engine.run([cell])[0]

        manifest = json.loads(Path(first.manifest_path).read_text(encoding="utf-8"))
        table = pq.read_table(first.silver_path)
        partials = list(Path(tmp).rglob("*.partial"))

    assert first.status == "completed"
    assert first.rows == 1
    assert second.status == "already_valid"
    assert transport.calls == 1
    assert manifest["credentials_persisted"] is False
    assert manifest["bronze_sha256"] and manifest["silver_sha256"]
    assert table.schema.metadata[b"schema_version"] == b"1.2.0"
    assert not partials


def test_engine_quarantines_noncanonical_partial_without_touching_canonical() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        orphan = root / "bronze" / "options" / "year=2024" / "month=03" / "orphan.json.partial"
        orphan.parent.mkdir(parents=True)
        orphan.write_bytes(b"interrupted")
        canonical_partial = root / "silver" / "options" / "year=2024" / "month=03" / "valid.parquet.partial"
        canonical_partial.parent.mkdir(parents=True)
        canonical_partial.write_bytes(b"unfinished sibling")
        canonical = canonical_partial.with_suffix("")
        canonical.write_bytes(b"canonical remains")

        records = quarantine_orphan_partials(root)

        assert len(records) == 2
        assert not orphan.exists()
        assert not canonical_partial.exists()
        assert canonical.read_bytes() == b"canonical remains"
        conflict = next(record for record in records if record["canonical_exists"])
        assert conflict["canonical_sha256"] == sha256_file(canonical)
        assert Path(conflict["quarantine_path"]).is_file()
        audit = list((root / "manifests" / "orphan_partials").glob("*.json"))
        assert len(audit) == 1


def test_parallel_engine_bounds_credential_failure_fanout() -> None:
    cells = [
        RequestCell("spot", "/charts/intraday", {"securityId": str(index)}, {})
        for index in range(100)
    ]
    error = DhanRequestError("invalid", status=401, code="DH-901", retryable=False)
    transport = FakeTransport(error=error)
    with tempfile.TemporaryDirectory() as tmp:
        engine = AcquisitionEngine(root=tmp, transport=transport, workers=5)
        outcomes = engine.run(cells)

        manifests = list((Path(tmp) / "manifests" / "requests").glob("*.json"))

    assert 1 <= transport.calls <= 5
    assert 1 <= len(outcomes) <= 5
    assert len(manifests) <= 5
    assert {outcome.status for outcome in outcomes} == {"credential_blocked"}


def test_schema_upgrade_rebuilds_silver_from_immutable_cached_bronze() -> None:
    response = {
        "timestamp": [1767249000], "open": [100], "high": [110], "low": [90],
        "close": [105], "volume": [10],
    }
    cell = plan_spot(date(2026, 1, 1), date(2026, 1, 1))[0]
    transport = FakeTransport(response)
    with tempfile.TemporaryDirectory() as tmp:
        engine = AcquisitionEngine(root=tmp, transport=transport, max_retries=1)
        first = engine.run([cell])[0]
        manifest_path = Path(first.manifest_path)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        bronze_before = Path(first.bronze_path).read_bytes()
        manifest["normalizer_version"] = "older"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        rebuilt = engine.run([cell])[0]
        bronze_after = Path(first.bronze_path).read_bytes()

    assert rebuilt.status == "completed"
    assert rebuilt.attempts == 0
    assert transport.calls == 1
    assert bronze_before == bronze_after


def test_offline_rebuild_requires_no_transport_or_credentials() -> None:
    response = {
        "timestamp": [1767249000], "open": [100], "high": [110], "low": [90],
        "close": [105], "volume": [10],
    }
    cell = plan_spot(date(2026, 1, 1), date(2026, 1, 1))[0]
    with tempfile.TemporaryDirectory() as tmp:
        first = AcquisitionEngine(root=tmp, transport=FakeTransport(response), max_retries=1).run([cell])[0]
        Path(first.silver_path).unlink()

        stats = rebuild_silver_from_bronze(tmp)

        manifest = json.loads(Path(first.manifest_path).read_text(encoding="utf-8"))
        silver_exists = Path(manifest["silver_path"]).is_file()
    assert stats.rebuilt == 1
    assert stats.failures == 0
    assert stats.rows == 1
    assert silver_exists


def test_engine_persists_only_redacted_credential_failure() -> None:
    error = DhanRequestError(
        "authorization: Bearer super-secret-token",
        status=401,
        code="DH-901",
        retryable=False,
    )
    transport = FakeTransport(error=error)
    cell = RequestCell("spot", "/charts/intraday", {"securityId": "13"})
    with tempfile.TemporaryDirectory() as tmp:
        outcome = AcquisitionEngine(root=tmp, transport=transport, max_retries=1).run([cell])[0]
        all_text = "\n".join(path.read_text(encoding="utf-8") for path in Path(tmp).rglob("*.json"))

    assert outcome.status == "credential_blocked"
    assert "super-secret-token" not in all_text
    assert "<redacted>" in all_text


def test_engine_quarantines_parallel_array_mismatch_with_bronze_hash() -> None:
    response = {
        "timestamp": [1767249000, 1767249060],
        "open": [100],
        "high": [110, 111],
        "low": [90, 91],
        "close": [105, 106],
        "volume": [10, 11],
    }
    cell = plan_spot(date(2026, 1, 1), date(2026, 1, 1))[0]
    with tempfile.TemporaryDirectory() as tmp:
        outcome = AcquisitionEngine(root=tmp, transport=FakeTransport(response), max_retries=1).run([cell])[0]
        manifest = json.loads(Path(outcome.manifest_path).read_text(encoding="utf-8"))
        exceptions = list(Path(tmp).glob("exceptions/responses/*.json"))

    assert outcome.status == "invalid_response"
    assert manifest["bronze_sha256"]
    assert manifest["error_code"] == "invalid_parallel_arrays"
    assert len(exceptions) == 1


def test_engine_quarantines_bad_rows_but_keeps_valid_partition() -> None:
    response = {
        "timestamp": [1767249000, 1767249060],
        "open": [100, 100], "high": [110, 90], "low": [90, 80],
        "close": [105, 95], "volume": [10, 11],
    }
    cell = plan_spot(date(2026, 1, 1), date(2026, 1, 1))[0]
    with tempfile.TemporaryDirectory() as tmp:
        outcome = AcquisitionEngine(root=tmp, transport=FakeTransport(response), max_retries=1).run([cell])[0]
        manifest = json.loads(Path(outcome.manifest_path).read_text(encoding="utf-8"))
        exception_table = pq.read_table(manifest["quality_exception_path"])

    assert outcome.status == "completed"
    assert outcome.rows == 1
    assert manifest["quality_exception_rows"] == 1
    assert exception_table.to_pylist()[0]["failure_code"] == "ohlc_invariant"


def test_redaction_removes_exact_secrets_headers_and_jwts() -> None:
    raw = "access-token: secret eyJhbGciOiJub25lIn0.eyJhIjoxfQ.signature"
    redacted = redact_secret_text(raw, ("secret",))
    assert "secret" not in redacted
    assert "eyJ" not in redacted
