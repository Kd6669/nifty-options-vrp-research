from __future__ import annotations

from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from dhan_data_fetch_stream.pre_bsm_quality_patch import _atomic_json, _patch_query


IST = ZoneInfo("Asia/Kolkata")


def _row(
    *,
    timestamp: datetime,
    label: str,
    strike: float,
    option_type: str,
    provider_spot: float,
    independent_spot: float,
) -> dict[str, object]:
    return {
        "timestamp_ist": timestamp,
        "trade_date": timestamp.date(),
        "underlying": "NIFTY",
        "expiry_flag": "MONTH",
        "expiry_code": 1,
        "moneyness_label": label,
        "strike": strike,
        "option_type": option_type,
        "provider_spot": provider_spot,
        "independent_nifty_spot": independent_spot,
        "bsm_gate_status": "READY",
        "bsm_gate_failure_reason": None,
    }


def test_ladder_and_independent_spot_moneyness_are_separate() -> None:
    timestamp = datetime(2024, 1, 2, 10, 0, tzinfo=IST)
    rows = [
        _row(
            timestamp=timestamp,
            label="ATM",
            strike=21_500.0,
            option_type="CALL",
            provider_spot=21_510.0,
            independent_spot=21_560.0,
        ),
        _row(
            timestamp=timestamp,
            label="ATM+1",
            strike=21_575.0,
            option_type="CALL",
            provider_spot=21_510.0,
            independent_spot=21_560.0,
        ),
    ]
    with TemporaryDirectory() as temp:
        source = Path(temp) / "source.parquet"
        pq.write_table(pa.Table.from_pylist(rows), source)
        table = duckdb.connect().execute(_patch_query(source)).to_arrow_table()

    by_label = {label: index for index, label in enumerate(table["moneyness_label"].to_pylist())}
    mismatch = by_label["ATM+1"]
    assert table["ladder_atm_strike"][mismatch].as_py() == 21_500.0
    assert table["expected_strike"][mismatch].as_py() == 21_550.0
    assert table["strike_ladder_valid"][mismatch].as_py() is False
    assert table["strike_ladder_failure_reason"][mismatch].as_py() == "strike_mismatch"
    assert table["recomputed_atm_strike"][mismatch].as_py() == 21_550.0
    assert table["computed_moneyness_offset"][mismatch].as_py() == 0.5
    assert table["computed_moneyness_label"][mismatch].as_py() == "NON_50_GRID"
    # A ladder provenance mismatch alone does not block valid pricing inputs.
    assert table["bsm_gate_status"][mismatch].as_py() == "READY"


def test_all_eight_proven_payload_rows_are_reason_coded_and_blocked() -> None:
    cases = [
        (datetime(2023, 1, 6, 15, 29, tzinfo=IST), 41_200.0, 42_218.0, 17_863.5),
        (datetime(2026, 1, 12, 10, 44, tzinfo=IST), 67_100.0, 68_101.9, 25_563.15),
        (datetime(2026, 1, 12, 11, 8, tzinfo=IST), 67_000.0, 67_957.7, 25_546.3),
        (datetime(2026, 1, 12, 11, 9, tzinfo=IST), 66_900.0, 67_923.35, 25_541.4),
    ]
    rows = []
    for timestamp, strike, provider, independent in cases:
        for option_type in ("CALL", "PUT"):
            rows.append(
                _row(
                    timestamp=timestamp,
                    label="ATM-10",
                    strike=strike,
                    option_type=option_type,
                    provider_spot=provider,
                    independent_spot=independent,
                )
            )
    with TemporaryDirectory() as temp:
        source = Path(temp) / "source.parquet"
        pq.write_table(pa.Table.from_pylist(rows), source)
        table = duckdb.connect().execute(_patch_query(source)).to_arrow_table()

    assert table.num_rows == 8
    assert table["proven_severe_payload_corruption"].to_pylist() == [True] * 8
    assert table["quality_severe_anomaly"].to_pylist() == [True] * 8
    assert table["bsm_gate_status"].to_pylist() == ["BLOCKED"] * 8
    assert set(table["bsm_gate_failure_reason"].to_pylist()) == {
        "proven_severe_provider_payload_corruption"
    }


def test_missing_atm_peer_is_audited_without_row_loss() -> None:
    row = _row(
        timestamp=datetime(2025, 4, 3, 10, 0, tzinfo=IST),
        label="ATM-8",
        strike=22_000.0,
        option_type="CALL",
        provider_spot=22_400.0,
        independent_spot=22_400.0,
    )
    with TemporaryDirectory() as temp:
        source = Path(temp) / "source.parquet"
        pq.write_table(pa.Table.from_pylist([row]), source)
        table = duckdb.connect().execute(_patch_query(source)).to_arrow_table()

    assert table.num_rows == 1
    assert table["strike_ladder_valid"][0].as_py() is False
    assert table["strike_ladder_failure_reason"][0].as_py() == "missing_atm_peer"
    assert table["bsm_gate_status"][0].as_py() == "READY"


def test_atomic_status_publish_retries_transient_windows_sharing_lock() -> None:
    with TemporaryDirectory() as temp:
        path = Path(temp) / "status.json"
        real_replace = __import__("os").replace
        attempts = 0

        def flaky_replace(source: Path, target: Path) -> None:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise PermissionError("transient sharing lock")
            real_replace(source, target)

        with patch("dhan_data_fetch_stream.pre_bsm_quality_patch.os.replace", flaky_replace), patch(
            "dhan_data_fetch_stream.pre_bsm_quality_patch.time.sleep"
        ):
            _atomic_json(path, {"state": "running"})

        assert attempts == 3
        assert path.read_text(encoding="utf-8").strip() == '{\n  "state": "running"\n}'
        assert not list(path.parent.glob("*.partial"))
