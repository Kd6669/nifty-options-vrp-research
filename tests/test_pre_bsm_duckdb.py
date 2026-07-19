from __future__ import annotations

from datetime import datetime
import json
import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from dhan_data_fetch_stream.pre_bsm_duckdb import DuckDbPreBsmConfig, run_pre_bsm_duckdb


IST = ZoneInfo("Asia/Kolkata")


def _write(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _option(ts: datetime, *, label: str, session: str = "regular_session") -> dict[str, object]:
    return {
        "request_id": f"option-{label}",
        "timestamp_ist": ts,
        "trade_date": ts.date(),
        "session_status": session,
        "underlying": "NIFTY",
        "expiry_flag": "WEEK",
        "expiry_code": 1,
        "moneyness_label": label,
        "strike": 14_000.0,
        "option_type": "CALL",
        "close": 100.0,
        "provider_spot": 13_995.0,
    }


def _market(ts: datetime, close: float, *, request: str, session: str = "regular_session") -> dict[str, object]:
    return {
        "request_id": request,
        "provider": "dhan_intraday",
        "timestamp_ist": ts,
        "trade_date": ts.date(),
        "session_status": session,
        "underlying": "NIFTY",
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 0,
        "security_id": "13",
        "open_interest": None,
    }


def _dimensions(root: Path) -> tuple[Path, Path]:
    expiry = root / "expiry.parquet"
    rules = root / "rules.parquet"
    expiry_rows = []
    for trade_date, actual_expiry in (("2021-01-04", "2021-01-14"), ("2021-08-04", "2021-08-12")):
        expiry_rows.append(
            {
                "trade_date": trade_date,
                "underlying": "NIFTY",
                "expiry_type": "weekly",
                "expiry_code": 1,
                "actual_expiry_date": actual_expiry,
                "actual_expiry_timestamp_ist": f"{actual_expiry}T15:30:00+05:30",
                "expiry_rule_weekday": "Thursday",
                "expiry_rule_effective_from": "2021-01-01",
                "expiry_rule_effective_to": "2025-08-31",
                "expiry_holiday_adjusted": False,
                "original_scheduled_expiry": actual_expiry,
                "mapping_status": "proven",
                "mapping_confidence": "high",
                "source_id": "NSE_BHAVCOPY",
                "circular_id": "NSE/FAOP/TEST",
                "source_sha256": "a" * 64,
            }
        )
    _write(expiry, expiry_rows)
    _write(
        rules,
        [
            {
                "underlying": "NIFTY",
                "expiry_type": "weekly",
                "contract_expiry_from": "2021-01-01",
                "contract_expiry_to": "2021-12-31",
                "contract_lot_size": 50,
                "market_lot": 50,
                "contract_multiplier": None,
                "trading_unit": "one market lot",
                "tick_size": None,
                "rule_id": "NIFTY_LOT_2021",
                "circular_id": "NSE/FAOP/TEST-LOT",
                "source_id": "NSE_CIRCULAR",
                "source_sha256": "b" * 64,
                "effective_from": "2021-01-01",
                "effective_to": "2021-12-31",
                "mapping_status": "proven",
                "mapping_confidence": "high",
            }
        ],
    )
    return rules, expiry


def test_duckdb_v2_strict_asof_vix_policy_and_resume() -> None:
    with TemporaryDirectory() as temporary:
        root = Path(temporary)
        options = root / "silver" / "options"
        spot = root / "silver" / "spot"
        vix = root / "silver" / "india_vix"
        output = root / "pre_bsm_v2"
        temp = root / "spill"
        jan_0915 = datetime(2021, 1, 4, 9, 15, tzinfo=IST)
        jan_0916 = datetime(2021, 1, 4, 9, 16, tzinfo=IST)
        jan_0917 = datetime(2021, 1, 4, 9, 17, tzinfo=IST)
        aug_1000 = datetime(2021, 8, 4, 10, 0, tzinfo=IST)
        aug_100030 = datetime(2021, 8, 4, 10, 0, 30, tzinfo=IST)

        _write(
            options / "year=2021" / "month=01" / "options.parquet",
            [_option(jan_0915, label="ATM-1"), _option(jan_0916, label="ATM"), _option(jan_0917, label="ATM+1")],
        )
        _write(options / "year=2021" / "month=08" / "options.parquet", [_option(aug_100030, label="ATM")])
        _write(
            spot / "year=2021" / "month=01" / "spot.parquet",
            [
                _market(jan_0916, 14_010.0, request="spot-0916"),
                _market(jan_0917, 14_020.0, request="spot-0917-a"),
                _market(jan_0917, 14_021.0, request="spot-0917-b"),
            ],
        )
        _write(spot / "year=2021" / "month=08" / "spot.parquet", [_market(aug_1000, 16_250.0, request="spot-aug")])
        vix_row = _market(aug_1000, 13.6, request="vix-aug")
        vix_row["underlying"] = "INDIA VIX"
        vix_row["security_id"] = "21"
        _write(vix / "year=2021" / "month=08" / "vix.parquet", [vix_row])
        rules, expiry = _dimensions(root)
        cfg = DuckDbPreBsmConfig(threads=2, memory_limit="1GB", row_group_size=10_000, acquisition_terminally_accounted=True)

        first = run_pre_bsm_duckdb(
            options_root=options,
            spot_root=spot,
            vix_root=vix,
            contract_rules=rules,
            actual_expiries=expiry,
            output_root=output,
            temp_directory=temp,
            config=cfg,
        )
        second = run_pre_bsm_duckdb(
            options_root=options,
            spot_root=spot,
            vix_root=vix,
            contract_rules=rules,
            actual_expiries=expiry,
            output_root=output,
            temp_directory=temp,
            config=cfg,
        )

        assert first.months_processed == 2
        assert first.input_rows == first.output_rows == 4
        assert first.duplicate_right_rows == 2
        assert second.months_processed == 0
        assert second.months_resumed == 2
        jan_path = output / "enriched_options" / "version=2.0.0" / "year=2021" / "month=01" / "pre_bsm.parquet"
        aug_path = output / "enriched_options" / "version=2.0.0" / "year=2021" / "month=08" / "pre_bsm.parquet"
        jan = {row["moneyness_label"]: row for row in pq.read_table(jan_path).to_pylist()}
        aug = pq.read_table(aug_path).to_pylist()[0]
        assert jan["ATM-1"]["nifty_spot_join_failure_reason"] == "future_only_right_rows"
        assert jan["ATM"]["nifty_spot_match_method"] == "exact_timestamp"
        assert jan["ATM"]["india_vix_join_status"] == "source_unavailable"
        assert jan["ATM"]["bsm_gate_status"] == "READY"
        assert jan["ATM+1"]["nifty_spot_join_failure_reason"] == "duplicate_right_timestamp"
        assert aug["nifty_spot_match_method"] == "backward_asof"
        assert aug["nifty_spot_age_seconds"] == 30.0
        assert aug["india_vix_join_status"] == "MATCHED"
        assert aug["bsm_gate_status"] == "READY"
        assert aug["mte"] > 0 and aug["dte"] == aug["mte"] / 1440.0
        assert not list(output.rglob("*.partial"))
        before = _sha256(aug_path)
        deterministic = run_pre_bsm_duckdb(
            options_root=options,
            spot_root=spot,
            vix_root=vix,
            contract_rules=rules,
            actual_expiries=expiry,
            output_root=output,
            temp_directory=temp,
            config=cfg,
            months=("2021-08",),
            resume=False,
        )
        assert deterministic.months_processed == 1
        assert _sha256(aug_path) == before


def test_duckdb_v2_reprocesses_corrupt_published_partition() -> None:
    with TemporaryDirectory() as temporary:
        root = Path(temporary)
        ts = datetime(2021, 8, 4, 10, 0, tzinfo=IST)
        options = root / "options"
        spot = root / "spot"
        vix = root / "vix"
        _write(options / "year=2021" / "month=08" / "o.parquet", [_option(ts, label="ATM")])
        _write(spot / "year=2021" / "month=08" / "s.parquet", [_market(ts, 16_250.0, request="spot")])
        vix_row = _market(ts, 13.6, request="vix")
        vix_row["underlying"] = "INDIA VIX"
        _write(vix / "year=2021" / "month=08" / "v.parquet", [vix_row])
        rules, expiry = _dimensions(root)
        kwargs = {
            "options_root": options,
            "spot_root": spot,
            "vix_root": vix,
            "contract_rules": rules,
            "actual_expiries": expiry,
            "output_root": root / "out",
            "temp_directory": root / "temp",
            "config": DuckDbPreBsmConfig(row_group_size=10_000, acquisition_terminally_accounted=True),
        }
        run_pre_bsm_duckdb(**kwargs)
        target = root / "out" / "enriched_options" / "version=2.0.0" / "year=2021" / "month=08" / "pre_bsm.parquet"
        target.write_bytes(b"corrupt")
        rerun = run_pre_bsm_duckdb(**kwargs)
        assert rerun.months_processed == 1
        assert duckdb.sql("SELECT count(*) FROM read_parquet(?)", params=[str(target)]).fetchone()[0] == 1
        manifest = json.loads((root / "out" / "enriched_options" / "version=2.0.0" / "manifests" / "month=2021-08.json").read_text())
        assert manifest["audit"]["orphan_partial_count"] == 0
