from __future__ import annotations

from datetime import date, datetime
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from dhan_data_fetch_stream.pre_bsm_runner import run_pre_bsm_incremental
from dhan_data_fetch_stream.enrichment import READY


IST = ZoneInfo("Asia/Kolkata")


def _write(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def test_incremental_pre_bsm_runner_writes_hashed_resume_boundary() -> None:
    with TemporaryDirectory() as temp:
        root = Path(temp)
        options = root / "silver" / "options"
        spot = root / "silver" / "spot"
        vix = root / "vix" / "silver" / "india_vix"
        output = root / "gold_preparation"
        timestamp = datetime(2026, 7, 14, 9, 16, tzinfo=IST)
        _write(
            options / ("a" * 64 + ".parquet"),
            [
                {
                    "timestamp_ist": timestamp,
                    "trade_date": date(2026, 7, 14),
                    "session_status": "regular_session",
                    "underlying": "NIFTY",
                    "expiry_flag": "WEEK",
                    "expiry_code": 1,
                    "moneyness_label": "ATM",
                    "strike": 25000.0,
                    "option_type": "CALL",
                    "close": 100.0,
                    "provider_spot": 24990.0,
                }
            ],
        )
        market_row = {
            "timestamp_ist": timestamp,
            "trade_date": date(2026, 7, 14),
            "session_status": "regular_session",
            "underlying": "NIFTY",
            "close": 25010.0,
        }
        _write(spot / "spot.parquet", [market_row])
        _write(vix / "vix.parquet", [{**market_row, "underlying": "INDIA VIX", "close": 13.4}])
        expiry_path = root / "expiry.json"
        expiry_path.write_text(
            json.dumps(
                {
                    "rows": [
                        {
                            "underlying": "NIFTY",
                            "trade_date": "2026-07-14",
                            "expiry_type": "weekly",
                            "expiry_code": 1,
                            "actual_expiry_date": "2026-07-21",
                            "actual_expiry_timestamp_ist": "2026-07-21T15:30:00+05:30",
                            "expiry_rule_weekday": "Tuesday",
                            "expiry_rule_effective_from": "2025-09-01",
                            "expiry_holiday_adjusted": False,
                            "original_scheduled_expiry": "2026-07-21",
                            "mapping_status": "proven",
                            "mapping_confidence": "high",
                            "source_id": "NSE_BHAVCOPY",
                            "circular_id": "NSE/FAOP/TEST",
                            "source_sha256": "a" * 64,
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        rules_path = root / "rules.json"
        rules_path.write_text(
            json.dumps(
                {
                    "rules": [
                        {
                            "underlying": "NIFTY",
                            "expiry_type": "weekly",
                            "contract_expiry_from": "2026-01-01",
                            "contract_expiry_to": "2026-12-31",
                            "contract_lot_size": 65,
                            "market_lot": 65,
                            "contract_multiplier": 1.0,
                            "trading_unit": "65 units",
                            "tick_size": 0.05,
                            "mapping_status": "proven",
                            "mapping_confidence": "high",
                            "rule_id": "lot-rule",
                            "circular_id": "NSE/FAOP/TEST-LOT",
                            "source_id": "NSE_CIRCULAR",
                            "source_sha256": "b" * 64,
                            "effective_from": "2026-01-01",
                            "effective_to": "2026-12-31",
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        first = run_pre_bsm_incremental(
            options_root=options,
            spot_root=spot,
            vix_root=vix,
            contract_rules=rules_path,
            actual_expiries=expiry_path,
            output_root=output,
            acquisition_terminally_accounted=False,
        )
        second = run_pre_bsm_incremental(
            options_root=options,
            spot_root=spot,
            vix_root=vix,
            contract_rules=rules_path,
            actual_expiries=expiry_path,
            output_root=output,
            acquisition_terminally_accounted=False,
        )
        gated = run_pre_bsm_incremental(
            options_root=options,
            spot_root=spot,
            vix_root=vix,
            contract_rules=rules_path,
            actual_expiries=expiry_path,
            output_root=output,
            acquisition_terminally_accounted=True,
        )

        assert first.options_files_processed == 1
        assert first.input_option_rows == 1
        assert first.canonical_rows == 1
        assert first.blocked_rows == 0
        assert first.bsm_executed is False
        assert second.options_files_processed == 0
        assert second.options_files_resumed == 1
        assert second.input_option_rows == 1
        assert gated.options_files_processed == 1
        assert gated.options_files_resumed == 0
        manifests = list((output / "enriched_options" / "version=1.0.0" / "manifests").glob("a*.json"))
        assert len(manifests) == 1
        payload = json.loads(manifests[0].read_text(encoding="utf-8"))
        assert payload["bsm_executed"] is False
        assert payload["bsm_gate"]["status"] == READY
        assert payload["input_lineage"]["options"]["row_count"] == 1
        assert payload["input_lineage"]["acquisition_terminally_accounted"] is True
        assert not list(output.rglob("*.partial"))
