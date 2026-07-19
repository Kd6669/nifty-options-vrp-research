from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow.parquet as pq

from dhan_data_fetch_stream.enrichment import (
    BLOCKED,
    ENRICHMENT_VERSION,
    READY,
    enrich_options_pre_bsm,
    load_dimension_rows,
    sha256_file,
    write_enriched_partitions,
)


def _option(timestamp: str = "2026-07-14T09:16:00+05:30", **updates: object) -> dict[str, object]:
    row: dict[str, object] = {
        "timestamp_ist": timestamp,
        "trade_date": timestamp[:10],
        "session_status": "regular_session",
        "underlying": "NIFTY",
        "expiry_flag": "WEEK",
        "expiry_code": 1,
        "moneyness_label": "ATM",
        "strike": 25000.0,
        "option_type": "CALL",
        "close": 125.0,
        "provider_spot": 24991.5,
    }
    row.update(updates)
    return row


def _market(timestamp: str, value: float, **updates: object) -> dict[str, object]:
    row: dict[str, object] = {
        "timestamp_ist": timestamp,
        "trade_date": timestamp[:10],
        "session_status": "regular_session",
        "underlying": "NIFTY",
        "close": value,
    }
    row.update(updates)
    return row


def _expiry(**updates: object) -> dict[str, object]:
    row: dict[str, object] = {
        "underlying": "NIFTY",
        "trade_date": "2026-07-14",
        "expiry_type": "weekly",
        "expiry_code": 1,
        "actual_expiry_date": "2026-07-21",
        "actual_expiry_timestamp_ist": "2026-07-21T15:30:00+05:30",
        "expiry_rule_weekday": "Tuesday",
        "expiry_rule_effective_from": "2025-09-01",
        "expiry_rule_effective_to": None,
        "expiry_holiday_adjusted": False,
        "original_scheduled_expiry": "2026-07-21",
        "mapping_status": "proven",
        "mapping_confidence": "high",
        "source_id": "NSE_EXPIRY_CALENDAR",
        "circular_id": "NSE/FAOP/TEST",
        "source_sha256": "a" * 64,
    }
    row.update(updates)
    return row


def _rule(**updates: object) -> dict[str, object]:
    row: dict[str, object] = {
        "underlying": "NIFTY",
        "expiry_type": "weekly",
        "contract_expiry_from": "2025-12-30",
        "contract_expiry_to": "2026-12-29",
        "contract_lot_size": 65,
        "market_lot": 65,
        "contract_multiplier": 1.0,
        "trading_unit": "65 units",
        "tick_size": 0.05,
        "mapping_status": "proven",
        "mapping_confidence": "high",
        "rule_id": "nifty-lot-v3",
        "circular_id": "NSE/FAOP/TEST-LOT",
        "source_id": "NSE_CONTRACT_MASTER",
        "source_sha256": "b" * 64,
        "effective_from": "2025-12-30",
        "effective_to": "2026-12-29",
    }
    row.update(updates)
    return row


def _ready_batch(*, option: dict[str, object] | None = None):
    options = [_option() if option is None else option]
    spot = [_market("2026-07-14T09:16:00+05:30", 25010.0)]
    vix = [_market("2026-07-14T09:15:30+05:30", 13.4)]
    return enrich_options_pre_bsm(
        options,
        spot,
        vix,
        [_rule()],
        [_expiry()],
        acquisition_terminally_accounted=True,
    )


def test_enrichment_preserves_provider_spot_and_uses_strict_independent_joins() -> None:
    batch = _ready_batch()
    row = batch.rows[0]

    assert row["provider_spot"] == 24991.5
    assert row["independent_nifty_spot"] == 25010.0
    assert row["nifty_spot_match_method"] == "exact_timestamp"
    assert row["nifty_spot_age_seconds"] == 0.0
    assert row["india_vix"] == 13.4
    assert row["india_vix_match_method"] == "backward_asof"
    assert row["india_vix_age_seconds"] == 30.0
    assert row["bsm_gate_status"] == READY
    assert batch.bsm_gate_status == READY


def test_join_never_looks_forward_or_crosses_trade_date_or_session() -> None:
    option = _option()
    future_and_cross_session = [
        _market("2026-07-14T09:16:01+05:30", 25011.0),
        _market("2026-07-14T09:16:00+05:30", 25012.0, session_status="auction"),
        _market("2026-07-13T15:30:00+05:30", 25013.0),
    ]
    batch = enrich_options_pre_bsm(
        [option],
        future_and_cross_session,
        [_market("2026-07-14T09:16:00+05:30", 13.4)],
        [_rule()],
        [_expiry()],
        acquisition_terminally_accounted=True,
    )

    row = batch.rows[0]
    assert row["independent_nifty_spot"] is None
    assert row["nifty_spot_join_failure_reason"] == "future_only_right_rows"
    assert row["bsm_gate_status"] == BLOCKED


def test_more_than_60_seconds_backward_is_blocked() -> None:
    batch = enrich_options_pre_bsm(
        [_option("2026-07-14T09:17:01+05:30")],
        [_market("2026-07-14T09:16:00+05:30", 25010.0)],
        [_market("2026-07-14T09:16:00+05:30", 13.4)],
        [_rule()],
        [_expiry()],
        acquisition_terminally_accounted=True,
    )

    assert batch.rows[0]["nifty_spot_join_failure_reason"] == "backward_outside_tolerance"
    assert batch.rows[0]["independent_nifty_spot"] is None


def test_duplicate_right_timestamps_are_quarantined_and_never_selected() -> None:
    duplicated = [
        _market("2026-07-14T09:16:00+05:30", 25010.0),
        _market("2026-07-14T09:16:00+05:30", 25011.0),
    ]
    batch = enrich_options_pre_bsm(
        [_option()],
        duplicated,
        [_market("2026-07-14T09:16:00+05:30", 13.4)],
        [_rule()],
        [_expiry()],
        acquisition_terminally_accounted=True,
    )

    assert batch.rows[0]["nifty_spot_join_failure_reason"] == "duplicate_right_timestamp"
    assert batch.rows[0]["independent_nifty_spot"] is None
    assert len(batch.duplicate_right_rows) == 2
    assert {row["quarantine_reason"] for row in batch.duplicate_right_rows} == {"duplicate_right_timestamp"}
    assert "duplicate_right_timestamps_quarantined" in batch.bsm_gate_reasons


def test_expiry_rule_and_fractional_time_are_point_in_time_mapped() -> None:
    row = _ready_batch().rows[0]
    expected_mte = 7 * 24 * 60 + (15 * 60 + 30) - (9 * 60 + 16)

    assert row["actual_expiry_date"].isoformat() == "2026-07-21"
    assert row["actual_expiry_timestamp_ist"].isoformat() == "2026-07-21T15:30:00+05:30"
    assert row["expiry_type"] == "weekly"
    assert row["contract_lot_size"] == 65.0
    assert row["contract_rule_id"] == "nifty-lot-v3"
    assert row["mte"] == expected_mte
    assert row["dte"] == expected_mte / 1440.0
    assert row["t_years_act365"] == expected_mte / (365.0 * 24.0 * 60.0)
    assert all(not key.startswith("bsm_iv") for key in row)


def test_ambiguous_expiry_is_explicitly_blocked_not_guessed() -> None:
    batch = enrich_options_pre_bsm(
        [_option()],
        [_market("2026-07-14T09:16:00+05:30", 25010.0)],
        [_market("2026-07-14T09:16:00+05:30", 13.4)],
        [_rule()],
        [_expiry(), _expiry(source_id="SECOND_OFFICIAL_SOURCE")],
        acquisition_terminally_accounted=True,
    )

    row = batch.rows[0]
    assert row["actual_expiry_date"] is None
    assert row["expiry_mapping_status"] == BLOCKED
    assert row["contract_rule_failure_reason"] == "actual_expiry_ambiguous"
    assert row["mte"] is None
    assert row["bsm_gate_status"] == BLOCKED


def test_lot_rule_is_selected_by_contract_expiry_not_observation_date() -> None:
    old_rule = _rule(
        rule_id="old-lot",
        contract_expiry_from="2024-01-01",
        contract_expiry_to="2026-07-20",
        contract_lot_size=50,
        market_lot=50,
    )
    batch = enrich_options_pre_bsm(
        [_option()],
        [_market("2026-07-14T09:16:00+05:30", 25010.0)],
        [_market("2026-07-14T09:16:00+05:30", 13.4)],
        [old_rule, _rule()],
        [_expiry()],
        acquisition_terminally_accounted=True,
    )

    assert batch.rows[0]["contract_rule_id"] == "nifty-lot-v3"
    assert batch.rows[0]["contract_lot_size"] == 65.0


def test_zero_or_post_expiry_time_is_null_and_blocked() -> None:
    expired = _expiry(
        actual_expiry_date="2026-07-14",
        actual_expiry_timestamp_ist="2026-07-14T09:16:00+05:30",
    )
    rule = _rule(actual_expiry_date="2026-07-14")
    batch = enrich_options_pre_bsm(
        [_option()],
        [_market("2026-07-14T09:16:00+05:30", 25010.0)],
        [_market("2026-07-14T09:16:00+05:30", 13.4)],
        [rule],
        [expired],
        acquisition_terminally_accounted=True,
    )

    row = batch.rows[0]
    assert row["mte"] is None
    assert row["dte"] is None
    assert row["t_years_act365"] is None
    assert row["time_to_expiry_failure_reason"] == "non_positive_mte"
    assert row["bsm_gate_status"] == BLOCKED


def test_out_of_session_options_are_retained_as_exceptions() -> None:
    batch = enrich_options_pre_bsm(
        [_option("2026-07-14T18:00:00+05:30", session_status="outside_regular_session")],
        [],
        [],
        [_rule()],
        [_expiry()],
        acquisition_terminally_accounted=True,
    )

    assert batch.rows == ()
    assert len(batch.exceptions) == 1
    assert batch.exceptions[0]["canonical_bsm_population"] is False
    assert batch.exceptions[0]["enrichment_exception"] == "outside_regular_session"


def test_dimension_loader_accepts_json_file_and_rows_mapping() -> None:
    with TemporaryDirectory() as temp:
        path = Path(temp) / "dimension.json"
        path.write_text(json.dumps({"rows": [_expiry()]}), encoding="utf-8")
        rows = load_dimension_rows(path)

    assert rows[0]["actual_expiry_date"] == "2026-07-21"
    assert load_dimension_rows({"only": _rule()})[0]["rule_id"] == "nifty-lot-v3"


def test_dimension_loader_expands_established_expiry_tuple_keys() -> None:
    evidence = _expiry()
    evidence.pop("trade_date")
    evidence.pop("expiry_type")
    evidence.pop("expiry_code")
    loaded = load_dimension_rows({("2026-07-14", "WEEK", 1): evidence})

    assert loaded[0]["trade_date"] == "2026-07-14"
    assert loaded[0]["expiry_flag"] == "WEEK"
    assert loaded[0]["expiry_code"] == 1


def test_partition_writer_is_atomic_hashed_and_cardinality_audited() -> None:
    batch = _ready_batch()
    with TemporaryDirectory() as temp:
        written = write_enriched_partitions(batch, temp)
        manifest = json.loads(Path(written.manifest_path).read_text(encoding="utf-8"))
        table = pq.read_table(written.parquet_paths[0])

        assert written.bsm_gate_status == READY
        assert sha256_file(written.manifest_path) == written.manifest_sha256
        assert Path(written.manifest_hash_path).read_text(encoding="utf-8").startswith(written.manifest_sha256)
        assert table.num_rows == 1
        assert str(table.schema.field("actual_expiry_timestamp_ist").type) == "timestamp[us, tz=Asia/Kolkata]"
        assert str(table.schema.field("actual_expiry_date").type) == "date32[day]"
        assert manifest["bsm_executed"] is False
        assert manifest["bsm_gate"]["status"] == READY
        assert manifest["partitions"][0]["sha256"] == sha256_file(written.parquet_paths[0])
        assert manifest["partitions"][0]["row_count"] == 1
        assert manifest["partitions"][0]["unique_primary_key_count"] == 1
        assert manifest["coverage"]["required_field_coverage"]["mte"]["null"] == 0
        assert not list(Path(temp).rglob("*.partial"))


def test_partition_writer_blocks_duplicate_enriched_primary_keys() -> None:
    spot = [_market("2026-07-14T09:16:00+05:30", 25010.0)]
    vix = [_market("2026-07-14T09:16:00+05:30", 13.4)]
    batch = enrich_options_pre_bsm(
        [_option(), _option()],
        spot,
        vix,
        [_rule()],
        [_expiry()],
        acquisition_terminally_accounted=True,
    )
    with TemporaryDirectory() as temp:
        written = write_enriched_partitions(batch, temp)
        manifest = json.loads(Path(written.manifest_path).read_text(encoding="utf-8"))

    assert written.bsm_gate_status == BLOCKED
    assert "duplicate_enriched_primary_keys" in manifest["bsm_gate"]["reasons"]
    assert manifest["partitions"][0]["row_count"] == 2
    assert manifest["partitions"][0]["unique_primary_key_count"] == 1


def test_batch_gate_requires_terminal_acquisition_accounting() -> None:
    batch = enrich_options_pre_bsm(
        [_option()],
        [_market("2026-07-14T09:16:00+05:30", 25010.0)],
        [_market("2026-07-14T09:16:00+05:30", 13.4)],
        [_rule()],
        [_expiry()],
    )

    assert batch.rows[0]["bsm_gate_status"] == READY
    assert batch.bsm_gate_status == BLOCKED
    assert batch.bsm_gate_reasons == ("options_acquisition_not_terminally_accounted",)
    assert batch.version == ENRICHMENT_VERSION
