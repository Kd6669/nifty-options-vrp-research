from __future__ import annotations

import unittest

from dhan_data_fetch_stream.schemas import (
    BRONZE_SCHEMA,
    FUTURES_SCHEMA,
    GOLD_PREP_SCHEMA,
    SILVER_SCHEMA,
    SPOT_SCHEMA,
    VIX_SCHEMA,
    arrow_schema,
    get_schema,
)


class SchemaTests(unittest.TestCase):
    def test_schemas_are_versioned_and_have_stable_keys(self) -> None:
        for schema in (
            BRONZE_SCHEMA,
            SILVER_SCHEMA,
            SPOT_SCHEMA,
            VIX_SCHEMA,
            FUTURES_SCHEMA,
            GOLD_PREP_SCHEMA,
        ):
            self.assertEqual(schema.version, "1.0.0")
            self.assertTrue(schema.primary_key)
            self.assertEqual(len(schema.field_names), len(set(schema.field_names)))

    def test_silver_preserves_provider_values_separately(self) -> None:
        self.assertIn("provider_iv_raw", SILVER_SCHEMA.field_names)
        self.assertIn("provider_iv_unit", SILVER_SCHEMA.field_names)
        self.assertNotIn("implied_volatility", SILVER_SCHEMA.field_names)

    def test_gold_prep_records_units_failures_and_pending_span_contract(self) -> None:
        required = {
            "bsm_failure_reason",
            "bsm_iv_close",
            "bsm_price_reconstructed",
            "bsm_price_residual_signed",
            "bsm_price_residual_abs",
            "bsm_theta_per_year",
            "bsm_theta_per_day_365",
            "bsm_vega_per_1",
            "bsm_vega_per_100",
            "bsm_rho_per_1",
            "bsm_rho_per_100",
            "bsm_model_version",
            "bsm_near_expiry",
            "bsm_numerical_epsilon_applied",
            "spot_join_status",
            "span_enrichment_status",
            "span_manifest_sha256",
            "span_effective_at",
            "india_vix_join_status",
            "india_vix_join_lag_seconds",
        }
        self.assertTrue(required.issubset(GOLD_PREP_SCHEMA.field_names))

    def test_option_schema_has_canonical_rolling_contract_keys_and_decimal_strike(self) -> None:
        self.assertEqual(
            SILVER_SCHEMA.primary_key,
            (
                "timestamp_ist",
                "trade_date",
                "underlying",
                "expiry_date",
                "expiry_flag",
                "expiry_code",
                "moneyness_label",
                "strike",
                "option_type",
            ),
        )
        required = {
            "trade_date",
            "expiry_date",
            "expiry_flag",
            "expiry_code",
            "moneyness_label",
        }
        self.assertTrue(required.issubset(SILVER_SCHEMA.field_names))
        schema = arrow_schema(SILVER_SCHEMA.name)
        self.assertEqual(str(schema.field("strike").type), "decimal128(18, 4)")

    def test_spot_and_futures_have_separate_versioned_schemas(self) -> None:
        self.assertEqual(SPOT_SCHEMA.primary_key, ("timestamp_ist", "trade_date", "underlying"))
        self.assertEqual(VIX_SCHEMA.primary_key, SPOT_SCHEMA.primary_key)
        self.assertEqual(VIX_SCHEMA.name, "dhan_india_vix_silver")
        self.assertEqual(
            FUTURES_SCHEMA.primary_key,
            ("timestamp_ist", "trade_date", "underlying", "security_id"),
        )
        self.assertIn("open_interest", FUTURES_SCHEMA.field_names)
        self.assertNotIn("strike", FUTURES_SCHEMA.field_names)

    def test_arrow_schema_has_version_metadata_and_timezones(self) -> None:
        schema = arrow_schema(GOLD_PREP_SCHEMA.name)

        self.assertEqual(schema.metadata[b"schema_version"], b"1.0.0")
        self.assertEqual(str(schema.field("timestamp_ist").type), "timestamp[us, tz=Asia/Kolkata]")
        self.assertEqual(str(schema.field("bsm_expiry_ts").type), "timestamp[us, tz=Asia/Kolkata]")

    def test_unknown_schema_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown schema"):
            get_schema("missing")


if __name__ == "__main__":
    unittest.main()
