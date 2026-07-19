"""Versioned bronze, silver, and Dhan-side gold-preparation schemas."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FieldDefinition:
    name: str
    logical_type: str
    nullable: bool = True


@dataclass(frozen=True)
class SchemaDefinition:
    name: str
    version: str
    primary_key: tuple[str, ...]
    fields: tuple[FieldDefinition, ...]

    @property
    def field_names(self) -> tuple[str, ...]:
        return tuple(field.name for field in self.fields)


BRONZE_SCHEMA = SchemaDefinition(
    name="dhan_option_bronze",
    version="1.0.0",
    primary_key=("request_id",),
    fields=(
        FieldDefinition("schema_version", "string", False),
        FieldDefinition("request_id", "string", False),
        FieldDefinition("provider", "string", False),
        FieldDefinition("dataset", "string", False),
        FieldDefinition("endpoint", "string", False),
        FieldDefinition("request_payload_json", "string", False),
        FieldDefinition("payload_json", "string", False),
        FieldDefinition("source_sha256", "string", False),
        FieldDefinition("ingested_at", "timestamp_utc_us", False),
    ),
)

SILVER_SCHEMA = SchemaDefinition(
    name="dhan_option_silver",
    version="1.0.0",
    primary_key=(
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
    fields=(
        FieldDefinition("schema_version", "string", False),
        FieldDefinition("provider", "string", False),
        FieldDefinition("request_id", "string", False),
        FieldDefinition("timestamp_ist", "timestamp_ist_us", False),
        FieldDefinition("trade_date", "date32", False),
        FieldDefinition("session_status", "string", False),
        FieldDefinition("underlying", "string", False),
        FieldDefinition("expiry_date", "date32"),
        FieldDefinition("expiry_flag", "string", False),
        FieldDefinition("expiry_code", "int32", False),
        FieldDefinition("moneyness_label", "string", False),
        FieldDefinition("strike", "decimal128_18_4", False),
        FieldDefinition("option_type", "string", False),
        FieldDefinition("open", "float64"),
        FieldDefinition("high", "float64"),
        FieldDefinition("low", "float64"),
        FieldDefinition("close", "float64", False),
        FieldDefinition("provider_iv_raw", "float64"),
        FieldDefinition("provider_iv_unit", "string", False),
        FieldDefinition("volume", "int64"),
        FieldDefinition("open_interest", "int64"),
        FieldDefinition("provider_spot", "float64"),
        FieldDefinition("expiry_resolution_status", "string", False),
    ),
)

SPOT_SCHEMA = SchemaDefinition(
    name="dhan_spot_silver",
    version="1.0.0",
    primary_key=("timestamp_ist", "trade_date", "underlying"),
    fields=(
        FieldDefinition("schema_version", "string", False),
        FieldDefinition("provider", "string", False),
        FieldDefinition("request_id", "string", False),
        FieldDefinition("security_id", "string", False),
        FieldDefinition("timestamp_ist", "timestamp_ist_us", False),
        FieldDefinition("trade_date", "date32", False),
        FieldDefinition("session_status", "string", False),
        FieldDefinition("underlying", "string", False),
        FieldDefinition("open", "float64"),
        FieldDefinition("high", "float64"),
        FieldDefinition("low", "float64"),
        FieldDefinition("close", "float64", False),
        FieldDefinition("volume", "int64"),
        FieldDefinition("open_interest", "int64"),
    ),
)

VIX_SCHEMA = SchemaDefinition(
    name="dhan_india_vix_silver",
    version="1.0.0",
    primary_key=("timestamp_ist", "trade_date", "underlying"),
    fields=SPOT_SCHEMA.fields,
)

FUTURES_SCHEMA = SchemaDefinition(
    name="dhan_futures_silver",
    version="1.0.0",
    primary_key=("timestamp_ist", "trade_date", "underlying", "security_id"),
    fields=(
        FieldDefinition("schema_version", "string", False),
        FieldDefinition("provider", "string", False),
        FieldDefinition("request_id", "string", False),
        FieldDefinition("security_id", "string", False),
        FieldDefinition("timestamp_ist", "timestamp_ist_us", False),
        FieldDefinition("trade_date", "date32", False),
        FieldDefinition("session_status", "string", False),
        FieldDefinition("underlying", "string", False),
        FieldDefinition("futures_expiry_text", "string"),
        FieldDefinition("series_label", "string"),
        FieldDefinition("open", "float64"),
        FieldDefinition("high", "float64"),
        FieldDefinition("low", "float64"),
        FieldDefinition("close", "float64", False),
        FieldDefinition("volume", "int64"),
        FieldDefinition("open_interest", "int64"),
    ),
)

GOLD_PREP_SCHEMA = SchemaDefinition(
    name="dhan_option_gold_prep",
    version="1.0.0",
    primary_key=SILVER_SCHEMA.primary_key,
    fields=SILVER_SCHEMA.fields
    + (
        FieldDefinition("spot", "float64"),
        FieldDefinition("spot_timestamp_ist", "timestamp_ist_us"),
        FieldDefinition("spot_join_status", "string", False),
        FieldDefinition("spot_join_lag_seconds", "float64"),
        FieldDefinition("bsm_status", "string", False),
        FieldDefinition("bsm_failure_reason", "string"),
        FieldDefinition("bsm_rate_cc", "float64", False),
        FieldDefinition("bsm_dividend_yield", "float64", False),
        FieldDefinition("bsm_time_basis", "string", False),
        FieldDefinition("bsm_iv_close", "float64"),
        FieldDefinition("bsm_iv_unit", "string", False),
        FieldDefinition("bsm_price_reconstructed", "float64"),
        FieldDefinition("bsm_price_residual_signed", "float64"),
        FieldDefinition("bsm_price_residual_abs", "float64"),
        FieldDefinition("bsm_delta", "float64"),
        FieldDefinition("bsm_gamma", "float64"),
        FieldDefinition("bsm_theta_per_year", "float64"),
        FieldDefinition("bsm_theta_per_day_365", "float64"),
        FieldDefinition("bsm_vega_per_1", "float64"),
        FieldDefinition("bsm_vega_per_100", "float64"),
        FieldDefinition("bsm_rho_per_1", "float64"),
        FieldDefinition("bsm_rho_per_100", "float64"),
        FieldDefinition("bsm_expiry_ts", "timestamp_ist_us", False),
        FieldDefinition("bsm_model_version", "string", False),
        FieldDefinition("bsm_near_expiry", "bool", False),
        FieldDefinition("bsm_numerical_epsilon_applied", "bool", False),
        FieldDefinition("span_enrichment_status", "string", False),
        FieldDefinition("span_manifest_sha256", "string"),
        FieldDefinition("span_slot_code", "string"),
        FieldDefinition("span_effective_at", "timestamp_utc_us"),
        FieldDefinition("india_vix", "float64"),
        FieldDefinition("india_vix_timestamp_ist", "timestamp_ist_us"),
        FieldDefinition("india_vix_join_status", "string", False),
        FieldDefinition("india_vix_join_lag_seconds", "float64"),
    ),
)

SCHEMAS = {
    BRONZE_SCHEMA.name: BRONZE_SCHEMA,
    SILVER_SCHEMA.name: SILVER_SCHEMA,
    SPOT_SCHEMA.name: SPOT_SCHEMA,
    VIX_SCHEMA.name: VIX_SCHEMA,
    FUTURES_SCHEMA.name: FUTURES_SCHEMA,
    GOLD_PREP_SCHEMA.name: GOLD_PREP_SCHEMA,
}


def get_schema(name: str) -> SchemaDefinition:
    try:
        return SCHEMAS[name]
    except KeyError as exc:
        raise ValueError(f"unknown schema: {name}") from exc


def arrow_schema(name: str) -> Any:
    """Return a pyarrow schema when pyarrow is installed.

    Import remains lazy so schema metadata can still be inspected in lightweight
    environments without pyarrow.
    """
    try:
        import pyarrow as pa  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - depends on operator environment.
        raise RuntimeError("pyarrow is required to materialize Arrow schemas") from exc
    definition = get_schema(name)
    arrow_fields = [
        pa.field(field.name, _arrow_type(pa, field.logical_type), nullable=field.nullable)
        for field in definition.fields
    ]
    return pa.schema(
        arrow_fields,
        metadata={
            b"schema_name": definition.name.encode("utf-8"),
            b"schema_version": definition.version.encode("utf-8"),
            b"primary_key": ",".join(definition.primary_key).encode("utf-8"),
        },
    )


def _arrow_type(pa: Any, logical_type: str) -> Any:
    mapping = {
        "string": pa.string(),
        "bool": pa.bool_(),
        "float64": pa.float64(),
        "int64": pa.int64(),
        "int32": pa.int32(),
        "date32": pa.date32(),
        "decimal128_18_4": pa.decimal128(18, 4),
        "timestamp_utc_us": pa.timestamp("us", tz="UTC"),
        "timestamp_ist_us": pa.timestamp("us", tz="Asia/Kolkata"),
    }
    return mapping[logical_type]
