"""Partition-incremental Dhan preparation without premature SPAN/gold claims."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable, Mapping

from .bsm import analyze_option, bsm_output_record
from .joins import JOIN_EXACT, JOIN_BACKWARD, join_options_to_india_vix, join_options_to_spot


@dataclass(frozen=True)
class ExpiryEvidence:
    expiry_ts: datetime
    verified: bool
    source_sha256: str


@dataclass(frozen=True)
class PreparedBatch:
    rows: tuple[dict[str, Any], ...]
    exceptions: tuple[dict[str, Any], ...]
    readiness: str = "SPAN_PENDING"


def prepare_dhan_partition(
    option_rows: Iterable[Mapping[str, Any]],
    spot_rows: Iterable[Mapping[str, Any]],
    india_vix_rows: Iterable[Mapping[str, Any]],
    *,
    expiry_evidence: Mapping[tuple[str, str, int], ExpiryEvidence] | None = None,
) -> PreparedBatch:
    """Prepare one independent partition with strict point-in-time inputs.

    Evidence keys are ``(trade_date, expiry_flag, expiry_code)``. Dhan rolling
    responses omit actual expiry, so absent/unverified evidence is a blocking
    status, never a guessed date. Outside-session rows are quarantined.
    """
    eligible: list[Mapping[str, Any]] = []
    exceptions: list[dict[str, Any]] = []
    for source_row in option_rows:
        row = dict(source_row)
        if row.get("session_status") != "regular_session":
            exceptions.append({**row, "preparation_exception": "outside_regular_session"})
        else:
            eligible.append(row)

    regular_spot = [row for row in spot_rows if row.get("session_status") == "regular_session"]
    regular_vix = [row for row in india_vix_rows if row.get("session_status") == "regular_session"]
    joined_spot = join_options_to_spot(
        eligible,
        regular_spot,
        option_timestamp_field="timestamp_ist",
        spot_timestamp_field="timestamp_ist",
        spot_value_field="close",
        session_field="trade_date",
        tolerance_seconds=60.0,
    )
    joined_all = join_options_to_india_vix(joined_spot, regular_vix)
    evidence_map = expiry_evidence or {}
    prepared: list[dict[str, Any]] = []
    for joined in joined_all:
        row = dict(joined)
        row["spot"] = row.pop("joined_spot")
        row["spot_timestamp_ist"] = row.pop("joined_spot_ts")
        row["span_enrichment_status"] = "SPAN_PENDING"
        row["span_manifest_sha256"] = None
        trade_date = _date_text(row.get("trade_date"))
        evidence = evidence_map.get(
            (trade_date, str(row.get("expiry_flag", "")), int(row.get("expiry_code", -1)))
        )
        join_ok = row.get("spot_join_status") in {JOIN_EXACT, JOIN_BACKWARD}
        if not join_ok or row.get("spot") is None:
            row.update(_blocked_bsm("spot_join_unavailable"))
        elif evidence is None or evidence.verified is not True:
            row.update(_blocked_bsm("actual_expiry_unverified"))
        else:
            analysis = analyze_option(
                spot=float(row["spot"]),
                strike=float(row["strike"]),
                observed_price=float(row["close"]),
                option_type=str(row["option_type"]),
                valuation_ts=_aware_datetime(row["timestamp_ist"]),
                expiry_ts=evidence.expiry_ts,
                expiry_verified=evidence.verified,
                provider_fields={
                    key: value
                    for key, value in row.items()
                    if key.startswith("provider_")
                },
            )
            row.update(bsm_output_record(analysis))
            row["expiry_evidence_sha256"] = evidence.source_sha256
        prepared.append(row)
    return PreparedBatch(tuple(prepared), tuple(exceptions))


def _blocked_bsm(reason: str) -> dict[str, Any]:
    return {
        "bsm_status": "blocked",
        "bsm_failure_reason": reason,
        "bsm_iv_close": None,
        "bsm_price_reconstructed": None,
        "bsm_delta": None,
        "bsm_gamma": None,
        "bsm_theta_per_year": None,
        "bsm_vega_per_1": None,
        "bsm_rho_per_1": None,
        "expiry_evidence_sha256": None,
    }


def _date_text(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _aware_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("preparation timestamps must be timezone-aware")
    return result
