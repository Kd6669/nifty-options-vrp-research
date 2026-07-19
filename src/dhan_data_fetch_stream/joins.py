"""Point-in-time-safe Dhan option-to-spot joins.

Candidates must belong to the same explicit session (or same IST calendar date
when no session field is present).  A spot observation is never selected from
the future: exact timestamps win, followed by the latest observation no more
than 60 seconds backward.
"""

from __future__ import annotations

from bisect import bisect_right
from datetime import datetime
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")

JOIN_EXACT = "exact_timestamp"
JOIN_BACKWARD = "backward_within_tolerance"
JOIN_NO_SPOT = "no_spot_for_session"
JOIN_OUTSIDE_TOLERANCE = "backward_outside_tolerance"
JOIN_FUTURE_FORBIDDEN = "future_spot_forbidden"
JOIN_INVALID = "invalid_timestamp"
JOIN_DUPLICATE_SPOT_KEY = "duplicate_spot_key"


def join_options_to_india_vix(
    option_rows: Iterable[Mapping[str, Any]],
    vix_rows: Iterable[Mapping[str, Any]],
    *,
    tolerance_seconds: float = 60.0,
) -> list[dict[str, Any]]:
    """Join INDIA VIX without conflating it with the NIFTY BSM spot input."""
    staged: list[dict[str, Any]] = []
    for source in option_rows:
        row = dict(source)
        for name in ("joined_spot", "joined_spot_ts", "spot_join_lag_seconds", "spot_join_status"):
            if name in row:
                row[f"_nifty_{name}"] = row.pop(name)
        staged.append(row)
    joined = join_options_to_spot(
        staged,
        vix_rows,
        option_timestamp_field="timestamp_ist",
        spot_timestamp_field="timestamp_ist",
        spot_value_field="close",
        session_field="trade_date",
        tolerance_seconds=tolerance_seconds,
    )
    output: list[dict[str, Any]] = []
    for row in joined:
        converted = dict(row)
        converted["india_vix"] = converted.pop("joined_spot")
        converted["india_vix_timestamp_ist"] = converted.pop("joined_spot_ts")
        converted["india_vix_join_lag_seconds"] = converted.pop("spot_join_lag_seconds")
        converted["india_vix_join_status"] = converted.pop("spot_join_status")
        for name in ("joined_spot", "joined_spot_ts", "spot_join_lag_seconds", "spot_join_status"):
            saved = f"_nifty_{name}"
            if saved in converted:
                converted[name] = converted.pop(saved)
        output.append(converted)
    return output


def join_options_to_spot(
    option_rows: Iterable[Mapping[str, Any]],
    spot_rows: Iterable[Mapping[str, Any]],
    *,
    option_timestamp_field: str = "timestamp",
    spot_timestamp_field: str = "timestamp",
    spot_value_field: str = "spot",
    session_field: str = "session",
    tolerance_seconds: float = 60.0,
) -> list[dict[str, Any]]:
    if tolerance_seconds < 0.0 or tolerance_seconds > 60.0:
        raise ValueError("tolerance_seconds must be between 0 and 60")
    by_session: dict[str, list[tuple[datetime, Any]]] = {}
    duplicate_keys: set[tuple[str, datetime]] = set()
    seen_keys: set[tuple[str, datetime]] = set()
    for row in spot_rows:
        try:
            ts = _as_aware_datetime(row[spot_timestamp_field])
        except (KeyError, TypeError, ValueError):
            continue
        session = _session_key(row, ts, session_field)
        key = (session, ts)
        if key in seen_keys:
            duplicate_keys.add(key)
        seen_keys.add(key)
        by_session.setdefault(session, []).append((ts, row.get(spot_value_field)))
    for values in by_session.values():
        values.sort(key=lambda pair: pair[0])

    output: list[dict[str, Any]] = []
    for row in option_rows:
        result = dict(row)
        result.update(
            {
                "joined_spot": None,
                "joined_spot_ts": None,
                "spot_join_lag_seconds": None,
                "spot_join_status": JOIN_INVALID,
            }
        )
        try:
            option_ts = _as_aware_datetime(row[option_timestamp_field])
        except (KeyError, TypeError, ValueError):
            output.append(result)
            continue
        session = _session_key(row, option_ts, session_field)
        candidates = by_session.get(session, [])
        if not candidates:
            result["spot_join_status"] = JOIN_NO_SPOT
            output.append(result)
            continue
        timestamps = [pair[0] for pair in candidates]
        position = bisect_right(timestamps, option_ts)
        if position == 0:
            result["spot_join_status"] = JOIN_FUTURE_FORBIDDEN
            output.append(result)
            continue
        spot_ts, spot_value = candidates[position - 1]
        lag = (option_ts - spot_ts).total_seconds()
        if (session, spot_ts) in duplicate_keys:
            result["spot_join_status"] = JOIN_DUPLICATE_SPOT_KEY
            result["spot_join_lag_seconds"] = lag
            output.append(result)
            continue
        if lag > tolerance_seconds:
            result["spot_join_status"] = JOIN_OUTSIDE_TOLERANCE
            result["spot_join_lag_seconds"] = lag
            output.append(result)
            continue
        result["joined_spot"] = spot_value
        result["joined_spot_ts"] = spot_ts.isoformat()
        result["spot_join_lag_seconds"] = lag
        result["spot_join_status"] = JOIN_EXACT if lag == 0.0 else JOIN_BACKWARD
        output.append(result)
    return output


def _session_key(row: Mapping[str, Any], timestamp: datetime, session_field: str) -> str:
    trade_date = timestamp.astimezone(IST).date().isoformat()
    explicit = row.get(session_field)
    if explicit not in (None, ""):
        return f"{trade_date}|{explicit}"
    return trade_date


def _as_aware_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise TypeError("timestamp must be datetime or ISO-8601 string")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed
