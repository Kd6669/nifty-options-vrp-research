"""Unconditional moneyness x horizon coverage across every date and start minute.

The denominator is fixed before looking at quote availability:

* every weekly-option trade date in the gold dataset;
* every standard-session minute from 09:15 IST for which t+h <= 15:29;
* every configured holding horizon;
* every rolling entry offset from ATM-10 through ATM+10.

Entry labels select contracts only at entry. Later quotes are matched by exact
trade date, expiry, strike, and option type.
"""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from typing import Any

import duckdb


HORIZONS = (15, 30, 60, 90, 120, 180, 240, 300)


def _records(connection: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    frame = connection.execute(sql).fetchdf()
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def _leg_bit(entry_offset: int, option_type: str) -> int:
    side = 0 if option_type == "CALL" else 1
    return 1 << ((entry_offset + 10) * 2 + side)


def _structure_rows() -> list[tuple[str, int, int, int]]:
    rows: list[tuple[str, int, int, int]] = []
    for wing in range(1, 11):
        required = (
            _leg_bit(0, "CALL")
            | _leg_bit(0, "PUT")
            | _leg_bit(wing, "CALL")
            | _leg_bit(-wing, "PUT")
        )
        rows.append(("iron_fly", 0, wing, required))
    for short in (1, 2, 3):
        for wing in range(short + 1, 11):
            required = (
                _leg_bit(short, "CALL")
                | _leg_bit(-short, "PUT")
                | _leg_bit(wing, "CALL")
                | _leg_bit(-wing, "PUT")
            )
            rows.append(("iron_condor", short, wing, required))
    return rows


def audit(
    gold_root: Path,
    output_dir: Path,
    *,
    session_mode: str,
    entry_offset_source: str,
) -> dict[str, Any]:
    parquet_glob = str(gold_root / "**" / "*.parquet").replace("\\", "/")
    output_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="nifty_unconditional_duckdb_") as temp_dir:
        connection = duckdb.connect(str(Path(temp_dir) / "audit.duckdb"))
        connection.execute("PRAGMA threads=8")
        connection.execute("PRAGMA memory_limit='12GB'")
        connection.execute(f"PRAGMA temp_directory='{temp_dir.replace(chr(92), '/')}'")
        source = f"read_parquet('{parquet_glob}', hive_partitioning=true)"
        session_filter = (
            "session_status = 'regular_session'"
            if session_mode == "standard"
            else "TRUE"
        )
        entry_offset_expression = (
            "provider_moneyness_offset"
            if entry_offset_source == "provider"
            else "try_cast(computed_moneyness_offset AS INTEGER)"
        )
        entry_quality_expression = (
            """
            quality_gate_status = 'pass'
            AND bsm_gate_status = 'READY'
            AND strike_ladder_valid
            AND provider_moneyness_matches_computed
            """
            if entry_offset_source == "provider"
            else """
            close IS NOT NULL
            AND close >= 0
            AND strike_ladder_valid
            AND NOT quality_severe_anomaly
            AND NOT proven_severe_payload_corruption
            AND computed_moneyness_offset = entry_offset
            """
        )

        connection.execute(
            f"""
            CREATE TABLE source_dates AS
            SELECT
              trade_date,
              mode(actual_expiry_date) FILTER (
                WHERE expiry_flag = 'WEEK' AND actual_expiry_date IS NOT NULL
              ) AS actual_expiry_date,
              dayname(trade_date) AS weekday,
              min(timestamp_ist) FILTER (
                WHERE expiry_flag = 'WEEK' AND {session_filter}
              ) AS observed_session_start,
              max(timestamp_ist) FILTER (
                WHERE expiry_flag = 'WEEK' AND {session_filter}
              ) AS observed_session_end
            FROM {source}
            WHERE expiry_flag = 'WEEK'
            GROUP BY 1
            """
        )
        connection.execute(
            f"""
            CREATE TABLE duplicate_identities AS
            SELECT
              trade_date,
              timestamp_ist,
              actual_expiry_date,
              strike,
              option_type,
              count(*) AS identity_rows
            FROM {source}
            WHERE
              expiry_flag = 'WEEK'
              AND {session_filter}
            GROUP BY 1, 2, 3, 4, 5
            HAVING count(*) > 1
            """
        )
        connection.execute(
            f"""
            CREATE TABLE surface_raw AS
            SELECT
              source.trade_date,
              source.timestamp_ist,
              source.actual_expiry_date,
              cast(source.strike AS DOUBLE) AS strike,
              source.option_type,
              {entry_offset_expression} AS entry_offset,
              source.close,
              source.independent_nifty_spot AS spot,
              source.recomputed_atm_strike AS atm_strike,
              source.quality_gate_status,
              source.bsm_gate_status,
              source.strike_ladder_valid,
              source.provider_moneyness_matches_computed,
              source.computed_moneyness_offset,
              source.quality_severe_anomaly,
              source.proven_severe_payload_corruption,
              duplicate.identity_rows IS NOT NULL AS duplicate_identity
            FROM {source} AS source
            LEFT JOIN duplicate_identities AS duplicate
              ON source.trade_date = duplicate.trade_date
             AND source.timestamp_ist = duplicate.timestamp_ist
             AND source.actual_expiry_date = duplicate.actual_expiry_date
             AND source.strike = duplicate.strike
             AND source.option_type = duplicate.option_type
            WHERE
              source.expiry_flag = 'WEEK'
              AND {session_filter.replace('session_status', 'source.session_status')}
              AND source.actual_expiry_date IS NOT NULL
              AND {entry_offset_expression.replace('provider_moneyness_offset', 'source.provider_moneyness_offset').replace('computed_moneyness_offset', 'source.computed_moneyness_offset')} BETWEEN -10 AND 10
            """
        )
        connection.execute(
            """
            CREATE TABLE quote_surface AS
            SELECT
              trade_date,
              timestamp_ist,
              actual_expiry_date,
              strike,
              option_type,
              any_value(close) AS close
            FROM surface_raw
            WHERE NOT duplicate_identity
            GROUP BY 1, 2, 3, 4, 5
            """
        )
        connection.execute(
            f"""
            CREATE TABLE entry_surface AS
            WITH ordered AS (
              SELECT
                *,
                CASE
                  WHEN lag(timestamp_ist) OVER exact_contract
                    = timestamp_ist - INTERVAL 1 MINUTE
                  THEN 0
                  ELSE 1
                END AS run_start
              FROM surface_raw
              WHERE NOT duplicate_identity
              WINDOW exact_contract AS (
                PARTITION BY
                  trade_date,
                  actual_expiry_date,
                  strike,
                  option_type
                ORDER BY timestamp_ist
              )
            ),
            runs AS (
              SELECT
                *,
                sum(run_start) OVER exact_contract AS run_id
              FROM ordered
              WINDOW exact_contract AS (
                PARTITION BY
                  trade_date,
                  actual_expiry_date,
                  strike,
                  option_type
                ORDER BY timestamp_ist
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
              )
            )
            SELECT
              *,
              max(timestamp_ist) OVER (
                PARTITION BY
                  trade_date,
                  actual_expiry_date,
                  strike,
                  option_type,
                  run_id
              ) AS continuous_run_end,
              (
                {entry_quality_expression}
              ) AS entry_quality_eligible,
              cast(
                1 AS UBIGINT
              ) << (
                (entry_offset + 10) * 2
                + CASE WHEN option_type = 'CALL' THEN 0 ELSE 1 END
              ) AS leg_bit
            FROM runs
            """
        )
        connection.execute(
            """
            CREATE TABLE spot_minutes AS
            SELECT
              trade_date,
              timestamp_ist,
              median(spot) AS spot,
              median(atm_strike) AS atm_strike
            FROM surface_raw
            WHERE spot IS NOT NULL
            GROUP BY 1, 2
            """
        )
        connection.execute(
            """
            CREATE TABLE structures(
              family VARCHAR,
              short_offset INTEGER,
              wing_offset INTEGER,
              required_mask UBIGINT
            )
            """
        )
        connection.executemany(
            "INSERT INTO structures VALUES (?, ?, ?, ?)",
            _structure_rows(),
        )
        connection.execute(
            """
            CREATE TABLE legs(
              entry_offset INTEGER,
              option_type VARCHAR,
              leg_bit UBIGINT
            )
            """
        )
        connection.executemany(
            "INSERT INTO legs VALUES (?, ?, ?)",
            [
                (offset, side, _leg_bit(offset, side))
                for offset in range(-10, 11)
                for side in ("CALL", "PUT")
            ],
        )

        result: dict[str, Any] = {
            "contract": {
                "dates": "every weekly-option trade_date in the gold dataset",
                "session_mode": session_mode,
                "entry_offset_source": entry_offset_source,
                "session_boundary": (
                    "09:15 through 15:29 IST"
                    if session_mode == "standard"
                    else "each date's observed first through last weekly-option minute"
                ),
                "horizons_minutes": list(HORIZONS),
                "entry_offsets": list(range(-10, 11)),
                "entry_denominator": (
                    "all theoretical date x 09:15-15:29 start-minute windows"
                    if session_mode == "standard"
                    else "all feasible start minutes inside each date's observed session envelope"
                ),
                "entry_missing_policy": "counted as unavailable; never dropped",
                "tracking_key": [
                    "trade_date",
                    "actual_expiry_date",
                    "strike",
                    "option_type",
                ],
                "exact_endpoint": "quote at exactly t+h",
                "strict_path": "continuous one-minute run from t through t+h",
                "stale_quote": "last exact-contract quote no later than t+h",
            },
            "source_dates": _records(
                connection,
                """
                SELECT
                  count(*) AS dataset_dates,
                  min(trade_date) AS first_date,
                  max(trade_date) AS last_date,
                  count(*) FILTER (
                    WHERE weekday IN ('Saturday', 'Sunday')
                  ) AS special_weekend_dates,
                  count(*) FILTER (
                    WHERE actual_expiry_date IS NULL
                  ) AS unresolved_daily_expiry_dates,
                  count(*) FILTER (
                    WHERE observed_session_start IS NULL
                       OR observed_session_end IS NULL
                  ) AS unresolved_session_envelope_dates
                FROM source_dates
                """,
            )[0],
            "duplicate_exact_identities": _records(
                connection,
                """
                SELECT
                  count(*) AS groups,
                  coalesce(sum(identity_rows - 1), 0) AS excess_rows,
                  count(DISTINCT trade_date) AS affected_dates
                FROM duplicate_identities
                """,
            )[0],
            "horizon_summaries": [],
            "leg_matrix": [],
            "structure_matrix": [],
            "structure_day_matrix": [],
            "key_structure_atm_shift": [],
        }

        start_time_frames = []
        for horizon in HORIZONS:
            connection.execute("DROP TABLE IF EXISTS grid")
            connection.execute("DROP TABLE IF EXISTS leg_status")
            connection.execute("DROP TABLE IF EXISTS masks")
            connection.execute("DROP TABLE IF EXISTS universe")
            connection.execute("DROP TABLE IF EXISTS structure_status")

            if session_mode == "standard":
                last_start_minutes = 929 - horizon
                grid_sql = f"""
                    SELECT
                      date.trade_date,
                      date.actual_expiry_date,
                      date.weekday,
                      timezone(
                        'Asia/Kolkata',
                        cast(date.trade_date AS TIMESTAMP)
                          + INTERVAL 555 MINUTE
                          + minute_index * INTERVAL 1 MINUTE
                      ) AS entry_timestamp,
                      strftime(
                        timezone(
                          'Asia/Kolkata',
                          cast(date.trade_date AS TIMESTAMP)
                            + INTERVAL 555 MINUTE
                            + minute_index * INTERVAL 1 MINUTE
                        ) AT TIME ZONE 'Asia/Kolkata',
                        '%H:%M'
                      ) AS entry_time
                    FROM source_dates AS date
                    CROSS JOIN generate_series(
                      0,
                      {last_start_minutes - 555},
                      1
                    ) AS series(minute_index)
                """
                entry_time_filter = f"""
                    extract(
                      hour FROM entry.timestamp_ist AT TIME ZONE 'Asia/Kolkata'
                    ) * 60
                    + extract(
                      minute FROM entry.timestamp_ist AT TIME ZONE 'Asia/Kolkata'
                    ) BETWEEN 555 AND {last_start_minutes}
                """
            else:
                grid_sql = f"""
                    SELECT
                      date.trade_date,
                      date.actual_expiry_date,
                      date.weekday,
                      series.entry_timestamp,
                      strftime(
                        series.entry_timestamp AT TIME ZONE 'Asia/Kolkata',
                        '%H:%M'
                      ) AS entry_time
                    FROM source_dates AS date
                    CROSS JOIN generate_series(
                      date.observed_session_start,
                      date.observed_session_end - INTERVAL {horizon} MINUTE,
                      INTERVAL 1 MINUTE
                    ) AS series(entry_timestamp)
                    WHERE
                      date.observed_session_start IS NOT NULL
                      AND date.observed_session_end
                        >= date.observed_session_start
                          + INTERVAL {horizon} MINUTE
                """
                entry_time_filter = f"""
                    entry.timestamp_ist
                      <= (
                        SELECT observed_session_end
                        FROM source_dates
                        WHERE trade_date = entry.trade_date
                      ) - INTERVAL {horizon} MINUTE
                """
            connection.execute(f"CREATE TABLE grid AS {grid_sql}")
            connection.execute(
                f"""
                CREATE TABLE leg_status AS
                SELECT
                  entry.trade_date,
                  entry.timestamp_ist AS entry_timestamp,
                  entry.actual_expiry_date,
                  entry.entry_offset,
                  entry.option_type,
                  entry.leg_bit,
                  entry.entry_quality_eligible,
                  entry.continuous_run_end
                    >= entry.timestamp_ist + INTERVAL {horizon} MINUTE
                    AS strict_path_complete,
                  quote.timestamp_ist AS matched_quote_timestamp,
                  quote.timestamp_ist
                    = entry.timestamp_ist + INTERVAL {horizon} MINUTE
                    AS exact_endpoint_available,
                  date_diff(
                    'minute',
                    quote.timestamp_ist,
                    entry.timestamp_ist + INTERVAL {horizon} MINUTE
                  ) AS staleness_minutes
                FROM entry_surface AS entry
                ASOF LEFT JOIN quote_surface AS quote
                  ON entry.trade_date = quote.trade_date
                 AND entry.actual_expiry_date = quote.actual_expiry_date
                 AND entry.strike = quote.strike
                 AND entry.option_type = quote.option_type
                 AND entry.timestamp_ist + INTERVAL {horizon} MINUTE
                   >= quote.timestamp_ist
                WHERE {entry_time_filter}
                """
            )
            connection.execute(
                """
                CREATE TABLE masks AS
                SELECT
                  trade_date,
                  entry_timestamp,
                  actual_expiry_date,
                  bit_or(leg_bit) AS present_mask,
                  bit_or(leg_bit) FILTER (
                    WHERE entry_quality_eligible
                  ) AS eligible_mask,
                  bit_or(leg_bit) FILTER (
                    WHERE
                      entry_quality_eligible
                      AND exact_endpoint_available
                  ) AS exact_mask,
                  bit_or(leg_bit) FILTER (
                    WHERE
                      entry_quality_eligible
                      AND strict_path_complete
                  ) AS path_mask,
                  bit_or(leg_bit) FILTER (
                    WHERE
                      entry_quality_eligible
                      AND staleness_minutes <= 1
                  ) AS stale_1m_mask,
                  bit_or(leg_bit) FILTER (
                    WHERE
                      entry_quality_eligible
                      AND staleness_minutes <= 2
                  ) AS stale_2m_mask,
                  bit_or(leg_bit) FILTER (
                    WHERE
                      entry_quality_eligible
                      AND staleness_minutes <= 5
                  ) AS stale_5m_mask,
                  bit_or(leg_bit) FILTER (
                    WHERE
                      entry_quality_eligible
                      AND staleness_minutes <= 10
                  ) AS stale_10m_mask
                FROM leg_status
                GROUP BY 1, 2, 3
                """
            )
            connection.execute(
                f"""
                CREATE TABLE universe AS
                SELECT
                  grid.*,
                  coalesce(mask.present_mask, 0::UBIGINT) AS present_mask,
                  coalesce(mask.eligible_mask, 0::UBIGINT) AS eligible_mask,
                  coalesce(mask.exact_mask, 0::UBIGINT) AS exact_mask,
                  coalesce(mask.path_mask, 0::UBIGINT) AS path_mask,
                  coalesce(mask.stale_1m_mask, 0::UBIGINT) AS stale_1m_mask,
                  coalesce(mask.stale_2m_mask, 0::UBIGINT) AS stale_2m_mask,
                  coalesce(mask.stale_5m_mask, 0::UBIGINT) AS stale_5m_mask,
                  coalesce(mask.stale_10m_mask, 0::UBIGINT) AS stale_10m_mask,
                  entry_spot.spot AS entry_spot,
                  entry_spot.atm_strike AS entry_atm_strike,
                  target_spot.spot AS target_spot,
                  target_spot.atm_strike AS target_atm_strike,
                  abs(target_spot.spot / entry_spot.spot - 1.0) * 10000.0
                    AS endpoint_abs_move_bps,
                  abs(
                    target_spot.atm_strike - entry_spot.atm_strike
                  ) / 50.0 AS endpoint_atm_shift_steps
                FROM grid
                LEFT JOIN masks AS mask
                  ON grid.trade_date = mask.trade_date
                 AND grid.entry_timestamp = mask.entry_timestamp
                 AND grid.actual_expiry_date = mask.actual_expiry_date
                LEFT JOIN spot_minutes AS entry_spot
                  ON grid.trade_date = entry_spot.trade_date
                 AND grid.entry_timestamp = entry_spot.timestamp_ist
                LEFT JOIN spot_minutes AS target_spot
                  ON grid.trade_date = target_spot.trade_date
                 AND target_spot.timestamp_ist
                   = grid.entry_timestamp + INTERVAL {horizon} MINUTE
                """
            )
            connection.execute(
                """
                CREATE TABLE structure_status AS
                SELECT
                  universe.*,
                  structure.family,
                  structure.short_offset,
                  structure.wing_offset,
                  structure.required_mask,
                  (
                    present_mask & required_mask
                  ) = required_mask AS entry_present,
                  (
                    eligible_mask & required_mask
                  ) = required_mask AS entry_eligible,
                  (
                    exact_mask & required_mask
                  ) = required_mask AS exact_complete,
                  (
                    path_mask & required_mask
                  ) = required_mask AS path_complete,
                  (
                    stale_1m_mask & required_mask
                  ) = required_mask AS stale_1m_complete,
                  (
                    stale_2m_mask & required_mask
                  ) = required_mask AS stale_2m_complete,
                  (
                    stale_5m_mask & required_mask
                  ) = required_mask AS stale_5m_complete,
                  (
                    stale_10m_mask & required_mask
                  ) = required_mask AS stale_10m_complete
                FROM universe
                CROSS JOIN structures AS structure
                """
            )

            theoretical_windows = connection.execute(
                "SELECT count(*) FROM universe"
            ).fetchone()[0]
            result["horizon_summaries"].append(
                {
                    "horizon_minutes": horizon,
                    "theoretical_windows": theoretical_windows,
                    "dataset_dates": connection.execute(
                        "SELECT count(DISTINCT trade_date) FROM universe"
                    ).fetchone()[0],
                    "start_minutes_per_date": connection.execute(
                        "SELECT count(DISTINCT entry_time) FROM universe"
                    ).fetchone()[0],
                }
            )

            leg_rows = _records(
                connection,
                f"""
                SELECT
                  {horizon} AS horizon_minutes,
                  leg.entry_offset,
                  leg.option_type,
                  count(*) AS theoretical_windows,
                  count(*) FILTER (
                    WHERE universe.present_mask & leg.leg_bit = leg.leg_bit
                  ) AS entry_present_windows,
                  count(*) FILTER (
                    WHERE universe.eligible_mask & leg.leg_bit = leg.leg_bit
                  ) AS entry_eligible_windows,
                  count(*) FILTER (
                    WHERE universe.exact_mask & leg.leg_bit = leg.leg_bit
                  ) AS exact_endpoint_windows,
                  count(*) FILTER (
                    WHERE universe.path_mask & leg.leg_bit = leg.leg_bit
                  ) AS strict_path_windows,
                  count(*) FILTER (
                    WHERE universe.stale_1m_mask & leg.leg_bit = leg.leg_bit
                  ) AS stale_1m_windows,
                  count(*) FILTER (
                    WHERE universe.stale_2m_mask & leg.leg_bit = leg.leg_bit
                  ) AS stale_2m_windows,
                  count(*) FILTER (
                    WHERE universe.stale_5m_mask & leg.leg_bit = leg.leg_bit
                  ) AS stale_5m_windows,
                  count(*) FILTER (
                    WHERE universe.stale_10m_mask & leg.leg_bit = leg.leg_bit
                  ) AS stale_10m_windows
                FROM universe
                CROSS JOIN legs AS leg
                GROUP BY 1, 2, 3
                ORDER BY 2, 3
                """,
            )
            result["leg_matrix"].extend(leg_rows)

            structure_rows = _records(
                connection,
                f"""
                SELECT
                  {horizon} AS horizon_minutes,
                  family,
                  short_offset,
                  wing_offset,
                  count(*) AS theoretical_windows,
                  count(*) FILTER (WHERE entry_present) AS entry_present_windows,
                  count(*) FILTER (WHERE entry_eligible) AS entry_eligible_windows,
                  count(*) FILTER (WHERE exact_complete) AS exact_endpoint_windows,
                  count(*) FILTER (WHERE path_complete) AS strict_path_windows,
                  count(*) FILTER (
                    WHERE stale_1m_complete
                  ) AS stale_1m_windows,
                  count(*) FILTER (
                    WHERE stale_2m_complete
                  ) AS stale_2m_windows,
                  count(*) FILTER (
                    WHERE stale_5m_complete
                  ) AS stale_5m_windows,
                  count(*) FILTER (
                    WHERE stale_10m_complete
                  ) AS stale_10m_windows
                FROM structure_status
                GROUP BY 1, 2, 3, 4
                ORDER BY 2, 3, 4
                """,
            )
            result["structure_matrix"].extend(structure_rows)

            day_rows = _records(
                connection,
                f"""
                WITH daily AS (
                  SELECT
                    trade_date,
                    family,
                    short_offset,
                    wing_offset,
                    count(*) AS theoretical_windows,
                    count(*) FILTER (WHERE entry_eligible) AS eligible_windows,
                    count(*) FILTER (WHERE exact_complete) AS exact_windows,
                    count(*) FILTER (WHERE path_complete) AS path_windows,
                    count(*) FILTER (
                      WHERE stale_5m_complete
                    ) AS stale_5m_windows,
                    count(*) FILTER (
                      WHERE stale_10m_complete
                    ) AS stale_10m_windows
                  FROM structure_status
                  GROUP BY 1, 2, 3, 4
                )
                SELECT
                  {horizon} AS horizon_minutes,
                  family,
                  short_offset,
                  wing_offset,
                  count(*) AS dataset_dates,
                  count(*) FILTER (
                    WHERE eligible_windows = theoretical_windows
                  ) AS perfect_entry_dates,
                  count(*) FILTER (
                    WHERE exact_windows = theoretical_windows
                  ) AS perfect_exact_dates,
                  count(*) FILTER (
                    WHERE path_windows = theoretical_windows
                  ) AS perfect_path_dates,
                  count(*) FILTER (
                    WHERE stale_5m_windows = theoretical_windows
                  ) AS perfect_stale_5m_dates,
                  count(*) FILTER (
                    WHERE stale_10m_windows = theoretical_windows
                  ) AS perfect_stale_10m_dates,
                  round(
                    100.0 * median(exact_windows / theoretical_windows),
                    4
                  ) AS median_daily_exact_pct,
                  round(
                    100.0 * approx_quantile(
                      exact_windows / theoretical_windows,
                      0.05
                    ),
                    4
                  ) AS p05_daily_exact_pct,
                  round(
                    100.0 * min(exact_windows / theoretical_windows),
                    4
                  ) AS worst_daily_exact_pct,
                  count(*) FILTER (
                    WHERE exact_windows / theoretical_windows >= 0.99
                  ) AS dates_at_least_99pct_exact,
                  count(*) FILTER (
                    WHERE exact_windows / theoretical_windows >= 0.95
                  ) AS dates_at_least_95pct_exact
                FROM daily
                GROUP BY 1, 2, 3, 4
                ORDER BY 2, 3, 4
                """,
            )
            result["structure_day_matrix"].extend(day_rows)

            shift_rows = _records(
                connection,
                f"""
                SELECT
                  {horizon} AS horizon_minutes,
                  CASE
                    WHEN endpoint_atm_shift_steps = 0 THEN '0'
                    WHEN endpoint_atm_shift_steps = 1 THEN '1'
                    WHEN endpoint_atm_shift_steps = 2 THEN '2'
                    WHEN endpoint_atm_shift_steps = 3 THEN '3'
                    WHEN endpoint_atm_shift_steps = 4 THEN '4'
                    WHEN endpoint_atm_shift_steps = 5 THEN '5'
                    ELSE '6_plus'
                  END AS endpoint_atm_shift_steps,
                  count(*) AS theoretical_windows,
                  count(*) FILTER (WHERE entry_eligible) AS eligible_windows,
                  count(*) FILTER (WHERE exact_complete) AS exact_windows,
                  count(*) FILTER (
                    WHERE stale_5m_complete
                  ) AS stale_5m_windows,
                  count(*) FILTER (
                    WHERE stale_10m_complete
                  ) AS stale_10m_windows
                FROM structure_status
                WHERE
                  family = 'iron_condor'
                  AND short_offset = 1
                  AND wing_offset = 3
                  AND endpoint_atm_shift_steps IS NOT NULL
                GROUP BY 1, 2
                ORDER BY 1, 2
                """,
            )
            result["key_structure_atm_shift"].extend(shift_rows)

            start_time_frames.append(
                connection.execute(
                    f"""
                    SELECT
                      {horizon} AS horizon_minutes,
                      entry_time,
                      family,
                      short_offset,
                      wing_offset,
                      count(*) AS theoretical_windows,
                      count(*) FILTER (
                        WHERE entry_present
                      ) AS entry_present_windows,
                      count(*) FILTER (
                        WHERE entry_eligible
                      ) AS entry_eligible_windows,
                      count(*) FILTER (
                        WHERE exact_complete
                      ) AS exact_endpoint_windows,
                      count(*) FILTER (
                        WHERE path_complete
                      ) AS strict_path_windows,
                      count(*) FILTER (
                        WHERE stale_5m_complete
                      ) AS stale_5m_windows,
                      count(*) FILTER (
                        WHERE stale_10m_complete
                      ) AS stale_10m_windows
                    FROM structure_status
                    GROUP BY 1, 2, 3, 4, 5
                    ORDER BY 1, 2, 3, 4, 5
                    """
                ).fetchdf()
            )
            print(f"completed horizon {horizon} minutes", flush=True)

        import pandas as pd

        start_time_frame = pd.concat(start_time_frames, ignore_index=True)
        start_time_path = output_dir / (
            "phase2_unconditional_start_time_matrix_"
            f"{session_mode}_{entry_offset_source}.parquet"
        )
        start_time_frame.to_parquet(start_time_path, index=False)
        result["detailed_start_time_matrix"] = {
            "path": str(start_time_path),
            "rows": len(start_time_frame),
        }
        connection.close()
        return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gold-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--session-mode",
        choices=("standard", "observed"),
        default="standard",
    )
    parser.add_argument(
        "--entry-offset-source",
        choices=("provider", "computed"),
        default="provider",
    )
    args = parser.parse_args()

    result = audit(
        args.gold_root.resolve(),
        args.output_dir.resolve(),
        session_mode=args.session_mode,
        entry_offset_source=args.entry_offset_source,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result["source_dates"], indent=2, sort_keys=True))
    print(json.dumps(result["detailed_start_time_matrix"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
