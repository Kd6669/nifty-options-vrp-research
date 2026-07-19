"""Map exact-quote availability by entry moneyness and intraday horizon.

The audit uses fixed daily entry anchors to avoid treating millions of
overlapping minute windows as independent research observations. Rolling
moneyness labels select a contract only at entry; every target observation is
resolved by exact trade date, expiry, strike, and option type.
"""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from typing import Any

import duckdb


HORIZONS = (15, 30, 60, 90, 120, 180, 240, 300)
ANCHOR_TIMES = ("09:30", "10:00", "11:00", "12:00", "13:00", "14:00")


def _records(connection: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    frame = connection.execute(sql).fetchdf()
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def audit(gold_root: Path) -> dict[str, Any]:
    parquet_glob = str(gold_root / "**" / "*.parquet").replace("\\", "/")
    with tempfile.TemporaryDirectory(prefix="nifty_boundary_duckdb_") as temp_dir:
        database_path = Path(temp_dir) / "boundary.duckdb"
        connection = duckdb.connect(str(database_path))
        connection.execute("PRAGMA threads=8")
        connection.execute("PRAGMA memory_limit='12GB'")
        connection.execute(f"PRAGMA temp_directory='{temp_dir.replace(chr(92), '/')}'")
        source = f"read_parquet('{parquet_glob}', hive_partitioning=true)"

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
              AND canonical_bsm_population
              AND session_status = 'regular_session'
            GROUP BY 1, 2, 3, 4, 5
            HAVING count(*) > 1
            """
        )
        connection.execute(
            f"""
            CREATE TABLE surface AS
            SELECT
              source.trade_date,
              source.timestamp_ist,
              source.actual_expiry_date,
              cast(source.strike AS DOUBLE) AS strike,
              source.option_type,
              source.provider_moneyness_offset AS entry_offset,
              source.close,
              source.independent_nifty_spot AS spot,
              source.recomputed_atm_strike AS atm_strike,
              source.dte,
              source.quality_gate_status,
              source.bsm_gate_status,
              source.strike_ladder_valid,
              source.provider_moneyness_matches_computed
            FROM {source} AS source
            ANTI JOIN duplicate_identities AS duplicate
              ON source.trade_date = duplicate.trade_date
             AND source.timestamp_ist = duplicate.timestamp_ist
             AND source.actual_expiry_date = duplicate.actual_expiry_date
             AND source.strike = duplicate.strike
             AND source.option_type = duplicate.option_type
            WHERE
              source.expiry_flag = 'WEEK'
              AND source.canonical_bsm_population
              AND source.session_status = 'regular_session'
              AND source.actual_expiry_date IS NOT NULL
              AND source.provider_moneyness_offset BETWEEN -10 AND 10
            """
        )
        lead_columns = ",\n".join(
            f"""lead(timestamp_ist, {horizon}) OVER exact_contract
                = timestamp_ist + INTERVAL {horizon} MINUTE
                AS path_{horizon}_complete"""
            for horizon in HORIZONS
        )
        connection.execute(
            f"""
            CREATE TABLE path_surface AS
            SELECT
              *,
              {lead_columns}
            FROM surface
            WINDOW exact_contract AS (
              PARTITION BY
                trade_date,
                actual_expiry_date,
                strike,
                option_type
              ORDER BY timestamp_ist
            )
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
            FROM surface
            WHERE spot IS NOT NULL
            GROUP BY 1, 2
            """
        )
        connection.execute(
            "CREATE TABLE horizons(horizon_minutes INTEGER)"
        )
        connection.executemany(
            "INSERT INTO horizons VALUES (?)",
            [(horizon,) for horizon in HORIZONS],
        )
        anchor_values = ", ".join(f"'{value}'" for value in ANCHOR_TIMES)
        path_case = "\n".join(
            (
                f"WHEN {horizon} THEN entry.path_{horizon}_complete"
                if index == 0
                else f"WHEN {horizon} THEN entry.path_{horizon}_complete"
            )
            for index, horizon in enumerate(HORIZONS)
        )
        connection.execute(
            f"""
            CREATE TABLE targets AS
            SELECT
              entry.trade_date,
              entry.timestamp_ist AS entry_timestamp,
              entry.actual_expiry_date,
              entry.strike,
              entry.option_type,
              entry.entry_offset,
              entry.close AS entry_close,
              entry.spot AS entry_spot,
              entry.atm_strike AS entry_atm_strike,
              entry.dte,
              strftime(
                entry.timestamp_ist AT TIME ZONE 'Asia/Kolkata',
                '%H:%M'
              ) AS entry_time,
              horizon.horizon_minutes,
              entry.timestamp_ist
                + horizon.horizon_minutes * INTERVAL 1 MINUTE
                AS target_timestamp,
              target_spot.spot AS target_spot,
              target_spot.atm_strike AS target_atm_strike,
              abs(target_spot.spot / entry.spot - 1.0) * 10000.0
                AS endpoint_abs_move_bps,
              abs(target_spot.atm_strike - entry.atm_strike) / 50.0
                AS endpoint_atm_shift_steps,
              CASE horizon.horizon_minutes
                {path_case}
              END AS strict_path_complete
            FROM path_surface AS entry
            CROSS JOIN horizons AS horizon
            LEFT JOIN spot_minutes AS target_spot
              ON entry.trade_date = target_spot.trade_date
             AND target_spot.timestamp_ist
               = entry.timestamp_ist
                 + horizon.horizon_minutes * INTERVAL 1 MINUTE
            WHERE
              strftime(
                entry.timestamp_ist AT TIME ZONE 'Asia/Kolkata',
                '%H:%M'
              ) IN ({anchor_values})
              AND entry.quality_gate_status = 'pass'
              AND entry.bsm_gate_status = 'READY'
              AND entry.strike_ladder_valid
              AND entry.provider_moneyness_matches_computed
              AND (
                extract(hour FROM entry.timestamp_ist AT TIME ZONE 'Asia/Kolkata')
                  * 60
                + extract(minute FROM entry.timestamp_ist AT TIME ZONE 'Asia/Kolkata')
                + horizon.horizon_minutes
              ) <= 929
            """
        )
        connection.execute(
            """
            CREATE TABLE quote_matches AS
            SELECT
              target.*,
              quote.timestamp_ist AS matched_quote_timestamp,
              quote.close AS matched_close,
              quote.entry_offset AS matched_offset,
              date_diff(
                'minute',
                quote.timestamp_ist,
                target.target_timestamp
              ) AS staleness_minutes,
              quote.timestamp_ist = target.target_timestamp
                AS exact_endpoint_available
            FROM targets AS target
            ASOF LEFT JOIN surface AS quote
              ON target.trade_date = quote.trade_date
             AND target.actual_expiry_date = quote.actual_expiry_date
             AND target.strike = quote.strike
             AND target.option_type = quote.option_type
             AND target.target_timestamp >= quote.timestamp_ist
            """
        )
        connection.execute(
            """
            CREATE TABLE usable_quotes AS
            SELECT
              *,
              matched_quote_timestamp >= entry_timestamp AS quote_after_entry,
              (
                matched_quote_timestamp >= entry_timestamp
                AND staleness_minutes <= 1
              ) AS usable_stale_1m,
              (
                matched_quote_timestamp >= entry_timestamp
                AND staleness_minutes <= 2
              ) AS usable_stale_2m,
              (
                matched_quote_timestamp >= entry_timestamp
                AND staleness_minutes <= 5
              ) AS usable_stale_5m,
              (
                matched_quote_timestamp >= entry_timestamp
                AND staleness_minutes <= 10
              ) AS usable_stale_10m
            FROM quote_matches
            """
        )

        # Four-leg families. The wing is the outer boundary and the short
        # offset remains fixed. These are availability contracts, not strategies.
        connection.execute(
            """
            CREATE TABLE structure_requirements(
              family VARCHAR,
              short_offset INTEGER,
              wing_offset INTEGER,
              option_type VARCHAR,
              entry_offset INTEGER
            )
            """
        )
        requirements: list[tuple[str, int, int, str, int]] = []
        for wing in range(1, 11):
            requirements.extend(
                [
                    ("iron_fly", 0, wing, "CALL", 0),
                    ("iron_fly", 0, wing, "PUT", 0),
                    ("iron_fly", 0, wing, "CALL", wing),
                    ("iron_fly", 0, wing, "PUT", -wing),
                ]
            )
        for short in (1, 2, 3):
            for wing in range(short + 1, 11):
                requirements.extend(
                    [
                        ("iron_condor", short, wing, "CALL", short),
                        ("iron_condor", short, wing, "PUT", -short),
                        ("iron_condor", short, wing, "CALL", wing),
                        ("iron_condor", short, wing, "PUT", -wing),
                    ]
                )
        connection.executemany(
            "INSERT INTO structure_requirements VALUES (?, ?, ?, ?, ?)",
            requirements,
        )
        connection.execute(
            """
            CREATE TABLE candidate_windows AS
            SELECT DISTINCT
              trade_date,
              entry_timestamp,
              actual_expiry_date,
              entry_time,
              horizon_minutes,
              dte,
              endpoint_abs_move_bps,
              endpoint_atm_shift_steps
            FROM usable_quotes
            """
        )
        connection.execute(
            """
            CREATE TABLE structure_windows AS
            SELECT
              candidate.trade_date,
              candidate.entry_timestamp,
              candidate.actual_expiry_date,
              candidate.entry_time,
              candidate.horizon_minutes,
              candidate.dte,
              candidate.endpoint_abs_move_bps,
              candidate.endpoint_atm_shift_steps,
              structure.family,
              structure.short_offset,
              structure.wing_offset,
              count(quote.strike) = 4 AS entry_complete,
              count(quote.strike) FILTER (
                WHERE quote.exact_endpoint_available
              ) = 4 AS exact_endpoint_complete,
              count(quote.strike) FILTER (
                WHERE quote.strict_path_complete
              ) = 4 AS strict_path_complete,
              count(quote.strike) FILTER (
                WHERE quote.usable_stale_1m
              ) = 4 AS stale_1m_complete,
              count(quote.strike) FILTER (
                WHERE quote.usable_stale_2m
              ) = 4 AS stale_2m_complete,
              count(quote.strike) FILTER (
                WHERE quote.usable_stale_5m
              ) = 4 AS stale_5m_complete,
              count(quote.strike) FILTER (
                WHERE quote.usable_stale_10m
              ) = 4 AS stale_10m_complete,
              max(quote.staleness_minutes) FILTER (
                WHERE quote.matched_quote_timestamp >= quote.entry_timestamp
              ) AS maximum_leg_staleness_minutes
            FROM candidate_windows AS candidate
            CROSS JOIN (
              SELECT DISTINCT family, short_offset, wing_offset
              FROM structure_requirements
            ) AS structure
            JOIN structure_requirements AS requirement
              ON requirement.family = structure.family
             AND requirement.short_offset = structure.short_offset
             AND requirement.wing_offset = structure.wing_offset
            LEFT JOIN usable_quotes AS quote
              ON candidate.trade_date = quote.trade_date
             AND candidate.entry_timestamp = quote.entry_timestamp
             AND candidate.actual_expiry_date = quote.actual_expiry_date
             AND candidate.horizon_minutes = quote.horizon_minutes
             AND requirement.option_type = quote.option_type
             AND requirement.entry_offset = quote.entry_offset
            GROUP BY
              candidate.trade_date,
              candidate.entry_timestamp,
              candidate.actual_expiry_date,
              candidate.entry_time,
              candidate.horizon_minutes,
              candidate.dte,
              candidate.endpoint_abs_move_bps,
              candidate.endpoint_atm_shift_steps,
              structure.family,
              structure.short_offset,
              structure.wing_offset
            """
        )

        result: dict[str, Any] = {
            "contract": {
                "horizons_minutes": list(HORIZONS),
                "entry_times_ist": list(ANCHOR_TIMES),
                "canonical_matrix_entry_time_ist": "10:00",
                "last_consistent_market_minute_ist": "15:29",
                "entry_selection": "rolling moneyness label only at entry",
                "tracking_key": [
                    "trade_date",
                    "actual_expiry_date",
                    "strike",
                    "option_type",
                ],
                "strict_path": "all horizon+1 one-minute observations exist",
                "stale_quote": "last exact-contract quote no later than target",
            },
            "leg_matrix_1000": _records(
                connection,
                """
                SELECT
                  abs(entry_offset) AS abs_entry_offset,
                  horizon_minutes,
                  count(*) AS entry_legs,
                  count(*) FILTER (
                    WHERE exact_endpoint_available
                  ) AS exact_endpoint_legs,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE exact_endpoint_available
                    ) / count(*),
                    4
                  ) AS exact_endpoint_pct,
                  count(*) FILTER (
                    WHERE strict_path_complete
                  ) AS strict_path_legs,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE strict_path_complete
                    ) / count(*),
                    4
                  ) AS strict_path_pct,
                  count(*) FILTER (WHERE usable_stale_1m) AS stale_1m_legs,
                  round(
                    100.0 * count(*) FILTER (WHERE usable_stale_1m) / count(*),
                    4
                  ) AS stale_1m_pct,
                  count(*) FILTER (WHERE usable_stale_2m) AS stale_2m_legs,
                  round(
                    100.0 * count(*) FILTER (WHERE usable_stale_2m) / count(*),
                    4
                  ) AS stale_2m_pct,
                  count(*) FILTER (WHERE usable_stale_5m) AS stale_5m_legs,
                  round(
                    100.0 * count(*) FILTER (WHERE usable_stale_5m) / count(*),
                    4
                  ) AS stale_5m_pct,
                  count(*) FILTER (WHERE usable_stale_10m) AS stale_10m_legs,
                  round(
                    100.0 * count(*) FILTER (WHERE usable_stale_10m) / count(*),
                    4
                  ) AS stale_10m_pct,
                  count(*) FILTER (
                    WHERE NOT coalesce(usable_stale_10m, false)
                  ) AS proxy_required_after_10m
                FROM usable_quotes
                WHERE entry_time = '10:00'
                GROUP BY 1, 2
                ORDER BY 1, 2
                """,
            ),
            "signed_leg_matrix_1000": _records(
                connection,
                """
                SELECT
                  entry_offset,
                  option_type,
                  horizon_minutes,
                  count(*) AS entry_legs,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE exact_endpoint_available
                    ) / count(*),
                    4
                  ) AS exact_endpoint_pct,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE strict_path_complete
                    ) / count(*),
                    4
                  ) AS strict_path_pct,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE usable_stale_5m
                    ) / count(*),
                    4
                  ) AS stale_5m_pct,
                  count(*) FILTER (
                    WHERE NOT coalesce(usable_stale_10m, false)
                  ) AS proxy_required_after_10m
                FROM usable_quotes
                WHERE entry_time = '10:00'
                GROUP BY 1, 2, 3
                ORDER BY 1, 2, 3
                """,
            ),
            "structure_matrix_1000": _records(
                connection,
                """
                SELECT
                  family,
                  short_offset,
                  wing_offset,
                  horizon_minutes,
                  count(*) FILTER (WHERE entry_complete) AS entry_windows,
                  count(*) FILTER (
                    WHERE entry_complete AND exact_endpoint_complete
                  ) AS exact_endpoint_windows,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND exact_endpoint_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    4
                  ) AS exact_endpoint_pct,
                  count(*) FILTER (
                    WHERE entry_complete AND strict_path_complete
                  ) AS strict_path_windows,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND strict_path_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    4
                  ) AS strict_path_pct,
                  count(*) FILTER (
                    WHERE entry_complete AND stale_1m_complete
                  ) AS stale_1m_windows,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND stale_1m_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    4
                  ) AS stale_1m_pct,
                  count(*) FILTER (
                    WHERE entry_complete AND stale_2m_complete
                  ) AS stale_2m_windows,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND stale_2m_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    4
                  ) AS stale_2m_pct,
                  count(*) FILTER (
                    WHERE entry_complete AND stale_5m_complete
                  ) AS stale_5m_windows,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND stale_5m_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    4
                  ) AS stale_5m_pct,
                  count(*) FILTER (
                    WHERE entry_complete AND stale_10m_complete
                  ) AS stale_10m_windows,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND stale_10m_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    4
                  ) AS stale_10m_pct,
                  count(*) FILTER (
                    WHERE
                      entry_complete
                      AND NOT coalesce(stale_10m_complete, false)
                  ) AS proxy_required_after_10m
                FROM structure_windows
                WHERE entry_time = '10:00'
                GROUP BY 1, 2, 3, 4
                ORDER BY 1, 2, 3, 4
                """,
            ),
            "anchor_sensitivity_atm3": _records(
                connection,
                """
                SELECT
                  family,
                  short_offset,
                  wing_offset,
                  entry_time,
                  horizon_minutes,
                  count(*) FILTER (WHERE entry_complete) AS entry_windows,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND exact_endpoint_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    4
                  ) AS exact_endpoint_pct,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND strict_path_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    4
                  ) AS strict_path_pct,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND stale_5m_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    4
                  ) AS stale_5m_pct,
                  count(*) FILTER (
                    WHERE
                      entry_complete
                      AND NOT coalesce(stale_10m_complete, false)
                  ) AS proxy_required_after_10m
                FROM structure_windows
                WHERE
                  (family = 'iron_fly' AND wing_offset = 3)
                  OR (
                    family = 'iron_condor'
                    AND short_offset IN (1, 2)
                    AND wing_offset = 3
                  )
                GROUP BY 1, 2, 3, 4, 5
                ORDER BY 1, 2, 3, 4, 5
                """,
            ),
            "staleness_distribution_missing_exact_1000": _records(
                connection,
                """
                SELECT
                  abs(entry_offset) AS abs_entry_offset,
                  horizon_minutes,
                  count(*) AS missing_exact_legs,
                  count(*) FILTER (
                    WHERE matched_quote_timestamp >= entry_timestamp
                  ) AS last_quote_after_entry,
                  approx_quantile(
                    staleness_minutes,
                    [0.5, 0.9, 0.95, 0.99, 1.0]
                  ) FILTER (
                    WHERE matched_quote_timestamp >= entry_timestamp
                  ) AS staleness_quantiles_minutes
                FROM usable_quotes
                WHERE
                  entry_time = '10:00'
                  AND NOT exact_endpoint_available
                GROUP BY 1, 2
                ORDER BY 1, 2
                """,
            ),
            "coverage_by_atm_shift_1000": _records(
                connection,
                """
                SELECT
                  family,
                  short_offset,
                  wing_offset,
                  horizon_minutes,
                  CASE
                    WHEN endpoint_atm_shift_steps = 0 THEN '0'
                    WHEN endpoint_atm_shift_steps = 1 THEN '1'
                    WHEN endpoint_atm_shift_steps = 2 THEN '2'
                    WHEN endpoint_atm_shift_steps = 3 THEN '3'
                    WHEN endpoint_atm_shift_steps = 4 THEN '4'
                    WHEN endpoint_atm_shift_steps = 5 THEN '5'
                    ELSE '6_plus'
                  END AS endpoint_atm_shift_steps,
                  count(*) FILTER (WHERE entry_complete) AS entry_windows,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND exact_endpoint_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    3
                  ) AS exact_endpoint_pct,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND stale_5m_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    3
                  ) AS stale_5m_pct,
                  count(*) FILTER (
                    WHERE
                      entry_complete
                      AND NOT coalesce(stale_10m_complete, false)
                  ) AS proxy_required_after_10m
                FROM structure_windows
                WHERE
                  entry_time = '10:00'
                  AND endpoint_atm_shift_steps IS NOT NULL
                  AND (
                    (family = 'iron_fly' AND wing_offset IN (3, 5, 7, 9))
                    OR (
                      family = 'iron_condor'
                      AND short_offset = 1
                      AND wing_offset IN (3, 5, 7, 9)
                    )
                  )
                GROUP BY 1, 2, 3, 4, 5
                ORDER BY 1, 2, 3, 4, 5
                """,
            ),
        }
        connection.close()
        return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gold-root", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = audit(args.gold_root.resolve())
    rendered = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()
