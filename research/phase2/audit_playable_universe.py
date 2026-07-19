"""Audit the exact-contract 60-minute research universe in the NIFTY gold data.

Rolling moneyness labels are used only to select legs at entry. Every later
observation is matched by trade date, actual expiry, strike, and option type.
"""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from typing import Any

import duckdb


STRUCTURE_REQUIREMENTS = (
    ("iron_fly_0_3", "CALL", 0),
    ("iron_fly_0_3", "PUT", 0),
    ("iron_fly_0_3", "CALL", 3),
    ("iron_fly_0_3", "PUT", -3),
    ("iron_condor_1_3", "CALL", 1),
    ("iron_condor_1_3", "PUT", -1),
    ("iron_condor_1_3", "CALL", 3),
    ("iron_condor_1_3", "PUT", -3),
    ("iron_condor_2_3", "CALL", 2),
    ("iron_condor_2_3", "PUT", -2),
    ("iron_condor_2_3", "CALL", 3),
    ("iron_condor_2_3", "PUT", -3),
)


def _records(connection: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    frame = connection.execute(sql).fetchdf()
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def audit(gold_root: Path) -> dict[str, Any]:
    parquet_glob = str(gold_root / "**" / "*.parquet").replace("\\", "/")

    with tempfile.TemporaryDirectory(prefix="nifty_phase2_duckdb_") as temp_dir:
        database_path = Path(temp_dir) / "audit.duckdb"
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
              source.moneyness_label,
              source.close,
              source.independent_nifty_spot AS spot,
              source.recomputed_atm_strike,
              source.dte
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

        connection.execute(
            """
            CREATE TABLE spot_minutes AS
            SELECT
              trade_date,
              timestamp_ist,
              median(spot) AS spot,
              median(recomputed_atm_strike) AS atm_strike
            FROM surface
            WHERE spot IS NOT NULL
            GROUP BY 1, 2
            """
        )

        connection.execute(
            """
            CREATE TABLE spot_windows AS
            SELECT * EXCLUDE (
              endpoint_timestamp,
              endpoint_spot,
              endpoint_atm_strike
            ),
            endpoint_timestamp = timestamp_ist + INTERVAL 60 MINUTE
              AS spot_path_complete,
            endpoint_spot,
            endpoint_atm_strike,
            CASE
              WHEN endpoint_timestamp = timestamp_ist + INTERVAL 60 MINUTE
              THEN abs(endpoint_spot / spot - 1.0) * 10000.0
            END AS endpoint_abs_move_bps,
            CASE
              WHEN endpoint_timestamp = timestamp_ist + INTERVAL 60 MINUTE
              THEN greatest(
                abs(path_max_spot / spot - 1.0),
                abs(path_min_spot / spot - 1.0)
              ) * 10000.0
            END AS max_abs_excursion_bps,
            CASE
              WHEN endpoint_timestamp = timestamp_ist + INTERVAL 60 MINUTE
              THEN abs(endpoint_atm_strike - atm_strike) / 50.0
            END AS endpoint_atm_shift_steps
            FROM (
              SELECT
                trade_date,
                timestamp_ist,
                spot,
                atm_strike,
                lead(timestamp_ist, 60) OVER window_60 AS endpoint_timestamp,
                lead(spot, 60) OVER window_60 AS endpoint_spot,
                lead(atm_strike, 60) OVER window_60 AS endpoint_atm_strike,
                max(spot) OVER (
                  PARTITION BY trade_date
                  ORDER BY timestamp_ist
                  ROWS BETWEEN CURRENT ROW AND 60 FOLLOWING
                ) AS path_max_spot,
                min(spot) OVER (
                  PARTITION BY trade_date
                  ORDER BY timestamp_ist
                  ROWS BETWEEN CURRENT ROW AND 60 FOLLOWING
                ) AS path_min_spot
              FROM spot_minutes
              WINDOW window_60 AS (
                PARTITION BY trade_date
                ORDER BY timestamp_ist
              )
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE path_surface AS
            SELECT
              *,
              lead(timestamp_ist, 60) OVER exact_contract = timestamp_ist + INTERVAL 60 MINUTE
                AS strict_path_complete
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
            CREATE TABLE leg_windows AS
            SELECT
              entry.trade_date,
              entry.timestamp_ist,
              entry.actual_expiry_date,
              entry.option_type,
              entry.entry_offset,
              entry.strike,
              entry.dte,
              exit_row.timestamp_ist IS NOT NULL AS endpoint_available,
              entry.strict_path_complete,
              exit_row.entry_offset AS endpoint_offset
            FROM path_surface AS entry
            LEFT JOIN surface AS exit_row
              ON entry.trade_date = exit_row.trade_date
             AND entry.actual_expiry_date = exit_row.actual_expiry_date
             AND entry.strike = exit_row.strike
             AND entry.option_type = exit_row.option_type
             AND exit_row.timestamp_ist = entry.timestamp_ist + INTERVAL 60 MINUTE
            WHERE
              entry.entry_offset BETWEEN -3 AND 3
              AND strftime(
                entry.timestamp_ist AT TIME ZONE 'Asia/Kolkata',
                '%H:%M'
              ) BETWEEN '09:15' AND '14:29'
            """
        )

        connection.execute(
            """
            CREATE TABLE requirements(
              structure VARCHAR,
              option_type VARCHAR,
              entry_offset INTEGER
            )
            """
        )
        connection.executemany(
            "INSERT INTO requirements VALUES (?, ?, ?)",
            STRUCTURE_REQUIREMENTS,
        )

        connection.execute(
            """
            CREATE TABLE candidate_windows AS
            SELECT
              surface.trade_date,
              surface.timestamp_ist,
              surface.actual_expiry_date,
              median(surface.dte) AS dte
            FROM surface
            WHERE
              strftime(
                surface.timestamp_ist AT TIME ZONE 'Asia/Kolkata',
                '%H:%M'
              ) BETWEEN '09:15' AND '14:29'
            GROUP BY 1, 2, 3
            """
        )

        connection.execute(
            """
            CREATE TABLE structure_windows AS
            SELECT
              candidate.trade_date,
              candidate.timestamp_ist,
              candidate.actual_expiry_date,
              candidate.dte,
              structure.structure,
              count(leg.strike) = 4 AS entry_complete,
              count(leg.strike) FILTER (WHERE leg.endpoint_available) = 4
                AS endpoint_complete,
              count(leg.strike) FILTER (WHERE leg.strict_path_complete) = 4
                AS strict_path_complete,
              spot.spot_path_complete,
              spot.endpoint_abs_move_bps,
              spot.max_abs_excursion_bps,
              spot.endpoint_atm_shift_steps,
              CASE
                WHEN candidate.trade_date < DATE '2025-09-01'
                THEN 'pre_tuesday_expiry'
                ELSE 'tuesday_expiry'
              END AS expiry_regime,
              strftime(
                candidate.timestamp_ist AT TIME ZONE 'Asia/Kolkata',
                '%H:%M'
              ) AS entry_time
            FROM candidate_windows AS candidate
            CROSS JOIN (SELECT DISTINCT structure FROM requirements) AS structure
            JOIN requirements AS requirement
              ON requirement.structure = structure.structure
            LEFT JOIN leg_windows AS leg
              ON candidate.trade_date = leg.trade_date
             AND candidate.timestamp_ist = leg.timestamp_ist
             AND candidate.actual_expiry_date = leg.actual_expiry_date
             AND requirement.option_type = leg.option_type
             AND requirement.entry_offset = leg.entry_offset
            LEFT JOIN spot_windows AS spot
              ON candidate.trade_date = spot.trade_date
             AND candidate.timestamp_ist = spot.timestamp_ist
            GROUP BY
              candidate.trade_date,
              candidate.timestamp_ist,
              candidate.actual_expiry_date,
              candidate.dte,
              structure.structure,
              spot.spot_path_complete,
              spot.endpoint_abs_move_bps,
              spot.max_abs_excursion_bps,
              spot.endpoint_atm_shift_steps
            """
        )

        result: dict[str, Any] = {
            "contract": {
                "horizon_minutes": 60,
                "entry_selection": "provider_moneyness_offset within ATM +/- 3",
                "tracking_key": [
                    "trade_date",
                    "actual_expiry_date",
                    "strike",
                    "option_type",
                ],
                "strict_path_observations": 61,
                "entry_clock_range_ist": ["09:15", "14:29"],
                "weekly_surface": (
                    "Dhan WEEK expiry code 1; provider response omits actual expiry, "
                    "audited mapping selects the second eligible weekly contract"
                ),
            },
            "source": _records(
                connection,
                """
                SELECT
                  count(*) AS clean_weekly_surface_rows,
                  count(DISTINCT trade_date) AS trade_dates,
                  min(trade_date) AS first_trade_date,
                  max(trade_date) AS last_trade_date
                FROM surface
                """,
            )[0],
            "excluded_duplicate_exact_identities": _records(
                connection,
                """
                SELECT
                  count(*) AS duplicate_identity_groups,
                  coalesce(sum(identity_rows - 1), 0) AS excess_rows
                FROM duplicate_identities
                """,
            )[0],
            "leg_coverage": _records(
                connection,
                """
                SELECT
                  entry_offset,
                  option_type,
                  count(*) AS entry_legs,
                  count(*) FILTER (WHERE endpoint_available) AS endpoint_legs,
                  round(
                    100.0 * count(*) FILTER (WHERE endpoint_available) / count(*),
                    4
                  ) AS endpoint_coverage_pct,
                  count(*) FILTER (WHERE strict_path_complete) AS strict_path_legs,
                  round(
                    100.0 * count(*) FILTER (WHERE strict_path_complete) / count(*),
                    4
                  ) AS strict_path_coverage_pct
                FROM leg_windows
                GROUP BY 1, 2
                ORDER BY 1, 2
                """,
            ),
            "endpoint_offset_migration": _records(
                connection,
                """
                SELECT
                  entry_offset,
                  option_type,
                  count(*) FILTER (WHERE endpoint_available) AS endpoint_legs,
                  count(*) FILTER (
                    WHERE endpoint_available AND abs(endpoint_offset) <= 3
                  ) AS endpoint_still_within_atm_3,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE endpoint_available AND abs(endpoint_offset) <= 3
                    ) / nullif(count(*) FILTER (WHERE endpoint_available), 0),
                    3
                  ) AS endpoint_still_within_atm_3_pct,
                  count(*) FILTER (
                    WHERE endpoint_available AND abs(endpoint_offset) > 3
                  ) AS endpoint_migrated_outside_atm_3,
                  max(abs(endpoint_offset)) FILTER (
                    WHERE endpoint_available
                  ) AS maximum_observed_abs_endpoint_offset
                FROM leg_windows
                GROUP BY 1, 2
                ORDER BY 1, 2
                """,
            ),
            "structure_coverage": _records(
                connection,
                """
                SELECT
                  structure,
                  count(*) AS possible_clock_windows,
                  count(*) FILTER (WHERE entry_complete) AS entry_complete_windows,
                  count(*) FILTER (
                    WHERE entry_complete AND endpoint_complete
                  ) AS endpoint_complete_windows,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND endpoint_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    4
                  ) AS endpoint_coverage_pct,
                  count(*) FILTER (
                    WHERE entry_complete AND strict_path_complete
                  ) AS strict_path_complete_windows,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND strict_path_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    4
                  ) AS strict_path_coverage_pct
                FROM structure_windows
                GROUP BY 1
                ORDER BY 1
                """,
            ),
            "anchor_time_coverage": _records(
                connection,
                """
                SELECT
                  structure,
                  entry_time,
                  count(*) FILTER (WHERE entry_complete) AS entry_windows,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND endpoint_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    3
                  ) AS endpoint_coverage_pct,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND strict_path_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    3
                  ) AS strict_path_coverage_pct
                FROM structure_windows
                WHERE
                  entry_time IN (
                    '09:30', '10:00', '10:30', '11:00', '11:30',
                    '12:00', '12:30', '13:00', '13:30', '14:00'
                  )
                GROUP BY 1, 2
                ORDER BY 1, 2
                """,
            ),
            "year_coverage": _records(
                connection,
                """
                SELECT
                  structure,
                  year(trade_date) AS trade_year,
                  count(*) FILTER (WHERE entry_complete) AS entry_windows,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND endpoint_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    3
                  ) AS endpoint_coverage_pct,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND strict_path_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    3
                  ) AS strict_path_coverage_pct
                FROM structure_windows
                GROUP BY 1, 2
                ORDER BY 1, 2
                """,
            ),
            "regime_coverage": _records(
                connection,
                """
                SELECT
                  structure,
                  expiry_regime,
                  count(*) FILTER (WHERE entry_complete) AS entry_windows,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND endpoint_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    3
                  ) AS endpoint_coverage_pct,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND strict_path_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    3
                  ) AS strict_path_coverage_pct
                FROM structure_windows
                GROUP BY 1, 2
                ORDER BY 1, 2
                """,
            ),
            "dte_coverage": _records(
                connection,
                """
                SELECT
                  structure,
                  CASE
                    WHEN dte <= 7.5 THEN '0_to_7_5'
                    WHEN dte <= 14.5 THEN '7_5_to_14_5'
                    ELSE '14_5_plus'
                  END AS dte_bucket,
                  count(*) FILTER (WHERE entry_complete) AS entry_windows,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND endpoint_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    3
                  ) AS endpoint_coverage_pct,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND strict_path_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    3
                  ) AS strict_path_coverage_pct
                FROM structure_windows
                GROUP BY 1, 2
                ORDER BY 1, 2
                """,
            ),
            "excursion_coverage": _records(
                connection,
                """
                SELECT
                  structure,
                  CASE
                    WHEN max_abs_excursion_bps < 25 THEN 'lt_25_bps'
                    WHEN max_abs_excursion_bps < 50 THEN '25_to_50_bps'
                    WHEN max_abs_excursion_bps < 100 THEN '50_to_100_bps'
                    WHEN max_abs_excursion_bps < 150 THEN '100_to_150_bps'
                    ELSE '150_plus_bps'
                  END AS max_excursion_bucket,
                  count(*) FILTER (
                    WHERE entry_complete AND spot_path_complete
                  ) AS entry_windows,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE
                        entry_complete
                        AND spot_path_complete
                        AND endpoint_complete
                    ) / nullif(count(*) FILTER (
                      WHERE entry_complete AND spot_path_complete
                    ), 0),
                    3
                  ) AS endpoint_coverage_pct,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE
                        entry_complete
                        AND spot_path_complete
                        AND strict_path_complete
                    ) / nullif(count(*) FILTER (
                      WHERE entry_complete AND spot_path_complete
                    ), 0),
                    3
                  ) AS strict_path_coverage_pct
                FROM structure_windows
                WHERE max_abs_excursion_bps IS NOT NULL
                GROUP BY 1, 2
                ORDER BY 1, 2
                """,
            ),
            "fixed_1000_excursion_coverage": _records(
                connection,
                """
                SELECT
                  structure,
                  CASE
                    WHEN max_abs_excursion_bps < 25 THEN 'lt_25_bps'
                    WHEN max_abs_excursion_bps < 50 THEN '25_to_50_bps'
                    WHEN max_abs_excursion_bps < 100 THEN '50_to_100_bps'
                    WHEN max_abs_excursion_bps < 150 THEN '100_to_150_bps'
                    ELSE '150_plus_bps'
                  END AS max_excursion_bucket,
                  count(*) FILTER (
                    WHERE entry_complete AND spot_path_complete
                  ) AS entry_windows,
                  count(*) FILTER (
                    WHERE
                      entry_complete
                      AND spot_path_complete
                      AND endpoint_complete
                  ) AS endpoint_complete_windows,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE
                        entry_complete
                        AND spot_path_complete
                        AND endpoint_complete
                    ) / nullif(count(*) FILTER (
                      WHERE entry_complete AND spot_path_complete
                    ), 0),
                    3
                  ) AS endpoint_coverage_pct,
                  count(*) FILTER (
                    WHERE
                      entry_complete
                      AND spot_path_complete
                      AND strict_path_complete
                  ) AS strict_path_complete_windows,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE
                        entry_complete
                        AND spot_path_complete
                        AND strict_path_complete
                    ) / nullif(count(*) FILTER (
                      WHERE entry_complete AND spot_path_complete
                    ), 0),
                    3
                  ) AS strict_path_coverage_pct
                FROM structure_windows
                WHERE
                  max_abs_excursion_bps IS NOT NULL
                  AND entry_time = '10:00'
                GROUP BY 1, 2
                ORDER BY 1, 2
                """,
            ),
            "fixed_1000_regime_coverage": _records(
                connection,
                """
                SELECT
                  structure,
                  expiry_regime,
                  count(*) FILTER (WHERE entry_complete) AS entry_windows,
                  count(*) FILTER (
                    WHERE entry_complete AND endpoint_complete
                  ) AS endpoint_complete_windows,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND endpoint_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    3
                  ) AS endpoint_coverage_pct,
                  count(*) FILTER (
                    WHERE entry_complete AND strict_path_complete
                  ) AS strict_path_complete_windows,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE entry_complete AND strict_path_complete
                    ) / nullif(count(*) FILTER (WHERE entry_complete), 0),
                    3
                  ) AS strict_path_coverage_pct
                FROM structure_windows
                WHERE entry_time = '10:00'
                GROUP BY 1, 2
                ORDER BY 1, 2
                """,
            ),
            "largest_missing_path_dates": _records(
                connection,
                """
                SELECT
                  trade_date,
                  count(*) FILTER (
                    WHERE entry_complete AND NOT strict_path_complete
                  ) AS missing_strict_windows,
                  count(*) FILTER (
                    WHERE entry_complete AND NOT endpoint_complete
                  ) AS missing_endpoint_windows,
                  round(max(max_abs_excursion_bps), 3) AS largest_excursion_bps
                FROM structure_windows
                WHERE structure = 'iron_condor_1_3'
                GROUP BY 1
                HAVING
                  count(*) FILTER (
                    WHERE entry_complete AND NOT strict_path_complete
                  ) > 0
                ORDER BY missing_strict_windows DESC, trade_date
                LIMIT 25
                """,
            ),
            "atm_shift_coverage": _records(
                connection,
                """
                SELECT
                  structure,
                  CASE
                    WHEN endpoint_atm_shift_steps = 0 THEN '0_steps'
                    WHEN endpoint_atm_shift_steps = 1 THEN '1_step'
                    WHEN endpoint_atm_shift_steps = 2 THEN '2_steps'
                    WHEN endpoint_atm_shift_steps = 3 THEN '3_steps'
                    ELSE '4_plus_steps'
                  END AS endpoint_atm_shift,
                  count(*) FILTER (
                    WHERE entry_complete AND spot_path_complete
                  ) AS entry_windows,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE
                        entry_complete
                        AND spot_path_complete
                        AND endpoint_complete
                    ) / nullif(count(*) FILTER (
                      WHERE entry_complete AND spot_path_complete
                    ), 0),
                    3
                  ) AS endpoint_coverage_pct,
                  round(
                    100.0 * count(*) FILTER (
                      WHERE
                        entry_complete
                        AND spot_path_complete
                        AND strict_path_complete
                    ) / nullif(count(*) FILTER (
                      WHERE entry_complete AND spot_path_complete
                    ), 0),
                    3
                  ) AS strict_path_coverage_pct
                FROM structure_windows
                WHERE endpoint_atm_shift_steps IS NOT NULL
                GROUP BY 1, 2
                ORDER BY 1, 2
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
