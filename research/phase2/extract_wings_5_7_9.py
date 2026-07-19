"""Extract and extend the unconditional audit for short-1 wide iron condors.

Most results are losslessly extracted from the completed observed-session,
computed-moneyness audit.  Only the endpoint-ATM-migration cross-tab is
recomputed from gold, because that conditional detail is not present in the
pooled artifact for wings wider than ATM +/-3.
"""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from typing import Any

import duckdb


HORIZONS = (15, 30, 60, 90, 120, 180, 240, 300)
TARGET_WINGS = (5, 7, 9)
ALL_REPORT_WINGS = (3, *TARGET_WINGS)


def _pct(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return round(100.0 * numerator / denominator, 4)


def _leg_bit(entry_offset: int, option_type: str) -> int:
    side = 0 if option_type == "CALL" else 1
    return 1 << ((entry_offset + 10) * 2 + side)


def _required_mask(wing: int) -> int:
    return (
        _leg_bit(1, "CALL")
        | _leg_bit(-1, "PUT")
        | _leg_bit(wing, "CALL")
        | _leg_bit(-wing, "PUT")
    )


def _records(connection: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    frame = connection.execute(sql).fetchdf()
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def _enrich_pooled(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    denominator = int(row["theoretical_windows"])
    for source, output in (
        ("entry_present_windows", "entry_present_pct"),
        ("entry_eligible_windows", "entry_eligible_pct"),
        ("exact_endpoint_windows", "exact_endpoint_pct"),
        ("strict_path_windows", "strict_path_pct"),
        ("stale_1m_windows", "stale_1m_pct"),
        ("stale_2m_windows", "stale_2m_pct"),
        ("stale_5m_windows", "stale_5m_pct"),
        ("stale_10m_windows", "stale_10m_pct"),
    ):
        result[output] = _pct(int(row[source]), denominator)

    present = int(row["entry_present_windows"])
    eligible = int(row["entry_eligible_windows"])
    exact = int(row["exact_endpoint_windows"])
    stale_5m = int(row["stale_5m_windows"])
    stale_10m = int(row["stale_10m_windows"])
    result["attribution"] = {
        "entry_missing_windows": denominator - present,
        "entry_quality_excluded_windows": present - eligible,
        "exact_endpoint_missing_after_eligible_entry": eligible - exact,
        "recovered_by_stale_5m": stale_5m - exact,
        "additional_recovered_by_stale_10m": stale_10m - stale_5m,
        "proxy_required_after_stale_10m": eligible - stale_10m,
        "proxy_required_pct_of_all_windows": _pct(eligible - stale_10m, denominator),
    }
    return result


def _enrich_start_time(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    denominator = int(row["theoretical_windows"])
    for source, output in (
        ("entry_eligible_windows", "entry_eligible_pct"),
        ("exact_endpoint_windows", "exact_endpoint_pct"),
        ("strict_path_windows", "strict_path_pct"),
        ("stale_5m_windows", "stale_5m_pct"),
        ("stale_10m_windows", "stale_10m_pct"),
    ):
        result[output] = _pct(int(row[source]), denominator)
    return result


def _summarize_clock_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for wing in ALL_REPORT_WINGS:
        for horizon in HORIZONS:
            selected = [
                row
                for row in rows
                if row["wing_offset"] == wing
                and row["horizon_minutes"] == horizon
                and row["theoretical_windows"] >= 1300
            ]
            summaries.append(
                {
                    "wing_offset": wing,
                    "horizon_minutes": horizon,
                    "high_support_definition": "at least 1,300 date observations",
                    "high_support_clock_buckets": len(selected),
                    "entry_eligible_ge_99pct_buckets": sum(
                        row["entry_eligible_pct"] >= 99 for row in selected
                    ),
                    "exact_ge_99pct_buckets": sum(
                        row["exact_endpoint_pct"] >= 99 for row in selected
                    ),
                    "path_ge_99pct_buckets": sum(
                        row["strict_path_pct"] >= 99 for row in selected
                    ),
                    "stale_5m_ge_99pct_buckets": sum(
                        row["stale_5m_pct"] >= 99 for row in selected
                    ),
                    "stale_10m_ge_99pct_buckets": sum(
                        row["stale_10m_pct"] >= 99 for row in selected
                    ),
                    "worst_exact_pct": min(
                        (row["exact_endpoint_pct"] for row in selected), default=None
                    ),
                    "worst_exact_entry_times": [
                        row["entry_time"]
                        for row in selected
                        if row["exact_endpoint_pct"]
                        == min(
                            (item["exact_endpoint_pct"] for item in selected),
                            default=None,
                        )
                    ],
                }
            )
    return summaries


def _pooled_boundaries(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    metrics = (
        "entry_eligible_pct",
        "exact_endpoint_pct",
        "strict_path_pct",
        "stale_5m_pct",
        "stale_10m_pct",
    )
    output: list[dict[str, Any]] = []
    for wing in ALL_REPORT_WINGS:
        selected = [row for row in rows if row["wing_offset"] == wing]
        entry: dict[str, Any] = {"wing_offset": wing}
        for metric in metrics:
            qualifying = [
                int(row["horizon_minutes"])
                for row in selected
                if row[metric] is not None and row[metric] >= 99.0
            ]
            entry[f"{metric}_horizons"] = qualifying
            entry[f"{metric}_max_horizon"] = max(qualifying, default=None)
        output.append(entry)
    return output


def _migration_tail_from_gold(gold_root: Path) -> list[dict[str, Any]]:
    parquet_glob = str(gold_root / "**" / "*.parquet").replace("\\", "/")
    with tempfile.TemporaryDirectory(prefix="nifty_wing_tail_duckdb_") as temp_dir:
        connection = duckdb.connect(str(Path(temp_dir) / "tail.duckdb"))
        connection.execute("PRAGMA threads=8")
        connection.execute("PRAGMA memory_limit='12GB'")
        sql_temp = temp_dir.replace("\\", "/")
        connection.execute(f"PRAGMA temp_directory='{sql_temp}'")
        source = f"read_parquet('{parquet_glob}', hive_partitioning=true)"

        connection.execute(
            f"""
            CREATE TABLE source_dates AS
            SELECT
              trade_date,
              mode(actual_expiry_date) FILTER (
                WHERE expiry_flag = 'WEEK' AND actual_expiry_date IS NOT NULL
              ) AS actual_expiry_date,
              min(timestamp_ist) FILTER (WHERE expiry_flag = 'WEEK')
                AS observed_session_start,
              max(timestamp_ist) FILTER (WHERE expiry_flag = 'WEEK')
                AS observed_session_end
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
            WHERE expiry_flag = 'WEEK'
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
              try_cast(source.computed_moneyness_offset AS INTEGER) AS entry_offset,
              source.close,
              source.independent_nifty_spot AS spot,
              source.recomputed_atm_strike AS atm_strike,
              source.strike_ladder_valid,
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
              AND source.actual_expiry_date IS NOT NULL
              AND try_cast(source.computed_moneyness_offset AS INTEGER)
                BETWEEN -10 AND 10
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
            """
            CREATE TABLE entry_surface AS
            SELECT
              *,
              (
                close IS NOT NULL
                AND close >= 0
                AND strike_ladder_valid
                AND NOT quality_severe_anomaly
                AND NOT proven_severe_payload_corruption
              ) AS entry_quality_eligible,
              cast(1 AS UBIGINT) << (
                (entry_offset + 10) * 2
                + CASE WHEN option_type = 'CALL' THEN 0 ELSE 1 END
              ) AS leg_bit
            FROM surface_raw
            WHERE
              NOT duplicate_identity
              AND (
                (option_type = 'CALL' AND entry_offset IN (1, 5, 7, 9))
                OR (option_type = 'PUT' AND entry_offset IN (-1, -5, -7, -9))
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
            FROM surface_raw
            WHERE spot IS NOT NULL
            GROUP BY 1, 2
            """
        )
        connection.execute(
            "CREATE TABLE structures(wing_offset INTEGER, required_mask UBIGINT)"
        )
        connection.executemany(
            "INSERT INTO structures VALUES (?, ?)",
            [(wing, _required_mask(wing)) for wing in TARGET_WINGS],
        )

        output: list[dict[str, Any]] = []
        for horizon in HORIZONS:
            connection.execute("DROP TABLE IF EXISTS grid")
            connection.execute("DROP TABLE IF EXISTS leg_status")
            connection.execute("DROP TABLE IF EXISTS masks")
            connection.execute("DROP TABLE IF EXISTS universe")
            connection.execute("DROP TABLE IF EXISTS structure_status")
            connection.execute(
                f"""
                CREATE TABLE grid AS
                SELECT
                  date.trade_date,
                  date.actual_expiry_date,
                  series.entry_timestamp
                FROM source_dates AS date
                CROSS JOIN generate_series(
                  date.observed_session_start,
                  date.observed_session_end - INTERVAL {horizon} MINUTE,
                  INTERVAL 1 MINUTE
                ) AS series(entry_timestamp)
                WHERE
                  date.observed_session_start IS NOT NULL
                  AND date.observed_session_end
                    >= date.observed_session_start + INTERVAL {horizon} MINUTE
                """
            )
            connection.execute(
                f"""
                CREATE TABLE leg_status AS
                SELECT
                  entry.trade_date,
                  entry.timestamp_ist AS entry_timestamp,
                  entry.actual_expiry_date,
                  entry.leg_bit,
                  entry.entry_quality_eligible,
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
                WHERE entry.timestamp_ist <= (
                  SELECT observed_session_end
                  FROM source_dates
                  WHERE trade_date = entry.trade_date
                ) - INTERVAL {horizon} MINUTE
                """
            )
            connection.execute(
                """
                CREATE TABLE masks AS
                SELECT
                  trade_date,
                  entry_timestamp,
                  actual_expiry_date,
                  bit_or(leg_bit) FILTER (
                    WHERE entry_quality_eligible
                  ) AS eligible_mask,
                  bit_or(leg_bit) FILTER (
                    WHERE entry_quality_eligible AND exact_endpoint_available
                  ) AS exact_mask,
                  bit_or(leg_bit) FILTER (
                    WHERE entry_quality_eligible AND staleness_minutes <= 5
                  ) AS stale_5m_mask,
                  bit_or(leg_bit) FILTER (
                    WHERE entry_quality_eligible AND staleness_minutes <= 10
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
                  coalesce(mask.eligible_mask, 0::UBIGINT) AS eligible_mask,
                  coalesce(mask.exact_mask, 0::UBIGINT) AS exact_mask,
                  coalesce(mask.stale_5m_mask, 0::UBIGINT) AS stale_5m_mask,
                  coalesce(mask.stale_10m_mask, 0::UBIGINT) AS stale_10m_mask,
                  abs(target_spot.atm_strike - entry_spot.atm_strike) / 50.0
                    AS endpoint_atm_shift_steps
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
                  structure.wing_offset,
                  (eligible_mask & required_mask) = required_mask AS entry_eligible,
                  (exact_mask & required_mask) = required_mask AS exact_complete,
                  (stale_5m_mask & required_mask) = required_mask AS stale_5m_complete,
                  (stale_10m_mask & required_mask) = required_mask
                    AS stale_10m_complete
                FROM universe
                CROSS JOIN structures AS structure
                """
            )
            output.extend(
                _records(
                    connection,
                    f"""
                    SELECT
                      {horizon} AS horizon_minutes,
                      wing_offset,
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
                      count(*) FILTER (WHERE stale_5m_complete) AS stale_5m_windows,
                      count(*) FILTER (WHERE stale_10m_complete) AS stale_10m_windows
                    FROM structure_status
                    WHERE endpoint_atm_shift_steps IS NOT NULL
                    GROUP BY 1, 2, 3
                    ORDER BY 1, 2, 3
                    """,
                )
            )
            print(f"completed migration-tail horizon {horizon}", flush=True)
        return output


def build(
    primary_path: Path,
    start_time_path: Path,
    gold_root: Path,
) -> dict[str, Any]:
    primary = json.loads(primary_path.read_text(encoding="utf-8"))
    contract = primary["contract"]
    if contract["session_mode"] != "observed":
        raise ValueError("primary artifact must use observed sessions")
    if contract["entry_offset_source"] != "computed":
        raise ValueError("primary artifact must use computed entry moneyness")

    pooled = [
        _enrich_pooled(row)
        for row in primary["structure_matrix"]
        if row["family"] == "iron_condor"
        and row["short_offset"] == 1
        and row["wing_offset"] in ALL_REPORT_WINGS
    ]
    daily = [
        row
        for row in primary["structure_day_matrix"]
        if row["family"] == "iron_condor"
        and row["short_offset"] == 1
        and row["wing_offset"] in ALL_REPORT_WINGS
    ]
    start_connection = duckdb.connect()
    try:
        start_rows = _records(
            start_connection,
            f"""
            SELECT *
            FROM read_parquet('{str(start_time_path).replace(chr(92), '/')}')
            WHERE
              family = 'iron_condor'
              AND short_offset = 1
              AND wing_offset IN (3, 5, 7, 9)
            ORDER BY wing_offset, horizon_minutes, entry_time
            """,
        )
    finally:
        start_connection.close()
    enriched_start = [_enrich_start_time(row) for row in start_rows]

    target_tail = _migration_tail_from_gold(gold_root)
    reference_tail = [
        {"wing_offset": 3, **row} for row in primary["key_structure_atm_shift"]
    ]
    migration_tail = reference_tail + target_tail

    pooled_lookup = {
        (row["horizon_minutes"], row["wing_offset"]): row for row in pooled
    }
    for row in target_tail:
        pooled_row = pooled_lookup[(row["horizon_minutes"], row["wing_offset"])]
        bucket_rows = [
            item
            for item in target_tail
            if item["horizon_minutes"] == row["horizon_minutes"]
            and item["wing_offset"] == row["wing_offset"]
        ]
        if sum(item["exact_windows"] for item in bucket_rows) != int(
            pooled_row["exact_endpoint_windows"]
        ):
            raise RuntimeError(
                "migration-tail exact total does not reproduce pooled total for "
                f"h={row['horizon_minutes']}, wing={row['wing_offset']}"
            )

    for row in migration_tail:
        denominator = int(row["theoretical_windows"])
        row["eligible_pct"] = _pct(int(row["eligible_windows"]), denominator)
        row["exact_pct"] = _pct(int(row["exact_windows"]), denominator)
        row["stale_5m_pct"] = _pct(int(row["stale_5m_windows"]), denominator)
        row["stale_10m_pct"] = _pct(int(row["stale_10m_windows"]), denominator)

    return {
        "contract": {
            "source_contract": contract,
            "family": "iron_condor",
            "short_offsets": {"call": 1, "put": -1},
            "target_wing_offsets": list(TARGET_WINGS),
            "reference_wing_offset": 3,
            "denominator": "all feasible start minutes inside every observed date session envelope",
            "quality_and_tracking": "identical to phase2_unconditional_observed_computed.json",
            "high_support_clock_definition": "at least 1,300 date observations",
        },
        "source_artifacts": {
            "primary_json": str(primary_path.resolve()),
            "start_time_parquet": str(start_time_path.resolve()),
            "gold_root": str(gold_root.resolve()),
        },
        "source_dates": primary["source_dates"],
        "horizon_summaries": primary["horizon_summaries"],
        "pooled_metrics": pooled,
        "daily_metrics": daily,
        "start_time_metrics": enriched_start,
        "high_support_start_time_summary": _summarize_clock_rows(enriched_start),
        "pooled_ge_99_boundaries": _pooled_boundaries(pooled),
        "atm_migration_tail_buckets": sorted(
            migration_tail,
            key=lambda row: (
                row["wing_offset"],
                row["horizon_minutes"],
                str(row["endpoint_atm_shift_steps"]),
            ),
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--primary",
        type=Path,
        default=Path("audit/phase2_unconditional_observed_computed.json"),
    )
    parser.add_argument(
        "--start-time",
        type=Path,
        default=Path(
            "audit/phase2_unconditional_start_time_matrix_observed_computed.parquet"
        ),
    )
    parser.add_argument("--gold-root", type=Path, required=True)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("audit/phase2_unconditional_wings_5_7_9.json"),
    )
    args = parser.parse_args()
    result = build(args.primary, args.start_time, args.gold_root)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(args.output)


if __name__ == "__main__":
    main()
