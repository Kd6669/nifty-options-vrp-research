"""Create compact audit tables for the defined-risk VRP path analysis."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def summarize(master_path: Path, structure_path: Path, output_path: Path) -> dict[str, Any]:
    master = json.loads(master_path.read_text(encoding="utf-8"))
    connection = duckdb.connect()
    connection.execute("PRAGMA threads=8")

    invariants = connection.execute(
        """
        SELECT
          count(*) AS row_count,
          count(DISTINCT entry_id || '|' || horizon_minutes) AS identities,
          max(abs(short_iron_condor__pnl_points + long_iron_condor__pnl_points))
            AS condor_inverse_error,
          max(abs(short_iron_fly__pnl_points + long_iron_fly__pnl_points))
            AS fly_inverse_error,
          count(*) FILTER (
            WHERE short_iron_condor__max_loss <= 0
               OR short_iron_condor__max_profit <= 0
          ) AS invalid_condor_risk
        FROM read_parquet(?)
        """,
        [str(structure_path)],
    ).fetchdf()

    boundary_grid = connection.execute(
        """
        SELECT
          vrp_tail,
          vrp_direction,
          count(short_iron_condor__pnl_points) AS observations,
          median(atm_iv) AS median_atm_iv,
          median(trailing_rv_act365) AS median_trailing_rv,
          median(short_iron_condor__pnl_points) AS short_condor_median,
          avg(short_iron_condor__pnl_points) AS short_condor_mean,
          avg(cast(short_iron_condor__pnl_points > 0 AS INTEGER))
            AS short_condor_win_rate,
          median(long_iron_condor__pnl_points) AS long_condor_median,
          avg(long_iron_condor__pnl_points) AS long_condor_mean,
          avg(cast(long_iron_condor__pnl_points > 0 AS INTEGER))
            AS long_condor_win_rate,
          median(long_call_butterfly__pnl_points) AS butterfly_median,
          avg(long_call_butterfly__pnl_points) AS butterfly_mean
        FROM read_parquet(?)
        WHERE horizon_minutes = 60
          AND research_dte BETWEEN 0.5 AND 3.5
          AND entry_time BETWEEN '10:45' AND '11:45'
          AND vrp_tail IN ('lower_10', 'upper_10')
        GROUP BY 1, 2
        ORDER BY 1, 2
        """,
        [str(structure_path)],
    ).fetchdf()

    daily_tail = connection.execute(
        """
        WITH eligible AS (
          SELECT *, row_number() OVER (
            PARTITION BY trade_date, vrp_tail ORDER BY entry_ts
          ) AS rn
          FROM read_parquet(?)
          WHERE horizon_minutes = 60
            AND research_dte BETWEEN 0.5 AND 3.5
            AND entry_time BETWEEN '10:45' AND '11:45'
            AND vrp_tail IN ('lower_10', 'upper_10')
            AND short_iron_condor__pnl_points IS NOT NULL
        )
        SELECT
          vrp_tail,
          count(*) AS observations,
          median(atm_iv) AS median_atm_iv,
          median(trailing_rv_act365) AS median_trailing_rv,
          median(short_iron_condor__pnl_points) AS short_condor_median,
          avg(short_iron_condor__pnl_points) AS short_condor_mean,
          avg(cast(short_iron_condor__pnl_points > 0 AS INTEGER))
            AS short_condor_win_rate,
          quantile_cont(short_iron_condor__pnl_points, 0.05) AS short_condor_p05,
          quantile_cont(short_iron_condor__pnl_points, 0.95) AS short_condor_p95,
          median(long_iron_condor__pnl_points) AS long_condor_median,
          avg(long_iron_condor__pnl_points) AS long_condor_mean,
          avg(cast(long_iron_condor__pnl_points > 0 AS INTEGER))
            AS long_condor_win_rate
        FROM eligible
        WHERE rn = 1
        GROUP BY 1
        ORDER BY 1
        """,
        [str(structure_path)],
    ).fetchdf()

    daily_crossings = connection.execute(
        """
        WITH eligible AS (
          SELECT *, row_number() OVER (
            PARTITION BY trade_date, vrp_crossing ORDER BY entry_ts
          ) AS rn
          FROM read_parquet(?)
          WHERE horizon_minutes = 60
            AND vrp_crossing IN ('cross_up', 'cross_down')
            AND short_iron_condor__pnl_points IS NOT NULL
        )
        SELECT
          vrp_crossing,
          count(*) AS observations,
          median(atm_iv) AS median_atm_iv,
          median(trailing_rv_act365) AS median_trailing_rv,
          median(atm_iv_change) AS median_atm_iv_change,
          median(abs(spot_return)) AS median_abs_spot_return,
          median(short_iron_condor__pnl_points) AS short_condor_median,
          avg(short_iron_condor__pnl_points) AS short_condor_mean,
          avg(cast(short_iron_condor__pnl_points > 0 AS INTEGER))
            AS short_condor_win_rate,
          quantile_cont(short_iron_condor__pnl_points, 0.05) AS short_condor_p05,
          quantile_cont(short_iron_condor__pnl_points, 0.95) AS short_condor_p95,
          median(long_iron_condor__pnl_points) AS long_condor_median,
          avg(long_iron_condor__pnl_points) AS long_condor_mean,
          avg(cast(long_iron_condor__pnl_points > 0 AS INTEGER))
            AS long_condor_win_rate
        FROM eligible
        WHERE rn = 1
        GROUP BY 1
        ORDER BY 1
        """,
        [str(structure_path)],
    ).fetchdf()

    annual_atm = connection.execute(
        """
        SELECT
          year(cast(trade_date AS DATE)) AS year,
          count(atm_iv) AS observations,
          quantile_cont(atm_iv, 0.05) AS atm_iv_p05,
          median(atm_iv) AS atm_iv_median,
          quantile_cont(atm_iv, 0.95) AS atm_iv_p95,
          median(trailing_rv_act365) AS trailing_rv_median,
          median(signal_vrp_var_act365) AS vrp_median,
          avg(cast(signal_vrp_var_act365 > 0 AS INTEGER)) AS positive_vrp_rate
        FROM read_parquet(?)
        WHERE horizon_minutes = 60
        GROUP BY 1
        ORDER BY 1
        """,
        [str(structure_path)],
    ).fetchdf()

    fixed_leg = connection.execute(
        """
        SELECT
          'tail' AS dimension,
          vrp_tail AS state,
          count(*) AS observations,
          median(atm_iv_change) AS rolling_atm_iv_change,
          median((contract_iv_change_p_m1 + contract_iv_change_c_p1) / 2)
            AS inner_fixed_iv_change,
          median((contract_iv_change_p_m3 + contract_iv_change_c_p3) / 2)
            AS wing_fixed_iv_change,
          median(short_iron_condor__pnl_points) AS short_condor_median
        FROM read_parquet(?)
        WHERE horizon_minutes = 60
          AND vrp_tail IN ('lower_10', 'upper_10')
        GROUP BY 1, 2
        UNION ALL
        SELECT
          'crossing',
          vrp_crossing,
          count(*),
          median(atm_iv_change),
          median((contract_iv_change_p_m1 + contract_iv_change_c_p1) / 2),
          median((contract_iv_change_p_m3 + contract_iv_change_c_p3) / 2),
          median(short_iron_condor__pnl_points)
        FROM read_parquet(?)
        WHERE horizon_minutes = 60
          AND vrp_crossing IN ('cross_up', 'cross_down')
        GROUP BY 1, 2
        ORDER BY 1, 2
        """,
        [str(structure_path), str(structure_path)],
    ).fetchdf()
    connection.close()

    summary = pd.DataFrame(master["structure_state_summary"])
    selected = summary[
        (summary["horizon_minutes"] == 60)
        & (
            (
                summary["dimension"].eq("vrp_crossing")
                & summary["state"].isin(["cross_up", "cross_down"])
            )
            | (
                summary["dimension"].eq("tail_x_direction")
                & summary["state"].isin(
                    [
                        "lower_10__decreasing",
                        "lower_10__increasing",
                        "upper_10__decreasing",
                        "upper_10__increasing",
                    ]
                )
            )
        )
        & summary["structure"].isin(
            [
                "short_iron_condor",
                "long_iron_condor",
                "short_iron_fly",
                "long_iron_fly",
            ]
        )
    ]

    horizon_tail = summary[
        summary["dimension"].eq("vrp_tail")
        & summary["state"].isin(["lower_10", "upper_10"])
        & summary["structure"].isin(["short_iron_condor", "long_iron_condor"])
    ]

    result = {
        "contract": master["contract"],
        "invariants": _records(invariants)[0],
        "boundary_60m_minute_grid": _records(boundary_grid),
        "boundary_60m_first_tail_event_per_day": _records(daily_tail),
        "first_crossing_event_per_day": _records(daily_crossings),
        "annual_atm_iv_rv_vrp": _records(annual_atm),
        "fixed_leg_iv_by_state": _records(fixed_leg),
        "selected_60m_structure_states": _records(selected),
        "tail_horizon_path": _records(horizon_tail),
        "local_chain_iv": master["local_chain_iv"],
        "structure_coverage": master["structure_coverage"],
    }
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--master",
        type=Path,
        default=Path("audit/phase2_defined_risk_vrp.json"),
    )
    parser.add_argument(
        "--structures",
        type=Path,
        default=Path("audit/phase2_defined_risk_structure_paths.parquet"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("audit/phase2_defined_risk_vrp_events.json"),
    )
    args = parser.parse_args()
    result = summarize(args.master.resolve(), args.structures.resolve(), args.output.resolve())
    print(json.dumps(result["invariants"], indent=2))


if __name__ == "__main__":
    main()
