"""Create a deterministic, non-secret research-facing sample from BOD-SPAN gold."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pyarrow.parquet as pq


DEFAULT_DATES = ("2021-01-04", "2025-09-02", "2026-07-14")
DEFAULT_TIMES = ("09:30", "12:00", "15:00")
IST = ZoneInfo("Asia/Kolkata")
SAMPLE_COLUMNS = (
    "schema_version",
    "request_id",
    "provider",
    "timestamp_ist",
    "trade_date",
    "session_status",
    "underlying",
    "expiry_flag",
    "expiry_code",
    "moneyness_label",
    "strike",
    "option_type",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "open_interest",
    "provider_iv_raw",
    "provider_iv_unit",
    "provider_spot",
    "independent_nifty_spot",
    "nifty_spot_age_seconds",
    "nifty_spot_join_status",
    "india_vix",
    "india_vix_age_seconds",
    "india_vix_join_status",
    "actual_expiry_date",
    "actual_expiry_timestamp_ist",
    "expiry_type",
    "expiry_rule_weekday",
    "expiry_holiday_adjusted",
    "contract_lot_size",
    "tick_size",
    "mte",
    "dte",
    "t_years_act365",
    "quality_severe_anomaly",
    "quality_gate_status",
    "quality_gate_failure_reason",
    "bsm_gate_status",
    "bsm_status",
    "bsm_failure_reason",
    "bsm_solver_method",
    "bsm_iv_close",
    "bsm_price_reconstructed",
    "bsm_price_residual_abs",
    "bsm_delta",
    "bsm_gamma",
    "bsm_theta_per_day_365",
    "bsm_vega_per_100",
    "bsm_rho_per_100",
    "bsm_rate_cc",
    "bsm_dividend_yield",
    "bsm_model_version",
    "span_join_policy",
    "span_join_status",
    "span_unmatched_reason",
    "span_time_slot",
    "span_effective_time_source",
    "span_effective_ts_ist",
    "span_slot_publication_times_proven",
    "span_intraday_asof_join_performed",
    "span_price",
    "span_delta",
    "span_implied_vol",
    "span_price_scan_range",
    "span_vol_scan_range",
    "span_cvf",
    "span_s1",
    "span_s2",
    "span_s3",
    "span_s4",
    "span_s5",
    "span_s6",
    "span_s7",
    "span_s8",
    "span_s9",
    "span_s10",
    "span_s11",
    "span_s12",
    "span_s13",
    "span_s14",
    "span_s15",
    "span_s16",
    "span_composite_delta",
    "span_source_sha256",
    "span_release_manifest_sha256",
    "span_gold_lineage_sha256",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def create_sample(
    dataset_root: Path,
    output: Path,
    manifest_path: Path,
    *,
    dates: tuple[str, ...] = DEFAULT_DATES,
    times: tuple[str, ...] = DEFAULT_TIMES,
) -> dict[str, object]:
    dataset_root = dataset_root.resolve()
    glob = (dataset_root / "year=*" / "month=*" / "part-*.parquet").as_posix()
    if not list(dataset_root.glob("year=*/month=*/part-*.parquet")):
        raise FileNotFoundError(f"no gold Parquet files below {dataset_root}")

    output.parent.mkdir(parents=True, exist_ok=True)
    output.unlink(missing_ok=True)
    connection = duckdb.connect()
    selected_dates = ", ".join(f"DATE {_quote(value)}" for value in dates)
    selected_times = ", ".join(_quote(value) for value in times)
    columns = ",\n  ".join(SAMPLE_COLUMNS)
    output_sql = output.resolve().as_posix().replace("'", "''")
    connection.execute(
        f"""
        COPY (
          SELECT
            {columns}
          FROM read_parquet('{glob.replace("'", "''")}', hive_partitioning=true)
          WHERE trade_date IN ({selected_dates})
            AND strftime(timestamp_ist, '%H:%M') IN ({selected_times})
          ORDER BY trade_date, timestamp_ist, expiry_flag, expiry_code,
                   moneyness_label, option_type, strike
        ) TO '{output_sql}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 10000)
        """
    )
    connection.close()

    parquet = pq.ParquetFile(output)
    table = pq.read_table(output, columns=["trade_date", "timestamp_ist"])
    observed_dates = sorted({str(value) for value in table.column("trade_date").to_pylist()})
    observed_times = sorted(
        {
            value.astimezone(IST).strftime("%H:%M")
            for value in table.column("timestamp_ist").to_pylist()
        }
    )
    manifest: dict[str, object] = {
        "sample_schema": "nifty_options_gold_sample",
        "sample_schema_version": "1.0.0",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_release": "nifty_gold_span_bod_20210101_20260715/version=1.4.0",
        "source_code_commit": "516d31b",
        "selection": {
            "trade_dates": list(dates),
            "times_ist": list(times),
            "ordering": [
                "trade_date",
                "timestamp_ist",
                "expiry_flag",
                "expiry_code",
                "moneyness_label",
                "option_type",
                "strike",
            ],
        },
        "observed_trade_dates": observed_dates,
        "observed_times_ist": observed_times,
        "rows": parquet.metadata.num_rows,
        "columns": parquet.schema_arrow.names,
        "bytes": output.stat().st_size,
        "sha256": sha256_file(output),
        "contains_credentials": False,
        "span_timing_warning": (
            "BOD is a conservative static fallback; historical publication/effective time is unproven"
        ),
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--dates", default=",".join(DEFAULT_DATES))
    parser.add_argument("--times", default=",".join(DEFAULT_TIMES))
    args = parser.parse_args()
    manifest = create_sample(
        args.dataset_root,
        args.output,
        args.manifest,
        dates=tuple(value.strip() for value in args.dates.split(",") if value.strip()),
        times=tuple(value.strip() for value in args.times.split(",") if value.strip()),
    )
    print(json.dumps({"rows": manifest["rows"], "sha256": manifest["sha256"]}))


if __name__ == "__main__":
    main()
