from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import date
import json
import os
import sys

from .acquisition import (
    AcquisitionEngine,
    DhanTransport,
    feasibility_cells,
    fetch_current_instrument_snapshot,
    plan_current_futures,
    plan_india_vix,
    plan_rolling_options,
    plan_spot,
    rebuild_silver_from_bronze,
    redact_secret_text,
)

from .core import (
    DEFAULT_DHAN_REST_STREAM,
    DEFAULT_DHAN_TBT_STREAM,
    DEFAULT_DHAN_UNDERLYING_SCRIP,
    DEFAULT_DHAN_UNDERLYING_SEGMENT,
    DhanCredentials,
    DhanHistoricalClient,
    DhanOptionChainClient,
    capture_dhan_rest_option_chain_to_redis,
    capture_dhan_tbt_to_redis,
    export_redis_stream_to_parquet,
    fetch_dhan_intraday_full_chain,
)
from .pre_bsm_runner import run_pre_bsm_incremental
from .pre_bsm_duckdb import DuckDbPreBsmConfig, run_pre_bsm_duckdb
from .bsm_v2_runner import run_bsm_v2_root
from .bsm_vectorized import VectorizedBsmConfig
from .span_gold import SpanGoldConfig, run_span_gold
from .span_first_seen import poll_span_archive_first_seen
from .span_release import SpanReleaseConfig, run_span_release
from .span_release_verify import verify_span_release
from .span_timing_release import SpanTimingConfig, run_span_timing_release


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dhan-data")
    subparsers = parser.add_subparsers(dest="command", required=True)

    historical = subparsers.add_parser(
        "fetch-historical-full-chain",
        help="Fetch Dhan 1-minute historical intraday candles for the full active NIFTY option chain",
    )
    historical.add_argument("--date", required=True, help="Trading date YYYY-MM-DD")
    historical.add_argument("--index", default="NIFTY")
    historical.add_argument("--exchange", default="NSE")
    historical.add_argument("--exchange-segment", default="NSE_FNO")
    historical.add_argument("--instrument", default="OPTIDX")
    historical.add_argument("--expiry", required=True, help="Option expiry YYYY-MM-DD")
    historical.add_argument(
        "--interval", default="1", choices=("1", "5", "15", "25", "60")
    )
    historical.add_argument(
        "--from-date", help="Dhan fromDate, e.g. '2026-06-25 09:15:00'"
    )
    historical.add_argument(
        "--to-date",
        help="Dhan toDate; defaults to now IST for today or 15:30 for past dates",
    )
    historical.add_argument("--no-oi", action="store_true")
    historical.add_argument("--out-dir", required=True)
    historical.add_argument("--sleep-seconds", type=float, default=0.75)
    historical.add_argument("--timeout-seconds", type=float, default=15.0)
    historical.add_argument("--max-retries", type=int, default=4)
    historical.add_argument(
        "--limit", type=int, default=0, help="Debug limit for first N instruments"
    )
    historical.add_argument("--json", action="store_true")

    rest = subparsers.add_parser(
        "capture-rest-redis",
        help="Poll Dhan option-chain REST and publish packets into Redis plus optional Parquet parts",
    )
    rest.add_argument("--index", default="NIFTY")
    rest.add_argument("--exchange", default="NSE")
    rest.add_argument(
        "--expiry", help="YYYY-MM-DD expiry; defaults to nearest future Dhan expiry"
    )
    rest.add_argument(
        "--underlying-scrip", type=int, default=DEFAULT_DHAN_UNDERLYING_SCRIP
    )
    rest.add_argument("--underlying-segment", default=DEFAULT_DHAN_UNDERLYING_SEGMENT)
    rest.add_argument(
        "--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    )
    rest.add_argument("--redis-stream", default=DEFAULT_DHAN_REST_STREAM)
    rest.add_argument(
        "--iterations", type=int, default=0, help="0 means run until interrupted"
    )
    rest.add_argument("--interval-seconds", type=float, default=3.2)
    rest.add_argument("--timeout-seconds", type=float, default=10.0)
    rest.add_argument("--maxlen", type=int, default=1_000_000)
    rest.add_argument("--parquet-dir")
    rest.add_argument("--parquet-prefix", default="dhan_rest_poll_packets")
    rest.add_argument("--parquet-flush-rows", type=int, default=10_000)
    rest.add_argument("--json", action="store_true")

    tbt = subparsers.add_parser(
        "capture-tbt-redis",
        help="Subscribe to Dhan MarketFeed option packets and publish Redis ticks plus optional Parquet parts",
    )
    tbt.add_argument("--index", default="NIFTY")
    tbt.add_argument("--exchange", default="NSE")
    tbt.add_argument("--exchange-segment", default="NSE_FNO")
    tbt.add_argument("--expiry", required=True)
    tbt.add_argument(
        "--spot",
        type=float,
        help="Startup spot for ATM ring mode; not required for --full-chain",
    )
    tbt.add_argument(
        "--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    )
    tbt.add_argument("--redis-stream", default=DEFAULT_DHAN_TBT_STREAM)
    tbt.add_argument("--ring-width-steps", type=int, default=12)
    tbt.add_argument("--strike-step", type=float, default=50.0)
    tbt.add_argument("--full-chain", action="store_true")
    tbt.add_argument("--feed-mode", choices=("ticker", "quote", "full"), default="full")
    tbt.add_argument(
        "--iterations",
        type=int,
        default=0,
        help="0 means run until interrupted/no-update timeout",
    )
    tbt.add_argument("--max-no-update-seconds", type=float, default=300.0)
    tbt.add_argument("--startup-timeout-seconds", type=float, default=30.0)
    tbt.add_argument("--maxlen", type=int, default=1_000_000)
    tbt.add_argument("--parquet-dir")
    tbt.add_argument("--parquet-prefix", default="dhan_tbt_feed_packets")
    tbt.add_argument("--parquet-flush-rows", type=int, default=10_000)
    tbt.add_argument("--json", action="store_true")

    export = subparsers.add_parser(
        "export-redis-stream-parquet",
        help="Export existing Redis stream packets into Parquet part files",
    )
    export.add_argument(
        "--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    )
    export.add_argument("--redis-stream", required=True)
    export.add_argument("--out-dir", required=True)
    export.add_argument("--prefix", default="redis_packets")
    export.add_argument("--start-id", default="0-0")
    export.add_argument("--max-packets", type=int, default=0)
    export.add_argument("--block-ms", type=int, default=1_000)
    export.add_argument("--count", type=int, default=500)
    export.add_argument("--idle-timeout-seconds", type=float)
    export.add_argument("--flush-rows", type=int, default=1_000)
    export.add_argument("--json", action="store_true")

    probe = subparsers.add_parser(
        "probe-feasibility",
        help="Run bounded, redacted Dhan spot/options/current-futures feasibility probes",
    )
    _add_acquisition_runtime_args(probe)
    probe.add_argument("--recent-start", default="2026-07-14")
    probe.add_argument("--recent-end", default="2026-07-15")
    probe.add_argument(
        "--boundary-dates",
        default="2021-01-04,2021-07-14,2021-07-15,2021-07-16,2025-09-02",
    )
    probe.add_argument("--json", action="store_true")

    rolling = subparsers.add_parser(
        "backfill-rolling-options",
        help="Resume Dhan's historical rolling NIFTY moneyness surface (not an absolute-strike full chain)",
    )
    _add_acquisition_runtime_args(rolling)
    _add_date_range_args(rolling)
    rolling.add_argument(
        "--expiry-codes",
        required=True,
        help="Comma-separated codes proven by probe-feasibility; official Dhan references conflict",
    )
    rolling.add_argument("--expiry-flags", default="WEEK,MONTH")
    rolling.add_argument("--option-types", default="CALL,PUT")
    rolling.add_argument("--moneyness-width", type=int, default=10)
    rolling.add_argument("--pilot-limit", type=int, default=0)
    rolling.add_argument("--json", action="store_true")

    spot = subparsers.add_parser(
        "backfill-spot", help="Resume 1-minute NIFTY INDEX acquisition"
    )
    _add_acquisition_runtime_args(spot)
    _add_date_range_args(spot)
    spot.add_argument("--pilot-limit", type=int, default=0)
    spot.add_argument("--json", action="store_true")

    india_vix = subparsers.add_parser(
        "backfill-india-vix",
        help="Resume independent 1-minute INDIA VIX INDEX acquisition (Dhan security ID 21)",
    )
    _add_acquisition_runtime_args(india_vix)
    _add_date_range_args(india_vix)
    india_vix.add_argument("--pilot-limit", type=int, default=0)
    india_vix.add_argument("--json", action="store_true")

    futures = subparsers.add_parser(
        "backfill-current-futures",
        help="Fetch current near/next/far FUTIDX contracts only; does not infer expired futures",
    )
    _add_acquisition_runtime_args(futures)
    _add_date_range_args(futures)
    futures.add_argument("--pilot-limit", type=int, default=0)
    futures.add_argument("--json", action="store_true")

    rebuild = subparsers.add_parser(
        "rebuild-silver",
        help="Rebuild normalized Dhan Parquet from existing immutable bronze without network or credentials",
    )
    rebuild.add_argument("--root", default="data/dhan")
    rebuild.add_argument("--json", action="store_true")

    enrich = subparsers.add_parser(
        "pre-bsm-enrich",
        help="Incrementally materialize the mandatory spot/VIX/NSE-enriched option layer without BSM",
    )
    enrich.add_argument("--options-root", required=True)
    enrich.add_argument("--spot-root", required=True)
    enrich.add_argument("--vix-root", required=True)
    enrich.add_argument("--contract-rules", required=True)
    enrich.add_argument("--actual-expiries", required=True)
    enrich.add_argument("--output-root", required=True)
    enrich.add_argument("--pilot-files", type=int, default=0)
    enrich.add_argument("--acquisition-terminally-accounted", action="store_true")
    enrich.add_argument("--no-resume", action="store_true")
    enrich.add_argument("--json", action="store_true")

    enrich_v2 = subparsers.add_parser(
        "pre-bsm-enrich-v2",
        help="Bulk materialize monthly spot/VIX/NSE-enriched options with DuckDB; never runs BSM",
    )
    enrich_v2.add_argument("--options-root", required=True)
    enrich_v2.add_argument("--spot-root", required=True)
    enrich_v2.add_argument("--vix-root", required=True)
    enrich_v2.add_argument("--contract-rules", required=True)
    enrich_v2.add_argument("--actual-expiries", required=True)
    enrich_v2.add_argument("--output-root", required=True)
    enrich_v2.add_argument("--temp-directory", required=True)
    enrich_v2.add_argument(
        "--months", help="Optional comma-separated YYYY-MM pilot/resume scope"
    )
    enrich_v2.add_argument("--threads", type=int, default=6)
    enrich_v2.add_argument("--memory-limit", default="9GB")
    enrich_v2.add_argument("--row-group-size", type=int, default=250_000)
    enrich_v2.add_argument("--acquisition-terminally-accounted", action="store_true")
    enrich_v2.add_argument("--no-resume", action="store_true")
    enrich_v2.add_argument("--json", action="store_true")

    bsm_v2 = subparsers.add_parser(
        "bsm-v2",
        help="Vectorized independent IV/Greeks over audited monthly pre-BSM-v2 Parquets",
    )
    bsm_v2.add_argument("--input-root", required=True)
    bsm_v2.add_argument("--output-root", required=True)
    bsm_v2.add_argument(
        "--months", help="Optional comma-separated YYYY-MM pilot/resume scope"
    )
    bsm_v2.add_argument("--row-group-size", type=int, default=250_000)
    bsm_v2.add_argument("--max-newton-iterations", type=int, default=20)
    bsm_v2.add_argument("--max-brent-iterations", type=int, default=100)
    bsm_v2.add_argument("--json", action="store_true")

    span_gold = subparsers.add_parser(
        "span-gold",
        help=(
            "Offline, month-resumable BOD SPAN enrichment of the audited BSM dataset; "
            "unknown intraday/EOD effective times are never guessed"
        ),
    )
    span_gold.add_argument("--bsm-root", required=True)
    span_gold.add_argument("--bsm-terminal-audit", required=True)
    span_gold.add_argument("--span-compacted-root", required=True)
    span_gold.add_argument("--span-completion", required=True)
    span_gold.add_argument("--span-matrix", required=True)
    span_gold.add_argument("--output-root", required=True)
    span_gold.add_argument(
        "--months", help="Optional comma-separated YYYY-MM pilot/resume scope"
    )
    span_gold.add_argument("--threads", type=int, default=8)
    span_gold.add_argument("--memory-limit", default="8GB")
    span_gold.add_argument("--row-group-size", type=int, default=250_000)
    span_gold.add_argument("--no-resume", action="store_true")
    span_gold.add_argument("--json", action="store_true")

    span_release = subparsers.add_parser(
        "span-release",
        help=(
            "Final hash-pinned BOD v1.4 and static six-slot v2.0 SPAN research "
            "representations; no publication time is inferred"
        ),
    )
    span_release.add_argument("--bsm-root", required=True)
    span_release.add_argument("--bsm-terminal-audit", required=True)
    span_release.add_argument("--span-compacted-root", required=True)
    span_release.add_argument("--span-release-manifest", required=True)
    span_release.add_argument("--span-handoff", required=True)
    span_release.add_argument("--span-source-gap-manifest", required=True)
    span_release.add_argument("--bod-output-root", required=True)
    span_release.add_argument("--six-slot-output-root", required=True)
    span_release.add_argument(
        "--months", help="Optional comma-separated YYYY-MM pilot/resume scope"
    )
    span_release.add_argument("--threads", type=int, default=8)
    span_release.add_argument("--memory-limit", default="8GB")
    span_release.add_argument("--row-group-size", type=int, default=250_000)
    span_release.add_argument("--no-resume", action="store_true")
    span_release.add_argument("--json", action="store_true")

    span_timing = subparsers.add_parser(
        "span-timing-release",
        help=(
            "Publish strict proven-timestamp as-of and six-slot reference-only "
            "research representations from accepted six-slot v2.0"
        ),
    )
    span_timing.add_argument("--base-six-slot-root", required=True)
    span_timing.add_argument("--strict-output-root", required=True)
    span_timing.add_argument("--research-output-root", required=True)
    span_timing.add_argument("--official-timing-document", required=True)
    span_timing.add_argument("--first-seen-manifest")
    span_timing.add_argument(
        "--months", help="Optional comma-separated YYYY-MM pilot/resume scope"
    )
    span_timing.add_argument("--threads", type=int, default=8)
    span_timing.add_argument("--memory-limit", default="8GB")
    span_timing.add_argument("--row-group-size", type=int, default=250_000)
    span_timing.add_argument("--no-resume", action="store_true")
    span_timing.add_argument("--json", action="store_true")

    span_first_seen = subparsers.add_parser(
        "span-first-seen",
        help="Bounded poller that persists first successful observation of an exact NSE archive SHA",
    )
    span_first_seen.add_argument("--url", required=True)
    span_first_seen.add_argument("--trading-date", required=True)
    span_first_seen.add_argument("--time-slot", required=True)
    span_first_seen.add_argument("--manifest", required=True)
    span_first_seen.add_argument("--archive-dir")
    span_first_seen.add_argument("--poll-seconds", type=float, default=30.0)
    span_first_seen.add_argument("--max-attempts", type=int, default=20)
    span_first_seen.add_argument("--timeout-seconds", type=float, default=20.0)
    span_first_seen.add_argument("--json", action="store_true")

    span_verify = subparsers.add_parser(
        "span-release-verify",
        help="Independently re-hash final SPAN month manifests/Parquets and metadata",
    )
    span_verify.add_argument("--bod-root", required=True)
    span_verify.add_argument("--six-slot-root", required=True)
    span_verify.add_argument("--expected-months", type=int, default=67)
    span_verify.add_argument("--expected-rows", type=int, default=43_018_677)
    span_verify.add_argument("--json", action="store_true")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "fetch-historical-full-chain":
            stats = fetch_dhan_intraday_full_chain(
                client=DhanHistoricalClient.from_env(),
                output_dir=args.out_dir,
                trading_date=args.date,
                index=args.index,
                expiry=args.expiry,
                exchange=args.exchange,
                exchange_segment=args.exchange_segment,
                instrument=args.instrument,
                interval=args.interval,
                oi=not args.no_oi,
                from_date=args.from_date,
                to_date=args.to_date,
                sleep_seconds=args.sleep_seconds,
                timeout_seconds=args.timeout_seconds,
                max_retries=args.max_retries,
                limit=args.limit,
            )
            return _print_stats(stats, json_output=args.json)
        if args.command == "capture-rest-redis":
            stats = capture_dhan_rest_option_chain_to_redis(
                client=DhanOptionChainClient.from_env(),
                redis_url=args.redis_url,
                redis_stream=args.redis_stream,
                index=args.index,
                expiry=args.expiry,
                exchange=args.exchange,
                underlying_scrip=args.underlying_scrip,
                underlying_segment=args.underlying_segment,
                interval_seconds=args.interval_seconds,
                iterations=args.iterations,
                maxlen=args.maxlen,
                parquet_dir=args.parquet_dir,
                parquet_prefix=args.parquet_prefix,
                parquet_flush_rows=args.parquet_flush_rows,
                timeout_seconds=args.timeout_seconds,
            )
            return _print_stats(stats, json_output=args.json)
        if args.command == "capture-tbt-redis":
            stats = capture_dhan_tbt_to_redis(
                credentials=DhanCredentials.from_env(),
                redis_url=args.redis_url,
                redis_stream=args.redis_stream,
                index=args.index,
                expiry=args.expiry,
                exchange=args.exchange,
                exchange_segment=args.exchange_segment,
                spot=args.spot,
                ring_width_steps=args.ring_width_steps,
                strike_step=args.strike_step,
                full_chain=args.full_chain,
                feed_mode=args.feed_mode,
                iterations=args.iterations,
                max_no_update_seconds=args.max_no_update_seconds,
                startup_timeout_seconds=args.startup_timeout_seconds,
                maxlen=args.maxlen,
                parquet_dir=args.parquet_dir,
                parquet_prefix=args.parquet_prefix,
                parquet_flush_rows=args.parquet_flush_rows,
            )
            return _print_stats(stats, json_output=args.json)
        if args.command == "export-redis-stream-parquet":
            stats = export_redis_stream_to_parquet(
                redis_url=args.redis_url,
                stream=args.redis_stream,
                output_dir=args.out_dir,
                prefix=args.prefix,
                start_id=args.start_id,
                max_packets=args.max_packets,
                block_ms=args.block_ms,
                count=args.count,
                idle_timeout_seconds=args.idle_timeout_seconds,
                flush_rows=args.flush_rows,
            )
            return _print_stats(stats, json_output=args.json)
        if args.command == "probe-feasibility":
            recent_start = date.fromisoformat(args.recent_start)
            recent_end = date.fromisoformat(args.recent_end)
            boundaries = tuple(
                date.fromisoformat(item) for item in _csv_values(args.boundary_dates)
            )
            cells = feasibility_cells(
                recent_start=recent_start,
                recent_end=recent_end,
                boundary_dates=boundaries,
            )
            snapshot = fetch_current_instrument_snapshot()
            cells.extend(plan_current_futures(recent_start, recent_end, snapshot))
            outcomes = _acquisition_engine(args).run(
                cells,
                resume=not args.no_resume,
                stop_on_credential_blocked=False,
            )
            return _print_outcomes(outcomes, json_output=args.json)
        if args.command == "backfill-rolling-options":
            cells = plan_rolling_options(
                start_date=date.fromisoformat(args.start_date),
                end_date=date.fromisoformat(args.end_date),
                expiry_flags=_csv_values(args.expiry_flags),
                expiry_codes=tuple(
                    int(item) for item in _csv_values(args.expiry_codes)
                ),
                moneyness_width=args.moneyness_width,
                option_types=_csv_values(args.option_types),
            )
            if args.pilot_limit > 0:
                cells = cells[: args.pilot_limit]
            return _print_outcomes(
                _acquisition_engine(args).run(cells, resume=not args.no_resume),
                json_output=args.json,
            )
        if args.command == "backfill-spot":
            cells = plan_spot(
                date.fromisoformat(args.start_date), date.fromisoformat(args.end_date)
            )
            if args.pilot_limit > 0:
                cells = cells[: args.pilot_limit]
            return _print_outcomes(
                _acquisition_engine(args).run(cells, resume=not args.no_resume),
                json_output=args.json,
            )
        if args.command == "backfill-india-vix":
            cells = plan_india_vix(
                date.fromisoformat(args.start_date), date.fromisoformat(args.end_date)
            )
            if args.pilot_limit > 0:
                cells = cells[: args.pilot_limit]
            return _print_outcomes(
                _acquisition_engine(args).run(cells, resume=not args.no_resume),
                json_output=args.json,
            )
        if args.command == "backfill-current-futures":
            snapshot = fetch_current_instrument_snapshot()
            cells = plan_current_futures(
                date.fromisoformat(args.start_date),
                date.fromisoformat(args.end_date),
                snapshot,
            )
            if args.pilot_limit > 0:
                cells = cells[: args.pilot_limit]
            return _print_outcomes(
                _acquisition_engine(args).run(cells, resume=not args.no_resume),
                json_output=args.json,
            )
        if args.command == "rebuild-silver":
            return _print_stats(
                rebuild_silver_from_bronze(args.root), json_output=args.json
            )
        if args.command == "pre-bsm-enrich":
            stats = run_pre_bsm_incremental(
                options_root=args.options_root,
                spot_root=args.spot_root,
                vix_root=args.vix_root,
                contract_rules=args.contract_rules,
                actual_expiries=args.actual_expiries,
                output_root=args.output_root,
                pilot_files=args.pilot_files,
                acquisition_terminally_accounted=args.acquisition_terminally_accounted,
                resume=not args.no_resume,
            )
            return _print_stats(stats, json_output=args.json)
        if args.command == "pre-bsm-enrich-v2":
            stats = run_pre_bsm_duckdb(
                options_root=args.options_root,
                spot_root=args.spot_root,
                vix_root=args.vix_root,
                contract_rules=args.contract_rules,
                actual_expiries=args.actual_expiries,
                output_root=args.output_root,
                temp_directory=args.temp_directory,
                config=DuckDbPreBsmConfig(
                    threads=args.threads,
                    memory_limit=args.memory_limit,
                    row_group_size=args.row_group_size,
                    acquisition_terminally_accounted=args.acquisition_terminally_accounted,
                ),
                months=tuple(
                    part.strip() for part in args.months.split(",") if part.strip()
                )
                if args.months
                else None,
                resume=not args.no_resume,
            )
            return _print_stats(stats, json_output=args.json)
        if args.command == "bsm-v2":
            stats = run_bsm_v2_root(
                args.input_root,
                args.output_root,
                months=tuple(
                    part.strip() for part in args.months.split(",") if part.strip()
                )
                if args.months
                else None,
                config=VectorizedBsmConfig(
                    max_newton_iterations=args.max_newton_iterations,
                    max_brent_iterations=args.max_brent_iterations,
                ),
                row_group_size=args.row_group_size,
            )
            return _print_stats(stats, json_output=args.json)
        if args.command == "span-gold":
            stats = run_span_gold(
                bsm_root=args.bsm_root,
                bsm_terminal_audit=args.bsm_terminal_audit,
                span_compacted_root=args.span_compacted_root,
                span_completion=args.span_completion,
                span_matrix=args.span_matrix,
                output_root=args.output_root,
                months=tuple(
                    part.strip() for part in args.months.split(",") if part.strip()
                )
                if args.months
                else None,
                config=SpanGoldConfig(
                    threads=args.threads,
                    memory_limit=args.memory_limit,
                    row_group_size=args.row_group_size,
                ),
                resume=not args.no_resume,
            )
            return _print_stats(stats, json_output=args.json)
        if args.command == "span-release":
            stats = run_span_release(
                bsm_root=args.bsm_root,
                bsm_terminal_audit=args.bsm_terminal_audit,
                span_compacted_root=args.span_compacted_root,
                span_release_manifest=args.span_release_manifest,
                span_handoff=args.span_handoff,
                span_source_gap_manifest=args.span_source_gap_manifest,
                bod_output_root=args.bod_output_root,
                six_slot_output_root=args.six_slot_output_root,
                months=tuple(
                    part.strip() for part in args.months.split(",") if part.strip()
                )
                if args.months
                else None,
                config=SpanReleaseConfig(
                    threads=args.threads,
                    memory_limit=args.memory_limit,
                    row_group_size=args.row_group_size,
                ),
                resume=not args.no_resume,
            )
            return _print_stats(stats, json_output=args.json)
        if args.command == "span-timing-release":
            stats = run_span_timing_release(
                base_six_slot_root=args.base_six_slot_root,
                strict_output_root=args.strict_output_root,
                research_output_root=args.research_output_root,
                official_timing_document=args.official_timing_document,
                first_seen_manifest=args.first_seen_manifest,
                months=tuple(
                    part.strip() for part in args.months.split(",") if part.strip()
                )
                if args.months
                else None,
                config=SpanTimingConfig(
                    threads=args.threads,
                    memory_limit=args.memory_limit,
                    row_group_size=args.row_group_size,
                ),
                resume=not args.no_resume,
            )
            return _print_stats(stats, json_output=args.json)
        if args.command == "span-first-seen":
            stats = poll_span_archive_first_seen(
                url=args.url,
                trading_date=args.trading_date,
                time_slot=args.time_slot,
                manifest_path=args.manifest,
                archive_dir=args.archive_dir,
                poll_seconds=args.poll_seconds,
                max_attempts=args.max_attempts,
                timeout_seconds=args.timeout_seconds,
            )
            return _print_stats(stats, json_output=args.json)
        if args.command == "span-release-verify":
            stats = verify_span_release(
                bod_root=args.bod_root,
                six_slot_root=args.six_slot_root,
                expected_months=args.expected_months,
                expected_rows=args.expected_rows,
            )
            return _print_stats(stats, json_output=args.json)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(
            "error: "
            + redact_secret_text(
                str(exc),
                (
                    os.environ.get("DHAN_ACCESS_TOKEN", ""),
                    os.environ.get("DHAN_TOKEN", ""),
                ),
            ),
            file=sys.stderr,
        )
        return 1
    return 2


def _print_stats(stats: object, *, json_output: bool) -> int:
    payload = asdict(stats)
    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(json.dumps(payload, sort_keys=True))
    return 0


def _add_date_range_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start-date", required=True, help="Inclusive YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="Inclusive YYYY-MM-DD")


def _add_acquisition_runtime_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--root", default="data/dhan")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--requests-per-second", type=float, default=5.0)
    parser.add_argument("--daily-budget", type=int, default=100_000)
    parser.add_argument("--max-retries", type=int, default=4)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--no-resume", action="store_true")


def _csv_values(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in str(value).split(",") if item.strip())


def _acquisition_engine(args: argparse.Namespace) -> AcquisitionEngine:
    return AcquisitionEngine(
        root=args.root,
        transport=DhanTransport(
            DhanCredentials.from_env(), timeout_seconds=args.timeout_seconds
        ),
        daily_budget=args.daily_budget,
        requests_per_second=args.requests_per_second,
        max_retries=args.max_retries,
        workers=args.workers,
    )


def _print_outcomes(outcomes: object, *, json_output: bool) -> int:
    rows = [asdict(item) for item in outcomes]
    statuses: dict[str, int] = {}
    for row in rows:
        status = str(row["status"])
        statuses[status] = statuses.get(status, 0) + 1
    payload = {
        "requests": len(rows),
        "rows": sum(int(row["rows"]) for row in rows),
        "status_counts": statuses,
        "outcomes": rows,
    }
    print(json.dumps(payload, indent=2 if json_output else None, sort_keys=True))
    return (
        0
        if not any(status in statuses for status in ("failed", "invalid_response"))
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
