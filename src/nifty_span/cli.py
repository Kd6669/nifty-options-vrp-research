from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

from nifty_span.span.extractor import DEFAULT_SYMBOLS_FILTER


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="groww-margin")
    subparsers = parser.add_subparsers(dest="command", required=True)
    _add_span_maintenance_parser(subparsers)
    _add_span_groww_margin_parity_parser(subparsers)
    _add_span_backfill_parser(subparsers)
    args = parser.parse_args(argv)
    return _dispatch(args)


def span_groww_margin_parity_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="span-groww-margin-parity")
    _add_span_groww_margin_parity_args(parser)
    args = parser.parse_args(argv)
    args.command = "span-groww-margin-parity"
    return _dispatch(args)


def span_maintenance_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="span-maintenance")
    _add_span_maintenance_args(parser)
    args = parser.parse_args(argv)
    args.command = "span-maintenance"
    return _dispatch(args)


def span_backfill_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="span-backfill")
    _add_span_backfill_mode_parsers(parser)
    args = parser.parse_args(argv)
    args.command = "span-backfill"
    return _dispatch(args)


def _add_span_maintenance_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "span-maintenance",
        help="Continuously download/extract NSE SPAN files and keep latest available slot ready",
    )
    _add_span_maintenance_args(parser)


def _add_span_maintenance_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--date", help="Trading date YYYY-MM-DD; defaults to today")
    parser.add_argument("--raw-root", default="data/span/raw")
    parser.add_argument("--parquet-dir", default="data/span/parquet")
    parser.add_argument("--time-slot", default="LATEST")
    parser.add_argument("--symbols", nargs="+", default=list(DEFAULT_SYMBOLS_FILTER))
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--interval-seconds", type=float, default=300.0)
    parser.add_argument(
        "--iterations", type=int, default=0, help="0 means run until interrupted"
    )
    parser.add_argument("--report-out", default="reports/span_maintenance_latest.json")
    parser.add_argument("--json", action="store_true")


def _add_span_groww_margin_parity_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "span-groww-margin-parity",
        help="Compare local SPAN Model-A margin decomposition against Groww basket-margin quotes",
    )
    _add_span_groww_margin_parity_args(parser)


def _add_span_groww_margin_parity_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--chain-parquet", required=True, help="Groww option-chain snapshot parquet"
    )
    parser.add_argument("--span-parquet-dir", default="data/span/parquet")
    parser.add_argument("--out-dir", default="reports/span_groww_margin_parity/latest")
    parser.add_argument(
        "--date",
        help="Trading date YYYY-MM-DD; omitted means infer from chain snapshot",
    )
    parser.add_argument(
        "--expiry", help="Expiry YYYY-MM-DD; omitted means infer from chain snapshot"
    )
    parser.add_argument(
        "--timestamp",
        help="Snapshot timestamp; omitted means latest snapshot in parquet",
    )
    parser.add_argument("--underlying", default="NIFTY")
    parser.add_argument("--span-time-slot", default="LATEST")
    parser.add_argument(
        "--lots", nargs="+", type=int, default=[1, 3], help="Lot multipliers to test"
    )
    parser.add_argument(
        "--max-baskets",
        type=int,
        default=0,
        help="0 means run the full generated basket set",
    )
    parser.add_argument(
        "--future-trading-symbol",
        help="Optional Groww NIFTY futures symbol for beta/future baskets",
    )
    parser.add_argument("--future-expiry", help="Optional futures expiry YYYY-MM-DD")
    parser.add_argument(
        "--future-price", type=float, help="Optional futures limit/reference price"
    )
    parser.add_argument("--future-lot-size", type=int, help="Optional futures lot size")
    parser.add_argument(
        "--poll-groww",
        action="store_true",
        help="Call Groww get_order_margin_details using GROWW_ACCESS_TOKEN.",
    )
    parser.add_argument(
        "--estimate-groww-charges",
        action="store_true",
        help="Add a local estimate of Groww margin API brokerage_and_charges to local totals.",
    )
    parser.add_argument("--warn-abs-inr", type=float, default=500.0)
    parser.add_argument("--fail-abs-inr", type=float, default=2000.0)
    parser.add_argument("--warn-pct", type=float, default=0.02)
    parser.add_argument("--fail-pct", type=float, default=0.05)
    parser.add_argument("--json", action="store_true")


def _add_span_backfill_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "span-backfill",
        help="Resumable historical NSE SPAN download, extraction, compaction, and audit",
    )
    _add_span_backfill_mode_parsers(parser)


def _add_span_backfill_mode_parsers(parser: argparse.ArgumentParser) -> None:
    modes = parser.add_subparsers(dest="span_backfill_mode", required=True)
    for mode, help_text in (
        ("download", "Download and validate immutable raw archives"),
        (
            "recover-corrupt",
            "Recover or source-classify latest corrupt slots from official static archives",
        ),
        ("classify", "Import official calendar evidence and classify missing cells"),
        ("extract", "Extract manifest-listed archives and compact each month once"),
        ("audit", "Reconcile the durable date/slot matrix through compacted Parquet"),
        ("backfill", "Run download, extraction, compaction, and audit in order"),
    ):
        child = modes.add_parser(mode, help=help_text)
        _add_span_backfill_common_args(child)
        if mode in {"download", "backfill"}:
            _add_span_download_args(child)
        if mode == "classify":
            child.add_argument("--availability-import", required=True)
            child.add_argument(
                "--provenance-root", default="data/span/availability_sources"
            )
        if mode == "recover-corrupt":
            child.add_argument("--corrupt-max-attempts", type=int, default=3)
            child.add_argument("--corrupt-timeout-seconds", type=float, default=600.0)
            child.add_argument(
                "--corrupt-only",
                action="store_true",
                help="Exclude terminal missing-slot cells from this recovery run",
            )
        if mode in {"extract", "backfill"}:
            child.add_argument("--batch-rows", type=int, default=50_000)
            child.add_argument("--parse-workers", type=int, default=4)


def _add_span_backfill_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start-date", required=True, help="Inclusive YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="Inclusive YYYY-MM-DD")
    parser.add_argument("--raw-root", default="data/span/raw")
    parser.add_argument(
        "--download-manifest", default="data/span/manifests/download_manifest.jsonl"
    )
    parser.add_argument(
        "--availability-manifest",
        default="data/span/manifests/availability_manifest.jsonl",
    )
    parser.add_argument("--fragment-root", default="data/span/extracted")
    parser.add_argument(
        "--extraction-manifest", default="data/span/manifests/extraction_manifest.jsonl"
    )
    parser.add_argument("--parquet-root", default="data/span/compacted")
    parser.add_argument(
        "--quarantine-root", default="data/span/exceptions/duplicate_conflicts"
    )
    parser.add_argument("--report-root", default="data/span/reports")
    parser.add_argument("--symbols", nargs="+", default=["NIFTY"])
    parser.add_argument("--json", action="store_true")


def _add_span_download_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--download-concurrency", type=int, default=4)
    parser.add_argument("--queue-size", type=int)
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument(
        "--retry-incomplete-passes",
        type=int,
        default=1,
        help="Bounded reconciliation passes over dates still missing or nonterminal",
    )
    parser.add_argument(
        "--repair-order",
        choices=("chronological", "unseen-first"),
        default="chronological",
        help=(
            "chronological scans every date; unseen-first covers never-observed dates first "
            "and then retries transport-only dates while leaving deterministic corrupt bundles "
            "for recover-corrupt"
        ),
    )
    parser.add_argument("--timeout-seconds", type=float, default=180.0)
    parser.add_argument("--session-refresh-requests", type=int, default=100)
    parser.add_argument(
        "--reprobe-missing",
        action="store_true",
        help="Re-request dates whose previous official response returned no archive",
    )


def _dispatch(args: argparse.Namespace) -> int:
    if args.command == "span-backfill":
        return _dispatch_span_backfill(args)
    if args.command == "span-maintenance":
        from nifty_span.span.maintenance import run_span_maintenance_loop

        trading_date = (
            date.today() if args.date is None else date.fromisoformat(args.date)
        )
        try:
            report = run_span_maintenance_loop(
                trading_date=trading_date,
                raw_root=Path(args.raw_root),
                parquet_dir=Path(args.parquet_dir),
                preferred_time_slot=args.time_slot,
                symbols_filter=tuple(args.symbols),
                max_workers=args.workers,
                interval_seconds=args.interval_seconds,
                iterations=args.iterations,
                report_out=Path(args.report_out),
                emit_json=args.json,
            )
        except KeyboardInterrupt:
            return 130
        except Exception as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        return 0 if report.ok else 1
    if args.command == "span-groww-margin-parity":
        from nifty_span.span.groww_margin_parity import run_margin_parity_check

        try:
            report = run_margin_parity_check(
                chain_parquet=Path(args.chain_parquet),
                span_parquet_dir=Path(args.span_parquet_dir),
                output_dir=Path(args.out_dir),
                trading_date=None
                if args.date is None
                else date.fromisoformat(args.date),
                expiry=args.expiry,
                timestamp=args.timestamp,
                underlying=args.underlying,
                span_time_slot=args.span_time_slot,
                lots=tuple(args.lots),
                max_baskets=args.max_baskets,
                poll_groww=args.poll_groww,
                estimate_groww_charges=args.estimate_groww_charges,
                future_trading_symbol=args.future_trading_symbol,
                future_expiry=args.future_expiry,
                future_price=args.future_price,
                future_lot_size=args.future_lot_size,
                warn_abs_inr=args.warn_abs_inr,
                fail_abs_inr=args.fail_abs_inr,
                warn_pct=args.warn_pct,
                fail_pct=args.fail_pct,
            )
        except Exception as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        if args.json:
            print(json.dumps(report.to_dict(), indent=2, sort_keys=True, default=str))
        else:
            print(
                f"ok={report.ok} baskets={report.basket_count} warn={report.warn_count} fail={report.fail_count}"
            )
            print(f"csv={report.csv_path}")
            print(f"markdown={report.markdown_path}")
        return 0 if report.ok else 1
    return 2


def _dispatch_span_backfill(args: argparse.Namespace) -> int:
    from nifty_span.span.backfill import (
        extract_and_compact_span_range,
        run_span_backfill_pipeline,
    )
    from nifty_span.span.backfill_audit import audit_span_backfill
    from nifty_span.span.backfill_downloader import download_span_backfill
    from nifty_span.span.availability import import_and_classify_availability
    from nifty_span.span.corrupt_recovery import (
        CorruptRecoveryConfig,
        recover_corrupt_span_cells,
    )

    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    common = {
        "start_date": start_date,
        "end_date": end_date,
        "raw_root": Path(args.raw_root),
        "download_manifest": Path(args.download_manifest),
    }
    mode = args.span_backfill_mode
    try:
        if mode == "classify":
            report = import_and_classify_availability(
                start_date=start_date,
                end_date=end_date,
                import_path=Path(args.availability_import),
                download_manifest=Path(args.download_manifest),
                availability_manifest=Path(args.availability_manifest),
                provenance_root=Path(args.provenance_root),
            )
            payload = report.to_dict()
            ok = report.unresolved_missing_cells == 0
        elif mode == "recover-corrupt":
            report = recover_corrupt_span_cells(
                start_date=start_date,
                end_date=end_date,
                raw_root=Path(args.raw_root),
                download_manifest=Path(args.download_manifest),
                availability_manifest=Path(args.availability_manifest),
                report_root=Path(args.report_root),
                config=CorruptRecoveryConfig(
                    max_attempts=args.corrupt_max_attempts,
                    timeout_seconds=args.corrupt_timeout_seconds,
                    include_missing_targets=not args.corrupt_only,
                ),
            )
            payload = report.to_dict()
            ok = report.ok
        elif mode == "download":
            report = download_span_backfill(
                start_date=start_date,
                end_date=end_date,
                output_root=Path(args.raw_root),
                manifest_path=Path(args.download_manifest),
                config=_backfill_config(args),
            )
            payload = report.to_dict()
            ok = report.failed_slots == 0 and report.downloaded_slots > 0
        elif mode == "extract":
            report = extract_and_compact_span_range(
                **common,
                fragment_root=Path(args.fragment_root),
                extraction_manifest=Path(args.extraction_manifest),
                compacted_root=Path(args.parquet_root),
                quarantine_root=Path(args.quarantine_root),
                symbols=tuple(args.symbols),
                batch_rows=args.batch_rows,
                parse_workers=args.parse_workers,
            )
            payload = report.to_dict()
            ok = report.ok
        elif mode == "audit":
            report = audit_span_backfill(
                **common,
                extraction_manifest=Path(args.extraction_manifest),
                fragment_root=Path(args.fragment_root),
                compacted_root=Path(args.parquet_root),
                report_root=Path(args.report_root),
                availability_manifest=Path(args.availability_manifest),
            )
            payload = report.to_dict(include_cells=False)
            ok = report.ok
        elif mode == "backfill":
            report = run_span_backfill_pipeline(
                **common,
                fragment_root=Path(args.fragment_root),
                extraction_manifest=Path(args.extraction_manifest),
                compacted_root=Path(args.parquet_root),
                quarantine_root=Path(args.quarantine_root),
                report_root=Path(args.report_root),
                symbols=tuple(args.symbols),
                batch_rows=args.batch_rows,
                parse_workers=args.parse_workers,
                download_config=_backfill_config(args),
                availability_manifest=Path(args.availability_manifest),
            )
            payload = report.to_dict()
            ok = report.ok
        else:  # pragma: no cover - argparse enforces the choices.
            return 2
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        print(f"mode={mode} ok={ok} start={start_date} end={end_date}")
        if mode in {"audit", "backfill"}:
            audit = report if mode == "audit" else report.audit
            print(f"summary={audit.summary_json}")
            print(f"matrix={audit.matrix_parquet}")
            print(f"markdown={audit.audit_markdown}")
    return 0 if ok else 1


def _backfill_config(args: argparse.Namespace) -> Any:
    from nifty_span.span.backfill_downloader import BackfillConfig

    return BackfillConfig(
        max_concurrent=args.download_concurrency,
        queue_size=args.queue_size,
        max_attempts=args.max_attempts,
        retry_incomplete_passes=args.retry_incomplete_passes,
        timeout_seconds=args.timeout_seconds,
        session_refresh_requests=args.session_refresh_requests,
        reprobe_missing=args.reprobe_missing,
        repair_order=args.repair_order,
    )


if __name__ == "__main__":
    raise SystemExit(main())
