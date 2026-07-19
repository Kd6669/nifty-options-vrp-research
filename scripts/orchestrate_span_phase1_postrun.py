#!/usr/bin/env python3
"""Run the conservative Windows SPAN Phase 1 post-download state machine."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPOSITORY_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from nifty_span.span.postrun_orchestrator import PostrunConfig, run_postrun  # noqa: E402


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPOSITORY_ROOT)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument(
        "--wait-for-pid",
        action="append",
        type=int,
        required=True,
        help="Exact currently active downloader PID; repeat for the process tree",
    )
    parser.add_argument(
        "--follower-pid",
        action="append",
        type=int,
        help=(
            "Exact existing completed-month follower PID; repeat when applicable. "
            "Required unless --skip-follower-catchup is set"
        ),
    )
    parser.add_argument("--log-prefix", required=True)
    parser.add_argument("--availability-manifest", type=Path, required=True)
    parser.add_argument("--availability-import", type=Path, required=True)
    parser.add_argument("--provenance-root", type=Path, required=True)
    parser.add_argument(
        "--benchmark-artifact", action="append", type=Path, required=True
    )
    parser.add_argument("--pilot-output-root", type=Path)
    parser.add_argument(
        "--skip-generic-repair",
        action="store_true",
        help=(
            "After waiting for the exact named downloader tree and proving no writer "
            "remains, continue directly to follower catch-up and dedicated recovery"
        ),
    )
    parser.add_argument(
        "--skip-follower-catchup",
        action="store_true",
        help=(
            "Require no follower PIDs and proceed to immutable full-range extraction "
            "without follower catch-up or retirement"
        ),
    )
    parser.add_argument(
        "--retire-followers-before-full-extract",
        action="store_true",
        help=(
            "After caught-up quiescence, retire only the revalidated explicit "
            "follower tree before frozen full-range extraction"
        ),
    )
    parser.add_argument(
        "--follower-retirement-timeout-seconds", type=float, default=300.0
    )
    parser.add_argument("--follower-timeout-seconds", type=float, default=21_600.0)
    parser.add_argument("--evidence-timeout-seconds", type=float, default=21_600.0)
    parser.add_argument("--quiescence-seconds", type=float, default=120.0)
    parser.add_argument("--poll-seconds", type=float, default=15.0)
    parser.add_argument("--test-result")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = run_postrun(
        PostrunConfig(
            repo_root=args.repo_root,
            run_root=args.run_root,
            wait_for_pids=tuple(args.wait_for_pid),
            follower_pids=tuple(args.follower_pid or ()),
            log_prefix=args.log_prefix,
            availability_manifest=args.availability_manifest,
            availability_import=args.availability_import,
            provenance_root=args.provenance_root,
            benchmark_artifacts=tuple(args.benchmark_artifact),
            pilot_output_root=args.pilot_output_root,
            skip_generic_repair=args.skip_generic_repair,
            skip_follower_catchup=args.skip_follower_catchup,
            retire_followers_before_full_extract=(
                args.retire_followers_before_full_extract
            ),
            follower_retirement_timeout_seconds=(
                args.follower_retirement_timeout_seconds
            ),
            follower_timeout_seconds=args.follower_timeout_seconds,
            evidence_timeout_seconds=args.evidence_timeout_seconds,
            quiescence_seconds=args.quiescence_seconds,
            poll_seconds=args.poll_seconds,
            test_result=args.test_result,
        )
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    outcome = result.get("outcome")
    if outcome == "PASS_READY":
        return 0
    if outcome == "WAITING":
        return 2
    if outcome == "BLOCKED_SOURCE":
        return 3
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
