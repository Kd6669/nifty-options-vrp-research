"""Finalize a quiescent full-range SPAN Phase 1 run."""

from __future__ import annotations

import argparse
from datetime import date
import json

from nifty_span.span.phase1_finalizer import finalize_span_phase1


def _tool_version(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("tool versions must use NAME=VERSION")
    name, version = value.split("=", 1)
    if not name.strip() or not version.strip():
        raise argparse.ArgumentTypeError(
            "tool versions must use non-empty NAME=VERSION"
        )
    return name.strip(), version.strip()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat)
    parser.add_argument("--availability-manifest", required=True)
    parser.add_argument("--benchmark-artifact", action="append", default=[])
    parser.add_argument("--pilot-artifact", action="append", default=[])
    parser.add_argument("--recovery-artifact", action="append", default=[])
    parser.add_argument("--commit-sha")
    parser.add_argument("--test-result")
    parser.add_argument(
        "--tool-version", action="append", default=[], type=_tool_version
    )
    arguments = parser.parse_args(argv)
    report = finalize_span_phase1(
        run_root=arguments.run_root,
        start_date=arguments.start_date,
        end_date=arguments.end_date,
        availability_manifest=arguments.availability_manifest,
        benchmark_artifacts=arguments.benchmark_artifact,
        pilot_artifacts=arguments.pilot_artifact,
        recovery_artifacts=arguments.recovery_artifact,
        commit_sha=arguments.commit_sha,
        test_result=arguments.test_result,
        tool_versions=dict(arguments.tool_version),
    )
    print(json.dumps(report.to_dict(), sort_keys=True, separators=(",", ":")))
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
