#!/usr/bin/env python3
"""Publish deterministic evidence for the three required SPAN Phase 1 pilots."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPOSITORY_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from nifty_span.span.required_pilots import audit_required_span_pilots  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Audit the 2024-03, 2025-09, and 2026-06 SPAN Phase 1 pilots."
    )
    parser.add_argument(
        "--run-root",
        required=True,
        type=Path,
        help="SPAN run root containing reports/monthly and compacted",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        help="Report directory (default: RUN_ROOT/reports/required_pilots)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the complete deterministic JSON payload instead of a short status",
    )
    args = parser.parse_args(argv)
    result = audit_required_span_pilots(args.run_root, args.output_root)
    if args.json:
        print(json.dumps(result.payload, indent=2, sort_keys=True))
    else:
        print(
            json.dumps(
                {
                    "overall_status": result.overall_status,
                    "json_path": result.json_path,
                    "markdown_path": result.markdown_path,
                },
                sort_keys=True,
            )
        )
    if result.overall_status == "PASS":
        return 0
    if result.overall_status == "WAITING":
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
