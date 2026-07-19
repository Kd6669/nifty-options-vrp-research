"""Export a stable latest-event SPAN manifest to canonical JSON and Parquet."""

from __future__ import annotations

import argparse
import json

from nifty_span.span.manifest_exports import export_latest_manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("download", "extraction"))
    parser.add_argument("--source", required=True, help="Append-only source JSONL manifest")
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--stem", help="Output filename stem (defaults to <kind>_manifest)")
    arguments = parser.parse_args()
    report = export_latest_manifest(
        arguments.source,
        arguments.output_root,
        manifest_kind=arguments.kind,
        stem=arguments.stem,
    )
    print(json.dumps(report.to_dict(), sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
