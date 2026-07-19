"""Publish the accepted-with-source-gaps SPAN release and Dhan handoff."""

from __future__ import annotations

import argparse
import json

from nifty_span.span.phase1_release import publish_phase1_release


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--repository-commit", required=True)
    parser.add_argument("--validation-evidence", required=True)
    parser.add_argument("--accepted-at")
    args = parser.parse_args(argv)
    result = publish_phase1_release(
        run_root=args.run_root,
        repository_commit=args.repository_commit,
        validation_evidence=args.validation_evidence,
        accepted_at=args.accepted_at,
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
