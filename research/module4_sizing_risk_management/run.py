"""Build or verify the closed Module 4 sizing and risk-management packet."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from research.module4_sizing_risk_management.closeout import verify_manifest, write_closeout


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("build", "verify"))
    parser.add_argument("--repo-root", type=Path, default=_repo_root())
    args = parser.parse_args()
    repo_root = args.repo_root.resolve()

    if args.command == "build":
        summary = write_closeout(repo_root)
        print(
            json.dumps(
                {
                    "status": "built",
                    "decision": summary["decision"],
                    "recommended_profile": summary["recommended_candidate"]["profile"],
                },
                indent=2,
            )
        )
        return

    failures = verify_manifest(repo_root)
    if failures:
        raise SystemExit("Manifest verification failed:\n" + "\n".join(failures))
    print("Module 4 manifest verification passed.")


if __name__ == "__main__":
    main()
