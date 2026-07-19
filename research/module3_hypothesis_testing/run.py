"""Command-line entrypoint for the closed Module 3 research packet."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from research.module3_hypothesis_testing.closeout import verify_manifest, write_closeout


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
        print(json.dumps({"decision": summary["decision"], "status": "built"}, indent=2))
        return

    failures = verify_manifest(repo_root)
    if failures:
        raise SystemExit("Manifest verification failed:\n" + "\n".join(failures))
    print("Module 3 manifest verification passed.")


if __name__ == "__main__":
    main()
