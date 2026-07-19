"""Build or verify the final submission packet."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from research.module5_final_submission.analysis import build_submission, verify_submission


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("build", "verify"))
    parser.add_argument("--repo-root", type=Path, default=_repo_root())
    args = parser.parse_args()
    root = args.repo_root.resolve()
    if args.command == "build":
        print(json.dumps(build_submission(root), indent=2))
        return
    failures = verify_submission(root)
    if failures:
        raise SystemExit("Submission verification failed:\n" + "\n".join(failures))
    print("Final submission verification passed.")


if __name__ == "__main__":
    main()
