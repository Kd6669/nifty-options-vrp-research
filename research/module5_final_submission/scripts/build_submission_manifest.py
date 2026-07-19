"""Hash final reviewer artifacts and their immediate reproducibility inputs."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
FILES = [
    "submission/NIFTY_VRP_Research_Memo.pdf",
    "submission/NIFTY_VRP_Research_Memo.tex",
    "submission/NIFTY_VRP_Research_Highlights.pdf",
    "submission/NIFTY_VRP_Research_Highlights.tex",
    "submission/figures/annual_costs.pdf",
    "submission/figures/coverage_boundary.pdf",
    "submission/figures/equity_drawdown.pdf",
    "submission/figures/execution_capacity.pdf",
    "submission/figures/hypothesis_ladder.pdf",
    "submission/figures/robustness_panels.pdf",
    "submission/NIFTY_VRP_Research_Tearsheet.xlsx",
    "research/module5_final_submission/results/manifest.json",
    "research/module5_final_submission/results/summary.json",
    "research/module5_final_submission/scripts/build_pdf.py",
    "research/module5_final_submission/scripts/build_workbook.mjs",
    "research/module5_final_submission/scripts/run_submission.ps1",
]


def main() -> None:
    members = []
    for relative in FILES:
        path = ROOT / relative
        if not path.exists():
            raise FileNotFoundError(relative)
        members.append(
            {
                "path": relative,
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    payload = {
        "schema_version": "final-submission-artifacts/v2",
        "decision": "SHADOW_ONLY_NOT_LIVE_CAPITAL_APPROVED",
        "members": members,
    }
    output = ROOT / "submission/manifest.json"
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
