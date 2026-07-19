from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path

from tools.team_bundle import MANIFEST_NAME, _is_excluded, load_contract, verify_bundle


ROOT = Path(__file__).resolve().parents[1]


def test_bundle_contract_covers_reviewer_surface() -> None:
    contract = load_contract(ROOT)
    required = set(contract["required_paths"])
    assert all((ROOT / path).is_file() for path in required)
    assert "submission/NIFTY_VRP_Research_Memo.pdf" in required
    assert "research/module5_final_submission/results/trades/final_trade_sheet.csv" in required


def test_bundle_contract_excludes_local_and_secret_paths() -> None:
    contract = load_contract(ROOT)
    for path in (
        ".env",
        ".git/config",
        ".venv/pyvenv.cfg",
        "data/raw/options.parquet",
        "dist/release.zip",
        "node_modules/package/index.js",
    ):
        assert _is_excluded(path, contract)


def test_archive_verifier_detects_no_errors_for_valid_fixture(tmp_path: Path) -> None:
    payload = b"review packet\n"
    member = {"path": "README.md", "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}
    manifest = {
        "schema_version": "team-share-bundle/v1",
        "archive_prefix": "fixture",
        "members": [member],
        "required_paths": ["README.md"],
    }
    archive_path = tmp_path / "fixture.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("fixture/README.md", payload)
        archive.writestr("fixture/" + MANIFEST_NAME, json.dumps(manifest).encode("utf-8"))
    digest = hashlib.sha256(archive_path.read_bytes()).hexdigest()
    archive_path.with_suffix(".zip.sha256").write_text(
        f"{digest}  {archive_path.name}\n", encoding="utf-8"
    )
    assert verify_bundle(archive_path) == []
