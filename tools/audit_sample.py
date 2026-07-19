"""Verify the committed sample and manifest without access to the full corpus."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: audit_sample.py SAMPLE.parquet SAMPLE.manifest.json")
    sample = Path(sys.argv[1])
    manifest_path = Path(sys.argv[2])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    parquet = pq.ParquetFile(sample)
    failures: list[str] = []
    if sha256_file(sample) != manifest["sha256"]:
        failures.append("sha256")
    if parquet.metadata.num_rows != int(manifest["rows"]):
        failures.append("rows")
    if parquet.schema_arrow.names != manifest["columns"]:
        failures.append("columns")
    if manifest.get("contains_credentials") is not False:
        failures.append("credential_declaration")
    table = pq.read_table(sample, columns=["trade_date", "timestamp_ist"])
    observed_dates = sorted({str(value) for value in table.column("trade_date").to_pylist()})
    ist = ZoneInfo("Asia/Kolkata")
    observed_times = sorted(
        {
            value.astimezone(ist).strftime("%H:%M")
            for value in table.column("timestamp_ist").to_pylist()
        }
    )
    if observed_dates != manifest.get("observed_trade_dates"):
        failures.append("observed_trade_dates")
    if observed_times != manifest.get("observed_times_ist"):
        failures.append("observed_times_ist")
    if observed_dates != manifest.get("selection", {}).get("trade_dates"):
        failures.append("selected_trade_dates")
    if observed_times != manifest.get("selection", {}).get("times_ist"):
        failures.append("selected_times_ist")
    if failures:
        raise SystemExit("sample audit failed: " + ", ".join(failures))
    print(
        json.dumps(
            {
                "status": "PASS",
                "rows": parquet.metadata.num_rows,
                "columns": parquet.metadata.num_columns,
                "sha256": manifest["sha256"],
            }
        )
    )


if __name__ == "__main__":
    main()
