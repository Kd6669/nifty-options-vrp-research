# Sample dataset

`nifty_gold_sample.parquet` is a deterministic 751-row, 89-column research-facing subset of the
BOD-SPAN convenience gold release. It covers three explicit market regimes:

- 2021-01-04 — early sample history;
- 2025-09-02 — first Tuesday-expiry regime date;
- 2026-07-14 — recent Tuesday-expiry date.

For each date it selects 09:30, 12:00, and 15:00 IST and retains the option, independent spot,
INDIA VIX, contract, quality-gate, BSM, and SPAN lineage fields needed for inspection and tests.
Absolute local source paths, credentials, raw payloads, and request headers are excluded.

The file is not statistically representative and must not be used for performance estimation. Its
purpose is schema inspection, examples, CI checks, and reviewer orientation.

Verify it with:

```powershell
python tools/audit_sample.py `
  samples/nifty_gold_sample.parquet `
  samples/nifty_gold_sample.manifest.json
```

The machine-readable manifest records the selection, row/column counts, warning about BOD SPAN
timing, and SHA-256.
