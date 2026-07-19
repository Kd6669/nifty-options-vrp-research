# Team-share release module

This module builds and verifies the credential-free team bundle from an exact committed Git
snapshot. It includes all tracked code, tests, documentation, compact results, final reviewer
artifacts and the deterministic sample. Git metadata, local datasets, caches, virtual
environments, credentials and temporary outputs are excluded by contract.

## Build and verify

Run from a clean repository checkout:

```powershell
team-bundle build
team-bundle verify
```

or use the wrapper:

```powershell
.\scripts\build_team_bundle.ps1
```

The default outputs are:

- `dist/nifty-options-vrp-research-team-bundle-v1.0.0.zip`;
- the matching `.zip.sha256` checksum;
- the matching `.zip.manifest.json` member manifest.

The ZIP is deterministic for a given Git commit. Every member is read from the committed Git
object rather than the mutable working tree, sorted, timestamp-normalized and SHA-256 recorded.
The build refuses a dirty working tree so the archive identity cannot disagree with the code under
review.

## Reproducibility boundary

The extracted bundle can rebuild and verify the compact Modules 3–5 result packet without the
multi-gigabyte research corpus. Recreating acquisition, cleaning, BSM, SPAN and the gold dataset
from provider data requires environment credentials and the external raw archive. That full-data
contract is documented in `REPRODUCE.md`; no raw dataset or secret is silently embedded here.
