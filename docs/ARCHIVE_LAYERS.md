# Data archive layers

The project keeps three durable data layers. They are separate contracts, not folders that may be
silently overwritten in place.

## Bronze — immutable source archive

Bronze stores exact Dhan responses, official NSE/NSE Clearing artifacts, request identities,
response hashes, endpoint status, and download/parse evidence. Successful bronze objects are
content-addressed and immutable. Credentials and request headers are never persisted.

Bronze answers: **What exactly did the source return?**

## Silver — typed normalized archive

Silver converts each source into versioned, partitioned Parquet with explicit schemas. It validates
parallel-array lengths, OHLC invariants, volume/OI signs, timestamp/session classification, natural
keys, and request-window boundaries. Rejected or conflicting rows go to linked exception tables.

Silver answers: **What source records are structurally usable, and what was quarantined?**

## Gold — research-ready derived archive

Gold joins option rows to independently sourced NIFTY spot and INDIA VIX without looking forward,
maps official expiry and lot-size rules, computes exact time to expiry, applies the quality gate,
recomputes BSM IV/Greeks, and publishes separated SPAN representations.

Gold answers: **What may downstream research consume, under which provenance and timing rules?**

Gold is not a promise that every optional field is available. Every row retains status/reason
columns. In particular:

- `bsm_status=ok` marks solved rows; blocked and no-arbitrage rows remain in the dataset.
- BOD SPAN is a conservative static fallback because historical file-arrival time is unknown.
- Strict SPAN has no historical matches without proven effective timestamps.
- Six-slot SPAN is reference-only sensitivity data, not point-in-time execution evidence.
- INDIA VIX before its proven source boundary remains null and explicit.
- Historical expired-futures minute data is source-blocked and is not synthesized from spot.

## Git versus archive storage

Git stores code, schemas, source rules, manifests, audit summaries, hashes, and a small sample. The
multi-GB bronze/silver/gold payloads remain in local/archive storage and are reproducible from the
documented commands. This avoids pretending that Git or Git LFS is the data lake.
