# Phase 2 Wide-Wing Playable-Universe Audit

## Decision

The unconditional evidence supports keeping every leg of the primary research
structure inside entry ATM +/-3 and limiting the maximum holding horizon to
180 minutes.

This is a data-support decision, not a statement that wider wings are
economically unattractive. The source is a rolling Dhan `WEEK`,
`expiryCode=1`, ATM +/-10 surface whose response omits the actual expiry; the
audited gold layer maps it to the second eligible weekly contract. As NIFTY
moves, a frozen wide-wing contract leaves the recorded
surface sooner than an ATM +/-3 contract. Wider structures therefore acquire
tail-correlated missing labels and paths even though their entry availability
is almost identical.

The audited structures are symmetric four-leg iron condors:

- short ATM-1 put and ATM+1 call;
- long put/call wings at ATM +/-3, +/-5, +/-7, or +/-9;
- every feasible entry minute on every observed date;
- 15, 30, 60, 90, 120, 180, 240, and 300-minute horizons;
- exact frozen-contract tracking by date, expiry, strike, and option type.

ATM +/-3 is the reference. ATM +/-5, +/-7, and +/-9 are the requested
extensions. No day, clock, or quote-quality subset is used to improve the
denominator.

## Reproduction and artifacts

The pooled, daily, and clock-level results are extracted losslessly from the
completed observed-session/computed-moneyness audit. Only the ATM-migration
cross-tab is recomputed from gold because that detail was previously retained
only for ATM +/-3.

```powershell
py -3.11 research/phase2/extract_wings_5_7_9.py `
  --gold-root "<gold-root>"
```

The command writes
`audit/phase2_unconditional_wings_5_7_9.json`. That JSON contains all 32
wing-by-horizon pooled rows, all 32 daily-distribution rows, 8,324 clock rows,
32 high-support clock summaries, the pooled 99% boundaries, and all 224
wing-by-horizon-by-ATM-migration rows.

## Pooled availability

All percentages use the full horizon-specific date-by-start-minute
denominator. `Proxy` is the eligible-entry population still missing at least
one frozen-contract terminal quote after allowing ten minutes of staleness.

| Horizon | Wing | Entry eligible | Exact endpoint | Strict path | Stale <=5m | Stale <=10m | Proxy |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 15m | +/-3 | 99.9074% | 99.8489% | 99.8257% | 99.8991% | 99.9033% | 0.0041% |
| 15m | +/-5 | 99.9090% | 99.8300% | 99.7989% | 99.8982% | 99.9045% | 0.0045% |
| 15m | +/-7 | 99.9092% | 99.7488% | 99.6825% | 99.8749% | 99.9015% | 0.0077% |
| 15m | +/-9 | 99.9058% | 97.7074% | 96.5282% | 99.2491% | 99.7508% | 0.1550% |
| 30m | +/-3 | 99.9114% | 99.8264% | 99.7700% | 99.8809% | 99.8908% | 0.0206% |
| 30m | +/-5 | 99.9131% | 99.7929% | 99.7126% | 99.8701% | 99.8817% | 0.0314% |
| 30m | +/-7 | 99.9133% | 99.5528% | 99.3209% | 99.7611% | 99.8226% | 0.0907% |
| 30m | +/-9 | 99.9097% | 94.2914% | 90.5518% | 96.8734% | 98.0888% | 1.8209% |
| 60m | +/-3 | 99.9146% | 99.7766% | 99.6754% | 99.8458% | 99.8546% | 0.0599% |
| 60m | +/-5 | 99.9164% | 99.6677% | 99.4829% | 99.7803% | 99.8063% | 0.1101% |
| 60m | +/-7 | 99.9166% | 98.9486% | 98.2137% | 99.3396% | 99.4880% | 0.4286% |
| 60m | +/-9 | 99.9127% | 87.8251% | 78.9038% | 91.3285% | 93.1926% | 6.7201% |
| 90m | +/-3 | 99.9207% | 99.7423% | 99.5537% | 99.8175% | 99.8306% | 0.0901% |
| 90m | +/-5 | 99.9228% | 99.5260% | 99.2101% | 99.6674% | 99.7097% | 0.2130% |
| 90m | +/-7 | 99.9230% | 98.0324% | 96.5403% | 98.6226% | 98.8934% | 1.0296% |
| 90m | +/-9 | 99.9189% | 82.3525% | 69.0987% | 86.1449% | 88.2321% | 11.6868% |
| 120m | +/-3 | 99.9286% | 99.7212% | 99.4421% | 99.8182% | 99.8319% | 0.0967% |
| 120m | +/-5 | 99.9309% | 99.2775% | 98.7790% | 99.4895% | 99.5686% | 0.3622% |
| 120m | +/-7 | 99.9312% | 97.1020% | 94.7111% | 97.8248% | 98.1475% | 1.7837% |
| 120m | +/-9 | 99.9271% | 77.5445% | 60.9070% | 81.4801% | 83.6794% | 16.2477% |
| 180m | +/-3 | 99.9396% | 99.5208% | 99.0926% | 99.6734% | 99.7068% | 0.2328% |
| 180m | +/-5 | 99.9426% | 98.6839% | 97.6385% | 99.0416% | 99.1886% | 0.7540% |
| 180m | +/-7 | 99.9438% | 94.3839% | 90.1660% | 95.4735% | 96.0206% | 3.9232% |
| 180m | +/-9 | 99.9389% | 69.1576% | 47.3723% | 73.2364% | 75.6256% | 24.3133% |
| 240m | +/-3 | 99.9134% | 99.1071% | 98.5074% | 99.3419% | 99.4047% | 0.5087% |
| 240m | +/-5 | 99.9177% | 97.7676% | 96.1057% | 98.2303% | 98.4171% | 1.5007% |
| 240m | +/-7 | 99.9194% | 91.3839% | 85.0034% | 92.8592% | 93.6184% | 6.3009% |
| 240m | +/-9 | 99.9156% | 62.0596% | 36.1583% | 66.2608% | 68.5521% | 31.3634% |
| 300m | +/-3 | 99.9028% | 98.5955% | 97.6245% | 98.9580% | 99.0475% | 0.8553% |
| 300m | +/-5 | 99.9106% | 96.3356% | 93.8357% | 97.0238% | 97.3037% | 2.6069% |
| 300m | +/-7 | 99.9135% | 88.0494% | 78.3286% | 89.8972% | 90.8079% | 9.1056% |
| 300m | +/-9 | 99.9106% | 55.3304% | 26.9338% | 59.6363% | 62.1353% | 37.7753% |

Entry eligibility stays near 99.9% for every wing because all requested legs
exist at entry. The divergence is almost entirely post-entry tracking. That is
exactly the failure expected from a frozen contract inside a rolling surface.

## Pooled 99% boundary

| Wing | Exact >=99% through | Path >=99% through | Stale-5 >=99% through | Stale-10 >=99% through |
|---:|---:|---:|---:|---:|
| +/-3 | 240m | 180m | 240m | 300m |
| +/-5 | 120m | 90m | 180m | 180m |
| +/-7 | 30m | 30m | 60m | 60m |
| +/-9 | none | none | 15m | 15m |

The ATM +/-3, 180-minute choice is the widest/horizon combination among these
candidate research boundaries that preserves at least 99% pooled strict-path
coverage. ATM +/-5 does not: its 180-minute strict path is 97.6385%.

## Clock dependence at the 180-minute boundary

Ordinary high-support clocks are those represented by at least 1,300 dates.
There are 195 such entry-clock buckets at 180 minutes.

| Wing | Exact >=99% clocks | Path >=99% clocks | Stale-10 >=99% clocks |
|---:|---:|---:|---:|
| +/-3 | 194/195 | 155/195 | 194/195 |
| +/-5 | 51/195 | 0/195 | 161/195 |
| +/-7 | 0/195 | 0/195 | 0/195 |
| +/-9 | 0/195 | 0/195 | 0/195 |

The pooled boundary does not mean every ATM +/-3 clock has a 99% uninterrupted
path. Intraday path research should therefore retain clock-of-day controls and
the earlier 09:15 caveat. Wide wings fail much more broadly across the day,
rather than only in one opening bucket.

## Day-weighted result at 180 minutes

| Wing | Median day exact | 5th-percentile day | Worst day | Dates >=99% | Dates >=95% |
|---:|---:|---:|---:|---:|---:|
| +/-3 | 100.0000% | 99.4872% | 8.7179% | 1,331/1,366 | 1,351/1,366 |
| +/-5 | 100.0000% | 96.4979% | 8.7179% | 1,264/1,366 | 1,304/1,366 |
| +/-7 | 100.0000% | 62.0092% | 1.0256% | 994/1,366 | 1,100/1,366 |
| +/-9 | 77.0408% | 12.3753% | 0.0000% | 202/1,366 | 342/1,366 |

ATM +/-9 is not merely weakened by a handful of outliers: its median day at
180 minutes has only 77.04% exact coverage. ATM +/-7 keeps a perfect median
but has a 62.01% fifth-percentile day, which is still unsuitable for an
unconditional primary result.

## ATM-migration mechanism

The six-or-more-strike migration bucket is rare but economically important.
At 180 minutes it contains 3,011 theoretical windows. Migration buckets are
conditioned on both the entry and endpoint ATM being observed; 229 of the
266,698 theoretical 180-minute windows have no computable endpoint shift.
Those unresolved-shift windows remain in every pooled headline above.

| Wing | Eligible | Exact | Stale <=5m | Stale <=10m |
|---:|---:|---:|---:|---:|
| +/-3 | 100.0000% | 72.7665% | 79.2096% | 81.9993% |
| +/-5 | 100.0000% | 0.0000% | 23.0156% | 35.8685% |
| +/-7 | 100.0000% | 0.0000% | 0.4318% | 1.4281% |
| +/-9 | 100.0000% | 0.0000% | 0.0000% | 0.2657% |

The zero exact coverage for wide wings in this bucket is geometric. If the ATM
moves six 50-point steps, the frozen opposite ATM +/-5 wing becomes eleven
steps from the new ATM and is outside an ATM +/-10 rolling capture. ATM +/-7
and ATM +/-9 leave the recorded surface after still smaller adverse shifts.

This is not missing at random and it is not evidence that a market quote did
not exist. It is evidence that the available dataset did not retain that
contract. Model-filling those labels would preferentially synthesize the most
directional and potentially highest-loss paths, so it cannot define the
primary sample.

## Final research universe

The defensible primary scope is:

- NIFTY index options only;
- one Dhan `WEEK`, `expiryCode=1` surface only; no cross-expiry comparison;
- intraday entry and exit only;
- defined-risk structures only;
- every leg within ATM +/-3 at entry;
- maximum holding horizon of 180 minutes;
- frozen date/expiry/strike/type contract identity after entry;
- exact quotes and observed paths primary, stale quotes separately labelled;
- no multi-expiry, calendar, or multi-day strategies.

This availability audit does not prove that the historical price series is
economically the nearest weekly contract. The separate volatility audit finds
that its prices and provider IV are much more consistent with the nearest
listed NIFTY expiry than with the audited second-weekly mapping. That expiry
identity discrepancy must remain explicit in any volatility or VRP result.

The 180-minute ceiling is a maximum, not a claim that every label type is
equally strong at that horizon. Full-path hypotheses should report the 15-180
minute horizon grid, clock-of-day results, day-weighted results, and the
ATM-migration sensitivity. Margin, transaction costs, volume/OI depth,
staleness penalties, and slippage are later execution layers and are not used
to manufacture data availability here.
