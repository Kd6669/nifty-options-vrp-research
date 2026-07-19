# Research note — what Module 4 adds

Module 3's base result remains unchanged: the tested normalized-VRP zero crossings, tail crossings,
dynamics, and reversals do not provide a reliable standalone defined-risk edge over 60–180 minutes
after one-lot costs. Module 4 asks a narrower post-hoc question: can the most promising upper-tail
short-volatility event be made economically coherent by excluding the empirically weak regime and
using conservative capital constraints?

## Result sequence

Phase 8 applies the three IV/RV gates and corrected quantity-aware execution to a ₹10 lakh account.
Its balanced policy is slightly positive over the full sample but loses in 2024. Phase 9 then tests
whether a causal discovery-fitted confidence score ranks one-lot outcomes before size is applied.
The combined later-period Spearman rho is positive, but its bootstrap interval crosses zero, so the
score fails the frozen statistical gate.

Phase 10 nevertheless treats the score as a research feature and explores 17,640 sizing policies.
Selection remains inside 2021–2023; 2024 and 2025–26 are reported afterward. The useful signal is
not an aggressive leverage optimum. It is a broad neighborhood: require a 40% score floor and keep
the margin ceiling low. The selected 35%-margin profile earns ₹108,983 net historically, with a
0.76% fixed-exit drawdown and positive reported P&L in both later slices.

## Interpretation

This is enough to freeze a candidate, not enough to deploy it. The score was designed after looking
at the archive, Phase 10 searches many policies, and the data lacks intratrade MTM and margin paths.
The appropriate next experiment is an untouched forward shadow run using config 5628 without any
parameter changes. Its primary acceptance evidence must include realized bid/ask execution,
intratrade margin, weekly and monthly stability, loss clustering, and non-overlapping trades.

Idle capital is not an invitation to increase the ceiling. The preferred profile intentionally
uses about 32% average and 35% maximum entry margin; policies above 50% show worse later-period
stability in the historical grid.
