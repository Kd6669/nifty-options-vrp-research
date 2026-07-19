# Quant Research — Take-Home
### NIFTY Index Options: Find an Edge, Test It Honestly, and Tell Us If It's Real

**Role:** Quantitative Researcher (systematic derivatives)
**Format:** A research problem — code plus a written memo.
**Time:** Give it 5–7 calendar days, but only about 10–15 hours of real work. Don't blow your week on this.
**What to send back:** A git repo (or a zip) and a PDF memo. Submission details are at the bottom.

---

## What we're actually asking you to do

Here's the short version, so nothing is ambiguous:

- **Objective 1 — Pick a fight you can lose.** State one specific, falsifiable hypothesis about the variance/volatility risk premium in short-dated NIFTY options. Precise enough that we could prove it wrong.
- **Objective 2 — Build just enough to test it.** A data pipeline, an entry rule, a defined-risk structure, an honest cost model, and an out-of-sample check. No more machinery than the question needs.
- **Objective 3 — Try to break your own result.** Where does the edge come from? Does it survive a plausible change in parameters, dates, or costs? Show us what kills it.
- **Objective 4 — Make a call.** Would you put real money on this? At what size, and with what kill-switch? Or would you shelve it? Either answer is fine — an undefended "it works" is not.

If you only remember one thing from this brief: **we care more about how you think than about your Sharpe.** A modest, well-understood finding with its weaknesses laid bare will beat a spectacular curve you can't defend. Every time.

Please read "How we grade" and "Instant red flags" *before* you start. They tell you exactly where the marks are.

---

## Why this task exists

We run a small, systematic index-options book on NSE — the NIFTY / BANKNIFTY family. We're not trying to find out whether you can manufacture a high backtested Sharpe. Anyone can do that, and we've all done it.

What we want to see is how you work as a researcher: can you form a hypothesis, test it without fooling yourself, separate the part that's real from the part that's artifact, and — this is the hard one — tell us plainly when your idea *doesn't* work.

Honesty about your own results is the single most valuable thing you can show us here.

---

## The problem

> **Is there a systematically harvestable premium in short-dated NIFTY index options — and could a small desk actually capture it, net of costs, slippage, and today's regulatory regime?**

The specific thing we want you to dig into is the **variance / volatility risk premium**: the well-documented habit of implied vol trading above the vol that later shows up as realized. The question is whether a *defined-risk, rules-based* strategy can turn that into money under the market structure we live in now.

How you go after it is up to you. The good submissions tend to:

1. **State a specific, falsifiable hypothesis.** Something like: "A delta-neutral NIFTY strangle held to expiry earns a positive risk premium net of costs, but it's concentrated in low-VIX regimes and inverts around events." Make it sharp enough to be wrong.
2. **Build the minimum apparatus to test it** — data pipeline, a signal or entry rule, a defined-risk structure, an execution/cost model, and an out-of-sample evaluation.
3. **Attack your own result.** Where's the edge coming from? Is it robust to a reasonable change in parameters, dates, or costs? What breaks it?
4. **Decide.** Real capital or not? At what size, with what kill-switch? Or shelved — and why?

You do **not** need a production-ready, live-tradeable system. You need a credible, reproducible answer to the question.

---

## Scope (please stay inside these lines)

- **Underlying:** NIFTY 50 index options — weekly and/or monthly. BANKNIFTY is fine as a secondary robustness check, but keep NIFTY the main event.
- **Structures:** defined-risk only. Verticals, iron condors, iron flies, credit/debit spreads, risk-defined calendars, or delta-hedged short premium with an explicit hedge rule. **No naked shorts in the headline strategy** — unhedged short gamma isn't a base case a small desk should model, and we want to watch you reason about tail risk.
- **Holding horizon:** anything from intraday to one expiry cycle. Just tell us which.
- **No lookahead, no survivorship shortcuts, no peeking.** More on this in the data section.

---

## The market structure you have to get right

Getting these wrong tells us you don't actually trade this market. All of it is in force as of 2026:

- **Weekly expiry:** NIFTY weeklies expire **Tuesday** (moved from Thursday, effective 1 Sep 2025). If Tuesday's a holiday, expiry rolls to the previous trading day.
- **Monthly expiry:** last **Tuesday** of the month for NIFTY on NSE. (BSE SENSEX runs a separate Thursday cycle — ignore it unless you're using it on purpose.)
- **One weekly per exchange:** since the Nov 2024 SEBI rationalisation, **only NIFTY** has weekly options on NSE. **BANKNIFTY, FINNIFTY, and MIDCPNIFTY are monthly-only.** Don't build anything that assumes BANKNIFTY weeklies still exist — they don't.
- **Contract value / lot size:** SEBI's Oct 2024 framework set the index-derivative contract value band at **₹15–20 lakh**, with lot sizes revised so `lot size × index level` stays in that band. Use the **current** lot size for sizing, not a stale one, and state the lot size and date you used.
- **Costs — this is where most backtests quietly lie:**
  - **STT** on the sell side of options (currently 0.1% of premium), *plus* STT on exercised/settled ITM options. Model expiry-settlement STT explicitly — it's a real drag on held-to-expiry short premium.
  - **Brokerage, exchange transaction charges, SEBI/stamp charges, and GST** on top of all that.
  - **Bid–ask slippage.** Don't assume mid-price fills — especially on far-OTM legs and in the last hour on expiry day.
  - **Upfront premium collection and higher margins** (post-2024). Margin is a binding constraint, so give us a capital-efficiency number, not just a % return on premium.
  - **Expiry-day specifics:** the calendar-spread margin benefit disappears on expiry day, and short index options carry an extra ELM that day. If your strategy touches expiry, this moves both your capital and your risk.

We'll judge you partly on whether your cost model is *conservative and explicit*. An optimistic cost model is worse than no backtest at all.

---

## Data

We're deliberately not handing you a clean dataset. Sourcing and cleaning judgment is part of the test.

- Use whatever you can legitimately get: free/public EOD options chains, historical IV / India VIX, spot/futures, or a vendor sample. **Tell us your source and its limitations.**
- Can't get granular options data? You may **approximate** — reconstruct option prices from spot + India VIX under a stated model, or use a shorter clean sample — *but say so plainly and discuss how it biases your conclusion.* Honest approximation beats fake precision.
- **The data hygiene we'll check:**
  - **No lookahead.** A signal at time *t* uses only what was knowable at *t*. Watch settlement prices, close-computed IV, and next-day fills.
  - **Correct expiry-day mechanics**, including the Tuesday shift and the holiday roll.
  - **Realistic fills** — not mid, not the touch on illiquid strikes.
  - **Events** — budget, RBI policy, elections, heavy-results weeks. At minimum, acknowledge them.
- A small, clean, correctly handled sample beats a big sloppy one.

---

## What to hand in

1. **Research memo (PDF, ~8 pages max).** This is the centrepiece. It should read like an internal research note, not a pitch deck. A spine that works:
   - The hypothesis and why it's plausible (a paragraph).
   - Your data and its limits.
   - Method: signal/entry, structure, exit, cost model, sizing.
   - Results: in-sample vs out-of-sample, with costs. Report the metrics that matter (below).
   - Robustness: what you varied, what held up, what didn't.
   - **"Why this might be fake":** your own strongest argument against your result.
   - The decision: trade it / size it / kill it — and the kill-switch.
2. **Reproducible code.** A repo or folder that runs end-to-end from a documented entry point and regenerates your headline numbers. A README with setup and a single command to reproduce. Readable beats clever.
3. **A results artifact** — an equity curve, tearsheet, or notebook — with drawdowns, a per-regime breakdown, and trade-level stats.

**Metrics we care about (report whatever applies):**
CAGR and vol *net of costs*; max drawdown and drawdown duration; return on *margin/capital* (not on premium); Sharpe **and** Sortino (and be honest that daily Sharpe on a short-premium book is flattered by fat left tails); tail metrics (worst day/week, CVaR); hit rate and average win/loss; turnover and total cost drag as a % of gross P&L; a capacity estimate (how much size before slippage eats the edge).

---

## How we grade (weighted)

| Weight | Dimension | What earns the marks |
|---|---|---|
| 30% | **Research honesty & self-critique** | You found the holes in your own work before we did. Realistic costs. Out-of-sample discipline. No overfitting theatre. |
| 25% | **Methodological soundness** | No lookahead, correct expiry/settlement mechanics, sensible statistics, honest handling of fat tails and small samples. |
| 20% | **Domain fluency** | You clearly understand NIFTY options, the current regime, the Greeks, and why a solo desk's constraints — capital, margin, one operator — matter. |
| 15% | **Reproducibility & code quality** | It runs. It's readable. Your numbers regenerate. |
| 10% | **Communication & decision** | The memo is clear, and you actually made a call — trade it or kill it, with sizing and a stop. |

Notice that **Sharpe isn't on this table.** A defensible 0.7 you understand completely beats an indefensible 2.5 you can't explain.

---

## Instant red flags (any of these can sink a submission)

- A daily Sharpe above ~2 on a short-premium book, presented without a hard look at the left tail. Extraordinary claims need extraordinary evidence — from you, unprompted.
- Mid-price fills, zero slippage, or STT/settlement costs left out or hand-waved.
- Lookahead: trading on close-computed IV at that same close, treating settlement prices as tradeable, next-bar leakage.
- Assuming BANKNIFTY weeklies exist, or using the old Thursday NIFTY expiry.
- Backtesting naked short options and calling it the strategy.
- An equity curve with no drawdown analysis and no per-regime breakdown.
- Over-optimised parameters with no out-of-sample test, dressed up as a finding.
- No discussion of capacity, margin, or what happens on a gap-down day.
- A memo that sells instead of reports.

---

## Optional stretch (only if your core is solid — don't rob the core for these)

- Condition the premium on a **regime signal** (India VIX level/term structure, realized-minus-implied spread, event calendar) and show it improves risk-adjusted return.
- Treat the **Nov-2024 regime change as a structural break** and test whether pre- and post-change behaviour actually differ. This is a genuinely open question, and we'd be curious what you find.
- Quantify **execution decay**: how fast does the edge fade as you widen assumed slippage or move fills from mid → touch → through?
- Sketch how you'd **monitor this live** on a solo desk — what you'd watch every day, and what would make you cut it.

---

## Logistics & submission

- **Time:** aim for ~10–15 focused hours. If you run short, ship a smaller honest result with a "what I'd do next" section rather than an unfinished sprawl. Knowing what to cut is itself a signal.
- **Tools:** any language or stack (we lean Python, but it's not required). Any libraries. LLM assistance is fine **as long as you understand and can defend every line** — because we'll ask.
- **Submit:** a git repo link (or a zip) with the code, README, results artifact, and the PDF memo.
- **The follow-up:** the technical round is us walking through *your* submission. Come ready to defend your cost model, prove there's no lookahead, talk us through your worst drawdown trade by trade, and answer "what would make you kill this in production?"

Good luck. We'd far rather hire a researcher we can trust to tell us the truth about a mediocre edge than one who can dress up a mirage.

---

*Notes for whoever's reusing this brief: it assumes a mid-to-senior quant researcher and a ~1-week take-home, weighted toward options-premium research and rigor rather than ML or microstructure. Easy to retune for a junior screen (narrow it, hand over clean data), a live 2-hour version, or an ML-signal focus.*
