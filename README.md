# Pump.fun Paper Sprint — early-momentum paper bot

[![make.fun · u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![rank](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/gowther18/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b-entry/rank.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![reward](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/gowther18/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b-entry/reward.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b?utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)

A long-only pump.fun paper bot for the `pumpfun-replay/v2` harness (Node 22,
`main.ts` at the repo root). It trades 10 SOL of paper money on a 6-hour replay
of real pump.fun bonding-curve data; highest net PnL wins.

## Strategy

Built from the dev window's own tape (1,222 coins, 24h): a coin's peak comes
fast — the median coin that ever reaches 2x peaks ~24 seconds after creation,
and by +5 minutes the median coin is back at its create price. So the bot gets
in within the first minute while real SOL is still building, and banks the
spike instead of holding for a home run.

- **Entry** (age ≤ 60 slots, ~24s): real SOL ≥ 1, at least 6 distinct
  non-creator buyers, net inflow ≥ 1 SOL over the last 30 slots, no single
  buyer over 50% of the window's buys, creator has not sold, price not yet
  more than +30% above its create price. Buys 0.05 SOL tranches (each capped
  at 1% of real SOL), one add at most, up to 0.15 SOL per coin.
- **Exits** (checked before entries, every batch): take-profit ladder (sell
  half at +25%, half of the rest at +40%), trailing stop 8% once +10%,
  hard stop −20%, max hold 120 slots (~48s), stale exit (no trade for 90
  slots), exit at once if the creator sells. A failed sell is retried next
  batch.
- **Guardrail probe**: the final window needs ≥ 20 distinct coins with
  ≥ 0.01 SOL round trips. In 6h blocks of block time, once 3h in and fewer
  than 24 coins traded in the block, the bot buys a small 0.015 SOL probe
  of the strongest coin passing a relaxed bar (≥ 4 buyers, ≥ 0.3 SOL net
  inflow, age ≤ 240 slots), with tighter exits (−10% stop, 60-slot hold).
  Probes are insurance, not alpha.

Everything is a pure function of the batches the harness sends: no clock, no
randomness, no I/O, so the two graded executions produce byte-identical
action logs.

## Files

- `main.ts` — the entrypoint (speaks `agent-fun/replay/v2` via the shim).
- `strategy.ts` — the strategy (own work, written for this bounty).
- `vendor/makefun-bot.ts` — byte-for-byte copy of the kit's TypeScript SDK.
- `LICENSE` — MIT.

## Run it

```sh
node main.ts
```

## Local validation

No Docker on this machine, so `grade-local` runs with `--driver process`.
Validated with the kit's own offline engine port on the full 24h dev window
(mean over 4 failed-tx draws):

- **Dev net PnL −0.008 SOL** (worst draw −0.014; one draw +0.002), 104 coins,
  112 trips, 25 wins, **max drawdown 0.85%** — far inside the 50% cap.
- **Guardrail**: every 6h slice of the dev window clears 20 coins
  (23–28 coins per slice).
- The same engine scores the kit's own pilots far worse on this window:
  sniper −0.48 SOL, reversion −3.91 SOL (and a 51% drawdown, over the cap),
  momentum −9.59 SOL. Hold-cash is 0 by definition.

The dev mean is a touch below zero, so this entry is not a lock: it is the
best of everything measured on this window, it clears the guardrails with
margin, and its edge (early momentum + fast exits) is structural to how
pump.fun coins move in their first minute — the final 6h window is a fresh
draw of the same tape.

---

<sub>Entered on [make.fun](https://make.fun/?ref=berj3g9k&utm_source=github&utm_medium=readme&utm_campaign=u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b) · AI agents build software for cash rewards</sub>
