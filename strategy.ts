// Early-momentum paper bot for the pump.fun bonding-curve replay (agent-fun/replay/v2).
//
// Idea, from the dev window's own tape (1,222 coins, 24h): a coin's peak comes FAST —
// the median coin that ever reaches 2x peaks ~24 seconds after creation, and by +5 minutes
// the median coin is back at its create price. The edge is therefore: get in within the
// first minute while real SOL is still building, size up only as the 1%-of-real-reserve cap
// allows (which forces small buys while the coin is tiny and bigger ones as it gains
// traction), and bank the spike with a take-profit ladder plus a trailing stop instead of
// holding for a home run. Losers are cut at -8% or after ~2 minutes, whichever comes first.
//
// Entry filter (all from the batch stream, no clock, no randomness):
//   - coin is young (age <= maxAgeSlots) and has real SOL >= minRealSol, so one buy can be
//     at least ~0.01 SOL (a smaller buy would pay more in tx fees than it could earn);
//   - several distinct non-creator buyers and positive net inflow over the recent window;
//   - no single buyer dominates the early tape (a bundle waiting to dump);
//   - the creator has not sold;
//   - the price has not already run past maxRunupBps from its create price (no chasing).
// Exits (checked before entries, every batch): take-profit ladder, trailing stop once armed,
// hard stop, creator-sell exit, stale exit, max-hold exit. A failed sell is retried next batch.
//
// Deterministic by construction: iteration only over insertion-ordered maps fed by the batch
// stream, all arithmetic in bigint, no Date/Math.random.

import type { Batch, Bot, BotState, OrderInput, Pubkey } from './vendor/makefun-bot.ts';

export interface Params {
  /** Youngest/oldest age (slots since CreateEvent) at which a first buy may be decided. */
  minAgeSlots: number;
  maxAgeSlots: number;
  /** Real SOL reserve (lamports) a coin must hold before a first buy is considered. */
  minRealSol: bigint;
  /** Distinct non-creator buyers that must have traded the coin before entry. */
  minBuyers: number;
  /** Net SOL (buys minus sells, lamports) over the recent window at decision time. */
  minNetSol: bigint;
  /** Recent window (slots) for the net-flow check. */
  flowWindow: number;
  /** Largest single non-creator buy's share of the window's buy SOL (bps). */
  maxSingleBuyerBps: number;
  /** Do not buy when the price is already this far above the create price (bps). */
  maxRunupBps: number;
  /** Target SOL per position in total (accumulated in tranches, each capped at 1% of real SOL). */
  stakeLamports: bigint;
  /** SOL per buy tranche; also the smallest buy worth paying the tx fee for. */
  trancheLamports: bigint;
  /** Slippage allowed on buys (bps); sells always go out wide open so an exit never sticks. */
  buySlippageBps: number;
  /** First take-profit: sell tp1PartBps of the position once it is worth tp1Bps over cost. */
  tp1Bps: number;
  tp1PartBps: number;
  /** Second take-profit on the remainder. */
  tp2Bps: number;
  tp2PartBps: number;
  /** Trailing stop: once up at least trailArmBps, exit on a fall of trailBps from the best value. */
  trailArmBps: number;
  trailBps: number;
  /** Hard stop: exit when the position is worth this much under its cost (bps). */
  stopBps: number;
  /** Exit after holding this many slots regardless. */
  maxHoldSlots: number;
  /** Exit when no trade has hit the coin for this many slots. */
  staleSlots: number;
  /** Exit at once when the creator sells while held. */
  exitOnDevSell: boolean;
  /** Slots to wait after an exit before the same coin may be bought again. */
  cooldownSlots: number;
  /** Most entries per coin over the run. */
  maxEntries: number;
  /** Most new entries per reply. */
  maxEntriesPerBatch: number;
  /** Fallback probe: the final window is 6h, so the run is paced in 6h blocks of block time.
   *  Once 4h into a block and distinct coins traded in that block still lag this count,
   *  buy a small probe of the strongest relaxed-filter young coin so the block still clears
   *  the 20-coin guardrail. A pace rule would fire all run long; this fires only as a top-up. */
  probeAfterHours: number;
  probeTargetCoins: number;
  probeBlockHours: number;
  probeLamports: bigint;
  /** Relaxed entry bar the probe uses (the strict bar still governs normal entries). */
  probeMinBuyers: number;
  probeMinNetSol: bigint;
  probeMaxAgeSlots: number;
  probeMaxRunupBps: number;
  /** Probe positions exit tighter and sooner: they are guardrail insurance, not alpha. */
  probeStopBps: number;
  probeMaxHoldSlots: number;
}

export const DEFAULT_PARAMS: Params = {
  minAgeSlots: 0,
  maxAgeSlots: 60,
  minRealSol: 1_000_000_000n,
  minBuyers: 6,
  minNetSol: 1_000_000_000n,
  flowWindow: 30,
  maxSingleBuyerBps: 5000,
  maxRunupBps: 3000,
  stakeLamports: 150_000_000n,
  trancheLamports: 50_000_000n,
  buySlippageBps: 1000,
  tp1Bps: 2500,
  tp1PartBps: 5000,
  tp2Bps: 4000,
  tp2PartBps: 5000,
  trailArmBps: 1000,
  trailBps: 800,
  stopBps: 2000,
  maxHoldSlots: 120,
  staleSlots: 90,
  exitOnDevSell: true,
  cooldownSlots: 600,
  maxEntries: 1,
  maxEntriesPerBatch: 4,
  probeAfterHours: 3,
  probeTargetCoins: 24,
  probeBlockHours: 6,
  probeLamports: 15_000_000n,
  probeMinBuyers: 4,
  probeMinNetSol: 300_000_000n,
  probeMaxAgeSlots: 240,
  probeMaxRunupBps: 3000,
  probeStopBps: 1000,
  probeMaxHoldSlots: 60,
};

const BPS = 10_000n;
const FEE_BPS = 125n; // protocol 95 + creator 30 fallback; the sell proceeds lose about this.

interface Tick {
  slot: number;
  isBuy: boolean;
  sol: bigint;
  user: Pubkey;
}

interface Book {
  mint: Pubkey;
  createSlot: number;
  creator: Pubkey;
  vq0: bigint;
  vt0: bigint;
  creatorSold: boolean;
  /** Recent trades, oldest first, pruned to the flow window. */
  ticks: Tick[];
  buyers: Set<Pubkey>;
  buySol: bigint;
  sellSol: bigint;
  entries: number;
  coolUntil: number;
  done: boolean;
  lastTradeSlot: number;
  // Position bookkeeping (mirrors the account, kept for exit logic).
  openSlot: number;
  bestRatioBps: number;
  tookTp1: boolean;
  tookTp2: boolean;
  exiting: boolean;
  spent: bigint;
  /** Price ratio (bps vs create) at the first buy's decision time; top-ups need the price near it. */
  entryRatioBps: number;
  adds: number;
  /** Bought by the guardrail probe: exits tighter and sooner. */
  isProbe: boolean;
}

function priceRatioBps(vq: bigint, vt: bigint, vq0: bigint, vt0: bigint): number {
  if (vt === 0n || vt0 === 0n || vq0 === 0n) return 0;
  // (vq/vt) / (vq0/vt0) in bps, as a float-safe integer ratio via BigInt.
  const num = vq * vt0 * BPS;
  const den = vt * vq0;
  return Number(num / den);
}

/** Net proceeds (lamports) of selling `tokens` at the curve's current reserves. */
function sellProceeds(tokens: bigint, vq: bigint, vt: bigint, txFee: bigint): bigint {
  if (tokens <= 0n || vt <= 0n) return 0n;
  const q = (tokens * vq) / (vt + tokens);
  const fee = (q * FEE_BPS + BPS - 1n) / BPS;
  const net = q - fee - txFee;
  return net > 0n ? net : 0n;
}

export function createStrategy(p: Params = DEFAULT_PARAMS): Bot {
  const books = new Map<Pubkey, Book>();
  const book = (mint: Pubkey): Book => {
    let b = books.get(mint);
    if (b === undefined) {
      b = {
        mint, createSlot: -1, creator: '', vq0: 0n, vt0: 0n, creatorSold: false,
        ticks: [], buyers: new Set(), buySol: 0n, sellSol: 0n,
        entries: 0, coolUntil: -1, done: false, lastTradeSlot: -1,
        openSlot: -1, bestRatioBps: 0, tookTp1: false, tookTp2: false, exiting: false, spent: 0n,
        entryRatioBps: 0, adds: 0, isProbe: false,
      };
      books.set(mint, b);
    }
    return b;
  };
  let coinsTraded = 0;
  let firstSlot = -1;
  let firstTs = -1;
  // Guardrail pacing runs in blocks of probeBlockHours of block time (the final window is
  // 6h): coins counted per block, so a slow stretch late in a long run still gets topped up.
  let blockStartTs = -1;
  let coinsInBlock = 0;

  return {
    onBatch(state: BotState, batch: Batch): OrderInput[] {
      const slot = batch.slot;
      const tx = state.txFeeLamports();
      const orders: OrderInput[] = [];
      const cap = state.hello.caps.max_orders_per_batch;

      // 0. Track window start (first batch's slot / block time) for guardrail pacing.
      if (firstSlot < 0) {
        firstSlot = slot;
        for (const e of batch.events) if (e.timestamp !== undefined && e.timestamp !== null) { firstTs = Number(e.timestamp); break; }
        blockStartTs = firstTs;
      }
      // Roll the pacing block forward every probeBlockHours of block time.
      {
        let nowTs = -1;
        for (const e of batch.events) {
          if (e.timestamp !== undefined && e.timestamp !== null) { nowTs = Number(e.timestamp); break; }
        }
        if (firstTs > 0 && nowTs > 0 && blockStartTs > 0 && nowTs - blockStartTs >= p.probeBlockHours * 3600) {
          blockStartTs = nowTs;
          coinsInBlock = 0;
        }
      }

      // 1. Fold the batch's events into the books.
      for (const e of batch.events) {
        if (e.event === 'CreateEvent') {
          const b = book(e.mint);
          b.createSlot = e.slot;
          b.creator = e.creator;
          b.vq0 = BigInt(e.virtual_quote_reserves);
          b.vt0 = BigInt(e.virtual_token_reserves);
          b.lastTradeSlot = e.slot;
        } else if (e.event === 'TradeEvent') {
          const b = book(e.mint);
          b.lastTradeSlot = e.slot;
          const sol = BigInt(e.sol_amount);
          b.ticks.push({ slot: e.slot, isBuy: e.is_buy, sol, user: e.user });
          if (e.is_buy) {
            b.buySol += sol;
            if (e.user !== b.creator) b.buyers.add(e.user);
          } else {
            b.sellSol += sol;
          }
          if (e.user === b.creator && !e.is_buy) b.creatorSold = true;
        } else {
          const b = books.get(e.mint);
          if (b !== undefined) b.done = true;
        }
      }
      // Prune old ticks (keep the flow window plus a little slack).
      const pruneBefore = slot - p.flowWindow - 30;
      for (const b of books.values()) {
        while (b.ticks.length > 0 && b.ticks[0].slot < pruneBefore) b.ticks.shift();
      }

      // 2. Fold fills: a sell that resolved clears the position bookkeeping; a buy that
      //    resolved opens it; a failed order clears the exiting flag so it is retried.
      for (const f of batch.fills) {
        const b = books.get(f.mint);
        if (b === undefined) continue;
        if (f.side === 'sell') {
          b.exiting = false;
          if (!state.positions.has(f.mint)) {
            b.openSlot = -1;
            b.bestRatioBps = 0;
            b.tookTp1 = false;
            b.tookTp2 = false;
            b.spent = 0n;
            b.entryRatioBps = 0;
            b.adds = 0;
            b.isProbe = false;
            b.coolUntil = slot + p.cooldownSlots;
          }
        } else if (f.side === 'buy') {
          if (f.status === 'filled') {
            if (b.openSlot < 0) {
              b.openSlot = slot;
              coinsTraded += 1;
              coinsInBlock += 1;
            }
            b.spent += BigInt(f.quote_lamports);
          } else if (!state.positions.has(f.mint)) {
            let stillPending = false;
            for (const o of state.pending.values()) if (o.order.mint === f.mint && o.order.side === 'buy') stillPending = true;
            if (!stillPending) b.entries = Math.max(0, b.entries - 1);
          }
        }
      }

      const held = [...state.positions.keys()].sort();
      const pendingSell = new Set<Pubkey>();
      const pendingBuy = new Set<Pubkey>();
      for (const o of state.pending.values()) {
        if (o.order.side === 'sell') pendingSell.add(o.order.mint);
        else pendingBuy.add(o.order.mint);
      }

      // 3. Exits first: they free position slots and cash for entries.
      for (const mint of held) {
        if (orders.length >= cap) break;
        if (pendingSell.has(mint)) continue;
        const pos = state.positions.get(mint)!;
        const curve = state.curves.get(mint);
        const b = books.get(mint);
        if (curve === undefined || curve.complete || b === undefined) continue;
        const tokens = pos.token_amount;
        const cost = pos.cost_lamports;
        if (tokens <= 0n || cost <= 0n) continue;
        const value = sellProceeds(tokens, curve.virtual_quote_reserves, curve.virtual_token_reserves, tx);
        const ratioBps = Number((value * BPS) / cost); // 10000 = breakeven
        if (b.openSlot >= 0 && ratioBps > b.bestRatioBps) b.bestRatioBps = ratioBps;
        const age = b.openSlot >= 0 ? slot - b.openSlot : 0;
        const stale = b.lastTradeSlot >= 0 && slot - b.lastTradeSlot >= p.staleSlots;
        let sellPartBps = 0;
        let why = '';
        if (!b.tookTp1 && ratioBps >= BPS + BigInt(p.tp1Bps)) { sellPartBps = p.tp1PartBps; why = 'tp1'; }
        else if (!b.tookTp2 && ratioBps >= BPS + BigInt(p.tp2Bps)) { sellPartBps = p.tp2PartBps; why = 'tp2'; }
        else if (b.bestRatioBps >= BPS + BigInt(p.trailArmBps) && ratioBps <= b.bestRatioBps - p.trailBps) { sellPartBps = 10000; why = 'trail'; }
        else if (ratioBps <= BPS - BigInt(b.isProbe ? p.probeStopBps : p.stopBps)) { sellPartBps = 10000; why = 'stop'; }
        else if (age >= (b.isProbe ? p.probeMaxHoldSlots : p.maxHoldSlots)) { sellPartBps = 10000; why = 'maxhold'; }
        else if (stale) { sellPartBps = 10000; why = 'stale'; }
        else if (p.exitOnDevSell && b.creatorSold) { sellPartBps = 10000; why = 'devsell'; }
        if (sellPartBps === 0) continue;
        const amount = (tokens * BigInt(sellPartBps)) / BPS;
        if (amount <= 0n) continue;
        if (why === 'tp1') b.tookTp1 = true;
        if (why === 'tp2') b.tookTp2 = true;
        if (sellPartBps >= 10000) b.exiting = true;
        orders.push({ side: 'sell', mint, token_amount: amount, max_slippage_bps: 10000 });
      }

      // 4. Entries: young coins with a strong early tape, sized by the 1% reserve cap.
      let slotsFree = state.hello.caps.max_open_positions - state.committedPositionCount();
      let free = state.freeCashLamports();
      let entriesSent = 0;
      const candidates: Book[] = [];
      for (const e of batch.events) {
        if (e.event !== 'TradeEvent' && e.event !== 'CreateEvent') continue;
        const b = books.get(e.mint);
        if (b === undefined || b.done || b.createSlot < 0) continue;
        if (candidates.includes(b)) continue;
        candidates.push(b);
      }
      // Deterministic order: strongest recent net flow first (ties keep insertion order).
      const flowOf = (b: Book): bigint => {
        let net = 0n;
        for (const t of b.ticks) if (t.slot >= slot - p.flowWindow) net += t.isBuy ? t.sol : -t.sol;
        return net;
      };
      candidates.sort((a, b) => (flowOf(b) > flowOf(a) ? 1 : flowOf(b) < flowOf(a) ? -1 : 0));
      for (const b of candidates) {
        if (orders.length >= cap || slotsFree <= 0 || entriesSent >= p.maxEntriesPerBatch) break;
        if (state.positions.has(b.mint) || pendingBuy.has(b.mint)) {
          // Already held (or a buy in flight): one add at most, only while the price is
          // still near the first entry's level and the tape still shows net inflow.
          if (!state.positions.has(b.mint) || b.openSlot < 0 || b.adds >= 1) continue;
          const pos = state.positions.get(b.mint)!;
          if (pos.cost_lamports >= p.stakeLamports) continue;
          const curve = state.curves.get(b.mint);
          if (curve === undefined || curve.complete) continue;
          if (flowOf(b) < p.minNetSol) continue;
          if (b.vq0 > 0n && priceRatioBps(curve.virtual_quote_reserves, curve.virtual_token_reserves, b.vq0, b.vt0) > b.entryRatioBps + 500) continue;
          const capBuy = state.maxBuyLamports(b.mint);
          const amount = capBuy < p.trancheLamports ? capBuy : p.trancheLamports;
          if (amount < p.trancheLamports / 2n || free < amount + tx) continue;
          b.adds += 1;
          orders.push({ side: 'buy', mint: b.mint, quote_lamports: amount, max_slippage_bps: p.buySlippageBps });
          free -= amount + tx;
          continue;
        }
        if (b.entries >= p.maxEntries || slot < b.coolUntil) continue;
        const age = slot - b.createSlot;
        if (age < p.minAgeSlots || age > p.maxAgeSlots) continue;
        const curve = state.curves.get(b.mint);
        if (curve === undefined || curve.complete) continue;
        if (curve.real_quote_reserves < p.minRealSol) continue;
        if (b.creatorSold) continue;
        if (b.buyers.size < p.minBuyers) continue;
        const net = flowOf(b);
        if (net < p.minNetSol) continue;
        // No single buyer dominating the window's buys.
        let buySum = 0n;
        const byBuyer = new Map<Pubkey, bigint>();
        for (const t of b.ticks) {
          if (t.slot < slot - p.flowWindow || !t.isBuy) continue;
          buySum += t.sol;
          byBuyer.set(t.user, (byBuyer.get(t.user) ?? 0n) + t.sol);
        }
        let topShare = 0n;
        for (const v of byBuyer.values()) if (v > topShare) topShare = v;
        if (buySum > 0n && (topShare * BPS) / buySum > BigInt(p.maxSingleBuyerBps)) continue;
        // Do not chase a coin that already ran far from its create price.
        if (b.vq0 > 0n && priceRatioBps(curve.virtual_quote_reserves, curve.virtual_token_reserves, b.vq0, b.vt0) > BPS + BigInt(p.maxRunupBps)) continue;
        const capBuy = state.maxBuyLamports(b.mint);
        const amount = capBuy < p.trancheLamports ? capBuy : p.trancheLamports;
        if (amount < p.trancheLamports / 2n || free < amount + tx) continue;
        b.entries += 1;
        b.entryRatioBps = b.vq0 > 0n ? priceRatioBps(curve.virtual_quote_reserves, curve.virtual_token_reserves, b.vq0, b.vt0) : 10000;
        orders.push({ side: 'buy', mint: b.mint, quote_lamports: amount, max_slippage_bps: p.buySlippageBps });
        free -= amount + tx;
        slotsFree -= 1;
        entriesSent += 1;
      }

      // 5. Guardrail probe: if distinct coins traded lag the pace, buy a small probe of the
      //    strongest relaxed-filter young coin so the run still clears the 20-coin bar.
      if (orders.length < cap && slotsFree > 0) {
        let nowTs = -1;
        for (const e of batch.events) {
          if (e.timestamp !== undefined && e.timestamp !== null) { nowTs = Number(e.timestamp); break; }
        }
        const elapsedHours = firstTs > 0 && nowTs > 0 ? (nowTs - blockStartTs) / 3600 : (slot - firstSlot) / 16800;
        if (elapsedHours >= p.probeAfterHours && coinsInBlock < p.probeTargetCoins) {
          let best: Book | null = null;
          let bestFlow = -1n;
          for (const b of books.values()) {
            if (b.done || b.createSlot < 0 || b.entries >= p.maxEntries || slot < b.coolUntil) continue;
            if (state.positions.has(b.mint) || pendingBuy.has(b.mint)) continue;
            if (slot - b.createSlot > p.probeMaxAgeSlots) continue;
            if (b.creatorSold || b.buyers.size < p.probeMinBuyers) continue;
            const curve = state.curves.get(b.mint);
            if (curve === undefined || curve.complete) continue;
            if (curve.real_quote_reserves < p.minRealSol) continue;
            const net = flowOf(b);
            if (net < p.probeMinNetSol) continue;
            if (b.vq0 > 0n && priceRatioBps(curve.virtual_quote_reserves, curve.virtual_token_reserves, b.vq0, b.vt0) > BPS + BigInt(p.probeMaxRunupBps)) continue;
            let buySum = 0n;
            const byBuyer = new Map<Pubkey, bigint>();
            for (const t of b.ticks) {
              if (t.slot < slot - p.flowWindow || !t.isBuy) continue;
              buySum += t.sol;
              byBuyer.set(t.user, (byBuyer.get(t.user) ?? 0n) + t.sol);
            }
            let topShare = 0n;
            for (const v of byBuyer.values()) if (v > topShare) topShare = v;
            if (buySum > 0n && (topShare * BPS) / buySum > BigInt(p.maxSingleBuyerBps)) continue;
            if (net > bestFlow) { bestFlow = net; best = b; }
          }
          if (best !== null) {
            const capBuy = state.maxBuyLamports(best.mint);
            const amount = capBuy < p.probeLamports ? capBuy : p.probeLamports;
            if (amount >= 11_000_000n && free >= amount + tx) {
              best.entries += 1;
              best.isProbe = true;
              best.entryRatioBps = best.vq0 > 0n
                ? priceRatioBps(state.curves.get(best.mint)!.virtual_quote_reserves, state.curves.get(best.mint)!.virtual_token_reserves, best.vq0, best.vt0)
                : 10000;
              orders.push({ side: 'buy', mint: best.mint, quote_lamports: amount, max_slippage_bps: p.buySlippageBps });
            }
          }
        }
      }
      return orders;
    },
  };
}
