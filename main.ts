// Pump.fun Paper Sprint entry: early-momentum paper bot (agent-fun/replay/v2).
//
// One program at main.ts (Node 22). The strategy lives in strategy.ts; this file only
// starts the protocol loop with the tuned defaults.
import { runBot } from './vendor/makefun-bot.ts';
import { createStrategy, DEFAULT_PARAMS } from './strategy.ts';

await runBot(createStrategy(DEFAULT_PARAMS));
