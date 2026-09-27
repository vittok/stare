import assert from "node:assert/strict";
import test from "node:test";

import { rankTopPicks, topPickScore } from "./top-picks.mjs";

test("fundamentals are primary and last-session data differentiates similar stocks", () => {
  const base = { trailing_pe: 14, price_to_book: 1.5, peg_ratio: 0.8, dividend_yield: 2.5, daily_percentile: 80 };
  const positive = topPickScore({ ...base, close_change_pct: 0.03 });
  const negative = topPickScore({ ...base, close_change_pct: -0.03 });
  assert.equal(positive.coverage, 4);
  assert.ok(positive.score > negative.score);
  assert.match(positive.rationale, /low P\/E/);
});

test("expensive fundamentals rank below supportive fundamentals despite activity", () => {
  const ranked = rankTopPicks([
    { ticker: "EXPENSIVE", trailing_pe: 70, price_to_book: 15, peg_ratio: 3, close_change_pct: 0.05, daily_percentile: 100, dollar_vol_latest: 1_000_000 },
    { ticker: "VALUE", trailing_pe: 12, price_to_book: 1.4, peg_ratio: 0.7, dividend_yield: 3, close_change_pct: -0.01, daily_percentile: 20, dollar_vol_latest: 100 }
  ]);
  assert.equal(ranked[0].ticker, "VALUE");
});

test("ranking is deterministic with missing inputs and liquidity tie-breaks", () => {
  const ranked = rankTopPicks([
    { ticker: "LOW", dollar_vol_latest: 10 },
    { ticker: "HIGH", dollar_vol_latest: 20 },
    { ticker: "NONE", dollar_vol_latest: null }
  ]);
  assert.deepEqual(ranked.map((stock) => stock.ticker), ["HIGH", "LOW", "NONE"]);
  assert.equal(ranked[0].top_pick.coverage, 0);
});
