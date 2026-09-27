import type { StockSnapshot } from "./portal-api";

export type TopPick = StockSnapshot & {
  daily_percentile?: number | null;
  top_pick: { score: number; coverage: number; rationale: string };
};

export function topPickScore(stock: StockSnapshot & { daily_percentile?: number | null }): TopPick["top_pick"];
export function rankTopPicks(stocks: (StockSnapshot & { daily_percentile?: number | null })[]): TopPick[];
