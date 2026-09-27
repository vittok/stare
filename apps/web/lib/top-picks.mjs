function numberValue(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function fundamentalValue(stock, field, nestedField) {
  return numberValue(stock[field] ?? stock.fundamentals?.[nestedField]);
}

export function topPickScore(stock) {
  const pe = fundamentalValue(stock, "trailing_pe", "trailingPE")
    ?? fundamentalValue(stock, "forward_pe", "forwardPE");
  const pb = fundamentalValue(stock, "price_to_book", "priceToBook");
  const peg = fundamentalValue(stock, "peg_ratio", "pegRatio");
  const dividend = fundamentalValue(stock, "dividend_yield", "dividendYield");
  const dailyMove = numberValue(stock.close_change_pct);
  const activity = numberValue(stock.daily_percentile ?? stock.daily_trading_percentile);

  let fundamentals = 0;
  let coverage = 0;
  const reasons = [];

  if (pe !== null) {
    coverage += 1;
    if (pe <= 0) { fundamentals -= 2; reasons.push("negative earnings"); }
    else if (pe < 15) { fundamentals += 2; reasons.push("low P/E"); }
    else if (pe <= 35) { fundamentals += 1; reasons.push("moderate P/E"); }
    else if (pe > 50) { fundamentals -= 2; reasons.push("very high P/E"); }
    else { fundamentals -= 1; reasons.push("elevated P/E"); }
  }
  if (pb !== null) {
    coverage += 1;
    if (pb > 0 && pb < 2) { fundamentals += 1; reasons.push("low P/B"); }
    else if (pb > 12) { fundamentals -= 1; reasons.push("high P/B"); }
  }
  if (peg !== null) {
    coverage += 1;
    if (peg <= 0) { fundamentals -= 1; reasons.push("negative PEG"); }
    else if (peg < 1) { fundamentals += 2; reasons.push("PEG below 1"); }
    else if (peg <= 2) { fundamentals += 1; reasons.push("balanced PEG"); }
    else { fundamentals -= 1; reasons.push("high PEG"); }
  }
  if (dividend !== null) {
    coverage += 1;
    if (dividend >= 4) { fundamentals += 1; reasons.push("high dividend yield"); }
    else if (dividend >= 2) { fundamentals += 0.5; reasons.push("dividend support"); }
  }

  // Last-session inputs add at most 1.5 points, so fundamentals remain primary.
  const daily = dailyMove === null ? 0 : Math.max(-1, Math.min(1, dailyMove * 20));
  const activityBonus = activity === null ? 0 : Math.max(0, Math.min(0.5, activity / 200));
  const score = fundamentals + daily + activityBonus;
  if (dailyMove !== null) reasons.push(`${dailyMove >= 0 ? "positive" : "negative"} last-session move`);
  if (activity !== null && activity >= 75) reasons.push("high last-session activity");

  return {
    score: Math.round(score * 100) / 100,
    coverage,
    rationale: reasons.slice(0, 4).join("; ") || "limited fundamentals and last-session data"
  };
}

export function rankTopPicks(stocks) {
  return stocks
    .map((stock) => ({ ...stock, top_pick: topPickScore(stock) }))
    .sort((left, right) =>
      right.top_pick.score - left.top_pick.score
      || right.top_pick.coverage - left.top_pick.coverage
      || (numberValue(right.dollar_vol_latest) || 0) - (numberValue(left.dollar_vol_latest) || 0)
      || String(left.ticker).localeCompare(String(right.ticker))
    );
}
