from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List


@dataclass
class HtmlConfig:
    in_json: Path = Path("reports/sector_dashboard.json")
    out_html: Path = Path("reports/sector_dashboard.html")
    title: str = "Sector Dashboard (S&P 500)"


def _safe(x: Any) -> str:
    if x is None:
        return ""
    return str(x)


def _fmt_pct(x: Any) -> str:
    try:
        if x is None:
            return ""
        return f"{float(x)*100:.2f}%"
    except Exception:
        return ""


def _fmt_num(x: Any, digits: int = 2) -> str:
    try:
        if x is None:
            return ""
        return f"{float(x):,.{digits}f}"
    except Exception:
        return ""


def _fmt_big(x: Any) -> str:
    """Format large numbers like market cap."""
    try:
        if x is None:
            return ""
        v = float(x)
        for unit, div in [("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)]:
            if abs(v) >= div:
                return f"{v/div:,.2f}{unit}"
        return f"{v:,.0f}"
    except Exception:
        return ""


def render_html(data: Dict[str, Any], title: str) -> str:
    sectors: List[Dict[str, Any]] = data.get("sectors", [])

    # Build a quick summary table
    summary_rows = []
    for s in sectors:
        summary_rows.append(
            f"""
            <tr>
              <td class="sector">{_safe(s.get('sector'))}</td>
              <td class="dir dir-{_safe(s.get('direction')).lower()}">{_safe(s.get('direction'))}</td>
              <td class="num">{_safe(s.get('strength'))}</td>
              <td class="num">{_fmt_num(s.get('raw_score'), 3)}</td>
              <td class="muted">{_safe(s.get('week_ending'))}</td>
            </tr>
            """
        )

    # Build per-sector blocks
    sector_blocks = []
    for s in sectors:
        sector_name = _safe(s.get("sector"))
        direction = _safe(s.get("direction"))
        strength = _safe(s.get("strength"))
        raw = _fmt_num(s.get("raw_score"), 3)
        week_ending = _safe(s.get("week_ending"))

        top10 = s.get("top10_active", []) or []

        # top10 table
        rows = []
        for r in top10:
            f = (r.get("fundamentals") or {})
            rows.append(
                f"""
                <tr>
                  <td class="num">{_safe(r.get("rank"))}</td>
                  <td class="ticker">{_safe(r.get("ticker"))}</td>
                  <td class="num">{_fmt_pct(r.get("weekly_return"))}</td>
                  <td class="num">{_fmt_big(r.get("dollar_vol_week"))}</td>
                  <td class="num">{_fmt_num(r.get("vol_ratio"), 2)}</td>

                  <td class="name">{_safe(f.get("shortName"))}</td>
                  <td class="muted">{_safe(f.get("industry"))}</td>

                  <td class="num">{_fmt_big(f.get("marketCap"))}</td>
                  <td class="num">{_fmt_num(f.get("trailingPE"), 2)}</td>
                  <td class="num">{_fmt_num(f.get("forwardPE"), 2)}</td>
                  <td class="num">{_fmt_num(f.get("priceToBook"), 2)}</td>

                  <td class="num">{_fmt_pct(f.get("profitMargins"))}</td>
                  <td class="num">{_fmt_pct(f.get("operatingMargins"))}</td>
                  <td class="num">{_fmt_pct(f.get("returnOnEquity"))}</td>
                  <td class="num">{_fmt_pct(f.get("dividendYield"))}</td>
                  <td class="num">{_fmt_num(f.get("beta"), 2)}</td>

                  <td class="muted">{_safe(f.get("exchange"))}</td>
                  <td class="muted">{_safe(f.get("currency"))}</td>
                </tr>
                """
            )

        sector_blocks.append(
            f"""
            <details class="sector-block" open>
              <summary>
                <div class="summary-line">
                  <span class="sector-title">{sector_name}</span>
                  <span class="pill dir-{direction.lower()}">{direction}</span>
                  <span class="pill strength">Strength: {strength}</span>
                  <span class="pill raw">Raw: {raw}</span>
                  <span class="pill week">Week: {week_ending}</span>
                </div>
              </summary>

              <div class="table-wrap">
                <table class="sortable">
                  <thead>
                    <tr>
                      <th>#</th>
                      <th>Ticker</th>
                      <th>Weekly Return</th>
                      <th>$ Vol (Week)</th>
                      <th>Vol Ratio</th>

                      <th>Name</th>
                      <th>Industry</th>

                      <th>Mkt Cap</th>
                      <th>P/E</th>
                      <th>Fwd P/E</th>
                      <th>P/B</th>

                      <th>Profit</th>
                      <th>Op</th>
                      <th>ROE</th>
                      <th>Div</th>
                      <th>Beta</th>

                      <th>Exch</th>
                      <th>CCY</th>
                    </tr>
                  </thead>
                  <tbody>
                    {''.join(rows)}
                  </tbody>
                </table>
                <div class="hint muted">Tip: click a column header to sort.</div>
              </div>
            </details>
            """
        )

    generated_from = _safe(data.get("generated_from"))

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <style>
    :root {{
      --bg: #0b0f14;
      --card: #121824;
      --text: #e8eef7;
      --muted: #9fb0c3;
      --border: #223044;
      --good: #34d399;
      --bad: #fb7185;
      --neutral: #fbbf24;
      --pill: #1a2433;
    }}
    body {{
      margin: 0; padding: 24px;
      background: var(--bg);
      color: var(--text);
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, "Noto Sans";
    }}
    h1 {{ margin: 0 0 6px; font-size: 22px; }}
    .sub {{ color: var(--muted); margin-bottom: 18px; }}
    .card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 16px;
      margin-bottom: 18px;
      box-shadow: 0 8px 24px rgba(0,0,0,.25);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 12.5px;
    }}
    th, td {{
      border-bottom: 1px solid var(--border);
      padding: 8px 10px;
      vertical-align: top;
    }}
    th {{
      text-align: left;
      cursor: pointer;
      position: sticky;
      top: 0;
      background: #0f1520;
      z-index: 1;
      user-select: none;
    }}
    tr:hover td {{ background: rgba(255,255,255,.03); }}
    .muted {{ color: var(--muted); }}
    .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .ticker {{ font-weight: 700; }}
    .sector {{ font-weight: 700; }}
    .name {{ max-width: 240px; }}
    .table-wrap {{ overflow-x: auto; border-radius: 12px; }}
    details.sector-block {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 12px 12px 6px;
      margin-bottom: 12px;
    }}
    summary {{
      list-style: none;
      cursor: pointer;
      outline: none;
    }}
    summary::-webkit-details-marker {{ display: none; }}
    .summary-line {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
      padding: 4px 2px 10px;
    }}
    .sector-title {{
      font-size: 16px;
      font-weight: 800;
      margin-right: 4px;
    }}
    .pill {{
      background: var(--pill);
      border: 1px solid var(--border);
      border-radius: 999px;
      padding: 4px 10px;
      font-size: 12px;
      color: var(--text);
    }}
    .dir-bullish {{ color: var(--good); border-color: rgba(52,211,153,.35); }}
    .dir-bearish {{ color: var(--bad); border-color: rgba(251,113,133,.35); }}
    .dir-neutral {{ color: var(--neutral); border-color: rgba(251,191,36,.35); }}
    .hint {{ margin-top: 8px; font-size: 12px; }}
    .footer {{ margin-top: 18px; color: var(--muted); font-size: 12px; }}
  </style>
</head>
<body>
  <h1>{title}</h1>
  <div class="sub">Generated from: {generated_from}</div>

  <div class="card">
    <h2 style="margin:0 0 10px; font-size:16px;">Sector Summary</h2>
    <div class="table-wrap">
      <table class="sortable" id="summary">
        <thead>
          <tr>
            <th>Sector</th>
            <th>Direction</th>
            <th class="num">Strength</th>
            <th class="num">Raw</th>
            <th>Week Ending</th>
          </tr>
        </thead>
        <tbody>
          {''.join(summary_rows)}
        </tbody>
      </table>
    </div>
    <div class="hint muted">Tip: click headers to sort. Expand sectors below for top 10 active + fundamentals.</div>
  </div>

  {''.join(sector_blocks)}

  <div class="footer">Static HTML report generated by the stare pipeline.</div>

<script>
(function() {{
  function getCellValue(tr, idx) {{
    const td = tr.children[idx];
    return td ? td.textContent.trim() : "";
  }}

  function parseMaybeNumber(v) {{
    // handle %, commas, and suffixes K/M/B/T
    if (!v) return NaN;
    let s = v.replace(/,/g, "");
    let mult = 1;
    if (s.endsWith("T")) {{ mult = 1e12; s = s.slice(0,-1); }}
    else if (s.endsWith("B")) {{ mult = 1e9; s = s.slice(0,-1); }}
    else if (s.endsWith("M")) {{ mult = 1e6; s = s.slice(0,-1); }}
    else if (s.endsWith("K")) {{ mult = 1e3; s = s.slice(0,-1); }}
    if (s.endsWith("%")) {{ mult = 0.01; s = s.slice(0,-1); }}
    const n = parseFloat(s);
    return isNaN(n) ? NaN : n * mult;
  }}

  function comparer(idx, asc) {{
    return function(a, b) {{
      const va = getCellValue(asc ? a : b, idx);
      const vb = getCellValue(asc ? b : a, idx);

      const na = parseMaybeNumber(va);
      const nb = parseMaybeNumber(vb);

      if (!isNaN(na) && !isNaN(nb)) return na - nb;
      return va.localeCompare(vb);
    }}
  }}

  document.querySelectorAll("table.sortable").forEach(function(table) {{
    const ths = table.querySelectorAll("thead th");
    ths.forEach(function(th, i) {{
      th.addEventListener("click", function() {{
        const tbody = table.querySelector("tbody");
        if (!tbody) return;
        const rows = Array.from(tbody.querySelectorAll("tr"));
        const asc = !(th.classList.contains("asc"));
        ths.forEach(x => x.classList.remove("asc", "desc"));
        th.classList.add(asc ? "asc" : "desc");
        rows.sort(comparer(i, asc)).forEach(r => tbody.appendChild(r));
      }});
    }});
  }});
}})();
</script>

</body>
</html>
"""
    return html


def main():
    cfg = HtmlConfig()
    if not cfg.in_json.exists():
        raise RuntimeError(f"Missing input JSON: {cfg.in_json}. Run src/build_sector_dashboard.py first.")

    data = json.loads(cfg.in_json.read_text(encoding="utf-8"))
    html = render_html(data, cfg.title)

    cfg.out_html.parent.mkdir(parents=True, exist_ok=True)
    cfg.out_html.write_text(html, encoding="utf-8")
    print("Wrote:", cfg.out_html.resolve())


if __name__ == "__main__":
    main()
