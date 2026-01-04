# STARE — Sector & Stock Trend Analysis Engine

**STARE** is a Python-based analytics pipeline that analyzes the S&P 500 universe using market data from Yahoo Finance.  
It computes **sector sentiment**, **top active stocks**, **fundamentals**, and produces **machine-readable and human-readable reports**, including a hosted HTML dashboard via GitHub Pages.

The project is designed to be:

- reproducible
- automated
- database-backed (SQLite)
- ready for future Django integration

---

## Features

- 📈 **S&P 500 universe ingestion**
- 💰 **Daily OHLCV price collection**
- 📊 **Weekly momentum & liquidity metrics**
- 🧭 **Sector sentiment scoring (Bullish / Bearish + strength)**
- 🔥 **Top 10 most active stocks per sector**
- 🧾 **Fundamentals snapshot for entire S&P 500**
- 📄 **Dashboard outputs**
  - JSON (API-ready)
  - CSV (analysis-friendly)
  - HTML (GitHub Pages hosted)
- 🤖 **Automated weekday pipeline via GitHub Actions**

---

## Repository Structure

```
stare/
├── src/
├── data/
├── reports/
├── docs/
├── .github/workflows/
├── requirements.txt
└── README.md
```

---

## Setup

```bash
git clone https://github.com/vittok/stare.git
cd stare
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Pipeline Steps

1. Universe ingestion  
2. Price fetching  
3. Weekly statistics  
4. Sector sentiment & top active stocks  
5. Fundamentals (S&P 500)  
6. Dashboard (JSON / CSV / HTML)  
7. GitHub Actions automation

---

## GitHub Pages

Dashboard available at:

https://vittok.github.io/stare/

---

## Disclaimer

Educational use only. Not investment advice.

---

## License

MIT License
