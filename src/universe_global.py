from __future__ import annotations

from pathlib import Path

import pandas as pd


OUT = Path("data/universe_global.csv")


ROWS = [
    # APAC: large/liquid markets represented by major Yahoo Finance symbols.
    ("APAC", "Japan", "Japan", "7203.T", "Toyota Motor", "Consumer Cyclical"),
    ("APAC", "Japan", "Japan", "6758.T", "Sony Group", "Technology"),
    ("APAC", "Japan", "Japan", "8306.T", "Mitsubishi UFJ Financial Group", "Financial Services"),
    ("APAC", "Japan", "Japan", "9984.T", "SoftBank Group", "Communication Services"),
    ("APAC", "Japan", "Japan", "6861.T", "Keyence", "Technology"),
    ("APAC", "Japan", "Japan", "8035.T", "Tokyo Electron", "Technology"),
    ("APAC", "Japan", "Japan", "9432.T", "Nippon Telegraph and Telephone", "Communication Services"),
    ("APAC", "Japan", "Japan", "7974.T", "Nintendo", "Communication Services"),
    ("APAC", "India", "India", "RELIANCE.NS", "Reliance Industries", "Energy"),
    ("APAC", "India", "India", "TCS.NS", "Tata Consultancy Services", "Technology"),
    ("APAC", "India", "India", "HDFCBANK.NS", "HDFC Bank", "Financial Services"),
    ("APAC", "India", "India", "ICICIBANK.NS", "ICICI Bank", "Financial Services"),
    ("APAC", "India", "India", "INFY.NS", "Infosys", "Technology"),
    ("APAC", "India", "India", "BHARTIARTL.NS", "Bharti Airtel", "Communication Services"),
    ("APAC", "India", "India", "SBIN.NS", "State Bank of India", "Financial Services"),
    ("APAC", "India", "India", "LT.NS", "Larsen & Toubro", "Industrials"),
    ("APAC", "Hong Kong", "Hong Kong", "0700.HK", "Tencent Holdings", "Communication Services"),
    ("APAC", "Hong Kong", "Hong Kong", "9988.HK", "Alibaba Group", "Consumer Cyclical"),
    ("APAC", "Hong Kong", "Hong Kong", "0005.HK", "HSBC Holdings", "Financial Services"),
    ("APAC", "Hong Kong", "Hong Kong", "1299.HK", "AIA Group", "Financial Services"),
    ("APAC", "Hong Kong", "Hong Kong", "0939.HK", "China Construction Bank", "Financial Services"),
    ("APAC", "Hong Kong", "Hong Kong", "0941.HK", "China Mobile", "Communication Services"),
    ("APAC", "Hong Kong", "Hong Kong", "0388.HK", "Hong Kong Exchanges and Clearing", "Financial Services"),
    ("APAC", "Hong Kong", "Hong Kong", "3690.HK", "Meituan", "Consumer Cyclical"),
    # EMEA: London, Paris/Euronext, and Germany.
    ("EMEA", "United Kingdom", "United Kingdom", "AZN.L", "AstraZeneca", "Healthcare"),
    ("EMEA", "United Kingdom", "United Kingdom", "SHEL.L", "Shell", "Energy"),
    ("EMEA", "United Kingdom", "United Kingdom", "HSBA.L", "HSBC Holdings", "Financial Services"),
    ("EMEA", "United Kingdom", "United Kingdom", "ULVR.L", "Unilever", "Consumer Defensive"),
    ("EMEA", "United Kingdom", "United Kingdom", "BP.L", "BP", "Energy"),
    ("EMEA", "United Kingdom", "United Kingdom", "GSK.L", "GSK", "Healthcare"),
    ("EMEA", "United Kingdom", "United Kingdom", "RIO.L", "Rio Tinto", "Basic Materials"),
    ("EMEA", "United Kingdom", "United Kingdom", "LSEG.L", "London Stock Exchange Group", "Financial Services"),
    ("EMEA", "Germany", "Germany", "SAP.DE", "SAP", "Technology"),
    ("EMEA", "Germany", "Germany", "SIE.DE", "Siemens", "Industrials"),
    ("EMEA", "Germany", "Germany", "DTE.DE", "Deutsche Telekom", "Communication Services"),
    ("EMEA", "Germany", "Germany", "ALV.DE", "Allianz", "Financial Services"),
    ("EMEA", "Germany", "Germany", "MBG.DE", "Mercedes-Benz Group", "Consumer Cyclical"),
    ("EMEA", "Germany", "Germany", "BMW.DE", "BMW", "Consumer Cyclical"),
    ("EMEA", "Germany", "Germany", "BAS.DE", "BASF", "Basic Materials"),
    ("EMEA", "Germany", "Germany", "IFX.DE", "Infineon Technologies", "Technology"),
    ("EMEA", "France", "France", "MC.PA", "LVMH", "Consumer Cyclical"),
    ("EMEA", "France", "France", "OR.PA", "L'Oreal", "Consumer Defensive"),
    ("EMEA", "France", "France", "RMS.PA", "Hermes International", "Consumer Cyclical"),
    ("EMEA", "France", "France", "TTE.PA", "TotalEnergies", "Energy"),
    ("EMEA", "France", "France", "SAN.PA", "Sanofi", "Healthcare"),
    ("EMEA", "France", "France", "AIR.PA", "Airbus", "Industrials"),
    ("EMEA", "France", "France", "SU.PA", "Schneider Electric", "Industrials"),
    ("EMEA", "France", "France", "BNP.PA", "BNP Paribas", "Financial Services"),
    # LAC: Brazil, Mexico, and Chile.
    ("LAC", "Brazil", "Brazil", "PETR4.SA", "Petrobras PN", "Energy"),
    ("LAC", "Brazil", "Brazil", "VALE3.SA", "Vale", "Basic Materials"),
    ("LAC", "Brazil", "Brazil", "ITUB4.SA", "Itau Unibanco PN", "Financial Services"),
    ("LAC", "Brazil", "Brazil", "BBDC4.SA", "Banco Bradesco PN", "Financial Services"),
    ("LAC", "Brazil", "Brazil", "ABEV3.SA", "Ambev", "Consumer Defensive"),
    ("LAC", "Brazil", "Brazil", "BBAS3.SA", "Banco do Brasil", "Financial Services"),
    ("LAC", "Brazil", "Brazil", "WEGE3.SA", "WEG", "Industrials"),
    ("LAC", "Brazil", "Brazil", "B3SA3.SA", "B3", "Financial Services"),
    ("LAC", "Mexico", "Mexico", "AMXB.MX", "America Movil", "Communication Services"),
    ("LAC", "Mexico", "Mexico", "WALMEX.MX", "Wal-Mart de Mexico", "Consumer Defensive"),
    ("LAC", "Mexico", "Mexico", "GMEXICOB.MX", "Grupo Mexico", "Basic Materials"),
    ("LAC", "Mexico", "Mexico", "FEMSAUBD.MX", "Fomento Economico Mexicano", "Consumer Defensive"),
    ("LAC", "Mexico", "Mexico", "GFNORTEO.MX", "Grupo Financiero Banorte", "Financial Services"),
    ("LAC", "Mexico", "Mexico", "CEMEXCPO.MX", "Cemex", "Basic Materials"),
    ("LAC", "Mexico", "Mexico", "BIMBOA.MX", "Grupo Bimbo", "Consumer Defensive"),
    ("LAC", "Mexico", "Mexico", "KOFUBL.MX", "Coca-Cola FEMSA", "Consumer Defensive"),
    ("LAC", "Chile", "Chile", "SQM-B.SN", "Sociedad Quimica y Minera de Chile", "Basic Materials"),
    ("LAC", "Chile", "Chile", "CHILE.SN", "Banco de Chile", "Financial Services"),
    ("LAC", "Chile", "Chile", "BSANTANDER.SN", "Banco Santander Chile", "Financial Services"),
    ("LAC", "Chile", "Chile", "COPEC.SN", "Empresas Copec", "Energy"),
    ("LAC", "Chile", "Chile", "FALABELLA.SN", "Falabella", "Consumer Cyclical"),
    ("LAC", "Chile", "Chile", "CENCOSUD.SN", "Cencosud", "Consumer Defensive"),
    ("LAC", "Chile", "Chile", "ENELCHILE.SN", "Enel Chile", "Utilities"),
    ("LAC", "Chile", "Chile", "CMPC.SN", "CMPC", "Basic Materials"),
]


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        ROWS,
        columns=["region", "market", "country", "ticker_yahoo", "security", "sector"],
    )
    df["ticker_original"] = df["ticker_yahoo"]
    df["sub_industry"] = df["market"]
    df = df[
        [
            "ticker_yahoo",
            "ticker_original",
            "security",
            "sector",
            "sub_industry",
            "region",
            "market",
            "country",
        ]
    ]
    df.to_csv(OUT, index=False)
    print(f"Wrote {OUT} ({len(df)} tickers)")
    print(df.groupby(["region", "market"]).size())


if __name__ == "__main__":
    main()
