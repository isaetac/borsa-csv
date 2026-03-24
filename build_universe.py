import re
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

NASDAQ_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
BIST_FALLBACK_CSV = "https://cdn.jsdelivr.net/gh/ahmeterenodaci/Istanbul-Stock-Exchange--BIST--including-symbols-and-logos/without_logo.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9,tr;q=0.8",
}


def normalize_us_symbol(symbol) -> str:
    if pd.isna(symbol):
        return ""
    symbol = str(symbol).strip()
    if not symbol:
        return ""
    return symbol.replace(".", "-")


def clean_text(value) -> str:
    if pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def is_equity_like_nasdaq_name(name: str) -> bool:
    n = clean_text(name).lower()

    blocked_terms = [
        "warrant",
        "rights",
        " right",
        "unit",
        "units",
        "preferred",
        "depositary receipt",
        "etf",
        "etn",
        "fund",
        "trust",
        "notes",
        "note",
        "debenture",
        "bond",
    ]
    if any(term in n for term in blocked_terms):
        return False

    good_terms = [
        "common stock",
        "ordinary shares",
        "ordinary share",
        "common shares",
        "common share",
        "class a common",
        "class b common",
        "class c common",
        "american depositary shares",
        "ads",
        "adr",
    ]
    return any(term in n for term in good_terms)


def fetch_nasdaq() -> pd.DataFrame:
    resp = requests.get(NASDAQ_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    df = pd.read_csv(StringIO(resp.text), sep="|")
    df = df[df["Symbol"] != "File Creation Time"].copy()

    if "Test Issue" in df.columns:
        df = df[df["Test Issue"] == "N"]
    if "ETF" in df.columns:
        df = df[df["ETF"] == "N"]

    df["Symbol"] = df["Symbol"].fillna("").astype(str).str.strip()
    df["Security Name"] = df["Security Name"].fillna("").astype(str).map(clean_text)

    df = df[df["Symbol"] != ""].copy()
    df = df[df["Security Name"] != ""].copy()
    df = df[df["Security Name"].map(is_equity_like_nasdaq_name)].copy()

    out = pd.DataFrame(
        {
            "symbol": df["Symbol"],
            "quote_symbol": df["Symbol"].map(normalize_us_symbol),
            "name": df["Security Name"],
            "country": "US",
            "is_bist": False,
            "is_nasdaq": True,
            "is_sp500": False,
        }
    )

    out = out[out["quote_symbol"] != ""].copy()
    return out.drop_duplicates(subset=["quote_symbol"]).reset_index(drop=True)


def fetch_bist() -> pd.DataFrame:
    resp = requests.get(BIST_FALLBACK_CSV, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    df = pd.read_csv(StringIO(resp.text))
    df.columns = [str(c).strip().lower() for c in df.columns]

    if "symbol" not in df.columns or "name" not in df.columns:
        raise RuntimeError(f"BIST fallback CSV beklenen kolonları vermedi: {list(df.columns)}")

    df["symbol"] = df["symbol"].fillna("").astype(str).str.strip().str.upper()
    df["name"] = df["name"].fillna("").astype(str).map(clean_text)

    df = df[df["symbol"] != ""].copy()
    df = df[df["name"] != ""].copy()
    df = df[df["symbol"].str.fullmatch(r"[A-Z0-9]{2,6}")].copy()

    out = pd.DataFrame(
        {
            "symbol": df["symbol"],
            "quote_symbol": df["symbol"] + ".IS",
            "name": df["name"],
            "country": "TR",
            "is_bist": True,
            "is_nasdaq": False,
            "is_sp500": False,
        }
    )

    if out.empty:
        raise RuntimeError("BIST fallback CSV boş geldi.")

    return out.drop_duplicates(subset=["quote_symbol"]).reset_index(drop=True)


def fetch_sp500() -> pd.DataFrame:
    resp = requests.get(SP500_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    tables = pd.read_html(StringIO(resp.text))
    if not tables:
        raise RuntimeError("S&P 500 tablosu bulunamadı.")

    df = tables[0].copy()

    symbol_col = "Symbol"
    name_col = "Security"

    df[symbol_col] = df[symbol_col].fillna("").astype(str).str.strip()
    df[name_col] = df[name_col].fillna("").astype(str).map(clean_text)

    df = df[df[symbol_col] != ""].copy()
    df = df[df[name_col] != ""].copy()

    out = pd.DataFrame(
        {
            "symbol": df[symbol_col],
            "quote_symbol": df[symbol_col].map(normalize_us_symbol),
            "name": df[name_col],
            "country": "US",
            "is_bist": False,
            "is_nasdaq": False,
            "is_sp500": True,
        }
    )

    out = out[out["quote_symbol"] != ""].copy()
    return out.drop_duplicates(subset=["quote_symbol"]).reset_index(drop=True)


def main() -> None:
    print("NASDAQ listesi alınıyor...")
    nasdaq = fetch_nasdaq()

    print("BIST listesi alınıyor...")
    bist = fetch_bist()

    print("S&P 500 listesi alınıyor...")
    sp500 = fetch_sp500()

    us = pd.concat([nasdaq, sp500], ignore_index=True)
    us = (
        us.groupby("quote_symbol", as_index=False)
        .agg(
            {
                "symbol": "first",
                "name": "first",
                "country": "first",
                "is_bist": "max",
                "is_nasdaq": "max",
                "is_sp500": "max",
            }
        )
    )

    all_df = pd.concat([bist, us], ignore_index=True)

    all_df["universe_tags"] = all_df.apply(
        lambda r: ";".join(
            [
                tag
                for cond, tag in [
                    (r["is_bist"], "BIST"),
                    (r["is_nasdaq"], "NASDAQ"),
                    (r["is_sp500"], "SP500"),
                ]
                if cond
            ]
        ),
        axis=1,
    )

    all_df = all_df[
        [
            "symbol",
            "quote_symbol",
            "name",
            "country",
            "is_bist",
            "is_nasdaq",
            "is_sp500",
            "universe_tags",
        ]
    ].sort_values(["country", "symbol"])

    out_path = DATA_DIR / "universe.csv"
    all_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"Kaydedildi: {out_path}")
    print(f"Toplam kayıt: {len(all_df)}")
    print(f"BIST: {int(all_df['is_bist'].sum())}")
    print(f"NASDAQ: {int(all_df['is_nasdaq'].sum())}")
    print(f"SP500: {int(all_df['is_sp500'].sum())}")


if __name__ == "__main__":
    main()