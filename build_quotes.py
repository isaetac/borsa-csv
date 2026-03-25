from datetime import datetime, timezone
from pathlib import Path
import time

import pandas as pd
from yahooquery import Ticker

DATA_DIR = Path("data")
PUBLIC_DIR = Path("public")
PUBLIC_DIR.mkdir(exist_ok=True)

UNIVERSE_CSV = DATA_DIR / "universe.csv"
EXCLUDED_CSV = DATA_DIR / "excluded_symbols.csv"

LATEST_CSV = PUBLIC_DIR / "latest.csv"
MISSING_CSV = PUBLIC_DIR / "missing_symbols.csv"
CANDIDATE_EXCLUSIONS_CSV = PUBLIC_DIR / "candidate_exclusions.csv"

FIRST_PASS_BATCH_SIZE = 1400
SECOND_PASS_BATCH_SIZE = 80

FIRST_PASS_SLEEP = 2.0
SECOND_PASS_SLEEP = 2.0


def chunked(items: list[str], size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def safe_get(d: dict, key: str):
    try:
        return d.get(key)
    except Exception:
        return None


def to_float(v):
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def to_int(v):
    try:
        if v is None or v == "":
            return None
        return int(float(v))
    except Exception:
        return None


def normalize_row(sym: str, item: dict) -> dict:
    last = to_float(safe_get(item, "regularMarketPrice"))
    prev_close = to_float(safe_get(item, "regularMarketPreviousClose"))
    open_ = to_float(safe_get(item, "regularMarketOpen"))
    high = to_float(safe_get(item, "regularMarketDayHigh"))
    low = to_float(safe_get(item, "regularMarketDayLow"))
    volume = to_int(safe_get(item, "regularMarketVolume"))

    change = None
    change_percent = None

    if last is not None and prev_close not in (None, 0):
        change = last - prev_close
        change_percent = (change / prev_close) * 100

    market_time_raw = safe_get(item, "regularMarketTime")
    market_time = None

    if market_time_raw is not None and market_time_raw != "":
        try:
            market_time = datetime.fromtimestamp(
                int(market_time_raw), tz=timezone.utc
            ).isoformat()
        except Exception:
            market_time = str(market_time_raw)

    return {
        "quote_symbol": sym,
        "display_name_quote": safe_get(item, "longName") or safe_get(item, "shortName"),
        "last": last,
        "change": change,
        "change_percent": change_percent,
        "volume": volume,
        "open": open_,
        "high": high,
        "low": low,
        "prev_close": prev_close,
        "currency": safe_get(item, "currency"),
        "exchange_name": safe_get(item, "exchangeName"),
        "exchange_code": safe_get(item, "exchange"),
        "market_state": safe_get(item, "marketState"),
        "market_time": market_time,
    }


def fetch_batch_quotes(symbols: list[str]) -> list[dict]:
    t = Ticker(
        symbols,
        asynchronous=True,
        max_workers=8,
        timeout=10,
        validate=False,
    )

    payload = t.quotes
    rows = []

    if not isinstance(payload, dict):
        return rows

    for sym in symbols:
        item = payload.get(sym)
        if not isinstance(item, dict):
            continue
        rows.append(normalize_row(sym, item))

    return rows


def fetch_batch_price(symbols: list[str]) -> list[dict]:
    t = Ticker(
        symbols,
        asynchronous=True,
        max_workers=8,
        timeout=10,
        validate=False,
    )

    payload = t.price
    rows = []

    if not isinstance(payload, dict):
        return rows

    for sym in symbols:
        item = payload.get(sym)
        if not isinstance(item, dict):
            continue
        rows.append(normalize_row(sym, item))

    return rows


def load_excluded_symbols() -> set[str]:
    if not EXCLUDED_CSV.exists():
        return set()

    try:
        df = pd.read_csv(EXCLUDED_CSV)
        if "quote_symbol" not in df.columns:
            return set()
        return set(df["quote_symbol"].dropna().astype(str).str.strip())
    except Exception:
        return set()


def first_pass(symbols: list[str]) -> pd.DataFrame:
    all_rows = []
    batches = list(chunked(symbols, FIRST_PASS_BATCH_SIZE))
    print(f"1. geçiş (quotes) | sembol={len(symbols)} | batch={len(batches)}")

    for i, batch in enumerate(batches, start=1):
        try:
            rows = fetch_batch_quotes(batch)
            all_rows.extend(rows)
            print(f"[quotes {i}/{len(batches)}] batch={len(batch)} | veri={len(rows)}")
        except Exception as e:
            print(f"[quotes {i}/{len(batches)}] hata -> {e}")

        time.sleep(FIRST_PASS_SLEEP)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    return df.drop_duplicates(subset=["quote_symbol"], keep="first")


def second_pass(symbols: list[str]) -> pd.DataFrame:
    all_rows = []
    batches = list(chunked(symbols, SECOND_PASS_BATCH_SIZE))
    print(f"2. geçiş (price fallback) | sembol={len(symbols)} | batch={len(batches)}")

    for i, batch in enumerate(batches, start=1):
        try:
            rows = fetch_batch_price(batch)
            all_rows.extend(rows)
            print(f"[price {i}/{len(batches)}] batch={len(batch)} | veri={len(rows)}")
        except Exception as e:
            print(f"[price {i}/{len(batches)}] hata -> {e}")

        time.sleep(SECOND_PASS_SLEEP)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    return df.drop_duplicates(subset=["quote_symbol"], keep="first")


def build_output(universe: pd.DataFrame, quotes: pd.DataFrame) -> pd.DataFrame:
    merged = universe.merge(quotes, on="quote_symbol", how="left")

    merged["display_name"] = merged["display_name_quote"].fillna(merged["name"])

    merged["market"] = merged.apply(
        lambda r: "BIST"
        if bool(r["is_bist"])
        else ("NASDAQ" if bool(r["is_nasdaq"]) else ("SP500" if bool(r["is_sp500"]) else r["country"])),
        axis=1,
    )

    merged["currency"] = merged["currency"].fillna(
        merged["country"].map({"TR": "TRY", "US": "USD"})
    )

    merged["exchange_name"] = merged["exchange_name"].fillna(
        merged["market"].map(
            {
                "BIST": "Borsa Istanbul",
                "NASDAQ": "NASDAQ",
                "SP500": "US Market",
            }
        )
    )

    merged["exchange_code"] = merged["exchange_code"].fillna(
        merged["market"].map(
            {
                "BIST": "XIST",
                "NASDAQ": "XNAS",
                "SP500": "US",
            }
        )
    )

    merged["exchange_tz"] = merged["country"].map(
        {"TR": "Europe/Istanbul", "US": "America/New_York"}
    )

    merged["retrieved_at_utc"] = datetime.now(timezone.utc).isoformat()

    final_cols = [
        "market",
        "symbol",
        "quote_symbol",
        "display_name",
        "country",
        "universe_tags",
        "last",
        "change",
        "change_percent",
        "volume",
        "open",
        "high",
        "low",
        "prev_close",
        "currency",
        "exchange_name",
        "exchange_code",
        "exchange_tz",
        "market_state",
        "market_time",
        "retrieved_at_utc",
    ]

    final = merged[final_cols].copy()
    final = final.sort_values(["market", "symbol"]).reset_index(drop=True)
    return final


def main() -> None:
    if not UNIVERSE_CSV.exists():
        raise FileNotFoundError("Önce build_universe.py çalıştır.")

    universe = pd.read_csv(UNIVERSE_CSV)

    excluded = load_excluded_symbols()
    if excluded:
        before = len(universe)
        universe = universe[~universe["quote_symbol"].astype(str).isin(excluded)].copy()
        print(f"excluded_symbols.csv uygulandı | çıkarılan={before - len(universe)}")

    symbols = (
        universe["quote_symbol"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    quotes_1 = first_pass(symbols)

    if quotes_1.empty:
        raise RuntimeError("İlk geçişten hiç veri dönmedi.")

    got_price = set(
        quotes_1.loc[quotes_1["last"].notna(), "quote_symbol"].astype(str).tolist()
    )
    missing_after_first = [s for s in symbols if s not in got_price]

    print(f"İlk geçiş sonrası eksik sembol: {len(missing_after_first)}")

    quotes_2 = pd.DataFrame()
    if missing_after_first:
        quotes_2 = second_pass(missing_after_first)

    if not quotes_2.empty:
        quotes = pd.concat([quotes_1, quotes_2], ignore_index=True)
        quotes = quotes.drop_duplicates(subset=["quote_symbol"], keep="last")
    else:
        quotes = quotes_1.copy()

    final = build_output(universe, quotes)
    final.to_csv(LATEST_CSV, index=False, encoding="utf-8-sig")

    missing = final[final["last"].isna()][
        ["market", "symbol", "quote_symbol", "display_name", "universe_tags"]
    ].copy()
    missing.to_csv(MISSING_CSV, index=False, encoding="utf-8-sig")
    missing.to_csv(CANDIDATE_EXCLUSIONS_CSV, index=False, encoding="utf-8-sig")

    filled = final["last"].notna().sum()
    print(f"\nKaydedildi: {LATEST_CSV}")
    print(f"Eksikler: {MISSING_CSV}")
    print(f"Aday hariç tutulacaklar: {CANDIDATE_EXCLUSIONS_CSV}")
    print(f"Toplam satır: {len(final)}")
    print(f"Fiyat dolu satır: {filled}")
    print(f"Boş satır: {len(final) - filled}")


if __name__ == "__main__":
    main()