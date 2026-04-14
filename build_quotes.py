from datetime import datetime, timedelta, timezone
from pathlib import Path
import time

import pandas as pd
import requests
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

TEFAS_HISTORY_URL = "https://www.tefas.gov.tr/api/DB/BindHistoryInfo"
TEFAS_REFERER = "https://www.tefas.gov.tr/TarihselVeriler.aspx"
TEFAS_CACHE_CSV = DATA_DIR / "tefas_cache.csv"


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


def _make_tefas_session() -> requests.Session:
    """TEFAS API için geçerli session cookie'si olan oturum oluşturur."""
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
        }
    )
    s.get(TEFAS_REFERER, timeout=30)
    return s


def fetch_tefas_quotes(fund_codes: list[str]) -> pd.DataFrame:
    """TEFAS API'den son işlem gününün NAV fiyatlarını çeker.

    Son 7 gün içindeki veriyi alıp fon başına en yeni iki günü kullanarak
    günlük değişimi hesaplar (hafta sonu / tatil günlerini otomatik atlar).
    TARIH alanı ms cinsinden Unix timestamp olarak gelir.
    """
    if not fund_codes:
        return pd.DataFrame()

    today = datetime.now(timezone.utc).date()
    from_date = today - timedelta(days=7)

    s = _make_tefas_session()
    api_headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": TEFAS_REFERER,
        "Origin": "https://www.tefas.gov.tr",
    }

    all_rows: list[dict] = []
    for fontip in ("YAT", "BYF"):
        try:
            resp = s.post(
                TEFAS_HISTORY_URL,
                data={
                    "fontip": fontip,
                    "sfonkod": "",
                    "bastarih": from_date.strftime("%d.%m.%Y"),
                    "bittarih": today.strftime("%d.%m.%Y"),
                },
                headers=api_headers,
                timeout=60,
            )
            resp.raise_for_status()
            payload = resp.json()
            all_rows.extend(payload.get("data", []))
        except Exception as e:
            print(f"TEFAS history {fontip} hatası: {e}")

    if not all_rows:
        print("TEFAS'tan hiç geçmiş veri alınamadı, cache kontrol ediliyor...")
        return _load_tefas_cache(fund_codes)

    df = pd.DataFrame(all_rows)
    df.columns = [str(c).strip().upper() for c in df.columns]

    if "FIYAT" not in df.columns or "FONKODU" not in df.columns or "TARIH" not in df.columns:
        print(f"TEFAS history: beklenen kolonlar eksik ({list(df.columns)})")
        return pd.DataFrame()

    # TARIH: ms cinsinden Unix timestamp string'i
    df["TARIH"] = pd.to_datetime(
        df["TARIH"].astype(str).str.strip().astype("int64"), unit="ms", utc=True
    )
    df["FIYAT"] = pd.to_numeric(df["FIYAT"], errors="coerce")
    df = df.dropna(subset=["TARIH", "FIYAT"]).copy()
    df = df.sort_values(["FONKODU", "TARIH"])

    fund_set = set(fund_codes)
    result_rows = []
    for kod, group in df.groupby("FONKODU"):
        if kod not in fund_set:
            continue

        last_row = group.iloc[-1]
        last = float(last_row["FIYAT"])
        prev_close = float(group.iloc[-2]["FIYAT"]) if len(group) >= 2 else None

        change = None
        change_pct = None
        if prev_close is not None and prev_close != 0:
            change = last - prev_close
            change_pct = (change / prev_close) * 100

        result_rows.append(
            {
                "quote_symbol": str(kod),
                "display_name_quote": None,
                "last": last,
                "change": change,
                "change_percent": change_pct,
                "volume": None,
                "open": None,
                "high": None,
                "low": None,
                "prev_close": prev_close,
                "currency": "TRY",
                "exchange_name": "TEFAS",
                "exchange_code": "TEFAS",
                "market_state": "CLOSED",
                "market_time": last_row["TARIH"].isoformat(),
            }
        )

    result = pd.DataFrame(result_rows)
    print(f"TEFAS: {len(result_rows)} fon fiyatı alındı.")
    _save_tefas_cache(result)
    return result


def _save_tefas_cache(df: pd.DataFrame) -> None:
    if df.empty:
        return
    try:
        df.to_csv(TEFAS_CACHE_CSV, index=False, encoding="utf-8-sig")
        print(f"TEFAS cache güncellendi: {TEFAS_CACHE_CSV}")
    except Exception as e:
        print(f"TEFAS cache yazılamadı: {e}")


def _load_tefas_cache(fund_codes: list[str]) -> pd.DataFrame:
    if not TEFAS_CACHE_CSV.exists():
        print("TEFAS cache bulunamadı.")
        return pd.DataFrame()
    try:
        df = pd.read_csv(TEFAS_CACHE_CSV)
        fund_set = set(fund_codes)
        df = df[df["quote_symbol"].astype(str).isin(fund_set)].copy()
        print(f"TEFAS cache kullanılıyor: {len(df)} fon (cache: {TEFAS_CACHE_CSV})")
        return df
    except Exception as e:
        print(f"TEFAS cache okunamadı: {e}")
        return pd.DataFrame()


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

    def _market(r) -> str:
        if bool(r.get("is_tefas")):
            return "TEFAS"
        if bool(r["is_bist"]):
            return "BIST"
        if bool(r["is_nasdaq"]):
            return "NASDAQ"
        if bool(r["is_sp500"]):
            return "SP500"
        return r["country"]

    merged["market"] = merged.apply(_market, axis=1)

    merged["currency"] = merged["currency"].fillna(
        merged["country"].map({"TR": "TRY", "US": "USD"})
    )

    merged["exchange_name"] = merged["exchange_name"].fillna(
        merged["market"].map(
            {
                "BIST": "Borsa Istanbul",
                "NASDAQ": "NASDAQ",
                "SP500": "US Market",
                "TEFAS": "TEFAS",
            }
        )
    )

    merged["exchange_code"] = merged["exchange_code"].fillna(
        merged["market"].map(
            {
                "BIST": "XIST",
                "NASDAQ": "XNAS",
                "SP500": "US",
                "TEFAS": "TEFAS",
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

    is_tefas_col = universe.get("is_tefas", pd.Series(False, index=universe.index))
    tefas_mask = is_tefas_col.astype(bool)

    tefas_universe = universe[tefas_mask].copy()
    yf_universe = universe[~tefas_mask].copy()

    tefas_symbols = (
        tefas_universe["quote_symbol"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    symbols = (
        yf_universe["quote_symbol"]
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

    print(f"\nTEFAS fon fiyatları alınıyor ({len(tefas_symbols)} fon)...")
    tefas_quotes = fetch_tefas_quotes(tefas_symbols)

    all_quotes = pd.concat([quotes, tefas_quotes], ignore_index=True) if not tefas_quotes.empty else quotes
    all_quotes = all_quotes.drop_duplicates(subset=["quote_symbol"], keep="last")

    final = build_output(universe, all_quotes)
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
