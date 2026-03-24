from datetime import datetime
from pathlib import Path

import pandas as pd

PUBLIC_DIR = Path("public")
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

MISSING_CSV = PUBLIC_DIR / "missing_symbols.csv"
EXCLUDED_CSV = DATA_DIR / "excluded_symbols.csv"


def main():
    if not MISSING_CSV.exists():
        raise FileNotFoundError(f"{MISSING_CSV} bulunamadı.")

    missing = pd.read_csv(MISSING_CSV)

    if missing.empty:
        print("missing_symbols.csv boş. Eklenecek sembol yok.")
        return

    required_cols = ["quote_symbol"]
    for col in required_cols:
        if col not in missing.columns:
            raise RuntimeError(f"missing_symbols.csv içinde '{col}' kolonu yok.")

    missing = missing.copy()
    missing["quote_symbol"] = missing["quote_symbol"].fillna("").astype(str).str.strip()
    missing = missing[missing["quote_symbol"] != ""].copy()

    missing["added_at"] = datetime.now().isoformat(timespec="seconds")
    missing["reason"] = "auto_excluded_from_missing_symbols"

    keep_cols = ["quote_symbol", "market", "symbol", "display_name", "universe_tags", "reason", "added_at"]
    for col in keep_cols:
        if col not in missing.columns:
            missing[col] = ""

    missing = missing[keep_cols].copy()

    if EXCLUDED_CSV.exists():
        existing = pd.read_csv(EXCLUDED_CSV)
        for col in keep_cols:
            if col not in existing.columns:
                existing[col] = ""
        combined = pd.concat([existing[keep_cols], missing], ignore_index=True)
    else:
        combined = missing.copy()

    combined["quote_symbol"] = combined["quote_symbol"].fillna("").astype(str).str.strip()
    combined = combined[combined["quote_symbol"] != ""].copy()
    combined = combined.drop_duplicates(subset=["quote_symbol"], keep="first")
    combined = combined.sort_values(["market", "symbol", "quote_symbol"]).reset_index(drop=True)

    combined.to_csv(EXCLUDED_CSV, index=False, encoding="utf-8-sig")

    print(f"Kaydedildi: {EXCLUDED_CSV}")
    print(f"Toplam dışlanan sembol: {len(combined)}")
    print(f"Bu tur eklenen: {len(missing)}")


if __name__ == "__main__":
    main()