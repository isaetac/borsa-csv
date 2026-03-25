# borsa-csv

BIST, NASDAQ ve S&P 500 hisselerini çekip statik bir web tablosunda gösteren proje.

Site GitHub Pages üzerinde yayınlanır. Veri, GitHub Actions workflow’u ile üretilir ve dış tetikleme için cron-job.org kullanılabilir.

## Canlı Site

GitHub Pages adresi:

`https://isaetac.github.io/borsa-csv/`

---

## Ne yapar?

Bu proje:

- BIST, NASDAQ ve S&P 500 evrenini oluşturur
- Güncel fiyat/veri bilgisini çeker
- `public/latest.csv` üretir
- CSV verisini frontend’de tablo olarak gösterir
- GitHub Pages ile yayına verir

---

## Özellikler

Frontend tarafında:

- arama
- market filtresi
- yükselen / düşen / değişmeyen filtresi
- en çok yükselen 20
- en çok düşen 20
- en hacimli 20
- otomatik yenileme
- Türkçeleştirilmiş piyasa durumu
- sabit ilk sütunlar ve geliştirilmiş tablo kullanımı

Backend tarafında:

- `change_percent` provider’dan doğrudan alınmaz  
  `last` ve `prev_close` üzerinden hesaplanır
- `market_time` düzeltilip ISO tarih olarak yazılır
- veri alınamayan semboller ayrı raporlanır

---

## Proje Yapısı

```text
borsa-csv/
├─ build_universe.py
├─ build_quotes.py
├─ requirements.txt
├─ .github/
│  └─ workflows/
│     └─ update-quotes.yml
├─ data/
│  └─ universe.csv
└─ public/
   ├─ index.html
   ├─ latest.csv
   ├─ missing_symbols.csv
   └─ candidate_exclusions.csv
