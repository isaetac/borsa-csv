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
````

### Dosyalar ne işe yarar?

#### `build_universe.py`

Hisse evrenini oluşturur.

Kaynaklar:

* BIST
* NASDAQ
* S&P 500

Çıktı:

* `data/universe.csv`

#### `build_quotes.py`

Evren içindeki semboller için fiyat/veri çeker.

Çıktılar:

* `public/latest.csv`
* `public/missing_symbols.csv`
* `public/candidate_exclusions.csv`

#### `public/index.html`

Tablo arayüzüdür. CSV’yi okuyup kullanıcıya gösterir.

#### `.github/workflows/update-quotes.yml`

Veriyi güncelleyen ve siteyi deploy eden GitHub Actions workflow dosyasıdır.

---

## Veri Akışı

Akış basitçe şöyledir:

1. `build_universe.py` çalışır
2. `build_quotes.py` fiyatları çeker
3. `public/latest.csv` üretilir
4. GitHub Pages güncel dosyaları yayınlar
5. `public/index.html` bu CSV’yi okuyup tabloyu gösterir

---

## Çalıştırma

### Gereksinimler

* Python 3.11+
* pip

### Kurulum

```bash
pip install -r requirements.txt
```

### Evren oluşturma

```bash
python build_universe.py
```

### Fiyatları çekme

```bash
python build_quotes.py
```

Sonrasında güncel veri burada oluşur:

```text
public/latest.csv
```

---

## Yayınlama Mantığı

Bu repo GitHub Pages üzerinde yayınlanır.

Pages ayarı:

* **Source:** GitHub Actions

Workflow:

* veriyi üretir
* gerekirse commit/push yapar
* siteyi deploy eder

---

## Otomasyon

GitHub’ın kendi `schedule` tetikleyicisi bazen güvenilmez davranabildiği için dış tetikleme kullanılabilir.

Bu projede mantık şu olabilir:

* cron-job.org belirli aralıklarla çalışır
* GitHub API üzerinden `workflow_dispatch` çağrılır
* workflow veriyi yeniler
* Pages güncellenir

Zaman dilimi:

* `Europe/Istanbul`

Örnek kullanım:

* test için 5 dakikada bir
* normal kullanım için 15 dakikada bir

---

## Önemli Notlar

### 1) History özelliği kaldırıldı

Bu projede geçmiş veri biriktiren `history` yapısı **bilinçli olarak kaldırıldı**.

Sebep:

* gereksiz repo şişmesi
* GitHub 100 MB limitine takılma riski
* bakım yükünü artırması

Yani proje şu anda yalnızca **güncel snapshot** mantığında çalışır.

### 2) Hacim gösterimi

Frontend tarafında hacim alanı doğrudan lot yerine görsel kullanım için `price * volume` mantığında gösterilebilir.

### 3) Veri eksikleri

Veri gelmeyen semboller burada raporlanır:

* `public/missing_symbols.csv`

Dışlanması düşünülebilecek semboller burada tutulur:

* `public/candidate_exclusions.csv`

---

## Sorun Giderme

### Site güncellenmiyor

Kontrol et:

* workflow başarılı mı?
* `public/latest.csv` gerçekten yenilenmiş mi?
* Pages deploy tamamlanmış mı?
* tarayıcı cache yüzünden eski dosya mı görünüyor?

Gerekirse:

* sayfayı `Ctrl + F5` ile sert yenile

### Bazı sembollerde veri yok

Bu normal olabilir. Kontrol et:

* `public/missing_symbols.csv`
* `public/candidate_exclusions.csv`

### Repo büyüyor

History sistemi kapalı olmalı. Büyük CSV/log/artifact birikimi olmamalı.

---

## Geliştirme Notları

Bu proje aktif geliştirme mantığıyla ilerliyor. En kritik alanlar:

* veri güvenilirliği
* frontend kullanım kolaylığı
* tablo performansı
* sembol temizliği / evren kalitesi
* otomasyon stabilitesi

---

## Katkı / Kullanım

Repo’yu inceleyen biri için temel mantık şudur:

1. önce `build_universe.py`
2. sonra `build_quotes.py`
3. çıktı `public/latest.csv`
4. arayüz `public/index.html`
5. yayın GitHub Pages üzerinden

---

## Lisans

İstenirse daha sonra eklenebilir.

```

