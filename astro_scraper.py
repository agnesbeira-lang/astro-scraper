"""Scraper Astro (API). Port dari notebook astro_api_scraper.ipynb (Cell 2 + Cell 3).

Perubahan v2 (supaya tidak berhenti di ~10.000 produk):
  * SWEEP default = "always".
  * API membatasi hasil tiap pencarian ke API_CAP (10.000 = 100 halaman x 100). Semua kata kunci a-z/0-9
    yang saling tumpang-tindih bisa mengembalikan 10.000 produk teratas yang SAMA, jadi total tidak naik.
  * Sapuan lanjutan ("deep"): memakai kata-kata dari nama produk yang sudah ditemukan sebagai kata kunci baru
    (merek, jenis produk, ukuran), berulang sampai tidak ada produk baru. Pencarian yang spesifik
    jumlah hasilnya di bawah batas, sehingga produk yang tidak masuk 10.000 teratas ikut terjaring.
  * Pencarian yang sudah pasti tidak menambah produk baru dihentikan setelah halaman pertama (hemat waktu).
  * Halaman di luar jendela API (HTTP 400 di halaman > 0) tidak lagi membuat scraper berhenti.
  * Batas waktu (ASTRO_TIME_BUDGET_MIN) supaya job tidak melewati batas GitHub Actions.
"""
import json
import os
import random
import re
import secrets
import time
from collections import Counter
from datetime import datetime, timezone, timedelta

import requests

# ── Config ─────────────────────────────────────────────────────────────────
API_BASE     = os.getenv("ASTRO_API_BASE", "https://api.astronauts.id")
SITE_BASE    = "https://www.astronauts.id"
LOCATION_IDS = [int(x) for x in os.getenv("ASTRO_LOCATION_IDS", "591").split(",")]
PAGE_SIZE    = int(os.getenv("ASTRO_PAGE_SIZE", "100"))   # 100 OK, 200 ditolak (HTTP 400)
MAX_PAGES    = int(os.getenv("ASTRO_MAX_PAGES", "0"))     # 0 = semua
HARD_CAP     = 1000                                       # pengaman: maks halaman per kata kunci
SWEEP        = os.getenv("ASTRO_SWEEP", "always")         # auto | always | never
API_CAP      = int(os.getenv("ASTRO_API_CAP", "10000"))   # batas hasil per pencarian di sisi API
DEEP         = os.getenv("ASTRO_DEEP", "1") == "1"        # sapuan lanjutan dari kata di nama produk
MAX_ROUNDS   = int(os.getenv("ASTRO_MAX_ROUNDS", "6"))    # maks ronde sapuan lanjutan
MIN_TOKEN_LEN = int(os.getenv("ASTRO_MIN_TOKEN_LEN", "3"))
TIME_BUDGET_MIN = float(os.getenv("ASTRO_TIME_BUDGET_MIN", "150"))   # 0 = tanpa batas waktu
DELAY        = (float(os.getenv("ASTRO_DELAY_MIN", "0.4")), float(os.getenv("ASTRO_DELAY_MAX", "1.0")))
KEYWORDS     = list("abcdefghijklmnopqrstuvwxyz0123456789")

DEVICE_ID = os.getenv("ASTRO_DEVICE_ID") or secrets.token_hex(32)
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "id-ID,id;q=0.9,en;q=0.8",
    "origin": SITE_BASE,
    "referer": SITE_BASE + "/",
    "user-agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36"),
    "x-api-version": os.getenv("ASTRO_API_VERSION", "1.9.11"),
    "x-device": "web-customer",
    "x-device-id": DEVICE_ID,
    "x-device-version": os.getenv("ASTRO_DEVICE_VERSION", "3.4.9"),
}

OUT_DIR = "data"
WIB     = timezone(timedelta(hours=7))
TODAY   = datetime.now(WIB).strftime("%Y-%m-%d")
os.makedirs(OUT_DIR, exist_ok=True)

BUDGET_HIT = False      # True kalau sapuan dihentikan karena batas waktu (hasil mungkin belum lengkap)


# ── Ambil data dari API ────────────────────────────────────────────────────
class ApiError(Exception):
    pass


RAW = {}   # (location_id, product_id) -> item JSON mentah


def to_int(v):
    s = re.sub(r"[^\d]", "", str(v)) if v not in (None, "") else ""
    return int(s) if s else None


def guess_pack_size(name):
    ms = re.findall(r"(\d+(?:[.,]\d+)?)\s?(ml|liter|lt|kg|gr|g|l|pcs|pc|sachet|lembar|roll)\b", name or "", re.I)
    return f"{ms[-1][0]}{ms[-1][1].lower()}" if ms else None


def flatten(d, prefix=""):
    """Ratakan JSON bersarang jadi satu baris datar. Tidak ada field yang dibuang."""
    out = {}
    for k, v in d.items():
        key = f"{prefix}{k}"
        if isinstance(v, dict):
            out.update(flatten(v, key + "_"))
        elif isinstance(v, list):
            if not v:
                out[key] = ""
            elif all(not isinstance(x, (dict, list)) for x in v):
                out[key] = "|".join(str(x) for x in v)
            else:
                out[key] = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
        else:
            out[key] = v
    return out


def fetch_page(session, location_id, name, page_index):
    url = f"{API_BASE}/api/product/location/{location_id}"
    params = {"name": name, "pageIndex": page_index, "pageSize": PAGE_SIZE, "searchmode": 1,
              "locationIds": location_id, "source": "srp", "layout_type": "grid", "items_per_row": 3}
    for attempt in range(4):
        try:
            r = session.get(url, params=params, timeout=30)
        except requests.RequestException:
            time.sleep(2 ** attempt)
            continue
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(5 * (attempt + 1))
            continue
        raise ApiError(f"HTTP {r.status_code}: {r.text[:200]}")
    raise ApiError("gagal setelah 4 percobaan (jaringan / server sibuk)")


def parse_item(it, location_id):
    """Kolom turunan di depan, lalu SEMUA field asli API (urut abjad)."""
    price = to_int(it.get("product_price"))
    disc  = to_int(it.get("product_discount_price"))
    name  = it.get("product_name")
    slug  = it.get("product_slug")
    derived = {
        "scrape_date": TODAY,
        "location_id": location_id,
        "product_id": it.get("product_id"),
        "product_name": name,
        "product_url": f"{SITE_BASE}/p/{slug}" if slug else None,
        "pack_size": (it.get("product_grammation_label") or "").strip() or guess_pack_size(name),
        "normal_price": price,
        "discount_price": disc,
        "selling_price": disc if disc else price,
        "discount_pct_calc": round((1 - disc / price) * 100, 1) if (price and disc) else 0,
    }
    flat = flatten(it)
    rest = {k: flat[k] for k in sorted(flat) if k not in derived}
    return {**derived, **rest}


def crawl_keyword(session, location_id, name, found, expect=None):
    """Telusuri halaman untuk satu kata kunci. Return (totalElements, produk_baru, selesai_normal).

    expect = jumlah produk yang SUDAH kita punya untuk kata ini. Kalau totalElements <= expect, tidak mungkin
    ada produk baru, jadi halaman berikutnya dilewati (hemat request).
    Halaman > 0 yang ditolak API (HTTP 400, di luar jendela hasil) dianggap batas API, bukan error fatal."""
    page, new, total = 0, 0, None
    while True:
        try:
            data = fetch_page(session, location_id, name, page)
        except ApiError as e:
            if page == 0:
                raise
            print(f"     ! '{name}' halaman {page} ditolak ({str(e)[:60]}) → dianggap batas jendela API", flush=True)
            return total, new, False
        content = data.get("content") or []
        total   = data.get("totalElements", total)
        for it in content:
            pid = it.get("product_id")
            if pid is not None and (location_id, pid) not in found:
                found[(location_id, pid)] = parse_item(it, location_id)
                RAW[(location_id, pid)] = it
                new += 1
        page += 1
        if page == 1 and expect is not None and total is not None and total <= expect:
            return total, new, True
        if data.get("last", True) or not content:
            return total, new, True
        if (MAX_PAGES and page >= MAX_PAGES) or page >= HARD_CAP:
            return total, new, False
        time.sleep(random.uniform(*DELAY))


# ── Sapuan ─────────────────────────────────────────────────────────────────
def _capped(total, complete):
    """True kalau hasil pencarian terpotong batas API (produk di luar jendela tidak terlihat)."""
    return (not complete) or (total is not None and total >= API_CAP)


def _over_budget(t0):
    global BUDGET_HIT
    if TIME_BUDGET_MIN and (time.time() - t0) / 60 > TIME_BUDGET_MIN:
        BUDGET_HIT = True
        return True
    return False


def _loc_count(found, loc):
    return sum(1 for k in found if k[0] == loc)


def _words(name):
    return set(re.findall(r"[a-z0-9]+", (name or "").lower()))


def _keyword_sweep(session, loc, found, queried, t0):
    """Tahap 1: kata kunci a-z dan 0-9."""
    for kw in KEYWORDS:
        if _over_budget(t0):
            print("  ! batas waktu tercapai, sapuan dihentikan", flush=True)
            return False
        names = [(v.get("product_name") or "").lower() for k, v in found.items() if k[0] == loc]
        expect = sum(1 for n in names if kw in n)
        total, new, complete = crawl_keyword(session, loc, kw, found, expect=expect)
        queried.add(kw)
        flag = "  [terpotong batas API]" if _capped(total, complete) else ""
        print(f"     '{kw}': total API {total} | +{new:<4} → {_loc_count(found, loc)}{flag}", flush=True)
        time.sleep(random.uniform(*DELAY))
    return True


def _deep_sweep(session, loc, found, queried, t0):
    """Tahap 2: kata dari nama produk yang sudah ditemukan, diulang sampai tidak ada produk baru."""
    for rnd in range(1, MAX_ROUNDS + 1):
        counter = Counter(w for k, v in found.items() if k[0] == loc for w in _words(v.get("product_name")))
        todo = [w for w, c in counter.most_common()
                if len(w) >= MIN_TOKEN_LEN and w not in queried and c < API_CAP]
        if not todo:
            print(f"  ronde {rnd}: tidak ada kata baru", flush=True)
            return True
        before = _loc_count(found, loc)
        print(f"  ronde {rnd}: {len(todo)} kata kunci dari nama produk", flush=True)
        for i, w in enumerate(todo, 1):
            if _over_budget(t0):
                print(f"  ! batas waktu tercapai di ronde {rnd} ({i}/{len(todo)}), sapuan dihentikan", flush=True)
                return False
            crawl_keyword(session, loc, w, found, expect=counter[w])
            queried.add(w)
            if i % 200 == 0:
                print(f"     {i}/{len(todo)} kata | total {_loc_count(found, loc)}", flush=True)
            time.sleep(random.uniform(*DELAY))
        gained = _loc_count(found, loc) - before
        print(f"  ronde {rnd} selesai: +{gained} produk → {_loc_count(found, loc)}", flush=True)
        if gained == 0:
            return True
    print(f"  ! berhenti di batas {MAX_ROUNDS} ronde (naikkan ASTRO_MAX_ROUNDS jika masih bertambah)", flush=True)
    return True


def run_crawl():
    session = requests.Session()
    session.headers.update(HEADERS)
    found = {}
    t0 = time.time()
    for loc in LOCATION_IDS:
        print(f"\n── Lokasi {loc}", flush=True)
        total, new, complete = crawl_keyword(session, loc, "", found)
        got = _loc_count(found, loc)
        capped = _capped(total, complete)
        print(f"  name=''  → totalElements={total} | terkumpul {got} | selesai normal={complete}"
              + ("  [TERPOTONG batas API]" if capped else ""), flush=True)

        incomplete = capped or total is None or got < total
        if SWEEP == "always" or (SWEEP == "auto" and incomplete):
            queried = set()
            print(f"  → sapuan kata kunci ({len(KEYWORDS)} kata)", flush=True)
            ok = _keyword_sweep(session, loc, found, queried, t0)
            if ok and DEEP:
                print("  → sapuan lanjutan (kata dari nama produk)", flush=True)
                _deep_sweep(session, loc, found, queried, t0)
        print(f"  ✓ lokasi {loc}: {_loc_count(found, loc)} produk unik "
              f"({(time.time() - t0) / 60:.1f} menit)", flush=True)
    return found
