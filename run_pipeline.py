"""Pipeline harian: scrape Astro -> BigQuery (master_astro) -> arsip Google Drive.

Dijalankan oleh GitHub Actions (lihat .github/workflows/daily_scrape.yml).
Secret yang dibaca dari environment variable:
  ASTRO_DEVICE_ID      x-device-id dari browser (opsional, kalau kosong memakai ID acak)
  GCP_SA_KEY           isi file JSON service account BigQuery
  DRIVE_CLIENT_ID, DRIVE_CLIENT_SECRET, DRIVE_REFRESH_TOKEN   untuk arsip Google Drive (opsional)
"""
import io
import json
import logging
import os
import sys
import time

import pandas as pd
from google.auth.transport.requests import Request
from google.cloud import bigquery
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

import astro_scraper as S

PROJECT = os.getenv("BQ_PROJECT", "marketplace-tracker-513006")
DATASET = os.getenv("BQ_DATASET", "daily_astro")
TABLE = os.getenv("BQ_TABLE", "master_astro")
TABLE_ID = f"{PROJECT}.{DATASET}.{TABLE}"
MIN_ROWS = int(os.getenv("MIN_ROWS", "1000"))          # pengaman: tolak hasil scrape yang terlalu sedikit
DRIVE_FOLDER_NAME = os.getenv("DRIVE_FOLDER_NAME", "astro-scrape")
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.file"]

FIRST = ["scrape_date", "location_id", "product_id", "product_name", "product_url", "pack_size",
         "normal_price", "discount_price", "selling_price", "discount_pct_calc"]
NUMERIC = ["product_id", "normal_price", "discount_price", "selling_price",
           "product_price", "product_discount_price"]

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pipeline")


# ── DataFrame hasil scrape ─────────────────────────────────────────────────
def build_dataframe(found):
    df = pd.DataFrame(list(found.values()))
    df = df[FIRST + sorted(c for c in df.columns if c not in FIRST)]
    for c in NUMERIC:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").round().astype("Int64")
    return df.sort_values(["location_id", "product_id"]).reset_index(drop=True)


# ── BigQuery ───────────────────────────────────────────────────────────────
def bq_client():
    key = os.getenv("GCP_SA_KEY")
    if key:
        creds = service_account.Credentials.from_service_account_info(
            json.loads(key), scopes=["https://www.googleapis.com/auth/bigquery"])
        return bigquery.Client(project=PROJECT, credentials=creds)
    return bigquery.Client(project=PROJECT)       # fallback: kredensial lokal (gcloud auth)


def _to_bool(v):
    if pd.isna(v):
        return None
    return "true" if str(v).strip().lower() in {"true", "1", "t", "yes", "y"} else "false"


def conform(df, schema):
    """Samakan kolom df dengan schema tabel master_astro (nama, urutan, tipe).
    CSV load di BigQuery mencocokkan kolom berdasarkan URUTAN, jadi urutan harus persis sama."""
    lower_map = {c.lower(): c for c in df.columns}
    out = pd.DataFrame(index=df.index)
    used, missing = set(), []
    for f in schema:
        src = f.name if f.name in df.columns else lower_map.get(f.name.lower())
        if src is None:
            missing.append(f.name)
            out[f.name] = pd.Series([None] * len(df), index=df.index, dtype="object")
            continue
        used.add(src)
        s = df[src]
        t = f.field_type.upper()
        if t in ("INTEGER", "INT64"):
            out[f.name] = pd.to_numeric(s, errors="coerce").round().astype("Int64")
        elif t in ("FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"):
            out[f.name] = pd.to_numeric(s, errors="coerce")
        elif t in ("BOOLEAN", "BOOL"):
            out[f.name] = s.map(_to_bool).astype("string")
        else:
            out[f.name] = s.astype("string")
    extra = [c for c in df.columns if c not in used]
    if missing:
        log.warning("Kolom ada di tabel tapi tidak ada di hasil scrape (diisi kosong): %s", missing)
    if extra:
        log.warning("Kolom BARU dari API yang belum ada di tabel (tidak dimuat): %s", extra)
    return out


def load_bigquery(df):
    client = bq_client()
    table = client.get_table(TABLE_ID)            # error jelas kalau tabel belum ada
    location = client.get_dataset(f"{PROJECT}.{DATASET}").location
    out = conform(df, table.schema)
    buf = io.BytesIO(out.to_csv(index=False).encode("utf-8"))
    cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=table.schema,
        allow_quoted_newlines=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,   # menimpa partisi hari ini saja
    )
    day = S.TODAY.replace("-", "")
    job = client.load_table_from_file(buf, f"{TABLE_ID}${day}", job_config=cfg, location=location)
    job.result()
    log.info("BigQuery OK: %s baris dimuat ke partisi %s", job.output_rows, S.TODAY)


# ── Google Drive (arsip) ───────────────────────────────────────────────────
def drive_service():
    creds = Credentials(
        None,
        refresh_token=os.environ["DRIVE_REFRESH_TOKEN"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.environ["DRIVE_CLIENT_ID"],
        client_secret=os.environ["DRIVE_CLIENT_SECRET"],
        scopes=DRIVE_SCOPES,
    )
    creds.refresh(Request())
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def get_or_create_folder(svc, name):
    # scope drive.file hanya melihat folder buatan aplikasi ini, jadi folder dibuat otomatis
    q = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    found = svc.files().list(q=q, fields="files(id)", pageSize=1).execute().get("files", [])
    if found:
        return found[0]["id"]
    meta = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
    return svc.files().create(body=meta, fields="id").execute()["id"]


def upload_drive(path):
    svc = drive_service()
    folder_id = get_or_create_folder(svc, DRIVE_FOLDER_NAME)
    name = os.path.basename(path)
    q = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    existing = svc.files().list(q=q, fields="files(id)", pageSize=1).execute().get("files", [])
    media = MediaFileUpload(path, mimetype="application/gzip", resumable=True)
    if existing:                                   # jalan ulang di hari yang sama = timpa, bukan dobel
        svc.files().update(fileId=existing[0]["id"], media_body=media).execute()
    else:
        svc.files().create(body={"name": name, "parents": [folder_id]},
                           media_body=media, fields="id").execute()
    log.info("Drive OK: %s", name)


# ── Main ───────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    log.info("Mulai scrape %s", S.TODAY)
    found = S.run_crawl()
    df = build_dataframe(found)
    log.info("Scrape selesai: %s baris, %s kolom, %.1f menit", len(df), df.shape[1], (time.time() - t0) / 60)

    if S.BUDGET_HIT:
        log.warning("Sapuan dihentikan oleh batas waktu (ASTRO_TIME_BUDGET_MIN): hasil mungkin belum lengkap")

    if len(df) < MIN_ROWS:
        raise RuntimeError(f"Hanya {len(df)} baris (< {MIN_ROWS}). Kemungkinan diblokir/terpotong, "
                           "data TIDAK dimuat agar partisi hari ini tidak rusak.")

    errors = []

    try:
        load_bigquery(df)
    except Exception:
        log.exception("Load BigQuery GAGAL")
        errors.append("bigquery")

    if os.getenv("DRIVE_REFRESH_TOKEN"):
        try:
            path = os.path.join(S.OUT_DIR, f"astro_{S.TODAY}.csv.gz")
            df.to_csv(path, index=False, compression="gzip", encoding="utf-8")
            upload_drive(path)
        except Exception:
            log.exception("Upload Drive GAGAL")
            errors.append("drive")
    else:
        log.info("DRIVE_REFRESH_TOKEN kosong, arsip Drive dilewati")

    if errors:
        sys.exit(f"Pipeline selesai dengan error di: {', '.join(errors)}")
    log.info("Pipeline selesai tanpa error")


if __name__ == "__main__":
    main()
