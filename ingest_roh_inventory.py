#!/usr/bin/env python3
"""
Rohana Inventory (Dropbox URL or local XLSX) -> Postgres schema invsync

Inputs:
- Either:
  --url  (Dropbox share link or direct xlsx URL)
  OR
  --file (local path to xlsx)

Behavior:
- Downloads XLSX if --url given
- Reads only sheet "All"
- Auto-detects header row by scanning first N rows to find required columns
- Forward-fills sparse leading "Unnamed:*" columns (series/flags)
- Loads into:
  invsync.vendors
  invsync.vendor_files
  invsync.vendor_stock_raw
  invsync.inventory_normalized

Usage (PowerShell):
python ingest_roh_inventory.py `
  --url "https://www.dropbox.com/scl/fi/.../Rohana-Inventory-02-13-2026.xlsx?dl=0" `
  --vendor-code "ROH" `
  --vendor-name "Rohana" `
  --as-of-date "2026-02-13"

Fallback local file:
python ingest_roh_inventory.py `
  --file "D:\app\InvSync\Vendor_Files\Rohana Inventory 01-30-2026.xlsx" `
  --vendor-code "ROH" `
  --vendor-name "Rohana" `
  --as-of-date "2026-02-06"
"""

import argparse
import hashlib
import json
import os
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import psycopg2
import psycopg2.extras
import requests

SCHEMA = "invsync"

SHEET_NAME = "All"
SKU_COL = "Part #"
QTY_COL = "Qty Available"


# ---------------------------
# Hash helpers
# ---------------------------
def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_date_from_filename(filename: str) -> Optional[date]:
    """
    Parses dates like:
      Rohana Inventory 01-16-2026.xlsx
      Rohana-Inventory-02-13-2026.xlsx
      Rohana Inventory 1-16-2026.xlsx
    """
    m = re.search(r"(\d{1,2})-(\d{1,2})-(\d{4})", filename)
    if not m:
        return None
    mm, dd, yyyy = map(int, m.groups())
    return date(yyyy, mm, dd)


# ---------------------------
# Postgres helpers
# ---------------------------
def get_conn():
    host = os.environ.get("PGHOST")
    db = os.environ.get("PGDATABASE")
    user = os.environ.get("PGUSER")
    pw = os.environ.get("PGPASSWORD")
    port = int(os.environ.get("PGPORT", "5432"))
    sslmode = os.environ.get("PGSSLMODE", "require")

    if not all([host, db, user, pw]):
        raise RuntimeError("Missing PG env vars. Need PGHOST, PGDATABASE, PGUSER, PGPASSWORD.")

    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pw, sslmode=sslmode)


def ensure_vendor(cur, vendor_code: str, vendor_name: str) -> int:
    cur.execute(
        f"""
        INSERT INTO {SCHEMA}.vendors (vendor_code, vendor_name)
        VALUES (%s, %s)
        ON CONFLICT (vendor_code) DO UPDATE
          SET vendor_name = EXCLUDED.vendor_name,
              updated_at = NOW()
        RETURNING vendor_id;
        """,
        (vendor_code, vendor_name),
    )
    return int(cur.fetchone()[0])


def insert_vendor_file(
    cur,
    vendor_id: int,
    file_path: str,
    file_hash: str,
    source_email: Optional[str],
    subject: Optional[str],
) -> Tuple[int, str]:
    filename = os.path.basename(file_path)
    size_bytes = os.path.getsize(file_path)

    cur.execute(
        f"""
        INSERT INTO {SCHEMA}.vendor_files
          (vendor_id, source_email, subject, filename, file_hash_sha256, file_size_bytes, received_at, status)
        VALUES
          (%s, %s, %s, %s, %s, %s, NOW(), 'received')
        ON CONFLICT (vendor_id, file_hash_sha256) DO UPDATE
          SET processed_at = COALESCE({SCHEMA}.vendor_files.processed_at, NOW()),
              status = {SCHEMA}.vendor_files.status
        RETURNING file_id, status::text;
        """,
        (vendor_id, source_email, subject, filename, file_hash, size_bytes),
    )
    file_id, status = cur.fetchone()
    return int(file_id), str(status)


# ---------------------------
# Normalization helpers
# ---------------------------
def to_int_qty(x: Any) -> int:
    if pd.isna(x):
        return 0
    if isinstance(x, (int, float)):
        q = int(x)
        return q if q >= 0 else 0
    s = str(x).strip()
    if s == "":
        return 0
    s = re.sub(r"[^\d\-]", "", s)
    try:
        q = int(s)
        return q if q >= 0 else 0
    except Exception:
        return 0


def normalize_colname(x: Any) -> str:
    return re.sub(r"\s+", " ", str(x).strip())


def detect_header_row_xlsx(xlsx_path: str, sheet_name: str, required_cols: List[str], scan_rows: int = 12) -> int:
    """
    Reads first `scan_rows` rows with header=None and tries to find the row
    that contains ALL required columns.
    Returns 0-based row index suitable for pandas header=...
    """
    preview = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=None, nrows=scan_rows, engine="openpyxl")
    required_norm = [normalize_colname(c).lower() for c in required_cols]

    for ridx in range(preview.shape[0]):
        row_vals = [normalize_colname(v).lower() for v in preview.iloc[ridx].tolist() if not pd.isna(v)]
        row_set = set(row_vals)
        if all(rc in row_set for rc in required_norm):
            return int(ridx)

    # fallback: keep old assumption (row 4 => header index 3)
    return 3


def clean_rohana_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [normalize_colname(c) for c in df.columns]

    # Forward-fill sparse leading columns if present
    for c in ["Unnamed: 0", "Unnamed: 1", "Unnamed: 2"]:
        if c in df.columns:
            df[c] = df[c].ffill()

    # Drop totally empty unnamed columns (common at the end)
    drop_cols = []
    for c in df.columns:
        if str(c).startswith("Unnamed:") and c not in ["Unnamed: 0", "Unnamed: 1", "Unnamed: 2"]:
            if df[c].isna().all():
                drop_cols.append(c)
    if drop_cols:
        df = df.drop(columns=drop_cols)

    # Optional rename
    rename_map = {}
    if "Unnamed: 0" in df.columns:
        rename_map["Unnamed: 0"] = "Series"
    if "Unnamed: 1" in df.columns:
        rename_map["Unnamed: 1"] = "Flag"
    if "Unnamed: 2" in df.columns:
        rename_map["Unnamed: 2"] = "Meta"
    if rename_map:
        df = df.rename(columns=rename_map)

    # Validate required columns
    missing = [c for c in [SKU_COL, QTY_COL] if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing required columns: {missing}. Found: {list(df.columns)}")

    return df


# ---------------------------
# Download helpers
# ---------------------------
def dropbox_to_direct(url: str) -> str:
    """
    Converts Dropbox share URL to direct download URL.
    - if dl=0 => dl=1
    - if no dl param => add dl=1
    """
    if "dropbox.com" not in url:
        return url
    if "dl=0" in url:
        return url.replace("dl=0", "dl=1")
    if "dl=1" in url:
        return url
    sep = "&" if "?" in url else "?"
    return url + f"{sep}dl=1"


def download_file(url: str, out_path: str, timeout: int = 120) -> None:
    url2 = dropbox_to_direct(url)
    with requests.get(url2, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


# ---------------------------
# Main ingest
# ---------------------------
def ingest_xlsx(file_path: str, vendor_code: str, vendor_name: str, as_of: date,
                source_email: Optional[str], subject: Optional[str]) -> None:

    # Validate sheet exists
    xl = pd.ExcelFile(file_path)
    if SHEET_NAME not in xl.sheet_names:
        raise RuntimeError(f"Expected sheet '{SHEET_NAME}' not found. Found: {xl.sheet_names}")

    header_row = detect_header_row_xlsx(file_path, SHEET_NAME, [SKU_COL, QTY_COL], scan_rows=12)
    df = pd.read_excel(file_path, sheet_name=SHEET_NAME, header=header_row, engine="openpyxl")
    df = clean_rohana_df(df)

    raw_rows: List[Tuple[int, int, Optional[str], Optional[str], Dict[str, Any], bool, Optional[str]]] = []
    norm_rows: List[Tuple[int, int, date, str, int, Optional[int]]] = []

    for i, row in df.iterrows():
        row_num = int(i) + 1

        raw_sku = None if pd.isna(row.get(SKU_COL)) else str(row.get(SKU_COL)).strip()
        raw_qty_val = row.get(QTY_COL)
        raw_qty = None if pd.isna(raw_qty_val) else str(raw_qty_val).strip()

        payload: Dict[str, Any] = {}
        for c in df.columns:
            v = row.get(c)
            payload[c] = None if pd.isna(v) else (v.item() if hasattr(v, "item") else v)

        parsed_ok = True
        parse_error = None
        if not raw_sku:
            parsed_ok = False
            parse_error = f"Missing SKU value in '{SKU_COL}'"

        qty = to_int_qty(raw_qty_val)

        raw_rows.append((0, row_num, raw_sku, raw_qty, payload, parsed_ok, parse_error))
        if parsed_ok:
            norm_rows.append((0, 0, as_of, raw_sku, qty, row_num))

    file_hash = sha256_file(file_path)

    with get_conn() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            vendor_id = ensure_vendor(cur, vendor_code, vendor_name)
            file_id, _status = insert_vendor_file(cur, vendor_id, file_path, file_hash, source_email, subject)

            raw_rows2 = [
                (file_id, rnum, sku, qty, json.dumps(payload), ok, err)
                for (_, rnum, sku, qty, payload, ok, err) in raw_rows
            ]
            norm_rows2 = [
                (file_id, vendor_id, as_of, sku, qty, src_row)
                for (_, _, as_of, sku, qty, src_row) in norm_rows
            ]

            psycopg2.extras.execute_values(
                cur,
                f"""
                INSERT INTO {SCHEMA}.vendor_stock_raw
                  (file_id, row_num, raw_sku, raw_qty, raw_payload, parsed_ok, parse_error)
                VALUES %s
                ON CONFLICT (file_id, row_num) DO UPDATE
                  SET raw_sku = EXCLUDED.raw_sku,
                      raw_qty = EXCLUDED.raw_qty,
                      raw_payload = EXCLUDED.raw_payload,
                      parsed_ok = EXCLUDED.parsed_ok,
                      parse_error = EXCLUDED.parse_error;
                """,
                raw_rows2,
                page_size=2000,
            )

            psycopg2.extras.execute_values(
                cur,
                f"""
                INSERT INTO {SCHEMA}.inventory_normalized
                  (file_id, vendor_id, as_of_date, sku, qty, source_row_num)
                VALUES %s
                ON CONFLICT (file_id, sku) DO UPDATE
                  SET qty = EXCLUDED.qty,
                      source_row_num = EXCLUDED.source_row_num,
                      normalized_at = NOW();
                """,
                norm_rows2,
                page_size=2000,
            )

            cur.execute(
                f"""
                UPDATE {SCHEMA}.vendor_files
                SET status = 'normalized',
                    processed_at = NOW(),
                    error_message = NULL
                WHERE file_id = %s;
                """,
                (file_id,),
            )

        conn.commit()

    print("Ingestion complete")
    print(f"  Vendor: {vendor_code} ({vendor_name})")
    print(f"  File:   {os.path.basename(file_path)}")
    print(f"  Hash:   {file_hash}")
    print(f"  as_of:  {as_of.isoformat()}")
    print(f"  Rows:   raw={len(raw_rows2)}, normalized={len(norm_rows2)}")
    print(f"  Header row detected (0-based): {header_row}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=None, help="Dropbox/direct URL to Rohana XLSX")
    ap.add_argument("--file", default=None, help="Local path to Rohana XLSX (fallback)")
    ap.add_argument("--vendor-code", required=True)
    ap.add_argument("--vendor-name", required=True)
    ap.add_argument("--as-of-date", default=None, help="YYYY-MM-DD. If omitted, parsed from filename if possible; else today.")
    ap.add_argument("--source-email", default=None)
    ap.add_argument("--subject", default=None)
    ap.add_argument("--timeout", type=int, default=120)
    ap.add_argument("--download-only", action="store_true")
    args = ap.parse_args()

    if not args.url and not args.file:
        raise RuntimeError("You must provide either --url or --file")

    # Determine as_of date
    if args.as_of_date:
        as_of = datetime.strptime(args.as_of_date, "%Y-%m-%d").date()
    else:
        # try parse from filename-like portion of URL or file name
        name_hint = ""
        if args.file:
            name_hint = os.path.basename(args.file)
        elif args.url:
            name_hint = args.url.split("?")[0].split("/")[-1]
        parsed = parse_date_from_filename(name_hint)
        as_of = parsed if parsed else date.today()

    # If URL, download to a local file
    if args.url:
        out_name = f"rohana_inventory_{as_of.isoformat()}.xlsx"
        print(f"Downloading Rohana XLSX from URL -> {out_name}")
        download_file(args.url, out_name, timeout=args.timeout)
        xlsx_path = out_name
    else:
        xlsx_path = args.file
        if not os.path.exists(xlsx_path):
            raise FileNotFoundError(xlsx_path)

    if args.download_only:
        print(f"Download-only complete. File at: {xlsx_path}")
        return

    ingest_xlsx(
        file_path=xlsx_path,
        vendor_code=args.vendor_code,
        vendor_name=args.vendor_name,
        as_of=as_of,
        source_email=args.source_email,
        subject=args.subject,
    )


if __name__ == "__main__":
    main()
