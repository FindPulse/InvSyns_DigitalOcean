#!/usr/bin/env python3
"""
XXR - Open Inventory & Blowouts -> Postgres (invsync)

This script ingests the XXR Excel inventory file into:
  invsync.vendors
  invsync.vendor_files
  invsync.vendor_stock_raw
  invsync.inventory_normalized

XXR file quirks handled:
- Data is in sheet: "Open Inventory"
- Header row is NOT at row 1; for this file we assume header is on Excel row 4 => pandas header=3
- Blank/null spacer rows exist
- Repeated header rows may appear inside data (e.g. row values 'Part #' / 'Style' etc.)
- Duplicate SKUs may appear within the same file -> we dedupe normalized records by SKU
  using MAX(qty) (safe default; avoids double counting)

Mapping:
- SKU column: "Part #"
- QTY column: "Available"

Usage (PowerShell):
python ingest_xxr_inventory.py `
  --file "D:\\app\\InvSync\\Vendor_Files\\Open Inventory & Blowouts (XXR) 1-6-26.xlsx" `
  --vendor-code "XXR" `
  --vendor-name "XXR - Primax" `
  --as-of-date "2026-01-06"
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


SCHEMA = "invsync"
SHEET_NAME = "Open Inventory"
HEADER_ROW = 3  # 0-based. If header changes, adjust this.

SKU_COL = "Part #"
QTY_COL = "Available"


# ---------------------------
# Helpers
# ---------------------------
def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_date_from_filename(filename: str) -> Optional[date]:
    """
    Supports:
      ... 01-06-2026.xlsx
      ... 1-6-2026.xlsx
      ... 1-6-26.xlsx (assume 20yy)
    """
    m = re.search(r"(\d{1,2})-(\d{1,2})-(\d{2,4})", filename)
    if not m:
        return None
    mm, dd, yy = m.groups()
    mm = int(mm)
    dd = int(dd)
    yy = int(yy)
    yyyy = (2000 + yy) if yy < 100 else yy
    return date(yyyy, mm, dd)


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


def dedupe_norm_rows_keep_max(
    norm_rows: List[Tuple[int, int, date, str, int, Optional[int]]]
) -> List[Tuple[int, int, date, str, int, Optional[int]]]:
    """
    norm_rows tuples:
      (file_id_placeholder, vendor_id_placeholder, as_of_date, sku, qty, source_row_num)

    Dedupes by sku, keeping maximum qty.
    If qty ties, keep smallest source_row_num.
    """
    best: Dict[str, Tuple[int, int, date, str, int, Optional[int]]] = {}

    for fid, vid, as_of, sku, qty, src_row in norm_rows:
        if sku is None:
            continue
        key = str(sku).strip()
        if key == "":
            continue

        if key not in best:
            best[key] = (fid, vid, as_of, key, qty, src_row)
            continue

        _, _, _, _, best_qty, best_src = best[key]
        src_row_val = src_row if src_row is not None else 10**9
        best_src_val = best_src if best_src is not None else 10**9

        if qty > best_qty or (qty == best_qty and src_row_val < best_src_val):
            best[key] = (fid, vid, as_of, key, qty, src_row)

    return list(best.values())


# ---------------------------
# Postgres
# ---------------------------
def get_conn():
    host = os.environ.get("PGHOST")
    db = os.environ.get("PGDATABASE")
    user = os.environ.get("PGUSER")
    pw = os.environ.get("PGPASSWORD")
    port = int(os.environ.get("PGPORT", "5432"))
    sslmode = os.environ.get("PGSSLMODE", "require")

    if not all([host, db, user, pw]):
        raise RuntimeError(
            "Missing PG env vars. Need PGHOST, PGDATABASE, PGUSER, PGPASSWORD (and optionally PGPORT, PGSSLMODE)."
        )

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=pw,
        sslmode=sslmode,
    )


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
# XXR Cleaning
# ---------------------------
def clean_xxr_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    missing = [c for c in [SKU_COL, QTY_COL] if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing required columns: {missing}. Found: {list(df.columns)}")

    # Drop fully empty rows
    df = df.dropna(how="all")

    # Drop rows where SKU is null/empty
    df = df[df[SKU_COL].notna()]
    df[SKU_COL] = df[SKU_COL].astype(str).str.strip()
    df = df[df[SKU_COL] != ""]

    # Remove repeated header rows inside the data
    df = df[df[SKU_COL].str.lower() != "part #"]
    if "Style" in df.columns:
        df = df[df["Style"].astype(str).str.strip().str.lower() != "style"]

    return df


# ---------------------------
# Main
# ---------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="Path to XXR inventory Excel file")
    ap.add_argument("--vendor-code", required=True, help="Vendor code (e.g., XXR)")
    ap.add_argument("--vendor-name", required=True, help="Vendor name (e.g., XXR - Primax)")
    ap.add_argument("--as-of-date", default=None, help="YYYY-MM-DD. If omitted, parsed from filename; else defaults to today.")
    ap.add_argument("--source-email", default=None)
    ap.add_argument("--subject", default=None)
    args = ap.parse_args()

    file_path = args.file
    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)

    # as_of_date
    if args.as_of_date:
        as_of = datetime.strptime(args.as_of_date, "%Y-%m-%d").date()
    else:
        parsed = parse_date_from_filename(os.path.basename(file_path))
        as_of = parsed if parsed else date.today()

    file_hash = sha256_file(file_path)

    xl = pd.ExcelFile(file_path)
    if SHEET_NAME not in xl.sheet_names:
        raise RuntimeError(f"Expected sheet '{SHEET_NAME}' not found. Found: {xl.sheet_names}")

    df = pd.read_excel(file_path, sheet_name=SHEET_NAME, header=HEADER_ROW)
    df = clean_xxr_df(df)

    raw_rows: List[Tuple[int, int, Optional[str], Optional[str], Dict[str, Any], bool, Optional[str]]] = []
    norm_rows: List[Tuple[int, int, date, str, int, Optional[int]]] = []

    # IMPORTANT: enumerate to ensure row_num is 1..N (unique)
    for row_num, (_, row) in enumerate(df.iterrows(), start=1):
        raw_sku = None if pd.isna(row.get(SKU_COL)) else str(row.get(SKU_COL)).strip()
        raw_qty_val = row.get(QTY_COL)
        raw_qty = None if pd.isna(raw_qty_val) else str(raw_qty_val).strip()

        payload: Dict[str, Any] = {}
        for c in df.columns:
            v = row.get(c)
            if pd.isna(v):
                payload[c] = None
            else:
                payload[c] = v.item() if hasattr(v, "item") else v

        parsed_ok = True
        parse_error = None
        if not raw_sku:
            parsed_ok = False
            parse_error = f"Missing SKU value in '{SKU_COL}'"

        qty = to_int_qty(raw_qty_val)

        raw_rows.append((0, row_num, raw_sku, raw_qty, payload, parsed_ok, parse_error))
        if parsed_ok:
            norm_rows.append((0, 0, as_of, raw_sku, qty, row_num))

    # Critical fix: prevent duplicate (file_id, sku) in same INSERT batch
    norm_rows = dedupe_norm_rows_keep_max(norm_rows)

    with get_conn() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            vendor_id = ensure_vendor(cur, args.vendor_code, args.vendor_name)
            file_id, _status = insert_vendor_file(cur, vendor_id, file_path, file_hash, args.source_email, args.subject)

            raw_rows2 = [
                (file_id, rnum, sku, qty, json.dumps(payload), ok, err)
                for (_, rnum, sku, qty, payload, ok, err) in raw_rows
            ]
            norm_rows2 = [
                (file_id, vendor_id, as_of, sku, qty, src_row)
                for (_, _, as_of, sku, qty, src_row) in norm_rows
            ]

            # Raw rows (unique by file_id,row_num) - safe
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

            # Normalized rows (unique by file_id,sku) - now deduped
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

    print(" Ingestion complete")
    print(f"   Vendor: {args.vendor_code} ({args.vendor_name})")
    print(f"   File:   {os.path.basename(file_path)}")
    print(f"   Hash:   {file_hash}")
    print(f"   as_of:  {as_of.isoformat()}")
    print(f"   Rows:   raw={len(raw_rows2)}, normalized={len(norm_rows2)}")


if __name__ == "__main__":
    main()