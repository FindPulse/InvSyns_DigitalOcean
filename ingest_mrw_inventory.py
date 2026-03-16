#!/usr/bin/env python3
"""
Ingest MRW inventory Excel into Postgres schema invsync:
- vendors (ensure vendor exists)
- vendor_files (dedupe by sha256)
- vendor_stock_raw (store every row + raw payload)
- inventory_normalized (sku + qty, qty>=0)

Usage:
  python3 ingest_mrw_inventory.py \
    --file "/path/MRW Inventory Report_1-19-2026.xlsx" \
    --vendor-code "MRW" \
    --vendor-name "Method Race Wheels" \
    --as-of-date "2026-01-19"

PG connection is read from env:
  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSSLMODE (optional)
"""

import argparse
import hashlib
import json
import os
import re
from datetime import date, datetime
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import psycopg2
import psycopg2.extras


SCHEMA = "invsync"
SHEET_NAME = "MRW Inventory Report"


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_date_from_filename(filename: str) -> date | None:
    """
    Tries to parse dates like:
      MRW Inventory Report_1-19-2026.xlsx
      MRW Inventory Report_01-19-2026.xlsx
    Returns date(2026,1,19) or None
    """
    m = re.search(r"(\d{1,2})-(\d{1,2})-(\d{4})", filename)
    if not m:
        return None
    mm, dd, yyyy = map(int, m.groups())
    return date(yyyy, mm, dd)


def get_conn():
    host = os.environ.get("PGHOST")
    db = os.environ.get("PGDATABASE")
    user = os.environ.get("PGUSER")
    pw = os.environ.get("PGPASSWORD")
    port = int(os.environ.get("PGPORT", "5432"))
    sslmode = os.environ.get("PGSSLMODE", "require")  # DO Managed PG typically requires SSL

    if not all([host, db, user, pw]):
        raise RuntimeError("Missing PG env vars. Need PGHOST, PGDATABASE, PGUSER, PGPASSWORD (and optionally PGPORT, PGSSLMODE).")

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


def insert_vendor_file(cur, vendor_id: int, file_path: str, file_hash: str, source_email: str | None, subject: str | None) -> Tuple[int, str]:
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


def detect_columns(df: pd.DataFrame) -> Tuple[str, str]:
    # Required: SKU + Quantity
    # MRW file uses: Item (sku), Available (qty)
    cols = {c.strip(): c for c in df.columns if isinstance(c, str)}
    if "Item" not in cols or "Available" not in cols:
        raise RuntimeError(f"Required columns not found. Detected columns: {list(df.columns)}")
    return cols["Item"], cols["Available"]


def to_int_qty(x: Any) -> int:
    if pd.isna(x):
        return 0
    if isinstance(x, (int, float)):
        q = int(x)
        return q if q >= 0 else 0
    s = str(x).strip()
    if s == "":
        return 0
    # remove commas etc.
    s = re.sub(r"[^\d\-]", "", s)
    try:
        q = int(s)
        return q if q >= 0 else 0
    except Exception:
        return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="Path to MRW inventory Excel file")
    ap.add_argument("--vendor-code", required=True, help="Vendor code (e.g., MRW)")
    ap.add_argument("--vendor-name", required=True, help="Vendor name (e.g., Method Race Wheels)")
    ap.add_argument("--as-of-date", default=None, help="YYYY-MM-DD. If omitted, parsed from filename; else defaults to today.")
    ap.add_argument("--source-email", default=None, help="Optional: source email address")
    ap.add_argument("--subject", default=None, help="Optional: email subject")
    args = ap.parse_args()

    file_path = args.file
    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)

    # as_of_date logic
    if args.as_of_date:
        as_of = datetime.strptime(args.as_of_date, "%Y-%m-%d").date()
    else:
        parsed = parse_date_from_filename(os.path.basename(file_path))
        as_of = parsed if parsed else date.today()

    file_hash = sha256_file(file_path)

    # Read Excel
    xl = pd.ExcelFile(file_path)
    if SHEET_NAME not in xl.sheet_names:
        raise RuntimeError(f"Expected sheet '{SHEET_NAME}' not found. Found: {xl.sheet_names}")

    df = xl.parse(SHEET_NAME)
    df.columns = [str(c).strip() for c in df.columns]

    sku_col, qty_col = detect_columns(df)

    # Build raw + normalized records
    raw_rows: List[Tuple[int, int, str | None, str | None, Dict[str, Any], bool, str | None]] = []
    norm_rows: List[Tuple[int, int, date, str, int, int | None]] = []

    # Row numbers: start at 1 for first data row (not header)
    for i, row in df.iterrows():
        row_num = int(i) + 1
        raw_sku = None if pd.isna(row.get(sku_col)) else str(row.get(sku_col)).strip()
        raw_qty_val = row.get(qty_col)
        raw_qty = None if pd.isna(raw_qty_val) else str(raw_qty_val).strip()

        payload = {}
        for c in df.columns:
            v = row.get(c)
            if pd.isna(v):
                payload[c] = None
            else:
                # keep as basic python types
                payload[c] = v.item() if hasattr(v, "item") else v

        parsed_ok = True
        parse_error = None

        if not raw_sku:
            parsed_ok = False
            parse_error = "Missing SKU (Item) value"

        qty = to_int_qty(raw_qty_val)

        raw_rows.append((0, row_num, raw_sku, raw_qty, payload, parsed_ok, parse_error))  # file_id placeholder

        if parsed_ok:
            sku = raw_sku
            norm_rows.append((0, 0, as_of, sku, qty, row_num))  # file_id/vendor_id placeholders

    with get_conn() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            vendor_id = ensure_vendor(cur, args.vendor_code, args.vendor_name)
            file_id, file_status = insert_vendor_file(cur, vendor_id, file_path, file_hash, args.source_email, args.subject)

            # If you want to skip reprocessing already-seen file hashes:
            # If status isn't 'received', it means it existed already. We still proceed safely
            # because we use ON CONFLICT where needed.

            # Fill placeholders
            raw_rows2 = [(file_id, rnum, sku, qty, json.dumps(payload), ok, err) for (_, rnum, sku, qty, payload, ok, err) in raw_rows]
            norm_rows2 = [(file_id, vendor_id, as_of, sku, qty, src_row) for (_, _, as_of, sku, qty, src_row) in norm_rows]

            # Insert raw rows
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

            # Insert normalized rows
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

            # Mark file as normalized
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
    print(f"   Vendor: {args.vendor_code} (vendor_id inserted/updated)")
    print(f"   File:   {os.path.basename(file_path)}")
    print(f"   Hash:   {file_hash}")
    print(f"   as_of:  {as_of.isoformat()}")
    print(f"   Rows:   raw={len(raw_rows2)}, normalized={len(norm_rows2)}")


if __name__ == "__main__":
    main()
