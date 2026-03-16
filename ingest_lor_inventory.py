#!/usr/bin/env python3
"""
Ingest Lock Off-Road inventory Excel into Postgres schema invsync.

Input:
  D:\app\InvSync\Vendor_Files\LockOffroadWheelsInventory.xlsx

Notes:
- First row is empty, header is on row 2 => header=1 in pandas.
- Required columns:
    PART NUMBER (SKU)
    QUANTITY    (QTY)

Loads into:
  invsync.vendors
  invsync.vendor_files (dedupe by sha256)
  invsync.vendor_stock_raw
  invsync.inventory_normalized

Usage:
python ingest_lockoff_inventory.py `
  --file "D:\app\InvSync\Vendor_Files\LockOffroadWheelsInventory.xlsx" `
  --vendor-code "LOF" `
  --vendor-name "Lock Off-Road" `
  --as-of-date "2026-01-23"
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

SKU_COL = "PART NUMBER"
QTY_COL = "QUANTITY"


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def get_conn():
    host = os.environ.get("PGHOST")
    db = os.environ.get("PGDATABASE")
    user = os.environ.get("PGUSER")
    pw = os.environ.get("PGPASSWORD")
    port = int(os.environ.get("PGPORT", "5432"))
    sslmode = os.environ.get("PGSSLMODE", "require")

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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="Path to Lock Off-Road inventory Excel file")
    ap.add_argument("--vendor-code", required=True, help="Vendor code (e.g., LOF)")
    ap.add_argument("--vendor-name", required=True, help="Vendor name (e.g., Lock Off-Road)")
    ap.add_argument("--as-of-date", default=None, help="YYYY-MM-DD. If omitted defaults to today.")
    ap.add_argument("--source-email", default=None)
    ap.add_argument("--subject", default=None)
    args = ap.parse_args()

    file_path = args.file
    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)

    as_of = datetime.strptime(args.as_of_date, "%Y-%m-%d").date() if args.as_of_date else date.today()
    file_hash = sha256_file(file_path)

    # Header on row 2 => header=1
    df = pd.read_excel(file_path, header=1)
    df.columns = [str(c).strip() for c in df.columns]

    missing = [c for c in [SKU_COL, QTY_COL] if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing required columns: {missing}. Found: {list(df.columns)}")

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

    with get_conn() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            vendor_id = ensure_vendor(cur, args.vendor_code, args.vendor_name)
            file_id, _status = insert_vendor_file(cur, vendor_id, file_path, file_hash, args.source_email, args.subject)

            raw_rows2 = [(file_id, rnum, sku, qty, json.dumps(payload), ok, err)
                         for (_, rnum, sku, qty, payload, ok, err) in raw_rows]
            norm_rows2 = [(file_id, vendor_id, as_of, sku, qty, src_row)
                          for (_, _, as_of, sku, qty, src_row) in norm_rows]

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

    print(" Ingestion complete")
    print(f"   Vendor: {args.vendor_code} ({args.vendor_name})")
    print(f"   File:   {os.path.basename(file_path)}")
    print(f"   Hash:   {file_hash}")
    print(f"   as_of:  {as_of.isoformat()}")
    print(f"   Rows:   raw={len(raw_rows2)}, normalized={len(norm_rows2)}")


if __name__ == "__main__":
    main()