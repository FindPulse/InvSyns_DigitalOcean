#!/usr/bin/env python3
"""
Build invsync.inventory_daily_final from invsync.inventory_normalized.

Rules (deterministic):
- For the selected as_of_date (and optionally vendor_code):
- For each SKU:
    1) choose the row with the highest qty
    2) if tie, choose the row from the most recently received file
    3) if tie, choose the highest file_id (stable)
- Upsert into inventory_daily_final (PK: as_of_date, sku)

Usage:
  # Build ALL vendors for a date (recommended)
  python build_inventory_daily_final.py --as-of-date 2026-02-06

  # Build only one vendor for a date (optional)
  python build_inventory_daily_final.py --as-of-date 2026-02-06 --vendor-code MRW
"""

import argparse
import os
from datetime import datetime
import psycopg2
from dotenv import load_dotenv

load_dotenv()
SCHEMA = "invsync"


def get_conn():
    host = os.environ.get("PGHOST")
    db = os.environ.get("PGDATABASE")
    user = os.environ.get("PGUSER")
    pw = os.environ.get("PGPASSWORD")
    port = int(os.environ.get("PGPORT", "5432"))
    sslmode = os.environ.get("PGSSLMODE", "disable")

    if not all([host, db, user, pw]):
        raise RuntimeError("Missing PG env vars. Need PGHOST, PGDATABASE, PGUSER, PGPASSWORD.")

    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pw, sslmode=sslmode)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--vendor-code", default=None, help="Optional vendor code (MRW/VOS/...)")
    args = ap.parse_args()

    as_of_date = datetime.strptime(args.as_of_date, "%Y-%m-%d").date()
    vendor_code = args.vendor_code.strip() if args.vendor_code else None

    # Optional vendor filter
    vendor_filter_sql = ""
    params = {"as_of_date": as_of_date}

    if vendor_code:
        vendor_filter_sql = "AND v.vendor_code = %(vendor_code)s"
        params["vendor_code"] = vendor_code

    sql = f"""
    WITH candidates AS (
      SELECT
        n.as_of_date,
        n.sku,
        n.qty,
        n.vendor_id,
        n.file_id,
        vf.received_at
      FROM {SCHEMA}.inventory_normalized n
      JOIN {SCHEMA}.vendor_files vf ON vf.file_id = n.file_id
      JOIN {SCHEMA}.vendors v ON v.vendor_id = n.vendor_id
      WHERE n.as_of_date = %(as_of_date)s
        AND v.is_active = TRUE
        {vendor_filter_sql}
    ),
    picked AS (
      SELECT DISTINCT ON (sku)
        as_of_date,
        sku,
        qty,
        vendor_id AS chosen_vendor_id,
        file_id  AS chosen_file_id
      FROM candidates
      ORDER BY sku, qty DESC, received_at DESC, file_id DESC
    )
    INSERT INTO {SCHEMA}.inventory_daily_final
      (as_of_date, sku, qty, chosen_vendor_id, chosen_file_id, computed_at)
    SELECT
      as_of_date, sku, qty, chosen_vendor_id, chosen_file_id, NOW()
    FROM picked
    ON CONFLICT (as_of_date, sku) DO UPDATE
      SET qty = EXCLUDED.qty,
          chosen_vendor_id = EXCLUDED.chosen_vendor_id,
          chosen_file_id = EXCLUDED.chosen_file_id,
          computed_at = NOW()
    RETURNING 1;
    """

    count_sql = f"""
    SELECT COUNT(*)
    FROM {SCHEMA}.inventory_daily_final
    WHERE as_of_date = %(as_of_date)s;
    """

    with get_conn() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(sql, params)
            written = cur.rowcount  # approximate (inserted/updated rows)

            cur.execute(count_sql, {"as_of_date": as_of_date})
            final_count = int(cur.fetchone()[0])

        conn.commit()

    scope = f"vendor={vendor_code}" if vendor_code else "ALL vendors"
    print("inventory_daily_final built successfully")
    print(f"  scope: {scope}")
    print(f"  as_of_date: {as_of_date.isoformat()}")
    print(f"  rows written this run (approx): {written}")
    print(f"  total rows in inventory_daily_final for date: {final_count}")


if __name__ == "__main__":
    main()
