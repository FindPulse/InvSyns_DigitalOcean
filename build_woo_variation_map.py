#!/usr/bin/env python3
"""
SKU -> (parent_id, variation_id) mapping using WooCommerce MySQL DB (NO Woo API).

Approach:
- Read SKUs from invsync.inventory_daily_final for as_of_date (Postgres)
- Query Woo MySQL to find matching *variation* rows by SKU
- Upsert into invsync.woo_variation_map (Postgres)
- Commit progress in batches (safe restart)

Usage:
  python build_woo_variation_map.py --as-of-date 2026-02-13
  python build_woo_variation_map.py --as-of-date 2026-02-13 --limit 500
"""

import argparse
import os
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

SCHEMA = "invsync"


# -------------------- Postgres --------------------

def get_pg_conn():
    host = os.environ.get("PGHOST")
    db = os.environ.get("PGDATABASE")
    user = os.environ.get("PGUSER")
    pw = os.environ.get("PGPASSWORD")
    port = int(os.environ.get("PGPORT", "5432"))
    sslmode = os.environ.get("PGSSLMODE", "disable")

    if not all([host, db, user, pw]):
        raise RuntimeError("Missing PG env vars (PGHOST/PGDATABASE/PGUSER/PGPASSWORD).")

    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pw, sslmode=sslmode)


def fetch_skus_to_map(cur, as_of: date, stale_days: int, limit: Optional[int]) -> List[str]:
    lim_sql = "LIMIT %(limit)s" if limit else ""
    cur.execute(
        f"""
        WITH inv AS (
          SELECT sku
          FROM {SCHEMA}.inventory_daily_final
          WHERE as_of_date = %(as_of)s
        )
        SELECT inv.sku
        FROM inv
        LEFT JOIN {SCHEMA}.woo_variation_map m
          ON m.sku = inv.sku
        WHERE
          m.sku IS NULL
          OR m.active = FALSE
          OR m.last_verified_at < (NOW() - (%(stale_days)s || ' days')::interval)
        ORDER BY inv.sku
        {lim_sql};
        """,
        {"as_of": as_of, "stale_days": stale_days, "limit": limit},
    )
    return [r[0] for r in cur.fetchall()]


def upsert_good(cur, rows: List[Tuple[str, int, int]]):
    # rows: (sku, parent_id, variation_id)
    psycopg2.extras.execute_values(
        cur,
        f"""
        INSERT INTO {SCHEMA}.woo_variation_map
          (sku, parent_id, variation_id, last_verified_at, active, notes)
        VALUES %s
        ON CONFLICT (sku) DO UPDATE
          SET parent_id = EXCLUDED.parent_id,
              variation_id = EXCLUDED.variation_id,
              last_verified_at = NOW(),
              active = TRUE,
              notes = NULL;
        """,
        rows,
        template="(%s,%s,%s,NOW(),TRUE,NULL)",
        page_size=2000,
    )


def upsert_bad(cur, rows: List[Tuple[str, str]]):
    # rows: (sku, notes)
    psycopg2.extras.execute_values(
        cur,
        f"""
        INSERT INTO {SCHEMA}.woo_variation_map
          (sku, parent_id, variation_id, last_verified_at, active, notes)
        VALUES %s
        ON CONFLICT (sku) DO UPDATE
          SET last_verified_at = NOW(),
              active = FALSE,
              notes = EXCLUDED.notes;
        """,
        rows,
        template="(%s,0,0,NOW(),FALSE,%s)",
        page_size=2000,
    )


# -------------------- MySQL (Woo) --------------------

def get_mysql_conn():
    """
    Uses PyMySQL (pure python). Install if needed:
      pip install pymysql
    """
    try:
        import pymysql  # type: ignore
    except Exception:
        raise RuntimeError("Missing dependency: pymysql. Install: pip install pymysql")

    host = os.environ.get("MYSQL_HOST")
    db = os.environ.get("MYSQL_DATABASE")
    user = os.environ.get("MYSQL_USER")
    pw = os.environ.get("MYSQL_PASSWORD")
    port = int(os.environ.get("MYSQL_PORT", "3306"))

    if not all([host, db, user, pw]):
        raise RuntimeError("Missing WP MySQL env vars: WP_DB_HOST, WP_DB_NAME, WP_DB_USER, WP_DB_PASSWORD (optional WP_DB_PORT).")

    return pymysql.connect(
        host=host,
        user=user,
        password=pw,
        database=db,
        port=port,
        autocommit=True,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def wp_prefix() -> str:
    # default WP prefix is 'wp_'
    return os.environ.get("WP_TABLE_PREFIX", "wp_").strip()


def mysql_map_variations_by_sku(skus: List[str]) -> Tuple[Dict[str, Tuple[int, int]], Dict[str, str]]:
    """
    Returns:
      good_map: {sku: (parent_id, variation_id)}
      bad_map:  {sku: reason}
    """
    if not skus:
        return {}, {}

    pfx = wp_prefix()
    posts = f"{pfx}posts"
    lookup = f"{pfx}wc_product_meta_lookup"
    postmeta = f"{pfx}postmeta"

    good: Dict[str, Tuple[int, int]] = {}
    bad: Dict[str, str] = {}

    # We'll try the FAST path first: wc_product_meta_lookup (has sku + product_id)
    # Then join wp_posts to ensure it's a variation and to get post_parent.
    placeholders = ",".join(["%s"] * len(skus))

    sql_fast = f"""
    SELECT
      l.sku AS sku,
      p.ID AS variation_id,
      p.post_parent AS parent_id,
      p.post_type AS post_type
    FROM {lookup} l
    JOIN {posts} p ON p.ID = l.product_id
    WHERE l.sku IN ({placeholders});
    """

    # Fallback path if lookup doesn't have the sku or isn't populated
    # (uses postmeta _sku; slower but reliable)
    sql_fallback = f"""
    SELECT
      pm.meta_value AS sku,
      p.ID AS variation_id,
      p.post_parent AS parent_id,
      p.post_type AS post_type
    FROM {postmeta} pm
    JOIN {posts} p ON p.ID = pm.post_id
    WHERE pm.meta_key = '_sku'
      AND pm.meta_value IN ({placeholders});
    """

    rows: List[dict] = []
    with get_mysql_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql_fast, skus)
                rows = list(cur.fetchall())
            except Exception:
                # If table doesn't exist or query fails, fallback
                rows = []

            if not rows:
                cur.execute(sql_fallback, skus)
                rows = list(cur.fetchall())

    # Group rows by sku (handle duplicates)
    by_sku: Dict[str, List[dict]] = {}
    for r in rows:
        sku = (r.get("sku") or "").strip()
        if not sku:
            continue
        by_sku.setdefault(sku, []).append(r)

    for sku in skus:
        sku_norm = (sku or "").strip()
        found = by_sku.get(sku_norm, [])

        if not found:
            bad[sku_norm] = "not_found_in_mysql"
            continue

        # If multiple rows share same SKU, choose best:
        # - prefer variation rows over product rows
        # - if multiple variations, mark duplicate (safer) unless they point to same IDs
        variations = [r for r in found if r.get("post_type") == "product_variation"]
        products = [r for r in found if r.get("post_type") == "product"]

        if variations:
            # If more than one unique variation_id, safer to mark duplicate
            unique = {(int(r["parent_id"] or 0), int(r["variation_id"] or 0)) for r in variations}
            unique = {u for u in unique if u[1] > 0}
            if len(unique) == 1:
                parent_id, variation_id = list(unique)[0]
                if parent_id > 0 and variation_id > 0:
                    good[sku_norm] = (parent_id, variation_id)
                else:
                    bad[sku_norm] = "variation_row_missing_parent_or_id"
            else:
                bad[sku_norm] = f"duplicate_sku_multiple_variations(count={len(unique)})"
            continue

        if products:
            bad[sku_norm] = "sku_found_but_not_variation(kind=product)"
            continue

        bad[sku_norm] = "sku_found_but_unknown_post_type"

    return good, bad


# -------------------- Main --------------------

def chunk_list(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--limit", type=int, default=None, help="Map only N SKUs (for testing)")
    ap.add_argument("--stale-days", type=int, default=30)
    ap.add_argument("--batch-commit", type=int, default=500, help="Commit to PG every N SKUs processed")
    ap.add_argument("--mysql-batch", type=int, default=1000, help="How many SKUs per MySQL IN() query")
    args = ap.parse_args()

    as_of = datetime.strptime(args.as_of_date, "%Y-%m-%d").date()

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            skus = fetch_skus_to_map(cur, as_of, args.stale_days, args.limit)

    if not skus:
        print("No SKUs need mapping.")
        return

    print(f"Mapping {len(skus)} SKUs using MySQL (no Woo API).")
    print(f"MySQL batches: {args.mysql_batch} | PG commit batch: {args.batch_commit}")

    processed = 0
    good_buf: List[Tuple[str, int, int]] = []
    bad_buf: List[Tuple[str, str]] = []

    with get_pg_conn() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:

            for group in chunk_list(skus, args.mysql_batch):
                good_map, bad_map = mysql_map_variations_by_sku(group)

                for sku, (parent_id, variation_id) in good_map.items():
                    good_buf.append((sku, parent_id, variation_id))

                for sku, reason in bad_map.items():
                    bad_buf.append((sku, reason))

                processed += len(group)

                # Commit periodically
                if processed % args.batch_commit == 0:
                    if good_buf:
                        upsert_good(cur, good_buf)
                        good_buf.clear()
                    if bad_buf:
                        upsert_bad(cur, bad_buf)
                        bad_buf.clear()
                    conn.commit()
                    print(f"  committed {processed}/{len(skus)}")

            # Final flush
            if good_buf:
                upsert_good(cur, good_buf)
            if bad_buf:
                upsert_bad(cur, bad_buf)
            conn.commit()

    print("Mapping finished.")
    print("Check unmapped:")
    print("  SELECT sku, notes FROM invsync.woo_variation_map WHERE active = FALSE ORDER BY last_verified_at DESC LIMIT 50;")


if __name__ == "__main__":
    main()