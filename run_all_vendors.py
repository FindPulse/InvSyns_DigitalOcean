#!/usr/bin/env python3
"""
InvSync Central Runner

Runs the full pipeline (vendor ingests -> daily final -> woo map -> woo batch update)
from one command, suitable for Task Scheduler / cron.

Example:
  python run_all_vendors.py --mode full
  python run_all_vendors.py --mode full --dry-run
  python run_all_vendors.py --mode ingest --only MRW,VOS,XXR

Notes:
- Script assumes it sits in InvSync root (same folder as vendor scripts).
- Vendor files are expected in Vendor_Files/ under InvSync root (configurable).
- In --mode full, downstream scripts are REQUIRED (no silent skipping).
"""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import os
import subprocess
import sys
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple


# -----------------------------
# Config (safe to edit)
# -----------------------------
INVROOT = os.path.dirname(os.path.abspath(__file__))
VENDOR_DIR_DEFAULT = os.path.join(INVROOT, "Vendor_Files")
LOG_DIR_DEFAULT = os.path.join(INVROOT, "logs")

# Ferrada Google Sheet ID (your provided one)
FERRADA_SHEET_ID = "1tubAbOzd-KSuJxr-9xQ1PjZKq6QPtrYCG6Qw_fg7EAM"

# Vossen URL (your provided one)
VOSSEN_URL = "http://inventory.vossenwheels.com/sdtireandwheel.aspx"


# Downstream scripts that MUST exist in --mode full
DOWNSTREAM_REQUIRED = [
    ("Build Daily Final", "build_inventory_daily_final.py"),
    ("Build Woo Variation Map", "build_woo_variation_map.py"),
    ("Woo Stock Batch Update", "woo_variation_stock_batch_update.py"),
]


@dataclass
class StepResult:
    name: str
    cmd: List[str]
    returncode: int
    seconds: float


def now_utc_iso() -> str:
    # timezone-aware UTC (no deprecation warning)
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def today_local_iso() -> str:
    return dt.date.today().isoformat()


def parse_only_list(s: Optional[str]) -> Optional[List[str]]:
    if not s:
        return None
    parts = [p.strip().upper() for p in s.split(",") if p.strip()]
    return parts if parts else None


def py_cmd(script: str, args: List[str]) -> List[str]:
    return [sys.executable, os.path.join(INVROOT, script), *args]


def find_latest_file(folder: str, patterns: List[str]) -> str:
    """
    Picks newest file across glob patterns by modified time.
    Raises FileNotFoundError if none found.
    """
    matches: List[str] = []
    for pat in patterns:
        matches.extend(glob.glob(os.path.join(folder, pat)))
    matches = [m for m in matches if os.path.isfile(m)]
    if not matches:
        raise FileNotFoundError(f"No files found in '{folder}' for patterns: {patterns}")
    matches.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return matches[0]


def require_script_exists(script_filename: str) -> str:
    full = os.path.join(INVROOT, script_filename)
    if not os.path.exists(full):
        raise FileNotFoundError(f"Required script missing: {script_filename} (expected at {full})")
    return full


def run_cmd(name: str, cmd: List[str], log_path: str) -> StepResult:
    """
    Runs a command, streams stdout/stderr to console AND appends to a log file.
    """
    start = dt.datetime.now()

    with open(log_path, "a", encoding="utf-8", errors="ignore") as logf:
        logf.write("\n" + "=" * 100 + "\n")
        logf.write(f"[{now_utc_iso()}] START {name}\n")
        logf.write("CMD: " + " ".join(cmd) + "\n")

        proc = subprocess.Popen(
            cmd,
            cwd=INVROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )

        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.write(line)
            logf.write(line)

        proc.wait()

        seconds = (dt.datetime.now() - start).total_seconds()
        logf.write(f"[{now_utc_iso()}] END {name} rc={proc.returncode} seconds={seconds:.2f}\n")
        logf.write("=" * 100 + "\n")

    return StepResult(name=name, cmd=cmd, returncode=int(proc.returncode), seconds=float(seconds))


# -----------------------------
# Build vendor steps
# -----------------------------
def build_vendor_steps(vendor_dir: str, as_of_date: str) -> Dict[str, Tuple[str, List[str]]]:
    """
    Returns dict: vendor_code -> (human_step_name, cmd_list)
    """

    # Auto-pick latest files by pattern (modify patterns if naming changes)
    mrw_file = find_latest_file(vendor_dir, ["MRW Inventory Report*.csv", "MRW*Inventory*.xlsx"])
    xxr_file = find_latest_file(vendor_dir, ["Open Inventory & Blowouts (XXR) *.xlsx", "Open Inventory*XXR*.xlsx"])
    roh_file = find_latest_file(vendor_dir, ["Rohana Inventory* .xlsx", "Rohana Inventory *.xlsx"])
    wtd_file = find_latest_file(vendor_dir, ["WTDwheeloffer*.xlsx","WTD*wheel*offer*.xlsx","*wheeloffer*.xlsx"])

    # Fixed-name files
    lockoff_file = os.path.join(vendor_dir, "LockOffroadWheelsInventory.xlsx")

    # Ensure scripts exist (fail early, loud)
    require_script_exists("ingest_mrw_inventory.py")
    require_script_exists("ingest_vos_inventory.py")
    require_script_exists("ingest_fer_inventory.py")
    require_script_exists("ingest_lor_inventory.py")
    require_script_exists("ingest_roh_inventory.py")
    require_script_exists("ingest_xxr_inventory.py")
    require_script_exists("ingest_DOL_KAT_inventory.py")

    steps: Dict[str, Tuple[str, List[str]]] = {}

    steps["MRW"] = (
        "Ingest MRW",
        py_cmd(
            "ingest_mrw_inventory.py",
            [
                "--file", mrw_file,
                "--vendor-code", "MRW",
                "--vendor-name", "Method Race Wheels",
                "--as-of-date", as_of_date,
            ],
        ),
    )

    steps["VOS"] = (
        "Ingest Vossen",
        py_cmd(
            "ingest_vos_inventory.py",
            [
                "--url", VOSSEN_URL,
                "--vendor-code", "VOS",
                "--vendor-name", "Vossen Wheels",
                "--as-of-date", as_of_date,
                "--timeout", "300",
            ],
        ),
    )

    steps["FER"] = (
        "Ingest Ferrada",
        py_cmd(
            "ingest_fer_inventory.py",
            [
                "--sheet-id", FERRADA_SHEET_ID,
                "--vendor-code", "FER",
                "--vendor-name", "Ferrada",
                "--as-of-date", as_of_date,
                "--timeout", "300",
            ],
        ),
    )

    steps["LOF"] = (
        "Ingest Lock Off-Road",
        py_cmd(
            "ingest_lor_inventory.py",
            [
                "--file", lockoff_file,
                "--vendor-code", "LOF",
                "--vendor-name", "Lock Off-Road",
                "--as-of-date", as_of_date,
            ],
        ),
    )


    steps["ROH"] = (
        "Ingest Rohana",
        py_cmd(
            "ingest_roh_inventory.py",
            [
                "--file", roh_file,
                "--vendor-code", "ROH",
                "--vendor-name", "Rohana",
                "--as-of-date", as_of_date,
            ],
        ),
    )
    steps["XXR"] = (
        "Ingest XXR",
        py_cmd(
            "ingest_xxr_inventory.py",
            [
                "--file", xxr_file,
                "--vendor-code", "XXR",
                "--vendor-name", "XXR - Primax",
                "--as-of-date", as_of_date,
            ],
        ),
    )

    # NEW: Dolce + Katana combined ingest (runs once)
    # Your ingest script expects --filename and looks inside Vendor_Files itself.
    # We pass ONLY the basename.
    steps["DK"] = (
        "Ingest Dolce + Katana (WTD wheel offer)",
        py_cmd(
            "ingest_DOL_KAT_inventory.py",
            [
                "--filename", os.path.basename(wtd_file),
            ],
        ),
    )

    return steps


def build_downstream_steps_or_fail(as_of_date: str, vendor_codes: List[str]) -> List[Tuple[str, List[str]]]:
    """
    In --mode full, downstream scripts MUST exist. If missing, fail early.
    Also: pass REQUIRED args for each script based on your CLI.
    """
    steps: List[Tuple[str, List[str]]] = []

    # Validate scripts exist
    for _human_name, script in DOWNSTREAM_REQUIRED:
        require_script_exists(script)

    # Expand DK -> DOL + KAT for daily final
    expanded_vendor_codes: List[str] = []
    for vcode in vendor_codes:
        if vcode == "DK":
            expanded_vendor_codes.extend(["DOL", "KAT"])
        else:
            expanded_vendor_codes.append(vcode)

    # 1) Daily final needs vendor-code, so run per vendor
    for vcode in expanded_vendor_codes:
        steps.append((
            f"Build Daily Final ({vcode})",
            py_cmd("build_inventory_daily_final.py", ["--as-of-date", as_of_date, "--vendor-code", vcode]),
        ))

    # 2) Woo variation map needs as-of-date
    steps.append((
        "Build Woo Variation Map",
        py_cmd("build_woo_variation_map.py", ["--as-of-date", as_of_date]),
    ))

    # 3) Woo stock batch update needs as-of-date
    steps.append((
        "Woo Stock Batch Update",
        py_cmd("woo_variation_stock_batch_update.py", ["--as-of-date", as_of_date]),
    ))

    return steps


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["ingest", "full"], default="ingest",
                    help="ingest = vendor ingests only; full = ingestion + downstream steps (required)")
    ap.add_argument("--vendor-dir", default=VENDOR_DIR_DEFAULT, help="Folder containing vendor files")
    ap.add_argument("--log-dir", default=LOG_DIR_DEFAULT, help="Folder to write logs")
    ap.add_argument("--as-of-date", default=None, help="YYYY-MM-DD; default=today")
    ap.add_argument("--only", default=None, help="Comma list of vendor codes, e.g. MRW,VOS,FER")
    ap.add_argument("--continue-on-error", action="store_true",
                    help="Continue running remaining vendors even if one fails")
    ap.add_argument("--dry-run", action="store_true", help="Print commands only, do not execute")
    args = ap.parse_args()

    as_of_date = args.as_of_date or today_local_iso()
    only = parse_only_list(args.only)

    ensure_dir(args.log_dir)
    log_path = os.path.join(args.log_dir, f"invsync_run_{as_of_date}.log")

    # Build vendor steps (may raise if files/scripts missing)
    vendor_steps = build_vendor_steps(args.vendor_dir, as_of_date)

    # Filter by --only if provided
    run_vendor_codes = list(vendor_steps.keys())
    if only:
        run_vendor_codes = [c for c in run_vendor_codes if c in set(only)]
        missing = [c for c in only if c not in vendor_steps]
        if missing:
            print(f" Unknown vendor codes in --only (ignored): {missing}")

    # Build full ordered steps list
    steps_to_run: List[Tuple[str, List[str]]] = []
    for code in run_vendor_codes:
        name, cmd = vendor_steps[code]
        steps_to_run.append((f"{code}: {name}", cmd))

    if args.mode == "full":
        downstream = build_downstream_steps_or_fail(as_of_date, run_vendor_codes)
        steps_to_run.extend(downstream)

    # Awareness: print what downstream scripts will run
    if args.mode == "full":
        print(" Downstream steps REQUIRED and detected:")
        for human_name, script in DOWNSTREAM_REQUIRED:
            print(f"   - {human_name}: {script}")

    # Log header
    with open(log_path, "a", encoding="utf-8", errors="ignore") as logf:
        logf.write("\n" + "#" * 100 + "\n")
        logf.write(f"[{now_utc_iso()}] InvSync Runner START\n")
        logf.write(f"mode={args.mode} as_of_date={as_of_date}\n")
        logf.write(f"vendor_dir={args.vendor_dir}\n")
        logf.write(f"only={only}\n")
        logf.write("#" * 100 + "\n")

    print(f"\n InvSync Runner | mode={args.mode} | as_of_date={as_of_date}")
    print(f" Log: {log_path}\n")

    if args.dry_run:
        print("DRY RUN (no execution):")
        for name, cmd in steps_to_run:
            print(f"- {name}\n  {' '.join(cmd)}\n")
        return 0

    results: List[StepResult] = []
    failed: List[StepResult] = []

    for name, cmd in steps_to_run:
        res = run_cmd(name, cmd, log_path)
        results.append(res)

        if res.returncode != 0:
            failed.append(res)
            if not args.continue_on_error:
                print(f" Stopping because step failed: {name} (rc={res.returncode}).")
                break

    # Summary
    total_s = sum(r.seconds for r in results)
    ok_count = sum(1 for r in results if r.returncode == 0)
    fail_count = len(failed)

    print("\n" + "=" * 80)
    print(f"Run summary: ok={ok_count} failed={fail_count} total_steps={len(results)} total_seconds={total_s:.2f}")
    for r in results:
        status = "OK" if r.returncode == 0 else f"FAIL(rc={r.returncode})"
        print(f"- {status:12s} | {r.seconds:7.2f}s | {r.name}")

    with open(log_path, "a", encoding="utf-8", errors="ignore") as logf:
        logf.write(f"\n[{now_utc_iso()}] InvSync Runner END ok={ok_count} failed={fail_count} total_seconds={total_s:.2f}\n")

    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
