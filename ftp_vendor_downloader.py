#!/usr/bin/env python3
"""
Auto-download vendor files from FTP server.

Connects to FTP, monitors /vendor_inbound folder, downloads new files,
and saves them to local Vendor_Files folder.

Usage:
  python ftp_vendor_downloader.py
  python ftp_vendor_downloader.py --dry-run
  python ftp_vendor_downloader.py --force-download
"""

import argparse
import csv
import os
import json
import ftplib
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import openpyxl
from dotenv import load_dotenv

load_dotenv()

# Configuration
FTP_HOST = os.environ.get("FTP_HOST", "ftp.sdtwdirectwholesale.com")
FTP_PORT = int(os.environ.get("FTP_PORT", "21"))
FTP_USER = os.environ.get("FTP_USER", "vendor_files@sdtwdirectwholesale.com")
FTP_PASS = os.environ.get("FTP_PASSWORD", "")
FTP_REMOTE_FOLDER = "/vendor_inbound"
ALLOWED_EXTENSIONS = {".xls", ".xlsx", ".csv"}

# Local paths
VENDOR_FILES_DIR = Path(__file__).parent / "Vendor_Files"
STATE_FILE = Path(__file__).parent / ".ftp_state.json"

# Ensure vendor directory exists
VENDOR_FILES_DIR.mkdir(exist_ok=True)


def get_ftp_connection():
    """Connect to FTP server."""
    if not FTP_PASS:
        raise RuntimeError("FTP_PASSWORD not set in .env file")
    
    try:
        ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS, timeout=30)
        ftp.set_pasv(True)
        print(f"✓ Connected to FTP: {FTP_HOST}:{FTP_PORT}")
        return ftp
    except ftplib.all_errors as e:
        print(f"✗ FTP connection failed: {e}")
        raise


def get_remote_files(ftp: ftplib.FTP) -> List[str]:
    """Get list of files in remote folder."""
    try:
        # Check if folder exists and change to it
        try:
            ftp.cwd(FTP_REMOTE_FOLDER)
        except ftplib.all_errors:
            print(f"✗ Remote folder '{FTP_REMOTE_FOLDER}' not found or not accessible")
            return []
        
        files = []
        ftp.retrlines("LIST", lambda line: files.append(line))
        
        # Parse filenames from LIST output
        filenames = []
        for line in files:
            # Skip empty lines and directories
            if not line or line.startswith("d"):
                continue
            
            parts = line.split()
            if len(parts) >= 9:
                # Join remaining parts as filename (handles spaces in names)
                filename = " ".join(parts[8:])
                # Only include allowed file types (.xls, .xlsx, .csv)
                ext = os.path.splitext(filename)[1].lower()
                if ext in ALLOWED_EXTENSIONS:
                    filenames.append(filename)
                else:
                    print(f"  Skipping (not xls/csv): {filename}")
        
        return sorted(filenames)
    except ftplib.all_errors as e:
        print(f"✗ Failed to list remote files: {e}")
        return []


def load_downloaded_state() -> Dict[str, str]:
    """Load state of previously downloaded files."""
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: Could not load state file: {e}")
    return {}


def save_downloaded_state(state: Dict[str, str]):
    """Save state of downloaded files."""
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"Warning: Could not save state file: {e}")


def convert_csv_to_xlsx(csv_path: Path) -> Path:
    """Convert a CSV file to XLSX format and remove the original CSV."""
    xlsx_path = csv_path.with_suffix(".xlsx")
    wb = openpyxl.Workbook()
    ws = wb.active

    with open(csv_path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for row in reader:
            ws.append(row)

    wb.save(xlsx_path)
    csv_path.unlink()  # remove original CSV
    return xlsx_path


def download_file(ftp: ftplib.FTP, filename: str) -> bool:
    """Download a file from FTP. CSV files are auto-converted to XLSX."""
    try:
        local_path = VENDOR_FILES_DIR / filename
        
        print(f"  Downloading: {filename}...", end=" ", flush=True)
        
        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR {filename}", f.write)
        
        file_size = local_path.stat().st_size
        print(f"✓ ({file_size} bytes)")

        # Convert CSV to XLSX
        if local_path.suffix.lower() == ".csv":
            xlsx_path = convert_csv_to_xlsx(local_path)
            print(f"    Converted to XLSX: {xlsx_path.name}")

        return True
    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def main():
    """Main function."""
    ap = argparse.ArgumentParser(description="Auto-download vendor files from FTP")
    ap.add_argument("--dry-run", action="store_true", help="Show what would be downloaded, don't actually download")
    ap.add_argument("--force-download", action="store_true", help="Re-download all files")
    args = ap.parse_args()

    print("=" * 60)
    print("FTP VENDOR FILE DOWNLOADER")
    print("=" * 60)
    print(f"Host:   {FTP_HOST}:{FTP_PORT}")
    print(f"User:   {FTP_USER}")
    print(f"Remote: {FTP_REMOTE_FOLDER}")
    print(f"Local:  {VENDOR_FILES_DIR}")
    print(f"Dry-run: {args.dry_run}")
    print(f"Force:  {args.force_download}")
    print("=" * 60)

    # Get FTP connection
    ftp = get_ftp_connection()

    try:
        # Convert any leftover CSV files from prior downloads
        for existing_file in VENDOR_FILES_DIR.iterdir():
            if existing_file.is_file() and existing_file.suffix.lower() == ".csv":
                print(f"  Converting leftover CSV: {existing_file.name}")
                convert_csv_to_xlsx(existing_file)

        # Get remote files
        remote_files = get_remote_files(ftp)
        print(f"\nFound {len(remote_files)} file(s) in remote folder:")
        for f in remote_files:
            print(f"  - {f}")

        # Load downloaded state
        state = load_downloaded_state() if not args.force_download else {}

        # Build set of local files (including .xlsx versions of .csv names)
        local_existing = {f.name for f in VENDOR_FILES_DIR.iterdir() if f.is_file()}

        def already_have(filename: str) -> bool:
            """Check if file was already downloaded (handles csv->xlsx rename)."""
            if filename in state or filename in local_existing:
                return True
            # A remote .csv is "done" if its .xlsx version exists locally
            if filename.lower().endswith(".csv"):
                xlsx_name = Path(filename).with_suffix(".xlsx").name
                if xlsx_name in local_existing or xlsx_name in state:
                    return True
            return False

        # Detect new files
        new_files = [f for f in remote_files if not already_have(f)]

        if not new_files:
            print("\n✓ No new files to download.")
            return 0

        print(f"\nNew files to download: {len(new_files)}")
        for f in new_files:
            print(f"  + {f}")

        if args.dry_run:
            print("\n[DRY-RUN] Would download these files.")
            return 0

        # Download new files
        print("\nDownloading...")
        downloaded_count = 0
        failed_count = 0

        for filename in new_files:
            if download_file(ftp, filename):
                state[filename] = datetime.now().isoformat()
                downloaded_count += 1
            else:
                failed_count += 1

        # Save state
        save_downloaded_state(state)

        print("\n" + "=" * 60)
        print(f"Downloaded: {downloaded_count}")
        print(f"Failed:     {failed_count}")
        print("=" * 60)

        return 0

    except Exception as e:
        print(f"\n✗ Error: {e}")
        return 1
    finally:
        try:
            ftp.quit()
        except:
            ftp.close()


if __name__ == "__main__":
    raise SystemExit(main())
