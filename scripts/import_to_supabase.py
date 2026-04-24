#!/usr/bin/env python3
"""
Import CVR JSON files into Supabase flat companies table.

Prerequisites:
    pip install ijson requests

Usage:
    # Full import (~30-60 min)
    python import_to_supabase.py

    # Test run (first 5000 companies only)
    python import_to_supabase.py --limit 5000

    # Point to different data directory
    python import_to_supabase.py --data-dir /path/to/json/files
"""

import ijson
import json
import os
import sys
import time
import argparse

try:
    import requests
except ImportError:
    print("Error: requests package required. Run: pip install requests ijson")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────

SUPABASE_URL = "https://cvbtnmqchpzgsjcjcorj.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

if not SUPABASE_KEY:
    print("Error: set SUPABASE_KEY environment variable before running.")
    print("  export SUPABASE_KEY=sb_secret_...")
    sys.exit(1)

DEFAULT_DATA_DIR = os.path.expanduser("~/Documents/newrepo")

FILES = {
    "virksomhed":    "CVR_V2_Virksomhed_TotalDownload_json_Current_345.json",
    "navn":          "CVR_V2_Navn_TotalDownload_json_Current_345.json",
    "adressering":   "CVR_V2_Adressering_TotalDownload_json_Current_345.json",
    "branche":       "CVR_V2_Branche_TotalDownload_json_Current_345.json",
    "telefon":       "CVR_V2_Telefonnummer_TotalDownload_json_Current_345.json",
    "email":         "CVR_V2_e-mailadresse_TotalDownload_json_Current_345.json",
    "form":          "CVR_V2_Virksomhedsform_TotalDownload_json_Current_345.json",
}

BATCH_SIZE = 500   # rows per Supabase upsert (keeps payload ~100KB)

# ── Supabase helpers ──────────────────────────────────────────────────────────

session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "apikey": SUPABASE_KEY,
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=minimal",
})

def upsert_batch(records: list) -> None:
    """Upsert a list of company dicts to Supabase."""
    url = f"{SUPABASE_URL}/rest/v1/companies"
    resp = session.post(url, data=json.dumps(records), timeout=60)
    if resp.status_code not in (200, 201):
        print(f"\n  ⚠️  Supabase error {resp.status_code}: {resp.text[:400]}")
        resp.raise_for_status()

# ── JSON streaming helpers ────────────────────────────────────────────────────

def stream_file(filepath: str):
    """Stream JSON array items from file using ijson (memory-efficient)."""
    with open(filepath, "r", encoding="utf-8") as f:
        yield from ijson.items(f, "item")

def is_current(item: dict) -> bool:
    """True if the record is currently valid (not ended)."""
    return item.get("virkningTil") is None and item.get("registreringTil") is None

# ── Import passes ─────────────────────────────────────────────────────────────

def load_virksomhed(filepath: str, limit: int | None) -> dict:
    """Pass 1: Load all companies → {id: {cvr, status, stiftelsesdato}}"""
    companies = {}
    for item in stream_file(filepath):
        cid = item.get("id")
        if not cid:
            continue
        # TotalDownload Current has one record per company, but guard duplicates
        if cid not in companies:
            companies[cid] = {
                "cvr":           str(item["CVRNummer"]),
                "status":        item.get("status", "ukendt"),
                "stiftelsesdato": item.get("virksomhedStartdato"),
            }
        if limit and len(companies) >= limit:
            break
    return companies


def load_navne(filepath: str, company_ids: set) -> dict:
    """Pass 2: Load primary name per company → {id: name}"""
    navne = {}
    best_sekvens = {}  # id -> lowest sekvens seen
    for item in stream_file(filepath):
        cid = item.get("CVREnhedsId")
        if cid not in company_ids:
            continue
        seq = item.get("sekvens", 999)
        # Keep sekvens=0 current name; fall back to any current if sekvens=0 not found
        if is_current(item) and seq < best_sekvens.get(cid, 999):
            navne[cid] = item.get("vaerdi", "")
            best_sekvens[cid] = seq
    return navne


def load_adresser(filepath: str, company_ids: set) -> dict:
    """Pass 3: Load primary address per company → {id: {vejnavn,...}}"""
    adresser: dict = {}
    # Prefer 'beliggenhedsadresse', fallback to any current address
    anv_priority = {"beliggenhedsadresse": 0, "postadresse": 1}
    best_priority: dict = {}

    for item in stream_file(filepath):
        cid = item.get("CVREnhedsId")
        if cid not in company_ids:
            continue
        if not is_current(item):
            continue
        anv = item.get("AdresseringAnvendelse", "")
        pri = anv_priority.get(anv, 2)
        if pri < best_priority.get(cid, 99):
            best_priority[cid] = pri
            adresser[cid] = {
                "vejnavn":     item.get("CVRAdresse_vejnavn"),
                "husnummer":   item.get("CVRAdresse_husnummerFra"),
                "postnummer":  item.get("CVRAdresse_postnummer"),
                "postdistrikt": item.get("CVRAdresse_postdistrikt"),
                "kommunenavn": item.get("CVRAdresse_kommunenavn"),
                "kommunekode": item.get("CVRAdresse_kommunekode"),
            }
    return adresser


def load_brancher(filepath: str, company_ids: set) -> dict:
    """Pass 4: Load primary branch per company → {id: {branchekode, branchetekst}}"""
    brancher: dict = {}
    for item in stream_file(filepath):
        cid = item.get("CVREnhedsId")
        if cid not in company_ids:
            continue
        if item.get("sekvens") == 0 and is_current(item):
            brancher[cid] = {
                "branchekode":  item.get("vaerdi"),
                "branchetekst": item.get("vaerdiTekst"),
            }
    return brancher


def load_simple(filepath: str, company_ids: set, field: str = "vaerdi") -> dict:
    """Generic pass: load first current value per company → {id: value}"""
    result: dict = {}
    for item in stream_file(filepath):
        cid = item.get("CVREnhedsId")
        if cid not in company_ids or cid in result:
            continue
        if is_current(item):
            result[cid] = item.get(field)
    return result


def load_virksomhedsform(filepath: str, company_ids: set) -> dict:
    """Pass 7: Load company type → {id: vaerdiTekst}"""
    return load_simple(filepath, company_ids, field="vaerdiTekst")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Import CVR JSON → Supabase")
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR,
                        help=f"Directory with JSON files (default: {DEFAULT_DATA_DIR})")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit to N companies (for testing, e.g. --limit 5000)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    args = parser.parse_args()

    data_dir = args.data_dir
    t_total = time.time()

    def fp(key):
        return os.path.join(data_dir, FILES[key])

    # Verify files exist
    for key, fname in FILES.items():
        path = os.path.join(data_dir, fname)
        if not os.path.exists(path):
            print(f"❌  Missing file: {path}")
            sys.exit(1)
    print(f"✅  All 7 data files found in {data_dir}\n")

    # ── Pass 1: Virksomhed ────────────────────────────────────────────────────
    t = time.time()
    print("Pass 1/7  Virksomhed ... ", end="", flush=True)
    companies = load_virksomhed(fp("virksomhed"), args.limit)
    company_ids = set(companies.keys())
    active = sum(1 for c in companies.values() if c["status"] == "aktiv")
    print(f"{len(companies):,} companies ({active:,} active) — {time.time()-t:.0f}s")

    # ── Pass 2: Navn ──────────────────────────────────────────────────────────
    t = time.time()
    print("Pass 2/7  Navn ...       ", end="", flush=True)
    navne = load_navne(fp("navn"), company_ids)
    print(f"{len(navne):,} names — {time.time()-t:.0f}s")

    # ── Pass 3: Adressering ───────────────────────────────────────────────────
    t = time.time()
    print("Pass 3/7  Adressering .. ", end="", flush=True)
    adresser = load_adresser(fp("adressering"), company_ids)
    print(f"{len(adresser):,} addresses — {time.time()-t:.0f}s")

    # ── Pass 4: Branche ───────────────────────────────────────────────────────
    t = time.time()
    print("Pass 4/7  Branche ...    ", end="", flush=True)
    brancher = load_brancher(fp("branche"), company_ids)
    print(f"{len(brancher):,} branches — {time.time()-t:.0f}s")

    # ── Pass 5: Telefonnummer ─────────────────────────────────────────────────
    t = time.time()
    print("Pass 5/7  Telefonnummer  ", end="", flush=True)
    telefoner = load_simple(fp("telefon"), company_ids)
    print(f"{len(telefoner):,} phones — {time.time()-t:.0f}s")

    # ── Pass 6: e-mailadresse ─────────────────────────────────────────────────
    t = time.time()
    print("Pass 6/7  e-mail ...     ", end="", flush=True)
    emails = load_simple(fp("email"), company_ids)
    print(f"{len(emails):,} emails — {time.time()-t:.0f}s")

    # ── Pass 7: Virksomhedsform ───────────────────────────────────────────────
    t = time.time()
    print("Pass 7/7  Virksomhedsform", end="", flush=True)
    former = load_virksomhedsform(fp("form"), company_ids)
    print(f" {len(former):,} forms — {time.time()-t:.0f}s")

    # ── Build flat records + upload ───────────────────────────────────────────
    print(f"\nUploading to Supabase ...")
    t_upload = time.time()
    batch: list = []
    total = 0
    errors = 0

    for cid, base in companies.items():
        adr    = adresser.get(cid, {})
        branch = brancher.get(cid, {})

        record = {
            "cvr":            base["cvr"],
            "navn":           navne.get(cid),
            "status":         base["status"],
            "stiftelsesdato": base["stiftelsesdato"],
            "vejnavn":        adr.get("vejnavn"),
            "husnummer":      adr.get("husnummer"),
            "postnummer":     adr.get("postnummer"),
            "postdistrikt":   adr.get("postdistrikt"),
            "kommunenavn":    adr.get("kommunenavn"),
            "kommunekode":    adr.get("kommunekode"),
            "branchekode":    branch.get("branchekode"),
            "branchetekst":   branch.get("branchetekst"),
            "virksomhedsform": former.get(cid),
            "telefon":        telefoner.get(cid),
            "email":          emails.get(cid),
        }
        batch.append(record)

        if len(batch) >= args.batch_size:
            try:
                upsert_batch(batch)
            except Exception as e:
                errors += 1
                print(f"\n  Batch error (skipping): {e}")
            total += len(batch)
            batch = []

            if total % 10_000 == 0:
                elapsed = time.time() - t_upload
                rate = total / elapsed
                pct = total / len(companies) * 100
                print(f"  {total:>8,} / {len(companies):,}  ({pct:.0f}%)  {rate:.0f} rows/s", flush=True)

    # Final batch
    if batch:
        try:
            upsert_batch(batch)
        except Exception as e:
            errors += 1
            print(f"\n  Final batch error: {e}")
        total += len(batch)

    elapsed_total = time.time() - t_total
    print(f"\n{'='*50}")
    print(f"✅  Done! {total:,} companies uploaded in {elapsed_total/60:.1f} min")
    if errors:
        print(f"⚠️  {errors} batch errors (those rows may be missing)")
    print(f"\nNext: deploy the MCP server and test it in Claude.ai")

if __name__ == "__main__":
    main()
