#!/usr/bin/env python3
"""
Manual sync: fetch all CVR changes since a given date via GraphQL and upsert to Supabase.
Use this to catch up on companies registered/changed since the original bulk import.

Usage:
    export SUPABASE_KEY=sb_secret_...
    python manual_sync.py --since 2026-04-11
"""

import requests, json, os, sys, time, argparse, re
from datetime import datetime, timezone


def title_case(s):
    """Convert ALL CAPS to title case, e.g. 'KØBENHAVN' → 'København'."""
    if not s:
        return s
    return re.sub(r"(?:^|\s|-)\S", lambda m: m.group().upper(), s.lower())

SUPABASE_URL = "https://cvbtnmqchpzgsjcjcorj.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
DF_KEY       = "5y7biTvR2sNblgl8v3ZLbG1JOVSF6MAS67E2EBadQkKppHZXx8cTD8OrzIKG2DqO6UZW7TUCibZnmq1tDVW10BxQqj68ar39y"
GQL_URL      = f"https://graphql.datafordeler.dk/CVR/v1?apikey={DF_KEY}"
PAGE_SIZE    = 200
BATCH        = 50
UPSERT_BATCH = 500

if not SUPABASE_KEY:
    print("Error: set SUPABASE_KEY"); sys.exit(1)

sb = requests.Session()
sb.headers.update({
    "Authorization": f"Bearer {SUPABASE_KEY}", "apikey": SUPABASE_KEY,
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=minimal",
})

gql_s = requests.Session()
gql_s.headers.update({"Content-Type": "application/json"})

def gql(q):
    return gql_s.post(GQL_URL, json={"query": q}, timeout=30).json()

def sb_upsert(rows):
    if not rows: return
    r = sb.post(f"{SUPABASE_URL}/rest/v1/companies", data=json.dumps(rows), timeout=60)
    if r.status_code not in (200, 201):
        print(f"  ⚠️  {r.status_code}: {r.text[:200]}")

def get_changed(since, cursor=None):
    after = f', after: "{cursor}"' if cursor else ""
    return gql(f"""{{
      CVR_Virksomhed(first: {PAGE_SIZE}, where: {{ registreringFra: {{ gt: "{since}" }} }}{after}) {{
        nodes {{ id CVRNummer status virksomhedStartdato registreringFra }}
        pageInfo {{ hasNextPage endCursor }}
      }}
    }}""")

def batch_fetch(entity, ids, fields, filter_extra=""):
    id_list = ", ".join(f'"{i}"' for i in ids)
    return gql(f"""{{
      {entity}(first: {len(ids)*5}, where: {{ CVREnhedsId: {{ in: [{id_list}] }}{filter_extra} }}) {{
        nodes {{ CVREnhedsId {fields} }}
      }}
    }}""")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", default="2026-04-11T00:00:00Z")
    args = parser.parse_args()

    since = args.since
    if "T" not in since: since += "T00:00:00Z"

    print(f"Syncing changes since {since}...")
    t_total = time.time()

    # Pass 1: collect all changed company IDs
    all_changed = []
    cursor = None
    page = 0
    while True:
        data = get_changed(since, cursor)
        nodes = data.get("data", {}).get("CVR_Virksomhed", {}).get("nodes", [])
        pi    = data.get("data", {}).get("CVR_Virksomhed", {}).get("pageInfo", {})
        all_changed.extend(nodes)
        page += 1
        if page % 10 == 0:
            print(f"  Page {page}: {len(all_changed):,} changed companies so far...")
        if not pi.get("hasNextPage"): break
        cursor = pi["endCursor"]

    print(f"Found {len(all_changed):,} changed companies (including duplicates)")

    # Deduplicate: same CVR can appear multiple times — keep latest registreringFra
    seen = {}
    for c in all_changed:
        cvr = str(c["CVRNummer"])
        if cvr not in seen or c.get("registreringFra", "") > seen[cvr].get("registreringFra", ""):
            seen[cvr] = c
    all_changed = list(seen.values())
    print(f"After dedup: {len(all_changed):,} unique companies")

    if not all_changed:
        print("Nothing to sync!"); return

    # Pass 2: batch fetch all entity data and upsert
    print("Fetching full data and upserting...")
    upsert_batch = []
    total = 0

    for i in range(0, len(all_changed), BATCH):
        chunk = all_changed[i:i+BATCH]
        ids   = [c["id"] for c in chunk]
        base  = {c["id"]: c for c in chunk}

        navne   = {n["CVREnhedsId"]: n["vaerdi"]
                   for n in (batch_fetch("CVR_Navn", ids,
                     "vaerdi sekvens virkningTil",
                     ', virkningTil: { eq: null }')
                     .get("data", {}).get("CVR_Navn", {}).get("nodes") or [])}

        adrs_raw = (batch_fetch("CVR_Adressering", ids,
                    "CVRAdresse_vejnavn CVRAdresse_husnummerFra CVRAdresse_postnummer CVRAdresse_postdistrikt CVRAdresse_kommunenavn CVRAdresse_kommunekode AdresseringAnvendelse",
                    ', registreringTil: { eq: null }')
                    .get("data", {}).get("CVR_Adressering", {}).get("nodes") or [])
        adrs = {}
        for a in adrs_raw:
            eid = a["CVREnhedsId"]
            if eid not in adrs or a["AdresseringAnvendelse"] == "beliggenhedsadresse":
                adrs[eid] = a

        brs   = {n["CVREnhedsId"]: n
                 for n in (batch_fetch("CVR_Branche", ids,
                   "vaerdi vaerdiTekst",
                   ', sekvens: { eq: 0 }, virkningTil: { eq: null }')
                   .get("data", {}).get("CVR_Branche", {}).get("nodes") or [])}

        tels  = {n["CVREnhedsId"]: n["vaerdi"]
                 for n in (batch_fetch("CVR_Telefonnummer", ids,
                   "vaerdi", ', virkningTil: { eq: null }')
                   .get("data", {}).get("CVR_Telefonnummer", {}).get("nodes") or [])}

        mails = {n["CVREnhedsId"]: n["vaerdi"]
                 for n in (batch_fetch("CVR_Emailadresse", ids,
                   "vaerdi", ', virkningTil: { eq: null }')
                   .get("data", {}).get("CVR_Emailadresse", {}).get("nodes") or [])}

        forms = {n["CVREnhedsId"]: n["vaerdiTekst"]
                 for n in (batch_fetch("CVR_Virksomhedsform", ids,
                   "vaerdiTekst", ', virkningTil: { eq: null }')
                   .get("data", {}).get("CVR_Virksomhedsform", {}).get("nodes") or [])}

        for c in chunk:
            eid = c["id"]
            adr = adrs.get(eid, {})
            br  = brs.get(eid, {})
            upsert_batch.append({
                "cvr":            str(c["CVRNummer"]),
                "navn":           navne.get(eid),
                "status":         c["status"],
                "stiftelsesdato": c["virksomhedStartdato"],
                "vejnavn":        adr.get("CVRAdresse_vejnavn"),
                "husnummer":      adr.get("CVRAdresse_husnummerFra"),
                "postnummer":     adr.get("CVRAdresse_postnummer"),
                "postdistrikt":   adr.get("CVRAdresse_postdistrikt"),
                "kommunenavn":    title_case(adr.get("CVRAdresse_kommunenavn")),
                "kommunekode":    adr.get("CVRAdresse_kommunekode"),
                "branchekode":    br.get("vaerdi"),
                "branchetekst":   br.get("vaerdiTekst"),
                "virksomhedsform": forms.get(eid),
                "telefon":        tels.get(eid),
                "email":          mails.get(eid),
            })

        if len(upsert_batch) >= UPSERT_BATCH:
            sb_upsert(upsert_batch)
            total += len(upsert_batch)
            upsert_batch = []

        if (i // BATCH) % 20 == 0:
            print(f"  {i+len(chunk):>6,} / {len(all_changed):,}  ({(i+len(chunk))/len(all_changed)*100:.0f}%)", flush=True)

    if upsert_batch:
        sb_upsert(upsert_batch)
        total += len(upsert_batch)

    print(f"\n✅ Done! {total:,} companies synced in {(time.time()-t_total)/60:.1f} min")

if __name__ == "__main__":
    main()
