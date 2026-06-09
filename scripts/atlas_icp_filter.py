#!/opt/atlas/venv/bin/python
"""
atlas_icp_filter.py  --  ICP (Ideal Customer Profile) gate for ATLAS intake + retro-sweep

PURPOSE (per UNIFIED_SCALE_TARGETING_STORAGE_PLAN.md, problem 2):
  Stop hoarding low-value rows. Keep only businesses that fit TuaniChat's product:
    - US,
    - in a target vertical (law / dental / med-spa / roofing / home-services / medical / ...),
    - with a crawlable website OR a discoverable name+city (so a domain CAN be found),
    - NOT a hospital / .edu / .gov / .mil / AmLaw-200 / national franchise / do-not-contact.

  Two uses:
    1) IMPORTABLE GATE: collectors call `icp_decision(row)` BEFORE inserting a business,
       so junk never lands (the cheapest possible filter).
    2) RETRO-SWEEP: this script run directly tags EXISTING atlas.business rows with
       lifecycle='icp_fail' (and optionally prunes them) so the 1.22M already-banked
       rows get classified without a re-import.

SAFETY / DISCIPLINE (mirrors socrata_import.py + atlas_enrich_worker.py):
  - Connection from /etc/atlas/db.env (PG* / DB_* / ATLAS_DB_* vars), psycopg2.
  - Schema-introspected at startup; NO CREATE/ALTER/DROP of existing columns.
    (It only writes to atlas.business.lifecycle, an existing text column.)
  - --selftest runs the pure-python decision logic on fixtures, NO DB writes, exit 0/1.
  - --dry-run (DEFAULT for the sweep) reports counts only, writes nothing.
  - Batched, idempotent, fail-soft per row, fail-loud only on DB-down/missing tables.
  - Non-overridable .gov/.mil suppression is in code.
  - PRUNE is OFF by default (ATLAS_ICP_PRUNE=1 to actually DELETE icp_fail rows);
    even then it deletes in bounded batches and never touches rows with provenance
    unless ATLAS_ICP_PRUNE_ENRICHED=1.

This script writes /var/lib/atlas/autopull/last_counts.json for status-back.
"""

import os
import re
import sys
import json
import time

# psycopg2 is imported lazily inside connect_pg() so that --selftest (pure logic,
# no DB) runs on hosts where the driver isn't installed yet. The manifest pip-installs
# it before the live run.

DB_ENV_PATH = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
SWEEP_BATCH = int(os.environ.get("ATLAS_ICP_BATCH", "5000"))
DRY_RUN     = os.environ.get("ATLAS_ICP_DRYRUN", "1") not in ("0", "false", "False")
DO_PRUNE    = os.environ.get("ATLAS_ICP_PRUNE", "0") in ("1", "true", "True")
PRUNE_ENRICHED = os.environ.get("ATLAS_ICP_PRUNE_ENRICHED", "0") in ("1", "true", "True")

COUNTS_PATH = os.environ.get(
    "ATLAS_COUNTS_PATH",
    os.path.join(os.environ.get("ATLAS_AUTOPULL_STATE", "/var/lib/atlas/autopull"),
                 "last_counts.json"),
)


def log(m):
    print(f"[atlas_icp_filter] {m}", flush=True)


# --------------------------------------------------------------------------- #
# ICP rule tables (the Brain reweights these over time; this is the seed)
# --------------------------------------------------------------------------- #
# Target verticals -> keyword/category tokens that indicate the vertical.
ICP_VERTICAL_TOKENS = {
    "law":            ["law", "lawyer", "attorney", "legal", "llp", "esq", "litigation"],
    "personal_injury":["injury", "accident", "trial", "personal injury"],
    "dental":         ["dental", "dentist", "orthodont", "endodont", "periodont"],
    "med_spa":        ["med spa", "medspa", "aesthetic", "botox", "laser", "skin clinic", "wellness spa"],
    "plastic_surgery":["plastic surgery", "cosmetic surgery", "rhinoplasty", "liposuction"],
    "roofing":        ["roofing", "roofer", "roof repair"],
    "hvac":           ["hvac", "heating", "air conditioning", "furnace", "ac repair"],
    "home_services":  ["plumb", "electrician", "remodel", "landscap", "pest control",
                       "garage door", "flooring", "painting", "fencing", "pool service"],
    "medical":        ["clinic", "physician", "family medicine", "urgent care", "chiropract",
                       "dermatolog", "optometr", "podiatr", "veterinar"],
    "real_estate":    ["realty", "real estate", "realtor", "broker", "properties"],
    "auto":           ["auto repair", "body shop", "tire", "transmission", "auto detail"],
    "accounting":     ["accounting", "cpa", "bookkeep", "tax service"],
}

# Hard EXCLUSIONS -- never an ICP buyer / reputation risk.
ICP_EXCLUDE_TOKENS = [
    "hospital", "medical center", "health system", "university", "college",
    "school district", "city of", "county of", "department of", "state of",
    "federal", "bureau", "amlaw", "fortune 500",
]
# Big national brands / franchises whose own domain is not the local biz.
ICP_EXCLUDE_DOMAINS = [
    "subway.com", "mcdonalds.com", "starbucks.com", "facebook.com", "yelp.com",
    "linktr.ee", "instagram.com", "google.com", "wix.com", "godaddy.com",
    "amazon.com", "walmart.com", "cvs.com", "walgreens.com",
]
EXCLUDE_TLDS = (".edu", ".gov", ".mil", ".fed.us")


def _txt(*vals):
    return " ".join(str(v) for v in vals if v).lower()


def is_us(row):
    country = (row.get("country") or "").strip().upper()
    if country in ("US", "USA", "UNITED STATES"):
        return True
    phone = row.get("phone_e164") or ""
    if phone.startswith("+1"):
        return True
    region = (row.get("region") or "").strip()
    # 2-letter US state code present is a decent US signal when country is blank
    if country == "" and re.fullmatch(r"[A-Za-z]{2}", region or ""):
        return True
    return False


def matched_vertical(row):
    hay = _txt(row.get("name"), row.get("category"), row.get("naics"), row.get("sic"))
    for vert, toks in ICP_VERTICAL_TOKENS.items():
        if any(t in hay for t in toks):
            return vert
    return None


def excluded(row):
    hay = _txt(row.get("name"), row.get("category"))
    if any(t in hay for t in ICP_EXCLUDE_TOKENS):
        return "exclude_token"
    dom = (row.get("domain") or row.get("website") or "").lower()
    if dom.endswith(EXCLUDE_TLDS) or any(t in dom for t in (".gov.", ".mil.")):
        return "gov_mil_edu"
    bare = re.sub(r"^https?://(www\.)?", "", dom).split("/")[0]
    if bare in ICP_EXCLUDE_DOMAINS:
        return "national_brand_domain"
    return None


def has_reach_signal(row):
    # crawlable website/domain, OR a name+city pair (so a domain can be discovered)
    if (row.get("website") or row.get("domain")):
        return True
    if (row.get("name") and row.get("city")):
        return True
    return False


def icp_decision(row):
    """Return (keep: bool, reason: str). Pure, no DB. The single source of truth."""
    ex = excluded(row)
    if ex:
        return (False, ex)
    if not is_us(row):
        return (False, "not_us")
    if not has_reach_signal(row):
        return (False, "no_reach_signal")
    vert = matched_vertical(row)
    if not vert:
        return (False, "no_target_vertical")
    return (True, "icp:" + vert)


# --------------------------------------------------------------------------- #
# DB plumbing (mirrors socrata_import.py exactly)
# --------------------------------------------------------------------------- #
def load_db_env(path):
    if not os.path.exists(path):
        log(f"WARNING: {path} not found; relying on existing environment.")
        return
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def connect_pg():
    import psycopg2
    def pick(*names, default=None):
        for n in names:
            if os.environ.get(n):
                return os.environ[n]
        return default
    conn = psycopg2.connect(
        host=pick("PGHOST", "DB_HOST", "ATLAS_DB_HOST", default="localhost"),
        port=pick("PGPORT", "DB_PORT", "ATLAS_DB_PORT", default="5432"),
        dbname=pick("PGDATABASE", "DB_NAME", "ATLAS_DB_NAME", default="tuanichat_atlas"),
        user=pick("PGUSER", "DB_USER", "ATLAS_DB_USER", default="atlas"),
        password=pick("PGPASSWORD", "DB_PASSWORD", "ATLAS_DB_PASSWORD", default=None),
    )
    conn.autocommit = False
    return conn


def assert_schema(cur):
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='atlas' AND table_name='business' AND column_name='lifecycle'
    """)
    if not cur.fetchone():
        sys.exit("FATAL: atlas.business.lifecycle column missing -- refusing to run.")


# --------------------------------------------------------------------------- #
# Retro-sweep over existing rows
# --------------------------------------------------------------------------- #
SELECT_COLS = ["id", "name", "category", "naics", "sic", "country",
               "region", "city", "domain", "website", "phone_e164"]


def sweep(conn):
    counts = {"scanned": 0, "icp_fail": 0, "icp_keep": 0,
              "tagged": 0, "pruned": 0, "dry_run": DRY_RUN, "prune": DO_PRUNE}
    cur = conn.cursor()
    assert_schema(cur)
    last_id = 0
    cols = ", ".join(SELECT_COLS)
    while True:
        cur.execute(
            f"SELECT {cols} FROM atlas.business "
            f"WHERE id > %s AND (lifecycle IS NULL OR lifecycle NOT IN ('icp_fail','icp_keep')) "
            f"ORDER BY id ASC LIMIT %s",
            (last_id, SWEEP_BATCH),
        )
        rows = cur.fetchall()
        if not rows:
            break
        fail_ids, keep_ids = [], []
        for r in rows:
            row = dict(zip(SELECT_COLS, r))
            last_id = row["id"]
            counts["scanned"] += 1
            keep, _reason = icp_decision(row)
            (keep_ids if keep else fail_ids).append(row["id"])
        counts["icp_keep"] += len(keep_ids)
        counts["icp_fail"] += len(fail_ids)
        if not DRY_RUN:
            wcur = conn.cursor()
            if keep_ids:
                wcur.execute("UPDATE atlas.business SET lifecycle='icp_keep' WHERE id = ANY(%s)", (keep_ids,))
                counts["tagged"] += wcur.rowcount
            if fail_ids:
                wcur.execute("UPDATE atlas.business SET lifecycle='icp_fail' WHERE id = ANY(%s)", (fail_ids,))
                counts["tagged"] += wcur.rowcount
            conn.commit()
        if DO_PRUNE and not DRY_RUN and fail_ids:
            pcur = conn.cursor()
            if PRUNE_ENRICHED:
                pcur.execute("DELETE FROM atlas.business WHERE id = ANY(%s)", (fail_ids,))
            else:
                # only prune icp_fail rows that have NO provenance (never enriched)
                pcur.execute(
                    "DELETE FROM atlas.business b WHERE b.id = ANY(%s) "
                    "AND NOT EXISTS (SELECT 1 FROM atlas.field_provenance p WHERE p.business_id=b.id)",
                    (fail_ids,),
                )
            counts["pruned"] += pcur.rowcount
            conn.commit()
    return counts


def write_counts(payload):
    try:
        os.makedirs(os.path.dirname(COUNTS_PATH), exist_ok=True)
        with open(COUNTS_PATH, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)
    except Exception as e:  # status-back is best-effort
        log(f"WARN could not write counts: {e}")


# --------------------------------------------------------------------------- #
# Selftest (NO DB) -- fixtures exercise the decision logic
# --------------------------------------------------------------------------- #
def selftest():
    cases = [
        ({"name": "Smith Dental Care", "city": "Naples", "country": "US",
          "website": "smithdental.com"}, True),
        ({"name": "Beverly Hills Med Spa", "region": "CA", "phone_e164": "+13105551212",
          "website": "bhmedspa.com"}, True),
        ({"name": "County of Cook", "country": "US", "website": "cookcountyil.gov"}, False),
        ({"name": "Harvard University", "country": "US", "website": "harvard.edu"}, False),
        ({"name": "Joe's Coffee Shop", "city": "Austin", "country": "US",
          "website": "joescoffee.com"}, False),  # not a target vertical
        ({"name": "Apex Roofing", "city": "Scottsdale", "country": "US"}, True),  # name+city reach
        ({"name": "Random GmbH", "country": "DE", "website": "random.de"}, False),  # not US
        ({"name": "Subway", "country": "US", "website": "subway.com"}, False),  # national brand
        ({"name": "Elite HVAC", "country": "US"}, False),  # no reach signal (no site, no city)
    ]
    ok = True
    for row, expect in cases:
        keep, reason = icp_decision(row)
        status = "PASS" if keep == expect else "FAIL"
        if keep != expect:
            ok = False
        log(f"  [{status}] {row.get('name'):26s} -> keep={keep} ({reason}) expected={expect}")
    log("SELFTEST OK" if ok else "SELFTEST FAILED")
    return 0 if ok else 1


def main():
    if "--selftest" in sys.argv:
        sys.exit(selftest())
    load_db_env(DB_ENV_PATH)
    try:
        conn = connect_pg()
    except Exception as e:
        sys.exit(f"FATAL: cannot connect to Postgres: {e}")
    t0 = time.time()
    counts = sweep(conn)
    counts["elapsed_s"] = round(time.time() - t0, 1)
    counts["ts"] = int(time.time())
    counts["lane"] = "icp_filter"
    write_counts(counts)
    log(json.dumps(counts))
    # never fail merely because an idempotent re-run tagged 0 (would wedge a timer)
    sys.exit(0)


if __name__ == "__main__":
    main()
