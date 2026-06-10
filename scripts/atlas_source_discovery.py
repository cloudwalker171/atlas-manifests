#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A.T.L.A.S. SOURCE-DISCOVERY STATE MACHINE -- never wire a 0-row source again.

WHAT THIS IS (review item 6)
----------------------------
Upgrades the daily hunter / source promoter into a disciplined state machine:

  SOURCE_CANDIDATE -> TESTED -> APPROVED -> WIRED -> MONITORED
                        |                              |
                        +--> REJECTED                  +--> DEGRADED

A source NEVER auto-ingests blindly. Before it can be APPROVED (and only an
APPROVED source may be WIRED to a collector), it must pass:
  * LEGAL-ACCESS classification   -- open-data / public-record / API-with-ToS-ok
                                     / SCRAPE-FORBIDDEN. Anything ToS-protected
                                     (LinkedIn/Indeed/Yelp/GBP/social) is hard
                                     REJECTED here -- can never reach WIRED.
  * UPDATE-FREQUENCY classification (realtime / daily / weekly / static / unknown)
  * RELEVANCE classification        (firmographic fit to the ICP: business
                                     identity / birth signal / contact / none)
  * a SAMPLE-ROW TEST              -- fetch a small sample and PROVE it yields
                                     >= MIN_SAMPLE_ROWS parseable, relevant rows.
                                     This is the exact guard that would have
                                     stopped the GLEIF/CKAN "deployed but 0 rows"
                                     situation: no rows in the sample => stays
                                     TESTED/REJECTED, never APPROVED, never WIRED.

It then RECOMMENDS a wiring priority (it does not itself deploy a collector --
wiring a collector is a signed manifest the publish session ships; this engine
catalogs, tests, classifies and recommends).

SAFETY: additive. Owns atlas.source_catalog (the state machine + classifications
+ last sample result). The sample-row test uses an INJECTABLE fetcher so the
selftest runs fully OFFLINE (no egress); on the box the real fetcher pulls a
bounded sample (open-data/API only -- SCRAPE-FORBIDDEN sources are rejected
before any fetch). Never writes business rows; never ALTERs other tables.
ToS-protected sources are refused. .gov public records may be SOURCED (not used
as contacts).

MODES: --migrate / --selftest / --once (classify+test pending candidates) /
       --loop / --seed '<json>' (add a candidate) / --report
"""

import datetime
import json
import os
import sys
import time

try:
    import psycopg2
except Exception:  # pragma: no cover
    psycopg2 = None

DB_ENV_PATH       = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
AUTOPULL_ENV_PATH = os.environ.get("ATLAS_AUTOPULL_CONF", "/etc/atlas/autopull.env")
NODE_ID           = os.environ.get("NODE_ID", "hetzner")
INTERVAL_SEC      = int(os.environ.get("ATLAS_SRCDISC_INTERVAL", "3600"))
MIN_SAMPLE_ROWS   = int(os.environ.get("ATLAS_SRCDISC_MIN_ROWS", "3"))
STATE_DIR         = os.environ.get("ATLAS_SRCDISC_STATE_DIR", "/var/lib/atlas/srcdisc")

# states
CANDIDATE, TESTED, APPROVED, WIRED, MONITORED = \
    "SOURCE_CANDIDATE", "TESTED", "APPROVED", "WIRED", "MONITORED"
REJECTED, DEGRADED = "REJECTED", "DEGRADED"

# legal-access classes
LEGAL_OPEN_DATA = "open_data"
LEGAL_PUBLIC_RECORD = "public_record"
LEGAL_API_TOS_OK = "api_tos_ok"
LEGAL_SCRAPE_FORBIDDEN = "scrape_forbidden"

# ToS-protected hosts that are HARD-rejected (the review's red line)
FORBIDDEN_HOST_SUBSTR = (
    "linkedin.com", "indeed.com", "yelp.com", "glassdoor.", "google.com/maps",
    "business.google.", "facebook.com", "instagram.com", "twitter.com",
    "x.com", "tiktok.com", "ziprecruiter.", "monster.com",
)


def log(msg):
    print("[srcdisc] %s %s" %
          (datetime.datetime.now(datetime.timezone.utc).isoformat(), msg), flush=True)


def load_env_file(path):
    if not os.path.exists(path):
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


def pick(*names, default=None):
    for n in names:
        if os.environ.get(n):
            return os.environ[n]
    return default


def connect_pg():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not installed")
    conn = psycopg2.connect(
        host=pick("PGHOST", "DB_HOST", "ATLAS_DB_HOST", default="localhost"),
        port=pick("PGPORT", "DB_PORT", "ATLAS_DB_PORT", default="5432"),
        dbname=pick("PGDATABASE", "DB_NAME", "ATLAS_DB_NAME", default="tuanichat_atlas"),
        user=pick("PGUSER", "DB_USER", "ATLAS_DB_USER", default="atlas"),
        password=pick("PGPASSWORD", "DB_PASSWORD", "ATLAS_DB_PASSWORD", default=None),
        connect_timeout=int(os.environ.get("ATLAS_DB_CONNECT_TIMEOUT", "10")),
        application_name="atlas_source_discovery",
    )
    conn.autocommit = False
    return conn


def regclass_exists(cur, qualified):
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (qualified,))
    return bool(cur.fetchone()[0])


# --------------------------------------------------------------------------- #
# classification -- pure, offline-testable
# --------------------------------------------------------------------------- #
def classify_legal(url, declared=None):
    """Returns a legal-access class. A forbidden host -> scrape_forbidden (which
    makes the source un-wireable). Declared open-data/API stays as declared."""
    u = (url or "").lower()
    for bad in FORBIDDEN_HOST_SUBSTR:
        if bad in u:
            return LEGAL_SCRAPE_FORBIDDEN
    if declared in (LEGAL_OPEN_DATA, LEGAL_PUBLIC_RECORD, LEGAL_API_TOS_OK):
        return declared
    # heuristic defaults from the URL shape
    if any(k in u for k in ("data.gov", "socrata", "ckan", "opendata", "/api/",
                            "arcgis", "census.gov", "sec.gov", "irs.gov",
                            "gleif.org", "sam.gov", "uspto.gov", "/resource/",
                            "://data.", ".data.", "datacatalog", "open.")):
        return LEGAL_OPEN_DATA
    return LEGAL_API_TOS_OK if "/api" in u else LEGAL_PUBLIC_RECORD


def classify_frequency(declared=None):
    if declared in ("realtime", "daily", "weekly", "monthly", "static"):
        return declared
    return "unknown"


def classify_relevance(sample_rows, fields_seen):
    """Relevance to the ICP from what the sample actually contained. firmographic
    if it carries business identity; birth_signal if it carries a birth event;
    contact if emails/phones; none otherwise."""
    f = set(x.lower() for x in (fields_seen or []))
    if {"name", "business_name", "legal_name", "organization", "entity"} & f:
        if {"created", "registration_date", "founded", "issue_date", "permit_date"} & f:
            return "birth_signal"
        return "firmographic"
    if {"email", "phone", "contact"} & f:
        return "contact"
    return "none"


def can_approve(legal_class, relevance, sample_row_count):
    """The gate to APPROVED. Must be lawful, relevant, AND have proven sample rows
    (the GLEIF/CKAN 0-row fix)."""
    reasons = []
    if legal_class == LEGAL_SCRAPE_FORBIDDEN:
        reasons.append("legal_scrape_forbidden")
    if relevance == "none":
        reasons.append("not_relevant_to_icp")
    if (sample_row_count or 0) < MIN_SAMPLE_ROWS:
        reasons.append("sample_below_min_rows(%d<%d)" % (sample_row_count or 0, MIN_SAMPLE_ROWS))
    return (len(reasons) == 0), reasons


# --------------------------------------------------------------------------- #
# the sample-row test -- INJECTABLE fetcher so selftest is offline.
# fetcher(url) -> list[dict] (a small sample). Returns (rows, fields_seen).
# --------------------------------------------------------------------------- #
def sample_test(url, legal_class, fetcher):
    """NEVER fetches a scrape-forbidden source. Returns (count, fields, error)."""
    if legal_class == LEGAL_SCRAPE_FORBIDDEN:
        return 0, [], "refused_scrape_forbidden"
    try:
        rows = fetcher(url) or []
    except Exception as e:
        return 0, [], "fetch_error_%s" % e
    fields = set()
    for r in rows[:50]:
        if isinstance(r, dict):
            fields.update(r.keys())
    return len(rows), sorted(fields), None


def _box_fetcher(url):
    """Real bounded sample fetcher used ON THE BOX (open-data/API only; the
    scrape-forbidden gate runs BEFORE this is ever called). Pulls a tiny JSON
    sample. Kept conservative; the box has egress, the build sandbox does not
    (selftest injects a fixture fetcher instead)."""
    import urllib.request
    req = urllib.request.Request(url, headers={"User-Agent": "atlas-srcdisc-sample"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = json.loads(resp.read().decode("utf-8", "replace"))
    if isinstance(data, list):
        return data[:50]
    if isinstance(data, dict):
        for k in ("results", "data", "records", "rows", "items"):
            if isinstance(data.get(k), list):
                return data[k][:50]
        return [data]
    return []


# --------------------------------------------------------------------------- #
# DDL
# --------------------------------------------------------------------------- #
def ensure_schema(conn):
    # FAIL-SOFT: each DDL committed independently; a warning never crashes --migrate.
    cur = conn.cursor()
    _ddls = [
        "CREATE SCHEMA IF NOT EXISTS atlas",
        """CREATE TABLE IF NOT EXISTS atlas.source_catalog (
        source_key text PRIMARY KEY,
        name text, url text,
        state text NOT NULL DEFAULT 'SOURCE_CANDIDATE',
        legal_class text, update_frequency text, relevance text,
        sample_row_count int NOT NULL DEFAULT 0,
        sample_fields jsonb, sample_error text,
        wiring_priority double precision NOT NULL DEFAULT 0,
        reject_reasons jsonb,
        last_tested_at timestamptz, last_state_at timestamptz NOT NULL DEFAULT now(),
        created_at timestamptz NOT NULL DEFAULT now())""",
        "CREATE INDEX IF NOT EXISTS source_catalog_state ON atlas.source_catalog (state)",
    ]
    for _d in _ddls:
        try:
            cur.execute(_d); conn.commit()
        except Exception as _e:
            try: conn.rollback()
            except Exception: pass
            sys.stderr.write("ensure_schema warn: %s\n" % str(_e)[:140])
    try: cur.close()
    except Exception: pass


def seed_candidate(conn, cand):
    ensure_schema(conn)
    cur = conn.cursor()
    cur.execute("""INSERT INTO atlas.source_catalog
        (source_key, name, url, state, update_frequency, last_state_at)
        VALUES (%s,%s,%s,'SOURCE_CANDIDATE',%s, now())
        ON CONFLICT (source_key) DO UPDATE SET name=EXCLUDED.name, url=EXCLUDED.url""",
        (cand["source_key"], cand.get("name"), cand.get("url"),
         classify_frequency(cand.get("update_frequency"))))
    conn.commit()
    cur.close()


def process_candidates(conn, fetcher=None):
    """Classify + sample-test every CANDIDATE/TESTED; promote to APPROVED only on
    pass; else REJECTED (with reasons). Never auto-WIRES (that is a manifest)."""
    ensure_schema(conn)
    fetcher = fetcher or _box_fetcher
    cur = conn.cursor()
    cur.execute("SELECT source_key, name, url, update_frequency FROM atlas.source_catalog "
                "WHERE state IN ('SOURCE_CANDIDATE','TESTED')")
    rows = cur.fetchall()
    cur.close()
    stats = {"tested": 0, "approved": 0, "rejected": 0}
    for source_key, name, url, freq in rows:
        legal = classify_legal(url, declared=None)
        count, fields, err = sample_test(url, legal, fetcher)
        relevance = classify_relevance(count, fields)
        ok, reasons = can_approve(legal, relevance, count)
        new_state = APPROVED if ok else (REJECTED if (legal == LEGAL_SCRAPE_FORBIDDEN
                                                      or relevance == "none") else TESTED)
        # wiring priority: relevance weight x freshness weight x sample richness
        rel_w = {"birth_signal": 1.0, "firmographic": 0.8, "contact": 0.6, "none": 0.0}[relevance]
        freq_w = {"realtime": 1.0, "daily": 0.8, "weekly": 0.6, "monthly": 0.4,
                  "static": 0.3, "unknown": 0.3}.get(classify_frequency(freq), 0.3)
        rich = min(1.0, count / max(1.0, MIN_SAMPLE_ROWS * 3.0))
        priority = round(rel_w * freq_w * (0.5 + 0.5 * rich), 4) if ok else 0.0
        c2 = conn.cursor()
        c2.execute("""UPDATE atlas.source_catalog SET state=%s, legal_class=%s,
            update_frequency=%s, relevance=%s, sample_row_count=%s, sample_fields=%s,
            sample_error=%s, wiring_priority=%s, reject_reasons=%s,
            last_tested_at=now(), last_state_at=now() WHERE source_key=%s""",
            (new_state, legal, classify_frequency(freq), relevance, count,
             json.dumps(fields), err, priority,
             json.dumps(reasons) if reasons else None, source_key))
        conn.commit()
        c2.close()
        stats["tested"] += 1
        if new_state == APPROVED:
            stats["approved"] += 1
        elif new_state == REJECTED:
            stats["rejected"] += 1
        log("%s -> %s (legal=%s relevance=%s rows=%d priority=%.3f%s)" %
            (source_key, new_state, legal, relevance, count, priority,
             "" if ok else " reasons=%s" % reasons))
    return stats


def write_local(obj, path):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(obj, fh)
    except OSError as e:
        log("WARNING could not write %s: %s" % (path, e))


def run_once(conn):
    stats = process_candidates(conn)
    cur = conn.cursor()
    cur.execute("SELECT state, count(*) FROM atlas.source_catalog GROUP BY state")
    by_state = {s: int(c) for s, c in cur.fetchall()}
    cur.execute("""SELECT source_key, name, wiring_priority FROM atlas.source_catalog
                   WHERE state='APPROVED' ORDER BY wiring_priority DESC LIMIT 20""")
    recommend = [{"source_key": k, "name": n, "wiring_priority": float(p)}
                 for k, n, p in cur.fetchall()]
    cur.close()
    body = {"schema": "atlas.srcdisc.v1", "node": NODE_ID, "ts": int(time.time()),
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "by_state": by_state, "this_pass": stats,
            "recommended_wiring": recommend,
            "honesty": ("A source reaches APPROVED only after passing legal-access, "
                        "relevance AND a sample-row test (>=%d rows) -- the exact "
                        "guard that stops 'deployed but 0 rows'. WIRING is a signed "
                        "manifest the publish session ships, never auto-applied here. "
                        "ToS-protected sources are hard-REJECTED before any fetch."
                        % MIN_SAMPLE_ROWS)}
    write_local(body, os.path.join(STATE_DIR, "last_srcdisc.json"))
    log("by_state=%s recommended_wiring=%d" % (by_state, len(recommend)))
    return 0


def selftest():
    load_env_file(DB_ENV_PATH)
    load_env_file(AUTOPULL_ENV_PATH)
    ok = True
    # ---- classification asserts ----
    assert classify_legal("https://www.linkedin.com/company/x") == LEGAL_SCRAPE_FORBIDDEN
    assert classify_legal("https://data.cityofchicago.org/resource/abc.json") == LEGAL_OPEN_DATA
    assert classify_legal("https://api.sam.gov/entity") == LEGAL_OPEN_DATA
    log("legal: LinkedIn->scrape_forbidden; Socrata/SAM->open_data")
    # ---- the GLEIF/CKAN 0-row guard ----
    def empty_fetcher(url):
        return []  # simulates GLEIF/CKAN endpoint that returned nothing
    cnt, fields, err = sample_test("https://gleif.org/api/x", LEGAL_OPEN_DATA, empty_fetcher)
    relevance = classify_relevance(cnt, fields)
    approved, reasons = can_approve(LEGAL_OPEN_DATA, relevance, cnt)
    assert not approved, "0-row source must NOT be approvable (GLEIF/CKAN fix)"
    assert any("sample_below_min_rows" in r for r in reasons), "must cite the 0-row reason (%s)" % reasons
    log("0-row guard: empty source NOT approved (%s)" % reasons)
    # ---- a good open-data source with real sample rows is APPROVED ----
    def good_fetcher(url):
        return [{"business_name": "Acme LLC", "registration_date": "2026-06-01",
                 "city": "Reno", "state": "NV"},
                {"business_name": "Beta Inc", "registration_date": "2026-06-02",
                 "city": "Reno", "state": "NV"},
                {"business_name": "Gamma Co", "registration_date": "2026-06-03",
                 "city": "Reno", "state": "NV"},
                {"business_name": "Delta Ltd", "registration_date": "2026-06-04"}]
    cnt2, fields2, _ = sample_test("https://data.nv.gov/resource/biz.json", LEGAL_OPEN_DATA, good_fetcher)
    rel2 = classify_relevance(cnt2, fields2)
    appr2, _ = can_approve(LEGAL_OPEN_DATA, rel2, cnt2)
    assert rel2 == "birth_signal", "name+registration_date -> birth_signal (got %s)" % rel2
    assert appr2, "good open-data sample with birth signal must be APPROVED"
    log("good source: rows=%d relevance=%s -> APPROVED" % (cnt2, rel2))
    # ---- a scrape-forbidden source is refused BEFORE fetch ----
    cnt3, _, err3 = sample_test("https://yelp.com/biz", LEGAL_SCRAPE_FORBIDDEN, good_fetcher)
    assert cnt3 == 0 and err3 == "refused_scrape_forbidden", "forbidden source must be refused pre-fetch"
    appr3, reasons3 = can_approve(LEGAL_SCRAPE_FORBIDDEN, "firmographic", 10)
    assert not appr3 and "legal_scrape_forbidden" in reasons3, "forbidden can never be approved"
    log("forbidden: refused pre-fetch + un-approvable")
    # ---- DB ----
    offline = os.environ.get("ATLAS_SELFTEST_OFFLINE", "") not in ("0", "", "no", "false")
    if psycopg2 is None:
        log("%s psycopg2 not installed" % ("WARN(offline)" if offline else "FAIL"))
        if not offline:
            ok = False
    else:
        try:
            conn = connect_pg()
            cur = conn.cursor()
            cur.execute("SELECT 1"); cur.fetchone()
            ensure_schema(conn)
            cur.execute("SELECT count(*) FROM atlas.source_catalog")
            log("source_catalog rows: %d" % cur.fetchone()[0])
            cur.close(); conn.close()
        except Exception as e:
            log("%s db connect/schema (%s)" % ("WARN(offline)" if offline else "FAIL", e))
            if not offline:
                ok = False
    print("SELFTEST %s" % ("OK" if ok else "FAILED"))
    return 0 if ok else 1


def main():
    if "--selftest" in sys.argv:
        sys.exit(selftest())
    load_env_file(DB_ENV_PATH)
    load_env_file(AUTOPULL_ENV_PATH)
    os.makedirs(STATE_DIR, exist_ok=True)
    if "--migrate" in sys.argv:
        try:
            conn = connect_pg(); ensure_schema(conn); conn.close()
        except Exception as _e:
            sys.stderr.write("migrate warn: %s\n" % str(_e)[:140])
        print("migrate OK (fail-soft)")
        return
    if "--seed" in sys.argv:
        i = sys.argv.index("--seed")
        cand = json.loads(sys.argv[i + 1])
        conn = connect_pg()
        try:
            seed_candidate(conn, cand)
            print("seeded %s" % cand.get("source_key"))
        finally:
            conn.close()
        return
    if "--report" in sys.argv:
        conn = connect_pg()
        try:
            ensure_schema(conn)
            cur = conn.cursor()
            cur.execute("SELECT state, count(*) FROM atlas.source_catalog GROUP BY state")
            print(json.dumps({s: int(c) for s, c in cur.fetchall()}))
            cur.close()
        finally:
            conn.close()
        return
    if "--loop" in sys.argv:
        while True:
            try:
                conn = connect_pg()
                try:
                    run_once(conn)
                finally:
                    conn.close()
            except Exception as e:
                log("loop error (retry next interval): %s" % e)
            time.sleep(INTERVAL_SEC)
    conn = connect_pg()
    try:
        run_once(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
