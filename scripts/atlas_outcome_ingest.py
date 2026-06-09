#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A.T.L.A.S. outcome ingester -- the missing outcome signal for the Smart Brain.

WHAT THIS IS
------------
The box-side half of the TuaniChat demo/chat OUTCOME LOOP. It pulls the small,
NON-PII aggregate JSON that the WordPress companion plugin (tnc-outcome-loop)
publishes to the manifests repo at `status/wp/tnc-outcomes.json`, and UPSERTs
those funnel counts into `atlas.outcome_stats` -- the exact table the Smart
Brain (seq-13, atlas_smart_brain.py) already reads to learn EV per (source,
path). We tag `path = industry`, so the brain finally learns expected value per
SOURCE x INDUSTRY from real demo opens / chats / leads / conversions.

Before this script, atlas.outcome_stats was empty and the OutcomeFeedbackRanker
sat at its neutral FIFO prior (learning_state = neutral_fifo_needs_live_outcomes).
This is what flips it to `learned`.

DATA FLOW (reverse of atlas_metrics_export.py -- same channel, no new surface)
------------------------------------------------------------------------------
  WordPress (chat.lionclickmedia.com)            Hetzner box (Postgres tuanichat_atlas)
    tnc-outcome-loop, WP-cron 15min                 atlas-outcome-ingest.timer (15min)
      computes per source+industry funnel             GET raw.githubusercontent.com/<repo>/
      PUT status/wp/tnc-outcomes.json  ───────────►   status/wp/tnc-outcomes.json
      (GitHub Contents API, STATUS_TOKEN)             UPSERT atlas.outcome_stats(source, path=industry)

- We only READ a static JSON over HTTPS from raw.githubusercontent.com (no DB
  creds on WordPress, no inbound port on the box).
- The payload is aggregate counts ONLY (per source+industry): demo_opened,
  chatted, lead_captured, converted. No PII, no contacts, no business rows.
- DB write is SCOPED: this script touches ONLY atlas.outcome_stats, via an
  additive UPSERT (INSERT ... ON CONFLICT DO UPDATE SET = the published value).
  It never ALTERs schema and never touches any other table.

MAPPING (WordPress funnel -> atlas.outcome_stats columns)
---------------------------------------------------------
  outcome_stats.source       <- payload source        (lead_hunter / manual / import ...)
  outcome_stats.path         <- payload industry       (med_spa / hvac / solar ...)
  outcome_stats.enriched     <- demo_opened            (a built+opened demo == an actioned lead)
  outcome_stats.contactable  <- chatted                (engaged == reachable / interested)
  outcome_stats.replied      <- lead_captured          (a real inbound contact)
  outcome_stats.converted    <- converted              (won / installed)
  outcome_stats.bounced      <- (left as-is; bounces come from the email engine)

The WordPress side publishes CUMULATIVE windowed totals, so the UPSERT SETs the
demo/chat-derived columns to the published value (idempotent -- re-running never
double counts). `bounced` is preserved (COALESCE), since it is owned by a
different lane (the email engine), not the demo loop.

DB CREDS come from /etc/atlas/db.env exactly like the other importers
(PGHOST/PGDATABASE/PGUSER/PGPASSWORD or DB_* / ATLAS_DB_* aliases).

USAGE
-----
  atlas_outcome_ingest.py            ingest once (the timer's normal run)
  atlas_outcome_ingest.py --selftest validate env loads, DB connect, table present
  atlas_outcome_ingest.py --migrate  CREATE TABLE IF NOT EXISTS atlas.outcome_stats
                                      (idempotent; the brain creates it too)
"""

import json
import os
import sys
import urllib.request

try:
    import psycopg2
except Exception:  # pragma: no cover
    psycopg2 = None

DB_ENV_PATH       = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
AUTOPULL_ENV_PATH = os.environ.get("ATLAS_AUTOPULL_CONF", "/etc/atlas/autopull.env")

# Where to read the WordPress-published outcomes. Overridable via env; default
# resolves the raw URL from STATUS_REPO (the same repo status-back already uses).
OUTCOMES_RAW_URL  = os.environ.get("TNC_OUTCOMES_URL", "")
OUTCOMES_REPO_PATH = os.environ.get("TNC_OUTCOMES_PATH", "status/wp/tnc-outcomes.json")


def log(msg):
    sys.stderr.write("[outcome-ingest] %s\n" % msg)


def load_env_file(path):
    """Source KEY=VALUE (with or without 'export'), exactly like the live importers."""
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
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def pick(*names, **kw):
    default = kw.get("default")
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
        application_name="atlas_outcome_ingest",
    )
    conn.autocommit = False
    return conn


def resolve_outcomes_url():
    if OUTCOMES_RAW_URL:
        return OUTCOMES_RAW_URL
    repo = os.environ.get("STATUS_REPO", "")
    branch = os.environ.get("STATUS_BRANCH", "main")
    if not repo:
        return ""
    return "https://raw.githubusercontent.com/%s/%s/%s" % (repo, branch, OUTCOMES_REPO_PATH)


def fetch_payload(url):
    req = urllib.request.Request(url, headers={"User-Agent": "atlas-outcome-ingest"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def ensure_table(conn):
    """Idempotent CREATE -- matches the brain's own DDL exactly (seq-13)."""
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS atlas")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS atlas.outcome_stats (
            source      text NOT NULL,
            path        text NOT NULL DEFAULT 'default',
            enriched    bigint NOT NULL DEFAULT 0,
            contactable bigint NOT NULL DEFAULT 0,
            bounced     bigint NOT NULL DEFAULT 0,
            replied     bigint NOT NULL DEFAULT 0,
            converted   bigint NOT NULL DEFAULT 0,
            updated_at  timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT outcome_stats_pk PRIMARY KEY (source, path)
        )""")
    conn.commit()
    cur.close()


def upsert(conn, stats):
    """Additive, idempotent UPSERT of the demo/chat-derived columns. bounced is
    preserved (owned by the email lane, not this loop)."""
    cur = conn.cursor()
    n = 0
    for row in stats:
        source = (row.get("source") or "manual").strip()[:64]
        path   = (row.get("path") or "default").strip()[:80]
        enriched    = int(row.get("enriched") or 0)
        contactable = int(row.get("contactable") or 0)
        replied     = int(row.get("replied") or 0)
        converted   = int(row.get("converted") or 0)
        cur.execute("""
            INSERT INTO atlas.outcome_stats
                (source, path, enriched, contactable, replied, converted, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (source, path) DO UPDATE SET
                enriched    = EXCLUDED.enriched,
                contactable = EXCLUDED.contactable,
                replied     = EXCLUDED.replied,
                converted   = EXCLUDED.converted,
                bounced     = atlas.outcome_stats.bounced,
                updated_at  = now()
        """, (source, path, enriched, contactable, replied, converted))
        n += 1
    conn.commit()
    cur.close()
    return n


def run_ingest():
    load_env_file(DB_ENV_PATH)
    load_env_file(AUTOPULL_ENV_PATH)
    url = resolve_outcomes_url()
    if not url:
        log("no outcomes URL (set TNC_OUTCOMES_URL or STATUS_REPO) -> nothing to do")
        return 0
    try:
        payload = fetch_payload(url)
    except Exception as e:
        log("fetch failed (%s) -> skip this cycle (fail-soft)" % e)
        return 0
    if payload.get("schema") != "tnc.outcomes.v1":
        log("unexpected payload schema %r -> skip" % payload.get("schema"))
        return 0
    stats = payload.get("stats") or []
    if not stats:
        log("payload has 0 stat rows -> nothing to ingest yet")
        return 0
    conn = connect_pg()
    try:
        ensure_table(conn)
        n = upsert(conn, stats)
        log("ingested %d source/industry outcome rows from %s" % (n, payload.get("generated_at")))
        return n
    finally:
        conn.close()


def selftest():
    load_env_file(DB_ENV_PATH)
    load_env_file(AUTOPULL_ENV_PATH)
    ok = True
    if psycopg2 is None:
        log("FAIL psycopg2 not installed"); ok = False
    try:
        conn = connect_pg()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        ensure_table(conn)
        cur.execute("SELECT count(*) FROM atlas.outcome_stats")
        log("outcome_stats rows: %d" % cur.fetchone()[0])
        cur.close()
        conn.close()
    except Exception as e:
        log("FAIL db connect/table (%s)" % e); ok = False
    url = resolve_outcomes_url()
    log("outcomes URL: %s" % (url or "(unset)"))
    print("SELFTEST %s" % ("OK" if ok else "FAILED"))
    return 0 if ok else 1


def main():
    if "--selftest" in sys.argv:
        sys.exit(selftest())
    if "--migrate" in sys.argv:
        load_env_file(DB_ENV_PATH)
        conn = connect_pg()
        ensure_table(conn)
        conn.close()
        print("migrate OK")
        return
    run_ingest()


if __name__ == "__main__":
    main()
