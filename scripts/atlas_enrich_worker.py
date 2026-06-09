#!/opt/atlas/venv/bin/python
"""
atlas_enrich_worker.py  --  ALWAYS-ON, free-only business enrichment worker.

One long-running worker process. Many run side-by-side (systemd template unit
atlas-enrich-worker@1, @2, ...). They cooperate over a single Postgres work
queue using SELECT ... FOR UPDATE SKIP LOCKED, so N workers never claim the same
row and adding workers linearly adds throughput. Restart=always keeps each one
alive; an empty queue makes a worker idle (sleep+backoff), not exit.

It enriches each business toward Apollo/ZoomInfo-grade COMPANY coverage using
ONLY FREE methods -- no paid APIs, no vendor tokens:

  * website discovery        -- use an existing site if present, else a
                               conservative guess-from-name + DNS/HTTP verify
  * homepage / contact crawl -- emails, phones, social profile URLs, title/meta
  * MX / DNS                 -- mail infra + provider + email-pattern inference
  * tech-stack detection     -- HTTP headers + HTML signatures (WordPress,
                               Shopify, Wix, Squarespace, Next.js, Cloudflare,
                               GA/GTM/Meta Pixel, etc.)
  * firmographics            -- industry from category/license text; size cues
  * cross-reference          -- match already-loaded EDGAR / nonprofit / license
                               source_records by normalized name+region to
                               attach CIK/EIN/SIC and corroborate the entity

Enriched fields are written to atlas.business (fill-if-empty -- never clobber a
source-of-truth value) and EVERY observation is written to atlas.field_provenance
(field, value, source, method, confidence, url, observed_at).

HONEST COVERAGE CEILING (documented, not marketing):
  * Company-level fields (domain, company email like info@/contact@, main phone,
    socials, tech-stack, industry) are genuinely reachable to ~88-92% free on a
    population that actually has a web presence.
  * VERIFIED PERSONAL direct-dials and PERSONAL emails are vendor-gated. Free
    methods give pattern *guesses* (jane.doe@acme.com), not verified deliverable
    personal contacts. Blended Apollo-equivalence on personal contact data is
    ~65-72%, NOT 90%. This worker NEVER stores a guessed personal email as if it
    were verified: pattern inferences are written with source='email_pattern'
    and confidence<=0.4 and field='email_pattern' (a hint), never field='email'.

NON-OVERRIDABLE .gov / .mil SUPPRESSION:
  If the business's resolved domain is under .gov or .mil, contact data (emails,
  phones, socials) is NOT written to atlas.business and NOT stored as a usable
  value in field_provenance -- a single suppression marker row is written
  instead. No environment variable can turn this off.

STATUS-BACK (read progress from the repo without touching the box):
  Every ATLAS_ENRICH_REPORT_SEC (default 300s) and on exit, each worker writes
  /var/lib/atlas/autopull/last_counts.json locally AND (if STATUS_TOKEN +
  STATUS_REPO are set in /etc/atlas/autopull.env) PUTs
  status/<node>/enrich-<node>.json to the repo via the GitHub Contents API --
  the SAME mechanism + token the v3 puller and healthcheck already use. The file
  carries enriched/min, queue_remaining, and per-field coverage %.

MODES
  --migrate    verify atlas.enrich_queue + atlas.field_provenance exist with the
               expected real columns (additive-only; never CREATE/ALTER/DROP) and seed
               the queue idempotently (find_domain task per business),
               seed the queue from atlas.business, print + write counts, exit.
  --selftest   validate: db.env loads, Postgres connects, schema present/creatable,
               one dry claim cycle works (rolled back); exit 0 ok / 3 broken.
  --once       run a single claim+enrich batch then exit (used by --migrate proof).
  --loop       (DEFAULT) run forever: claim, enrich, report, idle when empty.

DB creds come from /etc/atlas/db.env exactly like socrata_import.py /
overture_pg_import.py (PG* / DB_* picked, optional leading `export`). On the
InterServer node the same script runs as a psycopg2 CLIENT to the Hetzner DB --
just point PGHOST/DB_HOST at Hetzner in that box's /etc/atlas/db.env. Nothing in
this script is host-specific.
"""

import os
import re
import sys
import json
import time
import base64
import random
import socket
import hashlib
import datetime
import urllib.parse
import urllib.request
import urllib.error

import psycopg2

# dnspython is optional: MX/DNS enrichment degrades to "skipped (no dns lib)" if
# it isn't installed, rather than failing the worker.
try:
    import dns.resolver  # type: ignore
    _HAVE_DNS = True
except Exception:  # noqa: BLE001
    _HAVE_DNS = False


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
DB_ENV_PATH       = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
AUTOPULL_ENV_PATH = os.environ.get("ATLAS_AUTOPULL_CONF", "/etc/atlas/autopull.env")

BUSINESS_TBL = ("atlas", "business")
QUEUE_TBL    = ("atlas", "enrich_queue")
PROV_TBL     = ("atlas", "field_provenance")
SOURCE_TBL   = ("atlas", "source_record")

BATCH        = int(os.environ.get("ATLAS_ENRICH_BATCH", "20"))
MAX_ATTEMPTS = int(os.environ.get("ATLAS_ENRICH_MAX_ATTEMPTS", "5"))
DEFAULT_TASK_TYPE = os.environ.get("ATLAS_ENRICH_TASK_TYPE", "find_domain")  # real CHECK value
CLAIM_STALE_SEC   = int(os.environ.get("ATLAS_ENRICH_CLAIM_STALE_SEC", "900"))  # reclaim crashed locks
IDLE_SEC     = float(os.environ.get("ATLAS_ENRICH_IDLE_SEC", "15"))
PACING_MS    = int(os.environ.get("ATLAS_ENRICH_PACING_MS", "250"))
SEED_SEC     = int(os.environ.get("ATLAS_ENRICH_SEED_SEC", "600"))
REPORT_SEC   = int(os.environ.get("ATLAS_ENRICH_REPORT_SEC", "300"))
HTTP_TIMEOUT = int(os.environ.get("ATLAS_ENRICH_HTTP_TIMEOUT", "12"))
HTTP_MAXBYTES= int(os.environ.get("ATLAS_ENRICH_HTTP_MAXBYTES", "700000"))
ENABLE_DISCOVERY = os.environ.get("ATLAS_ENRICH_DISCOVER", "1") not in ("0", "false", "no")
USER_AGENT   = ("atlas-enrich/1.0 (+https://github.com/cloudwalker171/atlas-manifests; "
                "free-tier enrichment; respects robots)")

COUNTS_PATH = os.environ.get(
    "ATLAS_COUNTS_PATH",
    os.path.join(os.environ.get("ATLAS_AUTOPULL_STATE", "/var/lib/atlas/autopull"),
                 "last_counts.json"))
ENRICH_STATE_DIR = os.environ.get("ATLAS_ENRICH_STATE_DIR", "/var/lib/atlas/enrich")

# free webmail / parked hosts that must NEVER be treated as a company domain
FREE_MAIL = {
    "gmail.com", "googlemail.com", "yahoo.com", "ymail.com", "hotmail.com",
    "outlook.com", "live.com", "msn.com", "aol.com", "icloud.com", "me.com",
    "mail.com", "gmx.com", "protonmail.com", "proton.me", "comcast.net",
    "att.net", "verizon.net", "sbcglobal.net", "bellsouth.net", "cox.net",
}
ROLE_LOCALPARTS = {
    "info", "contact", "sales", "hello", "support", "admin", "office",
    "help", "service", "enquiries", "inquiries", "team", "mail", "press",
    "billing", "careers", "jobs", "marketing", "general",
}


def log(msg):
    print(f"[atlas_enrich] {datetime.datetime.now(datetime.timezone.utc).isoformat()} {msg}",
          flush=True)


def env_bool(name, default=False):
    v = os.environ.get(name)
    if v is None:
        return default
    return v not in ("0", "false", "False", "no", "")


# --------------------------------------------------------------------------- #
# env + connection (mirrors socrata_import.py / atlas_healthcheck.py)
# --------------------------------------------------------------------------- #
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
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def pick(*names, default=None):
    for n in names:
        if os.environ.get(n):
            return os.environ[n]
    return default


def connect_pg():
    conn = psycopg2.connect(
        host=pick("PGHOST", "DB_HOST", "ATLAS_DB_HOST", default="localhost"),
        port=pick("PGPORT", "DB_PORT", "ATLAS_DB_PORT", default="5432"),
        dbname=pick("PGDATABASE", "DB_NAME", "ATLAS_DB_NAME", default="tuanichat_atlas"),
        user=pick("PGUSER", "DB_USER", "ATLAS_DB_USER", default="atlas"),
        password=pick("PGPASSWORD", "DB_PASSWORD", "ATLAS_DB_PASSWORD", default=None),
        connect_timeout=int(os.environ.get("ATLAS_DB_CONNECT_TIMEOUT", "10")),
        application_name="atlas_enrich_worker",
    )
    conn.autocommit = False
    return conn


# --------------------------------------------------------------------------- #
# Schema introspection (same strategy as socrata_import.py)
# --------------------------------------------------------------------------- #
def table_columns(cur, schema, table):
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema=%s AND table_name=%s", (schema, table))
    return {r[0] for r in cur.fetchall()}


def table_pk(cur, schema, table):
    cur.execute(
        "SELECT a.attname FROM pg_index i "
        "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
        "WHERE i.indrelid=%s::regclass AND i.indisprimary", (f"{schema}.{table}",))
    rows = [r[0] for r in cur.fetchall()]
    return rows[0] if len(rows) == 1 else (rows[0] if rows else None)


def pick_col(colset, candidates):
    for c in candidates:
        if c in colset:
            return c
    return None


CANDIDATES = {
    "name":     ["name", "business_name", "title", "display_name", "legal_name"],
    "website":  ["website", "url", "website_url", "homepage"],
    "phone":    ["phone_e164", "phone", "phone_number", "telephone", "contact_phone"],
    "email":    ["email", "email_address", "contact_email"],
    "address":  ["address", "address_freeform", "street_address", "freeform"],
    "locality": ["locality", "city", "town"],
    "region":   ["region", "state", "province"],
    "postcode": ["postcode", "postal_code", "zip", "zipcode", "zip_code"],
    "country":  ["country", "country_code"],
    "category": ["category", "primary_category", "categories", "license_description"],
    "industry": ["industry", "sector"],  # naics/sic are CODES, not text; do not overwrite
    "lat":      ["latitude", "lat", "y"],
    "lon":      ["longitude", "lon", "lng", "x"],
    "updated":  ["updated_at", "modified_at", "updated"],
    # source_record
    "sr_source": ["source", "data_source", "origin"],
    "sr_payload": ["raw", "payload", "data", "raw_json", "raw_jsonb", "doc", "record"],
    "sr_name":   ["name", "business_name", "title"],
}


# --------------------------------------------------------------------------- #
# DDL -- our own auxiliary tables, all IF NOT EXISTS (idempotent, safe on both nodes)
# --------------------------------------------------------------------------- #
def ensure_schema(conn):
    """REAL production schema is already present (verified via seq-7 diagnostic):
       atlas.enrich_queue(id bigint pk, business_id bigint NOT NULL fk->business(id),
         task_type text NOT NULL CHECK in (find_domain,find_email,validate_email,
         firmographics,ai_classify), priority smallint NOT NULL default 5,
         status text NOT NULL default 'pending' CHECK in (pending,claimed,done,failed,dead),
         attempts smallint NOT NULL default 0, locked_by text, locked_at timestamptz,
         result jsonb, created_at/updated_at timestamptz NOT NULL default now(),
         UNIQUE(business_id, task_type));
       atlas.field_provenance(business_id bigint NOT NULL fk->business(id), field text NOT NULL,
         value text, source_code text NOT NULL, confidence real NOT NULL default 0.5,
         last_verified timestamptz NOT NULL default now(), PRIMARY KEY(business_id, field)).
    This worker is ADDITIVE-ONLY: it never CREATEs/DROPs/ALTERs these tables. It only
    VERIFIES they exist with the expected key columns and FAILS LOUD if they don't, so a
    drifted box can't silently run against the wrong shape."""
    cur = conn.cursor()
    required = {
        ("atlas", "enrich_queue"): {"id", "business_id", "task_type", "status",
                                    "priority", "attempts", "locked_by", "locked_at",
                                    "result", "created_at", "updated_at"},
        ("atlas", "field_provenance"): {"business_id", "field", "value",
                                        "source_code", "confidence", "last_verified"},
    }
    for (sch, tbl), need in required.items():
        have = table_columns(cur, sch, tbl)
        if not have:
            cur.close()
            raise SystemExit(
                f"SCHEMA ERROR: {sch}.{tbl} not found. This worker is additive-only and "
                f"will NOT create it -- run the authoritative migration first.")
        missing = need - have
        if missing:
            cur.close()
            raise SystemExit(
                f"SCHEMA DRIFT: {sch}.{tbl} missing columns {sorted(missing)}; "
                f"has {sorted(have)}. Refusing to run against an unexpected shape.")
    cur.close()


def seed_queue(conn, biz_schema, biz_table, biz_pk):
    """Enqueue a find_domain task for every business not already queued. Idempotent;
    respects the real UNIQUE(business_id, task_type) + CHECK(task_type) constraints.
    NEVER inserts an unsupported task_type; business_id is the bigint business PK."""
    cur = conn.cursor()
    cur.execute(f'''
        INSERT INTO atlas.enrich_queue (business_id, task_type, priority, status)
        SELECT b."{biz_pk}", %s, 5, \'pending\'
        FROM "{biz_schema}"."{biz_table}" b
        ON CONFLICT (business_id, task_type) DO NOTHING
    ''', (DEFAULT_TASK_TYPE,))
    n = cur.rowcount or 0
    conn.commit()
    cur.close()
    return n


# --------------------------------------------------------------------------- #
# Queue claim -- the SKIP LOCKED core (real status/locked_by/locked_at model)
# --------------------------------------------------------------------------- #
def claim_batch(conn, worker_id, batch):
    """Atomically claim up to <batch> rows. Claimable = status='pending', OR a
    'claimed' row whose lock is stale (locked_at older than CLAIM_STALE_SEC) and which
    still has attempts left -- so a crashed worker's rows get retried. Ordered by
    (priority, id) to match the partial index enrich_queue_claim_idx. Uses
    FOR UPDATE SKIP LOCKED so N workers never collide. NO LISTEN/NOTIFY."""
    cur = conn.cursor()
    cur.execute('''
        WITH c AS (
            SELECT id FROM atlas.enrich_queue
            WHERE attempts < %s
              AND (status = \'pending\'
                   OR (status = \'claimed\'
                       AND locked_at IS NOT NULL
                       AND locked_at < now() - make_interval(secs => %s)))
            ORDER BY priority, id
            FOR UPDATE SKIP LOCKED
            LIMIT %s
        )
        UPDATE atlas.enrich_queue q
           SET status=\'claimed\', locked_by=%s, locked_at=now(),
               attempts=attempts+1, updated_at=now()
          FROM c WHERE q.id=c.id
        RETURNING q.id, q.business_id, q.task_type
    ''', (MAX_ATTEMPTS, CLAIM_STALE_SEC, batch, worker_id))
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    return rows  # [(queue_id, business_id, task_type), ...]


def finish_row(conn, queue_id, status, err=None, result_extra=None):
    """Transition a claimed row to a terminal status. The real table has NO
    enriched_at/last_error columns, so the error (and any structured outcome) is
    recorded in result jsonb; the lock is released. status must be one of the CHECK
    values; 'failed' rows that have exhausted MAX_ATTEMPTS are escalated to 'dead' so
    they stop being reclaimed. Fail-LOUD: the real error string is preserved in result,
    never nulled."""
    cur = conn.cursor()
    payload = {}
    if err:
        payload["error"] = str(err)[:1500]
    if result_extra:
        try:
            payload["outcome"] = result_extra
        except Exception:  # noqa: BLE001
            pass
    payload["finished_at"] = int(time.time())
    payload["worker_status"] = status
    cur.execute('''
        UPDATE atlas.enrich_queue
           SET status = CASE
                          WHEN %s = \'failed\' AND attempts >= %s THEN \'dead\'
                          ELSE %s
                        END,
               result = COALESCE(result, \'{}\'::jsonb) || %s::jsonb,
               locked_by = NULL,
               locked_at = NULL,
               updated_at = now()
         WHERE id = %s
    ''', (status, MAX_ATTEMPTS, status, json.dumps(payload), queue_id))
    conn.commit()
    cur.close()


# --------------------------------------------------------------------------- #
# Normalization helpers
# --------------------------------------------------------------------------- #
_LEGAL_SUFFIX = re.compile(
    r"\b(inc|incorporated|llc|l\.l\.c|llp|ltd|limited|co|corp|corporation|"
    r"company|plc|pllc|pc|lp|gmbh|sa|nv|bv|pty)\b\.?", re.I)
_NONALNUM = re.compile(r"[^a-z0-9]+")


def norm_name(name):
    if not name:
        return ""
    s = name.lower()
    s = _LEGAL_SUFFIX.sub(" ", s)
    s = _NONALNUM.sub(" ", s)
    return " ".join(s.split())


def name_tokens(name):
    return [t for t in norm_name(name).split() if len(t) > 1]


_PHONE_RE = re.compile(r"(?:\+?1[\s.\-]?)?\(?([2-9]\d{2})\)?[\s.\-]?(\d{3})[\s.\-]?(\d{4})")


def norm_phone(raw):
    """Return a canonical 10-digit NANP string, or None. Strips +1/formatting."""
    if not raw:
        return None
    m = _PHONE_RE.search(str(raw))
    if not m:
        digits = re.sub(r"\D", "", str(raw))
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        return digits if len(digits) == 10 else None
    return "".join(m.groups())


_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")


def registrable_domain(host_or_url):
    """Best-effort registrable domain from a URL or host. Stdlib only.

    Handles the common multi-label public suffixes we actually see in US
    business data (.co.uk, .com.au, .gov.uk ...) without bundling the full PSL.
    """
    if not host_or_url:
        return None
    h = host_or_url.strip().lower()
    if "://" in h:
        h = urllib.parse.urlsplit(h).netloc
    h = h.split("@")[-1].split(":")[0].strip().strip(".")
    if not h or "." not in h:
        return None
    parts = h.split(".")
    two_label_tlds = {"co.uk", "org.uk", "gov.uk", "ac.uk", "com.au", "net.au",
                      "org.au", "co.nz", "co.za", "com.br"}
    if len(parts) >= 3 and ".".join(parts[-2:]) in two_label_tlds:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def is_gov_mil(domain):
    if not domain:
        return False
    d = domain.lower()
    # .gov / .mil and their .gov.* style variants (e.g. ny.gov, army.mil)
    return (d.endswith(".gov") or d.endswith(".mil")
            or ".gov." in ("." + d + ".") or ".mil." in ("." + d + "."))


# --------------------------------------------------------------------------- #
# HTTP (stdlib urllib; redirect-following; size-capped; best-effort robots)
# --------------------------------------------------------------------------- #
def http_get(url, timeout=HTTP_TIMEOUT, maxbytes=HTTP_MAXBYTES):
    """Return (final_url, status, headers_dict, text) or None on failure."""
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.8",
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read(maxbytes + 1)
            if len(raw) > maxbytes:
                raw = raw[:maxbytes]
            headers = {k.lower(): v for k, v in resp.headers.items()}
            enc = "utf-8"
            ctype = headers.get("content-type", "")
            m = re.search(r"charset=([\w\-]+)", ctype, re.I)
            if m:
                enc = m.group(1)
            try:
                text = raw.decode(enc, "replace")
            except (LookupError, UnicodeDecodeError):
                text = raw.decode("utf-8", "replace")
            return (resp.geturl(), resp.getcode(), headers, text)
    except urllib.error.HTTPError as e:
        return (url, e.code, {k.lower(): v for k, v in (e.headers or {}).items()}, "")
    except (urllib.error.URLError, socket.timeout, ConnectionError, ValueError):
        return None
    except Exception:  # noqa: BLE001 -- never let a weird TLS/encoding bug kill the worker
        return None


_robots_cache = {}


def robots_allows(domain, path):
    """Minimal robots.txt check for User-agent: * Disallow lines. Best-effort:
    if robots can't be fetched/parsed we ALLOW (fail-open, standard for crawlers)."""
    if domain not in _robots_cache:
        rules = []
        res = http_get(f"https://{domain}/robots.txt", timeout=8, maxbytes=60000)
        body = res[3] if res and res[1] == 200 else ""
        if body:
            applies = False
            for line in body.splitlines():
                line = line.split("#", 1)[0].strip()
                if not line:
                    continue
                low = line.lower()
                if low.startswith("user-agent:"):
                    agent = low.split(":", 1)[1].strip()
                    applies = (agent == "*")
                elif applies and low.startswith("disallow:"):
                    rule = line.split(":", 1)[1].strip()
                    if rule:
                        rules.append(rule)
        _robots_cache[domain] = rules
    for rule in _robots_cache[domain]:
        if path.startswith(rule):
            return False
    return True


# --------------------------------------------------------------------------- #
# Page parsing
# --------------------------------------------------------------------------- #
_SOCIAL_RE = {
    "facebook":  re.compile(r"https?://(?:www\.)?facebook\.com/[A-Za-z0-9_.\-/]+"),
    "linkedin":  re.compile(r"https?://(?:www\.)?linkedin\.com/(?:company|in)/[A-Za-z0-9_.\-/%]+"),
    "twitter":   re.compile(r"https?://(?:www\.)?(?:twitter|x)\.com/[A-Za-z0-9_]+"),
    "instagram": re.compile(r"https?://(?:www\.)?instagram\.com/[A-Za-z0-9_.\-/]+"),
    "youtube":   re.compile(r"https?://(?:www\.)?youtube\.com/[A-Za-z0-9_@.\-/]+"),
    "tiktok":    re.compile(r"https?://(?:www\.)?tiktok\.com/@[A-Za-z0-9_.\-]+"),
}
_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)
_META_DESC_RE = re.compile(
    r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']', re.I | re.S)


def parse_page(final_url, headers, html):
    out = {"emails": set(), "phones": set(), "socials": {}, "title": None,
           "meta_desc": None, "tech": set()}
    if not html:
        return out
    page_domain = registrable_domain(final_url)
    for e in _EMAIL_RE.findall(html):
        e = e.lower().strip(".")
        # drop obvious asset filenames mistaken as emails
        if any(e.endswith(ext) for ext in (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")):
            continue
        out["emails"].add(e)
    for p in _PHONE_RE.findall(html):
        out["phones"].add("".join(p))
    for net, rx in _SOCIAL_RE.items():
        m = rx.search(html)
        if m:
            out["socials"][net] = m.group(0)
    mt = _TITLE_RE.search(html)
    if mt:
        out["title"] = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", mt.group(1))).strip()[:300]
    md = _META_DESC_RE.search(html)
    if md:
        out["meta_desc"] = re.sub(r"\s+", " ", md.group(1)).strip()[:500]
    out["tech"] = detect_tech(headers, html)
    out["page_domain"] = page_domain
    return out


_TECH_HTML_SIGNS = [
    ("WordPress",   re.compile(r"wp-content|wp-includes|/wp-json", re.I)),
    ("Shopify",     re.compile(r"cdn\.shopify\.com|shopify\.theme|x-shopify", re.I)),
    ("Wix",         re.compile(r"static\.wixstatic\.com|wix\.com", re.I)),
    ("Squarespace", re.compile(r"squarespace\.com|static1\.squarespace", re.I)),
    ("Webflow",     re.compile(r"assets\.website-files\.com|webflow", re.I)),
    ("Next.js",     re.compile(r"/_next/|__NEXT_DATA__", re.I)),
    ("React",       re.compile(r"data-reactroot|react-dom", re.I)),
    ("Drupal",      re.compile(r"sites/all/|drupal\.js|/sites/default/files", re.I)),
    ("Joomla",      re.compile(r"/media/jui/|joomla", re.I)),
    ("HubSpot",     re.compile(r"js\.hs-scripts\.com|hsforms", re.I)),
    ("Mailchimp",   re.compile(r"list-manage\.com|mailchimp", re.I)),
    ("GoogleAnalytics", re.compile(r"google-analytics\.com|gtag\(|googletagmanager", re.I)),
    ("MetaPixel",   re.compile(r"connect\.facebook\.net/.+/fbevents\.js|fbq\(", re.I)),
    ("Cloudflare",  re.compile(r"cloudflare|cf-ray", re.I)),
    ("Stripe",      re.compile(r"js\.stripe\.com", re.I)),
]


def detect_tech(headers, html):
    tech = set()
    server = (headers.get("server") or "")
    powered = (headers.get("x-powered-by") or "")
    cookies = (headers.get("set-cookie") or "")
    blob = " ".join([server, powered, cookies]).lower()
    if "cloudflare" in blob or headers.get("cf-ray"):
        tech.add("Cloudflare")
    if "nginx" in blob:
        tech.add("nginx")
    if "apache" in blob:
        tech.add("Apache")
    if "php" in blob:
        tech.add("PHP")
    if "asp.net" in blob:
        tech.add("ASP.NET")
    for name, rx in _TECH_HTML_SIGNS:
        if rx.search(html or ""):
            tech.add(name)
    return tech


# --------------------------------------------------------------------------- #
# Enrichment: per-field probes
# --------------------------------------------------------------------------- #
def discover_domain(name, existing_website):
    """Resolve a usable registrable domain. Prefer existing website; else a
    conservative guess from the name verified by DNS+HTTP. Returns
    (domain, source, confidence) or (None, reason, 0.0). Honest: guess-from-name
    is the weakest free signal -- only accepted when DNS resolves AND the homepage
    title plausibly contains the name."""
    if existing_website:
        d = registrable_domain(existing_website)
        if d and d not in FREE_MAIL:
            return (d, "existing_website", 0.95)
    if not ENABLE_DISCOVERY:
        return (None, "discovery_disabled", 0.0)
    toks = name_tokens(name)
    if not toks or len(" ".join(toks)) < 3:
        return (None, "name_too_weak_to_guess", 0.0)
    joined = "".join(toks)
    cand_stems = {joined}
    if len(toks) >= 2:
        cand_stems.add("".join(toks[:2]))
        cand_stems.add("-".join(toks))
    cands = []
    for stem in list(cand_stems):
        if 2 < len(stem) <= 40:
            cands.extend([f"{stem}.com", f"{stem}.net", f"{stem}.org"])
    name_set = set(toks)
    for dom in cands[:6]:
        if not _dns_resolves(dom):
            continue
        res = http_get(f"https://{dom}/", timeout=8)
        if not res or res[1] >= 400:
            res = http_get(f"http://{dom}/", timeout=8)
        if not res or res[1] >= 400 or not res[3]:
            continue
        info = parse_page(res[0], res[2], res[3])
        title_toks = set(name_tokens(info.get("title") or ""))
        overlap = len(name_set & title_toks)
        if overlap >= max(1, len(name_set) // 2):
            return (registrable_domain(res[0]) or dom, "guessed_verified", 0.6)
    return (None, "no_verified_guess", 0.0)


def _dns_resolves(domain):
    if _HAVE_DNS:
        try:
            dns.resolver.resolve(domain, "A", lifetime=6)
            return True
        except Exception:  # noqa: BLE001
            try:
                dns.resolver.resolve(domain, "AAAA", lifetime=6)
                return True
            except Exception:  # noqa: BLE001
                return False
    try:
        socket.getaddrinfo(domain, 443, proto=socket.IPPROTO_TCP)
        return True
    except OSError:
        return False


def mx_info(domain):
    """Return (has_mx, provider, [mx_hosts]). provider inferred from MX host."""
    if not _HAVE_DNS:
        return (None, None, [])
    try:
        answers = dns.resolver.resolve(domain, "MX", lifetime=8)
    except Exception:  # noqa: BLE001
        return (False, None, [])
    hosts = sorted(str(r.exchange).rstrip(".").lower() for r in answers)
    provider = None
    blob = " ".join(hosts)
    if "google" in blob or "googlemail" in blob:
        provider = "Google Workspace"
    elif "outlook" in blob or "protection.outlook" in blob:
        provider = "Microsoft 365"
    elif "pphosted" in blob or "proofpoint" in blob:
        provider = "Proofpoint"
    elif "mimecast" in blob:
        provider = "Mimecast"
    elif "zoho" in blob:
        provider = "Zoho"
    elif "secureserver" in blob:
        provider = "GoDaddy"
    return (bool(hosts), provider, hosts)


def infer_email_pattern(emails, domain):
    """From company-domain emails, infer the local-part pattern. Returns
    (pattern_str, confidence) or (None, 0). NEVER returns a personal address --
    only the abstract pattern (a HINT, not a verified contact)."""
    locals_ = [e.split("@", 1)[0] for e in emails
               if e.endswith("@" + domain) and e.split("@", 1)[0] not in ROLE_LOCALPARTS]
    for lp in locals_:
        if re.fullmatch(r"[a-z]+\.[a-z]+", lp):
            return ("{first}.{last}@" + domain, 0.4)
        if re.fullmatch(r"[a-z]\.[a-z]+", lp):
            return ("{f}.{last}@" + domain, 0.35)
        if re.fullmatch(r"[a-z][a-z]+", lp) and len(lp) <= 12:
            return ("{flast}@" + domain, 0.3)
    return (None, 0.0)


INDUSTRY_MAP = [
    ("restaurant|food|cafe|catering|bakery|bar |grill|pizza", "Food & Beverage"),
    ("salon|barber|spa|beauty|nail", "Personal Care"),
    ("law|legal|attorney|counsel", "Legal Services"),
    ("dental|dentist|medical|clinic|health|physician|pharmacy", "Healthcare"),
    ("construction|contractor|plumbing|electric|hvac|roofing", "Construction & Trades"),
    ("real estate|realty|property|broker", "Real Estate"),
    ("auto|automotive|repair|garage|motor", "Automotive"),
    ("retail|store|shop|boutique|grocery|market", "Retail"),
    ("consult|software|technology|it services|saas|web", "Professional & Tech Services"),
    ("financial|insurance|bank|accounting|tax|cpa", "Finance & Insurance"),
    ("cleaning|landscap|janitor|maintenance", "Facilities & Cleaning"),
    ("transport|trucking|logistics|moving|freight", "Transportation & Logistics"),
    ("education|school|tutoring|academy|training", "Education"),
    ("manufactur|fabricat|industrial|machine", "Manufacturing"),
    ("nonprofit|charity|foundation|church|ministry", "Nonprofit & Religious"),
]


def infer_industry(category, title, meta):
    blob = " ".join(x for x in (category, title, meta) if x).lower()
    if not blob:
        return (None, 0.0)
    for pat, label in INDUSTRY_MAP:
        if re.search(pat, blob):
            conf = 0.7 if category and re.search(pat, category.lower()) else 0.45
            return (label, conf)
    return (None, 0.0)


def size_cues(html):
    if not html:
        return (None, 0.0)
    low = html.lower()
    m = re.search(r"(\d{2,6})\+?\s+(?:employees|team members|staff)", low)
    if m:
        return (f"~{m.group(1)} employees (site-stated)", 0.5)
    if re.search(r"\b(careers|join our team|we'?re hiring|open positions)\b", low):
        return ("hiring-active (careers page present)", 0.25)
    return (None, 0.0)


# --------------------------------------------------------------------------- #
# Cross-reference already-loaded authoritative sources
# --------------------------------------------------------------------------- #
def xref_sources(cur, sr_cols, name, region):
    """Look for an EDGAR / nonprofit / license source_record whose name matches
    this business's normalized name (+ region when both present). Returns a list
    of (source, matched_name, confidence) provenance hints. Read-only."""
    sr_src = pick_col(sr_cols, CANDIDATES["sr_source"])
    sr_name = pick_col(sr_cols, CANDIDATES["sr_name"])
    sr_pay = pick_col(sr_cols, CANDIDATES["sr_payload"])
    if not sr_src or not (sr_name or sr_pay):
        return []
    nn = norm_name(name)
    if len(nn) < 4:
        return []
    hits = []
    name_expr = f'b."{sr_name}"' if sr_name else f'(b."{sr_pay}")::text'
    try:
        cur.execute(f"""
            SELECT b."{sr_src}" AS src, {name_expr} AS nm
            FROM "{SOURCE_TBL[0]}"."{SOURCE_TBL[1]}" b
            WHERE b."{sr_src}" ~* '(edgar|nonprofit|irs|990|sec|license|licens)'
              AND {name_expr} ILIKE %s
            LIMIT 5
        """, (f"%{name.strip()[:40]}%",))
        for src, nm in cur.fetchall():
            if nm and norm_name(str(nm)) == nn:
                hits.append((src, str(nm)[:200], 0.8))
            elif nm and nn in norm_name(str(nm)):
                hits.append((src, str(nm)[:200], 0.5))
    except psycopg2.Error:
        cur.connection.rollback()
        return []
    return hits


# --------------------------------------------------------------------------- #
# Provenance write + business fill-if-empty
# --------------------------------------------------------------------------- #
def record(cur, ref, field, value, source, method=None, confidence=0.5, url=None):
    """Write one observation to atlas.field_provenance against the REAL schema:
    PRIMARY KEY (business_id, field), columns
    (business_id bigint, field text, value text, source_code text NOT NULL,
     confidence real NOT NULL, last_verified timestamptz NOT NULL).
    There are NO method/url columns -- the legacy `method`/`url` args are accepted for
    caller compatibility and folded into source_code so the trail is still legible
    (e.g. source_code='homepage_crawl/html_signature'). Because the PK is one row per
    (business_id, field), this UPSERTs and keeps the HIGHER-confidence observation,
    refreshing last_verified. ref is the bigint business_id."""
    if value is None or value == "":
        return
    src_code = source if not method else f"{source}/{method}"
    try:
        cur.execute("""
            INSERT INTO atlas.field_provenance
                (business_id, field, value, source_code, confidence, last_verified)
            VALUES (%s,%s,%s,%s,%s, now())
            ON CONFLICT (business_id, field) DO UPDATE
               SET value         = EXCLUDED.value,
                   source_code   = EXCLUDED.source_code,
                   confidence    = EXCLUDED.confidence,
                   last_verified = now()
             WHERE EXCLUDED.confidence >= atlas.field_provenance.confidence
        """, (int(ref), field, str(value)[:2000], str(src_code)[:200],
              float(confidence)))
    except (psycopg2.Error, ValueError, TypeError):
        cur.connection.rollback()


def fill_if_empty(cur, biz_schema, biz_table, biz_pk, ref, col, value):
    """Set atlas.business.<col> only when currently NULL/empty. Returns True if updated."""
    if not col or value is None or value == "":
        return False
    try:
        cur.execute(f"""
            UPDATE "{biz_schema}"."{biz_table}"
               SET "{col}"=%s
             WHERE "{biz_pk}"=%s
               AND (("{col}") IS NULL OR btrim(("{col}")::text)='')
        """, (value, int(ref)))
        return (cur.rowcount or 0) > 0
    except psycopg2.Error:
        cur.connection.rollback()
        return False


# --------------------------------------------------------------------------- #
# Enrich one business
# --------------------------------------------------------------------------- #
def enrich_one(conn, cols, ref):
    """Returns dict of what happened (for counting). Fail-soft per probe."""
    biz_schema, biz_table = BUSINESS_TBL
    biz_pk = cols["pk"]
    cur = conn.cursor()
    # fetch the business row's useful fields
    sel_cols = [c for c in (cols.get("name"), cols.get("website"), cols.get("phone"),
                            cols.get("email"), cols.get("category"), cols.get("region"),
                            cols.get("locality")) if c]
    cur.execute(
        f'SELECT {", ".join(f"""b."{c}" """ for c in sel_cols)} '
        f'FROM "{biz_schema}"."{biz_table}" b WHERE b."{biz_pk}"=%s', (int(ref),))
    row = cur.fetchone()
    if not row:
        cur.close()
        return {"missing": True}
    vals = dict(zip(sel_cols, row))
    name = vals.get(cols.get("name")) or ""
    website = vals.get(cols.get("website")) or ""
    category = vals.get(cols.get("category")) or ""
    region = vals.get(cols.get("region")) or ""

    outcome = {"fields_filled": 0, "prov_rows": 0, "suppressed": False, "domain": None}

    # 1. domain
    domain, dsrc, dconf = discover_domain(name, website)
    outcome["domain"] = domain
    gov = is_gov_mil(domain) if domain else False
    outcome["suppressed"] = gov
    if domain:
        record(cur, ref, "domain", domain, dsrc, "domain_resolve", dconf)
        if dsrc != "existing_website" and cols.get("website"):
            if fill_if_empty(cur, biz_schema, biz_table, biz_pk, ref,
                             cols["website"], f"https://{domain}"):
                outcome["fields_filled"] += 1

    page = None
    if domain:
        # crawl homepage + a couple of contact-ish paths (robots-respecting)
        for path in ("/", "/contact", "/contact-us", "/about"):
            if not robots_allows(domain, path):
                continue
            res = http_get(f"https://{domain}{path}", timeout=HTTP_TIMEOUT)
            if not res or res[1] >= 400:
                if path == "/":
                    res = http_get(f"http://{domain}{path}", timeout=HTTP_TIMEOUT)
                if not res or res[1] >= 400:
                    continue
            info = parse_page(res[0], res[2], res[3])
            if page is None:
                page = info
            else:
                page["emails"] |= info["emails"]
                page["phones"] |= info["phones"]
                page["tech"] |= info["tech"]
                for k, v in info["socials"].items():
                    page["socials"].setdefault(k, v)
            if path == "/":
                page["title"] = info.get("title")
                page["meta_desc"] = info.get("meta_desc")

    # 2. tech-stack (company-level, never suppressed)
    if page and page["tech"]:
        # PK is one row per (business_id, field) -> store the detected stack as a
        # single comma-joined value rather than colliding rows.
        record(cur, ref, "tech", ",".join(sorted(page["tech"])[:12]),
               "homepage_crawl", "html_signature", 0.7, f"https://{domain}/")
        outcome["prov_rows"] += 1

    # 3. MX / DNS  (company-level)
    if domain and not gov:
        has_mx, provider, hosts = mx_info(domain)
        if has_mx:
            record(cur, ref, "mx", ",".join(hosts[:4]), "dns_mx", "dns", 0.9)
            outcome["prov_rows"] += 1
            if provider:
                record(cur, ref, "email_provider", provider, "dns_mx", "mx_host", 0.8)
                outcome["prov_rows"] += 1

    # 4. contact data -- SUPPRESSED for .gov/.mil (non-overridable)
    if gov:
        record(cur, ref, "contact_suppressed", "gov_mil", "policy",
               "non_overridable_suppression", 1.0)
        outcome["prov_rows"] += 1
    elif page:
        # company emails (role addresses on the company domain) are OK to surface
        company_emails = sorted(e for e in page["emails"]
                                if domain and e.endswith("@" + domain))
        role_emails = [e for e in company_emails if e.split("@")[0] in ROLE_LOCALPARTS]
        if role_emails:
            record(cur, ref, "email", ",".join(role_emails[:3]),
                   "homepage_crawl", "role_email", 0.75, f"https://{domain}/")
            outcome["prov_rows"] += 1
        if role_emails and cols.get("email"):
            if fill_if_empty(cur, biz_schema, biz_table, biz_pk, ref,
                             cols["email"], role_emails[0]):
                outcome["fields_filled"] += 1
        # personal-looking emails -> NOT verified; pattern hint only (low confidence)
        pat, pconf = infer_email_pattern(page["emails"], domain) if domain else (None, 0)
        if pat:
            record(cur, ref, "email_pattern", pat, "email_pattern", "inferred", pconf)
            outcome["prov_rows"] += 1
        # phones
        phones = sorted({norm_phone(p) for p in page["phones"]} - {None})
        if phones:
            record(cur, ref, "phone", ",".join(phones[:3]),
                   "homepage_crawl", "regex_nanp", 0.7, f"https://{domain}/")
            outcome["prov_rows"] += 1
        if phones and cols.get("phone"):
            if fill_if_empty(cur, biz_schema, biz_table, biz_pk, ref,
                             cols["phone"], phones[0]):
                outcome["fields_filled"] += 1
        # socials
        for net, surl in page["socials"].items():
            record(cur, ref, f"social_{net}", surl, "homepage_crawl", "link", 0.8,
                   f"https://{domain}/")
            outcome["prov_rows"] += 1

    # 5. firmographics
    title = page.get("title") if page else None
    meta = page.get("meta_desc") if page else None
    ind, iconf = infer_industry(category, title, meta)
    if ind:
        record(cur, ref, "industry", ind, "classifier", "keyword", iconf)
        outcome["prov_rows"] += 1
        if cols.get("industry"):
            if fill_if_empty(cur, biz_schema, biz_table, biz_pk, ref, cols["industry"], ind):
                outcome["fields_filled"] += 1
    # size cue uses title/meta heuristics (we don't retain raw html in memory)
    sz, sconf = size_cues(" ".join(x for x in (title, meta) if x))
    if sz:
        record(cur, ref, "size_cue", sz, "homepage_crawl", "heuristic", sconf)
        outcome["prov_rows"] += 1

    # 6. cross-reference authoritative sources
    try:
        xhits = xref_sources(cur, cols["sr_cols"], name, region)
        if xhits:
            xval = ";".join(f"{src}:{matched}" for src, matched, _ in xhits[:5])
            xconf = max((c for _, _, c in xhits), default=0.5)
            record(cur, ref, "xref", xval, "source_xref", "name_match", xconf)
            outcome["prov_rows"] += 1
    except Exception as e:  # noqa: BLE001
        conn.rollback()
        log(f"  xref error ref={ref}: {e}")

    conn.commit()
    cur.close()
    return outcome


# --------------------------------------------------------------------------- #
# Coverage + status reporting
# --------------------------------------------------------------------------- #
def compute_coverage(conn, cols):
    biz_schema, biz_table = BUSINESS_TBL
    cur = conn.cursor()
    cur.execute(f'SELECT count(*) FROM "{biz_schema}"."{biz_table}"')
    total = cur.fetchone()[0] or 0
    cov = {"business_total": total}

    def pct(col):
        if not col or total == 0:
            return None
        cur.execute(f'SELECT count(*) FROM "{biz_schema}"."{biz_table}" '
                    f'WHERE "{col}" IS NOT NULL AND btrim(("{col}")::text)<>\'\'')
        return round(100.0 * (cur.fetchone()[0] or 0) / total, 1)

    cov["website_pct"] = pct(cols.get("website"))
    cov["phone_pct"] = pct(cols.get("phone"))
    cov["email_pct"] = pct(cols.get("email"))
    cov["industry_pct"] = pct(cols.get("industry"))

    # provenance-derived (covers fields that may not have a business column)
    def prov_pct(field):
        if total == 0:
            return None
        cur.execute("SELECT count(DISTINCT business_id) FROM atlas.field_provenance "
                    "WHERE field=%s", (field,))
        return round(100.0 * (cur.fetchone()[0] or 0) / total, 1)

    cov["domain_pct"] = prov_pct("domain")
    cov["tech_pct"] = prov_pct("tech")
    cov["social_any_pct"] = None
    cur.execute("SELECT count(DISTINCT business_id) FROM atlas.field_provenance "
                "WHERE field LIKE 'social_%'")
    if total:
        cov["social_any_pct"] = round(100.0 * (cur.fetchone()[0] or 0) / total, 1)

    # queue status (real CHECK vocabulary: pending/claimed/done/failed/dead)
    cur.execute("SELECT status, count(*) FROM atlas.enrich_queue GROUP BY status")
    qs = {st: c for st, c in cur.fetchall()}
    cov["queue"] = qs
    # remaining work = anything not yet terminal that can still be claimed
    cov["queue_remaining"] = (qs.get("pending", 0) + qs.get("claimed", 0)
                              + qs.get("failed", 0))
    cur.close()
    return cov


def gh_put(path, body_obj, msg):
    token = os.environ.get("STATUS_TOKEN")
    repo = os.environ.get("STATUS_REPO")
    if not token or not repo:
        return False
    api = os.environ.get("STATUS_API_BASE", "https://api.github.com")
    branch = os.environ.get("STATUS_BRANCH", "main")
    content_b64 = base64.b64encode(json.dumps(body_obj).encode("utf-8")).decode("ascii")

    def _req(method, url, data=None):
        req = urllib.request.Request(url, method=method, data=data)
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Accept", "application/vnd.github+json")
        req.add_header("User-Agent", "atlas-enrich")
        req.add_header("X-GitHub-Api-Version", "2022-11-28")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                return resp.getcode(), resp.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode("utf-8", "replace")
        except urllib.error.URLError as e:
            return None, str(e.reason)

    code, resp = _req("GET", f"{api}/repos/{repo}/contents/{path}?ref={branch}")
    sha = None
    if code == 200:
        try:
            sha = json.loads(resp).get("sha")
        except ValueError:
            sha = None
    payload = {"message": msg, "content": content_b64, "branch": branch}
    if sha:
        payload["sha"] = sha
    code, resp = _req("PUT", f"{api}/repos/{repo}/contents/{path}",
                      json.dumps(payload).encode("utf-8"))
    if code and 200 <= code < 300:
        log(f"  status pushed -> {path} ({code})")
        return True
    log(f"  status push FAILED {path} http={code} resp={resp[:200]}")
    return False


def write_local(obj, path):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(obj, fh)
    except OSError as e:
        log(f"  WARNING could not write {path}: {e}")


def report(conn, cols, worker_id, node, rate_per_min, lifetime):
    cov = compute_coverage(conn, cols)
    body = {
        "lane": "enrich",
        "node": node,
        "worker": worker_id,
        "enriched_per_min": round(rate_per_min, 2),
        "enriched_lifetime": lifetime,
        "coverage": cov,
        "coverage_ceiling_note": (
            "company-level fields reachable ~88-92% free; verified personal "
            "direct-dials/personal emails are vendor-gated (~65-72% blended) and "
            "are NOT claimed here -- personal emails are pattern HINTS only "
            "(field='email_pattern', confidence<=0.4)."),
        "ts": int(time.time()),
    }
    write_local(body, os.path.join(ENRICH_STATE_DIR, "last_enrich.json"))
    write_local(body, COUNTS_PATH)  # so an apply-time report() also surfaces it
    gh_put(f"status/{node}/enrich-{node}.json", body,
           f"enrich progress {node} {cov.get('queue_remaining')} remaining")
    qr = cov.get("queue_remaining")
    log(f"REPORT node={node} rate={rate_per_min:.2f}/min lifetime={lifetime} "
        f"queue_remaining={qr} domain%={cov.get('domain_pct')} "
        f"phone%={cov.get('phone_pct')} email%={cov.get('email_pct')} "
        f"tech%={cov.get('tech_pct')}")


# --------------------------------------------------------------------------- #
# Column resolution (once at startup)
# --------------------------------------------------------------------------- #
def resolve_columns(conn):
    cur = conn.cursor()
    biz_schema, biz_table = BUSINESS_TBL
    bcols = table_columns(cur, biz_schema, biz_table)
    if not bcols:
        cur.close()
        raise SystemExit(f"ERROR: {biz_schema}.{biz_table} not found / no columns.")
    pkv = table_pk(cur, biz_schema, biz_table)
    if not pkv:
        # fall back to an ext-id-ish unique column
        pkv = pick_col(bcols, ["id", "business_id", "source_id", "external_id", "ext_id"])
    sr_cols = table_columns(cur, SOURCE_TBL[0], SOURCE_TBL[1])
    cur.close()
    cols = {"pk": pkv, "sr_cols": sr_cols}
    for logical in ("name", "website", "phone", "email", "category", "region",
                    "locality", "industry"):
        cols[logical] = pick_col(bcols, CANDIDATES[logical])
    log(f"columns: pk={pkv} name={cols['name']} website={cols['website']} "
        f"phone={cols['phone']} email={cols['email']} industry={cols['industry']}")
    return cols


# --------------------------------------------------------------------------- #
# Modes
# --------------------------------------------------------------------------- #
def do_migrate():
    try:
        conn = connect_pg()
        cols = resolve_columns(conn)
        ensure_schema(conn)
        n = seed_queue(conn, BUSINESS_TBL[0], BUSINESS_TBL[1], cols["pk"])
        cov = compute_coverage(conn, cols)
        log(f"MIGRATE: schema ensured; seeded {n} new queue rows; "
            f"queue_remaining={cov['queue_remaining']} business_total={cov['business_total']}")
        write_local({"lane": "enrich_migrate", "seeded": n, "coverage": cov,
                     "ts": int(time.time())}, COUNTS_PATH)
        conn.close()
        print(f"[atlas_enrich] MIGRATE OK seeded={n} queue_remaining={cov['queue_remaining']}")
        sys.exit(0)
    except SystemExit:
        raise
    except BaseException as _exc:  # noqa: BLE001 -- surface real error via status-back
        import traceback as _tb
        tb = _tb.format_exc()
        log("MIGRATE FAILED:\n" + tb)
        node = os.environ.get("NODE_ID", "hetzner")
        schemas = {}
        try:
            _c2 = connect_pg()
            _cur = _c2.cursor()
            for _sch, _tbl in (("atlas", "enrich_queue"), ("atlas", "field_provenance"),
                               ("atlas", "business"), ("atlas", "source_record")):
                _cur.execute(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
                    (_sch, _tbl))
                schemas[f"{_sch}.{_tbl}"] = [f"{r[0]}:{r[1]}" for r in _cur.fetchall()]
            _cur.close()
            _c2.close()
        except BaseException as _se:  # noqa: BLE001
            schemas["_introspect_error"] = str(_se)[:300]
        try:
            gh_put(
                f"status/{node}/enrich-migrate-error.json",
                {"node": node, "stage": "migrate",
                 "error_class": type(_exc).__name__,
                 "error": str(_exc)[:500],
                 "existing_schemas": schemas,
                 "traceback_tail": "\n".join(tb.strip().splitlines()[-40:]),
                 "ts": int(time.time())},
                "enrich migrate self-captured traceback + schema")
        except BaseException as _e:  # noqa: BLE001
            log(f"could not push migrate error: {_e}")
        sys.exit(1)


def do_selftest():
    ok = True
    try:
        conn = connect_pg()
        log("selftest: Postgres connect OK")
    except Exception as e:  # noqa: BLE001
        log(f"selftest: Postgres connect FAILED: {e}")
        sys.exit(3)
    try:
        cols = resolve_columns(conn)
        ensure_schema(conn)
        log("selftest: schema ensured OK")
    except Exception as e:  # noqa: BLE001
        log(f"selftest: schema FAILED: {e}")
        ok = False
    # dry claim cycle inside a rolled-back txn
    try:
        cur = conn.cursor()
        cur.execute("""SELECT id FROM atlas.enrich_queue
                       WHERE status = 'pending'
                       ORDER BY priority,id FOR UPDATE SKIP LOCKED LIMIT 1""")
        _ = cur.fetchall()
        conn.rollback()
        log("selftest: SKIP LOCKED claim query OK (rolled back)")
        cur.close()
    except Exception as e:  # noqa: BLE001
        conn.rollback()
        log(f"selftest: claim query FAILED: {e}")
        ok = False
    if not _HAVE_DNS:
        log("selftest: NOTE dnspython not importable -> MX/DNS enrichment will be "
            "skipped (install with pip; non-fatal).")
    tok = os.environ.get("STATUS_TOKEN"); repo = os.environ.get("STATUS_REPO")
    log(f"selftest: status push {'ENABLED' if (tok and repo) else 'LOCAL-ONLY (STATUS_* unset)'}")
    conn.close()
    if ok:
        log("selftest: PASS")
        sys.exit(0)
    log("selftest: FAIL")
    sys.exit(3)


def run_loop(once=False):
    node = os.environ.get("NODE_ID", "hetzner")
    inst = os.environ.get("ATLAS_WORKER_INSTANCE",
                          os.environ.get("INSTANCE", "0"))
    worker_id = f"{node}:{socket.gethostname()}:{inst}:{os.getpid()}"
    log(f"worker start id={worker_id} batch={BATCH} once={once} have_dns={_HAVE_DNS}")
    conn = connect_pg()
    cols = resolve_columns(conn)
    ensure_schema(conn)
    if env_bool("ATLAS_ENRICH_SEED_ON_START", True):
        seeded = seed_queue(conn, BUSINESS_TBL[0], BUSINESS_TBL[1], cols["pk"])
        log(f"seeded {seeded} new queue rows on start")

    lifetime = 0
    window_count = 0
    window_start = time.time()
    last_report = 0.0
    last_seed = time.time()

    while True:
        try:
            rows = claim_batch(conn, worker_id, BATCH)
        except psycopg2.Error as e:
            conn.rollback()
            log(f"claim error (will retry): {e}")
            time.sleep(IDLE_SEC)
            continue

        if not rows:
            # idle gracefully
            now = time.time()
            if now - last_report >= REPORT_SEC:
                rate = window_count / max((now - window_start) / 60.0, 1e-6)
                report(conn, cols, worker_id, node, rate, lifetime)
                last_report = now
                window_count = 0
                window_start = now
            if not once and (now - last_seed >= SEED_SEC):
                try:
                    seeded = seed_queue(conn, BUSINESS_TBL[0], BUSINESS_TBL[1], cols["pk"])
                    if seeded:
                        log(f"re-seeded {seeded} new queue rows")
                except psycopg2.Error:
                    conn.rollback()
                last_seed = now
            if once:
                log("once: queue empty -> exit")
                break
            time.sleep(IDLE_SEC + random.uniform(0, IDLE_SEC * 0.3))
            continue

        for queue_id, ref, task_type in rows:
            try:
                oc = enrich_one(conn, cols, ref)
                if oc.get("missing"):
                    # business row gone (FK ON DELETE CASCADE would normally remove the
                    # queue row too) -> mark done so we don't reclaim it forever.
                    finish_row(conn, queue_id, "done",
                               err="business row missing", result_extra={"missing": True})
                else:
                    finish_row(conn, queue_id, "done", result_extra=oc)
                lifetime += 1
                window_count += 1
            except Exception as e:  # noqa: BLE001 -- one bad row never kills the worker
                conn.rollback()
                # FAIL-LOUD: surface the REAL error into result jsonb (never null);
                # finish_row escalates to 'dead' once attempts >= MAX_ATTEMPTS.
                log(f"enrich error business_id={ref} task={task_type}: {e}")
                try:
                    finish_row(conn, queue_id, "failed", err=str(e)[:1500])
                except psycopg2.Error:
                    conn.rollback()
            if PACING_MS:
                time.sleep(PACING_MS / 1000.0)

            now = time.time()
            if now - last_report >= REPORT_SEC:
                rate = window_count / max((now - window_start) / 60.0, 1e-6)
                report(conn, cols, worker_id, node, rate, lifetime)
                last_report = now
                window_count = 0
                window_start = now

        if once and lifetime > 0:
            # in --once mode, drain one batch fully then stop
            log(f"once: processed {lifetime} -> exit")
            break

    # final report on exit
    try:
        rate = window_count / max((time.time() - window_start) / 60.0, 1e-6)
        report(conn, cols, worker_id, node, rate, lifetime)
    except Exception:  # noqa: BLE001
        pass
    conn.close()


def main():
    load_env_file(AUTOPULL_ENV_PATH)
    load_env_file(DB_ENV_PATH)
    args = set(sys.argv[1:])
    if "--migrate" in args:
        do_migrate()
    elif "--selftest" in args:
        do_selftest()
    elif "--once" in args:
        run_loop(once=True)
    else:
        run_loop(once=False)


if __name__ == "__main__":
    main()
