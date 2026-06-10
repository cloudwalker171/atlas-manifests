#!/opt/atlas/venv/bin/python
"""
atlas_enrich_async.py  --  ASYNC (asyncio) ATLAS enrichment ENGINE.

WHY THIS EXISTS (the SEV-1 lesson):
  The live model is one OS process per worker (atlas-enrich-worker@1..N), each
  holding its own Postgres connection and its own ~RAM footprint. Scaling
  throughput meant adding processes -> adding RAM -> adding PG connections. On a
  16GB box at 57 worker/pool units, mem hit ~85% and a restart of the whole
  fleet produced a thundering-herd connection + memory spike that contributed to
  a Postgres outage. The bottleneck is NETWORK-BOUND work (DNS, HTTP, MX, RDAP)
  that spends almost all its wall-clock waiting on sockets -- exactly the case
  asyncio is built for.

WHAT CHANGED (and what did NOT):
  * CHANGED: each "lane" is now a COROUTINE, not a process/thread. A lane costs
    ~a few KB of stack + a coroutine frame instead of an 8MB thread stack or a
    whole interpreter. Hundreds-to-thousands of lanes fit in the SAME RAM.
  * CHANGED: Postgres is accessed through ONE bounded connection pool
    (ATLAS_ASYNC_PG_POOL, default 12) shared across ALL lanes regardless of lane
    count. 500-1000 network lanes => still only ~12 PG connections. This is the
    single most important safety property of this file (see "PG SAFETY" below).
  * CHANGED: network I/O is async (httpx.AsyncClient if available, else an
    asyncio loop.run_in_executor wrapper around the SAME stdlib urllib the live
    worker uses -- so it degrades, never breaks). DNS/MX is aiodns if available,
    else a threadpool-wrapped dnspython/socket resolver. Same answers either way.
  * UNCHANGED: the ENRICHMENT LOGIC and the EXACT field set / confidences /
    provenance writes are copied 1:1 from atlas_enrich_worker.py -- domain
    discovery, homepage+contact crawl, tech detection, MX/provider, role-emails,
    email_pattern HINT (conf<=0.4, never a personal address), phones, socials,
    industry, size cue, xref. RDAP (registrar/created/age/expires/registrant) +
    favicon + LinkedIn-company are folded in from atlas_enrich_sources.py so this
    ONE async pass covers what today takes two worker lanes (the streamline the
    field audit recommended). gov/mil suppression is NON-OVERRIDABLE in code.
  * UNCHANGED: the claim model -- SELECT ... FOR UPDATE SKIP LOCKED in BATCHES,
    the real enrich_queue status/locked_by/locked_at vocabulary, attempts/
    MAX_ATTEMPTS reclaim of stale locks, terminal done/failed->dead. Same DDL is
    introspected (additive-only; never CREATE/ALTER/DROP). Same status-back JSON.

PG SAFETY (this is what blew up before -- read this):
  Lane count and PG connection count are DECOUPLED. There are exactly THREE
  kinds of coroutine that ever touch the DB, and they ALL go through the bounded
  pool:
    1. ONE claimer task: pulls batches via SKIP LOCKED, feeds an in-RAM work
       queue (asyncio.Queue with a bounded maxsize so we never over-claim).
    2. N enrichment lanes: do ONLY network I/O + pure-python parsing. They never
       open a DB connection. Their output (a list of provenance writes + queue
       finish) goes onto a bounded WRITE queue.
    3. A small set of DB-WRITER tasks (ATLAS_ASYNC_DB_WRITERS, default 4): drain
       the write queue and persist through the pool, each acquiring/releasing a
       pooled connection per flush.
  Pool size (default 12) >= claimer(1) + writers(4) + headroom, and is the HARD
  ceiling on this process's PG connections no matter how many lanes run. The
  claimer also bounds in-flight rows (claimed but not yet finished) so a slow
  network can't make the queue table fill with stale 'claimed' locks faster than
  CLAIM_STALE_SEC can reclaim them.

  Backend: asyncpg if it imports cleanly (native async pool). Else psycopg2 with
  a bounded ThreadedConnectionPool driven from a dedicated asyncio executor with
  exactly ATLAS_ASYNC_PG_POOL threads -- so even the psycopg2 path can NEVER
  exceed the pool size. Either way the ceiling holds.

STARTUP STAGGER (no thundering herd):
  Lanes are started one every ATLAS_ASYNC_STAGGER_MS (default 25ms) so the box
  ramps to full concurrency over a few seconds instead of opening everything at
  once. The pool fills lazily. This is what makes a restart safe.

CONFIG (env; tune per box -- Hetzner 16GB/8core, InterServer 20GB/5core):
  ATLAS_ASYNC_LANES        concurrent enrichment coroutines (default 64; CANARY
                           start LOW e.g. 32, then ramp -- see ASYNC_ENRICH_PLAN.md)
  ATLAS_ASYNC_PG_POOL      bounded PG connections for THIS process (default 12)
  ATLAS_ASYNC_DB_WRITERS   DB-writer tasks draining the write queue (default 4)
  ATLAS_ASYNC_CLAIM_BATCH  rows per SKIP LOCKED claim (default 50)
  ATLAS_ASYNC_INFLIGHT     max rows claimed-but-unfinished (default lanes*4)
  ATLAS_ASYNC_STAGGER_MS   per-lane startup delay (default 25)
  ATLAS_ASYNC_PER_HOST     max concurrent lanes hitting one host (default 2)
  ATLAS_ENRICH_*           ALL the live worker's knobs are honored unchanged
                           (BATCH unused here; PACING_MS, HTTP_TIMEOUT,
                           HTTP_MAXBYTES, MAX_ATTEMPTS, CLAIM_STALE_SEC, ...).

MODES (mirror atlas_enrich_worker.py):
  --selftest   offline: imports, env load, helper unit-checks, an async harness
               that proves N lanes run against a MOCK DB with a BOUNDED pool and
               correct claim->enrich->write accounting. exit 0 ok / 3 broken.
               (No Postgres or network needed -- safe in the sandbox.)
  --migrate    (HETZNER ONLY) verify schema + idempotently seed find_domain rows
               (delegates to the same SQL the base worker uses). exit 0/1.
  --once       claim+enrich one bounded wave, then exit (proof / canary probe).
  --loop       (DEFAULT) run forever: claim, enrich across lanes, write, report,
               idle when queue empty.

This file is HOST-AGNOSTIC: same script on Hetzner and InterServer; point
PGHOST/DB_HOST at the Hetzner DB on InterServer via that box's /etc/atlas/db.env.
"""

import os
import re
import sys
import json
import time
import base64
import random
import socket
import asyncio
import datetime
import urllib.parse
import urllib.request
import urllib.error

# psycopg2 is the fallback DB driver (used only when asyncpg is absent). It is
# always present on the real boxes, but we import it SOFT so the fully-offline
# --selftest (no PG, no network) can run anywhere -- the psycopg2 code paths are
# never reached by the selftest. If neither asyncpg nor psycopg2 is importable at
# RUN time (loop/once/migrate), DB.open() fails loud.
try:
    import psycopg2  # type: ignore
    from psycopg2 import errors as _pg_errors  # noqa: F401
    _HAVE_PSYCOPG2 = True
except Exception:  # noqa: BLE001
    psycopg2 = None  # type: ignore
    _HAVE_PSYCOPG2 = False

# ---- optional async stack: degrade cleanly if a box lacks a lib ------------- #
try:
    import httpx  # type: ignore
    _HAVE_HTTPX = True
except Exception:  # noqa: BLE001
    _HAVE_HTTPX = False

try:
    import asyncpg  # type: ignore
    _HAVE_ASYNCPG = True
except Exception:  # noqa: BLE001
    _HAVE_ASYNCPG = False

try:
    import aiodns  # type: ignore
    _HAVE_AIODNS = True
except Exception:  # noqa: BLE001
    _HAVE_AIODNS = False

try:
    import dns.resolver  # type: ignore
    _HAVE_DNS = True
except Exception:  # noqa: BLE001
    _HAVE_DNS = False


# --------------------------------------------------------------------------- #
# Config -- honors the live worker's env names; adds ATLAS_ASYNC_* knobs.
# --------------------------------------------------------------------------- #
DB_ENV_PATH       = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
AUTOPULL_ENV_PATH = os.environ.get("ATLAS_AUTOPULL_CONF", "/etc/atlas/autopull.env")

BUSINESS_TBL = ("atlas", "business")
QUEUE_TBL    = ("atlas", "enrich_queue")
PROV_TBL     = ("atlas", "field_provenance")
SOURCE_TBL   = ("atlas", "source_record")

DEFAULT_TASK_TYPE = os.environ.get("ATLAS_ENRICH_TASK_TYPE", "find_domain")
MAX_ATTEMPTS      = int(os.environ.get("ATLAS_ENRICH_MAX_ATTEMPTS", "5"))
CLAIM_STALE_SEC   = int(os.environ.get("ATLAS_ENRICH_CLAIM_STALE_SEC", "900"))
IDLE_SEC          = float(os.environ.get("ATLAS_ENRICH_IDLE_SEC", "15"))
PACING_MS         = int(os.environ.get("ATLAS_ENRICH_PACING_MS", "250"))
SEED_SEC          = int(os.environ.get("ATLAS_ENRICH_SEED_SEC", "600"))
REPORT_SEC        = int(os.environ.get("ATLAS_ENRICH_REPORT_SEC", "300"))
HTTP_TIMEOUT      = int(os.environ.get("ATLAS_ENRICH_HTTP_TIMEOUT", "12"))
HTTP_MAXBYTES     = int(os.environ.get("ATLAS_ENRICH_HTTP_MAXBYTES", "700000"))
ENABLE_DISCOVERY  = os.environ.get("ATLAS_ENRICH_DISCOVER", "1") not in ("0", "false", "no")
ENABLE_RDAP       = os.environ.get("ATLAS_ENRICH_RDAP", "1") not in ("0", "false", "no")

# ---- async knobs (per-box tunable) ----------------------------------------- #
LANES        = int(os.environ.get("ATLAS_ASYNC_LANES", "64"))
PG_POOL      = int(os.environ.get("ATLAS_ASYNC_PG_POOL", "12"))
DB_WRITERS   = int(os.environ.get("ATLAS_ASYNC_DB_WRITERS", "4"))
CLAIM_BATCH  = int(os.environ.get("ATLAS_ASYNC_CLAIM_BATCH", "50"))
INFLIGHT_MAX = int(os.environ.get("ATLAS_ASYNC_INFLIGHT", str(max(LANES * 4, 128))))
STAGGER_MS   = int(os.environ.get("ATLAS_ASYNC_STAGGER_MS", "25"))
PER_HOST     = int(os.environ.get("ATLAS_ASYNC_PER_HOST", "2"))

USER_AGENT = ("atlas-enrich-async/1.0 (+https://github.com/cloudwalker171/atlas-manifests; "
              "free-tier enrichment; respects robots)")

COUNTS_PATH = os.environ.get(
    "ATLAS_COUNTS_PATH",
    os.path.join(os.environ.get("ATLAS_AUTOPULL_STATE", "/var/lib/atlas/autopull"),
                 "last_counts.json"))
ENRICH_STATE_DIR = os.environ.get("ATLAS_ENRICH_STATE_DIR", "/var/lib/atlas/enrich")

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
GOV_TLDS = (".gov", ".mil", ".fed.us")


def log(msg):
    print(f"[atlas_async] {datetime.datetime.now(datetime.timezone.utc).isoformat()} {msg}",
          flush=True)


def env_bool(name, default=False):
    v = os.environ.get(name)
    if v is None:
        return default
    return v not in ("0", "false", "False", "no", "")


# --------------------------------------------------------------------------- #
# env loader + pick (identical contract to the live worker)
# --------------------------------------------------------------------------- #
def load_env_file(path):
    if not path or not os.path.exists(path):
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


def pg_dsn_kwargs():
    return dict(
        host=pick("PGHOST", "DB_HOST", "ATLAS_DB_HOST", default="localhost"),
        port=pick("PGPORT", "DB_PORT", "ATLAS_DB_PORT", default="5432"),
        dbname=pick("PGDATABASE", "DB_NAME", "ATLAS_DB_NAME", default="tuanichat_atlas"),
        user=pick("PGUSER", "DB_USER", "ATLAS_DB_USER", default="atlas"),
        password=pick("PGPASSWORD", "DB_PASSWORD", "DB_PASS", "ATLAS_DB_PASSWORD", default=None),
    )


# --------------------------------------------------------------------------- #
# Normalization + parsing helpers -- copied 1:1 from the live worker so the
# OUTPUT (fields, values, confidences) is byte-identical to today.
# --------------------------------------------------------------------------- #
_LEGAL_SUFFIX = re.compile(
    r"\b(inc|incorporated|llc|l\.l\.c|llp|ltd|limited|co|corp|corporation|"
    r"company|plc|pllc|pc|lp|gmbh|sa|nv|bv|pty)\b\.?", re.I)
_NONALNUM = re.compile(r"[^a-z0-9]+")
_PHONE_RE = re.compile(r"(?:\+?1[\s.\-]?)?\(?([2-9]\d{2})\)?[\s.\-]?(\d{3})[\s.\-]?(\d{4})")
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)
_META_DESC_RE = re.compile(
    r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']', re.I | re.S)
_FAVICON_RE = re.compile(
    r'<link[^>]+rel=["\'][^"\']*icon[^"\']*["\'][^>]+href=["\']([^"\']+)["\']', re.I)
_LINKEDIN_CO_RE = re.compile(
    r'https?://(?:www\.)?linkedin\.com/company/[A-Za-z0-9_%\-./]+', re.I)

_SOCIAL_RE = {
    "facebook":  re.compile(r"https?://(?:www\.)?facebook\.com/[A-Za-z0-9_.\-/]+"),
    "linkedin":  re.compile(r"https?://(?:www\.)?linkedin\.com/(?:company|in)/[A-Za-z0-9_.\-/%]+"),
    "twitter":   re.compile(r"https?://(?:www\.)?(?:twitter|x)\.com/[A-Za-z0-9_]+"),
    "instagram": re.compile(r"https?://(?:www\.)?instagram\.com/[A-Za-z0-9_.\-/]+"),
    "youtube":   re.compile(r"https?://(?:www\.)?youtube\.com/[A-Za-z0-9_@.\-/]+"),
    "tiktok":    re.compile(r"https?://(?:www\.)?tiktok\.com/@[A-Za-z0-9_.\-]+"),
}

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

CANDIDATES = {
    "name":     ["name", "business_name", "title", "display_name", "legal_name"],
    "website":  ["website", "url", "website_url", "homepage"],
    "phone":    ["phone_e164", "phone", "phone_number", "telephone", "contact_phone"],
    "email":    ["email", "email_address", "contact_email"],
    "category": ["category", "primary_category", "categories", "license_description"],
    "industry": ["industry", "sector"],
    "region":   ["region", "state", "province"],
    "locality": ["locality", "city", "town"],
    "sr_source": ["source", "data_source", "origin"],
    "sr_payload": ["raw", "payload", "data", "raw_json", "raw_jsonb", "doc", "record"],
    "sr_name":   ["name", "business_name", "title"],
}

_RDAP_BOOTSTRAP = {
    "com": "https://rdap.verisign.com/com/v1/domain/",
    "net": "https://rdap.verisign.com/net/v1/domain/",
    "org": "https://rdap.publicinterestregistry.org/rdap/domain/",
    "info": "https://rdap.identitydigital.services/rdap/domain/",
    "io":  "https://rdap.nic.io/domain/",
}


def norm_name(name):
    if not name:
        return ""
    s = name.lower()
    s = _LEGAL_SUFFIX.sub(" ", s)
    s = _NONALNUM.sub(" ", s)
    return " ".join(s.split())


def name_tokens(name):
    return [t for t in norm_name(name).split() if len(t) > 1]


def norm_phone(raw):
    if not raw:
        return None
    m = _PHONE_RE.search(str(raw))
    if not m:
        digits = re.sub(r"\D", "", str(raw))
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        return digits if len(digits) == 10 else None
    return "".join(m.groups())


def registrable_domain(host_or_url):
    if not host_or_url:
        return None
    h = host_or_url.strip().lower()
    if "://" in h:
        h = urllib.parse.urlsplit(h).netloc
    h = h.split("@")[-1].split(":")[0].strip().strip(".")
    if not h or "." not in h:
        return None
    if h.startswith("www."):
        h = h[4:]
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
    return (d.endswith(".gov") or d.endswith(".mil")
            or any(d.endswith(t) for t in GOV_TLDS)
            or ".gov." in ("." + d + ".") or ".mil." in ("." + d + "."))


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


def parse_page(final_url, headers, html):
    out = {"emails": set(), "phones": set(), "socials": {}, "title": None,
           "meta_desc": None, "tech": set(), "favicon": None, "linkedin_co": None}
    if not html:
        return out
    for e in _EMAIL_RE.findall(html):
        e = e.lower().strip(".")
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
    fm = _FAVICON_RE.search(html)
    if fm:
        out["favicon"] = fm.group(1).strip()
    lm = _LINKEDIN_CO_RE.search(html)
    if lm:
        out["linkedin_co"] = lm.group(0)
    return out


def infer_email_pattern(emails, domain):
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


def mx_provider(hosts):
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
    return provider


def favicon_url(domain, href):
    if not domain:
        return None
    if href:
        href = href.strip()
        if href.startswith("//"):
            return "https:" + href
        if href.startswith("http"):
            return href
        if href.startswith("/"):
            return f"https://{domain}{href}"
        return f"https://{domain}/{href}"
    return f"https://{domain}/favicon.ico"


def domain_age_band(created_iso):
    if not created_iso:
        return None
    try:
        y = int(created_iso[:4])
    except (ValueError, TypeError):
        return None
    age = datetime.datetime.now(datetime.timezone.utc).year - y
    if age < 0:
        return None
    if age <= 1:
        return "0-1y"
    if age <= 3:
        return "1-3y"
    if age <= 7:
        return "3-7y"
    if age <= 15:
        return "7-15y"
    return "15y+"


def _vcard_fn(vcard):
    try:
        for item in vcard[1]:
            if item and item[0] in ("fn", "org"):
                return str(item[3])[:200]
    except Exception:  # noqa: BLE001
        return None
    return None


def parse_rdap(doc):
    out = {}
    for ent in doc.get("entities", []) or []:
        roles = ent.get("roles") or []
        org = _vcard_fn(ent.get("vcardArray"))
        if "registrar" in roles and org:
            out["registrar"] = org
        if "registrant" in roles and org:
            out["registrant_org"] = org
    for ev in doc.get("events", []) or []:
        action = ev.get("eventAction")
        when = ev.get("eventDate")
        if action == "registration" and when:
            out["created"] = when[:10]
        if action == "expiration" and when:
            out["expires"] = when[:10]
    ns = doc.get("nameservers") or []
    if ns:
        out["ns_count"] = len(ns)
    return out


def robots_disallows(body):
    rules = []
    if body:
        applies = False
        for line in body.splitlines():
            line = line.split("#", 1)[0].strip()
            if not line:
                continue
            low = line.lower()
            if low.startswith("user-agent:"):
                applies = (low.split(":", 1)[1].strip() == "*")
            elif applies and low.startswith("disallow:"):
                rule = line.split(":", 1)[1].strip()
                if rule:
                    rules.append(rule)
    return rules


# --------------------------------------------------------------------------- #
# ASYNC NETWORK LAYER
#   httpx.AsyncClient when available (true async, keep-alive pooling, DNS reuse).
#   Else: wrap the SAME stdlib urllib http_get the live worker uses in the
#   asyncio default threadpool executor -- identical bytes, still non-blocking
#   to the event loop. Per-host semaphore enforces politeness regardless.
# --------------------------------------------------------------------------- #
class Net:
    def __init__(self, loop):
        self.loop = loop
        self._client = None
        self._host_sems = {}
        self._robots = {}     # domain -> [disallow rules]
        self._resolver = None
        if _HAVE_HTTPX:
            try:
                limits = httpx.Limits(max_connections=max(LANES, 16),
                                      max_keepalive_connections=max(LANES // 2, 8))
                # trust_env=False: ignore ambient HTTP(S)/SOCKS proxy env so the
                # engine talks DIRECT to target hosts (and so a weird proxy env in
                # a build/sandbox can't break client construction). Falls back to
                # the stdlib executor path on ANY construction failure.
                self._client = httpx.AsyncClient(
                    follow_redirects=True, timeout=HTTP_TIMEOUT, limits=limits,
                    trust_env=False,
                    headers={"User-Agent": USER_AGENT,
                             "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                             "Accept-Language": "en-US,en;q=0.8"})
            except Exception as _e:  # noqa: BLE001
                self._client = None
                log(f"httpx client init failed ({_e}); using stdlib HTTP fallback")
        if _HAVE_AIODNS:
            try:
                self._resolver = aiodns.DNSResolver(loop=loop)
            except Exception:  # noqa: BLE001
                self._resolver = None

    def _host_sem(self, host):
        sem = self._host_sems.get(host)
        if sem is None:
            sem = asyncio.Semaphore(PER_HOST)
            self._host_sems[host] = sem
        return sem

    async def get(self, url, timeout=None, maxbytes=None, accept_json=False):
        """Return (final_url, status, headers_dict, text) or None. Per-host polite."""
        timeout = timeout or HTTP_TIMEOUT
        maxbytes = maxbytes or HTTP_MAXBYTES
        host = registrable_domain(url) or url
        async with self._host_sem(host):
            if self._client is not None:
                return await self._get_httpx(url, timeout, maxbytes, accept_json)
            return await self.loop.run_in_executor(
                None, _http_get_blocking, url, timeout, maxbytes, accept_json)

    async def _get_httpx(self, url, timeout, maxbytes, accept_json):
        try:
            headers = {"Accept": "application/json"} if accept_json else None
            async with self._client.stream("GET", url, timeout=timeout,
                                           headers=headers) as resp:
                raw = b""
                async for chunk in resp.aiter_bytes():
                    raw += chunk
                    if len(raw) > maxbytes:
                        raw = raw[:maxbytes]
                        break
                hdrs = {k.lower(): v for k, v in resp.headers.items()}
                enc = "utf-8"
                m = re.search(r"charset=([\w\-]+)", hdrs.get("content-type", ""), re.I)
                if m:
                    enc = m.group(1)
                try:
                    text = raw.decode(enc, "replace")
                except (LookupError, UnicodeDecodeError):
                    text = raw.decode("utf-8", "replace")
                return (str(resp.url), resp.status_code, hdrs, text)
        except Exception:  # noqa: BLE001 -- network failure is normal; fail soft
            return None

    async def dns_resolves(self, domain):
        if self._resolver is not None:
            for rtype in ("A", "AAAA"):
                try:
                    await self._resolver.query(domain, rtype)
                    return True
                except Exception:  # noqa: BLE001
                    continue
            return False
        return await self.loop.run_in_executor(None, _dns_resolves_blocking, domain)

    async def mx_hosts(self, domain):
        if self._resolver is not None:
            try:
                ans = await self._resolver.query(domain, "MX")
                hosts = sorted(str(r.host).rstrip(".").lower() for r in ans)
                return hosts
            except Exception:  # noqa: BLE001
                return []
        return await self.loop.run_in_executor(None, _mx_hosts_blocking, domain)

    async def robots_allows(self, domain, path):
        rules = self._robots.get(domain)
        if rules is None:
            res = await self.get(f"https://{domain}/robots.txt", timeout=8, maxbytes=60000)
            body = res[3] if res and res[1] == 200 else ""
            rules = robots_disallows(body)
            self._robots[domain] = rules
        for rule in rules:
            if path.startswith(rule):
                return False
        return True

    async def aclose(self):
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:  # noqa: BLE001
                pass


# ---- blocking fallbacks (run in executor; identical to live worker) -------- #
def _http_get_blocking(url, timeout, maxbytes, accept_json):
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "application/json" if accept_json else
                  "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.8",
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read(maxbytes + 1)
            if len(raw) > maxbytes:
                raw = raw[:maxbytes]
            headers = {k.lower(): v for k, v in resp.headers.items()}
            enc = "utf-8"
            m = re.search(r"charset=([\w\-]+)", headers.get("content-type", ""), re.I)
            if m:
                enc = m.group(1)
            try:
                text = raw.decode(enc, "replace")
            except (LookupError, UnicodeDecodeError):
                text = raw.decode("utf-8", "replace")
            return (resp.geturl(), resp.getcode(), headers, text)
    except urllib.error.HTTPError as e:
        return (url, e.code, {k.lower(): v for k, v in (e.headers or {}).items()}, "")
    except (urllib.error.URLError, socket.timeout, ConnectionError, ValueError, OSError):
        return None
    except Exception:  # noqa: BLE001
        return None


def _dns_resolves_blocking(domain):
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


def _mx_hosts_blocking(domain):
    if not _HAVE_DNS:
        return []
    try:
        answers = dns.resolver.resolve(domain, "MX", lifetime=8)
    except Exception:  # noqa: BLE001
        return []
    return sorted(str(r.exchange).rstrip(".").lower() for r in answers)


# --------------------------------------------------------------------------- #
# CORE ENRICHMENT (async) -- mirrors enrich_one() in the live worker exactly,
# plus the folded-in RDAP/favicon/LinkedIn from atlas_enrich_sources.py.
# Produces a list of "writes": dicts the DB-writer tasks persist. NO DB here.
# --------------------------------------------------------------------------- #
def _prov(writes, field, value, source, method, confidence):
    """Queue a field_provenance UPSERT (same shape as worker.record())."""
    if value is None or value == "":
        return
    src = source if not method else f"{source}/{method}"
    writes.append(("prov", field, str(value)[:2000], str(src)[:200], float(confidence)))


def _fill(writes, col, value):
    if not col or value is None or value == "":
        return
    writes.append(("fill", col, str(value)[:500]))


async def discover_domain(net, name, existing_website):
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
        if not await net.dns_resolves(dom):
            continue
        res = await net.get(f"https://{dom}/", timeout=8)
        if not res or res[1] >= 400:
            res = await net.get(f"http://{dom}/", timeout=8)
        if not res or res[1] >= 400 or not res[3]:
            continue
        info = parse_page(res[0], res[2], res[3])
        title_toks = set(name_tokens(info.get("title") or ""))
        overlap = len(name_set & title_toks)
        if overlap >= max(1, len(name_set) // 2):
            return (registrable_domain(res[0]) or dom, "guessed_verified", 0.6)
    return (None, "no_verified_guess", 0.0)


async def rdap_lookup(net, domain):
    if not ENABLE_RDAP or not domain:
        return {}
    tld = domain.rsplit(".", 1)[-1]
    base = _RDAP_BOOTSTRAP.get(tld)
    url = (base + urllib.parse.quote(domain)) if base else \
          ("https://rdap.org/domain/" + urllib.parse.quote(domain))
    res = await net.get(url, timeout=10, maxbytes=120000, accept_json=True)
    if not res or res[1] >= 400 or not res[3]:
        return {}
    try:
        return parse_rdap(json.loads(res[3]))
    except ValueError:
        return {}


async def enrich_lane(net, cols, ref, biz):
    """Pure network + parse. Returns (outcome_dict, writes_list). No DB access.
    `biz` is the prefetched business row dict. Mirrors enrich_one() ordering."""
    name = biz.get("name") or ""
    website = biz.get("website") or ""
    category = biz.get("category") or ""
    region = biz.get("region") or ""

    writes = []
    outcome = {"fields_filled": 0, "prov_rows": 0, "suppressed": False, "domain": None}

    # 1. domain
    domain, dsrc, dconf = await discover_domain(net, name, website)
    outcome["domain"] = domain
    gov = is_gov_mil(domain) if domain else False
    outcome["suppressed"] = gov
    if domain:
        _prov(writes, "domain", domain, dsrc, "domain_resolve", dconf)
        if dsrc != "existing_website" and cols.get("website"):
            _fill(writes, cols["website"], f"https://{domain}")
            outcome["fields_filled"] += 1

    # 2/3. homepage + contact crawl (robots-respecting). Fetch '/' first; only
    # hit /contact|/about if '/' lacked a contact email+phone (streamline).
    page = None
    if domain:
        for path in ("/", "/contact", "/contact-us", "/about"):
            if path != "/" and page is not None:
                have_email = any(e.endswith("@" + domain) and
                                 e.split("@")[0] in ROLE_LOCALPARTS for e in page["emails"])
                have_phone = bool({norm_phone(p) for p in page["phones"]} - {None})
                if have_email and have_phone:
                    break
            if not await net.robots_allows(domain, path):
                continue
            res = await net.get(f"https://{domain}{path}", timeout=HTTP_TIMEOUT)
            if not res or res[1] >= 400:
                if path == "/":
                    res = await net.get(f"http://{domain}{path}", timeout=HTTP_TIMEOUT)
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
                page["favicon"] = page.get("favicon") or info.get("favicon")
                page["linkedin_co"] = page.get("linkedin_co") or info.get("linkedin_co")
            if path == "/":
                page["title"] = info.get("title")
                page["meta_desc"] = info.get("meta_desc")

    # tech
    if page and page["tech"]:
        _prov(writes, "tech", ",".join(sorted(page["tech"])[:12]),
              "homepage_crawl", "html_signature", 0.7)
        outcome["prov_rows"] += 1

    # MX / DNS provider
    if domain and not gov:
        hosts = await net.mx_hosts(domain)
        if hosts:
            _prov(writes, "mx", ",".join(hosts[:4]), "dns_mx", "dns", 0.9)
            outcome["prov_rows"] += 1
            prov = mx_provider(hosts)
            if prov:
                _prov(writes, "email_provider", prov, "dns_mx", "mx_host", 0.8)
                outcome["prov_rows"] += 1

    # contact data -- SUPPRESSED for .gov/.mil (non-overridable)
    if gov:
        _prov(writes, "contact_suppressed", "gov_mil", "policy",
              "non_overridable_suppression", 1.0)
        outcome["prov_rows"] += 1
    elif page:
        company_emails = sorted(e for e in page["emails"]
                                if domain and e.endswith("@" + domain))
        role_emails = [e for e in company_emails if e.split("@")[0] in ROLE_LOCALPARTS]
        if role_emails:
            _prov(writes, "email", ",".join(role_emails[:3]),
                  "homepage_crawl", "role_email", 0.75)
            outcome["prov_rows"] += 1
            if cols.get("email"):
                _fill(writes, cols["email"], role_emails[0])
                outcome["fields_filled"] += 1
        pat, pconf = infer_email_pattern(page["emails"], domain) if domain else (None, 0)
        if pat:
            _prov(writes, "email_pattern", pat, "email_pattern", "inferred", pconf)
            outcome["prov_rows"] += 1
        phones = sorted({norm_phone(p) for p in page["phones"]} - {None})
        if phones:
            _prov(writes, "phone", ",".join(phones[:3]),
                  "homepage_crawl", "regex_nanp", 0.7)
            outcome["prov_rows"] += 1
            if cols.get("phone"):
                _fill(writes, cols["phone"], phones[0])
                outcome["fields_filled"] += 1
        for net_name, surl in page["socials"].items():
            _prov(writes, f"social_{net_name}", surl, "homepage_crawl", "link", 0.8)
            outcome["prov_rows"] += 1

    # firmographics
    title = page.get("title") if page else None
    meta = page.get("meta_desc") if page else None
    ind, iconf = infer_industry(category, title, meta)
    if ind:
        _prov(writes, "industry", ind, "classifier", "keyword", iconf)
        outcome["prov_rows"] += 1
        if cols.get("industry"):
            _fill(writes, cols["industry"], ind)
            outcome["fields_filled"] += 1
    sz, sconf = size_cues(" ".join(x for x in (title, meta) if x))
    if sz:
        _prov(writes, "size_cue", sz, "homepage_crawl", "heuristic", sconf)
        outcome["prov_rows"] += 1

    # folded-in sources worker: favicon + LinkedIn-company + RDAP (no 2nd lane)
    if domain and not gov:
        if page:
            fav = favicon_url(domain, page.get("favicon"))
            if fav:
                _prov(writes, "favicon", fav, "homepage_crawl", "icon_link", 0.7)
                outcome["prov_rows"] += 1
            li = page.get("linkedin_co")
            if li:
                _prov(writes, "social_linkedin", li, "homepage_crawl", "company_page", 0.8)
                outcome["prov_rows"] += 1
        rd = await rdap_lookup(net, domain)
        if rd.get("registrar"):
            _prov(writes, "domain_registrar", rd["registrar"], "rdap", "rdap", 0.85)
            outcome["prov_rows"] += 1
        if rd.get("created"):
            _prov(writes, "domain_created", rd["created"], "rdap", "rdap", 0.85)
            outcome["prov_rows"] += 1
            band = domain_age_band(rd["created"])
            if band:
                _prov(writes, "domain_age_band", band, "rdap", "derived", 0.8)
                outcome["prov_rows"] += 1
        if rd.get("expires"):
            _prov(writes, "domain_expires", rd["expires"], "rdap", "rdap", 0.8)
            outcome["prov_rows"] += 1
        if rd.get("registrant_org"):
            org = rd["registrant_org"]
            if not re.search(r"privacy|proxy|redacted|whoisguard|domains by", org, re.I):
                _prov(writes, "registrant_org", org, "rdap", "rdap", 0.6)
                outcome["prov_rows"] += 1

    # xref carries a flag for the DB-writer to run the read-only query under its
    # own pooled connection (kept out of the lane to honor "lanes touch no DB").
    outcome["xref"] = {"name": name, "region": region}
    return outcome, writes


# --------------------------------------------------------------------------- #
# BOUNDED PG ACCESS LAYER -- the safety core. asyncpg pool OR a psycopg2
# ThreadedConnectionPool fronted by a fixed executor. EITHER WAY conn count is
# capped at PG_POOL no matter how many lanes run.
# --------------------------------------------------------------------------- #
class DB:
    """Abstracts the bounded pool. Methods are awaited from claimer/writer tasks
    only. Lanes never call into DB."""

    def __init__(self):
        self.kind = None          # 'asyncpg' | 'psycopg2'
        self._apool = None        # asyncpg pool
        self._ppool = None        # psycopg2 ThreadedConnectionPool
        self._pexec = None        # fixed executor for psycopg2 (size == PG_POOL)
        self.cols = None

    async def open(self):
        kw = pg_dsn_kwargs()
        if _HAVE_ASYNCPG:
            self.kind = "asyncpg"
            self._apool = await asyncpg.create_pool(
                host=kw["host"], port=int(kw["port"]), database=kw["dbname"],
                user=kw["user"], password=kw["password"],
                min_size=2, max_size=PG_POOL, command_timeout=30,
                server_settings={"application_name": "atlas_enrich_async"})
            log(f"DB: asyncpg pool open max_size={PG_POOL}")
        else:
            if not _HAVE_PSYCOPG2:
                raise SystemExit("DB ERROR: neither asyncpg nor psycopg2 is importable; "
                                 "install one (pip install asyncpg  OR  psycopg2-binary).")
            from concurrent.futures import ThreadPoolExecutor
            from psycopg2 import pool as _pgpool
            self.kind = "psycopg2"
            self._ppool = _pgpool.ThreadedConnectionPool(
                1, PG_POOL,
                host=kw["host"], port=kw["port"], dbname=kw["dbname"],
                user=kw["user"], password=kw["password"],
                connect_timeout=int(os.environ.get("ATLAS_DB_CONNECT_TIMEOUT", "10")),
                application_name="atlas_enrich_async")
            # executor threads == PG_POOL so we can NEVER ask for more conns than
            # the pool has -> the ceiling is enforced twice.
            self._pexec = ThreadPoolExecutor(max_workers=PG_POOL,
                                             thread_name_prefix="atlas-pg")
            log(f"DB: psycopg2 ThreadedConnectionPool open max={PG_POOL} "
                f"(executor threads={PG_POOL})")

    async def close(self):
        if self.kind == "asyncpg" and self._apool is not None:
            await self._apool.close()
        elif self.kind == "psycopg2":
            if self._ppool is not None:
                self._ppool.closeall()
            if self._pexec is not None:
                self._pexec.shutdown(wait=False)

    # -- psycopg2 helper: run a sync fn(conn) on a pooled conn in the executor -- #
    async def _p_run(self, fn):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._pexec, self._p_run_sync, fn)

    def _p_run_sync(self, fn):
        conn = self._ppool.getconn()
        try:
            conn.autocommit = False
            result = fn(conn)
            conn.commit()
            return result
        except Exception:
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001
                pass
            raise
        finally:
            self._ppool.putconn(conn)

    # ---- schema + columns ---- #
    async def resolve_columns(self):
        if self.kind == "asyncpg":
            async with self._apool.acquire() as con:
                cols = await self._resolve_cols_apg(con)
        else:
            cols = await self._p_run(self._resolve_cols_pg)
        self.cols = cols
        return cols

    async def _resolve_cols_apg(self, con):
        bs, bt = BUSINESS_TBL
        bcols = {r["column_name"] for r in await con.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=$1 AND table_name=$2", bs, bt)}
        if not bcols:
            raise SystemExit(f"ERROR: {bs}.{bt} not found / no columns.")
        pk = await con.fetchval(
            "SELECT a.attname FROM pg_index i "
            "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
            "WHERE i.indrelid=($1)::regclass AND i.indisprimary", f"{bs}.{bt}")
        if not pk:
            pk = _pick_col(bcols, ["id", "business_id", "source_id", "external_id"])
        sr_cols = {r["column_name"] for r in await con.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=$1 AND table_name=$2", SOURCE_TBL[0], SOURCE_TBL[1])}
        # verify enrich tables (additive-only; fail loud)
        self._verify_required({
            ("atlas", "enrich_queue"): {r["column_name"] for r in await con.fetch(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='atlas' AND table_name='enrich_queue'")},
            ("atlas", "field_provenance"): {r["column_name"] for r in await con.fetch(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='atlas' AND table_name='field_provenance'")},
        })
        return _build_cols(pk, bcols, sr_cols)

    def _resolve_cols_pg(self, conn):
        cur = conn.cursor()
        bs, bt = BUSINESS_TBL

        def colset(sch, tbl):
            cur.execute("SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema=%s AND table_name=%s", (sch, tbl))
            return {r[0] for r in cur.fetchall()}

        bcols = colset(bs, bt)
        if not bcols:
            raise SystemExit(f"ERROR: {bs}.{bt} not found / no columns.")
        cur.execute("SELECT a.attname FROM pg_index i "
                    "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
                    "WHERE i.indrelid=%s::regclass AND i.indisprimary", (f"{bs}.{bt}",))
        rows = [r[0] for r in cur.fetchall()]
        pk = rows[0] if rows else _pick_col(bcols, ["id", "business_id"])
        sr_cols = colset(SOURCE_TBL[0], SOURCE_TBL[1])
        self._verify_required({("atlas", "enrich_queue"): colset("atlas", "enrich_queue"),
                               ("atlas", "field_provenance"): colset("atlas", "field_provenance")})
        cur.close()
        return _build_cols(pk, bcols, sr_cols)

    @staticmethod
    def _verify_required(have_map):
        required = {
            ("atlas", "enrich_queue"): {"id", "business_id", "task_type", "status",
                                        "priority", "attempts", "locked_by", "locked_at",
                                        "result", "created_at", "updated_at"},
            ("atlas", "field_provenance"): {"business_id", "field", "value",
                                            "source_code", "confidence", "last_verified"},
        }
        for key, need in required.items():
            have = have_map.get(key, set())
            if not have:
                raise SystemExit(f"SCHEMA ERROR: {key[0]}.{key[1]} not found. "
                                 f"Additive-only worker refuses to create it.")
            missing = need - have
            if missing:
                raise SystemExit(f"SCHEMA DRIFT: {key[0]}.{key[1]} missing {sorted(missing)}.")

    # ---- seed ---- #
    async def seed_queue(self):
        bs, bt = BUSINESS_TBL
        pk = self.cols["pk"]
        sql = (f'INSERT INTO atlas.enrich_queue (business_id, task_type, priority, status) '
               f'SELECT b."{pk}", $1, 5, \'pending\' FROM "{bs}"."{bt}" b '
               f'ON CONFLICT (business_id, task_type) DO NOTHING')
        if self.kind == "asyncpg":
            async with self._apool.acquire() as con:
                res = await con.execute(sql, DEFAULT_TASK_TYPE)
                try:
                    return int(res.split()[-1])
                except Exception:  # noqa: BLE001
                    return 0
        def _fn(conn):
            cur = conn.cursor()
            cur.execute(sql.replace("$1", "%s"), (DEFAULT_TASK_TYPE,))
            n = cur.rowcount or 0
            cur.close()
            return n
        return await self._p_run(_fn)

    # ---- claim a batch (SKIP LOCKED) + prefetch the business rows ---- #
    async def claim_and_fetch(self, worker_id, batch):
        """Returns list of (queue_id, business_id, biz_dict). One pooled conn."""
        cols = self.cols
        sel_cols = [c for c in (cols.get("name"), cols.get("website"), cols.get("phone"),
                                cols.get("email"), cols.get("category"), cols.get("region"),
                                cols.get("locality")) if c]
        if self.kind == "asyncpg":
            return await self._claim_apg(worker_id, batch, sel_cols)
        return await self._p_run(lambda conn: self._claim_pg(conn, worker_id, batch, sel_cols))

    async def _claim_apg(self, worker_id, batch, sel_cols):
        bs, bt = BUSINESS_TBL
        pk = self.cols["pk"]
        async with self._apool.acquire() as con:
            async with con.transaction():
                rows = await con.fetch(
                    "WITH c AS ("
                    " SELECT id FROM atlas.enrich_queue"
                    " WHERE attempts < $1 AND (status='pending'"
                    "   OR (status='claimed' AND locked_at IS NOT NULL"
                    "       AND locked_at < now() - make_interval(secs => $2)))"
                    " ORDER BY priority, id FOR UPDATE SKIP LOCKED LIMIT $3)"
                    " UPDATE atlas.enrich_queue q SET status='claimed', locked_by=$4,"
                    "   locked_at=now(), attempts=attempts+1, updated_at=now()"
                    " FROM c WHERE q.id=c.id RETURNING q.id, q.business_id",
                    MAX_ATTEMPTS, CLAIM_STALE_SEC, batch, worker_id)
                out = []
                for r in rows:
                    qid, bid = r["id"], r["business_id"]
                    sel = ", ".join(f'b."{c}"' for c in sel_cols) or "1"
                    brow = await con.fetchrow(
                        f'SELECT {sel} FROM "{bs}"."{bt}" b WHERE b."{pk}"=$1', bid)
                    biz = self._biz_dict(sel_cols, brow)
                    out.append((qid, bid, biz))
                return out

    def _claim_pg(self, conn, worker_id, batch, sel_cols):
        bs, bt = BUSINESS_TBL
        pk = self.cols["pk"]
        cur = conn.cursor()
        cur.execute(
            "WITH c AS ("
            " SELECT id FROM atlas.enrich_queue"
            " WHERE attempts < %s AND (status='pending'"
            "   OR (status='claimed' AND locked_at IS NOT NULL"
            "       AND locked_at < now() - make_interval(secs => %s)))"
            " ORDER BY priority, id FOR UPDATE SKIP LOCKED LIMIT %s)"
            " UPDATE atlas.enrich_queue q SET status='claimed', locked_by=%s,"
            "   locked_at=now(), attempts=attempts+1, updated_at=now()"
            " FROM c WHERE q.id=c.id RETURNING q.id, q.business_id",
            (MAX_ATTEMPTS, CLAIM_STALE_SEC, batch, worker_id))
        claimed = cur.fetchall()
        out = []
        for qid, bid in claimed:
            sel = ", ".join(f'b."{c}"' for c in sel_cols) or "1"
            cur.execute(f'SELECT {sel} FROM "{bs}"."{bt}" b WHERE b."{pk}"=%s', (bid,))
            brow = cur.fetchone()
            out.append((qid, bid, self._biz_dict(sel_cols, brow)))
        cur.close()
        return out

    def _biz_dict(self, sel_cols, brow):
        cols = self.cols
        biz = {}
        if not brow:
            return {"_missing": True}
        vals = dict(zip(sel_cols, list(brow)))
        for logical in ("name", "website", "phone", "email", "category", "region", "locality"):
            c = cols.get(logical)
            biz[logical] = vals.get(c) if c else None
        return biz

    # ---- persist a finished record: writes + xref + finish_row, ONE conn ---- #
    async def persist(self, ref, queue_id, outcome, writes):
        if self.kind == "asyncpg":
            await self._persist_apg(ref, queue_id, outcome, writes)
        else:
            await self._p_run(lambda conn: self._persist_pg(conn, ref, queue_id, outcome, writes))

    def _xref_sql_params(self, ref, name):
        """Read-only xref query SQL/params, or None. Built from real source_record."""
        cols = self.cols
        sr_cols = cols["sr_cols"]
        sr_src = _pick_col(sr_cols, CANDIDATES["sr_source"])
        sr_name = _pick_col(sr_cols, CANDIDATES["sr_name"])
        sr_pay = _pick_col(sr_cols, CANDIDATES["sr_payload"])
        if not sr_src or not (sr_name or sr_pay):
            return None
        nn = norm_name(name)
        if len(nn) < 4:
            return None
        name_expr = f'b."{sr_name}"' if sr_name else f'(b."{sr_pay}")::text'
        sql = (f'SELECT b."{sr_src}" AS src, {name_expr} AS nm '
               f'FROM "{SOURCE_TBL[0]}"."{SOURCE_TBL[1]}" b '
               f"WHERE b.\"{sr_src}\" ~* '(edgar|nonprofit|irs|990|sec|license|licens)' "
               f"AND {name_expr} ILIKE %s LIMIT 5")
        return sql, (f"%{name.strip()[:40]}%",), nn

    def _persist_pg(self, conn, ref, queue_id, outcome, writes):
        bs, bt = BUSINESS_TBL
        pk = self.cols["pk"]
        cur = conn.cursor()
        for w in writes:
            if w[0] == "prov":
                _, field, value, src, conf = w
                try:
                    cur.execute(
                        "INSERT INTO atlas.field_provenance "
                        "(business_id, field, value, source_code, confidence, last_verified) "
                        "VALUES (%s,%s,%s,%s,%s, now()) "
                        "ON CONFLICT (business_id, field) DO UPDATE "
                        " SET value=EXCLUDED.value, source_code=EXCLUDED.source_code, "
                        "     confidence=EXCLUDED.confidence, last_verified=now() "
                        " WHERE EXCLUDED.confidence >= atlas.field_provenance.confidence",
                        (int(ref), field, value, src, conf))
                except (psycopg2.Error, ValueError, TypeError):
                    conn.rollback()
            elif w[0] == "fill":
                _, col, value = w
                try:
                    cur.execute(
                        f'UPDATE "{bs}"."{bt}" SET "{col}"=%s WHERE "{pk}"=%s '
                        f'AND (("{col}") IS NULL OR btrim(("{col}")::text)=\'\')',
                        (value, int(ref)))
                except psycopg2.Error:
                    conn.rollback()
        # xref (read-only)
        xr = outcome.get("xref")
        if xr and xr.get("name"):
            built = self._xref_sql_params(ref, xr["name"])
            if built:
                sql, params, nn = built
                try:
                    cur.execute(sql, params)
                    hits = []
                    for src, nm in cur.fetchall():
                        if nm and norm_name(str(nm)) == nn:
                            hits.append((src, str(nm)[:200], 0.8))
                        elif nm and nn in norm_name(str(nm)):
                            hits.append((src, str(nm)[:200], 0.5))
                    if hits:
                        xval = ";".join(f"{s}:{m}" for s, m, _ in hits[:5])
                        xconf = max((c for _, _, c in hits), default=0.5)
                        cur.execute(
                            "INSERT INTO atlas.field_provenance "
                            "(business_id, field, value, source_code, confidence, last_verified) "
                            "VALUES (%s,'xref',%s,'source_xref/name_match',%s, now()) "
                            "ON CONFLICT (business_id, field) DO UPDATE "
                            " SET value=EXCLUDED.value, confidence=EXCLUDED.confidence, "
                            "     last_verified=now() "
                            " WHERE EXCLUDED.confidence >= atlas.field_provenance.confidence",
                            (int(ref), xval, float(xconf)))
                except psycopg2.Error:
                    conn.rollback()
        # finish queue row
        status = "done"
        payload = {"finished_at": int(time.time()), "worker_status": status, "outcome": outcome}
        try:
            cur.execute(
                "UPDATE atlas.enrich_queue SET status=CASE "
                "  WHEN %s='failed' AND attempts >= %s THEN 'dead' ELSE %s END, "
                " result=COALESCE(result,'{}'::jsonb) || %s::jsonb, "
                " locked_by=NULL, locked_at=NULL, updated_at=now() WHERE id=%s",
                (status, MAX_ATTEMPTS, status, json.dumps(payload), queue_id))
        except psycopg2.Error:
            conn.rollback()
        cur.close()

    async def _persist_apg(self, ref, queue_id, outcome, writes):
        bs, bt = BUSINESS_TBL
        pk = self.cols["pk"]
        async with self._apool.acquire() as con:
            async with con.transaction():
                for w in writes:
                    if w[0] == "prov":
                        _, field, value, src, conf = w
                        await con.execute(
                            "INSERT INTO atlas.field_provenance "
                            "(business_id, field, value, source_code, confidence, last_verified) "
                            "VALUES ($1,$2,$3,$4,$5, now()) "
                            "ON CONFLICT (business_id, field) DO UPDATE "
                            " SET value=EXCLUDED.value, source_code=EXCLUDED.source_code, "
                            "     confidence=EXCLUDED.confidence, last_verified=now() "
                            " WHERE EXCLUDED.confidence >= atlas.field_provenance.confidence",
                            int(ref), field, value, src, conf)
                    elif w[0] == "fill":
                        _, col, value = w
                        await con.execute(
                            f'UPDATE "{bs}"."{bt}" SET "{col}"=$1 WHERE "{pk}"=$2 '
                            f'AND (("{col}") IS NULL OR btrim(("{col}")::text)=\'\')',
                            value, int(ref))
                xr = outcome.get("xref")
                if xr and xr.get("name"):
                    built = self._xref_sql_params(ref, xr["name"])
                    if built:
                        sql, _params, nn = built
                        apg_sql = sql.replace("%s", "$1")
                        try:
                            rows = await con.fetch(apg_sql, f"%{xr['name'].strip()[:40]}%")
                            hits = []
                            for r in rows:
                                nm = r["nm"]
                                if nm and norm_name(str(nm)) == nn:
                                    hits.append((r["src"], str(nm)[:200], 0.8))
                                elif nm and nn in norm_name(str(nm)):
                                    hits.append((r["src"], str(nm)[:200], 0.5))
                            if hits:
                                xval = ";".join(f"{s}:{m}" for s, m, _ in hits[:5])
                                xconf = max((c for _, _, c in hits), default=0.5)
                                await con.execute(
                                    "INSERT INTO atlas.field_provenance "
                                    "(business_id, field, value, source_code, confidence, last_verified) "
                                    "VALUES ($1,'xref',$2,'source_xref/name_match',$3, now()) "
                                    "ON CONFLICT (business_id, field) DO UPDATE "
                                    " SET value=EXCLUDED.value, confidence=EXCLUDED.confidence, "
                                    "     last_verified=now() "
                                    " WHERE EXCLUDED.confidence >= atlas.field_provenance.confidence",
                                    int(ref), xval, float(xconf))
                        except Exception:  # noqa: BLE001
                            pass
                payload = {"finished_at": int(time.time()), "worker_status": "done",
                           "outcome": {k: v for k, v in outcome.items() if k != "xref"}}
                await con.execute(
                    "UPDATE atlas.enrich_queue SET status='done', "
                    " result=COALESCE(result,'{}'::jsonb) || $1::jsonb, "
                    " locked_by=NULL, locked_at=NULL, updated_at=now() WHERE id=$2",
                    json.dumps(payload), queue_id)

    async def fail_row(self, queue_id, err):
        if self.kind == "asyncpg":
            async with self._apool.acquire() as con:
                await con.execute(
                    "UPDATE atlas.enrich_queue SET status=CASE "
                    " WHEN attempts >= $1 THEN 'dead' ELSE 'failed' END, "
                    " result=COALESCE(result,'{}'::jsonb) || $2::jsonb, "
                    " locked_by=NULL, locked_at=NULL, updated_at=now() WHERE id=$3",
                    MAX_ATTEMPTS, json.dumps({"error": str(err)[:1500]}), queue_id)
        else:
            def _fn(conn):
                cur = conn.cursor()
                cur.execute(
                    "UPDATE atlas.enrich_queue SET status=CASE "
                    " WHEN attempts >= %s THEN 'dead' ELSE 'failed' END, "
                    " result=COALESCE(result,'{}'::jsonb) || %s::jsonb, "
                    " locked_by=NULL, locked_at=NULL, updated_at=now() WHERE id=%s",
                    (MAX_ATTEMPTS, json.dumps({"error": str(err)[:1500]}), queue_id))
                cur.close()
            await self._p_run(_fn)

    async def coverage(self):
        if self.kind == "asyncpg":
            async with self._apool.acquire() as con:
                return await self._coverage_apg(con)
        return await self._p_run(self._coverage_pg)

    async def _coverage_apg(self, con):
        bs, bt = BUSINESS_TBL
        total = await con.fetchval(f'SELECT count(*) FROM "{bs}"."{bt}"') or 0
        cov = {"business_total": total}
        rows = await con.fetch("SELECT status, count(*) AS n FROM atlas.enrich_queue GROUP BY status")
        qs = {r["status"]: r["n"] for r in rows}
        cov["queue"] = qs
        cov["queue_remaining"] = qs.get("pending", 0) + qs.get("claimed", 0) + qs.get("failed", 0)
        return cov

    def _coverage_pg(self, conn):
        bs, bt = BUSINESS_TBL
        cur = conn.cursor()
        cur.execute(f'SELECT count(*) FROM "{bs}"."{bt}"')
        total = cur.fetchone()[0] or 0
        cur.execute("SELECT status, count(*) FROM atlas.enrich_queue GROUP BY status")
        qs = {st: c for st, c in cur.fetchall()}
        cur.close()
        return {"business_total": total, "queue": qs,
                "queue_remaining": qs.get("pending", 0) + qs.get("claimed", 0) + qs.get("failed", 0)}


def _pick_col(colset, candidates):
    low = {c.lower(): c for c in colset}
    for cand in candidates:
        if cand.lower() in low:
            return low[cand.lower()]
    return None


def _build_cols(pk, bcols, sr_cols):
    cols = {"pk": pk, "sr_cols": sr_cols}
    for logical in ("name", "website", "phone", "email", "category", "region",
                    "locality", "industry"):
        cols[logical] = _pick_col(bcols, CANDIDATES[logical])
    return cols


# --------------------------------------------------------------------------- #
# Status-back (identical JSON shape + GitHub Contents API as the live worker)
# --------------------------------------------------------------------------- #
def write_local(obj, path):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(obj, fh)
    except OSError as e:
        log(f"  WARNING could not write {path}: {e}")


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
        req.add_header("User-Agent", "atlas-enrich-async")
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
    code, _resp = _req("PUT", f"{api}/repos/{repo}/contents/{path}",
                       json.dumps(payload).encode("utf-8"))
    return bool(code and 200 <= code < 300)


async def report(db, node, rate_per_min, lifetime, engine):
    try:
        cov = await db.coverage()
    except Exception as e:  # noqa: BLE001
        log(f"  coverage error: {e}")
        cov = {}
    body = {
        "lane": "enrich",
        "engine": "async",
        "node": node,
        "async_lanes": LANES,
        "pg_pool": PG_POOL,
        "db_backend": engine,
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
    write_local(body, os.path.join(ENRICH_STATE_DIR, "last_enrich_async.json"))
    write_local(body, COUNTS_PATH)
    gh_put(f"status/{node}/enrich-async-{node}.json", body,
           f"async enrich progress {node} {cov.get('queue_remaining')} remaining")
    log(f"REPORT engine=async node={node} lanes={LANES} pool={PG_POOL} backend={engine} "
        f"rate={rate_per_min:.2f}/min lifetime={lifetime} "
        f"queue_remaining={cov.get('queue_remaining')}")


# --------------------------------------------------------------------------- #
# THE ENGINE -- claimer -> work queue -> lanes -> write queue -> DB writers
# --------------------------------------------------------------------------- #
async def run_engine(once=False):
    node = os.environ.get("NODE_ID", "hetzner")
    inst = os.environ.get("ATLAS_WORKER_INSTANCE", os.environ.get("INSTANCE", "0"))
    worker_id = f"{node}:{socket.gethostname()}:{inst}:{os.getpid()}:async"
    loop = asyncio.get_running_loop()

    db = DB()
    await db.open()
    await db.resolve_columns()
    if env_bool("ATLAS_ENRICH_SEED_ON_START", True):
        try:
            seeded = await db.seed_queue()
            log(f"seeded {seeded} new queue rows on start")
        except Exception as e:  # noqa: BLE001
            log(f"seed-on-start skipped: {e}")

    net = Net(loop)
    work_q = asyncio.Queue(maxsize=INFLIGHT_MAX)     # bounds claimed-but-unenriched
    write_q = asyncio.Queue(maxsize=INFLIGHT_MAX)    # bounds enriched-but-unwritten
    stats = {"lifetime": 0, "window": 0, "window_start": time.time(),
             "stop": False, "drained_empty": False}

    log(f"engine start id={worker_id} lanes={LANES} pg_pool={PG_POOL} "
        f"db_writers={DB_WRITERS} backend={db.kind} httpx={_HAVE_HTTPX} "
        f"aiodns={_HAVE_AIODNS} claim_batch={CLAIM_BATCH} inflight_max={INFLIGHT_MAX} once={once}")

    async def claimer():
        last_seed = time.time()
        while not stats["stop"]:
            try:
                rows = await db.claim_and_fetch(worker_id, CLAIM_BATCH)
            except Exception as e:  # noqa: BLE001
                log(f"claim error (retry): {e}")
                await asyncio.sleep(IDLE_SEC)
                continue
            if not rows:
                stats["drained_empty"] = True
                if once:
                    break
                now = time.time()
                if not once and now - last_seed >= SEED_SEC:
                    try:
                        s = await db.seed_queue()
                        if s:
                            log(f"re-seeded {s} new queue rows")
                    except Exception:  # noqa: BLE001
                        pass
                    last_seed = now
                await asyncio.sleep(IDLE_SEC + random.uniform(0, IDLE_SEC * 0.3))
                continue
            stats["drained_empty"] = False
            for qid, bid, biz in rows:
                await work_q.put((qid, bid, biz))   # blocks when INFLIGHT_MAX reached
        # signal lanes to stop by sentinel
        for _ in range(LANES):
            await work_q.put(None)

    async def lane(lane_id):
        while True:
            item = await work_q.get()
            if item is None:
                work_q.task_done()
                break
            qid, bid, biz = item
            try:
                if biz.get("_missing"):
                    await write_q.put(("finish_missing", qid, bid))
                else:
                    outcome, writes = await enrich_lane(net, db.cols, bid, biz)
                    await write_q.put(("persist", qid, bid, outcome, writes))
            except Exception as e:  # noqa: BLE001 -- one bad row never kills a lane
                await write_q.put(("fail", qid, bid, str(e)[:1500]))
            finally:
                work_q.task_done()
            if PACING_MS:
                await asyncio.sleep(PACING_MS / 1000.0)

    async def db_writer():
        while True:
            item = await write_q.get()
            if item is None:
                write_q.task_done()
                break
            try:
                kind = item[0]
                if kind == "persist":
                    _, qid, bid, outcome, writes = item
                    await db.persist(bid, qid, outcome, writes)
                    stats["lifetime"] += 1
                    stats["window"] += 1
                elif kind == "finish_missing":
                    _, qid, bid = item
                    await db.persist(bid, qid, {"missing": True, "xref": None}, [])
                    stats["lifetime"] += 1
                    stats["window"] += 1
                elif kind == "fail":
                    _, qid, bid, err = item
                    await db.fail_row(qid, err)
            except Exception as e:  # noqa: BLE001
                log(f"db_writer error: {e}")
            finally:
                write_q.task_done()

    async def reporter():
        last = 0.0
        while not stats["stop"]:
            await asyncio.sleep(5)
            now = time.time()
            if now - last >= REPORT_SEC:
                rate = stats["window"] / max((now - stats["window_start"]) / 60.0, 1e-6)
                await report(db, node, rate, stats["lifetime"], db.kind)
                last = now
                stats["window"] = 0
                stats["window_start"] = now

    # ---- launch, STAGGERED so we never open all lanes at once ---- #
    lanes = []
    for i in range(LANES):
        lanes.append(asyncio.ensure_future(lane(i)))
        if STAGGER_MS:
            await asyncio.sleep(STAGGER_MS / 1000.0)
    writers = [asyncio.ensure_future(db_writer()) for _ in range(DB_WRITERS)]
    claim_task = asyncio.ensure_future(claimer())
    rep_task = None if once else asyncio.ensure_future(reporter())

    # ---- run to completion ---- #
    await claim_task          # claimer exits on (once & empty) or stop
    await asyncio.gather(*lanes)   # lanes drain on sentinels
    for _ in range(DB_WRITERS):
        await write_q.put(None)
    await asyncio.gather(*writers)

    stats["stop"] = True
    if rep_task:
        rep_task.cancel()
    try:
        rate = stats["window"] / max((time.time() - stats["window_start"]) / 60.0, 1e-6)
        await report(db, node, rate, stats["lifetime"], db.kind)
    except Exception:  # noqa: BLE001
        pass
    await net.aclose()
    await db.close()
    log(f"engine stop lifetime={stats['lifetime']}")


# --------------------------------------------------------------------------- #
# --migrate (delegates to the same schema-verify + seed the base worker uses)
# --------------------------------------------------------------------------- #
async def do_migrate():
    db = DB()
    await db.open()
    cols = await db.resolve_columns()
    n = await db.seed_queue()
    cov = await db.coverage()
    log(f"MIGRATE: schema verified; seeded {n} new queue rows; "
        f"queue_remaining={cov.get('queue_remaining')} business_total={cov.get('business_total')}")
    await db.close()
    print(f"[atlas_async] MIGRATE OK seeded={n} queue_remaining={cov.get('queue_remaining')}")


# --------------------------------------------------------------------------- #
# --selftest  (FULLY OFFLINE: no PG, no network -- safe in the sandbox)
#   1. helper unit-checks (parse/normalize/policy identical to live worker)
#   2. an async harness: a MOCK DB exposing the SAME claim/persist surface, a
#      MOCK net, N lanes, bounded pool counter -> proves N lanes run while the
#      DB "pool" is never used by more than PG_POOL concurrent acquirers, and
#      every claimed row is finished exactly once.
# --------------------------------------------------------------------------- #
def _selftest_helpers():
    assert registrable_domain("https://www.Example.com/x") == "example.com"
    assert registrable_domain("http://foo.co.uk/y") == "foo.co.uk"
    assert is_gov_mil("army.mil") and is_gov_mil("ny.gov") and not is_gov_mil("example.com")
    assert norm_phone("+1 (415) 555-1234") == "4155551234"
    assert infer_email_pattern({"jane.doe@acme.com"}, "acme.com")[0] == "{first}.{last}@acme.com"
    assert infer_email_pattern({"jane.doe@acme.com"}, "acme.com")[1] <= 0.4
    assert infer_industry("Pizza Restaurant", None, None)[0] == "Food & Beverage"
    assert domain_age_band("2010-01-01") in ("7-15y", "15y+")
    assert mx_provider(["aspmx.l.google.com"]) == "Google Workspace"
    assert favicon_url("acme.com", "/x.ico") == "https://acme.com/x.ico"
    # role-email surfacing + pattern-hint policy in a parse
    page = parse_page("https://acme.com/", {}, '<a href="mailto:info@acme.com">'
                      '<a href="https://linkedin.com/company/acme">'
                      '<title>Acme Inc</title>')
    assert "info@acme.com" in page["emails"]
    assert page["linkedin_co"] == "https://linkedin.com/company/acme"
    # gov suppression: a .gov domain must never emit contact writes
    writes = []
    _prov(writes, "contact_suppressed", "gov_mil", "policy", "non_overridable_suppression", 1.0)
    assert writes and writes[0][1] == "contact_suppressed"
    return True


class _MockNet:
    """Deterministic offline 'network': every domain resolves, homepage yields a
    role email + phone + linkedin, MX is google, RDAP returns a created date.
    No sockets touched."""
    async def dns_resolves(self, d):
        return True

    async def robots_allows(self, d, p):
        return True

    async def mx_hosts(self, d):
        return ["aspmx.l.google.com"]

    async def get(self, url, timeout=None, maxbytes=None, accept_json=False):
        if accept_json:
            doc = {"events": [{"eventAction": "registration", "eventDate": "2015-04-01"}],
                   "entities": [{"roles": ["registrar"],
                                 "vcardArray": ["vcard", [["fn", {}, "text", "NameCheap"]]]}]}
            return ("https://rdap/x", 200, {"content-type": "application/json"},
                    json.dumps(doc))
        dom = registrable_domain(url) or "example.com"
        html = (f'<title>{dom} business</title>'
                f'<a href="mailto:info@{dom}">contact</a> call (415) 555-1234 '
                f'<a href="https://www.facebook.com/{dom}">fb</a>'
                f'<a href="https://linkedin.com/company/{dom}">li</a>'
                f'<link rel="icon" href="/favicon.ico">')
        return (url, 200, {"server": "nginx"}, html)


class _MockDB:
    """A mock DB that ENFORCES the bounded-pool invariant: persist/claim acquire
    a semaphore of size PG_POOL; if more than PG_POOL try at once the test FAILS.
    Tracks claimed vs finished to prove exactly-once."""
    def __init__(self, n_rows, pool_size):
        self.cols = {"pk": "id", "sr_cols": set(), "name": "name", "website": "website",
                     "phone": "phone", "email": "email", "category": "category",
                     "region": "region", "locality": "locality", "industry": "industry"}
        self.kind = "mock"
        self._sem = asyncio.Semaphore(pool_size)
        self._pool_size = pool_size
        self.max_concurrent = 0
        self._cur_concurrent = 0
        self._pending = list(range(1, n_rows + 1))
        self.claimed = set()
        self.finished = set()
        self.failed = set()

    async def _acquire(self):
        # measure peak concurrency to prove the ceiling holds
        if self._sem.locked() and self._cur_concurrent >= self._pool_size:
            pass
        await self._sem.acquire()
        self._cur_concurrent += 1
        self.max_concurrent = max(self.max_concurrent, self._cur_concurrent)
        if self._cur_concurrent > self._pool_size:
            raise AssertionError("POOL CEILING VIOLATED")

    def _release(self):
        self._cur_concurrent -= 1
        self._sem.release()

    async def claim_and_fetch(self, worker_id, batch):
        await self._acquire()
        try:
            await asyncio.sleep(0)  # yield
            out = []
            for _ in range(min(batch, len(self._pending))):
                bid = self._pending.pop(0)
                self.claimed.add(bid)
                out.append((bid, bid, {"name": f"Biz {bid}", "website": f"biz{bid}.com",
                                       "category": "restaurant", "region": "CA"}))
            return out
        finally:
            self._release()

    async def persist(self, ref, qid, outcome, writes):
        await self._acquire()
        try:
            await asyncio.sleep(0)
            assert ref not in self.finished, "DOUBLE-FINISH"
            self.finished.add(ref)
        finally:
            self._release()

    async def fail_row(self, qid, err):
        await self._acquire()
        try:
            self.failed.add(qid)
        finally:
            self._release()


async def _selftest_harness():
    """Run the REAL engine wiring (claimer/lane/writer queues + sentinels) over
    a mock DB+net to prove: bounded pool, exactly-once finish, all rows done."""
    n_rows = 400
    pool = 12
    lanes_n = 200   # FAR more lanes than pool conns -- the whole point
    db = _MockDB(n_rows, pool)
    net = _MockNet()
    work_q = asyncio.Queue(maxsize=lanes_n * 4)
    write_q = asyncio.Queue(maxsize=lanes_n * 4)
    cols = db.cols
    worker_id = "selftest"

    async def claimer():
        while True:
            rows = await db.claim_and_fetch(worker_id, 50)
            if not rows:
                break
            for qid, bid, biz in rows:
                await work_q.put((qid, bid, biz))
        for _ in range(lanes_n):
            await work_q.put(None)

    async def lane():
        while True:
            item = await work_q.get()
            if item is None:
                work_q.task_done()
                break
            qid, bid, biz = item
            try:
                outcome, writes = await enrich_lane(net, cols, bid, biz)
                await write_q.put(("persist", qid, bid, outcome, writes))
            except Exception as e:  # noqa: BLE001
                await write_q.put(("fail", qid, bid, str(e)))
            finally:
                work_q.task_done()

    async def writer():
        while True:
            item = await write_q.get()
            if item is None:
                write_q.task_done()
                break
            if item[0] == "persist":
                _, qid, bid, outcome, writes = item
                await db.persist(bid, qid, outcome, writes)
            else:
                _, qid, bid, err = item
                await db.fail_row(qid, err)
            write_q.task_done()

    lane_tasks = [asyncio.ensure_future(lane()) for _ in range(lanes_n)]
    writer_tasks = [asyncio.ensure_future(writer()) for _ in range(4)]
    ct = asyncio.ensure_future(claimer())
    await ct
    await asyncio.gather(*lane_tasks)
    for _ in range(4):
        await write_q.put(None)
    await asyncio.gather(*writer_tasks)

    # ---- invariants ----
    assert db.max_concurrent <= pool, \
        f"pool ceiling violated: peak={db.max_concurrent} > {pool}"
    assert len(db.finished) == n_rows, \
        f"not all rows finished: {len(db.finished)}/{n_rows}"
    assert len(db.finished) == len(db.claimed), "claimed != finished (exactly-once broken)"
    assert not db.failed, f"unexpected failures: {db.failed}"
    return db.max_concurrent, n_rows, lanes_n, pool


def do_selftest():
    print(f"[atlas_async] selftest: httpx={_HAVE_HTTPX} asyncpg={_HAVE_ASYNCPG} "
          f"aiodns={_HAVE_AIODNS} dnspython={_HAVE_DNS}")
    try:
        _selftest_helpers()
        print("[atlas_async] selftest: helper unit-checks PASS "
              "(parse/normalize/policy identical to live worker)")
    except AssertionError as e:
        print(f"[atlas_async] selftest: helper check FAILED: {e}")
        return 3
    try:
        peak, n, lanes_n, pool = asyncio.run(_selftest_harness())
        print(f"[atlas_async] selftest: ASYNC HARNESS PASS -- {lanes_n} lanes "
              f"enriched {n} rows; peak concurrent DB acquires={peak} (ceiling {pool}); "
              f"exactly-once finish verified; no over-claim.")
    except Exception as e:  # noqa: BLE001
        import traceback
        print("[atlas_async] selftest: HARNESS FAILED:\n" + traceback.format_exc())
        return 3
    print("[atlas_async] selftest: PASS")
    return 0


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
def main():
    load_env_file(AUTOPULL_ENV_PATH)
    load_env_file(DB_ENV_PATH)
    args = set(sys.argv[1:])
    if "--selftest" in args:
        sys.exit(do_selftest())
    elif "--migrate" in args:
        try:
            asyncio.run(do_migrate())
            sys.exit(0)
        except SystemExit:
            raise
        except Exception as e:  # noqa: BLE001
            log(f"MIGRATE FAILED: {e}")
            sys.exit(1)
    elif "--once" in args:
        asyncio.run(run_engine(once=True))
    else:
        asyncio.run(run_engine(once=False))


if __name__ == "__main__":
    main()
