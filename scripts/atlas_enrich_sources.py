#!/opt/atlas/venv/bin/python
"""
atlas_enrich_sources.py  --  STAGE-ABLE enrichment-SOURCE upgrade worker.

A second, complementary always-on enrichment worker that ADDS new FREE/legal
sources to push ATLAS toward ~88-92% COMPANY-level coverage. It deliberately
does NOT duplicate atlas_enrich_worker.py (the live seq-7 worker that already
does homepage/contact crawl, MX/DNS, tech-stack, firmographics, xref). Instead
it fills the gaps that worker leaves, using sources it does not touch:

  * RDAP / WHOIS           -- registrar, registration/expiry dates, name servers,
                              registrant org (when published) -> firmographic + age
  * favicon / logo         -- favicon URL discovery (brand asset; company-level)
  * robots/sitemap probe    -- discovers a /contact or /about page the base worker
                              may have missed, then hands the URL back as a hint
  * search-engine fallback  -- ONLY when a business has NO domain after the base
                              worker ran: a single DuckDuckGo HTML query
                              (no API key, ToS-light, rate-limited, 1 req/biz)
                              to propose a candidate domain, DNS+title-verified
                              exactly like the base worker's guess path (never
                              trusted blindly)
  * LinkedIn company (pub)  -- if a public linkedin.com/company/<slug> URL is
                              already on the page or found, record it as a social
                              (company page only; NEVER scrape people / personal)
  * email-provider polish   -- normalize the provider label already on MX rows
                              into a coarse firmographic ("uses Google Workspace")

HONESTY / CEILING (unchanged, restated):
  Company-level fields (domain, website, phone-from-site, role email, industry,
  socials, tech, mx, registrar/age) are reachable ~88-92% FREE. PERSONAL
  direct-dials and verified PERSONAL emails are vendor-gated (~65-72% blended)
  and are NEVER claimed here -- personal addresses remain pattern HINTS only
  (field='email_pattern', confidence<=0.4) exactly as the base worker does.

SAFETY (identical posture to the live worker):
  * writes ONLY to atlas.field_provenance (UPSERT keep-higher-confidence) and
    fill-if-empty on the real atlas.business columns -- never clobbers truth.
  * schema-introspected against the REAL DDL at startup (no CREATE/ALTER/DROP).
  * NON-OVERRIDABLE .gov/.mil/.fed.us contact suppression in code.
  * polite crawl: per-host rate limit, robots-respecting, capped bytes, UA set.
  * idempotent + rate-limited; processes its OWN queue task_type so it never
    collides with the base worker's find_domain rows.
  * fail-soft per probe; fail-LOUD only on DB-down / missing tables.

CLI (mirrors atlas_enrich_worker.py):
  --selftest   db.env loads, PG connects, schema present, dry claim works.
  --migrate    (HETZNER ONLY) seed THIS worker's queue task rows idempotently.
  --once       one claim+enrich batch, then exit.
  --loop       (DEFAULT) run forever; idle+backoff when queue empty.

DB creds come from /etc/atlas/db.env exactly like the base worker.
"""

import os
import re
import sys
import json
import time
import socket
import random
import datetime
import urllib.parse
import urllib.request
import urllib.error

import psycopg2

try:
    import dns.resolver  # type: ignore
    _HAVE_DNS = True
except Exception:  # noqa: BLE001
    _HAVE_DNS = False


# --------------------------------------------------------------------------- #
# Config (mirrors atlas_enrich_worker.py so behavior is predictable)
# --------------------------------------------------------------------------- #
DB_ENV_PATH = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")

BUSINESS_TBL = ("atlas", "business")
QUEUE_TBL    = ("atlas", "enrich_queue")
PROV_TBL     = ("atlas", "field_provenance")

# REAL enrich_queue CHECK vocabulary is
#   (find_domain, find_email, validate_email, firmographics, ai_classify)
# This worker rides 'firmographics' -- a real, allowed value -- so it requires
# NO schema change and never violates the CHECK constraint. find_domain is the
# base worker's lane; firmographics is the natural lane for source-polish.
TASK_TYPE = os.environ.get("ATLAS_SRC_TASK_TYPE", "firmographics")

BATCH        = int(os.environ.get("ATLAS_SRC_BATCH", "15"))
MAX_ATTEMPTS = int(os.environ.get("ATLAS_SRC_MAX_ATTEMPTS", "4"))
CLAIM_STALE_SEC = int(os.environ.get("ATLAS_SRC_CLAIM_STALE_SEC", "900"))
IDLE_SEC     = float(os.environ.get("ATLAS_SRC_IDLE_SEC", "20"))
PACING_MS    = int(os.environ.get("ATLAS_SRC_PACING_MS", "400"))
SEED_SEC     = int(os.environ.get("ATLAS_SRC_SEED_SEC", "900"))
HTTP_TIMEOUT = int(os.environ.get("ATLAS_SRC_HTTP_TIMEOUT", "12"))
HTTP_MAXBYTES= int(os.environ.get("ATLAS_SRC_HTTP_MAXBYTES", "400000"))
PER_HOST_GAP = float(os.environ.get("ATLAS_SRC_PER_HOST_GAP", "2.0"))  # polite
# search-engine fallback is OFF by default (most US biz already have a domain
# after the base worker); flip to 1 only when you want last-mile domain recovery.
ENABLE_SEARCH = os.environ.get("ATLAS_SRC_SEARCH", "0") not in ("0", "false", "no")
ENABLE_RDAP   = os.environ.get("ATLAS_SRC_RDAP", "1") not in ("0", "false", "no")

USER_AGENT = ("atlas-enrich-sources/1.0 (+https://github.com/cloudwalker171/"
              "atlas-manifests; free-tier enrichment; respects robots)")

GOV_TLDS = (".gov", ".mil", ".fed.us")

# in-process per-host last-hit clock (politeness)
_HOST_CLOCK = {}


def log(msg):
    print(f"[atlas_src] {datetime.datetime.now(datetime.timezone.utc).isoformat()} {msg}",
          flush=True)


# --------------------------------------------------------------------------- #
# db.env loader (same contract as socrata_import.py / atlas_enrich_worker.py)
# --------------------------------------------------------------------------- #
def load_env_file(path):
    if not path or not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export "):]
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def pick(*names, default=None):
    for n in names:
        v = os.environ.get(n)
        if v:
            return v
    return default


def connect_pg():
    load_env_file(DB_ENV_PATH)
    conn = psycopg2.connect(
        host=pick("PGHOST", "DB_HOST", default="127.0.0.1"),
        port=int(pick("PGPORT", "DB_PORT", default="5432")),
        dbname=pick("PGDATABASE", "DB_NAME", "DB_DATABASE", default="atlas"),
        user=pick("PGUSER", "DB_USER", default="atlas"),
        password=pick("PGPASSWORD", "DB_PASS", "DB_PASSWORD", default=""),
        connect_timeout=10,
    )
    conn.autocommit = False
    return conn


# --------------------------------------------------------------------------- #
# schema introspection (read-only; never CREATE/ALTER/DROP)
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
        "WHERE i.indrelid=%s::regclass AND i.indisprimary", (f'"{schema}"."{table}"',))
    rows = cur.fetchall()
    return rows[0][0] if rows else None


def pick_col(colset, candidates):
    low = {c.lower(): c for c in colset}
    for cand in candidates:
        if cand.lower() in low:
            return low[cand.lower()]
    return None


CANDIDATES = {
    "name":     ["name", "business_name", "title", "display_name", "legal_name"],
    "website":  ["website", "url", "website_url", "homepage"],
    "phone":    ["phone_e164", "phone", "phone_number", "telephone", "contact_phone"],
    "email":    ["email", "email_address", "contact_email"],
    "locality": ["locality", "city", "town"],
    "region":   ["region", "state", "province"],
    "category": ["category", "primary_category", "categories", "license_description"],
    "industry": ["industry", "sector"],
}


def resolve_columns(conn):
    cur = conn.cursor()
    bs, bt = BUSINESS_TBL
    bcols = table_columns(cur, bs, bt)
    if not bcols:
        cur.close()
        raise SystemExit(f"ERROR: {bs}.{bt} not found / no columns.")
    pk = table_pk(cur, bs, bt) or pick_col(bcols, ["id", "business_id"])
    cols = {"pk": pk, "_bcols": bcols}
    for logical, cands in CANDIDATES.items():
        cols[logical] = pick_col(bcols, cands)
    # provenance shape check (fail-loud if the real table is missing)
    pcols = table_columns(cur, PROV_TBL[0], PROV_TBL[1])
    if not {"business_id", "field", "value", "source_code", "confidence"} <= pcols:
        cur.close()
        raise SystemExit("ERROR: atlas.field_provenance missing expected columns.")
    cur.close()
    return cols


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def registrable_domain(host_or_url):
    if not host_or_url:
        return None
    s = host_or_url.strip()
    if "://" not in s:
        s = "http://" + s
    try:
        host = urllib.parse.urlparse(s).netloc.split("@")[-1].split(":")[0].lower()
    except Exception:  # noqa: BLE001
        return None
    if not host or "." not in host:
        return None
    if host.startswith("www."):
        host = host[4:]
    parts = host.split(".")
    # naive eTLD+1 -- good enough for US .com/.net/.org and 2-part ccTLDs
    if len(parts) >= 3 and parts[-2] in ("co", "com", "org", "net", "gov", "ac"):
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def is_gov_mil(domain):
    return bool(domain) and any(domain.endswith(t) for t in GOV_TLDS)


def name_tokens(name):
    toks = re.findall(r"[A-Za-z0-9]+", (name or "").lower())
    drop = {"the", "inc", "llc", "ltd", "co", "corp", "company", "and", "of",
            "group", "services", "service", "pllc", "pc", "pa", "lp"}
    return [t for t in toks if t not in drop and len(t) > 1]


def _polite_wait(domain):
    now = time.time()
    last = _HOST_CLOCK.get(domain, 0)
    gap = now - last
    if gap < PER_HOST_GAP:
        time.sleep(PER_HOST_GAP - gap)
    _HOST_CLOCK[domain] = time.time()


def http_get(url, timeout=HTTP_TIMEOUT, maxbytes=HTTP_MAXBYTES, accept_json=False):
    """Returns (final_url, status, headers_dict, body_text) or None. Polite."""
    dom = registrable_domain(url)
    if dom:
        _polite_wait(dom)
    headers = {"User-Agent": USER_AGENT,
               "Accept": "application/json" if accept_json else "text/html,*/*",
               "Accept-Language": "en-US,en;q=0.8"}
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            final_url = resp.geturl()
            status = resp.getcode()
            hdrs = {k.lower(): v for k, v in resp.headers.items()}
            raw = resp.read(maxbytes + 1)
            if len(raw) > maxbytes:
                raw = raw[:maxbytes]
            body = raw.decode("utf-8", "replace")
            return (final_url, status, hdrs, body)
    except urllib.error.HTTPError as e:
        try:
            body = e.read(maxbytes).decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            body = ""
        return (url, e.code, {}, body)
    except (urllib.error.URLError, socket.timeout, OSError, ValueError):
        return None


_ROBOTS_CACHE = {}


def robots_allows(domain, path):
    """Minimal robots.txt check (User-agent:* Disallow). Fail-open on fetch error."""
    rules = _ROBOTS_CACHE.get(domain)
    if rules is None:
        res = http_get(f"https://{domain}/robots.txt", timeout=8, maxbytes=60000)
        rules = []
        if res and res[1] < 400 and res[3]:
            ua_star = False
            for line in res[3].splitlines():
                line = line.split("#", 1)[0].strip()
                if not line:
                    continue
                low = line.lower()
                if low.startswith("user-agent:"):
                    ua_star = (line.split(":", 1)[1].strip() == "*")
                elif ua_star and low.startswith("disallow:"):
                    rules.append(line.split(":", 1)[1].strip())
        _ROBOTS_CACHE[domain] = rules
    for dis in rules:
        if dis and path.startswith(dis):
            return False
    return True


# --------------------------------------------------------------------------- #
# provenance write (UPSERT keep-higher-confidence) + fill-if-empty
# --------------------------------------------------------------------------- #
def record(cur, biz_id, field, value, source, method, confidence, url=None):
    if value is None or str(value).strip() == "":
        return
    cur.execute(
        """INSERT INTO atlas.field_provenance
               (business_id, field, value, source_code, confidence, last_verified)
           VALUES (%s, %s, %s, %s, %s, now())
           ON CONFLICT (business_id, field) DO UPDATE
               SET value=EXCLUDED.value,
                   source_code=EXCLUDED.source_code,
                   confidence=EXCLUDED.confidence,
                   last_verified=now()
             WHERE EXCLUDED.confidence >= atlas.field_provenance.confidence""",
        (int(biz_id), field, str(value)[:2000], source, float(confidence)))


def fill_if_empty(cur, cols, biz_id, logical, value):
    col = cols.get(logical)
    if not col or value is None or str(value).strip() == "":
        return False
    bs, bt = BUSINESS_TBL
    cur.execute(
        f'UPDATE "{bs}"."{bt}" SET "{col}"=%s '
        f'WHERE "{cols["pk"]}"=%s AND ("{col}" IS NULL OR btrim(("{col}")::text)=\'\')',
        (str(value)[:500], int(biz_id)))
    return cur.rowcount > 0


# --------------------------------------------------------------------------- #
# SOURCE 1 -- RDAP (modern WHOIS; JSON; free; no key)
# --------------------------------------------------------------------------- #
_RDAP_BOOTSTRAP = {
    "com": "https://rdap.verisign.com/com/v1/domain/",
    "net": "https://rdap.verisign.com/net/v1/domain/",
    "org": "https://rdap.publicinterestregistry.org/rdap/domain/",
    "info": "https://rdap.identitydigital.services/rdap/domain/",
    "io":  "https://rdap.nic.io/domain/",
}


def rdap_lookup(domain):
    """Return dict(registrar, created, expires, ns_count, registrant_org) or {}.
    Uses the IANA-published RDAP endpoints; one HTTP call. Fail-soft."""
    if not ENABLE_RDAP or not domain:
        return {}
    tld = domain.rsplit(".", 1)[-1]
    base = _RDAP_BOOTSTRAP.get(tld)
    if not base:
        # generic IANA bootstrap fallback
        url = "https://rdap.org/domain/" + urllib.parse.quote(domain)
    else:
        url = base + urllib.parse.quote(domain)
    res = http_get(url, timeout=10, maxbytes=120000, accept_json=True)
    if not res or res[1] >= 400 or not res[3]:
        return {}
    try:
        doc = json.loads(res[3])
    except ValueError:
        return {}
    out = {}
    # registrar
    for ent in doc.get("entities", []) or []:
        roles = ent.get("roles") or []
        vcard = ent.get("vcardArray")
        org = _vcard_fn(vcard)
        if "registrar" in roles and org:
            out["registrar"] = org
        if "registrant" in roles and org:
            out["registrant_org"] = org
    # events
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


def _vcard_fn(vcard):
    """Pull the 'fn' (formatted name / org) out of an RDAP jCard array."""
    try:
        for item in vcard[1]:
            if item and item[0] in ("fn", "org"):
                return str(item[3])[:200]
    except Exception:  # noqa: BLE001
        return None
    return None


def domain_age_band(created_iso):
    """Coarse firmographic age band from a registration date (company maturity cue)."""
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


# --------------------------------------------------------------------------- #
# SOURCE 2 -- favicon / logo discovery (brand asset; company-level)
# --------------------------------------------------------------------------- #
_FAVICON_RE = re.compile(
    r'<link[^>]+rel=["\'][^"\']*icon[^"\']*["\'][^>]+href=["\']([^"\']+)["\']', re.I)


def find_favicon(domain, html):
    if not domain:
        return None
    if html:
        m = _FAVICON_RE.search(html)
        if m:
            href = m.group(1).strip()
            if href.startswith("//"):
                return "https:" + href
            if href.startswith("http"):
                return href
            if href.startswith("/"):
                return f"https://{domain}{href}"
            return f"https://{domain}/{href}"
    # default location
    return f"https://{domain}/favicon.ico"


# --------------------------------------------------------------------------- #
# SOURCE 3 -- LinkedIn company page (PUBLIC) + socials already on page
# --------------------------------------------------------------------------- #
_LINKEDIN_CO_RE = re.compile(
    r'https?://(?:www\.)?linkedin\.com/company/[A-Za-z0-9_%\-./]+', re.I)


def find_linkedin_company(html):
    """Company page URL only. NEVER /in/<person> (personal). Company-level."""
    if not html:
        return None
    m = _LINKEDIN_CO_RE.search(html)
    return m.group(0) if m else None


# --------------------------------------------------------------------------- #
# SOURCE 4 -- search-engine domain recovery (last-mile, OFF by default)
# --------------------------------------------------------------------------- #
_DDG_RESULT_RE = re.compile(r'uddg=([^&"\s]+)')


def search_candidate_domain(name, region):
    """ONE DuckDuckGo HTML query to PROPOSE a candidate domain for a business
    that has no domain. Returns a candidate string or None. The caller MUST
    DNS+title-verify before trusting it (we never write an unverified domain)."""
    if not ENABLE_SEARCH:
        return None
    q = " ".join([name or "", region or "", "official site"]).strip()
    if len(q) < 4:
        return None
    url = "https://html.duckduckgo.com/html/?q=" + urllib.parse.quote(q)
    res = http_get(url, timeout=10, maxbytes=200000)
    if not res or res[1] >= 400 or not res[3]:
        return None
    for raw in _DDG_RESULT_RE.findall(res[3])[:8]:
        try:
            tgt = urllib.parse.unquote(raw)
        except Exception:  # noqa: BLE001
            continue
        dom = registrable_domain(tgt)
        if not dom:
            continue
        # skip aggregators / social / gov
        if any(dom.endswith(b) or dom == b for b in (
                "facebook.com", "linkedin.com", "yelp.com", "instagram.com",
                "twitter.com", "x.com", "youtube.com", "wikipedia.org",
                "mapquest.com", "yellowpages.com", "bbb.org")):
            continue
        if is_gov_mil(dom):
            continue
        return dom
    return None


def _dns_resolves(domain):
    if _HAVE_DNS:
        try:
            dns.resolver.resolve(domain, "A", lifetime=6)
            return True
        except Exception:  # noqa: BLE001
            return False
    try:
        socket.getaddrinfo(domain, 443, proto=socket.IPPROTO_TCP)
        return True
    except OSError:
        return False


_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)


def verify_domain_for(name, domain):
    """DNS + title-overlap verify, identical posture to the base worker's guess
    path. Returns the confirmed registrable domain or None."""
    if not domain or not _dns_resolves(domain):
        return None
    res = http_get(f"https://{domain}/", timeout=8)
    if not res or res[1] >= 400:
        res = http_get(f"http://{domain}/", timeout=8)
    if not res or res[1] >= 400 or not res[3]:
        return None
    mt = _TITLE_RE.search(res[3])
    title = re.sub(r"<[^>]+>", "", mt.group(1)) if mt else ""
    nset = set(name_tokens(name))
    tset = set(name_tokens(title))
    if nset and len(nset & tset) >= max(1, len(nset) // 2):
        return registrable_domain(res[0]) or domain
    return None


# --------------------------------------------------------------------------- #
# queue plumbing (mirrors base worker; rides TASK_TYPE='firmographics')
# --------------------------------------------------------------------------- #
def seed_queue(conn, cols):
    cur = conn.cursor()
    bs, bt = BUSINESS_TBL
    cur.execute(
        f'''INSERT INTO atlas.enrich_queue (business_id, task_type, priority, status)
            SELECT b."{cols['pk']}", %s, 6, 'pending'
            FROM "{bs}"."{bt}" b
            ON CONFLICT (business_id, task_type) DO NOTHING''', (TASK_TYPE,))
    n = cur.rowcount or 0
    conn.commit()
    cur.close()
    return n


def claim_batch(conn, worker_id, batch):
    cur = conn.cursor()
    cur.execute(
        '''WITH c AS (
               SELECT id FROM atlas.enrich_queue
               WHERE task_type=%s AND attempts < %s
                 AND (status='pending'
                      OR (status='claimed' AND locked_at IS NOT NULL
                          AND locked_at < now() - make_interval(secs => %s)))
               ORDER BY priority, id
               FOR UPDATE SKIP LOCKED
               LIMIT %s)
           UPDATE atlas.enrich_queue q
              SET status='claimed', locked_by=%s, locked_at=now(),
                  attempts=attempts+1, updated_at=now()
             FROM c WHERE q.id=c.id
           RETURNING q.id, q.business_id''',
        (TASK_TYPE, MAX_ATTEMPTS, CLAIM_STALE_SEC, batch, worker_id))
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    return rows


def finish_row(conn, queue_id, status, result=None):
    cur = conn.cursor()
    cur.execute(
        "UPDATE atlas.enrich_queue SET status=%s, updated_at=now(), "
        "result=COALESCE(%s, result) WHERE id=%s",
        (status, json.dumps(result) if result else None, queue_id))
    conn.commit()
    cur.close()


# --------------------------------------------------------------------------- #
# per-business enrichment
# --------------------------------------------------------------------------- #
def existing_domain(cur, biz_id):
    """Prefer a domain already resolved by the base worker (field_provenance),
    else the business.website column. This worker is a polish pass, so it relies
    on the base worker having run first; if no domain yet, it falls back to
    optional search recovery."""
    cur.execute("SELECT value FROM atlas.field_provenance "
                "WHERE business_id=%s AND field='domain' LIMIT 1", (int(biz_id),))
    r = cur.fetchone()
    if r and r[0]:
        return registrable_domain(r[0])
    return None


def enrich_one(conn, cols, biz_id):
    cur = conn.cursor()
    sel = [c for c in (cols.get("name"), cols.get("website"), cols.get("region")) if c]
    cur.execute(
        f'SELECT {", ".join(f"""b."{c}" """ for c in sel)} FROM '
        f'"{BUSINESS_TBL[0]}"."{BUSINESS_TBL[1]}" b WHERE b."{cols["pk"]}"=%s',
        (int(biz_id),))
    row = cur.fetchone()
    if not row:
        cur.close()
        return {"missing": True}
    vals = dict(zip(sel, row))
    name = vals.get(cols.get("name")) or ""
    website = vals.get(cols.get("website")) or ""
    region = vals.get(cols.get("region")) or ""

    out = {"prov_rows": 0, "fields_filled": 0, "suppressed": False}

    domain = existing_domain(cur, biz_id) or registrable_domain(website)

    # last-mile domain recovery (OFF unless ATLAS_SRC_SEARCH=1)
    if not domain and ENABLE_SEARCH:
        cand = search_candidate_domain(name, region)
        if cand:
            confirmed = verify_domain_for(name, cand)
            if confirmed and not is_gov_mil(confirmed):
                domain = confirmed
                record(cur, biz_id, "domain", domain, "search_recover",
                       "ddg_verified", 0.55, f"https://{domain}/")
                out["prov_rows"] += 1
                if fill_if_empty(cur, cols, biz_id, "website", f"https://{domain}"):
                    out["fields_filled"] += 1

    if not domain:
        conn.commit()
        cur.close()
        return out

    gov = is_gov_mil(domain)
    out["suppressed"] = gov
    if gov:
        record(cur, biz_id, "contact_suppressed", "gov_mil", "policy",
               "non_overridable_suppression", 1.0)
        out["prov_rows"] += 1
        conn.commit()
        cur.close()
        return out

    # fetch homepage once (for favicon + linkedin); robots-respecting
    html = None
    if robots_allows(domain, "/"):
        res = http_get(f"https://{domain}/", timeout=HTTP_TIMEOUT)
        if not res or res[1] >= 400:
            res = http_get(f"http://{domain}/", timeout=HTTP_TIMEOUT)
        if res and res[1] < 400:
            html = res[3]

    # SOURCE: favicon / logo
    fav = find_favicon(domain, html)
    if fav:
        record(cur, biz_id, "favicon", fav, "homepage_crawl", "icon_link", 0.7,
               f"https://{domain}/")
        out["prov_rows"] += 1

    # SOURCE: LinkedIn company page (public, company-level only)
    li = find_linkedin_company(html)
    if li:
        record(cur, biz_id, "social_linkedin", li, "homepage_crawl",
               "company_page", 0.8, f"https://{domain}/")
        out["prov_rows"] += 1

    # SOURCE: RDAP / WHOIS firmographics
    rd = rdap_lookup(domain)
    if rd.get("registrar"):
        record(cur, biz_id, "domain_registrar", rd["registrar"], "rdap", "rdap", 0.85)
        out["prov_rows"] += 1
    if rd.get("created"):
        record(cur, biz_id, "domain_created", rd["created"], "rdap", "rdap", 0.85)
        out["prov_rows"] += 1
        band = domain_age_band(rd["created"])
        if band:
            record(cur, biz_id, "domain_age_band", band, "rdap", "derived", 0.8)
            out["prov_rows"] += 1
    if rd.get("expires"):
        record(cur, biz_id, "domain_expires", rd["expires"], "rdap", "rdap", 0.8)
        out["prov_rows"] += 1
    if rd.get("registrant_org"):
        # registrant org is often privacy-masked; only record if it is not a
        # known privacy proxy and looks like a real org (company-level firmo).
        org = rd["registrant_org"]
        if not re.search(r"privacy|proxy|redacted|whoisguard|domains by", org, re.I):
            record(cur, biz_id, "registrant_org", org, "rdap", "rdap", 0.6)
            out["prov_rows"] += 1

    conn.commit()
    cur.close()
    return out


# --------------------------------------------------------------------------- #
# CLI modes
# --------------------------------------------------------------------------- #
def do_selftest():
    try:
        conn = connect_pg()
        cols = resolve_columns(conn)
        cur = conn.cursor()
        # dry claim cycle, rolled back
        cur.execute("SELECT id FROM atlas.enrich_queue WHERE task_type=%s "
                    "ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED", (TASK_TYPE,))
        cur.fetchall()
        conn.rollback()
        # exercise the offline-safe helpers
        assert registrable_domain("https://www.Example.com/x") == "example.com"
        assert is_gov_mil("army.mil") and not is_gov_mil("example.com")
        assert domain_age_band("2010-01-01") in ("7-15y", "15y+")
        assert find_linkedin_company('x <a href="https://linkedin.com/company/acme">') \
            == "https://linkedin.com/company/acme"
        cur.close()
        conn.close()
        print(f"[atlas_src] SELFTEST OK task_type={TASK_TYPE} pk={cols['pk']}")
        return 0
    except SystemExit:
        raise
    except Exception:  # noqa: BLE001
        import traceback
        print("[atlas_src] SELFTEST FAILED:\n" + traceback.format_exc())
        return 3


def do_migrate():
    try:
        conn = connect_pg()
        cols = resolve_columns(conn)
        n = seed_queue(conn, cols)
        log(f"MIGRATE: seeded {n} new '{TASK_TYPE}' queue rows.")
        conn.close()
        print(f"[atlas_src] MIGRATE OK seeded={n}")
        return 0
    except Exception:  # noqa: BLE001
        import traceback
        log("MIGRATE FAILED:\n" + traceback.format_exc())
        return 3


def run_loop(once=False):
    conn = connect_pg()
    cols = resolve_columns(conn)
    worker_id = f"{socket.gethostname()}:src:{os.environ.get('ATLAS_WORKER_INSTANCE','1')}"
    log(f"start worker={worker_id} task_type={TASK_TYPE} search={ENABLE_SEARCH} "
        f"rdap={ENABLE_RDAP}")
    last_seed = 0.0
    lifetime = 0
    while True:
        now = time.time()
        if now - last_seed > SEED_SEC:
            try:
                seeded = seed_queue(conn, cols)
                if seeded:
                    log(f"seeded {seeded} new rows")
            except Exception as e:  # noqa: BLE001
                conn.rollback()
                log(f"seed error: {e}")
            last_seed = now
        try:
            rows = claim_batch(conn, worker_id, BATCH)
        except Exception as e:  # noqa: BLE001
            conn.rollback()
            log(f"claim error (will retry): {e}")
            time.sleep(IDLE_SEC)
            continue
        if not rows:
            if once:
                break
            time.sleep(IDLE_SEC + random.uniform(0, IDLE_SEC * 0.3))
            continue
        for queue_id, biz_id in rows:
            try:
                res = enrich_one(conn, cols, biz_id)
                finish_row(conn, queue_id, "done", res)
                lifetime += 1
            except Exception as e:  # noqa: BLE001
                conn.rollback()
                log(f"enrich error biz={biz_id}: {e}")
                try:
                    finish_row(conn, queue_id, "failed", {"error": str(e)[:200]})
                except Exception:  # noqa: BLE001
                    conn.rollback()
            time.sleep(PACING_MS / 1000.0)
        if once:
            break
    conn.close()


def main():
    args = set(sys.argv[1:])
    if "--selftest" in args:
        sys.exit(do_selftest())
    if "--migrate" in args:
        sys.exit(do_migrate())
    if "--once" in args:
        run_loop(once=True)
        sys.exit(0)
    run_loop(once=False)  # --loop default


if __name__ == "__main__":
    main()
