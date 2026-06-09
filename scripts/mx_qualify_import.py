#!/opt/atlas/venv/bin/python
"""
mx_qualify_import.py  --  real-time "new MX records" QUALIFIER + ENRICHER for the
                          CT/SSL fresh-business lane (seq-12 ct_new_ssl).

WHY THIS IS A QUALIFIER, NOT A NEW STREAM (honest architecture note)
--------------------------------------------------------------------
There is NO public global firehose of "new MX records" the way Certificate
Transparency is a firehose of new certs. You cannot subscribe to "the internet's
new MX records." So the correct design is NOT a second always-on stream; it is an
ENRICHMENT / QUALIFIER step layered on the domains we ALREADY discover in real
time from the CT/SSL tailer (ct_new_ssl_import.py, seq-12):

    CT log (new TLS cert = new website live)  ->  atlas.business + source_record
                |
                v
    THIS script: for each freshly-discovered domain, async-resolve MX (+A,+NS),
    classify the email provider, set has_business_email, and write it back as
    provenance/companion rows.  MX presence both:
      (a) QUALIFIES the lead  -- a domain standing up MX is setting up business
          email = a real, freshly-operating business (filters parked/cert-only), and
      (b) ENRICHES the lead   -- which provider (Google Workspace / Microsoft 365 /
          Zoho / Proofpoint / self-hosted / ...), deliverability hints.

So the real product is the COMBINED real-time pipeline:
    CT(new cert)  ->  MX-resolve(new email infra)  ->  qualified, enriched lead.

FRESH-MX TIMING (the reason for the recheck loop)
-------------------------------------------------
MX records frequently appear HOURS AFTER the first cert (people get the site/cert
up first, wire up Google Workspace / M365 later). So "no MX at cert time" is NOT a
no -- it's a "not yet." This script is FAIL-SOFT: a domain with no MX is KEPT,
marked status='pending' in atlas.mx_qualify_state with a backoff next_check_at,
and re-resolved on later runs until MX appears or we hit the give-up horizon.

HONEST DEDUP WITH THE CT LANE (no double counting)
--------------------------------------------------
This script NEVER inserts a new atlas.business row. It only reads businesses the
CT lane already created (source_record.source='ct_new_ssl' / business ext_id
'ct_new_ssl:<domain>') and ENRICHES them in place:
  * atlas.field_provenance rows are written with source='mx_qualify' and distinct
    method tags, and field_provenance's UNIQUE(business_ref,field,value,source)
    makes re-runs idempotent and NON-colliding with the enrich worker's dns_mx rows.
  * a single companion atlas.source_record is written with source='mx_qualify',
    ext_id 'mx_qualify:<domain>' (idempotent via WHERE NOT EXISTS on source+ext_id),
    so it can never be confused with or counted as a ct_new_ssl discovery.
  * atlas.mx_qualify_state holds one row per business_ref (PK) -> dedup + recheck.

OVERLAP WITH atlas_enrich_worker.py (called out, not hidden)
------------------------------------------------------------
The always-on enrich worker ALSO does an MX probe as part of broad enrichment
(field_provenance source='dns_mx'). This script is the FAST, REAL-TIME, RECHECK-
DRIVEN path tied to the CT intake; the worker is the slow, broad backfill. They
coexist because their provenance source tags differ (mx_qualify vs dns_mx) and
both upsert idempotently. Recommendation (see DELIVER): let this qualifier own the
at-cert-time + recheck MX pass; the worker's MX probe then almost always finds the
value already present and is a cheap confirmation. Tune as you measure.

SUPPRESSION (NON-OVERRIDABLE): .gov / .mil / .fed.us domains are never resolved
for contact infra and never written as contactable -- a single status='suppressed'
state row is recorded instead. No env flag disables this. (Defense in depth; the
CT lane already drops these at ingestion.)

US-scope-aware. Rate-limited + cached. Verbose. FAIL-LOUD only on real breakage
(DB down, atlas.business/source_record missing). Idempotent re-runs that resolve 0
NEW domains DO NOT fail (that would wedge the every-few-minutes timer).

Self-tests (no DB / no network needed for the classifier test):
    --classify-selftest   unit-test the provider classifier against fixed vectors
    --selftest            db.env loads + Postgres connects + schema present/creatable
                          + classifier vectors; exit 0 ok / 3 broken
    --once / (default)    one qualify pass (new CT domains + due rechecks), then exit

DB creds + env loading + schema introspection are byte-for-byte the same pattern as
ct_new_ssl_import.py / atlas_enrich_worker.py (PG* / DB_* / ATLAS_DB_* picked).
"""

import os
import re
import sys
import json
import time
import socket
import datetime

import psycopg2

# dnspython is required for MX/NS; A degrades to socket. If dnspython is missing we
# do NOT crash -- every domain is kept as status='pending' (fail-soft) so a later
# run (after the manifest's pip install lands) resolves them.
try:
    import dns.resolver  # type: ignore
    import dns.exception  # type: ignore
    _HAVE_DNS = True
except Exception:  # noqa: BLE001
    _HAVE_DNS = False


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
DB_ENV_PATH       = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
AUTOPULL_ENV_PATH = os.environ.get("ATLAS_AUTOPULL_CONF", "/etc/atlas/autopull.env")

BUSINESS_TBL = ("atlas", "business")
SOURCE_TBL   = ("atlas", "source_record")
PROV_TBL     = ("atlas", "field_provenance")
STATE_TBL    = ("atlas", "mx_qualify_state")

SOURCE_NAME  = "mx_qualify"          # our companion source tag (distinct from ct_new_ssl)
CT_SOURCE    = os.environ.get("MXQ_CT_SOURCE", "ct_new_ssl")

REQ_SLEEP    = float(os.environ.get("MXQ_SLEEP", "0.4"))     # polite delay between domains
DNS_LIFETIME = float(os.environ.get("MXQ_DNS_LIFETIME", "6"))
DNS_TIMEOUT  = float(os.environ.get("MXQ_DNS_TIMEOUT", "4"))
BATCH_COMMIT = int(os.environ.get("MXQ_BATCH", "100"))

# backoff (hours) for domains with no MX yet: recheck at +1h,+3h,+6h,+12h,+24h,+48h
# then give up. MX usually appears within the first day if it's coming at all.
BACKOFF_HOURS = [float(x) for x in os.environ.get(
    "MXQ_BACKOFF_HOURS", "1,3,6,12,24,48").split(",") if x.strip()]
GIVEUP_AFTER  = int(os.environ.get("MXQ_GIVEUP_AFTER", str(len(BACKOFF_HOURS))))

STATE_DIR   = os.environ.get("ATLAS_AUTOPULL_STATE", "/var/lib/atlas/autopull")
COUNTS_PATH = os.environ.get("ATLAS_COUNTS_PATH", os.path.join(STATE_DIR, "last_counts.json"))
COUNTS_PATH_SRC = os.path.join(STATE_DIR, f"last_counts_{SOURCE_NAME}.json")

USER_AGENT = os.environ.get(
    "MXQ_UA", "atlas-mx-qualify/1.0 (Michael Thomas; michael.thomas.global@gmail.com)")

# NON-OVERRIDABLE suppression. No env flag can disable this.
_GOV_MIL_RE = re.compile(r"(\.gov|\.mil|\.fed\.us)$", re.I)

# Free webmail apexes -- a *company* domain whose MX points at one of these is NOT
# standing up its own business email (it's forwarding to consumer mail); flag it.
FREE_MAIL = {
    "gmail.com", "googlemail.com", "yahoo.com", "ymail.com", "hotmail.com",
    "outlook.com", "live.com", "msn.com", "aol.com", "icloud.com", "me.com",
    "mail.com", "gmx.com", "protonmail.com", "proton.me", "comcast.net",
    "att.net", "verizon.net", "sbcglobal.net", "bellsouth.net", "cox.net",
}

_LABEL_RE = re.compile(r"^[a-z0-9-]{1,63}$")


def log(msg):
    print(f"[mx_qualify] {datetime.datetime.now(datetime.timezone.utc).isoformat()} {msg}",
          flush=True)


def _cap():
    if len(sys.argv) > 1 and str(sys.argv[1]).strip() and not sys.argv[1].startswith("--"):
        try:
            return int(sys.argv[1])
        except ValueError:
            pass
    return int(os.environ.get("MXQ_CAP", "2000") or "2000")


# --------------------------------------------------------------------------- #
# Domain helpers (US-scope + suppression mirror ct_new_ssl_import.py)
# --------------------------------------------------------------------------- #
def registrable_domain(host_or_url):
    """Lightweight eTLD+1 (US-focused), tolerant of a full URL or a bare host."""
    h = (host_or_url or "").strip().lower()
    h = re.sub(r"^[a-z]+://", "", h)
    h = h.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0]
    h = h.split("@")[-1].split(":", 1)[0].rstrip(".")
    if h.startswith("*."):
        h = h[2:]
    if not h or "." not in h:
        return None
    labels = h.split(".")
    if any(not _LABEL_RE.match(l) for l in labels):
        return None
    if labels[-1] == "us" and len(labels) >= 3 and len(labels[-2]) <= 3:
        return ".".join(labels[-3:])
    return ".".join(labels[-2:])


def is_gov_mil(domain):
    return bool(domain and _GOV_MIL_RE.search(domain))


# --------------------------------------------------------------------------- #
# Email-provider classification (extends atlas_enrich_worker.mx_info)
# Returns a label given the sorted MX exchange hostnames for `domain`.
# --------------------------------------------------------------------------- #
def classify_provider(domain, mx_hosts):
    """Return (provider_label, has_business_email).

    has_business_email = there is mail infrastructure for this domain that is NOT a
    consumer-webmail forward. provider_label is the best-effort vendor or
    'Self-hosted' / 'Other' / 'Consumer-webmail (forwarded)' / None (no MX)."""
    if not mx_hosts:
        return (None, False)
    blob = " ".join(mx_hosts)

    # Vendor signatures (substring on MX exchange host). Order matters: most specific
    # / most common first. Extend freely as you observe new MX footprints.
    table = [
        ("Google Workspace", ("google.com", "googlemail.com", "aspmx.l.google",
                               "googlemail", "google")),
        ("Microsoft 365",    ("protection.outlook.com", "mail.protection.outlook",
                               "outlook.com", "office365")),
        ("Proofpoint",       ("pphosted.com", "ppe-hosted.com", "proofpoint")),
        ("Mimecast",         ("mimecast.com", "mimecast")),
        ("Zoho",             ("zoho.com", "zoho.eu", "zohomail")),
        ("GoDaddy / Secureserver", ("secureserver.net", "godaddy")),
        ("Fastmail",         ("messagingengine.com", "fastmail")),
        ("Amazon SES / WorkMail", ("amazonaws.com", "awsapps.com", "amazonses")),
        ("Cloudflare Email", ("mx.cloudflare.net",)),
        ("ProtonMail",       ("protonmail.ch", "proton.me", "protonmail")),
        ("Rackspace",        ("emailsrvr.com", "rackspace")),
        ("Barracuda",        ("barracudanetworks.com", "barracuda")),
        ("Yandex",           ("yandex.net", "yandex")),
        ("Yahoo / AOL Biz",  ("yahoodns.net", "yahoo.com")),
        ("Intermedia",       ("intermedia.net",)),
        ("OVH",              ("ovh.net", "ovh.com")),
        ("Namecheap PrivateEmail", ("privateemail.com", "jellyfish.systems")),
    ]
    for label, needles in table:
        if any(n in blob for n in needles):
            # webmail-forward special case: MX exchange registrable domain is a
            # free consumer mail host -> not a business mailbox setup.
            return (label, True)

    # consumer webmail forward (e.g. MX -> gmail.com directly)
    for h in mx_hosts:
        rd = registrable_domain(h)
        if rd in FREE_MAIL:
            return ("Consumer-webmail (forwarded)", False)

    # self-hosted: every MX exchange is within the domain's own registrable domain
    dom_rd = registrable_domain(domain)
    if dom_rd and all((registrable_domain(h) == dom_rd) for h in mx_hosts):
        return ("Self-hosted", True)

    return ("Other", True)


# --------------------------------------------------------------------------- #
# DNS resolution (fail-soft per record type)
# --------------------------------------------------------------------------- #
def _resolver():
    r = dns.resolver.Resolver()
    r.lifetime = DNS_LIFETIME
    r.timeout = DNS_TIMEOUT
    return r


def resolve_mx(domain):
    """Return (state, hosts) where state in {'mx','nomx','dnsfail'}.
    'nomx' = domain resolves but has no MX (-> pending recheck);
    'dnsfail' = could not query at all (transient; -> pending recheck)."""
    if not _HAVE_DNS:
        return ("dnsfail", [])
    try:
        ans = _resolver().resolve(domain, "MX")
        hosts = sorted({str(r.exchange).rstrip(".").lower() for r in ans if str(r.exchange).strip(".")})
        return ("mx", hosts) if hosts else ("nomx", [])
    except dns.resolver.NoAnswer:
        return ("nomx", [])
    except dns.resolver.NXDOMAIN:
        return ("nomx", [])
    except (dns.resolver.NoNameservers, dns.exception.Timeout):
        return ("dnsfail", [])
    except Exception:  # noqa: BLE001
        return ("dnsfail", [])


def resolve_a(domain):
    if _HAVE_DNS:
        try:
            ans = _resolver().resolve(domain, "A")
            return sorted({r.address for r in ans})
        except Exception:  # noqa: BLE001
            pass
    try:
        infos = socket.getaddrinfo(domain, 443, proto=socket.IPPROTO_TCP)
        return sorted({i[4][0] for i in infos if ":" not in i[4][0]})
    except OSError:
        return []


def resolve_ns(domain):
    if not _HAVE_DNS:
        return []
    try:
        ans = _resolver().resolve(domain, "NS")
        return sorted({str(r.target).rstrip(".").lower() for r in ans})
    except Exception:  # noqa: BLE001
        return []


# --------------------------------------------------------------------------- #
# env + connection + introspection (verbatim pattern from the other importers)
# --------------------------------------------------------------------------- #
def load_env_file(path):
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
        application_name="atlas_mx_qualify",
    )
    conn.autocommit = False
    return conn


def table_columns(cur, s, t):
    cur.execute("SELECT column_name FROM information_schema.columns "
                "WHERE table_schema=%s AND table_name=%s", (s, t))
    return {r[0] for r in cur.fetchall()}


def table_pk(cur, s, t):
    cur.execute("SELECT a.attname FROM pg_index i JOIN pg_attribute a "
                "ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
                "WHERE i.indrelid=%s::regclass AND i.indisprimary", (f"{s}.{t}",))
    rows = [r[0] for r in cur.fetchall()]
    return rows[0] if rows else None


def pick_col(colset, candidates):
    for c in candidates:
        if c in colset:
            return c
    return None


CANDIDATES = {
    "biz_website":  ["website", "url", "website_url", "homepage"],
    "biz_ext_id":   ["source_id", "external_id", "ext_id", "overture_id", "source_record_id"],
    "biz_email_provider": ["email_provider", "mail_provider"],   # filled only if such a col exists
    "sr_business_fk": ["business_id", "biz_id", "business"],
    "sr_source":    ["source", "data_source", "origin"],
    "sr_ext_id":    ["source_id", "source_record_id", "external_id", "record_id", "ext_id"],
    "sr_payload":   ["raw", "payload", "data", "raw_json", "raw_jsonb", "doc", "record"],
    "sr_created":   ["created_at", "inserted_at", "fetched_at", "created"],
}


def ensure_aux_schema(conn):
    """Create the companion tables IF NOT EXISTS. field_provenance matches the
    enrich worker's definition byte-for-byte so the two writers share one table."""
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS atlas")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS atlas.field_provenance (
            id           bigserial PRIMARY KEY,
            business_ref text        NOT NULL,
            field        text        NOT NULL,
            value        text,
            source       text,
            method       text,
            confidence   real,
            url          text,
            observed_at  timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT field_prov_uniq UNIQUE (business_ref, field, value, source)
        )""")
    cur.execute("""CREATE INDEX IF NOT EXISTS field_prov_ref
                   ON atlas.field_provenance (business_ref)""")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS atlas.mx_qualify_state (
            business_ref       text PRIMARY KEY,
            domain             text NOT NULL,
            status             text NOT NULL DEFAULT 'pending',  -- pending|resolved|giveup|suppressed
            has_business_email boolean,
            provider           text,
            mx_hosts           text,
            attempts           int  NOT NULL DEFAULT 0,
            first_seen         timestamptz NOT NULL DEFAULT now(),
            last_checked_at    timestamptz,
            next_check_at      timestamptz,
            resolved_at        timestamptz,
            updated_at         timestamptz NOT NULL DEFAULT now()
        )""")
    cur.execute("""CREATE INDEX IF NOT EXISTS mx_qualify_due
                   ON atlas.mx_qualify_state (next_check_at)
                   WHERE status='pending'""")
    conn.commit()
    cur.close()


def record_prov(cur, ref, field, value, method, confidence, url=None):
    """Idempotent provenance write (source fixed to mx_qualify)."""
    if value is None or value == "":
        return
    try:
        cur.execute("""
            INSERT INTO atlas.field_provenance
                (business_ref, field, value, source, method, confidence, url)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (business_ref, field, value, source) DO NOTHING
        """, (ref, field, str(value)[:2000], SOURCE_NAME, method, confidence, url))
    except psycopg2.Error:
        cur.connection.rollback()


def write_companion_source_record(cur, sr_cols, sr_keys, biz_id, domain, payload, now):
    """One idempotent atlas.source_record (source=mx_qualify, ext_id=mx_qualify:<domain>).
    Uses WHERE NOT EXISTS on (source, ext_id) -- never an UPDATE, never a new business."""
    ext_ns = f"{SOURCE_NAME}:{domain}"
    row = {}
    if sr_keys["fk"] and biz_id is not None:
        row[sr_keys["fk"]] = biz_id
    if sr_keys["src"]:
        row[sr_keys["src"]] = SOURCE_NAME
    if sr_keys["ext"]:
        row[sr_keys["ext"]] = ext_ns
    if sr_keys["pay"]:
        p = dict(payload); p["domain"] = domain; p["_atlas_source"] = SOURCE_NAME
        row[sr_keys["pay"]] = json.dumps(p)
    if sr_keys["cre"]:
        row[sr_keys["cre"]] = now
    if not row or not (sr_keys["src"] and sr_keys["ext"]):
        return 0
    cols = list(row.keys())
    vals = [row[c] for c in cols]
    col_list = ", ".join(f'"{c}"' for c in cols)
    ph = ", ".join(["%s"] * len(cols))
    ss, st_ = SOURCE_TBL
    sql = (f'INSERT INTO "{ss}"."{st_}" ({col_list}) SELECT {ph} '
           f'WHERE NOT EXISTS (SELECT 1 FROM "{ss}"."{st_}" '
           f'WHERE "{sr_keys["src"]}"=%s AND "{sr_keys["ext"]}"=%s)')
    try:
        cur.execute(sql, vals + [SOURCE_NAME, ext_ns])
        return cur.rowcount or 0
    except psycopg2.Error as e:
        cur.connection.rollback()
        log(f"source_record insert error {ext_ns}: {e.pgerror or e}")
        return 0


# --------------------------------------------------------------------------- #
# Work selection: NEW ct_new_ssl businesses not yet in state + due rechecks
# --------------------------------------------------------------------------- #
def fetch_new_ct_targets(cur, biz_keys, sr_keys, cap):
    """Yield (business_ref, domain) for ct_new_ssl businesses not yet qualified.
    Prefers the source_record FK join; falls back to business.ext_id LIKE."""
    bs, bt = BUSINESS_TBL
    ss, st_ = SOURCE_TBL
    out = []
    if sr_keys["fk"] and sr_keys["src"] and biz_keys["pk"]:
        cur.execute(
            f'SELECT DISTINCT b."{biz_keys["pk"]}"::text, '
            f'       COALESCE(b."{biz_keys["website"]}", \'\') '
            f'FROM "{ss}"."{st_}" sr '
            f'JOIN "{bs}"."{bt}" b ON b."{biz_keys["pk"]}" = sr."{sr_keys["fk"]}" '
            f'LEFT JOIN "{STATE_TBL[0]}"."{STATE_TBL[1]}" mq '
            f'       ON mq.business_ref = b."{biz_keys["pk"]}"::text '
            f'WHERE sr."{sr_keys["src"]}" = %s AND mq.business_ref IS NULL '
            f'LIMIT %s',
            (CT_SOURCE, cap))
        for ref, website in cur.fetchall():
            dom = registrable_domain(website) or _domain_from_ext(cur, biz_keys, ref)
            if dom:
                out.append((ref, dom))
        return out
    # fallback: ext_id namespace on the business row
    if biz_keys["pk"] and biz_keys["ext"]:
        cur.execute(
            f'SELECT b."{biz_keys["pk"]}"::text, b."{biz_keys["ext"]}", '
            f'       COALESCE(b."{biz_keys["website"]}", \'\') '
            f'FROM "{bs}"."{bt}" b '
            f'LEFT JOIN "{STATE_TBL[0]}"."{STATE_TBL[1]}" mq '
            f'       ON mq.business_ref = b."{biz_keys["pk"]}"::text '
            f'WHERE b."{biz_keys["ext"]}" LIKE %s AND mq.business_ref IS NULL '
            f'LIMIT %s',
            (f"{CT_SOURCE}:%", cap))
        for ref, ext, website in cur.fetchall():
            dom = registrable_domain(website)
            if not dom and ext and ":" in ext:
                dom = registrable_domain(ext.split(":", 1)[1])
            if dom:
                out.append((ref, dom))
    return out


def _domain_from_ext(cur, biz_keys, ref):
    if not biz_keys["ext"]:
        return None
    bs, bt = BUSINESS_TBL
    cur.execute(f'SELECT "{biz_keys["ext"]}" FROM "{bs}"."{bt}" '
                f'WHERE "{biz_keys["pk"]}"::text=%s LIMIT 1', (ref,))
    g = cur.fetchone()
    if g and g[0] and ":" in g[0]:
        return registrable_domain(g[0].split(":", 1)[1])
    return None


def fetch_due_rechecks(cur, cap):
    cur.execute(
        f'SELECT business_ref, domain, attempts FROM "{STATE_TBL[0]}"."{STATE_TBL[1]}" '
        f"WHERE status='pending' AND (next_check_at IS NULL OR next_check_at <= now()) "
        f"ORDER BY next_check_at NULLS FIRST LIMIT %s", (cap,))
    return cur.fetchall()


def upsert_state(cur, ref, domain, status, has_biz, provider, mx_hosts, attempts,
                 next_check_at, resolved_at, now):
    cur.execute(f"""
        INSERT INTO "{STATE_TBL[0]}"."{STATE_TBL[1]}"
            (business_ref, domain, status, has_business_email, provider, mx_hosts,
             attempts, first_seen, last_checked_at, next_check_at, resolved_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (business_ref) DO UPDATE SET
            status=EXCLUDED.status,
            has_business_email=EXCLUDED.has_business_email,
            provider=EXCLUDED.provider,
            mx_hosts=EXCLUDED.mx_hosts,
            attempts=EXCLUDED.attempts,
            last_checked_at=EXCLUDED.last_checked_at,
            next_check_at=EXCLUDED.next_check_at,
            resolved_at=COALESCE("{STATE_TBL[1]}".resolved_at, EXCLUDED.resolved_at),
            updated_at=EXCLUDED.updated_at
    """, (ref, domain, status, has_biz, provider, (",".join(mx_hosts[:6]) if mx_hosts else None),
          attempts, now, now, next_check_at, resolved_at, now))


def _next_check(attempts, now):
    idx = min(attempts, len(BACKOFF_HOURS) - 1)
    return now + datetime.timedelta(hours=BACKOFF_HOURS[idx])


# --------------------------------------------------------------------------- #
# Qualify one domain
# --------------------------------------------------------------------------- #
def qualify_one(cur, biz_keys, sr_keys, ref, domain, attempts_prev, now, stat):
    # NON-OVERRIDABLE suppression
    if is_gov_mil(domain):
        upsert_state(cur, ref, domain, "suppressed", None, None, [], attempts_prev,
                     None, None, now)
        stat["suppressed"] += 1
        return

    mx_state, hosts = resolve_mx(domain)
    a_recs  = resolve_a(domain)
    ns_recs = resolve_ns(domain)
    attempts = attempts_prev + 1

    if mx_state == "mx":
        provider, has_biz = classify_provider(domain, hosts)
        upsert_state(cur, ref, domain, "resolved", has_biz, provider, hosts, attempts,
                     None, now, now)
        # provenance (idempotent, source=mx_qualify)
        record_prov(cur, ref, "mx", ",".join(hosts[:6]), "dns_mx_realtime", 0.95)
        record_prov(cur, ref, "has_business_email", "true" if has_biz else "false",
                    "mx_presence", 0.9)
        if provider:
            record_prov(cur, ref, "email_provider", provider, "mx_host_signature", 0.85)
        if a_recs:
            record_prov(cur, ref, "a", ",".join(a_recs[:6]), "dns_a", 0.8)
        if ns_recs:
            record_prov(cur, ref, "ns", ",".join(ns_recs[:6]), "dns_ns", 0.8)
        # fill atlas.business.email_provider only if such a column actually exists
        if biz_keys["email_provider"] and provider:
            _fill_if_empty(cur, biz_keys, ref, biz_keys["email_provider"], provider)
        payload = {"mx": hosts, "email_provider": provider, "has_business_email": has_biz,
                   "a": a_recs, "ns": ns_recs, "lane": "ct_new_ssl->mx_qualify",
                   "resolved_at": now.isoformat(), "attempts": attempts}
        n = write_companion_source_record(cur, None, sr_keys, _biz_id_for(cur, biz_keys, ref),
                                          domain, payload, now)
        stat["resolved"] += 1
        stat["new_source_record"] += n
        stat["providers"][provider or "Other"] = stat["providers"].get(provider or "Other", 0) + 1
        if has_biz:
            stat["has_business_email"] += 1
    else:
        # no MX yet (nomx) OR transient dnsfail -> KEEP, pending recheck (fail-soft)
        if attempts >= GIVEUP_AFTER:
            upsert_state(cur, ref, domain, "giveup", False, None, [], attempts, None, None, now)
            record_prov(cur, ref, "has_business_email", "false", "mx_absent_giveup", 0.5)
            stat["giveup"] += 1
        else:
            upsert_state(cur, ref, domain, "pending", None, None, [], attempts,
                         _next_check(attempts, now), None, now)
            stat["pending"] += 1
        if a_recs:   # site is live even if mail isn't -- record it
            record_prov(cur, ref, "a", ",".join(a_recs[:6]), "dns_a", 0.8)
        stat["nomx" if mx_state == "nomx" else "dnsfail"] += 1


def _biz_id_for(cur, biz_keys, ref):
    """Recover the native-typed business PK value from its text ref for the FK write."""
    if not biz_keys["pk"]:
        return None
    bs, bt = BUSINESS_TBL
    try:
        cur.execute(f'SELECT "{biz_keys["pk"]}" FROM "{bs}"."{bt}" '
                    f'WHERE "{biz_keys["pk"]}"::text=%s LIMIT 1', (ref,))
        g = cur.fetchone()
        return g[0] if g else None
    except psycopg2.Error:
        cur.connection.rollback()
        return None


def _fill_if_empty(cur, biz_keys, ref, col, value):
    bs, bt = BUSINESS_TBL
    try:
        cur.execute(f'UPDATE "{bs}"."{bt}" SET "{col}"=%s '
                    f'WHERE "{biz_keys["pk"]}"::text=%s '
                    f'AND (("{col}") IS NULL OR btrim(("{col}")::text)=\'\')',
                    (value, ref))
    except psycopg2.Error:
        cur.connection.rollback()


def write_counts(counts):
    for path in (COUNTS_PATH_SRC,):   # never clobber the shared last_counts.json blindly
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(counts, fh)
            log(f"wrote counts -> {path}")
        except OSError as e:
            log(f"WARNING: could not write {path}: {e}")


# --------------------------------------------------------------------------- #
# Self-tests
# --------------------------------------------------------------------------- #
_CLASSIFY_VECTORS = [
    ("acme.com", ["aspmx.l.google.com", "alt1.aspmx.l.google.com"], "Google Workspace", True),
    ("acme.com", ["acme-com.mail.protection.outlook.com"], "Microsoft 365", True),
    ("acme.com", ["mx.zoho.com", "mx2.zoho.com"], "Zoho", True),
    ("acme.com", ["mx1.emailsrvr.com"], "Rackspace", True),
    ("acme.com", ["mxa-00000000.gslb.pphosted.com"], "Proofpoint", True),
    ("acme.com", ["us-smtp-inbound-1.mimecast.com"], "Mimecast", True),
    ("acme.com", ["smtp.secureserver.net", "mailstore1.secureserver.net"],
     "GoDaddy / Secureserver", True),
    ("acme.com", ["mail.acme.com"], "Self-hosted", True),
    ("acme.com", ["gmail.com"], "Consumer-webmail (forwarded)", False),
    ("acme.com", [], None, False),
    ("acme.com", ["mx.someregionalisp.example"], "Other", True),
]


def do_classify_selftest():
    ok = True
    for dom, hosts, exp_prov, exp_biz in _CLASSIFY_VECTORS:
        prov, biz = classify_provider(dom, hosts)
        good = (prov == exp_prov) and (biz == exp_biz)
        ok = ok and good
        log(f"classify {hosts!r:55} -> ({prov!r}, {biz})  "
            f"{'OK' if good else 'FAIL exp=(' + repr(exp_prov) + ',' + str(exp_biz) + ')'}")
    log("classifier selftest: " + ("PASS" if ok else "FAIL"))
    return 0 if ok else 3


def do_selftest():
    rc = do_classify_selftest()
    if not _HAVE_DNS:
        log("selftest NOTE: dnspython not importable -> MX/NS resolution will be skipped "
            "(domains kept pending) until the manifest's pip install lands.")
    load_env_file(DB_ENV_PATH)
    load_env_file(AUTOPULL_ENV_PATH)
    try:
        conn = connect_pg()
    except Exception as e:  # noqa: BLE001
        log(f"selftest: Postgres connect FAILED: {e}")
        return 3
    try:
        cur = conn.cursor()
        for s, t in (BUSINESS_TBL, SOURCE_TBL):
            if not table_columns(cur, s, t):
                log(f"selftest: required table {s}.{t} MISSING")
                return 3
        ensure_aux_schema(conn)
        cur.execute(f'SELECT count(*) FROM "{STATE_TBL[0]}"."{STATE_TBL[1]}"')
        log(f"selftest: mx_qualify_state present, rows={cur.fetchone()[0]}")
        conn.rollback()
    finally:
        conn.close()
    log("selftest: DB + schema OK")
    return rc


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    arg = sys.argv[1] if len(sys.argv) > 1 else ""
    if arg == "--classify-selftest":
        sys.exit(do_classify_selftest())
    if arg == "--selftest":
        sys.exit(do_selftest())

    cap = _cap()
    load_env_file(DB_ENV_PATH)
    load_env_file(AUTOPULL_ENV_PATH)
    conn = connect_pg()
    cur = conn.cursor()
    bs, bt = BUSINESS_TBL
    ss, st_ = SOURCE_TBL
    biz_cols = table_columns(cur, bs, bt)
    sr_cols = table_columns(cur, ss, st_)
    if not biz_cols:
        raise SystemExit(f"ERROR: {bs}.{bt} not found.")
    if not sr_cols:
        raise SystemExit(f"ERROR: {ss}.{st_} not found.")
    ensure_aux_schema(conn)

    biz_keys = {
        "pk": table_pk(cur, bs, bt),
        "website": pick_col(biz_cols, CANDIDATES["biz_website"]),
        "ext": pick_col(biz_cols, CANDIDATES["biz_ext_id"]),
        "email_provider": pick_col(biz_cols, CANDIDATES["biz_email_provider"]),
    }
    sr_keys = {
        "fk":  pick_col(sr_cols, CANDIDATES["sr_business_fk"]),
        "src": pick_col(sr_cols, CANDIDATES["sr_source"]),
        "ext": pick_col(sr_cols, CANDIDATES["sr_ext_id"]),
        "pay": pick_col(sr_cols, CANDIDATES["sr_payload"]),
        "cre": pick_col(sr_cols, CANDIDATES["sr_created"]),
    }
    if not biz_keys["pk"]:
        raise SystemExit(f"ERROR: cannot determine PK of {bs}.{bt}.")
    log(f"keys biz={biz_keys} sr={sr_keys} ct_source={CT_SOURCE!r} cap={cap} "
        f"dns={'yes' if _HAVE_DNS else 'NO(pending-only)'}")

    now = datetime.datetime.now(datetime.timezone.utc)
    stat = {"new_targets": 0, "recheck_targets": 0, "resolved": 0, "pending": 0,
            "giveup": 0, "suppressed": 0, "nomx": 0, "dnsfail": 0,
            "has_business_email": 0, "new_source_record": 0, "providers": {}}

    # 1) brand-new CT domains not yet qualified
    new_targets = fetch_new_ct_targets(cur, biz_keys, sr_keys, cap)
    stat["new_targets"] = len(new_targets)
    # 2) due rechecks (MX may have appeared since last time)
    remaining = max(0, cap - len(new_targets))
    rechecks = fetch_due_rechecks(cur, remaining) if remaining else []
    stat["recheck_targets"] = len(rechecks)

    log(f"work: {len(new_targets)} new ct_new_ssl domains + {len(rechecks)} due rechecks")

    done = 0
    seen = set()
    for ref, dom in new_targets:
        if ref in seen:
            continue
        seen.add(ref)
        qualify_one(cur, biz_keys, sr_keys, ref, dom, 0, now, stat)
        done += 1
        if done % BATCH_COMMIT == 0:
            conn.commit()
            log(f"... committed at {done} (resolved={stat['resolved']} pending={stat['pending']})")
        time.sleep(REQ_SLEEP)
    for ref, dom, attempts_prev in rechecks:
        if ref in seen:
            continue
        seen.add(ref)
        qualify_one(cur, biz_keys, sr_keys, ref, dom, int(attempts_prev or 0), now, stat)
        done += 1
        if done % BATCH_COMMIT == 0:
            conn.commit()
        time.sleep(REQ_SLEEP)
    conn.commit()

    # read-back
    cur.execute(f'SELECT count(*) FROM "{STATE_TBL[0]}"."{STATE_TBL[1]}"')
    state_total = cur.fetchone()[0]
    cur.execute(f"SELECT count(*) FROM \"{STATE_TBL[0]}\".\"{STATE_TBL[1]}\" WHERE status='resolved'")
    state_resolved = cur.fetchone()[0]
    cur.execute(f"SELECT count(*) FROM \"{STATE_TBL[0]}\".\"{STATE_TBL[1]}\" WHERE status='pending'")
    state_pending = cur.fetchone()[0]

    log("=" * 70)
    log(f"SUMMARY new={stat['new_targets']} rechecks={stat['recheck_targets']} "
        f"resolved={stat['resolved']} has_business_email={stat['has_business_email']} "
        f"pending={stat['pending']} giveup={stat['giveup']} suppressed={stat['suppressed']}")
    log(f"raw_mx_outcomes nomx={stat['nomx']} dnsfail={stat['dnsfail']} "
        f"providers={stat['providers']} new_source_records={stat['new_source_record']}")
    log(f"READ-BACK mx_qualify_state total={state_total} resolved={state_resolved} "
        f"pending={state_pending}")
    log("=" * 70)

    counts = {
        "lane": SOURCE_NAME, "cap": cap,
        "new_targets": stat["new_targets"], "recheck_targets": stat["recheck_targets"],
        "resolved": stat["resolved"], "has_business_email": stat["has_business_email"],
        "pending": stat["pending"], "giveup": stat["giveup"], "suppressed": stat["suppressed"],
        "providers": stat["providers"], "new_source_record": stat["new_source_record"],
        "state_total": state_total, "state_resolved": state_resolved,
        "state_pending": state_pending, "dns_available": _HAVE_DNS, "ts": int(time.time()),
    }
    write_counts(counts)
    cur.close()
    conn.close()

    # FAIL-LOUD only on real breakage. 0 NEW work is normal for a frequent timer and
    # must NOT exit non-zero (that would wedge the every-few-minutes service).
    if not _HAVE_DNS and (stat["new_targets"] + stat["recheck_targets"]) > 0 \
            and stat["resolved"] == 0 and state_resolved == 0:
        log("WARN: dnspython unavailable so nothing could be resolved this run; all kept "
            "pending. Exit 0 (timer will retry once the pip install lands).")
    log(f"PASS: processed={done} resolved={stat['resolved']} pending_now={state_pending}.")
    sys.exit(0)


if __name__ == "__main__":
    main()
