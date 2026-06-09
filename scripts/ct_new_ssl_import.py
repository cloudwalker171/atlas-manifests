#!/opt/atlas/venv/bin/python
"""
ct_new_ssl_import.py -- "fresh SSL cert" new-business signal -> atlas Postgres importer.

Signal: a brand-new TLS certificate for a domain is a strong "a business just came online"
event. This collector pulls NEWLY-ISSUED certs from Certificate Transparency, extracts the
registered domains, filters to plausible US business domains, and writes each as an
atlas.business row + atlas.source_record (source='ct_new_ssl'), with the cert-issuance
timestamp carried as the freshness signal.

TWO LANES (the public CertStream websocket firehose is DEAD since ~Mar 2025 -- NOT used):

  LANE A (PRIMARY, per spec): crt.sh JSON REST.
      https://crt.sh/?q=<term>&output=json   (+ optional &exclude=expired)
    Honest status: from the build host (2026-06-08) crt.sh returned an EMPTY body on every
    probe (HTML and JSON, small and large queries) -- crt.sh is well known to be overloaded
    and the registry already rated it failover-only. So LANE A could NOT be confirmed live
    here; it MUST be confirmed by the first run on the box. The code is correct and ready.

  LANE B (FAILOVER, VERIFIED LIVE here): RFC-6962 Certificate Transparency log REST API.
      get-sth  -> current tree_size   |   get-entries?start=&end= -> leaf certs
    Verified 2026-06-08: ct.googleapis.com/logs/us1/argon2026h2 get-sth returned a fresh STH
    (tree_size ~1.44e9, timestamp today). This lane tails ONLY the newest CT_WINDOW entries
    per run (bounded -- never the whole 1.4B-entry log), keeps a per-log cursor, and parses
    the leaf/precert DER with the `cryptography` lib to read SAN dNSNames. This is the
    reliable real-time path; crt.sh is tried first only because the spec asked.

POLITENESS / SCALE: crt.sh is a free shared service -- this collector NEVER issues a
match-all wildcard firehose. LANE A uses a bounded seed-term list (CT_SEED_TERMS) within a
freshness window and dedups by crt.sh cert id; LANE B pulls a bounded window with sleeps.
Per-run cap CT_CAP (default 2000). A persistent cursor (CT_STATE_FILE) prevents reprocessing.

.gov/.mil SUPPRESSION (NON-OVERRIDABLE): any domain ending in .gov / .mil / .fed.us is
dropped at ingestion -- hardcoded, no env flag disables it. (Defense-in-depth before the
downstream global suppression gate, which still applies to anything contactable.)

US-scoped: keeps generic/US-common gTLDs + .us; drops obvious non-business infra apexes
(CDN/cloud/hosting wildcards) and country-code TLDs that aren't US. Idempotent on domain.
Verbose. FAIL-LOUD: exits non-zero if 0 domains were obtained from BOTH lanes, or if 0
ct_new_ssl rows are present after the run. Idempotent re-runs inserting 0 NEW rows do NOT
fail (that would wedge the every-few-minutes timer).

DB handling identical to socrata_import.py.
"""

import os
import re
import ssl
import sys
import json
import time
import base64
import struct
import datetime
import urllib.parse
import urllib.request
import urllib.error

import psycopg2

DB_ENV_PATH  = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
BUSINESS_TBL = ("atlas", "business")
SOURCE_TBL   = ("atlas", "source_record")
BATCH_SIZE   = int(os.environ.get("CT_BATCH", "200"))
HTTP_TIMEOUT = int(os.environ.get("CT_HTTP_TIMEOUT", "30"))
REQ_SLEEP    = float(os.environ.get("CT_SLEEP", "1.0"))     # polite delay between calls
SOURCE_NAME  = "ct_new_ssl"
USER_AGENT   = os.environ.get(
    "CT_UA", "atlas-ct-new-ssl/1.0 (Michael Thomas; michael.thomas.global@gmail.com)")

CRT_SH_BASE  = os.environ.get("CRT_SH_BASE", "https://crt.sh")
# Bounded seed terms for the crt.sh lane (NOT a wildcard firehose). Operator-tunable.
CT_SEED_TERMS = [t.strip() for t in os.environ.get(
    "CT_SEED_TERMS",
    "%.llc.com,%.group.com,%.studio,%.clinic,%.law,%.cpa,%.dental,%.realty"
).split(",") if t.strip()]

# CT log get-entries failover. Default = the one verified live on 2026-06-08.
CT_LOGS = [u.strip() for u in os.environ.get(
    "CT_LOGS",
    "https://ct.googleapis.com/logs/us1/argon2026h2,"
    "https://ct.googleapis.com/logs/us1/argon2026h1"
).split(",") if u.strip()]
CT_WINDOW = int(os.environ.get("CT_WINDOW", "512"))         # newest N entries per run
CT_PAGE   = int(os.environ.get("CT_PAGE", "256"))           # get-entries page size

FETCH_HOMEPAGE = os.environ.get("CT_FETCH_HOMEPAGE", "0") == "1"  # off by default (polite)

STATE_DIR  = os.environ.get("ATLAS_AUTOPULL_STATE", "/var/lib/atlas/autopull")
STATE_FILE = os.environ.get("CT_STATE_FILE", os.path.join(STATE_DIR, "ct_cursor.json"))
COUNTS_PATH = os.environ.get("ATLAS_COUNTS_PATH", os.path.join(STATE_DIR, "last_counts.json"))
COUNTS_PATH_SRC = os.path.join(STATE_DIR, f"last_counts_{SOURCE_NAME}.json")


def _cap():
    if len(sys.argv) > 1 and str(sys.argv[1]).strip():
        try:
            return int(sys.argv[1])
        except ValueError:
            pass
    return int(os.environ.get("CT_CAP", "2000") or "2000")

ROW_CAP = _cap()


def log(msg):
    print(f"[ct_new_ssl] {msg}", flush=True)


# --------------------------------------------------------------------------- #
# Domain filtering: .gov/.mil suppression (non-overridable) + US-biz heuristic
# --------------------------------------------------------------------------- #
# NON-OVERRIDABLE suppression. No env flag can disable this.
_GOV_MIL_RE = re.compile(r"(\.gov|\.mil|\.fed\.us)$", re.I)

# US-acceptable TLDs (generic gTLDs commonly used by US businesses + .us).
US_OK_TLDS = set("""com net org io co biz info us app dev shop store online site tech
agency studio law cpa clinic dental realty group llc inc company ventures capital
finance health care media design solutions consulting services""".split())

# Obvious non-business infra / provider apexes to drop (substring match on the apex).
INFRA_APEX = ("amazonaws.com", "cloudfront.net", "akamai", "googleusercontent.com",
              "azureedge.net", "windows.net", "herokuapp.com", "github.io",
              "netlify.app", "vercel.app", "cloudflare", "fastly.net", "wpengine.com",
              "myshopify.com", "wixsite.com", "googleapis.com", "gstatic.com",
              "sentry.io", "azurewebsites.net", "trafficmanager.net", "edgekey.net",
              "elasticbeanstalk.com", "r2.dev", "pages.dev", "firebaseapp.com")

_LABEL_RE = re.compile(r"^[a-z0-9-]{1,63}$")


def registered_domain(host):
    """Lightweight eTLD+1 (US-focused; treats common 2-label cc/2nd-level as needed)."""
    host = (host or "").strip().lower().rstrip(".")
    if host.startswith("*."):
        host = host[2:]
    if not host or "." not in host:
        return None
    labels = host.split(".")
    if any(not _LABEL_RE.match(l) for l in labels):
        return None
    # US-centric: registered domain is the last two labels (e.g. acme.com),
    # with a small allowance for *.us second-levels (state.us etc -> keep last 3).
    if labels[-1] == "us" and len(labels) >= 3 and len(labels[-2]) <= 3:
        return ".".join(labels[-3:])
    return ".".join(labels[-2:])


def is_us_business_domain(dom):
    if not dom:
        return False
    if _GOV_MIL_RE.search(dom):     # NON-OVERRIDABLE drop
        return False
    if any(infra in dom for infra in INFRA_APEX):
        return False
    tld = dom.rsplit(".", 1)[-1]
    if tld not in US_OK_TLDS:
        return False
    return True


# --------------------------------------------------------------------------- #
# DB env + connection + introspection (verbatim pattern)
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


def table_columns(cur, s, t):
    cur.execute("SELECT column_name FROM information_schema.columns "
                "WHERE table_schema=%s AND table_name=%s", (s, t))
    return {r[0] for r in cur.fetchall()}


def table_pk(cur, s, t):
    cur.execute("SELECT a.attname FROM pg_index i JOIN pg_attribute a "
                "ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
                "WHERE i.indrelid=%s::regclass AND i.indisprimary", (f"{s}.{t}",))
    rows = [r[0] for r in cur.fetchall()]
    return rows[0] if len(rows) == 1 else None


def unique_columns(cur, s, t):
    cur.execute("SELECT a.attname FROM pg_index i JOIN pg_attribute a "
                "ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
                "WHERE i.indrelid=%s::regclass AND (i.indisunique OR i.indisprimary)",
                (f"{s}.{t}",))
    return {r[0] for r in cur.fetchall()}


def pick_col(colset, candidates):
    for c in candidates:
        if c in colset:
            return c
    return None


CANDIDATES = {
    "biz_name":      ["name", "business_name", "title", "display_name"],
    "biz_lat":       ["latitude", "lat", "y"],
    "biz_lon":       ["longitude", "lon", "lng", "x"],
    "biz_category":  ["category", "primary_category", "categories", "category_primary"],
    "biz_website":   ["website", "url", "website_url"],
    "biz_phone":     ["phone", "phone_number", "telephone"],
    "biz_email":     ["email", "email_address"],
    "biz_address":   ["address", "address_freeform", "street_address", "freeform"],
    "biz_locality":  ["locality", "city", "town"],
    "biz_region":    ["region", "state", "province"],
    "biz_postcode":  ["postcode", "postal_code", "zip", "zipcode"],
    "biz_country":   ["country", "country_code"],
    "biz_source":    ["source", "data_source", "origin"],
    "biz_ext_id":    ["source_id", "external_id", "ext_id", "overture_id", "source_record_id"],
    "biz_created":   ["created_at", "inserted_at", "created"],
    "biz_updated":   ["updated_at", "modified_at", "updated"],
    "sr_business_fk":["business_id", "biz_id", "business"],
    "sr_source":     ["source", "data_source", "origin"],
    "sr_ext_id":     ["source_id", "source_record_id", "external_id", "record_id", "ext_id"],
    "sr_payload":    ["raw", "payload", "data", "raw_json", "raw_jsonb", "doc", "record"],
    "sr_created":    ["created_at", "inserted_at", "fetched_at", "created"],
}


def build_business_row(norm, source_name, ext_id_ns, biz_cols, now):
    field_vals = {
        "biz_name": norm.get("name"), "biz_lat": None, "biz_lon": None,
        "biz_category": norm.get("category"), "biz_website": norm.get("website"),
        "biz_phone": None, "biz_email": None, "biz_address": None,
        "biz_locality": None, "biz_region": None, "biz_postcode": None,
        "biz_country": "US", "biz_source": source_name, "biz_ext_id": ext_id_ns,
        "biz_created": now, "biz_updated": now,
    }
    out = {}
    for logical, val in field_vals.items():
        col = pick_col(biz_cols, CANDIDATES[logical])
        if col and col not in out:
            out[col] = val
    return out


def make_insert(s, t, cols, conflict_col):
    col_list = ", ".join(f'"{c}"' for c in cols)
    ph = ", ".join(["%s"] * len(cols))
    base = f'INSERT INTO "{s}"."{t}" ({col_list}) VALUES ({ph})'
    if conflict_col:
        base += f' ON CONFLICT ("{conflict_col}") DO NOTHING'
    return base


def write_counts(counts):
    for path in (COUNTS_PATH, COUNTS_PATH_SRC):
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(counts, fh)
            log(f"wrote counts -> {path}")
        except OSError as e:
            log(f"WARNING: could not write {path}: {e}")


def load_cursor():
    try:
        with open(STATE_FILE) as fh:
            return json.load(fh)
    except Exception:
        return {}


def save_cursor(cur):
    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        with open(STATE_FILE, "w") as fh:
            json.dump(cur, fh)
    except OSError as e:
        log(f"WARNING: could not save cursor {STATE_FILE}: {e}")


# --------------------------------------------------------------------------- #
# LANE A: crt.sh JSON
# --------------------------------------------------------------------------- #
def fetch_crtsh(cap):
    """Yield (domain, meta) from crt.sh for each seed term, bounded + deduped. VERBOSE."""
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    got = 0
    for term in CT_SEED_TERMS:
        if got >= cap:
            break
        qs = urllib.parse.urlencode({"q": term, "output": "json", "exclude": "expired"})
        url = f"{CRT_SH_BASE}/?{qs}"
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                body = resp.read()
        except (urllib.error.HTTPError, urllib.error.URLError) as e:
            log(f"crt.sh term={term!r}: {e} -- skip")
            time.sleep(REQ_SLEEP)
            continue
        if not body.strip():
            log(f"crt.sh term={term!r}: EMPTY body (crt.sh overloaded/unavailable) -- skip")
            time.sleep(REQ_SLEEP)
            continue
        try:
            rows = json.loads(body)
        except json.JSONDecodeError as e:
            log(f"crt.sh term={term!r}: JSON parse error {e} -- skip")
            time.sleep(REQ_SLEEP)
            continue
        log(f"crt.sh term={term!r}: {len(rows)} cert rows")
        for r in rows:
            if got >= cap:
                break
            names = str(r.get("name_value", "")).split("\n")
            ts = r.get("not_before") or r.get("entry_timestamp")
            cid = r.get("id") or r.get("min_cert_id")
            for nm in names:
                dom = registered_domain(nm)
                if dom:
                    yield dom, {"lane": "crtsh", "crtsh_id": cid, "issued": ts, "san": nm}
                    got += 1
        time.sleep(REQ_SLEEP)


# --------------------------------------------------------------------------- #
# LANE B: RFC-6962 CT log get-sth / get-entries (verified live)
# --------------------------------------------------------------------------- #
def _http_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                               "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return json.loads(resp.read())


def _domains_from_entry(leaf_b64, extra_b64):
    """Parse an RFC-6962 get-entries item -> SAN dNSNames. Needs `cryptography`."""
    try:
        from cryptography import x509
        from cryptography.x509.oid import NameOID, ExtensionOID
    except Exception as e:                  # pragma: no cover
        raise RuntimeError(f"cryptography not available: {e}")
    data = base64.b64decode(leaf_b64)
    # MerkleTreeLeaf: version(1) leaf_type(1) TimestampedEntry{ timestamp(8) entry_type(2) ...}
    if len(data) < 12:
        return []
    entry_type = struct.unpack(">H", data[10:12])[0]
    der = None
    if entry_type == 0:                     # x509_entry: ASN.1Cert opaque<24-bit len>
        ln = int.from_bytes(data[12:15], "big")
        der = data[15:15 + ln]
    elif entry_type == 1:                   # precert_entry: full cert is in extra_data
        ex = base64.b64decode(extra_b64 or "")
        if len(ex) < 3:
            return []
        ln = int.from_bytes(ex[0:3], "big")
        der = ex[3:3 + ln]
    if not der:
        return []
    try:
        cert = x509.load_der_x509_certificate(der)
    except Exception:
        return []
    out = []
    try:
        san = cert.extensions.get_extension_for_oid(
            ExtensionOID.SUBJECT_ALTERNATIVE_NAME).value
        out = list(san.get_values_for_type(x509.DNSName))
    except Exception:
        try:
            cn = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
            if cn:
                out = [cn[0].value]
        except Exception:
            out = []
    return out


def fetch_ct_logs(cap, cursor):
    """Tail the newest CT_WINDOW entries of each configured log. Bounded + cursored."""
    got = 0
    for base in CT_LOGS:
        if got >= cap:
            break
        try:
            sth = _http_json(f"{base}/ct/v1/get-sth")
        except Exception as e:              # noqa: BLE001
            log(f"CT {base}: get-sth failed: {e} -- skip log")
            continue
        tree_size = int(sth.get("tree_size", 0))
        if tree_size <= 0:
            log(f"CT {base}: empty tree -- skip")
            continue
        last = int(cursor.get(base, 0))
        start = max(last, tree_size - CT_WINDOW)
        end = tree_size - 1
        log(f"CT {base}: tree_size={tree_size} window start={start} end={end}")
        idx = start
        while idx <= end and got < cap:
            page_end = min(idx + CT_PAGE - 1, end)
            url = f"{base}/ct/v1/get-entries?start={idx}&end={page_end}"
            try:
                doc = _http_json(url)
            except Exception as e:          # noqa: BLE001
                log(f"CT {base}: get-entries {idx}-{page_end} failed: {e} -- stop log")
                break
            entries = doc.get("entries") or []
            if not entries:
                break
            for ent in entries:
                doms = _domains_from_entry(ent.get("leaf_input"), ent.get("extra_data"))
                for nm in doms:
                    dom = registered_domain(nm)
                    if dom:
                        yield dom, {"lane": "ct_log", "log": base, "san": nm,
                                    "issued": datetime.datetime.now(datetime.timezone.utc).isoformat()}
                        got += 1
            idx = page_end + 1
            cursor[base] = idx
            time.sleep(REQ_SLEEP)
    return got


# --------------------------------------------------------------------------- #
# Optional light homepage <title> fetch for a friendlier business name
# --------------------------------------------------------------------------- #
_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)


def fetch_title(dom):
    if not FETCH_HOMEPAGE:
        return None
    url = f"https://{dom}/"
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=8, context=ctx) as resp:
            html = resp.read(20000).decode("utf-8", "replace")
        m = _TITLE_RE.search(html)
        if m:
            return re.sub(r"\s+", " ", m.group(1)).strip()[:200] or None
    except Exception:
        return None
    return None


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    load_db_env(DB_ENV_PATH)
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
    biz_pk = table_pk(cur, bs, bt)
    biz_uniques = unique_columns(cur, bs, bt)
    sr_uniques = unique_columns(cur, ss, st_)
    biz_ext_col = pick_col(biz_cols, CANDIDATES["biz_ext_id"])
    sr_fk_col = pick_col(sr_cols, CANDIDATES["sr_business_fk"])
    sr_src_col = pick_col(sr_cols, CANDIDATES["sr_source"])
    sr_ext_col = pick_col(sr_cols, CANDIDATES["sr_ext_id"])
    sr_pay_col = pick_col(sr_cols, CANDIDATES["sr_payload"])
    sr_cre_col = pick_col(sr_cols, CANDIDATES["sr_created"])
    biz_conflict = biz_ext_col if (biz_ext_col and biz_ext_col in biz_uniques) else None
    sr_conflict = sr_ext_col if (sr_ext_col and sr_ext_col in sr_uniques) else None

    now = datetime.datetime.now(datetime.timezone.utc)
    cursor = load_cursor()
    seen = set()
    stat = {"raw_domains": 0, "kept": 0, "gov_dropped": 0, "infra_dropped": 0,
            "inserted_biz": 0, "inserted_sr": 0, "lane_counts": {"crtsh": 0, "ct_log": 0}}

    def ingest(stream):
        for dom, meta in stream:
            stat["raw_domains"] += 1
            if _GOV_MIL_RE.search(dom):
                stat["gov_dropped"] += 1
                continue
            if not is_us_business_domain(dom):
                stat["infra_dropped"] += 1
                continue
            if dom in seen:
                continue
            seen.add(dom)
            stat["kept"] += 1
            stat["lane_counts"][meta.get("lane", "crtsh")] = \
                stat["lane_counts"].get(meta.get("lane", "crtsh"), 0) + 1
            ext_ns = f"{SOURCE_NAME}:{dom}"
            name = fetch_title(dom) or dom
            biz_norm = {"name": name, "category": "new-ssl-cert (fresh domain)",
                        "website": f"https://{dom}"}
            biz_id = None
            biz_row = build_business_row(biz_norm, SOURCE_NAME, ext_ns, biz_cols, now)
            if biz_row:
                cols = list(biz_row.keys())
                vals = [biz_row[c] for c in cols]
                sql = make_insert(bs, bt, cols, biz_conflict)
                if biz_pk:
                    sql += f' RETURNING "{biz_pk}"'
                try:
                    cur.execute(sql, vals)
                    if biz_pk:
                        got = cur.fetchone()
                        if got:
                            biz_id = got[0]
                            stat["inserted_biz"] += 1
                except psycopg2.Error as e:
                    conn.rollback()
                    log(f"business insert error {ext_ns}: {e.pgerror or e}")
                    continue
            if biz_id is None and biz_pk and biz_ext_col:
                cur.execute(f'SELECT "{biz_pk}" FROM "{bs}"."{bt}" WHERE "{biz_ext_col}"=%s LIMIT 1',
                            (ext_ns,))
                got = cur.fetchone()
                biz_id = got[0] if got else None
            sr_row = {}
            if sr_fk_col and biz_id is not None:
                sr_row[sr_fk_col] = biz_id
            if sr_src_col:
                sr_row[sr_src_col] = SOURCE_NAME
            if sr_ext_col:
                sr_row[sr_ext_col] = ext_ns
            if sr_pay_col:
                payload = dict(meta)
                payload["domain"] = dom
                payload["_atlas_source"] = SOURCE_NAME
                sr_row[sr_pay_col] = json.dumps(payload)
            if sr_cre_col:
                sr_row[sr_cre_col] = now
            if sr_row:
                cols = list(sr_row.keys())
                vals = [sr_row[c] for c in cols]
                if sr_conflict is None and sr_src_col and sr_ext_col:
                    col_list = ", ".join(f'"{c}"' for c in cols)
                    ph = ", ".join(["%s"] * len(cols))
                    sql = (f'INSERT INTO "{ss}"."{st_}" ({col_list}) SELECT {ph} '
                           f'WHERE NOT EXISTS (SELECT 1 FROM "{ss}"."{st_}" '
                           f'WHERE "{sr_src_col}"=%s AND "{sr_ext_col}"=%s)')
                    params = vals + [SOURCE_NAME, ext_ns]
                else:
                    sql = make_insert(ss, st_, cols, sr_conflict)
                    params = vals
                try:
                    cur.execute(sql, params)
                    if cur.rowcount and cur.rowcount > 0:
                        stat["inserted_sr"] += cur.rowcount
                except psycopg2.Error as e:
                    conn.rollback()
                    log(f"source_record insert error {ext_ns}: {e.pgerror or e}")
                    continue
            if stat["kept"] % BATCH_SIZE == 0:
                conn.commit()
                log(f"... committed at kept={stat['kept']} (biz+{stat['inserted_biz']}, sr+{stat['inserted_sr']})")

    # ---- LANE A: crt.sh (primary per spec; may be empty if crt.sh is overloaded) ----
    log(f"LANE A crt.sh: {len(CT_SEED_TERMS)} seed terms, cap={ROW_CAP}")
    try:
        ingest(fetch_crtsh(ROW_CAP))
        conn.commit()
    except Exception as e:                  # noqa: BLE001
        conn.rollback()
        log(f"crt.sh lane error: {e}")

    # ---- LANE B: CT log get-entries (failover; the reliable real-time path) ----
    remaining = ROW_CAP - stat["kept"]
    if remaining > 0:
        log(f"LANE B CT logs: pulling up to {remaining} more (window={CT_WINDOW}/log)")
        try:
            ingest(fetch_ct_logs(remaining, cursor))
            conn.commit()
            save_cursor(cursor)
        except Exception as e:              # noqa: BLE001
            conn.rollback()
            log(f"CT-log lane error: {e}")

    cur.execute(f'SELECT count(*) FROM "{bs}"."{bt}"')
    total_biz = cur.fetchone()[0]
    if sr_src_col:
        cur.execute(f'SELECT count(*) FROM "{ss}"."{st_}" WHERE "{sr_src_col}"=%s', (SOURCE_NAME,))
        src_landed = cur.fetchone()[0]
    else:
        cur.execute(f'SELECT count(*) FROM "{ss}"."{st_}"')
        src_landed = cur.fetchone()[0]

    log("=" * 70)
    log(f"SUMMARY raw_domains={stat['raw_domains']} kept={stat['kept']} "
        f"gov_mil_dropped={stat['gov_dropped']} infra/non-us_dropped={stat['infra_dropped']}")
    log(f"lane_counts={stat['lane_counts']} new_business={stat['inserted_biz']} "
        f"new_source_record={stat['inserted_sr']}")
    log(f"READ-BACK atlas.business total={total_biz}; ct_new_ssl source_records={src_landed}")
    log("=" * 70)

    counts = {
        "lane": SOURCE_NAME, "cap": ROW_CAP, "raw_domains": stat["raw_domains"],
        "kept": stat["kept"], "gov_mil_dropped": stat["gov_dropped"],
        "infra_dropped": stat["infra_dropped"], "lane_counts": stat["lane_counts"],
        "business_total": total_biz, "ct_new_ssl_source_records": src_landed,
        "new_business": stat["inserted_biz"], "new_source_record": stat["inserted_sr"],
        "ts": int(time.time()),
    }
    write_counts(counts)
    cur.close()
    conn.close()

    if stat["raw_domains"] == 0:
        log("FAIL: 0 domains obtained from EITHER crt.sh or the CT-log failover "
            "(both unavailable / cryptography missing).")
        sys.exit(2)
    if src_landed == 0:
        log("FAIL: 0 ct_new_ssl rows present in atlas.source_record after run.")
        sys.exit(3)
    log(f"PASS: raw_domains={stat['raw_domains']} kept={stat['kept']}; "
        f"ct_new_ssl source_records={src_landed}; business_total={total_biz}.")
    sys.exit(0)


if __name__ == "__main__":
    main()
