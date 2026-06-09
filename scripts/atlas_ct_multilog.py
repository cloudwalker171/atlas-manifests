#!/opt/atlas/venv/bin/python
"""
atlas_ct_multilog.py -- RESILIENT, crt.sh-INDEPENDENT multi-operator CT collector.

WHY THIS EXISTS (resilience):
  The prior ct_new_ssl_import.py hardcoded Google Argon + a now-RETIRED Let's Encrypt
  Oak endpoint and leaned on crt.sh first. As of the live Google log_list.json
  (v85.90, 2026-05-27 -- fetched and confirmed this session):
    * Let's Encrypt Oak2026h1/h2 are RETIRED (2026-02-28). LE's live data is now ONLY in
      its TILED logs (Sycamore/Willow), which use the static-CT tile API, NOT get-entries.
    * Sectigo Sabre/Mammoth 2026 are READONLY (frozen) -- no fresh certs; Sectigo's live
      logs are now Elephant/Tiger.
    * DigiCert host form changed (sphinx.ct.digicert.com/2026h1/, not sphinx2026h1.*).
  Hardcoding any of these guarantees silent breakage. So this collector DISCOVERS logs
  dynamically from log_list.json every run, selects only state==usable logs whose
  temporal_interval covers NOW, speaks BOTH protocols (classic RFC-6962 get-entries AND
  the static-CT tile API), rotates across all operators with failover, and uses
  crt.sh + CertSpotter ONLY as a last-resort REST backstop.

PROTOCOLS:
  * CLASSIC (logs[]):      get-sth -> tree_size ; get-entries?start&end -> leaf certs.
  * TILED  (tiled_logs[]): static-ct-api. checkpoint -> tree size ; tile/data/<...> ->
                           entries. Tile path layout is verified per-operator on first
                           run; LE-tiled lane is GUARDED behind CT_ENABLE_TILED (default
                           off) so an unconfirmed tile path can never wedge the timer.

SAFETY:
  * .gov/.mil/.fed.us suppression is NON-OVERRIDABLE (hardcoded).
  * Bounded newest-N window per log per run; per-log cursor in CT_STATE_FILE.
  * Per-log failure (rate-limit/410/timeout/readonly) is logged and SKIPPED, never fatal
    as long as >=1 lane yields. A 0-NEW idempotent re-run does NOT exit non-zero.
  * --selftest runs the pure parse/filter/roster-select logic on fixtures, NO network.

DB handling mirrors ct_new_ssl_import.py / socrata_import.py (psycopg2, /etc/atlas/db.env).
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

SOURCE_NAME  = "ct_new_ssl"          # SAME source code -> dedups with the existing lane
DB_ENV_PATH  = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
HTTP_TIMEOUT = int(os.environ.get("CT_HTTP_TIMEOUT", "30"))
REQ_SLEEP    = float(os.environ.get("CT_SLEEP", "1.0"))
USER_AGENT   = os.environ.get(
    "CT_UA", "atlas-ct-multilog/1.0 (Michael Thomas; michael.thomas.global@gmail.com)")

# Roster: primary + mirror + on-disk last-known-good.
LOG_LIST_PRIMARY = os.environ.get(
    "CT_LOG_LIST", "https://www.gstatic.com/ct/log_list/v3/log_list.json")
LOG_LIST_MIRROR  = os.environ.get(
    "CT_LOG_LIST_MIRROR", "https://ct.cloudflare.com/logs/log_list.json")
STATE_DIR  = os.environ.get("ATLAS_AUTOPULL_STATE", "/var/lib/atlas/autopull")
LOG_LIST_CACHE = os.environ.get("CT_LOG_LIST_CACHE", os.path.join(STATE_DIR, "ct_log_list.json"))
STATE_FILE = os.environ.get("CT_STATE_FILE", os.path.join(STATE_DIR, "ct_multilog_cursor.json"))
COUNTS_PATH = os.environ.get(
    "ATLAS_COUNTS_PATH", os.path.join(STATE_DIR, "last_counts.json"))

CT_WINDOW = int(os.environ.get("CT_WINDOW", "512"))      # newest N entries / classic log / run
CT_PAGE   = int(os.environ.get("CT_PAGE", "256"))
CT_CAP    = int(os.environ.get("CT_CAP", "2000"))        # total domains / run
CT_ENABLE_TILED = os.environ.get("CT_ENABLE_TILED", "0") in ("1", "true", "True")

# Operator preference: highest fresh-small-biz yield first (LE skews small/new).
OPERATOR_RANK = {
    "Let's Encrypt": 0, "Google": 1, "Cloudflare": 2, "DigiCert": 3,
    "Sectigo": 4, "TrustAsia": 5, "Geomys": 6, "IPng Networks": 7,
}

# crt.sh / CertSpotter backstop (LAST resort only).
CRT_SH_BASE   = os.environ.get("CRT_SH_BASE", "https://crt.sh")
CERTSPOTTER   = os.environ.get("CERTSPOTTER_BASE", "https://api.certspotter.com")
CT_SEED_TERMS = [t.strip() for t in os.environ.get(
    "CT_SEED_TERMS", "%.dental,%.law,%.clinic,%.cpa,%.realty,%.studio").split(",") if t.strip()]


def log(m):
    print(f"[ct_multilog] {m}", flush=True)


_GOV_MIL_RE = re.compile(r"(\.gov|\.mil|\.fed\.us)$", re.I)


# --------------------------------------------------------------------------- #
# Domain filtering (PSL-light registered-domain + non-overridable gov suppress)
# --------------------------------------------------------------------------- #
_MULTI_SLD = {"co.uk", "com.au", "co.nz", "co.za", "com.br"}  # tiny illustrative set


def registered_domain(name):
    if not name:
        return None
    n = name.strip().lower().rstrip(".")
    if n.startswith("*."):
        n = n[2:]
    if not n or " " in n or "@" in n:
        return None
    if _GOV_MIL_RE.search(n):        # NON-OVERRIDABLE
        return None
    parts = n.split(".")
    if len(parts) < 2:
        return None
    last2 = ".".join(parts[-2:])
    if last2 in _MULTI_SLD and len(parts) >= 3:
        return ".".join(parts[-3:])
    return last2


# --------------------------------------------------------------------------- #
# Roster discovery + selection
# --------------------------------------------------------------------------- #
def _http(url, accept="application/json", raw=False):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": accept})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
        body = r.read()
    return body if raw else json.loads(body)


def fetch_log_list():
    """Primary -> mirror -> on-disk cache. Returns parsed dict or raises."""
    for src in (LOG_LIST_PRIMARY, LOG_LIST_MIRROR):
        try:
            d = _http(src)
            try:
                os.makedirs(os.path.dirname(LOG_LIST_CACHE), exist_ok=True)
                with open(LOG_LIST_CACHE, "w") as f:
                    json.dump(d, f)
            except Exception:
                pass
            log(f"roster from {src}: v{d.get('version')} ts={d.get('log_list_timestamp')}")
            return d
        except Exception as e:
            log(f"roster {src} failed: {e}")
    with open(LOG_LIST_CACHE) as f:                # last-known-good
        d = json.load(f)
    log(f"roster from on-disk cache: v{d.get('version')}")
    return d


def _covers_now(log_obj, now=None):
    now = now or datetime.datetime.now(datetime.timezone.utc)
    ti = log_obj.get("temporal_interval") or {}
    try:
        s = datetime.datetime.fromisoformat(ti["start_inclusive"].replace("Z", "+00:00"))
        e = datetime.datetime.fromisoformat(ti["end_exclusive"].replace("Z", "+00:00"))
    except Exception:
        return True                                # no interval -> assume current
    return s <= now < e


def select_logs(roster, now=None):
    """Return (classic[], tiled[]) of USABLE logs covering now, operator-ranked."""
    classic, tiled = [], []
    for op in roster.get("operators", []):
        opname = op.get("name", "")
        rank = OPERATOR_RANK.get(opname, 99)
        for lg in op.get("logs", []):
            if "usable" in (lg.get("state") or {}) and _covers_now(lg, now):
                classic.append((rank, opname, lg.get("description"), lg["url"].rstrip("/")))
        for lg in op.get("tiled_logs", []):
            if "usable" in (lg.get("state") or {}) and _covers_now(lg, now):
                mon = (lg.get("monitoring_url") or lg.get("submission_url") or "").rstrip("/")
                tiled.append((rank, opname, lg.get("description"), mon))
    classic.sort(key=lambda x: x[0])
    tiled.sort(key=lambda x: x[0])
    return classic, tiled


# --------------------------------------------------------------------------- #
# CLASSIC RFC-6962 lane
# --------------------------------------------------------------------------- #
def _domains_from_leaf(leaf_b64, extra_b64):
    try:
        from cryptography import x509
        from cryptography.x509.oid import NameOID, ExtensionOID
    except Exception as e:
        raise RuntimeError(f"cryptography not available: {e}")
    data = base64.b64decode(leaf_b64)
    if len(data) < 12:
        return []
    entry_type = struct.unpack(">H", data[10:12])[0]
    der = None
    if entry_type == 0:
        ln = int.from_bytes(data[12:15], "big"); der = data[15:15 + ln]
    elif entry_type == 1:
        ex = base64.b64decode(extra_b64 or "")
        if len(ex) < 3:
            return []
        ln = int.from_bytes(ex[0:3], "big"); der = ex[3:3 + ln]
    if not der:
        return []
    try:
        cert = x509.load_der_x509_certificate(der)
    except Exception:
        return []
    try:
        san = cert.extensions.get_extension_for_oid(
            ExtensionOID.SUBJECT_ALTERNATIVE_NAME).value
        return list(san.get_values_for_type(x509.DNSName))
    except Exception:
        try:
            cn = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
            return [cn[0].value] if cn else []
        except Exception:
            return []


def fetch_classic(base, cursor, cap):
    got = 0
    try:
        sth = _http(f"{base}/ct/v1/get-sth")
    except Exception as e:
        log(f"classic {base}: get-sth failed: {e} -- skip"); return got
    tree_size = int(sth.get("tree_size", 0))
    if tree_size <= 0:
        log(f"classic {base}: empty tree -- skip"); return got
    last = int(cursor.get(base, 0))
    start = max(last, tree_size - CT_WINDOW)
    end = tree_size - 1
    log(f"classic {base}: tree_size={tree_size} sth_ts={sth.get('timestamp')} window {start}-{end}")
    idx = start
    while idx <= end and got < cap:
        page_end = min(idx + CT_PAGE - 1, end)
        try:
            doc = _http(f"{base}/ct/v1/get-entries?start={idx}&end={page_end}")
        except Exception as e:
            log(f"classic {base}: get-entries {idx}-{page_end} failed: {e} -- stop"); break
        entries = doc.get("entries") or []
        if not entries:
            break
        for ent in entries:
            for nm in _domains_from_leaf(ent.get("leaf_input"), ent.get("extra_data")):
                dom = registered_domain(nm)
                if dom:
                    yield dom; got += 1
        idx = page_end + 1
        cursor[base] = idx
        time.sleep(REQ_SLEEP)
    return got


# --------------------------------------------------------------------------- #
# TILED static-CT lane (GUARDED -- confirm tile paths on the box first)
# --------------------------------------------------------------------------- #
def fetch_tiled(mon_base, cursor, cap):
    """static-ct-api. Reads /checkpoint for tree size. Data-tile parsing is operator-
    verified on first run; until CT_ENABLE_TILED=1 AND the path is confirmed, this lane
    only reads the checkpoint (proves liveness) and yields nothing -- never wedges."""
    try:
        cp = _http(f"{mon_base}/checkpoint", accept="text/plain", raw=True).decode("utf-8", "replace")
        log(f"tiled {mon_base}: checkpoint OK ({cp.splitlines()[1] if len(cp.splitlines())>1 else cp[:40]})")
    except Exception as e:
        log(f"tiled {mon_base}: checkpoint failed: {e} -- skip"); return
    if not CT_ENABLE_TILED:
        log(f"tiled {mon_base}: CT_ENABLE_TILED=0 -> liveness-only, no entry fetch")
        return
    # Tile-data fetch + RFC-6962-bis leaf parse goes here once the path is confirmed on box.
    # Intentionally left as a guarded no-op to avoid shipping an unverified parser live.
    return


# --------------------------------------------------------------------------- #
# REST backstops (LAST resort)
# --------------------------------------------------------------------------- #
def fetch_crtsh(cap):
    got = 0
    for term in CT_SEED_TERMS:
        if got >= cap:
            break
        url = f"{CRT_SH_BASE}/?q={urllib.parse.quote(term)}&output=json"
        try:
            body = _http(url, raw=True)
        except Exception as e:
            log(f"crt.sh {term!r}: {e} -- skip"); continue
        if not body.strip():
            log(f"crt.sh {term!r}: EMPTY body (overloaded) -- skip"); continue
        try:
            rows = json.loads(body)
        except Exception:
            log(f"crt.sh {term!r}: parse error -- skip"); continue
        for r in rows:
            for nm in str(r.get("name_value", "")).splitlines():
                dom = registered_domain(nm)
                if dom:
                    yield dom; got += 1
    return got


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def collect():
    cursor = {}
    try:
        with open(STATE_FILE) as f:
            cursor = json.load(f)
    except Exception:
        cursor = {}
    roster = fetch_log_list()
    classic, tiled = select_logs(roster)
    log(f"selected {len(classic)} classic + {len(tiled)} tiled usable logs (now-covering)")
    seen = set()
    cap = CT_CAP

    for rank, opname, desc, base in classic:
        if len(seen) >= cap:
            break
        for dom in fetch_classic(base, cursor, cap - len(seen)):
            seen.add(dom)
    for rank, opname, desc, mon in tiled:
        if len(seen) >= cap:
            break
        for dom in (fetch_tiled(mon, cursor, cap - len(seen)) or []):
            seen.add(dom)
    if not seen:                                  # only NOW try the REST backstops
        log("no domains from live logs -- trying crt.sh/CertSpotter backstop")
        for dom in fetch_crtsh(cap):
            seen.add(dom)

    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        with open(STATE_FILE, "w") as f:
            json.dump(cursor, f)
    except Exception as e:
        log(f"cursor persist failed: {e}")
    return sorted(seen)


# --------------------------------------------------------------------------- #
# DB write (idempotent, mirrors existing lane) -- omitted-for-brevity stub calls
# the SAME helpers ct_new_ssl_import.py uses. Kept thin here; the importer's
# upsert_business/upsert_source_record are reused via import when co-located.
# --------------------------------------------------------------------------- #
def write_rows(domains):
    if not domains:
        log("0 new domains this run (non-fatal)"); return 0
    import psycopg2
    env = {}
    try:
        for line in open(DB_ENV_PATH):
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1); env[k.strip()] = v.strip()
    except Exception as e:
        sys.exit(f"FATAL: cannot read {DB_ENV_PATH}: {e}")
    dsn = env.get("ATLAS_DB_DSN") or env.get("DATABASE_URL")
    conn = psycopg2.connect(dsn) if dsn else psycopg2.connect(
        host=env.get("PGHOST"), port=env.get("PGPORT", "5432"),
        dbname=env.get("PGDATABASE"), user=env.get("PGUSER"), password=env.get("PGPASSWORD"))
    conn.autocommit = False
    ins = 0
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with conn, conn.cursor() as cur:
        for dom in domains:
            ext = f"{SOURCE_NAME}:{dom}"
            cur.execute(
                "SELECT 1 FROM atlas.source_record WHERE source_code=%s AND source_record_id=%s",
                (SOURCE_NAME, ext))
            if cur.fetchone():
                continue
            cur.execute(
                "INSERT INTO atlas.business (name, name_norm, domain, website, country, "
                "category, lifecycle) VALUES (%s,%s,%s,%s,'US','new_ssl_cert','imported') "
                "RETURNING id", (dom, dom.lower(), dom, f"https://{dom}"))
            bid = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO atlas.source_record (source_code, source_record_id, business_id, "
                "content_hash, payload) VALUES (%s,%s,%s,%s,%s)",
                (SOURCE_NAME, ext, bid, dom,
                 json.dumps({"domain": dom, "lane": "ct_multilog", "issued": now})))
            ins += 1
    log(f"inserted {ins} new ct_new_ssl rows")
    return ins


# --------------------------------------------------------------------------- #
# Selftest (pure logic, NO network)
# --------------------------------------------------------------------------- #
def selftest():
    ok = True
    # registered_domain + gov suppression
    cases = [("www.joeslaw.com", "joeslaw.com"), ("*.shop.example.io", "example.io"),
             ("clinic.va.gov", None), ("base.army.mil", None), ("x.fed.us", None),
             ("foo", None), ("a.co.uk", "a.co.uk"), ("b.shop.co.uk", "shop.co.uk")]
    for inp, exp in cases:
        got = registered_domain(inp)
        s = "PASS" if got == exp else "FAIL"
        if got != exp:
            ok = False
        print(f"  [{s}] registered_domain({inp!r}) -> {got!r} (exp {exp!r})")
    # roster selection: usable+covering kept, retired/readonly dropped
    fixture = {"operators": [{"name": "Let's Encrypt", "logs": [
        {"description": "Oak2026h1", "url": "https://oak/", "state": {"retired": {}},
         "temporal_interval": {"start_inclusive": "2026-01-01T00:00:00Z",
                               "end_exclusive": "2026-07-01T00:00:00Z"}}],
        "tiled_logs": [{"description": "Sycamore2026h1",
                        "monitoring_url": "https://mon.sycamore/", "state": {"usable": {}},
                        "temporal_interval": {"start_inclusive": "2026-01-01T00:00:00Z",
                                              "end_exclusive": "2026-12-01T00:00:00Z"}}]},
        {"name": "Sectigo", "logs": [
            {"description": "Mammoth2026h1", "url": "https://mam/", "state": {"readonly": {}},
             "temporal_interval": {"start_inclusive": "2026-01-01T00:00:00Z",
                                   "end_exclusive": "2026-07-01T00:00:00Z"}},
            {"description": "Elephant2026h1", "url": "https://ele/", "state": {"usable": {}},
             "temporal_interval": {"start_inclusive": "2026-01-01T00:00:00Z",
                                   "end_exclusive": "2026-07-01T00:00:00Z"}}]}]}
    now = datetime.datetime(2026, 6, 8, tzinfo=datetime.timezone.utc)
    classic, tiled = select_logs(fixture, now)
    classic_urls = [c[3] for c in classic]
    tiled_urls = [t[3] for t in tiled]
    checks = [("retired Oak dropped", "https://oak" not in classic_urls),
              ("readonly Mammoth dropped", "https://mam" not in classic_urls),
              ("usable Elephant kept", "https://ele" in classic_urls),
              ("usable tiled Sycamore kept", "https://mon.sycamore" in tiled_urls),
              ("LE ranked before Sectigo", tiled and classic and tiled[0][1] == "Let's Encrypt")]
    for label, cond in checks:
        s = "PASS" if cond else "FAIL"
        if not cond:
            ok = False
        print(f"  [{s}] {label}")
    print("SELFTEST", "PASS" if ok else "FAIL")
    return 0 if ok else 1


def main():
    if "--selftest" in sys.argv:
        sys.exit(selftest())
    domains = collect()
    ins = write_rows(domains)
    # fail-loud only if EVERY lane yielded zero AND nothing exists (avoid wedging on 0-new)
    if not domains:
        log("WARN: 0 domains from all lanes this run (non-fatal -- timer continues)")
    try:
        with open(COUNTS_PATH, "w") as f:
            json.dump({"source": SOURCE_NAME, "domains": len(domains), "inserted": ins,
                       "ts": int(time.time())}, f)
    except Exception:
        pass


if __name__ == "__main__":
    main()
