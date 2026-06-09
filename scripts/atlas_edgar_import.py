#!/usr/bin/env python3
# ============================================================================
# atlas_edgar_import.py -- EDGAR (SEC) collector for ATLAS, written against the
# REAL production schema (same contract as socrata_import.py):
#   atlas.business(name NOT NULL, name_norm NOT NULL, website, email, phone_e164,
#     addr_line1, city, region, postal, country, lat, lon, category, naics, sic,
#     entity_type) -- we fill what EDGAR gives.
#   atlas.source_record(source_code NOT NULL, source_record_id NOT NULL,
#     business_id NOT NULL FK, content_hash NOT NULL, payload JSONB,
#     UNIQUE(source_code, source_record_id))  <-- idempotency anchor.
#
# Source: SEC EDGAR public JSON (no key, polite rate limit + declared UA):
#   https://www.sec.gov/files/company_tickers.json    (CIK/ticker/title list)
#   https://data.sec.gov/submissions/CIK##########.json  (address/SIC/phone)
# source_code='edgar'; source_record_id = zero-padded CIK (stable, unique).
#
# Idempotent: SR_EXISTS pre-check + ON CONFLICT(source_code,source_record_id)
# DO NOTHING. Additive-only (INSERT only; never UPDATE/DELETE business rows).
# Fail-LOUD: first error per source surfaced to counts/status-back, never nulled.
# Cap-driven (EDGAR_CAP) so a single run is bounded and re-runnable.
# Refuses to touch ssh/autopull/firewall (it is a pure importer).
# ============================================================================
import os, sys, json, time, hashlib, datetime, urllib.request, urllib.error
try:
    import psycopg2
except Exception as e:  # noqa: BLE001
    print(f"[edgar] psycopg2 import failed: {e}", flush=True); psycopg2 = None

DB_ENV_PATH  = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
SOURCE_CODE  = "edgar"
CAP          = int(os.environ.get("EDGAR_CAP", "5000"))
HTTP_TIMEOUT = int(os.environ.get("EDGAR_HTTP_TIMEOUT", "30"))
BATCH_SIZE   = int(os.environ.get("EDGAR_BATCH", "500"))
SUBMISSIONS  = os.environ.get("EDGAR_SUBMISSIONS", "1") not in ("0", "false", "no")
# SEC asks for a descriptive UA with contact; declared per their fair-access policy.
USER_AGENT   = os.environ.get(
    "EDGAR_UA", "atlas-edgar-import/1.0 (admin@cloudwalker171; +https://github.com/cloudwalker171/atlas-manifests)")
PACING_MS    = int(os.environ.get("EDGAR_PACING_MS", "120"))  # <10 req/s per SEC policy
TICKERS_URL  = "https://www.sec.gov/files/company_tickers.json"
SUBMIT_URL   = "https://data.sec.gov/submissions/CIK{cik10}.json"

COUNTS_PATH = os.environ.get(
    "ATLAS_ENRICH_COUNTS",
    os.path.join(os.environ.get("ATLAS_AUTOPULL_STATE", "/var/lib/atlas/autopull"),
                 "last_counts.json"))

BIZ_TBL = "atlas.business"
SR_TBL  = "atlas.source_record"


def log(m): print(f"[edgar] {m}", flush=True)


def load_db_env(path):
    if not os.path.exists(path): return
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"): continue
            if line.lower().startswith("export "): line = line[7:].strip()
            if "=" not in line: continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def pick(*names, default=None):
    for n in names:
        if os.environ.get(n): return os.environ[n]
    return default


def connect_pg():
    conn = psycopg2.connect(
        host=pick("PGHOST", "DB_HOST", "ATLAS_DB_HOST", default="localhost"),
        port=pick("PGPORT", "DB_PORT", "ATLAS_DB_PORT", default="5432"),
        dbname=pick("PGDATABASE", "DB_NAME", "ATLAS_DB_NAME", default="tuanichat_atlas"),
        user=pick("PGUSER", "DB_USER", "ATLAS_DB_USER", default="atlas"),
        password=pick("PGPASSWORD", "DB_PASSWORD", "ATLAS_DB_PASSWORD", default=None),
        connect_timeout=int(os.environ.get("ATLAS_DB_CONNECT_TIMEOUT", "10")),
        application_name="atlas_edgar_import")
    conn.autocommit = False
    return conn


def clip(s, n=512):
    if s is None: return None
    s = str(s).strip()
    return s[:n] if s else None


def norm_name(s):
    if not s: return None
    import re
    s = str(s).lower().strip()
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s or None


def http_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                               "Accept": "application/json",
                                               "Accept-Encoding": "gzip, deflate"})
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            data = r.read()
            enc = r.headers.get("Content-Encoding", "")
            if "gzip" in enc:
                import gzip
                data = gzip.decompress(data)
            elif "deflate" in enc:
                import zlib
                data = zlib.decompress(data)
            return json.loads(data.decode("utf-8", "replace"))
    except (urllib.error.HTTPError, urllib.error.URLError, ValueError) as e:
        log(f"http_json FAILED {url}: {e}")
        return None


BIZ_COLS = ["name", "name_norm", "website", "email", "phone_e164",
            "addr_line1", "city", "region", "postal", "country",
            "lat", "lon", "category", "sic", "entity_type"]
BIZ_INSERT = (f'INSERT INTO {BIZ_TBL} ({", ".join(BIZ_COLS)}) '
              f'VALUES ({", ".join(["%s"] * len(BIZ_COLS))}) RETURNING id')
SR_INSERT = (f'INSERT INTO {SR_TBL} '
             f'(source_code, source_record_id, business_id, content_hash, payload) '
             f'VALUES (%s,%s,%s,%s,%s) '
             f'ON CONFLICT (source_code, source_record_id) DO NOTHING RETURNING id')
SR_EXISTS = f'SELECT 1 FROM {SR_TBL} WHERE source_code=%s AND source_record_id=%s LIMIT 1'


def biz_values(n):
    return [clip(n.get("name")), norm_name(n.get("name")), clip(n.get("website")),
            None, clip(n.get("phone"), 32), clip(n.get("addr_line1")),
            clip(n.get("city"), 128), clip(n.get("region"), 64),
            clip(n.get("postal"), 32), clip(n.get("country"), 8) or "US",
            None, None, clip(n.get("category"), 256), clip(n.get("sic"), 16),
            clip(n.get("entity_type"), 64)]


def content_hash(payload_json): return hashlib.md5(payload_json.encode()).hexdigest()
def json_default(o):
    if isinstance(o, (datetime.date, datetime.datetime)): return o.isoformat()
    return str(o)


def fetch_company_list(cap):
    raw = http_json(TICKERS_URL)
    if not raw:
        return []
    # company_tickers.json is {"0":{"cik_str":..,"ticker":..,"title":..}, ...}
    items = list(raw.values()) if isinstance(raw, dict) else raw
    seen = set(); out = []
    for it in items:
        cik = it.get("cik_str")
        if cik is None or cik in seen: continue
        seen.add(cik)
        out.append({"cik": int(cik), "ticker": it.get("ticker"),
                    "title": it.get("title")})
        if len(out) >= cap: break
    return out


def enrich_with_submissions(rec):
    cik10 = f"{rec['cik']:010d}"
    sub = http_json(SUBMIT_URL.format(cik10=cik10))
    norm = {"name": rec.get("title"), "category": rec.get("ticker"),
            "source_record_id": cik10}
    if sub:
        addr = (sub.get("addresses") or {}).get("business") or {}
        norm["addr_line1"] = " ".join(x for x in (addr.get("street1"),
                                                  addr.get("street2")) if x) or None
        norm["city"] = addr.get("city")
        norm["region"] = addr.get("stateOrCountry")
        norm["postal"] = addr.get("zipCode")
        norm["phone"] = sub.get("phone")
        norm["website"] = sub.get("website") or None
        norm["sic"] = sub.get("sic")
        if sub.get("sicDescription"):
            norm["category"] = sub.get("sicDescription")
        norm["entity_type"] = sub.get("entityType") or "public_company"
        norm["_raw"] = {"cik": cik10, "ticker": rec.get("ticker"),
                        "name": sub.get("name"), "sic": sub.get("sic"),
                        "sicDescription": sub.get("sicDescription"),
                        "addresses": sub.get("addresses"),
                        "phone": sub.get("phone"), "website": sub.get("website"),
                        "entityType": sub.get("entityType")}
    else:
        norm["entity_type"] = "public_company"
        norm["_raw"] = {"cik": cik10, "ticker": rec.get("ticker"),
                        "title": rec.get("title")}
    return norm


def write_counts(counts):
    try:
        os.makedirs(os.path.dirname(COUNTS_PATH), exist_ok=True)
        json.dump(counts, open(COUNTS_PATH, "w"))
    except OSError as e:
        log(f"WARNING could not write counts: {e}")


def main():
    if psycopg2 is None:
        sys.exit("psycopg2 unavailable")
    load_db_env(DB_ENV_PATH)
    if "--selftest" in sys.argv:
        try:
            conn = connect_pg(); cur = conn.cursor()
            cur.execute("SELECT to_regclass('atlas.business'), to_regclass('atlas.source_record')")
            a, b = cur.fetchone()
            assert a and b, "business/source_record missing"
            log("selftest: schema OK")
            conn.close(); log("selftest: PASS"); sys.exit(0)
        except Exception as e:  # noqa: BLE001
            log(f"selftest FAIL: {e}"); sys.exit(3)

    conn = connect_pg(); cur = conn.cursor()
    stat = {"fetched": 0, "mapped": 0, "skipped_nokey": 0, "unchanged": 0,
            "inserted_biz": 0, "inserted_sr": 0, "errors": 0, "first_error": None}
    log(f"==== EDGAR import (source_code='{SOURCE_CODE}', cap={CAP}, submissions={SUBMISSIONS}) ====")
    try:
        companies = fetch_company_list(CAP)
        log(f"company list: {len(companies)} CIKs")
        for rec in companies:
            stat["fetched"] += 1
            srid = f"{rec['cik']:010d}"
            if not rec.get("title"):
                stat["skipped_nokey"] += 1; continue
            cur.execute(SR_EXISTS, (SOURCE_CODE, srid))
            if cur.fetchone():
                stat["unchanged"] += 1; continue
            if SUBMISSIONS:
                norm = enrich_with_submissions(rec)
                if PACING_MS: time.sleep(PACING_MS / 1000.0)
            else:
                norm = {"name": rec.get("title"), "category": rec.get("ticker"),
                        "entity_type": "public_company", "source_record_id": srid,
                        "_raw": rec}
            if not norm.get("name"):
                stat["skipped_nokey"] += 1; continue
            stat["mapped"] += 1
            payload = json.dumps(norm.get("_raw", rec), default=json_default, sort_keys=True)
            ch = content_hash(payload)
            cur.execute("SAVEPOINT rsp")
            try:
                cur.execute(BIZ_INSERT, biz_values(norm))
                biz_id = cur.fetchone()[0]
                cur.execute(SR_INSERT, (SOURCE_CODE, srid, biz_id, ch, payload))
                got = cur.fetchone()
                cur.execute("RELEASE SAVEPOINT rsp")
                stat["inserted_biz"] += 1
                if got: stat["inserted_sr"] += 1
                else: stat["unchanged"] += 1
            except psycopg2.Error as e:
                cur.execute("ROLLBACK TO SAVEPOINT rsp")
                stat["errors"] += 1
                msg = (e.pgerror or str(e)).strip()
                if stat["first_error"] is None:
                    stat["first_error"] = msg
                    log(f"INSERT ERROR (surfaced) srid={srid}: {msg}")
            if stat["fetched"] % BATCH_SIZE == 0:
                conn.commit()
                log(f"... committed at {stat['fetched']} (biz+{stat['inserted_biz']} "
                    f"sr+{stat['inserted_sr']} unchanged={stat['unchanged']} err={stat['errors']})")
        conn.commit()
    except Exception as e:  # noqa: BLE001
        conn.rollback()
        stat["first_error"] = stat["first_error"] or str(e)
        log(f"IMPORT FAILED: {e}")
    cur.execute(f"SELECT count(*) FROM {SR_TBL} WHERE source_code=%s", (SOURCE_CODE,))
    landed = cur.fetchone()[0]
    cur.execute(f"SELECT count(*) FROM {BIZ_TBL}")
    biz_total = cur.fetchone()[0]
    counts = {"lane": "edgar", "source": SOURCE_CODE, "cap": CAP,
              "fetched": stat["fetched"], "mapped": stat["mapped"],
              "new_business": stat["inserted_biz"], "new_source_record": stat["inserted_sr"],
              "unchanged": stat["unchanged"], "errors": stat["errors"],
              "first_error": stat["first_error"], "edgar_landed": landed,
              "business_total": biz_total, "ts": int(time.time())}
    write_counts(counts)
    conn.close()
    log("====================================================================")
    log(f"SUMMARY edgar fetched={stat['fetched']} new_business={stat['inserted_biz']} "
        f"new_source_record={stat['inserted_sr']} unchanged={stat['unchanged']} "
        f"errors={stat['errors']} edgar_landed={landed} business_total={biz_total}")
    print("[atlas_edgar_import] " + json.dumps(counts), flush=True)
    # fail-loud: non-zero exit if we landed nothing AND hit errors, so the
    # manifest apply rolls back instead of silently marking success.
    if stat["errors"] and stat["inserted_sr"] == 0 and landed == 0:
        sys.exit(2)
    sys.exit(0)


if __name__ == "__main__":
    main()
