#!/opt/atlas/venv/bin/python
"""
sos_new_business_import.py -- NO-AUTH state Secretary-of-State NEW-business-registration
                              Socrata datasets -> atlas Postgres.

WHY (Meta-Brain fit ~88/100): unlike a full entity registry (which mixes in millions of
stale, already-chatted businesses), these datasets are PRE-FILTERED by the state to
GENUINELY NEW filings -- "a business that just formed." That is a high chat-ICP signal:
new entity => likely building web presence => likely no chat widget yet. It complements the
real-time CT/SSL spine by catching businesses that REGISTER before they cert (or that are
offline-first).

VERIFIED LIVE (web-checked 2026-06-09 from this workspace via web_fetch):
  * Oregon SoS "New Businesses Registered Last Month"
      data.oregon.gov/resource/esjy-u4fc.json   200 OK, updated 2026-06-05, PUBLIC DOMAIN.
      Columns: business_name, entity_type, registry_date, city, state, zip_code,
               address_, registry_number, ...
Other state SoS "new business" Socrata datasets can be added via SOS_NEWBIZ_DATASETS
(comma list of  domain|fourbyfour|label  triples). The mapper is generic + introspective so
new datasets need no code change as long as they carry name/city/state-ish columns.

DATA MODEL: each row -> atlas.business + atlas.source_record (source='sos_new_business'),
idempotent on a namespaced ext id (sos_new_business:<domain>:<registry_number-or-hash>).
ICP-tag: the source_record payload carries _atlas_signal='new_registration'.

.gov/.mil COMPLIANCE: this is state-gov-SOURCED firmographic ENTITY data (no email). The
importer only sources the new-entity universe; it NEVER derives or stores a contact email
and NEVER contacts anyone. The non-overridable downstream .gov/.mil + contact suppression
gate (suppression.py / gov_suppression.py) still governs any later outreach; this script
does not bypass it. PERSON-LEVEL registries (e.g. tax-preparer rosters) are deliberately
NOT in the default set -- keep it firmographic.

US-scoped by construction (state SoS portals). Per-dataset cap default 5000
(argv[1] or SOS_NEWBIZ_LIMIT). One dataset failing never kills the others. FAIL-LOUD: exits
non-zero if 0 rows fetched from ANY dataset, or 0 sos_new_business rows landed. Idempotent
re-runs inserting 0 NEW rows do NOT fail. DB handling identical to socrata_cities_import.py.

--selftest : pure-logic (no network, no DB) -- exercises the mapper + ext-id + classifier and
             exits 0; exits 3 only if those invariants break.
"""

import os
import sys
import json
import time
import hashlib
import datetime
import urllib.parse
import urllib.request
import urllib.error

DB_ENV_PATH  = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
BUSINESS_TBL = ("atlas", "business")
SOURCE_TBL   = ("atlas", "source_record")
BATCH_SIZE   = int(os.environ.get("SOS_NEWBIZ_BATCH", "1000"))
PAGE_SIZE    = int(os.environ.get("SOS_NEWBIZ_PAGE", "1000"))
HTTP_TIMEOUT = int(os.environ.get("SOS_NEWBIZ_HTTP_TIMEOUT", "60"))
REQ_SLEEP    = float(os.environ.get("SOS_NEWBIZ_SLEEP", "0.2"))
USER_AGENT   = "atlas-sos-newbiz/1.0 (+https://github.com/cloudwalker171/atlas-manifests)"
APP_TOKEN    = os.environ.get("SOCRATA_APP_TOKEN", "").strip()
SOURCE_NAME  = "sos_new_business"

# domain|fourbyfour|label  triples. Default: Oregon SoS new-business (verified live).
DEFAULT_DATASETS = "data.oregon.gov|esjy-u4fc|oregon_sos_new"


def _datasets():
    raw = os.environ.get("SOS_NEWBIZ_DATASETS", DEFAULT_DATASETS).strip()
    out = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts = [p.strip() for p in chunk.split("|")]
        if len(parts) >= 2 and parts[0] and parts[1]:
            domain, fxf = parts[0], parts[1]
            label = parts[2] if len(parts) >= 3 and parts[2] else fxf
            out.append((domain, fxf, label))
    return out


def _cap():
    if len(sys.argv) > 1 and str(sys.argv[1]).strip() and sys.argv[1] != "--selftest":
        try:
            return int(sys.argv[1])
        except ValueError:
            pass
    return int(os.environ.get("SOS_NEWBIZ_LIMIT", "5000") or "5000")


ROW_CAP = _cap()
COUNTS_PATH = os.environ.get(
    "ATLAS_COUNTS_PATH",
    os.path.join(os.environ.get("ATLAS_AUTOPULL_STATE", "/var/lib/atlas/autopull"),
                 "last_counts.json"))
COUNTS_PATH_SRC = os.path.join(os.path.dirname(COUNTS_PATH),
                               f"last_counts_{SOURCE_NAME}.json")


def log(msg):
    print(f"[{SOURCE_NAME}] {msg}", flush=True)


def _join_addr(*parts):
    vals = [str(p).strip() for p in parts if p not in (None, "")]
    return " ".join(vals).strip() or None


def _coords(r):
    for key in ("location", "location_1", "georeference", "the_geom", "geocoded_column"):
        g = r.get(key)
        if isinstance(g, dict):
            coords = g.get("coordinates")
            if isinstance(coords, list) and len(coords) == 2:
                try:
                    return float(coords[1]), float(coords[0])  # GeoJSON [lon,lat]
                except (TypeError, ValueError):
                    pass
            lat, lon = g.get("latitude"), g.get("longitude")
            if lat and lon:
                try:
                    return float(lat), float(lon)
                except (TypeError, ValueError):
                    pass
    return None, None


def _g(row, *keys):
    for k in keys:
        if k in row and str(row[k]).strip():
            return str(row[k]).strip()
    return None


def map_newbiz(row):
    """Generic SoS new-registration row -> normalized business dict (firmographic only)."""
    lat, lon = _coords(row)
    name = _g(row, "business_name", "entity_name", "name", "legal_name", "legalentityname",
              "businessname", "corporation_name")
    addr = _join_addr(_g(row, "address_", "address", "address_1", "street", "addr1",
                          "mailing_address"),
                      _g(row, "address_continued", "address_2", "addr2"))
    entity_type = _g(row, "entity_type", "business_type", "type", "entitytype")
    return {
        "ext_id":   _g(row, "registry_number", "filing_number", "id", "ubi",
                       "registration", "entity_number"),
        "name":     name,
        "category": entity_type or "New business registration",
        "address":  addr,
        "locality": _g(row, "city", "locality", "town"),
        "region":   _g(row, "state", "region", "province") or None,
        "postcode": _g(row, "zip_code", "zip", "postcode", "postal_code"),
        "country":  "US",
        "lat": lat, "lon": lon,
        "website": None, "phone": None, "email": None,   # firmographic only; no contact derived
    }


def classify_signal(_norm):
    """Every row from these datasets is a new-registration signal (ICP tag)."""
    return "new_registration"


# --------------------------------------------------------------------------- #
# --selftest : pure logic, no network / no DB
# --------------------------------------------------------------------------- #
def selftest():
    sample = {
        "business_name": "ACME WIDGETS LLC", "entity_type": "Domestic Limited Liability Company",
        "registry_number": "1234567-99", "registry_date": "2026-06-01T00:00:00.000",
        "city": "Portland", "state": "OR", "zip_code": "97201", "address_": "123 SW Main St",
    }
    n = map_newbiz(sample)
    assert n["name"] == "ACME WIDGETS LLC", n
    assert n["ext_id"] == "1234567-99", n
    assert n["locality"] == "Portland" and n["region"] == "OR", n
    assert n["country"] == "US" and n["email"] is None, n
    assert classify_signal(n) == "new_registration"
    # ext-id namespacing is stable + deterministic
    eid = f"{SOURCE_NAME}:data.oregon.gov:{n['ext_id']}"
    assert eid == "sos_new_business:data.oregon.gov:1234567-99", eid
    # a row with no registry_number falls back to a content hash (still deterministic)
    n2 = map_newbiz({"business_name": "NoNumber Co", "city": "Salem", "state": "OR"})
    h = hashlib.sha1(json.dumps(n2, sort_keys=True).encode()).hexdigest()[:16]
    assert len(h) == 16
    print("[sos_new_business] --selftest OK (mapper + ext-id + classifier)")
    sys.exit(0)


# --------------------------------------------------------------------------- #
# DB env + connection + introspection (verbatim pattern from the live importers)
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
        "biz_name": norm.get("name"), "biz_lat": norm.get("lat"), "biz_lon": norm.get("lon"),
        "biz_category": norm.get("category"), "biz_website": norm.get("website"),
        "biz_phone": norm.get("phone"), "biz_email": norm.get("email"),
        "biz_address": norm.get("address"), "biz_locality": norm.get("locality"),
        "biz_region": norm.get("region"), "biz_postcode": norm.get("postcode"),
        "biz_country": norm.get("country"), "biz_source": source_name,
        "biz_ext_id": ext_id_ns, "biz_created": now, "biz_updated": now,
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


def fetch_page(domain, fxf, offset):
    qs = {"$limit": PAGE_SIZE, "$offset": offset, "$order": ":id"}
    url = f"https://{domain}/resource/{fxf}.json?" + urllib.parse.urlencode(qs)
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN
    req = urllib.request.Request(url, headers=headers)
    resp = urllib.request.urlopen(req, timeout=HTTP_TIMEOUT)
    return json.loads(resp.read().decode("utf-8", "replace"))


def ext_id_for(domain, norm):
    base = norm.get("ext_id")
    if base:
        return f"{SOURCE_NAME}:{domain}:{base}"
    h = hashlib.sha1(json.dumps(norm, sort_keys=True, default=str).encode()).hexdigest()[:16]
    return f"{SOURCE_NAME}:{domain}:{h}"


def main():
    if "--selftest" in sys.argv:
        selftest()

    load_db_env(DB_ENV_PATH)
    conn = connect_pg()
    cur = conn.cursor()
    import psycopg2  # noqa: F401  (already imported in connect_pg; for the except below)
    from psycopg2 import Error as PgError

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
    datasets = _datasets()
    total_fetched = 0
    inserted_biz = 0
    inserted_sr = 0
    per_ds = {}
    log(f"datasets={[d[2] for d in datasets]} cap={ROW_CAP}")

    for domain, fxf, label in datasets:
        if total_fetched >= ROW_CAP:
            break
        n_ds = 0
        offset = 0
        try:
            while total_fetched < ROW_CAP:
                rows = fetch_page(domain, fxf, offset)
                if not rows:
                    break
                for raw in rows:
                    if total_fetched >= ROW_CAP:
                        break
                    total_fetched += 1
                    n_ds += 1
                    norm = map_newbiz(raw)
                    if not norm.get("name"):
                        continue
                    ext_id_ns = ext_id_for(domain, norm)
                    biz_row = build_business_row(norm, SOURCE_NAME, ext_id_ns, biz_cols, now)
                    biz_id = None
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
                                    inserted_biz += 1
                        except PgError as e:
                            conn.rollback()
                            log(f"[{label}] business insert error {ext_id_ns}: {e.pgerror or e}")
                            continue
                    if biz_id is None and biz_pk and biz_ext_col:
                        cur.execute(
                            f'SELECT "{biz_pk}" FROM "{bs}"."{bt}" WHERE "{biz_ext_col}"=%s LIMIT 1',
                            (ext_id_ns,))
                        got = cur.fetchone()
                        biz_id = got[0] if got else None
                    sr_row = {}
                    if sr_fk_col and biz_id is not None:
                        sr_row[sr_fk_col] = biz_id
                    if sr_src_col:
                        sr_row[sr_src_col] = SOURCE_NAME
                    if sr_ext_col:
                        sr_row[sr_ext_col] = ext_id_ns
                    if sr_pay_col:
                        payload = dict(raw)
                        payload["_atlas_source"] = SOURCE_NAME
                        payload["_atlas_dataset"] = f"{domain}:{fxf}"
                        payload["_atlas_signal"] = classify_signal(norm)
                        sr_row[sr_pay_col] = json.dumps(payload, default=str)
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
                            params = vals + [SOURCE_NAME, ext_id_ns]
                        else:
                            sql = make_insert(ss, st_, cols, sr_conflict)
                            params = vals
                        try:
                            cur.execute(sql, params)
                            if cur.rowcount and cur.rowcount > 0:
                                inserted_sr += cur.rowcount
                        except PgError as e:
                            conn.rollback()
                            log(f"[{label}] source_record insert error {ext_id_ns}: {e.pgerror or e}")
                            continue
                    if total_fetched % BATCH_SIZE == 0:
                        conn.commit()
                        log(f"... committed at {total_fetched} fetched "
                            f"(biz+{inserted_biz}, sr+{inserted_sr})")
                conn.commit()
                if len(rows) < PAGE_SIZE:
                    break
                offset += PAGE_SIZE
                time.sleep(REQ_SLEEP)
            per_ds[label] = n_ds
            log(f"[{label}] done: {n_ds} rows read")
        except urllib.error.HTTPError as e:
            conn.rollback()
            per_ds[label] = f"HTTP {e.code}"
            log(f"[{label}] HTTP {e.code}: {e.reason} -- skip dataset")
        except Exception as e:                       # noqa: BLE001
            conn.rollback()
            per_ds[label] = f"ERR {e}"
            log(f"[{label}] FAILED: {e}")
        time.sleep(REQ_SLEEP)

    cur.execute(f'SELECT count(*) FROM "{bs}"."{bt}"')
    total_biz = cur.fetchone()[0]
    if sr_src_col:
        cur.execute(f'SELECT count(*) FROM "{ss}"."{st_}" WHERE "{sr_src_col}"=%s', (SOURCE_NAME,))
        src_landed = cur.fetchone()[0]
    else:
        cur.execute(f'SELECT count(*) FROM "{ss}"."{st_}"')
        src_landed = cur.fetchone()[0]

    log("=" * 70)
    log(f"SUMMARY fetched={total_fetched} new_business={inserted_biz} "
        f"new_source_record={inserted_sr}")
    log(f"per_dataset={per_ds}")
    log(f"READ-BACK atlas.business total={total_biz}; {SOURCE_NAME} source_records={src_landed}")
    log("=" * 70)

    counts = {
        "lane": SOURCE_NAME, "cap": ROW_CAP, "total_fetched": total_fetched,
        "business_total": total_biz, f"{SOURCE_NAME}_source_records": src_landed,
        "new_business": inserted_biz, "new_source_record": inserted_sr,
        "per_dataset": per_ds, "ts": int(time.time()),
    }
    write_counts(counts)
    cur.close()
    conn.close()

    if total_fetched == 0:
        log("FAIL: 0 rows fetched from ANY SoS new-business dataset.")
        sys.exit(2)
    if src_landed == 0:
        log(f"FAIL: 0 {SOURCE_NAME} rows present in atlas.source_record after run.")
        sys.exit(3)
    log(f"PASS: fetched={total_fetched}; {SOURCE_NAME} source_records={src_landed}; "
        f"business_total={total_biz}.")
    sys.exit(0)


if __name__ == "__main__":
    main()
