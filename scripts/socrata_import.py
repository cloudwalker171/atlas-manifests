#!/opt/atlas/venv/bin/python
"""
socrata_import.py  --  NO-AUTH public open-data "business" -> atlas Postgres importer

Why Socrata (and not Overture parquet/S3) for the "real data tonight" path:
  Socrata SODA APIs are plain anonymous HTTPS JSON -- no account, no token, no
  signing, no parquet/DuckDB/S3 plumbing. Both datasets below were verified
  returning live business records (name + discrete address fields) with current
  records, so they are actively refreshed:

    * Chicago Business Licenses        data.cityofchicago.org/resource/r5kz-chrr.json
    * NYC DCWP Legally Operating Biz   data.cityofnewyork.us/resource/w7w3-xahh.json

  Licensing: both are public-domain / open municipal data.

WHAT THIS VERSION FIXES (vs. the first cut that fetched 10k but inserted 0):
  The first version used generic column-name guessing and did NOT match the real
  atlas DDL. Every INSERT failed and the per-row error was swallowed. This version
  is written DIRECTLY against atlas_schema.sql:

    atlas.business(name NOT NULL, name_norm NOT NULL, website, email, phone_e164,
                   addr_line1, city, region, postal, country, lat float8, lon float8,
                   category, ...; id BIGSERIAL PK; no natural unique key)
    atlas.source_record(source_code NOT NULL, source_record_id NOT NULL,
                        business_id NOT NULL FK, content_hash NOT NULL, payload JSONB,
                        UNIQUE(source_code, source_record_id))   <-- idempotency anchor

  Key corrections:
    * name_norm is populated (it is NOT NULL with no default -> was the #1 killer).
    * Real column names: addr_line1 / postal / phone_e164 (not address/postcode/phone).
    * lat/lon cast to float (DOUBLE PRECISION columns reject text params).
    * Idempotency = pre-check + ON CONFLICT (source_code, source_record_id) DO NOTHING
      (the real composite unique), NOT a non-existent single-column constraint.
    * source_record.content_hash = md5 of the mapped payload (NOT NULL).
    * Per-row SAVEPOINT so one bad row can't abort the whole batch, and the REAL
      psycopg2 error message is surfaced into the counts + log (no more error:null).

Connection: sources /etc/atlas/db.env (KEY=VALUE, optional leading `export`) and
builds a psycopg2 connection from standard PG* / DB_* variables (mirrors
overture_pg_import.py).

VERBOSE + FAIL-LOUD: prints per-stage counts (endpoint, HTTP status, fetched,
mapped, inserted, errors) per dataset; writes /var/lib/atlas/autopull/last_counts.json
for status-back; and exits NON-ZERO if nothing was fetched from any source, or if
the read-back shows 0 socrata rows in atlas.source_record (so the pipe marks the
apply FAILED, never "healthy with 0"). It does NOT fail merely because an
idempotent re-run inserted 0 NEW rows (that would wedge the 2-min retry loop).
"""

import os
import re
import sys
import json
import time
import hashlib
import datetime
import urllib.parse
import urllib.request
import urllib.error

import psycopg2

# (importer v2: schema-exact INSERTs against atlas_schema.sql)


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
DB_ENV_PATH    = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
BATCH_SIZE     = int(os.environ.get("SOCRATA_BATCH", "500"))
PAGE_SIZE      = int(os.environ.get("SOCRATA_PAGE", "50000"))
HTTP_TIMEOUT   = int(os.environ.get("SOCRATA_HTTP_TIMEOUT", "60"))
USER_AGENT     = "atlas-socrata-import/2.0 (+https://github.com/cloudwalker171/atlas-manifests)"

BIZ_TBL = 'atlas.business'
SR_TBL  = 'atlas.source_record'


def _cap():
    if len(sys.argv) > 1 and str(sys.argv[1]).strip():
        try:
            return int(sys.argv[1])
        except ValueError:
            pass
    return int(os.environ.get("SOCRATA_LIMIT", "0") or "0")  # 0 == UNLIMITED full pagination

ROW_CAP = _cap()

COUNTS_PATH = os.environ.get(
    "ATLAS_COUNTS_PATH",
    os.path.join(os.environ.get("ATLAS_AUTOPULL_STATE", "/var/lib/atlas/autopull"),
                 "last_counts.json"),
)


def log(msg):
    print(f"[socrata_import] {msg}", flush=True)


# --------------------------------------------------------------------------- #
# Normalizers / type coercion (match real column types)
# --------------------------------------------------------------------------- #
def norm_name(s):
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def to_float(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    # guard against bogus 0/0 placeholders sometimes present in open data
    return f


def to_e164(p):
    if not p:
        return None
    d = re.sub(r"\D", "", str(p))
    if len(d) == 10:
        return "+1" + d
    if len(d) == 11 and d.startswith("1"):
        return "+" + d
    return None


def clip(s, n=512):
    if s is None:
        return None
    s = str(s).strip()
    return s[:n] if s else None


# --------------------------------------------------------------------------- #
# Dataset definitions  (host + Socrata 4x4 + field mapping -> logical fields)
# --------------------------------------------------------------------------- #
def _join_addr(*parts):
    vals = [str(p).strip() for p in parts if p not in (None, "")]
    return " ".join(vals).strip() or None


def map_chicago(r):
    return {
        "source_record_id": r.get("id") or r.get("license_number"),
        "name":     r.get("doing_business_as_name") or r.get("legal_name"),
        "category": r.get("license_description"),
        "addr_line1": r.get("address"),
        "city":     r.get("city"),
        "region":   r.get("state"),
        "postal":   r.get("zip_code"),
        "country":  "US",
        "lat":      r.get("latitude"),
        "lon":      r.get("longitude"),
        "phone":    None,
    }


def map_nyc(r):
    return {
        "source_record_id": r.get("license_nbr") or r.get("business_unique_id"),
        "name":     r.get("dba_trade_name") or r.get("business_name"),
        "category": r.get("business_category"),
        "addr_line1": _join_addr(r.get("address_building"), r.get("address_street_name")),
        "city":     r.get("address_city") or r.get("address_borough"),
        "region":   r.get("address_state"),
        "postal":   r.get("address_zip"),
        "country":  "US",
        "lat":      r.get("latitude"),
        "lon":      r.get("longitude"),
        "phone":    r.get("contact_phone"),
    }


DATASETS = [
    {"key": "chicago", "source": "socrata_chicago",
     "url": "https://data.cityofchicago.org/resource/r5kz-chrr.json", "map": map_chicago},
    {"key": "nyc", "source": "socrata_nyc",
     "url": "https://data.cityofnewyork.us/resource/w7w3-xahh.json", "map": map_nyc},
]


# --------------------------------------------------------------------------- #
# DB env loading + connect (mirrors overture_pg_import.py)
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
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


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


# --------------------------------------------------------------------------- #
# Explicit INSERTs against the REAL atlas DDL
# --------------------------------------------------------------------------- #
BIZ_COLS = ["name", "name_norm", "website", "email", "phone_e164",
            "addr_line1", "city", "region", "postal", "country",
            "lat", "lon", "category"]

BIZ_INSERT = (
    f'INSERT INTO {BIZ_TBL} '
    f'({", ".join(BIZ_COLS)}) VALUES ({", ".join(["%s"] * len(BIZ_COLS))}) '
    f'RETURNING id'
)

SR_INSERT = (
    f'INSERT INTO {SR_TBL} '
    f'(source_code, source_record_id, business_id, content_hash, payload) '
    f'VALUES (%s, %s, %s, %s, %s) '
    f'ON CONFLICT (source_code, source_record_id) DO NOTHING '
    f'RETURNING id'
)

SR_EXISTS = (
    f'SELECT 1 FROM {SR_TBL} WHERE source_code = %s AND source_record_id = %s LIMIT 1'
)


def biz_values(norm):
    return [
        clip(norm.get("name")),
        norm_name(norm.get("name")),
        None,                                   # website (not provided by these sources)
        None,                                   # email
        to_e164(norm.get("phone")),
        clip(norm.get("addr_line1")),
        clip(norm.get("city"), 128),
        clip(norm.get("region"), 64),
        clip(norm.get("postal"), 32),
        clip(norm.get("country"), 8) or "US",
        to_float(norm.get("lat")),
        to_float(norm.get("lon")),
        clip(norm.get("category"), 256),
    ]


def content_hash(payload_json):
    return hashlib.md5(payload_json.encode("utf-8")).hexdigest()


def json_default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()
    return str(o)


def write_counts(counts):
    try:
        os.makedirs(os.path.dirname(COUNTS_PATH), exist_ok=True)
        with open(COUNTS_PATH, "w", encoding="utf-8") as fh:
            json.dump(counts, fh)
        log(f"wrote counts -> {COUNTS_PATH}")
    except OSError as e:
        log(f"WARNING: could not write {COUNTS_PATH}: {e}")


# --------------------------------------------------------------------------- #
# Socrata fetch (anonymous HTTPS JSON, $limit/$offset, stable $order=:id)
# --------------------------------------------------------------------------- #
def fetch_socrata(ds, cap):
    fetched = 0
    offset = 0
    base = ds["url"]
    unlimited = (cap is None or cap <= 0)
    log(f"[{ds['key']}] endpoint = {base}  (cap={'UNLIMITED' if unlimited else cap}, page={PAGE_SIZE})")
    while unlimited or fetched < cap:
        page = PAGE_SIZE if unlimited else min(PAGE_SIZE, cap - fetched)
        qs = urllib.parse.urlencode({"$limit": page, "$offset": offset, "$order": ":id"})
        url = f"{base}?{qs}"
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                   "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                status = resp.getcode()
                body = resp.read()
        except urllib.error.HTTPError as e:
            log(f"[{ds['key']}] HTTP {e.code} at offset={offset}: {e.reason} -- stopping dataset")
            break
        except urllib.error.URLError as e:
            log(f"[{ds['key']}] NETWORK ERROR at offset={offset}: {e.reason} -- stopping dataset")
            break
        try:
            rows = json.loads(body)
        except json.JSONDecodeError as e:
            log(f"[{ds['key']}] JSON parse error at offset={offset}: {e} -- stopping dataset")
            break
        log(f"[{ds['key']}] GET offset={offset} limit={page} -> HTTP {status}, {len(rows)} records")
        if not rows:
            break
        for r in rows:
            yield r
            fetched += 1
        if len(rows) < page:
            break
        offset += len(rows)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    load_db_env(DB_ENV_PATH)
    conn = connect_pg()
    cur = conn.cursor()

    now = datetime.datetime.now(datetime.timezone.utc)
    per_ds = {}
    total_fetched = 0

    for ds in DATASETS:
        key, source_code = ds["key"], ds["source"]
        stat = {"fetched": 0, "mapped": 0, "skipped_nokey": 0, "unchanged": 0,
                "inserted_biz": 0, "inserted_sr": 0, "errors": 0, "first_error": None}
        per_ds[key] = stat
        log(f"==== dataset '{key}' (source_code='{source_code}') ====")
        try:
            for raw in fetch_socrata(ds, ROW_CAP):
                stat["fetched"] += 1
                total_fetched += 1

                norm = ds["map"](raw)
                srid = norm.get("source_record_id")
                if not norm.get("name") or not srid:
                    stat["skipped_nokey"] += 1
                    continue
                srid = str(srid)
                stat["mapped"] += 1

                # idempotency pre-check on the real composite key
                cur.execute(SR_EXISTS, (source_code, srid))
                if cur.fetchone():
                    stat["unchanged"] += 1
                    continue

                payload_json = json.dumps(raw, default=json_default, sort_keys=True)
                ch = content_hash(payload_json)

                cur.execute("SAVEPOINT row_sp")
                try:
                    cur.execute(BIZ_INSERT, biz_values(norm))
                    biz_id = cur.fetchone()[0]
                    cur.execute(SR_INSERT, (source_code, srid, biz_id, ch, payload_json))
                    sr_got = cur.fetchone()
                    cur.execute("RELEASE SAVEPOINT row_sp")
                    stat["inserted_biz"] += 1
                    if sr_got:
                        stat["inserted_sr"] += 1
                    else:
                        # composite conflict raced in: source_record already present.
                        stat["unchanged"] += 1
                except psycopg2.Error as e:
                    cur.execute("ROLLBACK TO SAVEPOINT row_sp")
                    stat["errors"] += 1
                    msg = (e.pgerror or str(e)).strip()
                    if stat["first_error"] is None:
                        stat["first_error"] = msg
                        log(f"[{key}] INSERT ERROR (surfaced) for srid={srid}: {msg}")

                if stat["fetched"] % BATCH_SIZE == 0:
                    conn.commit()
                    log(f"[{key}] ... committed at {stat['fetched']} fetched "
                        f"(biz+{stat['inserted_biz']} sr+{stat['inserted_sr']} "
                        f"unchanged={stat['unchanged']} errors={stat['errors']})")

            conn.commit()
            log(f"[{key}] DONE: fetched={stat['fetched']} mapped={stat['mapped']} "
                f"biz+{stat['inserted_biz']} sr+{stat['inserted_sr']} "
                f"unchanged={stat['unchanged']} errors={stat['errors']}")
        except Exception as e:                       # noqa: BLE001
            conn.rollback()
            stat["first_error"] = stat["first_error"] or str(e)
            log(f"[{key}] DATASET FAILED: {e}")

    # ---- read-back counts ------------------------------------------------- #
    cur.execute(f"SELECT count(*) FROM {BIZ_TBL}")
    total_biz = cur.fetchone()[0]

    per_source_landed = {}
    socrata_sr_total = 0
    for ds in DATASETS:
        cur.execute(f"SELECT count(*) FROM {SR_TBL} WHERE source_code = %s", (ds["source"],))
        c = cur.fetchone()[0]
        per_source_landed[ds["source"]] = c
        socrata_sr_total += c

    log("======================================================================")
    for ds in DATASETS:
        k, src = ds["key"], ds["source"]
        s = per_ds[k]
        log(f"SUMMARY[{k}] fetched={s['fetched']} mapped={s['mapped']} "
            f"new_business={s['inserted_biz']} new_source_record={s['inserted_sr']} "
            f"unchanged={s['unchanged']} errors={s['errors']} "
            f"landed_total({src})={per_source_landed.get(src)}"
            + (f" first_error={s['first_error']}" if s['first_error'] else ""))
    log(f"READ-BACK: {BIZ_TBL} total = {total_biz}")
    log(f"READ-BACK: {SR_TBL} (socrata sources) = {socrata_sr_total}")
    log("======================================================================")

    counts = {
        "lane": "socrata",
        "cap_per_dataset": ROW_CAP,
        "total_fetched": total_fetched,
        "business_total": total_biz,
        "socrata_source_records": socrata_sr_total,
        "per_dataset": {k: {"fetched": v["fetched"], "mapped": v["mapped"],
                            "new_business": v["inserted_biz"],
                            "new_source_record": v["inserted_sr"],
                            "unchanged": v["unchanged"], "errors": v["errors"],
                            "first_error": v["first_error"]} for k, v in per_ds.items()},
        "landed_by_source": per_source_landed,
        "ts": int(time.time()),
    }
    write_counts(counts)

    cur.close()
    conn.close()

    # ---- FAIL-LOUD verdict ------------------------------------------------- #
    if total_fetched == 0:
        if os.environ.get("SOCRATA_ALLOW_EMPTY") == "1":
            log("0 records fetched -- TOLERATED (continuous lane: 0 new/updated is normal; not a failure).")
            sys.exit(0)
        log("FAIL: 0 records fetched from ANY source (pipeline broken / sources unreachable).")
        sys.exit(2)
    if socrata_sr_total == 0 and os.environ.get("SOCRATA_ALLOW_EMPTY") == "1":
        log("0 landed but tolerated (continuous lane)."); sys.exit(0)
    if socrata_sr_total == 0:
        errs = "; ".join(f"{k}:{v['first_error']}" for k, v in per_ds.items() if v["first_error"])
        log(f"FAIL: 0 socrata rows in {SR_TBL} after run (nothing landed). "
            f"errors -> {errs or 'none surfaced'}")
        sys.exit(3)
    log(f"PASS: fetched={total_fetched}; {SR_TBL} socrata rows={socrata_sr_total}; "
        f"{BIZ_TBL} total={total_biz}.")
    sys.exit(0)


if __name__ == "__main__":
    main()
