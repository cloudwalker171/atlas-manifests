#!/opt/atlas/venv/bin/python
"""
socrata_import.py  --  NO-AUTH public open-data "business" -> atlas Postgres importer

Why Socrata (and not Overture parquet/S3) for the "real data tonight" path:
  Socrata SODA APIs are plain anonymous HTTPS JSON -- no account, no token, no
  signing, no parquet/DuckDB/S3 plumbing. The two datasets below were verified
  returning live business records (name + discrete address fields) on the same
  day this script was authored, and both carry current-week records, so they are
  actively refreshed:

    * Chicago Business Licenses        data.cityofchicago.org/resource/r5kz-chrr.json
    * NYC DCWP Legally Operating Biz   data.cityofnewyork.us/resource/w7w3-xahh.json

  Licensing: both are public-domain / open municipal data.

What this script does (network = stdlib urllib only; DB = psycopg2):
  * For each configured dataset, pages over the SODA API with $limit/$offset and
    a stable $order=:id, up to a per-dataset cap (default 5000; argv[1] or
    SOCRATA_LIMIT override).
  * Maps each record into atlas.business (the entity) + atlas.source_record
    (provenance row, source='socrata_chicago' / 'socrata_nyc'), idempotently
    (ON CONFLICT / NOT EXISTS).
  * Commits in batches; prints VERBOSE per-stage counts (endpoint, HTTP status,
    fetched, mapped, inserted) per dataset.
  * Writes /var/lib/atlas/autopull/last_counts.json so the autopull report()
    surfaces fetched/inserted counts in status/<node>/seq-3-*.json.
  * FAIL-LOUD: exits NON-ZERO if nothing was fetched from any source, or if the
    read-back shows 0 socrata rows in atlas.source_record (so the pipe marks the
    apply FAILED, never "healthy with 0"). It does NOT fail merely because an
    idempotent re-run inserted 0 new rows -- that would wedge the retry loop.

Connection handling mirrors overture_pg_import.py: it sources /etc/atlas/db.env
(KEY=VALUE, optional leading `export`) and builds a psycopg2 connection from the
standard PG* / DB_* variables.

-----------------------------------------------------------------------------
Column mapping is schema-introspected (same approach as overture_pg_import.py):
the live atlas.business / atlas.source_record column names are read from
information_schema.columns and INSERTs only target columns that actually exist,
choosing from a candidate-name list per logical field (CANDIDATES below).
Idempotency uses a real single-column UNIQUE/PK if present, else a NOT EXISTS
guard. The business/source external id is namespaced per source
("socrata_chicago:<id>") so ids from different sources never collide.
-----------------------------------------------------------------------------
"""

import os
import sys
import json
import time
import datetime
import urllib.parse
import urllib.request
import urllib.error

import psycopg2


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
DB_ENV_PATH    = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
BUSINESS_TBL   = ("atlas", "business")
SOURCE_TBL     = ("atlas", "source_record")
BATCH_SIZE     = int(os.environ.get("SOCRATA_BATCH", "1000"))
PAGE_SIZE      = int(os.environ.get("SOCRATA_PAGE", "1000"))
HTTP_TIMEOUT   = int(os.environ.get("SOCRATA_HTTP_TIMEOUT", "60"))
USER_AGENT     = "atlas-socrata-import/1.0 (+https://github.com/cloudwalker171/atlas-manifests)"

# Per-dataset row cap (the "first run" cap). argv[1] wins, then SOCRATA_LIMIT,
# else 5000/dataset -> 10k total across the two datasets (approved default).
def _cap():
    if len(sys.argv) > 1 and str(sys.argv[1]).strip():
        try:
            return int(sys.argv[1])
        except ValueError:
            pass
    return int(os.environ.get("SOCRATA_LIMIT", "5000") or "5000")

ROW_CAP = _cap()

# Where the autopull report() reads counts from for status-back.
COUNTS_PATH = os.environ.get(
    "ATLAS_COUNTS_PATH",
    os.path.join(os.environ.get("ATLAS_AUTOPULL_STATE", "/var/lib/atlas/autopull"),
                 "last_counts.json"),
)


def log(msg):
    print(f"[socrata_import] {msg}", flush=True)


# --------------------------------------------------------------------------- #
# Dataset definitions  (host + Socrata 4x4 + field mapping)
# --------------------------------------------------------------------------- #
def _join_addr(*parts):
    vals = [str(p).strip() for p in parts if p not in (None, "")]
    return " ".join(vals).strip() or None


def map_chicago(r):
    return {
        "ext_id":   r.get("id") or r.get("license_number"),
        "name":     r.get("doing_business_as_name") or r.get("legal_name"),
        "category": r.get("license_description"),
        "address":  r.get("address"),
        "locality": r.get("city"),
        "region":   r.get("state"),
        "postcode": r.get("zip_code"),
        "country":  "US",
        "lat":      r.get("latitude"),
        "lon":      r.get("longitude"),
        "website":  None,
        "phone":    None,
        "email":    None,
    }


def map_nyc(r):
    return {
        "ext_id":   r.get("license_nbr") or r.get("business_unique_id"),
        "name":     r.get("dba_trade_name") or r.get("business_name"),
        "category": r.get("business_category"),
        "address":  _join_addr(r.get("address_building"), r.get("address_street_name")),
        "locality": r.get("address_city") or r.get("address_borough"),
        "region":   r.get("address_state"),
        "postcode": r.get("address_zip"),
        "country":  "US",
        "lat":      r.get("latitude"),
        "lon":      r.get("longitude"),
        "website":  None,
        "phone":    r.get("contact_phone"),
        "email":    None,
    }


DATASETS = [
    {
        "key":    "chicago",
        "source": "socrata_chicago",
        "url":    "https://data.cityofchicago.org/resource/r5kz-chrr.json",
        "map":    map_chicago,
    },
    {
        "key":    "nyc",
        "source": "socrata_nyc",
        "url":    "https://data.cityofnewyork.us/resource/w7w3-xahh.json",
        "map":    map_nyc,
    },
]


# --------------------------------------------------------------------------- #
# DB env loading (mirrors overture_pg_import.py: source /etc/atlas/db.env)
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
# Schema introspection (identical strategy to overture_pg_import.py)
# --------------------------------------------------------------------------- #
def table_columns(cur, schema, table):
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return {r[0] for r in cur.fetchall()}


def table_pk(cur, schema, table):
    cur.execute(
        "SELECT a.attname FROM pg_index i "
        "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
        "WHERE i.indrelid = %s::regclass AND i.indisprimary",
        (f"{schema}.{table}",),
    )
    rows = [r[0] for r in cur.fetchall()]
    return rows[0] if len(rows) == 1 else None


def unique_columns(cur, schema, table):
    cur.execute(
        "SELECT a.attname FROM pg_index i "
        "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
        "WHERE i.indrelid = %s::regclass AND (i.indisunique OR i.indisprimary)",
        (f"{schema}.{table}",),
    )
    return {r[0] for r in cur.fetchall()}


def pick_col(colset, candidates):
    for c in candidates:
        if c in colset:
            return c
    return None


CANDIDATES = {
    # atlas.business
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
    # atlas.source_record
    "sr_business_fk":["business_id", "biz_id", "business"],
    "sr_source":     ["source", "data_source", "origin"],
    "sr_ext_id":     ["source_id", "source_record_id", "external_id", "record_id", "ext_id"],
    "sr_payload":    ["raw", "payload", "data", "raw_json", "raw_jsonb", "doc", "record"],
    "sr_created":    ["created_at", "inserted_at", "fetched_at", "created"],
}


# --------------------------------------------------------------------------- #
# Socrata fetch (anonymous HTTPS JSON, $limit/$offset paginated, $order=:id)
# --------------------------------------------------------------------------- #
def fetch_socrata(ds, cap):
    """Yield raw record dicts from a Socrata dataset, up to `cap` rows. VERBOSE."""
    fetched = 0
    offset = 0
    base = ds["url"]
    log(f"[{ds['key']}] endpoint = {base}  (cap={cap}, page={PAGE_SIZE})")
    while fetched < cap:
        page = min(PAGE_SIZE, cap - fetched)
        qs = urllib.parse.urlencode({"$limit": page, "$offset": offset, "$order": ":id"})
        url = f"{base}?{qs}"
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                   "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                status = resp.getcode()
                body = resp.read()
        except urllib.error.HTTPError as e:
            log(f"[{ds['key']}] HTTP {e.code} at offset={offset}: {e.reason} -- stopping this dataset")
            break
        except urllib.error.URLError as e:
            log(f"[{ds['key']}] NETWORK ERROR at offset={offset}: {e.reason} -- stopping this dataset")
            break

        try:
            rows = json.loads(body)
        except json.JSONDecodeError as e:
            log(f"[{ds['key']}] JSON parse error at offset={offset}: {e} -- stopping this dataset")
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


def first_or_none(seq):
    if seq is None:
        return None
    if isinstance(seq, (list, tuple)):
        return seq[0] if len(seq) else None
    return seq


def build_business_row(norm, source_name, ext_id_ns, biz_cols, now):
    field_vals = {
        "biz_name":     norm.get("name"),
        "biz_lat":      norm.get("lat"),
        "biz_lon":      norm.get("lon"),
        "biz_category": norm.get("category"),
        "biz_website":  norm.get("website"),
        "biz_phone":    norm.get("phone"),
        "biz_email":    norm.get("email"),
        "biz_address":  norm.get("address"),
        "biz_locality": norm.get("locality"),
        "biz_region":   norm.get("region"),
        "biz_postcode": norm.get("postcode"),
        "biz_country":  norm.get("country"),
        "biz_source":   source_name,
        "biz_ext_id":   ext_id_ns,
        "biz_created":  now,
        "biz_updated":  now,
    }
    out = {}
    for logical, val in field_vals.items():
        col = pick_col(biz_cols, CANDIDATES[logical])
        if col and col not in out:
            out[col] = val
    return out


def json_default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()
    return str(o)


def make_insert(schema, table, cols, conflict_col):
    col_list = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    base = f'INSERT INTO "{schema}"."{table}" ({col_list}) VALUES ({placeholders})'
    if conflict_col:
        base += f' ON CONFLICT ("{conflict_col}") DO NOTHING'
    return base


def write_counts(counts):
    try:
        os.makedirs(os.path.dirname(COUNTS_PATH), exist_ok=True)
        with open(COUNTS_PATH, "w", encoding="utf-8") as fh:
            json.dump(counts, fh)
        log(f"wrote counts -> {COUNTS_PATH}")
    except OSError as e:
        log(f"WARNING: could not write {COUNTS_PATH}: {e}")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    load_db_env(DB_ENV_PATH)
    conn = connect_pg()
    cur = conn.cursor()

    biz_schema, biz_table = BUSINESS_TBL
    sr_schema, sr_table = SOURCE_TBL

    biz_cols = table_columns(cur, biz_schema, biz_table)
    sr_cols  = table_columns(cur, sr_schema, sr_table)
    if not biz_cols:
        raise SystemExit(f"ERROR: {biz_schema}.{biz_table} not found / no columns.")
    if not sr_cols:
        raise SystemExit(f"ERROR: {sr_schema}.{sr_table} not found / no columns.")

    biz_pk      = table_pk(cur, biz_schema, biz_table)
    biz_uniques = unique_columns(cur, biz_schema, biz_table)
    sr_uniques  = unique_columns(cur, sr_schema, sr_table)

    biz_ext_col = pick_col(biz_cols, CANDIDATES["biz_ext_id"])
    sr_fk_col   = pick_col(sr_cols, CANDIDATES["sr_business_fk"])
    sr_src_col  = pick_col(sr_cols, CANDIDATES["sr_source"])
    sr_ext_col  = pick_col(sr_cols, CANDIDATES["sr_ext_id"])
    sr_pay_col  = pick_col(sr_cols, CANDIDATES["sr_payload"])
    sr_cre_col  = pick_col(sr_cols, CANDIDATES["sr_created"])

    biz_conflict = biz_ext_col if (biz_ext_col and biz_ext_col in biz_uniques) else None
    sr_conflict  = sr_ext_col if (sr_ext_col and sr_ext_col in sr_uniques) else None

    log(f"atlas.business cols: name->{pick_col(biz_cols, CANDIDATES['biz_name'])}, "
        f"lat->{pick_col(biz_cols, CANDIDATES['biz_lat'])}, "
        f"lon->{pick_col(biz_cols, CANDIDATES['biz_lon'])}, "
        f"ext_id->{biz_ext_col}, pk->{biz_pk}, conflict->{biz_conflict}")
    log(f"atlas.source_record cols: fk->{sr_fk_col}, source->{sr_src_col}, "
        f"ext_id->{sr_ext_col}, payload->{sr_pay_col}, conflict->{sr_conflict}")

    now = datetime.datetime.now(datetime.timezone.utc)
    per_ds = {}            # key -> {fetched, mapped, inserted_biz, inserted_sr, error}
    total_fetched = 0

    for ds in DATASETS:
        key, source_name = ds["key"], ds["source"]
        stat = {"fetched": 0, "mapped": 0, "inserted_biz": 0, "inserted_sr": 0, "error": None}
        per_ds[key] = stat
        log(f"==== dataset '{key}' (source='{source_name}') ====")
        try:
            for raw in fetch_socrata(ds, ROW_CAP):
                stat["fetched"] += 1
                total_fetched += 1

                norm = ds["map"](raw)
                if not norm.get("name") or not norm.get("ext_id"):
                    continue   # skip records with no usable name/id
                stat["mapped"] += 1
                ext_id_ns = f"{source_name}:{norm['ext_id']}"

                # ---- atlas.business ---------------------------------------- #
                biz_row = build_business_row(norm, source_name, ext_id_ns, biz_cols, now)
                biz_id = None
                if biz_row:
                    cols = list(biz_row.keys())
                    vals = [biz_row[c] for c in cols]
                    insert_sql = make_insert(biz_schema, biz_table, cols, biz_conflict)
                    if biz_pk:
                        insert_sql += f' RETURNING "{biz_pk}"'
                    try:
                        cur.execute(insert_sql, vals)
                        if biz_pk:
                            got = cur.fetchone()
                            if got:
                                biz_id = got[0]
                                stat["inserted_biz"] += 1
                    except psycopg2.Error as e:
                        conn.rollback()
                        log(f"[{key}] business insert error for {ext_id_ns}: {e.pgerror or e}")
                        continue

                if biz_id is None and biz_pk and biz_ext_col:
                    cur.execute(
                        f'SELECT "{biz_pk}" FROM "{biz_schema}"."{biz_table}" '
                        f'WHERE "{biz_ext_col}" = %s LIMIT 1',
                        (ext_id_ns,),
                    )
                    got = cur.fetchone()
                    biz_id = got[0] if got else None

                # ---- atlas.source_record ----------------------------------- #
                sr_row = {}
                if sr_fk_col and biz_id is not None:
                    sr_row[sr_fk_col] = biz_id
                if sr_src_col:
                    sr_row[sr_src_col] = source_name
                if sr_ext_col:
                    sr_row[sr_ext_col] = ext_id_ns
                if sr_pay_col:
                    payload = dict(raw)
                    payload["_atlas_source"] = source_name
                    sr_row[sr_pay_col] = json.dumps(payload, default=json_default)
                if sr_cre_col:
                    sr_row[sr_cre_col] = now

                if sr_row:
                    cols = list(sr_row.keys())
                    vals = [sr_row[c] for c in cols]
                    if sr_conflict is None and sr_src_col and sr_ext_col:
                        col_list = ", ".join(f'"{c}"' for c in cols)
                        ph = ", ".join(["%s"] * len(cols))
                        sql = (
                            f'INSERT INTO "{sr_schema}"."{sr_table}" ({col_list}) '
                            f'SELECT {ph} WHERE NOT EXISTS ('
                            f'  SELECT 1 FROM "{sr_schema}"."{sr_table}" '
                            f'  WHERE "{sr_src_col}" = %s AND "{sr_ext_col}" = %s)'
                        )
                        params = vals + [source_name, ext_id_ns]
                    else:
                        sql = make_insert(sr_schema, sr_table, cols, sr_conflict)
                        params = vals
                    try:
                        cur.execute(sql, params)
                        if cur.rowcount and cur.rowcount > 0:
                            stat["inserted_sr"] += cur.rowcount
                    except psycopg2.Error as e:
                        conn.rollback()
                        log(f"[{key}] source_record insert error for {ext_id_ns}: {e.pgerror or e}")
                        continue

                if stat["fetched"] % BATCH_SIZE == 0:
                    conn.commit()
                    log(f"[{key}] ... committed at {stat['fetched']} fetched "
                        f"(business+{stat['inserted_biz']}, source_record+{stat['inserted_sr']})")

            conn.commit()
            log(f"[{key}] DONE: fetched={stat['fetched']} mapped={stat['mapped']} "
                f"business+{stat['inserted_biz']} source_record+{stat['inserted_sr']}")
        except Exception as e:                       # noqa: BLE001 -- never let one dataset kill the run
            conn.rollback()
            stat["error"] = str(e)
            log(f"[{key}] DATASET FAILED: {e}")

    # ---- read-back counts ------------------------------------------------- #
    cur.execute(f'SELECT count(*) FROM "{biz_schema}"."{biz_table}"')
    total_biz = cur.fetchone()[0]

    sources = [ds["source"] for ds in DATASETS]
    per_source_landed = {}
    socrata_sr_total = 0
    if sr_src_col:
        for src in sources:
            cur.execute(
                f'SELECT count(*) FROM "{sr_schema}"."{sr_table}" WHERE "{sr_src_col}" = %s',
                (src,),
            )
            c = cur.fetchone()[0]
            per_source_landed[src] = c
            socrata_sr_total += c
    else:
        cur.execute(f'SELECT count(*) FROM "{sr_schema}"."{sr_table}"')
        socrata_sr_total = cur.fetchone()[0]

    log("======================================================================")
    for ds in DATASETS:
        k, src = ds["key"], ds["source"]
        s = per_ds[k]
        log(f"SUMMARY[{k}] fetched={s['fetched']} mapped={s['mapped']} "
            f"new_business={s['inserted_biz']} new_source_record={s['inserted_sr']} "
            f"landed_total({src})={per_source_landed.get(src, '?')}"
            + (f" ERROR={s['error']}" if s['error'] else ""))
    log(f"READ-BACK: atlas.business total = {total_biz}")
    log(f"READ-BACK: atlas.source_record (socrata sources) = {socrata_sr_total}")
    log("======================================================================")

    # ---- counts file for autopull status-back ----------------------------- #
    counts = {
        "lane": "socrata",
        "cap_per_dataset": ROW_CAP,
        "total_fetched": total_fetched,
        "business_total": total_biz,
        "socrata_source_records": socrata_sr_total,
        "per_dataset": {k: {"fetched": v["fetched"], "mapped": v["mapped"],
                            "new_business": v["inserted_biz"],
                            "new_source_record": v["inserted_sr"],
                            "error": v["error"]} for k, v in per_ds.items()},
        "landed_by_source": per_source_landed,
        "ts": int(time.time()),
    }
    write_counts(counts)

    cur.close()
    conn.close()

    # ---- FAIL-LOUD verdict ------------------------------------------------- #
    if total_fetched == 0:
        log("FAIL: 0 records fetched from ANY source (pipeline broken / sources unreachable).")
        sys.exit(2)
    if socrata_sr_total == 0:
        log("FAIL: 0 socrata rows present in atlas.source_record after run (nothing landed).")
        sys.exit(3)
    log(f"PASS: fetched={total_fetched}; atlas.source_record socrata rows={socrata_sr_total}; "
        f"atlas.business total={total_biz}.")
    sys.exit(0)


if __name__ == "__main__":
    main()
