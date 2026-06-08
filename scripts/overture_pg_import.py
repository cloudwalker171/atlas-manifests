#!/opt/atlas/venv/bin/python
"""
overture_pg_import.py  --  NO-AUTH Overture Maps "places" -> atlas Postgres importer

Why Overture instead of Foursquare bulk:
  Foursquare's bulk OS Places now sits behind a Hugging Face login/token. Overture
  Maps publishes the same class of POI data (Foursquare actually contributes its
  data into Overture, so coverage overlaps heavily) on a fully anonymous, public
  S3 bucket -- no account, no token, no signing:

      aws s3 ls --no-sign-request s3://overturemaps-us-west-2/release/

  Licensing is Apache-2.0 / ODbL-compatible (attribution: "(c) OpenStreetMap
  contributors, Overture Maps Foundation").

What this script does:
  * Reads ONE local Overture "places" GeoParquet file (downloaded out-of-band by
    run_overture_test.sh using the no-sign-request AWS CLI -- this script itself
    never touches the network / S3, it only reads a local .parquet via DuckDB).
  * Flattens the nested Overture columns (names.primary, categories.primary/.alternate,
    addresses[0].*, list-typed websites/socials/emails/phones, bbox->lon/lat).
  * Maps each place into atlas.business (the entity) + atlas.source_record
    (provenance row, source='overture'), idempotently (ON CONFLICT / NOT EXISTS).
  * Honors an OVERTURE_LIMIT row cap env var (the Overture analogue of FSQ_LIMIT).
  * Commits in batches and prints read-back counts at the end.

Connection handling mirrors fsq_pg_import.py: it sources /etc/atlas/db.env
(KEY=VALUE, with or without a leading `export`) and builds a psycopg2 connection
from the standard PG* / DB_* variables.

-----------------------------------------------------------------------------
IMPORTANT -- column mapping is schema-introspected.
Because atlas.business / atlas.source_record column names are defined on the box
(not visible from where this script was authored), the INSERTs are built
dynamically: the script reads information_schema.columns for each table and only
writes to columns that actually exist, choosing from a candidate-name list per
field (see CANDIDATES below). Idempotency uses a real unique constraint if one
exists on the source-record id, otherwise a NOT EXISTS guard. Adjust the
CANDIDATES lists if your fsq_pg_import.py uses different names -- everything else
is generic.
-----------------------------------------------------------------------------
"""

import os
import sys
import json
import datetime

import duckdb
import psycopg2
from psycopg2.extras import execute_values


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
DB_ENV_PATH   = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
SOURCE_NAME   = "overture"
BUSINESS_TBL  = ("atlas", "business")
SOURCE_TBL    = ("atlas", "source_record")
BATCH_SIZE    = int(os.environ.get("OVERTURE_BATCH", "1000"))

# Row cap -- Overture analogue of FSQ_LIMIT. 0 / unset == no cap.
ROW_LIMIT     = int(os.environ.get("OVERTURE_LIMIT", os.environ.get("FSQ_LIMIT", "0")) or "0")

# Local parquet path (downloaded by run_overture_test.sh). argv[1] wins.
PARQUET_PATH  = (sys.argv[1] if len(sys.argv) > 1 else
                 os.environ.get("OVERTURE_PARQUET", ""))


def log(msg):
    print(f"[overture_pg_import] {msg}", flush=True)


# --------------------------------------------------------------------------- #
# DB env loading (mirrors fsq_pg_import.py: source /etc/atlas/db.env)
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
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            os.environ.setdefault(key, val)


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
# Schema introspection
# --------------------------------------------------------------------------- #
def table_columns(cur, schema, table):
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return {r[0] for r in cur.fetchall()}


def table_pk(cur, schema, table):
    cur.execute(
        """
        SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = %s::regclass AND i.indisprimary
        """,
        (f"{schema}.{table}",),
    )
    rows = [r[0] for r in cur.fetchall()]
    return rows[0] if len(rows) == 1 else None


def unique_columns(cur, schema, table):
    """All column names that participate in any UNIQUE/PK index (single-col)."""
    cur.execute(
        """
        SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = %s::regclass AND (i.indisunique OR i.indisprimary)
        """,
        (f"{schema}.{table}",),
    )
    return {r[0] for r in cur.fetchall()}


def pick_col(colset, candidates):
    for c in candidates:
        if c in colset:
            return c
    return None


# Candidate column names per logical field. First match in the live table wins.
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
    "biz_confidence":["confidence", "confidence_score"],
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
# Read + flatten the Overture parquet via DuckDB
# --------------------------------------------------------------------------- #
def read_places(parquet_path, limit):
    if not parquet_path or not os.path.exists(parquet_path):
        raise SystemExit(f"ERROR: parquet not found: {parquet_path!r} "
                         f"(set OVERTURE_PARQUET or pass it as argv[1])")

    con = duckdb.connect()

    # Point coords come from bbox.xmin/ymin -- for point geometries this equals
    # the geometry X/Y and needs no spatial extension (keeps this dependency-free
    # and offline-safe). The raw WKB geometry is still preserved in the payload.
    limit_sql = f"LIMIT {int(limit)}" if limit and limit > 0 else ""
    sql = f"""
        SELECT
            id                                   AS ov_id,
            names.primary                        AS name,
            categories.primary                   AS category_primary,
            categories.alternate                 AS category_alternate,
            confidence                           AS confidence,
            websites                             AS websites,
            socials                              AS socials,
            emails                               AS emails,
            phones                               AS phones,
            addresses[1].freeform                AS addr_freeform,
            addresses[1].locality                AS addr_locality,
            addresses[1].region                  AS addr_region,
            addresses[1].postcode                AS addr_postcode,
            addresses[1].country                 AS addr_country,
            bbox.xmin                            AS lon,
            bbox.ymin                            AS lat
        FROM read_parquet('{parquet_path}')
        {limit_sql}
    """
    log(f"DuckDB reading {parquet_path} (limit={limit or 'none'}) ...")
    rel = con.execute(sql)
    cols = [d[0] for d in rel.description]
    for row in rel.fetchall():
        yield dict(zip(cols, row))
    con.close()


def first_or_none(seq):
    if seq is None:
        return None
    if isinstance(seq, (list, tuple)):
        return seq[0] if len(seq) else None
    return seq


def build_business_row(rec, biz_cols, now):
    """Map a flattened Overture record onto present atlas.business columns."""
    field_vals = {
        "biz_name":       rec.get("name"),
        "biz_lat":        rec.get("lat"),
        "biz_lon":        rec.get("lon"),
        "biz_category":   rec.get("category_primary"),
        "biz_website":    first_or_none(rec.get("websites")),
        "biz_phone":      first_or_none(rec.get("phones")),
        "biz_email":      first_or_none(rec.get("emails")),
        "biz_address":    rec.get("addr_freeform"),
        "biz_locality":   rec.get("addr_locality"),
        "biz_region":     rec.get("addr_region"),
        "biz_postcode":   rec.get("addr_postcode"),
        "biz_country":    rec.get("addr_country"),
        "biz_confidence": rec.get("confidence"),
        "biz_source":     SOURCE_NAME,
        "biz_ext_id":     rec.get("ov_id"),
        "biz_created":    now,
        "biz_updated":    now,
    }
    out = {}
    for logical, val in field_vals.items():
        col = pick_col(biz_cols, CANDIDATES[logical])
        if col and col not in out:
            out[col] = val
    return out


def build_payload(rec):
    """Full provenance payload (jsonb-ready)."""
    return {
        "id": rec.get("ov_id"),
        "name": rec.get("name"),
        "category_primary": rec.get("category_primary"),
        "category_alternate": rec.get("category_alternate"),
        "confidence": rec.get("confidence"),
        "websites": rec.get("websites"),
        "socials": rec.get("socials"),
        "emails": rec.get("emails"),
        "phones": rec.get("phones"),
        "address": {
            "freeform": rec.get("addr_freeform"),
            "locality": rec.get("addr_locality"),
            "region": rec.get("addr_region"),
            "postcode": rec.get("addr_postcode"),
            "country": rec.get("addr_country"),
        },
        "lon": rec.get("lon"),
        "lat": rec.get("lat"),
    }


def json_default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()
    return str(o)


# --------------------------------------------------------------------------- #
# Insert helpers (idempotent)
# --------------------------------------------------------------------------- #
def make_insert(schema, table, cols, conflict_col):
    col_list = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    base = f'INSERT INTO "{schema}"."{table}" ({col_list}) VALUES ({placeholders})'
    if conflict_col:
        base += f' ON CONFLICT ("{conflict_col}") DO NOTHING'
    return base


def main():
    if not PARQUET_PATH:
        raise SystemExit("ERROR: no parquet supplied. "
                         "Set OVERTURE_PARQUET=/path/to/part.parquet or pass argv[1].")

    load_db_env(DB_ENV_PATH)
    conn = connect_pg()
    cur = conn.cursor()

    biz_schema, biz_table = BUSINESS_TBL
    sr_schema, sr_table = SOURCE_TBL

    biz_cols   = table_columns(cur, biz_schema, biz_table)
    sr_cols    = table_columns(cur, sr_schema, sr_table)
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

    # Conflict targets for idempotency (only if a single-col unique exists).
    biz_conflict = biz_ext_col if (biz_ext_col and biz_ext_col in biz_uniques) else None
    sr_conflict  = sr_ext_col if (sr_ext_col and sr_ext_col in sr_uniques) else None

    log(f"atlas.business cols detected: name->{pick_col(biz_cols, CANDIDATES['biz_name'])}, "
        f"lat->{pick_col(biz_cols, CANDIDATES['biz_lat'])}, "
        f"lon->{pick_col(biz_cols, CANDIDATES['biz_lon'])}, "
        f"ext_id->{biz_ext_col}, pk->{biz_pk}, conflict->{biz_conflict}")
    log(f"atlas.source_record cols detected: fk->{sr_fk_col}, source->{sr_src_col}, "
        f"ext_id->{sr_ext_col}, payload->{sr_pay_col}, conflict->{sr_conflict}")

    now = datetime.datetime.now(datetime.timezone.utc)
    seen, inserted_biz, inserted_sr = 0, 0, 0

    for rec in read_places(PARQUET_PATH, ROW_LIMIT):
        seen += 1
        ov_id = rec.get("ov_id")

        # ---- atlas.business ------------------------------------------------ #
        biz_row = build_business_row(rec, biz_cols, now)
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
                    biz_id = got[0] if got else None
                    if got:
                        inserted_biz += 1
            except psycopg2.Error as e:
                conn.rollback()
                log(f"business insert error for {ov_id}: {e.pgerror or e}")
                continue

        # If ON CONFLICT DID NOTHING (no RETURNING row), look up existing id.
        if biz_id is None and biz_pk and biz_ext_col and ov_id is not None:
            cur.execute(
                f'SELECT "{biz_pk}" FROM "{biz_schema}"."{biz_table}" '
                f'WHERE "{biz_ext_col}" = %s LIMIT 1',
                (ov_id,),
            )
            got = cur.fetchone()
            biz_id = got[0] if got else None

        # ---- atlas.source_record ------------------------------------------ #
        if sr_cols:
            sr_row = {}
            if sr_fk_col and biz_id is not None:
                sr_row[sr_fk_col] = biz_id
            if sr_src_col:
                sr_row[sr_src_col] = SOURCE_NAME
            if sr_ext_col:
                sr_row[sr_ext_col] = ov_id
            if sr_pay_col:
                sr_row[sr_pay_col] = json.dumps(build_payload(rec), default=json_default)
            if sr_cre_col:
                sr_row[sr_cre_col] = now

            if sr_row:
                cols = list(sr_row.keys())
                vals = [sr_row[c] for c in cols]
                # NOT EXISTS guard when there's no unique constraint to conflict on.
                if sr_conflict is None and sr_src_col and sr_ext_col and ov_id is not None:
                    col_list = ", ".join(f'"{c}"' for c in cols)
                    ph = ", ".join(["%s"] * len(cols))
                    sql = (
                        f'INSERT INTO "{sr_schema}"."{sr_table}" ({col_list}) '
                        f'SELECT {ph} WHERE NOT EXISTS ('
                        f'  SELECT 1 FROM "{sr_schema}"."{sr_table}" '
                        f'  WHERE "{sr_src_col}" = %s AND "{sr_ext_col}" = %s)'
                    )
                    params = vals + [SOURCE_NAME, ov_id]
                else:
                    sql = make_insert(sr_schema, sr_table, cols, sr_conflict)
                    params = vals
                try:
                    cur.execute(sql, params)
                    inserted_sr += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
                except psycopg2.Error as e:
                    conn.rollback()
                    log(f"source_record insert error for {ov_id}: {e.pgerror or e}")
                    continue

        if seen % BATCH_SIZE == 0:
            conn.commit()
            log(f"... committed at {seen} rows "
                f"(business+{inserted_biz}, source_record+{inserted_sr})")

    conn.commit()
    log(f"DONE reading: {seen} rows processed; "
        f"business inserted {inserted_biz}, source_record inserted {inserted_sr}.")

    # ---- read-back counts ------------------------------------------------- #
    cur.execute(f'SELECT count(*) FROM "{biz_schema}"."{biz_table}"')
    total_biz = cur.fetchone()[0]
    if sr_src_col:
        cur.execute(
            f'SELECT count(*) FROM "{sr_schema}"."{sr_table}" WHERE "{sr_src_col}" = %s',
            (SOURCE_NAME,),
        )
        total_sr = cur.fetchone()[0]
    else:
        cur.execute(f'SELECT count(*) FROM "{sr_schema}"."{sr_table}"')
        total_sr = cur.fetchone()[0]

    log(f"READ-BACK: atlas.business total = {total_biz}")
    log(f"READ-BACK: atlas.source_record (source='{SOURCE_NAME}') = {total_sr}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
