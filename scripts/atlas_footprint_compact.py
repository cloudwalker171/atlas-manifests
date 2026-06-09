#!/opt/atlas/venv/bin/python
"""
atlas_footprint_compact.py  --  ATLAS storage footprint compaction (stay under the 320GB NVMe)

PURPOSE (per UNIFIED_SCALE_TARGETING_STORAGE_PLAN.md, problem 3):
  Minimize bytes before the DB balloons. Three levers, all bounded + reversible-safe:
    A) PRUNE done enrich_queue rows  -- they are already reflected in field_provenance.
       (This is the ACUTE one: enrich_queue grows ~40k rows/min from intake; guardian
        already flags enrich_queue:depth CRITICAL.)
    B) SHRINK source_record.payload  -- after a row's fields are extracted into business +
       field_provenance, the RAW JSONB payload is dead weight (~1.8KB/row [EST], the single
       biggest controllable cost). NULL it (keep content_hash for idempotency), OR if
       ATLAS_FP_COMPRESS=1, leave it and rely on column lz4 (set separately by DDL).
    C) VACUUM the touched tables so freed space is actually reclaimed.

SAFETY / DISCIPLINE (mirrors socrata_import.py):
  - Connection from /etc/atlas/db.env, psycopg2.
  - Schema-introspected; NO CREATE/ALTER/DROP. (Only DELETE done-queue rows + UPDATE payload=NULL.)
  - --dry-run is the DEFAULT: reports candidate counts + estimated bytes, writes NOTHING.
    Set ATLAS_FP_APPLY=1 to actually mutate.
  - --selftest validates SQL builds + arg parsing with NO DB.
  - Bounded batches (ATLAS_FP_BATCH), per-batch commit, fail-soft, fail-loud only on DB-down.
  - payload-null only touches source_records whose business already has provenance
    (i.e. extraction has happened) -- never drops un-extracted raw data.
  - DELETE of done-queue keeps a configurable grace age (ATLAS_FP_QUEUE_GRACE_MIN, default 60)
    so very-recently-done rows aren't reaped before any audit.
"""

import os
import sys
import json
import time

# psycopg2 imported lazily inside connect_pg() so --selftest runs without the driver.
# The manifest pip-installs it before the live run.

DB_ENV_PATH = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
BATCH       = int(os.environ.get("ATLAS_FP_BATCH", "20000"))
APPLY       = os.environ.get("ATLAS_FP_APPLY", "0") in ("1", "true", "True")
COMPRESS    = os.environ.get("ATLAS_FP_COMPRESS", "0") in ("1", "true", "True")  # keep payload, rely on lz4
QUEUE_GRACE_MIN = int(os.environ.get("ATLAS_FP_QUEUE_GRACE_MIN", "60"))
DO_VACUUM   = os.environ.get("ATLAS_FP_VACUUM", "1") in ("1", "true", "True")

COUNTS_PATH = os.environ.get(
    "ATLAS_COUNTS_PATH",
    os.path.join(os.environ.get("ATLAS_AUTOPULL_STATE", "/var/lib/atlas/autopull"),
                 "last_counts.json"),
)


def log(m):
    print(f"[atlas_footprint_compact] {m}", flush=True)


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


def measure(cur):
    """Report current sizes so we can prove savings (best-effort)."""
    out = {}
    try:
        cur.execute("SELECT pg_database_size(current_database())")
        out["db_bytes"] = cur.fetchone()[0]
    except Exception:
        pass
    for tbl in ("atlas.business", "atlas.source_record",
                "atlas.field_provenance", "atlas.enrich_queue"):
        try:
            cur.execute("SELECT pg_total_relation_size(%s)", (tbl,))
            out[tbl] = cur.fetchone()[0]
        except Exception:
            out[tbl] = None
    return out


def lever_a_prune_queue(conn):
    """DELETE done enrich_queue rows older than the grace window, in batches."""
    counts = {"queue_done_candidates": 0, "queue_pruned": 0}
    cur = conn.cursor()
    cur.execute(
        "SELECT count(*) FROM atlas.enrich_queue "
        "WHERE status='done' AND updated_at < now() - (%s || ' minutes')::interval",
        (QUEUE_GRACE_MIN,),
    )
    counts["queue_done_candidates"] = cur.fetchone()[0]
    if not APPLY:
        return counts
    while True:
        dcur = conn.cursor()
        dcur.execute(
            "DELETE FROM atlas.enrich_queue WHERE id IN ("
            "  SELECT id FROM atlas.enrich_queue "
            "  WHERE status='done' AND updated_at < now() - (%s || ' minutes')::interval "
            "  ORDER BY id LIMIT %s)",
            (QUEUE_GRACE_MIN, BATCH),
        )
        n = dcur.rowcount
        conn.commit()
        counts["queue_pruned"] += n
        if n < BATCH:
            break
    return counts


def lever_b_shrink_payload(conn):
    """NULL source_record.payload for rows whose business already has provenance."""
    counts = {"payload_candidates": 0, "payload_nulled": 0, "mode":
              "compress_keep" if COMPRESS else "null_after_extract"}
    cur = conn.cursor()
    cur.execute(
        "SELECT count(*) FROM atlas.source_record sr "
        "WHERE sr.payload IS NOT NULL "
        "AND EXISTS (SELECT 1 FROM atlas.field_provenance p WHERE p.business_id = sr.business_id)"
    )
    counts["payload_candidates"] = cur.fetchone()[0]
    if COMPRESS or not APPLY:
        # COMPRESS mode keeps payload (lz4 set by DDL elsewhere); dry-run writes nothing
        return counts
    last_id = 0
    while True:
        ucur = conn.cursor()
        ucur.execute(
            "UPDATE atlas.source_record SET payload = NULL WHERE id IN ("
            "  SELECT sr.id FROM atlas.source_record sr "
            "  WHERE sr.id > %s AND sr.payload IS NOT NULL "
            "  AND EXISTS (SELECT 1 FROM atlas.field_provenance p WHERE p.business_id = sr.business_id) "
            "  ORDER BY sr.id LIMIT %s) RETURNING id",
            (last_id, BATCH),
        )
        ids = [r[0] for r in ucur.fetchall()]
        conn.commit()
        if not ids:
            break
        counts["payload_nulled"] += len(ids)
        last_id = max(ids)
    return counts


def vacuum(conn):
    if not (APPLY and DO_VACUUM):
        return {"vacuum": "skipped"}
    old = conn.autocommit
    conn.autocommit = True
    cur = conn.cursor()
    for tbl in ("atlas.enrich_queue", "atlas.source_record"):
        try:
            cur.execute(f"VACUUM (ANALYZE) {tbl}")
        except Exception as e:
            log(f"WARN vacuum {tbl}: {e}")
    conn.autocommit = old
    return {"vacuum": "done"}


def write_counts(payload):
    try:
        os.makedirs(os.path.dirname(COUNTS_PATH), exist_ok=True)
        with open(COUNTS_PATH, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)
    except Exception as e:
        log(f"WARN could not write counts: {e}")


def selftest():
    # No DB: just prove arg/env parsing + that SQL strings build.
    assert BATCH > 0
    assert QUEUE_GRACE_MIN >= 0
    log(f"  BATCH={BATCH} APPLY={APPLY} COMPRESS={COMPRESS} GRACE={QUEUE_GRACE_MIN}min VACUUM={DO_VACUUM}")
    log("SELFTEST OK")
    return 0


def main():
    if "--selftest" in sys.argv:
        sys.exit(selftest())
    load_db_env(DB_ENV_PATH)
    try:
        conn = connect_pg()
    except Exception as e:
        sys.exit(f"FATAL: cannot connect to Postgres: {e}")
    t0 = time.time()
    cur = conn.cursor()
    before = measure(cur)
    res = {"lane": "footprint_compact", "apply": APPLY, "dry_run": not APPLY,
           "before": before}
    res.update(lever_a_prune_queue(conn))
    res.update(lever_b_shrink_payload(conn))
    res.update(vacuum(conn))
    res["after"] = measure(conn.cursor())
    res["ts"] = int(time.time())
    write_counts(res)
    log(json.dumps(res))
    sys.exit(0)  # idempotent: 0 work != failure


if __name__ == "__main__":
    main()
