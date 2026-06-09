#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A.T.L.A.S. metrics exporter -- read-only KPI bridge for the Jarvis V3 cockpit.

WHAT THIS IS
------------
A tiny, read-only metrics computer that runs ON THE BOX (the same Hetzner node that
hosts the live Postgres `tuanichat_atlas` / schema `atlas`). It computes the ATLAS
KPIs the V3 cockpit wants -- total businesses, enrichment queue depth
(pending/claimed/done/failed/dead), per-source counts, intake rate/min, last-updated --
and PUBLISHES them as a small JSON to the repo via the EXISTING status-back channel
(GitHub Contents API, STATUS_TOKEN/STATUS_REPO from /etc/atlas/autopull.env), writing
  status/<node>/atlas-metrics.json
The WordPress cockpit reads that file's RAW URL (server-side, cached).

WHY THIS DESIGN (security model, stated honestly)
-------------------------------------------------
The box currently exposes Postgres on :5432 to the operator only. We DO NOT open a new
public port and we DO NOT expose Postgres to the internet. Instead we reuse the proven
PUSH model already used by atlas_enrich_worker.py: the box computes the numbers locally
and PUSHes a token-authenticated JSON up to the manifests repo. WordPress only ever reads
a static JSON over HTTPS from raw.githubusercontent.com -- it never touches the DB and
needs no DB credentials. No secret lives in this file or in the repo; the GitHub token
comes from /etc/atlas/autopull.env (already on the box, already used for status-back).

DB CREDS come from /etc/atlas/db.env exactly like socrata_import.py / overture_pg_import.py
/ atlas_enrich_worker.py (PGHOST/PGDATABASE/PGUSER/PGPASSWORD or the DB_* / ATLAS_DB_*
aliases). Connection is READ-ONLY: we open a transaction, SET TRANSACTION READ ONLY,
run only SELECTs, and roll back. No DML, no DDL, ever.

SCHEMA IS INTROSPECTED, NOT GUESSED. Per the build rule, every table/column name is
resolved at runtime from information_schema (same CANDIDATES/pick_col strategy as the
live importers). If a column is named differently than expected, the affected KPI is
reported as null (honest) rather than crashing or faking a number.

FAIL-SOFT / FAIL-LOUD
---------------------
- Missing DNS / GitHub unreachable -> writes the metrics LOCALLY
  (/var/lib/atlas/autopull/atlas_metrics.json) and exits 0 (timer never wedges).
- DB down / atlas.business missing -> still PUBLISHES a JSON with
  status="unreachable" and reachable=false so the dashboard shows "ATLAS: connecting..."
  instead of a stale/fake number; exits 0.
- --selftest connects, verifies atlas.business exists, prints the computed metrics, and
  rolls back. Exit 0 on success, 3 if no Postgres (degrades cleanly, no traceback) so
  the install step can gate on it.

USAGE
-----
  atlas_metrics_export.py            compute + publish (status-back) + local cache
  atlas_metrics_export.py --selftest validate db.env loads, connect, schema present, dump
  atlas_metrics_export.py --dry-run  compute + print, do NOT push (local cache only)
"""
from __future__ import print_function
import base64
import json
import os
import sys
import time
import urllib.request
import urllib.error

try:
    import psycopg2
except Exception:
    psycopg2 = None

# --------------------------------------------------------------------------- #
# Config (env, all optional except DB creds which come from db.env)
# --------------------------------------------------------------------------- #
DB_ENV_PATH       = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
AUTOPULL_ENV_PATH = os.environ.get("ATLAS_AUTOPULL_CONF", "/etc/atlas/autopull.env")
STATE_DIR         = os.environ.get("ATLAS_AUTOPULL_STATE", "/var/lib/atlas/autopull")
LOCAL_OUT         = os.environ.get("ATLAS_METRICS_LOCAL",
                                   os.path.join(STATE_DIR, "atlas_metrics.json"))
SCHEMA            = os.environ.get("ATLAS_SCHEMA", "atlas")
# per-source breakdown is capped so the JSON stays tiny
TOP_SOURCES       = int(os.environ.get("ATLAS_METRICS_TOP_SOURCES", "24"))
INTAKE_WINDOW_MIN = int(os.environ.get("ATLAS_METRICS_INTAKE_WINDOW_MIN", "60"))


def log(msg):
    sys.stderr.write("[atlas-metrics] %s\n" % msg)
    sys.stderr.flush()


def load_env_file(path):
    """Source KEY=VALUE (with or without 'export'), exactly like the live importers."""
    if not os.path.exists(path):
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


def pick(*names, **kw):
    default = kw.get("default")
    for n in names:
        if os.environ.get(n):
            return os.environ[n]
    return default


def connect_pg():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not installed")
    conn = psycopg2.connect(
        host=pick("PGHOST", "DB_HOST", "ATLAS_DB_HOST", default="localhost"),
        port=pick("PGPORT", "DB_PORT", "ATLAS_DB_PORT", default="5432"),
        dbname=pick("PGDATABASE", "DB_NAME", "ATLAS_DB_NAME", default="tuanichat_atlas"),
        user=pick("PGUSER", "DB_USER", "ATLAS_DB_USER", default="atlas"),
        password=pick("PGPASSWORD", "DB_PASSWORD", "ATLAS_DB_PASSWORD", default=None),
        connect_timeout=int(os.environ.get("ATLAS_DB_CONNECT_TIMEOUT", "10")),
        application_name="atlas_metrics_export",
    )
    conn.autocommit = False
    return conn


# --------------------------------------------------------------------------- #
# Schema introspection (same strategy as the live importers -- DO NOT guess)
# --------------------------------------------------------------------------- #
def table_columns(cur, schema, table):
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema=%s AND table_name=%s", (schema, table))
    return {r[0] for r in cur.fetchall()}


def table_exists(cur, schema, table):
    cur.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema=%s AND table_name=%s", (schema, table))
    return cur.fetchone() is not None


def pick_col(colset, candidates):
    for c in candidates:
        if c in colset:
            return c
    return None


# logical field -> candidate column names (first present wins). Mirrors the
# CANDIDATES tables in overture_pg_import.py / atlas_enrich_worker.py.
CAND = {
    "biz_updated": ["last_updated", "updated_at", "modified_at", "updated"],
    "biz_created": ["created_at", "inserted_at", "first_seen", "first_observed", "created"],
    "sr_source":   ["source", "data_source", "origin", "source_code"],
    "sr_created":  ["created_at", "inserted_at", "fetched_at", "imported_at", "created"],
    "q_status":    ["status"],
}

# enrich_queue.status CHECK values (from atlas_enrich_worker.py): pending/claimed/done/failed/dead
QUEUE_STATES = ["pending", "claimed", "done", "failed", "dead"]


def _scalar(cur, sql, params=None):
    cur.execute(sql, params or ())
    row = cur.fetchone()
    return row[0] if row else None


def compute_metrics(conn):
    """READ-ONLY. Returns a dict of KPIs. Each KPI degrades to null if its
    source column/table isn't present (honest, never fabricated)."""
    cur = conn.cursor()
    # Hard read-only guarantee for this whole transaction.
    cur.execute("SET TRANSACTION READ ONLY")

    m = {
        "schema": SCHEMA,
        "business_total": None,
        "queue": {s: None for s in QUEUE_STATES},
        "queue_remaining": None,          # pending + claimed (work not yet done)
        "enrichment": {
            "done": None, "total": None, "progress_pct": None,
        },
        "sources": {},                    # source_name -> row count
        "source_count": None,             # number of distinct sources
        "intake_per_min": None,           # rows added in the last INTAKE_WINDOW_MIN
        "intake_window_min": INTAKE_WINDOW_MIN,
        "last_updated": None,             # max(business.last_updated) as epoch seconds
        "notes": [],
    }

    # ---- atlas.business total --------------------------------------------- #
    if not table_exists(cur, SCHEMA, "business"):
        raise RuntimeError("%s.business not found" % SCHEMA)
    m["business_total"] = int(_scalar(cur, 'SELECT count(*) FROM "%s"."business"' % SCHEMA) or 0)

    biz_cols = table_columns(cur, SCHEMA, "business")
    upd_col  = pick_col(biz_cols, CAND["biz_updated"])
    if upd_col:
        ts = _scalar(cur, 'SELECT extract(epoch FROM max("%s")) FROM "%s"."business"'
                     % (upd_col, SCHEMA))
        m["last_updated"] = int(ts) if ts is not None else None
    else:
        m["notes"].append("no last_updated-style column on business")

    # ---- enrich_queue depth by status ------------------------------------- #
    if table_exists(cur, SCHEMA, "enrich_queue"):
        q_cols = table_columns(cur, SCHEMA, "enrich_queue")
        st_col = pick_col(q_cols, CAND["q_status"])
        if st_col:
            cur.execute('SELECT "%s", count(*) FROM "%s"."enrich_queue" GROUP BY "%s"'
                        % (st_col, SCHEMA, st_col))
            seen = {}
            for state, n in cur.fetchall():
                seen[(state or "").lower()] = int(n)
            for s in QUEUE_STATES:
                m["queue"][s] = seen.get(s, 0)
            # any non-canonical statuses are surfaced honestly, not dropped
            extra = {k: v for k, v in seen.items() if k not in QUEUE_STATES}
            if extra:
                m["queue"].update(extra)
                m["notes"].append("non-canonical queue statuses present: %s"
                                  % ",".join(sorted(extra)))
            pend = m["queue"].get("pending") or 0
            clm  = m["queue"].get("claimed") or 0
            done = m["queue"].get("done") or 0
            tot  = sum(v for v in m["queue"].values() if isinstance(v, int))
            m["queue_remaining"] = pend + clm
            m["enrichment"]["done"] = done
            m["enrichment"]["total"] = tot
            m["enrichment"]["progress_pct"] = round(100.0 * done / tot, 1) if tot else None
        else:
            m["notes"].append("enrich_queue has no status column")
    else:
        m["notes"].append("%s.enrich_queue not present" % SCHEMA)

    # ---- per-source counts from source_record ----------------------------- #
    if table_exists(cur, SCHEMA, "source_record"):
        sr_cols = table_columns(cur, SCHEMA, "source_record")
        src_col = pick_col(sr_cols, CAND["sr_source"])
        if src_col:
            cur.execute(
                'SELECT "%s", count(*) AS n FROM "%s"."source_record" '
                'GROUP BY "%s" ORDER BY n DESC LIMIT %%s'
                % (src_col, SCHEMA, src_col), (TOP_SOURCES,))
            srcs = {}
            for src, n in cur.fetchall():
                srcs[(src if src is not None else "unknown")] = int(n)
            m["sources"] = srcs
            m["source_count"] = int(
                _scalar(cur, 'SELECT count(DISTINCT "%s") FROM "%s"."source_record"'
                        % (src_col, SCHEMA)) or 0)
        else:
            m["notes"].append("source_record has no source column")

        # ---- intake rate/min over the trailing window --------------------- #
        cre_col = pick_col(sr_cols, CAND["sr_created"])
        if cre_col:
            try:
                n = _scalar(
                    cur,
                    'SELECT count(*) FROM "%s"."source_record" '
                    'WHERE "%s" >= now() - (%%s || \' minutes\')::interval'
                    % (SCHEMA, cre_col), (INTAKE_WINDOW_MIN,))
                if n is not None:
                    m["intake_per_min"] = round(float(n) / float(INTAKE_WINDOW_MIN), 3)
            except Exception as e:
                # created column may not be a timestamp type -> honest null
                m["notes"].append("intake window not computable (%s)" % type(e).__name__)
                conn.rollback()
                cur = conn.cursor()
                cur.execute("SET TRANSACTION READ ONLY")
        else:
            m["notes"].append("source_record has no created_at-style column for intake rate")
    else:
        m["notes"].append("%s.source_record not present" % SCHEMA)

    conn.rollback()  # READ-ONLY: never commit
    return m


# --------------------------------------------------------------------------- #
# Status-back publish (GitHub Contents API) -- identical pattern to enrich worker
# --------------------------------------------------------------------------- #
def gh_put(path, body_obj, msg):
    token = os.environ.get("STATUS_TOKEN")
    repo = os.environ.get("STATUS_REPO")
    if not token or not repo:
        log("STATUS_TOKEN/STATUS_REPO unset -> local-only (%s)" % path)
        return False
    api = os.environ.get("STATUS_API_BASE", "https://api.github.com")
    branch = os.environ.get("STATUS_BRANCH", "main")
    content_b64 = base64.b64encode(json.dumps(body_obj).encode("utf-8")).decode("ascii")

    def _req(method, url, data=None):
        req = urllib.request.Request(url, method=method, data=data)
        req.add_header("Authorization", "Bearer %s" % token)
        req.add_header("Accept", "application/vnd.github+json")
        req.add_header("User-Agent", "atlas-metrics")
        req.add_header("X-GitHub-Api-Version", "2022-11-28")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                return resp.getcode(), resp.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode("utf-8", "replace")
        except urllib.error.URLError as e:
            return None, str(e.reason)

    code, resp = _req("GET", "%s/repos/%s/contents/%s?ref=%s" % (api, repo, path, branch))
    sha = None
    if code == 200:
        try:
            sha = json.loads(resp).get("sha")
        except ValueError:
            sha = None
    payload = {"message": msg, "content": content_b64, "branch": branch}
    if sha:
        payload["sha"] = sha
    code, resp = _req("PUT", "%s/repos/%s/contents/%s" % (api, repo, path),
                      json.dumps(payload).encode("utf-8"))
    if code and 200 <= code < 300:
        log("status pushed -> %s (%s)" % (path, code))
        return True
    log("status push FAILED %s http=%s resp=%s" % (path, code, (resp or "")[:200]))
    return False


def write_local(obj, path):
    try:
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(obj, fh)
        log("local cache -> %s" % path)
    except OSError as e:
        log("WARNING could not write %s: %s" % (path, e))


def node_id():
    return (os.environ.get("ATLAS_NODE")
            or os.environ.get("NODE_ID")
            or "hetzner")


def envelope(metrics, reachable, status):
    return {
        "kind": "atlas-metrics",
        "schema_version": 1,
        "node": node_id(),
        "reachable": reachable,
        "status": status,            # "ok" | "unreachable"
        "metrics": metrics,
        "ts": int(time.time()),
    }


def run(dry_run=False):
    load_env_file(DB_ENV_PATH)
    load_env_file(AUTOPULL_ENV_PATH)
    node = node_id()
    repo_path = "status/%s/atlas-metrics.json" % node

    metrics = None
    reachable = False
    status = "unreachable"
    try:
        conn = connect_pg()
        try:
            metrics = compute_metrics(conn)
            reachable = True
            status = "ok"
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as e:
        # FAIL-SOFT: publish an honest "unreachable" envelope so the dashboard shows
        # "ATLAS: connecting..." rather than a fake or stale number.
        log("metrics compute failed (%s): %s" % (type(e).__name__, e))
        # BULLETPROOF: a secondary-KPI failure (e.g. a slow GROUP BY on a multi-million
        # row table) must NEVER null the headline number or flip the dashboard to 0.
        # Re-fetch business_total in its own short isolated query and serve it reachable.
        metrics = {"error": "%s: %s" % (type(e).__name__, e)}
        try:
            _c2 = connect_pg()
            _cur2 = _c2.cursor()
            _cur2.execute("SET LOCAL statement_timeout = 8000")
            _bt = int((_scalar(_cur2, 'SELECT count(*) FROM "%s"."business"' % SCHEMA)) or 0)
            _cur2.close(); _c2.close()
            metrics = {"schema": SCHEMA, "business_total": _bt, "queue": {}, "sources": {},
                       "source_count": None, "intake_per_min": None,
                       "notes": ["degraded: full KPI pass failed (%s); headline served from isolated count" % type(e).__name__]}
            reachable = True
            status = "ok"
        except Exception as _e2:
            log("bulletproof fallback also failed: %s" % _e2)

    _LG = os.path.join(os.path.dirname(LOCAL_OUT) or ".", "metrics_last_good.json")
    _bt = (metrics or {}).get("business_total") if isinstance(metrics, dict) else None
    if reachable and isinstance(_bt, int) and _bt > 0:
        try:
            write_local({"business_total": _bt, "sources": (metrics.get("sources") or {}),
                         "source_count": metrics.get("source_count"),
                         "queue": (metrics.get("queue") or {}),
                         "queue_remaining": metrics.get("queue_remaining"),
                         "ts": int(time.time())}, _LG)
        except Exception:
            pass
    else:
        try:
            with open(_LG, "r", encoding="utf-8") as _fh:
                _lg = json.load(_fh)
            if isinstance(_lg.get("business_total"), int) and _lg["business_total"] > 0:
                metrics = {"schema": SCHEMA, "business_total": _lg["business_total"],
                           "sources": (_lg.get("sources") or {}), "source_count": _lg.get("source_count"),
                           "queue": (_lg.get("queue") or {}), "queue_remaining": _lg.get("queue_remaining"),
                           "intake_per_min": None, "stale": True, "last_good_ts": _lg.get("ts"),
                           "notes": ["served LAST-GOOD business_total (live query unavailable)"]}
                reachable = True; status = "ok"
        except Exception as _lge:
            log("no last-good cache yet: %s" % _lge)

    env = envelope(metrics, reachable, status)
    write_local(env, LOCAL_OUT)
    if reachable:
        log("business_total=%s queue_remaining=%s sources=%s intake/min=%s" % (
            metrics.get("business_total"),
            metrics.get("queue_remaining"),
            metrics.get("source_count"),
            metrics.get("intake_per_min")))
    if dry_run:
        print(json.dumps(env, indent=2))
        return 0
    gh_put(repo_path, env, "atlas metrics %s reachable=%s" % (node, reachable))
    return 0


def selftest():
    load_env_file(DB_ENV_PATH)
    load_env_file(AUTOPULL_ENV_PATH)
    if psycopg2 is None:
        log("SELFTEST: psycopg2 not installed -> degrade cleanly (exit 3)")
        return 3
    try:
        conn = connect_pg()
    except Exception as e:
        log("SELFTEST: no Postgres (%s) -> degrade cleanly (exit 3)" % e)
        return 3
    try:
        cur = conn.cursor()
        cur.execute("SET TRANSACTION READ ONLY")
        if not table_exists(cur, SCHEMA, "business"):
            log("SELFTEST FAIL: %s.business not found" % SCHEMA)
            return 2
        m = compute_metrics(conn)
        print(json.dumps(envelope(m, True, "ok"), indent=2))
        log("SELFTEST OK: business_total=%s" % m.get("business_total"))
        return 0
    finally:
        try:
            conn.rollback(); conn.close()
        except Exception:
            pass


def main(argv):
    if "--selftest" in argv:
        return selftest()
    if "--dry-run" in argv:
        return run(dry_run=True)
    return run(dry_run=False)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
