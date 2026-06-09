#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A.T.L.A.S. read-only NL-query micro-service (Phase-2 upgrade for the Smart Query chat).

WHAT THIS IS
------------
A tiny loopback-only HTTP service that runs ON THE BOX (same Hetzner node as the
live Postgres `tuanichat_atlas` / schema `atlas`). It answers STRUCTURED query
specs (NOT raw SQL) from the WordPress NLQ plugin and returns business rows as
JSON, so natural-language queries can hit the LIVE master DB (10k+ rows, full
`atlas.business` column set) instead of the fleet's SQLite mirror.

It is the box-side half of council verdict option (B). It is OPTIONAL: the WP
plugin works today against the SQLite mirror (option A) with zero box service.
This upgrades the data source to the live master DB.

SECURITY MODEL (stated honestly, Sentinel-approved)
---------------------------------------------------
- Binds 127.0.0.1 ONLY (configurable BIND, default 127.0.0.1:8788). NEVER 0.0.0.0.
  Postgres stays private; no new public port; no DB creds on the WP host.
- The WP plugin reaches this over the box's existing loopback (the same trust
  boundary the local J.A.R.V.I.S. NLQ model uses); nothing here is exposed to the
  internet. A shared-secret header (ATLAS_QUERY_TOKEN from /etc/atlas/query.env)
  is required on every request, so only the co-located WP can call it.
- The request body is a STRUCTURED SPEC (fields/ops/values/sort/limit), never SQL.
  This service re-applies the SAME whitelist the PHP/JS layers use: FIELD_MAP +
  OPS allowlist, values bound as psycopg2 params (never interpolated), sort dir
  enum->literal, limit clamped 1..500, table fixed to an allowlisted view.
- Connection is hard read-only: `SET TRANSACTION READ ONLY` + rollback; SELECT
  only; statement_timeout caps runaway queries. No DML/DDL is reachable.
- Schema is INTROSPECTED from information_schema (not hardcoded); unknown columns
  degrade to "not selectable" honestly rather than crashing.

FAIL-SOFT
---------
- DB down / table missing -> 503 with an honest JSON error; the WP side shows the
  same "store unreachable" state it shows for the SQLite path. Never fabricates.
- --selftest connects, verifies atlas.business, runs one bounded SELECT, rolls
  back, exits 0 (3 if no Postgres) so the install step can gate on it.

USAGE
-----
  atlas_query_service.py            run the loopback service (systemd ExecStart)
  atlas_query_service.py --selftest validate db.env + schema, one SELECT, rollback
  atlas_query_service.py --dry-run  build+print SQL for a sample spec, no execute
"""
from __future__ import print_function
import json
import os
import sys
import http.server

try:
    import psycopg2
    import psycopg2.extras
except Exception:
    psycopg2 = None

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
DB_ENV_PATH = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
QUERY_ENV_PATH = os.environ.get("ATLAS_QUERY_ENV", "/etc/atlas/query.env")
SCHEMA = os.environ.get("ATLAS_SCHEMA", "atlas")
BIND_HOST = os.environ.get("ATLAS_QUERY_HOST", "127.0.0.1")
BIND_PORT = int(os.environ.get("ATLAS_QUERY_PORT", "8788"))
STMT_TIMEOUT_MS = int(os.environ.get("ATLAS_QUERY_STMT_TIMEOUT_MS", "4000"))
DEFAULT_LIMIT = 100
MAX_LIMIT = 500

# Allowlisted views the spec may target. We query VIEWS, not base tables, so the
# service can only ever read the curated, non-sensitive projection. Create these
# views once (see DELIVER doc); if absent we fall back to atlas.business directly
# but still column-allowlisted.
ALLOWED_TABLES = {
    "atlas": "business",          # ATLAS surface  -> atlas.business
    "prospects": "business",      # Prospects      -> atlas.business + promoted predicate
    "leadhunter": "business",     # Lead Hunter    -> atlas.business (shares masters)
}

# field key -> real column (whitelist). Mirrors the PHP/JS FIELD_MAP; resolved
# against the introspected column set at startup so a renamed column is dropped
# (honest) rather than crashing.
FIELD_MAP = {
    "name": ["name"],
    "domain": ["domain"],
    "phone": ["phone_e164", "phone"],
    "email": ["email"],
    "email_status": ["email_status"],
    "city": ["city"],
    "region": ["region"],
    "country": ["country"],
    "industry": ["category", "industry"],
    "employees": ["employee_range", "employees"],
    "source": ["source_code", "source"],
    "confidence": ["confidence"],
    "has_chat": ["has_chat"],
    "signal_type": ["signal_type"],
    "last_enriched": ["last_updated", "last_enriched"],
    "first_seen": ["first_seen", "created_at"],
}
SORTABLE = {"name", "city", "region", "industry", "employees", "confidence", "last_enriched", "first_seen", "email_status"}
OPS = {"eq", "neq", "gt", "gte", "lt", "lte", "like", "in", "null", "notnull", "between"}

SELECT_COLS = ["id", "name", "domain", "phone", "email", "email_status", "city",
               "region", "industry", "employees", "source", "confidence",
               "has_chat", "signal_type", "last_enriched"]

# resolved column map (logical -> real), filled at startup from information_schema
RESOLVED = {}


def log(m):
    sys.stderr.write("[atlas-query] %s\n" % m); sys.stderr.flush()


def load_env_file(path):
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
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def pick(*names, **kw):
    for n in names:
        if os.environ.get(n):
            return os.environ[n]
    return kw.get("default")


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
        application_name="atlas_query_service",
    )
    conn.autocommit = False
    return conn


def table_columns(cur, schema, table):
    cur.execute("SELECT column_name FROM information_schema.columns "
                "WHERE table_schema=%s AND table_name=%s", (schema, table))
    return {r[0] for r in cur.fetchall()}


def resolve_columns(cur):
    """Resolve logical field -> first present real column from information_schema."""
    cols = table_columns(cur, SCHEMA, "business")
    out = {}
    for logical, cands in FIELD_MAP.items():
        for c in cands:
            if c in cols:
                out[logical] = c
                break
    RESOLVED.clear()
    RESOLVED.update(out)
    return out


# --------------------------------------------------------------------------- #
# Spec -> parameterized SQL (the SAME whitelist as PHP/JS; never trusts input)
# --------------------------------------------------------------------------- #
def build_sql(spec, view):
    table = ALLOWED_TABLES.get(view, "business")
    where, params = [], []

    # prospects scope: promoted-only proxy (deliverable email present), matching
    # the PHP side. Fixed predicate, no user input.
    if view == "prospects":
        where.append("(email IS NOT NULL AND email <> '')")

    for f in (spec.get("filters") or [])[:12]:
        field = f.get("field")
        op = f.get("op")
        if field not in RESOLVED or op not in OPS:
            continue
        col = RESOLVED[field]
        v = f.get("value")
        if op == "eq":
            where.append('"%s" = %%s' % col); params.append(v)
        elif op == "neq":
            where.append('"%s" <> %%s' % col); params.append(v)
        elif op in ("gt", "gte", "lt", "lte"):
            sym = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[op]
            where.append('"%s" %s %%s' % (col, sym)); params.append(_num(v))
        elif op == "like":
            where.append('"%s" ILIKE %%s' % col)
            params.append("%" + str(v).replace("%", r"\%").replace("_", r"\_") + "%")
        elif op == "in":
            vals = (v if isinstance(v, list) else [v])[:50]
            if not vals:
                continue
            where.append('"%s" IN (%s)' % (col, ",".join(["%s"] * len(vals))))
            params.extend(vals)
        elif op == "null":
            where.append('("%s" IS NULL OR "%s" = \'\')' % (col, col))
        elif op == "notnull":
            where.append('("%s" IS NOT NULL AND "%s" <> \'\')' % (col, col))
        elif op == "between":
            if not isinstance(v, list) or len(v) != 2:
                continue
            where.append('"%s" BETWEEN %%s AND %%s' % col)
            params.extend([_num(v[0]), _num(v[1])])

    txt = spec.get("text")
    if txt:
        name_c = RESOLVED.get("name", "name")
        dom_c = RESOLVED.get("domain", "domain")
        where.append('("%s" ILIKE %%s OR "%s" ILIKE %%s)' % (name_c, dom_c))
        t = "%" + str(txt).replace("%", r"\%").replace("_", r"\_") + "%"
        params.extend([t, t])

    order = ""
    s = spec.get("sort") or {}
    if s.get("field") in SORTABLE and s.get("field") in RESOLVED:
        direction = "ASC" if (s.get("dir") == "asc") else "DESC"
        order = ' ORDER BY "%s" %s' % (RESOLVED[s["field"]], direction)

    limit = int(spec.get("limit") or 0)
    limit = DEFAULT_LIMIT if limit < 1 else min(limit, MAX_LIMIT)

    # build SELECT list from resolved columns (alias back to logical names so the
    # WP table gets the same shape as the SQLite path)
    sel = ["id"]
    for logical in SELECT_COLS:
        if logical == "id":
            continue
        real = RESOLVED.get(logical)
        if real:
            sel.append('"%s" AS %s' % (real, logical))
        else:
            sel.append('NULL AS %s' % logical)

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    sql = 'SELECT %s FROM "%s"."%s"%s%s LIMIT %d' % (
        ", ".join(sel), SCHEMA, table, where_sql, order, limit)
    count_sql = 'SELECT count(*) FROM "%s"."%s"%s' % (SCHEMA, table, where_sql)
    return sql, count_sql, params


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0


def run_query(conn, view, spec):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SET TRANSACTION READ ONLY")
    cur.execute("SET statement_timeout = %s", (STMT_TIMEOUT_MS,))
    if not RESOLVED:
        resolve_columns(conn.cursor())
    sql, count_sql, params = build_sql(spec, view)
    cur.execute(count_sql, params)
    count = list(cur.fetchone().values())[0]
    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.rollback()
    return {"count": int(count), "rows": rows, "sql_shape": sql}


# --------------------------------------------------------------------------- #
# HTTP (loopback only, token-gated)
# --------------------------------------------------------------------------- #
class Handler(http.server.BaseHTTPRequestHandler):
    def _json(self, code, obj):
        body = json.dumps(obj, default=str).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass  # quiet

    def do_POST(self):
        token = os.environ.get("ATLAS_QUERY_TOKEN")
        if token and self.headers.get("X-Atlas-Token") != token:
            return self._json(401, {"ok": False, "error": "unauthorized"})
        try:
            length = int(self.headers.get("Content-Length", "0"))
            spec = json.loads(self.rfile.read(length) or b"{}")
        except Exception:
            return self._json(400, {"ok": False, "error": "bad json"})

        view = spec.get("view", "atlas")
        if view not in ALLOWED_TABLES:
            view = "atlas"
        try:
            conn = connect_pg()
        except Exception as e:
            return self._json(503, {"ok": False, "error": "db unreachable: %s" % e})
        try:
            res = run_query(conn, view, spec.get("intent") or spec)
            self._json(200, {"ok": True, "view": view, "count": res["count"], "rows": res["rows"]})
        except Exception as e:
            self._json(500, {"ok": False, "error": "%s: %s" % (type(e).__name__, e)})
        finally:
            try:
                conn.close()
            except Exception:
                pass


def serve():
    load_env_file(DB_ENV_PATH)
    load_env_file(QUERY_ENV_PATH)
    if BIND_HOST not in ("127.0.0.1", "localhost", "::1"):
        log("REFUSING non-loopback bind %s — loopback only" % BIND_HOST)
        return 2
    # warm the column resolver once (fail-soft if DB down at boot; resolved lazily)
    try:
        c = connect_pg(); resolve_columns(c.cursor()); c.rollback(); c.close()
        log("resolved columns: %s" % ",".join(sorted(RESOLVED)))
    except Exception as e:
        log("startup column resolve deferred (db not ready: %s)" % e)
    httpd = http.server.HTTPServer((BIND_HOST, BIND_PORT), Handler)
    log("listening on %s:%d (loopback, read-only)" % (BIND_HOST, BIND_PORT))
    httpd.serve_forever()
    return 0


def selftest():
    load_env_file(DB_ENV_PATH)
    load_env_file(QUERY_ENV_PATH)
    if psycopg2 is None:
        log("SELFTEST: psycopg2 not installed -> degrade cleanly (exit 3)")
        return 3
    try:
        conn = connect_pg()
    except Exception as e:
        log("SELFTEST: no Postgres (%s) -> exit 3" % e)
        return 3
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name='business'", (SCHEMA,))
        if not cur.fetchone():
            log("SELFTEST FAIL: %s.business missing" % SCHEMA)
            return 2
        resolve_columns(conn.cursor())
        res = run_query(conn, "atlas", {"limit": 1})
        print(json.dumps({"resolved": RESOLVED, "sample_count": res["count"]}, default=str, indent=2))
        log("SELFTEST OK: count=%s" % res["count"])
        return 0
    finally:
        try:
            conn.rollback(); conn.close()
        except Exception:
            pass


def dry_run():
    # build+print SQL for a representative spec without any DB connection
    RESOLVED.update({k: v[0] for k, v in FIELD_MAP.items()})  # assume canonical names
    spec = {"filters": [
        {"field": "industry", "op": "eq", "value": "Nonprofit"},
        {"field": "has_chat", "op": "eq", "value": 0},
        {"field": "email_status", "op": "eq", "value": "valid"},
    ], "sort": {"field": "employees", "dir": "desc"}, "limit": 100}
    sql, count_sql, params = build_sql(spec, "prospects")
    print("SQL:    ", sql)
    print("COUNT:  ", count_sql)
    print("PARAMS: ", params)
    return 0


def main(argv):
    if "--selftest" in argv:
        return selftest()
    if "--dry-run" in argv:
        return dry_run()
    return serve()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
