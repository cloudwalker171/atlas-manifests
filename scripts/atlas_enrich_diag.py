#!/usr/bin/env python3
# ============================================================================
# atlas_enrich_diag.py  --  READ-ONLY production schema diagnostic for the
# seq-7 enrichment-worker fix. Introspects the REAL atlas.enrich_queue /
# atlas.field_provenance shape (constraints, NOT NULL, defaults, the live
# `status` value vocabulary + distribution, whether task_type is required and
# its values, whether the existing rows are claimable) plus a redacted sample
# row of each table, and PUSHES the result to status/<node>/seq-7-diag.json
# via the GitHub Contents API -- the SAME channel the worker used for its
# self-captured migrate-error. NO writes to the DB. NO DDL. NO ssh/firewall/
# autopull touch. Mirrors load_env_file/connect_pg/gh_put from
# atlas_enrich_worker.py so it shares the exact env + auth path.
# ============================================================================
import os, sys, json, time, base64, urllib.request, urllib.error
try:
    import psycopg2
except Exception as e:  # noqa: BLE001
    print(f"[diag] psycopg2 import failed: {e}", flush=True); psycopg2 = None

DB_ENV_PATH = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
AUTOPULL_ENV_PATH = os.environ.get("ATLAS_AUTOPULL_CONF", "/etc/atlas/autopull.env")

def log(*a): print("[diag]", *a, flush=True)

def load_env_file(path):
    if not os.path.exists(path): return
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"): continue
            if line.lower().startswith("export "): line = line[7:].strip()
            if "=" not in line: continue
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))

def pick(*names, default=None):
    for n in names:
        if os.environ.get(n): return os.environ[n]
    return default

def connect_pg():
    return psycopg2.connect(
        host=pick("PGHOST","DB_HOST","ATLAS_DB_HOST", default="localhost"),
        port=pick("PGPORT","DB_PORT","ATLAS_DB_PORT", default="5432"),
        dbname=pick("PGDATABASE","DB_NAME","ATLAS_DB_NAME", default="tuanichat_atlas"),
        user=pick("PGUSER","DB_USER","ATLAS_DB_USER", default="atlas"),
        password=pick("PGPASSWORD","DB_PASSWORD","ATLAS_DB_PASSWORD", default=None),
        connect_timeout=int(os.environ.get("ATLAS_DB_CONNECT_TIMEOUT","10")),
        application_name="atlas_enrich_diag")

def gh_put(path, body_obj, msg):
    token = os.environ.get("STATUS_TOKEN"); repo = os.environ.get("STATUS_REPO")
    if not token or not repo:
        log(f"status: local-only ({path}) STATUS_TOKEN/REPO unset"); return False
    api = os.environ.get("STATUS_API_BASE","https://api.github.com")
    branch = os.environ.get("STATUS_BRANCH","main")
    content_b64 = base64.b64encode(json.dumps(body_obj).encode("utf-8")).decode("ascii")
    def _req(method, url, data=None):
        req = urllib.request.Request(url, method=method, data=data)
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Accept", "application/vnd.github+json")
        req.add_header("User-Agent", "atlas-enrich-diag")
        req.add_header("X-GitHub-Api-Version", "2022-11-28")
        if data is not None: req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                return resp.getcode(), resp.read().decode("utf-8","replace")
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode("utf-8","replace")
        except urllib.error.URLError as e:
            return None, str(e.reason)
    code, resp = _req("GET", f"{api}/repos/{repo}/contents/{path}?ref={branch}")
    sha = None
    if code == 200:
        try: sha = json.loads(resp).get("sha")
        except ValueError: sha = None
    payload = {"message": msg, "content": content_b64, "branch": branch}
    if sha: payload["sha"] = sha
    code, resp = _req("PUT", f"{api}/repos/{repo}/contents/{path}", json.dumps(payload).encode("utf-8"))
    if code and 200 <= code < 300:
        log(f"status pushed -> {path} ({code})"); return True
    log(f"status push FAILED {path} http={code} resp={resp[:200]}"); return False

def _redact_row(colnames, row):
    SENSITIVE = {"email","phone_e164","phone","addr_line1"}
    out = {}
    for c, v in zip(colnames, row):
        if c in SENSITIVE and v:
            sv = str(v); out[c] = sv[:2] + "***(redacted len=%d)" % len(sv)
        elif isinstance(v,(dict,list)): out[c] = v
        else: out[c] = (str(v)[:300] if v is not None else None)
    return out

def introspect(cur, schema, table):
    info = {}
    cur.execute("SELECT column_name, data_type, is_nullable, column_default, "
                "character_maximum_length, ordinal_position "
                "FROM information_schema.columns WHERE table_schema=%s AND table_name=%s "
                "ORDER BY ordinal_position", (schema, table))
    info["columns"] = [{"name":r[0],"type":r[1],"nullable":(r[2]=="YES"),
                        "default":r[3],"maxlen":r[4],"pos":r[5]} for r in cur.fetchall()]
    cur.execute("SELECT con.conname, con.contype, pg_get_constraintdef(con.oid) "
                "FROM pg_constraint con JOIN pg_class rel ON rel.oid=con.conrelid "
                "JOIN pg_namespace nsp ON nsp.oid=rel.relnamespace "
                "WHERE nsp.nspname=%s AND rel.relname=%s ORDER BY con.contype, con.conname",
                (schema, table))
    info["constraints"] = [{"name":r[0],"type":r[1],"def":r[2]} for r in cur.fetchall()]
    cur.execute("SELECT indexname, indexdef FROM pg_indexes WHERE schemaname=%s AND tablename=%s "
                "ORDER BY indexname", (schema, table))
    info["indexes"] = [{"name":r[0],"def":r[1]} for r in cur.fetchall()]
    cur.execute(f'SELECT count(*) FROM "{schema}"."{table}"')
    info["row_count"] = cur.fetchone()[0]
    return info

def main():
    out = {"seq":7,"node":os.environ.get("NODE_ID","hetzner"),"stage":"diag",
           "ts":int(time.time()),"ok":False}
    if psycopg2 is None:
        out["error"]="psycopg2 unavailable"
        gh_put(f"status/{out['node']}/seq-7-diag.json", out, "seq-7 diag (psycopg2 missing)")
        sys.exit(1)
    load_env_file(DB_ENV_PATH); load_env_file(AUTOPULL_ENV_PATH)
    try:
        conn = connect_pg(); conn.autocommit = True; cur = conn.cursor()
        out["tables"] = {}
        for sch, tbl in (("atlas","enrich_queue"),("atlas","field_provenance"),
                         ("atlas","business"),("atlas","source_record")):
            try: out["tables"][f"{sch}.{tbl}"] = introspect(cur, sch, tbl)
            except Exception as e: out["tables"][f"{sch}.{tbl}"] = {"_error":str(e)[:300]}
        cur.execute("SELECT status, count(*) FROM atlas.enrich_queue GROUP BY status ORDER BY 2 DESC")
        out["status_distribution"] = {r[0]:r[1] for r in cur.fetchall()}
        cur.execute("SELECT task_type, count(*) FROM atlas.enrich_queue GROUP BY task_type ORDER BY 2 DESC")
        out["task_type_distribution"] = {(r[0] if r[0] is not None else "<NULL>"):r[1] for r in cur.fetchall()}
        cur.execute("SELECT priority, count(*) FROM atlas.enrich_queue GROUP BY priority ORDER BY 1")
        out["priority_distribution"] = {str(r[0]):r[1] for r in cur.fetchall()}
        cur.execute("SELECT min(attempts), max(attempts), avg(attempts)::numeric(6,2) FROM atlas.enrich_queue")
        mn,mx,av = cur.fetchone()
        out["attempts_spread"] = {"min":mn,"max":mx,"avg":float(av) if av is not None else None}
        cur.execute("SELECT count(*) FROM atlas.enrich_queue WHERE status IN ('queued','error') "
                    "AND (locked_by IS NULL OR locked_at IS NULL OR locked_at < now() - interval '15 minutes')")
        out["claimable_now"] = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM atlas.enrich_queue WHERE locked_by IS NOT NULL")
        out["currently_locked"] = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM atlas.business")
        out["business_total"] = cur.fetchone()[0]
        cur.execute("SELECT count(DISTINCT business_id) FROM atlas.enrich_queue")
        out["queue_distinct_business"] = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM atlas.business b WHERE NOT EXISTS "
                    "(SELECT 1 FROM atlas.enrich_queue q WHERE q.business_id=b.id)")
        out["business_not_in_queue"] = cur.fetchone()[0]
        try:
            cur.execute("SELECT source_code, count(*) FROM atlas.field_provenance GROUP BY source_code ORDER BY 2 DESC LIMIT 30")
            out["provenance_source_codes"] = {r[0]:r[1] for r in cur.fetchall()}
            cur.execute("SELECT field, count(*) FROM atlas.field_provenance GROUP BY field ORDER BY 2 DESC LIMIT 30")
            out["provenance_fields"] = {r[0]:r[1] for r in cur.fetchall()}
        except Exception as e: out["provenance_error"] = str(e)[:300]
        out["sample_rows"] = {}
        for sch, tbl in (("atlas","enrich_queue"),("atlas","field_provenance"),("atlas","business")):
            try:
                cur.execute(f'SELECT * FROM "{sch}"."{tbl}" LIMIT 1')
                cols = [d[0] for d in cur.description]; row = cur.fetchone()
                out["sample_rows"][f"{sch}.{tbl}"] = (_redact_row(cols,row) if row else None)
            except Exception as e: out["sample_rows"][f"{sch}.{tbl}"] = {"_error":str(e)[:300]}
        cur.execute("SHOW server_version"); out["server_version"] = cur.fetchone()[0]
        cur.close(); conn.close(); out["ok"] = True
        log("diagnostic complete; pushing status-back")
    except Exception as e:
        import traceback
        out["error"] = str(e)[:500]
        out["traceback_tail"] = "\n".join(traceback.format_exc().strip().splitlines()[-25:])
        log("diagnostic FAILED:", out["error"])
    pushed = gh_put(f"status/{out['node']}/seq-7-diag.json", out, "seq-7 read-only schema diagnostic")
    if not out.get("ok"): sys.exit(1)
    if not pushed: log("WARNING: status push failed; payload follows in log")
    print("[atlas_enrich_diag] " + json.dumps(out)[:4000], flush=True)
    sys.exit(0)

if __name__ == "__main__":
    main()
