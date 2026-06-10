#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A.T.L.A.S. -> Lead Hunter handoff exporter -- read-only candidate feeder.

WHAT THIS IS
------------
A tiny, READ-ONLY exporter that runs ON THE BOX (the Hetzner node hosting the live
Postgres `tuanichat_atlas` / schema `atlas`). It selects `atlas.business` rows that
HAVE a website/domain (the Lead Hunter's whole pipeline needs a URL to crawl +
chat-detect), maps each to an OSM-shaped candidate the hunter's `stage_candidates()`
already understands, and PUBLISHES them as JSONL via the EXACT SAME status-back
channel the metrics bridge (seq-15) and enrich worker use:

    status/<node>/atlas-feed/atlas-feed-<batch>.jsonl   (the candidate rows)
    status/<node>/atlas-feed/atlas-feed-latest.json     (a manifest of recent batches
                                                          + the export cursor + counts)

The WordPress `tnc-atlas-feeder` plugin GETs those static files over HTTPS
(raw.githubusercontent.com) server-side and feeds the rows into the hunter.

WHY THIS DESIGN (security model, stated honestly)
-------------------------------------------------
Postgres is exposed on :5432 to the operator only. We DO NOT open a new public port
and we DO NOT expose Postgres to the internet. We reuse the proven PUSH model: the box
computes locally and PUTs token-authenticated files to the manifests repo via the
GitHub Contents API (STATUS_TOKEN/STATUS_REPO from /etc/atlas/autopull.env -- the same
token the enrich worker + seq-15 already use). WordPress only ever reads static HTTPS
JSON/JSONL; it never touches the DB and holds no DB credentials.

Connection is READ-ONLY: we open a transaction, SET TRANSACTION READ ONLY, run only
SELECTs, and roll back. No DML, no DDL, ever -- even a bug cannot write.

SCHEMA IS INTROSPECTED, NOT GUESSED. Every table/column name is resolved at runtime
from information_schema using the same CANDIDATES/pick_col strategy as the live
importers (overture_pg_import.py / atlas_enrich_worker.py / atlas_metrics_export.py).
If a column is named outside its candidate list, that field is emitted as null (honest)
rather than crashing or faking a value.

PRIORITY LADDER (council plan section 3):
    TIER 1  real-time qualified : source ~ ct_new_ssl/ct/cert AND mx-qualified
    TIER 2  real-time           : ct/cert/mx rows
    TIER 3  high-fit batch      : edgar / irs / nonprofit / socrata
    TIER 4  breadth batch       : overture / everything else
Rows are emitted highest-tier-first, freshest-first. If the enrich_queue carries a
`priority` column (Smart Brain seq-13 may write it), lower priority sorts first within
a tier. The tier is computed from the row's source provenance, so the hunter crawls the
greenfield/reachable-first rows before the bulk.

DEDUPE (so re-runs never re-export the same business):
    A cursor of already-exported business ids is kept LOCALLY at
    /var/lib/atlas/handoff/exported_ids.txt AND mirrored to the repo as
    status/<node>/atlas-feed/exported_cursor.json (count + max id + a bloom-ish id set
    capped to the last EXPORT_CURSOR_KEEP ids). On each run we exclude ids already in
    the local cursor; the WP side ALSO dedupes (the hunter's 4-key dedupe), so a missed
    cursor never double-stages -- it just wastes one crawl that the 4-key drops anyway.

.gov / .mil SUPPRESSION (defense-in-depth):
    Any row whose resolved domain is under .gov/.mil (incl. ny.gov / army.mil style) is
    DROPPED from the export. The enrich worker already suppresses contact data for these;
    we additionally never hand them to the hunter. Non-overridable.

MOJIBAKE SANITIZE: business names are repaired (common UTF-8-as-latin1 mis-decodes,
literal \\u2019 etc.) on the way out so the hunter never stages garbled names.

FAIL-SOFT / FAIL-LOUD
---------------------
- DB down / atlas.business missing -> writes an honest "unreachable" manifest
  (reachable=false) and exits 0 (the timer never wedges); no candidate file is written.
- GitHub unreachable -> writes the JSONL + manifest LOCALLY
  (/var/lib/atlas/handoff/) and exits 0.
- --selftest connects, verifies atlas.business + a website column exist, builds (but does
  NOT publish) one bounded batch, rolls back. Exit 0 ok / 3 no-Postgres (clean degrade,
  no traceback) so the install step can gate on it.

USAGE
-----
  atlas_handoff_export.py             select + publish a batch (+ update cursor)
  atlas_handoff_export.py --selftest  validate db.env, connect, schema, build one batch
  atlas_handoff_export.py --dry-run   build + print the batch, do NOT push, do NOT
                                      advance the cursor (local preview only)
"""
from __future__ import print_function
import base64
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error

try:
    import psycopg2
except Exception:
    psycopg2 = None

# --------------------------------------------------------------------------- #
# Config (env; all optional except DB creds which come from db.env)
# --------------------------------------------------------------------------- #
DB_ENV_PATH       = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
AUTOPULL_ENV_PATH = os.environ.get("ATLAS_AUTOPULL_CONF", "/etc/atlas/autopull.env")
STATE_DIR         = os.environ.get("ATLAS_HANDOFF_STATE", "/var/lib/atlas/handoff")
SCHEMA            = os.environ.get("ATLAS_SCHEMA", "atlas")
BATCH_SIZE        = int(os.environ.get("ATLAS_HANDOFF_BATCH", "2000"))
# How many already-exported ids to keep in the published cursor set (local keeps all).
EXPORT_CURSOR_KEEP = int(os.environ.get("ATLAS_HANDOFF_CURSOR_KEEP", "50000"))
# How many feed files to list in the latest manifest.
FEED_KEEP         = int(os.environ.get("ATLAS_HANDOFF_FEED_KEEP", "12"))

LOCAL_IDS_PATH    = os.path.join(STATE_DIR, "exported_ids.txt")
LOCAL_FEED_DIR    = os.path.join(STATE_DIR, "feed")


def log(msg):
    sys.stderr.write("[atlas-handoff] %s\n" % msg)
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
        application_name="atlas_handoff_export",
    )
    conn.autocommit = False
    return conn


# --------------------------------------------------------------------------- #
# Schema introspection (DO NOT guess -- same strategy as the live importers)
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


def table_pk(cur, schema, table):
    cur.execute(
        "SELECT a.attname FROM pg_index i "
        "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
        "WHERE i.indrelid=%s::regclass AND i.indisprimary", ("%s.%s" % (schema, table),))
    rows = [r[0] for r in cur.fetchall()]
    return rows[0] if rows else None


def pick_col(colset, candidates):
    for c in candidates:
        if c in colset:
            return c
    return None


# logical field -> candidate column names (first present wins). Aligned with
# atlas_enrich_worker.py CANDIDATES so the same real DDL is matched.
CAND = {
    "name":     ["name", "business_name", "title", "display_name", "legal_name"],
    "website":  ["website", "url", "website_url", "homepage", "domain"],
    "phone":    ["phone", "phone_number", "telephone", "contact_phone", "phone_e164"],
    "email":    ["email", "email_address", "contact_email"],
    "locality": ["locality", "city", "town"],
    "region":   ["region", "state", "province", "state_abbr"],
    "category": ["category", "primary_category", "categories", "license_description",
                 "industry", "naics", "sic", "sector"],
    "lat":      ["latitude", "lat", "y"],
    "lon":      ["longitude", "lon", "lng", "x"],
    "updated":  ["last_updated", "updated_at", "modified_at", "updated"],
    "created":  ["created_at", "inserted_at", "first_seen", "first_observed", "created"],
    # source_record provenance
    "sr_source": ["source", "data_source", "origin", "source_code"],
    "sr_bizref": ["business_ref", "business_id", "biz_id", "ref"],
}

# enrich_queue: status/state column + priority column candidates (Smart Brain may set
# priority). The worker uses `state`, the metrics exporter probed `status` -- cover both.
QUEUE_REF_CAND   = ["business_ref", "business_id", "biz_id", "ref"]
QUEUE_PRIO_CAND  = ["priority", "prio"]

# --- source -> tier mapping (council priority ladder, section 3) ------------- #
# A row's tier is the BEST (lowest number) tier of any of its source_records.
RE_REALTIME_CERT = re.compile(r"(ct[_\-]?new[_\-]?ssl|ct_log|ct\b|cert|ssl)", re.I)
RE_REALTIME_MX   = re.compile(r"(mx[_\-]?qual|mx\b)", re.I)
RE_HIGHFIT       = re.compile(r"(edgar|form[_\-]?d|irs|eo[_\-]?bmf|nonprofit|990|socrata)", re.I)
# everything else (overture, unknown) -> tier 4


def _to_text(v):
    if v is None:
        return None
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8", "replace")
        except Exception:
            return None
    return str(v)


# --------------------------------------------------------------------------- #
# Mojibake repair (names get garbled by latin1<->utf8 round-trips in some feeds)
# --------------------------------------------------------------------------- #
_MOJIBAKE_LITERALS = {
    "\\u2019": "’", "\\u2018": "‘", "\\u201c": "“",
    "\\u201d": "”", "\\u2013": "–", "\\u2014": "—",
    "\\u00e9": "é", "\\u00e8": "è", "\\u00f1": "ñ",
    "\\u00fc": "ü", "\\u00e1": "á", "\\u00ed": "í",
    "\\u00f3": "ó", "\\u00e7": "ç", "\\u0026": "&",
}
_MOJIBAKE_SEQ = {
    "â€™": "’",  # â€™ -> '
    "â€œ": "“",  # â€œ -> "
    "â€": "”",  # â€  -> "
    "â€“": "–",  # â€“ -> en dash
    "â€”": "—",  # â€” -> em dash
    "Ã©": "é",        # Ã© -> é
    "Ã±": "ñ",        # Ã± -> ñ
}


def sanitize_name(s):
    if not s:
        return s
    out = s
    for bad, good in _MOJIBAKE_LITERALS.items():
        if bad in out:
            out = out.replace(bad, good)
    for bad, good in _MOJIBAKE_SEQ.items():
        if bad in out:
            out = out.replace(bad, good)
    # collapse whitespace
    out = re.sub(r"\s+", " ", out).strip()
    return out


# --------------------------------------------------------------------------- #
# Domain helpers (stdlib only)
# --------------------------------------------------------------------------- #
def registrable_domain(host_or_url):
    if not host_or_url:
        return None
    h = host_or_url.strip().lower()
    if "://" in h:
        h = h.split("://", 1)[1]
    h = h.split("/")[0]
    h = h.split("@")[-1].split(":")[0].strip().strip(".")
    if not h or "." not in h:
        return None
    parts = h.split(".")
    two_label = {"co.uk", "org.uk", "gov.uk", "ac.uk", "com.au", "net.au",
                 "org.au", "co.nz", "co.za", "com.br"}
    if len(parts) >= 3 and ".".join(parts[-2:]) in two_label:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def is_gov_mil(domain):
    if not domain:
        return False
    d = domain.lower()
    return (d.endswith(".gov") or d.endswith(".mil")
            or ".gov." in ("." + d + ".") or ".mil." in ("." + d + "."))


# --------------------------------------------------------------------------- #
# Local exported-id cursor
# --------------------------------------------------------------------------- #
def load_exported_ids():
    ids = set()
    if os.path.exists(LOCAL_IDS_PATH):
        try:
            with open(LOCAL_IDS_PATH, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        ids.add(line)
        except OSError as e:
            log("WARNING could not read cursor %s: %s" % (LOCAL_IDS_PATH, e))
    return ids


def append_exported_ids(new_ids):
    if not new_ids:
        return
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        with open(LOCAL_IDS_PATH, "a", encoding="utf-8") as fh:
            for i in new_ids:
                fh.write("%s\n" % i)
    except OSError as e:
        log("WARNING could not append cursor %s: %s" % (LOCAL_IDS_PATH, e))


# --------------------------------------------------------------------------- #
# Build the source->tier map for the rows we are about to select
# --------------------------------------------------------------------------- #
def build_tier_map(cur, ref_to_bizid):
    """Return {business_ref(text) -> (tier_int, sources_csv)} from source_record.
    Read-only. If source_record/columns absent, everything is tier 4 (unknown)."""
    tiers = {}
    sources = {}
    if not ref_to_bizid:
        return tiers, sources
    if not table_exists(cur, SCHEMA, "source_record"):
        return tiers, sources
    sr_cols = table_columns(cur, SCHEMA, "source_record")
    src_col = pick_col(sr_cols, CAND["sr_source"])
    ref_col = pick_col(sr_cols, CAND["sr_bizref"])
    if not src_col or not ref_col:
        return tiers, sources
    refs = list(ref_to_bizid.keys())
    # chunk the IN list to keep the query bounded
    CH = 1000
    for i in range(0, len(refs), CH):
        chunk = refs[i:i + CH]
        cur.execute(
            'SELECT "%s"::text, "%s" FROM "%s"."source_record" '
            'WHERE "%s"::text = ANY(%%s)'
            % (ref_col, src_col, SCHEMA, ref_col), (chunk,))
        for ref, src in cur.fetchall():
            ref = _to_text(ref)
            src = (_to_text(src) or "").strip()
            sources.setdefault(ref, set()).add(src or "unknown")
    for ref, srcs in sources.items():
        tier = 4
        mx = any(RE_REALTIME_MX.search(s) for s in srcs)
        cert = any(RE_REALTIME_CERT.search(s) for s in srcs)
        highfit = any(RE_HIGHFIT.search(s) for s in srcs)
        if cert and mx:
            tier = 1
        elif cert or mx:
            tier = 2
        elif highfit:
            tier = 3
        tiers[ref] = tier
    return tiers, {r: sorted(s) for r, s in sources.items()}


# --------------------------------------------------------------------------- #
# Select + map candidates
# --------------------------------------------------------------------------- #
def select_candidates(conn, limit, exclude_ids):
    """READ-ONLY. Returns (rows, cols_meta). Each row is an OSM-shaped candidate
    dict the hunter's ingest_poi_result()/stage_candidates() consumes."""
    cur = conn.cursor()
    cur.execute("SET TRANSACTION READ ONLY")

    if not table_exists(cur, SCHEMA, "business"):
        raise RuntimeError("%s.business not found" % SCHEMA)

    bcols = table_columns(cur, SCHEMA, "business")
    pk = table_pk(cur, SCHEMA, "business")
    if not pk:
        pk = pick_col(bcols, ["id", "business_id", "source_id", "external_id", "ext_id"])
    if not pk:
        raise RuntimeError("%s.business has no resolvable primary key" % SCHEMA)

    col = {k: pick_col(bcols, CAND[k]) for k in
           ("name", "website", "phone", "email", "locality", "region",
            "category", "lat", "lon", "updated", "created")}
    if not col["website"]:
        raise RuntimeError("%s.business has no website/url/domain column to crawl" % SCHEMA)
    if not col["name"]:
        raise RuntimeError("%s.business has no name column" % SCHEMA)

    # Build the SELECT list. We over-select a multiple of `limit` because tier
    # ranking + .gov/.mil drop + already-exported drop happen in Python (the
    # source tier needs a join we keep simple/read-only).
    over = max(limit * 3, limit + 500)
    sel_cols = [pk] + [c for c in (
        col["name"], col["website"], col["phone"], col["email"],
        col["locality"], col["region"], col["category"],
        col["lat"], col["lon"]) if c]
    # de-dup the select list while preserving order
    seen_sel = set()
    sel_cols = [c for c in sel_cols if not (c in seen_sel or seen_sel.add(c))]
    order_col = col["updated"] or col["created"] or pk
    site_col = col["website"]
    # BRAIN-RANKED: if the Smart Brain's ranking table exists, order by its
    # closure expected-value first (highest EV = most likely to close), then
    # freshness. While EV is absent/uniform (no real outcomes yet) freshness
    # dominates -> identical to before; once real demo/chat/convert outcomes
    # flow into atlas.outcome_stats the brain's EV varies per source+industry
    # and this feed AUTO-FLIPS to highest-closure-first. Read-only LEFT JOIN.
    _bc = conn.cursor()
    _bc.execute("SELECT to_regclass('atlas.brain_ranking') IS NOT NULL")
    _has_brain = bool(_bc.fetchone()[0]); _bc.close()
    _selsql = ", ".join('b."%s"' % c for c in sel_cols)
    if _has_brain:
        sql = (
            'SELECT %s FROM "%s"."business" b '
            'LEFT JOIN (SELECT business_ref, max(ev) AS ev '
            'FROM atlas.brain_ranking GROUP BY business_ref) br '
            'ON br.business_ref = b."%s"::text '
            'WHERE b."%s" IS NOT NULL AND btrim((b."%s")::text) <> \'\' '
            'ORDER BY br.ev DESC NULLS LAST, b."%s" DESC NULLS LAST '
            'LIMIT %%s'
            % (_selsql, SCHEMA, pk, site_col, site_col, order_col))
    else:
        sql = (
            'SELECT %s FROM "%s"."business" b '
            'WHERE b."%s" IS NOT NULL AND btrim((b."%s")::text) <> \'\' '
            'ORDER BY b."%s" DESC NULLS LAST LIMIT %%s'
            % (_selsql, SCHEMA, site_col, site_col, order_col))
    cur.execute(sql, (over,))
    raw = cur.fetchall()
    idx = {c: i for i, c in enumerate(sel_cols)}

    # Resolve tiers for the candidate refs (the PK text == business_ref the
    # enrich worker uses).
    ref_to_bizid = {}
    for r in raw:
        ref = _to_text(r[idx[pk]])
        if ref is not None:
            ref_to_bizid[ref] = ref
    tier_map, src_map = build_tier_map(cur, ref_to_bizid)

    conn.rollback()  # READ-ONLY: never commit

    out = []
    dropped_gov = 0
    dropped_dup = 0
    dropped_nodom = 0
    for r in raw:
        bid = _to_text(r[idx[pk]])
        if bid is None or bid in exclude_ids:
            dropped_dup += 1
            continue
        site = _to_text(r[idx[site_col]]) or ""
        dom = registrable_domain(site)
        if not dom:
            dropped_nodom += 1
            continue
        if is_gov_mil(dom):
            dropped_gov += 1
            continue
        name = sanitize_name(_to_text(r[idx[col["name"]]]) or "")
        if not name:
            dropped_nodom += 1
            continue
        # ensure the website is a crawlable URL (scheme on)
        website = site if "://" in site else ("https://" + dom)
        phone = _to_text(r[idx[col["phone"]]]) if col["phone"] else None
        city  = _to_text(r[idx[col["locality"]]]) if col["locality"] else None
        state = _to_text(r[idx[col["region"]]]) if col["region"] else None
        if state:
            state = state.strip().upper()[:4]
        cat   = _to_text(r[idx[col["category"]]]) if col["category"] else None
        lat   = r[idx[col["lat"]]] if col["lat"] else None
        lon   = r[idx[col["lon"]]] if col["lon"] else None
        tier  = tier_map.get(bid, 4)
        srcs  = src_map.get(bid, [])
        atlas_source = srcs[0] if srcs else "atlas"

        out.append({
            # the WP feeder maps these straight into the hunter's OSM-shaped row
            # (osm_type/osm_id become the 4th dedupe key: atlas/<business_id>)
            "atlas_business_id": bid,
            "name": name,
            "website": website,
            "domain": dom,
            "phone": phone,
            "city": city,
            "state": state,
            "lat": _num(lat),
            "lon": _num(lon),
            "category": cat,
            "atlas_source": atlas_source,
            "atlas_sources": srcs,
            "tier": tier,
        })

    # priority ladder: tier asc (1 first), then we keep DB order (freshest first)
    out.sort(key=lambda x: x["tier"])
    kept = out[:limit]
    meta = {
        "pk": pk,
        "columns": {k: v for k, v in col.items()},
        "selected": len(raw),
        "emitted": len(kept),
        "dropped_gov_mil": dropped_gov,
        "dropped_already_exported": dropped_dup,
        "dropped_no_domain_or_name": dropped_nodom,
        "tier_breakdown": _tier_counts(kept),
    }
    cur.close()
    return kept, meta


def _num(v):
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _tier_counts(rows):
    c = {1: 0, 2: 0, 3: 0, 4: 0}
    for r in rows:
        c[r.get("tier", 4)] = c.get(r.get("tier", 4), 0) + 1
    return {("tier%d" % k): v for k, v in c.items()}


# --------------------------------------------------------------------------- #
# Status-back publish (GitHub Contents API) -- identical pattern to seq-15
# --------------------------------------------------------------------------- #
def gh_put(path, raw_bytes, msg):
    token = os.environ.get("STATUS_TOKEN")
    repo = os.environ.get("STATUS_REPO")
    if not token or not repo:
        log("STATUS_TOKEN/STATUS_REPO unset -> local-only (%s)" % path)
        return False
    api = os.environ.get("STATUS_API_BASE", "https://api.github.com")
    branch = os.environ.get("STATUS_BRANCH", "main")
    content_b64 = base64.b64encode(raw_bytes).decode("ascii")

    def _req(method, url, data=None):
        req = urllib.request.Request(url, method=method, data=data)
        req.add_header("Authorization", "Bearer %s" % token)
        req.add_header("Accept", "application/vnd.github+json")
        req.add_header("User-Agent", "atlas-handoff")
        req.add_header("X-GitHub-Api-Version", "2022-11-28")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
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


def write_local(raw_bytes, path):
    try:
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(raw_bytes)
        log("local cache -> %s" % path)
    except OSError as e:
        log("WARNING could not write %s: %s" % (path, e))


def node_id():
    return (os.environ.get("ATLAS_NODE")
            or os.environ.get("NODE_ID")
            or "hetzner")


def jsonl_bytes(rows):
    return ("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n").encode("utf-8")


# --------------------------------------------------------------------------- #
# Run
# --------------------------------------------------------------------------- #
def run(dry_run=False):
    load_env_file(DB_ENV_PATH)
    load_env_file(AUTOPULL_ENV_PATH)
    node = node_id()
    feed_dir = "status/%s/atlas-feed" % node
    batch = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())

    exclude = load_exported_ids()
    rows = []
    meta = {}
    reachable = False
    try:
        conn = connect_pg()
        try:
            rows, meta = select_candidates(conn, BATCH_SIZE, exclude)
            reachable = True
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as e:
        log("select failed (%s): %s" % (type(e).__name__, e))
        manifest = {
            "kind": "atlas-feed-manifest", "schema_version": 1, "node": node,
            "reachable": False, "status": "unreachable",
            "error": "%s: %s" % (type(e).__name__, e),
            "batches": [], "exported_total": len(exclude), "ts": int(time.time()),
        }
        body = json.dumps(manifest).encode("utf-8")
        write_local(body, os.path.join(STATE_DIR, "atlas-feed-latest.json"))
        if not dry_run:
            gh_put("%s/atlas-feed-latest.json" % feed_dir, body,
                   "atlas-feed %s unreachable" % node)
        return 0

    log("emitted=%d (tier=%s) dropped gov=%s dup=%s nodom=%s" % (
        len(rows), meta.get("tier_breakdown"), meta.get("dropped_gov_mil"),
        meta.get("dropped_already_exported"), meta.get("dropped_no_domain_or_name")))

    feed_name = "atlas-feed-%s.jsonl" % batch
    feed_body = jsonl_bytes(rows)
    new_ids = [r["atlas_business_id"] for r in rows]

    # the WP-side feeder reads this manifest first, then each listed JSONL.
    manifest = {
        "kind": "atlas-feed-manifest",
        "schema_version": 1,
        "node": node,
        "reachable": reachable,
        "status": "ok",
        "batch": batch,
        "row_shape": ["atlas_business_id", "name", "website", "domain", "phone",
                      "city", "state", "lat", "lon", "category", "atlas_source", "tier"],
        "tier_legend": {
            "1": "real-time qualified (CT/SSL + MX) -- hunt first",
            "2": "real-time (CT/SSL or MX)",
            "3": "high-fit batch (edgar/irs/nonprofit/socrata)",
            "4": "breadth batch (overture/other)",
        },
        "batches": [{"file": feed_name, "rows": len(rows), "batch": batch}],
        "emitted": len(rows),
        "tier_breakdown": meta.get("tier_breakdown"),
        "drops": {
            "gov_mil": meta.get("dropped_gov_mil"),
            "already_exported": meta.get("dropped_already_exported"),
            "no_domain_or_name": meta.get("dropped_no_domain_or_name"),
        },
        "exported_total": len(exclude) + len(new_ids),
        "ts": int(time.time()),
    }
    manifest_body = json.dumps(manifest).encode("utf-8")

    # cursor mirror (capped id set so the published JSON stays bounded)
    all_ids = list(exclude) + new_ids
    cursor = {
        "kind": "atlas-feed-cursor", "node": node,
        "exported_total": len(all_ids),
        "ids_tail": all_ids[-EXPORT_CURSOR_KEEP:],
        "ts": int(time.time()),
    }
    cursor_body = json.dumps(cursor).encode("utf-8")

    if dry_run:
        print(json.dumps(manifest, indent=2))
        print("--- first 3 candidate rows ---")
        for r in rows[:3]:
            print(json.dumps(r, ensure_ascii=False))
        log("DRY-RUN: cursor NOT advanced, nothing pushed")
        return 0

    # local cache first (always), then publish
    write_local(feed_body, os.path.join(LOCAL_FEED_DIR, feed_name))
    write_local(manifest_body, os.path.join(STATE_DIR, "atlas-feed-latest.json"))

    ok_feed = gh_put("%s/%s" % (feed_dir, feed_name), feed_body,
                     "atlas-feed %s %s rows=%d" % (node, batch, len(rows)))
    gh_put("%s/atlas-feed-latest.json" % feed_dir, manifest_body,
           "atlas-feed manifest %s %s" % (node, batch))
    gh_put("%s/exported_cursor.json" % feed_dir, cursor_body,
           "atlas-feed cursor %s total=%d" % (node, len(all_ids)))

    # only advance the local cursor once the candidate file is safely published
    # (or local-only mode); never lose a batch silently.
    if ok_feed or not os.environ.get("STATUS_TOKEN"):
        append_exported_ids(new_ids)
    else:
        log("feed publish failed -> cursor NOT advanced (will retry this batch)")
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
        bcols = table_columns(cur, SCHEMA, "business")
        if not pick_col(bcols, CAND["website"]):
            log("SELFTEST FAIL: no website/url/domain column on %s.business" % SCHEMA)
            return 2
        conn.rollback()
        rows, meta = select_candidates(conn, min(BATCH_SIZE, 50), set())
        log("SELFTEST OK: built %d candidates (tier=%s), nothing published"
            % (len(rows), meta.get("tier_breakdown")))
        print(json.dumps({"selftest": "ok", "candidates": len(rows),
                          "meta": meta}, indent=2))
        return 0
    except Exception as e:
        log("SELFTEST FAIL: %s: %s" % (type(e).__name__, e))
        return 2
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
