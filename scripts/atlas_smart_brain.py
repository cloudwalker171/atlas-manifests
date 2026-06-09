#!/usr/bin/env python3
"""
ATLAS V2 Smart Brain -- Postgres-backed decision/intelligence service
=====================================================================
The decision layer that ranks WHAT TO PULL / ENRICH / DEPLOY NEXT on the
Hetzner box. It WRAPS the proven, seed-42 TuaniChat V2 intelligence modules
(enrichment_brain_v2.py, jarvis_v2.py, guardians_v2.py) and applies them to
the live atlas.* Postgres schema:

  * cost-aware enrichment waterfall ordering  (CostAwareWaterfall)
  * EV-ranked queue prioritization            (OutcomeFeedbackRanker)
  * cross-source corroboration                (CorroborationScorer)
  * self-tuning entity-resolution threshold   (SelfTuningResolver)
  * predictive J.A.R.V.I.S. breach forecast   (BreachForecaster + ReasoningLog)
  * closed-loop / honest-counter guardians    (guardians_v2)

PROVENANCE / HONESTY
--------------------
The ALGORITHMS are proven 8/8 (seed=42) by atlas_brain_proof.py (byte-for-byte
the TuaniChat V2 harness). That gate runs at apply time and FAILS CLOSED.
This wrapper -- the GLUE that reads atlas.enrich_queue + outcome stats and
writes priorities/recommendations back -- is VALIDATED ONLY against the seeded
harness today. Its LIVE behavior cannot be validated until data + an
enrich_queue are actually flowing on the box. Every code path that depends on
live data is marked "NEEDS LIVE DATA" below and NO-OPS GRACEFULLY on empty.

POSTURE
-------
* Verbose / fail-loud: everything is logged; unexpected errors raise (nonzero
  exit) so the auto-pull apply step fails and the box auto-rolls-back + retries.
* Fail-closed: empty/missing tables are EXPECTED -> graceful no-op (exit 0);
  real faults (cannot connect, cannot write) -> loud error (nonzero exit).
* Additive only: never ALTERs atlas.business / atlas.source_record /
  atlas.field_provenance / atlas.enrich_queue structure. The only mutation of
  an existing table is enrich_queue.priority (a value, the literal job of a
  prioritizer) and that is skipped unless ATLAS_BRAIN_WRITE_PRIORITY=1.
* .gov / .mil suppression preserved: any business whose resolved domain is
  under .gov/.mil is EXCLUDED from ranking output entirely. The brain never
  prioritizes, surfaces, or recommends contact enrichment for gov/mil. (The
  enrich worker enforces the same at the contact-write layer; this is defense
  in depth at the decision layer.)
* The brain RECOMMENDS (writes reasoning + a pre_throttle recommendation); it
  NEVER stops/disables workers, ssh, or the firewall.

Subcommands
-----------
  --migrate    create atlas.* brain tables (idempotent, additive). HETZNER ONLY.
  --selftest   load env; import the V2 modules + psycopg2; connect Postgres;
               verify atlas schema reachable and brain tables present/creatable.
               Fail-loud gate (nonzero on any failure). Does NOT re-run the 8
               proofs -- atlas_brain_proof.py owns that gate.
  --once       one decision pass: rank the queue by learned EV, forecast ops
               breaches, surface status. No-ops gracefully on an empty queue.
  --loop       run --once every ATLAS_BRAIN_INTERVAL sec (default 300), idling
               gracefully when there is nothing to do.

Stdlib + psycopg2 only. No paid AI/API calls. Deterministic.
"""

import base64
import datetime
import json
import os
import sys
import time
import traceback
import urllib.error
import urllib.request

# psycopg2 is required for every DB path; import lazily so --help-ish misuse
# still prints something useful.
try:
    import psycopg2
except Exception:  # pragma: no cover
    psycopg2 = None

# The proven V2 intelligence modules live alongside this file in /opt/atlas/brain.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# --------------------------------------------------------------------------- #
# config (mirrors atlas_enrich_worker.py / atlas_healthcheck.py conventions)
# --------------------------------------------------------------------------- #
DB_ENV_PATH       = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
AUTOPULL_ENV_PATH = os.environ.get("ATLAS_AUTOPULL_CONF", "/etc/atlas/autopull.env")

BRAIN_STATE_DIR = os.environ.get("ATLAS_BRAIN_STATE_DIR", "/var/lib/atlas/brain")
NODE_ID         = os.environ.get("NODE_ID", "hetzner")
INTERVAL_SEC    = int(os.environ.get("ATLAS_BRAIN_INTERVAL", "300"))
RANK_LIMIT      = int(os.environ.get("ATLAS_BRAIN_RANK_LIMIT", "5000"))
OPS_CEILING     = int(os.environ.get("ATLAS_BRAIN_OPS_CEILING", "620000"))
WRITE_PRIORITY  = os.environ.get("ATLAS_BRAIN_WRITE_PRIORITY", "0") not in ("0", "false", "no", "")

QUEUE_TBL = ("atlas", "enrich_queue")


def log(msg):
    print(f"[atlas_brain] {datetime.datetime.now(datetime.timezone.utc).isoformat()} {msg}",
          flush=True)


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
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def pick(*names, default=None):
    for n in names:
        if os.environ.get(n):
            return os.environ[n]
    return default


def connect_pg():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is not installed (pip install psycopg2-binary)")
    conn = psycopg2.connect(
        host=pick("PGHOST", "DB_HOST", "ATLAS_DB_HOST", default="localhost"),
        port=pick("PGPORT", "DB_PORT", "ATLAS_DB_PORT", default="5432"),
        dbname=pick("PGDATABASE", "DB_NAME", "ATLAS_DB_NAME", default="tuanichat_atlas"),
        user=pick("PGUSER", "DB_USER", "ATLAS_DB_USER", default="atlas"),
        password=pick("PGPASSWORD", "DB_PASSWORD", "ATLAS_DB_PASSWORD", default=None),
        connect_timeout=int(os.environ.get("ATLAS_DB_CONNECT_TIMEOUT", "10")),
        application_name="atlas_smart_brain",
    )
    conn.autocommit = False
    return conn


# --------------------------------------------------------------------------- #
# .gov / .mil suppression -- same rule as atlas_enrich_worker.is_gov_mil
# --------------------------------------------------------------------------- #
def is_gov_mil(domain):
    if not domain:
        return False
    d = domain.strip().lower().rstrip(".")
    if not d:
        return False
    return (d.endswith(".gov") or d.endswith(".mil")
            or ".gov." in ("." + d + ".") or ".mil." in ("." + d + "."))


# --------------------------------------------------------------------------- #
# schema introspection helpers
# --------------------------------------------------------------------------- #
def regclass_exists(cur, qualified):
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (qualified,))
    return bool(cur.fetchone()[0])


def table_columns(cur, schema, table):
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema=%s AND table_name=%s", (schema, table))
    return {r[0] for r in cur.fetchall()}


# --------------------------------------------------------------------------- #
# DDL -- brain's own auxiliary tables. All IF NOT EXISTS, additive, idempotent.
# Nothing here alters existing atlas tables.
# --------------------------------------------------------------------------- #
def ensure_brain_schema(conn):
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS atlas")
    # learned outcome statistics per (source, path) -- fed by replies/bounces/
    # conversions as they flow. Empty = neutral = current FIFO behavior.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS atlas.outcome_stats (
            source      text NOT NULL,
            path        text NOT NULL DEFAULT 'default',
            enriched    bigint NOT NULL DEFAULT 0,
            contactable bigint NOT NULL DEFAULT 0,
            bounced     bigint NOT NULL DEFAULT 0,
            replied     bigint NOT NULL DEFAULT 0,
            converted   bigint NOT NULL DEFAULT 0,
            updated_at  timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT outcome_stats_pk PRIMARY KEY (source, path)
        )""")
    # learned waterfall step yield (ops_cost / historical yield ordering).
    cur.execute("""
        CREATE TABLE IF NOT EXISTS atlas.waterfall_yield (
            step       text PRIMARY KEY,
            runs       bigint NOT NULL DEFAULT 0,
            gain       double precision NOT NULL DEFAULT 0,
            updated_at timestamptz NOT NULL DEFAULT now()
        )""")
    # operator corrections that tune the entity-resolution threshold.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS atlas.er_corrections (
            id         bigserial PRIMARY KEY,
            kind       text NOT NULL CHECK (kind IN ('false_merge','false_split')),
            note       text,
            created_at timestamptz NOT NULL DEFAULT now()
        )""")
    # the brain's ranking snapshot -- "what to enrich next", append-mostly.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS atlas.brain_ranking (
            id           bigserial PRIMARY KEY,
            business_ref text NOT NULL,
            source       text,
            ev           double precision,
            rank         int,
            run_id       text NOT NULL,
            created_at   timestamptz NOT NULL DEFAULT now()
        )""")
    cur.execute("""CREATE INDEX IF NOT EXISTS brain_ranking_run
                   ON atlas.brain_ranking (run_id, rank)""")
    # genuinely-explainable reasoning log (recomputable decisions).
    cur.execute("""
        CREATE TABLE IF NOT EXISTS atlas.reasoning_log (
            id         bigserial PRIMARY KEY,
            decision   text NOT NULL,
            inputs     jsonb,
            rule       text,
            because    text,
            alternatives jsonb,
            confidence real,
            engine     text NOT NULL DEFAULT 'deterministic',
            created_at timestamptz NOT NULL DEFAULT now()
        )""")
    # meta-guardian findings (dead / ineffective / dishonest-counters).
    cur.execute("""
        CREATE TABLE IF NOT EXISTS atlas.meta_findings (
            id         bigserial PRIMARY KEY,
            guardian   text NOT NULL,
            finding    text NOT NULL,
            detail     jsonb,
            created_at timestamptz NOT NULL DEFAULT now()
        )""")
    conn.commit()
    cur.close()


# --------------------------------------------------------------------------- #
# status-back (same GitHub Contents API shape as the enrich worker)
# --------------------------------------------------------------------------- #
def gh_put(path, body_obj, msg):
    token = os.environ.get("STATUS_TOKEN")
    repo = os.environ.get("STATUS_REPO")
    if not token or not repo:
        log(f"  status: local-only ({path}) STATUS_TOKEN/REPO unset")
        return False
    api = os.environ.get("STATUS_API_BASE", "https://api.github.com")
    branch = os.environ.get("STATUS_BRANCH", "main")
    content_b64 = base64.b64encode(json.dumps(body_obj).encode("utf-8")).decode("ascii")

    def _req(method, url, data=None):
        req = urllib.request.Request(url, method=method, data=data)
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Accept", "application/vnd.github+json")
        req.add_header("User-Agent", "atlas-brain")
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

    code, resp = _req("GET", f"{api}/repos/{repo}/contents/{path}?ref={branch}")
    sha = None
    if code == 200:
        try:
            sha = json.loads(resp).get("sha")
        except ValueError:
            sha = None
    payload = {"message": msg, "content": content_b64, "branch": branch}
    if sha:
        payload["sha"] = sha
    code, resp = _req("PUT", f"{api}/repos/{repo}/contents/{path}",
                      json.dumps(payload).encode("utf-8"))
    if code and 200 <= code < 300:
        log(f"  status pushed -> {path} ({code})")
        return True
    log(f"  status push FAILED {path} http={code} resp={resp[:200]}")
    return False


def write_local(obj, path):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(obj, fh)
    except OSError as e:
        log(f"  WARNING could not write {path}: {e}")


def surface(body):
    body.setdefault("lane", "brain")
    body.setdefault("node", NODE_ID)
    body.setdefault("ts", int(time.time()))
    write_local(body, os.path.join(BRAIN_STATE_DIR, "last_brain.json"))
    gh_put(f"status/{NODE_ID}/brain-{NODE_ID}.json", body,
           f"brain {NODE_ID} mode={body.get('mode')} ranked={body.get('ranked')}")


# --------------------------------------------------------------------------- #
# load learned state from Postgres into the proven V2 objects
# --------------------------------------------------------------------------- #
def load_ranker(conn):
    """NEEDS LIVE DATA: with an empty atlas.outcome_stats the ranker stays at
    its Laplace-smoothed neutral prior, which is exactly FIFO (current V1
    behavior). It only gets smart as real reply/bounce/conversion rows land."""
    from enrichment_brain_v2 import OutcomeFeedbackRanker
    ranker = OutcomeFeedbackRanker()
    cur = conn.cursor()
    if not regclass_exists(cur, "atlas.outcome_stats"):
        cur.close()
        return ranker, 0
    cur.execute("SELECT source, path, enriched, contactable, bounced, replied, "
                "converted FROM atlas.outcome_stats")
    n = 0
    for source, path, enriched, contactable, bounced, replied, converted in cur.fetchall():
        s = ranker.stats[(source, path or "default")]
        s["enriched"] = int(enriched or 0)
        s["contactable"] = int(contactable or 0)
        s["bounced"] = int(bounced or 0)
        s["replied"] = int(replied or 0)
        s["converted"] = int(converted or 0)
        n += 1
    cur.close()
    return ranker, n


def load_resolver(conn):
    """SelfTuningResolver replays operator corrections; zero corrections = the
    fixed V1 threshold (no-data fallback)."""
    from enrichment_brain_v2 import SelfTuningResolver
    resolver = SelfTuningResolver()
    cur = conn.cursor()
    if regclass_exists(cur, "atlas.er_corrections"):
        cur.execute("SELECT kind FROM atlas.er_corrections ORDER BY id")
        for (kind,) in cur.fetchall():
            try:
                resolver.record_correction(kind)
            except AssertionError:
                pass
    cur.close()
    return resolver


# --------------------------------------------------------------------------- #
# resolve (source, domain) for queued business_refs -- best effort, wrapped
# --------------------------------------------------------------------------- #
def resolve_attrs(conn, refs):
    """Best-effort lookup of a source label and resolved domain per business_ref
    so the EV ranker has a (source, path) key and so we can apply .gov/.mil
    suppression. Returns {ref: {"source": str, "domain": str|None}}.

    NEEDS LIVE DATA: depends on atlas.source_record / atlas.field_provenance
    existing and populated. Anything missing degrades to source='unknown',
    domain=None (still rankable, just at the neutral prior)."""
    out = {ref: {"source": "unknown", "domain": None} for ref in refs}
    if not refs:
        return out
    cur = conn.cursor()
    # source label from atlas.source_record (if present)
    try:
        if regclass_exists(cur, "atlas.source_record"):
            cols = table_columns(cur, "atlas", "source_record")
            src_col = next((c for c in ("source", "data_source", "origin") if c in cols), None)
            ref_col = next((c for c in ("business_ref", "business_id", "ref", "id") if c in cols), None)
            if src_col and ref_col:
                cur.execute(
                    f'SELECT "{ref_col}"::text, "{src_col}" FROM atlas.source_record '
                    f'WHERE "{ref_col}"::text = ANY(%s)', (list(refs),))
                for ref, src in cur.fetchall():
                    if ref in out and src:
                        out[ref]["source"] = str(src)
    except Exception as e:
        log(f"  resolve_attrs: source lookup skipped ({e})")
    # resolved domain from atlas.field_provenance (field in domain/website)
    try:
        if regclass_exists(cur, "atlas.field_provenance"):
            cur.execute(
                "SELECT business_ref, value FROM atlas.field_provenance "
                "WHERE business_ref = ANY(%s) AND field IN "
                "('domain','website','registrable_domain','homepage')",
                (list(refs),))
            for ref, val in cur.fetchall():
                if ref in out and val and not out[ref]["domain"]:
                    out[ref]["domain"] = str(val)
    except Exception as e:
        log(f"  resolve_attrs: domain lookup skipped ({e})")
    cur.close()
    return out


# --------------------------------------------------------------------------- #
# ops proxy series for the breach forecaster
# --------------------------------------------------------------------------- #
def ops_series(conn, buckets=12, bucket_sec=300):
    """A real, if coarse, ops proxy: enriched rows per recent time bucket from
    atlas.enrich_queue.enriched_at. <3 buckets -> forecaster no-ops (V1
    reactive fallback). NEEDS LIVE DATA to be meaningful."""
    cur = conn.cursor()
    if not regclass_exists(cur, "atlas.enrich_queue"):
        cur.close()
        return []
    cols = table_columns(cur, "atlas", "enrich_queue")
    if "enriched_at" not in cols:
        cur.close()
        return []
    cur.execute(
        "SELECT floor(extract(epoch FROM (now() - enriched_at)) / %s)::int AS b, "
        "count(*) FROM atlas.enrich_queue "
        "WHERE enriched_at >= now() - (%s || ' seconds')::interval "
        "GROUP BY b ORDER BY b DESC", (bucket_sec, buckets * bucket_sec))
    by_bucket = {int(b): int(c) for b, c in cur.fetchall()}
    cur.close()
    # oldest -> newest, zero-filled
    return [by_bucket.get(buckets - 1 - i, 0) for i in range(buckets)]


# --------------------------------------------------------------------------- #
# subcommand: --once  (one decision pass)
# --------------------------------------------------------------------------- #
def cmd_once(conn):
    ensure_brain_schema(conn)
    cur = conn.cursor()

    # ---- gather the pending queue (the thing we prioritize) ----
    if not regclass_exists(cur, "atlas.enrich_queue"):
        cur.close()
        log("no atlas.enrich_queue yet -> brain idle (no-op). "
            "Install seq with the enrichment workers first; nothing to rank.")
        surface({"mode": "idle", "reason": "no_enrich_queue", "ranked": 0})
        return 0
    cur.execute(
        "SELECT id, business_ref FROM atlas.enrich_queue "
        "WHERE state IN ('queued','error') ORDER BY priority, id LIMIT %s",
        (RANK_LIMIT,))
    pending = cur.fetchall()
    cur.close()
    if not pending:
        log("enrich_queue has 0 pending rows -> brain idle (no-op). "
            "EV ranking begins once records are queued and outcomes flow.")
        surface({"mode": "idle", "reason": "queue_empty", "ranked": 0})
        return 0

    # ---- learned state (neutral on empty tables) ----
    ranker, stat_rows = load_ranker(conn)
    resolver = load_resolver(conn)
    attrs = resolve_attrs(conn, [ref for _, ref in pending])

    # ---- .gov/.mil suppression at the decision layer (defense in depth) ----
    records, suppressed = [], 0
    for qid, ref in pending:
        a = attrs.get(ref, {"source": "unknown", "domain": None})
        if is_gov_mil(a.get("domain")):
            suppressed += 1
            continue  # never rank / surface / recommend gov/mil contact work
        records.append({"id": qid, "business_ref": ref,
                        "source": a["source"], "queued_at": qid})

    if not records:
        log(f"all {suppressed} pending rows are .gov/.mil (suppressed) -> no-op")
        surface({"mode": "idle", "reason": "all_suppressed",
                 "ranked": 0, "suppressed_gov_mil": suppressed})
        return 0

    # ---- EV-ranked prioritization (Proof P1) ----
    ordered = ranker.rank_queue(records)
    run_id = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    cur = conn.cursor()
    for rank, r in enumerate(ordered):
        ev = ranker.expected_value(r["source"])
        cur.execute(
            "INSERT INTO atlas.brain_ranking (business_ref, source, ev, rank, run_id) "
            "VALUES (%s,%s,%s,%s,%s)",
            (r["business_ref"], r["source"], float(ev), rank, run_id))
    conn.commit()

    # optional: push the ranking back into enrich_queue.priority (the literal
    # job of a prioritizer). Off by default until an operator opts in, so the
    # first live runs are observable before they steer the workers.
    repriced = 0
    if WRITE_PRIORITY:
        for rank, r in enumerate(ordered):
            cur.execute("UPDATE atlas.enrich_queue SET priority=%s, updated_at=now() "
                        "WHERE id=%s AND state IN ('queued','error')", (rank, r["id"]))
            repriced += cur.rowcount or 0
        conn.commit()
    cur.close()

    learned = stat_rows > 0
    ev_table = ranker.source_report()
    log(f"ranked {len(ordered)} rows (run_id={run_id}); suppressed_gov_mil={suppressed}; "
        f"outcome_stat_rows={stat_rows} ({'LEARNED' if learned else 'NEUTRAL=FIFO, needs live outcomes'}); "
        f"ER threshold={round(resolver.threshold,4)}; repriced={repriced} "
        f"(write_priority={'on' if WRITE_PRIORITY else 'off'})")

    # ---- predictive J.A.R.V.I.S.: forecast ops-ceiling breach (Proof P8) ----
    forecast = {"status": "skipped", "reason": "insufficient_series_needs_live_metrics"}
    series = ops_series(conn)
    if len([v for v in series if v]) >= 3:
        from jarvis_v2 import BreachForecaster, ReasoningLog
        fc = BreachForecaster()
        for v in series:
            fc.observe("ops", v)
        breach_in = fc.ticks_until_breach("ops", OPS_CEILING)
        forecast = {"status": "ok", "series_tail": series[-6:],
                    "breach_in_ticks": breach_in}
        if breach_in is not None and breach_in <= 6:
            rlog = ReasoningLog()
            entry = rlog.log(
                "PRE_THROTTLE_RECOMMENDED",
                inputs={"series_tail": series[-6:], "breach_in_ticks": breach_in,
                        "ceiling": OPS_CEILING},
                rule="forecast_breach<=6_ticks",
                because=f"ops trend reaches {OPS_CEILING} in {breach_in} ticks",
                alternatives=["wait_for_breach(V1 reactive)"], confidence=0.9)
            c2 = conn.cursor()
            c2.execute(
                "INSERT INTO atlas.reasoning_log (decision,inputs,rule,because,"
                "alternatives,confidence,engine) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (entry["decision"], json.dumps(entry["inputs"]), entry["rule"],
                 entry["because"], json.dumps(entry["alternatives_rejected"]),
                 entry["confidence"], entry["engine"]))
            conn.commit()
            c2.close()
            forecast["recommendation"] = "pre_throttle"
            log(f"  FORECAST: {rlog.explain(0)}")
        else:
            log(f"  forecast ok: breach_in_ticks={breach_in} (no pre-throttle needed)")
    else:
        log("  forecast skipped: <3 non-zero ops buckets (needs live throughput)")

    surface({
        "mode": "active",
        "ranked": len(ordered),
        "suppressed_gov_mil": suppressed,
        "outcome_stat_rows": stat_rows,
        "learning_state": "learned" if learned else "neutral_fifo_needs_live_outcomes",
        "er_threshold": round(resolver.threshold, 4),
        "repriced": repriced,
        "write_priority": WRITE_PRIORITY,
        "ev_table": ev_table,
        "forecast": forecast,
        "run_id": run_id,
        "honesty": ("Algorithms proven 8/8 (seed=42). Live ranking quality "
                    "depends on outcome_stats + enrich_queue volume, which are "
                    "still filling. .gov/.mil suppressed at the decision layer."),
    })
    return 0


# --------------------------------------------------------------------------- #
# subcommand: --selftest  (fail-loud apply gate; DB + imports, not the 8 proofs)
# --------------------------------------------------------------------------- #
def cmd_selftest():
    # 1. proven modules must import
    from enrichment_brain_v2 import (OutcomeFeedbackRanker, CostAwareWaterfall,
                                     CorroborationScorer, SelfTuningResolver)  # noqa
    from jarvis_v2 import BreachForecaster, ReasoningLog, JarvisV2  # noqa
    from guardians_v2 import ClosedLoopGuardian, AnomalyGuardian, MetaGuardian  # noqa
    log("selftest: V2 modules import OK")
    # tiny smoke: ranker neutral prior must equal FIFO (no-data fallback)
    r = OutcomeFeedbackRanker()
    q = [{"source": "a", "queued_at": 1}, {"source": "b", "queued_at": 0}]
    assert [x["queued_at"] for x in r.rank_queue(q)] == [0, 1], "FIFO fallback broken"
    log("selftest: neutral-prior FIFO fallback OK")
    # 2. psycopg2 present
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not installed")
    log("selftest: psycopg2 import OK")
    # 3. DB reachable + atlas schema present + brain tables creatable
    conn = connect_pg()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.execute("SELECT to_regclass('atlas.business')")
        has_biz = cur.fetchone()[0] is not None
        cur.close()
        ensure_brain_schema(conn)  # idempotent; proves we can write DDL
        log(f"selftest: Postgres connect OK; atlas.business present={has_biz}; "
            f"brain tables ensured")
    finally:
        conn.close()
    log("SELFTEST PASS")
    return 0


# --------------------------------------------------------------------------- #
def cmd_migrate(conn):
    ensure_brain_schema(conn)
    log("migrate: atlas.* brain tables ensured (additive, idempotent). "
        "Existing atlas tables untouched.")
    return 0


def main():
    args = set(sys.argv[1:])
    load_env_file(DB_ENV_PATH)
    load_env_file(AUTOPULL_ENV_PATH)
    os.makedirs(BRAIN_STATE_DIR, exist_ok=True)

    try:
        if "--selftest" in args:
            return cmd_selftest()
        if "--migrate" in args:
            conn = connect_pg()
            try:
                return cmd_migrate(conn)
            finally:
                conn.close()
        if "--once" in args:
            conn = connect_pg()
            try:
                return cmd_once(conn)
            finally:
                conn.close()
        if "--loop" in args:
            log(f"loop: running --once every {INTERVAL_SEC}s (idle-friendly)")
            while True:
                try:
                    conn = connect_pg()
                    try:
                        cmd_once(conn)
                    finally:
                        conn.close()
                except Exception:
                    log("loop: --once raised:\n" + traceback.format_exc())
                    surface({"mode": "error", "ranked": 0})
                time.sleep(INTERVAL_SEC)
        sys.stderr.write(__doc__)
        return 2
    except Exception:
        # fail-loud: nonzero exit -> apply step fails -> box rolls back + retries
        log("FATAL:\n" + traceback.format_exc())
        try:
            surface({"mode": "error", "ranked": 0})
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    sys.exit(main())
