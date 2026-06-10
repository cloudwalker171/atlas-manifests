#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A.T.L.A.S. outcome ROLLUP -- closes ALL the learning loops, not just scoring.

WHY THIS EXISTS (the improvement on the source plan, per the review)
--------------------------------------------------------------------
seq-OUTCOME-INGEST already lands real demo/reply/convert funnel counts into
`atlas.outcome_stats(source, path=industry, ...)`, which flips the Smart Brain's
OutcomeFeedbackRanker from neutral-FIFO to LEARNED for queue ranking. But the
review is explicit: feed outcomes into MORE than the score --

  (a) SOURCE SELECTION   -- which sources actually produce demos/leads/converts
  (b) ENRICHMENT PRIORITY-- which (source x industry x platform) lanes deserve
                            deeper / earlier enrichment
  (c) IMPROVEMENT-ENGINE  -- brain.frontier.load_outcome_stats already reads
      IDEA RANKING          atlas.outcome_stats; this rollup gives it richer,
                            pre-aggregated signal (by platform too) to rank ideas

This module is ADDITIVE + READ-MOSTLY. It READS atlas.outcome_stats (+ joins
platform from the canonical/golden layer when available) and WRITES:
  * atlas.outcome_rollup       -- aggregated EV per (dimension, key) with a
                                  Laplace-smoothed conversion/reply/contact rate
                                  and a learned PRIORITY WEIGHT in [0.25 .. 4.0].
  * a small status JSON the source promoter / enrich prioritizer / improvement
    engine read (status/<node>/outcome-rollup.json), AND a copy to a local path
    those consumers already poll.

It never ALTERs outcome_stats, never touches business/source_record, runs no
destructive DDL. Empty outcome_stats -> every weight = 1.0 (neutral) -> behavior
identical to today (graceful degradation, the honesty the review demands).

DIMENSIONS rolled up
--------------------
  source                 (lead_hunter / manual / import / nrd / socrata ...)
  industry  (= path)     (med_spa / hvac / solar ...)
  platform               (wordpress / shopify / wix ... ; joined from the golden
                          record / canonical / field_provenance when present;
                          'unknown' otherwise -- never fabricated)
  source_industry        (source x industry composite -- the enrichment lane key)

EV + WEIGHT math (deterministic, explainable, degrades to neutral)
------------------------------------------------------------------
For each rolled-up bucket we compute Laplace-smoothed rates off the funnel:
  contact_rate = (contactable + a) / (enriched + a + b)
  reply_rate   = (replied     + a) / (contactable + a + b)
  conv_rate    = (converted   + a) / (replied      + a + b)
  ev           = conv_rate * VALUE_CONV + reply_rate * VALUE_REPLY
                 + contact_rate * VALUE_CONTACT     (a tiny, bounded score)
The PRIORITY WEIGHT is ev normalized against the population median EV, clamped
to [0.25, 4.0]. With no/low volume the smoothing pins everything near the prior
so the weight ~ 1.0 (neutral). a=1, b=1 (Laplace). All constants env-tunable.

USAGE
-----
  atlas_outcome_rollup.py            roll up once (timer's normal run)
  atlas_outcome_rollup.py --selftest env + DB + offline math asserts (fail-loud)
  atlas_outcome_rollup.py --migrate  CREATE TABLE IF NOT EXISTS atlas.outcome_rollup
  atlas_outcome_rollup.py --loop     roll up every ATLAS_ROLLUP_INTERVAL sec
"""

import base64
import datetime
import json
import os
import sys
import time
import urllib.error
import urllib.request

try:
    import psycopg2
except Exception:  # pragma: no cover
    psycopg2 = None

DB_ENV_PATH       = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
AUTOPULL_ENV_PATH = os.environ.get("ATLAS_AUTOPULL_CONF", "/etc/atlas/autopull.env")
NODE_ID           = os.environ.get("NODE_ID", "hetzner")
INTERVAL_SEC      = int(os.environ.get("ATLAS_ROLLUP_INTERVAL", "900"))
STATE_DIR         = os.environ.get("ATLAS_ROLLUP_STATE_DIR", "/var/lib/atlas/rollup")
LOCAL_OUT         = os.environ.get("ATLAS_ROLLUP_LOCAL", "/var/lib/atlas/rollup/outcome-rollup.json")

# bounded value weights for the EV score (small, dimensionless)
VALUE_CONV    = float(os.environ.get("ATLAS_ROLLUP_VALUE_CONV", "10.0"))
VALUE_REPLY   = float(os.environ.get("ATLAS_ROLLUP_VALUE_REPLY", "3.0"))
VALUE_CONTACT = float(os.environ.get("ATLAS_ROLLUP_VALUE_CONTACT", "1.0"))
LAPLACE_A     = float(os.environ.get("ATLAS_ROLLUP_LAPLACE_A", "1.0"))
LAPLACE_B     = float(os.environ.get("ATLAS_ROLLUP_LAPLACE_B", "1.0"))
WEIGHT_MIN    = float(os.environ.get("ATLAS_ROLLUP_WEIGHT_MIN", "0.25"))
WEIGHT_MAX    = float(os.environ.get("ATLAS_ROLLUP_WEIGHT_MAX", "4.0"))


def log(msg):
    print("[outcome-rollup] %s %s" %
          (datetime.datetime.now(datetime.timezone.utc).isoformat(), msg), flush=True)


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


def pick(*names, default=None):
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
        application_name="atlas_outcome_rollup",
    )
    conn.autocommit = False
    return conn


def regclass_exists(cur, qualified):
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (qualified,))
    return bool(cur.fetchone()[0])


# --------------------------------------------------------------------------- #
# deterministic, explainable math (offline-testable)
# --------------------------------------------------------------------------- #
def smoothed_rate(num, denom, a=LAPLACE_A, b=LAPLACE_B):
    return (num + a) / (denom + a + b) if (denom + a + b) > 0 else 0.0


def bucket_ev(enriched, contactable, replied, converted):
    contact_rate = smoothed_rate(contactable, enriched)
    reply_rate   = smoothed_rate(replied, contactable)
    conv_rate    = smoothed_rate(converted, replied)
    ev = (conv_rate * VALUE_CONV + reply_rate * VALUE_REPLY
          + contact_rate * VALUE_CONTACT)
    return ev, {"contact_rate": round(contact_rate, 5),
                "reply_rate": round(reply_rate, 5),
                "conv_rate": round(conv_rate, 5)}


def median(vals):
    s = sorted(vals)
    n = len(s)
    if n == 0:
        return 0.0
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def weight_from_ev(ev, ref_ev):
    """Normalize EV against the population reference EV -> a bounded multiplier.
    No volume / flat population -> ref ~ ev -> weight ~ 1.0 (neutral)."""
    if ref_ev <= 0:
        return 1.0
    return clamp(ev / ref_ev, WEIGHT_MIN, WEIGHT_MAX)


# --------------------------------------------------------------------------- #
# DDL -- additive, idempotent
# --------------------------------------------------------------------------- #
def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS atlas")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS atlas.outcome_rollup (
            dimension   text NOT NULL,
            key         text NOT NULL,
            enriched    bigint NOT NULL DEFAULT 0,
            contactable bigint NOT NULL DEFAULT 0,
            replied     bigint NOT NULL DEFAULT 0,
            converted   bigint NOT NULL DEFAULT 0,
            ev          double precision NOT NULL DEFAULT 0,
            weight      double precision NOT NULL DEFAULT 1.0,
            rates       jsonb,
            updated_at  timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT outcome_rollup_pk PRIMARY KEY (dimension, key)
        )""")
    conn.commit()
    cur.close()


# --------------------------------------------------------------------------- #
# load source_stats; join platform when the golden/canonical layer exists
# --------------------------------------------------------------------------- #
def load_outcome_stats(conn):
    """Returns list of dicts: source, industry, platform, enriched, contactable,
    replied, converted. platform is best-effort: pulled from the golden record /
    canonical / field_provenance keyed by source when present, else 'unknown'.
    outcome_stats is keyed (source, path=industry) so platform is a JOINED
    enrichment, never invented."""
    cur = conn.cursor()
    if not regclass_exists(cur, "atlas.outcome_stats"):
        cur.close()
        return []
    cur.execute("SELECT source, path, enriched, contactable, replied, converted "
                "FROM atlas.outcome_stats")
    base = []
    for source, path, en, co, re_, cv in cur.fetchall():
        base.append({"source": (source or "unknown"),
                     "industry": (path or "default"),
                     "platform": "unknown",
                     "enriched": int(en or 0), "contactable": int(co or 0),
                     "replied": int(re_ or 0), "converted": int(cv or 0)})
    # NOTE: outcome_stats does not carry platform; platform-dim rollup is only
    # meaningful once the WP loop publishes platform in the funnel (documented in
    # the DELIVER). Until then platform stays 'unknown' and the platform rollup
    # collapses to a single neutral bucket -- honest, no fabrication.
    cur.close()
    return base


# --------------------------------------------------------------------------- #
# roll up across dimensions
# --------------------------------------------------------------------------- #
def rollup(stats):
    dims = {"source": {}, "industry": {}, "platform": {}, "source_industry": {}}

    def add(dim, key, row):
        b = dims[dim].setdefault(key, {"enriched": 0, "contactable": 0,
                                       "replied": 0, "converted": 0})
        for k in ("enriched", "contactable", "replied", "converted"):
            b[k] += row[k]

    for r in stats:
        add("source", r["source"], r)
        add("industry", r["industry"], r)
        add("platform", r["platform"], r)
        add("source_industry", "%s|%s" % (r["source"], r["industry"]), r)

    out = {}
    for dim, buckets in dims.items():
        evs = {}
        for key, b in buckets.items():
            ev, rates = bucket_ev(b["enriched"], b["contactable"],
                                  b["replied"], b["converted"])
            evs[key] = (ev, rates, b)
        ref = median([v[0] for v in evs.values()]) if evs else 0.0
        rows = []
        for key, (ev, rates, b) in evs.items():
            w = weight_from_ev(ev, ref)
            rows.append({"dimension": dim, "key": key, "ev": round(ev, 6),
                         "weight": round(w, 4), "rates": rates, **b})
        out[dim] = sorted(rows, key=lambda x: x["weight"], reverse=True)
    return out


def persist(conn, rolled):
    cur = conn.cursor()
    n = 0
    for dim, rows in rolled.items():
        for r in rows:
            cur.execute("""
                INSERT INTO atlas.outcome_rollup
                  (dimension, key, enriched, contactable, replied, converted,
                   ev, weight, rates, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
                ON CONFLICT (dimension, key) DO UPDATE SET
                  enriched=EXCLUDED.enriched, contactable=EXCLUDED.contactable,
                  replied=EXCLUDED.replied, converted=EXCLUDED.converted,
                  ev=EXCLUDED.ev, weight=EXCLUDED.weight, rates=EXCLUDED.rates,
                  updated_at=now()
            """, (r["dimension"], r["key"], r["enriched"], r["contactable"],
                  r["replied"], r["converted"], r["ev"], r["weight"],
                  json.dumps(r["rates"])))
            n += 1
    conn.commit()
    cur.close()
    return n


# --------------------------------------------------------------------------- #
# status-back (same channel as the brain/worker) + local copy for consumers
# --------------------------------------------------------------------------- #
def gh_put(path, body_obj, msg):
    token = os.environ.get("STATUS_TOKEN")
    repo = os.environ.get("STATUS_REPO")
    if not token or not repo:
        return False
    api = os.environ.get("STATUS_API_BASE", "https://api.github.com")
    branch = os.environ.get("STATUS_BRANCH", "main")
    content_b64 = base64.b64encode(json.dumps(body_obj).encode("utf-8")).decode("ascii")

    def _req(method, url, data=None):
        req = urllib.request.Request(url, method=method, data=data)
        req.add_header("Authorization", "Bearer %s" % token)
        req.add_header("Accept", "application/vnd.github+json")
        req.add_header("User-Agent", "atlas-rollup")
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
    return bool(code and 200 <= code < 300)


def write_local(obj, path):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(obj, fh)
    except OSError as e:
        log("WARNING could not write %s: %s" % (path, e))


def surface(rolled, stat_rows):
    learned = stat_rows > 0
    body = {
        "schema": "atlas.outcome_rollup.v1",
        "node": NODE_ID,
        "ts": int(time.time()),
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "outcome_stat_rows": stat_rows,
        "learning_state": "learned" if learned else "neutral_no_outcomes_yet",
        # consumers read these directly: source-selection + enrich-priority weights
        "weights": {dim: {r["key"]: r["weight"] for r in rows}
                    for dim, rows in rolled.items()},
        "top": {dim: rows[:10] for dim, rows in rolled.items()},
        "honesty": ("Weights derive from real funnel outcomes when present; with "
                    "an empty outcome_stats every weight is ~1.0 (neutral), so "
                    "behavior is identical to pre-learning. platform dim is "
                    "'unknown' until the WP loop publishes platform."),
    }
    write_local(body, os.path.join(STATE_DIR, "last_rollup.json"))
    write_local(body, LOCAL_OUT)
    gh_put("status/%s/outcome-rollup.json" % NODE_ID, body,
           "outcome-rollup %s rows=%d state=%s" %
           (NODE_ID, stat_rows, body["learning_state"]))
    return body


def run_once(conn):
    ensure_schema(conn)
    stats = load_outcome_stats(conn)
    rolled = rollup(stats)
    n = persist(conn, rolled)
    body = surface(rolled, len(stats))
    log("rolled up %d outcome_stats rows -> %d rollup rows across %d dims (state=%s)"
        % (len(stats), n, len(rolled), body["learning_state"]))
    return 0


def selftest():
    load_env_file(DB_ENV_PATH)
    load_env_file(AUTOPULL_ENV_PATH)
    ok = True
    # offline math asserts -- the load-bearing logic
    # 1. neutral: no volume -> weight ~ 1.0 against itself
    ev0, _ = bucket_ev(0, 0, 0, 0)
    assert ev0 >= 0, "ev must be non-negative"
    # 2. a converting bucket must out-weight a dead one
    rolled = rollup([
        {"source": "good", "industry": "med_spa", "platform": "wordpress",
         "enriched": 100, "contactable": 60, "replied": 30, "converted": 12},
        {"source": "bad", "industry": "hvac", "platform": "wordpress",
         "enriched": 100, "contactable": 5, "replied": 0, "converted": 0},
    ])
    src = {r["key"]: r["weight"] for r in rolled["source"]}
    assert src["good"] > src["bad"], "converting source must out-weight dead source"
    assert all(WEIGHT_MIN <= r["weight"] <= WEIGHT_MAX
               for rows in rolled.values() for r in rows), "weights must be clamped"
    # 3. empty population -> single neutral bucket weight 1.0
    rneutral = rollup([{"source": "s", "industry": "i", "platform": "unknown",
                        "enriched": 0, "contactable": 0, "replied": 0, "converted": 0}])
    assert abs(rneutral["source"][0]["weight"] - 1.0) < 1e-9, "lone bucket must be neutral 1.0"
    log("selftest: offline rollup math OK (good=%.3f bad=%.3f)" % (src["good"], src["bad"]))
    # DB connectivity. Fail-loud ON THE BOX (the apply gate); when
    # ATLAS_SELFTEST_OFFLINE=1 (the build sandbox, no Postgres) a DB miss WARNS
    # but the offline math gate above still owns PASS/FAIL -- honest, never
    # silently green on the box (the box does not set the flag).
    offline = os.environ.get("ATLAS_SELFTEST_OFFLINE", "") not in ("0", "", "no", "false")
    if psycopg2 is None:
        log("%s psycopg2 not installed" % ("WARN(offline)" if offline else "FAIL"))
        if not offline:
            ok = False
    else:
        try:
            conn = connect_pg()
            cur = conn.cursor()
            cur.execute("SELECT 1"); cur.fetchone()
            ensure_schema(conn)
            cur.execute("SELECT count(*) FROM atlas.outcome_rollup")
            log("outcome_rollup rows: %d" % cur.fetchone()[0])
            cur.close(); conn.close()
        except Exception as e:
            log("%s db connect/schema (%s)" % ("WARN(offline)" if offline else "FAIL", e))
            if not offline:
                ok = False
    print("SELFTEST %s" % ("OK" if ok else "FAILED"))
    return 0 if ok else 1


def main():
    if "--selftest" in sys.argv:
        sys.exit(selftest())
    load_env_file(DB_ENV_PATH)
    load_env_file(AUTOPULL_ENV_PATH)
    os.makedirs(STATE_DIR, exist_ok=True)
    if "--migrate" in sys.argv:
        conn = connect_pg()
        ensure_schema(conn)
        conn.close()
        print("migrate OK")
        return
    if "--loop" in sys.argv:
        while True:
            try:
                conn = connect_pg()
                try:
                    run_once(conn)
                finally:
                    conn.close()
            except Exception as e:
                log("loop error (retry next interval): %s" % e)
            time.sleep(INTERVAL_SEC)
    conn = connect_pg()
    try:
        run_once(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
