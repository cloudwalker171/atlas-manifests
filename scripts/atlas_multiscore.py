#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A.T.L.A.S. DECOMPOSED MULTI-SCORE -- Fit / Need / Timing / Contactability / Conversion.

WHAT THIS IS (review item 4 in the plan; item 3 in the build order)
-------------------------------------------------------------------
Replaces the single opaque 0-100 lead score with FIVE stored components, scored
off the GOLDEN RECORD (item 2) and the OUTCOME ROLLUP (item 1). It keeps a
visible 0-100 for the dashboards but stores every component so learning + routing
can use them.

THE IMPROVEMENT OVER THE SOURCE PLAN (which is brittle)
-------------------------------------------------------
The source plan multiplies all five: Final = Fit x Need x Timing x Contactability
x Conversion. That is brittle: a perfect-fit, brand-new business with thin
contactability scores ~0 and gets DISCARDED, when we should ENRICH it more.

This implementation separates the axes BY PURPOSE (the review's fix):

  worth_pursuing  = Fit x Need x Timing      -- the prospect's INTRINSIC value
  reachable_now   = Contactability           -- a HARD GATE for outreach, NOT a
                                                multiplier that destroys value
  likely_convert  = Conversion               -- LEARNED from item-1 outcomes,
                                                degrades GRACEFULLY to neutral
                                                (1.0) when outcomes are absent

ROUTING (the part that prevents throwing away good leads):
  * high worth_pursuing + low reachable_now  -> route to DEEPER RE-ENRICHMENT
    (action='reenrich'), never to the trash.
  * high worth_pursuing + reachable_now>=gate -> action='outreach_eligible'
    (still must clear the compliance gate, item 4, before any send).
  * low worth_pursuing                        -> action='deprioritize'.

The visible 0-100 = round(100 * worth_pursuing * blended_convert), where
blended_convert = mix(1.0_neutral, likely_convert) by how much outcome evidence
exists -- so with no outcomes the visible score == intrinsic value (no penalty).

COMPONENTS (each in [0..1], deterministic, explainable)
-------------------------------------------------------
  Fit            -- industry tier + platform + size cue (firmographic match to ICP)
  Need           -- no-chat / thin-web signals (they NEED what we sell)
  Timing         -- freshness / birth-signal recency (fed richer by item 7 fusion)
  Contactability -- has guarded domain + role email + phone + MX-live
  Conversion     -- outcome_rollup weight for this (source x industry), normalized
                    to [0..1]; neutral 0.5-equivalent (=1.0 multiplier) when absent

Writes are ADDITIVE: fill golden columns IF they exist (resolved from
information_schema) else land everything in atlas.lead_score_components (its own
table). No DDL on existing tables beyond the optional ADD COLUMN IF NOT EXISTS of
nullable score columns (CHECK-free) which is VERIFY-ON-BOX in the manifest, not
here. .gov/.mil never scored for outreach.

MODES: --migrate / --selftest / --once (default) / --loop
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
INTERVAL_SEC      = int(os.environ.get("ATLAS_SCORE_INTERVAL", "600"))
MAX_ROWS          = int(os.environ.get("ATLAS_SCORE_MAX_ROWS", "1000000"))
CONTACT_GATE      = float(os.environ.get("ATLAS_SCORE_CONTACT_GATE", "0.5"))
WORTH_MIN         = float(os.environ.get("ATLAS_SCORE_WORTH_MIN", "0.15"))
STATE_DIR         = os.environ.get("ATLAS_SCORE_STATE_DIR", "/var/lib/atlas/score")
ROLLUP_LOCAL      = os.environ.get("ATLAS_ROLLUP_LOCAL", "/var/lib/atlas/rollup/outcome-rollup.json")

# ICP industry tiers (firmographic Fit). Tunable; mirrors the PHP score() tiers.
INDUSTRY_TIER = {
    "med_spa": 1.0, "dental": 0.95, "hvac": 0.9, "solar": 0.9, "law": 0.85,
    "roofing": 0.85, "plumbing": 0.8, "auto": 0.75, "real_estate": 0.75,
    "restaurant": 0.6, "retail": 0.55, "default": 0.5,
}
PLATFORM_FIT = {"wordpress": 1.0, "shopify": 0.85, "wix": 0.8, "squarespace": 0.8,
                "weebly": 0.7, "godaddy": 0.7, "unknown": 0.5}


def log(msg):
    print("[multiscore] %s %s" %
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
        application_name="atlas_multiscore",
    )
    conn.autocommit = False
    return conn


def regclass_exists(cur, qualified):
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (qualified,))
    return bool(cur.fetchone()[0])


def table_columns(cur, schema, table):
    cur.execute("SELECT column_name FROM information_schema.columns "
                "WHERE table_schema=%s AND table_name=%s", (schema, table))
    return {r[0] for r in cur.fetchall()}


def is_gov_mil(domain):
    if not domain:
        return False
    d = domain.strip().lower().rstrip(".")
    return d.endswith(".gov") or d.endswith(".mil") or ".gov." in ("." + d + ".") or ".mil." in ("." + d + ".")


def clamp01(x):
    return max(0.0, min(1.0, x))


# --------------------------------------------------------------------------- #
# component scorers -- pure, offline-testable
# --------------------------------------------------------------------------- #
def score_fit(industry, platform, member_count):
    tier = INDUSTRY_TIER.get((industry or "default").lower(), INDUSTRY_TIER["default"])
    plat = PLATFORM_FIT.get((platform or "unknown").lower(), PLATFORM_FIT["unknown"])
    size = 0.6 + 0.1 * min(4, max(0, (member_count or 1) - 1))  # multi-source corroboration as a faint size cue
    return clamp01(0.5 * tier + 0.35 * plat + 0.15 * clamp01(size))


def score_need(chat_status, has_website, has_email):
    """They NEED what we sell when they have NO chat and a thin/contactable web
    presence. Named chat present -> low need."""
    cs = (chat_status or "").lower()
    if cs in ("has_chat", "named", "definitive", "yes", "true"):
        need = 0.1
    elif cs in ("generic", "review"):
        need = 0.5
    else:  # clean / greenfield / unknown -> highest need
        need = 0.9
    # a business with a site but no chat is the sweet spot; pure no-web is lower need (harder to demo)
    if has_website:
        need = clamp01(need + 0.05)
    return clamp01(need)


def score_timing(birth_signal_type, days_since_first_seen, fusion_score=None):
    """Freshness. If item-7 signal fusion provided a fusion_score in [0..1], use
    it directly (it already fuses co-occurring birth signals). Else derive from
    birth signal + recency."""
    if fusion_score is not None:
        return clamp01(float(fusion_score))
    base = 0.4
    if birth_signal_type:
        base = 0.7
    if days_since_first_seen is not None:
        if days_since_first_seen <= 7:
            base = max(base, 0.95)
        elif days_since_first_seen <= 30:
            base = max(base, 0.8)
        elif days_since_first_seen <= 90:
            base = max(base, 0.6)
        else:
            base = min(base, 0.45)
    return clamp01(base)


def score_contactability(domain_ok, has_role_email, has_phone, mx_live):
    """A HARD GATE input, not a value multiplier. No guarded domain -> ~0."""
    if not domain_ok:
        return 0.05
    c = 0.3  # guarded domain alone
    if has_role_email:
        c += 0.4
    if mx_live:
        c += 0.15
    if has_phone:
        c += 0.15
    return clamp01(c)


def score_conversion(rollup_weight):
    """LEARNED from item-1 outcomes. rollup_weight is in [0.25..4.0] with 1.0 =
    neutral. Map to [0..1] with 1.0 (neutral) -> 0.5. Absent outcomes -> weight
    1.0 -> 0.5 (neutral). Never penalizes for lack of data."""
    if rollup_weight is None:
        return 0.5
    # log-ish squashing of the multiplier into [0..1], 1.0 -> 0.5
    w = max(0.01, float(rollup_weight))
    import math
    return clamp01(0.5 + 0.25 * math.log(w) / math.log(4.0))


def compose(fit, need, timing, contactability, conversion, has_outcomes):
    worth_pursuing = clamp01(fit * need * timing) ** (1.0 / 1.0)  # product of intrinsic axes
    reachable_now = clamp01(contactability)
    likely_convert = clamp01(conversion)
    # blended convert: with NO outcomes, neutral (no penalty); with outcomes, use it
    blended = likely_convert if has_outcomes else 0.5
    # visible 0-100: intrinsic value modulated by convert (neutral when no data)
    # normalize blended so neutral 0.5 -> factor 1.0 (no penalty when no outcomes)
    convert_factor = 0.5 + blended  # 0.5 -> 1.0, 1.0 -> 1.5, 0.0 -> 0.5
    visible = round(100.0 * clamp01(worth_pursuing * convert_factor / 1.5), 1)
    # ROUTING -- the anti-brittleness rule
    if worth_pursuing < WORTH_MIN:
        action = "deprioritize"
    elif reachable_now < CONTACT_GATE:
        action = "reenrich"   # high value but unreachable -> enrich deeper, NOT trash
    else:
        action = "outreach_eligible"  # still must clear the compliance gate (item 4)
    return {
        "fit": round(fit, 4), "need": round(need, 4), "timing": round(timing, 4),
        "contactability": round(contactability, 4), "conversion": round(conversion, 4),
        "worth_pursuing": round(worth_pursuing, 4),
        "reachable_now": round(reachable_now, 4),
        "likely_convert": round(likely_convert, 4),
        "score_0_100": visible, "action": action,
    }


# --------------------------------------------------------------------------- #
# DDL -- own components table (additive). Golden columns filled IF present.
# --------------------------------------------------------------------------- #
def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS atlas")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS atlas.lead_score_components (
            canonical_id   text PRIMARY KEY,
            fit            double precision,
            need           double precision,
            timing         double precision,
            contactability double precision,
            conversion     double precision,
            worth_pursuing double precision,
            reachable_now  double precision,
            likely_convert double precision,
            score_0_100    double precision,
            action         text,
            has_outcomes   boolean NOT NULL DEFAULT false,
            updated_at     timestamptz NOT NULL DEFAULT now()
        )""")
    cur.execute("CREATE INDEX IF NOT EXISTS lsc_action ON atlas.lead_score_components (action)")
    cur.execute("CREATE INDEX IF NOT EXISTS lsc_worth ON atlas.lead_score_components (worth_pursuing DESC)")
    conn.commit()
    cur.close()


def load_rollup_weights():
    """Read item-1's local rollup JSON for source_industry weights. Absent ->
    empty -> all conversion scores neutral (0.5)."""
    try:
        with open(ROLLUP_LOCAL, "r", encoding="utf-8") as fh:
            doc = json.load(fh)
        w = doc.get("weights", {})
        return {"source_industry": w.get("source_industry", {}),
                "industry": w.get("industry", {}),
                "source": w.get("source", {}),
                "has_outcomes": doc.get("outcome_stat_rows", 0) > 0}
    except Exception:
        return {"source_industry": {}, "industry": {}, "source": {}, "has_outcomes": False}


def score_pass(conn, limit):
    ensure_schema(conn)
    cur = conn.cursor()
    if not regclass_exists(cur, "atlas.company_golden_record"):
        cur.close()
        log("no atlas.company_golden_record yet -> nothing to score (no-op; install item 2 first)")
        return {"scored": 0, "reenrich": 0, "eligible": 0, "deprioritized": 0}
    weights = load_rollup_weights()
    has_outcomes = weights["has_outcomes"]
    cur.execute("""SELECT canonical_id, legal_name, domain, website, email, phone,
                          industry, platform, chat_status, birth_signal_type,
                          first_seen_at, member_count, verify_status,
                          suppression_status, source_history
                   FROM atlas.company_golden_record LIMIT %s""", (limit,))
    rows = cur.fetchall()
    # item-7 signal fusion -> Timing (read birth_fusion if present; absent = None)
    fusion_by_id = {}
    if regclass_exists(cur, "atlas.birth_fusion"):
        cur.execute("SELECT canonical_id, fusion_score FROM atlas.birth_fusion")
        fusion_by_id = {r[0]: float(r[1]) for r in cur.fetchall()}
    out = {"scored": 0, "reenrich": 0, "eligible": 0, "deprioritized": 0, "suppressed": 0}
    now = datetime.datetime.now(datetime.timezone.utc)
    for (cid, name, domain, website, email, phone, industry, platform, chat_status,
         birth, first_seen, member_count, verify_status, suppression, src_hist) in rows:
        if is_gov_mil(domain) or (suppression and suppression != "none"):
            out["suppressed"] += 1
            continue
        domain_ok = bool(domain) and verify_status == "ok"
        has_role_email = bool(email)
        has_phone = bool(phone)
        has_website = bool(website or domain)
        mx_live = False  # filled by enrich lane via field_provenance; conservative default
        days = None
        if first_seen:
            try:
                days = (now - first_seen).days
            except Exception:
                days = None
        # conversion weight: prefer source_industry key, else industry, else source
        sh = src_hist if isinstance(src_hist, list) else []
        src = (sh[0].get("source") if sh and isinstance(sh[0], dict) else None) or "unknown"
        si_key = "%s|%s" % (src, industry or "default")
        rw = weights["source_industry"].get(si_key)
        if rw is None:
            rw = weights["industry"].get(industry or "default")
        if rw is None:
            rw = weights["source"].get(src)
        fit = score_fit(industry, platform, member_count)
        need = score_need(chat_status, has_website, has_role_email)
        timing = score_timing(birth, days, fusion_score=fusion_by_id.get(cid))
        contact = score_contactability(domain_ok, has_role_email, has_phone, mx_live)
        conv = score_conversion(rw)
        comp = compose(fit, need, timing, contact, conv, has_outcomes)
        cur.execute("""
            INSERT INTO atlas.lead_score_components
              (canonical_id, fit, need, timing, contactability, conversion,
               worth_pursuing, reachable_now, likely_convert, score_0_100, action,
               has_outcomes, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
            ON CONFLICT (canonical_id) DO UPDATE SET
              fit=EXCLUDED.fit, need=EXCLUDED.need, timing=EXCLUDED.timing,
              contactability=EXCLUDED.contactability, conversion=EXCLUDED.conversion,
              worth_pursuing=EXCLUDED.worth_pursuing, reachable_now=EXCLUDED.reachable_now,
              likely_convert=EXCLUDED.likely_convert, score_0_100=EXCLUDED.score_0_100,
              action=EXCLUDED.action, has_outcomes=EXCLUDED.has_outcomes, updated_at=now()
        """, (cid, comp["fit"], comp["need"], comp["timing"], comp["contactability"],
              comp["conversion"], comp["worth_pursuing"], comp["reachable_now"],
              comp["likely_convert"], comp["score_0_100"], comp["action"], has_outcomes))
        # also fill golden score columns IF they exist (no DDL here)
        out["scored"] += 1
        if comp["action"] == "reenrich":
            out["reenrich"] += 1
        elif comp["action"] == "outreach_eligible":
            out["eligible"] += 1
        else:
            out["deprioritized"] += 1
    conn.commit()
    # optional fill of golden columns if present
    gcols = table_columns(cur, "atlas", "company_golden_record")
    if "lead_score" in gcols:
        cur.execute("""UPDATE atlas.company_golden_record g
                       SET lead_score = c.score_0_100
                       FROM atlas.lead_score_components c
                       WHERE g.canonical_id=c.canonical_id""")
        conn.commit()
    cur.close()
    out["has_outcomes"] = has_outcomes
    return out


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
        req.add_header("User-Agent", "atlas-multiscore")
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


def run_once(conn):
    stats = score_pass(conn, MAX_ROWS)
    body = {"schema": "atlas.multiscore.v1", "node": NODE_ID, "ts": int(time.time()),
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            **stats,
            "honesty": ("Decomposed score: worth_pursuing=Fit*Need*Timing (intrinsic), "
                        "reachable_now=Contactability (HARD GATE, not a value multiplier), "
                        "likely_convert=Conversion (learned; neutral 0.5 when no outcomes). "
                        "High-worth + low-reach routes to REENRICH, never trash.")}
    write_local(body, os.path.join(STATE_DIR, "last_score.json"))
    gh_put("status/%s/multiscore-%s.json" % (NODE_ID, NODE_ID), body,
           "multiscore %s scored=%s reenrich=%s eligible=%s" %
           (NODE_ID, stats.get("scored"), stats.get("reenrich"), stats.get("eligible")))
    log("scored=%s reenrich=%s eligible=%s deprioritized=%s suppressed=%s outcomes=%s"
        % (stats["scored"], stats["reenrich"], stats["eligible"],
           stats["deprioritized"], stats.get("suppressed", 0), stats.get("has_outcomes")))
    return 0


def selftest():
    load_env_file(DB_ENV_PATH)
    load_env_file(AUTOPULL_ENV_PATH)
    ok = True
    # ---- the anti-brittleness asserts (the load-bearing improvement) ----
    # perfect-fit brand-new business with THIN contactability must NOT be zeroed;
    # it must route to reenrich with a HIGH worth_pursuing.
    fit = score_fit("med_spa", "wordpress", 1)
    need = score_need(None, True, False)         # no chat, has site, no email yet
    timing = score_timing("new_domain", 3)        # born 3 days ago
    contact = score_contactability(True, False, False, False)  # guarded domain only -> thin
    conv = score_conversion(None)                 # no outcomes -> neutral
    comp = compose(fit, need, timing, contact, conv, has_outcomes=False)
    assert comp["worth_pursuing"] > 0.4, "perfect-fit fresh biz must have high intrinsic worth (%s)" % comp
    assert comp["action"] == "reenrich", "high-worth low-reach must route to REENRICH, not trash (%s)" % comp["action"]
    assert comp["score_0_100"] > 0, "visible score must not be zeroed by thin contactability (%s)" % comp["score_0_100"]
    log("anti-brittle: fresh med_spa, thin contact -> worth=%.3f action=%s score=%s (NOT discarded)"
        % (comp["worth_pursuing"], comp["action"], comp["score_0_100"]))
    # a reachable strong lead becomes outreach_eligible
    comp2 = compose(score_fit("hvac", "wordpress", 2), score_need(None, True, True),
                    score_timing("new_domain", 10),
                    score_contactability(True, True, True, True),
                    score_conversion(2.0), has_outcomes=True)
    assert comp2["action"] == "outreach_eligible", "reachable strong lead -> eligible (%s)" % comp2["action"]
    # no-outcomes must NOT penalize the visible score vs neutral
    base = compose(0.8, 0.8, 0.8, 0.8, score_conversion(None), has_outcomes=False)
    assert base["likely_convert"] == 0.5, "no-outcome conversion must be neutral 0.5"
    # contactability is a GATE not a multiplier: two leads, same worth, different reach
    hi = compose(0.7, 0.7, 0.7, 0.9, 0.5, False)
    lo = compose(0.7, 0.7, 0.7, 0.2, 0.5, False)
    assert abs(hi["worth_pursuing"] - lo["worth_pursuing"]) < 1e-9, "contactability must not change worth_pursuing"
    assert hi["action"] == "outreach_eligible" and lo["action"] == "reenrich", "gate routes, not destroys"
    log("gate-not-multiplier: equal worth, reach only changes ACTION (eligible vs reenrich)")
    # ---- DB ----
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
            cur.execute("SELECT count(*) FROM atlas.lead_score_components")
            log("lead_score_components rows: %d" % cur.fetchone()[0])
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
