#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A.T.L.A.S. SIGNAL FUSION -- the Channel-1 birth-graph (TITAN 'born today').

WHAT THIS IS (review item 5 in the plan; item 7 in the build order)
-------------------------------------------------------------------
Scores a business by how many independent BIRTH SIGNALS co-occur within a time
window. The thesis (the TITAN birth-graph): the freshest, highest-probability
prospects are the ones lighting up multiple birth signals at once --

  new registrable domain (NRD)  +  fresh SSL cert  +  live MX  +  a detected
  site platform (WordPress/Shopify/...)  +  NO existing chat  =  EXTREME priority
  (a business that just came online and needs exactly what we sell).

It reads the birth signals already captured per company (on the golden record's
birth_signal_type + field_provenance signal fields + first_seen_at) and computes
a fusion_score in [0..1] = a weighted, window-decayed combination of the
co-occurring signals, with a co-occurrence BONUS (the whole is worth more than
the parts -- 4 signals within 14 days >> 4 signals spread over a year).

It FEEDS THE TIMING SCORE (item 3): atlas_multiscore.score_timing() accepts a
fusion_score and uses it directly when present. This module writes
atlas.birth_fusion(canonical_id, fusion_score, signals, ...) which the scorer
reads. So fusion -> Timing -> worth_pursuing, closing item 5 into item 3.

SOURCE REPRIORITIZATION (infra-aware, per the review) -- documented + encoded as
the signal set this fusion actually consumes:
  * P0 achievable now : NRD / DNS / MX (NRD live), local permits & inspections,
                        per-state SoS (CA/TX/FL/NY first), city/county licenses.
  * P1                 : SAM.gov, USPTO trademarks, OpenCorporates (as the entity
                        resolver input), EDGAR (live).
  * P2/caution         : job/social/directory -- lawful sources only (company-owned
                        pages / public RSS / APIs); never scrape.
  * CT/SSL             : PARKED (box egress to CT logs is 403-blocked). NRD covers
                        the same-day-birth need today; SSL freshness is consumed
                        ONLY when it is already present in provenance (no live CT
                        fetch here). The fusion degrades gracefully without it.

SAFETY: additive + READ-mostly. Reads golden record + field_provenance birth
signals; writes ONLY atlas.birth_fusion (idempotent UPSERT). No live CT/egress
from here. Never writes business rows, never ALTERs other tables. .gov/.mil
excluded. Stdlib + psycopg2.

MODES: --migrate / --selftest / --once (default) / --loop
"""

import datetime
import json
import os
import sys
import time

try:
    import psycopg2
except Exception:  # pragma: no cover
    psycopg2 = None

DB_ENV_PATH       = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
AUTOPULL_ENV_PATH = os.environ.get("ATLAS_AUTOPULL_CONF", "/etc/atlas/autopull.env")
NODE_ID           = os.environ.get("NODE_ID", "hetzner")
INTERVAL_SEC      = int(os.environ.get("ATLAS_FUSION_INTERVAL", "600"))
MAX_ROWS          = int(os.environ.get("ATLAS_FUSION_MAX_ROWS", "1000000"))
WINDOW_DAYS       = int(os.environ.get("ATLAS_FUSION_WINDOW_DAYS", "30"))
STATE_DIR         = os.environ.get("ATLAS_FUSION_STATE_DIR", "/var/lib/atlas/fusion")

# birth-signal weights (independent evidence of a freshly-born business)
SIGNAL_WEIGHT = {
    "new_domain": 0.30,     # NRD -- newly registered registrable domain (P0, live)
    "fresh_ssl": 0.15,      # recent SSL cert (CT) -- consumed ONLY if present (CT parked)
    "live_mx": 0.15,        # MX present -> they can receive mail (P0, computed)
    "platform": 0.15,       # a detected site platform -> they built a site (P0)
    "no_chat": 0.10,        # no existing chat -> they NEED what we sell
    "new_registration": 0.20,  # SoS/permit/license birth event (P0/P1)
}
# co-occurrence bonus: many fresh signals together is disproportionately strong
COOCCUR_BONUS = {0: 0.0, 1: 0.0, 2: 0.05, 3: 0.12, 4: 0.20, 5: 0.28, 6: 0.35}


def log(msg):
    print("[fusion] %s %s" %
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
        application_name="atlas_signal_fusion",
    )
    conn.autocommit = False
    return conn


def regclass_exists(cur, qualified):
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (qualified,))
    return bool(cur.fetchone()[0])


def clamp01(x):
    return max(0.0, min(1.0, x))


# --------------------------------------------------------------------------- #
# the fusion math -- pure, offline-testable
# --------------------------------------------------------------------------- #
def window_decay(days, window_days=WINDOW_DAYS):
    """Linear decay: full weight inside the window, fading to a floor afterward.
    A signal 3 days old counts full; 90 days old counts little."""
    if days is None:
        return 0.7  # unknown age -> moderate (don't over-credit, don't zero)
    if days <= window_days:
        return 1.0
    if days <= window_days * 6:
        return clamp01(1.0 - (days - window_days) / float(window_days * 6))
    return 0.1


def fuse(signals, days_since_first_seen=None):
    """signals: set/list of present birth-signal names. Returns (fusion_score,
    detail). fusion = window-decayed weighted sum + co-occurrence bonus."""
    present = [s for s in signals if s in SIGNAL_WEIGHT]
    base = sum(SIGNAL_WEIGHT[s] for s in present)
    decay = window_decay(days_since_first_seen)
    n = len(present)
    bonus = COOCCUR_BONUS.get(min(n, 6), 0.35)
    score = clamp01((base + bonus) * decay)
    return round(score, 4), {"present": sorted(present), "base": round(base, 4),
                             "cooccur_bonus": bonus, "decay": round(decay, 4),
                             "count": n}


# --------------------------------------------------------------------------- #
# DDL
# --------------------------------------------------------------------------- #
def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS atlas")
    cur.execute("""CREATE TABLE IF NOT EXISTS atlas.birth_fusion (
        canonical_id text PRIMARY KEY,
        fusion_score double precision NOT NULL DEFAULT 0,
        signal_count int NOT NULL DEFAULT 0,
        signals jsonb,
        detail jsonb,
        updated_at timestamptz NOT NULL DEFAULT now())""")
    cur.execute("CREATE INDEX IF NOT EXISTS birth_fusion_score ON atlas.birth_fusion (fusion_score DESC)")
    conn.commit()
    cur.close()


def is_gov_mil(domain):
    if not domain:
        return False
    d = domain.strip().lower().rstrip(".")
    return d.endswith(".gov") or d.endswith(".mil") or ".gov." in ("." + d + ".") or ".mil." in ("." + d + ".")


def signals_for_company(prov_fields, birth_signal_type, chat_status):
    """Map the raw provenance fields + golden attrs onto the birth-signal set.
    Conservative: only credits a signal when there's positive evidence. CT/SSL
    (fresh_ssl) is credited ONLY if already present in provenance (CT is parked --
    no live fetch)."""
    s = set()
    f = set(x.lower() for x in (prov_fields or []))
    if birth_signal_type:
        bt = birth_signal_type.lower()
        if "domain" in bt or "nrd" in bt:
            s.add("new_domain")
        if "ssl" in bt or "cert" in bt:
            s.add("fresh_ssl")
        if "registration" in bt or "sos" in bt or "permit" in bt or "license" in bt:
            s.add("new_registration")
    if {"new_domain", "registrable_domain_age", "nrd"} & f:
        s.add("new_domain")
    if {"dns_mx", "mx", "email_provider"} & f:
        s.add("live_mx")
    if {"platform", "tech_platform", "cms"} & f:
        s.add("platform")
    if {"ssl_not_before", "cert_age", "fresh_ssl"} & f:
        s.add("fresh_ssl")
    if {"registration_date", "permit_date", "license_date"} & f:
        s.add("new_registration")
    cs = (chat_status or "").lower()
    if cs in ("", "clean", "greenfield", "none", "no", "false") or cs not in ("has_chat", "named", "yes", "true", "generic"):
        if cs not in ("has_chat", "named", "yes", "true"):
            s.add("no_chat")
    return s


def fusion_pass(conn, limit):
    ensure_schema(conn)
    cur = conn.cursor()
    if not regclass_exists(cur, "atlas.company_golden_record"):
        cur.close()
        log("no golden record yet -> nothing to fuse (no-op; install item 2 first)")
        return {"fused": 0, "extreme": 0}
    cur.execute("""SELECT canonical_id, domain, birth_signal_type, chat_status,
                          first_seen_at, source_history
                   FROM atlas.company_golden_record LIMIT %s""", (limit,))
    rows = cur.fetchall()
    now = datetime.datetime.now(datetime.timezone.utc)
    fused = 0
    extreme = 0
    for cid, domain, birth, chat_status, first_seen, src_hist in rows:
        if is_gov_mil(domain):
            continue
        prov_fields = []
        if isinstance(src_hist, list):
            for h in src_hist:
                if isinstance(h, dict) and h.get("field"):
                    prov_fields.append(h["field"])
        days = None
        if first_seen:
            try:
                days = (now - first_seen).days
            except Exception:
                days = None
        sigs = signals_for_company(prov_fields, birth, chat_status)
        score, detail = fuse(sigs, days)
        cur.execute("""INSERT INTO atlas.birth_fusion
            (canonical_id, fusion_score, signal_count, signals, detail, updated_at)
            VALUES (%s,%s,%s,%s,%s, now())
            ON CONFLICT (canonical_id) DO UPDATE SET
              fusion_score=EXCLUDED.fusion_score, signal_count=EXCLUDED.signal_count,
              signals=EXCLUDED.signals, detail=EXCLUDED.detail, updated_at=now()""",
            (cid, score, detail["count"], json.dumps(sorted(sigs)), json.dumps(detail)))
        fused += 1
        if score >= 0.7:
            extreme += 1
    conn.commit()
    cur.close()
    return {"fused": fused, "extreme": extreme}


def write_local(obj, path):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(obj, fh)
    except OSError as e:
        log("WARNING could not write %s: %s" % (path, e))


def run_once(conn):
    stats = fusion_pass(conn, MAX_ROWS)
    body = {"schema": "atlas.fusion.v1", "node": NODE_ID, "ts": int(time.time()),
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            **stats,
            "source_priority": {
                "P0_now": ["NRD/DNS/MX", "local_permits_inspections",
                           "per_state_SoS_CA_TX_FL_NY", "city_county_licenses"],
                "P1": ["SAM.gov", "USPTO_trademarks", "OpenCorporates_resolver", "EDGAR"],
                "P2_lawful_only": ["job_RSS", "company_owned_pages", "public_APIs"],
                "parked": ["CT_SSL_egress_403_blocked"]},
            "honesty": ("Fusion scores co-occurring birth signals within a window; "
                        "the co-occurrence bonus makes 'born today on all fronts' "
                        "extreme. CT/SSL is PARKED (egress blocked) -> fresh_ssl is "
                        "credited only if already in provenance; fusion degrades "
                        "gracefully without it. Feeds the item-3 Timing score.")}
    write_local(body, os.path.join(STATE_DIR, "last_fusion.json"))
    log("fused=%s extreme(>=0.7)=%s" % (stats["fused"], stats["extreme"]))
    return 0


def selftest():
    load_env_file(DB_ENV_PATH)
    load_env_file(AUTOPULL_ENV_PATH)
    ok = True
    # ---- the EXTREME case: all birth signals within the window ----
    extreme, det = fuse({"new_domain", "fresh_ssl", "live_mx", "platform", "no_chat"}, days_since_first_seen=3)
    assert extreme >= 0.7, "all-signals-fresh must be EXTREME (>=0.7), got %.3f %s" % (extreme, det)
    log("extreme: new_domain+ssl+mx+platform+no_chat @3d -> fusion=%.3f (EXTREME)" % extreme)
    # ---- co-occurrence beats the same signals spread out (window decay) ----
    spread, _ = fuse({"new_domain", "fresh_ssl", "live_mx", "platform", "no_chat"}, days_since_first_seen=400)
    assert spread < extreme, "old signals must decay below fresh co-occurrence (%.3f vs %.3f)" % (spread, extreme)
    log("decay: same signals @400d -> fusion=%.3f (< fresh %.3f)" % (spread, extreme))
    # ---- a single weak signal is low ----
    weak, _ = fuse({"no_chat"}, days_since_first_seen=10)
    assert weak < 0.3, "a lone weak signal must score low (%.3f)" % weak
    log("lone-signal: no_chat only -> fusion=%.3f (low)" % weak)
    # ---- co-occurrence bonus monotonic ----
    s2, _ = fuse({"new_domain", "live_mx"}, 5)
    s4, _ = fuse({"new_domain", "live_mx", "platform", "no_chat"}, 5)
    assert s4 > s2, "more co-occurring signals must raise the fusion score"
    # ---- the signal-mapper credits fresh_ssl ONLY when present (CT parked) ----
    sigs_nossl = signals_for_company(["dns_mx", "platform"], "new_domain", "clean")
    assert "fresh_ssl" not in sigs_nossl, "fresh_ssl must NOT be credited without CT evidence (CT parked)"
    assert {"new_domain", "live_mx", "platform", "no_chat"} <= sigs_nossl, "core P0 signals must map"
    sigs_ssl = signals_for_company(["dns_mx", "platform", "ssl_not_before"], "new_domain", "clean")
    assert "fresh_ssl" in sigs_ssl, "fresh_ssl credited when present in provenance"
    log("mapper: CT-parked (no fresh_ssl without evidence); core P0 signals map; ssl credited when present")
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
            cur.execute("SELECT count(*) FROM atlas.birth_fusion")
            log("birth_fusion rows: %d" % cur.fetchone()[0])
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
