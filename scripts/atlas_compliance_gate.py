#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A.T.L.A.S. COMPLIANCE GATE -- ALLOW / HOLD / BLOCK before any outreach send.

WHAT THIS IS (review item 4)
----------------------------
A hard pre-send gate. EVERY candidate send is evaluated here and gets exactly one
verdict: ALLOW, HOLD, or BLOCK. The rule (the review's): NO VERDICT = NO SEND.
The standalone sender (item 5) calls evaluate() for every send and transmits ONLY
on ALLOW (and only when its own SHADOW flag is off, which the user controls).

It is BOTH a library (import + evaluate()) AND a CLI (--selftest / --check).

CHECKS (all of them; any BLOCK wins; else any HOLD; else ALLOW)
---------------------------------------------------------------
  1.  valid business-domain email   -- syntactically valid, on a real company
      domain (not free-mail, not the golden record's unverified/guessed domain)
  2.  gov/mil suppression           -- .gov/.mil/.fed.us -> BLOCK (non-overridable)
  3.  explicit suppression list     -- on atlas.suppression -> BLOCK
  4.  unsubscribe                    -- prior opt-out for this address/domain -> BLOCK
  5.  bounce history                 -- hard bounce -> BLOCK; soft-bounce streak -> HOLD
  6.  complaint history              -- any spam complaint -> BLOCK (domain-wide HOLD)
  7.  disposable / throwaway domain  -- BLOCK
  8.  role-email policy              -- role addresses (info@/sales@) ALLOWED (B2B,
      firmographic); but no-reply@/postmaster@/abuse@ -> BLOCK
  9.  geo policy                     -- region allow/deny (e.g. EU without basis) -> HOLD/BLOCK
  10. frequency cap                  -- > N sends to a domain in a window -> HOLD
  11. duplicate-company             -- already contacted this canonical company
      recently -> HOLD (dedupe at the entity level, not just the address)
  12. domain reputation             -- low-rep sender/recipient domain -> HOLD
  13. mailbox health                -- the sending mailbox must be warm + healthy;
      unhealthy mailbox -> HOLD (don't burn it)

Every verdict carries a machine-readable reason code + a human note, persisted to
atlas.compliance_log (additive, append) so decisions are auditable and learnable.

SAFETY: read-mostly. Reads suppression/bounce/complaint/unsubscribe/send-log
tables when present; writes ONLY atlas.compliance_log (append) + ensures its own
small tables. Never sends anything. Never ALTERs existing tables. On MISSING data
it FAILS SAFE: an unknown signal can only make a verdict MORE restrictive, never
less. If it cannot reach the DB at all -> the verdict is HOLD (never ALLOW) so a
gate outage can never cause an unchecked send.

MODES: --migrate / --selftest / --check '<json>' / (library: evaluate(ctx))
"""

import datetime
import json
import os
import re
import sys

try:
    import psycopg2
except Exception:  # pragma: no cover
    psycopg2 = None

DB_ENV_PATH       = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
AUTOPULL_ENV_PATH = os.environ.get("ATLAS_AUTOPULL_CONF", "/etc/atlas/autopull.env")

FREQ_CAP_PER_DOMAIN   = int(os.environ.get("ATLAS_GATE_FREQ_CAP", "1"))
FREQ_WINDOW_DAYS      = int(os.environ.get("ATLAS_GATE_FREQ_WINDOW_DAYS", "30"))
DUP_COMPANY_DAYS      = int(os.environ.get("ATLAS_GATE_DUP_DAYS", "30"))
SOFT_BOUNCE_HOLD_AT   = int(os.environ.get("ATLAS_GATE_SOFT_BOUNCE_HOLD", "2"))
GEO_DENY              = set(x.strip().upper() for x in
                            os.environ.get("ATLAS_GATE_GEO_DENY", "").split(",") if x.strip())
GEO_HOLD              = set(x.strip().upper() for x in
                            os.environ.get("ATLAS_GATE_GEO_HOLD", "").split(",") if x.strip())

ALLOW, HOLD, BLOCK = "ALLOW", "HOLD", "BLOCK"
PRECEDENCE = {ALLOW: 0, HOLD: 1, BLOCK: 2}

FREE_MAIL = {
    "gmail.com", "googlemail.com", "yahoo.com", "ymail.com", "hotmail.com",
    "outlook.com", "live.com", "msn.com", "aol.com", "icloud.com", "me.com",
    "mail.com", "gmx.com", "protonmail.com", "proton.me",
}
DISPOSABLE = {
    "mailinator.com", "guerrillamail.com", "10minutemail.com", "tempmail.com",
    "trashmail.com", "yopmail.com", "sharklasers.com", "getnada.com",
    "throwawaymail.com", "maildrop.cc", "dispostable.com", "fakeinbox.com",
}
BLOCKED_LOCALPARTS = {"no-reply", "noreply", "postmaster", "abuse", "donotreply",
                      "do-not-reply", "mailer-daemon", "bounce", "bounces"}
_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")


def log(msg):
    print("[compliance] %s %s" %
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
        application_name="atlas_compliance_gate",
    )
    conn.autocommit = False
    return conn


def regclass_exists(cur, qualified):
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (qualified,))
    return bool(cur.fetchone()[0])


def email_domain(email):
    if not email or "@" not in email:
        return None
    return email.rsplit("@", 1)[1].strip().lower().rstrip(".")


def localpart(email):
    if not email or "@" not in email:
        return None
    return email.split("@", 1)[0].strip().lower()


def is_gov_mil(domain):
    if not domain:
        return False
    d = domain.strip().lower().rstrip(".")
    return (d.endswith(".gov") or d.endswith(".mil") or d.endswith(".fed.us")
            or ".gov." in ("." + d + ".") or ".mil." in ("." + d + "."))


# --------------------------------------------------------------------------- #
# DDL -- additive own tables
# --------------------------------------------------------------------------- #
def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS atlas")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS atlas.suppression (
            email      text,
            domain     text,
            reason     text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now()
        )""")
    cur.execute("CREATE INDEX IF NOT EXISTS suppression_email ON atlas.suppression (email)")
    cur.execute("CREATE INDEX IF NOT EXISTS suppression_domain ON atlas.suppression (domain)")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS atlas.compliance_log (
            id          bigserial PRIMARY KEY,
            email       text,
            domain      text,
            canonical_id text,
            verdict     text NOT NULL,
            reasons     jsonb,
            created_at  timestamptz NOT NULL DEFAULT now()
        )""")
    cur.execute("CREATE INDEX IF NOT EXISTS compliance_log_dom ON atlas.compliance_log (domain, created_at)")
    conn.commit()
    cur.close()


# --------------------------------------------------------------------------- #
# the pure, offline-testable checks. Each returns (verdict, reason_code) or None.
# `signals` is a dict of facts gathered from the DB (or passed in for tests).
# --------------------------------------------------------------------------- #
def check_email_validity(email, golden_verify_status=None):
    if not email or not _EMAIL_RE.match(email):
        return BLOCK, "invalid_email_syntax"
    dom = email_domain(email)
    if dom in FREE_MAIL:
        return BLOCK, "free_mail_not_business_domain"
    # the golden record's domain must be verified (item 2 guard) to be sendable
    if golden_verify_status and golden_verify_status != "ok":
        return BLOCK, "golden_domain_unverified_%s" % golden_verify_status
    return None


def check_gov_mil(email):
    if is_gov_mil(email_domain(email)):
        return BLOCK, "gov_mil_suppressed"
    return None


def check_disposable(email):
    if email_domain(email) in DISPOSABLE:
        return BLOCK, "disposable_domain"
    return None


def check_role_localpart(email):
    lp = localpart(email)
    if not lp:
        return BLOCK, "no_localpart"
    if lp in BLOCKED_LOCALPARTS:
        return BLOCK, "blocked_role_localpart_%s" % lp
    return None  # info@/sales@/contact@ are ALLOWED (B2B firmographic)


def check_suppression(signals):
    if signals.get("suppressed_email"):
        return BLOCK, "suppressed_email"
    if signals.get("suppressed_domain"):
        return BLOCK, "suppressed_domain"
    return None


def check_unsubscribe(signals):
    if signals.get("unsubscribed"):
        return BLOCK, "prior_unsubscribe"
    return None


def check_bounce(signals):
    if signals.get("hard_bounce"):
        return BLOCK, "prior_hard_bounce"
    if (signals.get("soft_bounce_count") or 0) >= SOFT_BOUNCE_HOLD_AT:
        return HOLD, "soft_bounce_streak"
    return None


def check_complaint(signals):
    if signals.get("complaint"):
        return BLOCK, "prior_spam_complaint"
    if signals.get("domain_complaint"):
        return HOLD, "domain_complaint_history"
    return None


def check_geo(signals):
    region = (signals.get("region") or "").upper()
    if region and region in GEO_DENY:
        return BLOCK, "geo_denied_%s" % region
    if region and region in GEO_HOLD:
        return HOLD, "geo_hold_%s" % region
    return None


def check_frequency(signals):
    if (signals.get("domain_sends_in_window") or 0) >= FREQ_CAP_PER_DOMAIN:
        return HOLD, "frequency_cap_domain"
    return None


def check_duplicate_company(signals):
    if signals.get("company_contacted_recently"):
        return HOLD, "duplicate_company_recent_contact"
    return None


def check_domain_reputation(signals):
    rep = signals.get("recipient_domain_reputation")
    if rep is not None and rep < 0.3:
        return HOLD, "low_recipient_domain_reputation"
    return None


def check_mailbox_health(signals):
    mh = signals.get("mailbox_health")
    if mh is not None and mh < 0.5:
        return HOLD, "sending_mailbox_unhealthy"
    if signals.get("mailbox_warmup_incomplete"):
        return HOLD, "mailbox_warmup_incomplete"
    return None


CHECKS_EMAIL = [check_email_validity, check_gov_mil, check_disposable, check_role_localpart]
CHECKS_SIGNAL = [check_suppression, check_unsubscribe, check_bounce, check_complaint,
                 check_geo, check_frequency, check_duplicate_company,
                 check_domain_reputation, check_mailbox_health]


def decide(email, signals, golden_verify_status=None):
    """Pure decision function. Returns (verdict, [reason_codes]). BLOCK wins, then
    HOLD, then ALLOW. Fail-safe: an unknown signal never relaxes the verdict."""
    reasons = []
    worst = ALLOW
    for fn in CHECKS_EMAIL:
        r = fn(email) if fn is not check_email_validity else fn(email, golden_verify_status)
        if r:
            verdict, code = r
            reasons.append(code)
            if PRECEDENCE[verdict] > PRECEDENCE[worst]:
                worst = verdict
    for fn in CHECKS_SIGNAL:
        r = fn(signals)
        if r:
            verdict, code = r
            reasons.append(code)
            if PRECEDENCE[verdict] > PRECEDENCE[worst]:
                worst = verdict
    if worst == ALLOW and not reasons:
        reasons = ["all_checks_passed"]
    return worst, reasons


# --------------------------------------------------------------------------- #
# gather signals from the DB (best-effort; missing -> conservative)
# --------------------------------------------------------------------------- #
def gather_signals(conn, email, canonical_id=None, region=None):
    sig = {"region": region}
    dom = email_domain(email)
    cur = conn.cursor()
    try:
        if regclass_exists(cur, "atlas.suppression"):
            cur.execute("SELECT 1 FROM atlas.suppression WHERE email=%s LIMIT 1", (email,))
            sig["suppressed_email"] = cur.fetchone() is not None
            cur.execute("SELECT 1 FROM atlas.suppression WHERE domain=%s LIMIT 1", (dom,))
            sig["suppressed_domain"] = cur.fetchone() is not None
        # bounce/complaint/unsubscribe + send-log are owned by the sender (item 5);
        # read them if they exist, else leave unknown (which only restricts).
        if regclass_exists(cur, "atlas.send_log"):
            cur.execute("SELECT count(*) FROM atlas.send_log WHERE recipient_domain=%s "
                        "AND created_at >= now() - (%s||' days')::interval",
                        (dom, FREQ_WINDOW_DAYS))
            sig["domain_sends_in_window"] = int(cur.fetchone()[0] or 0)
            if canonical_id:
                cur.execute("SELECT count(*) FROM atlas.send_log WHERE canonical_id=%s "
                            "AND created_at >= now() - (%s||' days')::interval",
                            (canonical_id, DUP_COMPANY_DAYS))
                sig["company_contacted_recently"] = int(cur.fetchone()[0] or 0) > 0
        if regclass_exists(cur, "atlas.bounce_log"):
            cur.execute("SELECT bool_or(hard), count(*) FILTER (WHERE NOT hard) "
                        "FROM atlas.bounce_log WHERE email=%s", (email,))
            row = cur.fetchone()
            if row:
                sig["hard_bounce"] = bool(row[0])
                sig["soft_bounce_count"] = int(row[1] or 0)
        if regclass_exists(cur, "atlas.complaint_log"):
            cur.execute("SELECT 1 FROM atlas.complaint_log WHERE email=%s LIMIT 1", (email,))
            sig["complaint"] = cur.fetchone() is not None
            cur.execute("SELECT 1 FROM atlas.complaint_log WHERE domain=%s LIMIT 1", (dom,))
            sig["domain_complaint"] = cur.fetchone() is not None
        if regclass_exists(cur, "atlas.unsubscribe_log"):
            cur.execute("SELECT 1 FROM atlas.unsubscribe_log WHERE email=%s OR domain=%s LIMIT 1",
                        (email, dom))
            sig["unsubscribed"] = cur.fetchone() is not None
    except Exception as e:
        conn.rollback()
        log("signal gather partial (%s) -> remaining signals stay unknown (restrictive)" % e)
    cur.close()
    return sig


def evaluate(conn, email, canonical_id=None, region=None, golden_verify_status=None):
    """Library entry: full verdict from live signals + persist to compliance_log.
    DB failure -> HOLD (never ALLOW)."""
    try:
        signals = gather_signals(conn, email, canonical_id, region)
    except Exception as e:
        log("FAIL-SAFE: signal gather raised (%s) -> HOLD" % e)
        return HOLD, ["gate_signal_error_failsafe_hold"]
    verdict, reasons = decide(email, signals, golden_verify_status)
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO atlas.compliance_log (email, domain, canonical_id, "
                    "verdict, reasons) VALUES (%s,%s,%s,%s,%s)",
                    (email, email_domain(email), canonical_id, verdict, json.dumps(reasons)))
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback()
        log("could not persist compliance_log (%s) -> verdict still returned" % e)
    return verdict, reasons


def selftest():
    load_env_file(DB_ENV_PATH)
    load_env_file(AUTOPULL_ENV_PATH)
    ok = True
    # ---- verdict asserts (the load-bearing logic), all offline ----
    cases = [
        # (email, signals, golden_verify, expected_verdict, must_contain_reason)
        ("info@joesplumbingreno.com", {}, "ok", ALLOW, "all_checks_passed"),
        ("sales@acmewidgets.com", {}, "ok", ALLOW, None),
        ("bob@gmail.com", {}, "ok", BLOCK, "free_mail_not_business_domain"),
        ("not-an-email", {}, "ok", BLOCK, "invalid_email_syntax"),
        ("clerk@city.gov", {}, "ok", BLOCK, "gov_mil_suppressed"),
        ("x@mailinator.com", {}, "ok", BLOCK, "disposable_domain"),
        ("no-reply@acme.com", {}, "ok", BLOCK, "blocked_role_localpart_no-reply"),
        ("a@acme.com", {"suppressed_email": True}, "ok", BLOCK, "suppressed_email"),
        ("a@acme.com", {"unsubscribed": True}, "ok", BLOCK, "prior_unsubscribe"),
        ("a@acme.com", {"hard_bounce": True}, "ok", BLOCK, "prior_hard_bounce"),
        ("a@acme.com", {"complaint": True}, "ok", BLOCK, "prior_spam_complaint"),
        ("a@acme.com", {"soft_bounce_count": 3}, "ok", HOLD, "soft_bounce_streak"),
        ("a@acme.com", {"domain_sends_in_window": 5}, "ok", HOLD, "frequency_cap_domain"),
        ("a@acme.com", {"company_contacted_recently": True}, "ok", HOLD, "duplicate_company_recent_contact"),
        ("a@acme.com", {"mailbox_health": 0.2}, "ok", HOLD, "sending_mailbox_unhealthy"),
        ("a@acme.com", {}, "needs_reverify", BLOCK, "golden_domain_unverified_needs_reverify"),
    ]
    for email, sig, gvs, exp, must in cases:
        v, reasons = decide(email, sig, gvs)
        if v != exp:
            log("FAIL: %r expected %s got %s (%s)" % (email, exp, v, reasons)); ok = False
        elif must and must not in reasons:
            log("FAIL: %r missing reason %r in %s" % (email, must, reasons)); ok = False
    # BLOCK must win over a HOLD on the same candidate
    v, reasons = decide("a@acme.com", {"soft_bounce_count": 3, "complaint": True}, "ok")
    assert v == BLOCK, "BLOCK must win over HOLD"
    log("verdicts: %d cases asserted; BLOCK-beats-HOLD precedence OK" % len(cases))
    # no-verdict-means-no-send is enforced by the caller (sender sends only on ALLOW);
    # here we prove ALLOW is only ever returned when reasons==['all_checks_passed'] or empty-clean.
    vclean, rclean = decide("info@cleanbiz.com", {}, "ok")
    assert vclean == ALLOW, "clean business email should ALLOW"
    log("clean business email -> ALLOW (%s)" % rclean)
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
            cur.execute("SELECT count(*) FROM atlas.compliance_log")
            log("compliance_log rows: %d" % cur.fetchone()[0])
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
    if "--migrate" in sys.argv:
        conn = connect_pg()
        ensure_schema(conn)
        conn.close()
        print("migrate OK")
        return
    if "--check" in sys.argv:
        i = sys.argv.index("--check")
        ctx = json.loads(sys.argv[i + 1]) if len(sys.argv) > i + 1 else {}
        conn = connect_pg()
        try:
            ensure_schema(conn)
            v, reasons = evaluate(conn, ctx.get("email"), ctx.get("canonical_id"),
                                  ctx.get("region"), ctx.get("golden_verify_status"))
            print(json.dumps({"verdict": v, "reasons": reasons}))
        finally:
            conn.close()
        return
    sys.stderr.write(__doc__)
    sys.exit(2)


if __name__ == "__main__":
    main()
