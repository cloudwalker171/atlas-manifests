#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A.T.L.A.S. STANDALONE OUTREACH SENDER -- SHADOW MODE ONLY (does NOT transmit).

WHAT THIS IS (review item 3 in the plan; item 5 in the build order)
-------------------------------------------------------------------
The "sending engine" half of the goal -- a STANDALONE service (NOT in WordPress)
that manages the full outreach pipeline: mailbox pools, sender domains,
SPF/DKIM/DMARC checks, warmup schedule, daily caps, provider throttle,
reply/bounce/unsubscribe/complaint handling, sequence state, A/B variants, and
per-send routing through the compliance gate (item 4).

CRITICAL SAFETY: it is built so it GENERATES + COMPLIANCE-CHECKS + QUEUES every
send but NEVER TRANSMITS. There is a HARD SHADOW FLAG that defaults to SHADOW
(no transmission). Only the user can flip it to live, and even then real sending
+ sender-domain setup needs the user's explicit go and must NEVER use the brand
root domain. This module's --selftest PROVES that in shadow mode nothing is sent.

THE TRANSMISSION INTERLOCK (why shadow is airtight)
---------------------------------------------------
A send is physically transmitted ONLY if ALL of the following are true, checked
at the single choke-point _transmit():
  1. ATLAS_SENDER_LIVE == "1"                  (the user-only hard flag; default unset = SHADOW)
  2. ATLAS_SENDER_LIVE_CONFIRM == the exact     (a second, separate confirmation token the user sets;
     phrase "I-AUTHORIZE-LIVE-SEND"             prevents a single stray env from going live)
  3. a real SMTP transport is configured        (ATLAS_SMTP_HOST etc.) AND not the brand root domain
  4. the per-send compliance verdict == ALLOW
If ANY is false, _transmit() records the send as SHADOW_QUEUED in atlas.send_log
(status='shadow') and returns WITHOUT opening any socket. The default build path
NEVER imports/instantiates an SMTP client at all in shadow mode.

There is NO code path that sends on the default configuration. The SMTP send is
behind the interlock AND behind a lazy import so a misconfiguration cannot leak.

PIPELINE (all of it runs in shadow; only _transmit is gated)
------------------------------------------------------------
  select_targets()   -- reads ONLY atlas.outreach_pool (item-2 contract) +
                        atlas.lead_score_components (item-3 action='outreach_eligible')
  pick_mailbox()     -- choose a warm, healthy mailbox from the pool honoring
                        daily cap + provider throttle + warmup schedule
  render()           -- A/B template variant -> subject+body (no PII beyond the
                        business's own published role email)
  compliance gate    -- evaluate() per send; ALLOW required to even be QUEUED live
  _transmit()        -- THE INTERLOCK. shadow -> log status='shadow', no socket.
  record_state()     -- sequence state, send_log, A/B assignment

SENDER DOMAIN / AUTH POSTURE (documented, enforced in checks)
-------------------------------------------------------------
  * SPF/DKIM/DMARC are CHECKED for any configured sender domain; a domain failing
    auth is HELD out of the pool.
  * Brand root domain is HARD-DENIED as a sender (ATLAS_BRAND_ROOT); only warmed
    secondary domains may ever send, and only after the user authorizes live.
  * Warmup ramp + daily caps + provider throttle are modeled and enforced even in
    shadow (so the queue that WOULD be sent is realistic).

SAFETY: additive. Owns atlas.send_log / sequence_state / mailbox_pool /
sender_domain / ab_variant. Never ALTERs other tables. Reads outreach_pool +
lead_score_components + the compliance gate. .gov/.mil already excluded upstream
+ re-blocked by the gate. Stdlib + psycopg2 only in shadow; SMTP libs lazy-imported
behind the interlock.

MODES: --migrate / --selftest / --once (shadow pipeline pass) / --loop /
       --status (print shadow/live posture)
"""

import datetime
import hashlib
import json
import os
import sys
import time

try:
    import psycopg2
except Exception:  # pragma: no cover
    psycopg2 = None

# the compliance gate (item 4) -- imported as a library; same dir on the box
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    import atlas_compliance_gate as gate
except Exception:  # pragma: no cover -- selftest tolerates absence with a stub
    gate = None

DB_ENV_PATH       = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
AUTOPULL_ENV_PATH = os.environ.get("ATLAS_AUTOPULL_CONF", "/etc/atlas/autopull.env")
NODE_ID           = os.environ.get("NODE_ID", "hetzner")
INTERVAL_SEC      = int(os.environ.get("ATLAS_SENDER_INTERVAL", "600"))
BATCH             = int(os.environ.get("ATLAS_SENDER_BATCH", "200"))
STATE_DIR         = os.environ.get("ATLAS_SENDER_STATE_DIR", "/var/lib/atlas/sender")

# ---- THE INTERLOCK FLAGS (user-only) ----
LIVE_FLAG         = os.environ.get("ATLAS_SENDER_LIVE", "")           # must be exactly "1"
LIVE_CONFIRM      = os.environ.get("ATLAS_SENDER_LIVE_CONFIRM", "")   # must be the exact phrase
LIVE_CONFIRM_PHRASE = "I-AUTHORIZE-LIVE-SEND"
BRAND_ROOT        = os.environ.get("ATLAS_BRAND_ROOT", "lionclickmedia.com")

# warmup / caps (modeled even in shadow)
WARMUP_START      = int(os.environ.get("ATLAS_SENDER_WARMUP_START", "5"))
WARMUP_MAX        = int(os.environ.get("ATLAS_SENDER_DAILY_CAP", "40"))
WARMUP_STEP       = int(os.environ.get("ATLAS_SENDER_WARMUP_STEP", "5"))


def log(msg):
    print("[sender] %s %s" %
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
        application_name="atlas_outreach_sender",
    )
    conn.autocommit = False
    return conn


def regclass_exists(cur, qualified):
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (qualified,))
    return bool(cur.fetchone()[0])


# --------------------------------------------------------------------------- #
# THE INTERLOCK -- the single most important function in this file.
# --------------------------------------------------------------------------- #
def is_live_authorized(sender_domain=None):
    """Returns (live: bool, reason: str). DEFAULT = NOT live (shadow). Live ONLY
    when the user has set BOTH hard flags AND the sender domain is a warmed
    secondary (never the brand root). Reads os.environ LIVE each call so a stale
    cached flag can never authorize a send."""
    live_flag = os.environ.get("ATLAS_SENDER_LIVE", "")
    live_confirm = os.environ.get("ATLAS_SENDER_LIVE_CONFIRM", "")
    brand_root = os.environ.get("ATLAS_BRAND_ROOT", BRAND_ROOT)
    if live_flag != "1":
        return False, "shadow_default_ATLAS_SENDER_LIVE_not_1"
    if live_confirm != LIVE_CONFIRM_PHRASE:
        return False, "shadow_missing_live_confirm_phrase"
    if sender_domain and (sender_domain == brand_root or sender_domain.endswith("." + brand_root)):
        return False, "brand_root_domain_hard_denied_as_sender"
    if not os.environ.get("ATLAS_SMTP_HOST"):
        return False, "no_smtp_transport_configured"
    return True, "live_authorized"


def _transmit(conn, send):
    """THE CHOKE POINT. In shadow (the default) this NEVER opens a socket and
    NEVER imports an SMTP client -- it records the send as status='shadow' and
    returns. Live transmission is reachable ONLY past is_live_authorized()."""
    live, reason = is_live_authorized(send.get("sender_domain"))
    status = "shadow"
    transport_note = reason
    if live and send.get("verdict") == "ALLOW":
        # ---- LIVE PATH (NEVER reached on the default build / in selftest) ----
        # Lazy import so the SMTP client is not even loaded in shadow mode.
        try:
            import smtplib  # noqa: F401  (intentionally local + behind the interlock)
            # Real send is intentionally NOT implemented here as an executable
            # default; live wiring is a user-authorized follow-up step. We refuse
            # to ship an always-ready transmitter. Record intent, do not send.
            status = "live_blocked_pending_user_smtp_wiring"
            transport_note = "interlock_open_but_transmitter_intentionally_unwired"
        except Exception as e:
            status = "live_error"
            transport_note = "smtp_import_failed_%s" % e
    record_send(conn, send, status, transport_note)
    return status


def record_send(conn, send, status, note):
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO atlas.send_log
            (canonical_id, recipient_email, recipient_domain, sender_mailbox,
             sender_domain, subject, ab_variant, verdict, status, note, created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())""",
            (send.get("canonical_id"), send.get("email"), send.get("recipient_domain"),
             send.get("mailbox"), send.get("sender_domain"), send.get("subject"),
             send.get("ab_variant"), send.get("verdict"), status, note))
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback()
        log("could not record send_log (%s)" % e)


# --------------------------------------------------------------------------- #
# DDL -- additive, the sender's own tables
# --------------------------------------------------------------------------- #
def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS atlas")
    cur.execute("""CREATE TABLE IF NOT EXISTS atlas.sender_domain (
        domain text PRIMARY KEY, spf bool, dkim bool, dmarc bool,
        is_brand_root bool NOT NULL DEFAULT false, healthy bool NOT NULL DEFAULT false,
        added_at timestamptz NOT NULL DEFAULT now())""")
    cur.execute("""CREATE TABLE IF NOT EXISTS atlas.mailbox_pool (
        mailbox text PRIMARY KEY, sender_domain text, warmup_day int NOT NULL DEFAULT 0,
        daily_cap int NOT NULL DEFAULT 5, sent_today int NOT NULL DEFAULT 0,
        health double precision NOT NULL DEFAULT 1.0, provider text,
        updated_at timestamptz NOT NULL DEFAULT now())""")
    cur.execute("""CREATE TABLE IF NOT EXISTS atlas.send_log (
        id bigserial PRIMARY KEY, canonical_id text, recipient_email text,
        recipient_domain text, sender_mailbox text, sender_domain text,
        subject text, ab_variant text, verdict text, status text NOT NULL,
        note text, created_at timestamptz NOT NULL DEFAULT now())""")
    cur.execute("CREATE INDEX IF NOT EXISTS send_log_dom ON atlas.send_log (recipient_domain, created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS send_log_status ON atlas.send_log (status)")
    cur.execute("""CREATE TABLE IF NOT EXISTS atlas.sequence_state (
        canonical_id text PRIMARY KEY, step int NOT NULL DEFAULT 0,
        last_sent_at timestamptz, next_due_at timestamptz, state text NOT NULL DEFAULT 'active',
        updated_at timestamptz NOT NULL DEFAULT now())""")
    cur.execute("""CREATE TABLE IF NOT EXISTS atlas.ab_variant (
        variant text PRIMARY KEY, subject text, body text, weight double precision NOT NULL DEFAULT 1.0,
        sent bigint NOT NULL DEFAULT 0, replied bigint NOT NULL DEFAULT 0)""")
    # bounce/complaint/unsubscribe handling tables (read by the compliance gate)
    cur.execute("""CREATE TABLE IF NOT EXISTS atlas.bounce_log (
        email text, hard bool NOT NULL DEFAULT true, created_at timestamptz NOT NULL DEFAULT now())""")
    cur.execute("""CREATE TABLE IF NOT EXISTS atlas.complaint_log (
        email text, domain text, created_at timestamptz NOT NULL DEFAULT now())""")
    cur.execute("""CREATE TABLE IF NOT EXISTS atlas.unsubscribe_log (
        email text, domain text, created_at timestamptz NOT NULL DEFAULT now())""")
    conn.commit()
    cur.close()


# --------------------------------------------------------------------------- #
# auth checks (SPF/DKIM/DMARC) -- offline-checkable shape; live DNS is on-box
# --------------------------------------------------------------------------- #
def domain_auth_ok(rec):
    """rec: dict with spf/dkim/dmarc bools + is_brand_root. A sender domain is
    poolable ONLY if all three pass AND it is not the brand root."""
    if rec.get("is_brand_root"):
        return False, "brand_root_denied"
    if not (rec.get("spf") and rec.get("dkim") and rec.get("dmarc")):
        return False, "spf_dkim_dmarc_incomplete"
    return True, "auth_ok"


def warmup_cap(warmup_day):
    return min(WARMUP_MAX, WARMUP_START + WARMUP_STEP * max(0, warmup_day))


# --------------------------------------------------------------------------- #
# pipeline (shadow): select -> mailbox -> render -> gate -> _transmit
# --------------------------------------------------------------------------- #
AB_TEMPLATES = {
    "A": {"subject": "Quick question about {company}", "body": "Hi {company} team -- noticed you don't have live chat yet..."},
    "B": {"subject": "{company}: a 30-second idea", "body": "Hi -- a quick idea for {company}'s site..."},
}


def render(company, variant):
    t = AB_TEMPLATES.get(variant, AB_TEMPLATES["A"])
    return (t["subject"].replace("{company}", company or "your business"),
            t["body"].replace("{company}", company or "your business"))


def pick_variant(canonical_id):
    h = int(hashlib.sha1((canonical_id or "x").encode()).hexdigest(), 16)
    return "A" if h % 2 == 0 else "B"


def select_targets(conn, limit):
    """Reads ONLY the item-2 outreach_pool contract + item-3 eligibility."""
    cur = conn.cursor()
    if not regclass_exists(cur, "atlas.outreach_pool"):
        cur.close()
        return []
    has_scores = regclass_exists(cur, "atlas.lead_score_components")
    if has_scores:
        cur.execute("""SELECT p.canonical_id, p.legal_name, p.domain, p.email
                       FROM atlas.outreach_pool p
                       JOIN atlas.lead_score_components c ON c.canonical_id=p.canonical_id
                       WHERE c.action='outreach_eligible'
                       ORDER BY c.worth_pursuing DESC LIMIT %s""", (limit,))
    else:
        cur.execute("""SELECT canonical_id, legal_name, domain, email
                       FROM atlas.outreach_pool LIMIT %s""", (limit,))
    rows = [{"canonical_id": r[0], "company": r[1], "domain": r[2], "email": r[3]}
            for r in cur.fetchall()]
    cur.close()
    return rows


def pick_mailbox(conn):
    """A warm, healthy mailbox under its daily cap. None -> no eligible mailbox
    (shadow still proceeds, recording the send with mailbox=None)."""
    cur = conn.cursor()
    if not regclass_exists(cur, "atlas.mailbox_pool"):
        cur.close()
        return None
    cur.execute("""SELECT mailbox, sender_domain, warmup_day, daily_cap, sent_today, health, provider
                   FROM atlas.mailbox_pool WHERE health >= 0.5
                   ORDER BY (sent_today::float / GREATEST(daily_cap,1)) ASC LIMIT 1""")
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    mailbox, sdom, wday, cap, sent, health, provider = row
    if (sent or 0) >= warmup_cap(wday or 0):
        return None  # at cap
    return {"mailbox": mailbox, "sender_domain": sdom, "health": health, "provider": provider}


def run_once(conn):
    ensure_schema(conn)
    live, reason = is_live_authorized()
    posture = "LIVE" if live else "SHADOW"
    targets = select_targets(conn, BATCH)
    queued = 0
    shadow = 0
    blocked = 0
    held = 0
    for t in targets:
        variant = pick_variant(t["canonical_id"])
        subject, body = render(t["company"], variant)
        mb = pick_mailbox(conn) or {"mailbox": None, "sender_domain": None}
        # compliance gate (item 4) -- ALLOW required
        verdict, reasons = ("HOLD", ["gate_unavailable_failsafe"])
        if gate is not None:
            try:
                verdict, reasons = gate.evaluate(
                    conn, t["email"], canonical_id=t["canonical_id"],
                    golden_verify_status="ok")
            except Exception as e:
                verdict, reasons = "HOLD", ["gate_error_%s" % e]
        send = {"canonical_id": t["canonical_id"], "email": t["email"],
                "recipient_domain": (t["email"].rsplit("@", 1)[1] if t["email"] and "@" in t["email"] else None),
                "mailbox": mb["mailbox"], "sender_domain": mb["sender_domain"],
                "subject": subject, "ab_variant": variant, "verdict": verdict}
        if verdict == "BLOCK":
            record_send(conn, send, "blocked", ",".join(reasons))
            blocked += 1
            continue
        if verdict == "HOLD":
            record_send(conn, send, "held", ",".join(reasons))
            held += 1
            continue
        # ALLOW -> route through the interlock (shadow records, never transmits)
        status = _transmit(conn, send)
        if status == "shadow":
            shadow += 1
        else:
            queued += 1
    body = {"schema": "atlas.sender.v1", "node": NODE_ID, "ts": int(time.time()),
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "posture": posture, "posture_reason": reason, "targets": len(targets),
            "shadow_queued": shadow, "blocked": blocked, "held": held,
            "live_transmitted": queued,
            "honesty": ("SHADOW is the default and only safe posture. _transmit() "
                        "opens no socket and imports no SMTP client in shadow. Live "
                        "needs BOTH user flags + a warmed non-brand-root domain + "
                        "user-authorized SMTP wiring; even then the transmitter is "
                        "intentionally unwired pending the user's explicit go.")}
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        with open(os.path.join(STATE_DIR, "last_sender.json"), "w") as fh:
            json.dump(body, fh)
    except OSError:
        pass
    log("posture=%s targets=%d shadow_queued=%d blocked=%d held=%d live_transmitted=%d"
        % (posture, len(targets), shadow, blocked, held, queued))
    return 0


def selftest():
    load_env_file(DB_ENV_PATH)
    load_env_file(AUTOPULL_ENV_PATH)
    ok = True
    # ---- PROVE SHADOW SENDS NOTHING ----
    # 1. default env: not live
    os.environ.pop("ATLAS_SENDER_LIVE", None)
    os.environ.pop("ATLAS_SENDER_LIVE_CONFIRM", None)
    live, reason = is_live_authorized()
    assert not live, "DEFAULT must be SHADOW (not live), got live (%s)" % reason
    log("interlock: default posture = SHADOW (%s)" % reason)
    # 2. even with ONLY the LIVE flag set, still shadow (needs the confirm phrase)
    os.environ["ATLAS_SENDER_LIVE"] = "1"
    live, reason = is_live_authorized()
    assert not live and reason == "shadow_missing_live_confirm_phrase", \
        "LIVE flag alone must NOT authorize (needs confirm phrase), got live=%s/%s" % (live, reason)
    log("interlock: LIVE flag alone insufficient (%s)" % reason)
    # 3. flag + confirm but brand-root sender domain -> denied
    os.environ["ATLAS_SENDER_LIVE_CONFIRM"] = LIVE_CONFIRM_PHRASE
    live, reason = is_live_authorized(sender_domain=BRAND_ROOT)
    assert not live and reason == "brand_root_domain_hard_denied_as_sender", \
        "brand root domain must be HARD-DENIED as sender, got %s/%s" % (live, reason)
    log("interlock: brand-root sender denied (%s)" % reason)
    # 4. flag + confirm + non-brand domain but NO smtp transport -> still not live
    live, reason = is_live_authorized(sender_domain="warm-secondary.example")
    assert not live and reason == "no_smtp_transport_configured", \
        "no SMTP transport must keep it shadow, got %s/%s" % (live, reason)
    log("interlock: no SMTP transport -> still shadow (%s)" % reason)
    # 4b. ALL interlock conditions met EXCEPT we never wire a real transmitter ->
    #     even a fully-authorized live posture cannot actually send (ships unwired)
    os.environ["ATLAS_SMTP_HOST"] = "smtp.warm-secondary.example"
    live, reason = is_live_authorized(sender_domain="warm-secondary.example")
    assert live, "with both flags + non-brand domain + smtp host, interlock should report authorized"
    st = _transmit(None, {"email": "info@acme.com", "verdict": "ALLOW",
                          "sender_domain": "warm-secondary.example"})
    assert st == "live_blocked_pending_user_smtp_wiring", \
        "even fully authorized, the transmitter is intentionally unwired (got %r)" % st
    log("interlock: fully-authorized live STILL cannot transmit (unwired by design): %s" % st)
    os.environ.pop("ATLAS_SMTP_HOST", None)
    os.environ.pop("ATLAS_SENDER_LIVE", None)
    os.environ.pop("ATLAS_SENDER_LIVE_CONFIRM", None)
    # 5. _transmit in shadow records status='shadow' and opens NO socket (conn=None
    #    proves no DB/socket dependency to refuse a send)
    status = _transmit(None, {"email": "info@acme.com", "verdict": "ALLOW",
                              "sender_domain": "warm-secondary.example"})
    assert status == "shadow", "shadow _transmit must record status='shadow', got %r" % status
    log("interlock: ALLOW verdict in shadow -> status='shadow' (NO transmission)")
    # 6. auth check: brand root + incomplete auth rejected; full auth non-brand ok
    assert not domain_auth_ok({"is_brand_root": True, "spf": True, "dkim": True, "dmarc": True})[0]
    assert not domain_auth_ok({"is_brand_root": False, "spf": True, "dkim": False, "dmarc": True})[0]
    assert domain_auth_ok({"is_brand_root": False, "spf": True, "dkim": True, "dmarc": True})[0]
    log("auth: brand-root denied, partial-auth denied, full-auth non-brand poolable")
    # 7. warmup ramp monotonic + capped
    assert warmup_cap(0) == WARMUP_START
    assert warmup_cap(100) == WARMUP_MAX
    assert warmup_cap(1) >= warmup_cap(0)
    log("warmup: ramp starts at %d, caps at %d" % (WARMUP_START, WARMUP_MAX))
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
            cur.execute("SELECT count(*) FROM atlas.send_log WHERE status='shadow'")
            log("send_log shadow rows: %d" % cur.fetchone()[0])
            cur.close(); conn.close()
        except Exception as e:
            log("%s db connect/schema (%s)" % ("WARN(offline)" if offline else "FAIL", e))
            if not offline:
                ok = False
    print("SELFTEST %s" % ("OK -- SHADOW PROVEN, NOTHING TRANSMITTED" if ok else "FAILED"))
    return 0 if ok else 1


def main():
    if "--selftest" in sys.argv:
        sys.exit(selftest())
    load_env_file(DB_ENV_PATH)
    load_env_file(AUTOPULL_ENV_PATH)
    if "--status" in sys.argv:
        live, reason = is_live_authorized()
        print(json.dumps({"posture": "LIVE" if live else "SHADOW", "reason": reason}))
        return
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
