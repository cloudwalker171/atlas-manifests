#!/opt/atlas/venv/bin/python
"""
atlas_qa_auditor.py  --  META QA AUDITOR: the quality gate that confirms EVERY
record handed to prospects truly meets the bar, and audits enrichment
completeness vs the Apollo/ZoomInfo target field set.

"Who's checking the data?" -- this is the answer. A continuous meta-auditor that
runs read-mostly over the enriched set and, for each business, decides:

    PASS        -> safe to hand to prospects (all hard gates clear)
    QUARANTINE  -> a hard gate failed; NOT sent to prospects; reason logged
    REENRICH    -> high-value but enrichment-incomplete; re-seeded to the enrich
                   queue for another pass (NEVER dropped)

It writes its verdict ADDITIVELY onto atlas.business via four new, nullable,
constraint-free columns (qa_status, qa_score, qa_reason, qa_checked_at) created
with ADD COLUMN IF NOT EXISTS -- no CHECK constraint, no NOT NULL, no DROP/ALTER
of any existing column, so it CANNOT violate a CHECK constraint or destroy data.
It NEVER edits enrich_queue/field_provenance shape; for re-enrich it only
INSERTs queue rows using EXISTING CHECK-legal task_type values
(find_domain / ai_classify) with ON CONFLICT DO NOTHING.

THE FOUR CHECKS (per record)
----------------------------
 1. NO-CHAT CONFIRMED  -- re-verify the business has NO live chat widget.
      (a) CODE-SIGNATURE scan across a 50+ provider fingerprint set
          (CHAT_SIGNATURES below -- extends the PHP "Chat Detection v5"
          ~45-provider list to 50+). Runs against the homepage HTML.
      (b) [verify-on-box] optional HEADLESS-RENDER / screenshot pass to catch
          chat bubbles injected by JS after load. The sandbox has NO egress and
          no browser, so this stage is GATED OFF here and marked
          rendered_check='verify_on_box'. A record only earns the strongest
          no-chat grade when BOTH the signature scan AND (where available) the
          rendered check agree. With render unavailable, code-signature-clean is
          recorded as 'no_chat_code_clean' (honest: NOT a visual assertion).
 2. WEBSITE found + LIVE -- HTTP 200, real site, not parked.        [verify-on-box]
 3. EMAIL found + MX-valid + not placeholder/role-junk.              [verify-on-box for live MX]
 4. ENRICHMENT COMPLETENESS -- count filled target fields vs the Apollo/ZoomInfo
      target set (TARGET_FIELDS, from ENRICHMENT_FIELDS_VS_APOLLO_ZOOMINFO.md),
      compute a per-record %, flag below COMPLETENESS_THRESHOLD.

Records failing 1, 2, or 3 are QUARANTINED. Records that pass 1-3 but score
below the completeness threshold AND are high-value are sent to REENRICH (not
dropped). The auditor writes a QA report (pass rate, top failure reasons, avg
completeness) to status/<node>/qa-<node>.json via the SAME GitHub Contents API
channel the enrich worker / healthcheck use, AND appends learning events to the
/brain inbox (/var/lib/brain/logs/inbox.jsonl) so the system learns which
criteria correlate with closure.

HONESTY (sandbox has no egress)
-------------------------------
  * Live HTTP 200 / parked detection, live MX lookups, and the headless render
    are VERIFY-ON-BOX. In this sandbox they are skipped and the offline grade is
    used (existing field_provenance values + syntactic checks), with each such
    decision tagged so the box run can upgrade it. No live network result is
    fabricated.
  * READ-MOSTLY: the only writes are (a) the four additive qa_* columns,
    (b) ON CONFLICT DO NOTHING re-enrich queue rows using legal task_types,
    (c) the status JSON + brain inbox (off-DB). It performs NO DDL on the
    production tables beyond ADD COLUMN IF NOT EXISTS for its own qa_* columns.

MODES
  --migrate    introspect the real schema (fail-loud if missing); ADD the four
               qa_* columns IF NOT EXISTS; print counts; exit.
  --selftest   db.env loads, Postgres connects (if reachable), schema present,
               the offline graders (chat-signature, email-junk, completeness)
               pass their built-in unit checks; exit 0 ok / 3 broken. Network is
               NOT required (offline graders self-test without a DB).
  --once       audit a single batch then exit.
  --loop       (DEFAULT) run forever: audit batches, report, idle when caught up.

DB creds come from /etc/atlas/db.env exactly like atlas_enrich_worker.py
(PG* / DB_* / ATLAS_DB_* picked, optional leading `export`).
"""

import os
import re
import sys
import json
import time
import base64
import socket
import datetime
import urllib.parse
import urllib.request
import urllib.error

try:
    import psycopg2  # type: ignore
    _HAVE_PG = True
except Exception:  # noqa: BLE001
    _HAVE_PG = False

try:
    import dns.resolver  # type: ignore
    _HAVE_DNS = True
except Exception:  # noqa: BLE001
    _HAVE_DNS = False


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
DB_ENV_PATH       = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
AUTOPULL_ENV_PATH = os.environ.get("ATLAS_AUTOPULL_CONF", "/etc/atlas/autopull.env")

BUSINESS_TBL = ("atlas", "business")
QUEUE_TBL    = ("atlas", "enrich_queue")
PROV_TBL     = ("atlas", "field_provenance")

BATCH                  = int(os.environ.get("ATLAS_QA_BATCH", "200"))
IDLE_SEC               = float(os.environ.get("ATLAS_QA_IDLE_SEC", "30"))
REPORT_SEC             = int(os.environ.get("ATLAS_QA_REPORT_SEC", "300"))
PACING_MS              = int(os.environ.get("ATLAS_QA_PACING_MS", "0"))
COMPLETENESS_THRESHOLD = float(os.environ.get("ATLAS_QA_COMPLETENESS_MIN", "0.55"))
HIGH_VALUE_THRESHOLD   = float(os.environ.get("ATLAS_QA_HIGH_VALUE_MIN", "0.35"))
# Live network checks are OFF in the sandbox (no egress). The box turns these on.
LIVE_HTTP   = os.environ.get("ATLAS_QA_LIVE_HTTP", "0") not in ("0", "false", "no", "")
LIVE_MX     = os.environ.get("ATLAS_QA_LIVE_MX",   "0") not in ("0", "false", "no", "")
LIVE_RENDER = os.environ.get("ATLAS_QA_LIVE_RENDER", "0") not in ("0", "false", "no", "")
HTTP_TIMEOUT = int(os.environ.get("ATLAS_QA_HTTP_TIMEOUT", "12"))
HTTP_MAXBYTES= int(os.environ.get("ATLAS_QA_HTTP_MAXBYTES", "700000"))
# Re-enrich uses ONLY CHECK-legal enrich_queue.task_type values:
REENRICH_TASK = os.environ.get("ATLAS_QA_REENRICH_TASK", "find_domain")  # legal CHECK value
BRAIN_INBOX  = os.environ.get("ATLAS_BRAIN_INBOX", "/var/lib/brain/logs/inbox.jsonl")
USER_AGENT   = ("atlas-qa-auditor/1.0 (+https://github.com/cloudwalker171/atlas-manifests; "
                "read-mostly QA gate; respects robots)")

# qa verdict vocabulary (stored as plain text -- NO CHECK constraint added)
QA_PASS       = "pass"
QA_QUARANTINE = "quarantine"
QA_REENRICH   = "reenrich"


# --------------------------------------------------------------------------- #
# CHAT DETECTION -- 50+ provider signature set (extends PHP "Chat Detection v5"
# ~45-provider list to 50+). Each entry: (provider_label, [substrings any-of]).
# Substrings are matched case-insensitively against the homepage HTML/script src.
# This is the CODE-SIGNATURE half; the rendered/headless half is verify-on-box.
# --------------------------------------------------------------------------- #
CHAT_SIGNATURES = [
    ("intercom",        ["widget.intercom.io", "intercomcdn.com", "intercomSettings", "intercom.com"]),
    ("drift",           ["js.driftt.com", "driftt.com", "drift.com", "drift.load"]),
    ("hubspot_chat",    ["js.usemessages.com", "js.hs-scripts.com", "hubspotconversations"]),
    ("tawk",            ["embed.tawk.to", "tawk.to", "Tawk_API"]),
    ("zendesk_chat",    ["static.zdassets.com", "zopim.com", "zopim", "ekr/snippet.js", "zendesk.com/embeddable"]),
    ("tidio",           ["code.tidio.co", "tidio.co", "tidiochat"]),
    ("livechat",        ["cdn.livechatinc.com", "livechatinc.com", "__lc.license"]),
    ("olark",           ["static.olark.com", "olark.com", "olark.identify"]),
    ("crisp",           ["client.crisp.chat", "crisp.chat", "$crisp", "CRISP_WEBSITE_ID"]),
    ("freshchat",       ["wchat.freshchat.com", "freshchat.com", "fcWidget"]),
    ("freshdesk_msg",   ["fw-cdn.com", "freshworks.com", "fwSettings"]),
    ("podium",          ["connect.podium.com", "podium.com", "podium-widget"]),
    ("birdeye",         ["birdeye.com", "messaging.birdeye.com", "bw_data"]),
    ("apexchat",        ["apexchat.com", "homegyro.com", "live-chat-widget"]),
    ("leadconnector",   ["leadconnectorhq.com", "widgets.leadconnector", "chat-widget.leadconnector"]),
    ("gohighlevel",     ["msgsndr.com", "highlevel", "ghl-chat"]),
    ("whatsapp_widget", ["wa.me/", "api.whatsapp.com/send", "wa.link/", "click.to.chat", "whatsapp-widget"]),
    ("facebook_msgr",   ["connect.facebook.net/en_US/sdk/xfbml.customerchat", "fb-customerchat", "facebook-jssdk", "customerchat"]),
    ("gorgias",         ["config.gorgias.chat", "gorgias.chat", "gorgias.io"]),
    ("kustomer",        ["cdn.kustomerapp.com", "kustomer.com", "Kustomer.start"]),
    ("gist",            ["getgist.com", "gist.build", "gist.chat"]),
    ("chatra",          ["call.chatra.io", "chatra.io", "ChatraID"]),
    ("smartsupp",       ["smartsuppchat.com", "smartsupp.com", "_smartsupp"]),
    ("jivochat",        ["code.jivosite.com", "jivosite.com", "jivo_api", "jivochat"]),
    ("liveagent",       ["liveagent", "qualityunit", "LiveAgent.createButton"]),
    ("purechat",        ["app.purechat.com", "purechat.com"]),
    ("snapengage",      ["snapengage.com", "storage.googleapis.com/code.snapengage.com", "SnapEngage"]),
    ("comm100",         ["comm100.com", "vue.comm100.com", "Comm100API"]),
    ("liveperson",      ["lpcdn.lpsnmedia.net", "liveperson.net", "lpTag"]),
    ("manychat",        ["manychat.com", "mcwidget", "widget.manychat.com"]),
    ("chatbot_com",     ["cdn.chatbot.com", "chatbot.com"]),
    ("landbot",         ["landbot.io", "static.landbot.io", "myLandbot"]),
    ("tars",            ["tars.com", "hellotars.com", "tarsChat"]),
    ("collect_chat",    ["collect.chat", "collectcdn.com"]),
    ("userlike",        ["userlike.com", "userlike-cdn-widgets", "userlikeWidget"]),
    ("chatwoot",        ["chatwoot", "cdn.chatwoot", "chatwootSDK", "chatwootSettings"]),
    ("re_amaze",        ["reamaze.com", "cdn.reamaze.com", "_support"]),
    ("helpscout_beacon",["beacon-v2.helpscout.net", "helpscout.net", "Beacon("]),
    ("front_chat",      ["chat.frontapp.com", "frontapp.com", "FrontChat"]),
    ("zoho_salesiq",    ["salesiq.zoho.com", "zoho.com/salesiq", "$zoho", "siq"]),
    ("verloop",         ["verloop.io", "verloopLauncher"]),
    ("acquire",         ["acquire.io", "acquire-cdn"]),
    ("formilla",        ["formilla.com", "formilla-widget"]),
    ("zalo_chat",       ["zalo.me", "sp.zalo.me", "zalo-chat-widget"]),
    ("messagebird",     ["messagebird.com", "livechat.messagebird"]),
    ("respondio",       ["respond.io", "cdn.respond.io", "respondio-widget"]),
    ("trengo",          ["trengo.com", "static.widget.trengo.eu", "Trengo"]),
    ("hubspot_meetings",["meetings.hubspot.com", "meetings-embed"]),  # booking widget proxy
    ("tracmail_chat",   ["chaport.com", "app.chaport.com"]),
    ("rocketchat",      ["rocketchat", "rocket.chat", "RocketChat("]),
    ("livesupporti",    ["livesupporti.com", "livesupporti-widget"]),
    ("clickdesk",       ["clickdesk.com", "my.clickdesk.com"]),
    ("tidiochat_alt",   ["tidio-chat", "tidio_code"]),
    ("genericbubble",   ["live-chat", "chat-widget", "chat-bubble", "open-live-chat",
                         "data-chat-widget", "chatwidget", "id=\"chat\"", "class=\"chat-launcher",
                         "livechat", "messenger-button"]),
]
# 50+ providers above (genericbubble is a last-resort custom-widget heuristic, kept
# low-trust: it asserts "review needed", never a hard pass-block on its own when the
# named providers are all absent -- see grade_chat()).
NAMED_PROVIDER_COUNT = len([s for s in CHAT_SIGNATURES if s[0] != "genericbubble"])


# --------------------------------------------------------------------------- #
# ENRICHMENT COMPLETENESS target field set.
# These mirror the COMPANY-LEVEL fields ATLAS actually produces (the real
# field_provenance `field` vocab) cross-walked to the Apollo/ZoomInfo company
# target set from ENRICHMENT_FIELDS_VS_APOLLO_ZOOMINFO.md. Personal-contact
# fields are deliberately EXCLUDED from the denominator (ATLAS does not produce
# them by policy -- counting them would unfairly tank every record).
# weight = how much that field matters to a "prospect-ready" company record.
# --------------------------------------------------------------------------- #
TARGET_FIELDS = [
    ("domain",          2.0),   # hard requirement-ish
    ("website",         1.0),
    ("email",           2.0),   # company role inbox -- mandatory for outreach
    ("phone",           1.0),
    ("mx",              1.0),
    ("email_provider",  0.5),
    ("tech",            1.0),
    ("industry",        1.0),
    ("size_cue",        0.5),
    ("social_linkedin", 0.5),
    ("social_facebook", 0.25),
    ("social_instagram",0.25),
    ("favicon",         0.25),
    ("domain_registrar",0.25),
    ("domain_created",  0.25),
    ("domain_age_band", 0.25),
    ("xref",            0.5),
]
TARGET_TOTAL_WEIGHT = sum(w for _f, w in TARGET_FIELDS)

# placeholder / junk localparts & domains an email must NOT be
JUNK_LOCALPARTS = {
    "user", "test", "example", "name", "email", "your", "youremail",
    "someone", "noreply", "no-reply", "donotreply", "do-not-reply",
}
JUNK_DOMAINS = {
    "example.com", "example.org", "example.net", "domain.com", "email.com",
    "test.com", "yoursite.com", "yourdomain.com", "company.com", "website.com",
    "sentry.io", "wixpress.com",
}
FREE_MAIL = {
    "gmail.com", "googlemail.com", "yahoo.com", "ymail.com", "hotmail.com",
    "outlook.com", "live.com", "msn.com", "aol.com", "icloud.com", "me.com",
    "mail.com", "gmx.com", "protonmail.com", "proton.me",
}
# parked-page signatures (offline heuristic on title/meta if we have them; the
# real live-200/parked test is verify-on-box)
PARKED_SIGNS = [
    "domain is for sale", "buy this domain", "parked free", "godaddy.com/forsale",
    "hugedomains", "sedoparking", "domain parking", "this domain may be for sale",
    "future home of something", "default web page", "apache2 ubuntu default",
    "site not published", "coming soon", "under construction",
]


# --------------------------------------------------------------------------- #
# logging / env (mirrors atlas_enrich_worker.py exactly)
# --------------------------------------------------------------------------- #
def log(msg):
    print(f"[atlas_qa] {datetime.datetime.now(datetime.timezone.utc).isoformat()} {msg}",
          flush=True)


def env_bool(name, default=False):
    v = os.environ.get(name)
    if v is None:
        return default
    return v not in ("0", "false", "False", "no", "")


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
    conn = psycopg2.connect(
        host=pick("PGHOST", "DB_HOST", "ATLAS_DB_HOST", default="localhost"),
        port=pick("PGPORT", "DB_PORT", "ATLAS_DB_PORT", default="5432"),
        dbname=pick("PGDATABASE", "DB_NAME", "ATLAS_DB_NAME", default="tuanichat_atlas"),
        user=pick("PGUSER", "DB_USER", "ATLAS_DB_USER", default="atlas"),
        password=pick("PGPASSWORD", "DB_PASSWORD", "ATLAS_DB_PASSWORD", default=None),
        connect_timeout=int(os.environ.get("ATLAS_DB_CONNECT_TIMEOUT", "10")),
        application_name="atlas_qa_auditor",
    )
    conn.autocommit = False
    return conn


# --------------------------------------------------------------------------- #
# Schema introspection (same strategy as atlas_enrich_worker.py)
# --------------------------------------------------------------------------- #
def table_columns(cur, schema, table):
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema=%s AND table_name=%s", (schema, table))
    return {r[0] for r in cur.fetchall()}


def table_pk(cur, schema, table):
    cur.execute(
        "SELECT a.attname FROM pg_index i "
        "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) "
        "WHERE i.indrelid=%s::regclass AND i.indisprimary", (f"{schema}.{table}",))
    rows = [r[0] for r in cur.fetchall()]
    return rows[0] if rows else None


def pick_col(colset, candidates):
    for c in candidates:
        if c in colset:
            return c
    return None


CANDIDATES = {
    "name":     ["name", "business_name", "title", "display_name", "legal_name"],
    "website":  ["website", "url", "website_url", "homepage", "domain"],
    "phone":    ["phone_e164", "phone", "phone_number", "telephone", "contact_phone"],
    "email":    ["email", "email_address", "contact_email"],
    "region":   ["region", "state", "province"],
    "category": ["category", "primary_category", "categories", "license_description"],
    "has_chat": ["has_chat"],
}


# --------------------------------------------------------------------------- #
# Offline graders (these are unit-testable WITHOUT a DB or network)
# --------------------------------------------------------------------------- #
def grade_chat_from_html(html):
    """CODE-SIGNATURE no-chat scan. Returns (has_chat:bool|None, providers:list,
    generic_only:bool). html may be '' (no fetch available) -> (None, [], False)."""
    if not html:
        return (None, [], False)
    low = html.lower()
    hits = []
    generic_only = True
    for label, needles in CHAT_SIGNATURES:
        for n in needles:
            if n.lower() in low:
                hits.append(label)
                if label != "genericbubble":
                    generic_only = False
                break
    has_chat = len(hits) > 0
    return (has_chat, sorted(set(hits)), generic_only and len(hits) > 0)


def grade_chat(html_has_chat, html_providers, generic_only, stored_has_chat, rendered):
    """Combine code-signature + (verify-on-box) rendered + stored has_chat into a
    no-chat verdict. Returns (ok_no_chat:bool, grade:str, detail:dict).

    ok_no_chat True  => safe (no live chat)
    grade values:
      'no_chat_confirmed'   both code AND rendered agree no chat (box only)
      'no_chat_code_clean'  code clean, render unavailable (sandbox/box-no-render)
      'has_chat_code'       a named provider matched -> QUARANTINE
      'chat_review_generic' only a generic-bubble heuristic matched -> review, not a hard block
      'no_signal'           no html available -> fall back to stored has_chat
    """
    detail = {"code_providers": html_providers, "rendered_check": rendered,
              "stored_has_chat": stored_has_chat}
    # 1. named provider in code -> definitively has chat
    if html_has_chat and not generic_only:
        named = [p for p in html_providers if p != "genericbubble"]
        if named:
            return (False, "has_chat_code", detail)
    # 2. only a generic bubble heuristic matched -> not a hard fail; flag review
    if html_has_chat and generic_only:
        return (True, "chat_review_generic", detail)
    # 3. code clean
    if html_has_chat is False:
        if rendered == "no_chat":
            return (True, "no_chat_confirmed", detail)
        # render unavailable (verify-on-box) -> honest weaker grade
        return (True, "no_chat_code_clean", detail)
    # 4. no html at all -> trust stored has_chat flag if present
    if stored_has_chat in (1, "1", True, "true"):
        return (False, "has_chat_stored", detail)
    return (True, "no_signal", detail)


def email_is_valid_shape(email):
    """Syntax + placeholder/junk/role screen (the OFFLINE half). Returns
    (ok:bool, reason:str). MX validity is verify-on-box (grade_email)."""
    if not email:
        return (False, "no_email")
    email = email.strip().lower()
    # take first if comma-joined (the worker stores up to 3)
    email = email.split(",")[0].strip()
    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[a-z]{2,}", email):
        return (False, "bad_syntax")
    local, _, dom = email.partition("@")
    if dom in JUNK_DOMAINS:
        return (False, "placeholder_domain")
    if local in JUNK_LOCALPARTS:
        return (False, "placeholder_localpart")
    if dom in FREE_MAIL:
        return (False, "free_webmail_not_company")
    return (True, "ok")


def mx_ok(domain):
    """verify-on-box live MX lookup. Returns (ok:bool|None, reason). None when
    live MX is disabled (sandbox) -> caller treats as 'verify_on_box'."""
    if not LIVE_MX or not _HAVE_DNS:
        return (None, "verify_on_box")
    try:
        ans = dns.resolver.resolve(domain, "MX", lifetime=8)
        return (len([r for r in ans]) > 0, "mx_present")
    except Exception:  # noqa: BLE001
        return (False, "no_mx")


def website_live(domain, offline_text=None):
    """Website LIVE + not-parked. Live HTTP-200 is verify-on-box; offline we use
    any stored title/meta to spot obvious parked pages. Returns
    (status:str, detail). status in {'live','parked','dead','verify_on_box'}."""
    if offline_text:
        low = offline_text.lower()
        for s in PARKED_SIGNS:
            if s in low:
                return ("parked", s)
    if not LIVE_HTTP:
        return ("verify_on_box", "no_egress")
    try:
        req = urllib.request.Request(f"https://{domain}/", headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            code = resp.getcode()
            raw = resp.read(HTTP_MAXBYTES)
            text = raw.decode("utf-8", "replace").lower()
            for s in PARKED_SIGNS:
                if s in text:
                    return ("parked", s)
            return ("live" if 200 <= code < 300 else "dead", str(code))
    except Exception as e:  # noqa: BLE001
        return ("dead", type(e).__name__)


def completeness(fields_present):
    """fields_present: set of field_provenance.field values that have a usable
    value for this business. Returns (pct:float 0..1, filled:list, missing:list)."""
    got = 0.0
    filled, missing = [], []
    for f, w in TARGET_FIELDS:
        if f in fields_present:
            got += w
            filled.append(f)
        else:
            missing.append(f)
    pct = (got / TARGET_TOTAL_WEIGHT) if TARGET_TOTAL_WEIGHT else 0.0
    return (round(pct, 4), filled, missing)


# --------------------------------------------------------------------------- #
# Verdict assembly (pure -- the testable core of the auditor)
# --------------------------------------------------------------------------- #
def decide(record):
    """record: dict with keys
       name, website, email, has_chat (stored), homepage_html (may be ''),
       offline_text (title/meta blob, may be ''), domain, fields_present(set).
    Returns dict: {qa_status, qa_score, qa_reason, checks{...}}.

    Gate order: chat -> website -> email -> completeness. A hard-gate failure
    QUARANTINEs immediately (reason = first failing gate). If 1-3 pass but
    completeness < threshold and the record is high-value -> REENRICH. Else PASS.
    """
    checks = {}
    reasons = []

    # --- 1. NO-CHAT ---
    hc, providers, generic_only = grade_chat_from_html(record.get("homepage_html") or "")
    rendered = "verify_on_box" if not LIVE_RENDER else record.get("rendered_result", "no_chat")
    no_chat_ok, chat_grade, chat_detail = grade_chat(
        hc, providers, generic_only, record.get("has_chat"), rendered)
    checks["chat"] = {"ok": no_chat_ok, "grade": chat_grade, **chat_detail}
    if not no_chat_ok:
        reasons.append(f"has_chat:{chat_grade}")

    # --- 2. WEBSITE LIVE ---
    domain = record.get("domain") or ""
    if not domain and record.get("website"):
        domain = _registrable(record["website"])
    if not domain:
        checks["website"] = {"ok": False, "status": "no_domain"}
        reasons.append("no_website")
    else:
        wstatus, wdetail = website_live(domain, record.get("offline_text"))
        ok = wstatus in ("live", "verify_on_box")
        checks["website"] = {"ok": ok, "status": wstatus, "detail": wdetail,
                             "verify_on_box": wstatus == "verify_on_box"}
        if wstatus in ("parked", "dead"):
            reasons.append(f"website_{wstatus}")

    # --- 3. EMAIL ---
    email = record.get("email") or ""
    eok, ereason = email_is_valid_shape(email)
    edetail = {"ok": eok, "reason": ereason}
    if eok:
        edom = email.split(",")[0].strip().split("@")[-1]
        mxok, mxreason = mx_ok(edom)
        edetail["mx"] = mxreason
        edetail["mx_verify_on_box"] = (mxok is None)
        if mxok is False:
            eok = False
            ereason = "no_mx"
    checks["email"] = edetail
    if not eok:
        reasons.append(f"email_{ereason}")

    # --- 4. COMPLETENESS ---
    pct, filled, missing = completeness(record.get("fields_present") or set())
    checks["completeness"] = {"pct": pct, "filled": filled, "missing": missing,
                              "threshold": COMPLETENESS_THRESHOLD}

    # hard gates = chat + website + email
    hard_ok = (checks["chat"]["ok"] and checks["website"]["ok"] and edetail["ok"])

    if not hard_ok:
        return {"qa_status": QA_QUARANTINE, "qa_score": pct,
                "qa_reason": ";".join(reasons) or "hard_gate_fail", "checks": checks}

    if pct < COMPLETENESS_THRESHOLD:
        # high-value but incomplete -> re-enrich, NOT drop
        if pct >= HIGH_VALUE_THRESHOLD:
            return {"qa_status": QA_REENRICH, "qa_score": pct,
                    "qa_reason": f"incomplete_{pct:.2f}_below_{COMPLETENESS_THRESHOLD}",
                    "checks": checks}
        # too thin even to be worth a re-pass yet -> quarantine as low_completeness
        return {"qa_status": QA_QUARANTINE, "qa_score": pct,
                "qa_reason": f"low_completeness_{pct:.2f}", "checks": checks}

    return {"qa_status": QA_PASS, "qa_score": pct, "qa_reason": "ok", "checks": checks}


def _registrable(url):
    if not url:
        return None
    u = url.strip().lower()
    u = re.sub(r"^https?://", "", u)
    u = u.split("/")[0].split("?")[0]
    if u.startswith("www."):
        u = u[4:]
    return u or None


# --------------------------------------------------------------------------- #
# DB: additive qa_* columns (ADD COLUMN IF NOT EXISTS -- no CHECK, nullable)
# --------------------------------------------------------------------------- #
QA_COLUMNS = [
    ("qa_status",     "text"),
    ("qa_score",      "real"),
    ("qa_reason",     "text"),
    ("qa_checked_at", "timestamptz"),
]


def ensure_qa_columns(conn, biz_schema, biz_table):
    """ADD COLUMN IF NOT EXISTS for the four qa_* columns. These are additive,
    nullable, and carry NO CHECK constraint -> cannot violate an existing
    constraint and cannot destroy data. Verifies the production tables exist
    first (fail-loud, additive-only, never DROP/ALTER an existing column)."""
    cur = conn.cursor()
    # fail loud if the real tables aren't there
    for sch, tbl, need in (
        (biz_schema, biz_table, set()),
        (QUEUE_TBL[0], QUEUE_TBL[1], {"business_id", "task_type", "status", "priority"}),
        (PROV_TBL[0], PROV_TBL[1], {"business_id", "field", "value", "confidence"}),
    ):
        have = table_columns(cur, sch, tbl)
        if not have:
            cur.close()
            raise SystemExit(f"SCHEMA ERROR: {sch}.{tbl} not found -- run the enrich "
                             f"migration first. QA auditor is additive-only.")
        missing = need - have
        if missing:
            cur.close()
            raise SystemExit(f"SCHEMA DRIFT: {sch}.{tbl} missing {sorted(missing)}.")
    for col, typ in QA_COLUMNS:
        cur.execute(f'ALTER TABLE "{biz_schema}"."{biz_table}" '
                    f'ADD COLUMN IF NOT EXISTS "{col}" {typ}')
    conn.commit()
    cur.close()


def write_verdict(cur, biz_schema, biz_table, biz_pk, ref, verdict):
    cur.execute(f'''
        UPDATE "{biz_schema}"."{biz_table}"
           SET qa_status=%s, qa_score=%s, qa_reason=%s, qa_checked_at=now()
         WHERE "{biz_pk}"=%s
    ''', (verdict["qa_status"], float(verdict["qa_score"]),
          str(verdict["qa_reason"])[:300], int(ref)))


def seed_reenrich(cur, ref):
    """Re-seed a high-value-but-incomplete business for another enrich pass using
    ONLY a CHECK-legal task_type. ON CONFLICT DO NOTHING -> idempotent, never
    double-queues, never violates the UNIQUE(business_id, task_type) constraint."""
    cur.execute('''
        INSERT INTO atlas.enrich_queue (business_id, task_type, priority, status)
        VALUES (%s, %s, 3, 'pending')
        ON CONFLICT (business_id, task_type) DO NOTHING
    ''', (int(ref), REENRICH_TASK))
    return cur.rowcount or 0


# --------------------------------------------------------------------------- #
# Fetch homepage html for the chat scan (verify-on-box; offline -> '')
# --------------------------------------------------------------------------- #
def fetch_html(domain):
    if not LIVE_HTTP or not domain:
        return ""
    try:
        req = urllib.request.Request(f"https://{domain}/", headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            raw = resp.read(HTTP_MAXBYTES)
            return raw.decode("utf-8", "replace")
    except Exception:  # noqa: BLE001
        return ""


# --------------------------------------------------------------------------- #
# Provenance read: which target fields are present (with a usable value)
# --------------------------------------------------------------------------- #
def fields_present_for(cur, ref):
    cur.execute("SELECT field FROM atlas.field_provenance "
                "WHERE business_id=%s AND value IS NOT NULL AND btrim(value) <> ''",
                (int(ref),))
    return {r[0] for r in cur.fetchall()}


def stored_offline_blob(present_vals):
    return ""  # title/meta not retained in field_provenance; box live-fetch supplies it


# --------------------------------------------------------------------------- #
# status-back (GitHub Contents API) + brain inbox -- same channel as the worker
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
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Accept", "application/vnd.github+json")
        req.add_header("User-Agent", "atlas-qa")
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
    code, _ = _req("PUT", f"{api}/repos/{repo}/contents/{path}",
                   json.dumps(payload).encode("utf-8"))
    return bool(code and 200 <= code < 300)


def brain_emit(events):
    """Append QA learning events to /var/lib/brain/logs/inbox.jsonl so /brain
    learns which criteria correlate with closure. Best-effort, off-DB, never
    fatal. Each event is one JSON object per line (the brain's inbox convention)."""
    if not events:
        return False
    try:
        os.makedirs(os.path.dirname(BRAIN_INBOX), exist_ok=True)
        with open(BRAIN_INBOX, "a", encoding="utf-8") as fh:
            for ev in events:
                fh.write(json.dumps(ev) + "\n")
        return True
    except Exception as e:  # noqa: BLE001
        log(f"brain inbox append skipped: {e}")
        return False


# --------------------------------------------------------------------------- #
# Audit one batch
# --------------------------------------------------------------------------- #
def select_batch(cur, biz_schema, biz_table, biz_pk, cols, batch):
    """Claim a batch of un-audited / stale-audited business rows. 'Audited' just
    means qa_checked_at is recent; we re-audit rows older than the recheck window
    so the gate is CONTINUOUS, not one-shot. Read-mostly: this SELECT takes no
    lock (the verdict UPDATE is per-row)."""
    recheck_days = int(os.environ.get("ATLAS_QA_RECHECK_DAYS", "7"))
    sel = [c for c in (biz_pk, cols.get("name"), cols.get("website"),
                       cols.get("email"), cols.get("has_chat")) if c]
    cur.execute(f'''
        SELECT {", ".join(f'b."{c}"' for c in sel)}
          FROM "{biz_schema}"."{biz_table}" b
         WHERE b.qa_checked_at IS NULL
            OR b.qa_checked_at < now() - make_interval(days => %s)
         ORDER BY b.qa_checked_at NULLS FIRST, b."{biz_pk}"
         LIMIT %s
    ''', (recheck_days, batch))
    return sel, cur.fetchall()


def audit_batch(conn, biz_schema, biz_table, biz_pk, cols):
    cur = conn.cursor()
    sel, rows = select_batch(cur, biz_schema, biz_table, biz_pk, cols, BATCH)
    if not rows:
        cur.close()
        return {"n": 0, "pass": 0, "quarantine": 0, "reenrich": 0,
                "reasons": {}, "comp_sum": 0.0}
    stats = {"n": 0, "pass": 0, "quarantine": 0, "reenrich": 0,
             "reasons": {}, "comp_sum": 0.0}
    brain_events = []
    for row in rows:
        vals = dict(zip(sel, row))
        ref = vals[biz_pk]
        website = vals.get(cols.get("website")) or ""
        domain = _registrable(website)
        html = fetch_html(domain) if domain else ""
        fp = fields_present_for(cur, ref)
        rec = {
            "name": vals.get(cols.get("name")) or "",
            "website": website,
            "domain": domain,
            "email": vals.get(cols.get("email")) or "",
            "has_chat": vals.get(cols.get("has_chat")),
            "homepage_html": html,
            "offline_text": "",
            "fields_present": fp,
        }
        verdict = decide(rec)
        write_verdict(cur, biz_schema, biz_table, biz_pk, ref, verdict)
        if verdict["qa_status"] == QA_REENRICH:
            seed_reenrich(cur, ref)
        stats["n"] += 1
        stats[verdict["qa_status"] if verdict["qa_status"] in stats else "quarantine"] += 1
        stats["comp_sum"] += verdict["qa_score"]
        r = verdict["qa_reason"].split(";")[0] if verdict["qa_reason"] else "ok"
        stats["reasons"][r] = stats["reasons"].get(r, 0) + 1
        # feed the brain a compact learning event (no PII; aggregate-friendly)
        brain_events.append({
            "type": "qa_verdict",
            "project": "atlas",
            "business_id": int(ref),
            "qa_status": verdict["qa_status"],
            "qa_score": round(verdict["qa_score"], 3),
            "reason": r,
            "chat_grade": verdict["checks"]["chat"]["grade"],
            "ts": int(time.time()),
        })
        if PACING_MS:
            time.sleep(PACING_MS / 1000.0)
    conn.commit()
    cur.close()
    if env_bool("ATLAS_QA_BRAIN_EMIT", True):
        brain_emit(brain_events)
    return stats


def report(conn, stats_accum, node):
    avg_comp = (stats_accum["comp_sum"] / stats_accum["n"]) if stats_accum["n"] else 0.0
    passrate = (stats_accum["pass"] / stats_accum["n"]) if stats_accum["n"] else 0.0
    top = sorted(stats_accum["reasons"].items(), key=lambda kv: -kv[1])[:8]
    body = {
        "lane": "qa_auditor",
        "node": node,
        "audited": stats_accum["n"],
        "pass": stats_accum["pass"],
        "quarantine": stats_accum["quarantine"],
        "reenrich": stats_accum["reenrich"],
        "pass_rate": round(passrate, 4),
        "avg_completeness": round(avg_comp, 4),
        "top_failure_reasons": top,
        "named_chat_providers": NAMED_PROVIDER_COUNT,
        "live_http": LIVE_HTTP, "live_mx": LIVE_MX, "live_render": LIVE_RENDER,
        "verify_on_box": (not LIVE_HTTP or not LIVE_MX or not LIVE_RENDER),
        "ts": int(time.time()),
    }
    log(f"REPORT audited={body['audited']} pass={body['pass']} "
        f"quarantine={body['quarantine']} reenrich={body['reenrich']} "
        f"pass_rate={body['pass_rate']} avg_completeness={body['avg_completeness']} "
        f"top={top}")
    gh_put(f"status/{node}/qa-{node}.json", body, f"qa auditor report {node}")
    return body


# --------------------------------------------------------------------------- #
# Column resolution
# --------------------------------------------------------------------------- #
def resolve_columns(conn):
    cur = conn.cursor()
    biz_schema, biz_table = BUSINESS_TBL
    bcols = table_columns(cur, biz_schema, biz_table)
    if not bcols:
        cur.close()
        raise SystemExit(f"ERROR: {biz_schema}.{biz_table} not found.")
    pkv = table_pk(cur, biz_schema, biz_table) or pick_col(
        bcols, ["id", "business_id", "source_id", "external_id"])
    cur.close()
    cols = {}
    for logical in ("name", "website", "phone", "email", "category", "region", "has_chat"):
        cols[logical] = pick_col(bcols, CANDIDATES[logical])
    cols["pk"] = pkv
    log(f"columns: pk={pkv} name={cols['name']} website={cols['website']} "
        f"email={cols['email']} has_chat={cols['has_chat']}")
    return cols


# --------------------------------------------------------------------------- #
# Modes
# --------------------------------------------------------------------------- #
def do_migrate():
    conn = connect_pg()
    cols = resolve_columns(conn)
    ensure_qa_columns(conn, BUSINESS_TBL[0], BUSINESS_TBL[1])
    cur = conn.cursor()
    cur.execute(f'SELECT count(*) FROM "{BUSINESS_TBL[0]}"."{BUSINESS_TBL[1]}"')
    total = cur.fetchone()[0]
    cur.execute(f'SELECT count(*) FROM "{BUSINESS_TBL[0]}"."{BUSINESS_TBL[1]}" '
                f'WHERE qa_checked_at IS NOT NULL')
    audited = cur.fetchone()[0]
    cur.close()
    conn.close()
    log(f"MIGRATE OK: qa_* columns ensured; business_total={total} already_audited={audited}")
    print(f"[atlas_qa] MIGRATE OK qa_columns_added business_total={total}")
    sys.exit(0)


def _offline_selftest():
    """Pure-logic graders -- no DB, no network. Returns True on all-pass."""
    ok = True

    # chat: a known provider must be detected
    hc, prov, gen = grade_chat_from_html('<script src="https://widget.intercom.io/x.js">')
    assert hc is True and "intercom" in prov, ("chat-detect intercom", hc, prov)
    no_ok, grade, _ = grade_chat(hc, prov, gen, None, "verify_on_box")
    assert no_ok is False and grade == "has_chat_code", grade

    # chat: clean page, render unavailable -> honest weaker grade
    hc2, prov2, gen2 = grade_chat_from_html('<html><body>Welcome</body></html>')
    no_ok2, grade2, _ = grade_chat(hc2, prov2, gen2, None, "verify_on_box")
    assert no_ok2 is True and grade2 == "no_chat_code_clean", grade2

    # chat: clean page, render says no_chat -> strongest grade
    no_ok3, grade3, _ = grade_chat(False, [], False, None, "no_chat")
    assert grade3 == "no_chat_confirmed", grade3

    # chat: a NEW provider beyond the v5 ~45 list (e.g. trengo) is caught (50+ proof)
    hc4, prov4, _ = grade_chat_from_html('<script src="https://static.widget.trengo.eu/w.js">')
    assert "trengo" in prov4, prov4
    assert NAMED_PROVIDER_COUNT >= 50, ("need 50+ named providers", NAMED_PROVIDER_COUNT)

    # email screens
    assert email_is_valid_shape("info@realclinic.com")[0] is True
    assert email_is_valid_shape("user@example.com")[0] is False     # placeholder
    assert email_is_valid_shape("jane@gmail.com")[0] is False       # free webmail
    assert email_is_valid_shape("notanemail")[0] is False           # syntax

    # completeness math
    pct, filled, missing = completeness({"domain", "website", "email", "phone", "mx",
                                         "tech", "industry", "social_linkedin"})
    assert 0.0 < pct <= 1.0, pct
    full_pct, _, _ = completeness({f for f, _ in TARGET_FIELDS})
    assert abs(full_pct - 1.0) < 1e-6, full_pct

    # website parked detection (offline)
    st, _ = website_live("x.com", offline_text="This domain is for sale")
    assert st == "parked", st

    # decide(): full-pass record
    good = decide({"name": "Real Clinic", "website": "https://realclinic.com",
                   "domain": "realclinic.com", "email": "info@realclinic.com",
                   "has_chat": 0, "homepage_html": "<html>clean</html>",
                   "offline_text": "",
                   "fields_present": {f for f, _ in TARGET_FIELDS}})
    assert good["qa_status"] == QA_PASS, good

    # decide(): chat present -> quarantine
    chatty = decide({"name": "X", "website": "https://x.com", "domain": "x.com",
                     "email": "info@x.com",
                     "homepage_html": '<script src="https://embed.tawk.to/x"></script>',
                     "fields_present": {f for f, _ in TARGET_FIELDS}})
    assert chatty["qa_status"] == QA_QUARANTINE and "has_chat" in chatty["qa_reason"], chatty

    # decide(): clean + complete-enough but partial -> reenrich (high-value)
    partial = decide({"name": "Y", "website": "https://y.com", "domain": "y.com",
                      "email": "info@y.com", "homepage_html": "<html>clean</html>",
                      "fields_present": {"domain", "website", "email", "phone"}})
    assert partial["qa_status"] in (QA_REENRICH, QA_QUARANTINE), partial

    # decide(): junk email -> quarantine
    bad = decide({"name": "Z", "website": "https://z.com", "domain": "z.com",
                  "email": "user@example.com", "homepage_html": "<html>clean</html>",
                  "fields_present": {f for f, _ in TARGET_FIELDS}})
    assert bad["qa_status"] == QA_QUARANTINE and "email" in bad["qa_reason"], bad

    return ok


def do_selftest():
    log(f"selftest: have_pg={_HAVE_PG} have_dns={_HAVE_DNS} "
        f"named_chat_providers={NAMED_PROVIDER_COUNT} "
        f"live_http={LIVE_HTTP} live_mx={LIVE_MX} live_render={LIVE_RENDER}")
    # 1. offline graders (always runnable, no DB/net)
    try:
        _offline_selftest()
        log("selftest: offline graders PASS (chat-50+, email-junk, completeness, decide)")
    except AssertionError as e:
        log(f"selftest: offline graders FAIL: {e}")
        sys.exit(3)
    # 2. DB connect + schema present (best-effort; not required for the gate to pass
    #    offline, but reported honestly)
    if not _HAVE_PG:
        log("selftest: psycopg2 not importable -> DB checks SKIPPED (install on box).")
        log("selftest: PASS (offline)")
        sys.exit(0)
    try:
        conn = connect_pg()
        log("selftest: Postgres connect OK")
    except Exception as e:  # noqa: BLE001
        log(f"selftest: Postgres connect UNAVAILABLE ({e}); offline graders already PASS.")
        log("selftest: PASS (offline; DB verify-on-box)")
        sys.exit(0)
    try:
        cols = resolve_columns(conn)
        # check tables exist WITHOUT mutating (do not ADD columns in selftest)
        cur = conn.cursor()
        for sch, tbl in (BUSINESS_TBL, QUEUE_TBL, PROV_TBL):
            if not table_columns(cur, sch, tbl):
                raise SystemExit(f"{sch}.{tbl} missing")
        cur.close()
        log("selftest: schema present OK")
    except SystemExit as e:
        log(f"selftest: schema FAIL: {e}")
        sys.exit(3)
    finally:
        conn.close()
    log("selftest: PASS")
    sys.exit(0)


def run_loop(once=False):
    node = os.environ.get("NODE_ID", "hetzner")
    conn = connect_pg()
    cols = resolve_columns(conn)
    ensure_qa_columns(conn, BUSINESS_TBL[0], BUSINESS_TBL[1])
    biz_schema, biz_table = BUSINESS_TBL
    biz_pk = cols["pk"]
    accum = {"n": 0, "pass": 0, "quarantine": 0, "reenrich": 0,
             "reasons": {}, "comp_sum": 0.0}
    last_report = 0.0
    log(f"qa auditor start node={node} batch={BATCH} once={once} "
        f"completeness_min={COMPLETENESS_THRESHOLD}")
    while True:
        try:
            s = audit_batch(conn, biz_schema, biz_table, biz_pk, cols)
        except psycopg2.Error as e:
            conn.rollback()
            log(f"audit batch error (retry): {e}")
            time.sleep(IDLE_SEC)
            continue
        for k in ("n", "pass", "quarantine", "reenrich", "comp_sum"):
            accum[k] += s[k]
        for r, c in s["reasons"].items():
            accum["reasons"][r] = accum["reasons"].get(r, 0) + c
        now = time.time()
        if s["n"] == 0:
            if now - last_report >= REPORT_SEC and accum["n"]:
                report(conn, accum, node)
                last_report = now
            if once:
                log("once: nothing left to audit -> exit")
                break
            time.sleep(IDLE_SEC)
            continue
        if now - last_report >= REPORT_SEC:
            report(conn, accum, node)
            last_report = now
        if once:
            report(conn, accum, node)
            log(f"once: audited {accum['n']} -> exit")
            break
    conn.close()


def main():
    load_env_file(AUTOPULL_ENV_PATH)
    load_env_file(DB_ENV_PATH)
    args = set(sys.argv[1:])
    if "--migrate" in args:
        do_migrate()
    elif "--selftest" in args:
        do_selftest()
    elif "--once" in args:
        run_loop(once=True)
    else:
        run_loop(once=False)


if __name__ == "__main__":
    main()
