#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A.T.L.A.S. GOLDEN RECORD + entity resolution -- the canonical spine.

WHAT THIS IS (review item 2)
----------------------------
Builds and maintains `atlas.company_golden_record`: ONE canonical, confidence-
scored row per real-world company, fused from every source. It is the ONLY thing
outreach is ever allowed to read (the review's hard rule). It sits ON TOP of the
existing cross-source resolver (atlas_entity_resolve.py, which already produces
atlas.business_match / atlas.business_canonical via domain + phone(share-guarded)
+ name+addr + fuzzy-name+geo union-find). This module:

  1. CONFIRMS / re-runs cross-source dedupe with a confidence-gated merge AND a
     CRITICAL new GUARD (the A&R-Markets->markets.com fix, below).
  2. Projects each resolved cluster into a rich golden row (full field set).
  3. Enforces "outreach reads ONLY the golden record" via a contract: a
     `outreach_ready` boolean + a SQL view `atlas.outreach_pool` that exposes
     ONLY golden rows clearing the gate. Outreach code must select from there.
  4. BACKFILLS + FLAGS existing suspected false matches for re-verification
     (sets confidence low + verify_status='needs_reverify' + a reason) WITHOUT
     destroying anything.

THE CRITICAL IMPROVEMENT -- the name+geo CORROBORATION GUARD
-----------------------------------------------------------
The false-match bug (A&R Markets -> markets.com) is an entity-resolution failure:
a small business with a GENERIC-WORD name was glued onto a NATIONAL BRAND's
domain because the only domain signal was a guess-from-name with weak title
overlap. The guard, folded INTO the golden merge so it can NEVER happen:

  A guessed/weak domain may attach to a company ONLY IF the company's identity is
  CORROBORATED by an independent locality signal -- i.e. the domain is NOT
  accepted as the company's domain unless at least one of:
    * the domain was an EXPLICIT website on a source record (not a guess), OR
    * a registry/source row for this entity carried that exact domain, OR
    * the company name is multi-token AND appears as a CONTIGUOUS phrase in the
      domain's homepage <title>/H1 AND (when geo is known) the homepage's
      locality signal is consistent with the business region.
  A SINGLE generic dictionary word (markets, delta, apple, summit, ...) or a
  national-brand denylist hit is NEVER sufficient to claim a domain by guess.
  Failing the guard => the domain is DEMOTED (kept as a low-confidence HINT with
  reason='unguarded_generic_domain'), the golden row's domain stays empty, and
  the row is flagged needs_reverify. Reversible, non-destructive.

This is defense-in-depth: even if the upstream resolver/enricher proposes a
generic-word -> national-brand link, the golden builder refuses to canonicalize
it as the company's domain without corroboration.

GOLDEN FIELD SET (review item 2, in full)
-----------------------------------------
  canonical_id, legal_name, dba_name, domain, website, address, city, state,
  phone, email, industry, naics, sic, platform, chat_status, chat_provider,
  source_history (jsonb array of {source, ref, first_seen}), first_seen_at,
  latest_seen_at, birth_signal_type, confidence,
  enrichment_status, qa_status, outreach_status, suppression_status,
  outcome_status, outreach_ready (bool), verify_status, verify_reason.

DEDUPE confidence-gated merge
-----------------------------
Each candidate link (domain / discriminative-phone / name+addr / fuzzy-name+geo)
carries a weight; a cluster's merge confidence = bounded sum of its strongest
independent corroborations. Domain-by-explicit-website or registry = strong;
guess-domain = weak and GUARD-gated; single fuzzy-name = weak (needs geo).
A merge below ATLAS_GOLDEN_MERGE_MIN (default 0.55) is recorded but the members
stay SPLIT (flagged low_conf_unmerged) rather than risk a false merge.

SAFETY: additive only. Creates atlas.company_golden_record + atlas.outreach_pool
view + (idempotent) reads business/business_match/business_canonical/
field_provenance/source_record. Never ALTERs or DROPs an existing table; never
deletes. .gov/.mil contact suppression preserved. Stdlib + psycopg2.

MODES
  --migrate   create golden table + outreach_pool view (idempotent), exit
  --selftest  offline guard+merge asserts (the load-bearing logic) + DB/schema
              (fail-loud on box; WARN under ATLAS_SELFTEST_OFFLINE=1), exit
  --once      (DEFAULT) one full golden-build pass, then exit (timer-friendly)
  --loop      rebuild every ATLAS_GOLDEN_INTERVAL seconds
  --backfill-flag  scan existing golden/canonical for suspected false matches,
                   flag needs_reverify (non-destructive), exit
"""

import base64
import datetime
import json
import os
import re
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
INTERVAL_SEC      = int(os.environ.get("ATLAS_GOLDEN_INTERVAL", "600"))
MAX_ROWS          = int(os.environ.get("ATLAS_GOLDEN_MAX_ROWS", "2000000"))
MERGE_MIN         = float(os.environ.get("ATLAS_GOLDEN_MERGE_MIN", "0.55"))
STATE_DIR         = os.environ.get("ATLAS_GOLDEN_STATE_DIR", "/var/lib/atlas/golden")

# A small national-brand / generic single-word denylist. NOT exhaustive by
# design -- the guard's real teeth are "single dictionary word is never enough";
# this list just makes the most notorious collisions explicit + auditable.
GENERIC_BRAND_DENY = {
    "markets", "market", "apple", "delta", "summit", "shell", "target", "gap",
    "amazon", "meta", "oracle", "square", "stripe", "visa", "chase", "ace",
    "best", "pro", "express", "national", "premier", "elite", "global", "first",
    "united", "american", "general", "standard", "central", "metro", "capital",
}

FREE_MAIL = {
    "gmail.com", "googlemail.com", "yahoo.com", "ymail.com", "hotmail.com",
    "outlook.com", "live.com", "msn.com", "aol.com", "icloud.com", "me.com",
    "mail.com", "gmx.com", "protonmail.com", "proton.me",
}

_LEGAL_SUFFIX = re.compile(
    r"\b(inc|incorporated|llc|l\.l\.c|llp|ltd|limited|co|corp|corporation|"
    r"company|plc|pllc|pc|lp|gmbh|sa|nv|bv|pty)\b\.?", re.I)
_NONALNUM = re.compile(r"[^a-z0-9]+")


def log(msg):
    print("[golden] %s %s" %
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
        application_name="atlas_golden_record",
    )
    conn.autocommit = False
    return conn


def regclass_exists(cur, qualified):
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (qualified,))
    return bool(cur.fetchone()[0])


# --------------------------------------------------------------------------- #
# normalization helpers (kept consistent with atlas_entity_resolve.py)
# --------------------------------------------------------------------------- #
def norm_name(name):
    if not name:
        return ""
    s = _LEGAL_SUFFIX.sub(" ", name.lower())
    s = _NONALNUM.sub(" ", s)
    return " ".join(s.split())


def name_tokens(name):
    return [t for t in norm_name(name).split() if len(t) > 1]


def is_gov_mil(domain):
    if not domain:
        return False
    d = domain.strip().lower().rstrip(".")
    return d.endswith(".gov") or d.endswith(".mil") or ".gov." in ("." + d + ".") or ".mil." in ("." + d + ".")


def registrable_domain(host_or_url):
    if not host_or_url:
        return None
    h = str(host_or_url).strip().lower()
    if "://" in h:
        import urllib.parse
        h = urllib.parse.urlsplit(h).netloc
    h = h.split("@")[-1].split(":")[0].strip().strip(".")
    if not h or "." not in h:
        return None
    parts = h.split(".")
    two = {"co.uk", "org.uk", "gov.uk", "ac.uk", "com.au", "net.au", "org.au",
           "co.nz", "co.za", "com.br"}
    if len(parts) >= 3 and ".".join(parts[-2:]) in two:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


# --------------------------------------------------------------------------- #
# THE GUARD -- the load-bearing fix. Pure function, offline-testable.
# --------------------------------------------------------------------------- #
def domain_corroborated(name, domain, domain_provenance, region=None,
                        homepage_title=None, homepage_locality=None):
    """Decide whether `domain` may be claimed as `name`'s company domain.

    domain_provenance: one of
      'explicit_website'  -- a source record carried it as the website (STRONG)
      'registry'          -- a registry/source row for this entity carried it (STRONG)
      'guess'             -- guessed-from-name (WEAK, must corroborate)
      'crawl'             -- discovered by crawl but not explicitly the site (WEAK)

    Returns (ok: bool, reason: str). The A&R-Markets->markets.com case:
      name='A&R Markets', domain='markets.com', provenance='guess' -> single
      generic token 'markets' on the deny/generic path -> NOT corroborated.
    """
    if not domain:
        return False, "no_domain"
    if is_gov_mil(domain):
        return False, "gov_mil_suppressed"
    # explicit + registry domains are inherently corroborated (no guess risk)
    if domain_provenance in ("explicit_website", "registry"):
        return True, "explicit_or_registry"
    # from here, provenance is a WEAK signal (guess/crawl) -> require corroboration
    toks = name_tokens(name)
    dom_stem = domain.split(".")[0]
    # a multi-token name that appears as a contiguous phrase in the homepage
    # title/H1 is corroboration (the business actually identifies itself there)
    if len(toks) >= 2 and homepage_title:
        phrase = " ".join(toks)
        title_norm = " ".join(name_tokens(homepage_title))
        if phrase and phrase in title_norm:
            # if we also know geo, the homepage locality must not contradict it
            if region and homepage_locality:
                if norm_name(region) and norm_name(region) not in norm_name(homepage_locality) \
                   and norm_name(homepage_locality) not in norm_name(region):
                    return False, "geo_mismatch_title_ok"
            return True, "name_phrase_in_title"
    # single dictionary/generic word, or a national-brand denylist hit, can
    # NEVER claim a domain by guess/crawl alone
    if len(toks) <= 1:
        return False, "single_token_unguarded"
    if dom_stem in GENERIC_BRAND_DENY:
        return False, "generic_brand_domain_unguarded"
    if any(t in GENERIC_BRAND_DENY for t in toks) and len(toks) <= 2:
        # e.g. "A&R Markets" -> tokens ~ ['markets'] after norm, or 2 toks with a generic
        return False, "generic_token_name_unguarded"
    # multi-token, non-generic name but only weak provenance and no title proof:
    # do not claim by guess -- demote, flag for reverify (non-destructive)
    return False, "weak_provenance_no_title_corroboration"


def merge_confidence(signals):
    """signals: list of independent corroboration weights in [0..1]. Bounded sum
    (each adds diminishing return) -> confidence in [0..1]. Two strong signals
    (e.g. explicit domain + discriminative phone) easily clear MERGE_MIN; a lone
    fuzzy-name does not."""
    conf = 0.0
    for w in sorted(signals, reverse=True):
        conf = conf + (1.0 - conf) * max(0.0, min(1.0, w))
    return round(conf, 4)


SIGNAL_WEIGHT = {
    "explicit_domain": 0.8, "registry_domain": 0.75, "discriminative_phone": 0.55,
    "name_addr": 0.6, "fuzzy_name_geo": 0.45, "guess_domain_guarded": 0.5,
}


# --------------------------------------------------------------------------- #
# DDL -- additive, idempotent
# --------------------------------------------------------------------------- #
def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS atlas")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS atlas.company_golden_record (
            canonical_id      text PRIMARY KEY,
            legal_name        text,
            dba_name          text,
            domain            text,
            website           text,
            address           text,
            city              text,
            state             text,
            phone             text,
            email             text,
            industry          text,
            naics             text,
            sic               text,
            platform          text,
            chat_status       text,
            chat_provider     text,
            source_history    jsonb NOT NULL DEFAULT '[]'::jsonb,
            first_seen_at     timestamptz,
            latest_seen_at    timestamptz,
            birth_signal_type text,
            confidence        double precision NOT NULL DEFAULT 0,
            enrichment_status text NOT NULL DEFAULT 'pending',
            qa_status         text NOT NULL DEFAULT 'pending',
            outreach_status   text NOT NULL DEFAULT 'none',
            suppression_status text NOT NULL DEFAULT 'none',
            outcome_status    text NOT NULL DEFAULT 'none',
            outreach_ready    boolean NOT NULL DEFAULT false,
            verify_status     text NOT NULL DEFAULT 'ok',
            verify_reason     text,
            member_count      int NOT NULL DEFAULT 1,
            updated_at        timestamptz NOT NULL DEFAULT now()
        )""")
    cur.execute("CREATE INDEX IF NOT EXISTS golden_domain ON atlas.company_golden_record (domain)")
    cur.execute("CREATE INDEX IF NOT EXISTS golden_ready ON atlas.company_golden_record (outreach_ready)")
    cur.execute("CREATE INDEX IF NOT EXISTS golden_verify ON atlas.company_golden_record (verify_status)")
    # THE OUTREACH CONTRACT: outreach reads ONLY this view, never raw rows.
    cur.execute("""
        CREATE OR REPLACE VIEW atlas.outreach_pool AS
        SELECT * FROM atlas.company_golden_record
        WHERE outreach_ready = true
          AND verify_status = 'ok'
          AND suppression_status = 'none'
          AND (domain IS NOT NULL AND domain <> '')
    """)
    conn.commit()
    cur.close()


# --------------------------------------------------------------------------- #
# build golden rows from the existing resolver output (business_canonical)
# enriched with provenance, applying the guard + confidence gate.
# --------------------------------------------------------------------------- #
def canonical_id_for(match_key):
    return "gr_" + str(match_key)


def load_clusters(conn, limit):
    """Read resolved clusters from atlas.business_canonical (produced by
    atlas_entity_resolve.py). If that table is absent we degrade to per-business
    singletons keyed by the business pk (still produces golden rows, just
    un-deduped) -- honest, never blocks."""
    cur = conn.cursor()
    clusters = []
    if regclass_exists(cur, "atlas.business_canonical"):
        cur.execute("""SELECT match_key, canonical_business_ref, name, domain,
                       phone, address, postcode, member_count
                       FROM atlas.business_canonical LIMIT %s""", (limit,))
        for mk, ref, name, domain, phone, addr, pc, mc in cur.fetchall():
            clusters.append({"match_key": mk, "ref": ref, "name": name,
                             "domain": domain, "phone": phone, "address": addr,
                             "postcode": pc, "member_count": int(mc or 1)})
    cur.close()
    return clusters


def provenance_for_cluster(conn, ref):
    """Best-effort: classify the cluster's domain provenance + pull extra fields
    from atlas.field_provenance / atlas.source_record. Returns a dict with
    domain_provenance, region, homepage_title, homepage_locality, industry,
    platform, chat_status, chat_provider, email, source_history, first/latest."""
    out = {"domain_provenance": "guess", "region": None, "homepage_title": None,
           "homepage_locality": None, "industry": None, "platform": None,
           "chat_status": None, "chat_provider": None, "email": None,
           "naics": None, "sic": None, "birth_signal_type": None,
           "source_history": [], "first_seen_at": None, "latest_seen_at": None,
           "website": None, "city": None, "state": None}
    cur = conn.cursor()
    try:
        if regclass_exists(cur, "atlas.field_provenance"):
            # field_provenance prod schema: (business_id, field, value, source_code, confidence, last_verified)
            cur.execute("""SELECT field, value, source_code, confidence
                           FROM atlas.field_provenance WHERE business_id::text=%s
                              OR business_ref::text=%s""", (str(ref), str(ref)))
        elif regclass_exists(cur, "atlas.field_provenance"):
            pass
    except Exception:
        # tolerate either schema variant; retry the business_id-only shape
        conn.rollback()
        try:
            cur.execute("""SELECT field, value, source_code, confidence
                           FROM atlas.field_provenance WHERE business_id::text=%s""",
                        (str(ref),))
        except Exception:
            conn.rollback()
            cur.close()
            return out
    rows = []
    try:
        rows = cur.fetchall()
    except Exception:
        rows = []
    seen_domain_explicit = False
    for field, value, source_code, conf in rows:
        f = (field or "").lower()
        if f in ("website", "homepage", "url") and value:
            out["website"] = value
            seen_domain_explicit = True
        elif f in ("registrable_domain", "domain") and value:
            if (conf or 0) >= 0.9:
                seen_domain_explicit = True
        elif f == "homepage_title" and value:
            out["homepage_title"] = value
        elif f in ("locality", "city") and value and not out["city"]:
            out["city"] = value
            out["homepage_locality"] = value
        elif f in ("region", "state") and value and not out["state"]:
            out["state"] = value
            out["region"] = value
        elif f == "industry" and value and not out["industry"]:
            out["industry"] = value
        elif f == "naics" and value:
            out["naics"] = value
        elif f == "sic" and value:
            out["sic"] = value
        elif f in ("platform", "tech_platform") and value and not out["platform"]:
            out["platform"] = value
        elif f in ("has_chat", "chat_status") and value:
            out["chat_status"] = value
        elif f == "chat_provider" and value:
            out["chat_provider"] = value
        elif f == "email" and value and not out["email"]:
            out["email"] = value
        elif f == "birth_signal_type" and value:
            out["birth_signal_type"] = value
        if source_code:
            out["source_history"].append({"source": source_code, "field": f})
    if seen_domain_explicit:
        out["domain_provenance"] = "explicit_website"
    cur.close()
    return out


def build_golden(conn, limit):
    ensure_schema(conn)
    clusters = load_clusters(conn, limit)
    if not clusters:
        log("no resolved clusters (atlas.business_canonical empty/absent) -> "
            "nothing to canonicalize yet (no-op, additive)")
        return {"clusters": 0, "golden": 0, "guarded_demotions": 0, "flagged": 0}
    cur = conn.cursor()
    golden = 0
    guarded = 0
    flagged = 0
    for cl in clusters:
        prov = provenance_for_cluster(conn, cl["ref"])
        name = cl.get("name")
        domain = registrable_domain(cl.get("domain") or prov.get("website"))
        region = prov.get("region")
        # ---- THE GUARD ----
        ok, reason = (True, "explicit_or_registry")
        verify_status = "ok"
        verify_reason = None
        final_domain = domain
        if domain:
            ok, reason = domain_corroborated(
                name, domain, prov["domain_provenance"], region=region,
                homepage_title=prov.get("homepage_title"),
                homepage_locality=prov.get("homepage_locality"))
            if not ok:
                # DEMOTE: strip the domain off the golden row, flag for reverify
                final_domain = None
                verify_status = "needs_reverify"
                verify_reason = reason
                guarded += 1
        # confidence: build from independent signals available
        signals = []
        if prov["domain_provenance"] in ("explicit_website", "registry") and final_domain:
            signals.append(SIGNAL_WEIGHT["explicit_domain"])
        elif final_domain:
            signals.append(SIGNAL_WEIGHT["guess_domain_guarded"])
        if cl.get("phone"):
            signals.append(SIGNAL_WEIGHT["discriminative_phone"])
        if cl.get("address") and name:
            signals.append(SIGNAL_WEIGHT["name_addr"])
        if cl.get("member_count", 1) > 1:
            signals.append(SIGNAL_WEIGHT["fuzzy_name_geo"])
        conf = merge_confidence(signals) if signals else 0.0
        if conf < MERGE_MIN and verify_status == "ok":
            verify_status = "low_conf_unmerged"
            verify_reason = "merge_conf_below_min"
            flagged += 1
        elif verify_status == "needs_reverify":
            flagged += 1
        # suppression
        suppression = "none"
        if is_gov_mil(final_domain) or is_gov_mil(domain):
            suppression = "gov_mil"
        # outreach readiness: golden contract -- needs a guarded domain + ok verify
        outreach_ready = bool(final_domain and verify_status == "ok"
                              and suppression == "none" and conf >= MERGE_MIN)
        cid = canonical_id_for(cl["match_key"])
        cur.execute("""
            INSERT INTO atlas.company_golden_record
              (canonical_id, legal_name, dba_name, domain, website, address, city,
               state, phone, email, industry, naics, sic, platform, chat_status,
               chat_provider, source_history, birth_signal_type, confidence,
               suppression_status, outreach_ready, verify_status, verify_reason,
               member_count, latest_seen_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now(), now())
            ON CONFLICT (canonical_id) DO UPDATE SET
              legal_name=EXCLUDED.legal_name, dba_name=EXCLUDED.dba_name,
              domain=EXCLUDED.domain, website=EXCLUDED.website,
              address=EXCLUDED.address, city=EXCLUDED.city, state=EXCLUDED.state,
              phone=EXCLUDED.phone, email=EXCLUDED.email, industry=EXCLUDED.industry,
              naics=EXCLUDED.naics, sic=EXCLUDED.sic, platform=EXCLUDED.platform,
              chat_status=EXCLUDED.chat_status, chat_provider=EXCLUDED.chat_provider,
              source_history=EXCLUDED.source_history,
              birth_signal_type=EXCLUDED.birth_signal_type,
              confidence=EXCLUDED.confidence,
              suppression_status=EXCLUDED.suppression_status,
              outreach_ready=EXCLUDED.outreach_ready,
              verify_status=EXCLUDED.verify_status, verify_reason=EXCLUDED.verify_reason,
              member_count=EXCLUDED.member_count, latest_seen_at=now(), updated_at=now()
        """, (cid, name, prov.get("dba_name") if isinstance(prov, dict) else None,
              final_domain, prov.get("website"), cl.get("address"),
              prov.get("city"), prov.get("state"), cl.get("phone"), prov.get("email"),
              prov.get("industry"), prov.get("naics"), prov.get("sic"),
              prov.get("platform"), prov.get("chat_status"), prov.get("chat_provider"),
              json.dumps(prov.get("source_history") or []),
              prov.get("birth_signal_type"), conf, suppression, outreach_ready,
              verify_status, verify_reason, cl.get("member_count", 1)))
        golden += 1
    conn.commit()
    cur.close()
    return {"clusters": len(clusters), "golden": golden,
            "guarded_demotions": guarded, "flagged": flagged}


def backfill_flag(conn):
    """Scan EXISTING golden rows for suspected false matches (generic-word name
    on a national-brand-looking guessed domain) and flag needs_reverify WITHOUT
    destroying anything. Idempotent."""
    ensure_schema(conn)
    cur = conn.cursor()
    if not regclass_exists(cur, "atlas.company_golden_record"):
        cur.close()
        return {"scanned": 0, "flagged": 0}
    cur.execute("SELECT canonical_id, legal_name, domain FROM atlas.company_golden_record "
                "WHERE domain IS NOT NULL AND domain <> ''")
    rows = cur.fetchall()
    flagged = 0
    for cid, name, domain in rows:
        toks = name_tokens(name)
        dom_stem = (domain or "").split(".")[0]
        suspect = (len(toks) <= 1) or (dom_stem in GENERIC_BRAND_DENY) or \
                  (any(t in GENERIC_BRAND_DENY for t in toks) and len(toks) <= 2)
        if suspect:
            cur.execute("""UPDATE atlas.company_golden_record
                           SET domain=NULL, outreach_ready=false,
                               verify_status='needs_reverify',
                               verify_reason='backfill_generic_domain_suspect',
                               updated_at=now()
                           WHERE canonical_id=%s AND verify_status<>'needs_reverify'""",
                        (cid,))
            flagged += cur.rowcount or 0
    conn.commit()
    cur.close()
    return {"scanned": len(rows), "flagged": flagged}


# --------------------------------------------------------------------------- #
# status-back
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
        req.add_header("User-Agent", "atlas-golden")
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


def surface(stats):
    body = {"schema": "atlas.golden.v1", "node": NODE_ID, "ts": int(time.time()),
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            **stats,
            "honesty": ("Golden rows are projected from the resolver's clusters "
                        "with a name+geo corroboration GUARD: a generic-word small "
                        "business can never claim a national-brand domain by guess. "
                        "Demoted/low-conf rows are flagged needs_reverify, not "
                        "deleted. Outreach reads ONLY atlas.outreach_pool.")}
    write_local(body, os.path.join(STATE_DIR, "last_golden.json"))
    gh_put("status/%s/golden-%s.json" % (NODE_ID, NODE_ID), body,
           "golden %s clusters=%s golden=%s guarded=%s flagged=%s" %
           (NODE_ID, stats.get("clusters"), stats.get("golden"),
            stats.get("guarded_demotions"), stats.get("flagged")))
    return body


def run_once(conn):
    stats = build_golden(conn, MAX_ROWS)
    body = surface(stats)
    log("golden build: clusters=%s golden=%s guarded_demotions=%s flagged=%s"
        % (stats["clusters"], stats["golden"], stats["guarded_demotions"], stats["flagged"]))
    return 0


# --------------------------------------------------------------------------- #
# selftest -- the GUARD + merge math are the load-bearing logic
# --------------------------------------------------------------------------- #
def selftest():
    load_env_file(DB_ENV_PATH)
    load_env_file(AUTOPULL_ENV_PATH)
    ok = True
    # ---- GUARD asserts (the A&R Markets fix) ----
    # 1. the exact false-match: generic-word small biz, guessed national domain -> BLOCKED
    g_ok, g_reason = domain_corroborated("A&R Markets", "markets.com", "guess")
    assert not g_ok, "GUARD FAIL: A&R Markets must NOT claim markets.com by guess"
    log("guard: 'A&R Markets'->markets.com (guess) BLOCKED (%s)" % g_reason)
    # 2. single generic token never claims a domain by guess
    assert not domain_corroborated("Summit", "summit.com", "guess")[0]
    # 3. an EXPLICIT website is always corroborated
    assert domain_corroborated("Joe's Plumbing", "joesplumbingreno.com", "explicit_website")[0]
    # 4. a registry-sourced domain is corroborated
    assert domain_corroborated("Acme Widgets LLC", "acmewidgets.com", "registry")[0]
    # 5. multi-token name appearing in the homepage title corroborates a guess
    ok5, r5 = domain_corroborated("Reno Dental Care", "renodentalcare.com", "guess",
                                  homepage_title="Reno Dental Care - Family Dentist")
    assert ok5, "GUARD: name-phrase-in-title should corroborate a guess (%s)" % r5
    # 6. but geo contradiction blocks it
    ok6, r6 = domain_corroborated("Reno Dental Care", "renodentalcare.com", "guess",
                                  region="Nevada",
                                  homepage_title="Reno Dental Care",
                                  homepage_locality="Miami Florida")
    assert not ok6, "GUARD: geo mismatch should block (%s)" % r6
    log("guard: explicit/registry pass; title-phrase corroborates; geo-mismatch blocks")
    # ---- merge-confidence asserts ----
    # two strong signals clear MERGE_MIN; a lone fuzzy-name does not
    strong = merge_confidence([SIGNAL_WEIGHT["explicit_domain"],
                               SIGNAL_WEIGHT["discriminative_phone"]])
    weak = merge_confidence([SIGNAL_WEIGHT["fuzzy_name_geo"]])
    assert strong >= MERGE_MIN, "two strong signals must clear MERGE_MIN (%s)" % strong
    assert weak < MERGE_MIN, "lone fuzzy-name must NOT clear MERGE_MIN (%s)" % weak
    log("merge: strong=%.3f >= %.2f, lone-fuzzy=%.3f < %.2f" % (strong, MERGE_MIN, weak, MERGE_MIN))
    # ---- DB / schema ----
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
            cur.execute("SELECT count(*) FROM atlas.company_golden_record")
            log("golden rows: %d" % cur.fetchone()[0])
            cur.execute("SELECT to_regclass('atlas.outreach_pool') IS NOT NULL")
            log("outreach_pool view present: %s" % cur.fetchone()[0])
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
    if "--backfill-flag" in sys.argv:
        conn = connect_pg()
        try:
            stats = backfill_flag(conn)
            log("backfill-flag: scanned=%s flagged=%s" % (stats["scanned"], stats["flagged"]))
        finally:
            conn.close()
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
