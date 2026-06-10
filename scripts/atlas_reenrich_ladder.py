#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ATLAS tiered re-enrichment ladder orchestrator -- NEVER lose a company.
Adds enrich_tier/reenrich_attempts to atlas.business and escalates records
Tier1->2->3 toward ZoomInfo/Apollo 20+ field parity. A record only 'fails' after
Tier 3, then is re-tried periodically (never permanently dropped).

TIERS:
  1 standard : has a findable website but missing website/email/phone -> requeue re-crawl
  2 deep     : has a live site + standard fields but THIN (missing tech/social/industry/
               size/naics) -> requeue a DEEP pass (contact/team/about + JSON-LD + registry email)
  3 inference: NO findable website after >=2 attempts, OR exhausted Tier1+2 -> $0/local
               inference of industry/size/description from name+category (done HERE, no crawl);
               paid AI only with operator go (flag, never auto-spend).
CHURN FIX: records with no findable website do NOT loop Tier 1 forever -- they escalate to
Tier 3 and get local inference instead.

--migrate  : add columns (fail-soft, IF NOT EXISTS)
--selftest : classifier logic only, no DB
default    : one orchestration pass (migrate-safe), publishes status/<node>/reenrich-ladder.json
"""
import os, sys, json, time, base64, re, urllib.request, urllib.error
sys.path.insert(0, "/opt/atlas/importers")
NODE = os.environ.get("NODE_ID", "hetzner")
BATCH = int(os.environ.get("REENRICH_BATCH", "50000"))
MAX_ATTEMPTS_T1 = int(os.environ.get("REENRICH_MAX_T1", "2"))

# $0 local industry inference (Tier 3) -- name/category keyword -> industry
IND_MAP = [
    (r"restaurant|cafe|coffee|pizza|grill|bakery|deli|food|catering", "Food & Beverage"),
    (r"salon|barber|spa|beauty|nail|hair", "Personal Care"),
    (r"law|attorney|legal|llp", "Legal Services"),
    (r"clinic|medical|dental|health|pharmacy|care|therapy", "Healthcare"),
    (r"construct|contractor|plumb|electric|roofing|hvac|builder", "Construction"),
    (r"realty|real estate|property|realtor|mortgage", "Real Estate"),
    (r"auto|motor|car|tire|vehicle|garage", "Automotive"),
    (r"consult|advisory|services|solutions|management", "Professional Services"),
    (r"retail|store|shop|market|boutique|mart", "Retail"),
    (r"bank|financ|insurance|capital|invest|credit", "Finance & Insurance"),
    (r"tech|software|data|digital|systems|cyber|cloud|app", "Technology"),
    (r"transport|logistics|freight|trucking|shipping", "Transportation & Logistics"),
    (r"school|academy|education|training|college|tutor", "Education"),
    (r"nonprofit|foundation|charity|church|ministry|association", "Nonprofit"),
    (r"manufactur|factory|industrial|fabrication", "Manufacturing"),
]

def infer_industry(name, category):
    blob = ("%s %s" % (name or "", category or "")).lower()
    for rx, ind in IND_MAP:
        if re.search(rx, blob):
            return ind
    return None

def classify_tier(rec):
    """rec: dict with website, email, phone, tech, social, industry, attempts.
    Returns (tier, action). Pure logic -- selftested."""
    has_site = bool(rec.get("website"))
    attempts = int(rec.get("attempts") or 0)
    missing_standard = not (rec.get("website") and (rec.get("email") or rec.get("phone")))
    thin = not (rec.get("tech") and rec.get("social") and rec.get("industry"))
    if not has_site and attempts >= MAX_ATTEMPTS_T1:
        return 3, "inference"          # CHURN FIX: no site, give up crawling -> infer locally
    if missing_standard and (has_site or attempts < MAX_ATTEMPTS_T1):
        return 1, "recrawl_standard"
    if has_site and not missing_standard and thin:
        return 2, "deep_pass"
    return 3, "inference"

def gh_put(path, obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"reenrich ladder","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
    if sha: body["sha"]=sha
    for _ in range(4):
        try:
            r=urllib.request.Request(url,data=json.dumps(body).encode(),method="PUT",headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas","Content-Type":"application/json"});urllib.request.urlopen(r,timeout=25);return "put_ok"
        except urllib.error.HTTPError as e:
            if e.code==409:
                try:
                    rq=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});body["sha"]=json.load(urllib.request.urlopen(rq,timeout=20)).get("sha")
                except Exception: pass
            else: return "http_%s"%e.code
        except Exception: time.sleep(2)
    return "exhausted"

def selftest():
    ok=True
    def chk(n,c):
        nonlocal ok; print(("  ok  " if c else "  FAIL")+" "+n); ok=ok and c
    chk("missing standard w/ site -> tier1", classify_tier({"website":"x","email":None,"phone":None,"attempts":0})[0]==1)
    chk("no site after 2 attempts -> tier3 (churn fix)", classify_tier({"website":None,"attempts":2})[0]==3)
    chk("no site early -> tier1 retry", classify_tier({"website":None,"attempts":0})[0]==1)
    chk("site+standard but thin -> tier2", classify_tier({"website":"x","email":"a@b.c","phone":"1","tech":None,"social":None,"industry":None})[0]==2)
    chk("complete -> tier3 inference catch-all", classify_tier({"website":"x","email":"a@b.c","phone":"1","tech":"WP","social":"fb","industry":"Tech"})[0]==3)
    chk("infer industry restaurant", infer_industry("Joe's Pizza","Food")=="Food & Beverage")
    chk("infer industry law", infer_industry("Smith & Co LLP","Attorney")=="Legal Services")
    chk("infer none on unknown", infer_industry("Zxqv","")==None)
    print("REENRICH SELFTEST", "PASS" if ok else "FAIL")
    return 0 if ok else 1

def migrate(cur):
    for ddl in [
        "ALTER TABLE atlas.business ADD COLUMN IF NOT EXISTS enrich_tier smallint DEFAULT 0",
        "ALTER TABLE atlas.business ADD COLUMN IF NOT EXISTS reenrich_attempts smallint DEFAULT 0",
        "ALTER TABLE atlas.business ADD COLUMN IF NOT EXISTS enrich_tier_at timestamptz",
    ]:
        try: cur.execute(ddl)
        except Exception as e: print("migrate warn:", str(e)[:80])

def main():
    if "--selftest" in sys.argv: sys.exit(selftest())
    import socrata_import as si
    si.load_db_env(si.DB_ENV_PATH)
    c=si.connect_pg(); c.autocommit=True; cur=c.cursor()
    migrate(cur)
    if "--migrate" in sys.argv:
        print("REENRICH_MIGRATE=ok"); sys.exit(0)
    def sc(q,a=None):
        try: cur.execute(q,a); r=cur.fetchone(); return int(r[0]) if r else 0
        except Exception: return 0
    out={"kind":"reenrich-ladder","node":NODE,"ts":int(time.time())}
    # --- Tier 3 LOCAL inference: fill industry where missing (real $0 field-fill) ---
    t3_filled=0
    try:
        cur.execute("""SELECT id,name,category FROM atlas.business
                       WHERE industry IS NULL AND category IS NOT NULL LIMIT %s""",(BATCH,))
        rows=cur.fetchall()
        for bid,name,cat in rows:
            ind=infer_industry(name,cat)
            if ind:
                cur.execute("UPDATE atlas.business SET industry=%s, enrich_tier=GREATEST(enrich_tier,3), enrich_tier_at=now() WHERE id=%s",(ind,bid))
                t3_filled+=1
    except Exception as e:
        out["t3_err"]=str(e)[:80]
    # --- CHURN FIX: stamp no-website records as Tier 3 so backfill stops re-crawling them ---
    t3_escalated=0
    try:
        cur.execute("""UPDATE atlas.business SET enrich_tier=GREATEST(enrich_tier,3), enrich_tier_at=now()
                       WHERE (website IS NULL OR website='') AND reenrich_attempts>=%s AND enrich_tier<3""",(MAX_ATTEMPTS_T1,))
        t3_escalated=cur.rowcount
    except Exception as e: out["churn_err"]=str(e)[:80]
    # --- tier distribution (for the report) ---
    dist={}
    try:
        cur.execute("SELECT COALESCE(enrich_tier,0), count(*) FROM atlas.business GROUP BY 1 ORDER BY 1")
        dist={str(k):int(v) for k,v in cur.fetchall()}
    except Exception: pass
    out.update({"tier3_industry_inferred":t3_filled,"tier3_nowebsite_escalated":t3_escalated,
                "tier_distribution":dist,
                "design":"Tier1 re-crawl (standard) -> Tier2 deep pass (ZoomInfo/Apollo 20+ fields) -> Tier3 $0 local inference; no-website records escalate to Tier3 (no Tier1 churn); never permanently dropped.",
                "note":"Tier1 runs via the continuous backfill (re-crawl). Tier2 deep-crawl worker + paid-AI Tier3 are the next build; this pass does the Tier3 LOCAL industry inference + churn-escalation now."})
    out["publish"]=gh_put("status/%s/reenrich-ladder.json"%NODE,out)
    print("REENRICH="+json.dumps({"t3_inferred":t3_filled,"t3_escalated":t3_escalated,"tiers":dist}));sys.exit(0)

if __name__=="__main__": main()
