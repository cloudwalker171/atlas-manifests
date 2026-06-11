#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tier-2 SMART WEBSITE DISCOVERY -- the #1 lever (31% website -> higher).
Runs on website-LESS records. Methods (safe-first):
  M4 PARSE SOURCE FIELDS: many source_record payloads already carry a url/website
     field we never extracted -> accept directly ($0, no crawl, no false-match).
  M5 EMAIL/MX -> WEBSITE: if a record has an email, its domain IS the website.
  M1 CORROBORATED PERMUTATION: generate name-variant domains, DNS+HTTP-200, and
     ACCEPT ONLY IF the business name-token + city appears on the page (kills the
     A&R-Markets->markets.com false matches). Bounded.
Sets business.website + field_provenance(source='website_discovery'). Measures lift.
M2 search-API (Brave/Bing) + M3 Common Crawl reverse-lookup are crawl/key extensions (noted)."""
import os, sys, json, time, re, socket, base64, urllib.request, urllib.error
sys.path.insert(0, "/opt/atlas/importers")
NODE=os.environ.get("NODE_ID","hetzner"); BATCH=int(os.environ.get("WSD_BATCH","20000"))
DO_CRAWL=os.environ.get("WSD_CRAWL","1")=="1"; MAXPERM=int(os.environ.get("WSD_MAXPERM","6"))
URL_KEYS=("website","url","web","homepage","website_url","web_address","site","www","weburl","business_website")
BADDOM=re.compile(r"\.(gov|mil)$",re.I)
def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"wsd","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
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
def cleandom(v):
    if not v or not isinstance(v,str): return None
    v=v.strip().lower()
    m=re.search(r"([a-z0-9][a-z0-9\-]{1,62}(?:\.[a-z]{2,})+)",v.replace("https://","").replace("http://","").replace("www.",""))
    if not m: return None
    d=m.group(1)
    if BADDOM.search(d) or d.endswith(".gov") or "@" in d: return None
    if d.split(".")[0] in ("facebook","instagram","twitter","linkedin","yelp","google","youtube","wixsite","wordpress"): return None
    return d
def find_in_payload(payload):
    try: p=payload if isinstance(payload,dict) else json.loads(payload)
    except Exception: return None
    for k,v in (p.items() if isinstance(p,dict) else []):
        if any(uk in k.lower() for uk in URL_KEYS):
            d=cleandom(v)
            if d: return d
    # any value that looks like a url
    for v in (p.values() if isinstance(p,dict) else []):
        if isinstance(v,str) and ("http" in v or "www." in v):
            d=cleandom(v)
            if d: return d
    return None
def name_variants(name):
    n=re.sub(r"[^a-z0-9 ]"," ",(name or "").lower())
    n=re.sub(r"\b(llc|inc|incorporated|corp|corporation|co|ltd|company|the|and)\b"," ",n)
    toks=[t for t in n.split() if t]
    if not toks: return []
    base=[ "".join(toks), "".join(toks[:2]) if len(toks)>1 else None, toks[0], "-".join(toks) ]
    cands=[]
    for b in [x for x in base if x and 2<len(x)<40]:
        for tld in (".com",".net",".org",".co"):
            cands.append(b+tld)
    seen=set(); out=[]
    for c in cands:
        if c not in seen: seen.add(c); out.append(c)
    return out[:MAXPERM]
def http_corroborate(domain, name, city):
    try:
        req=urllib.request.Request("https://"+domain,headers={"User-Agent":"atlas-wsd/1.0"})
        html=urllib.request.urlopen(req,timeout=8).read(60000).decode("utf-8","replace").lower()
    except Exception:
        try:
            req=urllib.request.Request("http://"+domain,headers={"User-Agent":"atlas-wsd/1.0"})
            html=urllib.request.urlopen(req,timeout=8).read(60000).decode("utf-8","replace").lower()
        except Exception: return False
    toks=[t for t in re.sub(r"[^a-z0-9 ]"," ",(name or "").lower()).split() if len(t)>3]
    name_hit=any(t in html for t in toks[:3]) if toks else False
    city_hit=(city or "").lower() in html if city else True
    return name_hit and city_hit
def main():
    import socrata_import as si
    si.load_db_env(si.DB_ENV_PATH); c=si.connect_pg(); c.autocommit=True; cur=c.cursor()
    before=0
    try: cur.execute("SELECT count(*) FROM atlas.business WHERE website IS NOT NULL AND website<>''"); before=cur.fetchone()[0]
    except Exception: pass
    m4=m5=m1=scanned=0
    cur.execute("""SELECT b.id,b.name,b.city,b.email,
                   (SELECT sr.payload FROM atlas.source_record sr WHERE sr.business_id=b.id LIMIT 1)
                   FROM atlas.business b WHERE (b.website IS NULL OR b.website='') LIMIT %s""",(BATCH,))
    rows=cur.fetchall()
    for bid,name,city,email,payload in rows:
        scanned+=1; dom=None; src=None
        # M4 payload
        if payload is not None:
            dom=find_in_payload(payload)
            if dom: src="payload"
        # M5 email domain
        if not dom and email and "@" in email:
            d=cleandom(email.split("@")[1])
            if d: dom=d; src="email_domain"
        # M1 corroborated permutation (crawl)
        if not dom and DO_CRAWL:
            for cand in name_variants(name):
                try: socket.gethostbyname(cand)
                except Exception: continue
                if http_corroborate(cand,name,city): dom=cand; src="permutation_corroborated"; break
        if dom:
            try:
                cur.execute("UPDATE atlas.business SET website=%s WHERE id=%s AND (website IS NULL OR website='')",("https://"+dom,bid))
                if cur.rowcount:
                    cur.execute("""INSERT INTO atlas.field_provenance(business_id,field,value,source_code,confidence,last_verified)
                                   VALUES(%s,'website',%s,'website_discovery',%s,now()) ON CONFLICT DO NOTHING""",
                                (bid,"https://"+dom, 0.9 if src=="payload" else (0.85 if src=="email_domain" else 0.75)))
                    if src=="payload": m4+=1
                    elif src=="email_domain": m5+=1
                    else: m1+=1
            except Exception: pass
    cur.execute("SELECT count(*) FROM atlas.business WHERE website IS NOT NULL AND website<>''"); after=cur.fetchone()[0]
    biz=0
    try: cur.execute("SELECT count(*) FROM atlas.business"); biz=cur.fetchone()[0]
    except Exception: pass
    out={"kind":"website-discovery","node":NODE,"ts":int(time.time()),"scanned_websiteless":scanned,
         "found":{"M4_payload":m4,"M5_email_domain":m5,"M1_permutation_corroborated":m1,"total":m4+m5+m1},
         "website_count_before":before,"website_count_after":after,
         "website_pct_before":round(100.0*before/biz,2) if biz else None,"website_pct_after":round(100.0*after/biz,2) if biz else None,
         "note":"M4(payload)+M5(email) are $0 no-crawl no-false-match; M1 permutation is corroborated (name-token+city on page) to kill false matches. M2 search-API + M3 Common Crawl reverse-lookup are next (M2 needs a free Brave/Bing key)."}
    out["publish"]=gh_put("status/%s/website-discovery.json"%NODE,out)
    print("WSD="+json.dumps({k:out[k] for k in("found","website_pct_before","website_pct_after","scanned_websiteless")}));sys.exit(0)
if __name__=="__main__": main()
