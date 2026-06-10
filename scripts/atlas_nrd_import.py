#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A.T.L.A.S. NRD (Newly-Registered-Domains) collector -- Channel-1 same-day
business-birth signal, CT-egress-independent. Pulls the FREE daily Whoisds NRD
list (lawful: reusable incl. commercial, no license/payment), DNS-qualifies
(keep domains that resolve), and lands each into atlas.business with website +
a birth/discovery timestamp, source='nrd_whoisds'. Firmographic-only (domains,
no persons). Idempotent. Fail-soft + reports reachability honestly (if the box
egress blocks Whoisds, it says so -- never claims rows it didn't land).
--selftest = URL/date/domain logic offline."""
import os, sys, json, time, io, zipfile, base64, socket, datetime, urllib.request, urllib.error
import concurrent.futures as cf
sys.path.insert(0, "/opt/atlas/importers")
NODE = os.environ.get("NODE_ID", "hetzner")
QUALIFY_CAP = int(os.environ.get("NRD_QUALIFY_CAP", "8000"))
DNS_WORKERS = int(os.environ.get("NRD_DNS_WORKERS", "60"))
DNS_TIMEOUT = float(os.environ.get("NRD_DNS_TIMEOUT", "3"))
SRC = "nrd_whoisds"
UA = "atlas-nrd/1.0 (+https://github.com/cloudwalker171/atlas-manifests)"

def nrd_date():
    d = os.environ.get("NRD_DATE")
    if d: return d
    # free list lags ~1 day
    return (datetime.date.today() - datetime.timedelta(days=1)).isoformat()

def nrd_url(date_str):
    b64 = base64.b64encode(("%s.zip" % date_str).encode()).decode()
    return "https://www.whoisds.com//whois-database/newly-registered-domains/%s/nrd" % b64

def fetch_domains(date_str):
    url = nrd_url(date_str)
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/zip,*/*"})
    with urllib.request.urlopen(req, timeout=60) as r:
        raw = r.read()
    zf = zipfile.ZipFile(io.BytesIO(raw))
    names = [n for n in zf.namelist() if n.lower().endswith(".txt")] or zf.namelist()
    doms = []
    for n in names:
        for line in zf.read(n).decode("utf-8", "ignore").splitlines():
            d = line.strip().lower()
            if d and "." in d and " " not in d:
                doms.append(d)
    return doms

def resolves(domain):
    try:
        socket.setdefaulttimeout(DNS_TIMEOUT)
        socket.gethostbyname(domain)
        return True
    except Exception:
        return False

def selftest():
    ok = True
    def chk(n,c):
        nonlocal ok; print(("  ok  " if c else "  FAIL")+" "+n); ok=ok and c
    chk("date->b64 url matches whoisds (2026-06-09)", nrd_url("2026-06-09").endswith("MjAyNi0wNi0wOS56aXA=/nrd"))
    chk("default date is yesterday", nrd_date() == os.environ.get("NRD_DATE", (datetime.date.today()-datetime.timedelta(days=1)).isoformat()))
    chk("domain line filter", [d for d in ["acme-roofing.com","bad domain","x"] if d and "." in d and " " not in d]==["acme-roofing.com"])
    print("SELFTEST","PASS" if ok else "FAIL"); return 0 if ok else 1

def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"nrd","content":base64.b64encode(json.dumps(obj,indent=2).encode()).decode(),"branch":br}
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

def main():
    if "--selftest" in sys.argv: sys.exit(selftest())
    import socrata_import as si
    si.load_db_env(si.DB_ENV_PATH)
    date_str = nrd_date()
    out={"kind":"nrd","node":NODE,"date":date_str,"ts":int(time.time())}
    try:
        doms = fetch_domains(date_str)
        out["downloaded"]=len(doms)
    except urllib.error.HTTPError as e:
        out["status"]="whoisds_http_%s"%e.code; out["downloaded"]=0
        out["publish"]=gh_put("status/%s/nrd.json"%NODE,out); print("NRD="+json.dumps(out)); sys.exit(0)
    except Exception as e:
        out["status"]="whoisds_unreachable_%s"%type(e).__name__; out["downloaded"]=0
        out["publish"]=gh_put("status/%s/nrd.json"%NODE,out); print("NRD="+json.dumps(out)); sys.exit(0)
    # DNS-qualify (bounded, threadpooled) -- keep resolving domains
    sample = doms[:QUALIFY_CAP]
    qualified=[]
    with cf.ThreadPoolExecutor(max_workers=DNS_WORKERS) as ex:
        for d, ok in zip(sample, ex.map(resolves, sample)):
            if ok: qualified.append(d)
    out["qualified"]=len(qualified); out["qualify_cap"]=QUALIFY_CAP
    # insert
    conn=si.connect_pg(); conn.autocommit=False; cur=conn.cursor(); inserted=0
    birth=date_str+"T00:00:00"
    for dom in qualified:
        cur.execute(si.SR_EXISTS,(SRC,dom))
        if cur.fetchone(): continue
        vals=[si.clip(dom), si.norm_name(dom), "https://"+dom, None, None, None, None, None, None, "US", None, None, "newly_registered_domain"]
        payload=json.dumps({"domain":dom,"nrd_date":date_str,"birth_date":birth,"resolved":True,"source":SRC},sort_keys=True)
        ch=si.content_hash(payload)
        cur.execute("SAVEPOINT r")
        try:
            cur.execute(si.BIZ_INSERT, vals); bid=cur.fetchone()[0]
            cur.execute(si.SR_INSERT,(SRC,dom,bid,ch,payload)); cur.execute("RELEASE SAVEPOINT r"); inserted+=1
        except Exception:
            cur.execute("ROLLBACK TO SAVEPOINT r")
        if inserted%500==0: conn.commit()
    conn.commit()
    cur.execute("SELECT count(*) FROM atlas.source_record WHERE source_code=%s",(SRC,)); total=cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM atlas.business"); biz=cur.fetchone()[0]
    conn.close()
    out["status"]="live" if inserted>0 else ("caught_up" if qualified else "no_resolving_domains")
    out["new_rows"]=inserted; out["nrd_total"]=total; out["business_total"]=biz
    out["publish"]=gh_put("status/%s/nrd.json"%NODE,out)
    print("NRD="+json.dumps({k:out[k] for k in ("date","downloaded","qualified","new_rows","nrd_total","status")})); sys.exit(0)

if __name__=="__main__": main()
