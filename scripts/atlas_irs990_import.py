#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""IRS Exempt-Organizations Business Master File collector (NON-Socrata, free,
no-auth). Streams the public IRS EO-BMF regional CSVs (irs.gov/pub/irs-soi/eoN.csv)
and lands nonprofits/exempt orgs as firmographic businesses. source_code='irs_eo_bmf',
source_record_id=EIN (stable, unique -> idempotent). Bounded per run; line cursor
per region. NOTE: the business records are nonprofits (their own identity); the
host is irs.gov but no .gov business domain is created. Self-reporting via
status/<node>/irs990.json -> 'live' only with REAL rows."""
import os,sys,json,time,csv,io,base64,urllib.request,urllib.error
sys.path.insert(0,"/opt/atlas/importers")
NODE=os.environ.get("NODE_ID","hetzner")
SRC="irs_eo_bmf"
REGIONS=os.environ.get("IRS_REGIONS","eo1,eo2,eo3,eo4").split(",")
MAX_ROWS=int(os.environ.get("IRS_MAX_ROWS","60000"))      # per run, across regions
UA="atlas-irs990/1.0 (+https://github.com/cloudwalker171/atlas-manifests)"
CUR_DIR="/var/lib/atlas/cursors"
def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"irs990","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
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
def _cur(region):
    p=os.path.join(CUR_DIR,"irs_%s_line"%region)
    try: return int(open(p).read().strip())
    except Exception: return 0
def _save(region,n):
    try: os.makedirs(CUR_DIR,exist_ok=True); open(os.path.join(CUR_DIR,"irs_%s_line"%region),"w").write(str(n))
    except Exception: pass
def main():
    import socrata_import as si
    si.load_db_env(si.DB_ENV_PATH)
    c=si.connect_pg(); c.autocommit=False; cur=c.cursor()
    fetched=inserted=dup=skip=0; err=None; per_region={}
    budget=MAX_ROWS
    try:
        for region in REGIONS:
            if budget<=0: break
            start=_cur(region); seen=0; rinserted=0
            url="https://www.irs.gov/pub/irs-soi/%s.csv"%region
            try:
                req=urllib.request.Request(url,headers={"User-Agent":UA})
                resp=urllib.request.urlopen(req,timeout=60)
            except Exception as e:
                per_region[region]="fetch_err:%s"%type(e).__name__; err=err or per_region[region]; continue
            reader=csv.DictReader(io.TextIOWrapper(resp,encoding="latin-1",errors="replace"))
            for i,row in enumerate(reader):
                if i<start: continue
                if budget<=0 or seen>=budget: break
                seen+=1; fetched+=1
                ein=(row.get("EIN") or "").strip()
                name=(row.get("NAME") or "").strip()
                if not ein or not name: skip+=1; continue
                norm={"name":name,"addr_line1":(row.get("STREET") or "").strip(),
                      "city":(row.get("CITY") or "").strip(),"region":(row.get("STATE") or "").strip(),
                      "postal":(row.get("ZIP") or "").strip(),"country":"US",
                      "category":"Nonprofit/Exempt (NTEE %s)"%(row.get("NTEE_CD") or "n/a")}
                cur.execute(si.SR_EXISTS,(SRC,ein))
                if cur.fetchone(): dup+=1; continue
                cur.execute(si.BIZ_INSERT, si.biz_values(norm)); bid=cur.fetchone()[0]
                payload=json.dumps(row,default=si.json_default)
                cur.execute(si.SR_INSERT,(SRC,ein,bid,si.content_hash(payload),payload))
                inserted+=1; rinserted+=1
                if seen%2000==0: c.commit()
            c.commit(); _save(region,start+seen); budget-=seen
            per_region[region]="rows_scanned=%d new=%d next_line=%d"%(seen,rinserted,start+seen)
    except Exception as e:
        c.rollback(); err=str(e)[:120]
    finally:
        try: c.commit()
        except Exception: pass
    cur.execute("SELECT count(*) FROM atlas.source_record WHERE source_code=%s",(SRC,))
    total=cur.fetchone()[0]; c.close()
    out={"kind":"irs990","node":NODE,"ts":int(time.time()),"source_code":SRC,
         "status":"live" if total>0 else ("blocked" if err else "no_rows"),
         "fetched":fetched,"inserted_new":inserted,"dup":dup,"skipped":skip,
         "per_region":per_region,"total_rows_all_time":int(total),"error":err}
    out["publish"]=gh_put("status/%s/irs990.json"%NODE,out)
    print("IRS990="+json.dumps({k:out[k] for k in("status","fetched","inserted_new","total_rows_all_time","error")}));sys.exit(0 if (not err or total>0) else 1)
if __name__=="__main__": main()
