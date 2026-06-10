#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GLEIF LEI collector (NON-Socrata, free, no-auth). Pulls US legal entities from
the public GLEIF API (api.gleif.org/api/v1/lei-records, JSON:API) and lands them
as firmographic businesses. source_code='gleif', source_record_id=LEI (stable,
unique -> idempotent). Bounded per run; page cursor persisted. Self-reporting:
publishes status/<node>/gleif.json with fetched/inserted/error so 'live' means
REAL ROWS, not just 'deployed'."""
import os,sys,json,time,base64,urllib.request,urllib.parse,urllib.error
sys.path.insert(0,"/opt/atlas/importers")
NODE=os.environ.get("NODE_ID","hetzner")
SRC="gleif"
PAGE_SIZE=int(os.environ.get("GLEIF_PAGE_SIZE","200"))
MAX_PAGES=int(os.environ.get("GLEIF_MAX_PAGES","40"))     # 40*200 = 8000/run
CURSOR="/var/lib/atlas/cursors/gleif_page"
UA="atlas-gleif/1.0 (+https://github.com/cloudwalker171/atlas-manifests)"
def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"gleif","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
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
def _load_cursor():
    try: return int(open(CURSOR).read().strip())
    except Exception: return 1
def _save_cursor(p):
    try: os.makedirs(os.path.dirname(CURSOR),exist_ok=True); open(CURSOR,"w").write(str(p))
    except Exception: pass
def fetch_page(page):
    qs=urllib.parse.urlencode({"filter[entity.legalAddress.country]":"US","page[size]":PAGE_SIZE,"page[number]":page})
    url="https://api.gleif.org/api/v1/lei-records?"+qs
    req=urllib.request.Request(url,headers={"User-Agent":UA,"Accept":"application/vnd.api+json"})
    return json.load(urllib.request.urlopen(req,timeout=40))
def maprec(rec):
    a=rec.get("attributes",{}) or {}; ent=a.get("entity",{}) or {}
    name=((ent.get("legalName") or {}).get("name"))
    la=ent.get("legalAddress",{}) or {}
    addr=" ".join(x for x in (la.get("addressLines") or []) if x) or la.get("mailRouting")
    return {"name":name,"addr_line1":addr,"city":la.get("city"),"region":la.get("region","").split("-")[-1] if la.get("region") else None,
            "postal":la.get("postalCode"),"country":(la.get("country") or "US"),
            "category":(ent.get("legalForm",{}) or {}).get("id") or "LEI-registered entity"}
def main():
    import socrata_import as si
    si.load_db_env(si.DB_ENV_PATH)
    c=si.connect_pg(); c.autocommit=False; cur=c.cursor()
    page=_load_cursor(); fetched=inserted=dup=skip=0; err=None; pages=0
    try:
        for _ in range(MAX_PAGES):
            try: doc=fetch_page(page)
            except Exception as e: err="fetch p%d: %s"%(page,type(e).__name__); break
            data=doc.get("data",[]) or []
            if not data:
                page=1; break  # wrapped/empty -> reset cursor
            for rec in data:
                fetched+=1; lei=rec.get("id")
                norm=maprec(rec)
                if not norm.get("name") or not lei: skip+=1; continue
                cur.execute(si.SR_EXISTS,(SRC,lei))
                if cur.fetchone(): dup+=1; continue
                cur.execute(si.BIZ_INSERT, si.biz_values(norm)); bid=cur.fetchone()[0]
                payload=json.dumps(rec,default=si.json_default)
                cur.execute(si.SR_INSERT,(SRC,lei,bid,si.content_hash(payload),payload))
                inserted+=1
            c.commit(); pages+=1; page+=1; time.sleep(0.3)
    except Exception as e:
        c.rollback(); err=str(e)[:120]
    finally:
        try: c.commit()
        except Exception: pass
    _save_cursor(page)
    cur.execute("SELECT count(*) FROM atlas.source_record WHERE source_code=%s",(SRC,))
    total=cur.fetchone()[0]; c.close()
    out={"kind":"gleif","node":NODE,"ts":int(time.time()),"source_code":SRC,
         "status":"live" if total>0 else ("blocked" if err else "no_rows"),
         "fetched":fetched,"inserted_new":inserted,"dup":dup,"skipped_no_name":skip,
         "pages":pages,"next_page":page,"total_rows_all_time":int(total),"error":err}
    out["publish"]=gh_put("status/%s/gleif.json"%NODE,out)
    print("GLEIF="+json.dumps({k:out[k] for k in("status","fetched","inserted_new","total_rows_all_time","error")}));sys.exit(0 if not err or total>0 else 1)
if __name__=="__main__": main()
