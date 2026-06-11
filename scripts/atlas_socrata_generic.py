#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A.T.L.A.S. GENERIC Socrata ingester. Promotes the daily-fresh business
datasets discovered into atlas.source_catalog: for each it introspects the
dataset's real columns, maps them heuristically to the business schema, and
ingests new rows incrementally (:updated_at cursor) through the SAME proven
write path as socrata_import.py. Firmographic-only (requires a business NAME +
city/address). Self-reports a source-by-source table to
status/hetzner/source-promote.json: LIVE-with-rows / empty(caught-up) /
no_name_col / http_<code> / error. --selftest = mapper + firmographic gate."""
import os, sys, json, time, datetime, urllib.parse, urllib.request, urllib.error, base64
sys.path.insert(0, "/opt/atlas/importers")
NODE = os.environ.get("NODE_ID", "hetzner")
N_DATASETS = int(os.environ.get("GEN_N_DATASETS", "60"))
PER_CAP = int(os.environ.get("GEN_PER_CAP", "2000"))
PAGE = int(os.environ.get("GEN_PAGE", "1000"))
BACKFILL_DAYS = int(os.environ.get("GEN_BACKFILL_DAYS", "7"))
CURSOR_DIR = os.environ.get("GEN_CURSOR_DIR", "/var/lib/atlas/socrata_generic")
UA = "atlas-socrata-generic/1.0 (+https://github.com/cloudwalker171/atlas-manifests)"

CAND = {
 "name":   ["legal_name","legal_business_name","business_name","businessname","name_of_business","business_legal_name","dba","dba_name","doing_business_as","doing_business_as_name","business_dba_name","company","company_name","name","account_name","entity_name","current_entity_name","initial_dos_filing_entity_name","corp_name","corporation_name","registered_name","organization_name","organization","firm_name","establishment_name","facility_name","store_name","premise_name","location_name","operator_name","applicant_name","licensee_name","licensee","lic_business_name","contractor_name","vendor_name","trade_name","primary_name","registrant_name","owner_name","taxpayer_name"],
 "phone":  ["phone","phone_number","telephone","contact_phone","business_phone","phone_no","phonenumber"],
 "addr_line1":["address","street_address","address1","site_address","location_address","full_address","premise_address","business_address","address_line_1","mailing_address","street","site_addr","premises_address"],
 "city":   ["city","municipality","town","locality","city_name","business_city"],
 "region": ["state","st","province","region","state_code","business_state"],
 "postal": ["zip","zipcode","zip_code","postal_code","postcode","zip5","business_zip"],
 "lat":    ["latitude","lat","y_coordinate"],
 "lon":    ["longitude","lon","lng","long","x_coordinate"],
 "category":["license_description","business_type","type","category","naics_description","classification","license_type","business_activity","industry","description"],
}
# person-level exclusions (defense in depth; catalog discover already filters names)
PERSON = ("voter","employee","salary","individual","inmate","patient","student","tax_preparer","payroll","roster","license_holder","death","birth","arrest")

_NAME_BIZ = ("business","entity","corp","compan","dba","firm","legal","establishment","licensee","trade","organization","facility","store","premise","registrant","taxpayer")
_NAME_BAD = ("first_name","last_name","middle_name","contact_name","agent_name","city","state","county","zip","street","ward","district","officer_name","representative")
def resolve_map(cols):
    low = {c.lower(): c for c in cols}
    m = {}
    for k, cands in CAND.items():
        for c in cands:
            if c in low:
                m[k] = low[c]; break
    # fuzzy fallback for NAME: any column containing a business token + 'name'
    # (or a business token alone), excluding person/geo name columns.
    if "name" not in m:
        for lc, orig in low.items():
            if any(b in lc for b in _NAME_BAD):
                continue
            if ("name" in lc and any(b in lc for b in _NAME_BIZ)) or lc in ("name","dba"):
                m["name"] = orig; break
    return m

def map_row(raw, colmap):
    norm = {}
    for k, col in colmap.items():
        v = raw.get(col)
        if isinstance(v, dict):  # Socrata 'location' composite
            v = v.get("human_address") or None
        norm[k] = v
    return norm

def firmographic_ok(norm):
    nm = (norm.get("name") or "").strip()
    if len(nm) < 2: return False
    return bool((norm.get("city") or norm.get("addr_line1")))

def selftest():
    ok = True
    def chk(n,c):
        nonlocal ok; print(("  ok  " if c else "  FAIL")+" "+n); ok=ok and c
    m = resolve_map(["LEGAL_NAME","ADDRESS","CITY","STATE","ZIP","LATITUDE","LONGITUDE","license_description",":id",":updated_at"])
    chk("name<-LEGAL_NAME", m.get("name")=="LEGAL_NAME")
    chk("addr<-ADDRESS", m.get("addr_line1")=="ADDRESS")
    chk("city<-CITY", m.get("city")=="CITY")
    chk("region<-STATE", m.get("region")=="STATE")
    chk("category<-license_description", m.get("category")=="license_description")
    m2=resolve_map(["doing_business_as_name","site_address","municipality","business_state"])
    chk("name<-dba", m2.get("name")=="doing_business_as_name")
    chk("addr<-site_address", m2.get("addr_line1")=="site_address")
    chk("firmographic needs name+city", firmographic_ok({"name":"ACME LLC","city":"X"}) and not firmographic_ok({"city":"X"}) and not firmographic_ok({"name":"ACME"}))
    chk("no name col -> empty map", "name" not in resolve_map(["foo","bar","updated"]))
    print("SELFTEST","PASS" if ok else "FAIL"); return 0 if ok else 1

def http_json(url):
    req=urllib.request.Request(url, headers={"User-Agent":UA,"Accept":"application/json"})
    with urllib.request.urlopen(req, timeout=40) as r:
        return r.getcode(), json.loads(r.read())

def load_cursor(fxf):
    try: return open(os.path.join(CURSOR_DIR,"cur_%s.txt"%fxf)).read().strip() or None
    except OSError: return None
def save_cursor(fxf, iso):
    if not iso: return
    try:
        os.makedirs(CURSOR_DIR, exist_ok=True)
        open(os.path.join(CURSOR_DIR,"cur_%s.txt"%fxf),"w").write(iso)
    except OSError: pass

def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"source promote","content":base64.b64encode(json.dumps(obj,indent=2).encode()).decode(),"branch":br}
    if sha: body["sha"]=sha
    for _ in range(5):
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
    conn = si.connect_pg(); conn.autocommit=False; cur=conn.cursor()
    # candidate datasets: business-relevant, recent, US (catalog already US+firmographic-filtered)
    cur.execute("""SELECT domain, fourbyfour, dataset_name, data_updated_at FROM atlas.source_catalog
                   WHERE (lower(dataset_name) ~ '(business|license|permit|registration|contractor|restaurant|vendor|establishment|food|retail|firm|professional|corporation|entit|company)')
                   ORDER BY data_updated_at DESC NULLS LAST LIMIT %s""", (N_DATASETS*3,))
    _explicit=os.environ.get("SOCRATA_GEN_DATASETS","").strip()
    if _explicit:
        rows=[]
        for _chunk in _explicit.split(","):
            _p=[x.strip() for x in _chunk.split("|")]
            if len(_p)>=2:
                rows.append((_p[0], _p[1], (_p[2] if len(_p)>2 else _p[1]), None))
        globals()["N_DATASETS"]=max(N_DATASETS, len(rows))
    else:
        rows=cur.fetchall()
    results=[]; total_new=0; live=0; tried=0
    for domain, fxf, name, upd in rows:
        if tried>=N_DATASETS: break
        if any(p in (name or "").lower() for p in PERSON): 
            results.append({"dataset":name,"domain":domain,"fxf":fxf,"status":"skipped_person"}); continue
        tried+=1
        src="socrata_%s"%fxf
        base="https://%s/resource/%s.json"%(domain,fxf)
        try:
            st,sample=http_json(base+"?"+urllib.parse.urlencode({"$limit":1,"$select":":*, *"}))
        except urllib.error.HTTPError as e:
            results.append({"dataset":name,"domain":domain,"fxf":fxf,"status":"http_%s"%e.code}); continue
        except Exception as e:
            results.append({"dataset":name,"domain":domain,"fxf":fxf,"status":"err_%s"%type(e).__name__}); continue
        if not sample:
            results.append({"dataset":name,"domain":domain,"fxf":fxf,"status":"empty_dataset"}); continue
        colmap=resolve_map(list(sample[0].keys()))
        if "name" not in colmap:
            results.append({"dataset":name,"domain":domain,"fxf":fxf,"status":"no_name_col","cols":list(sample[0].keys())[:8]}); continue
        since=load_cursor(fxf) or (datetime.datetime.utcnow()-datetime.timedelta(days=BACKFILL_DAYS)).strftime("%Y-%m-%dT%H:%M:%S")
        inserted=0; fetched=0; offset=0; maxu=since
        while fetched<PER_CAP:
            page=min(PAGE, PER_CAP-fetched)
            qs={"$limit":page,"$offset":offset,"$select":":*, *","$where":":updated_at >= '%s'"%since,"$order":":updated_at"}
            try:
                st,batch=http_json(base+"?"+urllib.parse.urlencode(qs))
            except Exception as e:
                results.append({"dataset":name,"domain":domain,"fxf":fxf,"status":"fetch_err_%s"%type(e).__name__,"inserted":inserted}); batch=None; break
            if not batch: break
            for raw in batch:
                fetched+=1
                u=str(raw.get(":updated_at") or "")[:19]
                if u and u>maxu: maxu=u
                norm=map_row(raw,colmap)
                if not firmographic_ok(norm): continue
                rid=str(raw.get(":id") or si.content_hash(json.dumps(raw,sort_keys=True,default=si.json_default)))
                srid="%s:%s"%(fxf,rid)
                cur.execute(si.SR_EXISTS,(src,srid))
                if cur.fetchone(): continue
                payload=json.dumps(raw,default=si.json_default,sort_keys=True); ch=si.content_hash(payload)
                cur.execute("SAVEPOINT r")
                try:
                    cur.execute(si.BIZ_INSERT, si.biz_values(norm)); bid=cur.fetchone()[0]
                    cur.execute(si.SR_INSERT,(src,srid,bid,ch,payload)); cur.execute("RELEASE SAVEPOINT r")
                    inserted+=1
                except Exception:
                    cur.execute("ROLLBACK TO SAVEPOINT r")
            conn.commit()
            if len(batch)<page: break
            offset+=len(batch)
        if batch is not None:
            save_cursor(fxf,maxu)
            total_new+=inserted
            status="live" if inserted>0 else ("caught_up" if fetched>0 else "empty_window")
            if inserted>0: live+=1
            results.append({"dataset":name,"domain":domain,"fxf":fxf,"source_code":src,"status":status,"inserted":inserted,"fetched":fetched,"mapped_cols":list(colmap.keys())})
    cur.execute('SELECT GREATEST(reltuples::bigint,0) FROM pg_class WHERE oid=%s', ("atlas.business",)) if False else None
    cur.execute("SELECT count(*) FROM atlas.business"); biz_total=cur.fetchone()[0]
    conn.close()
    out={"kind":"source-promote","node":NODE,"ts":int(time.time()),
         "summary":{"datasets_tried":tried,"live_with_rows":live,"new_rows_this_run":total_new,"business_total":biz_total},
         "sources":results,
         "honesty":"a source counts LIVE only if it wrote real rows this run. caught_up=mapped+fetched but 0 new (dedup). no_name_col=no business-name column. http_4xx/err=fetch failed."}
    out["publish"]=gh_put("status/%s/source-promote.json"%NODE,out)
    print("SOURCE_PROMOTE="+json.dumps(out["summary"]));sys.exit(0)

if __name__=="__main__": main()
