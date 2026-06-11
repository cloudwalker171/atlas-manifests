#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""READ-ONLY funnel + fresh-data + source-by-type report -> funnel.json.
Answers: same-day-creation last 24h, live sources by type, the full prospect
funnel (business->enriched->website->email->qa pass), hunter promotions."""
import os, sys, json, time, base64, urllib.request, urllib.error
sys.path.insert(0, "/opt/atlas/importers")
NODE=os.environ.get("NODE_ID","hetzner")
def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"funnel","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
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
    import socrata_import as si
    si.load_db_env(si.DB_ENV_PATH); c=si.connect_pg(); c.autocommit=True; cur=c.cursor()
    def sc(q,a=None):
        try: cur.execute(q,a); r=cur.fetchone(); return int(r[0]) if r and r[0] is not None else 0
        except Exception as e: return None
    def colexists(tbl,col):
        cur.execute("SELECT 1 FROM information_schema.columns WHERE table_schema='atlas' AND table_name=%s AND column_name=%s",(tbl,col)); return bool(cur.fetchone())
    biz=sc("SELECT count(*) FROM atlas.business")
    enriched=sc("SELECT count(DISTINCT business_id) FROM atlas.field_provenance")
    has_site=sc("SELECT count(*) FROM atlas.business WHERE website IS NOT NULL AND website<>''")
    has_email=sc("SELECT count(DISTINCT business_id) FROM atlas.field_provenance WHERE field='email'")
    qa_pass=sc("SELECT count(*) FROM atlas.business WHERE qa_status='pass'")
    qa_checked=sc("SELECT count(*) FROM atlas.business WHERE qa_status IS NOT NULL")
    # same-day fresh last 24h: source_record with a timestamp col, source nrd/ct/sos
    ts=None
    for cand in ("first_seen","created_at","inserted_at","seen_at","fetched_at","ts"):
        if colexists("source_record",cand): ts=cand; break
    fresh24=None; fresh_by=None
    if ts:
        cur.execute("""SELECT split_part(source_code,'_',1), count(*) FROM atlas.source_record
                       WHERE %s >= now()-interval '24 hours' GROUP BY 1 ORDER BY 2 DESC"""%('"'+ts+'"'))
        fresh_by={k:int(v) for k,v in cur.fetchall()}
        fresh24=sum(fresh_by.values())
    else:
        # fallback: business.birth_date = today (NRD sets birth_date)
        if colexists("business","birth_date"):
            fresh24=sc("SELECT count(*) FROM atlas.business WHERE birth_date >= (now()-interval '24 hours')::date")
            fresh_by={"birth_date_24h":fresh24}
    # source by type
    cur.execute("SELECT source_code, count(*) FROM atlas.source_record GROUP BY 1")
    src={k:int(v) for k,v in cur.fetchall()}
    from collections import defaultdict
    def typ(co):
        co=co.lower()
        if 'gleif' in co: return 'GLEIF'
        if 'irs_eo_bmf' in co or '990' in co: return 'IRS-990'
        if 'nrd' in co or 'whoisds' in co: return 'NRD'
        if co.startswith('edgar') or 'formd' in co: return 'EDGAR'
        if co.startswith('socrata') or co in('socrata_chicago','socrata_nyc'): return 'Socrata'
        if 'arcgis' in co: return 'ArcGIS'
        if 'ckan' in co: return 'CKAN'
        if 'sos' in co: return 'SoS'
        return 'other'
    byt=defaultdict(lambda:[0,0])
    for co,n in src.items(): t=typ(co); byt[t][0]+=1; byt[t][1]+=n
    # hunter promotions (source_catalog)
    hunter=None
    if colexists("source_catalog","state"):
        cur.execute("SELECT state, count(*) FROM atlas.source_catalog GROUP BY 1"); hunter={k:int(v) for k,v in cur.fetchall()}
    out={"kind":"funnel","node":NODE,"ts":int(time.time()),
      "same_day_fresh_24h":{"total":fresh24,"by_source_prefix":fresh_by,"basis":"source_record.%s"%ts if ts else "business.birth_date"},
      "sources_by_type":{t:{"source_codes":v[0],"rows":v[1]} for t,v in byt.items()},
      "total_source_codes":len(src),
      "hunter_catalog":hunter,
      "prospect_funnel":[
        {"step":"1_business_total","count":biz,"pct":100.0},
        {"step":"2_enriched","count":enriched,"pct":round(100.0*enriched/biz,1) if biz and enriched else None},
        {"step":"3_has_website","count":has_site,"pct":round(100.0*has_site/biz,1) if biz and has_site else None},
        {"step":"4_has_valid_email","count":has_email,"pct":round(100.0*has_email/biz,1) if biz and has_email else None},
        {"step":"5_qa_pass","count":qa_pass,"pct":round(100.0*qa_pass/biz,3) if biz and qa_pass else None,"of_checked":qa_checked},
      ],
      "note":"1.4M collapses to prospects via the HARD gate: working site + valid email + no-chat + score>=60. Most records aren't deeply enriched yet (only %s/%s have a website); the Tier-2 ladder grows the qualified pool over time. wp_tnc_prospects count is WP-side."%(has_site,biz)}
    out["publish"]=gh_put("status/%s/funnel.json"%NODE,out)
    print("FUNNEL="+json.dumps({"biz":biz,"enriched":enriched,"site":has_site,"email":has_email,"qa_pass":qa_pass,"fresh24":fresh24,"types":len(byt)}));sys.exit(0)
if __name__=="__main__": main()
