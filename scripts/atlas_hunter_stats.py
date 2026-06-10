#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Publishes status/<node>/hunter-stats.json so the V5 Lead Hunter panel stops
'PENDING FEED': prospect frame counts, qualified (qa pass), found-rate (leads/hr),
and the roster size -- from the box's real qa_status + handoff data. Read-only."""
import os, sys, json, time, base64, urllib.request, urllib.error
sys.path.insert(0, "/opt/atlas/importers")
NODE = os.environ.get("NODE_ID", "hetzner")
STATE = "/var/lib/atlas/hunter_stats_state.json"
def gh_put(path, obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"hunter stats","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
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
    si.load_db_env(si.DB_ENV_PATH)
    c=si.connect_pg(); c.autocommit=True; cur=c.cursor()
    def sc(q):
        try: cur.execute(q); r=cur.fetchone(); return int(r[0]) if r else 0
        except Exception: return 0
    qualified=sc("SELECT count(*) FROM atlas.business WHERE qa_status='pass'")
    checked=sc("SELECT count(*) FROM atlas.business WHERE qa_status IS NOT NULL")
    # roster / prospect frame = records with a crawlable website (handoff-eligible)
    roster=sc("SELECT count(*) FROM atlas.business WHERE website IS NOT NULL AND website<>''")
    now=time.time()
    try: st=json.load(open(STATE))
    except Exception: st={}
    leads_per_hour=None
    if st.get("ts") and st.get("qualified") is not None:
        secs=now-st["ts"]
        if secs>0: leads_per_hour=round(max(0,qualified-st["qualified"])*3600.0/secs,1)
    out={"kind":"hunter-stats","node":NODE,"ts":int(now),
         "prospects_total":roster,"qualified":qualified,"checked":checked,
         "pass_rate_pct":round(100.0*qualified/checked,1) if checked else None,
         "leads_per_hour":leads_per_hour,
         "note":"prospects_total = handoff-eligible records (crawlable website); qualified = qa_status='pass' (cleared the gate); leads_per_hour = delta of qualified over the sample window. Real box numbers."}
    try:
        os.makedirs(os.path.dirname(STATE),exist_ok=True); json.dump({"qualified":qualified,"ts":now},open(STATE,"w"))
    except Exception: pass
    out["publish"]=gh_put("status/%s/hunter-stats.json"%NODE,out)
    print("HUNTER_STATS="+json.dumps({k:out[k] for k in("prospects_total","qualified","checked","leads_per_hour","publish")}));sys.exit(0)
if __name__=="__main__": main()
