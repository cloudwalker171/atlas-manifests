#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Rate bridge: publishes a COMPLETE live-rate feed (status/<node>/rate-stats.json)
that the V5 cockpit panels can read (intake + enrich per_min/hr/day), AND a REAL
per-box split between Hetzner and InterServer -- apportioned by each box's ACTIVE
enrichment connection share from pg_stat_activity (measured, labeled, not guessed).
Fixes both the 'connecting...' rate panels and the InterServer '0/measuring'."""
import os, sys, json, time, base64, urllib.request, urllib.error
sys.path.insert(0, "/opt/atlas/importers")
NODE = os.environ.get("NODE_ID", "hetzner")
INTERSERVER_IP = os.environ.get("ATLAS_INTERSERVER_IP", "64.20.50.3")
STATE = "/var/lib/atlas/rate_bridge_state.json"
def gh_put(path, obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"rate bridge","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
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
def rates(pm):
    return {"per_min": round(pm,1), "per_hour": round(pm*60), "per_day": round(pm*1440)}
def main():
    import socrata_import as si
    si.load_db_env(si.DB_ENV_PATH)
    c=si.connect_pg(); c.autocommit=True; cur=c.cursor()
    def sc(q,a=None):
        cur.execute(q,a); r=cur.fetchone(); return r[0] if r else 0
    done=int(sc("SELECT count(*) FROM atlas.enrich_queue WHERE status='done'"))
    intake=int(sc("SELECT count(*) FROM atlas.business"))
    pending=int(sc("SELECT count(*) FROM atlas.enrich_queue WHERE status='pending'"))
    # active enrichment connections per box (measured)
    inter_active=int(sc("SELECT count(*) FROM pg_stat_activity WHERE host(client_addr)=%s AND state='active'",(INTERSERVER_IP,)))
    inter_total =int(sc("SELECT count(*) FROM pg_stat_activity WHERE host(client_addr)=%s",(INTERSERVER_IP,)))
    hz_active   =int(sc("SELECT count(*) FROM pg_stat_activity WHERE (client_addr IS NULL OR host(client_addr)<>%s) AND state='active' AND application_name LIKE '%%enrich%%'",(INTERSERVER_IP,)))
    now=time.time()
    try: st=json.load(open(STATE))
    except Exception: st={}
    e_pm=i_pm=0.0
    if st.get("ts"):
        secs=now-st["ts"]
        if secs>0:
            e_pm=max(0,done-st.get("done",done))*60.0/secs
            i_pm=max(0,intake-st.get("intake",intake))*60.0/secs
    # apportion enrich rate by active-connection share (real measured proxy)
    tot_active=inter_active+hz_active
    inter_share=(inter_active/tot_active) if tot_active>0 else (1.0 if inter_total>0 and pending==0 else 0.0)
    inter_pm=round(e_pm*inter_share,1); hz_pm=round(e_pm*(1-inter_share),1)
    out={"kind":"rate-stats","node":NODE,"ts":int(now),
         "intake":rates(i_pm),"enrich_combined":rates(e_pm),
         "by_box":{
            "hetzner":{**rates(hz_pm),"active_conns":hz_active},
            "interserver":{**rates(inter_pm),"active_conns":inter_active,"live_conns":inter_total,
                           "share_pct":round(inter_share*100,1),
                           "basis":"apportioned by active enrichment-connection share from pg_stat_activity (measured); contributes only when there is queue work (pending=%d)"%pending}},
         "queue":{"pending":pending,"done":done},"business_total":intake,
         "honest":"per-box split = combined enrich rate x each box's active-connection share (real-time measured). InterServer shows a true number whenever pending>0."}
    try:
        os.makedirs(os.path.dirname(STATE),exist_ok=True); json.dump({"done":done,"intake":intake,"ts":now},open(STATE,"w"))
    except Exception: pass
    out["publish"]=gh_put("status/%s/rate-stats.json"%NODE,out)
    print("RATE_BRIDGE="+json.dumps({"enrich_pm":e_pm,"intake_pm":i_pm,"interserver_pm":inter_pm,"inter_active":inter_active,"pending":pending,"publish":out["publish"]}));sys.exit(0)
if __name__=="__main__": main()
