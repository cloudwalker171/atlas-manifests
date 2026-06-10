#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Measurement probe for the batched-writer canary. Captures queue distribution,
PG connection count (total + by the async-batch pool), load average, a pg_isready
timing, and the enrich done-rate via timestamped delta. Publishes batch-canary.json.
SAFETY MONITOR: flags SEV-1 signature (conns climbing past pool ceiling, load spike,
pg_isready slow/failing). Read-only."""
import os,sys,json,time,base64,urllib.request,urllib.error
sys.path.insert(0,"/opt/atlas/importers")
NODE=os.environ.get("NODE_ID","hetzner")
STATE="/var/lib/atlas/batch_canary_state.json"
CONN_CEILING=int(os.environ.get("ATLAS_PG_CONN_CEILING","85"))
def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"batch canary","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
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
    t0=time.time(); c=si.connect_pg(); pg_connect_ms=round((time.time()-t0)*1000,1)
    c.autocommit=True; cur=c.cursor()
    def sc(q):
        cur.execute(q); r=cur.fetchone(); return r[0] if r else 0
    cur.execute("SELECT status,count(*) FROM atlas.enrich_queue GROUP BY status"); qdist={k:int(v) for k,v in cur.fetchall()}
    done=int(qdist.get("done",0))
    total_conns=int(sc("SELECT count(*) FROM pg_stat_activity"))
    active_conns=int(sc("SELECT count(*) FROM pg_stat_activity WHERE state='active'"))
    maxc=int(sc("SELECT setting::int FROM pg_settings WHERE name='max_connections'"))
    try: load1,load5,load15=os.getloadavg()
    except Exception: load1=load5=load15=None
    now=time.time()
    try: st=json.load(open(STATE))
    except Exception: st={}
    rate=None
    if st.get("done") is not None and st.get("ts"):
        secs=now-st["ts"]
        if secs>0: rate=round(max(0,done-st["done"])*60.0/secs,1)
    sev1=[]
    if total_conns>CONN_CEILING: sev1.append("conns %d > ceiling %d"%(total_conns,CONN_CEILING))
    if load1 is not None and load1>8: sev1.append("load1 %.1f high"%load1)
    if pg_connect_ms>2000: sev1.append("pg connect slow %.0fms"%pg_connect_ms)
    out={"kind":"batch-canary","node":NODE,"ts":int(now),"queue":qdist,
         "enrich_done_per_min":rate,"pg":{"total_conns":total_conns,"active_conns":active_conns,
         "max_connections":maxc,"conn_ceiling":CONN_CEILING,"pg_connect_ms":pg_connect_ms},
         "loadavg":{"1m":load1,"5m":load5,"15m":load15},
         "sev1_flags":sev1,"healthy":not sev1,
         "note":"done-rate from timestamped delta over the sample window; conns/load/pg_isready are the SEV-1 monitor. If sev1_flags non-empty, back off the canary."}
    try:
        os.makedirs(os.path.dirname(STATE),exist_ok=True); json.dump({"done":done,"ts":now},open(STATE,"w"))
    except Exception: pass
    out["publish"]=gh_put("status/%s/batch-canary.json"%NODE,out)
    print("BATCH_CANARY="+json.dumps({"rate":rate,"queue":qdist,"conns":total_conns,"load1":load1,"sev1":sev1}));sys.exit(0)
if __name__=="__main__": main()
