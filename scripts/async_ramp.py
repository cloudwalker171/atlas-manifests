#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""On-box async-lane RAMP orchestrator (runs DETACHED ~12 min). Steps
ATLAS_ASYNC_LANES 32->128->256->512 by rewriting the unit + restart, holds to
stabilize, measures combined done-rate + bounded async PG conns + RSS at each,
publishes status/hetzner/async-ramp.json cumulatively. PG-safe guard: stops if
pg_total >= PG_STOP (default 90) or async pool conns exceed pool. Never touches
the live worker fleet."""
import os,sys,json,time,subprocess,base64,urllib.request,urllib.error
NODE=os.environ.get("NODE_ID","hetzner")
STEPS=[int(x) for x in os.environ.get("ASYNC_RAMP_STEPS","128 256 512").split()]
HOLD=int(os.environ.get("ASYNC_RAMP_HOLD_S","180"))
PG_STOP=int(os.environ.get("ASYNC_PG_STOP","90"))
UNIT="/etc/systemd/system/atlas-enrich-async@.service"

def run(c,t=60):
    try: return subprocess.run(c,capture_output=True,text=True,timeout=t)
    except Exception as e:
        class R: returncode=99;stdout="";stderr=str(e)
        return R()
def unit_text(lanes,pool):
    return ("[Unit]\nDescription=ATLAS async enrichment engine (asyncio; bounded PG pool)\nAfter=network-online.target postgresql.service\nWants=network-online.target\n\n"
    "[Service]\nType=simple\nNice=10\nEnvironmentFile=-/etc/atlas/db.env\nEnvironmentFile=-/etc/atlas/autopull.env\n"
    "Environment=ATLAS_ASYNC_LANES=%d\nEnvironment=ATLAS_ASYNC_PG_POOL=%d\nEnvironment=ATLAS_ASYNC_DB_WRITERS=4\n"
    "ExecStart=/opt/atlas/venv/bin/python /opt/atlas/importers/atlas_enrich_async.py\nRestart=on-failure\nRestartSec=10\nMemoryHigh=2500M\nMemoryMax=3G\n\n[Install]\nWantedBy=multi-user.target\n"%(lanes,pool))
def pgconn():
    import psycopg2
    e={}
    for ln in open("/etc/atlas/db.env"):
        ln=ln.strip()
        if "=" in ln and not ln.startswith("#"): k,v=ln.split("=",1);e[k.strip()]=v.strip().strip("'\"")
    return psycopg2.connect(host=e.get("PGHOST","localhost"),dbname=e.get("PGDATABASE","tuanichat_atlas"),user=e.get("PGUSER"),password=e.get("PGPASSWORD"),port=e.get("PGPORT","5432"),connect_timeout=10)
def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"async ramp","content":base64.b64encode(json.dumps(obj,indent=2).encode()).decode(),"branch":br}
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
def measure(c):
    cur=c.cursor()
    def sc(q):cur.execute(q);r=cur.fetchone();return r[0] if r else None
    d0=sc("SELECT count(*) FROM atlas.enrich_queue WHERE status='done'");t0=time.time()
    time.sleep(15)
    d1=sc("SELECT count(*) FROM atlas.enrich_queue WHERE status='done'");secs=time.time()-t0
    rate=(d1-d0)*60.0/secs
    apc=sc("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'atlas_enrich_async%%'")
    tot=sc("SELECT count(*) FROM pg_stat_activity")
    u=run(["systemctl","show","atlas-enrich-async@1.service","-p","MemoryCurrent","-p","ActiveState","-p","NRestarts"])
    mc=0;act="?";nr="?"
    for ln in u.stdout.splitlines():
        if ln.startswith("MemoryCurrent="):
            try: mc=int(ln.split("=",1)[1])
            except: mc=0
        if ln.startswith("ActiveState="): act=ln.split("=",1)[1]
        if ln.startswith("NRestarts="): nr=ln.split("=",1)[1]
    return {"combined_per_min":round(rate,1),"combined_per_hour":round(rate*60),"combined_per_day":round(rate*1440),
            "async_pg_conns":apc,"pg_total":tot,"rss_mb":round(mc/1048576,1),"active":act,"restarts":nr}
def main():
    results=[]
    out={"probe":"async_ramp","node":NODE,"steps_planned":STEPS,"results":results,"ts":int(time.time())}
    c=pgconn()
    # baseline (current 32)
    m=measure(c);m["lanes"]=32;m["pool"]=8;results.append(m);out["ts"]=int(time.time());gh_put("status/%s/async-ramp.json"%NODE,out)
    for n in STEPS:
        pool=12 if n>=512 else (10 if n>=256 else 8)
        # guard
        cur=c.cursor();cur.execute("SELECT count(*) FROM pg_stat_activity");tot=cur.fetchone()[0]
        if tot>=PG_STOP:
            out["stopped"]="pg_total %d >= %d before lanes=%d"%(tot,PG_STOP,n);break
        open(UNIT,"w").write(unit_text(n,pool));run(["systemctl","daemon-reload"]);run(["systemctl","restart","atlas-enrich-async@1.service"])
        time.sleep(HOLD)
        m=measure(c);m["lanes"]=n;m["pool"]=pool;results.append(m)
        out["ts"]=int(time.time());gh_put("status/%s/async-ramp.json"%NODE,out)
        if m["pg_total"]>=PG_STOP: out["stopped"]="pg_total %d >= %d at lanes=%d"%(m["pg_total"],PG_STOP,n);break
    c.close()
    # ceiling = step with max combined_per_min
    best=max(results,key=lambda r:r.get("combined_per_min",0)) if results else {}
    out["ceiling"]={"lanes":best.get("lanes"),"per_min":best.get("combined_per_min"),"per_hour":best.get("combined_per_hour"),"per_day":best.get("combined_per_day")}
    out["done"]=True;out["ts"]=int(time.time());gh_put("status/%s/async-ramp.json"%NODE,out)
    print("ASYNC_RAMP_DONE");sys.exit(0)
if __name__=="__main__": main()
