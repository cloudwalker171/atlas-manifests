#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""READ-ONLY: measure the async canary -- RSS, is-active, its bounded PG pool
(application_name='atlas_enrich_async'), and the combined done-rate. Publishes
status/hetzner/async-measure.json."""
import os,sys,json,time,subprocess,base64,urllib.request,urllib.error
NODE=os.environ.get("NODE_ID","hetzner")
def run(c):
    try: return subprocess.run(c,capture_output=True,text=True,timeout=25)
    except Exception as e:
        class R: returncode=99;stdout="";stderr=str(e)
        return R()
def show(unit,props):
    p=run(["systemctl","show",unit,"--no-pager"]+sum([["-p",x] for x in props],[]));o={}
    for ln in p.stdout.splitlines():
        if "=" in ln: k,v=ln.split("=",1);o[k]=v
    return o
def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"})
        sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"async measure","content":base64.b64encode(json.dumps(obj,indent=2).encode()).decode(),"branch":br}
    if sha: body["sha"]=sha
    for _ in range(4):
        try:
            r=urllib.request.Request(url,data=json.dumps(body).encode(),method="PUT",headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas","Content-Type":"application/json"})
            urllib.request.urlopen(r,timeout=25);return "put_ok"
        except urllib.error.HTTPError as e:
            if e.code==409:
                try:
                    rq=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});body["sha"]=json.load(urllib.request.urlopen(rq,timeout=20)).get("sha")
                except Exception: pass
            else: return "put_http_%s"%e.code
        except Exception as e: time.sleep(2)
    return "put_retry_exhausted"
def main():
    out={"probe":"async_measure","node":NODE,"ts":int(time.time())}
    u=show("atlas-enrich-async@1.service",["ActiveState","SubState","MainPID","MemoryCurrent","NRestarts"])
    out["canary_unit"]=u
    try: out["canary_rss_mb"]=round(int(u.get("MemoryCurrent","0"))/1048576,1)
    except Exception: out["canary_rss_mb"]=None
    import psycopg2
    e={}
    for ln in open("/etc/atlas/db.env"):
        ln=ln.strip()
        if "=" in ln and not ln.startswith("#"):
            k,v=ln.split("=",1);e[k.strip()]=v.strip().strip("'\"")
    c=psycopg2.connect(host=e.get("PGHOST","localhost"),dbname=e.get("PGDATABASE","tuanichat_atlas"),user=e.get("PGUSER"),password=e.get("PGPASSWORD"),port=e.get("PGPORT","5432"),connect_timeout=10)
    c.autocommit=True;cur=c.cursor()
    def sc(q):cur.execute(q);r=cur.fetchone();return r[0] if r else None
    out["async_pg_connections"]=sc("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'atlas_enrich_async%%'")
    out["pg_total_connections"]=sc("SELECT count(*) FROM pg_stat_activity")
    out["pg_max_connections"]=sc("SHOW max_connections")
    d0=sc("SELECT count(*) FROM atlas.enrich_queue WHERE status='done'");t0=time.time()
    time.sleep(12)
    d1=sc("SELECT count(*) FROM atlas.enrich_queue WHERE status='done'");secs=time.time()-t0
    rate=(d1-d0)*60.0/secs
    out["combined_done_per_min"]=round(rate,1)
    out["combined_done_per_hour"]=round(rate*60);out["combined_done_per_day"]=round(rate*1440)
    c.close()
    out["publish"]=gh_put("status/%s/async-measure.json"%NODE,out)
    print("ASYNC_MEASURE="+json.dumps(out));sys.exit(0)
if __name__=="__main__": main()
