#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A.T.L.A.S. -- per-host enrich throughput probe (READ-ONLY). Samples the shared
enrich_queue 'done' count split by claimant host (InterServer vs Hetzner) over a
short window -> reports InterServer-alone, Hetzner-alone, and COMBINED done-rate
per min/hour/day, plus shared PG connection headroom + InterServer conn count.
Publishes status/hetzner/ramp-throughput.json. No writes."""
import os,sys,json,time,re,base64,urllib.request,urllib.error
NODE=os.environ.get("NODE_ID","hetzner"); PEER=os.environ.get("ATLAS_PG_PEER","64.20.50.3")
WIN=int(os.environ.get("RAMP_SAMPLE_S","12"))

def host_of(lb):
    if not lb: return "unknown"
    lb=str(lb).lower()
    if "interserver" in lb or PEER in lb: return "interserver"
    return "hetzner"

def rates(n):
    return {"per_min":round(n,1),"per_hour":round(n*60),"per_day":round(n*1440)}

def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN"); repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com"); br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped_no_token"
    url="%s/repos/%s/contents/%s"%(api,repo,path); sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas-autopull"})
        sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"ramp throughput %s"%NODE,"content":base64.b64encode(json.dumps(obj,indent=2).encode()).decode(),"branch":br}
    if sha: body["sha"]=sha
    r=urllib.request.Request(url,data=json.dumps(body).encode(),method="PUT",headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas-autopull","Content-Type":"application/json"})
    try: urllib.request.urlopen(r,timeout=20); return "put_ok"
    except urllib.error.HTTPError as e: return "put_http_%s"%e.code
    except Exception as e: return "put_err_%s"%type(e).__name__

def main():
    import psycopg2
    e={}
    for ln in open("/etc/atlas/db.env"):
        ln=ln.strip()
        if "=" in ln and not ln.startswith("#"):
            k,v=ln.split("=",1); e[k.strip()]=v.strip().strip("'\"")
    c=psycopg2.connect(host=e.get("PGHOST","localhost"),dbname=e.get("PGDATABASE","tuanichat_atlas"),user=e.get("PGUSER"),password=e.get("PGPASSWORD"),port=e.get("PGPORT","5432"),connect_timeout=10)
    c.autocommit=True; cur=c.cursor()
    def done_by_host():
        cur.execute("SELECT locked_by, count(*) FROM atlas.enrich_queue WHERE status='done' GROUP BY locked_by")
        agg={}
        for lb,n in cur.fetchall(): agg[host_of(lb)]=agg.get(host_of(lb),0)+int(n)
        return agg
    def total_done():
        cur.execute("SELECT count(*) FROM atlas.enrich_queue WHERE status='done'"); return cur.fetchone()[0]
    t0=time.time(); d0=done_by_host(); td0=total_done()
    time.sleep(WIN)
    d1=done_by_host(); td1=total_done(); secs=time.time()-t0
    f=60.0/secs
    inter=(d1.get("interserver",0)-d0.get("interserver",0))*f
    het=(d1.get("hetzner",0)-d0.get("hetzner",0))*f
    comb=(td1-td0)*f
    # pg conns
    cur.execute("SELECT count(*) FROM pg_stat_activity"); total_conns=cur.fetchone()[0]
    cur.execute("SHOW max_connections"); maxc=cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM pg_stat_activity WHERE host(client_addr)=%s",(PEER,)); inter_conns=cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM atlas.enrich_queue WHERE status='claimed'"); claimed=cur.fetchone()[0]
    c.close()
    out={"probe":"ramp_throughput","node":NODE,"sample_seconds":round(secs,1),
         "interserver_alone":rates(inter),"hetzner_alone":rates(het),"combined":rates(comb),
         "pg_total_connections":total_conns,"pg_max_connections":maxc,"pg_conn_ceiling":85,
         "interserver_pg_connections":inter_conns,"queue_claimed":claimed,"ts":int(time.time())}
    out["publish"]=gh_put("status/%s/ramp-throughput.json"%NODE,out)
    print("RAMP_THROUGHPUT="+json.dumps(out)); sys.exit(0)

if __name__=="__main__": main()
