#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ATLAS operational self-tuning advisor. Each run reads the live signals
(throughput/rate-stats/queue depth/per-box rates/backfill vs drain) and emits
RANKED operational recommendations -> status/<node>/ops-recommendations.json,
which the improvement engine surfaces. Cadence tunes = auto_safe (bounded);
box-adds / architecture = escalate-to-user (big-bet, never auto-applied). Read-only."""
import os, sys, json, time, base64, urllib.request, urllib.error
sys.path.insert(0, "/opt/atlas/importers")
NODE = os.environ.get("NODE_ID", "hetzner")
STATE = "/var/lib/atlas/ops_advisor_state.json"
BACKFILL_PER_MIN = float(os.environ.get("OPS_BACKFILL_PER_MIN", "10000"))  # 100k/10min
def gh_put(path, obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"ops advisor","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
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
def analyze(pending, pending_prev, secs, drain_pm, active_conns, max_conns, inter_pm):
    """Pure logic (selftested): returns ranked recommendations."""
    recs=[]
    # queue growth trend
    q_growth_pm = (pending - pending_prev)*60.0/secs if (pending_prev is not None and secs>0) else None
    if q_growth_pm is not None and q_growth_pm > 0 and BACKFILL_PER_MIN > drain_pm*1.5:
        recs.append({"rank":1,"area":"cadence","class":"auto_safe",
            "title":"Backfill outpacing drain — throttle re-enrich requeue",
            "finding":"pending growing ~%.0f/min; backfill ~%.0f/min >> drain ~%.0f/min"%(q_growth_pm,BACKFILL_PER_MIN,drain_pm),
            "action":"reduce backfill to <= drain (e.g. 20k/30min) OR pause until pending drains; 1.3M re-enrich backlog already queued"})
    # crawl-bound vs DB-bound
    db_bound = active_conns is not None and max_conns and active_conns > 0.6*max_conns
    crawl_bound = active_conns is not None and active_conns <= max(4, 0.1*(max_conns or 100))
    if crawl_bound:
        recs.append({"rank":2,"area":"bottleneck","class":"info",
            "title":"CRAWL-bound (not DB-bound) — more lanes/workers won't lift the rate",
            "finding":"active PG conns %s of %s (low) -> writes are free; ceiling is HTTP crawl latency"%(active_conns,max_conns),
            "action":"adding async lanes/workers gives little; to break the >2k ceiling, attack crawl latency"})
        recs.append({"rank":3,"area":"capacity","class":"escalate",
            "title":"Break the crawl-bound >2k ceiling: 3rd enrichment box OR Common Crawl bulk",
            "finding":"crawl-bound; combined ~%.0f/min, InterServer ~%.0f/min on httpx"%(drain_pm,inter_pm or 0),
            "action":"BIG-BET (escalate, never auto-apply): (a) add a 3rd async-httpx enrichment box (~+800-1200/min), or (b) switch to Common Crawl/CDX bulk HTML to remove per-site round-trips entirely"})
    elif db_bound:
        recs.append({"rank":2,"area":"scaling","class":"auto_safe",
            "title":"DB-bound — batched writes + bounded pool already mitigate; consider +workers",
            "finding":"active conns %s/%s high"%(active_conns,max_conns),
            "action":"ensure batched-write canary on; add workers only within conn ceiling"})
    # hunter cadence (source discovery)
    recs.append({"rank":5,"area":"cadence","class":"info",
        "title":"Source-hunter hourly cadence is appropriate",
        "finding":"hunter OnUnitActiveSec=1h (12min first-run); source discovery is low-churn",
        "action":"keep hourly; no need to run faster for source discovery"})
    return recs, q_growth_pm
def selftest():
    ok=True
    def chk(n,c):
        nonlocal ok; print(("  ok  " if c else "  FAIL")+" "+n); ok=ok and c
    r,_=analyze(1300000,1200000,600,400,2,100,90)
    titles=" ".join(x["title"] for x in r)
    chk("throttle rec when backfill outpaces drain", "throttle" in titles.lower())
    chk("crawl-bound flagged", "crawl-bound" in titles.lower())
    chk("3rd box escalated", any(x["class"]=="escalate" for x in r))
    chk("ranked", all("rank" in x for x in r))
    print("OPS SELFTEST","PASS" if ok else "FAIL"); return 0 if ok else 1
def _read_local(name):
    try: return json.load(open(os.path.join("/var/lib/atlas/status",name)))
    except Exception: return {}
def main():
    if "--selftest" in sys.argv: sys.exit(selftest())
    import socrata_import as si
    si.load_db_env(si.DB_ENV_PATH); c=si.connect_pg(); c.autocommit=True; cur=c.cursor()
    def sc(q):
        try: cur.execute(q); r=cur.fetchone(); return int(r[0]) if r else 0
        except Exception: return 0
    pending=sc("SELECT count(*) FROM atlas.enrich_queue WHERE status='pending'")
    done=sc("SELECT count(*) FROM atlas.enrich_queue WHERE status='done'")
    active=sc("SELECT count(*) FROM pg_stat_activity WHERE state='active'")
    maxc=sc("SELECT setting::int FROM pg_settings WHERE name='max_connections'")
    now=time.time()
    try: st=json.load(open(STATE))
    except Exception: st={}
    secs=now-st.get("ts",now-600); drain_pm=0.0
    if st.get("done") is not None and secs>0: drain_pm=max(0,done-st["done"])*60.0/secs
    rs=_read_local("rate-stats.json"); inter_pm=((rs.get("by_box") or {}).get("interserver") or {}).get("per_min")
    recs,qg=analyze(pending, st.get("pending"), secs, drain_pm, active, maxc, inter_pm)
    out={"kind":"ops-recommendations","node":NODE,"ts":int(now),
         "signals":{"pending":pending,"done":done,"queue_growth_per_min":round(qg,1) if qg is not None else None,
                    "drain_per_min":round(drain_pm,1),"active_conns":active,"max_conns":maxc,"interserver_per_min":inter_pm},
         "recommendations":recs,
         "auto_safe":[r for r in recs if r["class"]=="auto_safe"],
         "escalate":[r for r in recs if r["class"]=="escalate"]}
    try:
        os.makedirs(os.path.dirname(STATE),exist_ok=True); json.dump({"pending":pending,"done":done,"ts":now},open(STATE,"w"))
    except Exception: pass
    out["publish"]=gh_put("status/%s/ops-recommendations.json"%NODE,out)
    print("OPS="+json.dumps({"qgrowth_pm":out["signals"]["queue_growth_per_min"],"drain_pm":out["signals"]["drain_per_min"],"recs":[r["title"] for r in recs]}));sys.exit(0)
if __name__=="__main__": main()
