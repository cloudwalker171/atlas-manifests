#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PER-SOURCE OUTPUT-FRESHNESS MONITOR. Catches "stopped PRODUCING", not just
"stopped running". For every atlas-*.timer + every Tier-1 prime source it checks:
  (a) timer is ENABLED and has a scheduled next-run (a disabled/dead timer = failure,
      even though the oneshot service is not 'failed'),
  (b) the source is actually PRODUCING: max(business.first_seen) for that source's
      records is within its freshness SLA.
On STALE or DEAD-TIMER -> AUTO-REVIVE (daemon-reload; enable --now; reset-failed;
start) + escalate LOUDLY (freshness.json + alert file). Tier-1 (NRD/SoS/CT birth
feeds) get the tightest SLA and immediate escalation.  --selftest proves the
decision core catches a simulated stalled/disabled source before any deploy."""
import os,sys,json,time,base64,subprocess,urllib.request,urllib.error
sys.path.insert(0,"/opt/atlas/importers")
NODE=os.environ.get("NODE_ID","hetzner");NOW=int(time.time())

# Tier-1 PRIME birth feeds: db source code -> (timer unit, service unit, sla_hours)
TIER1={
 "nrd":("atlas-nrd.timer","atlas-nrd.service",26),
 "sos":("atlas-sos.timer","atlas-sos.service",30),
 "sos_new_business":("atlas-sos.timer","atlas-sos.service",30),
 "ct":("atlas-ct.timer","atlas-ct.service",30),
 "ssl_ct":("atlas-ct.timer","atlas-ct.service",30),
}
# default SLA for any other discovered timer (Tier-2/3 collectors)
DEFAULT_SLA_H=int(os.environ.get("FRESH_DEFAULT_SLA_H","50"))

def assess(timer_enabled, timer_scheduled, last_produce_age_h, sla_h):
    """PURE decision core (selftest-driven). Returns (state, revive?)."""
    if timer_enabled is False:            return ("DEAD_TIMER_DISABLED", True)
    if timer_scheduled is False:          return ("DEAD_TIMER_NOT_SCHEDULED", True)
    if last_produce_age_h is None:        return ("UNKNOWN_NO_DATA", False)
    if last_produce_age_h > sla_h:        return ("STALE_NOT_PRODUCING", True)
    if last_produce_age_h > 0.8*sla_h:    return ("WARN_NEARING_SLA", False)
    return ("OK", False)

def sh(args,timeout=25):
    try:
        p=subprocess.run(args,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=timeout)
        return p.returncode,(p.stdout or b"").decode("utf-8","replace").strip(),(p.stderr or b"").decode("utf-8","replace").strip()
    except Exception as e:
        return 1,"",str(e)

def timer_state(unit):
    rc,fs,_=sh(["systemctl","is-enabled",unit]);enabled=(fs.strip()=="enabled" or fs.strip()=="enabled-runtime")
    rc,act,_=sh(["systemctl","show",unit,"-p","ActiveState","-p","NextElapseUSecRealtime","-p","NextElapseUSecMonotonic","-p","LoadState"])
    kv=dict(l.split("=",1) for l in act.splitlines() if "=" in l)
    loaded=kv.get("LoadState")=="loaded"
    def _set(v): return bool(v) and v not in ("","0","infinity","n/a")
    scheduled=(kv.get("ActiveState")=="active") or _set(kv.get("NextElapseUSecRealtime")) or _set(kv.get("NextElapseUSecMonotonic"))
    return {"unit":unit,"enabled":enabled,"loaded":loaded,"active":kv.get("ActiveState"),"scheduled":scheduled,"next":nxt}

def revive(timer,service,actions):
    sh(["systemctl","daemon-reload"])
    r1=sh(["systemctl","enable","--now",timer]);r2=sh(["systemctl","reset-failed",service])
    r3=sh(["systemctl","start","--no-block",service])
    actions.append({"timer":timer,"service":service,"enable_rc":r1[0],"start_rc":r3[0],"err":(r1[2] or r3[2])[:120]})

def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO");api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo:return "skip"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception:sha=None
    body={"message":"freshness","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
    if sha:body["sha"]=sha
    for _ in range(4):
        try:
            r=urllib.request.Request(url,data=json.dumps(body).encode(),method="PUT",headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas","Content-Type":"application/json"});urllib.request.urlopen(r,timeout=25);return "put_ok"
        except urllib.error.HTTPError as e:
            if e.code==409:
                try:
                    rq=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});body["sha"]=json.load(urllib.request.urlopen(rq,timeout=20)).get("sha")
                except Exception:pass
            else:return "http_%s"%e.code
        except Exception:time.sleep(2)
    return "exhausted"

def selftest():
    cases=[
     ("disabled timer must revive", assess(False,True,1.0,26), "DEAD_TIMER_DISABLED", True),
     ("unscheduled timer must revive", assess(True,False,1.0,26), "DEAD_TIMER_NOT_SCHEDULED", True),
     ("stale producer must revive (NRD 40h>26h)", assess(True,True,40.0,26), "STALE_NOT_PRODUCING", True),
     ("healthy producer is OK", assess(True,True,5.0,26), "OK", False),
     ("nearing SLA warns not revive", assess(True,True,22.0,26), "WARN_NEARING_SLA", False),
     ("the exact NRD bug: enabled-but-not-scheduled+stale", assess(True,False,55.0,26), "DEAD_TIMER_NOT_SCHEDULED", True),
    ]
    ok=0
    for name,(st,rev),exp_st,exp_rev in cases:
        good=(st==exp_st and rev==exp_rev);ok+=good
        print(("PASS" if good else "FAIL"),name,"->",st,rev)
    print("SELFTEST %d/%d"%(ok,len(cases)));sys.exit(0 if ok==len(cases) else 1)

def main():
    if "--selftest" in sys.argv: selftest()
    import socrata_import as si
    si.load_db_env(si.DB_ENV_PATH);c=si.connect_pg();c.autocommit=True;cur=c.cursor()
    def produce_age_h(src):
        try:
            cur.execute("""SELECT EXTRACT(EPOCH FROM (now()-max(b.first_seen)))/3600.0
                           FROM atlas.source_record sr JOIN atlas.business b ON b.id=sr.business_id
                           WHERE sr.source=%s""",(src,))
            r=cur.fetchone();return round(float(r[0]),2) if r and r[0] is not None else None
        except Exception:
            try:c.rollback()
            except Exception:pass
            return None
    audit=[];actions=[];escalations=[]
    # --- discover ALL atlas-*.timer units (catch dead/disabled ones) ---
    rc,out,_=sh(["bash","-lc","systemctl list-timers --all --no-legend 'atlas-*' 2>/dev/null | awk '{print $NF}'; systemctl list-unit-files 'atlas-*.timer' --no-legend 2>/dev/null | awk '{print $1}'"])
    timers=sorted(set(t for t in out.split() if t.endswith(".timer")))
    seen=set()
    # --- Tier-1 prime sources first (tightest) ---
    for src,(tmr,svc,sla) in TIER1.items():
        st=timer_state(tmr)
        if not st["loaded"]: continue
        age=produce_age_h(src);state,rev=assess(st["enabled"],st["scheduled"],age,sla)
        row={"tier":1,"source":src,"timer":tmr,"sla_h":sla,"enabled":st["enabled"],"scheduled":st["scheduled"],"active":st["active"],"produce_age_h":age,"verdict":state}
        if rev: revive(tmr,svc,actions); row["revived"]=True; escalations.append("TIER1 %s %s -> revived"%(src,state))
        audit.append(row); seen.add(tmr)
    # --- every other timer: dead/disabled detection ---
    for tmr in timers:
        if tmr in seen: continue
        st=timer_state(tmr)
        if not st["loaded"]: continue
        svc=tmr[:-6]+".service"
        state,rev=assess(st["enabled"],st["scheduled"],None,DEFAULT_SLA_H)
        row={"tier":2,"timer":tmr,"enabled":st["enabled"],"scheduled":st["scheduled"],"active":st["active"],"verdict":state}
        if rev: revive(tmr,svc,actions); row["revived"]=True; escalations.append("%s %s -> revived"%(tmr,state))
        audit.append(row)
    stale=[a for a in audit if a.get("revived")]
    out={"kind":"freshness-monitor","node":NODE,"ts":NOW,"timers_checked":len(audit),
         "tier1_count":sum(1 for a in audit if a.get("tier")==1),
         "revived":len(stale),"escalations":escalations,"actions":actions,"audit":audit,
         "healthy":len(audit)-len(stale)}
    out["publish"]=gh_put("status/%s/freshness.json"%NODE,out)
    if escalations:
        gh_put("status/%s/alert-freshness-%s.json"%(NODE,time.strftime("%Y%m%dT%H%M%SZ",time.gmtime())),
               {"kind":"alert","severity":"high","reason":"source stopped producing / dead timer","detail":escalations,"ts":NOW})
    print("FRESH="+json.dumps({"checked":len(audit),"revived":len(stale),"escalations":escalations}));sys.exit(0)
if __name__=="__main__":main()
