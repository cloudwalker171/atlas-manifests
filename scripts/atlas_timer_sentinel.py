#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PROACTIVE 60s TIMER SENTINEL. Long-running, watched, Restart=always. EVERY cycle
(~60s) it verifies -- for every Tier-1 prime source AND every atlas-*.timer -- that
the timer is ENABLED and has a valid future NextElapse. A timer that SHOULD be enabled
but is disabled/inactive/unscheduled is the EXACT NRD failure mode (a oneshot whose
schedule died is NOT a 'failed' unit) -> caught within ONE cycle, re-enabled+started
IMMEDIATELY, escalated loudly. This is the PRIMARY detector (schedule-death in ~1 min);
the 26h output-staleness SLA (atlas_freshness_monitor) is only the final backstop.
--selftest proves the decision core catches a disabled timer; the deploy also runs a
LIVE disable->revive proof against a throwaway probe timer."""
import os,sys,json,time,base64,subprocess,urllib.request,urllib.error
NODE=os.environ.get("NODE_ID","hetzner")
INTERVAL=int(os.environ.get("SENTINEL_INTERVAL","60"))
# Tier-1 prime birth feeds that must ALWAYS be scheduled
TIER1_TIMERS=["atlas-nrd.timer","atlas-sos.timer","atlas-ct.timer"]
PROBE="atlas-sentinel-probe.timer"   # throwaway; used for the live disable->revive proof
# Timers intentionally disabled -- the sentinel must NEVER revive these:
EXCLUDE=set(filter(None,os.environ.get("SENTINEL_EXCLUDE","atlas-backup-status.timer").split(",")))

def assess(enabled, scheduled):
    """PURE core (selftest-driven). A timer that should run must be enabled AND scheduled."""
    if enabled is False:   return ("DEAD_TIMER_DISABLED", True)
    if scheduled is False: return ("DEAD_TIMER_NOT_SCHEDULED", True)
    return ("OK", False)

def is_scheduled(active, next_realtime, next_monotonic):
    """PURE: a timer is ARMED if active(waiting) OR has a next-elapse on EITHER clock.
    Monotonic (OnUnitActiveSec) timers leave NextElapseUSecRealtime empty -> MUST check
    NextElapseUSecMonotonic too, else healthy monotonic timers false-flag as dead."""
    def _set(v): return bool(v) and v not in ("","0","infinity","n/a")
    return bool(active) or _set(next_realtime) or _set(next_monotonic)

def sh(a,t=15):
    try:
        p=subprocess.run(a,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=t)
        return p.returncode,(p.stdout or b"").decode("utf-8","replace").strip(),(p.stderr or b"").decode("utf-8","replace").strip()
    except Exception as e:return 1,"",str(e)

def discover_timers():
    rc,out,_=sh(["bash","-lc","systemctl list-unit-files 'atlas-*.timer' --no-legend 2>/dev/null | awk '{print $1}'"])
    ts=set(x for x in out.split() if x.endswith(".timer"))
    ts.update(TIER1_TIMERS)
    return sorted(ts)

def state(tmr):
    _,en,_=sh(["systemctl","is-enabled",tmr]);enabled=en.strip() in ("enabled","enabled-runtime")
    _,sj,_=sh(["systemctl","show",tmr,"-p","NextElapseUSecRealtime","-p","NextElapseUSecMonotonic","-p","LoadState","-p","ActiveState"])
    kv=dict(l.split("=",1) for l in sj.splitlines() if "=" in l)
    active=kv.get("ActiveState")=="active"
    scheduled=is_scheduled(active,kv.get("NextElapseUSecRealtime"),kv.get("NextElapseUSecMonotonic"))
    return enabled,scheduled,kv.get("LoadState")=="loaded",kv.get("ActiveState")

def revive(tmr):
    svc=tmr[:-6]+".service"
    sh(["systemctl","daemon-reload"]);r=sh(["systemctl","enable","--now",tmr]);sh(["systemctl","reset-failed",svc]);sh(["systemctl","start","--no-block",svc])
    return r[0]

def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO");api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo:return "skip"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception:sha=None
    body={"message":"sentinel","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
    if sha:body["sha"]=sha
    for _ in range(3):
        try:
            r=urllib.request.Request(url,data=json.dumps(body).encode(),method="PUT",headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas","Content-Type":"application/json"});urllib.request.urlopen(r,timeout=25);return "put_ok"
        except urllib.error.HTTPError as e:
            if e.code==409:
                try:
                    rq=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});body["sha"]=json.load(urllib.request.urlopen(rq,timeout=20)).get("sha")
                except Exception:pass
            else:return "http_%s"%e.code
        except Exception:time.sleep(1)
    return "exhausted"

def one_cycle(cyc):
    revived=[];audit=[]
    for tmr in discover_timers():
        en,sch,loaded,act=state(tmr)
        if not loaded: continue
        if tmr in EXCLUDE:
            audit.append({"timer":tmr,"tier":3,"enabled":en,"scheduled":sch,"verdict":"INTENTIONALLY_DISABLED_SKIP"});continue
        verdict,need=assess(en,sch)
        tier=1 if tmr in TIER1_TIMERS else 2
        row={"timer":tmr,"tier":tier,"enabled":en,"scheduled":sch,"active":act,"verdict":verdict}
        if need:
            rc=revive(tmr);row["revived"]=True;row["revive_rc"]=rc;revived.append(tmr)
        audit.append(row)
    out={"kind":"timer-sentinel","node":NODE,"ts":int(time.time()),"cycle":cyc,"interval_s":INTERVAL,
         "timers_checked":len(audit),"revived":revived,"revived_count":len(revived),
         "tier1":[a for a in audit if a["tier"]==1],"audit":audit,
         "detector":"proactive enabled+scheduled (schedule-death caught in <=1 cycle)"}
    gh_put("status/%s/sentinel.json"%NODE,out)
    if revived:
        gh_put("status/%s/alert-sentinel-%s.json"%(NODE,time.strftime("%Y%m%dT%H%M%SZ",time.gmtime())),
               {"kind":"alert","severity":"high","reason":"timer schedule died (NRD failure mode) -> auto-revived in <=1 cycle","revived":revived,"ts":int(time.time())})
    return revived

def selftest():
    cases=[("disabled->revive",assess(False,True),"DEAD_TIMER_DISABLED",True),
           ("unscheduled->revive",assess(True,False),"DEAD_TIMER_NOT_SCHEDULED",True),
           ("healthy->ok",assess(True,True),"OK",False)]
    ok=0
    for n,(st,rev),es,er in cases:
        g=(st==es and rev==er);ok+=g;print(("PASS" if g else "FAIL"),n,"->",st,rev)
    # the false-positive bug the audit exposed: monotonic(OnUnitActiveSec) timers
    sc=[("monotonic waiting armed (was false-flagged)",is_scheduled(True,"","" ),True),
        ("realtime next set",is_scheduled(False,"123","" ),True),
        ("monotonic next set",is_scheduled(False,"","456"),True),
        ("truly dead: inactive+no next",is_scheduled(False,"",""),False),
        ("disabled probe still dead",is_scheduled(False,"0","0"),False)]
    for n,got,exp in sc:
        g=(got==exp);ok+=g;print(("PASS" if g else "FAIL"),n,"->",got);
    tot=len(cases)+len(sc)
    print("SELFTEST %d/%d"%(ok,tot));sys.exit(0 if ok==tot else 1)

def main():
    if "--selftest" in sys.argv: selftest()
    if "--once" in sys.argv:
        r=one_cycle(0);print("CYCLE0 revived=%s"%r);sys.exit(0)
    cyc=0
    while True:
        try: one_cycle(cyc)
        except Exception as e:
            print("sentinel cycle err",e,flush=True)
        cyc+=1; time.sleep(INTERVAL)
if __name__=="__main__": main()
