#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ATLAS WATCHDOG -- the watcher-of-watchers (who watches the watchman).
The Meta watcher drops a local heartbeat (/var/lib/atlas/watcher.alive) each run.
This INDEPENDENT unit (own timer + Restart=on-failure) checks that heartbeat's
freshness; if STALE it revives the watcher (reset-failed + restart), bounded with
park-after-N + anti-thrash. It also revives key supervisors (ops-advisor, the
improve engine) if dead. The watcher reciprocally checks THIS watchdog (mutual),
and both carry systemd Restart=on-failure -> no single point of failure.
Publishes status/<node>/watchdog.json. --selftest simulates a dead watcher."""
import os, sys, json, time, base64, subprocess, urllib.request, urllib.error
NODE = os.environ.get("NODE_ID", "hetzner")
ALIVE = os.environ.get("WATCHER_ALIVE", "/var/lib/atlas/watcher.alive")
STALE_SEC = int(os.environ.get("WATCHDOG_STALE_SEC", "210"))      # watcher runs /75s; 2.8 missed = stale
MAX_REVIVES = int(os.environ.get("WATCHDOG_MAX_REVIVES", "5"))     # park after N within the window
WINDOW_SEC = int(os.environ.get("WATCHDOG_WINDOW_SEC", "3600"))
STATE = "/var/lib/atlas/watchdog_state.json"
WATCHED = ["atlas-watcher.service", "atlas-ops-advisor.service", "brain-improve-hourly.service"]

def run(cmd, t=15):
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=t); return p.returncode, (p.stdout or "").strip()
    except Exception as e:
        return 99, str(e)[:80]

def gh_put(path, obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"watchdog","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
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

def decide(alive_age, revives_in_window):
    """PURE LOGIC (selftested). Returns 'revive' | 'parked' | 'ok'."""
    if alive_age is None or alive_age > STALE_SEC:
        if revives_in_window >= MAX_REVIVES:
            return "parked"      # anti-thrash: stop reviving, escalate
        return "revive"
    return "ok"

def selftest():
    ok=True
    def chk(n,c):
        nonlocal ok; print(("  ok  " if c else "  FAIL")+" "+n); ok=ok and c
    chk("fresh heartbeat -> ok", decide(30,0)=="ok")
    chk("STALE heartbeat -> revive (dead watcher revived)", decide(STALE_SEC+50,0)=="revive")
    chk("missing heartbeat (None) -> revive", decide(None,0)=="revive")
    chk("stale but over revive cap -> parked (anti-thrash)", decide(999,MAX_REVIVES)=="parked")
    chk("boundary just-fresh -> ok", decide(STALE_SEC-1,0)=="ok")
    print("WATCHDOG SELFTEST","PASS" if ok else "FAIL"); return 0 if ok else 1

def main():
    if "--selftest" in sys.argv: sys.exit(selftest())
    now=time.time()
    try: age=now-os.path.getmtime(ALIVE)
    except Exception: age=None
    try: st=json.load(open(STATE))
    except Exception: st={}
    # prune revive timestamps outside the window
    revs=[t for t in st.get("revives",[]) if now-t < WINDOW_SEC]
    action=decide(age, len(revs)); revived=[]
    if action=="revive":
        run(["systemctl","reset-failed","atlas-watcher.service"]); rc,_=run(["systemctl","restart","atlas-watcher.service"])
        revs.append(now); revived.append({"unit":"atlas-watcher.service","rc":rc})
    # also revive other dead key supervisors (bounded, not the lifelines)
    sup={}
    for u in WATCHED:
        rc,so=run(["systemctl","is-active",u]); sup[u]=so
        if u!="atlas-watcher.service" and so=="failed":
            run(["systemctl","reset-failed",u]); r2,_=run(["systemctl","restart",u]); revived.append({"unit":u,"rc":r2})
    out={"kind":"watchdog","node":NODE,"ts":int(now),"watcher_heartbeat_age_s":round(age,1) if age is not None else None,
         "stale_threshold_s":STALE_SEC,"action":action,"revived":revived,
         "revives_in_window":len(revs),"max_revives":MAX_REVIVES,"supervisors":sup,
         "escalate": action=="parked",
         "note":"watcher-of-watchers: revives the Meta watcher if its heartbeat goes stale; park-after-%d anti-thrash; mutual (watcher checks this watchdog); both carry systemd Restart=on-failure -> no single point of failure."%MAX_REVIVES}
    try:
        os.makedirs(os.path.dirname(STATE),exist_ok=True); json.dump({"revives":revs,"ts":now},open(STATE,"w"))
    except Exception: pass
    out["publish"]=gh_put("status/%s/watchdog.json"%NODE,out)
    print("WATCHDOG="+json.dumps({"action":action,"age":out["watcher_heartbeat_age_s"],"revived":[r["unit"] for r in revived],"supervisors":sup}));sys.exit(0)

if __name__=="__main__": main()
