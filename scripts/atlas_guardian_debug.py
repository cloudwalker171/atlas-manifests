#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""One-shot diagnostic: run the guardian meta_guardian.py --once capturing
stdout+stderr+exit code (and timing), publish to status/<node>/guardian-debug.json
so the real crash/timeout reason is visible without SSH. Read-only re: the guardian."""
import os,sys,json,time,base64,subprocess,urllib.request,urllib.error
NODE=os.environ.get("NODE_ID","hetzner")
GUARD="/opt/atlas/guardian/meta_guardian.py"
PY="/opt/atlas/venv/bin/python"
def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"guardian debug","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
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
    t0=time.time(); rc=None; out=err=""; timed_out=False
    try:
        p=subprocess.run([PY,GUARD,"--once"],capture_output=True,text=True,timeout=220)
        rc=p.returncode; out=p.stdout[-4000:]; err=p.stderr[-6000:]
    except subprocess.TimeoutExpired as e:
        timed_out=True; rc="timeout>220s"; out=(e.stdout or "")[-2000:] if e.stdout else ""; err=(e.stderr or "")[-4000:] if e.stderr else ""
    except Exception as e:
        rc="exec_err"; err=str(e)[:500]
    dur=round(time.time()-t0,1)
    res={"kind":"guardian-debug","node":NODE,"ts":int(time.time()),"duration_s":dur,
         "exit":rc,"timed_out":timed_out,"stdout_tail":out,"stderr_tail":err,
         "verdict":"timeout (needs longer TimeoutStartSec)" if timed_out else ("crash (see stderr_tail)" if rc not in (0,) else "ran clean")}
    res["publish"]=gh_put("status/%s/guardian-debug.json"%NODE,res)
    print("GUARDIAN_DEBUG="+json.dumps({"exit":rc,"dur":dur,"timed_out":timed_out,"err":err[:200]}));sys.exit(0)
if __name__=="__main__": main()
