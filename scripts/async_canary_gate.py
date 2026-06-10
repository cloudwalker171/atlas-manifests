#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Async-canary gate (idempotent, never wedges). Runs the async engine --selftest,
captures the box Python version + output, publishes status/hetzner/async-canary.json,
and starts atlas-enrich-async@1 ONLY if the selftest passes. Always exits 0 so a
selftest failure does not block the deploy queue."""
import os, sys, json, time, subprocess, base64, urllib.request, urllib.error
ENGINE = "/opt/atlas/importers/atlas_enrich_async.py"
NODE = os.environ.get("NODE_ID", "hetzner")

def gh_put(path, obj):
    tok=os.environ.get("STATUS_TOKEN"); repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com"); br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped_no_token"
    url="%s/repos/%s/contents/%s"%(api,repo,path); sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas-autopull"})
        sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"async canary %s"%NODE,"content":base64.b64encode(json.dumps(obj,indent=2).encode()).decode(),"branch":br}
    if sha: body["sha"]=sha
    import time as _t
    last="?"
    for _att in range(5):
        try:
            r=urllib.request.Request(url,data=json.dumps(body).encode(),method="PUT",headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas-autopull","Content-Type":"application/json"})
            urllib.request.urlopen(r,timeout=25); return "put_ok(att%d)"%(_att+1)
        except urllib.error.HTTPError as e:
            last="put_http_%s"%e.code
            if e.code==409:  # sha race -> refetch sha
                try:
                    rq=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas-autopull"})
                    body["sha"]=json.load(urllib.request.urlopen(rq,timeout=20)).get("sha")
                except Exception: pass
        except Exception as e:
            last="put_err_%s"%type(e).__name__
        _t.sleep(3)
    return last

def main():
    out={"step":"async_canary_gate","node":NODE,"py_version":sys.version,"py_exe":sys.executable,"ts":int(time.time())}
    try:
        p=subprocess.run([sys.executable, ENGINE, "--selftest"], capture_output=True, text=True, timeout=150)
        out["selftest_rc"]=p.returncode
        out["selftest_tail"]=((p.stdout or "")+("\n--STDERR--\n"+p.stderr if p.stderr else ""))[-2500:]
    except Exception as e:
        out["selftest_rc"]=-1; out["selftest_tail"]="gate exception: "+repr(e)
    if out.get("selftest_rc")==0:
        r=subprocess.run(["systemctl","start","atlas-enrich-async@1.service"],capture_output=True,text=True)
        out["canary_start_rc"]=r.returncode; out["canary_started"]=(r.returncode==0)
        # quick post-start liveness + this process's pg footprint
        a=subprocess.run(["systemctl","is-active","atlas-enrich-async@1.service"],capture_output=True,text=True)
        out["canary_active"]=a.stdout.strip()
    else:
        out["canary_started"]=False
    out["publish"]=gh_put("status/%s/async-canary.json"%NODE,out)
    print("ASYNC_CANARY_GATE="+json.dumps(out)); sys.exit(0)

if __name__=="__main__": main()
