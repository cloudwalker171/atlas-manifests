#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Publishes the Meta-Brain improvement-engine reports to the GitHub status feed
so the dashboards + daily watchdog (and the operator) can see the ideas. The
engine writes improvement-report-{hourly,daily,latest}.json to a local dir; this
helper waits for a fresh report then gh_puts the three files + a compact
improve-publish.json summary. Read-only w.r.t. the engine; idempotent."""
import os,sys,json,time,base64,urllib.request,urllib.error
NODE=os.environ.get("NODE_ID","hetzner")
RPTDIR=os.environ.get("ATLAS_IMPROVE_REPORTS","/var/lib/atlas/status")
FILES=["improvement-report-latest.json","improvement-report-hourly.json","improvement-report-daily.json"]
def gh_put(path,raw):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"improve report","content":base64.b64encode(raw).decode(),"branch":br}
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
    latest=os.path.join(RPTDIR,"improvement-report-latest.json")
    deadline=time.time()+80
    while time.time()<deadline:
        if os.path.exists(latest) and (time.time()-os.path.getmtime(latest))<3600: break
        time.sleep(4)
    published={}; summary={"kind":"improve-publish","node":NODE,"ts":int(time.time())}
    for fn in FILES:
        p=os.path.join(RPTDIR,fn)
        if os.path.exists(p):
            published[fn]=gh_put("status/%s/%s"%(NODE,fn), open(p,"rb").read())
    summary["published"]=published
    # compact idea-counts for quick visibility
    try:
        d=json.load(open(latest))
        ideas=d.get("ideas") or d.get("ranked_ideas") or []
        summary["pass_kind"]=d.get("pass") or d.get("pass_kind")
        summary["idea_count"]=len(ideas)
        summary["auto_safe"]=sum(1 for i in ideas if i.get("auto_class")=="auto_safe")
        summary["needs_human"]=sum(1 for i in ideas if i.get("auto_class")=="needs_human")
        summary["top_titles"]=[i.get("title") for i in ideas[:8]]
    except Exception as e:
        summary["report_read_err"]=str(e)[:80]
    summary["publish_self"]=gh_put("status/%s/improve-publish.json"%NODE, json.dumps(summary,indent=2).encode())
    print("IMPROVE_PUBLISH="+json.dumps({k:summary[k] for k in summary if k!="top_titles"}));sys.exit(0)
if __name__=="__main__": main()
