#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Daily executive digest -> digest.json: business_total + delta, rates, sources
live, prospects/qualified, what the engine/ops-advisor/frontier recommend,
escalations. Reads the published feeds (no DB needed). Read-only."""
import os, sys, json, time, base64, urllib.request, urllib.error
NODE=os.environ.get("NODE_ID","hetzner")
RAW=os.environ.get("ATLAS_RAW_BASE","https://raw.githubusercontent.com/cloudwalker171/atlas-manifests/main/status/hetzner")
STATE="/var/lib/atlas/digest_state.json"
def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"digest","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
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
def fetch(name):
    try:
        req=urllib.request.Request("%s/%s?cb=%d"%(RAW,name,int(time.time())),headers={"Cache-Control":"no-cache","User-Agent":"atlas"})
        return json.load(urllib.request.urlopen(req,timeout=12))
    except Exception: return {}
def main():
    rate=fetch("rate-stats.json"); hs=fetch("hunter-stats.json"); ops=fetch("ops-recommendations.json")
    rep=fetch("improvement-report-latest.json"); guard=fetch("guardian-latest.json"); bk=fetch("backup.json")
    pg=(guard.get("pg") or {}); biz=pg.get("business_total")
    try: st=json.load(open(STATE))
    except Exception: st={}
    delta=(biz-st["biz"]) if (biz is not None and st.get("biz") is not None) else None
    units=guard.get("units",{}); active=len([k for k,v in units.items() if v=='active' and k!='_failed'])
    out={"kind":"daily-digest","node":NODE,"ts":int(time.time()),"date":time.strftime("%Y-%m-%d"),
      "headline":{"business_total":biz,"delta_since_last_digest":delta,"queue_pending":(rate.get("queue") or {}).get("pending"),
                  "combined_enrich_per_min":(rate.get("enrich_combined") or {}).get("per_min"),
                  "interserver_per_min":((rate.get("by_box") or {}).get("interserver") or {}).get("per_min"),
                  "units_active":"%d/%d"%(active,len([k for k in units if k!='_failed']))},
      "prospects":{"total":hs.get("prospects_total"),"qualified":hs.get("qualified"),"leads_per_hour":hs.get("leads_per_hour")},
      "engine":{"last_pass":rep.get("pass"),"ideas":(rep.get("counts") or {}).get("total_ideas")},
      "ops_recommendations":[r.get("title") for r in ops.get("recommendations",[])][:5],
      "escalations":[r.get("title") for r in ops.get("escalate",[])] + ([] if not rep.get("needs_human_decisions") else [i.get("title") for i in rep.get("needs_human_decisions",[])]),
      "backup":{"retained_dumps":bk.get("retained_dumps"),"restore_tested":bk.get("restore_tested"),"off_box":(bk.get("off_box_dr") or {}).get("reachable")},
      "note":"auto daily executive summary; published once/day so the operator is always informed without asking."}
    try:
        os.makedirs(os.path.dirname(STATE),exist_ok=True); json.dump({"biz":biz,"ts":int(time.time())},open(STATE,"w"))
    except Exception: pass
    out["publish"]=gh_put("status/%s/digest.json"%NODE,out)
    print("DIGEST="+json.dumps({"biz":biz,"delta":delta,"units":out["headline"]["units_active"],"recs":out["ops_recommendations"]}));sys.exit(0)
if __name__=="__main__": main()
