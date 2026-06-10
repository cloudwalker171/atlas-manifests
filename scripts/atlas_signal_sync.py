#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Mirror the LIVE status JSONs from the repo to the improvement-engine's local
signals dir (/var/lib/atlas/status) so brain.improve generates RICH ideas
(RDAP/CT/favicon/concurrency/monetization) instead of 'restore missing signal'.
The box publishes these to GitHub; this pulls them back to local disk where the
engine reads. Fail-soft per-file; never wedges."""
import os,sys,json,urllib.request,time
NODE=os.environ.get("NODE_ID","hetzner")
DEST=os.environ.get("ATLAS_SIGNALS_DIR","/var/lib/atlas/status")
RAW=os.environ.get("ATLAS_RAW_BASE","https://raw.githubusercontent.com/cloudwalker171/atlas-manifests/main")
SIGNALS=["throughput.json","atlas-metrics.json","qa-hetzner.json","field-audit.json",
         "prospects-truth.json","source-promote.json","nrd.json","batch-canary.json",
         "requeue.json","improve-publish.json"]
def main():
    os.makedirs(DEST,exist_ok=True)
    got={}
    for name in SIGNALS:
        url="%s/status/%s/%s?cb=%d"%(RAW,NODE,name,int(time.time()))
        try:
            req=urllib.request.Request(url,headers={"Cache-Control":"no-cache","User-Agent":"atlas-signal-sync"})
            data=urllib.request.urlopen(req,timeout=15).read()
            json.loads(data)  # validate
            tmp=os.path.join(DEST,name+".tmp"); open(tmp,"wb").write(data); os.replace(tmp,os.path.join(DEST,name))
            got[name]="ok(%db)"%len(data)
        except Exception as e:
            got[name]="skip(%s)"%type(e).__name__
    print("SIGNAL_SYNC="+json.dumps(got));sys.exit(0)
if __name__=="__main__": main()
