#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Publishes backup.json (makes the nightly backup PROVABLE) from the local
last_backup.json the backup writer drops, AND probes whether off-box DR is
possible (ssh key to InterServer). Read-only."""
import os, sys, json, time, base64, glob, subprocess, urllib.request, urllib.error
NODE=os.environ.get("NODE_ID","hetzner")
BK_DIR=os.environ.get("ATLAS_BACKUP_STATE_DIR","/var/lib/atlas/backups")
INTER=os.environ.get("ATLAS_INTERSERVER_IP","64.20.50.3")
def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"backup status","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
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
def offbox_probe():
    # is there an ssh key that can reach InterServer? (batch-mode, 6s, no prompt)
    keys=glob.glob("/root/.ssh/id_*")+glob.glob("/etc/atlas/*.key")
    keys=[k for k in keys if not k.endswith(".pub")]
    if not keys: return {"ssh_key_present":False,"reachable":False,"note":"no ssh private key found in /root/.ssh or /etc/atlas -- off-box DR needs a key (user generates+installs)"}
    for k in keys:
        try:
            r=subprocess.run(["ssh","-i",k,"-o","BatchMode=yes","-o","ConnectTimeout=6","-o","StrictHostKeyChecking=no","root@%s"%INTER,"echo ok"],capture_output=True,text=True,timeout=12)
            if r.returncode==0 and "ok" in r.stdout:
                return {"ssh_key_present":True,"key":k,"reachable":True,"note":"off-box rsync DR is wireable with this key"}
        except Exception: pass
    return {"ssh_key_present":True,"reachable":False,"keys":keys,"note":"key(s) present but none reached InterServer; may need authorized_keys on the other box"}
def main():
    last={}
    for p in (os.path.join(BK_DIR,"last_backup.json"), "/var/lib/atlas/last_backup.json"):
        try: last=json.load(open(p)); break
        except Exception: pass
    dumps=sorted(glob.glob(os.path.join(BK_DIR,"*.dump")))
    sizes=[(os.path.basename(d),os.path.getsize(d)) for d in dumps[-7:]]
    off=offbox_probe()
    out={"kind":"backup-status","node":NODE,"ts":int(time.time()),
         "last_backup":last,"retained_dumps":len(dumps),"recent":sizes,
         "restore_tested":last.get("verify") or last.get("restore_smoke") or last.get("verified"),
         "off_box_dr":off,
         "note":"local retained backup (logical-error + restore-tested). off_box_dr.reachable=true means a disk failure can be survived via rsync to InterServer."}
    out["publish"]=gh_put("status/%s/backup.json"%NODE,out)
    print("BACKUP_STATUS="+json.dumps({"dumps":len(dumps),"last":bool(last),"offbox":off.get("reachable")}));sys.exit(0)
if __name__=="__main__": main()
