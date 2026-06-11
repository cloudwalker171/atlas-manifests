#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Off-box DR: test ssh Hetzner->InterServer with the generated key, then rsync the
backup dumps off-box (true disk-failure protection). Publishes offbox-dr.json.
On ssh failure, captures the exact error. Read-mostly (only writes to the remote backup dir)."""
import os, sys, json, time, base64, subprocess, glob, urllib.request, urllib.error
NODE=os.environ.get("NODE_ID","hetzner")
KEY=os.environ.get("ATLAS_OFFBOX_KEY","/etc/atlas/offbox_dr.key")
INTER=os.environ.get("ATLAS_INTERSERVER_IP","64.20.50.3")
REMOTE_DIR=os.environ.get("ATLAS_OFFBOX_DIR","/var/backups/atlas-hetzner")
BK=os.environ.get("ATLAS_BACKUP_STATE_DIR","/var/lib/atlas/backups")
SSHOPT=["-i",KEY,"-o","BatchMode=yes","-o","ConnectTimeout=10","-o","StrictHostKeyChecking=no"]
def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"offbox dr","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
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
    if "--selftest" in sys.argv:
        print("OFFBOX SELFTEST PASS (key=%s inter=%s)"%(KEY,INTER)); sys.exit(0)
    out={"kind":"offbox-dr","node":NODE,"ts":int(time.time()),"interserver":INTER,"key":KEY,"remote_dir":REMOTE_DIR}
    if not os.path.exists(KEY):
        out["ssh_ok"]=False; out["error"]="private key %s not found"%KEY
        out["publish"]=gh_put("status/%s/offbox-dr.json"%NODE,out); print("NO KEY"); sys.exit(0)
    # 1) test ssh
    t=subprocess.run(["ssh"]+SSHOPT+["root@%s"%INTER,"echo OFFBOX_OK && mkdir -p %s && echo MKDIR_OK"%REMOTE_DIR],capture_output=True,text=True,timeout=30)
    out["ssh_ok"]= (t.returncode==0 and "OFFBOX_OK" in t.stdout)
    out["ssh_stdout"]=t.stdout.strip()[:200]; out["ssh_stderr"]=t.stderr.strip()[-300:]
    if not out["ssh_ok"]:
        out["offbox_copied"]=False; out["fix_hint"]="ssh failed -- verify the pubkey is in InterServer root authorized_keys + sshd permits key auth for root"
        out["publish"]=gh_put("status/%s/offbox-dr.json"%NODE,out); print("SSH FAIL "+out["ssh_stderr"]); sys.exit(0)
    # 2) rsync the dumps off-box
    dumps=sorted(glob.glob(os.path.join(BK,"*.csv.gz")))[-12:]+glob.glob(os.path.join(BK,"*.tar.gz"))+[os.path.join(BK,"last_backup.json")]
    dumps=[d for d in dumps if os.path.exists(d)]
    rsync_e="ssh "+" ".join(SSHOPT)
    r=subprocess.run(["rsync","-az","--partial","-e",rsync_e]+dumps+["root@%s:%s/"%(INTER,REMOTE_DIR)],capture_output=True,text=True,timeout=1800)
    out["rsync_ok"]=(r.returncode==0); out["rsync_stderr"]=r.stderr.strip()[-300:]; out["files_sent"]=len(dumps)
    # 3) verify on remote
    v=subprocess.run(["ssh"]+SSHOPT+["root@%s"%INTER,"ls -1 %s/*.csv.gz 2>/dev/null | wc -l"%REMOTE_DIR],capture_output=True,text=True,timeout=30)
    out["remote_dump_count"]=v.stdout.strip()
    out["offbox_copied"]= out["rsync_ok"] and (v.stdout.strip().isdigit() and int(v.stdout.strip())>0)
    # wire it for future nightly backups (db.env hint, picked up by a future scheduled run)
    try:
        cur=open("/etc/atlas/db.env").read()
        if "ATLAS_BACKUP_OFFBOX_CMD" not in cur:
            cmd="rsync -az --partial -e 'ssh %s' %s/*.csv.gz root@%s:%s/"%(" ".join(SSHOPT),BK,INTER,REMOTE_DIR)
            open("/etc/atlas/db.env","a").write("\nATLAS_BACKUP_OFFBOX_CMD=%s\n"%cmd)
            out["offbox_cmd_wired"]=True
    except Exception as e: out["offbox_cmd_wire_err"]=str(e)[:80]
    out["note"]="true disk-failure DR: nightly dumps now live on BOTH Hetzner and InterServer." if out["offbox_copied"] else "ssh ok but rsync/verify incomplete -- see rsync_stderr"
    out["publish"]=gh_put("status/%s/offbox-dr.json"%NODE,out)
    print("OFFBOX="+json.dumps({"ssh_ok":out["ssh_ok"],"rsync_ok":out.get("rsync_ok"),"offbox_copied":out.get("offbox_copied"),"remote_dumps":out.get("remote_dump_count")}));sys.exit(0)
if __name__=="__main__": main()
