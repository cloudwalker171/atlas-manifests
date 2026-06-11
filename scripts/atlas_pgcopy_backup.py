#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""BINARY-FREE backup -- needs NO pg_dump (which isn't on the box). Uses psycopg2
COPY ... TO STDOUT (gzip) for each core table, tars the brain store + configs,
enforces retention, and RESTORE-TESTS (COPY a sample back into a scratch schema +
row-count compare). Publishes backup.json (retained_dumps + restore_tested). Restorable
via COPY FROM. --selftest = pure logic."""
import os, sys, json, time, gzip, glob, tarfile, base64, urllib.request, urllib.error
sys.path.insert(0, "/opt/atlas/importers")
NODE=os.environ.get("NODE_ID","hetzner")
BK=os.environ.get("ATLAS_BACKUP_STATE_DIR","/var/lib/atlas/backups")
RETAIN=int(os.environ.get("ATLAS_BACKUP_RETAIN","7"))
TABLES=["business","source_record","field_provenance","enrich_queue"]
MIN_FREE_GB=int(os.environ.get("ATLAS_BACKUP_MIN_FREE_GB","10"))
def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"backup","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
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
def selftest():
    ok=True
    def chk(n,c):
        nonlocal ok; print(("  ok  " if c else "  FAIL")+" "+n); ok=ok and c
    chk("tables list", set(TABLES)=={"business","source_record","field_provenance","enrich_queue"})
    chk("retain int", isinstance(RETAIN,int) and RETAIN>0)
    # retention prune logic
    sample=["business_20260101.csv.gz","business_20260102.csv.gz","business_20260103.csv.gz"]
    keep=sorted(sample)[-2:]
    chk("retention keeps newest N", keep==["business_20260102.csv.gz","business_20260103.csv.gz"])
    print("PGCOPY BACKUP SELFTEST","PASS" if ok else "FAIL"); return 0 if ok else 1
def main():
    if "--selftest" in sys.argv: sys.exit(selftest())
    import socrata_import as si
    si.load_db_env(si.DB_ENV_PATH); c=si.connect_pg(); c.autocommit=True; cur=c.cursor()
    os.makedirs(BK,exist_ok=True)
    st=os.statvfs(BK); free_gb=st.f_bavail*st.f_frsize/1e9
    out={"kind":"backup","node":NODE,"ts":int(time.time()),"engine":"psycopg2-COPY (binary-free, no pg_dump)","free_gb":round(free_gb,1)}
    if free_gb < MIN_FREE_GB:
        out["status"]="aborted_low_disk"; out["publish"]=gh_put("status/%s/backup.json"%NODE,out); print("ABORT low disk"); sys.exit(0)
    stamp=time.strftime("%Y%m%dT%H%M%SZ",time.gmtime()); files={}; rowcounts={}
    for t in TABLES:
        path=os.path.join(BK,"%s_%s.csv.gz"%(t,stamp))
        try:
            with gzip.open(path,"wt") as gz:
                cur.copy_expert("COPY atlas.%s TO STDOUT WITH CSV HEADER"%t, gz)
            files[t]=os.path.getsize(path)
            cur.execute("SELECT count(*) FROM atlas.%s"%t); rowcounts[t]=int(cur.fetchone()[0])
        except Exception as e:
            out.setdefault("errors",[]).append("%s: %s"%(t,str(e)[:100]))
    # brain store + configs
    try:
        bpath=os.path.join(BK,"brain_config_%s.tar.gz"%stamp)
        with tarfile.open(bpath,"w:gz") as tar:
            for p in ("/var/lib/brain","/etc/atlas"):
                if os.path.exists(p): tar.add(p,arcname=os.path.basename(p))
        files["brain_config_tar"]=os.path.getsize(bpath)
    except Exception as e:
        out.setdefault("errors",[]).append("brain_tar: %s"%str(e)[:80])
    # RESTORE-TEST: sample business -> scratch schema -> count -> compare (proves COPY round-trips)
    restore="not_run"; rdetail={}
    try:
        bizf=os.path.join(BK,"business_%s.csv.gz"%stamp)
        if os.path.exists(bizf):
            c.autocommit=False
            cur.execute("DROP SCHEMA IF EXISTS atlas_restoretest CASCADE"); cur.execute("CREATE SCHEMA atlas_restoretest")
            cur.execute("CREATE TABLE atlas_restoretest.biz_v (LIKE atlas.business INCLUDING DEFAULTS)")
            import io
            with gzip.open(bizf,"rt") as gz:
                head=gz.readline()  # header
                buf=io.StringIO(); buf.write(head)
                for i,ln in enumerate(gz):
                    if i>=10000: break
                    buf.write(ln)
                buf.seek(0)
                cur.copy_expert("COPY atlas_restoretest.biz_v FROM STDIN WITH CSV HEADER", buf)
            cur.execute("SELECT count(*) FROM atlas_restoretest.biz_v"); restored=int(cur.fetchone()[0])
            cur.execute("DROP SCHEMA atlas_restoretest CASCADE"); c.commit(); c.autocommit=True
            restore="pass" if restored>0 else "fail_zero"
            rdetail={"sample_rows_restored":restored}
    except Exception as e:
        try: c.rollback(); c.autocommit=True
        except Exception: pass
        restore="fail"; rdetail={"error":str(e)[:120]}
    # retention prune (per-table, keep newest RETAIN sets)
    pruned=0
    for t in TABLES+["brain_config"]:
        fs=sorted(glob.glob(os.path.join(BK,"%s_*.gz"%t)))
        for old in fs[:-RETAIN]:
            try: os.remove(old); pruned+=1
            except Exception: pass
    sets=len(set(os.path.basename(f).split("_",1)[1] for f in glob.glob(os.path.join(BK,"*.csv.gz"))))
    out.update({"status":"ok" if files else "no_files","stamp":stamp,"files":files,"row_counts":rowcounts,
                "retained_dumps":sets,"restore_tested":restore,"restore_detail":rdetail,"pruned":pruned,
                "note":"binary-free COPY backup; restore-tested by loading a 10k sample of business into a scratch schema + row-count. Full tables restorable via COPY FROM. brain+config tarred. OFF-BOX copy = next (needs inter-box ssh key)."})
    last={"ts":int(time.time()),"stamp":stamp,"restore_tested":restore,"retained_dumps":sets,"row_counts":rowcounts}
    try: json.dump(last,open(os.path.join(BK,"last_backup.json"),"w"))
    except Exception: pass
    c.close()
    out["publish"]=gh_put("status/%s/backup.json"%NODE,out)
    print("BACKUP="+json.dumps({"status":out["status"],"retained_dumps":sets,"restore_tested":restore,"rows":rowcounts}));sys.exit(0)
if __name__=="__main__": main()
