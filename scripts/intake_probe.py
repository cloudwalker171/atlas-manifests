#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A.T.L.A.S. -- intake/metrics diagnostic probe (READ-ONLY). Reports the LIVE
atlas.business count + freshness (independent of the metrics exporter cadence),
the socrata source_record counts, the atlas-socrata service/timer run state
(did the incremental run execute? did it insert?), the persisted Socrata
cursors, the importer's last_counts.json, and the metrics-exporter timer health.
Publishes status/<node>/intake-probe.json. NO writes to atlas tables.
"""
import os, sys, json, time, glob, subprocess, base64, urllib.request, urllib.error
NODE = os.environ.get("NODE_ID", "hetzner")

def run(cmd):
    try: return subprocess.run(cmd, capture_output=True, text=True, timeout=25)
    except Exception as ex:
        class R: returncode=99; stdout=""; stderr=str(ex)
        return R()

def sysshow(unit, props):
    p = run(["systemctl","show",unit,"--no-pager"]+sum([["-p",x] for x in props],[]))
    out={}
    for ln in p.stdout.splitlines():
        if "=" in ln: k,v=ln.split("=",1); out[k]=v
    return out

def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN"); repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com"); br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped_no_token"
    url="%s/repos/%s/contents/%s"%(api,repo,path); sha=None
    try:
        req=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas-autopull"})
        sha=json.load(urllib.request.urlopen(req,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"intake probe %s"%NODE,"content":base64.b64encode(json.dumps(obj,indent=2).encode()).decode(),"branch":br}
    if sha: body["sha"]=sha
    req=urllib.request.Request(url,data=json.dumps(body).encode(),method="PUT",headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas-autopull","Content-Type":"application/json"})
    try: urllib.request.urlopen(req,timeout=20); return "put_ok"
    except urllib.error.HTTPError as e: return "put_http_%s"%e.code
    except Exception as e: return "put_err_%s"%type(e).__name__

def main():
    out={"probe":"intake","node":NODE,"ts":int(time.time())}
    # --- DB ---
    try:
        import psycopg2
        e={}
        for ln in open("/etc/atlas/db.env"):
            ln=ln.strip()
            if "=" in ln and not ln.startswith("#"):
                k,v=ln.split("=",1); e[k.strip()]=v.strip().strip("'\"")
        c=psycopg2.connect(host=e.get("PGHOST","localhost"),dbname=e.get("PGDATABASE","tuanichat_atlas"),user=e.get("PGUSER"),password=e.get("PGPASSWORD"),port=e.get("PGPORT","5432"),connect_timeout=10)
        c.autocommit=True; cur=c.cursor()
        def sc(q,a=None):
            cur.execute(q,a); r=cur.fetchone(); return r[0] if r else None
        out["business_count_exact"]=sc("SELECT count(*) FROM atlas.business")
        out["business_reltuples"]=sc("SELECT GREATEST(reltuples::bigint,0) FROM pg_class WHERE oid='atlas.business'::regclass")
        # freshness column on business
        cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_schema='atlas' AND table_name='business' AND data_type LIKE '%timestamp%' ORDER BY column_name""")
        tcols=[r[0] for r in cur.fetchall()]; out["business_ts_cols"]=tcols
        fresh={}
        for col in [x for x in ("created_at","first_seen","inserted_at","updated_at","last_seen","last_updated") if x in tcols]:
            try: fresh[col]=str(sc('SELECT max("%s") FROM atlas.business'%col))
            except Exception as ex: fresh[col]="err"
        out["business_max_ts"]=fresh
        out["sr_chicago"]=sc("SELECT count(*) FROM atlas.source_record WHERE source_code='socrata_chicago'")
        out["sr_nyc"]=sc("SELECT count(*) FROM atlas.source_record WHERE source_code='socrata_nyc'")
        c.close()
    except Exception as ex:
        out["db_error"]=str(ex)[:200]
    # --- socrata service/timer state ---
    out["socrata_service"]=sysshow("atlas-socrata.service",["ActiveState","SubState","Result","ExecMainStatus","ExecMainStartTimestamp","ExecMainExitTimestamp"])
    out["socrata_timer"]=sysshow("atlas-socrata.timer",["ActiveState","LastTriggerUSec","NextElapseUSecRealtime"])
    # --- metrics exporter unit (try common names) ---
    for u in ("atlas-metrics.timer","atlas-metrics-export.timer","atlas-metrics.service"):
        st=sysshow(u,["ActiveState","SubState","LastTriggerUSec","ExecMainStartTimestamp"])
        if st.get("ActiveState") and st.get("ActiveState")!="inactive":
            out["metrics_unit"]=u; out["metrics_unit_state"]=st; break
    else:
        out["metrics_unit"]="(none active among tried)"
    # --- cursors + last_counts ---
    cur_files={}
    for f in glob.glob("/var/lib/atlas/socrata/cursor_*.txt"):
        try: cur_files[os.path.basename(f)]=open(f).read().strip()
        except Exception: cur_files[os.path.basename(f)]="err"
    out["socrata_cursors"]=cur_files or "(none yet)"
    try: out["last_counts"]=json.load(open("/var/lib/atlas/autopull/last_counts.json"))
    except Exception as ex: out["last_counts_err"]=str(ex)[:120]
    # tail of socrata journal (last run outcome)
    jr=run(["journalctl","-u","atlas-socrata.service","-n","12","--no-pager","-o","cat"])
    out["socrata_journal_tail"]=jr.stdout[-1500:] if jr.stdout else jr.stderr[:300]
    out["publish"]=gh_put("status/%s/intake-probe.json"%NODE,out)
    print("INTAKE_PROBE="+json.dumps(out)); sys.exit(0)

if __name__=="__main__": main()
