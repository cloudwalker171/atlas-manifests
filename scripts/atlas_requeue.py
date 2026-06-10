#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""BOUNDED requeue of real records that need depth (missing website OR email) back
to 'pending' for re-enrichment. Creates genuine write-load to test the batched
writer AND lifts website/email coverage. Bounded (default 200k) — never the whole
corpus. Reversible (rows re-enrich to done). Prints counts; publishes requeue.json."""
import os,sys,json,time,base64,urllib.request,urllib.error
sys.path.insert(0,"/opt/atlas/importers")
N=int(os.environ.get("ATLAS_REQUEUE_N", sys.argv[1] if len(sys.argv)>1 else "200000"))
NODE=os.environ.get("NODE_ID","hetzner")
def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"requeue","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
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
    import socrata_import as si
    si.load_db_env(si.DB_ENV_PATH)
    c=si.connect_pg(); c.autocommit=True; cur=c.cursor()
    def dist():
        cur.execute("SELECT status,count(*) FROM atlas.enrich_queue GROUP BY status"); return {k:int(v) for k,v in cur.fetchall()}
    before=dist()
    cur.execute("""UPDATE atlas.enrich_queue q SET status='pending', locked_by=NULL, locked_at=NULL
                   FROM (SELECT qq.id FROM atlas.enrich_queue qq
                         JOIN atlas.business b ON b.id=qq.business_id
                         WHERE qq.status='done' AND (b.website IS NULL OR b.website='' OR b.email IS NULL OR b.email='')
                         LIMIT %s) s
                   WHERE q.id=s.id""",(N,))
    requeued=cur.rowcount
    after=dist()
    out={"kind":"requeue","node":NODE,"ts":int(time.time()),"requested":N,"requeued":requeued,
         "queue_before":before,"queue_after":after,
         "note":"bounded requeue of records missing website/email -> pending for re-enrichment (refills queue to test batched writer + lifts coverage). Reversible."}
    out["publish"]=gh_put("status/%s/requeue.json"%NODE,out)
    print("REQUEUE="+json.dumps({"requeued":requeued,"queue_after":after}));sys.exit(0)
if __name__=="__main__": main()
