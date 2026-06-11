#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Channel-1 SAME-DAY fresh-business counter. Queries atlas.business.first_seen
directly (NRD-log-independent) -> real same-day counts: today, yesterday, 7d trend,
split by source (NRD newly-registered domains vs SoS/CT new-business births).
Publishes channel1.json."""
import os,sys,json,time,base64,urllib.request,urllib.error
sys.path.insert(0,"/opt/atlas/importers")
NODE=os.environ.get("NODE_ID","hetzner")
def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO");api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo:return "skip"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception:sha=None
    body={"message":"channel1","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
    if sha:body["sha"]=sha
    for _ in range(4):
        try:
            r=urllib.request.Request(url,data=json.dumps(body).encode(),method="PUT",headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas","Content-Type":"application/json"});urllib.request.urlopen(r,timeout=25);return "put_ok"
        except urllib.error.HTTPError as e:
            if e.code==409:
                try:
                    rq=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});body["sha"]=json.load(urllib.request.urlopen(rq,timeout=20)).get("sha")
                except Exception:pass
            else:return "http_%s"%e.code
        except Exception:time.sleep(2)
    return "exhausted"
def main():
    import socrata_import as si
    si.load_db_env(si.DB_ENV_PATH);c=si.connect_pg();c.autocommit=True;cur=c.cursor()
    out={"kind":"channel1","node":NODE,"ts":int(time.time())}
    # detect the freshness column
    col=None
    for cand in ("first_seen","created_at","inserted_at","first_observed"):
        try:
            cur.execute("SELECT %s FROM atlas.business LIMIT 1"%cand);col=cand;break
        except Exception:c.rollback() if False else None
        try:c.rollback()
        except Exception:pass
    if not col:
        out["error"]="no freshness column on atlas.business";out["publish"]=gh_put("status/%s/channel1.json"%NODE,out);print(json.dumps(out));sys.exit(0)
    out["freshness_col"]=col
    def q1(sql,args=()):
        try:cur.execute(sql,args);r=cur.fetchone();return int(r[0]) if r and r[0] is not None else 0
        except Exception as e:
            try:c.rollback()
            except Exception:pass
            return None
    today=q1("SELECT count(*) FROM atlas.business WHERE %s::date=current_date"%col)
    yest=q1("SELECT count(*) FROM atlas.business WHERE %s::date=current_date-1"%col)
    out["same_day"]={"today":today,"yesterday":yest}
    # 7-day trend
    trend=[]
    try:
        cur.execute("SELECT %s::date d,count(*) FROM atlas.business WHERE %s>=current_date-7 GROUP BY 1 ORDER BY 1 DESC"%(col,col))
        trend=[{"date":str(r[0]),"new":int(r[1])} for r in cur.fetchall()]
    except Exception:
        try:c.rollback()
        except Exception:pass
    out["trend_7d"]=trend
    # split by source for today+yesterday (join source_record)
    bysrc={}
    for label,days in (("today",0),("yesterday",1)):
        try:
            cur.execute("""SELECT sr.source,count(DISTINCT b.id) FROM atlas.business b
                           JOIN atlas.source_record sr ON sr.business_id=b.id
                           WHERE b.%s::date=current_date-%s GROUP BY 1 ORDER BY 2 DESC LIMIT 12"""%(col,days))
            bysrc[label]={r[0]:int(r[1]) for r in cur.fetchall()}
        except Exception:
            try:c.rollback()
            except Exception:pass
            bysrc[label]={}
    out["by_source"]=bysrc
    # qualified-today (has domain) as a tighter prospect-grade count
    out["today_with_domain"]=q1("SELECT count(*) FROM atlas.business WHERE %s::date=current_date AND domain IS NOT NULL AND domain<>''"%col)
    out["yesterday_with_domain"]=q1("SELECT count(*) FROM atlas.business WHERE %s::date=current_date-1 AND domain IS NOT NULL AND domain<>''"%col)
    out["publish"]=gh_put("status/%s/channel1.json"%NODE,out)
    print("CH1="+json.dumps({"today":today,"yesterday":yest,"col":col,"by_source":bysrc}));sys.exit(0)
if __name__=="__main__":main()
