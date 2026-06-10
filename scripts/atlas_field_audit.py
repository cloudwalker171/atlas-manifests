#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""READ-ONLY field-fill audit of atlas.business / atlas.field_provenance.
Reports exact counts + % per key field across the enriched corpus, the
done-vs-usable-email-vs-usable-phone split, 3-5 real sample records (fat + thin),
and a measured Apollo/ZoomInfo company-level completeness %. NO writes.
Publishes status/<node>/field-audit.json."""
import os,sys,json,time,base64,urllib.request,urllib.error
sys.path.insert(0,"/opt/atlas/importers")
NODE=os.environ.get("NODE_ID","hetzner")
def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"field audit","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
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
    def sc(q,a=None):
        cur.execute(q,a); r=cur.fetchone(); return r[0] if r else 0
    out={"kind":"field-audit","node":NODE,"ts":int(time.time())}
    biz=sc("SELECT count(*) FROM atlas.business")
    done=sc("SELECT count(*) FROM atlas.enrich_queue WHERE status='done'")
    out["business_total"]=biz; out["queue_done"]=done
    # field_provenance per-field distinct-business counts (where the rich enrichment lands)
    cur.execute("SELECT field, count(DISTINCT business_id) FROM atlas.field_provenance GROUP BY field ORDER BY 2 DESC")
    prov={f:int(n) for f,n in cur.fetchall()}
    out["field_provenance_distinct_business"]=prov
    # business-column fills (non-null/non-blank)
    cols={"name":"name","website":"website","email":"email","phone":"phone_e164","address":"addr_line1","city":"city","state":"region","category":"category"}
    fills={}
    for label,col in cols.items():
        try:
            n=sc('SELECT count(*) FROM atlas.business WHERE "%s" IS NOT NULL AND btrim(("%s")::text)<>\'\''%(col,col))
            fills[label]={"count":n,"pct":round(100.0*n/max(biz,1),1)}
        except Exception as e:
            fills[label]={"error":str(e)[:60]}
    out["business_column_fill"]=fills
    # the honest contact split: usable EMAIL = field_provenance field='email' (NOT email_pattern hint)
    usable_email=prov.get("email",0)
    usable_phone=max(prov.get("phone",0), fills.get("phone",{}).get("count",0))
    out["usable_contact"]={"done_records":done,"usable_email":usable_email,"usable_email_pct_of_biz":round(100.0*usable_email/max(biz,1),2),
                           "usable_phone":usable_phone,"usable_phone_pct_of_biz":round(100.0*usable_phone/max(biz,1),2),
                           "email_pattern_hints_only":prov.get("email_pattern",0)}
    # 3 fat + 2 thin samples
    cur.execute("""SELECT business_id, count(*) c FROM atlas.field_provenance GROUP BY business_id ORDER BY c DESC LIMIT 3""")
    fat=[r[0] for r in cur.fetchall()]
    cur.execute("""SELECT business_id, count(*) c FROM atlas.field_provenance GROUP BY business_id HAVING count(*) BETWEEN 1 AND 2 LIMIT 2""")
    thin=[r[0] for r in cur.fetchall()]
    samples=[]
    for bid in (fat+thin):
        cur.execute('SELECT name, website, phone_e164, email, city, region, category FROM atlas.business WHERE id=%s',(bid,))
        row=cur.fetchone()
        cur.execute("SELECT field, left(value,80), confidence FROM atlas.field_provenance WHERE business_id=%s ORDER BY field",(bid,))
        prov_rows={f:{"value":v,"conf":float(cf) if cf is not None else None} for f,v,cf in cur.fetchall()}
        samples.append({"id":bid,"name":row[0],"website":row[1],"phone":row[2],"email":row[3],"city":row[4],"state":row[5],"category":row[6],"field_count":len(prov_rows),"provenance":prov_rows})
    out["samples"]=samples
    # Apollo/ZoomInfo company-level completeness (measured): avg fill across the target set
    apollo=["name","website","email","phone","address","city","state","category"]
    target_pcts=[fills.get(k,{}).get("pct",0) for k in apollo]
    # add prov-based fields tech/social/industry/mx
    extra={"tech":prov.get("tech",prov.get("tech_stack",0)),"social":sum(v for k,v in prov.items() if k.startswith("social")),
           "industry":prov.get("industry",0),"mx":prov.get("mx",prov.get("mx_provider",0))}
    extra_pcts=[round(100.0*v/max(biz,1),1) for v in extra.values()]
    allp=target_pcts+extra_pcts
    out["apollo_completeness_pct_measured"]=round(sum(allp)/len(allp),1)
    out["apollo_field_pcts"]={**{k:fills.get(k,{}).get("pct",0) for k in apollo},**{k:round(100.0*v/max(biz,1),1) for k,v in extra.items()}}
    c.close()
    out["publish"]=gh_put("status/%s/field-audit.json"%NODE,out)
    print("FIELD_AUDIT="+json.dumps({k:out[k] for k in ("business_total","queue_done","usable_contact","apollo_completeness_pct_measured")},default=str));sys.exit(0)
if __name__=="__main__": main()
