#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Website cross-reference enricher ($0, no API, on-box). Two passes:
 A) INTRA-DB name+city cross-ref (works now): a website-LESS business that shares
    name_norm + city with a website-HAVING business inherits its website (same biz,
    multiple records -- one enriched, one not). Entity-resolution website fill.
 B) OSM/Overture cross-ref: if source_record payloads carry a website/contact:website
    tag (OSM tags / Overture 'websites'), pull it onto matching website-less records.
Measures website% before->after; publishes osm-xref.json."""
import os, sys, json, time, base64, urllib.request, urllib.error
sys.path.insert(0, "/opt/atlas/importers")
NODE=os.environ.get("NODE_ID","hetzner"); LIMIT=int(os.environ.get("XREF_LIMIT","300000"))
def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"osm xref","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
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
    si.load_db_env(si.DB_ENV_PATH); c=si.connect_pg(); c.autocommit=True; cur=c.cursor()
    def sc(q):
        try: cur.execute(q); r=cur.fetchone(); return int(r[0]) if r else 0
        except Exception: return None
    biz=sc("SELECT count(*) FROM atlas.business")
    before=sc("SELECT count(*) FROM atlas.business WHERE website IS NOT NULL AND website<>''")
    out={"kind":"osm-xref","node":NODE,"ts":int(time.time())}
    # PASS A: intra-DB name+city cross-ref (bounded UPDATE FROM self-join)
    a_filled=0
    try:
        cur.execute("""UPDATE atlas.business b
                       SET website=w.website
                       FROM (SELECT DISTINCT ON (name_norm,city) name_norm,city,website
                             FROM atlas.business
                             WHERE website IS NOT NULL AND website<>'' AND name_norm IS NOT NULL AND city IS NOT NULL) w
                       WHERE (b.website IS NULL OR b.website='')
                         AND b.name_norm=w.name_norm AND b.city=w.city
                         AND b.id IN (SELECT id FROM atlas.business WHERE (website IS NULL OR website='') AND name_norm IS NOT NULL AND city IS NOT NULL LIMIT %s)""",(LIMIT,))
        a_filled=cur.rowcount
    except Exception as e:
        out["passA_err"]=str(e)[:120]
    # PASS B: OSM/overture website tag from source_record payloads (if any present)
    b_filled=0
    try:
        cur.execute("""WITH cand AS (
                         SELECT sr.business_id,
                                COALESCE(sr.payload->>'website', sr.payload->>'contact:website',
                                         sr.payload->>'websites', sr.payload->>'url') AS w
                         FROM atlas.source_record sr
                         WHERE (sr.payload ? 'website' OR sr.payload ? 'contact:website' OR sr.payload ? 'websites')
                         LIMIT %s)
                       UPDATE atlas.business b SET website='https://'||regexp_replace(lower(cand.w),'^https?://(www\\.)?','')
                       FROM cand WHERE b.id=cand.business_id AND (b.website IS NULL OR b.website='')
                         AND cand.w IS NOT NULL AND cand.w<>'' AND cand.w !~* '(\\.gov|\\.mil)'""",(LIMIT,))
        b_filled=cur.rowcount
    except Exception as e:
        out["passB_err"]=str(e)[:120]
    after=sc("SELECT count(*) FROM atlas.business WHERE website IS NOT NULL AND website<>''")
    out.update({"business_total":biz,"website_before":before,"website_after":after,
                "filled":{"A_name_city_xref":a_filled,"B_osm_payload_tag":b_filled,"total":(a_filled or 0)+(b_filled or 0)},
                "website_pct_before":round(100.0*before/biz,2) if biz else None,
                "website_pct_after":round(100.0*after/biz,2) if biz else None,
                "note":"Pass A = entity-resolution website fill from duplicate name+city records (works now). Pass B = OSM/Overture website tags from source_record payloads (lights up once Overture/OSM POIs are imported). True Overpass/Overture POI ingestion is the next data-acquisition step to expand the match pool."})
    out["publish"]=gh_put("status/%s/osm-xref.json"%NODE,out)
    print("XREF="+json.dumps({"filled":out["filled"],"website_pct_before":out["website_pct_before"],"website_pct_after":out["website_pct_after"]}));sys.exit(0)
if __name__=="__main__": main()
