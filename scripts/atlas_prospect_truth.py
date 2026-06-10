#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""READ-ONLY prospects/QA truth feed. Publishes the REAL qualified count
(qa_status='pass'), the QA pass-rate, and a Render-Mix (HTML / JS-rendered /
Hybrid) derived from the tech-stack we already enrich. NO writes.
Publishes status/<node>/prospects-truth.json (the WP Truth-Fix plugin can map
qualified + render_mix from here)."""
import os,sys,json,time,base64,urllib.request,urllib.error
sys.path.insert(0,"/opt/atlas/importers")
NODE=os.environ.get("NODE_ID","hetzner")
JS={"Next.js","React","Vue","Angular","Gatsby","Nuxt","Svelte"}
CMS={"WordPress","Shopify","Squarespace","Wix","Drupal","Joomla","PHP","ASP.NET","Webflow"}
def gh_put(path,obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"prospect truth","content":base64.b64encode(json.dumps(obj,indent=2,default=str).encode()).decode(),"branch":br}
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
    out={"kind":"prospects-truth","node":NODE,"ts":int(time.time())}
    # ---- QA gate: qualified = qa_status='pass' ----
    qa={}
    try:
        cur.execute("SELECT COALESCE(qa_status,'(unchecked)'), count(*) FROM atlas.business GROUP BY 1")
        qa={k:int(v) for k,v in cur.fetchall()}
    except Exception as e:
        qa={"error":str(e)[:80]}
    checked=sum(v for k,v in qa.items() if k in ("pass","quarantine","reenrich"))
    qualified=qa.get("pass",0)
    out["qa"]={"distribution":qa,"qualified_pass":qualified,"checked":checked,
               "pass_rate_pct":round(100.0*qualified/checked,1) if checked else None,
               "note":"qualified = qa_status='pass' (cleared the no-chat + site + email + completeness hard gate). '(unchecked)' = QA auditor hasn't graded yet."}
    # ---- Render-Mix from stored tech-stack ----
    mix={"html_server":0,"js_rendered":0,"hybrid":0,"undetermined":0}
    try:
        cur.execute("SELECT value FROM atlas.field_provenance WHERE field='tech'")
        for (val,) in cur:
            toks=set(t.strip() for t in (val or "").split(",") if t.strip())
            has_js=bool(toks & JS); has_cms=bool(toks & CMS)
            if has_js and has_cms: mix["hybrid"]+=1
            elif has_js: mix["js_rendered"]+=1
            elif has_cms or toks: mix["html_server"]+=1
            else: mix["undetermined"]+=1
    except Exception as e:
        out["render_err"]=str(e)[:80]
    tot=sum(mix.values()) or 1
    out["render_mix"]={"counts":mix,"pct":{k:round(100.0*v/tot,1) for k,v in mix.items()},
                       "basis":tot,"note":"derived from detected platform/tech-stack (JS framework=>js_rendered, server CMS=>html_server, both=>hybrid). Not a headless-render measurement (egress-gated); honest proxy from real tech signals."}
    # ---- honest zeros, asserted from architecture ----
    out["honest_zeros"]={"pipeline_value":0,"response_rate_pct":0,"emails_today":0,
        "why":"No email SENDER exists in the codebase (verified: zero smtplib/sendgrid/mailgun/ses send calls). Pipeline stops at handoff->WP candidate feed; no campaign sends, so these are TRUE zeros, not display bugs."}
    c.close()
    out["publish"]=gh_put("status/%s/prospects-truth.json"%NODE,out)
    print("PROSPECT_TRUTH="+json.dumps({"qualified":qualified,"pass_rate":out["qa"]["pass_rate_pct"],"qa_dist":qa,"render_pct":out["render_mix"]["pct"],"publish":out["publish"]},default=str));sys.exit(0)
if __name__=="__main__": main()
