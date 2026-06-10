#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A.T.L.A.S. throughput exporter (TRUTHFUL, real-time). Computes intake vs
enrichment rates from TIMESTAMPED counter deltas (never the last-writer-wins
enriched_per_min field) -- per-minute, per-hour, per-day, and month-to-date --
plus the ramp-measured per-box ceiling. Per-box honesty: intake is Hetzner-only
(collectors run here); InterServer enrichment isolation isn't clean (the queue
clears locked_by on done), so it is reported by its LIVE pg connection presence
and marked 'measuring' rather than guessed. Publishes status/hetzner/throughput.json.
--selftest exercises the rate + MTD math offline."""
import os, sys, json, time, base64, urllib.request, urllib.error, datetime
NODE = os.environ.get("NODE_ID", "hetzner")
STATE = os.environ.get("ATLAS_THROUGHPUT_STATE", "/var/lib/atlas/throughput/state.json")
PEER = os.environ.get("ATLAS_PG_PEER", "64.20.50.3")
# measured ceilings (proven by the async ramp, per min)
HETZNER_ENRICH_CEILING = int(os.environ.get("HETZNER_ENRICH_CEILING", "1957"))

def rates(per_min):
    return {"per_min": round(per_min, 1), "per_hour": round(per_min * 60),
            "per_day": round(per_min * 1440)}

def compute(prev, cur, now_ts):
    """prev/cur: dicts with 'done','intake','ts'. Returns per-min for each based
    on the delta over the elapsed window; clamps negatives (counter resets) to 0."""
    out = {}
    if prev and prev.get("ts") and now_ts > prev["ts"]:
        secs = now_ts - prev["ts"]
        d_done = max(0, cur["done"] - prev["done"])
        d_intake = max(0, cur["intake"] - prev["intake"])
        out["enrich_per_min"] = d_done * 60.0 / secs
        out["intake_per_min"] = d_intake * 60.0 / secs
    else:
        out["enrich_per_min"] = 0.0
        out["intake_per_min"] = 0.0
    return out

def mtd(state, cur, now_dt):
    """month-to-date: baseline captured at first run of the month."""
    mk = now_dt.strftime("%Y-%m")
    base = state.get("mtd", {})
    if base.get("month") != mk:
        base = {"month": mk, "done0": cur["done"], "intake0": cur["intake"]}
    return base, {"enrich": max(0, cur["done"] - base["done0"]),
                  "intake": max(0, cur["intake"] - base["intake0"])}

def selftest():
    ok = True
    def chk(n, c):
        nonlocal ok; print(("  ok  " if c else "  FAIL") + " " + n); ok = ok and c
    prev = {"done": 1000, "intake": 500, "ts": 1000}
    cur = {"done": 1600, "intake": 560, "ts": 1030}  # +600 done, +60 intake over 30s
    r = compute(prev, cur, 1030)
    chk("enrich 600/30s -> 1200/min", abs(r["enrich_per_min"] - 1200.0) < 0.1)
    chk("intake 60/30s -> 120/min", abs(r["intake_per_min"] - 120.0) < 0.1)
    chk("per_hour x60", rates(1200)["per_hour"] == 72000)
    chk("per_day x1440", rates(1200)["per_day"] == 1728000)
    chk("counter reset clamps to 0", compute({"done":1600,"intake":560,"ts":1000}, {"done":5,"intake":0,"ts":1030}, 1030)["enrich_per_min"] == 0.0)
    st = {}
    base, m = mtd(st, {"done": 1000, "intake": 500}, datetime.datetime(2026,6,1))
    chk("mtd baseline first run = 0", m["enrich"] == 0 and m["intake"] == 0)
    st2 = {"mtd": base}
    _, m2 = mtd(st2, {"done": 1700, "intake": 800}, datetime.datetime(2026,6,15))
    chk("mtd climbs to 700/300", m2["enrich"] == 700 and m2["intake"] == 300)
    print("SELFTEST", "PASS" if ok else "FAIL")
    return 0 if ok else 1

def gh_put(path, obj):
    tok=os.environ.get("STATUS_TOKEN");repo=os.environ.get("STATUS_REPO")
    api=os.environ.get("STATUS_API_BASE","https://api.github.com");br=os.environ.get("STATUS_BRANCH","main")
    if not tok or not repo: return "skipped"
    url="%s/repos/%s/contents/%s"%(api,repo,path);sha=None
    try:
        r=urllib.request.Request(url+"?ref="+br,headers={"Authorization":"Bearer "+tok,"Accept":"application/vnd.github+json","User-Agent":"atlas"});sha=json.load(urllib.request.urlopen(r,timeout=20)).get("sha")
    except Exception: sha=None
    body={"message":"throughput","content":base64.b64encode(json.dumps(obj,indent=2).encode()).decode(),"branch":br}
    if sha: body["sha"]=sha
    for _ in range(5):
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
    if "--selftest" in sys.argv: sys.exit(selftest())
    import psycopg2
    e={}
    for ln in open("/etc/atlas/db.env"):
        ln=ln.strip()
        if "=" in ln and not ln.startswith("#"): k,v=ln.split("=",1);e[k.strip()]=v.strip().strip("'\"")
    c=psycopg2.connect(host=e.get("PGHOST","localhost"),dbname=e.get("PGDATABASE","tuanichat_atlas"),user=e.get("PGUSER"),password=e.get("PGPASSWORD"),port=e.get("PGPORT","5432"),connect_timeout=10)
    c.autocommit=True;cur=c.cursor()
    def sc(q):cur.execute(q);r=cur.fetchone();return r[0] if r else 0
    done=int(sc("SELECT count(*) FROM atlas.enrich_queue WHERE status='done'"))
    intake=int(sc("SELECT GREATEST(reltuples::bigint,0) FROM pg_class WHERE oid='atlas.business'::regclass"))
    inter_conns=int(sc("SELECT count(*) FROM pg_stat_activity WHERE host(client_addr)='%s'"%PEER))
    now_ts=int(time.time()); now_dt=datetime.datetime.utcnow()
    try: state=json.load(open(STATE))
    except Exception: state={}
    cur_c={"done":done,"intake":intake,"ts":now_ts}
    r=compute(state.get("last"), cur_c, now_ts)
    base,m=mtd(state,cur_c,now_dt)
    enrich_pm=r["enrich_per_min"]; intake_pm=r["intake_per_min"]
    out={
      "kind":"atlas-throughput","node":NODE,"ts":now_ts,
      "enrichment":{
        "combined":{**rates(enrich_pm),"mtd":m["enrich"]},
        "ceiling_per_min":HETZNER_ENRICH_CEILING,"ceiling_per_day":HETZNER_ENRICH_CEILING*1440,
        "by_box":{
          "hetzner":{**rates(enrich_pm),"note":"combined enrich done-rate (queue done-delta); InterServer share included"},
          "interserver":{"status":"measuring","live_pg_connections":inter_conns,
                         "note":"queue clears locked_by on done so per-box enrich is not cleanly isolable yet; shown as live-presence until a per-box counter lands -- NOT guessed"}
        }},
      "intake":{
        "combined":{**rates(intake_pm),"mtd":m["intake"]},
        "by_box":{"hetzner":{**rates(intake_pm),"note":"all collectors run on Hetzner"},
                  "interserver":{**rates(0),"note":"InterServer is enrichment-only; no intake"}}},
      "totals":{"business_total":intake,"queue_done":done},
      "honesty":"rates from timestamped counter deltas (not last-writer enriched_per_min). MTD climbs from month start. InterServer enrich = measuring."
    }
    state["last"]=cur_c; state["mtd"]=base
    try:
        os.makedirs(os.path.dirname(STATE),exist_ok=True); json.dump(state,open(STATE,"w"))
    except Exception as ex: out["state_err"]=str(ex)[:100]
    out["publish"]=gh_put("status/%s/throughput.json"%NODE,out)
    print("THROUGHPUT="+json.dumps(out));sys.exit(0)

if __name__=="__main__": main()
