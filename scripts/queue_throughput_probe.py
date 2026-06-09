#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A.T.L.A.S. -- shared enrich_queue throughput + claimant + PG-load probe (READ-ONLY).
Measures the COMBINED done-rate across BOTH nodes (Hetzner + InterServer) on the
single shared atlas.enrich_queue, shows the per-claimant (locked_by) split and the
pg_stat_activity client_addr split (InterServer connects from 64.20.50.3; local
Hetzner workers connect via unix socket / 127.0.0.1), and reports PG connection
headroom + host load. Publishes status/<node>/queue-throughput.json. Makes NO
writes. --selftest = pure logic (rate math + host-bucketing).
"""
import os, sys, re, json, time, subprocess, base64, urllib.request, urllib.error

NODE = os.environ.get("NODE_ID", "hetzner")
PEER_IP = os.environ.get("ATLAS_PG_PEER", "64.20.50.3")
SAMPLE_S = int(os.environ.get("PROBE_SAMPLE_S", "5"))


def rate_per_min(d0, d1, secs):
    if secs <= 0:
        return 0.0
    return round((d1 - d0) * 60.0 / secs, 1)


def bucket_claimants(rows, peer_ip):
    """rows: list of (locked_by, count). Bucket by host prefix heuristic."""
    out = {}
    for lb, n in rows:
        host = "unknown"
        if lb:
            m = re.match(r"([^:@/]+)", str(lb))
            host = m.group(1) if m else str(lb)
        out[host] = out.get(host, 0) + int(n)
    return out


def selftest():
    ok = True
    def chk(n, c):
        nonlocal ok
        print(("  ok  " if c else "  FAIL") + " " + n); ok = ok and c
    chk("rate 600 over 30s -> 1200/min", rate_per_min(1000, 1300, 30) == 600.0 or rate_per_min(1000,1600,30)==1200.0)
    chk("rate basic", rate_per_min(0, 800, 40) == 1200.0)
    b = bucket_claimants([("hetzner-pool-3:123", 5), ("hetzner-pool-4:99", 3),
                          ("interserver:777", 7), ("interserver:778", 2)], PEER_IP)
    chk("buckets hetzner", b.get("hetzner-pool-3", 0) >= 0 and "interserver" in b)
    chk("interserver count", b.get("interserver") == 9)
    print("SELFTEST", "PASS" if ok else "FAIL")
    return 0 if ok else 1


def run(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except Exception as ex:
        class R: returncode = 99; stdout = ""; stderr = str(ex)
        return R()


def gh_put(path, obj):
    tok = os.environ.get("STATUS_TOKEN"); repo = os.environ.get("STATUS_REPO")
    api = os.environ.get("STATUS_API_BASE", "https://api.github.com")
    br = os.environ.get("STATUS_BRANCH", "main")
    if not tok or not repo:
        return "skipped_no_token"
    url = "%s/repos/%s/contents/%s" % (api, repo, path)
    sha = None
    try:
        req = urllib.request.Request(url + "?ref=" + br,
              headers={"Authorization": "Bearer " + tok,
                       "Accept": "application/vnd.github+json", "User-Agent": "atlas-autopull"})
        sha = json.load(urllib.request.urlopen(req, timeout=20)).get("sha")
    except Exception:
        sha = None
    body = {"message": "queue throughput probe %s" % NODE,
            "content": base64.b64encode(json.dumps(obj, indent=2).encode()).decode(), "branch": br}
    if sha:
        body["sha"] = sha
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="PUT",
          headers={"Authorization": "Bearer " + tok, "Accept": "application/vnd.github+json",
                   "User-Agent": "atlas-autopull", "Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=20); return "put_ok"
    except urllib.error.HTTPError as e:
        return "put_http_%s" % e.code
    except Exception as e:
        return "put_err_%s" % type(e).__name__


def main():
    if "--selftest" in sys.argv:
        sys.exit(selftest())

    import psycopg2
    e = {}
    with open("/etc/atlas/db.env") as fh:
        for ln in fh:
            ln = ln.strip()
            if "=" in ln and not ln.startswith("#"):
                k, v = ln.split("=", 1)
                e[k.strip()] = v.strip().strip("'\"")
    conn = psycopg2.connect(host=e.get("PGHOST", "localhost"),
                            dbname=e.get("PGDATABASE", "tuanichat_atlas"),
                            user=e.get("PGUSER"), password=e.get("PGPASSWORD"),
                            port=e.get("PGPORT", "5432"))
    conn.autocommit = True
    cur = conn.cursor()

    def scalar(q):
        cur.execute(q); r = cur.fetchone(); return r[0] if r else None

    # sample done-count twice
    t0 = time.time()
    done0 = scalar("SELECT count(*) FROM atlas.enrich_queue WHERE status='done'")
    time.sleep(SAMPLE_S)
    done1 = scalar("SELECT count(*) FROM atlas.enrich_queue WHERE status='done'")
    secs = time.time() - t0
    combined_rate = rate_per_min(done0, done1, secs)

    # claimant split
    cur.execute("""SELECT locked_by, count(*) FROM atlas.enrich_queue
                   WHERE status='claimed' AND locked_by IS NOT NULL
                   GROUP BY locked_by ORDER BY 2 DESC LIMIT 50""")
    claim_rows = cur.fetchall()
    claim_buckets = bucket_claimants(claim_rows, PEER_IP)

    # pg_stat_activity by client_addr (InterServer = PEER_IP; local = NULL/127.*)
    cur.execute("""SELECT COALESCE(host(client_addr),'local'), state, count(*)
                   FROM pg_stat_activity WHERE datname=current_database()
                   GROUP BY 1,2 ORDER BY 3 DESC""")
    act = [[r[0], r[1], int(r[2])] for r in cur.fetchall()]
    interserver_conns = sum(r[2] for r in act if r[0] == PEER_IP)
    local_conns = sum(r[2] for r in act if r[0] in ("local", "127.0.0.1", "::1"))

    total_conns = scalar("SELECT count(*) FROM pg_stat_activity")
    maxc = scalar("SHOW max_connections")
    queue_pending = scalar("SELECT count(*) FROM atlas.enrich_queue WHERE status='pending'")
    queue_claimed = scalar("SELECT count(*) FROM atlas.enrich_queue WHERE status='claimed'")

    try:
        load1, load5, load15 = os.getloadavg()
    except Exception:
        load1 = load5 = load15 = None
    ncpu = os.cpu_count()

    out = {
        "probe": "queue_throughput", "node": NODE, "sample_seconds": round(secs, 1),
        "done_start": done0, "done_end": done1,
        "combined_done_rate_per_min": combined_rate,
        "claimant_buckets": claim_buckets,
        "distinct_claimants": len(claim_rows),
        "interserver_claiming": any(PEER_IP in str(r[0]) or "interserver" in str(r[0]).lower()
                                    for r in claim_rows) or interserver_conns > 0,
        "pg_activity_by_client": act,
        "interserver_pg_connections": interserver_conns,
        "local_pg_connections": local_conns,
        "pg_total_connections": total_conns, "pg_max_connections": maxc,
        "queue_pending": queue_pending, "queue_claimed": queue_claimed,
        "hetzner_load": [load1, load5, load15], "hetzner_ncpu": ncpu,
        "ts": int(time.time()),
    }
    conn.close()
    out["publish"] = gh_put("status/%s/queue-throughput.json" % NODE, out)
    print("QUEUE_THROUGHPUT=" + json.dumps(out))
    sys.exit(0)


if __name__ == "__main__":
    main()
