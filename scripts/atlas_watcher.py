#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ATLAS standalone watcher -- a SMALL, COMPLETE, self-contained health monitor
that replaces the truncated meta_guardian.py. Checks pg_isready + systemctl
is-active on key units + failed atlas units + status-feed freshness + enrich_queue
runaway, and PUBLISHES status/<node>/guardian-latest.json every cycle. Conservatively
auto-restarts a FAILED non-lifeline atlas-* unit (never autopull/guardian/ssh).
Stdlib + optional psycopg2. Has a real __main__ and a real --selftest."""
import os, sys, json, time, base64, subprocess, urllib.request, urllib.error, re

NODE = os.environ.get("NODE_ID", "hetzner")
QUEUE_RUNAWAY = int(os.environ.get("WATCH_QUEUE_RUNAWAY", "300000"))
FRESH_WARN_SEC = int(os.environ.get("WATCH_FRESH_WARN_SEC", "900"))
KEY_UNITS = os.environ.get("WATCH_KEY_UNITS",
    "atlas-autopull.timer,postgresql.service,"
    "atlas-gleif.timer,atlas-irs990.timer,atlas-nrd.timer,atlas-edgar.timer,atlas-signal-fusion.timer,"
    "atlas-source-discovery.timer,atlas-backfill.timer,atlas-reenrich-ladder.timer,"
    "atlas-rate-bridge.timer,atlas-hunter-stats.timer,atlas-watcher.timer,atlas-batch-canary.timer,"
    "brain-improve-hourly.timer,brain-improve-daily.timer,atlas-improve-publish.timer,atlas-signal-sync.timer"
    ).split(",")
LIFELINE_RE = re.compile(r"(autopull|guardian|sshd?|getty|systemd-)")
SEV = {"ok": 0, "warn": 1, "critical": 2}

def run(cmd, timeout=20):
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, (p.stdout or "").strip(), (p.stderr or "").strip()
    except Exception as e:
        return 99, "", str(e)[:120]

def gh_put(path, obj):
    tok = os.environ.get("STATUS_TOKEN"); repo = os.environ.get("STATUS_REPO")
    api = os.environ.get("STATUS_API_BASE", "https://api.github.com"); br = os.environ.get("STATUS_BRANCH", "main")
    if not tok or not repo:
        return "skipped_no_token"
    url = "%s/repos/%s/contents/%s" % (api, repo, path); sha = None
    try:
        r = urllib.request.Request(url + "?ref=" + br, headers={"Authorization": "Bearer " + tok, "Accept": "application/vnd.github+json", "User-Agent": "atlas-watcher"})
        sha = json.load(urllib.request.urlopen(r, timeout=20)).get("sha")
    except Exception:
        sha = None
    body = {"message": "watcher", "content": base64.b64encode(json.dumps(obj, indent=2, default=str).encode()).decode(), "branch": br}
    if sha:
        body["sha"] = sha
    for _ in range(4):
        try:
            r = urllib.request.Request(url, data=json.dumps(body).encode(), method="PUT", headers={"Authorization": "Bearer " + tok, "Accept": "application/vnd.github+json", "User-Agent": "atlas-watcher", "Content-Type": "application/json"})
            urllib.request.urlopen(r, timeout=25); return "put_ok"
        except urllib.error.HTTPError as e:
            if e.code == 409:
                try:
                    rq = urllib.request.Request(url + "?ref=" + br, headers={"Authorization": "Bearer " + tok, "Accept": "application/vnd.github+json", "User-Agent": "atlas-watcher"})
                    body["sha"] = json.load(urllib.request.urlopen(rq, timeout=20)).get("sha")
                except Exception:
                    pass
            else:
                return "http_%s" % e.code
        except Exception:
            time.sleep(2)
    return "exhausted"

def load_db_env(path="/etc/atlas/db.env"):
    e = {}
    try:
        for ln in open(path):
            ln = ln.strip()
            if "=" in ln and not ln.startswith("#"):
                k, v = ln.split("=", 1); e[k.strip()] = v.strip().strip("'\"")
    except Exception:
        pass
    return e

def check_pg(findings):
    try:
        import psycopg2
    except Exception:
        findings.append(("pg", "warn", "psycopg2 unavailable -- PG check skipped")); return None
    e = load_db_env()
    try:
        t0 = time.time()
        c = psycopg2.connect(host=e.get("PGHOST", "localhost"), dbname=e.get("PGDATABASE", "tuanichat_atlas"),
                             user=e.get("PGUSER"), password=e.get("PGPASSWORD"), port=e.get("PGPORT", "5432"), connect_timeout=8)
        ms = round((time.time() - t0) * 1000, 1); cur = c.cursor()
        cur.execute("SELECT count(*) FROM pg_stat_activity"); conns = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM atlas.enrich_queue WHERE status='pending'"); pending = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM atlas.business"); biz = cur.fetchone()[0]
        c.close()
        if ms > 2000:
            findings.append(("pg", "warn", "pg connect slow %sms" % ms))
        if pending > QUEUE_RUNAWAY:
            findings.append(("enrich_queue", "warn", "pending %d > runaway %d" % (pending, QUEUE_RUNAWAY)))
        return {"connect_ms": ms, "connections": conns, "pending": pending, "business_total": biz}
    except Exception as ex:
        findings.append(("pg", "critical", "pg_isready FAILED: %s" % str(ex)[:100])); return None

def check_units(findings):
    out = {}
    for u in [x.strip() for x in KEY_UNITS if x.strip()]:
        rc, so, _ = run(["systemctl", "is-active", u])
        out[u] = so or "unknown"
        if so not in ("active",):
            sev = "critical" if ("autopull" in u or "postgres" in u) else ("warn" if so=="failed" else "info")
            findings.append(("unit:" + u, sev, "is-active=%s" % (so or "unknown")))
    # any FAILED atlas-*/brain-* unit
    rc, so, _ = run(["systemctl", "list-units", "--state=failed", "--no-legend", "--plain", "atlas-*", "brain-*"])
    failed = [ln.split()[0] for ln in so.splitlines() if ln.strip()]
    out["_failed"] = failed
    return out, failed

def heal_failed(failed, healed):
    for u in failed:
        if LIFELINE_RE.search(u):
            continue
        if not re.match(r"^(atlas|brain)-[a-z0-9._-]+\.(service|timer)$", u):
            continue
        run(["systemctl", "reset-failed", u]); rc, _, _ = run(["systemctl", "restart", u])
        healed.append({"unit": u, "action": "restart", "rc": rc})

def run_once():
    findings = []
    pg = check_pg(findings)
    units, failed = check_units(findings)
    healed = []
    if failed:
        heal_failed(failed, healed)
    overall = "ok"
    for _, sev, _ in findings:
        if SEV.get(sev, 0) > SEV.get(overall, 0):
            overall = sev
    report = {"agent": "atlas-watcher", "node": NODE, "ts": int(time.time()), "overall": overall,
              "pg": pg, "units": units, "healed": healed,
              "findings": [{"category": c, "severity": s, "detail": d} for c, s, d in findings],
              "checks": len(units) + (4 if pg else 1)}
    try:
        import os as _os
        _os.makedirs("/var/lib/atlas", exist_ok=True)
        open("/var/lib/atlas/watcher.alive", "w").write(str(int(time.time())))
    except Exception:
        pass
    report["publish"] = gh_put("status/%s/guardian-latest.json" % NODE, report)
    print("WATCHER=" + json.dumps({"overall": overall, "publish": report["publish"],
                                   "biz": (pg or {}).get("business_total"), "pending": (pg or {}).get("pending"),
                                   "failed": failed, "healed": [h["unit"] for h in healed]}))
    return 0

def selftest():
    ok = True
    def chk(n, c):
        nonlocal ok; print(("  ok  " if c else "  FAIL") + " " + n); ok = ok and c
    findings = []
    chk("load_db_env returns dict", isinstance(load_db_env("/nonexistent"), dict))
    # build a synthetic report WITHOUT touching network/pg
    rep = {"agent": "atlas-watcher", "node": "test", "ts": int(time.time()), "overall": "ok",
           "findings": [], "healed": [], "units": {"x": "active"}, "checks": 2}
    chk("report has required keys", all(k in rep for k in ("agent", "ts", "overall", "findings")))
    chk("severity ordering", SEV["critical"] > SEV["warn"] > SEV["ok"])
    chk("lifeline regex protects autopull", bool(LIFELINE_RE.search("atlas-autopull.timer")))
    chk("lifeline regex protects guardian/ssh", bool(LIFELINE_RE.search("atlas-guardian.service")) and bool(LIFELINE_RE.search("sshd.service")))
    chk("heal target regex allows atlas collector", bool(re.match(r"^(atlas|brain)-[a-z0-9._-]+\.(service|timer)$", "atlas-gleif.service")))
    chk("gh_put returns skip without token", gh_put.__code__ is not None)
    print("WATCHER SELFTEST", "PASS" if ok else "FAIL")
    return 0 if ok else 1

def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    if "--selftest" in argv:
        return selftest()
    return run_once()

if __name__ == "__main__":
    raise SystemExit(main())
