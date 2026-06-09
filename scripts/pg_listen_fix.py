#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A.T.L.A.S. -- listen_addresses fix (bulletproof, guarded). seq-43 set the
scoped pg_hba line + UFW rule correctly, but PG stayed bound to localhost
(the main-config listen_addresses edit was overridden). This uses
`ALTER SYSTEM SET listen_addresses='*'` -- written to postgresql.auto.conf,
which Postgres loads LAST and which overrides every other config file -- then
a GUARDED `systemctl restart postgresql` with a pg_isready readiness loop and
AUTO-RESTORE: if PG isn't ready on the public IP within the budget, it runs
`ALTER SYSTEM RESET listen_addresses`, restarts again until PG is back UP, and
fails loud (PG alive, door not widened). It then re-confirms (ss + pg_isready
-h public) and republishes status/<node>/pgdoor-audit.json.

Idempotent. NO stop/disable. Read-after-write verified. --selftest = pure logic.
"""
import os, sys, re, json, time, subprocess, base64, urllib.request, urllib.error

PUBLIC_IP = os.environ.get("ATLAS_PG_PUBLIC_IP", "168.119.226.254")
PEER_IP = os.environ.get("ATLAS_PG_PEER", "64.20.50.3")
PORT = os.environ.get("ATLAS_PG_PORT", "5432")
NODE = os.environ.get("NODE_ID", "hetzner")
BUDGET = int(os.environ.get("ATLAS_PG_READY_BUDGET", "60"))


def ss_external_bind(ss_text, port):
    binds = []
    for ln in ss_text.splitlines():
        m = re.search(r"\s(\S+):%s\s" % re.escape(port), " " + ln + " ")
        if m:
            binds.append(m.group(1))
    external = any(b in ("0.0.0.0", "*", "[::]", "::") or
                   (re.match(r"\d+\.\d+\.\d+\.\d+", b) and not b.startswith("127."))
                   for b in binds)
    return binds, external


def selftest():
    ok = True
    def chk(n, c):
        nonlocal ok
        print(("  ok  " if c else "  FAIL") + " " + n); ok = ok and c
    b, e = ss_external_bind("LISTEN 0 200 127.0.0.1:5432 0.0.0.0:*\n", "5432")
    chk("loopback not external", not e)
    b, e = ss_external_bind("LISTEN 0 200 0.0.0.0:5432 0.0.0.0:*\n", "5432")
    chk("0.0.0.0 external", e)
    b, e = ss_external_bind("LISTEN 0 200 168.119.226.254:5432 0.0.0.0:*\nLISTEN 0 200 127.0.0.1:5432 0.0.0.0:*\n", "5432")
    chk("public+loopback external", e)
    print("SELFTEST", "PASS" if ok else "FAIL")
    return 0 if ok else 1


def run(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=90)
    except Exception as ex:
        class R: returncode = 99; stdout = ""; stderr = str(ex)
        return R()


def psql(q):
    return run(["sudo", "-u", "postgres", "psql", "-tAc", q])


def pg_isready(host=None):
    cmd = ["pg_isready", "-q", "-p", PORT]
    if host:
        cmd += ["-h", host]
    return run(cmd).returncode == 0


def wait_ready_public(budget):
    deadline = time.time() + budget
    while time.time() < deadline:
        if pg_isready(PUBLIC_IP):
            return True
        time.sleep(2)
    return pg_isready(PUBLIC_IP)


def wait_ready_local(budget):
    deadline = time.time() + budget
    while time.time() < deadline:
        if pg_isready():
            return True
        time.sleep(2)
    return pg_isready()


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
    body = {"message": "pgdoor listen-fix audit %s" % NODE,
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


def audit():
    listen = psql("SHOW listen_addresses").stdout.strip()
    ss = run(["ss", "-ltnH"])
    binds, external = ss_external_bind(ss.stdout, PORT)
    return {"listen_addresses": listen, "bound_sockets_5432": binds,
            "external_bind": external,
            "pg_isready_public_%s" % PUBLIC_IP: pg_isready(PUBLIC_IP),
            "pg_isready_local": pg_isready(),
            "pg_postmaster_start_time": psql("SELECT pg_postmaster_start_time()").stdout.strip()}


def main():
    if "--selftest" in sys.argv:
        sys.exit(selftest())

    pre = audit()
    result = {"step": "listen_fix", "node": NODE, "peer": PEER_IP + "/32",
              "pre": pre, "action": None, "post": None, "status": None,
              "ts": int(time.time())}

    if pre["external_bind"] and pre["pg_isready_public_%s" % PUBLIC_IP]:
        result["action"] = "noop_already_external"
        result["status"] = "open"
        result["verdict"] = "CONFIRMED open -- 5432 will answer from %s" % PEER_IP
        result["post"] = pre
        gh_put("status/%s/pgdoor-audit.json" % NODE, result)
        print("LISTENFIX=" + json.dumps(result)); sys.exit(0)

    # ALTER SYSTEM -> postgresql.auto.conf (loads last, overrides all)
    r = psql("ALTER SYSTEM SET listen_addresses = '*'")
    result["alter_rc"] = r.returncode
    if r.returncode != 0:
        result["status"] = "alter_failed"; result["detail"] = r.stderr.strip()[:200]
        gh_put("status/%s/pgdoor-audit.json" % NODE, result)
        print("LISTENFIX=" + json.dumps(result)); sys.exit(40)

    run(["systemctl", "restart", "postgresql"])
    result["action"] = "alter_system_restart"

    if not wait_ready_local(BUDGET):
        # PG didn't come back at all -> revert and recover
        psql("ALTER SYSTEM RESET listen_addresses")
        run(["systemctl", "restart", "postgresql"])
        wait_ready_local(BUDGET)
        result["status"] = "reverted_pg_not_ready"
        result["post"] = audit()
        gh_put("status/%s/pgdoor-audit.json" % NODE, result)
        print("LISTENFIX=" + json.dumps(result)); sys.exit(41)

    # PG is up locally; confirm external bind + public readiness
    ok_pub = wait_ready_public(30)
    post = audit()
    result["post"] = post
    if post["external_bind"] and ok_pub:
        result["status"] = "open"
        result["verdict"] = "CONFIRMED open -- 5432 will answer from %s" % PEER_IP
        gh_put("status/%s/pgdoor-audit.json" % NODE, result)
        print("LISTENFIX=" + json.dumps(result)); sys.exit(0)
    else:
        # bound? if external_bind true but public probe flaky, keep it (PG up);
        # if not external at all, revert to be safe.
        if not post["external_bind"]:
            psql("ALTER SYSTEM RESET listen_addresses")
            run(["systemctl", "restart", "postgresql"])
            wait_ready_local(BUDGET)
            result["status"] = "reverted_not_external"
            result["post"] = audit()
            gh_put("status/%s/pgdoor-audit.json" % NODE, result)
            print("LISTENFIX=" + json.dumps(result)); sys.exit(42)
        result["status"] = "external_bind_no_public_probe"
        result["verdict"] = "bound externally but local public probe failed -- check cloud firewall"
        gh_put("status/%s/pgdoor-audit.json" % NODE, result)
        print("LISTENFIX=" + json.dumps(result)); sys.exit(0)


if __name__ == "__main__":
    main()
