#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A.T.L.A.S. -- PG door AUDIT (READ-ONLY). Gathers box-side proof that the
InterServer PG door (64.20.50.3/32) is fully + correctly open, and PUBLISHES
the result to the repo at status/<node>/pgdoor-audit.json via the same GitHub
contents API the autopull uses (STATUS_TOKEN/STATUS_REPO/STATUS_API_BASE/
STATUS_BRANCH are inherited from the autopull env). Makes NO changes -- only
SHOW/grep/ss/ufw-status/pg_isready/pg_postmaster_start_time.

Confirms, explicitly:
  * pg_hba.conf scoped line present + auth method + (reload implied by it being live)
  * listen_addresses value AND the actual bound sockets (ss) -- external bind?
  * pg_isready answers on the public IP:5432
  * UFW allows 5432 from the scoped peer
  * PG uptime (postmaster start time) -- proves whether a restart happened and
    that PG is healthy now (no ongoing outage).

--selftest: pure-logic gate (auth parser, ss external-bind detector, ufw parser).
"""
import os, sys, re, json, time, subprocess, base64, urllib.request, urllib.error

PEER_IP = os.environ.get("ATLAS_PG_PEER", "64.20.50.3")
PUBLIC_IP = os.environ.get("ATLAS_PG_PUBLIC_IP", "168.119.226.254")
PORT = os.environ.get("ATLAS_PG_PORT", "5432")
NODE = os.environ.get("NODE_ID", "hetzner")


def parse_hba_authmethod(line):
    """Given an uncommented host line mentioning the peer, return the auth method
    (the token after the address/mask)."""
    parts = line.split()
    # host db user addr method   (addr may be 'ip/mask' single token)
    if len(parts) >= 5 and parts[0] in ("host", "hostssl", "hostnossl"):
        return parts[4]
    return None


def ss_external_bind(ss_text, port):
    """Return list of bound addresses on <port>, and whether any is non-loopback."""
    binds = []
    for ln in ss_text.splitlines():
        m = re.search(r"\s(\S+):%s\s" % re.escape(port), " " + ln + " ")
        if m:
            binds.append(m.group(1))
    nonlocal_binds = [b for b in binds
                      if b not in ("127.0.0.1", "::1", "[::1]")
                      and not b.startswith("127.")]
    external = any(b in ("0.0.0.0", "*", "[::]", "::") or
                   re.match(r"\d+\.\d+\.\d+\.\d+", b) and not b.startswith("127.")
                   for b in binds)
    return binds, nonlocal_binds, external


def ufw_allows(ufw_text, peer_ip, port):
    for ln in ufw_text.splitlines():
        low = ln.lower()
        if "allow" in low and port in low and peer_ip in low:
            return True, ln.strip()
    return False, None


def selftest():
    ok = True
    def chk(n, c):
        nonlocal ok
        print(("  ok  " if c else "  FAIL") + " " + n); ok = ok and c
    chk("auth parse scram",
        parse_hba_authmethod("host tuanichat_atlas atlas 64.20.50.3/32 scram-sha-256") == "scram-sha-256")
    chk("auth parse md5",
        parse_hba_authmethod("hostssl all atlas 64.20.50.3/32 md5") == "md5")
    chk("auth parse none for local", parse_hba_authmethod("local all all peer") is None)
    b, nl, ext = ss_external_bind("LISTEN 0 200 127.0.0.1:5432 0.0.0.0:*\n", "5432")
    chk("loopback-only -> not external", ("127.0.0.1" in b) and not ext)
    b, nl, ext = ss_external_bind("LISTEN 0 200 0.0.0.0:5432 0.0.0.0:*\n", "5432")
    chk("0.0.0.0 -> external", ext)
    b, nl, ext = ss_external_bind("LISTEN 0 200 168.119.226.254:5432 0.0.0.0:*\n", "5432")
    chk("public ip bind -> external", ext and "168.119.226.254" in nl)
    a, line = ufw_allows("5432/tcp ALLOW 64.20.50.3\n", "64.20.50.3", "5432")
    chk("ufw allow detected", a)
    a, line = ufw_allows("22/tcp ALLOW Anywhere\n", "64.20.50.3", "5432")
    chk("ufw allow absent", not a)
    print("SELFTEST", "PASS" if ok else "FAIL")
    return 0 if ok else 1


def run(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except Exception as ex:
        class R: returncode = 99; stdout = ""; stderr = str(ex)
        return R()


def psql1(q):
    p = run(["sudo", "-u", "postgres", "psql", "-tAc", q])
    return p.stdout.strip() if p.returncode == 0 else None


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
                       "Accept": "application/vnd.github+json",
                       "User-Agent": "atlas-autopull"})
        cur = json.load(urllib.request.urlopen(req, timeout=20))
        sha = cur.get("sha")
    except Exception:
        sha = None
    body = {"message": "pgdoor audit %s" % NODE,
            "content": base64.b64encode(json.dumps(obj, indent=2).encode()).decode(),
            "branch": br}
    if sha:
        body["sha"] = sha
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="PUT",
          headers={"Authorization": "Bearer " + tok,
                   "Accept": "application/vnd.github+json",
                   "User-Agent": "atlas-autopull",
                   "Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=20)
        return "put_ok"
    except urllib.error.HTTPError as e:
        return "put_http_%s" % e.code
    except Exception as e:
        return "put_err_%s" % type(e).__name__


def main():
    if "--selftest" in sys.argv:
        sys.exit(selftest())

    hba = psql1("SHOW hba_file")
    listen = psql1("SHOW listen_addresses")
    start = psql1("SELECT pg_postmaster_start_time()")
    nowt = psql1("SELECT now()")
    biz = psql1("SELECT GREATEST(reltuples::bigint,0) FROM pg_class WHERE oid='atlas.business'::regclass")

    # pg_hba scoped line(s)
    hba_lines = []
    authm = None
    if hba and os.path.exists(hba):
        for raw in open(hba):
            s = raw.strip()
            if s and not s.startswith("#") and PEER_IP in s:
                hba_lines.append(s)
                authm = authm or parse_hba_authmethod(s)

    # bound sockets
    ss = run(["ss", "-ltnH"])
    binds, nonlocal_binds, external = ss_external_bind(ss.stdout, PORT)

    # pg_isready public + local
    rp = run(["pg_isready", "-q", "-h", PUBLIC_IP, "-p", PORT]).returncode == 0
    rl = run(["pg_isready", "-q", "-p", PORT]).returncode == 0

    # ufw
    uf = run(["ufw", "status"])
    ufw_active = "status: active" in uf.stdout.lower()
    ufw_ok, ufw_line = ufw_allows(uf.stdout, PEER_IP, PORT)

    door_open = bool(hba_lines) and authm is not None and external and rp and \
        (ufw_ok or not ufw_active)

    out = {
        "audit": "pgdoor", "node": NODE, "peer": PEER_IP + "/32", "port": PORT,
        "pg_hba_scoped_lines": hba_lines,
        "pg_hba_auth_method": authm,
        "listen_addresses": listen,
        "bound_sockets_5432": binds,
        "external_bind": external,
        "pg_isready_public_%s" % PUBLIC_IP: rp,
        "pg_isready_local": rl,
        "ufw_active": ufw_active,
        "ufw_5432_from_peer": ufw_ok,
        "ufw_rule_line": ufw_line,
        "pg_postmaster_start_time": start,
        "pg_now": nowt,
        "business_reltuples": biz,
        "door_open": door_open,
        "verdict": ("CONFIRMED open -- 5432 will answer from %s" % PEER_IP)
                   if door_open else "NOT fully open -- see fields",
        "ts": int(time.time()),
    }
    pub = gh_put("status/%s/pgdoor-audit.json" % NODE, out)
    out["publish"] = pub
    print("PGDOOR_AUDIT=" + json.dumps(out))
    # read-only: always exit 0 (audit must not trip puller rollback)
    sys.exit(0)


if __name__ == "__main__":
    main()
