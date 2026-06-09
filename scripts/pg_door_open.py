#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A.T.L.A.S. -- InterServer PG door opener (lifeline-grade, PG-SAFE, idempotent).

Opens the Hetzner Postgres to EXACTLY ONE scoped peer (the InterServer worker
node, default 64.20.50.3/32) for the `atlas` role over scram-sha-256, so the
second node's enrichment worker can connect and contribute rows. It does NOT
widen access to anyone else.

SAFETY CONTRACT (this script self-heals; it does not rely on the puller's
file-snapshot rollback, which only captures write_file steps):
  * Backs up pg_hba.conf and postgresql.conf BEFORE any edit.
  * pg_hba change is applied with `systemctl reload postgresql` (ZERO downtime).
  * listen_addresses is only touched if the current value does NOT already
    expose a non-local bind; when it must change, it is done with a careful
    `systemctl restart postgresql` GUARDED by a pg_isready readiness loop with
    AUTO-RESTORE: if PG is not ready within the budget, the original config is
    restored and PG is restarted again until it is back up -- then the script
    FAILS LOUD (so the operator sees the door did not open) but leaves PG UP.
  * UFW: SSH (OpenSSH/22) is (re)allowed FIRST, defensively, then a SCOPED
    `allow from <peer> to any port 5432 proto tcp` is added. UFW is NEVER
    enabled-from-inactive (we never risk locking out a remote box), NEVER
    disabled, and the ssh rule is never removed. This keeps the puller's
    `ufw-ssh-allowed` health check green.
  * No systemctl stop/disable. Only reload / restart.
  * Idempotent: re-runs add nothing, reload-only, and report OPEN.

NORMAL run prints exactly one line:  PGDOOR_RESULT={...json...}
and exits 0 on OPEN, non-zero (PG restored & up) on failure.

--selftest runs pure-logic checks only (no DB, no net, no subprocess side
effects) and is the manifest's fail-loud gate.
"""
import os, sys, re, json, time, shutil, subprocess, datetime

PEER_IP   = os.environ.get("ATLAS_PG_PEER", "64.20.50.3")
PEER_CIDR = PEER_IP + "/32"
ROLE      = os.environ.get("ATLAS_PG_ROLE", "atlas")
PORT      = os.environ.get("ATLAS_PG_PORT", "5432")
MARKER    = "atlas-pgdoor"
READY_BUDGET_S = int(os.environ.get("ATLAS_PG_READY_BUDGET", "60"))
BACKUP_ROOT = "/var/lib/atlas/pgdoor"

# ---------------------------------------------------------------------------
# Pure helpers (exercised by --selftest; no side effects)
# ---------------------------------------------------------------------------
def hba_line(db, role, cidr):
    # padded host line, scram-sha-256
    return "host\t%s\t%s\t%s\tscram-sha-256\t# %s" % (db, role, cidr, MARKER)

def _norm(s):
    return re.sub(r"\s+", " ", s.strip())

def hba_has_peer(hba_text, db, role, cidr):
    """True if an active (uncommented) host line already authorizes role@cidr
    for db (or all). Whitespace-insensitive."""
    want_cidr = cidr
    for raw in hba_text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 5:
            continue
        typ, d, u, addr = parts[0], parts[1], parts[2], parts[3]
        if typ not in ("host", "hostssl", "hostnossl"):
            continue
        if addr != want_cidr:
            continue
        db_ok = d in (db, "all")
        role_ok = u in (role, "all")
        if db_ok and role_ok:
            return True
    return False

def listen_exposes_nonlocal(value):
    """Given a listen_addresses value string, return True if it already binds
    something other than purely loopback/localhost."""
    v = value.strip().strip("'\"").lower()
    if v == "":
        return False
    if "*" in v or "0.0.0.0" in v or "::" == v:
        return True
    toks = [t.strip() for t in v.split(",") if t.strip()]
    nonlocal_toks = [t for t in toks if t not in ("localhost", "127.0.0.1", "::1")]
    return len(nonlocal_toks) > 0

def ufw_has_scoped_5432(ufw_status_text, peer_ip, port):
    """True if ufw status already shows an ALLOW rule for port from peer."""
    for raw in ufw_status_text.splitlines():
        line = raw.lower()
        if "allow" in line and port in line and peer_ip in line:
            return True
    return False

def merged_listen_value(current):
    """Produce a listen_addresses value that keeps localhost AND adds a public
    bind. We use '*' (all interfaces); the firewall (UFW scoped 5432 + tight
    pg_hba) is the access-control layer, not the bind list."""
    return "*"

# ---------------------------------------------------------------------------
# Side-effecting helpers (NOT used in selftest)
# ---------------------------------------------------------------------------
def run(cmd, check=False, timeout=60):
    p = subprocess.run(cmd, shell=isinstance(cmd, str), capture_output=True,
                       text=True, timeout=timeout)
    if check and p.returncode != 0:
        raise RuntimeError("cmd failed (%s): %s" % (cmd, p.stderr.strip()))
    return p

def psql_show(setting):
    p = run(["sudo", "-u", "postgres", "psql", "-tAc", "SHOW %s" % setting])
    if p.returncode != 0:
        raise RuntimeError("SHOW %s failed: %s" % (setting, p.stderr.strip()))
    return p.stdout.strip()

def pg_isready(host=None):
    cmd = ["pg_isready", "-q", "-p", PORT]
    if host:
        cmd += ["-h", host]
    return run(cmd).returncode == 0

def wait_ready(host=None, budget=READY_BUDGET_S):
    deadline = time.time() + budget
    while time.time() < deadline:
        if pg_isready(host):
            return True
        time.sleep(2)
    return pg_isready(host)

def backup_file(path, bdir):
    if os.path.exists(path):
        dst = os.path.join(bdir, os.path.basename(path))
        shutil.copy2(path, dst)
        return dst
    return None

def db_name():
    # prefer puller's configured DB if present
    for envf in ("/etc/atlas/db.env",):
        if os.path.exists(envf):
            for ln in open(envf):
                ln = ln.strip()
                if ln.startswith("PGDATABASE="):
                    return ln.split("=", 1)[1].strip().strip("'\"")
    return os.environ.get("PGDATABASE", "tuanichat_atlas")

# ---------------------------------------------------------------------------
def selftest():
    ok = True
    def chk(name, cond):
        nonlocal ok
        print(("  ok  " if cond else "  FAIL") + " " + name)
        ok = ok and cond
    # hba builder + detector
    db = "tuanichat_atlas"
    line = hba_line(db, "atlas", "64.20.50.3/32")
    chk("hba_line has scram + cidr + marker",
        "scram-sha-256" in line and "64.20.50.3/32" in line and MARKER in line)
    sample = "local all all peer\nhost all all 127.0.0.1/32 scram-sha-256\n"
    chk("hba_has_peer false when absent",
        not hba_has_peer(sample, db, "atlas", "64.20.50.3/32"))
    sample2 = sample + "host\ttuanichat_atlas\tatlas\t64.20.50.3/32\tscram-sha-256\n"
    chk("hba_has_peer true when present (whitespace-insensitive)",
        hba_has_peer(sample2, db, "atlas", "64.20.50.3/32"))
    sample3 = sample + "host all atlas 64.20.50.3/32 scram-sha-256\n"
    chk("hba_has_peer true for db=all role match",
        hba_has_peer(sample3, db, "atlas", "64.20.50.3/32"))
    sample4 = sample + "# host all atlas 64.20.50.3/32 scram-sha-256\n"
    chk("hba_has_peer ignores commented line",
        not hba_has_peer(sample4, db, "atlas", "64.20.50.3/32"))
    sample5 = sample + "host all atlas 64.20.50.4/32 scram-sha-256\n"
    chk("hba_has_peer false for different ip",
        not hba_has_peer(sample5, db, "atlas", "64.20.50.3/32"))
    # listen_addresses coverage
    chk("listen localhost -> not exposed", not listen_exposes_nonlocal("localhost"))
    chk("listen '127.0.0.1,::1' -> not exposed", not listen_exposes_nonlocal("127.0.0.1,::1"))
    chk("listen '*' -> exposed", listen_exposes_nonlocal("*"))
    chk("listen '0.0.0.0' -> exposed", listen_exposes_nonlocal("0.0.0.0"))
    chk("listen 'localhost,168.119.226.254' -> exposed",
        listen_exposes_nonlocal("localhost,168.119.226.254"))
    chk("listen '' -> not exposed", not listen_exposes_nonlocal(""))
    # ufw parser
    uf = "To                         Action      From\n5432/tcp                   ALLOW       64.20.50.3\n22/tcp ALLOW Anywhere\n"
    chk("ufw_has_scoped true", ufw_has_scoped_5432(uf, "64.20.50.3", "5432"))
    chk("ufw_has_scoped false for other ip",
        not ufw_has_scoped_5432(uf, "9.9.9.9", "5432"))
    chk("merged_listen -> '*'", merged_listen_value("localhost") == "*")
    print("SELFTEST", "PASS" if ok else "FAIL")
    return 0 if ok else 1

# ---------------------------------------------------------------------------
def main():
    if "--selftest" in sys.argv:
        sys.exit(selftest())

    result = {"peer": PEER_CIDR, "role": ROLE, "port": PORT,
              "hba_added": False, "listen_changed": False, "restarted": False,
              "reloaded": False, "ufw_active": None, "ufw_rule_added": False,
              "ssh_allowed_ensured": False, "pg_ready_public": None,
              "listen_addresses": None, "status": "unknown", "ts": int(time.time())}

    # preflight: PG must be up to start
    if not pg_isready():
        # try local socket via psql before giving up
        if run(["sudo", "-u", "postgres", "psql", "-tAc", "select 1"]).returncode != 0:
            print("PGDOOR_RESULT=" + json.dumps({"status": "abort_pg_down"}))
            sys.exit(20)

    db = db_name()
    hba = psql_show("hba_file")
    conf = psql_show("config_file")
    cur_listen = psql_show("listen_addresses")
    result["listen_addresses"] = cur_listen

    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    bdir = os.path.join(BACKUP_ROOT, ts)
    os.makedirs(bdir, exist_ok=True)
    hba_bak = backup_file(hba, bdir)
    conf_bak = backup_file(conf, bdir)
    with open(os.path.join(bdir, "orig_listen_addresses.txt"), "w") as f:
        f.write(cur_listen + "\n")

    # ---- 1. pg_hba: add scoped line if absent --------------------------------
    hba_text = open(hba).read()
    if not hba_has_peer(hba_text, db, ROLE, PEER_CIDR):
        with open(hba, "a") as f:
            if not hba_text.endswith("\n"):
                f.write("\n")
            f.write(hba_line(db, ROLE, PEER_CIDR) + "\n")
        result["hba_added"] = True

    # ---- 2. listen_addresses: change only if needed --------------------------
    need_restart = not listen_exposes_nonlocal(cur_listen)
    if need_restart:
        conf_text = open(conf).read()
        new_val = merged_listen_value(cur_listen)
        repl = "listen_addresses = '%s'\t# %s\n" % (new_val, MARKER)
        if re.search(r"(?m)^\s*#?\s*listen_addresses\s*=", conf_text):
            conf_text = re.sub(r"(?m)^\s*#?\s*listen_addresses\s*=.*$",
                               repl.rstrip("\n"), conf_text, count=1)
        else:
            conf_text = conf_text.rstrip("\n") + "\n" + repl
        with open(conf, "w") as f:
            f.write(conf_text)
        result["listen_changed"] = True

    # ---- 3. apply: reload for hba; restart (guarded) only if listen changed --
    def restore_and_recover(reason):
        # auto-restore configs, bring PG back up, then fail loud
        if conf_bak:
            shutil.copy2(conf_bak, conf)
        if hba_bak:
            shutil.copy2(hba_bak, hba)
        run(["systemctl", "restart", "postgresql"])
        wait_ready(budget=READY_BUDGET_S)
        result["status"] = "restored_" + reason
        print("PGDOOR_RESULT=" + json.dumps(result))
        sys.exit(30)

    if result["listen_changed"]:
        run(["systemctl", "restart", "postgresql"])
        result["restarted"] = True
        if not wait_ready(budget=READY_BUDGET_S):
            restore_and_recover("restart_not_ready")
    else:
        run(["systemctl", "reload", "postgresql"])
        result["reloaded"] = True
        if not wait_ready(budget=30):
            # reload shouldn't drop PG; if it somehow did, restore hba & reload
            restore_and_recover("reload_not_ready")

    # ---- 4. UFW: ssh first (defensive), then scoped 5432 ---------------------
    if shutil.which("ufw"):
        st = run(["ufw", "status"])
        active = "status: active" in st.stdout.lower()
        result["ufw_active"] = active
        # always (re)assert ssh allow so we can never strand the box
        run(["ufw", "allow", "OpenSSH"])
        run(["ufw", "allow", "22/tcp"])
        result["ssh_allowed_ensured"] = True
        if not ufw_has_scoped_5432(st.stdout, PEER_IP, PORT):
            run(["ufw", "allow", "from", PEER_IP, "to", "any",
                 "port", PORT, "proto", "tcp"])
            # re-read to confirm
            st2 = run(["ufw", "status"])
            result["ufw_rule_added"] = ufw_has_scoped_5432(st2.stdout, PEER_IP, PORT)
        else:
            result["ufw_rule_added"] = True
        # NOTE: we deliberately do NOT `ufw enable` if inactive -- never risk
        # locking out a remote box from a script. If inactive, the cloud
        # firewall governs 5432 and the operator opens it there.
    else:
        result["ufw_active"] = False

    # ---- 5. verify door from the box's own vantage ---------------------------
    result["listen_addresses"] = psql_show("listen_addresses")
    # local probe that PG answers on the public bind (proves listen+running)
    try:
        result["pg_ready_public"] = pg_isready(host="168.119.226.254")
    except Exception:
        result["pg_ready_public"] = None
    # re-read hba to confirm the line is present now
    result["hba_present"] = hba_has_peer(open(hba).read(), db, ROLE, PEER_CIDR)

    door_open = result["hba_present"] and (
        result["ufw_rule_added"] or result["ufw_active"] is False)
    result["status"] = "open" if door_open else "partial"
    print("PGDOOR_RESULT=" + json.dumps(result))
    sys.exit(0 if door_open else 31)

if __name__ == "__main__":
    main()
