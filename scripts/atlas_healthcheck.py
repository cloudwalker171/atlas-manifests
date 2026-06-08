#!/opt/atlas/venv/bin/python
"""
atlas_healthcheck.py  --  fast, cheap liveness probe with INSTANT repo alerting.

Runs from its own systemd timer (~every 30s), SEPARATE from the 2-min deploy
puller. On ANY detected failure it immediately (a) writes an alert status file to
the GitHub repo via the Contents API -- status/<node>/alert-<ts>.json -- so you can
read it within seconds without touching the box, and (b) logs it loudly. It does
NOT wait for the next deploy cycle.

Checks (all cheap -- no heavy queries, sub-second target):
  * pg_up        : `pg_isready` (preferred) or a 1-row `SELECT 1` with a short timeout.
  * autopull     : `systemctl is-active atlas-autopull.timer` == active.
  * disk         : % used on each watched mount (default '/' and the backup dir);
                   FAIL at >= ATLAS_HEALTH_DISK_PCT (default 90).
  * workers      : each unit in ATLAS_HEALTH_WORKER_UNITS (space-separated) must be
                   `is-active`. HONEST DEFAULT: if that var is empty/unset, worker
                   liveness is reported as "skipped: no worker units defined yet"
                   -- NOT a failure. Workers don't exist on this box yet; define
                   them later by adding ATLAS_HEALTH_WORKER_UNITS to
                   /etc/atlas/autopull.env (no redeploy of this script needed).

Alerting cadence (so a persistent failure doesn't push a commit every 30s):
  * EDGE-TRIGGERED: pushes immediately the first time a failure appears, and again
    whenever the SET of failing checks changes.
  * RE-PUSH: while still failing, re-pushes every ATLAS_HEALTH_REPUSH_SEC
    (default 600s) so a failure is never silently stale (and so a missed first push
    retries). Set to 0 to push EVERY cycle (truly every ~30s) if you prefer.
  * RECOVERY: on FAIL->OK transition, pushes one status/<node>/alert-<ts>.json with
    status="recovered".
  Local state is tracked in <state_dir>/health_state.json; the latest snapshot is
  always written to <state_dir>/last_health.json regardless of push.

Exit codes (probe semantics):
  0  = the probe RAN successfully (whether infra was healthy or not -- infra health
       is conveyed by the alert file, not the exit code). This keeps the 30s oneshot
       unit from spamming the journal as "failed" while infra is down.
  3  = the probe ITSELF could not run (config/tooling broken). Use --selftest at
       install time to surface this.

Credentials: sourced from /etc/atlas/autopull.env (the same file the puller uses):
  STATUS_TOKEN, STATUS_REPO (e.g. cloudwalker171/atlas-manifests), STATUS_BRANCH
  (default main), STATUS_API_BASE (default https://api.github.com), NODE_ID
  (default hetzner). PG creds come from /etc/atlas/db.env. No secret is hardcoded;
  if STATUS_TOKEN/STATUS_REPO are unset the probe runs LOCAL-ONLY (logs, no push).

USAGE
  /opt/atlas/venv/bin/python atlas_healthcheck.py            # one probe cycle (timer default)
  /opt/atlas/venv/bin/python atlas_healthcheck.py --once     # same, explicit
  /opt/atlas/venv/bin/python atlas_healthcheck.py --selftest # validate the probe can run
                                                             # (config/tooling/repo reachable); no real alert
"""

import os
import sys
import json
import time
import base64
import shutil
import socket
import datetime
import subprocess
import urllib.request
import urllib.error

DB_ENV_PATH = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
AUTOPULL_ENV_PATH = os.environ.get("ATLAS_AUTOPULL_CONF", "/etc/atlas/autopull.env")


def log(msg):
    print(f"[atlas_health] {datetime.datetime.now(datetime.timezone.utc).isoformat()} {msg}",
          flush=True)


def load_env_file(path):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def pick(*names, default=None):
    for n in names:
        if os.environ.get(n):
            return os.environ[n]
    return default


def run(cmd, timeout=10, env=None):
    try:
        proc = subprocess.run(cmd, timeout=timeout, env=env,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return (proc.returncode,
                proc.stdout.decode("utf-8", "replace"),
                proc.stderr.decode("utf-8", "replace"))
    except subprocess.TimeoutExpired:
        return 124, "", f"timeout {timeout}s"
    except FileNotFoundError as e:
        return 127, "", str(e)


# --------------------------------------------------------------------------- #
# Individual checks -> each returns (status, detail)  status in ok|fail|skip
# --------------------------------------------------------------------------- #
def check_pg():
    pg_isready = os.environ.get("ATLAS_PG_ISREADY") or shutil.which("pg_isready")
    host = pick("PGHOST", "DB_HOST", "ATLAS_DB_HOST", default="localhost")
    port = pick("PGPORT", "DB_PORT", "ATLAS_DB_PORT", default="5432")
    db = pick("PGDATABASE", "DB_NAME", "ATLAS_DB_NAME", default="tuanichat_atlas")
    if pg_isready:
        rc, out, err = run([pg_isready, "-h", host, "-p", str(port), "-d", db, "-t", "5"], timeout=8)
        if rc == 0:
            return "ok", f"pg_isready: {out.strip() or 'accepting connections'}"
        return "fail", f"pg_isready rc={rc}: {out.strip() or err.strip() or 'not accepting connections'}"
    # fallback: psql SELECT 1
    psql = os.environ.get("ATLAS_PSQL") or shutil.which("psql")
    if not psql:
        return "fail", "neither pg_isready nor psql found to probe Postgres"
    env = dict(os.environ)
    pw = pick("PGPASSWORD", "DB_PASSWORD", "ATLAS_DB_PASSWORD")
    if pw:
        env["PGPASSWORD"] = pw
    env["PGCONNECT_TIMEOUT"] = "5"
    rc, out, err = run([psql, "-h", host, "-p", str(port), "-U",
                        pick("PGUSER", "DB_USER", "ATLAS_DB_USER", default="atlas"),
                        "-d", db, "-tAqc", "SELECT 1"], timeout=8, env=env)
    if rc == 0 and out.strip() == "1":
        return "ok", "psql SELECT 1 OK"
    return "fail", f"psql probe rc={rc}: {out.strip() or err.strip()}"


def check_systemd_active(unit):
    rc, out, err = run(["systemctl", "is-active", unit], timeout=8)
    state = out.strip() or err.strip()
    if rc == 0 and state == "active":
        return "ok", f"{unit}=active"
    return "fail", f"{unit}={state or 'unknown'} (rc={rc})"


def check_disk():
    pct_limit = int(os.environ.get("ATLAS_HEALTH_DISK_PCT", "90"))
    mounts = os.environ.get("ATLAS_HEALTH_DISK_MOUNTS", "").split()
    if not mounts:
        mounts = ["/"]
        bdir = os.environ.get("ATLAS_BACKUP_DIR", "/opt/atlas/backups")
        # add the backup volume if it resolves to a different existing path
        cand = bdir if os.path.isdir(bdir) else "/opt/atlas"
        if os.path.exists(cand) and cand not in mounts:
            mounts.append(cand)
    worst = []
    failed = False
    for m in mounts:
        try:
            total, used, free = shutil.disk_usage(m)
        except OSError as e:
            worst.append(f"{m}=ERR({e})")
            failed = True
            continue
        pct = used * 100 // total
        worst.append(f"{m}={pct}%")
        if pct >= pct_limit:
            failed = True
    detail = f"limit {pct_limit}% | " + ", ".join(worst)
    return ("fail" if failed else "ok"), detail


def check_workers():
    units = os.environ.get("ATLAS_HEALTH_WORKER_UNITS", "").split()
    if not units:
        return "skip", ("no worker units defined yet (set ATLAS_HEALTH_WORKER_UNITS in "
                        "/etc/atlas/autopull.env once workers exist)")
    bad = []
    for u in units:
        st, d = check_systemd_active(u)
        if st != "ok":
            bad.append(d)
    if bad:
        return "fail", "workers down: " + "; ".join(bad)
    return "ok", f"{len(units)} worker unit(s) active"


def run_checks():
    results = {}
    results["pg_up"] = check_pg()
    results["autopull_timer"] = check_systemd_active(
        os.environ.get("ATLAS_HEALTH_AUTOPULL_UNIT", "atlas-autopull.timer"))
    results["disk"] = check_disk()
    results["workers"] = check_workers()
    return results


# --------------------------------------------------------------------------- #
# GitHub Contents API push (stdlib only; mirrors the puller's gh_put)
# --------------------------------------------------------------------------- #
def gh_request(method, url, token, data=None, timeout=20):
    req = urllib.request.Request(url, method=method, data=data)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("User-Agent", "atlas-healthcheck")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except urllib.error.URLError as e:
        return None, str(e.reason)


def gh_put(path, body_obj, msg):
    token = os.environ.get("STATUS_TOKEN")
    repo = os.environ.get("STATUS_REPO")
    if not token or not repo:
        log(f"  status: LOCAL-ONLY ({path}); STATUS_TOKEN/STATUS_REPO unset -> no push")
        return False
    api = os.environ.get("STATUS_API_BASE", "https://api.github.com")
    branch = os.environ.get("STATUS_BRANCH", "main")
    content_b64 = base64.b64encode(json.dumps(body_obj).encode("utf-8")).decode("ascii")
    # GET existing sha (alert files are new each ts, but be robust)
    code, resp = gh_request("GET", f"{api}/repos/{repo}/contents/{path}?ref={branch}", token)
    sha = None
    if code == 200:
        try:
            sha = json.loads(resp).get("sha")
        except ValueError:
            sha = None
    payload = {"message": msg, "content": content_b64, "branch": branch}
    if sha:
        payload["sha"] = sha
    code, resp = gh_request("PUT", f"{api}/repos/{repo}/contents/{path}", token,
                            data=json.dumps(payload).encode("utf-8"))
    if code and 200 <= code < 300:
        log(f"  status pushed -> {path} ({code})")
        return True
    log(f"  status push FAILED {path} http={code} resp={resp[:240]}")
    return False


# --------------------------------------------------------------------------- #
# State (edge-trigger + re-push)
# --------------------------------------------------------------------------- #
def load_state(state_dir):
    path = os.path.join(state_dir, "health_state.json")
    if os.path.exists(path):
        try:
            return json.load(open(path))
        except (OSError, ValueError):
            pass
    return {"failing": [], "last_push_ts": 0}


def save_state(state_dir, state):
    try:
        os.makedirs(state_dir, exist_ok=True)
        json.dump(state, open(os.path.join(state_dir, "health_state.json"), "w"))
    except OSError as e:
        log(f"  WARNING could not save state: {e}")


def write_snapshot(state_dir, snap):
    try:
        os.makedirs(state_dir, exist_ok=True)
        json.dump(snap, open(os.path.join(state_dir, "last_health.json"), "w"))
    except OSError as e:
        log(f"  WARNING could not write last_health.json: {e}")


# --------------------------------------------------------------------------- #
def selftest():
    """Validate the probe can run: config loads, tooling present, repo reachable.
    Does NOT push a real alert and does NOT fail on unhealthy infra."""
    log("--selftest: validating probe is functional")
    ok = True
    # tooling
    if not (shutil.which("systemctl")):
        log("  [!!] systemctl not found"); ok = False
    if not (os.environ.get("ATLAS_PG_ISREADY") or shutil.which("pg_isready")
            or os.environ.get("ATLAS_PSQL") or shutil.which("psql")):
        log("  [!!] neither pg_isready nor psql found"); ok = False
    # run the checks once (report, don't fail)
    results = run_checks()
    for name, (st, detail) in results.items():
        log(f"  check {name}: {st} -- {detail}")
    # repo reachability (GET, no write)
    token = os.environ.get("STATUS_TOKEN"); repo = os.environ.get("STATUS_REPO")
    if token and repo:
        api = os.environ.get("STATUS_API_BASE", "https://api.github.com")
        code, resp = gh_request("GET", f"{api}/repos/{repo}", token)
        if code and 200 <= code < 300:
            log(f"  repo reachable + token valid: {repo} ({code})")
        else:
            log(f"  [!!] repo/token check failed http={code}: {resp[:200]}"); ok = False
    else:
        log("  status: STATUS_TOKEN/STATUS_REPO unset -> probe will be LOCAL-ONLY (no push). "
            "Set them in /etc/atlas/autopull.env to enable repo alerting.")
    if ok:
        log("--selftest: PASS (probe is functional)")
        sys.exit(0)
    log("--selftest: FAIL (probe cannot run correctly -- fix before relying on alerting)")
    sys.exit(3)


def main():
    args = sys.argv[1:]
    load_env_file(AUTOPULL_ENV_PATH)
    load_env_file(DB_ENV_PATH)

    if "--selftest" in args:
        selftest()
        return

    node = os.environ.get("NODE_ID", "hetzner")
    state_dir = os.environ.get("ATLAS_HEALTH_STATE_DIR", "/var/lib/atlas/health")
    repush_sec = int(os.environ.get("ATLAS_HEALTH_REPUSH_SEC", "600"))
    hostname = socket.gethostname()

    results = run_checks()
    failing = sorted([name for name, (st, _) in results.items() if st == "fail"])
    now = int(time.time())
    ts_iso = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    detail_map = {name: {"status": st, "detail": d} for name, (st, d) in results.items()}
    snapshot = {
        "node": node, "host": hostname, "ts": now, "ts_iso": ts_iso,
        "ok": len(failing) == 0, "failing": failing, "checks": detail_map, "agent": "health-v1",
    }
    write_snapshot(state_dir, snapshot)

    line = " | ".join(f"{n}:{results[n][0]}" for n in results)
    log(f"node={node} {'HEALTHY' if not failing else 'UNHEALTHY'} :: {line}")
    for n in failing:
        log(f"  FAIL {n}: {results[n][1]}")

    state = load_state(state_dir)
    prev_failing = state.get("failing", [])
    last_push = state.get("last_push_ts", 0)

    should_push = False
    reason = ""
    if failing:
        if failing != prev_failing:
            should_push, reason = True, "new/changed failure set"
        elif repush_sec == 0:
            should_push, reason = True, "repush=0 (every cycle)"
        elif now - last_push >= repush_sec:
            should_push, reason = True, f"re-push (>{repush_sec}s still failing)"
    elif prev_failing:
        should_push, reason = True, "recovery"

    if should_push:
        status = "recovered" if (not failing and prev_failing) else "alert"
        alert = {
            "node": node, "host": hostname, "status": status, "ts": now, "ts_iso": ts_iso,
            "failing": failing, "recovered_from": prev_failing if status == "recovered" else None,
            "checks": detail_map, "reason": reason, "agent": "health-v1",
        }
        path = f"status/{node}/alert-{ts_iso}.json"
        log(f"  pushing {status} ({reason}) -> {path}")
        pushed = gh_put(path, alert, f"health {status} {node} {','.join(failing) or 'recovered'} {ts_iso}")
        # only advance last_push_ts if the push actually landed, so a failed push retries next cycle
        if pushed:
            state["last_push_ts"] = now
    state["failing"] = failing
    save_state(state_dir, state)

    # probe ran successfully regardless of infra health -> exit 0
    sys.exit(0)


if __name__ == "__main__":
    main()
