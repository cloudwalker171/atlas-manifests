#!/opt/atlas/venv/bin/python
"""
meta_guardian.py  --  ATLAS Meta Guardian (the watcher-of-watchers)

The higher-level self-heal + decision/escalation layer for the TuaniChat / ATLAS
pipe. It sits ON TOP of the raw health/heartbeat lane (session cc607d9d) and the
v3 auto-pull deploy pipe. It does NOT duplicate the raw heartbeat checks; it
CONSUMES their signals and adds diagnosis, safe auto-healing, and escalation.

=====================================================================
DIVISION OF LABOR (so the two lanes never fight)
  * Heartbeat / fast-alert lane (cc607d9d) = DETECT + ALERT.
      - lightweight, frequent, raw liveness + instant "X is down" alerts.
      - owns its own timer + its own alert namespace (alert-heartbeat-*.json).
  * Meta Guardian (this file)              = DIAGNOSE + AUTO-HEAL + ESCALATE.
      - reads the heartbeat lane's state (if present), the v3 puller's state,
        the backup lane's state, Postgres, and systemd.
      - decides what it can safely fix, fixes it, and escalates the rest with a
        diagnosis to  status/<node>/alert-guardian-<node>-<ts>.json  in the repo.
      - owns ONLY the atlas-guardian.timer cadence + the alert-guardian-* and
        guardian-latest.json namespaces. It never creates or touches the
        autopull timer or any heartbeat-lane timer.

WHAT IT AUTO-HEALS (safe, bounded, rate-limited, fail-loud):
  H1  restart a dead/failed worker or collector systemd unit (atlas-* only,
      NEVER atlas-autopull / ssh / sshd / firewall). Cooldown + max-attempts.
  H2  kick a stalled deploy pipe by `systemctl start atlas-autopull.service`
      (start, never stop/disable/restart-of-the-script).
  H3  clear a stuck enrich_queue claim (status='claimed' older than a threshold
      back to 'queued') so a crashed worker's rows are reworked.
  H4  (OPT-IN, default OFF) roll back to /opt/atlas/restore/last-good. Default
      behavior is to ESCALATE instead, because a blind rollback can fight the
      deploy queue. Enable with GUARDIAN_ALLOW_ROLLBACK=1 only if you mean it.

WHAT IT ESCALATES (cannot safely self-heal -> writes an alert file to the repo):
  E1  Postgres down / unreachable (DB restart is opt-in via GUARDIAN_ALLOW_PG_RESTART).
  E2  the deploy pipe is stale beyond the threshold even after a kick.
  E3  schema drift vs the recorded baseline (never auto-"fixes" schema).
  E4  repeated source failures (same source failing N runs in a row).
  E5  throughput regression while the queue is non-empty and workers are active.
  E6  enrich_queue runaway (depth climbing, age old, nothing draining).
  E7  backup lane stale / missing (consumed from the backup lane's state file).
  E8  box health critical (disk almost full, load pegged) it can't fix itself.
  E9  any heartbeat-lane "down" signal it could not resolve.

HONEST / UNTESTED-UNTIL-LIVE  (flagged inline with  # LIVE-ONLY):
  Workers and collectors DO NOT EXIST YET (data is just starting). Every
  liveness check NO-OPS GRACEFULLY when its target is absent:
    - a systemd unit that isn't installed  -> reported "absent", not "down".
    - a Postgres table that doesn't exist   -> that check is skipped.
    - no psql / no creds                     -> DB checks are skipped (warned).
    - no fleet METRIC log yet                -> throughput check is a no-op.
  The pieces marked # LIVE-ONLY can only be fully validated once the real units
  and Postgres are running. --selftest exercises the pure decision logic with
  synthetic inputs and mutates nothing.

Stdlib + subprocess ONLY (no pip), matching atlas_backup.py / socrata_import.py.
It shells out to systemctl / psql and talks to the GitHub Contents API with
urllib (same mechanism the v3 puller's report() uses), reading STATUS_TOKEN /
STATUS_REPO from /etc/atlas/autopull.env.

MODES
  --once        run one full cycle: check -> heal -> escalate -> write summary  (timer default)
  --dry-run     check + report only; NO heal actions, NO repo push (local files only)
  --check-only  alias of --dry-run
  --selftest    exercise decision logic on synthetic inputs; mutate nothing; PASS/FAIL
  --no-push     run + heal locally but do NOT push to the repo (local mirror only)

EXIT CODES
  0   cycle completed (even if it found and escalated incidents -- incidents are
      DATA, not guardian failures; the timer must keep running)
  2   guardian-internal failure (bad config / unreadable env) -- fail loud
  3   --selftest failed
"""

import os
import re
import sys
import json
import time
import glob
import base64
import socket
import datetime
import subprocess
import urllib.request
import urllib.error

# --------------------------------------------------------------------------- #
# Paths / config (all env-overridable; sensible defaults match the rest of the pipe)
# --------------------------------------------------------------------------- #
AUTOPULL_ENV   = os.environ.get("ATLAS_AUTOPULL_CONF", "/etc/atlas/autopull.env")
DB_ENV_PATH    = os.environ.get("ATLAS_DB_ENV",        "/etc/atlas/db.env")

STATE_DIR      = os.environ.get("GUARDIAN_STATE_DIR",  "/var/lib/atlas/guardian")
AUTOPULL_STATE = os.environ.get("ATLAS_AUTOPULL_STATE","/var/lib/atlas/autopull")
BACKUP_STATE   = os.environ.get("ATLAS_BACKUP_STATE_DIR","/var/lib/atlas/backups")
# Where the fast heartbeat lane is expected to drop its state (tolerated absent):
HEALTH_STATE   = os.environ.get("ATLAS_HEALTH_STATE_DIR","/var/lib/atlas/health")
FLEET_LOG      = os.environ.get("ATLAS_FLEET_LOG",     "/var/log/atlas-fleet/worker.log")
RESTORE_LASTGOOD = os.environ.get("ATLAS_RESTORE_LASTGOOD","/opt/atlas/restore/last-good")

NODE_ID        = os.environ.get("NODE_ID", "")  # filled from autopull.env if empty

def _envint(name, default):
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return int(default)

def _envfloat(name, default):
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return float(default)

# Thresholds
PULL_STALE_SEC      = _envint("GUARDIAN_PULL_STALE_SEC", 600)      # puller runs /2min; 5 missed cycles = stale
QUEUE_AGE_WARN_SEC  = _envint("GUARDIAN_QUEUE_AGE_WARN_SEC", 3600) # oldest queued row older than 1h
STUCK_CLAIM_SEC     = _envint("GUARDIAN_STUCK_CLAIM_SEC", 1800)    # claimed but not done for 30m = stuck
QUEUE_RUNAWAY_DEPTH = _envint("GUARDIAN_QUEUE_RUNAWAY_DEPTH", 250000)
BACKUP_STALE_SEC    = _envint("GUARDIAN_BACKUP_STALE_SEC", 93600)  # 26h
DISK_WARN_PCT       = _envint("GUARDIAN_DISK_WARN_PCT", 85)
DISK_CRIT_PCT       = _envint("GUARDIAN_DISK_CRIT_PCT", 95)
LOAD_WARN_PER_CORE  = _envfloat("GUARDIAN_LOAD_WARN_PER_CORE", 4.0)
MEM_WARN_PCT        = _envint("GUARDIAN_MEM_WARN_PCT", 92)
THROUGHPUT_DROP_PCT = _envint("GUARDIAN_THROUGHPUT_DROP_PCT", 60)
HEAL_COOLDOWN_SEC   = _envint("GUARDIAN_HEAL_COOLDOWN_SEC", 600)
HEAL_MAX_PER_WINDOW = _envint("GUARDIAN_HEAL_MAX_PER_WINDOW", 3)
HEAL_WINDOW_SEC     = _envint("GUARDIAN_HEAL_WINDOW_SEC", 3600)
SOURCE_FAIL_STREAK  = _envint("GUARDIAN_SOURCE_FAIL_STREAK", 3)
HISTORY_KEEP        = _envint("GUARDIAN_HISTORY_KEEP", 240)        # rolling samples (~12h at 3min)

ALLOW_ROLLBACK      = os.environ.get("GUARDIAN_ALLOW_ROLLBACK", "0") == "1"
ALLOW_PG_RESTART    = os.environ.get("GUARDIAN_ALLOW_PG_RESTART", "0") == "1"

# --------------------------------------------------------------------------- #
# Safety: units the guardian is NEVER allowed to touch (mirrors the puller's
# lifeline guardrail spirit). Healing is restricted to atlas-* worker/collector
# units, explicitly excluding the deploy pipe, ssh, the guardian itself, etc.
# --------------------------------------------------------------------------- #
FORBID_UNIT_RE = re.compile(r"(autopull)|(\bssh\b)|(sshd)|(^systemd-)|(guardian)|(getty)|(networking)|(firewall)|(ufw)|(iptables)")
SAFE_HEAL_UNIT_RE = re.compile(r"^atlas-(?!autopull)(?!guardian)[a-z0-9][a-z0-9._-]*\.(service|timer)$")

SEV_ORDER = {"info": 0, "warn": 1, "degraded": 2, "critical": 3}


def now_utc():
    return datetime.datetime.now(datetime.timezone.utc)


def log(msg):
    print(f"[meta_guardian] {now_utc().isoformat()} {msg}", flush=True)


def die(code, msg):
    log(f"FATAL({code}): {msg}")
    sys.exit(code)


# --------------------------------------------------------------------------- #
# env-file loader (KEY=VALUE, optional leading `export`, strips quotes).
# Returns a dict; does NOT pollute os.environ (autopull.env holds the PAT).
# --------------------------------------------------------------------------- #
def load_env_file(path):
    out = {}
    if not os.path.exists(path):
        return out
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if line.lower().startswith("export "):
                    line = line[7:].strip()
                if "=" not in line:
                    continue
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip().strip('"').strip("'")
    except OSError as e:
        log(f"  WARNING could not read {path}: {e}")
    return out


def run(cmd, timeout=30, env=None):
    """Run a command -> (rc, stdout, stderr). Never raises on non-zero."""
    try:
        p = subprocess.run(cmd, env=env, timeout=timeout,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return (p.returncode,
                (p.stdout or b"").decode("utf-8", "replace"),
                (p.stderr or b"").decode("utf-8", "replace"))
    except subprocess.TimeoutExpired:
        return 124, "", f"timeout after {timeout}s"
    except FileNotFoundError as e:
        return 127, "", str(e)


# --------------------------------------------------------------------------- #
# Finding: one observation from a check.
# --------------------------------------------------------------------------- #
class Finding:
    def __init__(self, category, severity, state, detail,
                 evidence=None, needs_human=False):
        assert severity in SEV_ORDER, severity
        self.category = category          # e.g. "postgres", "worker:atlas-fleet"
        self.severity = severity          # info | warn | degraded | critical
        self.state = state                # ok | absent | degraded | down | drift | stale
        self.detail = detail
        self.evidence = evidence or {}
        self.needs_human = needs_human
        self.heal_attempted = None
        self.heal_result = None

    def as_dict(self):
        return {
            "category": self.category,
            "severity": self.severity,
            "state": self.state,
            "detail": self.detail,
            "evidence": self.evidence,
            "needs_human": self.needs_human,
            "heal_attempted": self.heal_attempted,
            "heal_result": self.heal_result,
        }


# --------------------------------------------------------------------------- #
# systemd helpers (all NO-OP gracefully if the unit is absent)
# --------------------------------------------------------------------------- #
def unit_exists(unit):
    rc, out, _ = run(["systemctl", "list-unit-files", unit, "--no-legend"], timeout=15)
    if rc == 0 and out.strip():
        return True
    # list-units catches transient/template units that list-unit-files may miss
    rc, out, _ = run(["systemctl", "list-units", unit, "--all", "--no-legend"], timeout=15)
    return rc == 0 and bool(out.strip())


def unit_props(unit):
    rc, out, _ = run(["systemctl", "show", unit,
                      "-p", "LoadState,ActiveState,SubState,UnitFileState,Result"], timeout=15)
    props = {}
    for line in out.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            props[k] = v
    return props


def list_atlas_units(suffix):
    """Return {name: props} for atlas-*.<suffix> units currently known to systemd."""
    units = {}
    rc, out, _ = run(["systemctl", "list-units", f"atlas-*.{suffix}", "--all",
                      "--no-legend", "--plain"], timeout=20)
    for line in out.splitlines():
        parts = line.split()
        if not parts:
            continue
        name = parts[0]
        if name.endswith("." + suffix) and name.startswith("atlas-"):
            units[name] = unit_props(name)
    # also pick up installed-but-never-started units
    rc, out, _ = run(["systemctl", "list-unit-files", f"atlas-*.{suffix}",
                      "--no-legend"], timeout=20)
    for line in out.splitlines():
        parts = line.split()
        if parts and parts[0] not in units and parts[0].startswith("atlas-"):
            units[parts[0]] = unit_props(parts[0])
    return units


# --------------------------------------------------------------------------- #
# Postgres helpers (skip cleanly if psql/creds/table absent)
# --------------------------------------------------------------------------- #
def pg_context():
    dbenv = load_env_file(DB_ENV_PATH)
    def pick(*names, default=None):
        for n in names:
            if os.environ.get(n):
                return os.environ[n]
            if dbenv.get(n):
                return dbenv[n]
        return default
    ctx = {
        "host": pick("PGHOST", "DB_HOST", "ATLAS_DB_HOST", default="localhost"),
        "port": pick("PGPORT", "DB_PORT", "ATLAS_DB_PORT", default="5432"),
        "db":   pick("PGDATABASE", "DB_NAME", "ATLAS_DB_NAME", default="tuanichat_atlas"),
        "user": pick("PGUSER", "DB_USER", "ATLAS_DB_USER", default="atlas"),
        "pw":   pick("PGPASSWORD", "DB_PASSWORD", "ATLAS_DB_PASSWORD", default=None),
    }
    env = dict(os.environ)
    env["PGHOST"] = ctx["host"]; env["PGPORT"] = str(ctx["port"])
    env["PGDATABASE"] = ctx["db"]; env["PGUSER"] = ctx["user"]
    if ctx["pw"] is not None:
        env["PGPASSWORD"] = ctx["pw"]
    env.setdefault("PGCONNECT_TIMEOUT", "8")
    ctx["env"] = env
    return ctx


def have_psql():
    rc, _, _ = run(["bash", "-lc", "command -v psql"], timeout=10)
    return rc == 0


def psql_scalar(ctx, sql, timeout=25):
    """Try creds first, then fall back to `sudo -u postgres` (peer auth).
       -> (value_str_or_None, error_or_None)."""
    rc, out, err = run(["psql", "-d", ctx["db"], "-tAqc", sql], env=ctx["env"], timeout=timeout)
    if rc == 0:
        return out.strip(), None
    rc2, out2, err2 = run(["sudo", "-u", "postgres", "psql", "-d", ctx["db"], "-tAqc", sql],
                          timeout=timeout)
    if rc2 == 0:
        return out2.strip(), None
    return None, (err.strip() or err2.strip() or "psql failed")


def table_exists(ctx, schema, table):
    val, _ = psql_scalar(ctx, f"SELECT to_regclass('{schema}.{table}') IS NOT NULL")
    return (val or "").lower().startswith("t")


# --------------------------------------------------------------------------- #
# GitHub Contents API push (mirrors the v3 puller's report(): create+update via
# sha, Bearer token, JSON-safe). urllib only. Falls back to local-only if no token.
# --------------------------------------------------------------------------- #
def gh_put(cfg, repo_path, body_bytes, msg):
    token = cfg.get("STATUS_TOKEN", "")
    repo  = cfg.get("STATUS_REPO", "")
    api   = cfg.get("STATUS_API_BASE", "https://api.github.com")
    branch = cfg.get("STATUS_BRANCH", "main")
    # always write a local mirror so there is a record even when push is off/fails
    local = os.path.join(STATE_DIR, "repo_mirror", repo_path)
    try:
        os.makedirs(os.path.dirname(local), exist_ok=True)
        with open(local, "wb") as fh:
            fh.write(body_bytes)
    except OSError as e:
        log(f"  WARNING could not write local mirror {local}: {e}")
    if not token or not repo:
        log(f"  status: local-only ({repo_path}) STATUS_TOKEN/REPO unset")
        return False
    content_b64 = base64.b64encode(body_bytes).decode("ascii")
    url = f"{api}/repos/{repo}/contents/{repo_path}"

    def _api(method, data=None):
        req = urllib.request.Request(url + (f"?ref={branch}" if method == "GET" else ""),
                                     data=data, method=method)
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Accept", "application/vnd.github+json")
        req.add_header("User-Agent", "atlas-meta-guardian")
        req.add_header("X-GitHub-Api-Version", "2022-11-28")
        return urllib.request.urlopen(req, timeout=20)

    sha = None
    try:
        with _api("GET") as r:
            sha = json.loads(r.read().decode("utf-8")).get("sha")
    except urllib.error.HTTPError as e:
        if e.code != 404:
            log(f"  status GET {repo_path} http={e.code} (continuing as create)")
    except (urllib.error.URLError, ValueError, socket.timeout) as e:
        log(f"  status GET {repo_path} error={e} (continuing as create)")

    payload = {"message": msg, "content": content_b64, "branch": branch}
    if sha:
        payload["sha"] = sha
    try:
        with _api("PUT", json.dumps(payload).encode("utf-8")) as r:
            code = r.getcode()
        log(f"  status pushed -> {repo_path} ({code})")
        return True
    except urllib.error.HTTPError as e:
        detail = (e.read()[:240].decode("utf-8", "replace") if e.fp else "")
        log(f"  status push FAILED {repo_path} http={e.code} resp={detail}")
        return False
    except (urllib.error.URLError, socket.timeout) as e:
        log(f"  status push FAILED {repo_path} err={e}")
        return False


# --------------------------------------------------------------------------- #
# Persistent guardian state (history, heal ledger, schema baseline, source streaks)
# --------------------------------------------------------------------------- #
def _state_path(name):
    return os.path.join(STATE_DIR, name)


def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return default


def save_json(path, obj):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(obj, fh)
        os.replace(tmp, path)
    except OSError as e:
        log(f"  WARNING could not write {path}: {e}")


# --------------------------------------------------------------------------- #
# Heal primitives (rate-limited, never touch forbidden units)
# --------------------------------------------------------------------------- #
def heal_allowed(unit, ledger):
    if FORBID_UNIT_RE.search(unit):
        return False, "forbidden unit (pipe/ssh/guardian/firewall) -- never auto-healed"
    if not SAFE_HEAL_UNIT_RE.match(unit):
        return False, "unit does not match safe atlas-* heal pattern"
    hist = [t for t in ledger.get(unit, []) if (time.time() - t) < HEAL_WINDOW_SEC]
    if hist:
        if (time.time() - hist[-1]) < HEAL_COOLDOWN_SEC:
            return False, f"in cooldown ({HEAL_COOLDOWN_SEC}s)"
        if len(hist) >= HEAL_MAX_PER_WINDOW:
            return False, f"max {HEAL_MAX_PER_WINDOW} heals/window reached (flapping -> escalate)"
    return True, "ok"


def record_heal(unit, ledger):
    hist = [t for t in ledger.get(unit, []) if (time.time() - t) < HEAL_WINDOW_SEC]
    hist.append(time.time())
    ledger[unit] = hist


def heal_restart_unit(unit, ledger, dry):
    ok, why = heal_allowed(unit, ledger)
    if not ok:
        return False, f"NOT healed: {why}"
    if dry:
        return False, "dry-run: would restart"
    rc, out, err = run(["systemctl", "restart", unit], timeout=60)
    record_heal(unit, ledger)
    if rc == 0:
        return True, "restarted ok"
    return False, f"restart rc={rc}: {err.strip()[:160]}"


def heal_kick_pull(ledger, dry):
    # start (never stop/disable) the puller's service once to force a cycle.
    unit = "atlas-autopull.service"
    key = "__pull_kick__"
    hist = [t for t in ledger.get(key, []) if (time.time() - t) < HEAL_WINDOW_SEC]
    if hist and (time.time() - hist[-1]) < HEAL_COOLDOWN_SEC:
        return False, "kick in cooldown"
    if dry:
        return False, "dry-run: would `systemctl start atlas-autopull.service`"
    rc, _, err = run(["systemctl", "start", unit], timeout=60)
    hist.append(time.time()); ledger[key] = hist
    return (rc == 0), ("kicked pull" if rc == 0 else f"kick rc={rc}: {err.strip()[:160]}")


# --------------------------------------------------------------------------- #
# CHECKS
# --------------------------------------------------------------------------- #
def check_box_health():
    findings = []
    # load average (always available on Linux)
    try:
        la1, la5, la15 = os.getloadavg()
        cores = os.cpu_count() or 1
        per = la5 / cores
        sev = "critical" if per >= LOAD_WARN_PER_CORE * 2 else ("warn" if per >= LOAD_WARN_PER_CORE else "info")
        findings.append(Finding("box:load", sev, "ok" if sev == "info" else "degraded",
                                f"load5={la5:.2f} over {cores} cores ({per:.2f}/core)",
                                {"load1": la1, "load5": la5, "load15": la15, "cores": cores},
                                needs_human=(sev == "critical")))
    except OSError:
        pass
    # disk on a few key mounts
    for mount in ("/", "/opt/atlas", os.path.dirname(BACKUP_STATE) or "/var/lib"):
        try:
            total, used, free = __import__("shutil").disk_usage(mount)
            pct = used * 100 // total
            sev = "critical" if pct >= DISK_CRIT_PCT else ("warn" if pct >= DISK_WARN_PCT else "info")
            findings.append(Finding(f"box:disk:{mount}", sev,
                                    "ok" if sev == "info" else ("down" if sev == "critical" else "degraded"),
                                    f"{pct}% used, {free // (1024**3)}GB free",
                                    {"pct_used": pct, "free_gb": free // (1024**3)},
                                    needs_human=(sev == "critical")))
        except OSError:
            continue
    # memory
    try:
        meminfo = {}
        with open("/proc/meminfo") as fh:
            for line in fh:
                k, _, v = line.partition(":")
                meminfo[k.strip()] = int(v.strip().split()[0])  # kB
        total = meminfo.get("MemTotal", 0)
        avail = meminfo.get("MemAvailable", meminfo.get("MemFree", 0))
        if total:
            used_pct = (total - avail) * 100 // total
            sev = "warn" if used_pct >= MEM_WARN_PCT else "info"
            findings.append(Finding("box:mem", sev, "ok" if sev == "info" else "degraded",
                                    f"{used_pct}% memory used",
                                    {"used_pct": used_pct, "total_kb": total}))
    except (OSError, ValueError):
        pass
    return findings


def check_pipe(cfg):
    """The self-update deploy pipe must keep running or NOTHING else ships.
       LIVE-ONLY for the active/enabled states; freshness uses the puller's own
       state files written by report()/heartbeat()."""
    findings = []
    tprops = unit_props("atlas-autopull.timer")
    if tprops.get("LoadState") != "loaded":
        findings.append(Finding("pipe:timer", "critical", "absent",
                                "atlas-autopull.timer not loaded -- the v3 puller is not installed yet",
                                tprops, needs_human=True))
        return findings  # nothing else to check until the pipe exists
    active = tprops.get("ActiveState")
    enabled = tprops.get("UnitFileState")
    sev = "info" if (active == "active") else "critical"
    findings.append(Finding("pipe:timer",
                            "info" if active == "active" and enabled in ("enabled", "enabled-runtime") else "critical",
                            "ok" if active == "active" else "down",
                            f"atlas-autopull.timer active={active} enabled={enabled}",
                            tprops, needs_human=(active != "active")))
    # freshness: last_seq mtime + last_status.json
    last_seq_f = os.path.join(AUTOPULL_STATE, "last_seq")
    last_status_f = os.path.join(AUTOPULL_STATE, "last_status.json")
    newest = 0
    for f in (last_seq_f, last_status_f):
        try:
            newest = max(newest, os.path.getmtime(f))
        except OSError:
            pass
    if newest == 0:
        findings.append(Finding("pipe:freshness", "warn", "absent",
                                "no autopull state yet (puller may not have run a full cycle)",
                                {"state_dir": AUTOPULL_STATE}))
    else:
        age = int(time.time() - newest)
        last_seq = "?"
        try:
            last_seq = open(last_seq_f).read().strip()
        except OSError:
            pass
        sev = "critical" if age > PULL_STALE_SEC else "info"
        findings.append(Finding("pipe:freshness", sev,
                                "ok" if sev == "info" else "stale",
                                f"last pipe activity {age}s ago (last_seq={last_seq}); threshold {PULL_STALE_SEC}s",
                                {"age_sec": age, "last_seq": last_seq},
                                needs_human=(sev == "critical")))
    return findings


def check_postgres(ctx, psql_ok):
    if not psql_ok:
        return [Finding("postgres", "warn", "absent",
                        "psql not found -- DB checks skipped (LIVE-ONLY: needs a provisioned PG box)")]
    val, err = psql_scalar(ctx, "SELECT 1")
    if val == "1":
        return [Finding("postgres", "info", "ok", "Postgres accepting queries", {"db": ctx["db"]})]
    return [Finding("postgres", "critical", "down",
                    f"Postgres unreachable: {err}", {"db": ctx["db"], "host": ctx["host"]},
                    needs_human=True)]


def check_schema_drift(ctx, psql_ok):
    """Fingerprint atlas.* columns and compare to a recorded baseline.
       First run records the baseline (no drift). Never auto-fixes schema."""
    if not psql_ok:
        return []
    if not table_exists(ctx, "atlas", "business"):
        return [Finding("schema", "warn", "absent",
                        "atlas.business not present yet (schema not loaded) -- LIVE-ONLY")]
    sql = ("SELECT table_name||':'||column_name||':'||data_type "
           "FROM information_schema.columns WHERE table_schema='atlas' "
           "ORDER BY 1")
    val, err = psql_scalar(ctx, sql, timeout=40)
    if val is None:
        return [Finding("schema", "warn", "degraded", f"could not read schema: {err}")]
    current = sorted([l.strip() for l in val.splitlines() if l.strip()])
    fp = __import__("hashlib").sha256("\n".join(current).encode()).hexdigest()
    base_path = _state_path("schema_baseline.json")
    baseline = load_json(base_path, None)
    if not baseline:
        save_json(base_path, {"fingerprint": fp, "columns": current, "ts": int(time.time())})
        return [Finding("schema", "info", "ok",
                        f"schema baseline recorded ({len(current)} atlas.* columns)",
                        {"fingerprint": fp[:12]})]
    if baseline.get("fingerprint") == fp:
        return [Finding("schema", "info", "ok",
                        f"schema matches baseline ({len(current)} columns)", {"fingerprint": fp[:12]})]
    old = set(baseline.get("columns", []))
    new = set(current)
    added = sorted(new - old)
    removed = sorted(old - new)
    return [Finding("schema", "degraded", "drift",
                    f"schema drift vs baseline: +{len(added)} -{len(removed)} columns "
                    f"(importers may break -- review, then re-baseline by deleting {base_path})",
                    {"added": added[:30], "removed": removed[:30]}, needs_human=True)]


def check_workers(ctx, psql_ok, ledger, dry):
    """Workers (atlas-fleet / atlas-secondary). NO-OP if absent (they don't exist yet)."""
    findings = []
    worker_units = ["atlas-fleet.service", "atlas-secondary.service"]
    any_active = False
    for unit in worker_units:
        props = unit_props(unit)
        if props.get("LoadState") != "loaded":
            findings.append(Finding(f"worker:{unit}", "info", "absent",
                                    f"{unit} not installed yet (expected pre-launch)", props))
            continue
        active = props.get("ActiveState")
        result = props.get("Result", "")
        if active == "active":
            any_active = True
            findings.append(Finding(f"worker:{unit}", "info", "ok", f"{unit} active", props))
        elif active == "failed" or result not in ("success", ""):
            f = Finding(f"worker:{unit}", "critical", "down",
                        f"{unit} active={active} result={result}", props, needs_human=True)
            ok, msg = heal_restart_unit(unit, ledger, dry)        # H1
            f.heal_attempted = "restart"; f.heal_result = msg
            if ok:
                f.severity, f.state, f.needs_human = "warn", "healed", False
            findings.append(f)
        else:  # inactive/dead but not failed -> enabled but idle is OK for a oneshot-ish worker
            sev = "warn" if props.get("UnitFileState") == "enabled" else "info"
            findings.append(Finding(f"worker:{unit}", sev,
                                    "degraded" if sev == "warn" else "ok",
                                    f"{unit} active={active} (enabled but not running)", props))
    # throughput (LIVE-ONLY -- needs a real fleet METRIC log + counters)
    if any_active:
        findings.extend(_check_throughput(ctx, psql_ok))
    return findings


def _parse_last_metric(path):
    """Parse the last `METRIC ... ops_per_min=N ...` line from the fleet log."""
    try:
        with open(path, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.seek(max(0, size - 65536))
            tail = fh.read().decode("utf-8", "replace")
    except OSError:
        return None
    last = None
    for line in tail.splitlines():
        if "METRIC" in line and "ops_per_min=" in line:
            last = line
    if not last:
        return None
    m = {}
    for tok in last.split():
        if "=" in tok:
            k, v = tok.split("=", 1)
            try:
                m[k] = float(v)
            except ValueError:
                m[k] = v
    return m


def _check_throughput(ctx, psql_ok):
    findings = []
    metric = _parse_last_metric(FLEET_LOG)
    if not metric:
        return [Finding("throughput", "info", "absent",
                        "no fleet METRIC line yet (LIVE-ONLY: needs a running worker)")]
    opm = metric.get("ops_per_min")
    hist_path = _state_path("throughput_history.jsonl")
    prior = []
    try:
        with open(hist_path) as fh:
            for line in fh.readlines()[-HISTORY_KEEP:]:
                try:
                    prior.append(json.loads(line))
                except ValueError:
                    pass
    except OSError:
        pass
    # only compare when the queue is non-empty (a drop on an empty queue is expected)
    queue_nonempty = True
    if psql_ok and table_exists(ctx, "atlas", "enrich_queue"):
        val, _ = psql_scalar(ctx, "SELECT count(*) FROM atlas.enrich_queue WHERE status IN ('queued','pending')")
        queue_nonempty = (val or "0").isdigit() and int(val) > 0
    prior_opm = sorted([p["ops_per_min"] for p in prior if isinstance(p.get("ops_per_min"), (int, float))])
    median = prior_opm[len(prior_opm) // 2] if prior_opm else None
    # append current sample
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        with open(hist_path, "a") as fh:
            fh.write(json.dumps({"ts": int(time.time()), "ops_per_min": opm}) + "\n")
    except OSError:
        pass
    if median and isinstance(opm, (int, float)) and queue_nonempty and median > 0:
        drop = (median - opm) * 100.0 / median
        if drop >= THROUGHPUT_DROP_PCT:
            findings.append(Finding("throughput", "degraded", "degraded",
                                    f"throughput regression: ops_per_min={opm:.0f} vs median {median:.0f} "
                                    f"({drop:.0f}% drop) while queue non-empty",
                                    {"ops_per_min": opm, "median": median, "drop_pct": round(drop, 1)},
                                    needs_human=True))
            return findings
    findings.append(Finding("throughput", "info", "ok",
                            f"ops_per_min={opm} (median {median})",
                            {"ops_per_min": opm, "median": median}))
    return findings


def check_collectors(ledger, dry, streaks):
    """Every atlas-* collector service/timer. NO-OP if none installed.
       Repeated per-source failures (streak) escalate."""
    findings = []
    services = list_atlas_units("service")
    known_infra = {"atlas-autopull.service", "atlas-guardian.service",
                   "atlas-fleet.service", "atlas-secondary.service",
                   "atlas-backup.service", "atlas-pg.service", "atlas-brain.service",
                   "atlas-jarvis.service", "atlas-dashboard.service"}
    collectors = {n: p for n, p in services.items() if n not in known_infra}
    if not collectors:
        findings.append(Finding("collectors", "info", "absent",
                                "no collector units installed yet (expected pre-launch)"))
        return findings
    for unit, props in sorted(collectors.items()):
        active = props.get("ActiveState"); result = props.get("Result", "")
        if props.get("LoadState") != "loaded":
            continue
        if active == "failed" or result not in ("success", ""):
            streaks[unit] = streaks.get(unit, 0) + 1
            f = Finding(f"collector:{unit}", "degraded", "down",
                        f"{unit} active={active} result={result} (failure streak {streaks[unit]})",
                        props)
            if streaks[unit] >= SOURCE_FAIL_STREAK:
                f.severity, f.needs_human = "critical", True
                f.detail += f" -- repeated source failure (>= {SOURCE_FAIL_STREAK}); ESCALATING, not auto-restarting again"
            else:
                ok, msg = heal_restart_unit(unit, ledger, dry)   # H1
                f.heal_attempted = "restart"; f.heal_result = msg
                if ok:
                    f.severity, f.state = "warn", "healed"
            findings.append(f)
        else:
            streaks[unit] = 0
            findings.append(Finding(f"collector:{unit}", "info", "ok",
                                    f"{unit} active={active}", props))
    return findings


def check_enrich_queue(ctx, psql_ok, ledger, dry):
    """Depth, oldest-age, stuck claims. NO-OP if table absent."""
    if not psql_ok:
        return []
    if not table_exists(ctx, "atlas", "enrich_queue"):
        return [Finding("enrich_queue", "info", "absent",
                        "atlas.enrich_queue not present yet (LIVE-ONLY)")]
    findings = []
    depth, _ = psql_scalar(ctx, "SELECT count(*) FROM atlas.enrich_queue WHERE status IN ('queued','pending')")
    depth = int(depth) if (depth or "").isdigit() else 0
    # oldest queued age -- tolerate either a created_at or claimed_at column
    age_sec = None
    for col in ("created_at", "queued_at", "inserted_at"):
        val, err = psql_scalar(ctx,
            f"SELECT EXTRACT(EPOCH FROM now()-min({col}))::bigint FROM atlas.enrich_queue "
            f"WHERE status IN ('queued','pending')")
        if val and val.lstrip("-").isdigit():
            age_sec = int(val); break
    sev = "info"
    if depth >= QUEUE_RUNAWAY_DEPTH:
        sev = "critical"
    elif age_sec is not None and age_sec > QUEUE_AGE_WARN_SEC:
        sev = "warn"
    findings.append(Finding("enrich_queue:depth",
                            sev, "ok" if sev == "info" else "degraded",
                            f"queued={depth} oldest_age={age_sec}s (warn>{QUEUE_AGE_WARN_SEC}s, runaway>={QUEUE_RUNAWAY_DEPTH})",
                            {"queued": depth, "oldest_age_sec": age_sec},
                            needs_human=(sev == "critical")))
    # stuck claims -> H3 (clear back to queued)
    stuck = None
    for col in ("claimed_at", "claimed_ts"):
        val, _ = psql_scalar(ctx,
            f"SELECT count(*) FROM atlas.enrich_queue WHERE status='claimed' "
            f"AND {col} < now() - interval '{STUCK_CLAIM_SEC} seconds'")
        if (val or "").isdigit():
            stuck = int(val); stuck_col = col; break
    if stuck:
        f = Finding("enrich_queue:stuck", "degraded", "degraded",
                    f"{stuck} rows stuck in 'claimed' > {STUCK_CLAIM_SEC}s (crashed worker?)",
                    {"stuck": stuck})
        key = "__stuck_claims__"
        hist = [t for t in ledger.get(key, []) if (time.time() - t) < HEAL_WINDOW_SEC]
        if dry:
            f.heal_attempted = "clear_stuck_claims"; f.heal_result = "dry-run: would reset to 'queued'"
        elif hist and (time.time() - hist[-1]) < HEAL_COOLDOWN_SEC:
            f.heal_attempted = "clear_stuck_claims"; f.heal_result = "in cooldown -- escalating"
            f.needs_human = True
        else:
            val, err = psql_scalar(ctx,
                f"UPDATE atlas.enrich_queue SET status='queued' WHERE status='claimed' "
                f"AND {stuck_col} < now() - interval '{STUCK_CLAIM_SEC} seconds'")
            hist.append(time.time()); ledger[key] = hist
            f.heal_attempted = "clear_stuck_claims"
            f.heal_result = "reset stuck claims to 'queued'" if err is None else f"FAILED: {err}"
            if err is None:
                f.severity, f.state = "warn", "healed"
            else:
                f.needs_human = True
        findings.append(f)
    return findings


def check_backup_lane():
    """CONSUME the backup lane's state (don't duplicate it). Escalate if stale/missing."""
    path = os.path.join(BACKUP_STATE, "last_backup.json")
    data = load_json(path, None)
    if not data:
        return [Finding("backup_lane", "warn", "absent",
                        "no backup state yet (backup lane not deployed or never ran) -- consumed signal")]
    ts = data.get("ts", 0)
    age = int(time.time() - ts) if ts else None
    if age is None:
        return [Finding("backup_lane", "warn", "degraded", "backup state has no ts", data)]
    sev = "critical" if age > BACKUP_STALE_SEC else "info"
    return [Finding("backup_lane", sev, "ok" if sev == "info" else "stale",
                    f"last backup {age}s ago ({data.get('dump_size_human','?')}, "
                    f"verify_entries={data.get('verify_toc_entries')}); threshold {BACKUP_STALE_SEC}s",
                    {"age_sec": age, "dump": data.get("dump"), "offbox": data.get("offbox")},
                    needs_human=(sev == "critical"))]


def check_heartbeat_lane():
    """CONSUME the fast heartbeat/alert lane's (atlas-health, session cc607d9d)
       last_health.json snapshot -- the documented contract is:
         {"ok": bool, "failing": [names], "checks": {name:{status,detail}}, "ts": int, "agent":"health-v1"}
       The guardian keys off that real schema (not a substring scan), surfaces
       any failing checks for diagnosis, and -- importantly -- detects a DEAD
       probe (stale snapshot) since a silent fast-alerter is its own incident.
       Tolerates total absence (guardian then runs standalone, no conflict)."""
    if not os.path.isdir(HEALTH_STATE):
        return [Finding("heartbeat_lane", "info", "absent",
                        f"heartbeat lane state dir {HEALTH_STATE} not present -- "
                        f"guardian running standalone (no conflict)")]
    snap = load_json(os.path.join(HEALTH_STATE, "last_health.json"), None)
    if not snap:
        return [Finding("heartbeat_lane", "info", "absent",
                        f"{HEALTH_STATE} exists but no last_health.json yet "
                        f"(atlas-health probe not run / not deployed)")]
    findings = []
    # dead-probe detection: it runs every ~30s; a snapshot older than 5 min means
    # the fast-alert lane itself is down -- the guardian must say so loudly.
    ts = snap.get("ts", 0)
    age = int(time.time() - ts) if ts else None
    if age is not None and age > 300:
        findings.append(Finding("heartbeat_lane:liveness", "critical", "stale",
                                f"atlas-health snapshot is {age}s old (probe runs /30s) -- "
                                f"the FAST-ALERT lane appears DEAD; raw alerting is not covered",
                                {"age_sec": age}, needs_human=True))
    if snap.get("ok") is True:
        findings.append(Finding("heartbeat_lane", "info", "ok",
                                f"atlas-health reports healthy (agent={snap.get('agent')})",
                                {"age_sec": age}))
    else:
        failing = snap.get("failing", []) or []
        findings.append(Finding("heartbeat_lane", "degraded", "degraded",
                                f"atlas-health reports failing checks: {', '.join(failing) or 'unknown'} "
                                f"-- guardian surfacing for diagnosis/heal",
                                {"failing": failing, "checks": snap.get("checks"), "age_sec": age},
                                needs_human=True))
    return findings


# --------------------------------------------------------------------------- #
# Escalation
# --------------------------------------------------------------------------- #
def escalate(cfg, findings, node, push):
    """Write a rolling guardian-latest.json every run, plus a timestamped
       alert-guardian-<node>-<ts>.json whenever there is anything >= degraded
       (or a needs_human finding). Repo path mirrors the puller's status/<node>/."""
    incidents = [f for f in findings if SEV_ORDER[f.severity] >= SEV_ORDER["degraded"] or f.needs_human]
    worst = "info"
    for f in findings:
        if SEV_ORDER[f.severity] > SEV_ORDER[worst]:
            worst = f.severity
    summary = {
        "agent": "meta-guardian",
        "node": node,
        "ts": int(time.time()),
        "overall": worst,
        "checks": len(findings),
        "incidents": len(incidents),
        "healed": sum(1 for f in findings if f.state == "healed"),
        "escalated": sum(1 for f in findings if f.needs_human),
        "findings": [f.as_dict() for f in findings],
    }
    body = json.dumps(summary, indent=2).encode("utf-8")
    # always: rolling latest (guardian's own heartbeat)
    save_json(_state_path("guardian-latest.json"), summary)
    if push:
        gh_put(cfg, f"status/{node}/guardian-latest.json", body, f"guardian {node} {worst}")
    # incident alert
    if incidents:
        ts = now_utc().strftime("%Y%m%dT%H%M%SZ")
        alert = dict(summary)
        alert["findings"] = [f.as_dict() for f in incidents]
        abody = json.dumps(alert, indent=2).encode("utf-8")
        save_json(_state_path(f"alert-guardian-{node}-{ts}.json"), alert)
        if push:
            gh_put(cfg, f"status/{node}/alert-guardian-{node}-{ts}.json", abody,
                   f"ALERT guardian {node} {worst} ({len(incidents)} incident(s))")
    return summary


# --------------------------------------------------------------------------- #
# Selftest: exercise pure decision logic; mutate nothing.
# --------------------------------------------------------------------------- #
def selftest():
    rc = 0
    def check(name, cond):
        nonlocal rc
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}")
        if not cond:
            rc = 1

    # 1. the heal guardrail must REFUSE the pipe / ssh / guardian itself
    led = {}
    for bad in ("atlas-autopull.service", "atlas-autopull.timer", "ssh.service",
                "sshd.service", "atlas-guardian.service", "ufw.service",
                "systemd-journald.service"):
        ok, _ = heal_allowed(bad, led)
        check(f"refuse heal of {bad}", ok is False)
    # 2. it must ALLOW a real worker/collector
    for good in ("atlas-fleet.service", "atlas-secondary.service",
                 "atlas-chicago.service", "atlas-nyc-dcwp.timer"):
        ok, why = heal_allowed(good, led)
        check(f"allow heal of {good}", ok is True)
    # 3. cooldown / max-per-window logic
    led2 = {"atlas-fleet.service": [time.time()]}
    ok, why = heal_allowed("atlas-fleet.service", led2)
    check("cooldown blocks rapid re-heal", ok is False and "cooldown" in why)
    led3 = {"atlas-x.service": [time.time() - HEAL_COOLDOWN_SEC - 1] * HEAL_MAX_PER_WINDOW}
    ok, why = heal_allowed("atlas-x.service", led3)
    check("max-per-window blocks flapping", ok is False and "max" in why)
    # 4. severity escalation selection
    fs = [Finding("a", "info", "ok", "x"),
          Finding("b", "degraded", "drift", "y"),
          Finding("c", "warn", "degraded", "z", needs_human=True)]
    incidents = [f for f in fs if SEV_ORDER[f.severity] >= SEV_ORDER["degraded"] or f.needs_human]
    check("escalation selects degraded + needs_human", len(incidents) == 2)
    # 5. status json is valid + base64-roundtrips (matches puller's content encoding)
    body = json.dumps({"node": "hetzner", "overall": "critical"}).encode()
    b64 = base64.b64encode(body)
    check("base64 status body roundtrips", base64.b64decode(b64) == body)
    # 6. metric tail parser
    import tempfile
    with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False) as fh:
        fh.write("noise\nMETRIC window_s=60 workers=50 ops_per_min=1234 total_enriched=9\nMETRIC window_s=60 workers=50 ops_per_min=88 total_enriched=10\n")
        tmp = fh.name
    m = _parse_last_metric(tmp)
    os.unlink(tmp)
    check("metric parser reads LAST ops_per_min", m and m.get("ops_per_min") == 88.0)
    # 7. forbidden-unit regex catches substrings
    check("forbid regex catches atlas-autopull anywhere", bool(FORBID_UNIT_RE.search("atlas-autopull.timer")))

    print("META GUARDIAN SELFTEST:", "PASS" if rc == 0 else "FAIL")
    return rc


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    args = sys.argv[1:]
    if "--selftest" in args:
        sys.exit(3 if selftest() else 0)

    dry = ("--dry-run" in args) or ("--check-only" in args)
    push = ("--no-push" not in args) and not dry

    os.makedirs(STATE_DIR, exist_ok=True)
    cfg = load_env_file(AUTOPULL_ENV)
    node = NODE_ID or cfg.get("NODE_ID") or os.environ.get("NODE_ID") or socket.gethostname() or "hetzner"
    log("=" * 72)
    log(f"META GUARDIAN start node={node} mode={'dry-run' if dry else 'live'} push={push}")
    log(f"  rollback_enabled={ALLOW_ROLLBACK} pg_restart_enabled={ALLOW_PG_RESTART}")

    ledger  = load_json(_state_path("heal_ledger.json"), {})
    streaks = load_json(_state_path("source_streaks.json"), {})

    psql_ok = have_psql()
    ctx = pg_context()

    findings = []
    try:
        findings += check_box_health()
        findings += check_pipe(cfg)
        findings += check_postgres(ctx, psql_ok)
        findings += check_schema_drift(ctx, psql_ok)
        findings += check_workers(ctx, psql_ok, ledger, dry)
        findings += check_collectors(ledger, dry, streaks)
        findings += check_enrich_queue(ctx, psql_ok, ledger, dry)
        findings += check_backup_lane()
        findings += check_heartbeat_lane()
    except Exception as e:  # noqa: BLE001 -- a check bug must not crash the watcher silently
        log(f"  WARNING a check raised (continuing): {type(e).__name__}: {e}")
        findings.append(Finding("guardian:self", "warn", "degraded",
                                f"a check raised {type(e).__name__}: {e}"))

    # H2: if the pipe is stale/down, kick it once (start only).
    pipe_bad = any(f.category.startswith("pipe") and SEV_ORDER[f.severity] >= SEV_ORDER["critical"]
                   for f in findings)
    if pipe_bad:
        ok, msg = heal_kick_pull(ledger, dry)
        for f in findings:
            if f.category == "pipe:freshness" or f.category == "pipe:timer":
                f.heal_attempted = "kick_pull"; f.heal_result = msg
                if ok:
                    f.state = "healed" if f.state != "absent" else f.state
        log(f"  pipe heal (kick): {msg}")

    # E1: Postgres restart is opt-in only.
    if ALLOW_PG_RESTART:
        for f in findings:
            if f.category == "postgres" and f.state == "down" and not dry:
                rc, _, err = run(["systemctl", "restart", "postgresql"], timeout=90)
                f.heal_attempted = "restart postgresql"
                f.heal_result = "restarted" if rc == 0 else f"FAILED rc={rc}: {err.strip()[:160]}"
                if rc == 0:
                    f.state = "healed"; f.severity = "warn"

    # persist learning state
    save_json(_state_path("heal_ledger.json"), ledger)
    save_json(_state_path("source_streaks.json"), streaks)

    summary = escalate(cfg, findings, node, push)

    # verbose per-finding log
    for f in findings:
        tag = f.severity.upper()
        heal = f" heal={f.heal_attempted}:{f.heal_result}" if f.heal_attempted else ""
        log(f"  [{tag:8}] {f.category:28} {f.state:8} {f.detail}{heal}")
    log(f"SUMMARY node={node} overall={summary['overall']} checks={summary['checks']} "
        f"incidents={summary['incidents']} healed={summary['healed']} "
        f"escalated={summary['escalated']}")
    log("=" * 72)
    # incidents are DATA, not guardian failure -> exit 0 so the timer keeps running.
    sys.exit(0)


if __name__ == "__main__":
    main()
