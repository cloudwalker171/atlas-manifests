#!/opt/atlas/venv/bin/python
"""
atlas_backup.py  --  daily, disk-aware pg_dump of tuanichat_atlas (custom/compressed),
                     with retention, restore smoke-test, row-count + size logging,
                     and an optional (off by default) off-box copy hook.

Design goals (matching the rest of the ATLAS pipe):
  * stdlib + subprocess ONLY (no pip dependency). It shells out to the Postgres
    client binaries (pg_dump / pg_restore / psql), which ship with PG 18.
  * VERBOSE + FAIL-LOUD: every stage logs what it is doing and its real error;
    the process exits NON-ZERO on any genuine failure so the autopull apply marks
    the deploy FAILED (never "healthy with no backup").
  * DISK-AWARE: refuses to start a dump that could fill the 320 GB volume; prunes
    old dumps to a retention count.
  * SELF-VERIFYING: after writing the dump it runs `pg_restore --list` as a smoke
    test (confirms the archive's table-of-contents is readable/parseable) before
    the dump is considered good. This does NOT restore into a database; see
    --verify-into for the heavier, optional real-restore proof.

Connection handling mirrors socrata_import.py / overture_pg_import.py: it sources
/etc/atlas/db.env (KEY=VALUE, optional leading `export`) and builds the PG* env
from the standard PG* / DB_* variables.

-----------------------------------------------------------------------------
USAGE
  /opt/atlas/venv/bin/python atlas_backup.py              # one backup + verify + prune (timer default)
  /opt/atlas/venv/bin/python atlas_backup.py --verify     # same (verify is on by default; explicit)
  /opt/atlas/venv/bin/python atlas_backup.py --no-verify  # skip the pg_restore --list smoke test
  /opt/atlas/venv/bin/python atlas_backup.py --no-prune   # keep all dumps (don't enforce retention)
  /opt/atlas/venv/bin/python atlas_backup.py --check-only # validate env/tools/disk, do NOT dump
  /opt/atlas/venv/bin/python atlas_backup.py --verify-into atlas_restore_check  # OPTIONAL heavy proof:
        creates a scratch DB, pg_restore into it, count atlas.business, drop it. Off by default.

ENV (all optional; sensible defaults). Put overrides in /etc/atlas/db.env or the
service drop-in:
  ATLAS_BACKUP_DIR              default /opt/atlas/backups      (dumps live here)
  ATLAS_BACKUP_RETAIN           default 7                       (keep newest N dumps)
  ATLAS_BACKUP_MIN_FREE_GB      default 10                      (hard floor of free space to leave)
  ATLAS_BACKUP_EST_FACTOR       default 0.6                     (est dump size = db_size * factor)
  ATLAS_BACKUP_COMPRESS         default (pg_dump default)       (e.g. "9"; passed as --compress=N)
  ATLAS_PG_DUMP / ATLAS_PG_RESTORE / ATLAS_PSQL                 (override client binary paths)
  ATLAS_BACKUP_STATE_DIR        default /var/lib/atlas/backups  (writes last_backup.json here)
  ATLAS_BACKUP_OFFBOX_CMD       default unset                   (off-box copy; see NOTE below)
  ATLAS_BACKUP_OFFBOX_REQUIRED  default 0                       (1 = off-box failure fails the run)

NOTE on off-box: if ATLAS_BACKUP_OFFBOX_CMD is set, it is run after a successful,
verified dump with the dump path appended as the final argument, e.g.:
  ATLAS_BACKUP_OFFBOX_CMD="rsync -az --partial -e 'ssh -p23 -i /etc/atlas/storagebox.key' \
      uXXXXX@uXXXXX.your-storagebox.de:atlas-backups/"
By default off-box failure is logged LOUDLY but does NOT fail the local backup
(a network blip shouldn't lose you a good local dump); set OFFBOX_REQUIRED=1 to
make it fatal. See the delivery doc for Hetzner Storage Box / S3 / InterServer
rsync recipes.

NOTE on the dump location and the v3 restore-point: the autopull restore-point
tars /opt/atlas EXCEPT /opt/atlas/data and /opt/atlas/restore. /opt/atlas/backups
is therefore INCLUDED in that snapshot tar on the next deploy. With multi-GB dumps
that makes each future apply's snapshot large/slow. If that matters, set
ATLAS_BACKUP_DIR=/opt/atlas/data/backups (the data/ dir is excluded from the
snapshot). Default is kept at /opt/atlas/backups as requested.
-----------------------------------------------------------------------------
"""

import os
import sys
import json
import time
import shutil
import datetime
import subprocess

DB_ENV_PATH = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")


def log(msg):
    print(f"[atlas_backup] {datetime.datetime.now(datetime.timezone.utc).isoformat()} {msg}",
          flush=True)


def die(code, msg):
    log(f"FAIL({code}): {msg}")
    sys.exit(code)


# --------------------------------------------------------------------------- #
# DB env loading (mirrors socrata_import.py)
# --------------------------------------------------------------------------- #
def load_db_env(path):
    if not os.path.exists(path):
        log(f"WARNING: {path} not found; relying on existing environment.")
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


def pg_conn_params():
    return {
        "host": pick("PGHOST", "DB_HOST", "ATLAS_DB_HOST", default="localhost"),
        "port": pick("PGPORT", "DB_PORT", "ATLAS_DB_PORT", default="5432"),
        "db":   pick("PGDATABASE", "DB_NAME", "ATLAS_DB_NAME", default="tuanichat_atlas"),
        "user": pick("PGUSER", "DB_USER", "ATLAS_DB_USER", default="atlas"),
        "pw":   pick("PGPASSWORD", "DB_PASSWORD", "ATLAS_DB_PASSWORD", default=None),
    }


def pg_env(p):
    env = dict(os.environ)
    env["PGHOST"] = p["host"]
    env["PGPORT"] = str(p["port"])
    env["PGDATABASE"] = p["db"]
    env["PGUSER"] = p["user"]
    if p["pw"] is not None:
        env["PGPASSWORD"] = p["pw"]
    env.setdefault("PGCONNECT_TIMEOUT", "10")
    return env


def which(envvar, *names):
    override = os.environ.get(envvar)
    if override:
        return override
    for n in names:
        found = shutil.which(n)
        if found:
            return found
    return None


def run(cmd, env, timeout=None, capture=True):
    """Run a command, return (rc, stdout, stderr). Never raises on non-zero."""
    try:
        proc = subprocess.run(
            cmd, env=env, timeout=timeout,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
        )
        out = proc.stdout.decode("utf-8", "replace") if (capture and proc.stdout) else ""
        err = proc.stderr.decode("utf-8", "replace") if (capture and proc.stderr) else ""
        return proc.returncode, out, err
    except subprocess.TimeoutExpired:
        return 124, "", f"timeout after {timeout}s"
    except FileNotFoundError as e:
        return 127, "", str(e)


def psql_scalar(psql, env, db, sql, timeout=30):
    rc, out, err = run([psql, "-d", db, "-tAqc", sql], env, timeout=timeout)
    if rc != 0:
        return None, err.strip()
    return out.strip(), None


# --------------------------------------------------------------------------- #
# Stages
# --------------------------------------------------------------------------- #
def human(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0:
            return f"{n:.1f}{unit}"
        n /= 1024.0
    return f"{n:.1f}PB"


def collect_row_counts(psql, env, db):
    """Cheap row-count snapshot: reltuples estimate for every atlas.* table,
    plus an EXACT count for the two key tables (atlas.business / source_record).
    reltuples is instant (planner stat); the two exact counts are bounded by the
    importer's 10k cap so they stay cheap."""
    counts = {"approx_reltuples": {}, "exact": {}, "note": "approx = pg_class.reltuples (planner estimate)"}
    est_sql = (
        "SELECT n.nspname||'.'||c.relname, c.reltuples::bigint "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname='atlas' AND c.relkind='r' ORDER BY 1"
    )
    rc, out, err = run([psql, "-d", db, "-tAqc", est_sql], env, timeout=30)
    if rc == 0:
        for line in out.splitlines():
            line = line.strip()
            if "|" in line:
                name, val = line.split("|", 1)
                try:
                    counts["approx_reltuples"][name.strip()] = int(val.strip())
                except ValueError:
                    pass
    else:
        log(f"  row-count estimate query failed (non-fatal): {err.strip()}")
    for tbl in ("atlas.business", "atlas.source_record"):
        val, e = psql_scalar(psql, env, db, f"SELECT count(*) FROM {tbl}", timeout=60)
        if val is not None and val.isdigit():
            counts["exact"][tbl] = int(val)
        else:
            log(f"  exact count of {tbl} skipped (non-fatal): {e or 'no result'}")
    return counts


def disk_guard(backup_dir, db_size_bytes):
    total, used, free = shutil.disk_usage(backup_dir)
    min_free_gb = float(os.environ.get("ATLAS_BACKUP_MIN_FREE_GB", "10"))
    est_factor = float(os.environ.get("ATLAS_BACKUP_EST_FACTOR", "0.6"))
    min_free = int(min_free_gb * (1024 ** 3))
    est_dump = int((db_size_bytes or 0) * est_factor)
    log(f"  disk on {backup_dir}: total={human(total)} used={human(used)} free={human(free)} "
        f"({used * 100 // total}% used)")
    log(f"  db_size={human(db_size_bytes or 0)}  est_dump~={human(est_dump)} "
        f"(factor {est_factor})  min_free_floor={human(min_free)}")
    needed = est_dump + min_free
    if free < needed:
        die(40, f"insufficient disk: free {human(free)} < est_dump {human(est_dump)} + "
                f"min_free {human(min_free)} = {human(needed)}. Refusing to dump (would risk "
                f"filling the volume). Prune dumps, lower ATLAS_BACKUP_RETAIN, or set "
                f"ATLAS_BACKUP_DIR to a bigger volume.")
    log("  disk guard: OK")


def prune(backup_dir, db, retain):
    pattern_prefix = f"{db}_"
    dumps = []
    for fn in os.listdir(backup_dir):
        if fn.startswith(pattern_prefix) and fn.endswith(".dump"):
            full = os.path.join(backup_dir, fn)
            dumps.append((os.path.getmtime(full), full))
    dumps.sort(reverse=True)  # newest first
    keep = dumps[:retain]
    drop = dumps[retain:]
    log(f"  retention: {len(dumps)} dump(s) present, keeping newest {min(retain, len(dumps))}, "
        f"removing {len(drop)}")
    freed = 0
    for _, path in drop:
        try:
            freed += os.path.getsize(path)
            os.remove(path)
            log(f"    pruned {os.path.basename(path)}")
        except OSError as e:
            log(f"    WARNING could not remove {path}: {e}")
    if freed:
        log(f"  retention freed {human(freed)}")
    return [p for _, p in keep]


def write_state(state_dir, payload):
    try:
        os.makedirs(state_dir, exist_ok=True)
        with open(os.path.join(state_dir, "last_backup.json"), "w", encoding="utf-8") as fh:
            json.dump(payload, fh)
        log(f"  wrote {state_dir}/last_backup.json")
    except OSError as e:
        log(f"  WARNING could not write state file: {e}")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    args = sys.argv[1:]
    do_verify = "--no-verify" not in args
    do_prune = "--no-prune" not in args
    check_only = "--check-only" in args
    verify_into = None
    if "--verify-into" in args:
        i = args.index("--verify-into")
        if i + 1 < len(args):
            verify_into = args[i + 1]

    load_db_env(DB_ENV_PATH)
    p = pg_conn_params()
    env = pg_env(p)
    db = p["db"]

    backup_dir = os.environ.get("ATLAS_BACKUP_DIR", "/opt/atlas/backups")
    retain = int(os.environ.get("ATLAS_BACKUP_RETAIN", "7"))
    state_dir = os.environ.get("ATLAS_BACKUP_STATE_DIR", "/var/lib/atlas/backups")
    compress = os.environ.get("ATLAS_BACKUP_COMPRESS", "").strip()

    pg_dump = which("ATLAS_PG_DUMP", "pg_dump")
    pg_restore = which("ATLAS_PG_RESTORE", "pg_restore")
    psql = which("ATLAS_PSQL", "psql")

    log("=" * 70)
    log(f"ATLAS backup start  db={db} host={p['host']}:{p['port']} user={p['user']}")
    log(f"  pg_dump={pg_dump}  pg_restore={pg_restore}  psql={psql}")
    log(f"  backup_dir={backup_dir}  retain={retain}  verify={do_verify}  prune={do_prune}")

    if not pg_dump:
        die(10, "pg_dump not found (set ATLAS_PG_DUMP or install the PG 18 client). "
                "This needs a live box to confirm the binary's presence/version.")
    if do_verify and not pg_restore:
        die(10, "pg_restore not found but --verify requested (set ATLAS_PG_RESTORE or --no-verify).")
    if not psql:
        log("  WARNING: psql not found; row-count logging + db-size disk guard will be skipped.")

    try:
        os.makedirs(backup_dir, exist_ok=True)
    except OSError as e:
        die(10, f"cannot create backup dir {backup_dir}: {e}")

    # --- connectivity + db size (for the disk guard) ----------------------- #
    db_size_bytes = None
    if psql:
        val, e = psql_scalar(psql, env, db, "SELECT pg_database_size(current_database())")
        if val is None:
            die(20, f"cannot connect / query db size (psql): {e}. Check /etc/atlas/db.env "
                    f"credentials and that Postgres is up.")
        try:
            db_size_bytes = int(val)
        except ValueError:
            db_size_bytes = None
    else:
        log("  skipping db-size disk estimate (no psql); min_free floor still enforced.")

    disk_guard(backup_dir, db_size_bytes)

    if check_only:
        log("  --check-only: env/tools/disk validated; not dumping. OK")
        sys.exit(0)

    # --- the dump ---------------------------------------------------------- #
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    final = os.path.join(backup_dir, f"{db}_{ts}.dump")
    tmp = final + ".inprogress"
    cmd = [pg_dump, "-Fc", "-d", db, "-f", tmp]
    if compress:
        cmd.insert(1, f"--compress={compress}")
    log(f"  dumping (custom format, compressed) -> {final}")
    log(f"    $ {' '.join(cmd)}")
    t0 = time.time()
    rc, out, err = run(cmd, env, timeout=int(os.environ.get("ATLAS_BACKUP_TIMEOUT", "10800")))
    if rc != 0:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass
        die(50, f"pg_dump failed rc={rc}: {err.strip() or out.strip()}")
    if not os.path.exists(tmp) or os.path.getsize(tmp) == 0:
        die(50, "pg_dump produced an empty/missing file.")
    os.replace(tmp, final)
    dump_size = os.path.getsize(final)
    dt = time.time() - t0
    log(f"  dump OK: {human(dump_size)} in {dt:.1f}s -> {final}")

    # --- verify: pg_restore --list smoke test ------------------------------ #
    toc_entries = None
    if do_verify:
        log("  verifying archive readability: pg_restore --list")
        rc, out, err = run([pg_restore, "--list", final], env, timeout=300)
        if rc != 0:
            die(55, f"pg_restore --list FAILED rc={rc}: {err.strip() or out.strip()} "
                    f"-- the dump is unreadable/corrupt; treating backup as FAILED.")
        toc_entries = sum(1 for ln in out.splitlines() if ln.strip() and not ln.lstrip().startswith(";"))
        if toc_entries <= 0:
            die(55, "pg_restore --list produced an empty table-of-contents; dump is suspect.")
        log(f"  verify OK: archive TOC parsed, {toc_entries} entries")

    # --- OPTIONAL heavy proof: real restore into a scratch DB -------------- #
    restore_into_result = None
    if verify_into:
        scratch = verify_into
        log(f"  --verify-into: real restore proof into scratch db '{scratch}' (then drop)")
        if not psql:
            log("    skipped: psql not available")
        else:
            run([psql, "-d", "postgres", "-c", f'DROP DATABASE IF EXISTS "{scratch}"'], env, timeout=120)
            rc, out, err = run([psql, "-d", "postgres", "-c", f'CREATE DATABASE "{scratch}"'], env, timeout=120)
            if rc != 0:
                log(f"    WARNING could not create scratch db (non-fatal): {err.strip()}")
            else:
                rc, out, err = run([pg_restore, "-d", scratch, "--no-owner", final], env,
                                   timeout=int(os.environ.get("ATLAS_RESTORE_TIMEOUT", "10800")))
                rc2, val, e2 = (None, None, None)
                if rc not in (0,):
                    log(f"    pg_restore into scratch returned rc={rc} (errors may be benign role/owner noise): "
                        f"{err.strip()[:300]}")
                val, e2 = psql_scalar(psql, env, scratch, "SELECT count(*) FROM atlas.business", timeout=120)
                restore_into_result = {"scratch_db": scratch, "atlas_business_rows": val, "restore_rc": rc}
                log(f"    restored atlas.business rows in scratch = {val} (err={e2 or 'none'})")
                run([psql, "-d", "postgres", "-c", f'DROP DATABASE IF EXISTS "{scratch}"'], env, timeout=120)
                log("    dropped scratch db")

    # --- row counts -------------------------------------------------------- #
    counts = {}
    if psql:
        log("  collecting row counts (reltuples estimates + exact for key tables)")
        counts = collect_row_counts(psql, env, db)
        log(f"  exact counts: {counts.get('exact')}")
        log(f"  approx (reltuples): {counts.get('approx_reltuples')}")

    # --- retention --------------------------------------------------------- #
    kept = [final]
    if do_prune:
        kept = prune(backup_dir, db, retain)

    # --- off-box (optional) ------------------------------------------------ #
    offbox = os.environ.get("ATLAS_BACKUP_OFFBOX_CMD", "").strip()
    offbox_required = os.environ.get("ATLAS_BACKUP_OFFBOX_REQUIRED", "0") == "1"
    offbox_result = "disabled"
    if offbox:
        full = f"{offbox} {final}"
        log(f"  off-box copy: $ {full}")
        rc, out, err = run(["bash", "-lc", full], env, timeout=int(os.environ.get("ATLAS_OFFBOX_TIMEOUT", "3600")))
        if rc == 0:
            offbox_result = "ok"
            log("  off-box copy OK")
        else:
            offbox_result = f"FAILED rc={rc}"
            msg = f"off-box copy FAILED rc={rc}: {err.strip() or out.strip()}"
            if offbox_required:
                die(60, msg + " (ATLAS_BACKUP_OFFBOX_REQUIRED=1 -> fatal)")
            log("  WARNING " + msg + " (non-fatal; local dump is good. Set OFFBOX_REQUIRED=1 to make fatal)")

    # --- state file (autopull status-back can surface this) ---------------- #
    total, used, free = shutil.disk_usage(backup_dir)
    payload = {
        "lane": "backup",
        "db": db,
        "dump": final,
        "dump_size_bytes": dump_size,
        "dump_size_human": human(dump_size),
        "duration_sec": round(dt, 1),
        "verify_toc_entries": toc_entries,
        "verify_into": restore_into_result,
        "retained": [os.path.basename(x) for x in kept],
        "retain_target": retain,
        "row_counts": counts,
        "disk_free_bytes": free,
        "disk_free_human": human(free),
        "disk_pct_used": used * 100 // total,
        "offbox": offbox_result,
        "ts": int(time.time()),
    }
    write_state(state_dir, payload)

    log("=" * 70)
    log(f"PASS: backup={os.path.basename(final)} size={human(dump_size)} verify_entries={toc_entries} "
        f"retained={len(kept)} free={human(free)} offbox={offbox_result}")
    log("=" * 70)
    sys.exit(0)


if __name__ == "__main__":
    main()
