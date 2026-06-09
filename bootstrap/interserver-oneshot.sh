#!/usr/bin/env bash
# ============================================================================
# InterServer second-node install -- SINGLE-PASTE, scp-free, EOL-CentOS7/OpenVZ
# hardened. Paste this whole block into a ROOT shell (DirectAdmin terminal,
# VNC/serial console, or pipe it over ssh).  It is IDEMPOTENT and FAIL-LOUD:
# it ends with exactly one of:
#     INSTALL_RESULT=OK (rows:N driver:<psycopg2|pg8000>)
#     INSTALL_RESULT=FAILED: <reason>
#
# This node is an ENRICHMENT WORKER: it runs scripts/atlas_enrich_worker.py
# against the Hetzner Postgres (168.119.226.254 / tuanichat_atlas / role atlas)
# so it actually CONTRIBUTES rows -- it does not merely install.
#
# >>> The atlas DB password is read from the environment (ATLAS_DB_PW=... or P).
#     The 2-command PowerShell runner passes it over ssh; nothing to edit here. <<<
# ============================================================================
set -Eeuo pipefail

# --- fail-loud trap ----------------------------------------------------------
FAIL_REASON=""
fail(){ FAIL_REASON="${1:-unknown}"; echo "INSTALL_RESULT=FAILED: ${FAIL_REASON}"; exit 1; }
trap 'rc=$?; [ $rc -ne 0 ] && [ -z "$FAIL_REASON" ] && echo "INSTALL_RESULT=FAILED: unexpected error (rc=$rc) at line $LINENO"; exit $rc' ERR

ATLAS_DB_PW="${ATLAS_DB_PW:-${P:-}}"

# --- pins / endpoints --------------------------------------------------------
REPO_RAW_BASE='https://raw.githubusercontent.com/cloudwalker171/atlas-manifests/main/bootstrap/atlas-autopull.v3.sh'
AUTOPULL_SHA256='1171a33bdb5c5b533ee2321b18a90b2276e48a263ea273107e44849e295dfd22'
WORKER_RAW='https://raw.githubusercontent.com/cloudwalker171/atlas-manifests/main/scripts/atlas_enrich_worker.py'
RAW_BASE='https://raw.githubusercontent.com/cloudwalker171/atlas-manifests/main'
BRAIN_URL="${BRAIN_URL:-$RAW_BASE}"

NODE_ID="${NODE_ID:-interserver}"
ATLAS_HOME=/opt/atlas ; VENV="$ATLAS_HOME/venv" ; ETC=/etc/atlas
PGHOST=168.119.226.254 ; PGPORT=5432 ; PGDATABASE=tuanichat_atlas ; PGUSER=atlas
ENRICH_CMD="${ENRICH_CMD:-$VENV/bin/python $ATLAS_HOME/scripts/atlas_enrich_worker.py --loop}"

# --- preflight ---------------------------------------------------------------
[ "$(id -u)" -eq 0 ] || fail "must run as root"
[ -n "$ATLAS_DB_PW" ] || fail "no DB password (set ATLAS_DB_PW=... or P=... before running)"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found -- attempting yum (mirrors may be EOL)..."
  yum -y install python3 >/dev/null 2>&1 || true
  command -v python3 >/dev/null 2>&1 || \
    fail "python3 missing and yum could not install it (EOL CentOS7 mirrors). Install python3.6 manually then re-run."
fi
PYSYS="$(command -v python3)"
PYVER="$("$PYSYS" -c 'import sys;print("%d.%d"%sys.version_info[:2])' 2>/dev/null || echo unknown)"
echo "python3 = $PYSYS (v$PYVER)"

if ! command -v curl >/dev/null 2>&1; then
  yum -y install curl >/dev/null 2>&1 || true
  command -v curl >/dev/null 2>&1 || fail "curl missing and yum could not install it"
fi
CA=""
for c in /etc/pki/tls/certs/ca-bundle.crt /etc/ssl/certs/ca-bundle.crt \
         /etc/ssl/certs/ca-certificates.crt /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem; do
  [ -s "$c" ] && { CA="$c"; break; }
done
if [ -z "$CA" ]; then
  echo "no CA bundle found -- trying yum ca-certificates..."
  yum -y install ca-certificates >/dev/null 2>&1 || true
  for c in /etc/pki/tls/certs/ca-bundle.crt /etc/ssl/certs/ca-bundle.crt; do
    [ -s "$c" ] && { CA="$c"; break; }
  done
fi
CURL=(curl -fsSL --connect-timeout 20 --max-time 120 --retry 3 --retry-delay 3)
[ -n "$CA" ] && CURL+=(--cacert "$CA")
echo "CA bundle = ${CA:-<none found -- relying on curl default trust store>}"

# --- dirs + secrets ----------------------------------------------------------
install -d -m 700 "$ETC"
install -d -m 755 "$ATLAS_HOME" "$ATLAS_HOME/scripts"
install -d -m 755 /var/lib/atlas/autopull /var/lib/atlas/enrich /var/log/atlas 2>/dev/null || true
umask 077

cat > "$ETC/db.env" <<EOF
PGHOST=${PGHOST}
PGPORT=${PGPORT}
PGDATABASE=${PGDATABASE}
PGUSER=${PGUSER}
PGPASSWORD=${ATLAS_DB_PW}
EOF
chmod 600 "$ETC/db.env"

cat > "$ETC/autopull.env" <<EOF
BRAIN_URL=${BRAIN_URL}
NODE_ID=${NODE_ID}
PGHOST=${PGHOST}
PGPORT=${PGPORT}
PGDATABASE=${PGDATABASE}
PGUSER=${PGUSER}
PGPASSWORD=${ATLAS_DB_PW}
EOF
chmod 600 "$ETC/autopull.env"

cat > "$ETC/atlas.secret.env" <<EOF
PGPASSWORD=${ATLAS_DB_PW}
REPO_RAW_BASE=${REPO_RAW_BASE}
AUTOPULL_SHA256=${AUTOPULL_SHA256}
EOF
chmod 600 "$ETC/atlas.secret.env"

# --- venv (idempotent; OpenVZ-safe pip bootstrap) ----------------------------
if [ ! -x "$VENV/bin/python" ]; then
  echo "creating venv at $VENV ..."
  if ! "$PYSYS" -m venv "$VENV" 2>/dev/null; then
    echo "venv stdlib path failed -- bootstrapping pip manually (py3.6 get-pip)..."
    "$PYSYS" -m venv --without-pip "$VENV" || fail "python3 -m venv failed (venv/ensurepip module missing -- install python3-venv/python36-libs)"
    "${CURL[@]}" "https://bootstrap.pypa.io/pip/3.6/get-pip.py" -o "$ATLAS_HOME/get-pip.py" \
      || fail "could not download get-pip.py (TLS/CA or network to bootstrap.pypa.io)"
    "$VENV/bin/python" "$ATLAS_HOME/get-pip.py" || fail "get-pip.py failed to install pip into the venv"
  fi
fi
VPY="$VENV/bin/python"
[ -x "$VPY" ] || fail "venv python missing after creation ($VPY)"

"$VPY" -m pip install --disable-pip-version-check -q --upgrade 'pip>=21.0' setuptools wheel \
  || fail "could not upgrade pip/setuptools/wheel in the venv"

# --- DB driver: psycopg2 prebuilt wheel, with ZERO-COMPILE pg8000 fallback ----
DB_DRIVER=""
echo "installing DB driver (try psycopg2-binary==2.8.6 prebuilt wheel)..."
if "$VPY" -m pip install --disable-pip-version-check -q --only-binary :all: "psycopg2-binary==2.8.6" 2>/dev/null \
   && "$VPY" -c "import psycopg2" 2>/dev/null; then
  DB_DRIVER="psycopg2"
  echo "  psycopg2-binary 2.8.6 wheel installed (no compilation)."
else
  echo "  psycopg2 wheel unavailable -- falling back to pg8000 (pure-python, zero compile)."
  if "$VPY" -m pip install --disable-pip-version-check -q --only-binary :all: "pg8000" 2>/dev/null \
     && "$VPY" -c "import pg8000.dbapi" 2>/dev/null; then
    SITE="$("$VPY" -c 'import site;print(site.getsitepackages()[0])')"
    cat > "$SITE/psycopg2.py" <<'SHIM'
# psycopg2 -> pg8000 compatibility shim (pure-python, no compiler).
import pg8000.dbapi as _pg
Error = _pg.Error
DatabaseError = getattr(_pg, "DatabaseError", _pg.Error)
OperationalError = getattr(_pg, "OperationalError", _pg.Error)
IntegrityError = getattr(_pg, "IntegrityError", _pg.Error)
ProgrammingError = getattr(_pg, "ProgrammingError", _pg.Error)
InterfaceError = getattr(_pg, "InterfaceError", _pg.Error)
def connect(dsn=None, **kw):
    if dsn and not kw:
        import urllib.parse as up
        u = up.urlparse(dsn)
        if u.scheme:
            kw = dict(host=u.hostname, port=u.port, user=u.username,
                      password=u.password, dbname=(u.path or "/").lstrip("/"))
    m = {}
    if kw.get("host") is not None: m["host"] = kw["host"]
    if kw.get("port") is not None: m["port"] = int(kw["port"])
    db = kw.get("dbname") or kw.get("database")
    if db is not None: m["database"] = db
    if kw.get("user") is not None: m["user"] = kw["user"]
    if kw.get("password") is not None: m["password"] = kw["password"]
    if kw.get("connect_timeout") is not None:
        try: m["timeout"] = int(kw["connect_timeout"])
        except Exception: pass
    if kw.get("application_name") is not None:
        m["application_name"] = kw["application_name"]
    conn = _pg.connect(**m)
    try: conn.autocommit = False
    except Exception: pass
    return conn
SHIM
    "$VPY" -c "import psycopg2; print('shim import ok')" >/dev/null 2>&1 \
      || fail "pg8000 psycopg2-shim failed to import"
    DB_DRIVER="pg8000"
    echo "  pg8000 + psycopg2-shim installed (pure-python)."
  else
    fail "could not install ANY Postgres driver (neither psycopg2 wheel nor pg8000)"
  fi
fi

"$VPY" -m pip install --disable-pip-version-check -q "dnspython" 2>/dev/null \
  && echo "  dnspython installed (MX/DNS enrichment enabled)." \
  || echo "  dnspython not installed (MX/DNS enrichment skipped -- not fatal)."

# --- fetch + sha-verify the v3 puller (stable, pinned) -----------------------
BOOT="$ATLAS_HOME/atlas-autopull.v3.sh"
"${CURL[@]}" "$REPO_RAW_BASE" -o "$BOOT.incoming" || fail "could not fetch the v3 puller (TLS/CA or raw-CDN not yet propagated -- wait ~5 min and re-run)"
GOT="$(sha256sum "$BOOT.incoming" | awk '{print $1}')"
if [ "$GOT" != "$AUTOPULL_SHA256" ]; then
  rm -f "$BOOT.incoming"
  fail "v3 puller SHA mismatch (got=$GOT want=$AUTOPULL_SHA256) -- raw CDN may be mid-propagation; wait ~5 min and re-run"
fi
mv -f "$BOOT.incoming" "$BOOT"; chmod 0755 "$BOOT"
echo "v3 puller fetched + sha-verified."

# --- fetch the ENRICH worker (sanity-checked, not sha-pinned) ----------------
WORKER="$ATLAS_HOME/scripts/atlas_enrich_worker.py"
"${CURL[@]}" "$WORKER_RAW" -o "$WORKER.incoming" || fail "could not fetch atlas_enrich_worker.py (raw CDN -- wait ~5 min and re-run)"
[ -s "$WORKER.incoming" ] || fail "fetched worker is empty (raw CDN mid-propagation -- wait ~5 min)"
head -1 "$WORKER.incoming" | grep -q "python" || fail "fetched worker does not look like a python file (bad/partial download)"
mv -f "$WORKER.incoming" "$WORKER"; chmod 0755 "$WORKER"
"$VPY" -c "import ast; ast.parse(open('$WORKER').read())" 2>/dev/null \
  || fail "fetched worker fails to parse under python3 (partial download)"
echo "enrich worker fetched + parse-checked."

# --- PG SELFTEST (proves firewall + creds + driver) --------------------------
echo "running PG connectivity selftest to ${PGHOST}:${PGPORT}/${PGDATABASE} ..."
set +e
SELFTEST_OUT="$(ATLAS_DB_ENV="$ETC/db.env" "$VPY" - <<'PY' 2>&1
import os, sys
p = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
if os.path.exists(p):
    for raw in open(p, encoding="utf-8"):
        s = raw.strip()
        if not s or s.startswith("#"): continue
        if s.lower().startswith("export "): s = s[7:].strip()
        if "=" not in s: continue
        k, v = s.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
import psycopg2
def pick(*names, default=None):
    for n in names:
        if os.environ.get(n): return os.environ[n]
    return default
try:
    c = psycopg2.connect(
        host=pick("PGHOST","DB_HOST",default="localhost"),
        port=pick("PGPORT","DB_PORT",default="5432"),
        dbname=pick("PGDATABASE","DB_NAME",default="tuanichat_atlas"),
        user=pick("PGUSER","DB_USER",default="atlas"),
        password=pick("PGPASSWORD","DB_PASSWORD",default=None),
        connect_timeout=10, application_name="atlas-interserver-selftest")
    cur = c.cursor()
    cur.execute("select current_database(), current_user")
    db, usr = cur.fetchone()
    try:
        cur.execute("select count(*) from atlas.business")
        rows = cur.fetchone()[0]
    except Exception:
        rows = "n/a"
    print("PG_OK %s %s rows=%s" % (db, usr, rows))
    c.close(); sys.exit(0)
except Exception as e:
    print("PG_FAIL %r" % (e,)); sys.exit(7)
PY
)"
SELFTEST_RC=$?
set -e
echo "$SELFTEST_OUT"
[ $SELFTEST_RC -eq 0 ] || fail "PG selftest could not connect ($SELFTEST_OUT) -- check the Hetzner firewall allows ${PGHOST}:${PGPORT} from this IP and that the password is correct"
ROWS="$(printf '%s' "$SELFTEST_OUT" | sed -n 's/.*rows=\([0-9na/]*\).*/\1/p' | head -1)"; [ -n "$ROWS" ] || ROWS="?"

# --- queue migrate/seed (idempotent) -----------------------------------------
echo "verifying + seeding the enrich queue (idempotent)..."
ATLAS_DB_ENV="$ETC/db.env" NODE_ID="$NODE_ID" "$VPY" "$WORKER" --migrate >/var/log/atlas/enrich-migrate.log 2>&1 \
  && echo "  queue migrate/seed OK (see /var/log/atlas/enrich-migrate.log)" \
  || echo "  WARN: --migrate returned non-zero (queue may already be seeded; continuing). See /var/log/atlas/enrich-migrate.log"

# --- run the worker: systemd if it actually works, else nohup+cron -----------
START_MODE=""
SYSTEMD_OK=0
if command -v systemctl >/dev/null 2>&1 && systemctl list-units >/dev/null 2>&1; then SYSTEMD_OK=1; fi

write_enrich_runner(){
  cat > "$ATLAS_HOME/run-enrich.sh" <<EOF
#!/usr/bin/env bash
set -a
[ -f "$ETC/db.env" ] && . "$ETC/db.env"
[ -f "$ETC/autopull.env" ] && . "$ETC/autopull.env"
export ATLAS_DB_ENV="$ETC/db.env" ATLAS_AUTOPULL_CONF="$ETC/autopull.env"
export NODE_ID="$NODE_ID" ATLAS_HOME="$ATLAS_HOME"
set +a
exec $ENRICH_CMD
EOF
  chmod 0755 "$ATLAS_HOME/run-enrich.sh"
}
write_enrich_runner

if [ "$SYSTEMD_OK" -eq 1 ]; then
  echo "systemd is functional -- installing atlas-enrich.service + atlas-autopull timer..."
  cat > /etc/systemd/system/atlas-enrich.service <<EOF
[Unit]
Description=ATLAS enrichment worker (interserver node)
After=network-online.target
Wants=network-online.target
[Service]
Type=simple
ExecStart=$ATLAS_HOME/run-enrich.sh
Restart=always
RestartSec=10
[Install]
WantedBy=multi-user.target
EOF
  cat > /etc/systemd/system/atlas-autopull.service <<EOF
[Unit]
Description=ATLAS auto-pull (one cycle)
After=network-online.target
Wants=network-online.target
[Service]
Type=oneshot
Environment=ATLAS_AUTOPULL_CONF=$ETC/autopull.env
Environment=NODE_ID=$NODE_ID
Environment=ATLAS_HOME=$ATLAS_HOME
ExecStart=$BOOT --once
EOF
  cat > /etc/systemd/system/atlas-autopull.timer <<EOF
[Unit]
Description=ATLAS auto-pull every 2 min
[Timer]
OnBootSec=60
OnUnitActiveSec=2min
Unit=atlas-autopull.service
[Install]
WantedBy=timers.target
EOF
  if systemctl daemon-reload 2>/dev/null \
     && systemctl enable --now atlas-enrich.service 2>/dev/null \
     && systemctl enable --now atlas-autopull.timer 2>/dev/null; then
    sleep 3
    if systemctl is-active --quiet atlas-enrich.service; then
      START_MODE="systemd"; echo "  atlas-enrich.service is active under systemd."
    else
      echo "  systemd accepted units but worker not active -- falling back to nohup."; SYSTEMD_OK=0
    fi
  else
    echo "  systemd enable/start failed (common on OpenVZ) -- falling back to nohup+cron."; SYSTEMD_OK=0
  fi
fi

if [ "$SYSTEMD_OK" -ne 1 ]; then
  echo "using nohup + cron watchdog (OpenVZ-safe, no systemd)..."
  cat > "$ATLAS_HOME/enrich-watchdog.sh" <<EOF
#!/usr/bin/env bash
PIDF=/var/lib/atlas/enrich/worker.pid
if [ -f "\$PIDF" ] && kill -0 "\$(cat "\$PIDF" 2>/dev/null)" 2>/dev/null; then exit 0; fi
nohup "$ATLAS_HOME/run-enrich.sh" >>/var/log/atlas/enrich.log 2>&1 &
echo \$! > "\$PIDF"
EOF
  chmod 0755 "$ATLAS_HOME/enrich-watchdog.sh"
  "$ATLAS_HOME/enrich-watchdog.sh"
  sleep 2
  if [ -f /var/lib/atlas/enrich/worker.pid ] && kill -0 "$(cat /var/lib/atlas/enrich/worker.pid)" 2>/dev/null; then
    START_MODE="nohup"; echo "  enrich worker started under nohup (pid $(cat /var/lib/atlas/enrich/worker.pid))."
  else
    fail "could not start the enrich worker under nohup -- see /var/log/atlas/enrich.log"
  fi
  ( crontab -l 2>/dev/null | grep -v 'atlas-autopull' | grep -v 'enrich-watchdog' ; \
    echo "* * * * * $ATLAS_HOME/enrich-watchdog.sh >>/var/log/atlas/enrich-watchdog.log 2>&1" ; \
    echo "*/2 * * * * ATLAS_AUTOPULL_CONF='$ETC/autopull.env' NODE_ID=$NODE_ID ATLAS_HOME=$ATLAS_HOME $BOOT --once >>$ATLAS_HOME/pull.log 2>&1" ) | crontab - \
    || echo "  WARN: could not install crontab -- worker running but won't auto-restart on reboot"
  echo "  cron watchdog (1 min) + puller (2 min) installed."
fi

ATLAS_AUTOPULL_CONF="$ETC/autopull.env" NODE_ID="$NODE_ID" ATLAS_HOME="$ATLAS_HOME" \
  "$BOOT" --once >/var/log/atlas/autopull-first.log 2>&1 \
  && echo "first puller pass OK" \
  || echo "first puller pass returned non-zero (nothing-new or no DEPLOY_SECRET on a worker node -- not fatal). See /var/log/atlas/autopull-first.log"

echo "start mode = ${START_MODE:-unknown}"
echo "INSTALL_RESULT=OK (rows:${ROWS} driver:${DB_DRIVER})"
