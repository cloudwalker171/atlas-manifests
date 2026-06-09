#!/usr/bin/env bash
# ============================================================================
# A.T.L.A.S. -- InterServer enrich-worker RAMP (run ON the InterServer node as
# root). CentOS7 / OpenVZ friendly. Scales InterServer enrichment by running N
# parallel SKIP-LOCKED workers (atlas_enrich_worker.py --loop) ON TOP of the
# baseline atlas-enrich.service (~1 worker). Each worker uses ~1 Postgres
# connection on the SHARED Hetzner DB, so the binding constraint is the COMBINED
# connection count (max_connections=100). SELF-GUARDING + PG-SAFE:
#   * Refuses to scale up if InterServer free RAM < MIN_FREE_MB (default 400).
#   * Refuses to push SHARED PG past PG_CONN_CEILING (default 85 / 100).
#   * Never touches Hetzner; never needs the deploy secret or status token.
#
#   status      show added workers + free RAM + shared PG conns
#   set N       run exactly N ADDED ramp workers (on top of the systemd baseline)
#   auto        staged ceiling ramp: added=1,2,4,6,8 with 4-min holds + guards
#   stop        stop all ADDED ramp workers (baseline service untouched)
# ============================================================================
set -u
ATLAS_HOME="${ATLAS_HOME:-/opt/atlas}"; VENV="${VENV:-$ATLAS_HOME/venv}"
WORKER="${WORKER:-$ATLAS_HOME/scripts/atlas_enrich_worker.py}"
RUN_DIR="${RUN_DIR:-/run/atlas-ramp}"; ENV_FILE="${ENV_FILE:-/etc/atlas/db.env}"
MIN_FREE_MB="${MIN_FREE_MB:-400}"; PG_CONN_CEILING="${PG_CONN_CEILING:-85}"
RAMP_STEPS="${RAMP_STEPS:-1 2 4 6 8}"; RAMP_HOLD_S="${RAMP_HOLD_S:-240}"
mkdir -p "$RUN_DIR"
free_mb(){ free -m 2>/dev/null | awk '/^Mem:/{print ($7?$7:$4)}'; }
shared_pg_conns(){ set -a; [ -f "$ENV_FILE" ] && . "$ENV_FILE"; set +a
  "$VENV/bin/python" - <<'PY' 2>/dev/null
import os
try:
    import psycopg2
    c=psycopg2.connect(host=os.environ.get("PGHOST"),dbname=os.environ.get("PGDATABASE"),
        user=os.environ.get("PGUSER"),password=os.environ.get("PGPASSWORD"),
        port=os.environ.get("PGPORT","5432"),connect_timeout=8)
    cur=c.cursor(); cur.execute("select (select count(*) from pg_stat_activity),(select setting from pg_settings where name='max_connections')")
    n,mx=cur.fetchone(); print("%s/%s"%(n,mx)); c.close()
except Exception as e: print("unknown")
PY
}
running_workers(){ ls "$RUN_DIR"/worker-*.pid 2>/dev/null | while read -r p; do pid=$(cat "$p" 2>/dev/null); if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then echo "$pid"; else rm -f "$p"; fi; done; }
count_workers(){ running_workers | wc -l | tr -d ' '; }
start_one(){ set -a; [ -f "$ENV_FILE" ] && . "$ENV_FILE"; set +a
  nohup "$VENV/bin/python" "$WORKER" --loop >>"/var/log/atlas-ramp-$1.log" 2>&1 & echo $! > "$RUN_DIR/worker-$1.pid"; }
stop_all(){ running_workers | while read -r pid; do kill "$pid" 2>/dev/null; done; rm -f "$RUN_DIR"/worker-*.pid; echo "stopped all added ramp workers"; }
scale_to(){ local TARGET="$1" CUR; CUR=$(count_workers)
  if [ "$TARGET" -gt "$CUR" ]; then
    local FREE CONNS; FREE=$(free_mb); CONNS=$(shared_pg_conns | cut -d/ -f1)
    if [ -n "$FREE" ] && [ "$FREE" -lt "$MIN_FREE_MB" ]; then echo "GUARD-ABORT free RAM ${FREE}MB < ${MIN_FREE_MB}"; return 2; fi
    if [ -n "$CONNS" ] && [ "$CONNS" -ge "$PG_CONN_CEILING" ]; then echo "GUARD-ABORT shared PG ${CONNS} >= ${PG_CONN_CEILING}"; return 3; fi
    local i="$CUR"; while [ "$i" -lt "$TARGET" ]; do i=$((i+1)); start_one "$i"; sleep 2; done
  elif [ "$TARGET" -lt "$CUR" ]; then
    local i="$CUR"; while [ "$i" -gt "$TARGET" ]; do local f="$RUN_DIR/worker-$i.pid"; [ -f "$f" ] && { kill "$(cat "$f")" 2>/dev/null; rm -f "$f"; }; i=$((i-1)); done
  fi; return 0; }
case "${1:-status}" in
  status) echo "added ramp workers: $(count_workers)"; echo "free RAM (MB)     : $(free_mb)"; echo "shared PG conns   : $(shared_pg_conns) (ceiling ${PG_CONN_CEILING}/100)";;
  set) scale_to "${2:?usage: set N}"; echo "now added=$(count_workers) freeRAM=$(free_mb)MB sharedPG=$(shared_pg_conns)";;
  auto)
    echo "AUTO-RAMP start $(date -u +%H:%M:%SZ) steps=[$RAMP_STEPS] hold=${RAMP_HOLD_S}s ceiling=${PG_CONN_CEILING}/100 minRAM=${MIN_FREE_MB}MB"
    for N in $RAMP_STEPS; do
      if ! scale_to "$N"; then echo "AUTO-RAMP STOP at added=$(count_workers) (guard) $(date -u +%H:%M:%SZ)"; break; fi
      echo "AUTO-RAMP STEP added=$N total~=$((N+1)) freeRAM=$(free_mb)MB sharedPG=$(shared_pg_conns) @ $(date -u +%H:%M:%SZ)"
      sleep "$RAMP_HOLD_S"
    done
    echo "AUTO-RAMP end added=$(count_workers) freeRAM=$(free_mb)MB sharedPG=$(shared_pg_conns) $(date -u +%H:%M:%SZ)";;
  stop) stop_all;;
  *) echo "usage: $0 {status|set N|auto|stop}"; exit 1;;
esac
