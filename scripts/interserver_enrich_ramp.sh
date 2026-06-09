#!/usr/bin/env bash
# ============================================================================
# A.T.L.A.S. -- InterServer enrich-worker RAMP (run ON the InterServer node,
# as root, in a DirectAdmin terminal / ssh shell). CentOS7 / OpenVZ friendly.
#
# Scales the InterServer enrichment by running N parallel copies of the
# pooled SKIP-LOCKED worker (atlas_enrich_worker.py --loop). Each worker uses
# ~1 Postgres connection on the SHARED Hetzner DB, so the binding constraint
# is the COMBINED connection count (max_connections=100; Hetzner already uses
# ~60-68). This script is CONSERVATIVE and SELF-GUARDING:
#   * Refuses to scale up if InterServer free RAM < MIN_FREE_MB.
#   * Refuses to push the SHARED PG past PG_CONN_CEILING connections.
#   * Ramps ONE step at a time; you watch, then run the next step.
#   * Never touches Hetzner; never needs the deploy secret or status token.
#
# USAGE (step the target up slowly, judging each step before the next):
#   ./interserver_enrich_ramp.sh status        # show current workers + RAM + shared PG conns
#   ./interserver_enrich_ramp.sh set 2         # run 2 workers
#   ./interserver_enrich_ramp.sh set 4         # ...then 4, watch, then 6, 8 ...
#   ./interserver_enrich_ramp.sh stop          # back to 0 ATLAS ramp workers
#
# Recommended path: 1 -> 2 -> 4 -> 6 -> 8, pausing ~3-5 min each step to let
# the Hetzner guardian + queue-throughput probe report the combined rate and
# the shared connection count BEFORE you go higher. STOP when either:
#   - combined done-rate stops rising (InterServer CPU/RAM saturated), or
#   - shared PG connections approach PG_CONN_CEILING, or
#   - Hetzner load or InterServer RAM gets tight.
# ============================================================================
set -u

ATLAS_HOME="${ATLAS_HOME:-/opt/atlas}"
VENV="${VENV:-$ATLAS_HOME/venv}"
WORKER="${WORKER:-$ATLAS_HOME/scripts/atlas_enrich_worker.py}"
RUN_DIR="${RUN_DIR:-/run/atlas-ramp}"           # pid files live here
ENV_FILE="${ENV_FILE:-/etc/atlas/db.env}"

MIN_FREE_MB="${MIN_FREE_MB:-400}"               # don't scale up below this free RAM on InterServer
PG_CONN_CEILING="${PG_CONN_CEILING:-85}"        # keep SHARED connections under this (of 100)
PEER_SELF="${PEER_SELF:-}"                       # optional: this box's source IP, informational

mkdir -p "$RUN_DIR"

free_mb(){ free -m 2>/dev/null | awk '/^Mem:/{print $7?$7:$4}'; }   # available (or free)

shared_pg_conns(){
  # ask the shared DB how many total connections are open right now
  set -a; [ -f "$ENV_FILE" ] && . "$ENV_FILE"; set +a
  PGPASSWORD="${PGPASSWORD:-}" "$VENV/bin/python" - <<'PY' 2>/dev/null
import os
try:
    import psycopg2
    c=psycopg2.connect(host=os.environ.get("PGHOST"),dbname=os.environ.get("PGDATABASE"),
        user=os.environ.get("PGUSER"),password=os.environ.get("PGPASSWORD"),
        port=os.environ.get("PGPORT","5432"),connect_timeout=8)
    cur=c.cursor(); cur.execute("select count(*), setting from pg_stat_activity, pg_settings where name='max_connections' group by setting")
    n,mx=cur.fetchone(); print("%s/%s"%(n,mx)); c.close()
except Exception as e:
    print("unknown (%s)"%type(e).__name__)
PY
}

running_workers(){ ls "$RUN_DIR"/worker-*.pid 2>/dev/null | while read -r p; do pid=$(cat "$p" 2>/dev/null); if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then echo "$pid"; else rm -f "$p"; fi; done; }
count_workers(){ running_workers | wc -l | tr -d ' '; }

start_one(){
  local idx="$1"
  set -a; [ -f "$ENV_FILE" ] && . "$ENV_FILE"; set +a
  nohup "$VENV/bin/python" "$WORKER" --loop >>"/var/log/atlas-ramp-$idx.log" 2>&1 &
  echo $! > "$RUN_DIR/worker-$idx.pid"
}

stop_all(){ running_workers | while read -r pid; do kill "$pid" 2>/dev/null; done; rm -f "$RUN_DIR"/worker-*.pid; echo "stopped all ramp workers"; }

case "${1:-status}" in
  status)
    echo "InterServer ramp workers : $(count_workers)"
    echo "InterServer free RAM (MB): $(free_mb)"
    echo "SHARED PG connections    : $(shared_pg_conns)   (ceiling $PG_CONN_CEILING/100)"
    ;;
  set)
    TARGET="${2:?usage: set N}"
    CUR=$(count_workers)
    echo "current=$CUR target=$TARGET"
    if [ "$TARGET" -gt "$CUR" ]; then
      FREE=$(free_mb); CONNS=$(shared_pg_conns | cut -d/ -f1)
      echo "guard: freeRAM=${FREE}MB sharedPGconns=${CONNS}"
      if [ -n "$FREE" ] && [ "$FREE" -lt "$MIN_FREE_MB" ]; then echo "ABORT: free RAM ${FREE}MB < ${MIN_FREE_MB}MB"; exit 2; fi
      if [ -n "$CONNS" ] && [ "$CONNS" -ge "$PG_CONN_CEILING" ]; then echo "ABORT: shared PG conns ${CONNS} >= ceiling ${PG_CONN_CEILING}"; exit 3; fi
      i="$CUR"
      while [ "$i" -lt "$TARGET" ]; do i=$((i+1)); start_one "$i"; echo "started worker-$i"; sleep 2; done
    elif [ "$TARGET" -lt "$CUR" ]; then
      i="$CUR"
      while [ "$i" -gt "$TARGET" ]; do pidf="$RUN_DIR/worker-$i.pid"; [ -f "$pidf" ] && { kill "$(cat "$pidf")" 2>/dev/null; rm -f "$pidf"; echo "stopped worker-$i"; }; i=$((i-1)); done
    fi
    sleep 1; echo "now running: $(count_workers) workers; freeRAM $(free_mb)MB; sharedPG $(shared_pg_conns)"
    ;;
  stop) stop_all ;;
  *) echo "usage: $0 {status|set N|stop}"; exit 1 ;;
esac
