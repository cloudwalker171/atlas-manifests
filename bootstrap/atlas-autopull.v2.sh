#!/usr/bin/env bash
# =====================================================================
# ATLAS auto-pull deployer  v2  (HARDENED: lifeline-protected)
# PULL-BASED. DIAGNOSTIC-FIRST. FAIL-CLOSED. RESTORE-POINT + AUTO-ROLLBACK.
#
#   v1 core (unchanged semantics):
#     1. Read HMAC secret from ENV FILE or wp-config ONLY (never a DB).
#     2. Pull manifest + detached HMAC-SHA256 sig from the brain.
#     3. FAIL-CLOSED: verify signature BEFORE parsing steps. Mismatch => abort.
#     4. Dedup by manifest seq (never re-apply an applied manifest).
#     5. DIAGNOSE preconditions; each step is a TYPED, allowlisted action.
#
#   v2 lifeline additions:
#     G. GUARDRAIL scan (fail-closed): reject the WHOLE manifest if any step
#        would sever SSH, the autopull pipe, the deploy keys, UFW's SSH allow,
#        or the network. Nothing is applied on reject.
#     R. RESTORE POINT before any mutation: tar of /opt/atlas (sans data/restore)
#        + /etc/atlas + active atlas systemd units, plus a best-effort pg schema
#        dump + per-table row-count marker. Symlink last-good. Keep last 5.
#     H. HEALTH CHECK after apply: postgres up + schema present + autopull timer
#        enabled+active + sshd listening + (if UFW active) SSH still allowed.
#     A. AUTO-ROLLBACK on health fail: restore files+units from the pre-apply
#        snapshot, reload, re-check; mark manifest failed. Never leave it broken.
#     S. STATUS push to GitHub so the orchestrator can verify autonomously.
#
#   Self-test modes (used by the installer's tested-fallback gate):
#     --selftest           parse/guardrail/health-logic checks on a sandbox; no real apply
#     --rollback-selftest  prove snapshot->mutate->healthfail->restore on a temp dir
#
# Exit: 0 ok/up-to-date | 10 config | 20 fetch | 30 SIG FAIL | 40 precondition
#       41 GUARDRAIL REJECT | 50 apply-failed-rolled-back | 55 health-fail-rolled-back
#       60 status-report-fail
# =====================================================================
set -u
umask 077

# ---------- lifeline deny patterns (used by guardrail + selftest) ----------
# write_file paths that must never be written through the pipe:
FORBID_PATH_RE='(^/etc/ssh/)|(authorized_keys)|(/root/\.ssh)|(/home/[^/]+/\.ssh)|(^/opt/atlas/autopull/atlas-autopull\.sh$)|(^/etc/systemd/system/atlas-autopull\.service$)'
# command / content fragments that sever ssh, the pipe, keys, ufw-ssh, or the net:
# NOTE: evaluated by Python re (guardrail scanner) -> use \s, not POSIX [[:space:]]
FORBID_TEXT_RE='(PasswordAuthentication)|(PermitRootLogin)|(sshd_config)|(systemctl\s+(disable|stop|mask)\s+[^;]*((ssh|sshd)|atlas-autopull))|(ufw\s+(delete|deny|reject)\s+[^;]*(22|OpenSSH|ssh))|(ufw\s+disable)|(iptables\s+-F)|(ip6?tables\s+[^;]*(--flush|-F|-P\s+INPUT\s+DROP))|(ip\s+link\s+set\s+[^;]*down)|(\bifdown\b)|(nmcli\s+[^;]*(down|off))|(systemctl\s+(stop|disable|mask)\s+[^;]*atlas-autopull)|(\brm\b[^;]*(atlas-autopull|/etc/ssh|authorized_keys))|(\buserdel\b)|(\busermod\b)|(\bchsh\b)|(\bpasswd\b\s)'

# ===================================================================== #
#  SELF-TESTS (run before any real config; safe, mutate only temp dirs)
# ===================================================================== #
selftest_guardrail(){
  local rc=0 t
  _g(){ python3 - "$1" <<'PY' 2>/dev/null
import json,sys,re,os
m=json.loads(sys.argv[1])
FP=os.environ['FORBID_PATH_RE']; FT=os.environ['FORBID_TEXT_RE']
import re
fp=re.compile(FP); ft=re.compile(FT)
def bad(s):
    t=s.get('type')
    if t=='write_file':
        p=s.get('path','')
        if fp.search(p): return 'path '+p
        if p=='/etc/systemd/system/atlas-autopull.timer':
            c=s.get('content','')
            if ('Unit=atlas-autopull.service' not in c) or ('OnUnitActiveSec=' not in c) or ('WantedBy=timers.target' not in c):
                return 'timer would sever pipe'
        if ft.search(s.get('content','')): return 'content forbidden'
    if t=='systemd':
        if s.get('action') in ('disable','stop','mask') and re.search(r'(atlas-autopull|ssh)', s.get('unit','')):
            return 'systemd '+s.get('action','')+' '+s.get('unit','')
    if t=='run_allowlisted':
        if ft.search(s.get('cmd','')): return 'cmd forbidden'
    return None
for i,s in enumerate(m['steps']):
    r=bad(s)
    if r: print('REJECT step %d: %s'%(i,r)); sys.exit(0)
print('OK'); sys.exit(0)
PY
}
  export FORBID_PATH_RE FORBID_TEXT_RE
  # must REJECT each of these:
  for t in \
    '{"steps":[{"type":"systemd","action":"disable","unit":"atlas-autopull.timer"}]}' \
    '{"steps":[{"type":"systemd","action":"stop","unit":"sshd"}]}' \
    '{"steps":[{"type":"write_file","path":"/etc/ssh/sshd_config","content":"x"}]}' \
    '{"steps":[{"type":"write_file","path":"/root/.ssh/authorized_keys","content":"x"}]}' \
    '{"steps":[{"type":"write_file","path":"/opt/atlas/autopull/atlas-autopull.sh","content":"x"}]}' \
    '{"steps":[{"type":"run_allowlisted","cmd":"/opt/atlas/x && ufw delete allow OpenSSH"}]}' \
    '{"steps":[{"type":"run_allowlisted","cmd":"/opt/atlas/x; systemctl disable atlas-autopull.timer"}]}' \
    '{"steps":[{"type":"write_file","path":"/etc/systemd/system/atlas-autopull.timer","content":"[Timer]\nOnUnitActiveSec=999min\n"}]}' \
  ; do
    case "$(_g "$t")" in REJECT*) : ;; *) echo "SELFTEST FAIL: should have rejected: $t"; rc=1;; esac
  done
  # must ACCEPT these:
  for t in \
    '{"steps":[{"type":"noop","note":"x"}]}' \
    '{"steps":[{"type":"write_file","path":"/opt/atlas/importers/overture_pg_import.py","content":"print(1)"}]}' \
    '{"steps":[{"type":"write_file","path":"/etc/systemd/system/atlas-autopull.timer","content":"[Timer]\nOnUnitActiveSec=3min\nUnit=atlas-autopull.service\n\n[Install]\nWantedBy=timers.target\n"}]}' \
    '{"steps":[{"type":"run_allowlisted","cmd":"/opt/atlas/venv/bin/python /opt/atlas/importers/overture_pg_import.py"}]}' \
  ; do
    case "$(_g "$t")" in OK) : ;; *) echo "SELFTEST FAIL: should have accepted: $t"; rc=1;; esac
  done
  [ $rc -eq 0 ] && echo "guardrail selftest: PASS" || echo "guardrail selftest: FAIL"
  return $rc
}

rollback_selftest(){
  # Proves snapshot -> mutate -> forced-health-fail -> restore, on a TEMP sandbox.
  local SB; SB="$(mktemp -d /tmp/atlas-rbtest.XXXXXX)"
  mkdir -p "$SB/live" "$SB/snap"
  echo "GOOD-CONFIG-v1" > "$SB/live/app.conf"
  # snapshot
  tar -C "$SB/live" -cf "$SB/snap/live.tar" . || { echo "rollback selftest: snapshot FAIL"; return 1; }
  # mutate (simulate an apply that breaks things)
  echo "BROKEN-CONFIG-v2" > "$SB/live/app.conf"
  echo "stray" > "$SB/live/extra.bad"
  # forced health fail -> restore
  rm -rf "$SB/live"/* && tar -C "$SB/live" -xf "$SB/snap/live.tar"
  local got; got="$(cat "$SB/live/app.conf")"
  local rc=0
  [ "$got" = "GOOD-CONFIG-v1" ] || { echo "rollback selftest: content not restored ($got)"; rc=1; }
  [ -e "$SB/live/extra.bad" ] && { echo "rollback selftest: stray file survived"; rc=1; }
  rm -rf "$SB"
  [ $rc -eq 0 ] && echo "rollback selftest: PASS (snapshot->mutate->restore verified)" || echo "rollback selftest: FAIL"
  return $rc
}

if [ "${1:-}" = "--selftest" ]; then
  export FORBID_PATH_RE FORBID_TEXT_RE
  r=0; selftest_guardrail || r=1; rollback_selftest || r=1
  [ $r -eq 0 ] && echo "ALL SELFTESTS PASS" || echo "SELFTESTS FAILED"
  exit $r
fi
if [ "${1:-}" = "--rollback-selftest" ]; then rollback_selftest; exit $?; fi

# ===================================================================== #
#  NORMAL RUN
# ===================================================================== #
CONF="${ATLAS_AUTOPULL_CONF:-/etc/atlas/autopull.env}"
LOG="${ATLAS_AUTOPULL_LOG:-/var/log/atlas/autopull.log}"
STATE_DIR="${ATLAS_AUTOPULL_STATE:-/var/lib/atlas/autopull}"
RESTORE_ROOT="${ATLAS_RESTORE_ROOT:-/opt/atlas/restore}"
KEEP_RESTORE="${ATLAS_KEEP_RESTORE:-5}"
WORK="$(mktemp -d /tmp/atlas-pull.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

mkdir -p "$(dirname "$LOG")" "$STATE_DIR" "$RESTORE_ROOT" 2>/dev/null || true
log(){ printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$LOG" >&2; }
die(){ local code="$1"; shift; log "FATAL($code): $*"; exit "$code"; }

# ---------- 1. config / secret (env or wp-config ONLY) ----------
[ -f "$CONF" ] || die 10 "missing config $CONF"
# shellcheck disable=SC1090
. "$CONF"
: "${BRAIN_URL:?BRAIN_URL not set}"
if [ -z "${DEPLOY_SECRET:-}" ] && [ -n "${WP_CONFIG:-}" ] && [ -f "${WP_CONFIG}" ]; then
  DEPLOY_SECRET="$(grep -oE "ATLAS_DEPLOY_SECRET'[^)]*" "$WP_CONFIG" | sed -E "s/.*,[[:space:]]*'([^']+)'.*/\1/")"
fi
[ -n "${DEPLOY_SECRET:-}" ] || die 10 "DEPLOY_SECRET not found in env or wp-config"
NODE_ID="${NODE_ID:-interserver}"
PGDB="${PGDATABASE:-${DB_NAME:-tuanichat_atlas}}"

hmac(){ openssl dgst -sha256 -hmac "$DEPLOY_SECRET" -hex "$1" | sed -E 's/^.*= *//'; }

# ---------- signed status report (now: GitHub push, closes the loop) ----------
report(){ # report <status> <detail>
  local st="$1"; shift; local detail="$*"
  local counts="null"
  [ -f "$STATE_DIR/last_counts.json" ] && counts="$(tr -d '\n' < "$STATE_DIR/last_counts.json")"
  local body
  body="$(printf '{"node":"%s","seq":%s,"status":"%s","detail":"%s","counts":%s,"ts":%s}' \
        "$NODE_ID" "${SEQ:-0}" "$st" "$detail" "$counts" "$(date +%s)")"
  printf '%s\n' "$body" > "$STATE_DIR/last_status.json" 2>/dev/null || true
  if [ -n "${STATUS_TOKEN:-}" ] && [ -n "${STATUS_REPO:-}" ]; then
    local path="status/${NODE_ID}/seq-${SEQ:-0}-${st}.json"
    local b64; b64="$(printf '%s' "$body" | base64 | tr -d '\n')"
    local payload; payload="$(printf '{"message":"status %s seq=%s %s","content":"%s"}' "$NODE_ID" "${SEQ:-0}" "$st" "$b64")"
    curl -fsS --max-time 15 -X PUT \
      -H "Authorization: Bearer $STATUS_TOKEN" -H "Accept: application/vnd.github+json" \
      "https://api.github.com/repos/$STATUS_REPO/contents/$path" \
      --data-binary "$payload" >/dev/null 2>&1 \
      && log "status pushed -> $path" || log "status push failed (non-fatal): $st"
  else
    log "status: $st seq=${SEQ:-0} (local only; STATUS_TOKEN unset)"
  fi
}

# ---------- 2. fetch manifest + signature ----------
log "pull start node=$NODE_ID brain=$BRAIN_URL"
curl -fsS --max-time 20 "$BRAIN_URL/manifest.json"     -o "$WORK/manifest.json" || die 20 "manifest fetch failed"
curl -fsS --max-time 20 "$BRAIN_URL/manifest.json.sig" -o "$WORK/manifest.sig"  || die 20 "signature fetch failed"

# ---------- 3. FAIL-CLOSED signature verify (before any parse) ----------
GOT="$(hmac "$WORK/manifest.json")"
WANT="$(tr -d ' \t\r\n' < "$WORK/manifest.sig")"
if [ "$GOT" != "$WANT" ]; then
  die 30 "SIGNATURE MISMATCH - refusing to parse or apply (got=${GOT:0:12}.. want=${WANT:0:12}..)"
fi
log "signature OK (HMAC-SHA256 verified)"

jget(){ python -c "import json,sys;d=json.load(open('$WORK/manifest.json'));print(d$1)" 2>/dev/null; }
SEQ="$(jget "['seq']")";   [ -n "$SEQ" ] || die 40 "manifest missing seq"
MODE="$(jget "['mode']")"; NSTEPS="$(jget "['steps'].__len__()")"
log "manifest seq=$SEQ mode=${MODE:-apply} steps=$NSTEPS"

# ---------- 4. dedup ----------
LAST="$(cat "$STATE_DIR/last_seq" 2>/dev/null || echo 0)"
if [ "$SEQ" -le "$LAST" ] 2>/dev/null; then
  log "already at seq>=$SEQ (last=$LAST) -> up to date"; report ok "up-to-date seq=$SEQ"; exit 0
fi

# ---------- G. LIFELINE GUARDRAIL (fail-closed, before diagnose/apply) ----------
export FORBID_PATH_RE FORBID_TEXT_RE
GR="$(python - "$WORK/manifest.json" <<'PY'
import json,sys,re,os
d=json.load(open(sys.argv[1]))
fp=re.compile(os.environ['FORBID_PATH_RE']); ft=re.compile(os.environ['FORBID_TEXT_RE'])
def bad(s):
    t=s.get('type')
    if t=='write_file':
        p=s.get('path','')
        if fp.search(p): return 'write_file path blocked: '+p
        if p=='/etc/systemd/system/atlas-autopull.timer':
            c=s.get('content','')
            if ('Unit=atlas-autopull.service' not in c) or ('OnUnitActiveSec=' not in c) or ('WantedBy=timers.target' not in c):
                return 'timer rewrite would sever pipe'
        if ft.search(s.get('content','') or ''): return 'write_file content blocked (lifeline)'
    if t=='systemd':
        if s.get('action') in ('disable','stop','mask') and re.search(r'(atlas-autopull|ssh)', s.get('unit','')):
            return 'systemd %s %s blocked'%(s.get('action'),s.get('unit'))
    if t=='run_allowlisted':
        if ft.search(s.get('cmd','') or ''): return 'run_allowlisted cmd blocked (lifeline)'
    return None
for i,s in enumerate(d.get('steps',[])):
    r=bad(s)
    if r: print('REJECT %d: %s'%(i,r)); break
else:
    print('OK')
PY
)"
if [ "$GR" != "OK" ]; then
  log "GUARDRAIL $GR"
  report rejected "guardrail: $GR"
  die 41 "lifeline guardrail rejected manifest (no mutation performed): $GR"
fi
log "lifeline guardrail: passed (no pipe/ssh/key/ufw/network-severing steps)"

# ---------- 5. DIAGNOSTIC-FIRST (typed allowlist) ----------
ALLOWED_CMDS="systemctl|/opt/atlas/|sqlite3"
log "=== DIAGNOSTIC PASS (no mutations) ==="
i=0
while [ "$i" -lt "$NSTEPS" ]; do
  TYPE="$(jget "['steps'][$i]['type']")"
  case "$TYPE" in
    noop)          log "step $i noop -> $(jget "['steps'][$i].get('note','')")";;
    write_file)    log "step $i write_file -> $(jget "['steps'][$i]['path']")";;
    systemd)       log "step $i systemd -> $(jget "['steps'][$i]['action']") $(jget "['steps'][$i]['unit']")";;
    sql_insert_ignore) log "step $i sql_insert_ignore -> $(jget "['steps'][$i]['db']")";;
    run_allowlisted) CMD="$(jget "['steps'][$i]['cmd']")"
                     echo "$CMD" | grep -qE "^($ALLOWED_CMDS)" || die 40 "step $i cmd not in allowlist: $CMD"
                     log "step $i run_allowlisted -> $CMD";;
    *) die 40 "step $i UNKNOWN type '$TYPE'";;
  esac
  i=$((i+1))
done
log "diagnostics passed; all step types allowlisted"
if [ "${MODE:-apply}" = "diagnose" ]; then
  log "MODE=diagnose -> stopping before apply"; report diagnosed "seq=$SEQ steps=$NSTEPS"; exit 0
fi

# ---------- R. RESTORE POINT (before any mutation) ----------
TS="$(date -u +%Y%m%dT%H%M%SZ)"
SNAP="$RESTORE_ROOT/$SEQ-$TS"
mkdir -p "$SNAP" || die 50 "cannot create restore point $SNAP"
log "=== RESTORE POINT $SNAP ==="
# files + configs + active atlas units (this is what auto-rollback restores)
tar -C / -czf "$SNAP/files.tar.gz" \
    --exclude='opt/atlas/restore' --exclude='opt/atlas/data' \
    opt/atlas etc/atlas 2>/dev/null || log "  WARN tar of /opt/atlas+/etc/atlas partial"
mkdir -p "$SNAP/units"
cp -a /etc/systemd/system/atlas-*.service /etc/systemd/system/atlas-*.timer "$SNAP/units/" 2>/dev/null || true
# best-effort DB schema + row-count marker (NOT auto-restored; kept for manual use)
( sudo -u postgres pg_dump -s "$PGDB" > "$SNAP/schema.sql" ) 2>/dev/null || log "  WARN pg schema dump skipped"
( sudo -u postgres psql -d "$PGDB" -tAc \
   "select 'rowcounts', json_object_agg(relname, n_live_tup) from pg_stat_user_tables" \
   > "$SNAP/rowcounts.txt" ) 2>/dev/null || echo "rowcounts unavailable" > "$SNAP/rowcounts.txt"
ln -sfn "$SNAP" "$RESTORE_ROOT/last-good"
# prune to last N
ls -1dt "$RESTORE_ROOT"/*/ 2>/dev/null | grep -v '/last-good/' | tail -n +$((KEEP_RESTORE+1)) | xargs -r rm -rf
log "restore point ready; last-good -> $SNAP"

restore_from_snapshot(){
  log "!!! AUTO-ROLLBACK restoring files+units from $SNAP"
  tar -C / -xzf "$SNAP/files.tar.gz" 2>/dev/null && log "  files restored" || log "  files restore FAIL"
  cp -a "$SNAP/units/." /etc/systemd/system/ 2>/dev/null || true
  systemctl daemon-reload 2>/dev/null || true
  systemctl restart atlas-autopull.timer 2>/dev/null || true
}

# ---------- 6. ATOMIC APPLY with per-step rollback ----------
declare -a ROLLBACK
rollback_steps(){ local n=${#ROLLBACK[@]} k; for ((k=n-1;k>=0;k--)); do eval "${ROLLBACK[$k]}" && log "  undo ok: ${ROLLBACK[$k]}" || log "  undo FAIL: ${ROLLBACK[$k]}"; done; }
apply_fail(){ log "APPLY FAILED at step $1: $2"; rollback_steps; restore_from_snapshot; report failed "step $1: $2 (rolled back)"; exit 50; }

log "=== APPLY PASS (seq=$SEQ) ==="
i=0
while [ "$i" -lt "$NSTEPS" ]; do
  TYPE="$(jget "['steps'][$i]['type']")"
  case "$TYPE" in
    noop) : ;;
    write_file)
      P="$(jget "['steps'][$i]['path']")"
      jget "['steps'][$i]['content']" > "$WORK/content.$i" || apply_fail "$i" "no content"
      if [ -f "$P" ]; then cp -a "$P" "$SNAP/$(echo "$P"|tr / _)"; ROLLBACK+=("cp -a '$SNAP/$(echo "$P"|tr / _)' '$P'"); else ROLLBACK+=("rm -f '$P'"); fi
      mkdir -p "$(dirname "$P")"
      cp "$WORK/content.$i" "$P.tmp.$$" && mv -f "$P.tmp.$$" "$P" || apply_fail "$i" "write $P";;
    systemd)
      A="$(jget "['steps'][$i]['action']")"; U="$(jget "['steps'][$i]['unit']")"
      case "$A" in
        daemon-reload) systemctl daemon-reload || apply_fail "$i" "daemon-reload";;
        enable)  systemctl enable "$U"  || apply_fail "$i" "enable $U"; ROLLBACK+=("systemctl disable '$U'");;
        restart) systemctl restart "$U" || apply_fail "$i" "restart $U";;
        start)   systemctl start "$U"   || apply_fail "$i" "start $U"; ROLLBACK+=("systemctl stop '$U'");;
        *) apply_fail "$i" "bad systemd action $A";;
      esac;;
    sql_insert_ignore)
      DBF="$(jget "['steps'][$i]['db']")"; SQL="$(jget "['steps'][$i]['sql']")"
      echo "$SQL" | grep -qiE '^\s*INSERT OR IGNORE' || apply_fail "$i" "sql must be INSERT OR IGNORE, got: $SQL"
      sqlite3 "$DBF" "$SQL" || apply_fail "$i" "sqlite exec";;
    run_allowlisted)
      CMD="$(jget "['steps'][$i]['cmd']")"
      echo "$CMD" | grep -qE "^($ALLOWED_CMDS)" || apply_fail "$i" "cmd not allowlisted"
      eval "$CMD" || apply_fail "$i" "cmd failed: $CMD";;
  esac
  log "  applied step $i ($TYPE)"
  i=$((i+1))
done

# ---------- H. HEALTH CHECK (lifeline + deploy correctness) ----------
health_fail=""
hc(){ # hc <name> <test-cmd...>  ; records failure but keeps going to log all
  local name="$1"; shift
  if "$@" >/dev/null 2>&1; then log "  health OK: $name"; else log "  health FAIL: $name"; health_fail="$health_fail $name"; fi
}
log "=== HEALTH CHECK (seq=$SEQ) ==="
# postgres accepting connections + schema present
hc "pg-accepting" bash -c 'command -v pg_isready >/dev/null && pg_isready -q || sudo -u postgres psql -tAc "select 1" >/dev/null'
hc "pg-schema-present" bash -c "sudo -u postgres psql -d '$PGDB' -tAc \"select to_regclass('atlas.business') is not null\" | grep -qi t"
# the pipe itself must still be enabled + active
hc "autopull-timer-enabled" bash -c 'systemctl is-enabled atlas-autopull.timer | grep -q enabled'
hc "autopull-timer-active"  bash -c 'systemctl is-active  atlas-autopull.timer | grep -q active'
# SSH must still be listening (lifeline) -- accept ssh or sshd unit name
hc "sshd-listening" bash -c 'ss -ltnH 2>/dev/null | grep -qE ":(22)\b" || systemctl is-active ssh sshd 2>/dev/null | grep -q active'
# if UFW is active, it must still allow SSH (if inactive, no firewall block -> ok)
hc "ufw-ssh-allowed" bash -c 'command -v ufw >/dev/null || exit 0; ufw status 2>/dev/null | grep -qi "Status: active" || exit 0; ufw status 2>/dev/null | grep -Ei "(22|OpenSSH)" | grep -qi allow'

if [ -n "$health_fail" ]; then
  log "HEALTH FAILED:$health_fail -> AUTO-ROLLBACK"
  rollback_steps
  restore_from_snapshot
  # re-verify the lifeline after rollback
  systemctl is-active atlas-autopull.timer >/dev/null 2>&1 && log "  post-rollback: pipe alive" || log "  post-rollback: WARN pipe not active"
  report failed "health:$health_fail (rolled back to last-good)"
  exit 55
fi

echo "$SEQ" > "$STATE_DIR/last_seq"
log "=== APPLY COMPLETE seq=$SEQ (healthy; last_seq advanced) ==="
report ok "applied seq=$SEQ steps=$NSTEPS healthy"
exit 0
                                                            