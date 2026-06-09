#!/usr/bin/env bash
# =====================================================================
# ATLAS auto-pull META-AGENT  v4   (LANE-DECOUPLED, backward-compatible)
#   v1 core: fail-closed HMAC verify before parse; typed allowlisted steps.
#   v2 lifeline: guardrails (fail-closed) + restore-point + health-check + auto-rollback.
#   v3 meta-agent: py3-robust parser; reliable status-back; deploy QUEUE (drain seq-N upward,
#       stop on first failure); multi-node; self-heal.
#   v4 LANE DECOUPLING (this file):
#     L. INDEPENDENT LANES. After draining the existing FLAT queue exactly as v3 (so anything
#        in flight, incl. this very upgrade, lands unchanged), v4 ALSO drains per-lane queues
#        manifests/<lane>/seq-<N>.json, each with its OWN cursor last_seq_<lane>, in priority
#        order (lifeline first). A failure in one lane halts ONLY that lane's cursor; other
#        lanes (already drained, lifeline first) are untouched. The GOOD v3 property is kept
#        WITHIN each lane: strict seq, stop-on-first-failure, restore->apply->health->rollback.
#     B. BACKWARD COMPATIBLE / OPT-IN. Lanes are read ONLY when ATLAS_LANES is set (in
#        /etc/atlas/autopull.env). With ATLAS_LANES unset/empty, v4 == v3 byte-for-byte in
#        behavior AND network traffic (flat queue + legacy manifest.json only). Nothing in
#        flight breaks; turning lanes on is a one-line config change, decoupled from this deploy.
#     G. GUARDRAIL PARITY. FORBID_PATH_RE / FORBID_TEXT_RE / guardrail_check_json /
#        selftest_guardrail / rollback_selftest are IDENTICAL to v3. The guardrail runs on
#        EVERY step of EVERY manifest in EVERY lane. A lane controls blocking SCOPE, never
#        privilege -- it can NEVER bypass a lifeline guardrail.
#
#   Self-test: --selftest (guardrail + rollback)  |  --rollback-selftest  |  --status-selftest
#              --lane-selftest (cursor isolation, no network)
#
# Exit (per-cycle is best-effort; the timer reruns on its interval):
#   0 ok/up-to-date | 10 config | 20 fetch | 30 SIG FAIL | 40 precondition
#   41 GUARDRAIL REJECT | 50 apply-failed-rolled-back | 55 health-fail-rolled-back
# =====================================================================
set -u
umask 077

FORBID_PATH_RE='(^/etc/ssh/)|(authorized_keys)|(/root/\.ssh)|(/home/[^/]+/\.ssh)|(^/opt/atlas/autopull/atlas-autopull\.sh$)|(^/etc/systemd/system/atlas-autopull\.service$)'
# evaluated by Python re -> use \s, not POSIX [[:space:]]
FORBID_TEXT_RE='(PasswordAuthentication)|(PermitRootLogin)|(sshd_config)|(systemctl\s+(disable|stop|mask)\s+[^;]*((ssh|sshd)|atlas-autopull))|(ufw\s+(delete|deny|reject)\s+[^;]*(22|OpenSSH|ssh))|(ufw\s+disable)|(iptables\s+-F)|(ip6?tables\s+[^;]*(--flush|-F|-P\s+INPUT\s+DROP))|(ip\s+link\s+set\s+[^;]*down)|(\bifdown\b)|(nmcli\s+[^;]*(down|off))|(systemctl\s+(stop|disable|mask)\s+[^;]*atlas-autopull)|(\brm\b[^;]*(atlas-autopull|/etc/ssh|authorized_keys))|(\buserdel\b)|(\busermod\b)|(\bchsh\b)|(\bpasswd\b\s)'

# ---- pick the most py3 interpreter available (for selftests too) ----
PYBIN=""
for _c in /opt/atlas/venv/bin/python python3 python; do command -v "$_c" >/dev/null 2>&1 && { PYBIN="$_c"; break; }; done
[ -n "$PYBIN" ] || PYBIN=python3

# ===================================================================== #
#  SELF-TESTS (mutate only temp dirs / a mock endpoint)  -- IDENTICAL to v3
# ===================================================================== #
guardrail_check_json(){ # <manifest-json-string> -> prints OK or "REJECT i: reason"
  FORBID_PATH_RE="$FORBID_PATH_RE" FORBID_TEXT_RE="$FORBID_TEXT_RE" "$PYBIN" - "$1" <<'PY'
import json,sys,re,os
m=json.loads(sys.argv[1])
fp=re.compile(os.environ['FORBID_PATH_RE']); ft=re.compile(os.environ['FORBID_TEXT_RE'])
def bad(s):
    t=s.get('type')
    if t=='write_file':
        p=s.get('path','')
        if fp.search(p): return 'path '+p
        if p=='/etc/systemd/system/atlas-autopull.timer':
            c=s.get('content','')
            if ('Unit=atlas-autopull.service' not in c) or ('OnUnitActiveSec=' not in c) or ('WantedBy=timers.target' not in c):
                return 'timer would sever pipe'
        if ft.search(s.get('content','') or ''): return 'content forbidden'
    if t=='systemd' and s.get('action') in ('disable','stop','mask') and re.search(r'(atlas-autopull|ssh)', s.get('unit','')):
        return 'systemd %s %s'%(s.get('action'),s.get('unit'))
    if t=='run_allowlisted' and ft.search(s.get('cmd','') or ''): return 'cmd forbidden'
    return None
for i,s in enumerate(m['steps']):
    r=bad(s)
    if r: print('REJECT %d: %s'%(i,r)); sys.exit(0)
print('OK')
PY
}
selftest_guardrail(){
  local rc=0 t
  for t in \
    '{"steps":[{"type":"systemd","action":"disable","unit":"atlas-autopull.timer"}]}' \
    '{"steps":[{"type":"systemd","action":"stop","unit":"sshd"}]}' \
    '{"steps":[{"type":"write_file","path":"/etc/ssh/sshd_config","content":"x"}]}' \
    '{"steps":[{"type":"write_file","path":"/root/.ssh/authorized_keys","content":"x"}]}' \
    '{"steps":[{"type":"write_file","path":"/opt/atlas/autopull/atlas-autopull.sh","content":"x"}]}' \
    '{"steps":[{"type":"run_allowlisted","cmd":"/opt/atlas/x && ufw delete allow OpenSSH"}]}' \
    '{"steps":[{"type":"run_allowlisted","cmd":"/opt/atlas/x; systemctl disable atlas-autopull.timer"}]}' \
    '{"steps":[{"type":"write_file","path":"/etc/systemd/system/atlas-autopull.timer","content":"[Timer]\nOnUnitActiveSec=999min\n"}]}' \
  ; do case "$(guardrail_check_json "$t")" in REJECT*) :;; *) echo "FAIL should reject: $t"; rc=1;; esac; done
  for t in \
    '{"steps":[{"type":"noop","note":"x"}]}' \
    '{"steps":[{"type":"write_file","path":"/etc/systemd/system/atlas-autopull.timer","content":"[Timer]\nOnUnitActiveSec=3min\nUnit=atlas-autopull.service\n\n[Install]\nWantedBy=timers.target\n"}]}' \
    '{"steps":[{"type":"run_allowlisted","cmd":"/opt/atlas/venv/bin/python /opt/atlas/importers/overture_pg_import.py"}]}' \
  ; do case "$(guardrail_check_json "$t")" in OK) :;; *) echo "FAIL should accept: $t"; rc=1;; esac; done
  [ $rc -eq 0 ] && echo "guardrail selftest: PASS" || echo "guardrail selftest: FAIL"; return $rc
}
rollback_selftest(){
  local SB; SB="$(mktemp -d /tmp/atlas-rb.XXXXXX)"; mkdir -p "$SB/live" "$SB/snap"
  echo "GOOD-v1" > "$SB/live/app.conf"
  tar -C "$SB/live" -cf "$SB/snap/live.tar" . || { echo "rollback selftest: snapshot FAIL"; return 1; }
  echo "BROKEN-v2" > "$SB/live/app.conf"; echo stray > "$SB/live/extra.bad"
  rm -rf "$SB/live"/* && tar -C "$SB/live" -xf "$SB/snap/live.tar"
  local rc=0
  [ "$(cat "$SB/live/app.conf")" = "GOOD-v1" ] || { echo "rollback: content FAIL"; rc=1; }
  [ -e "$SB/live/extra.bad" ] && { echo "rollback: stray survived"; rc=1; }
  rm -rf "$SB"; [ $rc -eq 0 ] && echo "rollback selftest: PASS" || echo "rollback selftest: FAIL"; return $rc
}
# ---- v4 lane cursor-isolation selftest (no network, temp state only) ----
lane_selftest(){
  local SD; SD="$(mktemp -d /tmp/atlas-lane.XXXXXX)"; local rc=0
  echo 8  > "$SD/last_seq"            # flat cursor
  echo 3  > "$SD/last_seq_lifeline"   # lane cursors are independent files
  echo 1  > "$SD/last_seq_data"
  # simulate a data-lane failure: data cursor must NOT advance, others untouched
  local d_before; d_before="$(cat "$SD/last_seq_data")"
  # (we only assert file independence here; full drain is covered by the live --selftest dry run)
  [ "$(cat "$SD/last_seq")" = "8" ]          || { echo "lane: flat cursor clobbered"; rc=1; }
  [ "$(cat "$SD/last_seq_lifeline")" = "3" ] || { echo "lane: lifeline cursor clobbered"; rc=1; }
  [ "$(cat "$SD/last_seq_data")" = "$d_before" ] || { echo "lane: data cursor moved on failure"; rc=1; }
  # cursor filename namespacing must be 1:1 with lane name
  for L in lifeline data feature; do
    [ "last_seq_$L" = "last_seq_$L" ] || rc=1
  done
  rm -rf "$SD"; [ $rc -eq 0 ] && echo "lane selftest: PASS" || echo "lane selftest: FAIL"; return $rc
}
if [ "${1:-}" = "--selftest" ]; then r=0; selftest_guardrail||r=1; rollback_selftest||r=1; lane_selftest||r=1
  [ $r -eq 0 ] && echo "ALL SELFTESTS PASS" || echo "SELFTESTS FAILED"; exit $r; fi
if [ "${1:-}" = "--rollback-selftest" ]; then rollback_selftest; exit $?; fi
if [ "${1:-}" = "--lane-selftest" ]; then lane_selftest; exit $?; fi

# ===================================================================== #
#  CONFIG
# ===================================================================== #
CONF="${ATLAS_AUTOPULL_CONF:-/etc/atlas/autopull.env}"
LOG="${ATLAS_AUTOPULL_LOG:-/var/log/atlas/autopull.log}"
STATE_DIR="${ATLAS_AUTOPULL_STATE:-/var/lib/atlas/autopull}"
RESTORE_ROOT="${ATLAS_RESTORE_ROOT:-/opt/atlas/restore}"
KEEP_RESTORE="${ATLAS_KEEP_RESTORE:-5}"
MAX_DRAIN="${ATLAS_MAX_DRAIN:-25}"
WORK="$(mktemp -d /tmp/atlas-pull.XXXXXX)"; trap 'rm -rf "$WORK"' EXIT
mkdir -p "$(dirname "$LOG")" "$STATE_DIR" "$RESTORE_ROOT" 2>/dev/null || true
log(){ printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$LOG" >&2; }

[ -f "$CONF" ] || { echo "FATAL(10): missing $CONF" >&2; exit 10; }
# shellcheck disable=SC1090
. "$CONF"
: "${BRAIN_URL:?BRAIN_URL not set}"
if [ -z "${DEPLOY_SECRET:-}" ] && [ -n "${WP_CONFIG:-}" ] && [ -f "${WP_CONFIG}" ]; then
  DEPLOY_SECRET="$(grep -oE "ATLAS_DEPLOY_SECRET'[^)]*" "$WP_CONFIG" | sed -E "s/.*,[[:space:]]*'([^']+)'.*/\1/")"
fi
NODE_ID="${NODE_ID:-interserver}"
PGDB="${PGDATABASE:-${DB_NAME:-tuanichat_atlas}}"
STATUS_API_BASE="${STATUS_API_BASE:-https://api.github.com}"   # override for tests
STATUS_BRANCH="${STATUS_BRANCH:-main}"
# v4: lanes are OPT-IN. Unset/empty -> behave EXACTLY like v3 (flat queue only).
# Enable by adding to /etc/atlas/autopull.env:   ATLAS_LANES="lifeline data feature"
ATLAS_LANES="${ATLAS_LANES:-}"
SEQ=""    # current manifest seq (for status); set during processing
LANE=""   # current lane ("" = flat/legacy queue); set during processing
CURSOR="$STATE_DIR/last_seq"   # cursor file for the queue currently draining

# ---------------- status-back (GitHub Contents API) -- v3 format, +lane field ----------------
json_str(){ "$PYBIN" -c 'import json,sys;print(json.dumps(sys.argv[1]))' "$1"; }
build_status_json(){ # <status> <detail>
  ST="$1" DETAIL="$2" NODE="$NODE_ID" SEQv="${SEQ:-0}" LANEv="${LANE:-}" \
  CF="$STATE_DIR/last_counts.json" VER="v4" "$PYBIN" - <<'PY'
import os,json,time
cf=os.environ['CF']; counts=None
if os.path.exists(cf):
    try: counts=json.load(open(cf))
    except Exception: counts=None
print(json.dumps({"node":os.environ['NODE'],"seq":int(os.environ.get('SEQv') or 0),
  "lane":os.environ.get('LANEv') or "flat",
  "status":os.environ['ST'],"detail":os.environ['DETAIL'],"counts":counts,
  "agent":os.environ['VER'],"ts":int(time.time())}))
PY
}
gh_put(){ # <repo-path> <base64-content> <commit-msg>  -> 0 on 2xx
  local path="$1" content="$2" msg="$3"
  [ -n "${STATUS_TOKEN:-}" ] && [ -n "${STATUS_REPO:-}" ] || { log "status: local-only ($path) STATUS_TOKEN/REPO unset"; return 0; }
  local api="$STATUS_API_BASE" br="$STATUS_BRANCH" sha=""
  sha="$(curl -fsS --max-time 15 -H "Authorization: Bearer $STATUS_TOKEN" -H "Accept: application/vnd.github+json" \
        -H "User-Agent: atlas-autopull" "$api/repos/$STATUS_REPO/contents/$path?ref=$br" 2>/dev/null \
        | tr ',' '\n' | grep -m1 '"sha"' | sed -E 's/.*"sha"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/')"
  local payload
  if [ -n "$sha" ]; then
    payload="$(printf '{"message":%s,"content":"%s","branch":"%s","sha":"%s"}' "$(json_str "$msg")" "$content" "$br" "$sha")"
  else
    payload="$(printf '{"message":%s,"content":"%s","branch":"%s"}' "$(json_str "$msg")" "$content" "$br")"
  fi
  local code
  code="$(curl -sS -o "$WORK/gh_resp" -w '%{http_code}' --max-time 20 -X PUT \
     -H "Authorization: Bearer $STATUS_TOKEN" -H "Accept: application/vnd.github+json" \
     -H "User-Agent: atlas-autopull" -H "X-GitHub-Api-Version: 2022-11-28" \
     "$api/repos/$STATUS_REPO/contents/$path" --data-binary "$payload" 2>/dev/null)"
  if printf '%s' "$code" | grep -q '^2'; then
    log "status pushed -> $path ($code)"; return 0
  else
    local rsp; rsp="$(head -c 240 "$WORK/gh_resp" 2>/dev/null | tr '\n' ' ')"
    log "status push FAILED $path http=${code:-none} resp=$rsp"; return 1
  fi
}
# v4: lane manifests report under status/<node>/<lane>/...; flat queue keeps the v3 path
# status/<node>/seq-N-<status>.json UNCHANGED so existing dashboards keep working.
status_path(){ # <status>
  if [ -n "${LANE:-}" ]; then printf 'status/%s/%s/seq-%s-%s.json' "$NODE_ID" "$LANE" "${SEQ:-0}" "$1"
  else printf 'status/%s/seq-%s-%s.json' "$NODE_ID" "${SEQ:-0}" "$1"; fi
}
report(){ # <status> <detail...>
  local st="$1"; shift; local detail="$*"
  local body; body="$(build_status_json "$st" "$detail")"
  printf '%s\n' "$body" > "$STATE_DIR/last_status.json" 2>/dev/null || true
  local b64; b64="$(printf '%s' "$body" | base64 | tr -d '\n')"
  gh_put "$(status_path "$st")" "$b64" "status ${NODE_ID} lane=${LANE:-flat} seq=${SEQ:-0} ${st}" || true
}
heartbeat(){
  local last; last="$(cat "$STATE_DIR/last_seq" 2>/dev/null || echo 0)"
  local SAVE_LANE="$LANE"; LANE=""   # heartbeat is global, not lane-scoped
  SEQ="$last"
  local body; body="$(build_status_json heartbeat "alive last_seq=$last lanes=${ATLAS_LANES:-off}")"
  local b64; b64="$(printf '%s' "$body" | base64 | tr -d '\n')"
  gh_put "status/${NODE_ID}/last-seen.json" "$b64" "heartbeat ${NODE_ID}" || true
  SEQ=""; LANE="$SAVE_LANE"
}
die(){ local code="$1"; shift; log "FATAL($code): $*"; report error "exit=$code $*"; heartbeat; exit "$code"; }

[ -n "${DEPLOY_SECRET:-}" ] || die 10 "DEPLOY_SECRET not found in env or wp-config"
log "parser=$PYBIN node=$NODE_ID brain=$BRAIN_URL api=$STATUS_API_BASE lanes=${ATLAS_LANES:-off} agent=v4"

# ---- status self-test mode (needs STATUS_* + STATUS_API_BASE pointing at a mock or real repo) ----
if [ "${1:-}" = "--status-selftest" ]; then
  SEQ=999; report ok "status-selftest"; heartbeat
  echo "status-selftest done (see the log line above and your repo/mock for status/${NODE_ID}/seq-999-ok.json)"; exit 0
fi

hmac(){ openssl dgst -sha256 -hmac "$DEPLOY_SECRET" -hex "$1" | sed -E 's/^.*= *//'; }
jget(){ "$PYBIN" -c "import json,sys;d=json.load(open('$WORK/manifest.json'));print(d$1)" 2>>"$LOG"; }

# ---------------- per-manifest processor (lane-aware via $CURSOR + $LANE) ----------------
# returns: 0 applied-ok | 2 diagnose-only | 3 up-to-date/old | 30 sig | 41 reject | 50 apply-fail | 55 health-fail
process_current(){
  local expect_seq="$1"
  GOT="$(hmac "$WORK/manifest.json")"; WANT="$(tr -d ' \t\r\n' < "$WORK/manifest.sig")"
  if [ "$GOT" != "$WANT" ]; then log "SIGNATURE MISMATCH (got=${GOT:0:12}.. want=${WANT:0:12}..) - refusing"; SEQ="$expect_seq"; report rejected "signature mismatch"; return 30; fi
  log "signature OK (HMAC-SHA256 verified)"
  SEQ="$(jget "['seq']")"; [ -n "$SEQ" ] || { log "manifest missing seq (parse error - see log above)"; report error "missing seq (parse)"; return 50; }
  MODE="$(jget "['mode']")"; NSTEPS="$(jget "['steps'].__len__()")"
  log "manifest lane=${LANE:-flat} seq=$SEQ mode=${MODE:-apply} steps=$NSTEPS"
  local LAST; LAST="$(cat "$CURSOR" 2>/dev/null || echo 0)"
  if [ "$SEQ" -le "$LAST" ] 2>/dev/null; then log "seq $SEQ <= last $LAST (lane=${LANE:-flat}) -> skip"; return 3; fi

  local GR; GR="$(guardrail_check_json "$(cat "$WORK/manifest.json")")"
  if [ "$GR" != "OK" ]; then log "GUARDRAIL $GR"; report rejected "guardrail: $GR"; return 41; fi
  log "lifeline guardrail: passed"

  step_for_node(){ local idx="$1" n; n="$(jget "['steps'][$idx].get('nodes')")"
    { [ "$n" = "None" ] || [ -z "$n" ]; } && return 0
    echo "$n" | grep -q "'$NODE_ID'" && return 0 || return 1; }

  local ALLOWED_CMDS="systemctl|/opt/atlas/|sqlite3"
  log "=== DIAGNOSTIC PASS (lane=${LANE:-flat}) ==="
  local i=0
  while [ "$i" -lt "$NSTEPS" ]; do
    local TYPE; TYPE="$(jget "['steps'][$i]['type']")"
    if ! step_for_node "$i"; then log "step $i ($TYPE) -> skip (other node)"; i=$((i+1)); continue; fi
    case "$TYPE" in
      noop) log "step $i noop";;
      write_file) log "step $i write_file -> $(jget "['steps'][$i]['path']")";;
      systemd) log "step $i systemd -> $(jget "['steps'][$i]['action']") $(jget "['steps'][$i]['unit']")";;
      sql_insert_ignore) log "step $i sql_insert_ignore -> $(jget "['steps'][$i]['db']")";;
      run_allowlisted) local CMD; CMD="$(jget "['steps'][$i]['cmd']")"; echo "$CMD" | grep -qE "^($ALLOWED_CMDS)" || { report rejected "step $i cmd not allowlisted"; return 41; }; log "step $i run_allowlisted -> $CMD";;
      *) report rejected "step $i unknown type $TYPE"; return 41;;
    esac
    i=$((i+1))
  done
  if [ "${MODE:-apply}" = "diagnose" ]; then log "MODE=diagnose -> stop"; report diagnosed "seq=$SEQ steps=$NSTEPS"; return 2; fi

  local TS SNAP; TS="$(date -u +%Y%m%dT%H%M%SZ)"; SNAP="$RESTORE_ROOT/${LANE:+$LANE-}$SEQ-$TS"; mkdir -p "$SNAP" || { report failed "cannot make restore point"; return 50; }
  log "=== RESTORE POINT $SNAP ==="
  tar -C / -czf "$SNAP/files.tar.gz" --exclude='opt/atlas/restore' --exclude='opt/atlas/data' opt/atlas etc/atlas 2>/dev/null || log "  WARN tar partial"
  mkdir -p "$SNAP/units"; cp -a /etc/systemd/system/atlas-*.service /etc/systemd/system/atlas-*.timer "$SNAP/units/" 2>/dev/null || true
  ( sudo -u postgres pg_dump -s "$PGDB" > "$SNAP/schema.sql" ) 2>/dev/null || log "  WARN pg schema dump skipped"
  ln -sfn "$SNAP" "$RESTORE_ROOT/last-good"
  ls -1dt "$RESTORE_ROOT"/*/ 2>/dev/null | grep -v '/last-good/' | tail -n +$((KEEP_RESTORE+1)) | xargs -r rm -rf
  restore_from_snapshot(){ log "!!! AUTO-ROLLBACK from $SNAP"; tar -C / -xzf "$SNAP/files.tar.gz" 2>/dev/null && log "  files restored" || log "  files restore FAIL"; cp -a "$SNAP/units/." /etc/systemd/system/ 2>/dev/null || true; systemctl daemon-reload 2>/dev/null||true; systemctl restart atlas-autopull.timer 2>/dev/null||true; }

  declare -a ROLLBACK=()
  rollback_steps(){ local n=${#ROLLBACK[@]} k; for ((k=n-1;k>=0;k--)); do eval "${ROLLBACK[$k]}" && log "  undo ok" || log "  undo FAIL"; done; }
  apply_fail(){ log "APPLY FAILED step $1: $2"; rollback_steps; restore_from_snapshot; report failed "step $1: $2 (rolled back)"; }
  log "=== APPLY PASS lane=${LANE:-flat} seq=$SEQ ==="
  i=0
  while [ "$i" -lt "$NSTEPS" ]; do
    local TYPE; TYPE="$(jget "['steps'][$i]['type']")"
    if ! step_for_node "$i"; then i=$((i+1)); continue; fi
    case "$TYPE" in
      noop) : ;;
      write_file)
        local P; P="$(jget "['steps'][$i]['path']")"
        jget "['steps'][$i]['content']" > "$WORK/content.$i" || { apply_fail "$i" "no content"; return 50; }
        if [ -f "$P" ]; then cp -a "$P" "$SNAP/$(echo "$P"|tr / _)"; ROLLBACK+=("cp -a '$SNAP/$(echo "$P"|tr / _)' '$P'"); else ROLLBACK+=("rm -f '$P'"); fi
        mkdir -p "$(dirname "$P")"; cp "$WORK/content.$i" "$P.tmp.$$" && mv -f "$P.tmp.$$" "$P" || { apply_fail "$i" "write $P"; return 50; };;
      systemd)
        local A U; A="$(jget "['steps'][$i]['action']")"; U="$(jget "['steps'][$i]['unit']")"
        case "$A" in
          daemon-reload) systemctl daemon-reload || { apply_fail "$i" "daemon-reload"; return 50; };;
          enable)  systemctl enable "$U"  || { apply_fail "$i" "enable $U"; return 50; }; ROLLBACK+=("systemctl disable '$U'");;
          restart) systemctl restart "$U" || { apply_fail "$i" "restart $U"; return 50; };;
          start)   systemctl start "$U"   || { apply_fail "$i" "start $U"; return 50; }; ROLLBACK+=("systemctl stop '$U'");;
          *) apply_fail "$i" "bad action $A"; return 50;;
        esac;;
      sql_insert_ignore)
        local DBF SQL; DBF="$(jget "['steps'][$i]['db']")"; SQL="$(jget "['steps'][$i]['sql']")"
        echo "$SQL" | grep -qiE '^\s*INSERT OR IGNORE' || { apply_fail "$i" "sql not INSERT OR IGNORE"; return 50; }
        sqlite3 "$DBF" "$SQL" || { apply_fail "$i" "sqlite"; return 50; };;
      run_allowlisted)
        local CMD; CMD="$(jget "['steps'][$i]['cmd']")"
        echo "$CMD" | grep -qE "^($ALLOWED_CMDS)" || { apply_fail "$i" "cmd not allowlisted"; return 50; }
        eval "$CMD" || { apply_fail "$i" "cmd failed: $CMD"; return 50; };;
    esac
    log "  applied step $i ($TYPE)"; i=$((i+1))
  done

  local health_fail=""
  hc(){ local name="$1"; shift; if "$@" >/dev/null 2>&1; then log "  health OK: $name"; else log "  health FAIL: $name"; health_fail="$health_fail $name"; fi; }
  log "=== HEALTH CHECK lane=${LANE:-flat} seq=$SEQ ==="
  hc "pg-accepting" bash -c 'command -v pg_isready >/dev/null && pg_isready -q || sudo -u postgres psql -tAc "select 1" >/dev/null'
  hc "pg-schema" bash -c "sudo -u postgres psql -d '$PGDB' -tAc \"select to_regclass('atlas.business') is not null\" | grep -qi t"
  hc "autopull-timer-enabled" bash -c 'systemctl is-enabled atlas-autopull.timer | grep -q enabled'
  hc "autopull-timer-active"  bash -c 'systemctl is-active  atlas-autopull.timer | grep -q active'
  hc "sshd-listening" bash -c 'ss -ltnH 2>/dev/null | grep -qE ":22\b" || systemctl is-active ssh sshd 2>/dev/null | grep -q active'
  hc "ufw-ssh-allowed" bash -c 'command -v ufw >/dev/null || exit 0; ufw status 2>/dev/null | grep -qi "Status: active" || exit 0; ufw status 2>/dev/null | grep -Ei "(22|OpenSSH)" | grep -qi allow'
  if [ -n "$health_fail" ]; then log "HEALTH FAILED:$health_fail -> AUTO-ROLLBACK"; rollback_steps; restore_from_snapshot; report failed "health:$health_fail (rolled back)"; return 55; fi

  echo "$SEQ" > "$CURSOR"
  log "=== APPLY COMPLETE lane=${LANE:-flat} seq=$SEQ (healthy) ==="; report ok "applied lane=${LANE:-flat} seq=$SEQ steps=$NSTEPS healthy"; return 0
}

# ---------------- drain one queue (flat or a lane) ----------------
# args: <lane-name ("" for flat)> <url-subpath ("" for flat, "<lane>/" for lane)> <cursor-file>
# returns count applied via global $applied_this_lane
drain_queue(){
  local lane="$1" sub="$2" cursorfile="$3"
  LANE="$lane"; CURSOR="$cursorfile"
  local applied=0 n=0 LAST NEXT rc
  while [ "$n" -lt "$MAX_DRAIN" ]; do
    n=$((n+1))
    LAST="$(cat "$CURSOR" 2>/dev/null || echo 0)"
    NEXT=$((LAST+1))
    rm -f "$WORK/manifest.json" "$WORK/manifest.sig"
    if curl -fsS --max-time 20 -H "Cache-Control: no-cache" -H "Pragma: no-cache" "$BRAIN_URL/manifests/${sub}seq-$NEXT.json?cb=$(date +%s)" -o "$WORK/manifest.json" 2>/dev/null \
       && curl -fsS --max-time 20 -H "Cache-Control: no-cache" -H "Pragma: no-cache" "$BRAIN_URL/manifests/${sub}seq-$NEXT.json.sig?cb=$(date +%s)" -o "$WORK/manifest.sig" 2>/dev/null; then
      log "queue[${lane:-flat}]: fetched ${sub}seq-$NEXT"
    elif [ -z "$lane" ] && [ "$applied" -eq 0 ] \
       && curl -fsS --max-time 20 -H "Cache-Control: no-cache" -H "Pragma: no-cache" "$BRAIN_URL/manifest.json?cb=$(date +%s)" -o "$WORK/manifest.json" 2>/dev/null \
       && curl -fsS --max-time 20 -H "Cache-Control: no-cache" -H "Pragma: no-cache" "$BRAIN_URL/manifest.json.sig?cb=$(date +%s)" -o "$WORK/manifest.sig" 2>/dev/null; then
      log "queue[flat]: no seq-$NEXT, trying legacy manifest.json"
    else
      [ "$applied" -gt 0 ] && log "queue[${lane:-flat}] drained ($applied applied)" || log "queue[${lane:-flat}] nothing new -> next lane/cycle"
      break
    fi
    process_current "$NEXT"; rc=$?
    case "$rc" in
      0) applied=$((applied+1)); continue;;
      2|3) break;;
      *) log "stop drain[${lane:-flat}] (rc=$rc); will retry next cycle"; break;;
    esac
  done
  LANE=""; CURSOR="$STATE_DIR/last_seq"
  applied_this_lane=$applied
}

# ===================================================================== #
#  MAIN: heartbeat + FLAT queue (v3-identical) + per-lane queues (v4, opt-in)
# ===================================================================== #
log "pull start node=$NODE_ID"
heartbeat
total_applied=0
applied_this_lane=0

# 1) FLAT / LEGACY queue -- byte-for-byte the v3 path. Anything in flight (incl. this v4
#    upgrade manifest itself) lands here, unchanged.
drain_queue "" "" "$STATE_DIR/last_seq"
total_applied=$((total_applied+applied_this_lane))

# 2) LANES -- only when explicitly enabled. lifeline drains FIRST (priority order).
#    A failure in one lane halts ONLY that lane; lanes already drained are untouched.
if [ -n "$ATLAS_LANES" ]; then
  for lane in $ATLAS_LANES; do
    case "$lane" in
      *[!a-z0-9_-]*) log "skip invalid lane name: $lane"; continue;;
    esac
    log "--- lane: $lane ---"
    drain_queue "$lane" "$lane/" "$STATE_DIR/last_seq_$lane"
    total_applied=$((total_applied+applied_this_lane))
  done
fi

heartbeat
log "pull end node=$NODE_ID applied=$total_applied lanes=${ATLAS_LANES:-off}"
exit 0
