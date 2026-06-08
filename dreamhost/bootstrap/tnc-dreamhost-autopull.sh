#!/usr/bin/env bash
# =====================================================================
# TNC DreamHost auto-pull puller   (shared-hosting WordPress deploy lane)
# ---------------------------------------------------------------------
# Mirrors the safety model of the v3 Linux puller, adapted for SHARED
# HOSTING (DreamHost): NO systemd, NO root, cron-driven, php for JSON.
#
#   Safety model (same spirit as atlas-autopull.v3):
#     * FAIL-CLOSED HMAC-SHA256 verify BEFORE the manifest is parsed.
#     * sha256-PIN every package before it is allowed to touch the disk.
#     * BACKUP the current plugin dir to a restore point first.
#     * ATOMIC-ISH swap (stage -> mv-aside -> mv-in; restore on any error).
#     * VERIFY after apply (plugin header + php -l on the main file).
#     * AUTO-ROLLBACK on any failure; last_seq is NOT advanced so it retries.
#     * STATUS-BACK every cycle (success AND failure), readable over HTTPS.
#     * SINGLE-FLIGHT lock so overlapping cron runs can't collide.
#     * QUEUE drain: applies manifests/seq-<last+1>.json upward, in order,
#       one at a time, stopping on the first failure (retried next cycle).
#
#   What this script does NOT pretend it can do on shared hosting:
#     * It cannot manage services (no systemd) or edit firewall/sshd.
#     * It can only ACTIVATE a brand-new plugin if wp-cli is present;
#       otherwise it installs the files and reports ACTIVATION_PENDING.
#       (Updating an already-active plugin needs no activation.)
#
#   Modes:  --selftest        HMAC + backup/rollback unit tests (temp dirs only)
#           --status-selftest  writes a seq-999 status object you can read
#           (no flag)          one normal cron cycle
#
#   Exit (per-cycle is best-effort; cron reruns every few minutes):
#     0 ok / up-to-date      10 config       20 fetch
#     30 SIGNATURE FAIL      40 sha/precondition
#     50 apply-failed-rolled-back     55 verify-failed-rolled-back
# =====================================================================
set -u
umask 077

# ------------------------------------------------------------------ #
#  interpreter pick for JSON: php is guaranteed on DreamHost; if php is
#  somehow absent we fall back to python3 (php stays primary). The whole
#  parser surface is funnelled through these helpers so both paths agree.
# ------------------------------------------------------------------ #
PHPBIN=""
for _c in php php8.3 php8.2 php8.1 php8.0 php7.4; do command -v "$_c" >/dev/null 2>&1 && { PHPBIN="$_c"; break; }; done
PYBIN=""
for _c in python3 python; do command -v "$_c" >/dev/null 2>&1 && { PYBIN="$_c"; break; }; done

# jval <file> <dotted-path>     -> prints scalar (empty if missing)
#   paths used: seq | mode | modules.<slug>.<field>
jval(){
  local f="$1" path="$2"
  if [ -n "$PHPBIN" ]; then
    "$PHPBIN" -r '$d=json_decode(file_get_contents($argv[1]),true);
      if($d===null){fwrite(STDERR,"JSON parse error\n");exit(7);}
      $v=$d; foreach(explode(".",$argv[2]) as $k){ if(is_array($v)&&array_key_exists($k,$v)){$v=$v[$k];}else{$v=null;break;} }
      if(is_bool($v)){echo $v?"true":"false";}elseif($v===null){echo "";}elseif(is_array($v)){echo json_encode($v);}else{echo $v;}' \
      "$f" "$path" 2>>"${LOG:-/dev/stderr}"
  else
    "$PYBIN" - "$f" "$path" 2>>"${LOG:-/dev/stderr}" <<'PY'
import json,sys
d=json.load(open(sys.argv[1])); v=d
for k in sys.argv[2].split("."):
    if isinstance(v,dict) and k in v: v=v[k]
    else: v=None; break
if isinstance(v,bool): print("true" if v else "false")
elif v is None: print("",end="")
elif isinstance(v,(dict,list)): print(json.dumps(v))
else: print(v)
PY
  fi
}
# jmodkeys <file>  -> module keys, one per line
jmodkeys(){
  local f="$1"
  if [ -n "$PHPBIN" ]; then
    "$PHPBIN" -r '$d=json_decode(file_get_contents($argv[1]),true);
      foreach(array_keys($d["modules"]??[]) as $k){echo $k."\n";}' "$f" 2>>"${LOG:-/dev/stderr}"
  else
    "$PYBIN" - "$f" 2>>"${LOG:-/dev/stderr}" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]))
for k in (d.get("modules") or {}): print(k)
PY
  fi
}
# json_emit <status> <seq> <node> <detail> <wpcli>  -> one-line status JSON
json_emit(){
  if [ -n "$PHPBIN" ]; then
    "$PHPBIN" -r 'echo json_encode(["node"=>$argv[3],"seq"=>intval($argv[2]),"status"=>$argv[1],
      "detail"=>$argv[4],"agent"=>"dreamhost-v1","wpcli"=>$argv[5],"ts"=>time(),"ts_iso"=>gmdate("c")],JSON_UNESCAPED_SLASHES);' \
      "$1" "$2" "$3" "$4" "$5"
  else
    "$PYBIN" - "$1" "$2" "$3" "$4" "$5" <<'PY'
import json,sys,time,datetime
print(json.dumps({"node":sys.argv[3],"seq":int(sys.argv[2]),"status":sys.argv[1],
 "detail":sys.argv[4],"agent":"dreamhost-v1","wpcli":sys.argv[5],"ts":int(time.time()),
 "ts_iso":datetime.datetime.now(datetime.timezone.utc).isoformat()},separators=(",",":")))
PY
  fi
}

# ------------------------------------------------------------------ #
#  SELF-TESTS (touch only temp dirs / no network, no real plugin dir)
# ------------------------------------------------------------------ #
selftest_hmac(){
  local d sec got want rc=0
  d="$(mktemp -d "${TMPDIR:-/tmp}/tnc-st.XXXXXX")"
  sec="topsecret-demo-key"
  printf '{"seq":1,"mode":"diagnose"}' > "$d/m.json"
  want="$(openssl dgst -sha256 -hmac "$sec" -hex "$d/m.json" | sed -E 's/^.*= *//')"
  got="$(openssl dgst -sha256 -hmac "$sec" -hex "$d/m.json" | sed -E 's/^.*= *//')"
  [ "$got" = "$want" ] || { echo "  hmac: match FAIL"; rc=1; }
  # tamper -> must differ
  printf '{"seq":2,"mode":"apply"}' > "$d/m.json"
  got="$(openssl dgst -sha256 -hmac "$sec" -hex "$d/m.json" | sed -E 's/^.*= *//')"
  [ "$got" != "$want" ] || { echo "  hmac: tamper-not-detected FAIL"; rc=1; }
  rm -rf "$d"
  [ $rc -eq 0 ] && echo "hmac selftest: PASS" || echo "hmac selftest: FAIL"
  return $rc
}
selftest_rollback(){
  # simulate: good plugin dir -> bad swap -> restore from backup
  local SB rc=0
  SB="$(mktemp -d "${TMPDIR:-/tmp}/tnc-rb.XXXXXX")"
  mkdir -p "$SB/plugins/demo" "$SB/backup" "$SB/stage/demo"
  echo "GOOD-v1" > "$SB/plugins/demo/main.php"
  # backup
  tar -C "$SB/plugins" -czf "$SB/backup/demo.tgz" demo || { echo "  backup FAIL"; rc=1; }
  # bad new payload + a simulated failure -> roll back
  echo "BROKEN-v2" > "$SB/stage/demo/main.php"; echo stray > "$SB/stage/demo/extra.bad"
  mv "$SB/plugins/demo" "$SB/plugins/demo.old"
  mv "$SB/stage/demo"   "$SB/plugins/demo"
  # "verify" fails -> rollback: remove bad, restore .old
  rm -rf "$SB/plugins/demo" && mv "$SB/plugins/demo.old" "$SB/plugins/demo"
  [ "$(cat "$SB/plugins/demo/main.php")" = "GOOD-v1" ] || { echo "  rollback content FAIL"; rc=1; }
  [ -e "$SB/plugins/demo/extra.bad" ] && { echo "  stray survived FAIL"; rc=1; }
  # and prove the tarball restore path works too
  rm -rf "$SB/plugins/demo"; tar -C "$SB/plugins" -xzf "$SB/backup/demo.tgz"
  [ "$(cat "$SB/plugins/demo/main.php")" = "GOOD-v1" ] || { echo "  tar-restore FAIL"; rc=1; }
  rm -rf "$SB"
  [ $rc -eq 0 ] && echo "rollback selftest: PASS" || echo "rollback selftest: FAIL"
  return $rc
}
if [ "${1:-}" = "--selftest" ]; then
  command -v openssl >/dev/null 2>&1 || { echo "FATAL: openssl not found"; exit 10; }
  [ -n "$PHPBIN" ] || [ -n "$PYBIN" ] || { echo "FATAL: neither php nor python3 found (need one to parse manifests)"; exit 10; }
  echo "parser: ${PHPBIN:-$PYBIN}"
  r=0; selftest_hmac || r=1; selftest_rollback || r=1
  [ $r -eq 0 ] && echo "ALL SELFTESTS PASS" || echo "SELFTESTS FAILED"
  exit $r
fi

# ===================================================================== #
#  CONFIG
# ===================================================================== #
CONF="${TNC_AUTOPULL_CONF:-$HOME/.tnc-deploy/autopull.env}"
[ -f "$CONF" ] || { echo "FATAL(10): missing config $CONF (run install_dreamhost_pipe.sh)" >&2; exit 10; }
# shellcheck disable=SC1090
. "$CONF"

LOG="${TNC_AUTOPULL_LOG:-$HOME/.tnc-deploy/logs/autopull.log}"
STATE_DIR="${TNC_AUTOPULL_STATE:-$HOME/.tnc-deploy/state}"
BACKUP_ROOT="${TNC_BACKUP_ROOT:-$HOME/.tnc-deploy/backups}"
KEEP_BACKUPS="${TNC_KEEP_BACKUPS:-7}"
MAX_DRAIN="${TNC_MAX_DRAIN:-10}"
LOCK_DIR="${TNC_LOCK:-$HOME/.tnc-deploy/.lock}"
SECRET_FILE="${TNC_SECRET_FILE:-$HOME/.tnc-deploy/secret}"
mkdir -p "$(dirname "$LOG")" "$STATE_DIR" "$BACKUP_ROOT" 2>/dev/null || true
WORK="$(mktemp -d "${TMPDIR:-/tmp}/tnc-pull.XXXXXX")"; trap 'rm -rf "$WORK"; rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT
log(){ printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$LOG" >&2; }

# required interpreters
command -v openssl >/dev/null 2>&1 || { echo "FATAL(10): openssl missing" >&2; exit 10; }
command -v curl    >/dev/null 2>&1 || { echo "FATAL(10): curl missing"    >&2; exit 10; }
command -v unzip   >/dev/null 2>&1 || { echo "FATAL(10): unzip missing"   >&2; exit 10; }
[ -n "$PHPBIN" ] || [ -n "$PYBIN" ] || { echo "FATAL(10): no php/python3 (manifest parser)" >&2; exit 10; }

# required config
: "${BRAIN_RAW_BASE:?BRAIN_RAW_BASE not set in $CONF}"     # e.g. https://raw.githubusercontent.com/cloudwalker171/atlas-manifests/main/dreamhost
: "${WP_CONTENT:?WP_CONTENT not set in $CONF}"             # absolute path to wp-content
PLUGINS_DIR="${PLUGINS_DIR:-$WP_CONTENT/plugins}"
WP_ROOT="${WP_ROOT:-$(dirname "$WP_CONTENT")}"
NODE_ID="${NODE_ID:-dreamhost}"
# status-back (public-but-unguessable URL on the same site; no extra creds)
STATUS_DIR="${STATUS_DIR:-$WP_CONTENT/uploads/tnc-deploy/status/${STATUS_TOKEN:-unset}}"
STATUS_URL_BASE="${STATUS_URL_BASE:-}"                     # e.g. https://chat.lionclickmedia.com/wp-content/uploads/tnc-deploy/status/<token>
# optional GitHub status push (mirrors v3); leave unset to use public-URL only
STATUS_API_BASE="${STATUS_API_BASE:-https://api.github.com}"
STATUS_BRANCH="${STATUS_BRANCH:-main}"
# fetch auth for PRIVATE repos (optional). If set, fetch via Contents API instead of raw.
FETCH_TOKEN="${FETCH_TOKEN:-}"

[ -f "$SECRET_FILE" ] || { echo "FATAL(10): deploy secret file missing: $SECRET_FILE" >&2; exit 10; }
DEPLOY_SECRET="$(tr -d ' \t\r\n' < "$SECRET_FILE")"
[ -n "$DEPLOY_SECRET" ] || { echo "FATAL(10): deploy secret empty" >&2; exit 10; }

SEQ=""   # current seq, used by report()

# ------------------------------------------------------------------ #
#  single-flight lock (mkdir is atomic on shared hosting)
# ------------------------------------------------------------------ #
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  # stale lock guard: if older than 30 min, steal it
  if [ -n "$(find "$LOCK_DIR" -maxdepth 0 -mmin +30 2>/dev/null)" ]; then
    log "stale lock >30m, stealing"; rmdir "$LOCK_DIR" 2>/dev/null || true; mkdir "$LOCK_DIR" 2>/dev/null || { log "lock busy, exit"; exit 0; }
  else
    log "another run holds the lock, exit"; exit 0
  fi
fi

# ------------------------------------------------------------------ #
#  status-back
# ------------------------------------------------------------------ #
WPCLI=""; command -v wp >/dev/null 2>&1 && WPCLI="$(command -v wp)"
build_status(){ # <status> <detail>
  json_emit "$1" "${SEQ:-0}" "$NODE_ID" "$2" "${WPCLI:-none}"
}
status_local(){ # <status> <detail>  -> public file (always works on shared hosting)
  local body; body="$(build_status "$1" "$2")"
  printf '%s\n' "$body" > "$STATE_DIR/last_status.json" 2>/dev/null || true
  [ -n "${STATUS_TOKEN:-}" ] || return 0
  mkdir -p "$STATUS_DIR" 2>/dev/null || return 0
  # block directory listing; the token segment is the obscurity, files are world-readable
  [ -f "$STATUS_DIR/.htaccess" ] || printf 'Options -Indexes\n' > "$STATUS_DIR/.htaccess" 2>/dev/null || true
  printf '%s\n' "$body" > "$STATUS_DIR/seq-${SEQ:-0}-$1.json" 2>/dev/null || true
  printf '%s\n' "$body" > "$STATUS_DIR/last-seen.json"        2>/dev/null || true
  chmod 644 "$STATUS_DIR"/*.json 2>/dev/null || true
}
status_github(){ # optional: push to repo via Contents API (needs STATUS_TOKEN_GH + STATUS_REPO)
  [ -n "${STATUS_TOKEN_GH:-}" ] && [ -n "${STATUS_REPO:-}" ] || return 0
  local path="status/${NODE_ID}/seq-${SEQ:-0}-$1.json" body b64 sha payload code
  body="$(build_status "$1" "$2")"; b64="$(printf '%s' "$body" | base64 | tr -d '\n')"
  sha="$(curl -fsS --max-time 15 -H "Authorization: Bearer $STATUS_TOKEN_GH" -H "Accept: application/vnd.github+json" \
        -H "User-Agent: tnc-dreamhost-autopull" "$STATUS_API_BASE/repos/$STATUS_REPO/contents/$path?ref=$STATUS_BRANCH" 2>/dev/null \
        | tr ',' '\n' | grep -m1 '"sha"' | sed -E 's/.*"sha"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/')"
  local msg="status $NODE_ID seq=${SEQ:-0} $1"
  if [ -n "$PHPBIN" ]; then
    if [ -n "$sha" ]; then
      payload="$("$PHPBIN" -r 'echo json_encode(["message"=>$argv[1],"content"=>$argv[2],"branch"=>$argv[3],"sha"=>$argv[4]]);' "$msg" "$b64" "$STATUS_BRANCH" "$sha")"
    else
      payload="$("$PHPBIN" -r 'echo json_encode(["message"=>$argv[1],"content"=>$argv[2],"branch"=>$argv[3]]);' "$msg" "$b64" "$STATUS_BRANCH")"
    fi
  else
    payload="$("$PYBIN" -c 'import json,sys
o={"message":sys.argv[1],"content":sys.argv[2],"branch":sys.argv[3]}
if len(sys.argv)>4 and sys.argv[4]: o["sha"]=sys.argv[4]
print(json.dumps(o))' "$msg" "$b64" "$STATUS_BRANCH" "$sha")"
  fi
  code="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 20 -X PUT \
     -H "Authorization: Bearer $STATUS_TOKEN_GH" -H "Accept: application/vnd.github+json" \
     -H "User-Agent: tnc-dreamhost-autopull" "$STATUS_API_BASE/repos/$STATUS_REPO/contents/$path" --data-binary "$payload" 2>/dev/null)"
  printf '%s' "$code" | grep -q '^2' && log "status pushed to repo ($code)" || log "status repo push http=$code (non-fatal)"
}
report(){ status_local "$1" "$2"; status_github "$1" "$2"; }
die(){ local code="$1"; shift; log "FATAL($code): $*"; report error "exit=$code $*"; exit "$code"; }

if [ "${1:-}" = "--status-selftest" ]; then
  SEQ=999; report ok "status-selftest from node=$NODE_ID"
  log "status-selftest written. local=$STATE_DIR/last_status.json public=${STATUS_URL_BASE:-<STATUS_URL_BASE unset>}/last-seen.json"
  exit 0
fi

log "start node=$NODE_ID php=$PHPBIN wpcli=${WPCLI:-none} brain=$BRAIN_RAW_BASE plugins=$PLUGINS_DIR"

# ------------------------------------------------------------------ #
#  fetch helpers
# ------------------------------------------------------------------ #
fetch(){ # <repo-relative-path> <out-file>  (raw, or Contents API if FETCH_TOKEN)
  local rel="$1" out="$2"
  if [ -n "$FETCH_TOKEN" ] && [ -n "${STATUS_REPO:-}" ]; then
    # private repo: Contents API returns base64; decode
    local sub="${BRAIN_RAW_BASE##*/main/}"   # path under repo root, e.g. dreamhost
    curl -fsS --max-time 30 -H "Authorization: Bearer $FETCH_TOKEN" -H "Accept: application/vnd.github.raw" \
      -H "User-Agent: tnc-dreamhost-autopull" "$STATUS_API_BASE/repos/$STATUS_REPO/contents/$sub/$rel?ref=$STATUS_BRANCH" -o "$out" 2>/dev/null
  else
    curl -fsS --max-time 30 "$BRAIN_RAW_BASE/$rel" -o "$out" 2>/dev/null
  fi
}
fetch_url(){ # absolute URL (package may be hosted anywhere) <url> <out>
  curl -fsS --max-time 120 "$1" -o "$2" 2>/dev/null
}

sha256_of(){ if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
             else shasum -a 256 "$1" | cut -d' ' -f1; fi; }
hmac(){ openssl dgst -sha256 -hmac "$DEPLOY_SECRET" -hex "$1" | sed -E 's/^.*= *//'; }

prune_backups(){ ls -1dt "$BACKUP_ROOT"/*/ 2>/dev/null | tail -n +$((KEEP_BACKUPS+1)) | xargs -r rm -rf; }

# ------------------------------------------------------------------ #
#  apply one module (download -> sha-pin -> backup -> swap -> verify -> rollback)
#  args: <slug> <package-url> <sha256> <install_path> <activate> <snapdir> <mode>
#  returns 0 ok / 40 sha / 50 apply-fail / 55 verify-fail
# ------------------------------------------------------------------ #
apply_module(){
  local slug="$1" url="$2" want_sha="$3" inst="$4" activate="$5" SNAP="$6" mode="$7"
  # derive a safe target dir under PLUGINS_DIR from the install_path basename
  local base; base="$(basename "$inst")"
  case "$base" in ''|.|..|*/*) log "  module $slug: bad install_path '$inst'"; return 50;; esac
  local target="$PLUGINS_DIR/$base"
  log "  module=$slug target=$target activate=$activate"

  # 1. download package
  local zip="$WORK/$slug.zip"
  fetch_url "$url" "$zip" || { log "  download FAIL: $url"; return 50; }

  # 2. sha256 PIN (fail-closed)
  local got_sha; got_sha="$(sha256_of "$zip")"
  if [ "$got_sha" != "$want_sha" ]; then
    log "  SHA MISMATCH $slug got=${got_sha:0:12}.. want=${want_sha:0:12}.. -> refuse"; return 40
  fi
  log "  sha256 pinned OK (${got_sha:0:12}..)"

  # 3. extract to staging + sanity (must contain the slug dir with a main php)
  local stage="$WORK/stage-$slug"; mkdir -p "$stage"
  unzip -q -o "$zip" -d "$stage" || { log "  unzip FAIL"; return 50; }
  local src="$stage/$base"
  [ -d "$src" ] || src="$(find "$stage" -maxdepth 1 -mindepth 1 -type d | head -1)"
  [ -d "$src" ] || { log "  extracted payload has no plugin dir"; return 50; }
  # plugin main file: <base>.php with a WP plugin header, OR any php with 'Plugin Name:'
  local mainok=""
  if grep -rqsI "Plugin Name:" "$src" 2>/dev/null; then mainok="yes"; fi
  [ -n "$mainok" ] || log "  WARN: no 'Plugin Name:' header found in payload (continuing)"

  if [ "$mode" = "diagnose" ]; then
    log "  diagnose: would install $slug -> $target (sha ok, payload sane). NO changes made."
    return 0
  fi

  # 4. BACKUP existing install (restore point)
  mkdir -p "$SNAP"
  if [ -d "$target" ]; then
    tar -C "$PLUGINS_DIR" -czf "$SNAP/$base.tgz" "$base" 2>/dev/null || { log "  backup FAIL"; return 50; }
    log "  backed up current $base -> $SNAP/$base.tgz"
  else
    : > "$SNAP/$base.NEW"   # marker: this was a fresh install (rollback = remove)
    log "  no existing $base (fresh install)"
  fi

  # 5. ATOMIC-ISH swap
  local aside="$PLUGINS_DIR/.$base.old.$$"
  if [ -d "$target" ]; then mv "$target" "$aside" || { log "  mv-aside FAIL"; return 50; }; fi
  if ! cp -a "$src" "$target"; then
    log "  install FAIL, restoring"
    rm -rf "$target"
    [ -d "$aside" ] && mv "$aside" "$target"
    return 50
  fi

  # 6. VERIFY (php -l on every php file; plugin header present)
  local verify_fail=""
  if [ -n "$PHPBIN" ]; then
    while IFS= read -r f; do
      "$PHPBIN" -l "$f" >/dev/null 2>>"$LOG" || { verify_fail="lint:$f"; break; }
    done < <(find "$target" -type f -name '*.php')
  fi
  if [ -z "$verify_fail" ] && [ -n "$mainok" ] && ! grep -rqsI "Plugin Name:" "$target" 2>/dev/null; then
    verify_fail="missing-plugin-header"
  fi
  if [ -n "$verify_fail" ]; then
    log "  VERIFY FAILED ($verify_fail) -> ROLLBACK"
    rm -rf "$target"
    if [ -d "$aside" ]; then mv "$aside" "$target"
    elif [ -f "$SNAP/$base.tgz" ]; then tar -C "$PLUGINS_DIR" -xzf "$SNAP/$base.tgz"; fi
    return 55
  fi
  rm -rf "$aside" 2>/dev/null || true
  log "  verify OK (php -l clean, header present)"

  # 7. ACTIVATION (only meaningful for a NEW plugin; updates stay active)
  if [ "$activate" = "true" ]; then
    if [ -n "$WPCLI" ]; then
      if "$WPCLI" --path="$WP_ROOT" plugin is-active "$base" >/dev/null 2>&1; then
        log "  $base already active"
      elif "$WPCLI" --path="$WP_ROOT" plugin activate "$base" >/dev/null 2>>"$LOG"; then
        log "  activated $base via wp-cli"
      else
        log "  wp-cli activate FAILED (files installed; activate once in wp-admin) -> ACTIVATION_PENDING"
      fi
      "$WPCLI" --path="$WP_ROOT" cache flush >/dev/null 2>&1 || true
    else
      log "  wp-cli not present: $base FILES INSTALLED but ACTIVATION_PENDING (one wp-admin click, or install wp-cli)"
    fi
  fi
  return 0
}

# ------------------------------------------------------------------ #
#  process one manifest (verify sig -> parse -> per-module apply)
#  returns 0 applied | 2 diagnosed | 3 up-to-date | 30 sig | 40 sha | 50/55 fail
# ------------------------------------------------------------------ #
process_current(){
  local expect_seq="$1"
  # FAIL-CLOSED HMAC verify BEFORE parse
  local got want
  got="$(hmac "$WORK/manifest.json")"
  want="$(tr -d ' \t\r\n' < "$WORK/manifest.sig")"
  if [ "$got" != "$want" ]; then
    log "SIGNATURE MISMATCH (got=${got:0:12}.. want=${want:0:12}..) -> REFUSE"
    SEQ="$expect_seq"; report rejected "signature mismatch seq=$expect_seq"; return 30
  fi
  log "signature OK (HMAC-SHA256 verified)"

  SEQ="$(jval "$WORK/manifest.json" 'seq')"
  [ -n "$SEQ" ] || { log "manifest missing seq (parse error)"; report error "missing seq"; return 50; }
  local mode; mode="$(jval "$WORK/manifest.json" 'mode')"; [ -n "$mode" ] || mode="apply"
  local last; last="$(cat "$STATE_DIR/last_seq" 2>/dev/null || echo 0)"
  if [ "$SEQ" -le "$last" ] 2>/dev/null; then log "seq $SEQ <= last $last -> skip"; return 3; fi
  log "manifest seq=$SEQ mode=$mode"

  # restore point dir for this seq
  local TS SNAP; TS="$(date -u +%Y%m%dT%H%M%SZ)"; SNAP="$BACKUP_ROOT/$SEQ-$TS"

  local rc=0 applied_any=0
  while IFS= read -r slug; do
    [ -n "$slug" ] || continue
    local url sha inst act
    url="$(jval "$WORK/manifest.json"  "modules.$slug.package")"
    sha="$(jval "$WORK/manifest.json"  "modules.$slug.sha256")"
    inst="$(jval "$WORK/manifest.json" "modules.$slug.install_path")"
    act="$(jval "$WORK/manifest.json"  "modules.$slug.activate")"; [ "$act" = "true" ] || act="false"
    [ -n "$url" ] && [ -n "$sha" ] && [ -n "$inst" ] || { log "  module $slug missing package/sha/install_path"; report error "module $slug malformed seq=$SEQ"; rc=50; break; }
    apply_module "$slug" "$url" "$sha" "$inst" "$act" "$SNAP" "$mode"; local mrc=$?
    if [ "$mrc" -ne 0 ]; then rc=$mrc; break; fi
    applied_any=1
  done < <(jmodkeys "$WORK/manifest.json")

  if [ "$rc" -ne 0 ]; then
    case "$rc" in
      40) report rejected "sha mismatch seq=$SEQ";;
      55) report failed   "verify failed, rolled back seq=$SEQ";;
      *)  report failed   "apply failed, rolled back seq=$SEQ (rc=$rc)";;
    esac
    return "$rc"
  fi
  if [ "$mode" = "diagnose" ]; then
    log "MODE=diagnose -> no changes; seq NOT advanced so re-runs stay diagnostic"
    report diagnosed "seq=$SEQ (diagnose only, nothing changed)"
    return 2
  fi
  echo "$SEQ" > "$STATE_DIR/last_seq"
  prune_backups
  log "APPLY COMPLETE seq=$SEQ (healthy)"
  report ok "applied seq=$SEQ"
  return 0
}

# ===================================================================== #
#  MAIN: heartbeat + drain queue (manifests/seq-N.json), in order
# ===================================================================== #
SEQ=""; report heartbeat "alive last_seq=$(cat "$STATE_DIR/last_seq" 2>/dev/null || echo 0)"
applied=0; n=0
while [ "$n" -lt "$MAX_DRAIN" ]; do
  n=$((n+1))
  last="$(cat "$STATE_DIR/last_seq" 2>/dev/null || echo 0)"
  next=$((last+1))
  rm -f "$WORK/manifest.json" "$WORK/manifest.sig"
  if fetch "manifests/seq-$next.json" "$WORK/manifest.json" \
     && fetch "manifests/seq-$next.json.sig" "$WORK/manifest.sig"; then
    log "queue: fetched seq-$next"
  else
    [ "$applied" -gt 0 ] && log "queue drained ($applied applied)" || log "nothing new (no seq-$next / offline) -> wait next cron"
    break
  fi
  process_current "$next"; rc=$?
  case "$rc" in
    0) applied=$((applied+1)); continue;;
    2) break;;
    3) break;;
    *) log "stop drain (rc=$rc); retry next cron"; break;;
  esac
done
SEQ=""; report heartbeat "cycle end last_seq=$(cat "$STATE_DIR/last_seq" 2>/dev/null || echo 0) applied=$applied"
log "end node=$NODE_ID applied=$applied"
exit 0
