#!/usr/bin/env bash
# ============================================================================
# ONE-TIME installer for the TNC DreamHost auto-pull pipe. Run it ON the
# DreamHost shell account over SSH. No root, no systemd required.
#
#   It will:
#     1. create ~/.tnc-deploy/ (config, secret, state, backups, logs) chmod 700
#     2. store your deploy secret OUTSIDE the web root (~/.tnc-deploy/secret, 600)
#     3. fetch the puller from the repo, sha256-PIN it, run --selftest
#     4. write the config env (repo, wp-content path, status URL, node id)
#     5. install a crontab line that runs the puller every CRON_MIN minutes
#
#   Usage (positional or env):
#     bash install_dreamhost_pipe.sh \
#        --wp-content /home/USER/chat.lionclickmedia.com/wp-content \
#        --repo cloudwalker171/atlas-manifests \
#        [--branch main] [--node dreamhost] [--cron-min 4] \
#        [--site-url https://chat.lionclickmedia.com] \
#        [--pinned-sha <sha256 of the puller>] \
#        [--status-token <random>]   # default: generated for you
#
#   The deploy secret is read interactively (hidden) unless TNC_DEPLOY_SECRET is set.
# ============================================================================
set -euo pipefail

REPO="cloudwalker171/atlas-manifests"
BRANCH="main"
NODE="dreamhost"
CRON_MIN="4"
WP_CONTENT=""
SITE_URL=""
PINNED_SHA=""
STATUS_TOKEN=""
SUBDIR="dreamhost"

while [ $# -gt 0 ]; do
  case "$1" in
    --wp-content) WP_CONTENT="$2"; shift 2;;
    --repo)       REPO="$2"; shift 2;;
    --branch)     BRANCH="$2"; shift 2;;
    --node)       NODE="$2"; shift 2;;
    --cron-min)   CRON_MIN="$2"; shift 2;;
    --site-url)   SITE_URL="$2"; shift 2;;
    --pinned-sha) PINNED_SHA="$2"; shift 2;;
    --status-token) STATUS_TOKEN="$2"; shift 2;;
    --subdir)     SUBDIR="$2"; shift 2;;
    *) echo "unknown arg: $1"; exit 1;;
  esac
done

[ -n "$WP_CONTENT" ] || { echo "ERROR: --wp-content <abs path to wp-content> is required"; exit 1; }
[ -d "$WP_CONTENT" ] || { echo "ERROR: wp-content not found at: $WP_CONTENT"; exit 1; }
[ -d "$WP_CONTENT/plugins" ] || { echo "ERROR: $WP_CONTENT/plugins missing — is this the real wp-content?"; exit 1; }

# preflight: required tools on this shared host
for t in bash curl openssl unzip tar; do command -v "$t" >/dev/null 2>&1 || { echo "ERROR: '$t' not available on this host"; exit 1; }; done
PHPBIN=""; for c in php php8.2 php8.1 php8.0 php7.4; do command -v "$c" >/dev/null 2>&1 && { PHPBIN="$c"; break; }; done
[ -n "$PHPBIN" ] || { echo "ERROR: php CLI not found (needed to parse manifests)"; exit 1; }
WPCLI="$(command -v wp 2>/dev/null || true)"

BASE="$HOME/.tnc-deploy"
mkdir -p "$BASE/state" "$BASE/backups" "$BASE/logs"; chmod 700 "$BASE"
RAW_BASE="https://raw.githubusercontent.com/$REPO/$BRANCH/$SUBDIR"

# ---- deploy secret -> outside web root ----
if [ -z "${TNC_DEPLOY_SECRET:-}" ]; then
  printf "Paste your deploy secret (input hidden): " >&2
  read -rs TNC_DEPLOY_SECRET; echo >&2
fi
[ -n "${TNC_DEPLOY_SECRET:-}" ] || { echo "ERROR: empty deploy secret"; exit 1; }
printf '%s' "$TNC_DEPLOY_SECRET" > "$BASE/secret"; chmod 600 "$BASE/secret"
echo ">> secret stored at $BASE/secret (600, outside web root)"

# ---- status token (unguessable path segment for the public status URL) ----
if [ -z "$STATUS_TOKEN" ]; then
  STATUS_TOKEN="$(openssl rand -hex 16)"
fi
if [ -z "$SITE_URL" ]; then
  echo ">> NOTE: --site-url not given; status files will still be written to disk,"
  echo "         but I can't print the public URL. Pass --site-url next time to get it."
fi
STATUS_DIR="$WP_CONTENT/uploads/tnc-deploy/status/$STATUS_TOKEN"
STATUS_URL_BASE=""
[ -n "$SITE_URL" ] && STATUS_URL_BASE="${SITE_URL%/}/wp-content/uploads/tnc-deploy/status/$STATUS_TOKEN"
mkdir -p "$STATUS_DIR"; printf 'Options -Indexes\n' > "$STATUS_DIR/.htaccess" 2>/dev/null || true

# ---- fetch + sha-pin the puller ----
PULLER="$BASE/tnc-dreamhost-autopull.sh"
TMP="$(mktemp)"
echo ">> fetching puller from $RAW_BASE/bootstrap/tnc-dreamhost-autopull.sh"
curl -fsS --max-time 30 "$RAW_BASE/bootstrap/tnc-dreamhost-autopull.sh" -o "$TMP"
GOT="$(sha256sum "$TMP" 2>/dev/null | cut -d' ' -f1 || shasum -a 256 "$TMP" | cut -d' ' -f1)"
if [ -n "$PINNED_SHA" ]; then
  [ "$GOT" = "$PINNED_SHA" ] || { echo "SHA MISMATCH got=$GOT want=$PINNED_SHA — aborting, nothing installed"; rm -f "$TMP"; exit 1; }
  echo "   puller sha OK ($GOT)"
else
  echo "   puller sha = $GOT  (no --pinned-sha given; recommend pinning next time)"
fi
bash -n "$TMP"
echo ">> running selftest before install"
bash "$TMP" --selftest || { echo "SELFTEST FAILED — not installing"; rm -f "$TMP"; exit 1; }
install -m700 "$TMP" "$PULLER"; rm -f "$TMP"

# ---- write config env ----
ENVF="$BASE/autopull.env"
WP_ROOT="$(dirname "$WP_CONTENT")"
cat > "$ENVF" <<EOF
# TNC DreamHost auto-pull config  (generated $(date -u +%Y-%m-%dT%H:%M:%SZ))
BRAIN_RAW_BASE="$RAW_BASE"
WP_CONTENT="$WP_CONTENT"
PLUGINS_DIR="$WP_CONTENT/plugins"
WP_ROOT="$WP_ROOT"
NODE_ID="$NODE"
STATUS_TOKEN="$STATUS_TOKEN"
STATUS_DIR="$STATUS_DIR"
STATUS_URL_BASE="$STATUS_URL_BASE"
STATUS_REPO="$REPO"
STATUS_BRANCH="$BRANCH"
TNC_KEEP_BACKUPS="7"
# --- optional: push status objects into the repo too (fine-grained PAT, Contents:RW) ---
# STATUS_TOKEN_GH="github_pat_..."
# --- optional: fetch from a PRIVATE repo (fine-grained PAT, Contents:Read) ---
# FETCH_TOKEN="github_pat_..."
EOF
chmod 600 "$ENVF"
echo ">> wrote config $ENVF"

# ---- install crontab line (idempotent) ----
CRON_LINE="*/$CRON_MIN * * * * $PULLER >> $BASE/logs/cron.log 2>&1"
TMPC="$(mktemp)"
crontab -l 2>/dev/null | grep -v "tnc-dreamhost-autopull.sh" > "$TMPC" || true
echo "$CRON_LINE" >> "$TMPC"
crontab "$TMPC"; rm -f "$TMPC"
echo ">> crontab installed: $CRON_LINE"

echo "============================================================"
echo " INSTALLED. node=$NODE  every ${CRON_MIN} min."
echo " wp-cli: ${WPCLI:-NOT FOUND (new-plugin activation will be PENDING)}"
echo " puller : $PULLER"
echo " config : $ENVF   secret: $BASE/secret (600)"
if [ -n "$STATUS_URL_BASE" ]; then
  echo " status (read me to verify):"
  echo "   $STATUS_URL_BASE/last-seen.json"
else
  echo " status files on disk: $STATUS_DIR/  (pass --site-url to get the public URL)"
fi
echo
echo " Smoke test now (writes a seq-999 status object, changes nothing):"
echo "   $PULLER --status-selftest"
echo " First real run (or just wait for cron):"
echo "   $PULLER"
echo "============================================================"
