#!/usr/bin/env bash
# ============================================================================
# InterServer second-node install -- SINGLE-PASTE fallback (no scp needed).
# Paste this whole block into a ROOT shell in DirectAdmin's terminal or the
# VNC/serial console. It writes /etc/atlas/atlas.secret.env inline (pre-filled)
# and runs the full node install. CentOS7/OpenVZ-safe.
#
# >>> EDIT ONE LINE FIRST: put the atlas DB password between the quotes. <<<
# ============================================================================
ATLAS_DB_PW="${ATLAS_DB_PW:-${P:-}}"   # <-- from env (ATLAS_DB_PW=...) passed over ssh, or P

set -Eeuo pipefail
REPO_RAW_BASE='https://raw.githubusercontent.com/cloudwalker171/atlas-manifests/main/bootstrap/atlas-autopull.v3.sh'
AUTOPULL_SHA256='1171a33bdb5c5b533ee2321b18a90b2276e48a263ea273107e44849e295dfd22'
NODE_ID=interserver
ATLAS_HOME=/opt/atlas ; VENV=$ATLAS_HOME/venv ; ETC=/etc/atlas
PGHOST=168.119.226.254 ; PGPORT=5432 ; PGDATABASE=tuanichat_atlas ; PGUSER=atlas

[ "$(id -u)" -eq 0 ] || { echo "run as root"; exit 1; }
[ -n "$ATLAS_DB_PW" ] || { echo "no password set -- run the read -rsp ... P command first"; exit 1; }

install -d -m 700 "$ETC"; mkdir -p "$ATLAS_HOME"
umask 077
cat > "$ETC/atlas.secret.env" <<EOF
PGPASSWORD=${ATLAS_DB_PW}
REPO_RAW_BASE=${REPO_RAW_BASE}
AUTOPULL_SHA256=${AUTOPULL_SHA256}
EOF
chmod 600 "$ETC/atlas.secret.env"

# python3 (never bare python -- that 2.7 parser bug is what broke Hetzner)
command -v python3 >/dev/null 2>&1 || yum -y install python3 python3-pip
command -v curl    >/dev/null 2>&1 || yum -y install curl ca-certificates

# venv + psycopg2-binary (no compiler; upgrade pip so manylinux wheel resolves)
if [ ! -x "$VENV/bin/python" ]; then
  python3 -m venv "$VENV" 2>/dev/null || { python3 -m venv --without-pip "$VENV"; \
    curl -fsSL https://bootstrap.pypa.io/pip/3.6/get-pip.py -o "$ATLAS_HOME/get-pip.py"; \
    "$VENV/bin/python" "$ATLAS_HOME/get-pip.py"; }
fi
VPY="$VENV/bin/python"
"$VPY" -m pip install --upgrade 'pip>=21.0' setuptools wheel
"$VPY" -m pip install 'psycopg2-binary>=2.9'

# fetch + sha-verify + run the v3 puller as NODE_ID=interserver
BOOT="$ATLAS_HOME/atlas-autopull.v3.sh"
curl -fsSL "$REPO_RAW_BASE" -o "$BOOT.incoming"
GOT="$(sha256sum "$BOOT.incoming" | awk '{print $1}')"
[ "$GOT" = "$AUTOPULL_SHA256" ] || { echo "PIN MISMATCH got=$GOT"; rm -f "$BOOT.incoming"; exit 1; }
mv -f "$BOOT.incoming" "$BOOT"; chmod 0755 "$BOOT"

DSN="postgresql://${PGUSER}:$("$VPY" -c "import urllib.parse,os;print(urllib.parse.quote(os.environ['P'],safe=''))" P="$ATLAS_DB_PW" 2>/dev/null || echo "$ATLAS_DB_PW")@${PGHOST}:${PGPORT}/${PGDATABASE}?application_name=atlas-${NODE_ID}"
export ATLAS_PG_DSN="$DSN" ATLAS_PYTHON="$VPY" NODE_ID="$NODE_ID" ATLAS_HOME="$ATLAS_HOME"

# PG selftest (proves firewall + creds + py3 driver)
"$VPY" - <<PY
import os,psycopg2
c=psycopg2.connect(os.environ["ATLAS_PG_DSN"],connect_timeout=10);cur=c.cursor()
cur.execute("select current_database(),current_user,now()");print("PG_OK",cur.fetchone())
PY

# first puller pass (it selftests + auto-reverts itself)
"$BOOT" --once || echo "puller pass returned non-zero (may have reverted; check log)"

# keep the puller running for the seq-N queue (every 2 min)
( crontab -l 2>/dev/null | grep -v atlas-autopull ; \
  echo "*/2 * * * * ATLAS_PG_DSN='$DSN' ATLAS_PYTHON='$VPY' NODE_ID=$NODE_ID ATLAS_HOME=$ATLAS_HOME $BOOT --once >>$ATLAS_HOME/pull.log 2>&1" ) | crontab -
echo "=== InterServer node bootstrap done. Check status/interserver/last-seen.json in the repo in ~2-4 min. ==="
