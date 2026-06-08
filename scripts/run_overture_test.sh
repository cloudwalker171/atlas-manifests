#!/usr/bin/env bash
#
# run_overture_test.sh -- end-to-end NO-AUTH smoke test for the Overture importer.
#
# Deliberately does NOT use `set -e`: every step is run explicitly, its real
# error is echoed, and we decide whether to abort. Silent aborts hide which step
# died, which is exactly what we don't want in a one-shot smoke test.
#
# Steps:
#   1. Auto-discover the latest Overture release version  (anonymous S3, no token)
#   2. Pick + download ONE places parquet part            (anonymous S3, no token)
#   3. Print the parquet schema                           (DuckDB DESCRIBE)
#   4. Run overture_pg_import.py with a 10,000-row cap
#   5. Print atlas.business / atlas.source_record counts  (sudo -u postgres psql)
#   6. Clear PASS/FAIL verdict
#
# No Hugging Face token, no AWS credentials -- everything uses --no-sign-request.

VENV=/opt/atlas/venv
AWS="$VENV/bin/aws"
PY="$VENV/bin/python"
IMPORTER=/opt/atlas/importers/overture_pg_import.py
DATA_DIR=/opt/atlas/data/overture_test
BUCKET=overturemaps-us-west-2
PGDB=tuanichat_atlas
LIMIT=10000

step()  { echo ""; echo "==== $* ===="; }
ok()    { echo "  [ok] $*"; }
warn()  { echo "  [!!] $*"; }
die()   { echo ""; echo " FAIL: $*"; exit 1; }

echo "======================================================================"
echo " Overture Maps -> atlas Postgres : NO-AUTH import smoke test"
echo " $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "======================================================================"

# --- sanity: required tools present ---------------------------------------- #
step "0. Checking prerequisites"
[ -x "$AWS" ]      || die "aws CLI not found/executable at $AWS"
[ -x "$PY" ]       || die "python not found/executable at $PY"
[ -f "$IMPORTER" ] || die "importer not found at $IMPORTER (scp it there first)"
ok "aws:      $AWS"
ok "python:   $PY"
ok "importer: $IMPORTER"
mkdir -p "$DATA_DIR" || die "could not create $DATA_DIR"
ok "data dir: $DATA_DIR"

# --- 1. discover latest release version ------------------------------------ #
step "1. Discovering latest Overture release (anonymous S3)"
echo "  \$ $AWS s3 ls --no-sign-request s3://$BUCKET/release/"
RELEASE_LS="$($AWS s3 ls --no-sign-request "s3://$BUCKET/release/" 2>&1)"
RC=$?
echo "$RELEASE_LS"
[ $RC -eq 0 ] || die "could not list s3://$BUCKET/release/ (rc=$RC). Output above is the real error."

LATEST="$(printf '%s\n' "$RELEASE_LS" | awk '/PRE/ {print $2}' | sed 's#/##' | sort -V | tail -1)"
[ -n "$LATEST" ] || die "could not parse a release version from the listing above."
ok "latest release = $LATEST"

PREFIX="release/$LATEST/theme=places/type=place/"

# --- 2. pick + download ONE parquet part ----------------------------------- #
step "2. Listing place parquet parts under $PREFIX"
echo "  \$ $AWS s3 ls --no-sign-request s3://$BUCKET/$PREFIX"
PARTS_LS="$($AWS s3 ls --no-sign-request "s3://$BUCKET/$PREFIX" 2>&1)"
RC=$?
echo "$PARTS_LS" | head -n 5
echo "  ... ($(printf '%s\n' "$PARTS_LS" | grep -c '\.parquet') parquet parts total)"
[ $RC -eq 0 ] || die "could not list s3://$BUCKET/$PREFIX (rc=$RC). Output above is the real error."

PART="$(printf '%s\n' "$PARTS_LS" | awk '{print $NF}' | grep '\.parquet$' | head -1)"
[ -n "$PART" ] || die "no .parquet part found under $PREFIX"
ok "selected part = $PART"

LOCAL="$DATA_DIR/$PART"
step "2b. Downloading ONE part to $LOCAL"
echo "  \$ $AWS s3 cp --no-sign-request s3://$BUCKET/$PREFIX$PART $LOCAL"
$AWS s3 cp --no-sign-request "s3://$BUCKET/$PREFIX$PART" "$LOCAL"
RC=$?
[ $RC -eq 0 ] || die "download failed (rc=$RC)."
[ -s "$LOCAL" ] || die "downloaded file is empty: $LOCAL"
ok "downloaded $(du -h "$LOCAL" | cut -f1) -> $LOCAL"

# --- 3. print parquet schema ----------------------------------------------- #
step "3. Parquet schema (DuckDB DESCRIBE)"
$PY - "$LOCAL" <<'PYEOF'
import sys, duckdb
path = sys.argv[1]
try:
    rel = duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{path}')")
    print(rel)
except Exception as e:
    print(f"  [!!] could not describe parquet: {e}")
    sys.exit(2)
PYEOF
RC=$?
[ $RC -eq 0 ] || warn "schema print failed (rc=$RC) -- continuing to import anyway."

# --- 4. run the importer with a row cap ------------------------------------ #
step "4. Importing (cap = $LIMIT rows) via $IMPORTER"
echo "  \$ OVERTURE_LIMIT=$LIMIT OVERTURE_PARQUET=$LOCAL $PY $IMPORTER"
OVERTURE_LIMIT=$LIMIT OVERTURE_PARQUET="$LOCAL" "$PY" "$IMPORTER"
RC=$?
[ $RC -eq 0 ] || die "importer exited non-zero (rc=$RC). See its error output above."
ok "importer finished cleanly"

# --- 5. read-back counts via postgres -------------------------------------- #
step "5. Read-back counts (sudo -u postgres psql -d $PGDB)"
BCOUNT="$(sudo -u postgres psql -d "$PGDB" -tAc "SELECT count(*) FROM atlas.business;" 2>/dev/null | tr -d '[:space:]')"
RC1=$?
OCOUNT="$(sudo -u postgres psql -d "$PGDB" -tAc "SELECT count(*) FROM atlas.source_record WHERE source='overture';" 2>/dev/null | tr -d '[:space:]')"
RC2=$?
echo "  business_total = ${BCOUNT:-?}"
echo "  overture_source_records = ${OCOUNT:-?}"

# Drop a local counts file. The patched deployer report() (which holds the GitHub
# status token in its OWN shell) reads this and pushes it to the repo. The secret
# is NEVER handed to this child script.
STATE_DIR_LOCAL="/var/lib/atlas/autopull"
mkdir -p "$STATE_DIR_LOCAL" 2>/dev/null || true
printf '{"lane":"overture","business_total":%s,"overture_source_records":%s,"cap":%s,"ts":%s}\n' \
  "${BCOUNT:-0}" "${OCOUNT:-0}" "$LIMIT" "$(date +%s)" > "$STATE_DIR_LOCAL/last_counts.json"
echo "  wrote $STATE_DIR_LOCAL/last_counts.json"

# --- 6. verdict ------------------------------------------------------------ #
echo ""
echo "======================================================================"
if [ "${RC1:-1}" -eq 0 ] && [ "${RC2:-1}" -eq 0 ] && [ -n "${BCOUNT:-}" ]; then
  echo "PASS: discovered $LATEST, downloaded $PART, imported (cap $LIMIT); business=$BCOUNT overture=$OCOUNT"
else
  die "import ran but read-back count queries failed -- check psql access above."
fi
echo "======================================================================"
