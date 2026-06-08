#!/usr/bin/env bash
# ============================================================================
# Re-sign a manifest with your REAL deploy secret, producing <manifest>.sig
# (HMAC-SHA256 hex). Run this LOCALLY before you commit a manifest to the repo.
# The SAME secret must be in ~/.tnc-deploy/secret on the DreamHost box.
#
#   usage:  bash sign-manifest.sh path/to/seq-2.json
#           TNC_DEPLOY_SECRET=... bash sign-manifest.sh path/to/seq-2.json
#
# The .sig is the HMAC of the EXACT bytes of the manifest file, so sign the
# file you will actually commit (don't reformat it afterward).
# ============================================================================
set -euo pipefail
M="${1:?usage: bash sign-manifest.sh <manifest.json>}"
[ -f "$M" ] || { echo "no such file: $M"; exit 1; }
if [ -z "${TNC_DEPLOY_SECRET:-}" ]; then
  printf "Deploy secret (hidden): " >&2; read -rs TNC_DEPLOY_SECRET; echo >&2
fi
[ -n "${TNC_DEPLOY_SECRET:-}" ] || { echo "empty secret"; exit 1; }
SIG="$(openssl dgst -sha256 -hmac "$TNC_DEPLOY_SECRET" -hex "$M" | sed -E 's/^.*= *//')"
printf '%s\n' "$SIG" > "$M.sig"
echo "signed: $M"
echo "  -> $M.sig  ($SIG)"
echo "Commit BOTH $M and $M.sig to the repo."
