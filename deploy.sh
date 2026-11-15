#!/usr/bin/env bash
# relay, the hosted half, on a Cloudflare account.
#
#   ./deploy.sh deploy     uploads worker/ with the admin key's HASH, prints the relay's address
#   ./deploy.sh status     which programs are deployed on the account
#
# The admin key stays on this machine (relay admin-key creates it and prints only its sha256); the Worker
# gets the hash as the plain var ADMIN.
#
# The API token is read from $CF_WORKERS_TOKEN_FILE (default ~/.config/relay/cloudflare.token, mode
# 600), reaches the deploy tool through its environment and is never printed. It needs one permission on
# the account: Workers Scripts:Edit.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
API="https://api.cloudflare.com/client/v4"
TOKEN=$(tr -d '[:space:]' < "${CF_WORKERS_TOKEN_FILE:-$HOME/.config/relay/cloudflare.token}")
cf() { printf 'header = "Authorization: Bearer %s"\n' "$TOKEN" | curl -sS -K - -X GET "$API$1"; }
ACCOUNT=$(cf "/accounts" | python3 -c '
import json, sys
r = json.load(sys.stdin).get("result") or []
sys.exit("cloudflare: the token reaches %d accounts, expected one" % len(r)) if len(r) != 1 else print(r[0]["id"])')

case "${1:-}" in
  deploy)
    ADMIN=$("$HERE/plugin/bin/relay" admin-key)
    case "$ADMIN" in *[!0-9a-f]*|"") echo "deploy: relay admin-key did not print a hash" >&2; exit 1 ;; esac
    cd "$HERE/worker"
    CLOUDFLARE_API_TOKEN="$TOKEN" CLOUDFLARE_ACCOUNT_ID="$ACCOUNT" WRANGLER_SEND_METRICS=false \
      npx --yes wrangler@4 deploy --var "ADMIN:$ADMIN" 2>&1 | sed -e "s/$ACCOUNT/<account>/g"
    ;;
  status)
    cf "/accounts/$ACCOUNT/workers/scripts" | python3 -c '
import json, sys
j = json.load(sys.stdin)
print("deployed programs:", ", ".join(s["id"] for s in j.get("result") or []) or "none") if j.get("success") else sys.exit("cloudflare: %s" % j.get("errors"))'
    ;;
  *) sed -n '2,12p' "$0"; exit 2 ;;
esac
