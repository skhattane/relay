#!/usr/bin/env bash
# One step: registers this clone as a plugin marketplace and installs the relay plugin from it (the server
# and the /relay command together). Run it from the clone; run it again after a `git pull` — it is safe.
#
#   ./install.sh            install, or refresh an install
#   ./install.sh remove     uninstall the plugin and forget the marketplace (rooms and seats are kept)
#
# It changes the CLI's own settings, so the person runs it, not a session. $RELAY_CLI names the CLI.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd -P)
CLI="${RELAY_CLI:-claude}"
case "${1:-}" in
  remove)
    "$CLI" plugin uninstall relay@relay || true
    "$CLI" plugin marketplace remove relay || true
    echo "removed. Your rooms and seats stay in ~/.config/relay."
    ;;
  "")
    "$CLI" plugin marketplace add "$HERE" 2>/dev/null || "$CLI" plugin marketplace update relay
    "$CLI" plugin install relay@relay 2>/dev/null || "$CLI" plugin update relay@relay || true
    # What the earlier, launcher-based install left behind: the plugin replaces it.
    for old in "$HOME/.local/bin/relay-lead" "$HOME/.config/relay/leads.json"; do
      [ -e "$old" ] && rm -- "$old"
    done
    [ -d "$HOME/.config/relay/mod" ] && rm -r -- "$HOME/.config/relay/mod"
    echo "installed. Start the CLI as usual; in a session: /relay help"
    ;;
  *) sed -n '2,8p' "$0"; exit 2 ;;
esac
