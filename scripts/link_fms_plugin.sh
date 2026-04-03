#!/usr/bin/env bash
# Symlink (or copy) PI_CockpitdecksFMS.py from this repo into X-Plane's PythonPlugins.
# Usage: ./scripts/link_fms_plugin.sh [XPLANE_ROOT]
#   or:  XPLANE_ROOT=/path/to/X-Plane\ 12 ./scripts/link_fms_plugin.sh
#   or:  ./scripts/link_fms_plugin.sh --copy /path/to/X-Plane\ 12  (use copy instead of symlink)
#
# After running: Reload Python plugins in X-Plane (Plugins → XPPython3 → Reload)
# or restart X-Plane. Delete *.pyc in PythonPlugins if reload doesn't pick up changes.

set -e

USE_COPY=false
[[ "$1" == "--copy" ]] && { USE_COPY=true; shift; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLUGIN_SRC="${REPO_ROOT}/cockpitdecks_xp/resources/xppython3-plugins/PI_CockpitdecksFMS.py"
XPLANE_ROOT="${XPLANE_ROOT:-$1}"
PLUGINS_DIR="${XPLANE_ROOT}/Resources/plugins/PythonPlugins"
PLUGIN_DST="${PLUGINS_DIR}/PI_CockpitdecksFMS.py"

if [[ -z "$XPLANE_ROOT" ]]; then
  echo "Usage: $0 <XPLANE_ROOT>"
  echo "   or: XPLANE_ROOT=/path/to/X-Plane\\ 12 $0"
  echo ""
  echo "Example: $0 \"$HOME/X-Plane 12\""
  exit 1
fi

if [[ ! -f "$PLUGIN_SRC" ]]; then
  echo "ERROR: Plugin source not found: $PLUGIN_SRC"
  exit 1
fi

if [[ ! -d "$PLUGINS_DIR" ]]; then
  echo "ERROR: X-Plane PythonPlugins dir not found: $PLUGINS_DIR"
  echo "       Is XPLANE_ROOT correct? $XPLANE_ROOT"
  exit 1
fi

if [[ -L "$PLUGIN_DST" ]]; then
  rm "$PLUGIN_DST"
elif [[ -f "$PLUGIN_DST" ]]; then
  echo "Removing existing plugin (backup to ${PLUGIN_DST}.bak)"
  mv "$PLUGIN_DST" "${PLUGIN_DST}.bak"
fi

# Remove bytecode cache so X-Plane reloads the .py (avoids stale .pyc)
rm -f "${PLUGINS_DIR}"/PI_CockpitdecksFMSBrowser.cpython-*.pyc 2>/dev/null || true

if [[ "$USE_COPY" == true ]]; then
  cp "$PLUGIN_SRC" "$PLUGIN_DST"
  echo "Copied: $PLUGIN_SRC -> $PLUGIN_DST"
else
  ln -s "$PLUGIN_SRC" "$PLUGIN_DST"
  echo "Linked: $PLUGIN_DST -> $PLUGIN_SRC"
fi
RELEASE=$(grep -E '^\s*RELEASE\s*=' "$PLUGIN_SRC" | head -1 | sed 's/.*"\([^"]*\)".*/\1/')
echo "Plugin version: $RELEASE"
echo "Restart Python plugins (Plugins → XPPython3 → Reload plugins) or X-Plane to use the development plugin."
