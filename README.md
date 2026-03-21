# Cockpitdecks Extension for X-Plane Flight Simulator software

This extension allows Cockpitdecks to connect to Laminar X-Plane Flight Simulator.

## Development: symlink FMS plugin

To use the development version of `PI_CockpitdecksFMSBrowser.py` (instead of the pip-installed copy), symlink it into X-Plane's PythonPlugins folder:

```bash
./scripts/link_fms_plugin.sh "/path/to/X-Plane 12"
# or
XPLANE_ROOT="$HOME/X-Plane 12" ./scripts/link_fms_plugin.sh
```

This links `Resources/plugins/PythonPlugins/PI_CockpitdecksFMSBrowser.py` to the repo copy. Restart Python plugins (Plugins → XPPython3 → Reload plugins) or X-Plane after linking.

**Edit the repo file only** — `cockpitdecks_xp/resources/xppython3-plugins/PI_CockpitdecksFMSBrowser.py`. Do not save or copy a new file over the path inside X-Plane: that can **replace the symlink with a regular file** and detach your install from the repo. Saving through the symlink is fine (it updates the repo target).
