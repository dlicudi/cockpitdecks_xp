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
