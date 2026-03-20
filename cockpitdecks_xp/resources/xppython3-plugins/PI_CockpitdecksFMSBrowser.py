import math
import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from XPPython3 import xp


@dataclass
class FlightPlanInfo:
    filename: str
    full_path: str
    display_name: str
    dep: str
    dest: str
    cycle: str
    dep_runway: str = ""
    dest_runway: str = ""
    sid: str = ""
    star: str = ""
    waypoint_count: int = 0
    total_distance_nm: float = 0.0
    waypoint_list: str = ""
    max_altitude: int = 0
    # File modification time (epoch seconds) for “newest first” sort
    file_mtime: float = 0.0


@dataclass
class FlightPlanEntry:
    entry_type: int
    ident: str
    altitude: int
    lat: float
    lon: float


class PythonInterface:
    NAME = "Cockpitdecks FMS Browser"
    SIG = "xppython3.cockpitdecksfmsbrowser"
    DESC = (
        "Browse Output/FMS plans and load selected plan into default FMS. "
        "LOAD page (fms_load): list_row_1..3 + list_scroll_up/down move by page (plans 1–3, 4–6, …), "
        "same 3-row paging as fms_legs on fms_fpl. "
        "Encoder commands order (Cockpitdecks): [0]=CCW→list_scroll_up, [1]=CW→list_scroll_down — same as fms_fpl E0."
    )
    RELEASE = "2.0.10"

    DREF_PREFIX = "cockpitdecks/fms_browser"
    CMD_PREFIX = "cockpitdecks/fms_browser"

    LEGS_DREF_PREFIX = "cockpitdecks/fms_legs"
    LEGS_CMD_PREFIX = "cockpitdecks/fms_legs"
    LEGS_VISIBLE_ROWS = 3

    # Plan file browser: 3 visible rows (Loupedeck Live 4×3), same paging idea as fms_legs
    PLAN_LIST_VISIBLE_ROWS = 3

    ACTION_NONE = 0
    ACTION_PREVIOUS = 1
    ACTION_NEXT = 2
    ACTION_REFRESH = 3
    ACTION_LOAD = 4
    ACTION_OPEN_FPL = 5

    FMS_TYPE_TO_NAV = {
        1: xp.Nav_Airport,
        2: xp.Nav_NDB,
        3: xp.Nav_VOR,
        11: xp.Nav_Fix,
        28: xp.Nav_LatLon,
    }

    def __init__(self):
        self.enabled = False
        self.trace = True
        self.info = f"{self.NAME} (rel. {self.RELEASE})"

        self.plans: List[FlightPlanInfo] = []
        # Selected plan for load (0..n-1), or -1 = none (after paging, like legs_selected=-1)
        self.index = -1
        self.loaded = 0
        self.loaded_filename = ""
        self.loaded_index = 0
        self.loaded_sid = ""
        self.loaded_star = ""
        self.loaded_distance_nm = 0.0
        self.last_status = "INIT"
        self.last_error = ""

        self.map_mode = 0  # 0 = G1000, 1 = GCU478
        self.map_mode_names = ["G1000", "GCU478"]
        self.map_range_cmds = {
            0: ("sim/GPS/g1000n1_range_down", "sim/GPS/g1000n1_range_up"),
            1: ("sim/GPS/gcu478/range_down", "sim/GPS/gcu478/range_up"),
        }
        self.map_cmd_refs = {}

        # LEGS scrollable list state
        self.legs_selected = -1    # 0-based FMS entry index; -1 = none selected
        self.legs_window_start = 0  # 0-based first visible row

        # Plan list window (Output/FMS plans): first visible row index into self.plans
        self.browser_list_window_start = 0
        # 0 = sort by filename A–Z; 1 = sort by file mtime descending (newest first)
        self.plan_sort_mode = 0

        self.accessors = []
        self.commands: Dict[str, Dict[str, object]] = {}
        self.fpl_cmd = None

        self.string_values: Dict[str, str] = {
            "plan_name": "No flight plans",
            "plan_departure": "----",
            "plan_destination": "----",
            "plan_cycle": "",
            "plan_filename": "",
            "plan_path": "",
            "plan_dep_runway": "",
            "plan_dest_runway": "",
            "plan_sid": "",
            "plan_star": "",
            "plan_waypoints": "",
            "loaded_sid": "",
            "loaded_star": "",
            "status": "INIT",
            "last_error": "",
        }
        self.int_values: Dict[str, int] = {
            "index": 0,
            "count": 0,
            "loaded": 0,
            "action": 0,
            "last_action": 0,
            "action_ack": 0,
            "plan_waypoint_count": 0,
            "plan_max_altitude": 0,
        }
        self.float_values: Dict[str, float] = {
            "plan_distance_nm": 0.0,
            "loaded_distance_nm": 0.0,
        }

        # FPL catalog: rescan disk when the set of .fms filenames changes (debounced on dataref reads).
        self._fms_names_snapshot: tuple = ()
        self._last_plans_dir_exists: Optional[bool] = None  # set after each full _refresh_plan_list
        self._last_plan_dir_check_monotonic: float = 0.0
        self.PLAN_DIR_POLL_MIN_INTERVAL_SEC = 0.25

    def _log(self, *parts):
        if self.trace:
            print(self.info, *parts)

    def XPluginStart(self):
        self._log("XPluginStart", f"LEGS_VISIBLE_ROWS={self.LEGS_VISIBLE_ROWS}")

        _plan_sync = True
        self._register_string_dref("plan_name", sync_plans=_plan_sync)
        self._register_string_dref("plan_departure", sync_plans=_plan_sync)
        self._register_string_dref("plan_destination", sync_plans=_plan_sync)
        self._register_string_dref("plan_cycle", sync_plans=_plan_sync)
        self._register_string_dref("plan_filename", sync_plans=_plan_sync)
        self._register_string_dref("plan_path", sync_plans=_plan_sync)
        self._register_string_dref("plan_dep_runway", sync_plans=_plan_sync)
        self._register_string_dref("plan_dest_runway", sync_plans=_plan_sync)
        self._register_string_dref("plan_sid", sync_plans=_plan_sync)
        self._register_string_dref("plan_star", sync_plans=_plan_sync)
        self._register_string_dref("plan_waypoints", sync_plans=_plan_sync)
        self._register_string_dref("status")
        self._register_string_dref("last_error")
        self._register_string_dref("loaded_filename")
        self._register_string_dref("loaded_sid")
        self._register_string_dref("loaded_star")
        self._register_string_dref("map_mode")

        self._register_int_dref("index", sync_plans=True)
        self._register_int_dref("count", sync_plans=True)
        self._register_int_dref("loaded")
        self._register_int_dref("loaded_index")
        self._register_writable_action_dref("action")
        self._register_int_dref("last_action")
        self._register_int_dref("action_ack")
        self._register_int_dref("plan_waypoint_count", sync_plans=True)
        self._register_int_dref("plan_max_altitude", sync_plans=True)

        self._register_float_dref("plan_distance_nm", sync_plans=True)
        self._register_float_dref("loaded_distance_nm")

        self._register_live_fms_drefs()

        self._create_command("previous", "Select previous FMS plan", self._cmd_previous)
        self._create_command("next", "Select next FMS plan", self._cmd_next)
        self._create_command("refresh", "Refresh FMS plan list", self._cmd_refresh)
        self._create_command("load", "Load selected FMS plan", self._cmd_load)
        self._create_command("open_fpl", "Open G1000 FPL page", self._cmd_open_fpl)
        self._create_command("wp_next", "Display next FMS waypoint", self._cmd_wp_next)
        self._create_command("wp_previous", "Display previous FMS waypoint", self._cmd_wp_previous)
        self._create_command("wp_direct", "Direct-to displayed FMS waypoint", self._cmd_wp_direct)
        self._create_command("clear_fms_entry", "Clear displayed FMS entry", self._cmd_clear_fms_entry)
        self._create_command("map_range_down", "Map range zoom in", self._cmd_map_range_down)
        self._create_command("map_range_up", "Map range zoom out", self._cmd_map_range_up)
        self._create_command("map_toggle", "Toggle map range target", self._cmd_map_toggle)

        self._register_plan_list_window_drefs()
        self._create_plan_list_window_commands()

        self.fpl_cmd = xp.findCommand("sim/GPS/g1000n1_fpl")
        self._log("findCommand sim/GPS/g1000n1_fpl ->", self.fpl_cmd)

        for mode, (down_cmd, up_cmd) in self.map_range_cmds.items():
            self.map_cmd_refs[mode] = (xp.findCommand(down_cmd), xp.findCommand(up_cmd))
            self._log("findCommand map range", mode, down_cmd, "->", self.map_cmd_refs[mode][0],
                      up_cmd, "->", self.map_cmd_refs[mode][1])

        self._register_legs_drefs()
        self._create_legs_commands()

        self._refresh_plan_list()
        return self.NAME, self.SIG, self.DESC

    def XPluginStop(self):
        for key, meta in self.commands.items():
            try:
                xp.unregisterCommandHandler(meta["ref"], meta["fun"], 1, None)
                self._log("Unregistered command handler", key)
            except Exception as exc:
                self._log("Failed to unregister command handler", key, exc)
        self.commands = {}

        for accessor in self.accessors:
            try:
                xp.unregisterDataAccessor(accessor)
            except Exception as exc:
                self._log("Failed to unregister data accessor", accessor, exc)
        self.accessors = []

        self._log("XPluginStop")
        return None

    def XPluginEnable(self):
        self.enabled = True
        self._log("XPluginEnable")
        return 1

    def XPluginDisable(self):
        self.enabled = False
        self._log("XPluginDisable")
        return None

    def XPluginReceiveMessage(self, inFromWho, inMessage, inParam):
        if inMessage == xp.MSG_PLANE_LOADED and inParam == 0:
            self._log("User aircraft loaded; refreshing plan list")
            self._refresh_plan_list()
            self._publish_state()
        return None

    def _register_string_dref(self, suffix: str, sync_plans: bool = False):
        name = f"{self.DREF_PREFIX}/{suffix}"

        def read_data(refCon, values, offset, count):
            if sync_plans:
                self._ensure_plans_fresh_for_read()
            text = self.string_values.get(suffix, "")
            data = list(text.encode("utf-8"))
            if values is None:
                return len(data)
            if offset >= len(data):
                return 0
            values.extend(data[offset: offset + count])
            return min(count, len(data) - offset)

        accessor = xp.registerDataAccessor(
            name,
            dataType=xp.Type_Data,
            writable=0,
            readData=read_data,
            readRefCon=suffix,
        )
        self.accessors.append(accessor)
        self._log("Registered string dataref", name, "->", accessor)

    def _register_int_dref(self, suffix: str, sync_plans: bool = False):
        name = f"{self.DREF_PREFIX}/{suffix}"

        def read_int(refCon):
            if sync_plans:
                self._ensure_plans_fresh_for_read()
            return int(self.int_values.get(suffix, 0))

        accessor = xp.registerDataAccessor(
            name,
            dataType=xp.Type_Int,
            writable=0,
            readInt=read_int,
            readRefCon=suffix,
        )
        self.accessors.append(accessor)
        self._log("Registered int dataref", name, "->", accessor)

    def _register_float_dref(self, suffix: str, sync_plans: bool = False):
        name = f"{self.DREF_PREFIX}/{suffix}"

        def read_float(refCon):
            if sync_plans:
                self._ensure_plans_fresh_for_read()
            return float(self.float_values.get(suffix, 0.0))

        accessor = xp.registerDataAccessor(
            name,
            dataType=xp.Type_Float,
            writable=0,
            readFloat=read_float,
            readRefCon=suffix,
        )
        self.accessors.append(accessor)
        self._log("Registered float dataref", name, "->", accessor)

    def _register_writable_action_dref(self, suffix: str):
        name = f"{self.DREF_PREFIX}/{suffix}"

        def read_int(refCon):
            return int(self.int_values.get(suffix, 0))

        def write_int(refCon, value):
            try:
                action = int(value)
            except Exception:
                action = 0
            self._log("Action dataref write", name, "=", action)
            self.int_values[suffix] = action
            self._perform_action(action)
            self.int_values[suffix] = 0

        accessor = xp.registerDataAccessor(
            name,
            dataType=xp.Type_Int,
            writable=1,
            readInt=read_int,
            writeInt=write_int,
            readRefCon=suffix,
            writeRefCon=suffix,
        )
        self.accessors.append(accessor)
        self._log("Registered writable action dataref", name, "->", accessor)

    def _create_command(self, suffix: str, desc: str, callback, prefix: str = None):
        name = f"{prefix or self.CMD_PREFIX}/{suffix}"
        cmd_ref = xp.createCommand(name, desc)
        self._log("createCommand", name, "->", cmd_ref)
        if not cmd_ref:
            self._log("ERROR: command creation failed for", name)
            return

        def handler(commandRef, phase, refcon):
            if phase == xp.CommandBegin:
                self._log("Command begin", name)
                callback()
            return 1

        xp.registerCommandHandler(cmd_ref, handler, 1, None)
        self._log("registerCommandHandler", name, "-> OK")
        self.commands[suffix] = {"ref": cmd_ref, "fun": handler}

    def _set_status(self, text: str, error: str = ""):
        self.last_status = text
        self.last_error = error
        self.string_values["status"] = text
        self.string_values["last_error"] = error

    def _selected_plan(self) -> Optional[FlightPlanInfo]:
        if not self.plans:
            return None
        if self.index < 0 or self.index >= len(self.plans):
            return None
        return self.plans[self.index]

    def _publish_state(self):
        plan = self._selected_plan()

        self.int_values["count"] = len(self.plans)
        if self.plans and 0 <= self.index < len(self.plans):
            self.int_values["index"] = self.index + 1
        else:
            self.int_values["index"] = 0
        self.int_values["loaded"] = int(self.loaded)

        if plan is None:
            if not self.plans:
                self.string_values["plan_name"] = "No flight plans"
            else:
                self.string_values["plan_name"] = "Select plan"
            self.string_values["plan_departure"] = "----"
            self.string_values["plan_destination"] = "----"
            self.string_values["plan_cycle"] = ""
            self.string_values["plan_filename"] = ""
            self.string_values["plan_path"] = ""
            self.string_values["plan_dep_runway"] = ""
            self.string_values["plan_dest_runway"] = ""
            self.string_values["plan_sid"] = ""
            self.string_values["plan_star"] = ""
            self.string_values["plan_waypoints"] = ""
            self.int_values["plan_waypoint_count"] = 0
            self.int_values["plan_max_altitude"] = 0
            self.float_values["plan_distance_nm"] = 0.0
        else:
            self.string_values["plan_name"] = plan.display_name
            self.string_values["plan_departure"] = plan.dep
            self.string_values["plan_destination"] = plan.dest
            self.string_values["plan_cycle"] = plan.cycle
            self.string_values["plan_filename"] = os.path.splitext(plan.filename)[0]
            self.string_values["plan_path"] = plan.full_path
            self.string_values["plan_dep_runway"] = plan.dep_runway
            self.string_values["plan_dest_runway"] = plan.dest_runway
            self.string_values["plan_sid"] = plan.sid
            self.string_values["plan_star"] = plan.star
            self.string_values["plan_waypoints"] = plan.waypoint_list
            self.int_values["plan_waypoint_count"] = plan.waypoint_count
            self.int_values["plan_max_altitude"] = plan.max_altitude
            self.float_values["plan_distance_nm"] = plan.total_distance_nm

        self.string_values["loaded_filename"] = self.loaded_filename
        self.int_values["loaded_index"] = self.loaded_index
        self.string_values["loaded_sid"] = self.loaded_sid
        self.string_values["loaded_star"] = self.loaded_star
        self.float_values["loaded_distance_nm"] = self.loaded_distance_nm
        self.string_values["map_mode"] = self.map_mode_names[self.map_mode]

        self.string_values["status"] = self.last_status
        self.string_values["last_error"] = self.last_error
        self._log(
            "State",
            f"index={self.int_values['index']}",
            f"count={self.int_values['count']}",
            f"plan={self.string_values['plan_filename']}",
            f"status={self.string_values['status']}",
        )

    def _record_action(self, action: int):
        self.int_values["last_action"] = int(action)
        self.int_values["action_ack"] += 1

    def _perform_action(self, action: int):
        self._record_action(action)
        if action == self.ACTION_PREVIOUS:
            self._cmd_previous()
        elif action == self.ACTION_NEXT:
            self._cmd_next()
        elif action == self.ACTION_REFRESH:
            self._cmd_refresh()
        elif action == self.ACTION_LOAD:
            self._cmd_load()
        elif action == self.ACTION_OPEN_FPL:
            self._cmd_open_fpl()
        else:
            self._log("Ignoring action", action)

    # ── Live FMS state (read from X-Plane SDK on each call) ──

    def _register_live_fms_drefs(self):
        self._register_live_int_dref("fms_entry_count", self._read_fms_entry_count)
        self._register_live_int_dref("fms_active_index", self._read_fms_active_index)
        self._register_live_int_dref("fms_active_altitude", self._read_fms_active_altitude)
        self._register_live_int_dref("fms_displayed_index", self._read_fms_displayed_index)
        self._register_live_int_dref("fms_displayed_altitude", self._read_fms_displayed_altitude)
        self._register_live_string_dref("fms_active_ident", self._read_fms_active_ident)
        self._register_live_string_dref("fms_displayed_ident", self._read_fms_displayed_ident)
        self._register_live_string_dref("fms_first_ident", self._read_fms_first_ident)
        self._register_live_int_dref("fms_first_altitude", self._read_fms_first_altitude)
        self._register_live_string_dref("fms_last_ident", self._read_fms_last_ident)
        self._register_live_int_dref("fms_last_altitude", self._read_fms_last_altitude)

    def _register_live_int_dref(self, suffix: str, read_fn, prefix: str = None):
        name = f"{prefix or self.DREF_PREFIX}/{suffix}"

        def read_int(refCon):
            return read_fn()

        accessor = xp.registerDataAccessor(
            name,
            dataType=xp.Type_Int,
            writable=0,
            readInt=read_int,
            readRefCon=suffix,
        )
        self.accessors.append(accessor)
        self._log("Registered live int dataref", name, "->", accessor)

    def _register_writable_legs_window_start(self, prefix: str = None):
        """Register writable window_start: write 1-based PAGE number (1=rows 1-3, 2=4-6, ...)."""
        p = prefix or self.LEGS_DREF_PREFIX
        name = f"{p}/window_start"

        def read_int(refCon):
            # Return 1-based page number (1, 2, 3...) so encoder-value with step 1 steps by page
            count = self._read_fms_entry_count()
            if count <= 0:
                return 1
            return self.legs_window_start // self.LEGS_VISIBLE_ROWS + 1

        def write_int(refCon, value):
            # Interpret value as 1-based page number: 1=rows 1-3, 2=rows 4-6, etc.
            # This ensures encoder-value with step 1 steps by page, not by row.
            try:
                page = max(1, int(value))
            except (TypeError, ValueError):
                page = 1
            count = self._read_fms_entry_count()
            if count <= 0:
                return
            # Allow partial last page (e.g. 5 waypoints: page 2 shows 4, 5, empty row)
            max_start = max(0, count - 1)
            new_start = (page - 1) * self.LEGS_VISIBLE_ROWS
            self.legs_window_start = max(0, min(max_start, new_start))
            self.legs_selected = -1  # clear selection when paging; user taps to select
            self._log("window_start write: page", page, "-> window", self.legs_window_start)

        accessor = xp.registerDataAccessor(
            name,
            dataType=xp.Type_Int,
            writable=1,
            readInt=read_int,
            writeInt=write_int,
            readRefCon=None,
            writeRefCon=None,
        )
        self.accessors.append(accessor)
        self._log("Registered writable legs window_start dataref", name, "->", accessor)

    def _register_live_string_dref(self, suffix: str, read_fn, prefix: str = None):
        name = f"{prefix or self.DREF_PREFIX}/{suffix}"

        def read_data(refCon, values, offset, count):
            text = read_fn()
            data = list(text.encode("utf-8"))
            if values is None:
                return len(data)
            if offset >= len(data):
                return 0
            values.extend(data[offset: offset + count])
            return min(count, len(data) - offset)

        accessor = xp.registerDataAccessor(
            name,
            dataType=xp.Type_Data,
            writable=0,
            readData=read_data,
            readRefCon=suffix,
        )
        self.accessors.append(accessor)
        self._log("Registered live string dataref", name, "->", accessor)

    def _safe_fms_entry_info(self, index: int):
        try:
            count = xp.countFMSEntries()
            if count <= 0 or index < 0 or index >= count:
                return None
            return xp.getFMSEntryInfo(index)
        except Exception:
            return None

    def _read_fms_entry_count(self) -> int:
        try:
            return xp.countFMSEntries()
        except Exception:
            return 0

    def _read_fms_active_index(self) -> int:
        try:
            return xp.getDestinationFMSEntry() + 1
        except Exception:
            return 0

    def _read_fms_active_ident(self) -> str:
        info = self._safe_fms_entry_info(xp.getDestinationFMSEntry())
        return info.navAidID if info else "----"

    def _read_fms_active_altitude(self) -> int:
        info = self._safe_fms_entry_info(xp.getDestinationFMSEntry())
        return info.altitude if info else 0

    def _read_fms_displayed_index(self) -> int:
        try:
            return xp.getDisplayedFMSEntry() + 1
        except Exception:
            return 0

    def _read_fms_displayed_ident(self) -> str:
        info = self._safe_fms_entry_info(xp.getDisplayedFMSEntry())
        return info.navAidID if info else "----"

    def _read_fms_displayed_altitude(self) -> int:
        info = self._safe_fms_entry_info(xp.getDisplayedFMSEntry())
        return info.altitude if info else 0

    def _read_fms_first_ident(self) -> str:
        try:
            count = xp.countFMSEntries()
            if count <= 0:
                return "----"
            info = self._safe_fms_entry_info(0)
            return info.navAidID if info else "----"
        except Exception:
            return "----"

    def _read_fms_first_altitude(self) -> int:
        try:
            count = xp.countFMSEntries()
            if count <= 0:
                return 0
            info = self._safe_fms_entry_info(0)
            return info.altitude if info else 0
        except Exception:
            return 0

    def _read_fms_last_ident(self) -> str:
        try:
            count = xp.countFMSEntries()
            if count <= 0:
                return "----"
            info = self._safe_fms_entry_info(count - 1)
            return info.navAidID if info else "----"
        except Exception:
            return "----"

    def _read_fms_last_altitude(self) -> int:
        try:
            count = xp.countFMSEntries()
            if count <= 0:
                return 0
            info = self._safe_fms_entry_info(count - 1)
            return info.altitude if info else 0
        except Exception:
            return 0

    # ── Waypoint navigation commands ──

    def _cmd_wp_next(self):
        try:
            count = xp.countFMSEntries()
            if count <= 0:
                return
            current = xp.getDisplayedFMSEntry()
            next_idx = (current + 1) % count
            xp.setDisplayedFMSEntry(next_idx)
            self._log("wp_next: displayed", next_idx)
        except Exception as exc:
            self._log("wp_next error:", exc)

    def _cmd_wp_previous(self):
        try:
            count = xp.countFMSEntries()
            if count <= 0:
                return
            current = xp.getDisplayedFMSEntry()
            prev_idx = (current - 1) % count
            xp.setDisplayedFMSEntry(prev_idx)
            self._log("wp_previous: displayed", prev_idx)
        except Exception as exc:
            self._log("wp_previous error:", exc)

    def _cmd_wp_direct(self):
        try:
            count = xp.countFMSEntries()
            if count <= 0:
                return
            displayed = xp.getDisplayedFMSEntry()
            xp.setDestinationFMSEntry(displayed)
            info = self._safe_fms_entry_info(displayed)
            ident = info.navAidID if info else "?"
            self._log("wp_direct: destination set to", displayed, ident)
        except Exception as exc:
            self._log("wp_direct error:", exc)

    def _cmd_clear_fms_entry(self):
        try:
            count = xp.countFMSEntries()
            if count <= 0:
                return
            displayed = xp.getDisplayedFMSEntry()
            xp.clearFMSEntry(displayed)
            info = self._safe_fms_entry_info(displayed)
            ident = info.navAidID if info else "?"
            self._log("clear_fms_entry: cleared", displayed, "now showing", ident)
        except Exception as exc:
            self._log("clear_fms_entry error:", exc)

    # ── Map range toggle ──

    def _cmd_map_range_down(self):
        refs = self.map_cmd_refs.get(self.map_mode)
        if refs and refs[0]:
            xp.commandOnce(refs[0])
            self._log("map_range_down", self.map_mode_names[self.map_mode])

    def _cmd_map_range_up(self):
        refs = self.map_cmd_refs.get(self.map_mode)
        if refs and refs[1]:
            xp.commandOnce(refs[1])
            self._log("map_range_up", self.map_mode_names[self.map_mode])

    def _cmd_map_toggle(self):
        self.map_mode = 1 - self.map_mode
        self.string_values["map_mode"] = self.map_mode_names[self.map_mode]
        self._log("map_toggle ->", self.map_mode_names[self.map_mode])

    # ── Plan list window (3 rows / page, like fms_legs) ──
    # Pages are 1-3 | 4-6 | 7-9 | … : window_start is always a multiple of 3
    # (0, 3, 6, …). Last page may show fewer than 3 plans; its start is
    # ((n-1)//3)*3 — same rule as fms_legs/window_start writes.

    def _plan_list_max_aligned_window_start(self, n: int) -> int:
        """Max valid window_start for n plans (0-based indices), page-aligned by 3."""
        if n <= 0:
            return 0
        return ((n - 1) // self.PLAN_LIST_VISIBLE_ROWS) * self.PLAN_LIST_VISIBLE_ROWS

    def _plan_list_align_window_start(self, w: int, n: int) -> int:
        """Snap w down to a multiple of 3 and clamp to [0, max_aligned]."""
        max_w = self._plan_list_max_aligned_window_start(n)
        aligned = (max(0, int(w)) // self.PLAN_LIST_VISIBLE_ROWS) * self.PLAN_LIST_VISIBLE_ROWS
        return max(0, min(aligned, max_w))

    def _plan_list_plan_index_for_row(self, row: int) -> int:
        """0-based index into self.plans for visible row (1..3), or -1 if empty slot."""
        self._ensure_plans_fresh_for_read()
        if not self.plans:
            return -1
        idx = self.browser_list_window_start + (row - 1)
        if idx < 0 or idx >= len(self.plans):
            return -1
        return idx

    def _plan_list_read_row_plan_index(self, row: int) -> str:
        pi = self._plan_list_plan_index_for_row(row)
        if pi < 0:
            return ""
        return str(pi + 1)

    def _plan_list_read_row_dep(self, row: int) -> str:
        pi = self._plan_list_plan_index_for_row(row)
        if pi < 0:
            return ""
        d = (self.plans[pi].dep or "").strip()
        if not d or d == "----":
            return ""
        return d

    def _plan_list_read_row_dest(self, row: int) -> str:
        pi = self._plan_list_plan_index_for_row(row)
        if pi < 0:
            return ""
        d = (self.plans[pi].dest or "").strip()
        if not d or d == "----":
            return ""
        return d

    def _plan_list_read_row_route(self, row: int) -> str:
        """DEP ARR for annunciator second segment (single line)."""
        dep = self._plan_list_read_row_dep(row)
        dest = self._plan_list_read_row_dest(row)
        if not dep and not dest:
            return ""
        if dep and dest:
            return f"{dep} {dest}"
        return dep or dest

    def _plan_list_read_row_wpt_count(self, row: int) -> int:
        pi = self._plan_list_plan_index_for_row(row)
        if pi < 0:
            return 0
        return int(self.plans[pi].waypoint_count)

    def _plan_list_read_row_distance_nm(self, row: int) -> float:
        pi = self._plan_list_plan_index_for_row(row)
        if pi < 0:
            return 0.0
        return float(self.plans[pi].total_distance_nm)

    def _plan_list_read_row_is_selected(self, row: int) -> int:
        pi = self._plan_list_plan_index_for_row(row)
        if pi < 0 or self.index < 0:
            return 0
        return 1 if pi == self.index else 0

    def _plan_list_read_row_status(self, row: int) -> str:
        return "SEL" if self._plan_list_read_row_is_selected(row) else ""

    def _plan_list_read_page_indicator(self) -> str:
        self._ensure_plans_fresh_for_read()
        n = len(self.plans)
        if n <= 0:
            return ""
        page = self.browser_list_window_start // self.PLAN_LIST_VISIBLE_ROWS + 1
        total = (n + self.PLAN_LIST_VISIBLE_ROWS - 1) // self.PLAN_LIST_VISIBLE_ROWS
        return f"{page}/{total}"

    def _plan_list_read_selected_over_count(self) -> str:
        self._ensure_plans_fresh_for_read()
        if not self.plans:
            return "0/0"
        if self.index < 0:
            return f"-/{len(self.plans)}"
        return f"{self.index + 1}/{len(self.plans)}"

    def _plan_list_read_snapshot(self) -> str:
        """JSON bundle for encoder dataref (same role as fms_legs/snapshot on fms_fpl E0)."""
        import json
        self._ensure_plans_fresh_for_read()
        n = len(self.plans)
        data = {
            "window_start": self.browser_list_window_start,
            "plan_count": n,
            "selected_plan_index": self.index,
            "sort_mode": self.plan_sort_mode,
            "page": self.browser_list_window_start // self.PLAN_LIST_VISIBLE_ROWS + 1 if n else 0,
            "page_count": (n + self.PLAN_LIST_VISIBLE_ROWS - 1) // self.PLAN_LIST_VISIBLE_ROWS if n else 0,
        }
        for row in range(1, self.PLAN_LIST_VISIBLE_ROWS + 1):
            pi = self._plan_list_plan_index_for_row(row)
            data[f"row_{row}_list_index"] = self._plan_list_read_row_plan_index(row)
            data[f"row_{row}_is_selected"] = int(pi >= 0 and self.index >= 0 and pi == self.index)
        return json.dumps(data, separators=(",", ":"))

    def _register_plan_list_window_drefs(self):
        p = self.DREF_PREFIX
        self._register_live_string_dref("list_page", self._plan_list_read_page_indicator, prefix=p)
        self._register_live_string_dref("list_sel_count", self._plan_list_read_selected_over_count, prefix=p)
        self._register_live_string_dref("list_sort_mode", self._plan_list_read_sort_mode_label, prefix=p)
        self._register_live_string_dref("list_snapshot", self._plan_list_read_snapshot, prefix=p)
        for row in range(1, self.PLAN_LIST_VISIBLE_ROWS + 1):
            self._register_live_string_dref(
                f"list_row_{row}_index", lambda r=row: self._plan_list_read_row_plan_index(r), prefix=p)
            self._register_live_string_dref(
                f"list_row_{row}_dep", lambda r=row: self._plan_list_read_row_dep(r), prefix=p)
            self._register_live_string_dref(
                f"list_row_{row}_dest", lambda r=row: self._plan_list_read_row_dest(r), prefix=p)
            self._register_live_string_dref(
                f"list_row_{row}_route", lambda r=row: self._plan_list_read_row_route(r), prefix=p)
            self._register_live_int_dref(
                f"list_row_{row}_wpt_count", lambda r=row: self._plan_list_read_row_wpt_count(r), prefix=p)
            self._register_live_float_dref(
                f"list_row_{row}_distance_nm", lambda r=row: self._plan_list_read_row_distance_nm(r), prefix=p)
            self._register_live_int_dref(
                f"list_row_{row}_is_selected", lambda r=row: self._plan_list_read_row_is_selected(r), prefix=p)
            self._register_live_string_dref(
                f"list_row_{row}_status", lambda r=row: self._plan_list_read_row_status(r), prefix=p)

    def _register_live_float_dref(self, suffix: str, read_fn, prefix: str = None):
        name = f"{prefix or self.DREF_PREFIX}/{suffix}"

        def read_float(refCon):
            return float(read_fn())

        accessor = xp.registerDataAccessor(
            name,
            dataType=xp.Type_Float,
            writable=0,
            readFloat=read_float,
            readRefCon=suffix,
        )
        self.accessors.append(accessor)
        self._log("Registered live float dataref", name, "->", accessor)

    def _create_plan_list_window_commands(self):
        p = self.CMD_PREFIX
        self._create_command(
            "list_scroll_up", "Scroll plan list up (previous page of 3)", self._cmd_list_scroll_up, prefix=p)
        self._create_command(
            "list_scroll_down", "Scroll plan list down (next page of 3)", self._cmd_list_scroll_down, prefix=p)
        self._create_command(
            "list_select_row_1", "Select plan in list row 1", self._cmd_list_select_row_1, prefix=p)
        self._create_command(
            "list_select_row_2", "Select plan in list row 2", self._cmd_list_select_row_2, prefix=p)
        self._create_command(
            "list_select_row_3", "Select plan in list row 3", self._cmd_list_select_row_3, prefix=p)
        self._create_command(
            "list_toggle_sort",
            "Toggle plan list sort: A-Z vs newest file first",
            self._cmd_list_toggle_sort,
            prefix=p,
        )

    def _plan_list_read_sort_mode_label(self) -> str:
        self._ensure_plans_fresh_for_read()
        return "DATE" if self.plan_sort_mode == 1 else "A-Z"

    def _sort_plans(self):
        """Reorder self.plans in place (does not change selection index)."""
        if not self.plans:
            return
        if self.plan_sort_mode == 0:
            self.plans.sort(key=lambda p: p.filename.lower())
        else:
            self.plans.sort(key=lambda p: (-p.file_mtime, p.filename.lower()))

    def _cmd_list_toggle_sort(self):
        self._ensure_plans_fresh_for_read(force=True)
        if not self.plans:
            self._set_status("EMPTY")
            self._publish_state()
            return
        sel_fn = self.plans[self.index].filename if 0 <= self.index < len(self.plans) else None
        self.plan_sort_mode = 1 - self.plan_sort_mode
        self._sort_plans()
        if sel_fn is not None:
            self.index = next((i for i, p in enumerate(self.plans) if p.filename == sel_fn), -1)
        if self.index >= 0:
            self._plan_list_ensure_index_visible()
        else:
            self.browser_list_window_start = self._plan_list_align_window_start(
                self.browser_list_window_start, len(self.plans))
        self._set_status("READY")
        self._publish_state()

    def _plan_list_ensure_index_visible(self):
        if not self.plans:
            self.browser_list_window_start = 0
            return
        if self.index < 0:
            return
        n = len(self.plans)
        max_w = self._plan_list_max_aligned_window_start(n)
        page_start = (self.index // self.PLAN_LIST_VISIBLE_ROWS) * self.PLAN_LIST_VISIBLE_ROWS
        self.browser_list_window_start = max(0, min(page_start, max_w))

    def _cmd_list_scroll_up(self):
        """Previous page: 1-3, 4-6, 7-9, … (same as fms_legs scroll_up)."""
        self._ensure_plans_fresh_for_read(force=True)
        if not self.plans:
            return
        new_start = max(0, self.browser_list_window_start - self.PLAN_LIST_VISIBLE_ROWS)
        if new_start != self.browser_list_window_start:
            self.browser_list_window_start = new_start
            self.index = -1  # like fms_legs: clear selection when paging
            self._log(
                "list_scroll_up: page",
                self.browser_list_window_start // self.PLAN_LIST_VISIBLE_ROWS + 1,
                "window_start=", self.browser_list_window_start,
            )
            self._set_status("READY")
            self._publish_state()

    def _cmd_list_scroll_down(self):
        """Next page: 1-3, 4-6, 7-9, … partial last page (e.g. 5 plans: row3 empty)."""
        self._ensure_plans_fresh_for_read(force=True)
        if not self.plans:
            return
        n = len(self.plans)
        max_w = self._plan_list_max_aligned_window_start(n)
        next_start = self.browser_list_window_start + self.PLAN_LIST_VISIBLE_ROWS
        new_start = min(next_start, max_w)
        if new_start != self.browser_list_window_start:
            self.browser_list_window_start = new_start
            self.index = -1  # like fms_legs: clear selection when paging
            self._log(
                "list_scroll_down: page",
                self.browser_list_window_start // self.PLAN_LIST_VISIBLE_ROWS + 1,
                "window_start=", self.browser_list_window_start,
            )
            self._set_status("READY")
            self._publish_state()

    def _cmd_list_select_row_1(self):
        self._cmd_list_select_row(1)

    def _cmd_list_select_row_2(self):
        self._cmd_list_select_row(2)

    def _cmd_list_select_row_3(self):
        self._cmd_list_select_row(3)

    def _cmd_list_select_row(self, row: int):
        """Tap row to select plan, or tap again to clear (same toggle idea as fms_legs)."""
        self._ensure_plans_fresh_for_read(force=True)
        pi = self.browser_list_window_start + (row - 1)
        if not self.plans or pi < 0 or pi >= len(self.plans):
            return
        if self.index == pi:
            self.index = -1
            self._log("list_select_row", row, "-> unselect")
        else:
            self.index = pi
            self._log("list_select_row", row, "-> plan index", pi)
        self._set_status("READY")
        self._publish_state()

    # ── File browser ──

    def _plans_dir(self) -> str:
        system_path = xp.getSystemPath()
        return os.path.join(system_path, "Output", "FMS plans")

    def _current_fms_names_tuple(self, plans_dir: str) -> tuple:
        """Sorted tuple of .fms basenames under plans_dir (directory must exist)."""
        return tuple(sorted(
            f for f in os.listdir(plans_dir) if f.lower().endswith(".fms")
        ))

    def _ensure_plans_fresh_for_read(self, force: bool = False) -> None:
        """If plans folder appeared/disappeared or *.fms set changed, rescan and republish.

        Debounced on dataref reads (UDP/UI may poll often); use force=True for
        prev/next/load so user actions always see an up-to-date file list.
        """
        plans_dir = self._plans_dir()
        exists = os.path.isdir(plans_dir)

        if not force:
            now = time.monotonic()
            if now - self._last_plan_dir_check_monotonic < self.PLAN_DIR_POLL_MIN_INTERVAL_SEC:
                return
            self._last_plan_dir_check_monotonic = now

        if self._last_plans_dir_exists is None:
            self._refresh_plan_list()
            return
        if exists != self._last_plans_dir_exists:
            self._refresh_plan_list()
            return
        if not exists:
            return

        try:
            names = self._current_fms_names_tuple(plans_dir)
        except OSError as exc:
            self._log("ensure_plans_fresh: listdir failed", plans_dir, exc)
            return

        if names != self._fms_names_snapshot:
            self._refresh_plan_list()

    def _refresh_plan_list(self):
        plans_dir = self._plans_dir()
        self._log("Refreshing plans from", plans_dir)

        self.plans = []
        self.loaded = 0

        if not os.path.isdir(plans_dir):
            self._fms_names_snapshot = ()
            self._last_plans_dir_exists = False
            self.browser_list_window_start = 0
            self.index = -1
            self._set_status("NO DIR", f"Missing folder: {plans_dir}")
            self._publish_state()
            return

        self._last_plans_dir_exists = True
        filenames = sorted(
            [f for f in os.listdir(plans_dir) if f.lower().endswith(".fms")]
        )
        self._fms_names_snapshot = tuple(filenames)

        for filename in filenames:
            full_path = os.path.join(plans_dir, filename)
            info = self._parse_fms_file(full_path)
            if info is not None:
                self.plans.append(info)

        self._sort_plans()

        if not self.plans:
            self.index = -1
            self.browser_list_window_start = 0
            self._set_status("EMPTY")
        else:
            if self.index >= len(self.plans):
                self.index = -1
            self.browser_list_window_start = self._plan_list_align_window_start(
                self.browser_list_window_start, len(self.plans))
            self._set_status("READY")
        self._publish_state()

    @staticmethod
    def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        R_NM = 3440.065
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (math.sin(dlat / 2) ** 2
             + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
             * math.sin(dlon / 2) ** 2)
        return R_NM * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    def _parse_fms_file(self, path: str) -> Optional[FlightPlanInfo]:
        dep = "----"
        dest = "----"
        cycle = ""
        dep_runway = ""
        dest_runway = ""
        sid = ""
        star = ""
        filename = os.path.basename(path)

        try:
            file_mtime = float(os.path.getmtime(path))
        except OSError:
            file_mtime = 0.0

        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                lines = [line.strip() for line in f.readlines()]
        except Exception as exc:
            self._log("Skipping unreadable file", filename, exc)
            return None

        for line in lines[:20]:
            if line.startswith("CYCLE "):
                cycle = line.split(" ", 1)[1].strip()
            elif line.startswith("ADEP "):
                dep = line.split(" ", 1)[1].strip()
            elif line.startswith("ADES "):
                dest = line.split(" ", 1)[1].strip()
            elif line.startswith("DEPRWY RW"):
                dep_runway = line.split("RW", 1)[1].strip()
            elif line.startswith("DESRWY RW"):
                dest_runway = line.split("RW", 1)[1].strip()
            elif line.startswith("SID "):
                sid = line.split(" ", 1)[1].strip()
            elif line.startswith("STAR "):
                star = line.split(" ", 1)[1].strip()

        entries = self._parse_fms_entries(path)
        waypoint_count = len(entries)
        idents = [e.ident for e in entries]
        max_altitude = max((e.altitude for e in entries), default=0)

        total_distance = 0.0
        for i in range(1, len(entries)):
            total_distance += self._haversine_nm(
                entries[i - 1].lat, entries[i - 1].lon,
                entries[i].lat, entries[i].lon,
            )

        stem = os.path.splitext(filename)[0]
        if dep != "----" and dest != "----":
            display_name = f"{dep} {dest}"
        else:
            display_name = stem

        return FlightPlanInfo(
            filename=filename,
            full_path=path,
            display_name=display_name,
            dep=dep,
            dest=dest,
            cycle=cycle,
            dep_runway=dep_runway,
            dest_runway=dest_runway,
            sid=sid,
            star=star,
            waypoint_count=waypoint_count,
            total_distance_nm=round(total_distance, 1),
            waypoint_list=",".join(idents),
            max_altitude=max_altitude,
            file_mtime=file_mtime,
        )

    def _parse_fms_entries(self, path: str) -> List[FlightPlanEntry]:
        entries: List[FlightPlanEntry] = []

        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith(("I", "A", "CYCLE", "NUMENR", "DEPRWY", "DESRWY", "SID", "STAR")):
                    continue
                parts = line.split()
                if len(parts) < 6:
                    continue
                try:
                    entry_type = int(parts[0])
                    ident = parts[1]
                    altitude = int(float(parts[3]))
                    lat = float(parts[4])
                    lon = float(parts[5])
                except (TypeError, ValueError):
                    continue
                entries.append(
                    FlightPlanEntry(
                        entry_type=entry_type,
                        ident=ident,
                        altitude=altitude,
                        lat=lat,
                        lon=lon,
                    )
                )

        return entries

    def _clear_fms(self):
        count = xp.countFMSEntries()
        for index in range(count - 1, -1, -1):
            xp.clearFMSEntry(index)

    def _load_entry_into_fms(self, index: int, entry: FlightPlanEntry):
        nav_type = self.FMS_TYPE_TO_NAV.get(entry.entry_type)
        if nav_type == xp.Nav_LatLon:
            xp.setFMSEntryLatLon(index, entry.lat, entry.lon, entry.altitude)
            return

        nav_ref = xp.NAV_NOT_FOUND
        if nav_type is not None:
            nav_ref = xp.findNavAid(None, entry.ident, None, None, None, nav_type)

        if nav_ref != xp.NAV_NOT_FOUND:
            xp.setFMSEntryInfo(index, nav_ref, entry.altitude)
            return

        self._log("FMS nav lookup fallback", entry.ident, entry.entry_type, entry.lat, entry.lon)
        xp.setFMSEntryLatLon(index, entry.lat, entry.lon, entry.altitude)

    def _cmd_previous(self):
        self._ensure_plans_fresh_for_read(force=True)
        if not self.plans:
            self._set_status("EMPTY")
            self._publish_state()
            return
        cur = self.index if self.index >= 0 else 0
        self.index = (cur - 1) % len(self.plans)
        self._plan_list_ensure_index_visible()
        self._set_status("READY")
        self._publish_state()

    def _cmd_next(self):
        self._ensure_plans_fresh_for_read(force=True)
        if not self.plans:
            self._set_status("EMPTY")
            self._publish_state()
            return
        cur = self.index if self.index >= 0 else 0
        self.index = (cur + 1) % len(self.plans)
        self._plan_list_ensure_index_visible()
        self._set_status("READY")
        self._publish_state()

    def _cmd_refresh(self):
        self._refresh_plan_list()

    def _cmd_load(self):
        self._ensure_plans_fresh_for_read(force=True)
        plan = self._selected_plan()
        if plan is None:
            self.loaded = 0
            if self.plans:
                self._set_status("SELECT", "Tap a row or turn E1, then LOAD")
            else:
                self._set_status("EMPTY")
            self._publish_state()
            return

        try:
            entries = self._parse_fms_entries(plan.full_path)
            if not entries:
                self.loaded = 0
                self._set_status("LOAD FAIL", "No loadable FMS entries found")
                self._publish_state()
                return

            self._clear_fms()
            for index, entry in enumerate(entries):
                self._load_entry_into_fms(index, entry)

            xp.setDisplayedFMSEntry(0)
            xp.setDestinationFMSEntry(len(entries) - 1)
            self._log("Loaded FMS plan", plan.filename, "entries=", len(entries))
            self.loaded = 1
            self.loaded_filename = os.path.splitext(plan.filename)[0]
            self.loaded_index = self.index + 1
            self.loaded_sid = plan.sid
            self.loaded_star = plan.star
            self.loaded_distance_nm = plan.total_distance_nm
            self._set_status("LOADED")
            self._legs_init_after_load()
        except Exception as exc:
            self.loaded = 0
            self._set_status("LOAD ERR", str(exc))

        self._publish_state()

    def _cmd_open_fpl(self):
        if self.fpl_cmd:
            self._log("Executing sim/GPS/g1000n1_fpl")
            xp.commandOnce(self.fpl_cmd)
            self._set_status("FPL OPEN")
        else:
            self._set_status("NO FPL CMD", "sim/GPS/g1000n1_fpl not found")
        self._publish_state()

    # ── LEGS scrollable list ──────────────────────────────────

    def _legs_read_snapshot(self) -> str:
        """Build a JSON snapshot of all legs state — one dataref = one atomic WS push."""
        import json
        data = {
            "selected_index": self._legs_read_selected_index(),
            "active_index": self._legs_read_active_index(),
            "entry_count": self._legs_read_entry_count(),
            "window_start": self._legs_read_window_start(),
        }
        for row in range(1, self.LEGS_VISIBLE_ROWS + 1):
            data[f"row_{row}_index"] = self._legs_read_row_index(row)
            data[f"row_{row}_ident"] = self._legs_read_row_ident(row)
            data[f"row_{row}_alt"] = self._legs_read_row_alt(row)
            data[f"row_{row}_is_active"] = self._legs_read_row_is_active(row)
            data[f"row_{row}_is_selected"] = self._legs_read_row_is_selected(row)
            data[f"row_{row}_status"] = self._legs_read_row_status(row)
        return json.dumps(data, separators=(",", ":"))

    def _register_legs_drefs(self):
        p = self.LEGS_DREF_PREFIX
        # Global state
        self._register_live_int_dref("selected_index", self._legs_read_selected_index, prefix=p)
        self._register_live_int_dref("active_index", self._legs_read_active_index, prefix=p)
        self._register_live_int_dref("entry_count", self._legs_read_entry_count, prefix=p)
        self._register_writable_legs_window_start(prefix=p)
        # Per-row datarefs (rows 1-3)
        for row in range(1, self.LEGS_VISIBLE_ROWS + 1):
            self._register_live_string_dref(
                f"row_{row}_index", lambda r=row: self._legs_read_row_index(r), prefix=p)
            self._register_live_string_dref(
                f"row_{row}_ident", lambda r=row: self._legs_read_row_ident(r), prefix=p)
            self._register_live_string_dref(
                f"row_{row}_alt", lambda r=row: self._legs_read_row_alt(r), prefix=p)
            self._register_live_int_dref(
                f"row_{row}_is_active", lambda r=row: self._legs_read_row_is_active(r), prefix=p)
            self._register_live_int_dref(
                f"row_{row}_is_selected", lambda r=row: self._legs_read_row_is_selected(r), prefix=p)
            self._register_live_string_dref(
                f"row_{row}_status", lambda r=row: self._legs_read_row_status(r), prefix=p)
        # Single JSON snapshot — all legs data in one WS push
        self._register_live_string_dref("snapshot", self._legs_read_snapshot, prefix=p)

    def _create_legs_commands(self):
        p = self.LEGS_CMD_PREFIX
        self._create_command("scroll_up", "Scroll LEGS selection up", self._cmd_legs_scroll_up, prefix=p)
        self._create_command("scroll_down", "Scroll LEGS selection down", self._cmd_legs_scroll_down, prefix=p)
        self._create_command("direct_to", "Direct-to selected LEGS waypoint", self._cmd_legs_direct_to, prefix=p)
        self._create_command("select_row_1", "Select waypoint in row 1", self._cmd_legs_select_row_1, prefix=p)
        self._create_command("select_row_2", "Select waypoint in row 2", self._cmd_legs_select_row_2, prefix=p)
        self._create_command("select_row_3", "Select waypoint in row 3", self._cmd_legs_select_row_3, prefix=p)
        self._create_command("clear_selected", "Clear selected LEGS waypoint", self._cmd_legs_clear_selected, prefix=p)

    # ── LEGS state helpers ──

    def _legs_fms_index_for_row(self, row: int) -> int:
        """Convert visible row (1-3) to 0-based FMS entry index. Returns -1 if out of range."""
        idx = self.legs_window_start + (row - 1)
        count = self._read_fms_entry_count()
        if idx < 0 or idx >= count:
            return -1
        return idx

    def _legs_ensure_visible(self):
        """Adjust window_start so legs_selected is visible in the 3-row window."""
        count = self._read_fms_entry_count()
        if count <= 0:
            self.legs_selected = -1
            self.legs_window_start = 0
            return
        if self.legs_selected >= 0:
            self.legs_selected = max(0, min(self.legs_selected, count - 1))
        if self.legs_selected >= 0 and self.legs_selected < self.legs_window_start:
            self.legs_window_start = self.legs_selected
        elif self.legs_selected >= 0 and self.legs_selected >= self.legs_window_start + self.LEGS_VISIBLE_ROWS:
            self.legs_window_start = self.legs_selected - self.LEGS_VISIBLE_ROWS + 1
        # Allow partial last page (empty rows when count not multiple of 3)
        max_start = max(0, count - 1)
        self.legs_window_start = max(0, min(self.legs_window_start, max_start))

    def _legs_init_after_load(self):
        """Set LEGS to page containing active leg after a plan load."""
        try:
            count = xp.countFMSEntries()
            if count <= 0:
                self.legs_selected = 0
                self.legs_window_start = 0
                return
            active = xp.getDestinationFMSEntry()
            active = max(0, min(active, count - 1))
            # Page-based: window_start = start of page containing active
            self.legs_window_start = (active // self.LEGS_VISIBLE_ROWS) * self.LEGS_VISIBLE_ROWS
            max_start = max(0, count - 1)
            self.legs_window_start = max(0, min(self.legs_window_start, max_start))
            self.legs_selected = active
            self._log("legs_init_after_load: selected=", self.legs_selected,
                      "window=", self.legs_window_start, "count=", count)
        except Exception as exc:
            self._log("legs_init_after_load error:", exc)
            self.legs_selected = -1
            self.legs_window_start = 0

    # ── LEGS dataref readers ──

    def _legs_read_selected_index(self) -> int:
        return self.legs_selected + 1 if self.legs_selected >= 0 else 0  # 1-based for display; 0 when none

    def _legs_read_active_index(self) -> int:
        return self._read_fms_active_index()  # already 1-based

    def _legs_read_entry_count(self) -> int:
        return self._read_fms_entry_count()

    def _legs_read_window_start(self) -> int:
        return self.legs_window_start + 1  # 1-based for display

    def _legs_read_row_index(self, row: int) -> str:
        idx = self._legs_fms_index_for_row(row)
        if idx < 0:
            return ""
        return str(idx + 1)  # 1-based display

    def _legs_format_ident(self, info) -> str:
        """Return waypoint ident; for lat/lon entries with empty navAidID, format coords."""
        if not info:
            return ""
        ident = (info.navAidID or "").strip()
        if ident:
            return ident
        # Lat/lon or user waypoint with no navAidID — show truncated coords
        lat = getattr(info, "latitude", None) or getattr(info, "lat", None)
        lon = getattr(info, "longitude", None) or getattr(info, "lon", None)
        if lat is not None and lon is not None:
            ns = "N" if lat >= 0 else "S"
            ew = "E" if lon >= 0 else "W"
            return f"{ns}{abs(lat):.1f}{ew}{abs(lon):.1f}"
        return "?"

    def _legs_read_row_ident(self, row: int) -> str:
        idx = self._legs_fms_index_for_row(row)
        if idx < 0:
            return ""
        info = self._safe_fms_entry_info(idx)
        return self._legs_format_ident(info)

    def _legs_read_row_alt(self, row: int) -> str:
        idx = self._legs_fms_index_for_row(row)
        if idx < 0:
            return ""
        info = self._safe_fms_entry_info(idx)
        if not info or info.altitude <= 0:
            return ""
        return str(info.altitude)

    def _legs_read_row_is_active(self, row: int) -> int:
        idx = self._legs_fms_index_for_row(row)
        if idx < 0:
            return 0
        try:
            return 1 if idx == xp.getDestinationFMSEntry() else 0
        except Exception:
            return 0

    def _legs_read_row_is_selected(self, row: int) -> int:
        idx = self._legs_fms_index_for_row(row)
        if idx < 0:
            return 0
        return 1 if idx == self.legs_selected else 0

    def _legs_read_row_status(self, row: int) -> str:
        is_act = self._legs_read_row_is_active(row)
        is_sel = self._legs_read_row_is_selected(row)
        if is_act and is_sel:
            return "A+S"
        elif is_act:
            return "ACT"
        elif is_sel:
            return "SEL"
        return ""

    # ── LEGS commands ──

    def _cmd_legs_scroll_up(self):
        """Previous page (1-3, 4-6, 7-9, ...). Moves window by 3 waypoints."""
        count = self._read_fms_entry_count()
        if count <= 0:
            return
        new_start = max(0, self.legs_window_start - self.LEGS_VISIBLE_ROWS)
        if new_start != self.legs_window_start:
            self.legs_window_start = new_start
            self.legs_selected = -1  # clear selection when paging; user taps to select
            self._log("legs_scroll_up: page", self.legs_window_start // self.LEGS_VISIBLE_ROWS + 1,
                      "window=", self.legs_window_start, "showing", self.legs_window_start + 1, "-",
                      min(self.legs_window_start + 3, count))
        else:
            self._log("legs_scroll_up: already at first page")

    def _cmd_legs_scroll_down(self):
        """Next page (1-3, 4-6, 7-9, ...). Partial last page OK (e.g. 5 wpts: page 2 shows 4, 5, empty).

        Window start stays a multiple of 3, matching fms_legs/window_start write semantics:
        last page starts at ((count-1)//3)*3, not count-1.
        """
        count = self._read_fms_entry_count()
        if count <= 0:
            return
        max_w = ((count - 1) // self.LEGS_VISIBLE_ROWS) * self.LEGS_VISIBLE_ROWS
        next_start = self.legs_window_start + self.LEGS_VISIBLE_ROWS
        new_start = min(next_start, max_w)
        if new_start != self.legs_window_start:
            self.legs_window_start = new_start
            self.legs_selected = -1  # clear selection when paging; user taps to select
            self._log("legs_scroll_down: page", self.legs_window_start // self.LEGS_VISIBLE_ROWS + 1,
                      "window=", self.legs_window_start, "showing", self.legs_window_start + 1, "-",
                      min(self.legs_window_start + 3, count))
        else:
            self._log("legs_scroll_down: already at last page")

    def _cmd_legs_direct_to(self):
        try:
            count = xp.countFMSEntries()
            if count <= 0 or self.legs_selected < 0:
                return
            target = max(0, min(self.legs_selected, count - 1))
            xp.setDestinationFMSEntry(target)
            info = self._safe_fms_entry_info(target)
            ident = info.navAidID if info else "?"
            self._log("legs_direct_to:", target, ident)
        except Exception as exc:
            self._log("legs_direct_to error:", exc)

    def _cmd_legs_select_row_1(self):
        """Select the waypoint visible in row 1 (tap-to-select)."""
        self._cmd_legs_select_row(1)

    def _cmd_legs_select_row_2(self):
        """Select the waypoint visible in row 2 (tap-to-select)."""
        self._cmd_legs_select_row(2)

    def _cmd_legs_select_row_3(self):
        """Select the waypoint visible in row 3 (tap-to-select)."""
        self._cmd_legs_select_row(3)

    def _cmd_legs_select_row(self, row: int):
        """Toggle selection: select the waypoint in row (1-3), or unselect if already selected."""
        idx = self._legs_fms_index_for_row(row)
        if idx >= 0:
            if idx == self.legs_selected:
                self.legs_selected = -1
                self._log("legs_select_row:", row, "-> unselected")
            else:
                self.legs_selected = idx
                self._log("legs_select_row:", row, "-> index", idx)

    def _cmd_legs_clear_selected(self):
        """Clear the selected LEGS waypoint from the route."""
        try:
            count = xp.countFMSEntries()
            if count <= 0 or self.legs_selected < 0:
                return
            target = max(0, min(self.legs_selected, count - 1))
            info = self._safe_fms_entry_info(target)
            ident = info.navAidID if info else "?"
            xp.clearFMSEntry(target)
            # Indices shifted: entries after target moved down
            if self.legs_selected > target:
                self.legs_selected -= 1
            # New count is count-1; clamp selection to valid range
            self.legs_selected = max(0, min(self.legs_selected, count - 2))
            # Clamp window to valid range; allow partial last page
            new_count = count - 1
            max_start = max(0, new_count - 1)
            self.legs_window_start = max(0, min(self.legs_window_start, max_start))
            self._log("legs_clear_selected: cleared", target, ident)
        except Exception as exc:
            self._log("legs_clear_selected error:", exc)
