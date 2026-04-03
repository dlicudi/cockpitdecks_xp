import math
import os
import time
from dataclasses import dataclass
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
    file_timestamp: float = 0.0
    # File modification time (epoch seconds) for “newest first” sort
    file_mtime: float = 0.0


@dataclass
class FlightPlanEntry:
    entry_type: int
    ident: str
    altitude: int
    lat: float
    lon: float


@dataclass
class ProcedureInfo:
    name: str            # raw procedure identifier, e.g. "CHATY5", "I06L"
    proc_type: str       # "SID" | "STAR" | "APP"
    transition: str      # runway for SID (e.g. "06B"), entry fix for STAR, blank for APP common
    waypoints: List[str] # ordered fix idents (vector legs excluded)
    display_name: str    # human-readable, e.g. "CHATY5 06B", "ILS 06L"
    display_runway: str  # runway portion for the second column, e.g. "06B", "24R"


class PythonInterface:
    NAME = "Cockpitdecks FMS"
    SIG = "xppython3.cockpitdecksfms"
    DESC = (
        "Cockpitdecks FMS plugin: browse and load Output/FMS plans (cockpitdecks/fms/load), "
        "scrollable LEGS list with waypoint selection/activation/direct-to (cockpitdecks/fms/legs), "
        "live FMS state datarefs, and map range control."
    )
    RELEASE = "2.0.22"

    DREF_PREFIX = "cockpitdecks/fms/load"
    CMD_PREFIX = "cockpitdecks/fms/load"

    LEGS_DREF_PREFIX = "cockpitdecks/fms/legs"
    LEGS_CMD_PREFIX = "cockpitdecks/fms/legs"
    LEGS_VISIBLE_ROWS = 3

    DEP_DREF_PREFIX = "cockpitdecks/fms/dep"
    DEP_CMD_PREFIX = "cockpitdecks/fms/dep"
    ARR_DREF_PREFIX = "cockpitdecks/fms/arr"
    ARR_CMD_PREFIX = "cockpitdecks/fms/arr"
    APP_DREF_PREFIX = "cockpitdecks/fms/app"
    APP_CMD_PREFIX = "cockpitdecks/fms/app"
    PROC_VISIBLE_ROWS = 3
    KINDS = ("dep", "arr", "app")
    _KIND_CIFP_TYPE = {"dep": "SID", "arr": "STAR", "app": "APP"}

    _APP_TYPE_LABELS = {
        "I": "ILS", "R": "RNAV", "V": "VOR", "N": "NDB", "L": "LOC",
        "D": "DME", "S": "RNAV", "B": "LOC BC", "T": "TACAN",
        "U": "SDF", "H": "HUD", "P": "GPS", "Q": "RNAV", "X": "LDA",
    }

    # Plan file browser: 3 visible rows (Loupedeck Live 4×3), same paging idea as fms_legs
    PLAN_LIST_VISIBLE_ROWS = 3
    # Maximum plans loaded — always the N most recent by file modification time.
    PLAN_LIST_MAX_PLANS = 9

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
        # Sort controls for the plan browser.
        # plan_sort_key: 0 = filename, 1 = file timestamp
        # plan_sort_desc: False = ascending, True = descending
        self.plan_sort_key = 0
        self.plan_sort_desc = False

        self.accessors = []
        self.commands: Dict[str, Dict[str, object]] = {}

        # Procedure state (dep=SID, arr=STAR, app=APP — each section independent)
        self.proc_dep_icao = ""
        self.proc_dest_icao = ""
        self._cifp_cache: Dict[str, List[ProcedureInfo]] = {}
        self._proc_procs: Dict[str, List[ProcedureInfo]] = {k: [] for k in self.KINDS}
        self._proc_index: Dict[str, int] = {k: -1 for k in self.KINDS}
        self._proc_window: Dict[str, int] = {k: 0 for k in self.KINDS}
        self._proc_cache_valid: Dict[str, bool] = {k: False for k in self.KINDS}
        self._proc_rows_cache: Dict[str, Dict[int, Dict[str, object]]] = {k: {} for k in self.KINDS}
        self._proc_status: Dict[str, str] = {k: "INIT" for k in self.KINDS}
        # FMS splice point: index at which arr/app was last inserted (for replace-not-append)
        self._proc_splice_point: Dict[str, int] = {k: -1 for k in self.KINDS}
        # Display name of the last successfully activated procedure per kind
        self._proc_loaded: Dict[str, str] = {k: "" for k in self.KINDS}

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
            "loaded_filename": "",
            "loaded_sid": "",
            "loaded_star": "",
            "map_mode": "",
            "status": "INIT",
            "last_error": "",
        }
        self.int_values: Dict[str, int] = {
            "index": 0,
            "count": 0,
            "loaded": 0,
            "loaded_index": 0,
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

        self._perf_enabled = True
        self._list_rows_cache: Dict[int, Dict[str, object]] = {}
        self._list_cache_valid = False
        # Cache for parsed FMS entries, keyed by (filename, mtime_ns, size).
        # Avoids re-reading every .fms file on the REFRESH command when files haven't changed.
        self._entry_parse_cache: Dict[tuple, list] = {}

    def _log(self, *parts):
        if self.trace:
            print(self.info, *parts)

    def _perf_log(self, *parts):
        # Inside X-Plane only. Use SDK logging so lines reliably land in Log.txt / XPPython3Log.txt
        # (plain print() is not consistently captured the same way as trace _log output).
        if not self._perf_enabled:
            return
        line = f"{self.info} [perf] " + " ".join(str(p) for p in parts) + "\n"
        try:
            xp.debugString(line)
        except Exception:
            print(self.info, "[perf]", *parts)

    def XPluginStart(self):
        self._log("XPluginStart", f"LEGS_VISIBLE_ROWS={self.LEGS_VISIBLE_ROWS}")

        self._register_string_dref("plan_name")
        self._register_string_dref("plan_departure")
        self._register_string_dref("plan_destination")
        self._register_string_dref("plan_cycle")
        self._register_string_dref("plan_filename")
        self._register_string_dref("plan_path")
        self._register_string_dref("plan_dep_runway")
        self._register_string_dref("plan_dest_runway")
        self._register_string_dref("plan_sid")
        self._register_string_dref("plan_star")
        self._register_string_dref("plan_waypoints")
        self._register_string_dref("status")
        self._register_string_dref("last_error")
        self._register_string_dref("loaded_filename")
        self._register_string_dref("loaded_sid")
        self._register_string_dref("loaded_star")
        self._register_string_dref("map_mode")

        self._register_int_dref("index")
        self._register_int_dref("count")
        self._register_int_dref("loaded")
        self._register_int_dref("loaded_index")
        self._register_writable_action_dref("action")
        self._register_int_dref("last_action")
        self._register_int_dref("action_ack")
        self._register_int_dref("plan_waypoint_count")
        self._register_int_dref("plan_max_altitude")

        self._register_float_dref("plan_distance_nm")
        self._register_float_dref("loaded_distance_nm")

        self._register_live_fms_drefs()

        self._create_command("previous", "Select previous FMS plan", self._cmd_previous)
        self._create_command("next", "Select next FMS plan", self._cmd_next)
        self._create_command("refresh", "Refresh FMS plan list", self._cmd_refresh)
        self._create_command("load", "Load selected FMS plan", self._cmd_load)
        self._create_command("open_fpl", "Open G1000 FPL page", self._cmd_open_fpl)
        self._create_command("wp_next", "Display next FMS waypoint", self._cmd_wp_next)
        self._create_command("wp_previous", "Display previous FMS waypoint", self._cmd_wp_previous)
        self._create_command("wp_activate", "Activate displayed FMS waypoint", self._cmd_wp_activate)
        self._create_command("wp_direct", "Direct-to displayed FMS waypoint", self._cmd_wp_direct)
        self._create_command("clear_fms_entry", "Clear displayed FMS entry", self._cmd_clear_fms_entry)
        self._create_command("map_range_down", "Map range zoom in", self._cmd_map_range_down)
        self._create_command("map_range_up", "Map range zoom out", self._cmd_map_range_up)
        self._create_command("map_toggle", "Toggle map range target", self._cmd_map_toggle)

        self._register_plan_list_window_drefs()
        self._create_plan_list_window_commands()

        for mode, (down_cmd, up_cmd) in self.map_range_cmds.items():
            self.map_cmd_refs[mode] = (xp.findCommand(down_cmd), xp.findCommand(up_cmd))
            self._log("findCommand map range", mode, down_cmd, "->", self.map_cmd_refs[mode][0],
                      up_cmd, "->", self.map_cmd_refs[mode][1])

        self._register_legs_drefs()
        self._create_legs_commands()

        self._proc_register_section("dep", self.DEP_DREF_PREFIX, self.DEP_CMD_PREFIX)
        self._proc_register_section("arr", self.ARR_DREF_PREFIX, self.ARR_CMD_PREFIX)
        self._proc_register_section("app", self.APP_DREF_PREFIX, self.APP_CMD_PREFIX)

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
        self._proc_airports_from_fms()
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

    def _register_string_dref(self, suffix: str):
        name = f"{self.DREF_PREFIX}/{suffix}"

        def read_data(refCon, values, offset, count):
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

    def _register_int_dref(self, suffix: str):
        name = f"{self.DREF_PREFIX}/{suffix}"

        def read_int(refCon):
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

    def _register_float_dref(self, suffix: str):
        name = f"{self.DREF_PREFIX}/{suffix}"

        def read_float(refCon):
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
        self.commands[name] = {"ref": cmd_ref, "fun": handler}

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
        t0 = time.perf_counter()
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
        self._perf_log(f"publish_state_ms={(time.perf_counter() - t0) * 1000.0:.2f}")

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

    def _cmd_wp_activate(self):
        try:
            count = xp.countFMSEntries()
            if count <= 0:
                return
            displayed = xp.getDisplayedFMSEntry()
            xp.setDestinationFMSEntry(displayed)
            info = self._safe_fms_entry_info(displayed)
            ident = info.navAidID if info else "?"
            self._log("wp_activate: destination set to", displayed, ident)
        except Exception as exc:
            self._log("wp_activate error:", exc)

    def _cmd_wp_direct(self):
        try:
            count = xp.countFMSEntries()
            if count <= 0:
                return
            displayed = xp.getDisplayedFMSEntry()
            fp = getattr(xp, "FMSFlightPlan_Active", getattr(xp, "ActiveFlightPlan", 0))
            if hasattr(xp, "setDirectToFMSFlightPlanEntry"):
                xp.setDirectToFMSFlightPlanEntry(fp, displayed)
            else:
                self._log("wp_direct: API xp.setDirectToFMSFlightPlanEntry not available (XP12 feature)")
                return
            info = self._safe_fms_entry_info(displayed)
            ident = info.navAidID if info else "?"
            self._log("wp_direct: direct-to set to", displayed, ident)
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

    def _invalidate_list_cache(self) -> None:
        self._list_cache_valid = False

    def _ensure_list_cache(self) -> None:
        if self._list_cache_valid:
            return
        t0 = time.perf_counter()
        rows: Dict[int, Dict[str, object]] = {}
        n = len(self.plans)
        w = self.browser_list_window_start
        page = w // self.PLAN_LIST_VISIBLE_ROWS + 1 if n else 0
        page_count = (n + self.PLAN_LIST_VISIBLE_ROWS - 1) // self.PLAN_LIST_VISIBLE_ROWS if n else 0

        for row in range(1, self.PLAN_LIST_VISIBLE_ROWS + 1):
            pi = w + (row - 1)
            if pi < 0 or pi >= n:
                rows[row] = {
                    "plan_index": -1,
                    "index": "",
                    "filename": "",
                    "timestamp": "",
                    "dep": "",
                    "dest": "",
                    "route": "",
                    "wpt_count": 0,
                    "max_alt_ft": 0,
                    "distance_nm": 0.0,
                    "is_selected": 0,
                    "status": "",
                }
                continue

            plan = self.plans[pi]
            dep = (plan.dep or "").strip()
            dep = "" if (not dep or dep == "----") else dep
            dest = (plan.dest or "").strip()
            dest = "" if (not dest or dest == "----") else dest
            if dep and dest:
                route = f"{dep} {dest}"
            else:
                route = dep or dest
            is_selected = int(self.index >= 0 and pi == self.index)
            rows[row] = {
                "plan_index": pi,
                "index": str(pi + 1),
                "filename": os.path.splitext(plan.filename)[0],
                "timestamp": self._format_file_timestamp(plan.file_timestamp),
                "dep": dep,
                "dest": dest,
                "route": route,
                "wpt_count": int(plan.waypoint_count),
                "max_alt_ft": int(plan.max_altitude),
                "distance_nm": float(plan.total_distance_nm),
                "is_selected": is_selected,
                "status": "SEL" if is_selected else "",
            }

        self._list_rows_cache = rows
        self._list_cache_valid = True
        self._perf_log(
            f"list_cache_rebuild_ms={(time.perf_counter() - t0) * 1000.0:.2f}",
            f"plans={n}",
            f"selected={self.index}",
            f"window_start={w}",
            f"page={page}",
            f"page_count={page_count}",
        )

    def _plan_list_read_row_plan_index(self, row: int) -> str:
        self._ensure_list_cache()
        return str(self._list_rows_cache.get(row, {}).get("index", ""))

    def _plan_list_read_row_filename(self, row: int) -> str:
        self._ensure_list_cache()
        return str(self._list_rows_cache.get(row, {}).get("filename", ""))

    def _format_file_timestamp(self, ts: float) -> str:
        if ts <= 0:
            return ""
        try:
            return time.strftime("%Y-%m-%d %H:%M", time.localtime(ts))
        except Exception:
            return ""

    def _plan_list_read_row_timestamp(self, row: int) -> str:
        self._ensure_list_cache()
        return str(self._list_rows_cache.get(row, {}).get("timestamp", ""))

    def _plan_list_read_row_dep(self, row: int) -> str:
        self._ensure_list_cache()
        return str(self._list_rows_cache.get(row, {}).get("dep", ""))

    def _plan_list_read_row_dest(self, row: int) -> str:
        self._ensure_list_cache()
        return str(self._list_rows_cache.get(row, {}).get("dest", ""))

    def _plan_list_read_row_route(self, row: int) -> str:
        """DEP ARR for annunciator second segment (single line)."""
        self._ensure_list_cache()
        return str(self._list_rows_cache.get(row, {}).get("route", ""))

    def _plan_list_read_row_wpt_count(self, row: int) -> int:
        self._ensure_list_cache()
        return int(self._list_rows_cache.get(row, {}).get("wpt_count", 0))

    def _plan_list_read_row_max_alt_ft(self, row: int) -> int:
        """Max waypoint altitude in the .fms file (ft MSL), 0 if row empty or no points."""
        self._ensure_list_cache()
        return int(self._list_rows_cache.get(row, {}).get("max_alt_ft", 0))

    def _plan_list_read_selected_row(self) -> int:
        w = self.browser_list_window_start
        n = len(self.plans)
        if self.index >= 0 and 0 <= self.index < n:
            row_on_page = self.index - w + 1
            if 1 <= row_on_page <= self.PLAN_LIST_VISIBLE_ROWS:
                res = int(row_on_page)
                self._log("POLL list_selected_row ->", res)
                return res
        return 0

    def _plan_list_read_row_distance_nm(self, row: int) -> int:
        self._ensure_list_cache()
        return int(round(self._list_rows_cache.get(row, {}).get("distance_nm", 0.0)))

    def _plan_list_read_row_status(self, row: int) -> str:
        self._ensure_list_cache()
        return str(self._list_rows_cache.get(row, {}).get("status", ""))

    def _plan_list_read_page_indicator(self) -> str:
        n = len(self.plans)
        if n <= 0:
            return ""
        page = self.browser_list_window_start // self.PLAN_LIST_VISIBLE_ROWS + 1
        total = (n + self.PLAN_LIST_VISIBLE_ROWS - 1) // self.PLAN_LIST_VISIBLE_ROWS
        return f"{page}/{total}"

    def _plan_list_read_selected_over_count(self) -> str:
        """SEL on LOAD: which of the 3 visible rows is selected (1–3), not global plan index."""
        nrows = self.PLAN_LIST_VISIBLE_ROWS
        if not self.plans:
            return "0/0"
        if self.index < 0:
            return f"-/{nrows}"
        w = self.browser_list_window_start
        row = self.index - w + 1  # 1-based row on this page
        if row < 1 or row > nrows:
            return f"-/{nrows}"
        return f"{row}/{nrows}"

    def _plan_list_read_sort_key_label(self) -> str:
        return "NAME" if self.plan_sort_key == 0 else "DATE"

    def _plan_list_read_sort_dir_label(self) -> str:
        return "DESC" if self.plan_sort_desc else "ASC"

    def _plan_list_read_window_page(self) -> int:
        """1-based plan-list page (rows 1–3 = page 1, etc.); same stepping idea as fms_legs/window_start."""
        n = len(self.plans)
        if n <= 0:
            return 1
        w = self.browser_list_window_start
        page = w // self.PLAN_LIST_VISIBLE_ROWS + 1
        return max(1, int(page))

    def _register_plan_list_window_drefs(self):
        p = self.DREF_PREFIX
        self._register_live_string_dref("list_page", self._plan_list_read_page_indicator, prefix=p)
        self._register_live_string_dref("list_sel_count", self._plan_list_read_selected_over_count, prefix=p)
        self._register_live_string_dref("list_sort_key", self._plan_list_read_sort_key_label, prefix=p)
        self._register_live_string_dref("list_sort_direction", self._plan_list_read_sort_dir_label, prefix=p)
        self._register_live_int_dref("list_window_page", self._plan_list_read_window_page, prefix=p)
        for row in range(1, self.PLAN_LIST_VISIBLE_ROWS + 1):
            self._register_live_string_dref(
                f"list_row_{row}_index", lambda r=row: self._plan_list_read_row_plan_index(r), prefix=p)
            self._register_live_string_dref(
                f"list_row_{row}_filename", lambda r=row: self._plan_list_read_row_filename(r), prefix=p)
            self._register_live_string_dref(
                f"list_row_{row}_timestamp", lambda r=row: self._plan_list_read_row_timestamp(r), prefix=p)
            self._register_live_string_dref(
                f"list_row_{row}_dep", lambda r=row: self._plan_list_read_row_dep(r), prefix=p)
            self._register_live_string_dref(
                f"list_row_{row}_dest", lambda r=row: self._plan_list_read_row_dest(r), prefix=p)
            self._register_live_string_dref(
                f"list_row_{row}_route", lambda r=row: self._plan_list_read_row_route(r), prefix=p)
            self._register_live_int_dref(
                f"list_row_{row}_wpt_count", lambda r=row: self._plan_list_read_row_wpt_count(r), prefix=p)
            self._register_live_int_dref(
                f"list_row_{row}_max_alt_ft", lambda r=row: self._plan_list_read_row_max_alt_ft(r), prefix=p)
            self._register_live_int_dref(
                f"list_row_{row}_distance_nm", lambda r=row: self._plan_list_read_row_distance_nm(r), prefix=p)
            self._register_live_string_dref(
                f"list_row_{row}_status", lambda r=row: self._plan_list_read_row_status(r), prefix=p)

        self._register_live_int_dref(
            "list_selected_row", lambda: self._plan_list_read_selected_row(), prefix=p)
        for row in range(1, self.PLAN_LIST_VISIBLE_ROWS + 1):
            self._register_live_int_dref(
                f"list_row_{row}_is_selected",
                lambda r=row: 1 if self._plan_list_read_selected_row() == r else 0,
                prefix=p)

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
            "list_sort_filename",
            "Sort plan list by filename",
            self._cmd_list_sort_filename,
            prefix=p,
        )
        self._create_command(
            "list_sort_timestamp",
            "Sort plan list by file timestamp",
            self._cmd_list_sort_timestamp,
            prefix=p,
        )
        self._create_command(
            "list_sort_toggle_key",
            "Toggle plan list sort key",
            self._cmd_list_toggle_sort_key,
            prefix=p,
        )
        self._create_command(
            "list_sort_asc",
            "Sort plan list ascending",
            self._cmd_list_sort_asc,
            prefix=p,
        )
        self._create_command(
            "list_sort_desc",
            "Sort plan list descending",
            self._cmd_list_sort_desc,
            prefix=p,
        )
        self._create_command(
            "list_sort_toggle_direction",
            "Toggle plan list sort direction",
            self._cmd_list_toggle_sort_direction,
            prefix=p,
        )

    def _sort_plans(self):
        """Reorder self.plans in place (does not change selection index)."""
        if not self.plans:
            return
        if self.plan_sort_key == 0:
            self.plans.sort(key=lambda p: p.filename.lower(), reverse=self.plan_sort_desc)
        else:
            if self.plan_sort_desc:
                self.plans.sort(key=lambda p: (-p.file_timestamp, p.filename.lower()))
            else:
                self.plans.sort(key=lambda p: (p.file_timestamp, p.filename.lower()))

    def _plan_list_apply_sort(self, key: Optional[int] = None, desc: Optional[bool] = None):
        if not self.plans:
            self._set_status("EMPTY")
            self._invalidate_list_cache()
            self._publish_state()
            return

        sel_fn = self.plans[self.index].filename if 0 <= self.index < len(self.plans) else None
        if key is not None:
            self.plan_sort_key = 1 if int(key) else 0
        if desc is not None:
            self.plan_sort_desc = bool(desc)

        self._sort_plans()

        if sel_fn is not None:
            self.index = next((i for i, p in enumerate(self.plans) if p.filename == sel_fn), -1)
        if self.index >= 0:
            self._plan_list_ensure_index_visible()
        else:
            self.browser_list_window_start = self._plan_list_align_window_start(
                self.browser_list_window_start, len(self.plans))
        sort_key = "NAME" if self.plan_sort_key == 0 else "DATE"
        sort_dir = "DESC" if self.plan_sort_desc else "ASC"
        self._set_status(f"SORT {sort_key} {sort_dir}")
        self._invalidate_list_cache()
        self._publish_state()

    def _cmd_list_sort_filename(self):
        self._plan_list_apply_sort(key=0)

    def _cmd_list_sort_timestamp(self):
        self._plan_list_apply_sort(key=1)

    def _cmd_list_toggle_sort_key(self):
        self._plan_list_apply_sort(key=1 - self.plan_sort_key)

    def _cmd_list_sort_asc(self):
        self._plan_list_apply_sort(desc=False)

    def _cmd_list_sort_desc(self):
        self._plan_list_apply_sort(desc=True)

    def _cmd_list_toggle_sort_direction(self):
        self._plan_list_apply_sort(desc=not self.plan_sort_desc)

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
        if not self.plans:
            return
        new_start = max(0, self.browser_list_window_start - self.PLAN_LIST_VISIBLE_ROWS)
        if new_start != self.browser_list_window_start:
            self.browser_list_window_start = new_start
            self.index = -1  # like fms_legs: clear selection when paging
            self._invalidate_list_cache()
            self._log(
                "list_scroll_up: page",
                self.browser_list_window_start // self.PLAN_LIST_VISIBLE_ROWS + 1,
                "window_start=", self.browser_list_window_start,
            )
            self._set_status("READY")
            self._publish_state()

    def _cmd_list_scroll_down(self):
        """Next page: 1-3, 4-6, 7-9, … partial last page (e.g. 5 plans: row3 empty)."""
        if not self.plans:
            return
        n = len(self.plans)
        max_w = self._plan_list_max_aligned_window_start(n)
        next_start = self.browser_list_window_start + self.PLAN_LIST_VISIBLE_ROWS
        new_start = min(next_start, max_w)
        if new_start != self.browser_list_window_start:
            self.browser_list_window_start = new_start
            self.index = -1  # like fms_legs: clear selection when paging
            self._invalidate_list_cache()
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
        """Tap a row to select that plan. Tapping the same row again keeps it selected."""
        pi = self.browser_list_window_start + (row - 1)
        if not self.plans or pi < 0 or pi >= len(self.plans):
            return
        self.index = pi
        self._log("list_select_row", row, "-> plan index", pi)
        self._invalidate_list_cache()
        self._set_status("READY")
        self._publish_state()

    # ── File browser ──

    def _plans_dir(self) -> str:
        system_path = xp.getSystemPath()
        return os.path.join(system_path, "Output", "FMS plans")

    def _refresh_plan_list(self):
        t_total_0 = time.perf_counter()
        plans_dir = self._plans_dir()
        self._log("Refreshing plans from", plans_dir)

        # Remember the currently selected filename so we can restore it after refresh.
        selected_filename = (
            self.plans[self.index].filename
            if 0 <= self.index < len(self.plans) else None
        )

        self.plans = []
        self.loaded = 0
        self._invalidate_list_cache()

        if not os.path.isdir(plans_dir):
            self.browser_list_window_start = 0
            self.index = -1
            self._set_status("NO DIR", f"Missing folder: {plans_dir}")
            self._invalidate_list_cache()
            self._publish_state()
            return

        t_scan_0 = time.perf_counter()
        # Single scandir pass: collect filenames and build parse-cache key tuples.
        filenames = []
        snapshot_rows = []
        with os.scandir(plans_dir) as it:
            for entry in it:
                if not entry.name.lower().endswith(".fms") or not entry.is_file(follow_symlinks=False):
                    continue
                filenames.append(entry.name)
                try:
                    st = entry.stat(follow_symlinks=False)
                    snapshot_rows.append((entry.name, st.st_mtime_ns, st.st_size))
                except OSError:
                    pass
        # Purge cache entries for files that are no longer present or have changed.
        current_keys = set(snapshot_rows)
        self._entry_parse_cache = {
            k: v for k, v in self._entry_parse_cache.items() if k in current_keys
        }
        t_scan_ms = (time.perf_counter() - t_scan_0) * 1000.0

        t_parse_0 = time.perf_counter()
        for filename in filenames:
            full_path = os.path.join(plans_dir, filename)
            info = self._parse_fms_file(full_path)
            if info is not None:
                self.plans.append(info)
        t_parse_ms = (time.perf_counter() - t_parse_0) * 1000.0

        t_sort_0 = time.perf_counter()
        # Always show the most recent PLAN_LIST_MAX_PLANS plans, newest first.
        self.plans.sort(key=lambda p: (-p.file_timestamp, p.filename.lower()))
        self.plans = self.plans[:self.PLAN_LIST_MAX_PLANS]
        t_sort_ms = (time.perf_counter() - t_sort_0) * 1000.0

        if not self.plans:
            self.index = -1
            self.browser_list_window_start = 0
            self._set_status("EMPTY")
        else:
            # Restore selection by filename so a background rescan doesn't silently
            # lose the highlight or point self.index at the wrong plan after a re-sort.
            if selected_filename is not None:
                self.index = next(
                    (i for i, p in enumerate(self.plans) if p.filename == selected_filename), -1
                )
            elif self.index >= len(self.plans):
                self.index = -1
            # Keep selected plan visible in the window.
            if self.index >= 0:
                self._plan_list_ensure_index_visible()
            else:
                self.browser_list_window_start = self._plan_list_align_window_start(
                    self.browser_list_window_start, len(self.plans))
            self._set_status("READY")
        self._invalidate_list_cache()
        t_publish_0 = time.perf_counter()
        self._publish_state()
        t_publish_ms = (time.perf_counter() - t_publish_0) * 1000.0
        self._perf_log(
            f"refresh_total_ms={(time.perf_counter() - t_total_0) * 1000.0:.2f}",
            f"scan_ms={t_scan_ms:.2f}",
            f"parse_ms={t_parse_ms:.2f}",
            f"sort_ms={t_sort_ms:.2f}",
            f"publish_ms={t_publish_ms:.2f}",
            f"files={len(filenames)}",
            f"plans={len(self.plans)}",
        )

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
            stat = os.stat(path)
            file_mtime = float(stat.st_mtime)
            file_timestamp = float(getattr(stat, "st_birthtime", stat.st_mtime))
        except OSError:
            file_mtime = 0.0
            file_timestamp = 0.0

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

        # Use entry cache keyed by (filename, mtime_ns, size) to avoid re-parsing
        # unchanged files on every directory rescan. Reuses the stat() already done above.
        try:
            cache_key = (
                filename,
                getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1e9)),
                stat.st_size,
            )
        except (OSError, NameError):
            cache_key = None

        if cache_key is not None and cache_key in self._entry_parse_cache:
            entries = self._entry_parse_cache[cache_key]
        else:
            entries = self._parse_fms_entries(path)
            if cache_key is not None:
                self._entry_parse_cache[cache_key] = entries

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
            file_timestamp=file_timestamp,
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
        """E1 CCW: select previous plan within current visible page only."""
        if not self.plans:
            self._set_status("EMPTY")
            self._invalidate_list_cache()
            self._publish_state()
            return

        start = self.browser_list_window_start
        end = min(start + self.PLAN_LIST_VISIBLE_ROWS, len(self.plans))
        if end <= start:
            self._set_status("READY")
            self._invalidate_list_cache()
            self._publish_state()
            return

        if self.index < start or self.index >= end:
            # Nothing selected on this page: CCW starts from the last visible row.
            self.index = end - 1
        else:
            self.index = max(start, self.index - 1)

        self._set_status("READY")
        self._invalidate_list_cache()
        self._publish_state()

    def _cmd_next(self):
        """E1 CW: select next plan within current visible page only."""
        if not self.plans:
            self._set_status("EMPTY")
            self._invalidate_list_cache()
            self._publish_state()
            return

        start = self.browser_list_window_start
        end = min(start + self.PLAN_LIST_VISIBLE_ROWS, len(self.plans))
        if end <= start:
            self._set_status("READY")
            self._invalidate_list_cache()
            self._publish_state()
            return

        if self.index < start or self.index >= end:
            # Nothing selected on this page: CW starts from the first visible row.
            self.index = start
        else:
            self.index = min(end - 1, self.index + 1)

        self._set_status("READY")
        self._invalidate_list_cache()
        self._publish_state()

    def _cmd_refresh(self):
        self._refresh_plan_list()

    def _get_cached_entries(self, plan: FlightPlanInfo) -> List[FlightPlanEntry]:
        """Return parsed FMS entries for plan, using the entry cache when possible."""
        try:
            stat = os.stat(plan.full_path)
            cache_key = (
                plan.filename,
                getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1e9)),
                stat.st_size,
            )
        except OSError:
            cache_key = None
        if cache_key is not None and cache_key in self._entry_parse_cache:
            return self._entry_parse_cache[cache_key]
        entries = self._parse_fms_entries(plan.full_path)
        if cache_key is not None:
            self._entry_parse_cache[cache_key] = entries
        return entries

    def _cmd_load(self):
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
            entries = self._get_cached_entries(plan)
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
            # Refresh procedure lists for the new departure/destination airports
            new_dep = (plan.dep or "").strip().upper()
            new_dest = (plan.dest or "").strip().upper()
            if new_dep != self.proc_dep_icao or new_dest != self.proc_dest_icao:
                self.proc_dep_icao = new_dep
                self.proc_dest_icao = new_dest
                self._proc_refresh()
        except Exception as exc:
            self.loaded = 0
            self._set_status("LOAD ERR", str(exc))

        self._publish_state()

    def _cmd_open_fpl(self):
        cmd = xp.findCommand("sim/GPS/g1000n1_fpl")
        if cmd:
            self._log("Executing sim/GPS/g1000n1_fpl")
            xp.commandOnce(cmd)
            self._set_status("FPL OPEN")
        else:
            self._set_status("NO FPL CMD", "sim/GPS/g1000n1_fpl not found")
        self._publish_state()

    # ── LEGS scrollable list ──────────────────────────────────

    def _register_legs_drefs(self):
        p = self.LEGS_DREF_PREFIX
        # Global state
        self._register_live_int_dref("selected_index", self._legs_read_selected_index, prefix=p)
        self._register_live_int_dref("active_index", self._legs_read_active_index, prefix=p)
        self._register_live_int_dref("entry_count", self._legs_read_entry_count, prefix=p)
        self._register_live_string_dref("page", self._legs_read_page_indicator, prefix=p)
        self._register_live_string_dref("sel_count", self._legs_read_selected_over_count, prefix=p)
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

    def _create_legs_commands(self):
        p = self.LEGS_CMD_PREFIX
        self._create_command("scroll_up", "Scroll LEGS selection up", self._cmd_legs_scroll_up, prefix=p)
        self._create_command("scroll_down", "Scroll LEGS selection down", self._cmd_legs_scroll_down, prefix=p)
        self._create_command("previous", "Select previous visible LEGS row", self._cmd_legs_previous, prefix=p)
        self._create_command("next", "Select next visible LEGS row", self._cmd_legs_next, prefix=p)
        self._create_command("activate", "Activate selected LEGS waypoint", self._cmd_legs_activate, prefix=p)
        self._create_command("direct_to", "Direct-to selected LEGS waypoint", self._cmd_legs_direct_to, prefix=p)
        self._create_command("select_row_1", "Select waypoint in row 1", self._cmd_legs_select_row_1, prefix=p)
        self._create_command("select_row_2", "Select waypoint in row 2", self._cmd_legs_select_row_2, prefix=p)
        self._create_command("select_row_3", "Select waypoint in row 3", self._cmd_legs_select_row_3, prefix=p)
        self._create_command("clear_selected", "Clear selected LEGS waypoint", self._cmd_legs_clear_selected, prefix=p)
        self._create_command("clear_from_here", "Clear from selected waypoint to end", self._cmd_legs_clear_from_here, prefix=p)
        self._create_command("clear_all", "Clear entire FMS route", self._cmd_legs_clear_all, prefix=p)
        self._create_command("direct_to_destination", "Direct-to destination (last FMS entry)", self._cmd_legs_direct_to_destination, prefix=p)

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
        count = self._read_fms_entry_count()
        if count <= 0 or self.legs_selected < 0 or self.legs_selected >= count:
            return 0
        return self.legs_selected + 1  # 1-based for display; 0 when none

    def _legs_read_active_index(self) -> int:
        return self._read_fms_active_index()  # already 1-based

    def _legs_read_entry_count(self) -> int:
        return self._read_fms_entry_count()

    def _legs_read_window_start(self) -> int:
        return self.legs_window_start + 1  # 1-based for display

    def _legs_read_page_indicator(self) -> str:
        """Return current/total pages, e.g. 3/7."""
        count = self._read_fms_entry_count()
        if count <= 0:
            return "0/0"
        page = self.legs_window_start // self.LEGS_VISIBLE_ROWS + 1
        total = (count + self.LEGS_VISIBLE_ROWS - 1) // self.LEGS_VISIBLE_ROWS
        return f"{page}/{total}"

    def _legs_read_selected_over_count(self) -> str:
        """Return selected row in current page over visible rows, e.g. 1/3."""
        count = self._read_fms_entry_count()
        if count <= 0:
            return "0/0"
        if self.legs_selected < 0:
            return f"-/{self.LEGS_VISIBLE_ROWS}"
        row = self.legs_selected - self.legs_window_start + 1
        if row < 1 or row > self.LEGS_VISIBLE_ROWS:
            return f"-/{self.LEGS_VISIBLE_ROWS}"
        return f"{row}/{self.LEGS_VISIBLE_ROWS}"

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
        # Bypass cache: always read live from X-Plane so the active leg is never stale.
        idx = self._legs_fms_index_for_row(row)
        if idx < 0:
            return 0
        try:
            return 1 if idx == xp.getDestinationFMSEntry() else 0
        except Exception:
            return 0

    def _legs_read_row_is_selected(self, row: int) -> int:
        # Bypass cache: computed directly so every poll reflects the current selection
        # without a race between per-row drefs being polled at different instants.
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

    def _cmd_legs_previous(self):
        """Select previous waypoint within current visible 3-row page only (no page jump)."""
        count = self._read_fms_entry_count()
        if count <= 0:
            self.legs_selected = -1
            return
        start = self.legs_window_start
        end = min(start + self.LEGS_VISIBLE_ROWS, count)
        if end <= start:
            self.legs_selected = -1
            return
        if self.legs_selected < start or self.legs_selected >= end:
            self.legs_selected = start
        else:
            self.legs_selected = max(start, self.legs_selected - 1)

    def _cmd_legs_next(self):
        """Select next waypoint within current visible 3-row page only (no page jump)."""
        count = self._read_fms_entry_count()
        if count <= 0:
            self.legs_selected = -1
            return
        start = self.legs_window_start
        end = min(start + self.LEGS_VISIBLE_ROWS, count)
        if end <= start:
            self.legs_selected = -1
            return
        if self.legs_selected < start or self.legs_selected >= end:
            self.legs_selected = start
        else:
            self.legs_selected = min(end - 1, self.legs_selected + 1)

    def _cmd_legs_activate(self):
        try:
            count = xp.countFMSEntries()
            if count <= 0 or self.legs_selected < 0:
                return
            target = max(0, min(self.legs_selected, count - 1))
            xp.setDestinationFMSEntry(target)
            info = self._safe_fms_entry_info(target)
            ident = info.navAidID if info else "?"
            self._log("legs_activate:", target, ident)
        except Exception as exc:
            self._log("legs_activate error:", exc)

    def _cmd_legs_direct_to(self):
        try:
            count = xp.countFMSEntries()
            if count <= 0 or self.legs_selected < 0:
                return
            target = max(0, min(self.legs_selected, count - 1))
            fp = getattr(xp, "FMSFlightPlan_Active", getattr(xp, "ActiveFlightPlan", 0))
            if hasattr(xp, "setDirectToFMSFlightPlanEntry"):
                xp.setDirectToFMSFlightPlanEntry(fp, target)
            else:
                self._log("legs_direct_to: API xp.setDirectToFMSFlightPlanEntry not available (XP12 feature)")
                return
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
            new_count = count - 1
            if new_count <= 0:
                self.legs_selected = -1
                self.legs_window_start = 0
            else:
                # Clamp selection to valid range
                self.legs_selected = max(0, min(self.legs_selected, new_count - 1))
                # Clamp window to valid range; allow partial last page
                max_start = max(0, new_count - 1)
                self.legs_window_start = max(0, min(self.legs_window_start, max_start))
            self._log("legs_clear_selected: cleared", target, ident)
        except Exception as exc:
            self._log("legs_clear_selected error:", exc)

    def _cmd_legs_clear_from_here(self):
        """Clear from selected waypoint to end of route."""
        try:
            count = xp.countFMSEntries()
            if count <= 0 or self.legs_selected < 0:
                return
            target = max(0, min(self.legs_selected, count - 1))
            for i in range(count - 1, target - 1, -1):
                xp.clearFMSEntry(i)
            new_count = target
            if new_count <= 0:
                self.legs_selected = -1
                self.legs_window_start = 0
            else:
                self.legs_selected = min(self.legs_selected, new_count - 1)
                max_start = max(0, new_count - 1)
                self.legs_window_start = max(0, min(self.legs_window_start, max_start))
            self._log("legs_clear_from_here: cleared from", target, "count was", count)
        except Exception as exc:
            self._log("legs_clear_from_here error:", exc)

    def _cmd_legs_clear_all(self):
        """Clear entire FMS route."""
        try:
            self._clear_fms()
            self.legs_selected = -1
            self.legs_window_start = 0
            self._log("legs_clear_all")
        except Exception as exc:
            self._log("legs_clear_all error:", exc)

    def _cmd_legs_direct_to_destination(self):
        """Direct-to the last FMS entry (destination)."""
        try:
            count = xp.countFMSEntries()
            if count <= 0:
                return
            dest_idx = count - 1
            fp = getattr(xp, "FMSFlightPlan_Active", getattr(xp, "ActiveFlightPlan", 0))
            if hasattr(xp, "setDirectToFMSFlightPlanEntry"):
                xp.setDirectToFMSFlightPlanEntry(fp, dest_idx)
            else:
                xp.setDestinationFMSEntry(dest_idx)
            info = self._safe_fms_entry_info(dest_idx)
            ident = info.navAidID if info else "?"
            self._log("direct_to_destination:", dest_idx, ident)
        except Exception as exc:
            self._log("direct_to_destination error:", exc)

    # ── Procedures (DEP / ARR / APP) ───────────────────────────

    def _cifp_path(self, icao: str) -> Optional[str]:
        """Return path to CIFP file for airport, preferring Custom Data over default data."""
        system_path = xp.getSystemPath()
        for subdir in ("Custom Data", os.path.join("Resources", "default data")):
            path = os.path.join(system_path, subdir, "CIFP", f"{icao.upper()}.dat")
            if os.path.isfile(path):
                return path
        return None

    def _parse_cifp(self, icao: str) -> List[ProcedureInfo]:
        """Parse CIFP file for airport and return ProcedureInfo list (all modes)."""
        if icao in self._cifp_cache:
            return self._cifp_cache[icao]

        path = self._cifp_path(icao)
        if not path:
            self._log("No CIFP file for", icao)
            self._cifp_cache[icao] = []
            return []

        # Collect raw legs: {(proc_type, proc_name, transition): [(seq, fix_ident), ...]}
        raw: Dict[tuple, list] = {}
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.strip()
                    if not line or ":" not in line:
                        continue
                    rec_type, _, rest = line.partition(":")
                    rec_type = rec_type.strip()
                    if rec_type not in ("SID", "STAR", "APPCH"):
                        continue
                    rest = rest.rstrip(";")
                    fields = [fld.strip() for fld in rest.split(",")]
                    if len(fields) < 5:
                        continue
                    try:
                        seq = int(fields[0])
                    except (ValueError, TypeError):
                        seq = 0
                    proc_name = fields[2]
                    transition = fields[3]
                    fix_ident = fields[4]
                    if not proc_name or not fix_ident:
                        continue
                    pt = "APP" if rec_type == "APPCH" else rec_type
                    key = (pt, proc_name, transition)
                    raw.setdefault(key, []).append((seq, fix_ident))
        except Exception as exc:
            self._log("CIFP parse error", icao, exc)
            self._cifp_cache[icao] = []
            return []

        procedures: List[ProcedureInfo] = []

        for (pt, proc_name, transition), legs in sorted(raw.items()):
            # SID: only create entries for runway transitions (transition starts with "RW")
            # STAR: only create entries for named enroute transitions (non-blank transition)
            # APP: create one entry per (proc_name, transition) combination
            if pt == "SID" and not transition.startswith("RW"):
                continue
            if pt == "STAR" and not transition:
                continue

            # Waypoints: transition-specific legs (sorted by seq)
            t_legs = sorted(legs, key=lambda x: x[0])
            wpts = list(dict.fromkeys(fix for _, fix in t_legs if fix))

            # Merge in common-route legs (blank transition) if present
            common_key = (pt, proc_name, "")
            if common_key in raw:
                c_legs = sorted(raw[common_key], key=lambda x: x[0])
                c_wpts = list(dict.fromkeys(fix for _, fix in c_legs if fix))
                if pt == "SID":
                    # runway transition first, then common route
                    wpts = wpts + [w for w in c_wpts if w not in wpts]
                else:
                    # transition (enroute entry / IAF) leads into common route — transition first
                    wpts = wpts + [w for w in c_wpts if w not in wpts]

            if not wpts:
                continue

            if pt == "SID":
                rwy = transition[2:] if transition.startswith("RW") else transition
                display_name = f"{proc_name} {rwy}" if rwy else proc_name
                display_runway = rwy
            elif pt == "STAR":
                display_name = f"{proc_name} {transition}" if transition else proc_name
                display_runway = transition
            else:  # APP
                app_type = self._APP_TYPE_LABELS.get(proc_name[0], proc_name[0]) if proc_name else ""
                rwy = proc_name[1:] if len(proc_name) > 1 else proc_name
                display_name = f"{app_type} {rwy}".strip()
                display_runway = rwy

            procedures.append(ProcedureInfo(
                name=proc_name,
                proc_type=pt,
                transition=transition,
                waypoints=wpts,
                display_name=display_name,
                display_runway=display_runway,
            ))

        # Deduplicate APP entries: one per proc_name, preferring a non-blank transition (IAF)
        # so the loaded approach includes IAF→FAF→MAP, not just FAF→MAP.
        # If only a blank (common route) entry exists, fall back to that.
        raw_app_count = sum(1 for p in procedures if p.proc_type == "APP")
        self._log("CIFP pre-dedup APP count:", raw_app_count, "for", icao)
        app_rep: Dict[str, ProcedureInfo] = {}
        for p in procedures:
            if p.proc_type != "APP":
                continue
            existing = app_rep.get(p.name)
            if existing is None:
                app_rep[p.name] = p
            elif existing.transition == "" and p.transition != "":
                # Upgrade from common-route-only to a full IAF transition
                app_rep[p.name] = p
        deduped: List[ProcedureInfo] = []
        seen_app: set = set()
        for p in procedures:
            if p.proc_type != "APP":
                deduped.append(p)
            elif p.name not in seen_app:
                deduped.append(app_rep[p.name])
                seen_app.add(p.name)
        procedures = deduped

        self._cifp_cache[icao] = procedures
        by_type = {}
        for p in procedures:
            by_type[p.proc_type] = by_type.get(p.proc_type, 0) + 1
        self._log("CIFP parsed", icao, "->", len(procedures), "procedures", by_type)
        # Debug: log APP entries so we can diagnose missing approaches
        for p in procedures:
            if p.proc_type == "APP":
                self._log("  APP:", p.display_name, "trans=", p.transition, "wpts=", len(p.waypoints))
        return procedures

    def _proc_airport_for(self, kind: str) -> str:
        return self.proc_dep_icao if kind == "dep" else self.proc_dest_icao

    def _proc_invalidate_cache(self, kind: str) -> None:
        self._proc_cache_valid[kind] = False

    def _proc_selected_name(self, kind: str) -> str:
        idx = self._proc_index.get(kind, -1)
        procs = self._proc_procs.get(kind, [])
        return procs[idx].display_name if 0 <= idx < len(procs) else ""

    def _proc_selected_runway(self, kind: str) -> str:
        idx = self._proc_index.get(kind, -1)
        procs = self._proc_procs.get(kind, [])
        return procs[idx].display_runway if 0 <= idx < len(procs) else ""

    def _proc_window_page(self, kind: str) -> int:
        n = len(self._proc_procs.get(kind, []))
        if n <= 0:
            return 1
        return self._proc_window.get(kind, 0) // self.PROC_VISIBLE_ROWS + 1

    def _proc_list_page_str(self, kind: str) -> str:
        n = len(self._proc_procs.get(kind, []))
        if n <= 0:
            return ""
        page = self._proc_window.get(kind, 0) // self.PROC_VISIBLE_ROWS + 1
        total = (n + self.PROC_VISIBLE_ROWS - 1) // self.PROC_VISIBLE_ROWS
        return f"{page}/{total}"

    def _proc_sel_count_str(self, kind: str) -> str:
        n = len(self._proc_procs.get(kind, []))
        if n <= 0:
            return "0/0"
        idx = self._proc_index.get(kind, -1)
        w = self._proc_window.get(kind, 0)
        if idx >= 0:
            row_on_page = idx - w + 1
            if 1 <= row_on_page <= self.PROC_VISIBLE_ROWS:
                return f"{row_on_page}/{self.PROC_VISIBLE_ROWS}"
            return f"-/{self.PROC_VISIBLE_ROWS}"
        return f"-/{self.PROC_VISIBLE_ROWS}"

    def _proc_ensure_cache(self, kind: str) -> None:
        if self._proc_cache_valid.get(kind, False):
            return
        procs = self._proc_procs.get(kind, [])
        n = len(procs)
        w = self._proc_window.get(kind, 0)
        idx = self._proc_index.get(kind, -1)
        rows: Dict[int, Dict[str, object]] = {}
        for row in range(1, self.PROC_VISIBLE_ROWS + 1):
            pi = w + (row - 1)
            if pi < 0 or pi >= n:
                rows[row] = {"plan_index": -1, "index": "", "name": "", "runway": "", "is_selected": 0, "status": ""}
                continue
            proc = procs[pi]
            is_sel = int(idx >= 0 and pi == idx)
            rows[row] = {
                "plan_index": pi,
                "index": str(pi + 1),
                "name": proc.display_name,
                "runway": proc.display_runway,
                "is_selected": is_sel,
                "status": "SEL" if is_sel else "",
            }
        self._proc_rows_cache[kind] = rows
        self._proc_cache_valid[kind] = True

    def _proc_read_row_str(self, kind: str, row: int, field: str) -> str:
        self._proc_ensure_cache(kind)
        return str(self._proc_rows_cache.get(kind, {}).get(row, {}).get(field, ""))

    def _proc_read_row_int(self, kind: str, row: int, field: str) -> int:
        self._proc_ensure_cache(kind)
        val = self._proc_rows_cache.get(kind, {}).get(row, {}).get(field, 0)
        return val if isinstance(val, int) else 0

    def _proc_max_aligned_window_start(self, n: int) -> int:
        if n <= 0:
            return 0
        return ((n - 1) // self.PROC_VISIBLE_ROWS) * self.PROC_VISIBLE_ROWS

    def _proc_airports_from_fms(self) -> None:
        """Populate proc_dep_icao/proc_dest_icao from live FMS airport entries.

        Only updates if airports are not already known — avoids clobbering good values
        after a SID is loaded (which replaces the departure airport with SID fixes).
        Valid ICAO codes are exactly 4 characters; longer idents are SID/STAR fixes.
        """
        if self.proc_dep_icao and self.proc_dest_icao:
            return  # already have both airports; don't clobber with FMS fix names
        try:
            count = xp.countFMSEntries()
            if count <= 0:
                return
            dep_icao = self.proc_dep_icao
            dest_icao = self.proc_dest_icao
            if not dep_icao:
                for i in range(min(count, 6)):
                    info = xp.getFMSEntryInfo(i)
                    if getattr(info, "type", None) == xp.Nav_Airport:
                        icao = (getattr(info, "navAidID", "") or "").strip().upper()
                        if len(icao) == 4:
                            dep_icao = icao
                            break
            if not dest_icao:
                for i in range(count - 1, max(count - 7, -1), -1):
                    info = xp.getFMSEntryInfo(i)
                    if getattr(info, "type", None) == xp.Nav_Airport:
                        icao = (getattr(info, "navAidID", "") or "").strip().upper()
                        if len(icao) == 4:
                            dest_icao = icao
                            break
            if dep_icao or dest_icao:
                changed = (dep_icao != self.proc_dep_icao or dest_icao != self.proc_dest_icao)
                self.proc_dep_icao = dep_icao
                self.proc_dest_icao = dest_icao
                self._log("proc_airports_from_fms: dep=", dep_icao, "dest=", dest_icao)
                if changed:
                    self._proc_refresh()
        except Exception as exc:
            self._log("proc_airports_from_fms error:", exc)

    def _proc_refresh(self) -> None:
        """Reload procedures from CIFP for dep/dest airports."""
        dep = self.proc_dep_icao.strip().upper()
        dest = self.proc_dest_icao.strip().upper()

        dep_procs: List[ProcedureInfo] = self._parse_cifp(dep) if dep else []
        dest_procs: List[ProcedureInfo] = self._parse_cifp(dest) if dest else []

        self._proc_procs["dep"] = [p for p in dep_procs if p.proc_type == "SID"]
        self._proc_procs["arr"] = [p for p in dest_procs if p.proc_type == "STAR"]
        self._proc_procs["app"] = [p for p in dest_procs if p.proc_type == "APP"]

        for k in self.KINDS:
            self._proc_index[k] = -1
            self._proc_window[k] = 0
            self._proc_cache_valid[k] = False
            self._proc_status[k] = "READY"
            self._proc_splice_point[k] = -1
            self._proc_loaded[k] = ""

        self._log("proc_refresh: dep(SID)", len(self._proc_procs["dep"]),
                  "arr(STAR)", len(self._proc_procs["arr"]),
                  "app(APP)", len(self._proc_procs["app"]))

    def _proc_register_section(self, kind: str, dref_prefix: str, cmd_prefix: str) -> None:
        p = dref_prefix
        c = cmd_prefix
        self._register_live_string_dref("airport", lambda k=kind: self._proc_airport_for(k), prefix=p)
        self._register_live_string_dref("status", lambda k=kind: self._proc_status.get(k, ""), prefix=p)
        self._register_live_string_dref("loaded_name", lambda k=kind: self._proc_loaded.get(k, ""), prefix=p)
        self._register_live_string_dref("selected_name", lambda k=kind: self._proc_selected_name(k), prefix=p)
        self._register_live_string_dref("selected_runway", lambda k=kind: self._proc_selected_runway(k), prefix=p)
        self._register_live_string_dref("list_page", lambda k=kind: self._proc_list_page_str(k), prefix=p)
        self._register_live_string_dref("list_sel_count", lambda k=kind: self._proc_sel_count_str(k), prefix=p)
        self._register_live_int_dref("count", lambda k=kind: len(self._proc_procs.get(k, [])), prefix=p)
        self._register_live_int_dref("index", lambda k=kind: (self._proc_index.get(k, -1) + 1) if self._proc_index.get(k, -1) >= 0 else 0, prefix=p)
        self._register_live_int_dref("list_window_page", lambda k=kind: self._proc_window_page(k), prefix=p)
        for row in range(1, self.PROC_VISIBLE_ROWS + 1):
            self._register_live_string_dref(f"list_row_{row}_name", lambda k=kind, r=row: self._proc_read_row_str(k, r, "name"), prefix=p)
            self._register_live_string_dref(f"list_row_{row}_runway", lambda k=kind, r=row: self._proc_read_row_str(k, r, "runway"), prefix=p)
            self._register_live_string_dref(f"list_row_{row}_index", lambda k=kind, r=row: self._proc_read_row_str(k, r, "index"), prefix=p)
            self._register_live_string_dref(f"list_row_{row}_status", lambda k=kind, r=row: self._proc_read_row_str(k, r, "status"), prefix=p)
            self._register_live_int_dref(f"list_row_{row}_is_selected", lambda k=kind, r=row: self._proc_read_row_int(k, r, "is_selected"), prefix=p)
        self._create_command("scroll_up", f"Scroll {kind} procedure list up", lambda k=kind: self._cmd_proc_scroll_up(k), prefix=c)
        self._create_command("scroll_down", f"Scroll {kind} procedure list down", lambda k=kind: self._cmd_proc_scroll_down(k), prefix=c)
        self._create_command("select_row_1", f"Select {kind} row 1", lambda k=kind: self._cmd_proc_select_row(k, 1), prefix=c)
        self._create_command("select_row_2", f"Select {kind} row 2", lambda k=kind: self._cmd_proc_select_row(k, 2), prefix=c)
        self._create_command("select_row_3", f"Select {kind} row 3", lambda k=kind: self._cmd_proc_select_row(k, 3), prefix=c)
        self._create_command("previous", f"Select previous {kind} procedure", lambda k=kind: self._cmd_proc_previous(k), prefix=c)
        self._create_command("next", f"Select next {kind} procedure", lambda k=kind: self._cmd_proc_next(k), prefix=c)
        self._create_command("clear_selected", f"Clear {kind} selection", lambda k=kind: self._cmd_proc_clear_selected(k), prefix=c)
        self._create_command("activate", f"Insert selected {kind} procedure into FMS", lambda k=kind: self._cmd_proc_activate(k), prefix=c)
        self._create_command("refresh", f"Reload {kind} procedures from CIFP", lambda k=kind: self._cmd_proc_refresh(k), prefix=c)

    # ── PROC command handlers ──

    def _cmd_proc_scroll_up(self, kind: str) -> None:
        procs = self._proc_procs.get(kind, [])
        if not procs:
            return
        w = self._proc_window.get(kind, 0)
        new_start = max(0, w - self.PROC_VISIBLE_ROWS)
        if new_start != w:
            self._proc_window[kind] = new_start
            self._proc_index[kind] = -1
            self._proc_invalidate_cache(kind)
            self._log(f"proc_scroll_up({kind}) ->", new_start)

    def _cmd_proc_scroll_down(self, kind: str) -> None:
        procs = self._proc_procs.get(kind, [])
        n = len(procs)
        if n <= 0:
            return
        w = self._proc_window.get(kind, 0)
        max_w = self._proc_max_aligned_window_start(n)
        new_start = min(w + self.PROC_VISIBLE_ROWS, max_w)
        if new_start != w:
            self._proc_window[kind] = new_start
            self._proc_index[kind] = -1
            self._proc_invalidate_cache(kind)
            self._log(f"proc_scroll_down({kind}) ->", new_start)

    def _cmd_proc_select_row(self, kind: str, row: int) -> None:
        procs = self._proc_procs.get(kind, [])
        w = self._proc_window.get(kind, 0)
        pi = w + (row - 1)
        if not procs or pi < 0 or pi >= len(procs):
            return
        self._proc_index[kind] = pi
        self._proc_invalidate_cache(kind)
        self._log(f"proc_select_row({kind}, {row}) -> index={pi}")

    def _cmd_proc_previous(self, kind: str) -> None:
        procs = self._proc_procs.get(kind, [])
        if not procs:
            return
        w = self._proc_window.get(kind, 0)
        idx = self._proc_index.get(kind, -1)
        end = min(w + self.PROC_VISIBLE_ROWS, len(procs))
        if idx < w or idx >= end:
            self._proc_index[kind] = end - 1
        else:
            self._proc_index[kind] = max(w, idx - 1)
        self._proc_invalidate_cache(kind)
        self._log(f"proc_previous({kind}) -> index={self._proc_index[kind]}")

    def _cmd_proc_next(self, kind: str) -> None:
        procs = self._proc_procs.get(kind, [])
        if not procs:
            return
        w = self._proc_window.get(kind, 0)
        idx = self._proc_index.get(kind, -1)
        end = min(w + self.PROC_VISIBLE_ROWS, len(procs))
        if idx < w or idx >= end:
            self._proc_index[kind] = w
        else:
            self._proc_index[kind] = min(end - 1, idx + 1)
        self._proc_invalidate_cache(kind)
        self._log(f"proc_next({kind}) -> index={self._proc_index[kind]}")

    def _cmd_proc_clear_selected(self, kind: str) -> None:
        self._proc_index[kind] = -1
        self._proc_invalidate_cache(kind)
        self._log(f"proc_clear_selected({kind})")

    def _cmd_proc_refresh(self, kind: str) -> None:
        self._cifp_cache.clear()
        if not self.proc_dep_icao and not self.proc_dest_icao:
            self._proc_airports_from_fms()
        else:
            self._proc_refresh()

    def _cmd_proc_activate(self, kind: str) -> None:
        """Insert selected procedure waypoints into the FMS."""
        procs = self._proc_procs.get(kind, [])
        idx = self._proc_index.get(kind, -1)
        if idx < 0 or idx >= len(procs):
            self._log(f"proc_activate({kind}): nothing selected")
            return
        proc = procs[idx]
        if not proc.waypoints:
            self._log(f"proc_activate({kind}): no waypoints for", proc.display_name)
            return

        # Look up airport coordinates to constrain navaid searches to the correct region.
        apt_lat, apt_lon = None, None
        apt_icao = self._proc_airport_for(kind)
        if apt_icao:
            apt_ref = xp.findNavAid(None, apt_icao, None, None, None, xp.Nav_Airport)
            if apt_ref != xp.NAV_NOT_FOUND:
                try:
                    apt_info = xp.getNavAidInfo(apt_ref)
                    apt_lat = apt_info.latitude
                    apt_lon = apt_info.longitude
                except Exception:
                    pass

        # Resolve nav refs for all procedure waypoints, constraining by airport lat/lon.
        proc_nav = []
        for ident in proc.waypoints:
            ref = xp.findNavAid(None, ident, apt_lat, apt_lon, None, xp.Nav_Fix)
            if ref == xp.NAV_NOT_FOUND:
                ref = xp.findNavAid(None, ident, apt_lat, apt_lon, None, xp.Nav_VOR)
            if ref == xp.NAV_NOT_FOUND:
                ref = xp.findNavAid(None, ident, apt_lat, apt_lon, None, xp.Nav_NDB)
            if ref == xp.NAV_NOT_FOUND:
                ref = xp.findNavAid(None, ident, None, None, None, xp.Nav_Airport)
            proc_nav.append((ref, ident))

        write_idx = 0
        try:
            if proc.proc_type == "SID":
                # Prepend: snapshot current FMS, clear, write SID then existing entries
                count = xp.countFMSEntries()
                existing = [xp.getFMSEntryInfo(i) for i in range(count)]
                self._clear_fms()
                for ref, ident in proc_nav:
                    if ref != xp.NAV_NOT_FOUND:
                        xp.setFMSEntryInfo(write_idx, ref, 0)
                        write_idx += 1
                for info in existing:
                    try:
                        lat = getattr(info, "lat", getattr(info, "latitude", 0.0))
                        lon = getattr(info, "lon", getattr(info, "longitude", 0.0))
                        nav_id = getattr(info, "navAidID", "").strip()
                        if getattr(info, "type", None) == xp.Nav_LatLon or not nav_id:
                            xp.setFMSEntryLatLon(write_idx, lat, lon, info.altitude)
                        else:
                            ref = xp.findNavAid(None, nav_id, lat, lon, None, xp.Nav_Fix)
                            if ref == xp.NAV_NOT_FOUND:
                                ref = xp.findNavAid(None, nav_id, lat, lon, None, xp.Nav_VOR)
                            if ref != xp.NAV_NOT_FOUND:
                                xp.setFMSEntryInfo(write_idx, ref, info.altitude)
                            else:
                                xp.setFMSEntryLatLon(write_idx, lat, lon, info.altitude)
                        write_idx += 1
                    except Exception:
                        pass
                # SID clears any stored arr/app splice points — plan structure changed
                self._proc_splice_point["arr"] = -1
                self._proc_splice_point["app"] = -1
            else:
                # STAR or APP: replace previous insertion of same kind if splice point known,
                # otherwise append at end. Snapshot entries up to splice point, rewrite, then
                # write new procedure waypoints starting at splice point.
                splice = self._proc_splice_point.get(kind, -1)
                count = xp.countFMSEntries()
                if 0 <= splice <= count:
                    keep = [xp.getFMSEntryInfo(i) for i in range(splice)]
                    self._clear_fms()
                    write_idx = 0
                    for info in keep:
                        try:
                            lat = getattr(info, "lat", getattr(info, "latitude", 0.0))
                            lon = getattr(info, "lon", getattr(info, "longitude", 0.0))
                            nav_id = getattr(info, "navAidID", "").strip()
                            if getattr(info, "type", None) == xp.Nav_LatLon or not nav_id:
                                xp.setFMSEntryLatLon(write_idx, lat, lon, info.altitude)
                            else:
                                ref = xp.findNavAid(None, nav_id, lat, lon, None, xp.Nav_Fix)
                                if ref == xp.NAV_NOT_FOUND:
                                    ref = xp.findNavAid(None, nav_id, lat, lon, None, xp.Nav_VOR)
                                if ref != xp.NAV_NOT_FOUND:
                                    xp.setFMSEntryInfo(write_idx, ref, info.altitude)
                                else:
                                    xp.setFMSEntryLatLon(write_idx, lat, lon, info.altitude)
                            write_idx += 1
                        except Exception:
                            pass
                else:
                    write_idx = xp.countFMSEntries()
                self._proc_splice_point[kind] = write_idx
                for ref, ident in proc_nav:
                    if ref != xp.NAV_NOT_FOUND:
                        xp.setFMSEntryInfo(write_idx, ref, 0)
                        write_idx += 1

            self._legs_init_after_load()
            self._proc_loaded[kind] = proc.display_name
            self._proc_status[kind] = f"LOADED {proc.display_name}"
            self._log(f"proc_activate({kind}):", proc.proc_type, proc.display_name,
                      "waypoints=", len(proc.waypoints), "written=", write_idx)
        except Exception as exc:
            self._proc_status[kind] = f"ERR {exc}"
            self._log(f"proc_activate({kind}) error:", exc)

        self._proc_invalidate_cache(kind)
