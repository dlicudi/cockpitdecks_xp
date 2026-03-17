import math
import os
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
    DESC = "Browse Output/FMS plans and load selected plan into default FMS"
    RELEASE = "2.0.0"

    DREF_PREFIX = "cockpitdecks/fms_browser"
    CMD_PREFIX = "cockpitdecks/fms_browser"

    LEGS_DREF_PREFIX = "cockpitdecks/fms_legs"
    LEGS_CMD_PREFIX = "cockpitdecks/fms_legs"
    LEGS_VISIBLE_ROWS = 3

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
        self.index = 0
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
        self.legs_selected = 0      # 0-based FMS entry index
        self.legs_window_start = 0  # 0-based first visible row

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

    def _log(self, *parts):
        if self.trace:
            print(self.info, *parts)

    def XPluginStart(self):
        self._log("XPluginStart")

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
        self._create_command("wp_direct", "Direct-to displayed FMS waypoint", self._cmd_wp_direct)
        self._create_command("clear_fms_entry", "Clear displayed FMS entry", self._cmd_clear_fms_entry)
        self._create_command("map_range_down", "Map range zoom in", self._cmd_map_range_down)
        self._create_command("map_range_up", "Map range zoom out", self._cmd_map_range_up)
        self._create_command("map_toggle", "Toggle map range target", self._cmd_map_toggle)

        self.fpl_cmd = xp.findCommand("sim/GPS/g1000n1_fpl")
        self._log("findCommand sim/GPS/g1000n1_fpl ->", self.fpl_cmd)

        for mode, (down_cmd, up_cmd) in self.map_range_cmds.items():
            self.map_cmd_refs[mode] = (xp.findCommand(down_cmd), xp.findCommand(up_cmd))
            self._log("findCommand map range", mode, down_cmd, "->", self.map_cmd_refs[mode][0],
                      up_cmd, "->", self.map_cmd_refs[mode][1])

        self._register_legs_drefs()
        self._create_legs_commands()

        self._refresh_plan_list()
        self._publish_state()
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
        self.commands[suffix] = {"ref": cmd_ref, "fun": handler}

    def _set_status(self, text: str, error: str = ""):
        self.last_status = text
        self.last_error = error
        self.string_values["status"] = text
        self.string_values["last_error"] = error

    def _selected_plan(self) -> Optional[FlightPlanInfo]:
        if not self.plans:
            return None
        if self.index < 0:
            self.index = 0
        if self.index >= len(self.plans):
            self.index = 0
        return self.plans[self.index]

    def _publish_state(self):
        plan = self._selected_plan()

        self.int_values["count"] = len(self.plans)
        self.int_values["index"] = self.index + 1 if self.plans else 0
        self.int_values["loaded"] = int(self.loaded)

        if plan is None:
            self.string_values["plan_name"] = "No flight plans"
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

    # ── File browser ──

    def _plans_dir(self) -> str:
        system_path = xp.getSystemPath()
        return os.path.join(system_path, "Output", "FMS plans")

    def _refresh_plan_list(self):
        plans_dir = self._plans_dir()
        self._log("Refreshing plans from", plans_dir)

        self.plans = []
        self.loaded = 0

        if not os.path.isdir(plans_dir):
            self._set_status("NO DIR", f"Missing folder: {plans_dir}")
            self._publish_state()
            return

        filenames = sorted(
            [f for f in os.listdir(plans_dir) if f.lower().endswith(".fms")]
        )

        for filename in filenames:
            full_path = os.path.join(plans_dir, filename)
            info = self._parse_fms_file(full_path)
            if info is not None:
                self.plans.append(info)

        if not self.plans:
            self.index = 0
            self._set_status("EMPTY")
        else:
            if self.index >= len(self.plans):
                self.index = 0
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
        if not self.plans:
            self._set_status("EMPTY")
            self._publish_state()
            return
        self.index = (self.index - 1) % len(self.plans)
        self._set_status("READY")
        self._publish_state()

    def _cmd_next(self):
        if not self.plans:
            self._set_status("EMPTY")
            self._publish_state()
            return
        self.index = (self.index + 1) % len(self.plans)
        self._set_status("READY")
        self._publish_state()

    def _cmd_refresh(self):
        self._refresh_plan_list()

    def _cmd_load(self):
        plan = self._selected_plan()
        if plan is None:
            self.loaded = 0
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

    def _register_legs_drefs(self):
        p = self.LEGS_DREF_PREFIX
        # Global state
        self._register_live_int_dref("selected_index", self._legs_read_selected_index, prefix=p)
        self._register_live_int_dref("active_index", self._legs_read_active_index, prefix=p)
        self._register_live_int_dref("entry_count", self._legs_read_entry_count, prefix=p)
        self._register_live_int_dref("window_start", self._legs_read_window_start, prefix=p)
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
        self._create_command("direct_to", "Direct-to selected LEGS waypoint", self._cmd_legs_direct_to, prefix=p)

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
            self.legs_selected = 0
            self.legs_window_start = 0
            return
        self.legs_selected = max(0, min(self.legs_selected, count - 1))
        if self.legs_selected < self.legs_window_start:
            self.legs_window_start = self.legs_selected
        elif self.legs_selected >= self.legs_window_start + self.LEGS_VISIBLE_ROWS:
            self.legs_window_start = self.legs_selected - self.LEGS_VISIBLE_ROWS + 1
        max_start = max(0, count - self.LEGS_VISIBLE_ROWS)
        self.legs_window_start = max(0, min(self.legs_window_start, max_start))

    def _legs_init_after_load(self):
        """Set LEGS selection to active leg after a plan load."""
        try:
            count = xp.countFMSEntries()
            if count <= 0:
                self.legs_selected = 0
                self.legs_window_start = 0
                return
            active = xp.getDestinationFMSEntry()
            # Default to active leg; if active is last entry (destination),
            # prefer first entry instead (spec: not destination)
            if active >= count - 1 and count > 1:
                self.legs_selected = 0
            else:
                self.legs_selected = max(0, min(active, count - 1))
            self._legs_ensure_visible()
            self._log("legs_init_after_load: selected=", self.legs_selected,
                      "window=", self.legs_window_start, "count=", count)
        except Exception as exc:
            self._log("legs_init_after_load error:", exc)
            self.legs_selected = 0
            self.legs_window_start = 0

    # ── LEGS dataref readers ──

    def _legs_read_selected_index(self) -> int:
        return self.legs_selected + 1  # 1-based for display

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

    def _legs_read_row_ident(self, row: int) -> str:
        idx = self._legs_fms_index_for_row(row)
        if idx < 0:
            return ""
        info = self._safe_fms_entry_info(idx)
        return info.navAidID if info else ""

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
        count = self._read_fms_entry_count()
        if count <= 0:
            return
        if self.legs_window_start > 0:
            self.legs_window_start -= 1
            # Keep selection within visible window
            if self.legs_selected >= self.legs_window_start + self.LEGS_VISIBLE_ROWS:
                self.legs_selected = self.legs_window_start + self.LEGS_VISIBLE_ROWS - 1
            self._log("legs_scroll_up: window=", self.legs_window_start, "selected=", self.legs_selected)

    def _cmd_legs_scroll_down(self):
        count = self._read_fms_entry_count()
        if count <= 0:
            return
        max_start = max(0, count - self.LEGS_VISIBLE_ROWS)
        if self.legs_window_start < max_start:
            self.legs_window_start += 1
            # Keep selection within visible window
            if self.legs_selected < self.legs_window_start:
                self.legs_selected = self.legs_window_start
            self._log("legs_scroll_down: window=", self.legs_window_start, "selected=", self.legs_selected)

    def _cmd_legs_direct_to(self):
        try:
            count = xp.countFMSEntries()
            if count <= 0:
                return
            target = max(0, min(self.legs_selected, count - 1))
            xp.setDestinationFMSEntry(target)
            info = self._safe_fms_entry_info(target)
            ident = info.navAidID if info else "?"
            self._log("legs_direct_to:", target, ident)
        except Exception as exc:
            self._log("legs_direct_to error:", exc)
