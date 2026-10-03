"""
Rich TUI for latzero-server  —  prompt_toolkit + FormattedTextControl.
"""

import asyncio
import json
import os
import time
from contextlib import suppress
from typing import Dict, List, Optional, Tuple

from prompt_toolkit.application import Application
from prompt_toolkit.formatted_text import FormattedText
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, Layout, VSplit
from prompt_toolkit.layout.containers import Window
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.layout.dimension import D
from prompt_toolkit.styles import Style
from prompt_toolkit.widgets import Box, Frame

from .server import LatZeroServer

# ---------------------------------------------------------------------------
# Image → half-block art helper
# ---------------------------------------------------------------------------

# Background colour of the TUI (matches style "": "bg:#0d0c0a …")
_BG_RGB = (13, 12, 10)

# Logo tint – applied to any non-transparent pixel (orange from the palette)
_LOGO_TINT = (240, 136, 62)   # #f0883e


def _hex_to_rgb(h: str) -> Tuple[int, int, int]:
    h = h.lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def _composite(pixel_rgba, bg: Tuple[int,int,int]) -> Tuple[int,int,int]:
    """Alpha-composite an RGBA pixel over bg, then tint toward _LOGO_TINT."""
    r, g, b, a = pixel_rgba
    # composite over bg
    t = a / 255.0
    cr = int(r * t + bg[0] * (1 - t))
    cg = int(g * t + bg[1] * (1 - t))
    cb = int(b * t + bg[2] * (1 - t))
    # tint: push non-bg pixels toward the logo colour
    if t > 0.05:
        mix = t * 0.85          # strength of tint
        cr = int(cr * (1 - mix) + _LOGO_TINT[0] * mix)
        cg = int(cg * (1 - mix) + _LOGO_TINT[1] * mix)
        cb = int(cb * (1 - mix) + _LOGO_TINT[2] * mix)
    return cr, cg, cb


def _build_logo_art(
    path: str,
    target_cols: int = 36,
    target_rows: int = 6,       # terminal rows (each uses 2 image rows)
) -> Optional[List[Tuple[str, str]]]:
    """Return a FormattedText-style list for the logo, or None on failure."""
    try:
        from PIL import Image
    except ImportError:
        return None

    try:
        img = Image.open(path).convert("RGBA")
    except Exception:
        return None

    # Resize to (target_cols, target_rows * 2) preserving aspect ratio
    target_h = target_rows * 2
    resampling = getattr(Image, "Resampling", Image).LANCZOS
    img.thumbnail((target_cols, target_h), resampling)
    img = img.resize((target_cols, target_h), resampling)

    pixels = img.load()
    w, h = img.size

    result: List[Tuple[str, str]] = []
    for row in range(0, h - 1, 2):          # step 2 image rows per terminal row
        for col in range(w):
            top    = _composite(pixels[col, row],     _BG_RGB)
            bottom = _composite(pixels[col, row + 1], _BG_RGB)

            fg = "#{:02x}{:02x}{:02x}".format(*top)
            bg = "#{:02x}{:02x}{:02x}".format(*bottom)
            style = f"fg:{fg} bg:{bg}"
            result.append((style, "▀"))
        result.append(("", "\n"))

    return result


# Build at module load so it happens once.
_LOGO_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logo.png")
_LOGO_ART: Optional[List[Tuple[str, str]]] = _build_logo_art(_LOGO_PATH)

# ---------------------------------------------------------------------------
# Type alias
FT = List[Tuple[str, str]]
# ---------------------------------------------------------------------------


def _c(cls: str, text: str) -> FT:
    return [(cls, text)]


def _nl() -> FT:
    return [("", "\n")]


def cat(*parts: FT) -> FT:
    r: FT = []
    for p in parts:
        r.extend(p)
    return r


# ---------------------------------------------------------------------------
# Colour shortcuts
# ---------------------------------------------------------------------------
def cyan(t):    return _c("class:cyan",     t)
def green(t):   return _c("class:green",    t)
def red(t):     return _c("class:red",      t)
def yellow(t):  return _c("class:yellow",   t)
def blue(t):    return _c("class:blue",     t)
def orange(t):  return _c("class:orange",   t)
def white(t):   return _c("class:white",    t)
def muted(t):   return _c("class:muted",    t)
def dim(t):     return _c("class:dim",      t)
def bold_c(t):  return _c("class:cyan bold",t)
def sel(t):     return _c("class:selected", t)
def kw(t):      return _c("class:key",      t)
def kdesc(t):   return _c("class:kdesc",    t)


# ---------------------------------------------------------------------------
# Unicode Helpers
# ---------------------------------------------------------------------------

def sparkline(data: List[float], width: int = 10) -> FT:
    """Render a tiny unicode sparkline."""
    if not data:
        return [("class:dim", " " * width)]
    bars = " ▂▃▄▅▆▇█"
    min_v, max_v = min(data), max(data)
    rng = (max_v - min_v) or 1
    
    result = []
    # Take last N items
    window = list(data)[-width:]
    # Pad if needed
    window = [min_v] * (width - len(window)) + window
    
    for v in window:
        idx = int(((v - min_v) / rng) * (len(bars) - 1))
        result.append(("class:cyan", bars[idx]))
    return result

def activity_dot(active: bool) -> FT:
    """Pulsing dot helper."""
    if active:
        # We simulate pulse by checking time
        pulse = int(time.time() * 2) % 2 == 0
        return [("class:green" if pulse else "class:dim", "●")]
    return [("class:dim", "○")]

# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

class ServerDashboard:
    """High-fidelity infrastructure control plane for LatZero."""

    PANES = ("pools", "clients", "processes", "buffers", "events", "workers", "queue")

    def __init__(self, server: LatZeroServer, refresh_interval: float = 0.5):
        self.server = server
        self.refresh_interval = refresh_interval
        self.active_pane = "pools"
        self._sel: Dict[str, int] = {p: 0 for p in self.PANES}
        self._running = True
        self._snap: dict = {}
        self._cache: Dict[str, FT] = {}
        
        # History for sparklines
        self._history: Dict[str, List[float]] = {
            "tps": [],
            "latency": [],
            "queue_depth": [],
            "worker_count": [],
        }

        def mk(key): return FormattedTextControl(lambda k=key: self._cache.get(k, [("", "")]), focusable=False)

        self._ctrls = {p: mk(p) for p in self.PANES}
        self._ctrls["header"] = FormattedTextControl(self._render_header, focusable=False)
        self._ctrls["detail"] = mk("detail")
        self._ctrls["footer"] = FormattedTextControl(self._render_footer, focusable=False)

        self.application = Application(
            layout=self._build_layout(),
            key_bindings=self._build_keys(),
            full_screen=True,
            mouse_support=False,
            style=self._build_style(),
        )

    # ------------------------------------------------------------------
    # Style
    # ------------------------------------------------------------------

    def _build_style(self) -> Style:
        return Style.from_dict({
            "":              "bg:#0d0c0a fg:#d4cfc7",
            "frame.border":  "fg:#3d3630",
            "frame.border.active": "fg:#f0883e",
            "frame.label":   "fg:#766e66",
            "frame.label.active": "fg:#ff9e64 bold",

            "area-header":   "bg:#0a0908",
            "area-pane":     "bg:#12100e",
            "area-detail":   "bg:#12100e",
            "area-footer":   "bg:#0a0908",

            "cyan":          "fg:#ff9e64",
            "cyan bold":     "fg:#ff9e64 bold",
            "green":         "fg:#3fb950",
            "red":           "fg:#f85149",
            "yellow":        "fg:#d29922",
            "blue":          "fg:#e8883a",
            "orange":        "fg:#f0883e",
            "white":         "fg:#f0ede8",
            "muted":         "fg:#8b8680",
            "dim":           "fg:#4d4640",

            "selected":      "bg:#ff9e64 fg:#0d0c0a bold",
            "hotkey.bracket": "fg:#f0883e bold",
            "key":           "fg:#f0ede8 bold",
            "kdesc":         "fg:#766e66",

            "ev-info":       "fg:#3fb950",
            "ev-warn":       "fg:#d29922",
            "ev-error":      "fg:#f85149",
            "ev-debug":      "fg:#584f48",

            "pool-name":     "fg:#ff9e64 bold",
            "client-id":     "fg:#ffb86c",
            "proc-id":       "fg:#ffe0b2",
            "buf-key":       "fg:#ffa657",
            "stat-lbl":      "fg:#4d4640",
            "stat-val":      "fg:#ffa657 bold",
        })

    # ------------------------------------------------------------------
    # Layout
    # ------------------------------------------------------------------

    def _make_win(self, key: str, style: str, **kw) -> Window:
        return Window(content=self._ctrls[key], style=style, wrap_lines=False, **kw)

    def _build_layout(self) -> Layout:
        def _box(key, title_text, **kw):
            def _title():
                is_active = self.active_pane == key
                style = "class:frame.label.active" if is_active else "class:frame.label"
                marker = "● " if is_active else ""
                return [(style, f" {marker}{title_text} ")]
            
            return Frame(
                self._make_win(key, "class:area-pane"),
                title=_title,
                style="class:frame.border",
                **kw
            )

        top_row = VSplit([
            _box("pools",     " Pools ",     width=D(preferred=28)),
            _box("clients",   " Clients ",   width=D(preferred=30)),
            _box("processes", " Processes ", width=D(preferred=38)),
            _box("buffers",   " Buffers ",   width=D(preferred=36)),
        ])

        scaling_row = VSplit([
            _box("workers", " Workers ",     width=D(preferred=50)),
            _box("queue",   " Queue ",       width=D(preferred=46)),
        ])

        body = HSplit([
            top_row,
            scaling_row,
            VSplit([
                _box("events", " Events Log "),
                _box("detail", " Inspector ", width=D(preferred=60)),
            ])
        ])

        root = HSplit([
            Window(content=self._ctrls["header"], height=2, style="class:area-header"),
            Window(height=1, char=" "), # spacer
            body,
            self._make_win("footer", "class:area-footer", height=1),
        ])
        return Layout(root)

    # ------------------------------------------------------------------
    # Key bindings
    # ------------------------------------------------------------------

    def _build_keys(self) -> KeyBindings:
        kb = KeyBindings()

        # Quit on Q (shift+q) and ctrl+c — freeing lowercase q for the Queue pane
        @kb.add("Q")
        @kb.add("c-c")
        def _quit(ev):
            self._running = False
            ev.app.exit()

        for key, pane in [
            ("p", "pools"), ("c", "clients"), ("x", "processes"),
            ("b", "buffers"), ("e", "events"), ("w", "workers"), ("q", "queue"),
        ]:
            @kb.add(key)
            def _jump(ev, p=pane):
                self.active_pane = p
                self._refresh()

        @kb.add("tab")
        @kb.add("right")
        def _next(ev):
            i = self.PANES.index(self.active_pane)
            self.active_pane = self.PANES[(i + 1) % len(self.PANES)]
            self._refresh()

        @kb.add("s-tab")
        @kb.add("left")
        def _prev(ev):
            i = self.PANES.index(self.active_pane)
            self.active_pane = self.PANES[(i - 1) % len(self.PANES)]
            self._refresh()

        @kb.add("down")
        @kb.add("j")
        def _dn(ev):
            self._move(1)
            self._refresh()

        @kb.add("up")
        @kb.add("k")
        def _up(ev):
            self._move(-1)
            self._refresh()

        @kb.add("r")
        def _r(ev):
            self._refresh()

        return kb

    # ------------------------------------------------------------------
    # Selection navigation
    # ------------------------------------------------------------------

    def _move(self, delta: int) -> None:
        snap = self._snap
        p = self.active_pane
        if p == "pools":
            n = max(len(snap.get("pools", [])), 1)
        elif p == "clients":
            n = max(len(self._sel_pool().get("clients", [])), 1)
        elif p == "processes":
            n = max(len(self._sel_pool().get("processes", {})), 1)
        elif p == "buffers":
            n = max(len(self._sel_pool().get("buffers", [])), 1)
        elif p == "workers":
            n = max(len(snap.get("scale_events", [])), 1)
        elif p == "queue":
            n = max(len(self._history.get("queue_depth", [])), 1)
        else:
            n = max(len(snap.get("events", [])), 1)
        self._sel[p] = (self._sel[p] + delta) % n

    def _sel_pool(self) -> dict:
        pools = self._snap.get("pools", [])
        if not pools:
            return {}
        return pools[min(self._sel["pools"], len(pools) - 1)]

    # ------------------------------------------------------------------
    # Refresh
    # ------------------------------------------------------------------

    def _refresh(self) -> None:
        self._snap = self.server.get_dashboard_snapshot()
        
        # Update history
        self._history["tps"].append(self._snap.get("tps", 0))
        self._history["latency"].append(self._snap.get("avg_latency", 0))
        self._history["queue_depth"].append(self._snap.get("queue_depth", 0))
        self._history["worker_count"].append(self._snap.get("worker_count", 0))
        for k in self._history:
            if len(self._history[k]) > 100:
                self._history[k].pop(0)

        pool = self._sel_pool()
        self._cache["pools"]     = self._r_pools(self._snap.get("pools", []))
        self._cache["clients"]   = self._r_clients(pool)
        self._cache["processes"] = self._r_processes(pool)
        self._cache["buffers"]   = self._r_buffers(pool)
        self._cache["events"]    = self._r_events(self._snap.get("events", []))
        self._cache["detail"]    = self._r_detail(pool)
        self._cache["workers"]   = self._r_workers(self._snap)
        self._cache["queue"]     = self._r_queue(self._snap)
        self.application.invalidate()

    # ------------------------------------------------------------------
    # Header
    # ------------------------------------------------------------------

    def _render_header(self) -> FT:
        snap = self._snap
        uptime = max(0, int(time.time() - snap.get("started_at", time.time())))
        h, rem = divmod(uptime, 3600)
        m, s = divmod(rem, 60)
        ut = f"{h:02d}:{m:02d}:{s:02d}"

        mem = f"{snap.get('memory_rss', 0) / 1024 / 1024:.1f}MB"
        tps = f"{snap.get('tps', 0):.1f}"
        lat = f"{snap.get('avg_latency', 0) * 1000:.2f}ms"
        wkr = snap.get('worker_count', 0)
        wkr_max = snap.get('worker_max', 0)
        qdepth = snap.get('queue_depth', 0)
        pred = snap.get('predicted_queue_depth', 0.0)
        conf = snap.get('prediction_confidence', 0.0)

        # Trend indicators
        def trend(hist, lower_is_better=False):
            if len(hist) < 2: return dim("→")
            if hist[-1] > hist[-2]: return red("↑") if lower_is_better else green("↑")
            if hist[-1] < hist[-2]: return green("↓") if lower_is_better else red("↓")
            return dim("→")

        tps_arrow = trend(self._history["tps"])
        lat_arrow = trend(self._history["latency"], lower_is_better=True)
        q_arrow   = trend(self._history["queue_depth"], lower_is_better=True)
        w_arrow   = trend(self._history["worker_count"])

        # Prediction confidence arrow
        if conf > 0.7:
            pred_indicator = green(f"~{pred:.0f}")
        elif conf > 0.4:
            pred_indicator = yellow(f"~{pred:.0f}")
        else:
            pred_indicator = dim(f"~{pred:.0f}")

        return cat(
            [("class:orange bold", " LatØ ")], [("class:dim", "Dashboard  ")],
            [("class:dim", f"  {snap.get('host','?')}:{snap.get('port','?')}  ")],
            [("", " " * 2)],
            dim("[ "), muted("TPS "), cyan(f"{tps:>5} "), tps_arrow, dim(" ]  [ "),
            muted("LAT "), cyan(f"{lat:>8} "), lat_arrow, dim(" ]  [ "),
            muted("MEM "), cyan(f"{mem:>7} "), dim(" ]  [ "),
            muted("WKR "), cyan(f"{wkr}/{wkr_max} "), w_arrow, dim(" ]  [ "),
            muted("Q "), cyan(f"{qdepth} "), q_arrow, dim(" ] "),
            [("", "\n")],
            [("", "  ")], sparkline(self._history["tps"], 20),
            [("", " " * 4)], sparkline(self._history["latency"], 20),
            [("", " " * 4)], sparkline(self._history["queue_depth"], 20),
            [("", " ")], dim(" pred:"), pred_indicator,
        )

    # ------------------------------------------------------------------
    # Footer
    # ------------------------------------------------------------------

    def _render_footer(self) -> FT:
        keys = [
            ("P", "pools"),("C", "clients"),("X", "procs"),
            ("B", "buffers"),("E", "events"),("W", "workers"),("Q", "queue"),
            ("R", "refresh"),("⇧Q", "quit"),
        ]
        res: FT = [("class:dim", "  PANE: "), ("class:cyan bold", self.active_pane.upper()), ("class:dim", "   ")]
        for k, d in keys:
            res.extend([("class:hotkey.bracket", "["), ("class:key", k), ("class:hotkey.bracket", "]"), ("class:kdesc", f" {d}  ")])
        return res

    # ------------------------------------------------------------------
    # Panes
    # ------------------------------------------------------------------

    def _r_pools(self, pools: list) -> FT:
        if not pools:
            return cat(_nl(), dim("  No pools connected.\n\n  waiting_for_activity... "), activity_dot(True))
        
        result: FT = []
        active = self.active_pane == "pools"
        for i, pool in enumerate(pools):
            is_sel = active and i == self._sel["pools"]
            style = "class:selected" if is_sel else "class:pool-name"
            
            result.extend([("", " ")])
            result.extend([(style, f" {pool['pool_id']:<24} ")])
            result.extend(_nl())
            result.extend(dim(f"  {len(pool['clients']):>2} clients  {len(pool['buffers']):>2} bufs "))
            result.extend(_nl())
        return result

    def _r_clients(self, pool: dict) -> FT:
        clients = pool.get("clients", [])
        if not clients:
            return cat(_nl(), dim("  No clients in pool.\n\n  waiting_for_clients... "), activity_dot(True))
            
        result: FT = []
        active = self.active_pane == "clients"
        for i, cid in enumerate(clients):
            is_sel = active and i == self._sel["clients"]
            style = "class:selected" if is_sel else "class:client-id"
            
            result.extend([("", " ")])
            result.extend([(style, f" {cid:<26} ")])
            result.extend(_nl())
        return result

    def _r_processes(self, pool: dict) -> FT:
        procs = pool.get("processes", {})
        if not procs:
            return cat(_nl(), dim("  No processes registered.\n\n  waiting_for_procs... "), activity_dot(True))
            
        result: FT = []
        active = self.active_pane == "processes"
        items = sorted(procs.items())
        for i, (pid, owner) in enumerate(items):
            is_sel = active and i == self._sel["processes"]
            style = "class:selected" if is_sel else "class:proc-id"
            
            result.extend([("", " ")])
            result.extend([(style, f" {pid:<34} ")])
            result.extend(_nl())
            result.extend(dim(f"  owner: {owner} "))
            result.extend(_nl())
        return result

    def _r_buffers(self, pool: dict) -> FT:
        bufs = pool.get("buffers", [])
        if not bufs:
            return cat(_nl(), dim("  No active buffers.\n\n  waiting_for_data... "), activity_dot(True))
            
        result: FT = []
        active = self.active_pane == "buffers"
        for i, buf in enumerate(bufs):
            is_sel = active and i == self._sel["buffers"]
            style = "class:selected" if is_sel else "class:buf-key"
            
            persist = green(" ● ") if buf["persistent"] else dim(" ○ ")
            result.extend([("", " ")])
            result.extend([(style, f" {buf['key']:<30} ")])
            result.extend(persist)
            result.extend(_nl())
            result.extend(dim(f"  v{buf['version']}  subs {buf['subscriber_count']} "))
            result.extend(_nl())
        return result

    def _r_events(self, events: list) -> FT:
        if not events:
            return cat(_nl(), dim("  Event log empty.\n\n  listening... "), activity_dot(True))
            
        result: FT = []
        active = self.active_pane == "events"
        for i, ev in enumerate(events[:50]):
            is_sel = active and i == self._sel["events"]
            ts = time.strftime("%H:%M:%S", time.localtime(ev["time"]))
            lvl = ev["level"].upper()
            
            lvl_ft = {
                "INFO":  green("┃ INFO  "),
                "WARN":  yellow("┃ WARN  "),
                "ERROR": red("┃ ERROR "),
            }.get(lvl, dim(f"┃ {lvl[:5]:<5} "))

            line_style = "class:selected" if is_sel else ""
            result.extend(dim(f" {ts} "))
            result.extend(lvl_ft)
            result.extend([(line_style, f" {ev['event']} ")])
            if ev['pool']: result.extend(dim(f" [{ev['pool']}] "))
            result.extend(_nl())
        return result

    def _r_workers(self, snap: dict) -> FT:
        """Render the Workers auto-scaling pane."""
        worker_count = snap.get("worker_count", 0)
        worker_max   = snap.get("worker_max", 1280)
        worker_min   = snap.get("worker_min", 4)
        scale_events = snap.get("scale_events", [])
        predicted    = snap.get("predicted_queue_depth", 0.0)
        confidence   = snap.get("prediction_confidence", 0.0)

        result: FT = [("", "\n")]

        # Worker count bar
        bar_width = 20
        filled = int((worker_count / max(worker_max, 1)) * bar_width)
        bar = "█" * filled + "░" * (bar_width - filled)
        result.extend(dim("  "))
        result.extend(muted("Workers  "))
        result.extend(cyan(f"{worker_count:>3}"))
        result.extend(dim(f" │ "))
        result.extend([(("class:green" if worker_count < worker_max * 0.8 else "class:yellow"), bar)])
        result.extend(dim(f" max:{worker_max}"))
        result.extend(_nl())

        # Sparkline
        result.extend(dim("  hist "))
        result.extend(sparkline(self._history.get("worker_count", []), 24))
        result.extend(_nl())
        result.extend(_nl())

        # Predictive pre-scaling row
        if confidence > 0.4:
            pred_style = "class:green" if confidence > 0.7 else "class:yellow"
            result.extend(dim("  Forecast  "))
            result.extend([(pred_style, f"~{predicted:.0f} msgs  conf:{confidence*100:.0f}%")])
            result.extend(_nl())
        else:
            result.extend(dim("  Forecast  "))
            result.extend(dim(f"learning... ({len(self._history.get('queue_depth', []))} samples)"))
            result.extend(_nl())

        result.extend(_nl())
        result.extend(dim("  ─── Scale Events ───────────────────\n"))

        # Scale events list
        if not scale_events:
            result.extend(dim("  No scaling events yet.\n"))
        else:
            active = self.active_pane == "workers"
            for i, ev in enumerate(scale_events[:8]):
                is_sel = active and i == self._sel.get("workers", 0)
                ts = time.strftime("%H:%M:%S", time.localtime(ev["timestamp"]))
                arrow = "▲" if ev["direction"] == "up" else "▼"
                arrow_style = "class:green" if ev["direction"] == "up" else "class:red"
                line_style  = "class:selected" if is_sel else ""
                result.extend(dim(f" {ts} "))
                result.extend([(arrow_style, f"{arrow} ")])
                result.extend([(line_style, f"{ev['old_count']}→{ev['new_count']} ")])
                reason_short = ev.get("reason", "")[:30]
                result.extend(dim(reason_short))
                result.extend(_nl())

        return result

    def _r_queue(self, snap: dict) -> FT:
        """Render the Queue depth pane."""
        depth      = snap.get("queue_depth", 0)
        up_thresh  = 50   # matches config default
        predicted  = snap.get("predicted_queue_depth", 0.0)
        confidence = snap.get("prediction_confidence", 0.0)
        worker_tps = snap.get("worker_tps", 0.0)

        result: FT = [("", "\n")]

        # Queue depth bar
        bar_width = 20
        pct = min(depth / max(up_thresh, 1), 1.0)
        filled = int(pct * bar_width)
        depth_style = "class:green" if depth < up_thresh * 0.5 else ("class:yellow" if depth < up_thresh else "class:red")
        bar = "█" * filled + "░" * (bar_width - filled)
        result.extend(dim("  "))
        result.extend(muted("Q Depth  "))
        result.extend([(depth_style, f"{depth:>4}")])
        result.extend(dim(" │ "))
        result.extend([(depth_style, bar)])
        result.extend(dim(f" trig:{up_thresh}"))
        result.extend(_nl())

        # Sparkline of queue depth history
        result.extend(dim("  hist "))
        result.extend(sparkline(self._history.get("queue_depth", []), 24))
        result.extend(_nl())
        result.extend(_nl())

        # Worker throughput
        result.extend(dim("  Worker TPS   "))
        result.extend(cyan(f"{worker_tps:.1f} msg/s"))
        result.extend(_nl())

        # Prediction
        if confidence > 0.4:
            pred_style = "class:green" if predicted < up_thresh else "class:yellow"
            result.extend(dim("  Predicted    "))
            result.extend([(pred_style, f"~{predicted:.0f}")])
            result.extend(dim(f"  conf:{confidence*100:.0f}%"))
        else:
            result.extend(dim("  Predicted    "))
            result.extend(dim("learning..."))
        result.extend(_nl())
        result.extend(_nl())

        result.extend(dim("  ─── Depth Trend ─────────────────────\n"))
        history = self._history.get("queue_depth", [])
        if len(history) >= 2:
            recent = history[-10:]
            avg = sum(recent) / len(recent)
            peak = max(recent)
            result.extend(dim(f"  avg(10s): "))
            result.extend(cyan(f"{avg:.1f}"))
            result.extend(dim("   peak: "))
            result.extend(cyan(f"{peak:.0f}"))
            result.extend(_nl())

        return result

    def _r_detail(self, pool: dict) -> FT:

        p = self.active_pane
        
        def _kv(key: str, val: str, is_list=False) -> FT:
            return cat(dim(f"  {key:<12} "), cyan(val) if not is_list else dim(val), _nl())

        if p == "pools":
            if not pool: return []
            res = cat(_nl(), orange(f"  {pool['pool_id']}"), _nl(), dim("  " + "─"*30), _nl())
            res.extend(_kv("STATUS", "active"))
            res.extend(_kv("AUTH", "required" if pool["auth_required"] else "none"))
            res.extend(_kv("CLIENTS", str(len(pool["clients"]))))
            res.extend(_kv("BUFFERS", str(len(pool.get("buffers", [])))))
            res.extend(_kv("PROCESSES", str(len(pool.get("processes", {})))))
            return res
            
        if p == "clients":
            clients = pool.get("clients", [])
            if not clients: return []
            cid = clients[min(self._sel["clients"], len(clients)-1)]
            res = cat(_nl(), orange(f"  {cid}"), _nl(), dim("  " + "─"*30), _nl())
            res.extend(_kv("POOL", pool.get("pool_id", "")))
            owned_bufs = [b["key"] for b in pool.get("buffers", []) if b.get("updated_by") == cid]
            owned_procs = [pid for pid, owner in pool.get("processes", {}).items() if owner == cid]
            res.extend(_kv("OWNED BUFS", str(len(owned_bufs))))
            res.extend(_kv("OWNED PROCS", str(len(owned_procs))))
            return res
            
        if p == "processes":
            procs = pool.get("processes", {})
            if not procs: return []
            items = sorted(procs.items())
            pid, owner = items[min(self._sel["processes"], len(items)-1)]
            res = cat(_nl(), orange(f"  {pid}"), _nl(), dim("  " + "─"*30), _nl())
            res.extend(_kv("OWNER", owner))
            res.extend(_kv("STATUS", "registered"))
            return res
            
        if p == "buffers":
            bufs = pool.get("buffers", [])
            if not bufs: return []
            buf = bufs[min(self._sel["buffers"], len(bufs)-1)]
            res = cat(_nl(), orange(f"  {buf['key']}"), _nl(), dim("  " + "─"*30), _nl())
            res.extend(_kv("VERSION", str(buf['version'])))
            res.extend(_kv("PERSISTENT", str(buf['persistent'])))
            res.extend(_kv("SUBSCRIBERS", str(buf['subscriber_count'])))
            res.extend(_kv("UPDATED BY", buf['updated_by']))
            
            val_str = json.dumps(buf.get('value'))
            if len(val_str) > 50: val_str = val_str[:47] + "..."
            res.extend(_kv("VALUE", val_str))
            return res

        if p == "workers":
            snap = self._snap
            res = cat(_nl(), orange("  Worker Pool"), _nl(), dim("  " + "─"*30), _nl())
            res.extend(_kv("ACTIVE", str(snap.get("worker_count", 0))))
            res.extend(_kv("MIN", str(snap.get("worker_min", 0))))
            res.extend(_kv("MAX", str(snap.get("worker_max", 0))))
            scale_events = snap.get("scale_events", [])
            res.extend(_kv("EVENTS", str(len(scale_events))))
            if scale_events:
                last = scale_events[0]
                arrow = green("▲") if last["direction"] == "up" else red("▼")
                res.extend(cat(dim("  LAST SCALE   "), arrow, dim(f" {last['old_count']}→{last['new_count']}"), _nl()))
                res.extend(dim(f"  {last['reason'][:38]}"))
                res.extend(_nl())
            return res

        if p == "queue":
            snap = self._snap
            depth = snap.get("queue_depth", 0)
            pred = snap.get("predicted_queue_depth", 0.0)
            conf = snap.get("prediction_confidence", 0.0)
            res = cat(_nl(), orange("  Dispatch Queue"), _nl(), dim("  " + "─"*30), _nl())
            res.extend(_kv("DEPTH", str(depth)))
            res.extend(_kv("PREDICTED", f"{pred:.1f}"))
            res.extend(_kv("CONFIDENCE", f"{conf*100:.0f}%"))
            res.extend(_kv("WORKER TPS", f"{snap.get('worker_tps', 0.0):.1f}"))
            return res
        
        events = self._snap.get("events", [])
        if not events: return []
        ev = events[min(self._sel["events"], len(events)-1)]
        res = cat(_nl(), orange(f"  {ev['event']}"), _nl(), dim("  " + "─"*30), _nl())
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ev["time"]))
        res.extend(_kv("TIME", ts))
        res.extend(_kv("LEVEL", ev['level'].upper()))
        if ev.get('pool'): res.extend(_kv("POOL", ev['pool']))
        if ev.get('client_id'): res.extend(_kv("CLIENT", ev['client_id']))
        if ev.get('extra'):
            res.extend(_kv("EXTRA", json.dumps(ev['extra'])))
        return res

    # ------------------------------------------------------------------
    # Async run loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        while self._running:
            self._refresh()
            await asyncio.sleep(self.refresh_interval)

    async def run(self) -> None:
        self._refresh()
        task = asyncio.create_task(self._loop())
        try:
            await self.application.run_async()
        finally:
            self._running = False
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
