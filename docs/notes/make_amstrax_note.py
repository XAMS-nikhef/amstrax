"""Explainer: how strax + amstrax (3.x) turn redax chunks into the event tables (events, sites, event_peaks,
peak_context, event_cuts), where each plugin enters the reconstruction stream, and what the event tables hold (with
the old event_info format of amstrax 2.x and where its columns went). Plugin names, files, line numbers, versions, depends_on and settings are read from this amstrax checkout
(parsed with ast), corrections from amstrax_files next to it (../amstrax_files, or $XAMS_AMSTRAX_FILES_DIR); the
layout and the short descriptions are written here. No data. Needs matplotlib and reportlab, and amstrax importable (the column
tables come from st.data_info), e.g. the xamssim env with this checkout installed.
Usage (from the amstrax checkout): python docs/notes/make_amstrax_note.py [out.pdf]
    (default docs/notes/XAMS_amstrax_overview.pdf, figures in docs/notes/figures/)"""
import ast
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, Rectangle  # noqa: E402
from reportlab.lib import colors  # noqa: E402
from reportlab.lib.pagesizes import A4  # noqa: E402
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet  # noqa: E402
from reportlab.lib.units import mm  # noqa: E402
from reportlab.pdfbase import pdfmetrics  # noqa: E402
from reportlab.pdfbase.pdfmetrics import registerFontFamily  # noqa: E402
from reportlab.pdfbase.ttfonts import TTFont  # noqa: E402
from reportlab.platypus import (Image, KeepTogether, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table,  # noqa: E402
                                TableStyle)

FD = "/System/Library/Fonts/Supplemental/"
pdfmetrics.registerFont(TTFont("Arial", FD + "Arial.ttf")); pdfmetrics.registerFont(TTFont("Arial-Bold", FD + "Arial Bold.ttf"))
pdfmetrics.registerFont(TTFont("Arial-Italic", FD + "Arial Italic.ttf"))
registerFontFamily("Arial", normal="Arial", bold="Arial-Bold", italic="Arial-Italic", boldItalic="Arial-Bold")
NAVY, LIGHT, GREEN, AMBER, GRID = (colors.HexColor(c) for c in ("#1F3A5F", "#E8EEF5", "#E3F2E1", "#FFF4D6", "#9AA8B8"))
ss = getSampleStyleSheet()
base = ParagraphStyle("b", parent=ss["Normal"], fontName="Arial", fontSize=9.3, leading=12.3)
small = ParagraphStyle("s", parent=base, fontSize=7.6, leading=9.6)
cap = ParagraphStyle("c", parent=small, textColor=colors.HexColor("#4A5A6A"), spaceAfter=6)
h1 = ParagraphStyle("h1", parent=base, fontName="Arial-Bold", fontSize=17, leading=21, textColor=NAVY)
sub = ParagraphStyle("sub", parent=base, fontSize=9.5, textColor=colors.HexColor("#4A5A6A"), spaceAfter=5)
h2 = ParagraphStyle("h2", parent=base, fontName="Arial-Bold", fontSize=11.5, leading=14.5, textColor=NAVY, spaceBefore=8, spaceAfter=3)
hdr = ParagraphStyle("hdr", parent=small, fontName="Arial-Bold", textColor=colors.white)
W = A4[0] - 30 * mm
HERE = Path(__file__).resolve().parent                              # amstrax/docs/notes
AX = Path(os.environ.get("XAMS_AMSTRAX_DIR", HERE.parents[1]))      # this checkout; override to document another
AXF = Path(os.environ.get("XAMS_AMSTRAX_FILES_DIR", AX.parent / "amstrax_files"))
FIG = HERE / "figures"
CORR_VERSION = "PROD5"

# ------------------------------------------------------------------------------------------------ read amstrax source
def camel_to_snake(name):
    """as strax.camel_to_snake: LEDCalibration -> led_calibration"""
    return re.sub(r"((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))", r"_\1", name).lower()


def as_tuple(v):
    if v is None:
        return ()
    return (v,) if isinstance(v, str) else tuple(v)


def class_options(node):
    """XAMSConfig options defined in the body of a class: name -> default"""
    out = {}
    for st in node.body:
        if isinstance(st, ast.Assign) and len(st.targets) == 1 and isinstance(st.targets[0], ast.Name):
            tgt, val = st.targets[0].id, st.value
        elif isinstance(st, ast.AnnAssign) and isinstance(st.target, ast.Name) and st.value is not None:
            tgt, val = st.target.id, st.value
        else:
            continue
        if isinstance(val, ast.Call) and ast.unparse(val.func).endswith("XAMSConfig"):
            d = {k.arg: k.value for k in val.keywords}.get("default")
            try:
                out[tgt] = ast.literal_eval(d) if d is not None else None
            except Exception:
                out[tgt] = ast.unparse(d)
    return out


def parse_plugins():
    """every strax plugin class under amstrax/plugins: file, lines, version, depends_on, provides, data_kind,
    settings (also those of mixin classes in the same file, e.g. _Corrections of Sites and Events)"""
    out = {}
    for f in sorted((AX / "amstrax" / "plugins").rglob("*.py")):
        tree = ast.parse(f.read_text())
        mixins = {n.name: class_options(n) for n in tree.body if isinstance(n, ast.ClassDef)}
        for node in tree.body:
            if not isinstance(node, ast.ClassDef):
                continue
            bases = [ast.unparse(b) for b in node.bases]
            if not any("Plugin" in b for b in bases):
                continue
            p = dict(cls=node.name, file=f.relative_to(AX / "amstrax").as_posix(), line=node.lineno,
                     base=bases[0].replace("strax.", ""), cfg={}, compute=None, setup=None)
            for st in node.body:
                tgt = val = None
                if isinstance(st, ast.Assign) and len(st.targets) == 1 and isinstance(st.targets[0], ast.Name):
                    tgt, val = st.targets[0].id, st.value
                elif isinstance(st, ast.AnnAssign) and isinstance(st.target, ast.Name) and st.value is not None:
                    tgt, val = st.target.id, st.value
                elif isinstance(st, ast.FunctionDef) and st.name in ("compute", "setup"):
                    p[st.name] = st.lineno
                if tgt is None:
                    continue
                if isinstance(val, ast.Call) and ast.unparse(val.func).endswith("XAMSConfig"):
                    kw = {k.arg: k.value for k in val.keywords}
                    d = kw.get("default")
                    try:
                        dv = ast.literal_eval(d) if d is not None else None
                    except Exception:
                        dv = ast.unparse(d)
                    p["cfg"][tgt] = dv
                elif tgt in ("depends_on", "provides", "__version__", "save_when", "data_kind"):
                    try:
                        p[tgt] = ast.literal_eval(val)
                    except Exception:
                        p[tgt] = ast.unparse(val)
            for b in bases:
                if b in mixins:
                    p["cfg"] = {**mixins[b], **p["cfg"]}
            dep = p.get("depends_on")
            p["depends_on"] = () if dep == "tuple()" else as_tuple(dep)
            p["provides"] = as_tuple(p.get("provides")) or (camel_to_snake(node.name),)
            out[node.name] = p
    return out


def registered():
    """plugin classes registered in the default context (contexts.COMMON_OPT_XAMS, commented lines skipped)"""
    txt = (AX / "amstrax" / "contexts.py").read_text()
    block = txt[txt.index("register=["):]
    block = block[:block.index("]")]
    names = {m for line in block.splitlines() if not line.strip().startswith("#") for m in re.findall(r"ax\.(\w+)", line)}
    # tuples of plugin classes registered with *ax.NAME (e.g. EVENT_PLUGINS in event_structure.py)
    for name in [n for n in names if n.isupper()]:
        for f in (AX / "amstrax" / "plugins").rglob("*.py"):
            m = re.search(rf"^{name}\s*=\s*\(([^)]*)\)", f.read_text(), re.M)
            if m:
                names |= {c.strip() for c in m.group(1).split(",") if c.strip()}
    return names


def find_line(rel, pattern):
    for i, line in enumerate((AX / "amstrax" / rel).read_text().splitlines(), 1):
        if re.search(pattern, line):
            return i
    raise ValueError(f"{pattern!r} not found in {rel}")


def git_info():
    try:
        h = subprocess.run(["git", "-C", str(AX), "log", "-1", "--format=%h %ad", "--date=short"], capture_output=True,
                           text=True, check=True).stdout.strip()
        br = subprocess.run(["git", "-C", str(AX), "branch", "--show-current"], capture_output=True, text=True,
                            check=True).stdout.strip()
        return f"{br} {h}"
    except Exception:
        return "unknown version"


PL = parse_plugins()
REG = registered()
GIT = git_info()
CORR = json.load(open(AXF / "corrections" / "_global" / f"_global_{CORR_VERSION}.json"))


def loc(cls, what="line"):
    p = PL[cls]
    return f"{p['file']}:{p[what]}"


def short_file(cls):
    return f"{Path(PL[cls]['file']).name}:{PL[cls]['line']}"


def save_mode(p):
    s = p.get("save_when")
    if s is None:
        return "ALWAYS (default)"
    if isinstance(s, str):
        found = re.findall(r"(\w+)\s*=\s*strax\.SaveWhen\.(\w+)|['\"](\w+)['\"]\s*:\s*strax\.SaveWhen\.(\w+)|^strax\.SaveWhen\.(\w+)$", s)
        parts = []
        for a, b, c, d, e in found:
            parts.append(e if e else f"{a or c}: {b or d}")
        return ", ".join(parts) if parts else s
    return str(s)


def rundoc_classes():
    return sorted(c for c, p in PL.items() if any(isinstance(v, str) and v.startswith("rundoc://") for v in p["cfg"].values()))


def users_of(option):
    return sorted(c for c, p in PL.items() if option in p["cfg"])


# ------------------------------------------------------------------------------------------------ texts per plugin
WHAT = {
    "DAQReader": "Reads the redax chunk files of one run, splits the records by channel range (run-doc channel_map) into "
                 "TPC / external / SiPM and makes the time absolute.",
    "PulseProcessing": "Keeps TPC channels; raw_to_records; baseline per channel from the first baseline_samples, "
                       "flips the negative PMT pulses; integrates each pulse; counts pulses per channel.",
    "Peaks": "find_hits (samples above threshold) → find_peaks (hits closer than peak_gap_threshold, ≥ peak_min_pmts, "
             "area × to_pe) → sum_waveform → split_peaks → widths.",
    "PeakBasics": "Area, widths, area_fraction_top, n_channels, saturation; type 1 = S1, 2 = S2, 0 = unknown from "
                  "the width / area / AFT / channel cuts.",
    "PeakPositions": "u, v from the top-array light fractions; x, y, r from pos_rec_map (default: latest map), with "
                     "or without gains (without: ~2 mm worse); NaN without a map. No polynomial since 2.0.0.",
    "EventWindows": "Triggers = S2s with area > trigger_min_area; window [t − left_event_extension, t + "
                    "right_event_extension]; overlapping windows merged (find_peak_groups). Only time windows "
                    "(event_number, time, endtime). Called 'events' until amstrax 2.x.",
    "EventPeaks": "Every peak fully inside an event window: peak_basics columns + x, y, with event_number, "
                  "peak_index (row in the run), rank by area within its type and dt_s1 (time after the main S1 = "
                  "largest S1 before the largest S2).",
    "Sites": "Every S2 of event_peaks above site_min_s2_area: drift time, x, y, z, r, cS2 (lifetime + S2_top map, "
             "radial factor), cS2 top / bottom, rank (1 = main S2), in_drift. Corrections options from _Corrections.",
    "Events": "The event as a whole: counts of peaks / S1s / S2s / sites, cS1 (S1 / cs2_top-weighted response "
              "over the in-drift sites), sums of cS2 over the in-drift sites, energy, time to previous / next event.",
    "PeakContext": "Every peak of the run (also between events) with the last S2 > 1 kPE and > 10 kPE before it: time "
                   "since, area, event. For delayed electrons and photoionisation.",
    "EventCuts": "One boolean per cut per event (basic, isolated S1 / S2, S1 AFT and S2 width bands, cut C, "
                 "fiducial, all) from events + sites + event_peaks, references per run set from amstrax_files.",
    "PulseProcessingEXT": "Baseline + integration for the external channel (NaI).",
    "PeaksEXT": "Hit and peak finding on records_ext.",
    "PeakBasicsEXT": "Peak properties of the external peaks.",
    "PulseProcessingSiPM": "Baseline + integration for the SiPM channels.",
    "PeaksSiPM": "Hit and peak finding on records_sipm (area_per_channel up to max_sipm_channel).",
    "PeakBasicsSiPM": "Peak properties of the SiPM peaks.",
    "RecordsLED": "LED window cut from raw_records + raw_records_sipm; flips only negative_polarity_channels.",
    "LEDCalibration": "Amplitude and area per LED trigger (led_window vs noise_window) → input for the gain fits.",
}
# settings that matter most, per plugin (checked against the parsed option names)
KEYCFG = {
    "DAQReader": ["channel_map", "daq_chunk_duration"],
    "PulseProcessing": ["baseline_samples", "n_tpc_pmts"],
    "Peaks": ["gain_to_pe_array", "peak_gap_threshold", "peak_min_pmts", "peak_min_area"],
    "PeakBasics": ["s1_min_area", "s1_max_width", "s2_min_area", "s2_min_width", "channel_map"],
    "PeakPositions": ["pos_rec_map", "channel_map"],
    "EventWindows": ["trigger_min_area", "left_event_extension", "right_event_extension"],
    "EventPeaks": [],
    "Sites": ["site_min_s2_area", "sr_corrections", "sr_maps", "elife", "drift_time_cathode"],
    "Events": ["sr_corrections", "sr_maps", "s1_naive_z_correction"],
    "PeakContext": [],
    "EventCuts": ["event_cut_references"],
    "PulseProcessingEXT": [], "PeaksEXT": [], "PeakBasicsEXT": [],
    "PulseProcessingSiPM": ["baseline_samples"], "PeaksSiPM": ["gain_to_pe_array", "max_sipm_channel", "channel_map"],
    "PeakBasicsSiPM": ["channel_map"],
    "RecordsLED": ["negative_polarity_channels"], "LEDCalibration": ["led_window", "noise_window"],
}
for c, names in KEYCFG.items():
    missing = [n for n in names if n not in PL[c]["cfg"]]
    assert not missing, f"{c}: options {missing} not in the source any more"
assert set(KEYCFG) == set(PL), f"plugins without description: {set(PL) ^ set(KEYCFG)}"

TAGS = {}   # (until 2.x: where the SR-2026.2 event display / delayed scan entered; not used now)

# ------------------------------------------------------------------------------------------------ figure: generic strax request
C_RAW, C_REC, C_PK, C_EV, C_INFO, C_LIVE = "#ECE6F5", "#DCEAF7", "#E3F2E1", "#FFF1D6", "#1F3A5F", "#F2F2F2"
C_TAG, C_CFG, C_EDGE, C_NOTE = "#2E7D32", "#B26A00", "#4A5A6A", "#B03A2E"


def fig_generic():
    fig, ax = plt.subplots(figsize=(7.1, 2.55))
    ax.set_xlim(0, 10); ax.set_ylim(0, 3.6); ax.axis("off")

    def b(x, y, w, h, t, fc, fs=6.4, bold_first=True):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.08", fc=fc, ec=C_EDGE, lw=0.7))
        lines = t.split("\n")
        ax.text(x + w / 2, y + h - 0.13, lines[0], ha="center", va="top", fontsize=fs, fontweight="bold" if bold_first else None)
        ax.text(x + w / 2, y + h - 0.40, "\n".join(lines[1:]), ha="center", va="top", fontsize=fs - 0.9, linespacing=1.25)

    def arr(p, q, lab=None, off=(0, 0.08)):
        ax.add_patch(FancyArrowPatch(p, q, arrowstyle="-|>", mutation_scale=8, lw=0.8, color=C_EDGE))
        if lab:
            ax.text((p[0] + q[0]) / 2 + off[0], (p[1] + q[1]) / 2 + off[1], lab, ha="center", va="bottom", fontsize=5.6, color=C_EDGE)

    b(0.05, 2.25, 1.75, 1.2, "st.get_array(run,\n'event_info')\nask for one data type\n(or st.make: compute + save)", "#FFFFFF", fs=6.0)
    b(2.15, 2.25, 1.75, 1.2, "1  dependency tree\nfrom depends_on / provides\nof the registered plugins\nevent_info → … → raw_records", C_LIVE)
    b(4.25, 2.25, 1.75, 1.2, "2  lineage hash\nper data type: plugin name,\n__version__, tracked settings\n+ lineage of all its inputs", C_LIVE)
    b(6.35, 2.25, 1.75, 1.2, "3  storage lookup\nRunDB (run doc data[] in Mongo)\nthen DataDirectory folders\nsame hash = same data", C_LIVE)
    b(8.3, 2.55, 1.65, 0.9, "found\nload chunks from disk", C_PK)
    b(6.35, 0.15, 1.75, 1.45, "4  compute\nsetup() once per run,\ncompute() per time chunk,\ninputs streamed chunk by chunk,\nparallel where allowed", C_EV)
    b(4.25, 0.15, 1.75, 1.45, "5  save\naccording to save_when:\nALWAYS / TARGET (only if asked\nfor) / NEVER; written next\nto the hash in the output folder", C_EV)
    b(2.15, 0.15, 1.75, 1.45, "result\nnumpy structured array,\none row per item (pulse, peak,\nevent), fields = dtype;\nst.get_df gives pandas", "#FFFFFF")
    arr((1.8, 2.85), (2.15, 2.85)); arr((3.9, 2.85), (4.25, 2.85)); arr((6.0, 2.85), (6.35, 2.85))
    arr((8.1, 3.0), (8.3, 3.0), "yes", off=(0, 0.05))
    arr((7.22, 2.25), (7.22, 1.6), "no → make it,\nfrom its inputs\n(recursively 3 → 4)", off=(0.72, -0.35))
    arr((6.35, 0.88), (6.0, 0.88)); arr((4.25, 0.88), (3.9, 0.88))
    ax.add_patch(FancyArrowPatch((9.12, 2.55), (3.03, 1.6), connectionstyle="arc3,rad=-0.12", arrowstyle="-|>",
                                 mutation_scale=8, lw=0.8, color=C_EDGE, ls="--"))
    fig.tight_layout(pad=0.1)
    fig.savefig(FIG / "generic.png", dpi=220)
    plt.close(fig)


# ------------------------------------------------------------------------------------------------ figure: the reconstruction stream
X_EXT, X_TPC, X_SIPM, X_LED = 1.15, 3.75, 6.35, 8.95
BW, BH = 2.3, 1.1
MAXC = 40     # characters per line that fit in a box at 4.7 pt
Y = {i: 14.0 - 1.36 * i for i in range(12)}


def wrap(text, indent):
    """split a comma list over lines of at most MAXC characters"""
    out, cur = [], ""
    for part in text.split(", "):
        if cur and len(cur) + len(part) + 2 > MAXC:
            out.append(cur + ","); cur = indent + part
        else:
            cur = f"{cur}, {part}" if cur else part
    return out + [cur]
POS = {  # node -> (x, row)
    "raw:raw_records_ext": (X_EXT, 2), "raw:raw_records": (X_TPC, 2), "raw:raw_records_sipm": (X_SIPM, 2),
    "PulseProcessingEXT": (X_EXT, 3), "PulseProcessing": (X_TPC, 3), "PulseProcessingSiPM": (X_SIPM, 3), "RecordsLED": (X_LED, 3),
    "PeaksEXT": (X_EXT, 4), "Peaks": (X_TPC, 4), "PeaksSiPM": (X_SIPM, 4), "LEDCalibration": (X_LED, 4),
    "PeakBasicsEXT": (X_EXT, 5), "PeakBasics": (X_TPC, 5), "PeakBasicsSiPM": (X_SIPM, 5),
    "PeakPositions": (X_TPC, 6),
    "EventWindows": (X_TPC, 7),
    "EventPeaks": (X_TPC, 8), "PeakContext": (X_EXT, 8),
    "Sites": (X_TPC, 9),
    "Events": (X_TPC, 10),
    "EventCuts": (X_TPC, 11),
}
FINAL = ("Events", "EventCuts")      # what an analysis reads first (with sites, event_peaks, peak_context)
assert set(PL) - {"DAQReader"} <= set(POS), f"plugins without a place in the figure: {set(PL) - set(POS) - {'DAQReader'}}"
LEVEL = {"raw": C_RAW, "rec": C_REC, "pk": C_PK, "ev": C_EV}


def level_of(node):
    if node.startswith("raw:"):
        return "raw"
    row = POS[node][1]
    if node in ("RecordsLED", "LEDCalibration") or row == 3:
        return "rec"
    if node == "PeakContext":
        return "pk"
    return "pk" if row <= 6 else "ev"


def fig_stream():
    fig, ax = plt.subplots(figsize=(7.1, 10.0))
    ax.set_xlim(-0.8, 10.25); ax.set_ylim(Y[11] - 0.85, Y[0] + 0.62); ax.axis("off")

    def top(n):
        x, r = POS[n]; return (x, Y[r] + BH / 2)

    def bot(n):
        x, r = POS[n]; return (x, Y[r] - BH / 2)

    def left(n, dy=0.0):
        x, r = POS[n]; return (x - BW / 2, Y[r] + dy)

    def right(n, dy=0.0):
        x, r = POS[n]; return (x + BW / 2, Y[r] + dy)

    def arrow(p, q, cs="arc3,rad=0", ls="-", color=C_EDGE, lw=0.8, z=1):
        ax.add_patch(FancyArrowPatch(p, q, connectionstyle=cs, arrowstyle="-|>", mutation_scale=7, lw=lw, ls=ls,
                                     color=color, zorder=z, shrinkA=0, shrinkB=0))

    # data-kind containers for the TPC chain
    for r0, r1, lab in ((4, 6, "data kind 'peaks': one row per peak — peak_basics and peak_positions add columns to the same rows"),):
        y0, y1 = Y[r1] - BH / 2 - 0.07, Y[r0] + BH / 2 + 0.15
        ax.add_patch(Rectangle((X_TPC - BW / 2 - 0.1, y0), BW + 0.2, y1 - y0, fc="none", ec=C_EDGE, lw=0.6, ls=(0, (4, 2)), zorder=0))
        ax.text(X_TPC - BW / 2 - 0.06, y1 - 0.03, lab.split(":")[0], fontsize=5.0, style="italic", color=C_EDGE, va="top")

    # live data, DAQReader
    ax.add_patch(FancyBboxPatch((-0.55, Y[0] - 0.36), 10.6, 0.72, boxstyle="round,pad=0.02,rounding_size=0.08", fc=C_LIVE, ec=C_EDGE, lw=0.7))
    ax.text(4.75, Y[0] + 0.15, "redax strax chunks of one run", ha="center", va="center", fontsize=7, fontweight="bold")
    ax.text(4.75, Y[0] - 0.14, "xams: /home/xams/data/live_data  →  copy_live  →  STBC live-data folder;  daq_config (run doc) gives chunk length, "
            "fragment size, compressor", ha="center", va="center", fontsize=5.4)
    d = PL["DAQReader"]
    x0, x1 = -0.05, X_SIPM + BW / 2
    ax.add_patch(FancyBboxPatch((x0, Y[1] - 0.42), x1 - x0, 0.84, boxstyle="round,pad=0.02,rounding_size=0.08", fc=C_RAW, ec=C_EDGE, lw=0.8))
    ax.text((x0 + x1) / 2, Y[1] + 0.2, f"DAQReader  v{d['__version__']}   ·   {d['file']}:{d['line']}   ·   compute :{d['compute']}",
            ha="center", va="center", fontsize=6.4, fontweight="bold")
    ax.text((x0 + x1) / 2, Y[1] - 0.12, "one plugin, three outputs: records split by the channel ranges of the run-doc channel_map\n"
            "(bottom + top → TPC, external → _ext, sipm → _sipm; a channel outside every range stops the run)",
            ha="center", va="center", fontsize=5.3, linespacing=1.2)
    arrow((4.75 - 1.5, Y[0] - 0.36), (4.75 - 1.5, Y[1] + 0.42))
    for n in ("raw:raw_records_ext", "raw:raw_records", "raw:raw_records_sipm"):
        arrow((POS[n][0], Y[1] - 0.42), top(n))

    # stage divider
    ys = (Y[2] - BH / 2 + Y[3] + BH / 2) / 2
    ax.plot([-0.7, 10.2], [ys, ys], color=C_NOTE, lw=0.8, ls=(0, (6, 3)), zorder=0)
    sx = X_SIPM + BW / 2 + 0.15
    ax.text(sx, Y[1] + 0.45, "processing on STBC in two stages", fontsize=5.6, fontweight="bold", color=C_NOTE, va="top")
    ax.text(sx, Y[1] + 0.2,
            "above the red line — stage 1:\n  raw_st.make(run, 'raw_records')\n  in context_for_daq_reader()\n"
            "below — stage 2: st.make(run, targets)\n  in the main context xams();\n  raw_records only read there",
            fontsize=5.0, color=C_NOTE, va="top", linespacing=1.25)

    # nodes
    for n, (x, r) in POS.items():
        y = Y[r]
        lv = level_of(n)
        if n.startswith("raw:"):
            dt = n[4:]
            lines = [dt, "output of DAQReader", f"data kind '{dt}'", f"save: {save_mode(d)}"]
            ec, lw, ls = C_EDGE, 0.8, "-"
        else:
            p = PL[n]
            outs = " + ".join(p["provides"])
            kind = p.get("data_kind") if isinstance(p.get("data_kind"), str) else p["provides"][0]
            lines = [outs, f"{n}  v{p.get('__version__', '?')}" + (f" · kind {kind}" if r >= 7 or n == "PeakContext" else "")]
            fl = f"{short_file(n)} · compute :{p['compute']}" if p["compute"] else short_file(n)
            lines += [fl] if len(fl) <= MAXC else [short_file(n), f"compute :{p['compute']}"]
            lines += wrap("← " + ", ".join(p["depends_on"]), "   ")
            cfg = ""
            for name in KEYCFG[n]:
                if len(cfg) + len(name) + 7 > MAXC:
                    break
                cfg = f"{cfg}, {name}" if cfg else name
            if cfg:
                lines.append(f"cfg: {cfg}")
            sm = save_mode(p)
            ec, lw, ls = C_EDGE, 0.8, "-"
            if "TARGET" in sm and "ALWAYS" not in sm:
                ls = (0, (3, 1.5))
            if n == "PulseProcessing":
                ls = (0, (3, 1.5))
            if n not in REG:
                ls, ec = (0, (1, 1.2)), C_NOTE
            if n in FINAL:
                ec, lw = C_INFO, 2.0
        ax.add_patch(FancyBboxPatch((x - BW / 2, y - BH / 2), BW, BH, boxstyle="round,pad=0.01,rounding_size=0.06",
                                    fc=LEVEL[lv], ec=ec, lw=lw, ls=ls, zorder=2))
        fs_t = 6.2 if len(lines[0]) < 24 else (5.4 if len(lines[0]) < 30 else 4.6)
        ax.text(x, y + BH / 2 - 0.07, lines[0], ha="center", va="top", fontsize=fs_t, fontweight="bold", zorder=3)
        body = lines[1:]
        dep = False
        for k, t in enumerate(body):
            dep = t.startswith("←") or (dep and t.startswith("   "))
            col = C_CFG if t.startswith("cfg:") else ("#555555" if dep else "black")
            ax.text(x, y + BH / 2 - 0.29 - 0.14 * k, t, ha="center", va="top", fontsize=4.7, color=col, zorder=3)
        for k, tg in enumerate(TAGS.get(n, [])):
            tx, ty = x + BW / 2 - 0.2 - 0.32 * k, y + BH / 2 + 0.07
            ax.add_patch(FancyBboxPatch((tx - 0.13, ty - 0.09), 0.26, 0.18, boxstyle="round,pad=0.01,rounding_size=0.05",
                                        fc=C_TAG, ec="none", zorder=4))
            ax.text(tx, ty, tg, ha="center", va="center", fontsize=4.6, color="white", fontweight="bold", zorder=5)

    # edges (all depends_on are listed in the boxes; drawn here is the main flow)
    for a, b in (("raw:raw_records_ext", "PulseProcessingEXT"), ("raw:raw_records", "PulseProcessing"),
                 ("raw:raw_records_sipm", "PulseProcessingSiPM"),
                 ("PulseProcessingEXT", "PeaksEXT"), ("PeaksEXT", "PeakBasicsEXT"),
                 ("PulseProcessing", "Peaks"), ("Peaks", "PeakBasics"), ("PeakBasics", "PeakPositions"),
                 ("PulseProcessingSiPM", "PeaksSiPM"), ("PeaksSiPM", "PeakBasicsSiPM"),
                 ("RecordsLED", "LEDCalibration"),
                 ("EventWindows", "EventPeaks"), ("EventPeaks", "Sites"), ("Sites", "Events"), ("Events", "EventCuts")):
        arrow(bot(a), top(b))
    # skip edge inside the peaks kind: peaks -> peak_positions (right side)
    # (arc arms are in points)
    arrow(right("Peaks", -0.25), right("PeakPositions", 0.25), cs="arc,angleA=0,angleB=0,armA=7,armB=7,rad=3")
    # LED inputs (LED context only)
    arrow((X_TPC + BW / 2 - 0.15, Y[2] - BH / 2), (X_LED - 0.5, Y[3] + BH / 2), ls=(0, (3, 2)), color=C_NOTE)
    arrow((X_SIPM + BW / 2 - 0.15, Y[2] - BH / 2), (X_LED - 0.15, Y[3] + BH / 2), ls=(0, (3, 2)), color=C_NOTE)
    # peaks kind -> event_windows; peak_basics + peak_positions -> event_peaks (right); event_peaks -> events (skip sites)
    y_pk_bottom = Y[6] - BH / 2 - 0.07
    arrow((X_TPC, y_pk_bottom), top("EventWindows"))
    ax.text(X_TPC + 0.08, Y[7] + BH / 2 + 0.08, "peaks + peak_basics", fontsize=4.8, color=C_EDGE, va="center")
    arrow((X_TPC + BW / 2 + 0.1, Y[5]), right("EventPeaks", 0.2), cs="arc,angleA=0,angleB=0,armA=16,armB=16,rad=4")
    ax.text(X_TPC + BW / 2 + 0.42, Y[7], "peak_basics,\npeak_positions", fontsize=4.8, color=C_EDGE, va="center", ha="left")
    arrow(right("EventPeaks", -0.3), right("Events", 0.25), cs="arc,angleA=0,angleB=0,armA=9,armB=9,rad=3")
    arrow(right("EventPeaks", -0.42), right("EventCuts", 0.25), cs="arc,angleA=0,angleB=0,armA=24,armB=24,rad=4", ls=(0, (3, 2)))
    ax.text(X_TPC + BW / 2 + 0.75, Y[10], "event_peaks\n(+ sites) also\nread by events,\nevent_cuts", fontsize=4.8,
            color=C_EDGE, va="center", ha="left")
    # peak_context: all peaks of the run with the last large S2 before them
    arrow(left("PeakBasics", -0.3), top("PeakContext"), cs="arc3,rad=0.15")
    arrow(left("EventWindows"), right("PeakContext", 0.2))
    ax.text(X_EXT, Y[9] + 0.05, "same rows as peaks (kind peaks):\nevery peak of the run, also\nbetween events",
            ha="center", va="center", fontsize=4.8, color=C_EDGE)
    # link keys
    ax.text(X_TPC - BW / 2 - 0.15, Y[11] - BH / 2 - 0.25, "links: event_number (events, sites, event_peaks, event_cuts) · "
            "peak_index = row in the run's peaks (sites, event_peaks, peak_context)", fontsize=5.0, color=C_NOTE, va="center")

    # no SiPM at event level
    ax.text(X_SIPM + 0.35, Y[6], "✗  no event-level SiPM or NaI plugin\nnothing reads peaks_sipm / peaks_ext;\n"
            "match peak_basics_sipm / _ext in\ntime to the event tables yourself", ha="center", va="center", fontsize=5.0, color=C_NOTE,
            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec=C_NOTE, lw=0.6))

    # legend
    lx, ly = X_LED - BW / 2 - 0.05, Y[5] + 0.45
    ax.text(lx, ly, "legend", fontsize=6, fontweight="bold", va="top")
    items = [(C_RAW, "-", C_EDGE, "raw_records level (DAQReader)"), (C_REC, "-", C_EDGE, "records level (pulses)"),
             (C_PK, "-", C_EDGE, "peaks level"), (C_EV, "-", C_EDGE, "events level")]
    for k, (fc, ls, ec, t) in enumerate(items):
        yy = ly - 0.32 - 0.25 * k
        ax.add_patch(Rectangle((lx, yy - 0.08), 0.32, 0.16, fc=fc, ec=ec, lw=0.6))
        ax.text(lx + 0.42, yy, t, fontsize=5.0, va="center")
    yy = ly - 0.32 - 0.25 * 4
    for k, (ls, ec, lw, t) in enumerate([((0, (3, 1.5)), C_EDGE, 0.8, "saved only when asked for (TARGET)"),
                                         ((0, (1, 1.2)), C_NOTE, 0.8, "not in the default context (xams_led())"),
                                         ("-", C_INFO, 2.0, "what an analysis reads first")]):
        y2 = yy - 0.25 * k
        ax.add_patch(Rectangle((lx, y2 - 0.08), 0.32, 0.16, fc="white", ec=ec, lw=lw, ls=ls))
        ax.text(lx + 0.42, y2, t, fontsize=5.0, va="center")
    y3 = yy - 0.25 * 3 - 0.08
    lines = [("bold line", "output data type(s)"), ("2nd / 3rd", "plugin class, version, file:line"), ("← …", "depends_on (all inputs)"),
             ("cfg:", "main settings (options)")]
    for k, (a, b) in enumerate(lines):
        ax.text(lx, y3 - 0.22 * k, a, fontsize=5.0, va="center", color=C_CFG if a == "cfg:" else ("#555555" if a.startswith("←") else "black"),
                fontweight="bold" if a == "bold line" else None)
        ax.text(lx + 0.75, y3 - 0.22 * k, b, fontsize=5.0, va="center")
    fig.tight_layout(pad=0.05)
    fig.savefig(FIG / "stream.png", dpi=230)
    plt.close(fig)


# ------------------------------------------------------------------------------------------------ note
def P(t, st=base):
    return Paragraph(t, st)


def box(flow, bg):
    t = Table([[flow]], colWidths=[W])
    t.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), bg), ("BOX", (0, 0), (-1, -1), 0.6, GRID),
                           ("LEFTPADDING", (0, 0), (-1, -1), 7), ("RIGHTPADDING", (0, 0), (-1, -1), 7),
                           ("TOPPADDING", (0, 0), (-1, -1), 4), ("BOTTOMPADDING", (0, 0), (-1, -1), 4)]))
    return t


def table(data, widths):
    d = [[Paragraph(str(c), hdr if i == 0 else small) for c in r] for i, r in enumerate(data)]
    t = Table(d, colWidths=widths, repeatRows=1)
    t.setStyle(TableStyle([("GRID", (0, 0), (-1, -1), 0.4, GRID), ("BACKGROUND", (0, 0), (-1, 0), NAVY), ("VALIGN", (0, 0), (-1, -1), "TOP")] +
                          [("BACKGROUND", (0, i), (-1, i), LIGHT) for i in range(2, len(data), 2)]))
    return t


def img(name, caption, width=W):
    from reportlab.lib.utils import ImageReader
    p = FIG / name
    iw, ih = ImageReader(str(p)).getSize()
    return KeepTogether([Image(str(p), width=width, height=width * ih / iw), P(caption, cap)])


def esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def fmt_default(v):
    s = repr(v) if not isinstance(v, str) else v
    if isinstance(v, str) and v.startswith("rundoc://"):
        return "run doc"
    return s if len(s) <= 22 else s[:20] + "…"


def plugin_row(i, c):
    p = PL[c]
    where = f"<b>{c}</b> v{esc(p.get('__version__', '?'))}<br/>{esc(p['file'])}:{p['line']}"
    if p["compute"]:
        where += f"<br/>compute :{p['compute']}"
    cfg = "<br/>".join(f"{n} = {esc(fmt_default(p['cfg'][n]))}" for n in KEYCFG[c]) or "–"
    return [str(i), f"<b>{esc(' + '.join(p['provides']))}</b><br/><font color='#4A5A6A'>{esc(save_mode(p))}</font>", where,
            esc(", ".join(p["depends_on"])) or "(files)", cfg, esc(WHAT[c])]


EVENT_TABLES = ("event_windows", "events", "sites", "event_peaks", "peak_context", "event_cuts")


def data_info():
    """columns (name, type, comment) of the event tables, from st.data_info of this checkout (no run database)"""
    sys.path.insert(0, str(AX))
    import amstrax  # noqa: E402
    assert Path(amstrax.__file__).resolve().is_relative_to(AX.resolve()), f"amstrax from {amstrax.__file__}, not {AX}"
    st = amstrax.contexts.xams(output_folder="/tmp/amstrax_note_none", init_rundb=False)
    out = {}
    for dt in EVENT_TABLES:
        info = st.data_info(dt)
        c = list(info.columns)
        out[dt] = [(str(r[c[0]]), str(r[c[1]]), str(r[c[2]])) for _, r in info.iterrows()]
    return out


def fig_tables():
    """the event tables and their keys"""
    fig, ax = plt.subplots(figsize=(7.1, 3.15))
    ax.set_xlim(0, 10); ax.set_ylim(0, 4.5); ax.axis("off")
    boxes = {  # name: (x, y, w, h, colour, lines)
        "event_windows": (0.1, 3.35, 2.75, 1.0, C_LIVE, ["one row per event window", "event_number, time, endtime",
                                                         "base of all event tables"]),
        "events": (0.1, 1.55, 2.75, 1.55, C_EV, ["one row per event", "n_peaks, n_s1, n_s2, n_sites(_in_drift)",
                                                 "s1_peak_index", "cs1, cs2, cs2_top, cs2_bottom (sums)", "energy",
                                                 "dt_prev_event, dt_next_event"]),
        "event_cuts": (0.1, 0.1, 2.75, 1.2, C_EV, ["one row per event (same rows)", "cut_basic, cut_iso_s1, cut_iso_s2,",
                                                   "cut_s1_aft, cut_s2_width, cut_c,", "cut_fiducial, cut_all"]),
        "sites": (3.55, 1.55, 2.6, 1.55, "#FCE8D5", ["one row per S2 > 300 PE in an event", "event_number, peak_index, rank",
                                                      "drift_time, x, y, z, r", "cs2, cs2_top, cs2_bottom", "dt_prev_s2, in_drift"]),
        "event_peaks": (3.55, 3.35, 2.6, 1.0, C_PK, ["one row per peak in an event window", "event_number, peak_index, type, rank,",
                                                      "dt_s1, area, AFT, widths, x, y"]),
        "peak_context": (6.95, 3.35, 2.95, 1.0, "#EFE6F5", ["one row per peak of the run", "peak_index, event_number (-1 between)",
                                                         "last S2 > 1 kPE / 10 kPE: dt, area, event"]),
        "peaks · peak_basics · peak_positions": (6.95, 1.55, 2.95, 1.55, C_PK, ["one row per peak of the run",
                                                                            "row i = peak_index i (full run)",
                                                                            "waveforms (data, data_top),", "area_per_channel,",
                                                                            "u, v, x_cgr, y_cgr, max_diff"]),
    }
    for name, (x, y, w, h, fc, lines) in boxes.items():
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.06", fc=fc, ec=C_EDGE,
                                    lw=2.0 if name in ("events", "event_cuts") else 0.8))
        ax.text(x + 0.08, y + h - 0.1, name, fontsize=7, fontweight="bold", va="top")
        ax.text(x + 0.08, y + h - 0.36, "\n".join(lines), fontsize=5.6, va="top", linespacing=1.3)

    def link(p, q, lab, col):
        ax.add_patch(FancyArrowPatch(p, q, arrowstyle="-", lw=1.0, color=col))
        ax.text((p[0] + q[0]) / 2, (p[1] + q[1]) / 2 + 0.06, lab, fontsize=5.2, color=col, ha="center", va="bottom")
    ce, cp = "#1F3A5F", "#B03A2E"
    link((2.85, 2.3), (3.55, 2.3), "event_number", ce)
    link((2.85, 3.85), (3.55, 3.85), "event_number", ce)
    link((1.2, 1.55), (1.2, 1.3), "", ce)
    ax.text(1.27, 1.42, "same rows", fontsize=5.2, color=ce, va="center")
    link((4.85, 3.35), (4.85, 3.1), "", cp)
    ax.text(4.92, 3.22, "peak_index", fontsize=5.2, color=cp, va="center")
    link((6.15, 3.85), (6.95, 3.85), "peak_index", cp)
    link((6.15, 2.3), (6.95, 2.3), "peak_index", cp)
    ax.text(3.2, 0.75, "main S1 = event_peaks row with type 1 and dt_s1 = 0 (largest S1 before the main S2)\n"
            "main S2 = sites row with rank 1 (largest S2) and its event_peaks row\n"
            "amstrax.main_interaction(st, run): events + main S1 + main site + its S2 in one flat row (join, not stored)",
            fontsize=5.6, va="center", ha="left", color=C_NOTE, linespacing=1.4)
    fig.tight_layout(pad=0.1)
    fig.savefig(FIG / "tables.png", dpi=220)
    plt.close(fig)


# event_info column -> new format (old format: tag prod4-final; checked on run 007790, see the handbook)
OLD_MAP = [
    ["event_info (amstrax ≤ 2.2)", "amstrax 3", "note"],
    ["time, endtime, event_number, n_peaks", "events (same names)", "same windows for the same settings"],
    ["cs1", "events.cs1", "same for one site; with more sites the S1 response is the cs2_top-weighted mean"],
    ["cs2, cs2_top", "main site: sites.cs2 / cs2_top of rank 1 (main_interaction: site_cs2, site_cs2_top)",
     "events.cs2, cs2_top are <b>sums over all in-drift sites</b>"],
    ["energy", "events.energy", "from the sums; the same for one site"],
    ["alt_cs1 / alt_cs2", "— / sites.cs2 of rank 2", "alt S1: event_peaks row, uncorrected; alt S2 only if > 300 PE"],
    ["drift_time, x, y, z, r (and s2_x, s2_y)", "sites of rank 1 (main_interaction: same names)", "every site has them"],
    ["phi", "arctan2(y, x)", "not stored"],
    ["u, v, s2_x_cgr, s2_y_cgr", "peak_positions[peak_index of the main S2]", "not in the event tables"],
    ["s1_* / s2_* peak properties", "event_peaks row of the main S1 / S2 (main_interaction: s1_area, s1_area_fraction_top, "
     "s1_range_50p_area, s1_n_channels, s1_tight_coincidence, s2_area, s2_area_fraction_top, s2_range_50p_area, "
     "s2_range_90p_area)", "the rest from event_peaks, without prefix"],
    ["alt_s1_*, alt_s2_*", "event_peaks rows (rank, dt_s1)", "second largest made explicit by rank"],
    ["s1_index, s2_index", "events.s1_peak_index, sites.peak_index", "<b>changed meaning</b>: row in the run, not position in the event"],
    ["s1_max_diff, s1_min_diff", "peaks[peak_index].max_diff / min_diff", ""],
    ["area_before_main_s2, large_s2_before_main_s2", "events (same names)", ""],
    ["event_area_per_channel, event_waveform", "peaks[peak_index]: area_per_channel, data, data_top", "removed types"],
    ["peak_coincidences, event_coincidences", "—", "removed: match peak_basics_ext in time"],
    ["events (windows); events_v2 (2.x optional)", "event_windows; events", "renamed in 3.0"],
]


def main():
    FIG.mkdir(parents=True, exist_ok=True)
    fig_generic(); fig_stream(); fig_tables()
    cols = data_info()
    out = sys.argv[1] if len(sys.argv) > 1 else str(HERE / "XAMS_amstrax_overview.pdf")
    doc = SimpleDocTemplate(out, pagesize=A4, leftMargin=15 * mm, rightMargin=15 * mm, topMargin=12 * mm, bottomMargin=12 * mm,
                            title="strax + amstrax 3: the XAMS reconstruction stream and event tables", author="XAMS")
    l_raw = find_line("auto_processing_new/process.py", r"raw_st\.make\(")
    l_mk = find_line("auto_processing_new/process.py", r"self\.st\.make\(")
    l_main = find_line("auto_processing_new/process.py", r"^def main")
    l_ctx = find_line("contexts.py", r"^def xams\(")
    l_ctxr = find_line("contexts.py", r"^def context_for_daq_reader")
    l_led = find_line("contexts.py", r"^def xams_led")
    l_corr = find_line("contexts.py", r"^def apply_global_correction_version")
    l_cfg = find_line("xams_config.py", r"^class XAMSConfig")
    l_rd = find_line("xams_config.py", r"def _fetch_from_rundoc_url")
    l_db = find_line("rundb.py", r"^class RunDB")
    l_reg = find_line("contexts.py", r"register=\[")
    l_cv = find_line("auto_processing_new/process.py", r"--corrections_version")
    main_chain = ["DAQReader", "PulseProcessing", "Peaks", "PeakBasics", "PeakPositions", "EventWindows", "EventPeaks",
                  "Sites", "Events", "PeakContext", "EventCuts"]
    side = ["PulseProcessingEXT", "PeaksEXT", "PeakBasicsEXT", "PulseProcessingSiPM", "PeaksSiPM", "PeakBasicsSiPM",
            "RecordsLED", "LEDCalibration"]
    assert set(main_chain + side) == set(PL)
    widths = [8 * mm, 31 * mm, 40 * mm, 25 * mm, 33 * mm, W - 137 * mm]
    head = ["#", "output (save)", "plugin · file:line", "depends_on", "main settings (default)", "what it does"]

    # corrections table
    corr_rows = [["setting", f"value in _global_{CORR_VERSION}.json", "used by"]]
    for k, v in CORR.items():
        u = users_of(k)
        corr_rows.append([esc(k), esc(v if isinstance(v, str) else json.dumps(v)), esc(", ".join(u)) if u else "<i>no plugin option of this name</i>"])

    s = [P("strax + amstrax 3: the XAMS reconstruction stream and event tables", h1),
         P(f"Explainer — from redax chunks to the event tables, where every plugin enters, what the tables hold and "
           f"where the old event_info columns went (amstrax {esc(GIT)}, generated from the source; 2026-11-09)", sub),
         box(P("<b>In one sentence.</b> <b>strax</b> is a generic engine: it knows how to chain plugins, stream data "
               "in time chunks and store results under a hash of everything that made them. <b>amstrax</b> is the XAMS "
               "part: the plugins (what happens to pulses, peaks and events), the contexts (which plugins, where data "
               "lives, the run database) and the settings (run doc, corrections from amstrax_files). You ask for "
               "one data type; strax works out and runs everything below it."), GREEN),
         P("1. strax in seven words", h2),
         table([["word", "meaning", "in amstrax"],
                ["plugin", "Python class: <b>depends_on</b> (inputs), <b>provides</b> (output name), <b>dtype</b> (columns), "
                           "<b>compute()</b> (the work), options as class attributes", f"{len(PL)} plugin classes in amstrax/plugins/"],
                ["data type", "named product, a numpy structured array; every row has time and endtime (ns since 1970)",
                 "raw_records, records, peaks, peak_basics, events, sites, …"],
                ["data kind", "data types with the same rows (one per pulse / peak / event); a plugin that depends on several "
                              "of the same kind gets them merged column-wise", "'peaks' = peaks + peak_basics + peak_positions"],
                ["context", "registry of plugins + settings + storage list; st.get_array / st.make go through it",
                 f"contexts.py:{l_ctx} xams(), :{l_ctxr} context_for_daq_reader(), :{l_led} xams_led()"],
                ["option", "setting of a plugin (default in the class, overridden with st.set_config); tracked options "
                           "enter the lineage", f"XAMSConfig (xams_config.py:{l_cfg}): also file://, rundoc:// values"],
                ["lineage", "hash of plugin name + version + tracked options + lineage of the inputs: same hash = same data, "
                            "change anything → new hash → recomputed, never mixed", "data folders <run>-<type>-<hash>"],
                ["storage", "where strax looks for / writes data, in order; save_when decides what is written",
                 f"RunDB (rundb.py:{l_db}, run doc data[]) + DataDirectory folders"]],
               [20 * mm, 92 * mm, W - 112 * mm]),
         P("2. What happens when you ask for a data type", h2),
         img("generic.png", "Figure 1. One request, generic strax. The dashed arrow: after computing (and saving) you get the "
             "same array as when it was loaded. Only what is missing is computed; a changed setting or plugin version "
             "makes a new lineage, so the old data is not used for it."),
         P("3. How the central processing on STBC runs it", h2),
         P(f"auto_processing_new/process.py (main :{l_main}) runs per run, as xamsdata. <b>Stage 1</b> (:{l_raw}): "
           f"<font name='Arial-Bold'>raw_st.make(run, 'raw_records')</font> in the DAQ-reader context — only there may the "
           f"DAQReader run; it reads the live data. <b>Stage 2</b> (:{l_mk}): <font name='Arial-Bold'>st.make(run, target)</font> "
           f"for the other targets in the main context; raw_records are only read there (forbid_creation_of). Targets come "
           f"from the command line (online and offline: peak_basics events sites event_peaks peak_context event_cuts); "
           f"peak_basics_ext / peak_basics_sipm are added when the run's DAQ configuration enables those channels. "
           f"<b>Corrections</b>: --corrections_version (:{l_cv}, default None) picks amstrax_files/corrections/_global/_global_&lt;version&gt;.json; "
           f"apply_global_correction_version (contexts.py:{l_corr}) turns every entry into st.set_config. Without it, "
           f"gain_to_pe_array stays None → to_pe = 1 → peak areas in ADC·samples, not PE. "
           f"Since 2026-10-18 the online loop runs with <b>PROD0</b> (approved gains + position map): runs with "
           f"approved gains get PE; later runs (null gains) stay in ADC·samples. Positions come from the map in both cases. "
           f"The SR-2026.3 science runs 7762–8109 are reprocessed with <b>PROD5</b> (2026-11-06: amstrax 3.1, corrections v10, "
           f"cut references; table in section 7)."),
         PageBreak(),
         P("4. The reconstruction stream", h2),
         img("stream.png", width=0.93 * W, caption="Figure 2. Every amstrax plugin and where it enters. Columns: external PMT (NaI) | TPC PMTs (main chain) "
             "| SiPMs | LED calibration. Each box is one plugin; its text is read from the source. Arrows show the main flow; "
             "the full input list is the ← line in each box. Dashed frame: one data kind (same rows, more columns); the "
             "event-level boxes name their data kind. Thick frame: what an analysis reads first. "
             f"Registered in the default context: contexts.py:{l_reg}."),
         PageBreak(),
         P("5. Main chain, stage by stage", h2),
         P("Paths relative to amstrax/amstrax/. The line numbers are those of the checkout named on page 1."),
         table([head] + [plugin_row(i, c) for i, c in enumerate(main_chain, 1)], widths),
         P("6. Side branches", h2),
         table([head] + [plugin_row(i, c) for i, c in enumerate(side, 1)], widths),
         PageBreak(),
         P("7. Where the settings come from (later overrides earlier)", h2),
         table([["#", "source", "what / where"],
                ["1", "plugin default", "default= of each option in the plugin class (column 'main settings' above)"],
                ["2", "context", f"XAMS_COMMON_CONFIG in contexts.py (now only n_tpc_pmts = 5); without a run database "
                                 f"(init_rundb=False) also the default channel_map"],
                ["3", "run doc", "options with a rundoc:// default read the run's own value: channel_map from "
                                 "xams_bookkeeping.channel_map, else daq_config.channel_map, else the XAMS default "
                                 f"(xams_config.py:{l_rd}). Plugins: " + esc(", ".join(rundoc_classes())) +
                                 ". DAQReader also gets chunk length, fragment size, compressor and threads from daq_config."],
                ["4", "corrections_version", f"_global_&lt;version&gt;.json in amstrax_files: file:// entries are fetched per run "
                                             f"(run ranges inside the file), numbers set directly. Table below: {CORR_VERSION}."],
                ["5", "st.set_config(…)", "your own overrides in an analysis (e.g. a smaller peak gap for single photons) → own "
                                          "lineage, own data folder"]],
               [6 * mm, 30 * mm, W - 36 * mm]),
         Spacer(1, 4),
         table(corr_rows, [38 * mm, 62 * mm, W - 100 * mm]),
         P("8. Where the SiPM and NaI data are", h2),
         box(P("<b>Up to peaks only.</b> raw_records_sipm → records_sipm → peaks_sipm → peak_basics_sipm, and the same for the "
               "external NaI (_ext). No plugin reads them at event level. Match peak_basics_sipm / peak_basics_ext in time to "
               "the event tables (event_peaks, sites) in your own analysis, or rebuild from raw_records_sipm. The plugins "
               "read the run's own channel_map (both SiPM channels, 6 and 7, since PR #340, 2026-10-17)."), AMBER),
         KeepTogether([P("9. Things to know", h2),
         P("• <b>Positions</b>: the (u, v) → (x, y) map per peak (peak_positions; PROD5: "
           f"{esc(CORR.get('pos_rec_map', '?'))}); NaN only for runs without a map. Without gains (u, v) are in "
           "ADC·samples (~2 mm median shift).", small),
         P("• <b>Event window</b>: triggers are S2s above trigger_min_area; plugin defaults left 500 µs / right 50 µs, PROD5 "
           f"{esc(CORR.get('left_event_extension', '?'))} / {esc(CORR.get('right_event_extension', '?'))} ns with "
           f"trigger_min_area = {esc(CORR.get('trigger_min_area', '?'))} PE. Delayed light up to ms after an S2 lies "
           "outside the events: peak_context has it.", small),
         P("• <b>Polarity</b>: PulseProcessing flips every TPC channel (baseline_per_channel(flip=True)); the polarity "
           "registers of the run are not read.", small),
         P("• <b>records are not kept</b>: PulseProcessing saves records only as a target (pulse_counts always). Anything "
           "above records needs the raw_records again if it must be recomputed.", small),
         P("• <b>Renewing</b>: python docs/notes/make_amstrax_note.py after a change to amstrax — names, versions, lines, "
           "dependencies and the columns of the event tables follow; a new or removed plugin stops the script until it gets "
           "a place and a description.", small)]),
         PageBreak(),
         P("10. The event tables (amstrax 3)", h2),
         P("Since amstrax 3.0 (deployed 2026-11-06) the event level is four linked tables on top of the event windows, plus "
           "one table of cut booleans. Each holds one kind of object: the event, the interaction site (S2), the peak. "
           "Nothing is copied between them; they link by <b>event_number</b> and <b>peak_index</b> (the row of the peak in "
           "the run's peaks, counted from the first peak). The old wide row per event (event_info) is gone; "
           "<font name='Arial-Bold'>amstrax.main_interaction(st, run)</font> makes its equivalent as a join."),
         img("tables.png", "Figure 3. The event tables and their keys. events and event_cuts have the same rows (data kind "
             "events), so one st.get_df(run, ['events', 'event_cuts']) loads both; sites, event_peaks and peak_context are "
             "their own data kinds and are loaded separately and joined by key."),
         img("example_event.png", "Figure 4. Where the peaks of an event go (sketch): the S1 and every peak in the window are "
             "rows of event_peaks; every S2 above 300 PE is also a site (rank 1 = main S2); peaks after the window, until the "
             "next event, are only in peak_context, which links every peak to the last large S2 before it."),
         table([["table", "one row per", "made from", "use it for"],
                ["events", "event", "event_windows, event_peaks, sites",
                 "spectra, rates; cs1 and the sums of cs2 over the in-drift sites; energy = W (cs1/g1 + cs2_top/g2_top)"],
                ["sites", "S2 above site_min_s2_area (300 PE) in an event", "event_peaks + corrections",
                 "position and corrected S2 of every interaction; multiple scatters; the main site (rank 1)"],
                ["event_peaks", "peak inside an event window", "event_windows, peak_basics, peak_positions",
                 "properties of the main S1 / S2 and of every other peak of the event (rank, dt_s1)"],
                ["peak_context", "peak of the run (also between events)", "event_windows, peak_basics",
                 "delayed electrons, photoionisation: time since and area of the last S2 > 1 kPE / > 10 kPE"],
                ["event_cuts", "event (same rows as events)", "events, sites, event_peaks + cut references",
                 "cut_all and the single cuts; rejected events stay in the data"],
                ["event_windows", "event", "peaks, peak_basics", "only where events start and stop; not an analysis table"]],
               [22 * mm, 36 * mm, 42 * mm, W - 100 * mm]),
         P("<b>Definitions.</b> Main S2 = largest S2 of the event (site of rank 1). Main S1 = largest S1 before the main S2 "
           "(event_peaks row with type 1 and dt_s1 = 0). dt_s1 = center_time minus that of the main S1; for a site it is the "
           "drift time. A site is in_drift if 0 ≤ drift_time ≤ 1.05 × drift_time_cathode; only those enter the sums of "
           "events. Fake sites (split S2s, photoionisation, trains) are kept: on science run 007790 about 40 % of the "
           "events with a site have two or more in-drift sites; select single scatters with cut_iso_s2 (in cut_all) or "
           "n_sites_in_drift == 1.", small),
         P("<b>Corrections</b> (Sites, Events; options of _Corrections, the names of the former corrected_areas). Mode 1, "
           "when sr_corrections and sr_maps are set (PROD5): cs2 = S2 · exp(drift_time / τ(t)) / f2(x, y) with the "
           "electron lifetime τ of the run, the S2_top (x, y) map and the radial S2_top factor where the maps hold one; "
           "cs1 = S1 / response, the S1 (x, y) map × S1(drift time) averaged over the in-drift sites with weight cs2_top; "
           "energy if the run is calibrated. Mode 2 (no maps): cs2 = S2 · exp(drift_time / elife), cs1 with the naive "
           "linear z correction. cs2_top = cs2 · AFT, cs2_bottom = cs2 − cs2_top. For an event with one site the values "
           "equal those of the former event_info (validated on 65 runs).", small),
         P("<b>Cuts</b> (event_cuts): cut_basic (main S1 and main S2 > 300 PE), cut_iso_s1 (alternative S1 &lt; max(10 % of "
           "the main S1, 30 PE)), cut_iso_s2 (no other S2 ≥ 5 % of the main S2), cut_s1_aft and cut_s2_width (3σ bands "
           "against drift time), cut_c (odd-event cut: S2 AFT and width against references), cut_fiducial, cut_all. "
           "References per run set in amstrax_files (event_cut_references); has_references is False outside a calibrated "
           "set and then the reference cuts are False.", small),
         P("11. Columns of the event tables", h2),
         P(f"From st.data_info of this checkout. Times in ns since 1970, widths in ns, areas in PE, positions in mm.", small),
         ] + [KeepTogether([P(f"<b>{dt}</b>", base),
                            table([["column", "type", "meaning"]] + [[f"<b>{esc(n)}</b>", esc(t), esc(c)] for n, t, c in cols[dt]],
                                  [36 * mm, 16 * mm, W - 52 * mm]), Spacer(1, 4)])
              for dt in ("event_windows", "events", "sites", "event_peaks", "peak_context", "event_cuts")] + [
         PageBreak(),
         P("12. Loading", h2),
         P("<font name='Arial-Bold'>ev = st.get_df(run, ['events', 'event_cuts'])</font> — one frame (same data kind). "
           "<font name='Arial-Bold'>m = amstrax.main_interaction(st, run)</font> — one flat row per event: the events "
           "columns, s1_area, s1_area_fraction_top, s1_range_50p_area, s1_n_channels, s1_tight_coincidence, drift_time, "
           "x, y, z, r, site_cs2, site_cs2_top, site_cs2_bottom, site_in_drift, s2_area, s2_area_fraction_top, "
           "s2_range_50p_area, s2_range_90p_area. <font name='Arial-Bold'>st.get_df(run, 'sites')</font>, "
           "<font name='Arial-Bold'>'event_peaks'</font>, <font name='Arial-Bold'>['peak_basics', 'peak_context']</font> — "
           "joined by key. Waveforms and per-channel areas: <font name='Arial-Bold'>st.get_array(run, 'peaks')[peak_index]</font> "
           "(full run). Worked examples (spectrum, multiple scatters, all peaks of an event, delayed signals, waveforms, "
           "porting old code), checked on run 007790: XAMS handbook, Data processing → Data format: use cases.", small),
         P("13. The old event format (amstrax ≤ 2.2, PROD0–PROD4)", h2),
         P("Until 2026-11-06 the event level was one wide row per event: events (the windows) → event_basics (main and "
           "alternative S1 / S2 with their peak properties copied into the row, drift time) → event_positions (x, y, r, "
           "phi, z, u, v of the main S2) → corrected_areas (cs1, cs2 and cs2_top of the main S2, alt_cs1 / alt_cs2, "
           "energy) → event_info (stored merge of the three, 98 columns), plus event_area_per_channel, event_waveform and "
           "the NaI coincidences. Data processed then keep these types on disk; the current amstrax cannot load them. "
           "Read them with the frozen environment of tag prod4-final: "
           "<font name='Arial-Bold'>source /data/xenon/xams_v2/setup_prod4.sh</font>, then "
           "st.get_df(run, 'event_info') with corrections_version='PROD4' (reading only, never new processing). "
           "All old columns: XAMS handbook, Data format: old format.", small),
         table(OLD_MAP,
               [44 * mm, 66 * mm, W - 110 * mm]),
         P("Checked on run 007790 (173 651 events in both formats, the same event numbers): for single-site events "
           "drift_time, x, y of the main site equal those of event_info and z agrees within 0.1 %; cs1, cs2 and energy "
           "differ by 0.02–0.04 % (median), from the corrections (PROD4 → PROD5), not from the format.", small),
         ]
    doc.build(s)
    print("written", out)


if __name__ == "__main__":
    main()
