"""Explainer: how strax + amstrax turn redax chunks into event_info, and where each plugin enters the reconstruction
stream. Plugin names, files, line numbers, versions, depends_on and settings are read from this amstrax checkout
(parsed with ast), corrections from amstrax_files next to it (../amstrax_files, or $XAMS_AMSTRAX_FILES_DIR); the
layout and the short descriptions are written here. No data. Needs matplotlib and reportlab.
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
CORR_VERSION = "v4"

# ------------------------------------------------------------------------------------------------ read amstrax source
def camel_to_snake(name):
    """as strax.camel_to_snake: LEDCalibration -> led_calibration"""
    return re.sub(r"((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))", r"_\1", name).lower()


def as_tuple(v):
    if v is None:
        return ()
    return (v,) if isinstance(v, str) else tuple(v)


def parse_plugins():
    """every strax plugin class under amstrax/plugins: file, lines, version, depends_on, provides, settings"""
    out = {}
    for f in sorted((AX / "amstrax" / "plugins").rglob("*.py")):
        tree = ast.parse(f.read_text())
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
                elif tgt in ("depends_on", "provides", "__version__", "save_when"):
                    try:
                        p[tgt] = ast.literal_eval(val)
                    except Exception:
                        p[tgt] = ast.unparse(val)
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
    return {m for line in block.splitlines() if not line.strip().startswith("#") for m in re.findall(r"ax\.(\w+)", line)}


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
    "Events": "Triggers = peaks with area > trigger_min_area; window [t − left_event_extension, t + right_event_extension]; "
              "overlapping windows merged (find_peak_groups). Only time windows, no physics.",
    "EventBasics": "Per event: main and alt S1 / S2 (largest; S2 within the maximum drift), drift time, properties of "
                   "those peaks (incl. s2_x, s2_y from peak_positions) copied into the event row.",
    "EventPositions": "x, y, r, phi of main and alt S2 from event_basics (map positions), u, v, z from the drift time "
                      "(drift_time_gate, drift_time_cathode, gate_cathode_distance).",
    "CorrectedAreas": "cS1 (naive z correction) and cS2 (electron lifetime elife), each also split top / bottom.",
    "EventInfo": "Merge only: event_basics + corrected_areas + event_positions in one table. What the analysis reads.",
    "PulseProcessingEXT": "Baseline + integration for the external channel (NaI).",
    "PeaksEXT": "Hit and peak finding on records_ext.",
    "PeakBasicsEXT": "Peak properties of the external peaks.",
    "PeakCoincidences": "TPC peaks matched in time to external peaks (max_delay; 511-keV window).",
    "EventCoincidences": "Events matched in time to external peaks.",
    "PulseProcessingSiPM": "Baseline + integration for the SiPM channels.",
    "PeaksSiPM": "Hit and peak finding on records_sipm (area_per_channel up to max_sipm_channel).",
    "PeakBasicsSiPM": "Peak properties of the SiPM peaks.",
    "EventAreaPerChannel": "Area per channel and number of channels of main / alt S1 and S2.",
    "EventWaveform": "Summed waveform (total and top) of main / alt S1 and S2.",
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
    "Events": ["trigger_min_area", "left_event_extension", "right_event_extension"],
    "EventBasics": ["electron_drift_velocity", "max_drift_length"],
    "EventPositions": ["drift_time_gate", "drift_time_cathode"],
    "CorrectedAreas": ["elife", "s1_naive_z_correction"],
    "EventInfo": [],
    "PulseProcessingEXT": [], "PeaksEXT": [], "PeakBasicsEXT": [],
    "PeakCoincidences": ["max_delay"], "EventCoincidences": ["max_delay"],
    "PulseProcessingSiPM": ["baseline_samples"], "PeaksSiPM": ["gain_to_pe_array", "max_sipm_channel", "channel_map"],
    "PeakBasicsSiPM": ["channel_map"], "EventAreaPerChannel": ["channel_map"], "EventWaveform": [],
    "RecordsLED": ["negative_polarity_channels"], "LEDCalibration": ["led_window", "noise_window"],
}
for c, names in KEYCFG.items():
    missing = [n for n in names if n not in PL[c]["cfg"]]
    assert not missing, f"{c}: options {missing} not in the source any more"
assert set(KEYCFG) == set(PL), f"plugins without description: {set(PL) ^ set(KEYCFG)}"

# where your own analysis code enters the stream (measurements/analysis_SR2026.2)
TAGS = {"raw:raw_records": ["ED"], "raw:raw_records_sipm": ["ED", "DS"], "Peaks": ["ED"],
        "PeakBasics": ["ED", "DS"], "EventInfo": ["ED", "DS"]}

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
    "PeakCoincidences": (X_EXT, 6), "PeakPositions": (X_TPC, 6),
    "Events": (X_TPC, 7),
    "EventCoincidences": (X_EXT, 8), "EventBasics": (X_TPC, 8),
    "EventPositions": (X_TPC, 9), "EventAreaPerChannel": (X_SIPM, 9),
    "CorrectedAreas": (X_TPC, 10), "EventWaveform": (X_SIPM, 10),
    "EventInfo": (X_TPC, 11),
}
assert set(PL) - {"DAQReader"} <= set(POS), f"plugins without a place in the figure: {set(PL) - set(POS) - {'DAQReader'}}"
LEVEL = {"raw": C_RAW, "rec": C_REC, "pk": C_PK, "ev": C_EV}


def level_of(node):
    if node.startswith("raw:"):
        return "raw"
    row = POS[node][1]
    if node in ("RecordsLED", "LEDCalibration") or row == 3:
        return "rec"
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
    for r0, r1, lab in ((4, 6, "data kind 'peaks': one row per peak — peak_basics and peak_positions add columns to the same rows"),
                        (7, 10, "data kind 'events': one row per event — event_basics, event_positions, corrected_areas add columns")):
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
            lines = [outs, f"{n}  v{p.get('__version__', '?')}"]
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
            if n == "EventInfo":
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
                 ("EventBasics", "EventPositions"), ("EventPositions", "CorrectedAreas")):
        arrow(bot(a), top(b))
    # skip edge inside the peaks kind: peaks -> peak_positions (right side)
    # (arc arms are in points)
    arrow(right("Peaks", -0.25), right("PeakPositions", 0.25), cs="arc,angleA=0,angleB=0,armA=7,armB=7,rad=3")
    # LED inputs (LED context only)
    arrow((X_TPC + BW / 2 - 0.15, Y[2] - BH / 2), (X_LED - 0.5, Y[3] + BH / 2), ls=(0, (3, 2)), color=C_NOTE)
    arrow((X_SIPM + BW / 2 - 0.15, Y[2] - BH / 2), (X_LED - 0.15, Y[3] + BH / 2), ls=(0, (3, 2)), color=C_NOTE)
    # peaks kind -> events, events -> event_basics, peaks kind -> event_basics
    y_pk_bottom = Y[6] - BH / 2 - 0.07
    arrow((X_TPC, y_pk_bottom), top("Events"))
    ax.text(X_TPC + 0.08, Y[7] + BH / 2 + 0.08, "peaks + peak_basics", fontsize=4.8, color=C_EDGE, va="center")
    arrow(bot("Events"), top("EventBasics"))
    arrow((X_TPC + BW / 2 + 0.1, Y[5]), right("EventBasics", 0.2), cs="arc,angleA=0,angleB=0,armA=16,armB=16,rad=4")
    ax.text(X_TPC + BW / 2 + 0.42, Y[7], "peak_basics,\npeak_positions\n(+ peaks)", fontsize=4.8, color=C_EDGE,
            va="center", ha="left")
    # events kind -> event_info
    y_ev_bottom = Y[10] - BH / 2 - 0.07
    arrow((X_TPC, y_ev_bottom), top("EventInfo"), lw=1.2)
    ax.text(X_TPC + BW / 2 + 0.15, (y_ev_bottom + Y[11] + BH / 2) / 2, "merge: event_basics + event_positions\n+ corrected_areas",
            fontsize=4.8, color=C_EDGE, va="center")
    # coincidences with the external peaks (left gutter) and event extras
    arrow(left("PeaksEXT", -0.25), left("PeakCoincidences", 0.15), cs="arc,angleA=180,angleB=180,armA=9,armB=9,rad=3")
    arrow(left("PeaksEXT", 0.05), left("EventCoincidences", 0.15), cs="arc,angleA=180,angleB=180,armA=17,armB=17,rad=3")
    arrow(left("Peaks", -0.3), right("PeakCoincidences", 0.25))
    arrow(left("EventBasics"), right("EventCoincidences"))
    arrow(right("EventBasics", -0.25), left("EventAreaPerChannel", 0.2))
    arrow(right("EventBasics", -0.4), left("EventWaveform", 0.2))
    ax.text(X_SIPM, Y[11] + 0.05, "EventAreaPerChannel, EventWaveform\nalso read peaks", ha="center", va="center", fontsize=4.8, color=C_EDGE)

    # no SiPM at event level
    ax.text(X_SIPM + 0.35, Y[6], "✗  no event-level SiPM plugin\nnothing reads peaks_sipm;\nSiPM light per event only in\n"
            "own analysis, rebuilt from\nraw_records_sipm (ED, DS)", ha="center", va="center", fontsize=5.0, color=C_NOTE,
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
                                         ("-", C_INFO, 2.0, "final product, always saved")]):
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
    y4 = y3 - 0.22 * 4 - 0.12
    for k, (tg, t) in enumerate([("ED", "your event display reads it\n(lib/event_display.py)"),
                                 ("DS", "delayed-signal scan reads it\n(studies/delayed_scan.py)")]):
        y5 = y4 - 0.42 * k
        ax.add_patch(FancyBboxPatch((lx + 0.03, y5 - 0.09), 0.26, 0.18, boxstyle="round,pad=0.01,rounding_size=0.05", fc=C_TAG, ec="none"))
        ax.text(lx + 0.16, y5, tg, ha="center", va="center", fontsize=4.6, color="white", fontweight="bold")
        ax.text(lx + 0.42, y5 + 0.07, t, fontsize=5.0, va="top", linespacing=1.15)
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


def main():
    FIG.mkdir(parents=True, exist_ok=True)
    fig_generic(); fig_stream()
    out = sys.argv[1] if len(sys.argv) > 1 else str(HERE / "XAMS_amstrax_overview.pdf")
    doc = SimpleDocTemplate(out, pagesize=A4, leftMargin=15 * mm, rightMargin=15 * mm, topMargin=12 * mm, bottomMargin=12 * mm,
                            title="strax + amstrax: the XAMS reconstruction stream", author="XAMS")
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
    main_chain = ["DAQReader", "PulseProcessing", "Peaks", "PeakBasics", "PeakPositions", "Events", "EventBasics",
                  "EventPositions", "CorrectedAreas", "EventInfo"]
    side = ["PulseProcessingEXT", "PeaksEXT", "PeakBasicsEXT", "PeakCoincidences", "EventCoincidences",
            "PulseProcessingSiPM", "PeaksSiPM", "PeakBasicsSiPM", "EventAreaPerChannel", "EventWaveform",
            "RecordsLED", "LEDCalibration"]
    assert set(main_chain + side) == set(PL)
    widths = [7 * mm, 31 * mm, 40 * mm, 25 * mm, 33 * mm, W - 136 * mm]
    head = ["#", "output (save)", "plugin · file:line", "depends_on", "main settings (default)", "what it does"]

    # corrections table
    corr_rows = [["setting", f"value in _global_{CORR_VERSION}.json", "used by"]]
    for k, v in CORR.items():
        u = users_of(k)
        corr_rows.append([esc(k), esc(v if isinstance(v, str) else json.dumps(v)), esc(", ".join(u)) if u else "<i>no plugin option of this name</i>"])

    s = [P("strax + amstrax: the XAMS reconstruction stream", h1),
         P(f"Explainer — from redax chunks to event_info, and where every plugin enters (amstrax {esc(GIT)}, generated "
           f"from the source; 2026-10-18)", sub),
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
                 "raw_records, records, peaks, peak_basics, events, event_info, …"],
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
           f"from the command line; peak_basics_ext is added when the run has the NaI channel. "
           f"<b>Corrections</b>: --corrections_version (:{l_cv}, default None) picks amstrax_files/corrections/_global/_global_vN.json; "
           f"apply_global_correction_version (contexts.py:{l_corr}) turns every entry into st.set_config. Without it, "
           f"gain_to_pe_array stays None → to_pe = 1 → peak areas in ADC·samples, not PE. "
           f"Since 2026-10-18 the online loop runs with <b>PROD0</b> (approved gains + position map): runs with "
           f"approved gains get PE; later runs (null gains) stay in ADC·samples. Positions come from the map in both cases."),
         PageBreak(),
         P("4. The reconstruction stream", h2),
         img("stream.png", width=0.93 * W, caption="Figure 2. Every amstrax plugin and where it enters. Columns: external PMT (NaI) | TPC PMTs (main chain) "
             "| SiPMs | LED calibration. Each box is one plugin; its text is read from the source. Arrows show the main flow; "
             "the full input list is the ← line in each box. Dashed frames: one data kind (same rows, more columns). "
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
                ["4", "corrections_version", f"_global_vN.json in amstrax_files: file:// entries are fetched per run "
                                             f"(run ranges inside the file), numbers set directly. Table below: {CORR_VERSION}."],
                ["5", "st.set_config(…)", "your own overrides in an analysis, e.g. PHOTON_CONFIG in studies/delayed_scan.py "
                                          "(50 ns gap, no minimum area) → own lineage, own data folder"]],
               [6 * mm, 30 * mm, W - 36 * mm]),
         Spacer(1, 4),
         table(corr_rows, [38 * mm, 62 * mm, W - 100 * mm]),
         P("8. Where the SiPM data is", h2),
         box(P("<b>Up to peaks only.</b> raw_records_sipm → records_sipm → peaks_sipm → peak_basics_sipm. No plugin reads "
               "peaks_sipm, so nothing SiPM reaches events or event_info. The SiPM light per event (event display) and the "
               "SiPM pulse rate after an S2 (delayed scan) are rebuilt in your own analysis from raw_records_sipm, matched in "
               "time to event_info / peak_basics. Since PR #340 (2026-10-17) the plugins read the run's own channel_map and "
               "PeaksSiPM keeps both channels (6 and 7); before that, the context forced sipm = (6, 6) for every plugin "
               "except the DAQReader."), AMBER),
         P("9. Where an analysis enters (example: the SR-2026.2 analysis, acolijn/xams-analysis-sr2026.2)", h2),
         table([["code", "reads", "own reconstruction"],
                ["lib/event_display.py (ED)", "event_info; peaks, peak_basics in the event window; raw_records, raw_records_sipm; "
                                              "to_pe of Peaks, baseline_samples of PulseProcessing",
                 "raw pulses per channel, own baseline, summed waveforms, SiPM PE in the S1 / S2 windows"],
                ["studies/delayed_scan.py (DS)", "peak_basics (central v4, and photon-mode in /tmp); event_info; raw_records_sipm; "
                                                 "to_pe of Peaks",
                 "parents = merged large S2s; photon peaks and SiPM pulses attached to the last parent (time since its end)"],
                ["scratch/sipm_after_s2.py", "event_info, peak_basics, raw_records_sipm",
                 "cumulative SiPM PE in windows after the S2, minus the rate before the S1"]],
               [36 * mm, 70 * mm, W - 106 * mm]),
         KeepTogether([P("10. Things to know", h2),
         P("• <b>Positions: one algorithm</b>: the (u, v) → (x, y) map (posrec v2) per peak, by default the latest map "
           "(pos_rec_map_v1), with or without gains (without gains (u, v) are in ADC·samples: ~2 mm median shift); NaN only "
           "for runs without a map (before 7601). The old polynomial (pos_rec_params, x_corr) is gone. Production (PROD0) "
           f"and analysis (v4, {esc(CORR.get('gain_to_pe_array', '?'))}) give identical x, y for 7601–7723.", small),
         P("• <b>Event window</b>: the plugin defaults are left 500 µs / right 50 µs (Events); v4 sets both to 50 µs. "
           "Delayed light up to ms after an S2 is outside every event — so it is analysed on peaks, not events.", small),
         P("• <b>Polarity</b>: PulseProcessing flips every TPC channel (baseline_per_channel(flip=True)); the polarity "
           "registers of the run are not read. daq_config.extract_channel_polarity exists but nothing calls it now.", small),
         P("• <b>records are not kept</b>: PulseProcessing saves records only as a target (pulse_counts always). Anything "
           "above records needs the raw_records again if it must be recomputed.", small),
         P("• <b>Renewing</b>: python docs/notes/make_amstrax_note.py after a change to amstrax — names, versions, lines and "
           "dependencies follow; a new or removed plugin stops the script until it gets a place and a description.", small)]),
         ]
    doc.build(s)
    print("written", out)


if __name__ == "__main__":
    main()
