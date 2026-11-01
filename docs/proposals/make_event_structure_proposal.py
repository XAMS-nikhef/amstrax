"""Proposal (for discussion): a structured event data model in amstrax — four linked tables (events, sites, event_peaks,
peak_context) instead of the flat main / alt columns. Version 2 (01-11-2026).

Numbers: sizes measured on PROD3 run 7790 (blosc, in memory, nothing written).
Usage: python make_event_structure_proposal.py [XAMS_event_structure_proposal.pdf]
"""
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
from reportlab.lib.utils import ImageReader  # noqa: E402
from reportlab.pdfbase import pdfmetrics  # noqa: E402
from reportlab.pdfbase.pdfmetrics import registerFontFamily  # noqa: E402
from reportlab.pdfbase.ttfonts import TTFont  # noqa: E402
from reportlab.platypus import Image, KeepTogether, PageBreak, Paragraph, SimpleDocTemplate, Table, TableStyle  # noqa: E402

HERE = Path(__file__).resolve().parent
FIG = HERE / "figures"
FIG.mkdir(exist_ok=True)
PDF = sys.argv[1] if len(sys.argv) > 1 else str(HERE / "XAMS_event_structure_proposal.pdf")

plt.rcParams.update({"font.family": "sans-serif", "font.sans-serif": ["Arial", "DejaVu Sans"], "font.size": 8, "savefig.dpi": 300})
NAVY_, BLUE, ORANGE, GREEN_, GREY_, RED = "#1F3A5F", "#4C78A8", "#F28E2B", "#59A14F", "#BAB0AC", "#C0392B"
BLOCK = {"event": "#9AA8B8", "S1 main": "#4C78A8", "S1 alt": "#9ECAE9", "S2 main": "#F28E2B", "S2 alt": "#FFBF79",
         "positions": "#59A14F", "corrected": "#B07AA1"}


def save(fig, name):
    fig.savefig(FIG / f"{name}.png", dpi=300); fig.savefig(FIG / f"{name}.pdf"); plt.close(fig)
    return FIG / f"{name}.png"


def fig_now_vs_new():
    """Left: one wide row per event (event_info, 104 columns) in blocks. Right: the four proposed tables."""
    fig = plt.figure(figsize=(7.4, 3.2))
    ax = fig.add_axes([0, 0, 1, 1]); ax.set_xlim(0, 100); ax.set_ylim(0, 42); ax.axis("off")
    ax.text(1, 40.2, "now: event_info, one row per event, 104 columns", fontsize=8.5, weight="bold", color=NAVY_)
    blocks = [("event", 7), ("S1 main", 17), ("S1 alt", 19), ("S2 main", 19), ("S2 alt", 21), ("positions", 11), ("corrected", 12)]
    x0, w_tot = 1, 42
    for row in range(5):
        x = x0
        for name, n in blocks:
            w = w_tot * n / 106
            ax.add_patch(Rectangle((x, 33 - row * 2.6), w, 2.2, fc=BLOCK[name], ec="white", lw=0.4))
            x += w
    x = x0
    for name, n in blocks:
        w = w_tot * n / 106
        ax.text(x + w / 2, 36.0, f"{name}\n{n}", ha="center", va="bottom", fontsize=5.6)
        x += w
    ax.text(x0, 19.6, "• the same peak quantities 4× (main / alt S1, main / alt S2)\n"
                      "• room for 2 S1s and 2 S2s: a 3rd S2 is lost\n"
                      "• event_info is a stored merge of event_basics,\n   positions and corrected areas: stored twice\n"
                      "• small peaks and the tail after the event: only in\n   peak_basics, without a link to the event",
            fontsize=6.4, va="top", linespacing=1.5)
    ax.text(47, 40.2, "proposed: four linked tables", fontsize=8.5, weight="bold", color=GREEN_)
    def tbl(x, y, w, h, title, lines, fc, ec):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.2,rounding_size=0.8", fc=fc, ec=ec, lw=0.8))
        ax.text(x + w / 2, y + h - 1.6, title, ha="center", fontsize=7.6, weight="bold", color=ec)
        ax.text(x + w / 2, y + h - 3.0, lines, ha="center", va="top", fontsize=5.7, linespacing=1.35)
    tbl(47, 24, 24, 13.5, "events", "one row per event\nn_s1, n_s2, n_sites\ncs1, cs2_top / _bottom (sum), energy\nquality, dt to prev / next event",
        "#E8EEF5", NAVY_)
    tbl(74, 24, 24.5, 13.5, "sites", "one row per S2 above the\nsite threshold (with main S1)\ndrift_time, x, y, z, r\ncs2_top, cs2_bottom, cs2\nrank (by S2 area)",
        "#FDEBD8", "#C76B12")
    tbl(47, 5, 24, 15.5, "event_peaks", "one row per peak inside\nthe event window, no limit\ntype, rank, dt_s1, area,\nshape, channels; x, y (S2)\nsmall peaks included",
        "#EAF4E6", GREEN_)
    tbl(74, 5, 24.5, 15.5, "peak_context", "one row per peak in the run\n(inside and between events)\ndt and area of the last\nS2 > 1 kPE and > 10 kPE,\nthe event it belongs to",
        "#F1ECF6", "#7D4E9E")
    for (a, b) in (((71.2, 31), (73.8, 31)), ((59, 23.8), (59, 20.9)), ((86, 23.8), (86, 20.9))):
        ax.add_patch(FancyArrowPatch(a, b, arrowstyle="<|-|>", mutation_scale=7, lw=0.8, color=NAVY_))
    ax.text(85.5, 1.2, "linked by event_number / peak_index", ha="center", fontsize=5.8, style="italic", color=NAVY_)
    return save(fig, "now_vs_proposed")


def fig_example_event():
    """One S1, two S2 sites, small peaks inside the event and a tail after the event window."""
    import numpy as np
    fig, ax = plt.subplots(figsize=(7.4, 2.2))
    t = np.linspace(0, 140, 4000)
    def pulse(t0, a, w):
        return a * np.exp(-0.5 * ((t - t0) / w) ** 2)
    rng = np.random.default_rng(3)
    small_in = [26, 29, 33, 38, 41, 47, 52]
    small_out = [78, 86, 95, 104, 118, 131]
    y = pulse(4, 1.0, 0.15) + pulse(22, 2.2, 0.9) + pulse(35, 1.3, 0.9)
    for t0 in small_in + small_out:
        y += pulse(t0, 0.18 + 0.08 * rng.random(), 0.35)
    ax.plot(t, y, color="k", lw=0.7)
    ax.axvspan(0, 62, color="#E8EEF5", zorder=0); ax.text(31, 2.85, "event window", ha="center", fontsize=6.6, color=NAVY_)
    ax.text(100, 2.85, "after the event (until the next one)", ha="center", fontsize=6.6, color="#7D4E9E")
    for x0, lab, c in ((4, "S1", BLUE), (22, "S2 site 1\n(rank 1)", ORANGE), (35, "S2 site 2\n(rank 2)", "#C76B12")):
        ax.annotate(lab, (x0, 0.05), xytext=(x0, 2.45 if "site 1" in lab else 1.75), ha="center", fontsize=6.2, color=c,
                    arrowprops=dict(arrowstyle="-", color=c, lw=0.6))
    ax.annotate("small peaks: event_peaks", (47, 0.25), xytext=(38, 1.25), fontsize=6.2, color=GREEN_,
                arrowprops=dict(arrowstyle="-", color=GREEN_, lw=0.6))
    ax.annotate("tail: peak_context\n(dt since the parent S2, its area)", (95, 0.25), xytext=(97, 1.25), fontsize=6.2,
                color="#7D4E9E", arrowprops=dict(arrowstyle="-", color="#7D4E9E", lw=0.6))
    ax.set(xlabel="time [µs]", ylabel="PE / sample", ylim=(0, 3.1), xlim=(0, 140)); ax.set_yticks([])
    fig.tight_layout(pad=0.4)
    return save(fig, "example_event")


FD = "/System/Library/Fonts/Supplemental/"
pdfmetrics.registerFont(TTFont("Arial", FD + "Arial.ttf")); pdfmetrics.registerFont(TTFont("Arial-Bold", FD + "Arial Bold.ttf"))
pdfmetrics.registerFont(TTFont("Arial-Italic", FD + "Arial Italic.ttf"))
pdfmetrics.registerFont(TTFont("Courier-Mono", FD + "Courier New.ttf"))
registerFontFamily("Arial", normal="Arial", bold="Arial-Bold", italic="Arial-Italic", boldItalic="Arial-Bold")
NAVY, LIGHT, GREEN, AMBER, GRID, ROSE = (colors.HexColor(c) for c in ("#1F3A5F", "#E8EEF5", "#E3F2E1", "#FFF4D6", "#9AA8B8", "#FBE3E0"))
ss = getSampleStyleSheet()
base = ParagraphStyle("b", parent=ss["Normal"], fontName="Arial", fontSize=8.6, leading=11.2)
small = ParagraphStyle("s", parent=base, fontSize=7.4, leading=9.2)
code = ParagraphStyle("c", parent=small, fontName="Courier-Mono", fontSize=6.9, leading=8.4)
cap = ParagraphStyle("cap", parent=small, textColor=colors.HexColor("#4A5A6A"), spaceAfter=4)
hdr = ParagraphStyle("hdr", parent=small, fontName="Arial-Bold", textColor=colors.white)
h1 = ParagraphStyle("h1", parent=base, fontName="Arial-Bold", fontSize=15, leading=18, textColor=NAVY)
sub = ParagraphStyle("sub", parent=base, fontSize=8.4, textColor=colors.HexColor("#4A5A6A"), spaceAfter=4)
h2 = ParagraphStyle("h2", parent=base, fontName="Arial-Bold", fontSize=10.5, leading=13, textColor=NAVY, spaceBefore=6, spaceAfter=3)
WW = A4[0] - 24 * mm


def P(t, st=base):
    return Paragraph(t, st)


def box(flow, bg, w=WW):
    t = Table([[flow]], colWidths=[w])
    t.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), bg), ("BOX", (0, 0), (-1, -1), 0.6, GRID), ("LEFTPADDING", (0, 0), (-1, -1), 6),
                           ("RIGHTPADDING", (0, 0), (-1, -1), 6), ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3)]))
    return t


def table(rows, widths, st=small, shade=None):
    data = [[c if isinstance(c, Paragraph) else Paragraph(str(c), hdr if i == 0 else st) for c in r] for i, r in enumerate(rows)]
    t = Table(data, colWidths=widths, repeatRows=1)
    style = [("GRID", (0, 0), (-1, -1), 0.35, GRID), ("BACKGROUND", (0, 0), (-1, 0), NAVY), ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
             ("TOPPADDING", (0, 0), (-1, -1), 1.5), ("BOTTOMPADDING", (0, 0), (-1, -1), 1.5)]
    style += [("BACKGROUND", (0, i), (-1, i), LIGHT) for i in range(2, len(rows), 2)]
    style += shade or []
    t.setStyle(TableStyle(style))
    return t


def img(p, w):
    iw, ih = ImageReader(str(p)).getSize()
    return Image(str(p), width=w, height=w * ih / iw)


EVENT_COLS = [
    ("identity", "time, endtime, event_number"),
    ("content", "n_peaks, n_s1, n_s2, n_sites"),
    ("energy", "cs1 (S1 corrected with the S2-weighted position of the sites), cs2_top, cs2_bottom, cs2 (sums over sites), energy (from cs2_top)"),
    ("quality", "area_before_main_s2, large_s2_before_main_s2"),
    ("neighbours", "dt_prev_event, dt_next_event (exposure for delayed signals)"),
]
SITE_COLS = [
    ("link", "event_number, peak_index, rank (by S2 area; 1 = main)"),
    ("position", "drift_time (from the main S1), x, y, z, r"),
    ("corrected", "cs2_top, cs2_bottom, cs2 (own position and depth; bottom can saturate for large S2s)"),
    ("flags", "dt to the previous S2, width vs depth (split S2, photoionisation at t_max)"),
]
PEAK_COLS = [
    ("link", "event_number, peak_index (row in peaks), type (1 S1, 2 S2, 0 other), rank (by area within type)"),
    ("time", "time, endtime, center_time, dt_s1 (time after the main S1)"),
    ("size / shape", "area, area_fraction_top, range_50p_area, range_90p_area, rise_time"),
    ("channels", "n_channels, n_hits, max_pmt, max_pmt_area, tight_coincidence, n_saturated_channels"),
    ("position (S2)", "x, y"),
]
CONTEXT_COLS = [
    ("link", "peak_index; event_number (−1 between events)"),
    ("parent S2", "dt and area of the last S2 &gt; 1 kPE, and of the last S2 &gt; 10 kPE; prev_event (event of that S2)"),
]
HOWTO = [
    ("main interaction (flat)", "e = event_info", "amstrax.main_interaction(st, run)  (join, not stored)"),
    ("2nd largest S2", "e['alt_s2_area']", "s[s['rank'] == 2]   (sites)  or  p[(p.type == 2) &amp; (p.rank == 2)]"),
    ("3rd largest S2", "not available", "s[s['rank'] == 3]   (sites)  or  p[(p.type == 2) &amp; (p.rank == 3)]"),
    ("all S2s &gt; 500 PE", "not available", "p[(p.type == 2) &amp; (p.area &gt; 500)]"),
    ("single scatters", "approximate (alt_s2 &lt; cut)", "ev[ev.n_sites == 1]"),
    ("energy of a double scatter", "main S2 only", "ev.energy  (S1 + all sites)"),
    ("small peaks in the event", "peak_basics + own matching", "p[p.event_number == 17]"),
    ("delayed signals after S2s", "own script (parents, exposure)", "pc[(pc.dt_prev_s2_10k &gt; 0) &amp; (pc.dt_prev_s2_10k &lt; 2000)]"),
]


def main():
    p1, p2 = fig_now_vs_new(), fig_example_event()
    s = [P("Proposal: a structured event data model in amstrax", h1),
         P("For discussion with the XAMS group. Version 2, 01-11-2026 (v1: 31-10-2026). No code yet. Sizes measured on PROD3 run "
           "7790 (Cs, 97 V/cm; 173 651 events, 779 905 peaks).", sub),
         box(P("<b>In short.</b> Replace the flat main / alt columns by <b>four linked tables</b>: <b>events</b> (the event as a "
               "whole: counts, total energy, neighbours), <b>sites</b> (one row per S2 interaction site: position and corrected S2), "
               "<b>event_peaks</b> (every peak inside the event, small ones included) and <b>peak_context</b> (every peak of the run, "
               "linked to the last large S2 before it). Each quantity is stored once; no main / alt names. The event level gets "
               "~30–40 % smaller. Introduced as optional data types first; the switch is a separate decision."), GREEN),
         P("1  Now and proposed", h2),
         img(p1, WW),
         P("Figure 1. Left: the 104 columns of event_info in blocks. Right: the proposed tables.", cap),
         P("2  Why sites, and where the energy lives", h2),
         P("A gamma that scatters three times gives one S1 (the scatters are ns apart) and three S2s (different depths). Each S2 is "
           "one <b>site</b> with its own drift time, x, y, z and corrected S2. The S1 cannot be split over the sites, so cS1 and the "
           "energy belong to the <b>event</b>: E = W (cS1 / g1 + Σ cS2_top,i / g2), with cS1 corrected for the S1 light collection "
           "averaged over the sites (S2-weighted). For a single scatter this is the familiar energy. Small S2-like peaks (delayed "
           "electrons, photoionisation) are not sites and are not in the energy: their charge is already in the large S2s."),
         img(p2, WW),
         P("Figure 2. One S1, two sites, small peaks inside the event window (event_peaks) and the tail after it (peak_context).", cap),
         P("3  The tables", h2),
         table([["events (one row per event)", "columns"]] + [[a, b] for a, b in EVENT_COLS], [34 * mm, WW - 34 * mm]),
         P("No main-S1 / main-S2 columns: those are the rank-1 rows of event_peaks and sites.", cap),
         table([["sites (one row per S2 above the site threshold)", "columns"]] + [[a, b] for a, b in SITE_COLS], [34 * mm, WW - 34 * mm]),
         table([["event_peaks (one row per peak in the event)", "columns"]] + [[a, b] for a, b in PEAK_COLS], [34 * mm, WW - 34 * mm]),
         table([["peak_context (one row per peak in the run)", "columns"]] + [[a, b] for a, b in CONTEXT_COLS], [34 * mm, WW - 34 * mm]),
         P("peak_context links to the last <i>large S2</i>, not to the previous event: delayed electrons of a 50 kPE S2 last ms, "
           "and a small event in between is not their source. Two thresholds show whether a smaller S2 came in between.", cap),
         P("4  How do I …", h2),
         table([["question", "now (event_info)", "proposed (ev = events, s = sites, p = event_peaks, pc = peak_context)"]]
               + [[a, P(b, code), P(c, code)] for a, b, c in HOWTO],
               [38 * mm, 42 * mm, WW - 80 * mm],
               shade=[("BACKGROUND", (1, 3), (1, 7), ROSE)]),
         P("The flat main-interaction table that most plots use (s1_area, s2_area, x, y, z, cs1, cs2_top, energy) comes from one "
           "amstrax function: a join over the tables, not stored.", cap),
         P("5  Size (PROD3 run 7790, blosc as strax stores it)", h2),
         table([["data", "rows", "MB", "note"],
                ["raw_records", "", "4 970", "90 % of the run; unchanged"],
                ["peaks / peak_basics / peak_positions", "780 k peaks", "395 / 39 / 29", "unchanged"],
                [P("<b>now, event level</b>"), "174 k events", P("<b>≈ 88</b>"), "event_basics 34 + event_positions 8.7 + corrected 4.0 + event_info 41 (stored merge)"],
                ["events (slim)", "174 k", "7.5", "14 columns, 68 B/row"],
                ["sites (S2 &gt; 300 PE)", "285 k (1.64 / event)", "5.6", "many 2nd S2s are trains / split S2s: threshold matters"],
                ["event_peaks", "576 k (3.32 / event)", "38.2", "23 columns, 96 B/row"],
                [P("<b>proposed, event level</b>"), "", P("<b>≈ 51</b>"), "−40 %"],
                ["peak_context", "780 k (all peaks)", "11.5", "204 k of them between events; with it ≈ 63 MB (−30 %)"]],
               [46 * mm, 30 * mm, 22 * mm, WW - 98 * mm],
               shade=[("BACKGROUND", (0, 3), (-1, 3), ROSE), ("BACKGROUND", (0, 7), (-1, 7), GREEN)]),
         P("The total per run (≈ 5.5 GB) hardly changes: the event level is ~1.5 % of it. During the optional phase (both formats) "
           "the extra is ~50–60 MB per run (~1 %). Estimate built in memory from PROD3 data; x / y / z were filled with random numbers "
           "(compress worse than real values, so slightly pessimistic). Runs with lower thresholds or larger S2s have more peaks.", cap),
         P("6  Introduction: optional first", h2),
         table([["step", "what"],
                ["1", "Agree on the tables, columns and the open choices (this note)."],
                ["2", "amstrax: new plugins events_v2 / sites / event_peaks / peak_context and the loader main_interaction(). They only "
                      "<i>read</i> existing data types (peak_basics, peak_positions, event_basics), so the hashes of existing data do not "
                      "change. Not in the auto-processing targets: made only on request."],
                ["3", "Try: compute them privately from the stored PROD3 peaks (no raw data, no central storage) and rewrite one or two "
                      "analyses with them."],
                ["4", "Decision (user). Then: add them to the targets of a new version (PROD4), dashboard event view and handbook, "
                      "notebooks move with PROD4."],
                ["5", "A later version drops event_info and the alt_* columns from the targets. PROD0–3 keep their stored event_info."]],
               [12 * mm, WW - 12 * mm]),
         P("Breaks after the switch: code that uses alt_* names, the 3 copies of x / y or event_info directly (≈ 150 files in the SR "
           "analyses, the dashboard dataview, the event displays). Most notebooks change only their load line to main_interaction().", cap),
         P("7  Open choices", h2),
         table([["question", "options (first proposal in bold)"],
                ["site threshold", P("<b>minimum S2 area (a few 100 PE, scales with anode voltage)</b>, from the 2nd-largest-S2 "
                                     "spectrum; optionally a width-vs-depth check", small)],
                ["fake 2nd sites (split S2, photoionisation at t_max, trains)", P("<b>keep as sites, with flags</b>, or leave out of sites", small)],
                ["which S2s pair with which S1", P("<b>all sites with the main S1; flag n_s1 &gt; 1 (pile-up)</b>, or every S1-S2 pair", small)],
                ["rank", P("<b>by S2 area</b> (independent of corrections), or by energy", small)],
                ["small peaks in event_peaks", P("<b>all</b> (event window bounds the number), or a minimum area", small)],
                ["peak_context thresholds", P("<b>1 kPE and 10 kPE</b>, or set per analysis", small)],
                ["single electrons", "below peak_min_area (10 PE) and the 15 ADC hit threshold: not in any table. Lower them centrally "
                                     "(more peaks), or keep photon-level reprocessing for dedicated studies"]],
               [55 * mm, WW - 55 * mm])]
    SimpleDocTemplate(PDF, pagesize=A4, leftMargin=12 * mm, rightMargin=12 * mm, topMargin=11 * mm, bottomMargin=10 * mm,
                      title="XAMS event data structure proposal", author="XAMS").build(s)
    print("written", PDF)


if __name__ == "__main__":
    main()
