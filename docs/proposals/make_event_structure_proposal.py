"""Proposal (for discussion): a structured event data model in amstrax — a slim event table plus a table of all peaks
per event, instead of the flat main / alt columns.

Numbers: amstrax master 761f62f (data_info per data type); peaks per event and record sizes from PROD3 runs 7790 / 7912.
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
    """Left: one wide row per event (event_info, 104 columns) in blocks. Right: event table + peak table."""
    fig = plt.figure(figsize=(7.4, 3.0))
    ax = fig.add_axes([0, 0, 1, 1]); ax.set_xlim(0, 100); ax.set_ylim(0, 40); ax.axis("off")
    ax.text(1, 38.2, "now: event_info, one row per event, 104 columns", fontsize=8.5, weight="bold", color=NAVY_)
    blocks = [("event", 7), ("S1 main", 17), ("S1 alt", 19), ("S2 main", 19), ("S2 alt", 21), ("positions", 11), ("corrected", 12)]
    x0, w_tot = 1, 46
    for row in range(5):
        x = x0
        for name, n in blocks:
            w = w_tot * n / 106
            ax.add_patch(Rectangle((x, 31 - row * 2.6), w, 2.2, fc=BLOCK[name], ec="white", lw=0.4))
            x += w
    x = x0
    for name, n in blocks:
        w = w_tot * n / 106
        ax.text(x + w / 2, 34.0, f"{name}\n{n}", ha="center", va="bottom", fontsize=5.8)
        x += w
    ax.text(x0, 17.6, "• the same 23 peak quantities 4× (main / alt S1, main / alt S2):\n   76 of the 83 event_basics columns\n"
                      "• room for 2 S1s and 2 S2s only: a 3rd S2 is lost\n"
                      "• corrections only for main and alt; names differ per\n   peak (s2_area, alt_s2_area, …)",
            fontsize=6.6, va="top", linespacing=1.5)
    # proposed
    ax.text(53, 38.2, "proposed: two linked tables", fontsize=8.5, weight="bold", color=GREEN_)
    ax.add_patch(FancyBboxPatch((53, 22.5), 18, 13.5, boxstyle="round,pad=0.2,rounding_size=0.8", fc="#E8EEF5", ec=NAVY_, lw=0.8))
    ax.text(62, 34.6, "events", ha="center", fontsize=8, weight="bold", color=NAVY_)
    ax.text(62, 33.3, "one row per event\n~28 columns\nmain S1 / S2, z, cs1, cs2_top,\nenergy, n_s1, n_s2", ha="center", va="top", fontsize=6.0, linespacing=1.35)
    ax.add_patch(FancyBboxPatch((77, 6), 21.5, 30, boxstyle="round,pad=0.2,rounding_size=0.8", fc="#EAF4E6", ec=GREEN_, lw=0.8))
    ax.text(87.75, 34.6, "event_peaks", ha="center", fontsize=8, weight="bold", color=GREEN_)
    ax.text(87.75, 33.3, "one row per peak in an event\n~22 columns, no limit", ha="center", va="top", fontsize=6.0, linespacing=1.35)
    rows = [("S1", 1, BLUE), ("S2", 1, ORANGE), ("S2", 2, "#FFBF79"), ("S2", 3, "#FFDDB0"), ("other", 1, GREY_)]
    for k, (t, rk, c) in enumerate(rows):
        y = 26.5 - k * 3.6
        ax.add_patch(Rectangle((78.5, y), 18.5, 2.8, fc=c, ec="white"))
        ax.text(79.3, y + 1.4, f"event 17 · {t} · rank {rk}", va="center", fontsize=5.9)
    ax.text(87.75, 7.6, "rank 1 = main, rank 2 = 'alt', …", ha="center", fontsize=5.8, style="italic")
    ax.add_patch(FancyArrowPatch((71.2, 29), (78.3, 27.9), arrowstyle="-|>", mutation_scale=8, lw=0.9, color=NAVY_))
    ax.text(74.7, 30.2, "event_number", ha="center", fontsize=5.8, color=NAVY_)
    ax.text(53, 19.5, "• each quantity once, one name\n   for every peak\n• any number of S1s / S2s\n"
                      "• corrections per peak (cs, x, y, z)\n• flat (pandas) or nested\n   (awkward) on load",
            fontsize=6.6, va="top", linespacing=1.5)
    return save(fig, "now_vs_proposed")


def fig_example_event():
    """One S1 and three S2s: what is kept now and in the proposal."""
    fig, axs = plt.subplots(1, 2, figsize=(7.4, 2.05), gridspec_kw={"width_ratios": [1.35, 1]})
    ax = axs[0]
    import numpy as np
    t = np.linspace(0, 60, 1500)
    def pulse(t0, a, w):
        return a * np.exp(-0.5 * ((t - t0) / w) ** 2)
    y = pulse(4, 1.0, 0.15) + pulse(22, 2.2, 0.9) + pulse(31, 1.4, 0.9) + pulse(47, 0.8, 0.9)
    ax.plot(t, y, color="k", lw=0.8)
    for x0, lab, c in ((4, "S1", BLUE), (22, "S2 #1\n(main)", ORANGE), (31, "S2 #2\n(alt)", "#E8A04A"), (47, "S2 #3\nnow lost", RED)):
        ax.annotate(lab, (x0, 0.05), xytext=(x0, 2.55 if "#1" in lab else 1.75), ha="center", fontsize=6.3, color=c,
                    arrowprops=dict(arrowstyle="-", color=c, lw=0.6))
    ax.set(xlabel="time [µs]", ylabel="PE / sample", ylim=(0, 3.1), xlim=(0, 60)); ax.set_yticks([])
    ax.set_title("a triple scatter: one S1, three S2s", fontsize=7.5)
    ax = axs[1]; ax.axis("off")
    rows = [["", "now (event_basics)", "proposed (event_peaks)"],
            ["S1", "s1_area …", "row: S1, rank 1"],
            ["S2 #1", "s2_area …", "row: S2, rank 1"],
            ["S2 #2", "alt_s2_area …", "row: S2, rank 2"],
            ["S2 #3", "—", "row: S2, rank 3"]]
    tb = ax.table(cellText=rows, loc="center", cellLoc="left", colWidths=[0.18, 0.41, 0.41])
    tb.auto_set_font_size(False); tb.set_fontsize(6.6); tb.scale(1, 1.45)
    for (r, c), cell in tb.get_celld().items():
        cell.set_edgecolor("#9AA8B8"); cell.set_linewidth(0.4)
        if r == 0:
            cell.set_facecolor(NAVY_); cell.get_text().set_color("white"); cell.get_text().set_weight("bold")
        elif r == 4 and c == 1:
            cell.set_facecolor("#FBE3E0")
        elif r > 0 and c == 2:
            cell.set_facecolor("#EAF4E6")
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
    ("identity", "time, endtime, event_number", "as now"),
    ("content", "n_peaks, n_s1, n_s2", "n_s1 / n_s2 new"),
    ("main S1", "s1_index, s1_area, s1_area_fraction_top, s1_range_50p_area, s1_n_channels, s1_tight_coincidence", "6 of 17"),
    ("main S2", "s2_index, s2_area, s2_area_fraction_top, s2_range_50p_area, s2_n_channels", "5 of 19"),
    ("interaction", "drift_time, x, y, r, z", "one x / y (was 3 copies)"),
    ("corrected", "cs1, cs2, cs2_top, energy", "corrected_areas 1.0"),
    ("quality", "area_before_main_s2, large_s2_before_main_s2", "as now"),
]
PEAK_COLS = [
    ("link", "event_number, peak_index (row in peaks), type (1 S1, 2 S2, 0 other), rank (by area within type; 1 = main)"),
    ("time", "time, endtime, center_time, dt_s1 (time after the main S1: the drift time of an S2)"),
    ("size / shape", "area, area_fraction_top, range_50p_area, range_90p_area, rise_time"),
    ("channels", "n_channels, n_hits, max_pmt, max_pmt_area, tight_coincidence, n_saturated_channels"),
    ("position (S2)", "x, y, z (z of an S2 from dt_s1)"),
    ("corrected", "cs (S1: z and (x, y) of the main S2; S2: top array, own z and (x, y))"),
]
HOWTO = [
    ("main S2 area", "e['s2_area']", "ev['s2_area']   (unchanged)"),
    ("2nd largest S2", "e['alt_s2_area']", "p[(p.type == 2) &amp; (p.rank == 2)]"),
    ("3rd largest S2", "not available", "p[(p.type == 2) &amp; (p.rank == 3)]"),
    ("all S2s &gt; 500 PE", "not available", "p[(p.type == 2) &amp; (p.area &gt; 500)]"),
    ("events with exactly two S2s", "approximate (alt_s2 &gt; cut)", "ev[ev.n_s2 == 2]"),
    ("nested, per event", "—", "ev = load_events(run, nested=True);  ev.s2.area[:, :3]"),
]


def main():
    p1, p2 = fig_now_vs_new(), fig_example_event()
    s = [P("Proposal: a structured event data model in amstrax", h1),
         P("For discussion with the XAMS group, 31-10-2026. No code yet. Status of amstrax master 761f62f; numbers from "
           "PROD3 runs 7790 (Cs, 97 V/cm) and 7912 (Cs, 50 V/cm).", sub),
         box(P("<b>In short.</b> Replace the flat main / alternative columns by <b>two linked tables</b>: a slim <b>events</b> table "
               "(one row per event, ~28 columns instead of 104) and an <b>event_peaks</b> table (one row per peak inside an event, ~22 "
               "columns, any number of S1s and S2s). A loader gives the familiar flat table or a nested view (event → its S1s, S2s). "
               "Storage stays about the same; multiple scatters become straightforward."), GREEN),
         P("1  Now and proposed", h2),
         img(p1, WW),
         P("Figure 1. Left: the 104 columns of event_info in blocks (proportional widths). Right: the proposed tables, linked by "
           "event_number; within an event the peaks of one type are ranked by area.", cap),
         P("2  The two tables", h2),
         table([["events (one row per event)", "columns", "vs now"]] + [[a, b, c] for a, b, c in EVENT_COLS],
               [24 * mm, WW - 24 * mm - 34 * mm, 34 * mm]),
         P("≈ 28 columns. Kept on purpose: everything a typical plot of the main interaction needs, so most analyses never "
           "touch the peak table.", cap),
         table([["event_peaks (one row per peak in an event)", "columns"]] + [[a, b] for a, b in PEAK_COLS],
               [36 * mm, WW - 36 * mm]),
         P("≈ 22 columns. 'alt' is no longer a separate set of names: the alternative S2 is the S2 with rank 2.", cap),
         PageBreak(),
         P("3  Example: a triple scatter", h2),
         img(p2, WW),
         P("Figure 2. With two S2 slots the third interaction disappears from the event data; in event_peaks it is one more row.", cap),
         P("4  How do I …", h2),
         table([["question", "now (event_info)", "proposed (ev = events, p = event_peaks)"]] + [[a, P(b, code), P(c, code)] for a, b, c in HOWTO],
               [40 * mm, 50 * mm, WW - 90 * mm],
               shade=[("BACKGROUND", (1, 3), (1, 4), ROSE)]),
         P("5  Size and speed", h2),
         table([["", "now", "proposed"],
                ["rows", "1 per event", "events: 1 per event; event_peaks: 3.3–3.5 per event (7790, 7912)"],
                ["bytes per event", "event_basics 360 + positions + corrected ≈ 470", "events ≈ 120 + peaks 3.4 × ≈ 70 ≈ 360"],
                ["loading the main interaction", "one table", "one (slim) table: faster"],
                ["multiple-peak questions", "not possible beyond 2", "one filter / group-by on event_peaks"]],
               [36 * mm, 58 * mm, WW - 94 * mm]),
         P("Both are ordinary strax data types (fixed columns, stored per chunk), so processing, storage and the dashboard machinery "
           "stay as they are; the nested view (awkward-array) is built only in the loader.", cap),
         P("6  What changes for users, and migration", h2),
         table([["step", "what"],
                ["1", "Agree on the column lists (this note) — which columns do the students actually use?"],
                ["2", "amstrax: plugins events (slim) and event_peaks; corrections per peak (same correction files as corrected_areas 1.0); "
                      "loader load_events(run, nested=False / True)."],
                ["3", "Transition: event_info stays for one version next to the new tables (deprecated), so notebooks keep working."],
                ["4", "Regenerate the event level of the versions in use (from stored peaks / event_basics, no raw data): snapshot first, "
                      "then the shared checkout — no gap for users."],
                ["5", "Dashboard (event view, dataview columns), handbook page, example notebook; then retire event_info."]],
               [12 * mm, WW - 12 * mm]),
         P("Breaks: code that uses alt_* names or the 3 copies of x / y (the dashboard dataview, the event displays of the SR "
           "analyses). Does not break: peaks, peak_basics, records and everything below the event level.", cap),
         P("7  Open questions for the discussion", h2),
         table([["question", "options"],
                ["rank by area, or by time?", "area (as main / alt now); time order is the dt_s1 column"],
                ["which peaks belong to an event?", "all peaks inside the event window (as now), or only S1s before / S2s after the main S1"],
                ["S1 correction for several S2s", "with the position of the main S2 (as now), or one cs1 per S1-S2 pair"],
                ["nested view", "awkward-array (extra dependency) or pandas with a MultiIndex (event, peak)"],
                ["keep event_info?", "one version as a transition, or permanently as a convenience merge"]],
               [55 * mm, WW - 55 * mm])]
    SimpleDocTemplate(PDF, pagesize=A4, leftMargin=12 * mm, rightMargin=12 * mm, topMargin=11 * mm, bottomMargin=10 * mm,
                      title="XAMS event data structure proposal", author="XAMS").build(s)
    print("written", PDF)


if __name__ == "__main__":
    main()
