"""Design note (for discussion): dual-range TPC readout in amstrax for SR-2026.4 — every TPC sensor read in two digitizer
channels, one at 0.5 Vpp and one at 2 Vpp (V1725 register 0x1n28), 10 TPC channels in all. Version 1 (25-11-2026).

No code yet. Facts on the current code from amstrax master dd0cf01.
Usage: python make_dual_range_design.py [XAMS_dual_range_design.pdf]
"""
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch  # noqa: E402

from reportlab.lib import colors  # noqa: E402
from reportlab.lib.pagesizes import A4  # noqa: E402
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet  # noqa: E402
from reportlab.lib.units import mm  # noqa: E402
from reportlab.lib.utils import ImageReader  # noqa: E402
from reportlab.pdfbase import pdfmetrics  # noqa: E402
from reportlab.pdfbase.pdfmetrics import registerFontFamily  # noqa: E402
from reportlab.pdfbase.ttfonts import TTFont  # noqa: E402
from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Table, TableStyle  # noqa: E402

HERE = Path(__file__).resolve().parent
FIG = HERE / "figures"
FIG.mkdir(exist_ok=True)
PDF = sys.argv[1] if len(sys.argv) > 1 else str(HERE / "XAMS_dual_range_design.pdf")

plt.rcParams.update({"font.family": "sans-serif", "font.sans-serif": ["Arial", "DejaVu Sans"], "font.size": 8, "savefig.dpi": 300})
NAVY_, BLUE_, ORANGE_, GREEN_, GREY_ = "#1F3A5F", "#4C78A8", "#C76B12", "#3B7D34", "#7F7F7F"


def save(fig, name):
    fig.savefig(FIG / f"{name}.png", dpi=300)
    fig.savefig(FIG / f"{name}.pdf")
    plt.close(fig)
    return FIG / f"{name}.png"


def fig_chain():
    """The TPC chain now (unchanged for every run) and the parallel 2 Vpp chain with the merge at peak level."""
    fig = plt.figure(figsize=(7.4, 2.9))
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 40)
    ax.axis("off")

    def node(x, y, w, txt, fc, ec, bold=False):
        ax.add_patch(FancyBboxPatch((x, y), w, 5.2, boxstyle="round,pad=0.25,rounding_size=0.8", fc=fc, ec=ec, lw=0.8))
        ax.text(x + w / 2, y + 2.6, txt, ha="center", va="center", fontsize=6.6, weight="bold" if bold else "normal", color=ec)

    def arrow(a, b, c=NAVY_, ls="-"):
        ax.add_patch(FancyArrowPatch(a, b, arrowstyle="-|>", mutation_scale=7, lw=0.8, color=c, linestyle=ls))

    ax.text(1, 37.5, "TPC, 0.5 Vpp channels 0-4 (bottom 0, top 1-4): the chain of today, n_tpc_pmts = 5, data types unchanged",
            fontsize=7.2, weight="bold", color=NAVY_)
    xs = [1, 16, 31, 46, 61, 77]
    names = ["raw_records", "records", "peaks", "peak_basics", "peak_positions", "events, sites, ..."]
    for x, n in zip(xs, names):
        node(x, 28, 11.5 if n != "events, sites, ..." else 15, n, "#E8EEF5", NAVY_)
    for a, b in zip(xs[:-1], xs[1:]):
        arrow((a + 11.8, 30.6), (b - 0.3, 30.6))
    ax.text(1, 21.5, "new: 2 Vpp channels 5-9,\nparallel chain", fontsize=7.2, weight="bold", color=ORANGE_, va="center")
    node(1, 11, 11.5, "raw_records_he", "#FDEBD8", ORANGE_)
    node(16, 11, 11.5, "records_he", "#FDEBD8", ORANGE_)
    node(31, 11, 22, "peak_areas_he  (area per channel\nof records_he in each peak window)", "#FDEBD8", ORANGE_)
    arrow((12.8, 13.6), (15.7, 13.6), ORANGE_)
    arrow((27.8, 13.6), (30.7, 13.6), ORANGE_)
    arrow((37, 27.7), (37, 16.7), ORANGE_, "--")
    node(57, 11, 24, "peak_basics_dr  (saturated channels\nreplaced: area_he x range ratio)", "#E3F2E1", GREEN_, bold=True)
    arrow((53.3, 13.6), (56.7, 13.6), GREEN_)
    arrow((51.7, 27.7), (63, 16.7), GREEN_, "--")
    arrow((81.3, 13.6), (84, 27.7), GREEN_)
    ax.text(86, 21, "SR-2026.4 context:\nevents read\npeak_basics_dr", fontsize=6.2, color=GREEN_, va="center")
    ax.text(1, 4.5, "daqreader: the group 'tpc_he' of the run's channel map -> raw_records_he (today every channel outside "
                    "tpc / external / sipm is dropped).\nOld runs: no 'tpc_he' in their channel map -> the new types are empty; "
                    "the SR-2026.3 context does not register peak_basics_dr.", fontsize=6.4, va="top", color=GREY_, linespacing=1.4)
    return save(fig, "dual_range_chain")


FD = "/System/Library/Fonts/Supplemental/"
pdfmetrics.registerFont(TTFont("Arial", FD + "Arial.ttf"))
pdfmetrics.registerFont(TTFont("Arial-Bold", FD + "Arial Bold.ttf"))
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


NOW = [
    ("channel groups", "daqreader.py knows three groups: tpc (bottom + top), external (NaI) and sipm. Records of any other "
                       "channel are dropped (aqmon, channel 40, already today).",
     "a 2 Vpp group would be lost silently"),
    ("number of TPC channels", "n_tpc_pmts = 5 for the whole context (contexts.py); it sets the dtype of peaks "
                               "(area_per_channel, saturated_channel), the gains array, hit thresholds and pulse counts "
                               "(8 files, 25 places).", "10 TPC channels would count every sensor twice and change every "
                                                       "data type of every run"),
    ("top / bottom", "peak_basics sums area_per_channel over the channel_map ranges top and bottom of the run.",
     "fine if the main range keeps bottom 0, top 1-4"),
    ("channel map", "per run from the run doc (xams_bookkeeping.channel_map, else daq_config.channel_map), "
                    "default bottom (0,0), top (1,4), external (5,5), sipm (6,6), aqmon (40,40).",
     "SR-2026.4 must move external / sipm off channels 5-9"),
    ("saturation", "peaks carry saturated_channel per channel and n_saturated_channels; nothing is done with them.",
     "the flag to use for the merge exists"),
]

STEPS = [
    ("1", "DAQ", "Split every TPC sensor into two channels: 0-4 at 0.5 Vpp, 5-9 at 2 Vpp (register 0x1n28 per channel, "
                 "V1725 manual UM2792 §10.1; DC offset ±0.25 V at 0.5 Vpp). Channel map with a group tpc_he = (5, 9), "
                 "external / sipm elsewhere. DAQConfigure.py: 10 channels, 0x1n28, and polarity set per channel after "
                 "0x8080 (the known pitfall). Self-trigger of the 2 Vpp channels high (only pulses near the 0.5 Vpp "
                 "saturation), to limit the data volume."),
    ("2", "reader", "daqreader: any group in the channel map besides tpc / external / sipm becomes raw_records_&lt;group&gt; "
                    "(here raw_records_he), instead of being dropped. No change for runs without the group."),
    ("3", "records_he", "Copy of PulseProcessing for the group (baseline, hit finding, its own thresholds), as "
                        "records_ext and records_sipm already are."),
    ("4", "peak_areas_he", "For every main peak: the area per 2 Vpp channel of records_he inside [time, endtime] and a "
                           "saturation flag. Depends on peaks + records_he; one row per peak."),
    ("5", "peak_basics_dr", "peak_basics with, for every saturated main channel, area_per_channel replaced by area_he x "
                            "r_i (range ratio of channel i), then area, area_fraction_top and the widths' area recomputed. "
                            "Registered (as peak_basics) only in the SR-2026.4 context: the SR-2026.3 context and its "
                            "data types do not change."),
    ("6", "calibration", "Range ratio r_i per channel (≈ 4 nominal, measured): from pulses below the 0.5 Vpp saturation "
                         "seen in both ranges (LED and S2), per gain period, in amstrax_files like the gains. Timing "
                         "offset between the two channels of a sensor (cables)."),
    ("7", "tests", "(a) SR-2026.3 runs (Cs, field scan, zero field) reprocessed with the new code: identical lineage and "
                   "identical numbers. (b) Synthetic records with clipped pulses: the merge restores the area. (c) A short "
                   "test run with the SR-2026.4 DAQ config, processed privately on STBC (announced), before data taking."),
]

OPEN = [
    ("which range is the main chain", P("<b>0.5 Vpp</b> (small S1, single electrons: 4× finer ADC steps, 31 µV per LSB vs 122) "
                                        "with 2 Vpp only where 0.5 Vpp saturates; or 2 Vpp main (large S2 linear) with 0.5 Vpp for "
                                        "small peaks", small)),
    ("where to merge", P("<b>peak level</b> (this note: per-channel areas, simple, no waveform work); or record level "
                         "(replace clipped samples by the scaled 2 Vpp waveform: waveform-level quantities such as widths also "
                         "corrected, more work, record-level lineage changes)", small)),
    ("splitting the signal", P("passive split (each range sees half the amplitude) or the dual-range amplifier of the SiPM "
                               "tile readout (separate unit, designed later)", small)),
    ("2 Vpp threshold", P("<b>high</b> (only near saturation: small extra volume, ratio calibrated with large pulses) or low "
                          "(every pulse twice: ratio from everything, data volume ×2 on STBC and dCache, copy to dCache at "
                          "~2 MB/s)", small)),
    ("one context for SR-2026.3 and SR-2026.4", P("<b>no: one context per science run</b> (the global PRODn config names the "
                                                  "science run); or a per-run n_tpc_pmts / plugin choice inside one "
                                                  "context (more invasive)", small)),
]


def main():
    p1 = fig_chain()
    s = [P("Design note: dual-range TPC readout in amstrax (SR-2026.4)", h1),
         P("For discussion with the XAMS group. Version 1, 25-11-2026. No code yet; facts on the current code from amstrax master "
           "dd0cf01.", sub),
         box(P("<b>In short.</b> SR-2026.4 reads every TPC sensor twice, at 0.5 Vpp and at 2 Vpp (10 channels). Declaring 10 TPC "
               "channels would count every sensor twice and change every data type, so SR-2026.3 could no longer be processed "
               "with the same code. Instead: the 0.5 Vpp channels stay the TPC chain of today (5 channels, data types unchanged), "
               "the 2 Vpp channels get a <b>parallel chain</b> (raw_records_he → records_he → peak_areas_he, as straxen does for "
               "XENON's high-energy channels), and a <b>peak-level merge</b> replaces the area of every saturated channel by the "
               "2 Vpp area times the measured range ratio. Old runs have no 2 Vpp group: they process exactly as now, which a "
               "regression test proves before anything is merged."), GREEN),
         P("1  What amstrax does today", h2),
         table([["item", "now (master dd0cf01)", "consequence for 10 channels"]] + [list(r) for r in NOW],
               [30 * mm, WW - 30 * mm - 52 * mm, 52 * mm], shade=[("BACKGROUND", (2, 1), (2, 2), ROSE)]),
         P("2  Proposed chain", h2),
         img(p1, WW),
         P("Figure 1. Top: the TPC chain of today, kept for the 0.5 Vpp channels. Bottom: the parallel chain for the 2 Vpp "
           "channels and the merge at peak level, used only in the SR-2026.4 context.", cap),
         P("3  Steps", h2),
         table([["#", "part", "what"]] + [list(r) for r in STEPS], [7 * mm, 24 * mm, WW - 31 * mm]),
         P("Effort: steps 2-4 are mostly copy-and-adapt of the existing chains (days); step 5 (saturation criterion, the merge, "
           "recomputing the area-derived quantities) and step 6 (calibrating the ratio) are the real development; step 7c needs "
           "the hardware.", cap),
         P("4  Backward compatibility (the requirement)", h2),
         P("SR-2026.3 must stay reproducible. (1) Everything per run comes from the run doc (channel map, DAQ config): the "
           "SR-2026.3 run docs do not change. (2) No change to the data types of the existing chain: the new plugins are new "
           "data types, and the merged peak_basics is registered only in the SR-2026.4 context. (3) Regression test on SR-2026.3 "
           "runs in the amstrax tests (identical lineage hashes and numbers) before every merge that touches the reader or the "
           "TPC chain. (4) As a fallback, PROD6 is frozen like PROD4 (tag, conda env, setup script)."),
         P("5  Open choices", h2),
         table([["question", "options (first proposal in bold)"]] + [[a, b] for a, b in OPEN], [48 * mm, WW - 48 * mm])]
    SimpleDocTemplate(PDF, pagesize=A4, leftMargin=12 * mm, rightMargin=12 * mm, topMargin=11 * mm, bottomMargin=10 * mm,
                      title="XAMS dual-range readout design", author="XAMS").build(s)
    print("written", PDF)


if __name__ == "__main__":
    main()
