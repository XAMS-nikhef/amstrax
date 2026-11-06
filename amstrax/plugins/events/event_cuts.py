"""event_cuts: one boolean per cut per event (data kind events), from events, sites and event_peaks.

The cuts of the SR-2026.3 analysis (measurements/SR2026.3: lib/selection.py, lib/oddcut.py, corrections v10), with the
references frozen per run set in ``event_cut_references`` (amstrax_files, by run range):

  cut_basic      main S1 and main S2 present, main S2 > s2_min_PE (the site threshold, 300 PE)
  cut_iso_s1     alt S1 < max(10 % of the main S1, 30 PE) or none (alt S1: largest other S1 before the main S2)
  cut_iso_s2     alt S2 < 5 % of the main S2 or none (alt S2: largest other S2 after the main S1; any other without S1)
  cut_s1_aft     |S1 AFT - med(t)| < 3 sig(t)            (bands of the set, t = drift time clipped to [0, cathode])
  cut_s2_width   |log10 S2 w50[ns] - med(t)| < 3 sig(t)
  cut_c          odd-event cut C: S2 AFT - AFT_ref(r) > -0.06 and 0.6 < w50 / w50_ref(t) < 2.6
  cut_fiducial   r < r_max and z_min < z < z_max of the main site
  cut_all        all of the above
Main S1 = event_peaks type 1 with dt_s1 = 0; main S2 = the site of rank 1 and its peak. Without references for the run
the reference-dependent cuts (bands, cut C, fiducial) are False and has_references is False.
"""
import numpy as np
import strax

import amstrax

export, __all__ = strax.exporter()

CUTS = ("cut_basic", "cut_iso_s1", "cut_iso_s2", "cut_s1_aft", "cut_s2_width", "cut_c", "cut_fiducial", "cut_all")
DELTA_AFT, W_MIN, W_MAX = 0.06, 0.6, 2.6


def _lookup(keys, sorted_keys, order):
    """Row (in the original order) of every key in sorted_keys (sorted via order), and whether it was found."""
    if not len(sorted_keys):
        return np.zeros(len(keys), np.int64), np.zeros(len(keys), bool)
    i = np.clip(np.searchsorted(sorted_keys, keys), 0, len(sorted_keys) - 1)
    return order[i], sorted_keys[i] == keys


def main_interaction_arrays(events, sites, peaks):
    """Per event (rows of events): main S1 area / AFT, main S2 area / AFT / w50, drift time, x, y, z, alt S1, alt S2 (NaN
    where absent), with the event_basics definitions (validated in SR2026.3 corrections step 02)."""
    n = len(events)
    out = {k: np.full(n, np.nan) for k in ("s1_area", "s1_aft", "s2_area", "s2_aft", "s2_w50", "drift_time", "x", "y", "z",
                                           "alt_s1_area", "alt_s2_area")}
    en = np.asarray(events["event_number"])
    eo = np.argsort(en, kind="stable")
    es = en[eo]

    def rows(keys):
        return _lookup(np.asarray(keys), es, eo)

    if len(peaks):
        s1 = peaks[(peaks["type"] == 1) & (peaks["dt_s1"] == 0)]
        r, ok = rows(s1["event_number"])
        out["s1_area"][r[ok]] = s1["area"][ok]
        out["s1_aft"][r[ok]] = s1["area_fraction_top"][ok]
        # time of the largest S2 (also below the site threshold) per event: the alt S1 must come before it
        m2 = peaks[(peaks["type"] == 2) & (peaks["rank"] == 1)]
        t2 = np.full(n, np.iinfo(np.int64).max, dtype=np.int64)
        r, ok = rows(m2["event_number"])
        t2[r[ok]] = m2["time"][ok]
        o1 = peaks[(peaks["type"] == 1) & (peaks["dt_s1"] != 0)]
        r, ok = rows(o1["event_number"])
        sel = ok & (o1["time"] < t2[r])
        np.fmax.at(out["alt_s1_area"], r[sel], o1["area"][sel].astype(np.float64))
        o2 = peaks[(peaks["type"] == 2) & (peaks["rank"] > 1)]
        after = np.isnan(o2["dt_s1"]) | (o2["dt_s1"] > 0)
        o2 = o2[after]
        r, ok = rows(o2["event_number"])
        np.fmax.at(out["alt_s2_area"], r[ok], o2["area"][ok].astype(np.float64))
    if len(sites):
        main = sites[sites["rank"] == 1]
        r, ok = rows(main["event_number"])
        for k in ("drift_time", "x", "y", "z"):
            out[k][r[ok]] = main[k][ok]
        if len(peaks):
            po = np.argsort(peaks["peak_index"], kind="stable")
            j, found = _lookup(np.asarray(main["peak_index"]), peaks["peak_index"][po], po)
            sel = ok & found
            out["s2_area"][r[sel]] = peaks["area"][j[sel]]
            out["s2_aft"][r[sel]] = peaks["area_fraction_top"][j[sel]]
            out["s2_w50"][r[sel]] = peaks["range_50p_area"][j[sel]]
    return out


def cuts(m, ref):
    """The cut booleans from main_interaction_arrays() and the references of the run (None: reference cuts False)."""
    n = len(m["s1_area"])
    s1, s2, a1, a2 = m["s1_area"], m["s2_area"], m["alt_s1_area"], m["alt_s2_area"]
    with np.errstate(invalid="ignore"):
        s2_min = (ref or {}).get("bands", {}).get("s2_min_PE", 300.0)
        c = {"cut_basic": (s1 > 0) & (s2 > s2_min),
             "cut_iso_s1": (a1 < np.maximum(0.1 * s1, 30)) | ~np.isfinite(a1),
             "cut_iso_s2": (a2 < 0.05 * s2) | ~np.isfinite(a2)}
        if ref:
            b = ref["bands"]
            t = np.clip(m["drift_time"] / 1e3, 0, b["cathode_us"])
            ba, bw = b["s1_aft"], b["s2_log10_w50_ns"]
            c["cut_s1_aft"] = np.abs(m["s1_aft"] - np.interp(t, ba["t_us"], ba["med"])) < 3 * np.interp(t, ba["t_us"], ba["sig"])
            lw = np.log10(np.maximum(m["s2_w50"], 1))
            c["cut_s2_width"] = np.abs(lw - np.interp(t, bw["t_us"], bw["med"])) < 3 * np.interp(t, bw["t_us"], bw["sig"])
            o = ref["oddcut"]
            r = np.hypot(m["x"], m["y"])
            da = m["s2_aft"] - np.interp(r, o["r_mm"], o["aft_ref"])
            wr = (m["s2_w50"] / 1e3) / np.interp(m["drift_time"] / 1e3, o["t_us"], o["w50_ref_us"])
            c["cut_c"] = (da > -DELTA_AFT) & (wr > W_MIN) & (wr < W_MAX)
            f = ref["fiducial"]
            c["cut_fiducial"] = (r < f["r_max_mm"]) & (m["z"] > f["z_mm"][0]) & (m["z"] < f["z_mm"][1])
        else:
            for k in ("cut_s1_aft", "cut_s2_width", "cut_c", "cut_fiducial"):
                c[k] = np.zeros(n, bool)
    c["cut_all"] = np.logical_and.reduce([c[k] for k in CUTS if k != "cut_all"])
    return c


@export
class EventCuts(strax.Plugin):
    """One boolean per cut per event; definitions in the module docstring (amstrax.plugins.events.event_cuts)."""

    __version__ = "1.0.0"
    depends_on = ("events", "sites", "event_peaks")
    provides = "event_cuts"
    data_kind = "events"

    event_cut_references = amstrax.XAMSConfig(
        default=None,
        help="per run (range): {'bands': selection bands, 'oddcut': cut C references, 'fiducial': {'r_max_mm', 'z_mm'}}")

    def infer_dtype(self):
        return strax.time_fields + [(k, np.bool_, d) for k, d in (
            ("cut_basic", "Main S1 and main S2 (> s2_min_PE) present"),
            ("cut_iso_s1", "No large alternative S1"),
            ("cut_iso_s2", "No alternative S2 >= 5 % of the main S2"),
            ("cut_s1_aft", "Main S1 AFT in the band of its drift time"),
            ("cut_s2_width", "Main S2 width in the band of its drift time"),
            ("cut_c", "Odd-event cut C (S2 AFT and width against the set's references)"),
            ("cut_fiducial", "Main site inside the fiducial volume"),
            ("cut_all", "All cuts"),
            ("has_references", "Cut references available for the run"))]

    def compute(self, events, sites, event_peaks):
        r = np.zeros(len(events), self.dtype)
        r["time"], r["endtime"] = events["time"], events["endtime"]
        ref = self.event_cut_references
        c = cuts(main_interaction_arrays(events, sites, event_peaks), ref)
        for k in CUTS:
            r[k] = c[k]
        r["has_references"] = ref is not None
        return r
