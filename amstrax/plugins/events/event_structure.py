"""Structured event data model (proposal docs/proposals/XAMS_event_structure_proposal.pdf): four linked tables.

  event_peaks   every peak inside an event window (peaks / peak_basics / peak_positions + event link, rank, dt_s1)
  sites         every S2 of an event above ``site_min_s2_area``: drift time, position, corrected S2 (top, bottom, total)
  events_v2     the event as a whole: counts, cS1 (S2-weighted over the sites), sum of cS2 over the sites, energy
  peak_context  every peak of the run, linked to the last S2 > 1 kPE and > 10 kPE before it

Linked by event_number and peak_index (row of the peak in the run, counted from the first peak). The plugins only read
existing data types, so the lineage of the existing ones does not change. Optional: registered only with
``amstrax.contexts.xams(event_structure_v2=True)``. ``main_interaction()`` gives the flat table (one row per event:
main S1, main site and its S2) as a join, not stored.

Corrections are those of corrected_areas (same options, same two modes), so for an event with one site cs1, cs2,
cs2_top and energy equal the event_info values.
"""
import numba
import numpy as np
import strax
import amstrax
from amstrax.plugins.events.corrected_areas import W_KEV, CorrectedAreas

export, __all__ = strax.exporter()
__all__ += ["EVENT_STRUCTURE_V2", "EVENT_STRUCTURE_V2_TYPES", "main_interaction"]

MAX_DRIFT_MARGIN = 1.05     # sites up to 1.05 x drift_time_cathode count as inside the drift region
CONTEXT_S2_AREAS = (1_000, 10_000)   # peak_context: parents are the last S2s above these areas [PE]
THRESHOLDS = np.array(CONTEXT_S2_AREAS, dtype=np.float64)

PEAK_FIELDS = (
    ("center_time", np.int64, "Weighted center time since unix epoch [ns]"),
    ("area", np.float32, "Area, uncorrected [PE]"),
    ("area_fraction_top", np.float32, "Fraction of the area seen by the top array"),
    ("range_50p_area", np.float32, "Width, 50% area [ns]"),
    ("range_90p_area", np.float32, "Width, 90% area [ns]"),
    ("rise_time", np.float32, "Time between 10% and 50% area quantiles [ns]"),
    ("n_channels", np.int16, "Number of contributing PMTs"),
    ("n_hits", np.int32, "Number of hits contributing at least one sample to the peak"),
    ("max_pmt", np.int16, "PMT that contributes the most PE"),
    ("max_pmt_area", np.float32, "Area in the largest-contributing PMT [PE]"),
    ("tight_coincidence", np.int16, "Number of PMTs with hits within tight range of mean"),
    ("n_saturated_channels", np.int16, "Number of saturated channels"),
)


@numba.njit(cache=True)
def _rank_and_main(ev, ptype, area, time):
    """Per peak: rank by area within its type and event (1 = largest). Per peak: row of the main S1 of its event
    (largest S1 before the main S2, the largest S2; -1 if none). ``ev`` (event row per peak) must be grouped."""
    n = len(ev)
    rank = np.zeros(n, np.int16)
    main_s1 = np.full(n, -1, np.int64)
    i = 0
    while i < n:
        j = i
        while j < n and ev[j] == ev[i]:
            j += 1
        s2 = -1
        for k in range(i, j):
            if ptype[k] == 2 and (s2 < 0 or area[k] > area[s2]):
                s2 = k
        t_max = time[s2] if s2 >= 0 else np.iinfo(np.int64).max
        s1 = -1
        for k in range(i, j):
            if ptype[k] == 1 and time[k] < t_max and (s1 < 0 or area[k] > area[s1]):
                s1 = k
            r = 1
            for m in range(i, j):
                if ptype[m] == ptype[k] and (area[m] > area[k] or (area[m] == area[k] and m < k)):
                    r += 1
            rank[k] = r
        main_s1[i:j] = s1
        i = j
    return rank, main_s1


@export
class EventPeaks(strax.Plugin):
    """Every peak fully inside an event window, with the event it belongs to, its rank by area within its type
    (1 = largest) and its time after the main S1 (largest S1 before the main S2, as in event_basics)."""

    __version__ = "0.1.0"
    depends_on = ("events", "peak_basics", "peak_positions")
    provides = "event_peaks"
    data_kind = "event_peaks"
    parallel = False
    rechunk_on_save = False

    def infer_dtype(self):
        return strax.time_fields + [
            ("event_number", np.int64, "Event number in this dataset"),
            ("peak_index", np.int64, "Row of the peak in peaks (counted from the first peak of the run)"),
            ("type", np.int16, "Peak type (1 S1, 2 S2, 0 other)"),
            ("rank", np.int16, "Rank by area within the type in this event (1 = largest)"),
            ("dt_s1", np.float32, "center_time minus that of the main S1 [ns]; NaN without S1"),
        ] + list(PEAK_FIELDS) + [
            ("x", np.float32, "x-position from the pos_rec_map [mm]"),
            ("y", np.float32, "y-position from the pos_rec_map [mm]"),
        ]

    def setup(self):
        self.peaks_seen = 0

    def compute(self, events, peaks):
        index = self.peaks_seen + np.arange(len(peaks))
        self.peaks_seen += len(peaks)
        ev = strax.fully_contained_in(peaks, events)
        sel = np.where(ev >= 0)[0]
        sel = sel[np.argsort(ev[sel], kind="stable")]
        p = peaks[sel]
        rank, main = _rank_and_main(ev[sel], p["type"], p["area"], p["time"])
        r = np.zeros(len(p), self.dtype)
        for name in ("time", "endtime", "type", "x", "y") + tuple(f[0] for f in PEAK_FIELDS):
            r[name] = p[name]
        r["event_number"] = events["event_number"][ev[sel]]
        r["peak_index"] = index[sel]
        r["rank"] = rank
        has_s1 = main >= 0
        r["dt_s1"] = np.where(has_s1, p["center_time"] - p["center_time"][np.where(has_s1, main, 0)], np.nan)
        return r[np.argsort(sel, kind="stable")]


class _Corrections:
    """Options and corrections shared with corrected_areas (same names and defaults, so a global version sets them
    for all plugins)."""

    drift_time_gate = amstrax.XAMSConfig(default=3000, help="Drift time belonging to the gate in ns")
    drift_time_cathode = amstrax.XAMSConfig(default=39500, help="Drift time belonging to the cathode in ns")
    gate_cathode_distance = amstrax.XAMSConfig(default=50.5, help="Distance between gate and cathode in mm")
    elife = amstrax.XAMSConfig(default=30000, help="electron lifetime in [ns] (mode 2)")
    s1_naive_z_correction = amstrax.XAMSConfig(default=[-52, 0, 700, -7.69],
        help="Parameters for the z-dependent S1 correction [zmin, zmax, y0, a] where y0 + a*z is the correction (mode 2)")
    sr_corrections = amstrax.XAMSConfig(default=None, help="per-run constants: lifetime, g1, g2_top, energy_calibrated (mode 1)")
    sr_maps = amstrax.XAMSConfig(default=None, help="S1 / S2_top (x, y) maps and S1(z) of the run range (mode 1)")

    @property
    def sr_mode(self):
        return self.sr_corrections is not None and self.sr_maps is not None

    def z_of(self, drift_time):
        slope = -self.gate_cathode_distance / (self.drift_time_cathode - self.drift_time_gate)
        return slope * (drift_time - self.drift_time_gate)

    def s1_response(self, z, x, y):
        """Relative S1 light collection at (x, y, z): cs1 = S1 / response (as in corrected_areas)."""
        if self.sr_mode:
            s1z = self.sr_maps["s1_z"]
            q = np.asarray(s1z["quadratic_in_drift_us"])
            t_ref = s1z["ref_t_gate_us"] - z / s1z["ref_v_drift_mm_per_us"]
            f1, _ = CorrectedAreas.xy_factors(x, y, self.sr_maps)
            return np.polyval(q, t_ref) / np.polyval(q, s1z["t_norm_us"]) * f1
        zmin, zmax, y0, a = self.s1_naive_z_correction
        return (y0 + a * z) / (y0 + a * (zmin + zmax) / 2)


@export
class Sites(_Corrections, strax.Plugin):
    """One row per S2 of an event above ``site_min_s2_area``: an interaction site paired with the main S1.

    drift_time = dt_s1 of the S2 (NaN without S1); z as in event_positions; x, y from peak_positions.
    cs2 = S2 corrected for lifetime and the S2_top (x, y) map (mode 1) or lifetime only (mode 2), as corrected_areas;
    cs2_top = cs2 x AFT, cs2_bottom = cs2 - cs2_top (no bottom map yet: same correction factor as the total).
    Fake sites (split S2s, photoionisation, trains) are kept; in_drift is False for sites before the main S1 or beyond
    the cathode, and those do not enter the event sums.
    """

    __version__ = "0.1.0"
    depends_on = ("event_peaks",)
    provides = "sites"
    data_kind = "sites"
    rechunk_on_save = False

    site_min_s2_area = amstrax.XAMSConfig(default=300, help="Minimum S2 area of a site [PE]")

    def infer_dtype(self):
        return strax.time_fields + [
            ("event_number", np.int64, "Event number in this dataset"),
            ("peak_index", np.int64, "Row of the S2 in peaks (link to event_peaks)"),
            ("rank", np.int16, "Rank by S2 area in this event (1 = main S2)"),
            ("drift_time", np.float32, "S2 center_time minus that of the main S1 [ns]"),
            ("x", np.float32, "x-position [mm]"),
            ("y", np.float32, "y-position [mm]"),
            ("z", np.float32, "Depth [mm] (0 at the gate)"),
            ("r", np.float32, "Radius [mm]"),
            ("cs2", np.float32, "Corrected S2, total [PE]"),
            ("cs2_top", np.float32, "Corrected S2 seen by the top array [PE]"),
            ("cs2_bottom", np.float32, "Corrected S2 seen by the bottom array [PE]"),
            ("dt_prev_s2", np.float32, "Time since the previous S2 of any size in this event (center times) [ns]; NaN if none"),
            ("in_drift", np.bool_, "0 <= drift_time <= 1.05 drift_time_cathode (enters the event sums)"),
        ]

    def compute(self, event_peaks):
        p = event_peaks
        s2 = p["type"] == 2
        prev = np.full(len(p), np.nan)
        i = np.where(s2)[0]
        same = np.diff(p["event_number"][i]) == 0
        prev[i[1:][same]] = np.diff(p["center_time"][i])[same]
        keep = s2 & (p["area"] > self.site_min_s2_area)
        p, prev = p[keep], prev[keep]
        r = np.zeros(len(p), self.dtype)
        for name in ("time", "endtime", "event_number", "peak_index", "rank", "x", "y"):
            r[name] = p[name]
        dt = p["dt_s1"].astype(np.float64)
        r["drift_time"], r["dt_prev_s2"] = dt, prev
        r["z"] = self.z_of(dt)
        r["r"] = np.hypot(p["x"], p["y"])
        r["in_drift"] = (dt >= 0) & (dt <= MAX_DRIFT_MARGIN * self.drift_time_cathode)
        if self.sr_mode:
            tau = CorrectedAreas.tau_us(p["time"], self.sr_corrections["tau"])
            _, f2 = CorrectedAreas.xy_factors(p["x"], p["y"], self.sr_maps)
            cs2 = p["area"] * np.exp(dt / 1e3 / tau) / f2
        else:
            cs2 = p["area"] * np.exp(dt / self.elife)
        r["cs2"], r["cs2_top"] = cs2, cs2 * p["area_fraction_top"]
        r["cs2_bottom"] = cs2 - r["cs2_top"]
        return r


@export
class EventsV2(_Corrections, strax.Plugin):
    """The event as a whole (no main / alt peak columns: those are rows of event_peaks and sites).

    cs1 = S1 / <S1 response>, the response averaged over the in-drift sites weighted with their cs2_top (the charge,
    so roughly the energy, of each site); cs2, cs2_top, cs2_bottom = sums over the in-drift sites;
    energy = W (cs1 / g1 + cs2_top / g2_top) in mode 1 when the run is calibrated, else NaN. For one site this is the
    energy of corrected_areas. dt_next_event is -1 for the last event of a chunk.
    """

    __version__ = "0.1.0"
    depends_on = ("events", "event_peaks", "sites")
    provides = "events_v2"
    data_kind = "events"
    parallel = False

    def infer_dtype(self):
        return strax.time_fields + [
            ("event_number", np.int64, "Event number in this dataset"),
            ("n_peaks", np.int32, "Number of peaks in the event"),
            ("n_s1", np.int32, "Number of S1s"),
            ("n_s2", np.int32, "Number of S2s"),
            ("n_sites", np.int32, "Number of sites (S2 > site_min_s2_area)"),
            ("n_sites_in_drift", np.int32, "Number of sites inside the drift region"),
            ("s1_peak_index", np.int64, "peak_index of the main S1; -1 if none"),
            ("cs1", np.float32, "Main S1 corrected with the cs2_top-weighted response of the sites [PE]"),
            ("cs2", np.float32, "Sum of cs2 over the in-drift sites [PE]"),
            ("cs2_top", np.float32, "Sum of cs2_top over the in-drift sites [PE]"),
            ("cs2_bottom", np.float32, "Sum of cs2_bottom over the in-drift sites [PE]"),
            ("energy", np.float32, "W (cs1 / g1 + cs2_top / g2_top) [keV]; NaN if not calibrated"),
            ("area_before_main_s2", np.float32, "Sum of the peak areas before the main S2 [PE]"),
            ("large_s2_before_main_s2", np.float32, "Largest S2 before the main S2 [PE]"),
            ("dt_prev_event", np.int64, "Start of this event minus end of the previous one [ns]; -1 for the first"),
            ("dt_next_event", np.int64, "Start of the next event minus end of this one [ns]; -1 at a chunk end"),
        ]

    def setup(self):
        self.last_endtime = -1

    def compute(self, events, event_peaks, sites):
        n = len(events)
        r = np.zeros(n, self.dtype)
        for name in ("time", "endtime", "event_number"):
            r[name] = events[name]
        p, s = event_peaks, sites
        ip = np.searchsorted(events["event_number"], p["event_number"])
        r["n_peaks"] = np.bincount(ip, minlength=n)
        r["n_s1"] = np.bincount(ip, p["type"] == 1, minlength=n)
        r["n_s2"] = np.bincount(ip, p["type"] == 2, minlength=n)
        is_ = np.searchsorted(events["event_number"], s["event_number"])
        r["n_sites"] = np.bincount(is_, minlength=n)

        s1 = (p["type"] == 1) & (p["dt_s1"] == 0)
        s1_area = np.full(n, np.nan)
        r["s1_peak_index"] = -1
        r["s1_peak_index"][ip[s1]], s1_area[ip[s1]] = p["peak_index"][s1], p["area"][s1]

        d = s["in_drift"]
        sd, idd = s[d], is_[d]
        r["n_sites_in_drift"] = np.bincount(idd, minlength=n)
        for k in ("cs2", "cs2_top", "cs2_bottom"):
            r[k] = np.bincount(idd, sd[k], minlength=n)
        w = sd["cs2_top"].astype(np.float64)
        with np.errstate(invalid="ignore", divide="ignore"):
            resp = (np.bincount(idd, w * self.s1_response(sd["z"], sd["x"], sd["y"]), minlength=n)
                    / np.bincount(idd, w, minlength=n))
            r["cs1"] = s1_area / resp
        none = r["n_sites_in_drift"] == 0
        for k in ("cs1", "cs2", "cs2_top", "cs2_bottom"):
            r[k][none] = np.nan
        c = self.sr_corrections
        r["energy"] = W_KEV * (r["cs1"] / c["g1"] + r["cs2_top"] / c["g2_top"]) if self.sr_mode and c["energy_calibrated"] else np.nan

        main_s2 = (p["type"] == 2) & (p["rank"] == 1)
        t_s2 = np.full(n, np.iinfo(np.int64).max)
        t_s2[ip[main_s2]] = p["time"][main_s2]
        before = p["time"] < t_s2[ip]
        r["area_before_main_s2"] = np.bincount(ip, np.where(before, p["area"], 0), minlength=n)
        r["large_s2_before_main_s2"] = 0
        np.maximum.at(r["large_s2_before_main_s2"], ip, np.where(before & (p["type"] == 2), p["area"], 0))

        if n:
            prev_end = np.concatenate([[self.last_endtime], events["endtime"][:-1]])
            r["dt_prev_event"] = np.where(prev_end >= 0, events["time"] - prev_end, -1)
            r["dt_next_event"][:-1] = events["time"][1:] - events["endtime"][:-1]
            r["dt_next_event"][-1] = -1
            self.last_endtime = events["endtime"][-1]
        return r


@numba.njit(cache=True)
def _context(center, ptype, area, ev_number, last_t, last_a, last_e):
    """Per peak: the last S2 above each CONTEXT_S2_AREAS threshold before it. last_t / last_a / last_e (center time,
    area, event per threshold; last_t -1 = none yet) are carried over chunks and updated in place."""
    n = len(center)
    nt = len(last_t)
    dt = np.full((n, nt), -1, np.int64)
    a = np.full((n, nt), np.nan, np.float32)
    e = np.full((n, nt), -1, np.int64)
    for i in range(n):
        for k in range(nt):
            if last_t[k] >= 0:
                dt[i, k] = center[i] - last_t[k]
                a[i, k] = last_a[k]
                e[i, k] = last_e[k]
        if ptype[i] == 2:
            for k in range(nt):
                if area[i] > THRESHOLDS[k]:
                    last_t[k], last_a[k], last_e[k] = center[i], area[i], ev_number[i]
    return dt, a, e



@export
class PeakContext(strax.Plugin):
    """Every peak of the run (inside and between events) linked to the last S2 > 1 kPE and the last S2 > 10 kPE
    before it (delayed electrons, photoionisation): time since that S2 (center times), its area and its event."""

    __version__ = "0.1.0"
    depends_on = ("events", "peak_basics")
    provides = "peak_context"
    data_kind = "peaks"
    parallel = False
    rechunk_on_save = False

    def infer_dtype(self):
        dtype = strax.time_fields + [
            ("peak_index", np.int64, "Row of the peak in peaks (counted from the first peak of the run)"),
            ("event_number", np.int64, "Event that contains the peak; -1 between events"),
        ]
        for a in CONTEXT_S2_AREAS:
            tag = f"{a // 1000}k"
            dtype += [
                (f"dt_prev_s2_{tag}", np.int64, f"Time since the last S2 > {a} PE (center times) [ns]; -1 if none"),
                (f"area_prev_s2_{tag}", np.float32, f"Area of the last S2 > {a} PE [PE]"),
                (f"event_prev_s2_{tag}", np.int64, f"Event of the last S2 > {a} PE; -1 if between events"),
            ]
        return dtype

    def setup(self):
        self.peaks_seen = 0
        nt = len(CONTEXT_S2_AREAS)
        self.last_t, self.last_a, self.last_e = np.full(nt, -1, np.int64), np.full(nt, np.nan), np.full(nt, -1, np.int64)

    def compute(self, events, peaks):
        r = np.zeros(len(peaks), self.dtype)
        r["time"], r["endtime"] = peaks["time"], peaks["endtime"]
        r["peak_index"] = self.peaks_seen + np.arange(len(peaks))
        self.peaks_seen += len(peaks)
        ev = strax.fully_contained_in(peaks, events)
        r["event_number"] = -1
        r["event_number"][ev >= 0] = events["event_number"][ev[ev >= 0]]
        dt, a, e = _context(peaks["center_time"], peaks["type"], peaks["area"].astype(np.float64), r["event_number"],
                            self.last_t, self.last_a, self.last_e)
        for k, thr in enumerate(CONTEXT_S2_AREAS):
            tag = f"{thr // 1000}k"
            r[f"dt_prev_s2_{tag}"], r[f"area_prev_s2_{tag}"], r[f"event_prev_s2_{tag}"] = dt[:, k], a[:, k], e[:, k]
        return r


EVENT_STRUCTURE_V2 = (EventPeaks, Sites, EventsV2, PeakContext)
EVENT_STRUCTURE_V2_TYPES = ("event_peaks", "sites", "events_v2", "peak_context")

S1_COLUMNS = ("area", "area_fraction_top", "range_50p_area", "n_channels", "tight_coincidence")
S2_COLUMNS = ("area", "area_fraction_top", "range_50p_area", "range_90p_area")
SITE_COLUMNS = ("drift_time", "x", "y", "z", "r", "cs2", "cs2_top", "cs2_bottom", "in_drift")


def main_interaction(st, run_id, **kwargs):
    """Flat table, one row per event: events_v2 (cs2* = sums over the sites), the main S1 (s1_*), the main site
    (rank 1: drift_time, x, y, z, r, site_cs2*, site_in_drift) and its S2 (s2_*).
    A join over events_v2, event_peaks and sites (not stored); kwargs go to st.get_df."""
    ev = st.get_df(run_id, "events_v2", **kwargs)
    p = st.get_df(run_id, "event_peaks", **kwargs)
    s = st.get_df(run_id, "sites", **kwargs)
    s1 = p[(p["type"] == 1) & (p["dt_s1"] == 0)][["event_number", *S1_COLUMNS]]
    s1.columns = ["event_number"] + [f"s1_{c}" for c in S1_COLUMNS]
    main = s[s["rank"] == 1][["event_number", "peak_index", *SITE_COLUMNS]]
    s2 = p[["peak_index", *S2_COLUMNS]]
    s2.columns = ["peak_index"] + [f"s2_{c}" for c in S2_COLUMNS]
    main = main.merge(s2, on="peak_index", how="left").drop(columns="peak_index")
    main = main.rename(columns={c: f"site_{c}" for c in ("cs2", "cs2_top", "cs2_bottom", "in_drift")})
    return ev.merge(s1, on="event_number", how="left").merge(main, on="event_number", how="left")
