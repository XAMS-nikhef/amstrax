import numpy as np
import strax
import amstrax

export, __all__ = strax.exporter()

W_KEV = 13.7e-3  # average energy per quantum (photon or electron) in LXe [keV]


@export
class CorrectedAreas(strax.Plugin):
    """Corrected S1 / S2 areas of the main and alternative peaks and the combined energy.

    Two modes, chosen by the corrections of the version:

    1. With ``sr_corrections`` and ``sr_maps`` for the run (first: SR-2026.3, analysis corrections SR2026.3_v5, PROD4):
         cs1     = S1 / S1(z) / S1(x, y), S1(z) taken at the drift time z would have in the reference drift field
         cs2_top = S2 x AFT x exp(drift_time / tau) / S2_top(x, y)   (top array: the bottom PMT misses S2 light of
                   small S2s, so the total S2 is not linear)
         cs2     = S2 x exp(drift_time / tau) / S2_top(x, y)        (total S2; not for energy)
         energy  = W (cs1 / g1 + cs2_top / g2_top), W = 13.7 eV; NaN where g1 / g2_top are not verified for the run's
                   condition (SR-2026.3: drift-field scan, zero field) and outside the (x, y) maps
       tau is a model in time (per event) or a fixed value per run; z comes from event_positions (drift velocity per
       run via drift_time_gate / drift_time_cathode). The alternative S1 uses the main interaction's position, the
       alternative S2 its own position and drift time.
    2. Otherwise (older versions): cs1 = S1 x naive linear z correction, cs2 = S2 x exp(drift_time / elife),
       cs2_top = cs2 x AFT, energy NaN.
    """

    __version__ = "1.0.0"

    depends_on = ("event_basics", "event_positions")

    elife = amstrax.XAMSConfig(default=30000, help="electron lifetime in [ns] (mode 2)")

    s1_naive_z_correction = amstrax.XAMSConfig(default=[-52, 0, 700, -7.69],
        help="Parameters for the z-dependent S1 correction \
            [zmin, zmax, y0, a] where y0 + a*z is the correction (mode 2)",
    )

    sr_corrections = amstrax.XAMSConfig(default=None, help="per-run constants: lifetime, g1, g2_top, energy_calibrated (mode 1)")
    sr_maps = amstrax.XAMSConfig(default=None, help="S1 / S2_top (x, y) maps and S1(z) of the run range (mode 1)")

    def infer_dtype(self):
        return strax.time_fields + [
            ("cs1", np.float32, "Corrected area of the main S1 [PE]"),
            ("cs2", np.float32, "Corrected area of the main S2, total (not for energy) [PE]"),
            ("cs2_top", np.float32, "Corrected area of the main S2 seen by the top array [PE]"),
            ("alt_cs1", np.float32, "Corrected area of the alternate S1 [PE]"),
            ("alt_cs2", np.float32, "Corrected area of the alternate S2, total [PE]"),
            ("energy", np.float32, "Combined energy W (cs1 / g1 + cs2_top / g2_top) [keV]; NaN if not calibrated"),
        ]

    def get_s1_naive_z_correction(self, z):
        """
        Apply a naive z-dependent S1 correction.
        Returns the correction factor for the S1 area.
        """

        s1_correction_function = lambda z: y0 + a * z
        zmin, zmax, y0, a = self.s1_naive_z_correction
        s1_correction_average = s1_correction_function((zmin + zmax) / 2)
        correction = s1_correction_average/s1_correction_function(z)

        return correction

    def compute(self, events):
        result = np.zeros(len(events), self.dtype)
        result["time"] = events["time"]
        result["endtime"] = events["endtime"]
        if self.sr_corrections is not None and self.sr_maps is not None:
            self.compute_sr(events, result, self.sr_corrections, self.sr_maps)
            return result

        for peak_type in ["", "alt_"]:
            result[f"{peak_type}cs1"] = events[f"{peak_type}s1_area"] * self.get_s1_naive_z_correction(events["z"])
            result[f"{peak_type}cs2"] = events[f"{peak_type}s2_area"] * np.exp(events["drift_time"] / self.elife)
        result["cs2_top"] = result["cs2"] * events["s2_area_fraction_top"]
        result["energy"] = np.nan
        return result

    def compute_sr(self, events, result, c, m):
        s1z = m["s1_z"]
        q = np.asarray(s1z["quadratic_in_drift_us"])
        t_ref = s1z["ref_t_gate_us"] - events["z"] / s1z["ref_v_drift_mm_per_us"]
        s1_z = np.polyval(q, t_ref) / np.polyval(q, s1z["t_norm_us"])
        tau = self.tau_us(events["time"], c["tau"])
        f1, f2 = self.xy_factors(events["s2_x"], events["s2_y"], m)
        _, f2_alt = self.xy_factors(events["alt_s2_x"], events["alt_s2_y"], m)
        t, t_alt = events["drift_time"] / 1e3, events["alt_s2_interaction_drift_time"] / 1e3
        result["cs1"] = events["s1_area"] / s1_z / f1
        result["alt_cs1"] = events["alt_s1_area"] / s1_z / f1
        att = np.exp(t / tau)
        result["cs2"] = events["s2_area"] * att / f2
        result["cs2_top"] = events["s2_area"] * events["s2_area_fraction_top"] * att / f2
        result["alt_cs2"] = events["alt_s2_area"] * np.exp(t_alt / tau) / f2_alt
        if c["energy_calibrated"]:
            result["energy"] = W_KEV * (result["cs1"] / c["g1"] + result["cs2_top"] / c["g2_top"])
        else:
            result["energy"] = np.nan

    @staticmethod
    def tau_us(time_ns, tau):
        """Electron lifetime [us]: {"fixed_us": x} or {"model": "exponential", "params": [p0, p1, p2], "t0_ns": t0} with
        1/tau [1/us] = p0 + p1 exp(-h / p2), h = hours since t0."""
        if "fixed_us" in tau:
            return np.full(len(time_ns), tau["fixed_us"], dtype=np.float64)
        p0, p1, p2 = tau["params"]
        h = (np.asarray(time_ns, dtype=np.float64) - tau["t0_ns"]) / 3.6e12
        return 1 / (p0 + p1 * np.exp(-h / p2))

    @staticmethod
    def xy_factors(x, y, m):
        """Relative S1 and S2_top response of the 4 mm cell of (x, y); NaN outside the map and in cells without a value."""
        edges = np.asarray(m["edges_mm"])
        s1 = np.array([[np.nan if v is None else v for v in row] for row in m["s1_rel"]], dtype=np.float64)
        s2 = np.array([[np.nan if v is None else v for v in row] for row in m["s2_top_rel"]], dtype=np.float64)
        nb = len(edges) - 1
        x, y = np.nan_to_num(x, nan=1e9), np.nan_to_num(y, nan=1e9)
        i = np.clip(np.digitize(x, edges) - 1, 0, nb - 1)
        j = np.clip(np.digitize(y, edges) - 1, 0, nb - 1)
        inside = (x >= edges[0]) & (x < edges[-1]) & (y >= edges[0]) & (y < edges[-1])
        return np.where(inside, s1[i, j], np.nan), np.where(inside, s2[i, j], np.nan)
