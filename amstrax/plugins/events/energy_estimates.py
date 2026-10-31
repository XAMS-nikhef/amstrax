import numpy as np
import strax
import amstrax

export, __all__ = strax.exporter()

W_KEV = 13.7e-3  # average energy per quantum (photon or electron) in LXe [keV]


@export
class EnergyEstimates(strax.Plugin):
    """Position- and lifetime-corrected S1 / top-array S2 and the combined energy of the main interaction.

    Corrections of a science run (first: SR-2026.3, analysis corrections SR2026.3_v5), from two correction files:
      - ``sr_corrections`` (per run): drift velocity and gate drift time of the run's drift field, the electron
        lifetime (a model in time, or a fixed value), g1, g2_top and whether the energy scale is calibrated for
        this run's condition;
      - ``sr_maps`` (per run range): S1 and S2_top (x, y) maps (4 mm cells) and S1(z).
    Per event:
      z            = -v_drift (drift_time - t_gate)                                           (0 at the gate)
      energy_cs1   = S1 / S1(z) / S1(x, y), S1(z) taken at the drift time z would have in the reference field
      energy_cs2_top = S2 x AFT x exp(drift_time / tau(t)) / S2_top(x, y)
      energy       = W (energy_cs1 / g1 + energy_cs2_top / g2_top), W = 13.7 eV
    The top-array S2 is used because the bottom PMT misses S2 light of small S2s (SR-2026.3: S2 total 13 %
    super-linear between 511 and 1332 keV). Not the same as corrected_areas (cs1 / cs2: total S2, one lifetime).
    energy_calibrated is False where g1 / g2_top are not verified for the run's condition (SR-2026.3: drift-field
    scan, the energy there is off by -3 ... +12 %); energy_map_ok is False outside the maps (and the central cell).
    Runs without ``sr_corrections`` (None) get NaN.
    """

    __version__ = "1.0.0"

    depends_on = ("event_basics",)
    provides = "energy_estimates"

    sr_corrections = amstrax.XAMSConfig(default=None, help="per-run energy constants (drift, lifetime, g1, g2_top); None = no energy")
    sr_maps = amstrax.XAMSConfig(default=None, help="S1 / S2_top (x, y) maps and S1(z) of the run range; None = no energy")

    def infer_dtype(self):
        return strax.time_fields + [
            ("energy", np.float32, "Combined energy W (cS1 / g1 + cS2_top / g2_top) [keV]"),
            ("energy_cs1", np.float32, "Main S1 corrected for z and (x, y) [PE]"),
            ("energy_cs2_top", np.float32, "Main S2 x AFT, corrected for lifetime and (x, y) [PE]"),
            ("energy_z", np.float32, "Depth from the drift time with the run's drift velocity [mm] (0 at the gate)"),
            ("energy_tau", np.float32, "Electron lifetime used [us]"),
            ("energy_map_ok", np.bool_, "Event inside the (x, y) maps"),
            ("energy_calibrated", np.bool_, "g1 / g2_top verified for this run's condition"),
        ]

    def compute(self, events):
        r = np.zeros(len(events), self.dtype)
        r["time"], r["endtime"] = events["time"], strax.endtime(events)
        c, m = self.sr_corrections, self.sr_maps
        if c is None or m is None:
            for k in ("energy", "energy_cs1", "energy_cs2_top", "energy_z", "energy_tau"):
                r[k] = np.nan
            return r
        t = events["drift_time"] / 1e3
        z = -c["v_drift_mm_per_us"] * (t - c["t_gate_us"])
        s1z = m["s1_z"]
        t_ref = s1z["ref_t_gate_us"] - z / s1z["ref_v_drift_mm_per_us"]
        q = np.asarray(s1z["quadratic_in_drift_us"])
        f1, f2 = self.xy_factors(events["s2_x"], events["s2_y"], m)
        tau = self.tau_us(events["time"], c["tau"])
        cs1 = events["s1_area"] / (np.polyval(q, t_ref) / np.polyval(q, s1z["t_norm_us"])) / f1
        cs2t = events["s2_area"] * events["s2_area_fraction_top"] * np.exp(t / tau) / f2
        r["energy_z"], r["energy_tau"], r["energy_cs1"], r["energy_cs2_top"] = z, tau, cs1, cs2t
        r["energy"] = W_KEV * (cs1 / c["g1"] + cs2t / c["g2_top"])
        r["energy_map_ok"] = np.isfinite(f1) & np.isfinite(f2)
        r["energy_calibrated"] = bool(c["energy_calibrated"])
        return r

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
        i = np.clip(np.digitize(x, edges) - 1, 0, nb - 1)
        j = np.clip(np.digitize(y, edges) - 1, 0, nb - 1)
        inside = (x >= edges[0]) & (x < edges[-1]) & (y >= edges[0]) & (y < edges[-1])
        return np.where(inside, s1[i, j], np.nan), np.where(inside, s2[i, j], np.nan)
