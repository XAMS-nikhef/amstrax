import numpy as np

import amstrax
from amstrax.plugins.events.corrected_areas import W_KEV, CorrectedAreas

EDGES = list(np.arange(-32, 33, 4.0))
NB = len(EDGES) - 1
FIELDS = ("cs1", "cs2", "cs2_top", "alt_cs1", "alt_cs2", "energy")


def _events(n=3):
    fields = [("time", np.int64), ("endtime", np.int64), ("z", np.float32), ("drift_time", np.float32),
              ("alt_s2_interaction_drift_time", np.float32)]
    for p in ("", "alt_"):
        for s in ("s1", "s2"):
            fields += [(f"{p}{s}_area", np.float32), (f"{p}{s}_area_fraction_top", np.float32)]
        fields += [(f"{p}s2_x", np.float32), (f"{p}s2_y", np.float32)]
    ev = np.zeros(n, dtype=fields)
    ev["time"] = 10 ** 18 + np.arange(n) * 3_600 * 10 ** 9   # one hour apart
    ev["endtime"] = ev["time"] + 1
    ev["drift_time"], ev["alt_s2_interaction_drift_time"] = 21_000, 30_000
    ev["z"] = -1.325 * (21 - 1.45)
    for p in ("", "alt_"):
        ev[f"{p}s1_area"], ev[f"{p}s2_area"] = 1720, 9000
        ev[f"{p}s1_area_fraction_top"], ev[f"{p}s2_area_fraction_top"] = 0.3, 0.66
        ev[f"{p}s2_x"], ev[f"{p}s2_y"] = 5, 5
    return ev


def _maps(s1=1.0, s2=1.0, q=(0.0, 0.0, 1.0)):
    s1m = [[s1] * NB for _ in range(NB)]
    s2m = [[s2] * NB for _ in range(NB)]
    s2m[0][0] = None                                       # a cell without a value
    return {"edges_mm": EDGES, "s1_rel": s1m, "s2_top_rel": s2m,
            "s1_z": {"quadratic_in_drift_us": list(q), "t_norm_us": 21.0, "ref_v_drift_mm_per_us": 1.325, "ref_t_gate_us": 1.45}}


def _corr(tau=None, calibrated=True):
    return {"tau": tau or {"fixed_us": 500.0}, "g1": 0.0568, "g2_top": 0.332, "energy_calibrated": calibrated}


def _plugin(config):
    st = amstrax.contexts.xams(init_rundb=False)
    st.set_config(config)
    return st.get_single_plugin("007790", "corrected_areas")


def test_fields():
    res = _plugin({"elife": 100_000}).compute(_events())
    assert set(res.dtype.names) == {"time", "endtime", *FIELDS}


def test_without_sr_corrections():
    """Older versions: naive S1(z), total S2 with one lifetime, cs2_top = cs2 x AFT, no energy."""
    res = _plugin({"elife": 100_000, "s1_naive_z_correction": [-52, 0, 700, -7.69]}).compute(_events())
    np.testing.assert_allclose(res["cs2"], 9000 * np.exp(0.21), rtol=1e-6)
    np.testing.assert_allclose(res["cs2_top"], 0.66 * res["cs2"], rtol=1e-6)
    assert np.isnan(res["energy"]).all()


def test_sr_formula():
    ev = _events()
    res = _plugin({"sr_corrections": _corr(), "sr_maps": _maps(s1=0.9, s2=1.1)}).compute(ev)
    cs1 = 1720 / 0.9
    cs2t = 9000 * 0.66 * np.exp(21 / 500) / 1.1
    np.testing.assert_allclose(res["cs1"], cs1, rtol=1e-5)
    np.testing.assert_allclose(res["cs2_top"], cs2t, rtol=1e-5)
    np.testing.assert_allclose(res["cs2"], 9000 * np.exp(21 / 500) / 1.1, rtol=1e-5)
    np.testing.assert_allclose(res["alt_cs2"], 9000 * np.exp(30 / 500) / 1.1, rtol=1e-5)
    np.testing.assert_allclose(res["energy"], W_KEV * (cs1 / 0.0568 + cs2t / 0.332), rtol=1e-5)


def test_s1_z_in_reference_field():
    """Same z in another drift field: S1(z) is evaluated at the reference drift time of that z."""
    q = (0.78, 0.85, 1346.0)
    ev = _events()
    ev["z"] = -0.753 * (21 - 1.55)                         # 25 V/cm
    res = _plugin({"sr_corrections": _corr(), "sr_maps": _maps(q=q)}).compute(ev)
    t_ref = 1.45 - ev["z"][0] / 1.325
    np.testing.assert_allclose(res["cs1"], 1720 / (np.polyval(q, t_ref) / np.polyval(q, 21.0)), rtol=1e-5)


def test_lifetime_model_in_time():
    p, t0 = [1.42e-3, 1.40e-3, 5.4], 10 ** 18
    ev = _events()
    tau = CorrectedAreas.tau_us(ev["time"], {"model": "exponential", "params": p, "t0_ns": t0})
    np.testing.assert_allclose(tau, 1 / (p[0] + p[1] * np.exp(-np.arange(3.0) / p[2])), rtol=1e-9)
    res = _plugin({"sr_corrections": _corr(tau={"model": "exponential", "params": p, "t0_ns": t0}), "sr_maps": _maps()}).compute(ev)
    np.testing.assert_allclose(res["cs2_top"], 9000 * 0.66 * np.exp(21 / tau), rtol=1e-5)


def test_not_calibrated_and_outside_map():
    ev = _events()
    ev["s2_x"][1], ev["s2_y"][1] = 40, 0                   # outside the map
    ev["s2_x"][2], ev["s2_y"][2] = -31, -31                # S2 cell without a value
    res = _plugin({"sr_corrections": _corr(), "sr_maps": _maps()}).compute(ev)
    assert np.isfinite(res["energy"][0]) and np.isnan(res["energy"][1:]).all()
    assert np.isfinite(res["cs1"][2]) and np.isnan(res["cs2_top"][2])
    res = _plugin({"sr_corrections": _corr(calibrated=False), "sr_maps": _maps()}).compute(_events())
    assert np.isnan(res["energy"]).all() and np.isfinite(res["cs1"]).all()
