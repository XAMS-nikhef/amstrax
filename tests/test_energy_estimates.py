import numpy as np

import amstrax
from amstrax.plugins.events.energy_estimates import W_KEV, EnergyEstimates

EDGES = list(np.arange(-32, 33, 4.0))
NB = len(EDGES) - 1


def _maps(s1=1.0, s2=1.0, q=(0.0, 0.0, 1.0)):
    s1m = [[s1] * NB for _ in range(NB)]
    s2m = [[s2] * NB for _ in range(NB)]
    s2m[0][0] = None                                       # a cell without a value
    return {"edges_mm": EDGES, "s1_rel": s1m, "s2_top_rel": s2m,
            "s1_z": {"quadratic_in_drift_us": list(q), "t_norm_us": 21.0, "ref_v_drift_mm_per_us": 1.325, "ref_t_gate_us": 1.45}}


def _corr(tau=None, calibrated=True):
    return {"v_drift_mm_per_us": 1.325, "t_gate_us": 1.45, "tau": tau or {"fixed_us": 500.0}, "g1": 0.0568, "g2_top": 0.332,
            "energy_calibrated": calibrated}


def _events(n=3):
    fields = [("time", np.int64), ("endtime", np.int64), ("drift_time", np.float32), ("s1_area", np.float32), ("s2_area", np.float32),
              ("s2_area_fraction_top", np.float32), ("s2_x", np.float32), ("s2_y", np.float32)]
    ev = np.zeros(n, dtype=fields)
    ev["time"] = 10 ** 18 + np.arange(n) * 3_600 * 10 ** 9   # one hour apart
    ev["endtime"] = ev["time"] + 1
    ev["drift_time"] = 21_000
    ev["s1_area"], ev["s2_area"], ev["s2_area_fraction_top"] = 1720, 9000, 0.66
    ev["s2_x"], ev["s2_y"] = 5, 5
    return ev


def _plugin(corr, maps):
    st = amstrax.contexts.xams(init_rundb=False)
    st.set_config({"sr_corrections": corr, "sr_maps": maps})
    return st.get_single_plugin("007790", "energy_estimates")


def test_energy_formula():
    ev = _events()
    r = _plugin(_corr(), _maps(s1=0.9, s2=1.1)).compute(ev)
    cs1 = 1720 / 0.9
    cs2t = 9000 * 0.66 * np.exp(21 / 500) / 1.1
    np.testing.assert_allclose(r["energy_cs1"], cs1, rtol=1e-5)
    np.testing.assert_allclose(r["energy_cs2_top"], cs2t, rtol=1e-5)
    np.testing.assert_allclose(r["energy"], W_KEV * (cs1 / 0.0568 + cs2t / 0.332), rtol=1e-5)
    np.testing.assert_allclose(r["energy_z"], -1.325 * (21 - 1.45), rtol=1e-5)
    assert r["energy_map_ok"].all() and r["energy_calibrated"].all()


def test_s1_z_in_reference_field():
    """Same z in another drift field: S1(z) is evaluated at the reference drift time of that z."""
    q = (0.78, 0.85, 1346.0)
    corr = dict(_corr(), v_drift_mm_per_us=0.753, t_gate_us=1.55)       # 25 V/cm
    ev = _events()
    r = _plugin(corr, _maps(q=q)).compute(ev)
    z = -0.753 * (21 - 1.55)
    t_ref = 1.45 - z / 1.325
    np.testing.assert_allclose(r["energy_cs1"], 1720 / (np.polyval(q, t_ref) / np.polyval(q, 21.0)), rtol=1e-5)


def test_lifetime_model_in_time():
    p, t0 = [1.42e-3, 1.40e-3, 5.4], 10 ** 18
    ev = _events()
    tau = EnergyEstimates.tau_us(ev["time"], {"model": "exponential", "params": p, "t0_ns": t0})
    h = np.arange(3.0)
    np.testing.assert_allclose(tau, 1 / (p[0] + p[1] * np.exp(-h / p[2])), rtol=1e-9)
    r = _plugin(_corr(tau={"model": "exponential", "params": p, "t0_ns": t0}), _maps()).compute(ev)
    np.testing.assert_allclose(r["energy_tau"], tau, rtol=1e-6)


def test_outside_map_and_empty_cell():
    ev = _events()
    ev["s2_x"][1], ev["s2_y"][1] = 40, 0                   # outside the map
    ev["s2_x"][2], ev["s2_y"][2] = -31, -31                # cell without a value
    r = _plugin(_corr(calibrated=False), _maps()).compute(ev)
    assert list(r["energy_map_ok"]) == [True, False, False]
    assert np.isnan(r["energy"][1]) and np.isnan(r["energy_cs2_top"][2]) and np.isfinite(r["energy_cs1"][2])
    assert not r["energy_calibrated"].any()


def test_no_corrections_gives_nan():
    r = _plugin(None, None).compute(_events())
    assert np.isnan(r["energy"]).all() and not r["energy_map_ok"].any()
