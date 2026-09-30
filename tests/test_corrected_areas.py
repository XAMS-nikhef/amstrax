import numpy as np
import strax

import amstrax


def _events(n=4):
    fields = [("time", np.int64), ("endtime", np.int64), ("z", np.float32), ("drift_time", np.float32)]
    for p in ("", "alt_"):
        for s in ("s1", "s2"):
            fields += [(f"{p}{s}_area", np.float32), (f"{p}{s}_area_fraction_top", np.float32)]
    ev = np.zeros(n, dtype=fields)
    ev["time"] = np.arange(n)
    ev["endtime"] = ev["time"] + 1
    ev["z"] = -20
    ev["drift_time"] = 10_000
    for p in ("", "alt_"):
        ev[f"{p}s1_area"] = 100
        ev[f"{p}s2_area"] = 5000
        ev[f"{p}s1_area_fraction_top"] = 0.3
        ev[f"{p}s2_area_fraction_top"] = 0.6
    return ev


def test_top_bottom_split():
    st = amstrax.contexts.xams(init_rundb=False)
    st.set_config({"elife": 100_000, "s1_naive_z_correction": [-52, 0, 700, -7.69]})
    plugin = st.get_single_plugin("007719", "corrected_areas")
    res = plugin.compute(_events())
    for p in ("", "alt_"):
        for s, aft in (("s1", 0.3), ("s2", 0.6)):
            total = res[f"{p}c{s}"]
            np.testing.assert_allclose(res[f"{p}c{s}_top"] + res[f"{p}c{s}_bottom"], total, rtol=1e-6)
            np.testing.assert_allclose(res[f"{p}c{s}_top"], aft * total, rtol=1e-6)
    np.testing.assert_allclose(res["cs2"], 5000 * np.exp(0.1), rtol=1e-6)
