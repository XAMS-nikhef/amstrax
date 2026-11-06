"""event_cuts: the cut booleans from events, sites and event_peaks; the radial S2_top factor of sites."""
import numpy as np

from amstrax.plugins.events.event_cuts import CUTS, cuts, main_interaction_arrays
from amstrax.plugins.events.event_structure import s2_radial


def _tables():
    ev = np.zeros(3, dtype=[("event_number", np.int64), ("time", np.int64), ("endtime", np.int64)])
    ev["event_number"] = [1, 2, 3]
    pk = np.zeros(6, dtype=[("event_number", np.int64), ("peak_index", np.int64), ("type", np.int16), ("rank", np.int16),
                            ("dt_s1", np.float32), ("time", np.int64), ("area", np.float32),
                            ("area_fraction_top", np.float32), ("range_50p_area", np.float32)])
    #            ev  idx type rank dt_s1   time  area  aft  w50
    rows = [(1, 10, 1, 1, 0, 100, 1700, 0.30, 80),
            (1, 11, 2, 1, 20000, 20100, 9000, 0.66, 1000),
            (1, 12, 2, 2, 25000, 25100, 100, 0.60, 900),      # alt S2 1.1 % of the main: isolated
            (2, 20, 1, 1, 0, 200, 1500, 0.30, 80),
            (2, 21, 2, 1, 10000, 10200, 5000, 0.66, 1000),
            (2, 22, 2, 2, 12000, 12200, 1000, 0.60, 900)]     # alt S2 20 %: not isolated
    for i, r in enumerate(rows):
        pk[i] = r
    si = np.zeros(2, dtype=[("event_number", np.int64), ("peak_index", np.int64), ("rank", np.int16),
                            ("drift_time", np.float32), ("x", np.float32), ("y", np.float32), ("z", np.float32)])
    si[0] = (1, 11, 1, 20000, 5, 5, -20)
    si[1] = (2, 21, 1, 10000, 25, 0, -10)                     # r = 25 mm: outside the fiducial radius
    return ev, si, pk


def _ref():
    band = {"t_us": [0.0, 40.0], "med": [0.3, 0.3], "sig": [0.05, 0.05]}
    wband = {"t_us": [0.0, 40.0], "med": [3.0, 3.0], "sig": [0.1, 0.1]}
    return {"bands": {"cathode_us": 40.0, "s2_min_PE": 300.0, "s1_aft": band, "s2_log10_w50_ns": wband},
            "oddcut": {"r_mm": [0.0, 30.0], "aft_ref": [0.65, 0.65], "t_us": [0.0, 40.0], "w50_ref_us": [1.0, 1.0]},
            "fiducial": {"r_max_mm": 20.0, "z_mm": [-45.0, -7.0]}}


def test_main_interaction_and_cuts():
    ev, si, pk = _tables()
    m = main_interaction_arrays(ev, si, pk)
    np.testing.assert_allclose(m["s1_area"], [1700, 1500, np.nan])
    np.testing.assert_allclose(m["s2_area"], [9000, 5000, np.nan])
    np.testing.assert_allclose(m["alt_s2_area"], [100, 1000, np.nan])
    c = cuts(m, _ref())
    assert list(c["cut_basic"]) == [True, True, False]
    assert list(c["cut_iso_s2"]) == [True, False, True]
    assert list(c["cut_c"][:2]) == [True, True]
    assert list(c["cut_fiducial"]) == [True, False, False]
    assert list(c["cut_all"]) == [True, False, False]


def test_without_references():
    ev, si, pk = _tables()
    c = cuts(main_interaction_arrays(ev, si, pk), None)
    for k in ("cut_s1_aft", "cut_s2_width", "cut_c", "cut_fiducial", "cut_all"):
        assert not c[k].any()
    assert set(c) == set(CUTS)


def test_s2_radial():
    x, y = np.array([0.0, 10.0, 20.0]), np.zeros(3)
    np.testing.assert_allclose(s2_radial(x, y, {}), 1)
    m = {"s2_radial": {"r_mm": [2.0, 10.0, 18.0], "f": [1.0, 1.0, 0.9]}}
    np.testing.assert_allclose(s2_radial(x, y, m), [1.0, 1.0, 0.9])
