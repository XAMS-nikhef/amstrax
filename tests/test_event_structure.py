import numpy as np

import amstrax
from amstrax.plugins.events.event_structure import PEAK_FIELDS, W_KEV
from corr_helpers import _corr, _maps

T0 = 10 ** 18


def _context(config=None):
    st = amstrax.contexts.xams(init_rundb=False)
    if config:
        st.set_config(config)
    return st


def _plugin(name, config=None):
    return _context(config).get_single_plugin("007790", name)


def _peaks():
    """Event 7 [0, 100 us]: S1, main S2 9000 PE, small S2 50 PE, 2nd S2 500 PE (beyond the cathode). Event 8 [200, 300 us]: S2 only.
    One peak between the events (S2 2000 PE at 150 us)."""
    rows = [  # time [us], type, area, aft
        (10, 1, 1700, 0.3), (31, 2, 9000, 0.66), (40, 2, 50, 0.6), (60, 2, 500, 0.6),
        (150, 2, 2000, 0.6),
        (220, 2, 20000, 0.6),
    ]
    dtype = [("time", np.int64), ("endtime", np.int64), ("type", np.int16), ("x", np.float32), ("y", np.float32)]
    dtype += [(n, t) for n, t, _ in PEAK_FIELDS]
    p = np.zeros(len(rows), dtype)
    for i, (t, typ, a, aft) in enumerate(rows):
        p[i]["time"] = T0 + t * 1000
        p[i]["endtime"] = p[i]["time"] + 1000
        p[i]["center_time"] = p[i]["time"] + 500
        p[i]["type"], p[i]["area"], p[i]["area_fraction_top"] = typ, a, aft
        p[i]["x"], p[i]["y"] = 5, 5
    return p


def _events():
    ev = np.zeros(2, [("event_number", np.int64), ("time", np.int64), ("endtime", np.int64)])
    ev["event_number"] = [7, 8]
    ev["time"] = [T0, T0 + 200_000]
    ev["endtime"] = [T0 + 100_000, T0 + 300_000]
    return ev


def test_event_peaks():
    ep = _plugin("event_peaks").compute(_events(), _peaks())
    assert list(ep["peak_index"]) == [0, 1, 2, 3, 5]                  # the peak between events is not in it
    assert list(ep["event_number"]) == [7, 7, 7, 7, 8]
    assert list(ep["rank"]) == [1, 1, 3, 2, 1]
    np.testing.assert_allclose(ep["dt_s1"][:4], [0, 21_000, 30_000, 50_000])
    assert np.isnan(ep["dt_s1"][4])                                    # no S1 in event 8


def test_peak_index_continues_over_chunks():
    pl = _plugin("event_peaks")
    pl.compute(_events(), _peaks())
    assert pl.compute(_events(), _peaks())["peak_index"][0] == 6


def test_sites_mode2():
    ep = _plugin("event_peaks").compute(_events(), _peaks())
    s = _plugin("sites", {"elife": 100_000}).compute(ep)
    assert list(s["peak_index"]) == [1, 3, 5]                          # S2 > 300 PE
    assert list(s["rank"]) == [1, 2, 1]
    np.testing.assert_allclose(s["cs2"][0], 9000 * np.exp(0.21), rtol=1e-6)
    np.testing.assert_allclose(s["cs2_top"] + s["cs2_bottom"], s["cs2"], rtol=1e-6)
    np.testing.assert_allclose(s["dt_prev_s2"][:2], [np.nan, 20_000])  # previous S2 of any size (the 50 PE one)
    assert list(s["in_drift"]) == [True, False, False]                 # 50 us > 1.05 x 39.5 us; no S1 in event 8


def test_single_site_corrections():
    """One in-drift site, mode 1: cs1 = S1 / (S1(z) S1(x, y)), cs2_top = S2 AFT exp(t / tau) / S2_top(x, y),
    energy = W (cs1 / g1 + cs2_top / g2_top) (the formulas of the former corrected_areas)."""
    q = (0.0, 0.01, 1.0)
    cfg = {"sr_corrections": _corr(), "sr_maps": _maps(s1=0.9, s2=1.1, q=q)}
    ep = _plugin("event_peaks").compute(_events(), _peaks())
    s = _plugin("sites", cfg).compute(ep)
    ev = _plugin("events", cfg).compute(_events(), ep, s)
    assert list(ev["n_sites"]) == [2, 1] and list(ev["n_sites_in_drift"]) == [1, 0]
    t_us, z = s["drift_time"][0] / 1e3, s["z"][0]
    t_ref = 1.45 - z / 1.325
    s1z = np.polyval(q, t_ref) / np.polyval(q, 21.0)
    cs1 = 1700 / (s1z * 0.9)
    cs2 = 9000 * np.exp(t_us / 500.0) / 1.1
    np.testing.assert_allclose(ev["cs1"][0], cs1, rtol=1e-5)
    np.testing.assert_allclose(ev["cs2"][0], cs2, rtol=1e-5)
    np.testing.assert_allclose(ev["cs2_top"][0], 0.66 * cs2, rtol=1e-5)
    np.testing.assert_allclose(ev["energy"][0], W_KEV * (cs1 / 0.0568 + 0.66 * cs2 / 0.332), rtol=1e-5)
    assert np.isnan(ev["cs1"][1]) and np.isnan(ev["energy"][1])


def test_events_counts_and_neighbours():
    ep = _plugin("event_peaks").compute(_events(), _peaks())
    s = _plugin("sites").compute(ep)
    pl = _plugin("events")
    ev = pl.compute(_events(), ep, s)
    assert list(ev["n_peaks"]) == [4, 1] and list(ev["n_s1"]) == [1, 0] and list(ev["n_s2"]) == [3, 1]
    assert list(ev["s1_peak_index"]) == [0, -1]
    np.testing.assert_allclose(ev["area_before_main_s2"], [1700, 0])
    assert list(ev["dt_prev_event"]) == [-1, 100_000] and list(ev["dt_next_event"]) == [100_000, -1]
    assert np.isnan(ev["energy"]).all()                                # no sr_corrections


def test_peak_context():
    pc = _plugin("peak_context").compute(_events(), _peaks())
    assert list(pc["event_number"]) == [7, 7, 7, 7, -1, 8]
    assert list(pc["dt_prev_s2_1k"]) == [-1, -1, 9_000, 29_000, 119_000, 70_000]
    assert list(pc["event_prev_s2_1k"]) == [-1, -1, 7, 7, 7, -1]       # last parent: the S2 between the events
    assert list(pc["dt_prev_s2_10k"]) == [-1] * 6                      # no S2 > 10 kPE before the last one
    np.testing.assert_allclose(pc["area_prev_s2_1k"][-1], 2000)


def test_only_the_event_structure():
    """amstrax 3: the four linked tables on the event windows; the old event format is gone (tag prod4-final)."""
    reg = amstrax.contexts.xams(init_rundb=False)._plugin_class_registry
    for t in ("event_windows", "events", "sites", "event_peaks", "peak_context"):
        assert t in reg
    for t in ("event_basics", "event_positions", "corrected_areas", "event_info", "events_v2",
              "event_coincidences", "peak_coincidences"):
        assert t not in reg
    assert reg["events"].depends_on == ("event_windows", "event_peaks", "sites")


def test_types_match_plugins():
    import strax
    assert set(amstrax.EVENT_TYPES) == {t for p in amstrax.EVENT_PLUGINS for t in strax.to_str_tuple(p.provides)}
