"""Bottom-PMT S1 non-linearity correction in events (option s1_linearity, SR2026.3 s1_linearity_v1)."""
import numpy as np

from amstrax.plugins.events.event_structure import s1_linearity_corrected, s1_linearity_factor
from corr_helpers import _corr, _maps
from test_event_structure import _events, _peaks, _plugin

P = {"a": 0.0694, "S0_PE": 2000.0, "S_min_PE": 900.0, "S_max_PE": 4500.0}


# copy of the reference implementation (measurements/SR2026.3/lib/s1_linearity.py), not imported from there
def _ref_factor(s_bot, p):
    s = np.clip(np.asarray(s_bot, dtype=float), p["S_min_PE"], p["S_max_PE"])
    return 1.0 + p["a"] * np.log(s / p["S0_PE"])


def _ref_corrected_s1(area, aft, p):
    area = np.asarray(area, dtype=float)
    aft = np.clip(np.nan_to_num(np.asarray(aft, dtype=float), nan=0.0), 0.0, 1.0)
    top, bot = area * aft, area * (1.0 - aft)
    return top + bot / _ref_factor(bot, p)


def test_formula_matches_reference():
    rng = np.random.default_rng(1)
    area = rng.uniform(0, 20_000, 10_000)
    aft = rng.uniform(-0.2, 1.2, 10_000)
    aft[::97] = np.nan
    area[::101] = np.nan
    np.testing.assert_array_equal(s1_linearity_corrected(area, aft, P), _ref_corrected_s1(area, aft, P))
    np.testing.assert_array_equal(s1_linearity_factor(area, P), _ref_factor(area, P))


def test_edge_cases():
    # AFT NaN -> 0 (all bottom), AFT outside [0, 1] clipped
    np.testing.assert_allclose(s1_linearity_corrected([3000.0], [np.nan], P), 3000 / (1 + 0.0694 * np.log(1.5)))
    np.testing.assert_allclose(s1_linearity_corrected([3000.0], [-0.5], P), 3000 / (1 + 0.0694 * np.log(1.5)))
    np.testing.assert_allclose(s1_linearity_corrected([3000.0], [1.5], P), 3000.0)       # all top: unchanged
    # S_bot below / above the clamp: factor held at S_min / S_max
    f_lo, f_hi = 1 + 0.0694 * np.log(900 / 2000), 1 + 0.0694 * np.log(4500 / 2000)
    np.testing.assert_allclose(s1_linearity_factor([10.0, 900.0, 1e6, 4500.0], P), [f_lo, f_lo, f_hi, f_hi])
    np.testing.assert_allclose(s1_linearity_corrected([500.0], [0.2], P), 100 + 400 / f_lo)
    np.testing.assert_allclose(s1_linearity_corrected([10_000.0], [0.2], P), 2000 + 8000 / f_hi)
    np.testing.assert_allclose(s1_linearity_factor([2000.0], P), 1.0)                    # S0: no correction
    # S1 NaN stays NaN
    assert np.isnan(s1_linearity_corrected([np.nan], [0.3], P)).all()


def _run(cfg):
    ep = _plugin("event_peaks").compute(_events(), _peaks())
    s = _plugin("sites", cfg).compute(ep)
    return _plugin("events", cfg).compute(_events(), ep, s)


def test_option_none_is_unchanged():
    cfg = {"sr_corrections": _corr(), "sr_maps": _maps(s1=0.9, s2=1.1, q=(0.0, 0.01, 1.0))}
    for c in (cfg, {}):                                              # mode 1 and mode 2
        ev = _run(c)
        np.testing.assert_array_equal(ev["cs1"], ev["cs1_nolin"])
        assert ev["s1_lin_factor"][0] == 1.0
        assert np.isnan(ev["s1_lin_factor"][1]) and np.isnan(ev["cs1"][1])     # event without S1 / site


def test_mode2_ignores_s1_linearity():
    """Without sr_corrections / sr_maps (mode 2, MC) the correction is never applied."""
    ev = _run({"s1_linearity": P})
    np.testing.assert_array_equal(ev["cs1"], ev["cs1_nolin"])
    assert ev["s1_lin_factor"][0] == 1.0


def test_linearity_in_events():
    q = (0.0, 0.01, 1.0)
    base = {"sr_corrections": _corr(), "sr_maps": _maps(s1=0.9, s2=1.1, q=q)}
    ev0 = _run(base)
    ev = _run({**base, "s1_linearity": P})
    s1_lin = _ref_corrected_s1([1700.0], [np.float32(0.3)], P)[0]  # main S1 of _peaks: 1700 PE, AFT 0.3
    np.testing.assert_array_equal(ev["cs1_nolin"], ev0["cs1"])      # same corrections without the linearity
    np.testing.assert_allclose(ev["cs1"][0], ev0["cs1"][0] * s1_lin / 1700, rtol=1e-6)
    np.testing.assert_allclose(ev["s1_lin_factor"][0], ev["cs1"][0] / ev["cs1_nolin"][0], rtol=1e-6)
    assert ev["s1_lin_factor"][0] > 1                               # S_bot = 1190 PE < S0: f < 1, S1 goes up
    c = _corr()
    np.testing.assert_allclose(ev["energy"][0], 13.7e-3 * (ev["cs1"][0] / c["g1"] + ev["cs2_top"][0] / c["g2_top"]),
                               rtol=1e-5)
    assert np.isnan(ev["cs1"][1]) and np.isnan(ev["cs1_nolin"][1]) and np.isnan(ev["s1_lin_factor"][1])
    for k in ("cs2", "cs2_top", "cs2_bottom", "n_sites"):
        np.testing.assert_array_equal(ev[k], ev0[k])
