"""Position reconstruction v2 only: the pos_rec_map is applied per peak (peak_positions) when the run has a map and
gains; event_basics copies the S2 positions, event_positions passes them on. No polynomial fallback."""
import unittest

import numpy as np
import strax

import amstrax
from amstrax.plugins.peaks.peak_positions import grid_lookup, uv_from_cgr


def linear_map(n=41, a=30.0, b=-20.0):
    """synthetic map: x = a * u, y = b * v (bilinear interpolation reproduces it exactly)"""
    g = np.linspace(-1.0, 1.0, n)
    gu, gv = np.meshgrid(g, g, indexing="ij")
    return dict(uv_min=-1.0, step=float(g[1] - g[0]), n=n, x=(a * gu).tolist(), y=(b * gv).tolist())


class TestGridLookup(unittest.TestCase):
    def test_nodes_and_interpolation(self):
        rng = np.random.default_rng(1)
        u, v = rng.uniform(-0.99, 0.99, 1000), rng.uniform(-0.99, 0.99, 1000)
        x, y = grid_lookup(u, v, linear_map())
        np.testing.assert_allclose(x, 30.0 * u, atol=1e-9)
        np.testing.assert_allclose(y, -20.0 * v, atol=1e-9)

    def test_clipping_and_nan(self):
        x, y = grid_lookup(np.array([2.0, np.nan]), np.array([-3.0, 0.0]), linear_map())
        self.assertAlmostEqual(x[0], 30.0)
        self.assertAlmostEqual(y[0], 20.0)
        self.assertTrue(np.isnan(x[1]) and np.isnan(y[1]))

    def test_uv_convention(self):
        # x_cgr = top-row fraction (ch1 + ch2), y_cgr = left-column fraction (ch1 + ch3)
        u, v = uv_from_cgr(np.array([1.0, 0.5]), np.array([0.5, 1.0]))
        np.testing.assert_allclose(u, [0.0, 1.0])
        np.testing.assert_allclose(v, [1.0, 0.0])

    def test_event_positions_module_still_exports_helpers(self):
        from amstrax.plugins.events.event_positions import grid_lookup as g, uv_from_cgr as f
        self.assertIs(g, grid_lookup)
        self.assertIs(f, uv_from_cgr)


class TestPeakPositions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.st = amstrax.contexts.xams(init_rundb=False)

    def _plugin(self, **cfg):
        st = self.st.new_context()
        st.set_config(cfg)
        return st.get_single_plugin("000001", "peak_positions")

    @staticmethod
    def _peaks():
        dt = [("area", np.float32), ("area_fraction_top", np.float32), ("area_per_channel", np.float32, 5)] + strax.time_fields
        p = np.zeros(3, dtype=dt)
        p["time"], p["endtime"] = [0, 10, 20], [5, 15, 25]
        # top channels 1..4; event 1: ch1 + ch2 = 75 % (top row), ch1 + ch3 = 25 % (left column)
        p["area_per_channel"][0] = [0, 25, 25, 25, 25]
        p["area_per_channel"][1] = [0, 0, 75, 25, 0]
        p["area_per_channel"][2] = [10, 0, 0, 0, 0]           # no top light
        p["area"] = p["area_per_channel"].sum(axis=1)
        p["area_fraction_top"] = p["area_per_channel"][:, 1:].sum(axis=1) / p["area"]
        return p

    def test_no_polynomial(self):
        p = self._plugin()
        names = p.dtype_for("peak_positions").names
        for f in ("x_corr", "y_corr", "r_corr"):
            self.assertNotIn(f, names)
        self.assertNotIn("pos_rec_params", p.takes_config)
        self.assertNotIn("default_reconstruction_algorithm", p.takes_config)

    def test_map_and_gains(self):
        p = self._plugin(pos_rec_map=linear_map(), gain_to_pe_array=list(np.ones(8)))
        r = p.compute(self._peaks())
        np.testing.assert_allclose(r["x_cgr"][:2], [0.5, 0.75])
        np.testing.assert_allclose(r["y_cgr"][:2], [0.5, 0.25])
        # event 1: u = 2 * 0.25 - 1 = -0.5, v = 2 * 0.75 - 1 = 0.5 -> x = -15, y = -10
        self.assertAlmostEqual(float(r["u"][1]), -0.5, places=6)
        self.assertAlmostEqual(float(r["v"][1]), 0.5, places=6)
        self.assertAlmostEqual(float(r["x"][1]), -15.0, places=4)
        self.assertAlmostEqual(float(r["y"][1]), -10.0, places=4)
        self.assertAlmostEqual(float(r["r"][1]), np.hypot(15, 10), places=4)
        self.assertTrue(np.isnan(r["x"][2]))

    def test_nan_without_map_or_gains(self):
        for cfg in (dict(), dict(pos_rec_map=linear_map()), dict(gain_to_pe_array=list(np.ones(8)))):
            r = self._plugin(**cfg).compute(self._peaks())
            self.assertTrue(np.all(np.isnan(r["x"])), cfg.keys())
            self.assertTrue(np.all(np.isnan(r["y"])), cfg.keys())
            self.assertTrue(np.isfinite(r["u"][1]))               # u, v always there


class TestEventPositions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.st = amstrax.contexts.xams(init_rundb=False)
        cls.cls = cls.st._plugin_class_registry["event_positions"]

    @staticmethod
    def _events():
        names = [f"{p}{c}" for p in ("s2_", "alt_s2_") for c in ("x", "y", "x_cgr", "y_cgr")]
        dt = [(n, np.float32) for n in names] + [("drift_time", np.int64)] + strax.time_fields
        ev = np.zeros(3, dtype=dt)
        ev["time"] = [0, 10, 20]
        ev["endtime"] = [5, 15, 25]
        ev["s2_x"] = [1.0, -15.0, np.nan]
        ev["s2_y"] = [2.0, -10.0, np.nan]
        ev["s2_x_cgr"] = [0.5, 0.75, np.nan]
        ev["s2_y_cgr"] = [0.5, 0.25, 0.5]
        for c in ("x", "y", "x_cgr", "y_cgr"):
            ev[f"alt_s2_{c}"] = np.nan
        ev["drift_time"] = [3000, 20000, 39500]
        return ev

    def _plugin(self):
        p = self.cls()
        p.run_id = "999999"
        p.config = dict(drift_time_gate=3000, drift_time_cathode=39500, gate_cathode_distance=50.5)
        p.dtype = strax.to_numpy_dtype(p.infer_dtype())
        return p

    def test_fields(self):
        names = self._plugin().dtype.names
        for f in ("x", "y", "r", "phi", "u", "v", "z", "alt_s2_x", "alt_s2_y", "alt_s2_r", "alt_s2_phi"):
            self.assertIn(f, names)

    def test_positions_from_event_basics(self):
        p = self._plugin()
        ev = self._events()
        r = strax.dict_to_rec(p.compute(ev), dtype=p.dtype)
        np.testing.assert_allclose(r["x"], ev["s2_x"])
        np.testing.assert_allclose(r["y"], ev["s2_y"])
        self.assertAlmostEqual(float(r["r"][1]), np.hypot(15, 10), places=4)
        self.assertAlmostEqual(float(r["phi"][1]), np.arctan2(-10, -15), places=5)
        self.assertAlmostEqual(float(r["u"][1]), -0.5, places=6)
        self.assertAlmostEqual(float(r["v"][1]), 0.5, places=6)
        self.assertTrue(np.all(np.isnan(r["alt_s2_x"])))
        np.testing.assert_allclose(r["z"], [0.0, -50.5 * 17000 / 36500, -50.5], rtol=1e-6)


class TestNullCorrectionRange(unittest.TestCase):
    """A run range may hold null on purpose (e.g. no approved gains yet); a run in no range is an error."""

    def setUp(self):
        self.cfg = amstrax.XAMSConfig(default=None)
        self.data = {"000000-007600": None, "007601-007723": [1.0, 2.0], "007724-999999": None}

    def test_value_null_and_missing(self):
        self.assertEqual(self.cfg.find_correction_value(self.data, 7700, "x_v1.json"), [1.0, 2.0])
        self.assertIsNone(self.cfg.find_correction_value(self.data, 7800, "x_v1.json"))
        self.assertIsNone(self.cfg.find_correction_value(self.data, 100, "x_v1.json"))
        with self.assertRaises(ValueError):
            self.cfg.find_correction_value({"007601-007723": [1.0]}, 7800, "x_v1.json")


if __name__ == "__main__":
    unittest.main()
