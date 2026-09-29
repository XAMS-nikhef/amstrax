import unittest

import numpy as np
import strax

import amstrax
from amstrax.plugins.events.event_positions import grid_lookup, uv_from_cgr


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


class TestEventPositions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.st = amstrax.contexts.xams(init_rundb=False)
        cls.cls = cls.st._plugin_class_registry["event_positions"]

    def _events(self):
        names = [f"{p}{c}" for p in ("s2_", "alt_s2_") for c in ("x_cgr", "y_cgr", "x_corr", "y_corr")]
        dt = [(n, np.float32) for n in names] + [("drift_time", np.int64)] + strax.time_fields
        ev = np.zeros(3, dtype=dt)
        ev["time"] = [0, 10, 20]
        ev["endtime"] = [5, 15, 25]
        ev["s2_x_cgr"] = [0.5, 0.75, np.nan]
        ev["s2_y_cgr"] = [0.5, 0.25, 0.5]
        ev["s2_x_corr"] = [1.0, 2.0, 3.0]
        ev["s2_y_corr"] = [-1.0, -2.0, -3.0]
        for c in ("x_cgr", "y_cgr", "x_corr", "y_corr"):
            ev[f"alt_s2_{c}"] = np.nan
        ev["drift_time"] = [3000, 20000, 39500]
        return ev

    def _plugin(self, **cfg):
        p = self.cls()
        p.run_id = "999999"
        p.config = dict(default_reconstruction_algorithm="corr", pos_rec_map=None, drift_time_gate=3000,
                        drift_time_cathode=39500, gate_cathode_distance=50.5)
        p.config.update(cfg)
        p.dtype = strax.to_numpy_dtype(p.infer_dtype())
        return p

    def test_fields(self):
        names = self._plugin().dtype.names
        for f in ("x", "y", "r", "phi", "u", "v", "z", "alt_s2_x", "alt_s2_y", "alt_s2_r", "alt_s2_phi"):
            self.assertIn(f, names)

    def test_without_map_is_unchanged(self):
        ev = self._events()
        r = strax.dict_to_rec(self._plugin().compute(ev), dtype=self._plugin().dtype)
        np.testing.assert_allclose(r["x"], ev["s2_x_corr"])
        np.testing.assert_allclose(r["y"], ev["s2_y_corr"])
        np.testing.assert_allclose(r["z"], [0.0, -50.5 * 17000 / 36500, -50.5], rtol=1e-6)

    def test_with_map(self):
        p = self._plugin(pos_rec_map=linear_map())
        r = strax.dict_to_rec(p.compute(self._events()), dtype=p.dtype)
        # event 1: u = 2 * 0.25 - 1 = -0.5, v = 2 * 0.75 - 1 = 0.5 -> x = -15, y = -10
        self.assertAlmostEqual(float(r["u"][1]), -0.5, places=6)
        self.assertAlmostEqual(float(r["v"][1]), 0.5, places=6)
        self.assertAlmostEqual(float(r["x"][1]), -15.0, places=4)
        self.assertAlmostEqual(float(r["y"][1]), -10.0, places=4)
        self.assertAlmostEqual(float(r["r"][1]), np.hypot(15, 10), places=4)
        self.assertAlmostEqual(float(r["phi"][1]), np.arctan2(-10, -15), places=5)
        self.assertTrue(np.isnan(r["x"][2]))
        self.assertTrue(np.all(np.isnan(r["alt_s2_x"])))


if __name__ == "__main__":
    unittest.main()
