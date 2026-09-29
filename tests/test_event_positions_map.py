import unittest

import numpy as np
import strax

import amstrax
from amstrax.plugins.events.event_positions_map import grid_lookup, uv_from_cgr


def linear_map(n=41, a=30.0, b=-20.0):
    """synthetic map: x = a * u, y = b * v (bilinear interpolation reproduces it exactly)"""
    g = np.linspace(-1.0, 1.0, n)
    gu, gv = np.meshgrid(g, g, indexing="ij")
    return dict(uv_min=-1.0, step=float(g[1] - g[0]), n=n, x=(a * gu).tolist(), y=(b * gv).tolist())


class TestGridLookup(unittest.TestCase):
    def test_nodes_and_interpolation(self):
        m = linear_map()
        rng = np.random.default_rng(1)
        u, v = rng.uniform(-0.99, 0.99, 1000), rng.uniform(-0.99, 0.99, 1000)
        x, y = grid_lookup(u, v, m)
        np.testing.assert_allclose(x, 30.0 * u, atol=1e-9)
        np.testing.assert_allclose(y, -20.0 * v, atol=1e-9)

    def test_clipping_and_nan(self):
        m = linear_map()
        x, y = grid_lookup(np.array([2.0, np.nan]), np.array([-3.0, 0.0]), m)
        self.assertAlmostEqual(x[0], 30.0)
        self.assertAlmostEqual(y[0], 20.0)
        self.assertTrue(np.isnan(x[1]) and np.isnan(y[1]))

    def test_uv_convention(self):
        # x_cgr = top-row fraction (ch1 + ch2), y_cgr = left-column fraction (ch1 + ch3)
        u, v = uv_from_cgr(np.array([1.0, 0.5]), np.array([0.5, 1.0]))
        np.testing.assert_allclose(u, [0.0, 1.0])
        np.testing.assert_allclose(v, [1.0, 0.0])


class TestEventPositionsMapPlugin(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.st = amstrax.contexts.xams(init_rundb=False)
        cls.run_id = "999999"

    def _events(self):
        dt = [(n, np.float32) for n in ("s2_x_cgr", "s2_y_cgr", "alt_s2_x_cgr", "alt_s2_y_cgr")] + strax.time_fields
        ev = np.zeros(3, dtype=dt)
        ev["time"] = [0, 10, 20]
        ev["endtime"] = [5, 15, 25]
        ev["s2_x_cgr"] = [0.5, 0.75, np.nan]
        ev["s2_y_cgr"] = [0.5, 0.25, 0.5]
        ev["alt_s2_x_cgr"] = np.nan
        ev["alt_s2_y_cgr"] = np.nan
        return ev

    def test_registered(self):
        self.assertIn("event_positions_map", self.st._plugin_class_registry)

    def test_compute_with_and_without_map(self):
        cls = self.st._plugin_class_registry["event_positions_map"]
        p = cls()
        p.run_id = self.run_id
        p.config = {"pos_rec_map": None}
        p.dtype = strax.to_numpy_dtype(p.infer_dtype())
        r = p.compute(self._events())
        self.assertTrue(np.all(np.isnan(r["s2_x_map"])))
        p = cls()
        p.run_id = self.run_id
        p.config = {"pos_rec_map": linear_map()}
        p.dtype = strax.to_numpy_dtype(p.infer_dtype())
        r = p.compute(self._events())
        # event 1: u = 2 * 0.25 - 1 = -0.5, v = 2 * 0.75 - 1 = 0.5 -> x = -15, y = -10
        self.assertAlmostEqual(float(r["s2_x_map"][1]), -15.0, places=4)
        self.assertAlmostEqual(float(r["s2_y_map"][1]), -10.0, places=4)
        self.assertAlmostEqual(float(r["s2_r_map"][1]), np.hypot(15, 10), places=4)
        self.assertTrue(np.isnan(r["s2_x_map"][2]))
        self.assertTrue(np.all(np.isnan(r["alt_s2_x_map"])))


if __name__ == "__main__":
    unittest.main()
