import numpy as np
import strax

import amstrax

export, __all__ = strax.exporter()


def uv_from_cgr(x_cgr, y_cgr):
    """Light-sharing asymmetries of an S2 on the 2x2 top array.

    ``x_cgr`` holds the top-row fraction (ch1 + ch2) and ``y_cgr`` the left-column fraction (ch1 + ch3)
    of the top area (see PeakPositions), so u = left - right and v = top - bottom, both in [-1, 1].
    """
    return 2 * y_cgr - 1, 2 * x_cgr - 1


def grid_lookup(u, v, grid):
    """Bilinear interpolation of the map grid at (u, v); (u, v) outside [uv_min, -uv_min] are clipped.

    ``grid`` is the correction value: dict with ``uv_min``, ``step``, ``n`` and the (n, n) arrays ``x``, ``y``
    (position in mm at u = uv_min + i * step, v = uv_min + j * step).
    """
    u0, step = float(grid["uv_min"]), float(grid["step"])
    gx, gy = np.asarray(grid["x"], dtype=np.float64), np.asarray(grid["y"], dtype=np.float64)
    n = gx.shape[0]
    x = np.full(len(u), np.nan, dtype=np.float64)
    y = np.full(len(u), np.nan, dtype=np.float64)
    ok = np.isfinite(u) & np.isfinite(v)
    fu = (np.clip(u[ok], u0, -u0) - u0) / step
    fv = (np.clip(v[ok], u0, -u0) - u0) / step
    i = np.clip(np.floor(fu).astype(np.int64), 0, n - 2)
    j = np.clip(np.floor(fv).astype(np.int64), 0, n - 2)
    t, s = fu - i, fv - j
    for out, g in ((x, gx), (y, gy)):
        out[ok] = ((1 - t) * (1 - s) * g[i, j] + t * (1 - s) * g[i + 1, j]
                   + (1 - t) * s * g[i, j + 1] + t * s * g[i + 1, j + 1])
    return x, y


@export
class EventPositionsMap(strax.Plugin):
    """(x, y) of the main and alternative S2 from a (u, v) -> (x, y) lookup map.

    The map (correction ``pos_rec_map``, e.g. ``pos_rec_map_v0.json``) is made from the gate-mesh hole pattern
    and a light-response model of the 2x2 top array (SR-2026.2 analysis, position reconstruction v2).
    It is a separate data type so that existing products (peak_positions, event_positions, event_info)
    keep their lineage; load it together with event_info: ``st.get_array(run, ("event_info", "event_positions_map"))``.
    Without a map (default None) the positions are NaN.
    """

    __version__ = "0.1.0"
    depends_on = ("event_basics",)
    provides = "event_positions_map"

    pos_rec_map = amstrax.XAMSConfig(
        default=None,
        help="(u, v) -> (x, y) lookup grid: dict with uv_min, step, n, x, y (file://pos_rec_map?filename=...)",
    )

    def infer_dtype(self):
        dtype = []
        for prefix, name in (("", "Main"), ("alt_", "Alternative")):
            dtype += [
                (f"{prefix}s2_x_map", np.float32, f"{name} S2 x-position from the (u, v) map [mm]"),
                (f"{prefix}s2_y_map", np.float32, f"{name} S2 y-position from the (u, v) map [mm]"),
                (f"{prefix}s2_r_map", np.float32, f"{name} S2 r from the (u, v) map [mm]"),
            ]
        return dtype + strax.time_fields

    def compute(self, events):
        result = np.zeros(len(events), dtype=self.dtype)
        result["time"] = events["time"]
        result["endtime"] = strax.endtime(events)
        for prefix in ("", "alt_"):
            u, v = uv_from_cgr(events[f"{prefix}s2_x_cgr"].astype(np.float64),
                               events[f"{prefix}s2_y_cgr"].astype(np.float64))
            if self.pos_rec_map is None:
                x = y = np.full(len(events), np.nan)
            else:
                x, y = grid_lookup(u, v, self.pos_rec_map)
            result[f"{prefix}s2_x_map"] = x
            result[f"{prefix}s2_y_map"] = y
            result[f"{prefix}s2_r_map"] = np.sqrt(x ** 2 + y ** 2)
        return result
