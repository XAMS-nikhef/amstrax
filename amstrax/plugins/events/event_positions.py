import numpy as np
import amstrax


DEFAULT_POSREC_ALGO = 'corr'

import strax

export, __all__ = strax.exporter()


def uv_from_cgr(x_cgr, y_cgr):
    """Light-sharing asymmetries of an S2 on the 2x2 top array.

    ``x_cgr`` holds the top-row fraction (ch1 + ch2) and ``y_cgr`` the left-column fraction (ch1 + ch3)
    of the top area (see PeakPositions), so u = left - right and v = top - bottom, both in [-1, 1].
    """
    return 2 * y_cgr - 1, 2 * x_cgr - 1


def grid_lookup(u, v, grid):
    """Bilinear interpolation of a (u, v) -> (x, y) map at (u, v); (u, v) outside the grid are clipped.

    ``grid`` is the value of the correction ``pos_rec_map``: dict with ``uv_min``, ``step``, ``n`` and the
    (n, n) arrays ``x``, ``y`` (position in mm at u = uv_min + i * step, v = uv_min + j * step).
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
class EventPositions(strax.Plugin):
    """
    Position of the main S2 / S1 pair and the alternative S2: x, y, r, phi and z.

    (x, y) come from the (u, v) -> (x, y) map ``pos_rec_map`` when it is set (position reconstruction v2:
    gate-mesh holes + light-response model of the 2x2 top array, e.g. ``pos_rec_map_v0.json`` in
    ``_global_v4``). Without a map they are the S2 positions of ``default_reconstruction_algorithm`` from
    event_basics (``s2_x_corr``: one cubic polynomial per axis from ``pos_rec_params``), as before.
    u, v are the light-sharing asymmetries of the main S2 (u = left - right, v = top - bottom).
    """

    depends_on = ('event_basics',)

    __version__ = '1.2.0'

    default_reconstruction_algorithm = amstrax.XAMSConfig(
        default=DEFAULT_POSREC_ALGO,
        help="reconstruction algorithm that provides (x, y) when no pos_rec_map is set")
    pos_rec_map = amstrax.XAMSConfig(
        default=None,
        help="(u, v) -> (x, y) lookup grid (dict with uv_min, step, n, x, y; "
             "file://pos_rec_map?filename=...). If set, it provides x, y.")
    drift_time_gate = amstrax.XAMSConfig(
        default=3000,
        help='Drift time belonging to the gate in ns')
    drift_time_cathode = amstrax.XAMSConfig(
        default=39500,
        help='Drift time belonging to the cathode in ns')
    gate_cathode_distance = amstrax.XAMSConfig(
        default=50.5,
        help='Distance between gate and cathode in mm')

    def infer_dtype(self):
        dtype = []
        for prefix, name in (('', 'Main'), ('alt_s2_', 'Alternative S2')):
            dtype += [
                (f'{prefix}x', np.float32, f'{name} interaction x-position [mm]'),
                (f'{prefix}y', np.float32, f'{name} interaction y-position [mm]'),
                (f'{prefix}r', np.float32, f'{name} interaction r [mm]'),
                (f'{prefix}phi', np.float32, f'{name} interaction azimuth atan2(y, x) [rad]'),
            ]
        dtype += [
            ('u', np.float32, 'Main S2 light-sharing asymmetry left - right (top array)'),
            ('v', np.float32, 'Main S2 light-sharing asymmetry top - bottom (top array)'),
            ('z', np.float32, 'Interaction depth z-position [mm] (0 at the gate)'),
        ]
        return dtype + strax.time_fields

    def compute(self, events):

        result = {'time': events['time'],
                  'endtime': strax.endtime(events)}

        algo = self.default_reconstruction_algorithm
        for prefix, peak in (('', 's2_'), ('alt_s2_', 'alt_s2_')):
            u, v = uv_from_cgr(events[f'{peak}x_cgr'].astype(np.float64),
                               events[f'{peak}y_cgr'].astype(np.float64))
            if self.pos_rec_map is not None:
                x, y = grid_lookup(u, v, self.pos_rec_map)
            else:
                x, y = events[f'{peak}x_{algo}'], events[f'{peak}y_{algo}']
            result[f'{prefix}x'] = x
            result[f'{prefix}y'] = y
            result[f'{prefix}r'] = np.sqrt(x ** 2 + y ** 2)
            result[f'{prefix}phi'] = np.arctan2(y, x)
            if prefix == '':
                result['u'], result['v'] = u, v

        slope = -self.gate_cathode_distance / (self.drift_time_cathode - self.drift_time_gate)
        result['z'] = slope * (events['drift_time'] - self.drift_time_gate)

        return result
