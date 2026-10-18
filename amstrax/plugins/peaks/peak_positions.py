import numpy as np
import strax
from immutabledict import immutabledict
import amstrax

export, __all__ = strax.exporter()
__all__ += ["uv_from_cgr", "grid_lookup"]


def uv_from_cgr(x_cgr, y_cgr):
    """Light-sharing asymmetries of a peak on the 2x2 top array.

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


def positions_available(pos_rec_map, gain_to_pe_array):
    """The map was made with (u, v) from per-anode gains in PE: apply it only when both are set for the run."""
    return pos_rec_map is not None and gain_to_pe_array is not None


@export
class PeakPositions(strax.Plugin):
    """
    Position of every peak from the light pattern on the 2x2 top array (position reconstruction v2).

    x_cgr / y_cgr are the top-row (ch1 + ch2) and left-column (ch1 + ch3) fractions of the top area, u, v the
    asymmetries made from them, and x, y, r the position from the (u, v) -> (x, y) map ``pos_rec_map``
    (e.g. ``pos_rec_map_v1.json``). Without a map or without gains for the run, x, y, r are NaN: there is no
    fallback (the old cubic polynomial, ``pos_rec_params`` / ``x_corr``, was removed in 2.0.0).
    """
    depends_on = ('peaks', 'peak_basics')
    rechunk_on_save = False
    __version__ = '2.0.0'
    dtype = [
        ('x_cgr', np.float32,
         'Top-row fraction (ch1 + ch2) of the top area'),
        ('y_cgr', np.float32,
         'Left-column fraction (ch1 + ch3) of the top area'),
        ('u', np.float32,
         'Light-sharing asymmetry left - right (top array)'),
        ('v', np.float32,
         'Light-sharing asymmetry top - bottom (top array)'),
        ('x', np.float32,
         'x-position from the pos_rec_map [mm]'),
        ('y', np.float32,
         'y-position from the pos_rec_map [mm]'),
        ('r', np.float32,
         'r from the pos_rec_map [mm]'),
        ('time', np.int64, 'Start time of the peak (ns since unix epoch)'),
        ('endtime', np.int64, 'End time of the peak (ns since unix epoch)')
    ]

    channel_map = amstrax.XAMSConfig(
        default="rundoc://?path=xams_bookkeeping.channel_map,daq_config.channel_map&fallback=xams_default",
        type=immutabledict,
        track=False,
        help="Map of channel groups loaded from rundoc xams_bookkeeping.channel_map",
    )
    pos_rec_map = amstrax.XAMSConfig(
        default=None,
        help="(u, v) -> (x, y) lookup grid (dict with uv_min, step, n, x, y; "
             "file://pos_rec_map?filename=...). Without it x, y, r are NaN.")
    gain_to_pe_array = amstrax.XAMSConfig(
        default=None,
        help="Gains of the run (same option as Peaks). The map needs (u, v) in PE: without gains x, y, r are NaN.")

    def compute(self, peaks):

        result = np.empty(len(peaks), dtype=self.dtype)
        result['time'] = peaks['time']
        result['endtime'] = peaks['endtime']

        top_sum = peaks['area']*peaks['area_fraction_top']

        # top is going to be (1,4)
        top_map = self.channel_map['top']
        top_indeces = np.arange(top_map[0], top_map[1]+1)

        # a bit convoluted, but necessary to avoid division by zero
        numerator_12 = peaks['area_per_channel'][:,top_indeces[0]]+peaks['area_per_channel'][:,top_indeces[1]]
        numerator_13 = peaks['area_per_channel'][:,top_indeces[0]]+peaks['area_per_channel'][:,top_indeces[2]]
        denominator = top_sum[:]

        mask = (denominator != 0)

        # f_12 is the fraction of the top area in the top row
        # if top is (1,4) this means that we take the area of channel 1 and 2
        # f_13 is the fraction of the top area in the first column
        # if top is (1,4) this means that we take the area of channel 1 and 3
        f_12 = np.full(len(peaks), np.nan, dtype=np.float32)
        f_13 = np.full(len(peaks), np.nan, dtype=np.float32)

        f_12[mask] = numerator_12[mask]/denominator[mask]
        f_13[mask] = numerator_13[mask]/denominator[mask]

        result['x_cgr'] = f_12
        result['y_cgr'] = f_13
        u, v = uv_from_cgr(f_12.astype(np.float64), f_13.astype(np.float64))
        result['u'], result['v'] = u, v

        if positions_available(self.pos_rec_map, self.gain_to_pe_array):
            x, y = grid_lookup(u, v, self.pos_rec_map)
        else:
            x = y = np.full(len(peaks), np.nan)
        result['x'], result['y'] = x, y
        result['r'] = np.sqrt(x ** 2 + y ** 2)

        return result
