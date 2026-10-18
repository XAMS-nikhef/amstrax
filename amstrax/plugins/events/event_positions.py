import numpy as np
import amstrax
import strax

# kept here as well: older code imports them from this module
from amstrax.plugins.peaks.peak_positions import grid_lookup, uv_from_cgr  # noqa: F401

export, __all__ = strax.exporter()


@export
class EventPositions(strax.Plugin):
    """
    Position of the main S2 / S1 pair and the alternative S2: x, y, r, phi and z.

    (x, y) are the S2 positions of event_basics (s2_x, s2_y, alt_s2_x, alt_s2_y), which come from the
    (u, v) -> (x, y) map ``pos_rec_map`` applied in peak_positions (position reconstruction v2). Without a map or
    without gains for the run they are NaN; there is no fallback (the old cubic polynomial was removed in 2.0.0).
    u, v are the light-sharing asymmetries of the main S2 (u = left - right, v = top - bottom).
    """

    depends_on = ('event_basics',)

    __version__ = '2.0.0'

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

        for prefix, peak in (('', 's2_'), ('alt_s2_', 'alt_s2_')):
            x, y = events[f'{peak}x'], events[f'{peak}y']
            result[f'{prefix}x'] = x
            result[f'{prefix}y'] = y
            result[f'{prefix}r'] = np.sqrt(x ** 2 + y ** 2)
            result[f'{prefix}phi'] = np.arctan2(y, x)

        result['u'], result['v'] = uv_from_cgr(events['s2_x_cgr'].astype(np.float64),
                                               events['s2_y_cgr'].astype(np.float64))

        slope = -self.gate_cathode_distance / (self.drift_time_cathode - self.drift_time_gate)
        result['z'] = slope * (events['drift_time'] - self.drift_time_gate)

        return result
