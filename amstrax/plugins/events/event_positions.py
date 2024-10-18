import numpy as np
import amstrax


DEFAULT_POSREC_ALGO = 'corr'

import strax

export, __all__ = strax.exporter()


@export
@strax.takes_config(
    strax.Option('default_reconstruction_algorithm',
                 default=DEFAULT_POSREC_ALGO,
                 help="default reconstruction algorithm that provides (x,y)"),
    strax.Option('drift_time_gate',
                 default=3000,
                 help='Drift time belonging to the gate in ns'),
    strax.Option('drift_time_cathode',
                 default=39500,
                 help='Drift time belonging to the cathode in ns'),
    strax.Option('gate_cathode_distance',
                 default=505,
                 help='Distance between gate and cathode in mm'),    
)

class EventPositions(strax.Plugin):
    """
    Computes the observed and corrected position for the main S1/S2
    pairs in an event. For XENONnT data, it returns the FDC corrected
    positions of the default_reconstruction_algorithm. In case the fdc_map
    is given as a file (not through CMT), then the coordinate system
    should be given as (x, y, z), not (x, y, drift_time).
    """

    depends_on = ('event_basics',)

    __version__ = '1.1.20'


    def infer_dtype(self):
        dtype = []
        for j in 'x y r'.split():
            comment = f'Main interaction {j}-position'
            dtype += [(j, np.float32, comment)]
            for s_i in [2, ]:
                comment = f'Alternative S{s_i} interaction (rel. main S{3 - s_i}) {j}-position'
                field = f'alt_s{s_i}_{j}'
                dtype += [(field, np.float32, comment)]

        dtype += [('z', np.float32,
         'Interaction depth z-position')]
        
        return dtype + strax.time_fields

    def setup(self):

        self.default_reconstruction_algorithm = self.config['default_reconstruction_algorithm']
        self.drift_time_gate = self.config['drift_time_gate']
        self.drift_time_cathode = self.config['drift_time_cathode']
        self.gate_cathode_distance = self.config['gate_cathode_distance']
        
    def compute(self, events):

        result = {'time': events['time'],
                  'endtime': strax.endtime(events)}

        # cope the values from the S2s
        algo = self.default_reconstruction_algorithm

        for j in 'x y'.split():
            field = f'{j}'
            result[j] = events[f's2_{j}_{algo}']

            field = f'alt_s2_{j}'
            result[field] = events[f'alt_s2_{j}_{algo}']

        result['r'] = np.sqrt(result['x'] ** 2 + result['y'] ** 2)
        result['alt_s2_r'] = np.sqrt(result['alt_s2_x'] ** 2 + result['alt_s2_y'] ** 2)

        slope = -self.gate_cathode_distance / (self.drift_time_cathode - self.drift_time_gate)
        result['z'] = slope * (events['drift_time'] - self.drift_time_gate)

        return result
