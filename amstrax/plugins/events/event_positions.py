import numpy as np
import amstrax


DEFAULT_POSREC_ALGO = 'cgr'

import strax

export, __all__ = strax.exporter()


@export
@strax.takes_config(
    strax.Option('electron_drift_velocity',
                 default=0.0000016,
                 help='Vertical electron drift velocity in cm/ns (1e4 m/ms)'),
    strax.Option('electron_drift_time_gate',
                 default=1,
                 help='Electron drift time from the gate in ns'),
    strax.Option('default_reconstruction_algorithm',
                 default=DEFAULT_POSREC_ALGO,
                 help="default reconstruction algorithm that provides (x,y)"),
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

    __version__ = '0.3.0'


    def infer_dtype(self):
        dtype = []
        for j in 'x y r'.split():
            comment = f'Main interaction {j}-position'
            dtype += [(j, np.float32, comment)]
            for s_i in [2, ]:
                comment = f'Alternative S{s_i} interaction (rel. main S{3 - s_i}) {j}-position'
                field = f'alt_s{s_i}_{j}'
                dtype += [(field, np.float32, comment)]

        return dtype + strax.time_fields

    def setup(self):

        self.electron_drift_velocity = self.config['electron_drift_velocity']
        self.electron_drift_time_gate = self.config['electron_drift_time_gate']
        self.default_reconstruction_algorithm = self.config['default_reconstruction_algorithm']
        
        self.coordinate_scales = [1., 1., - self.electron_drift_velocity]
        # self.map = self.fdc_map

    def compute(self, events):

        result = {'time': events['time'],
                  'endtime': strax.endtime(events)}

        algo = self.default_reconstruction_algorithm

        for j in 'x y r'.split():
            field = f's2_{j}'
            result[j] = events[f's2_{j}_{algo}']

            field = f'alt_s2_{j}'
            result[field] = events[f'alt_s2_{j}_{algo}']

        return result