import numpy as np
import amstrax


DEFAULT_POSREC_ALGO = 'corr'

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
    strax.Option('chebyshev_coefficients',
                 default=[-20.610498575132848, -25.011741532892245, 1.5014394901565762, -1.9897069599813497, 0.9003989634988041, -0.9360882311673664, 0.3205446171520987, -0.31507601715307304, -0.11157849428915143, 0.06805340886247688, -0.29731923422516193, 0.20130571476398995, -0.27433679192849914, 0.14124101107660128, -0.1423385040662297, 0.02543495639467936, -0.003942936477649859, -0.06027758452189014, 0.06644908962655806, -0.07892464092207617, 0.06395035504495723, -0.03346169811390071, 0.015301990300138332, 0.02074806803955231, -0.023897590505947888, 0.05290043645359529, -0.04171261919713702, 0.04635244560925833, -0.023740447547251964, 0.017317746198669496, 0.008129269124318644, -0.015813652571722153, 0.03280797293563488, -0.0319597933948091, 0.033244932242107145, -0.02499733182975444, 0.01804703343996559, -0.003734756903266918, -0.010310284576649408, 0.014162576952560277, -0.02107880499178727, 0.02353782385611963, -0.023161397137303634, 0.01638318547950904, -0.009949657230580528, 0.0028289370052051086, 0.00819342916648231, -0.010233170352421222, 0.01760163054647025, -0.01272257423130424, 0.014630281086449799],
                 help="coefficients for the drift time to z fit"),
    strax.Option('chebyshev_interval',
                 default=[1219.0999755859375, 39380.8984375],
                 help="interval of the chebyshev fit"),
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

    __version__ = '1.1.16'


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

        self.electron_drift_velocity = self.config['electron_drift_velocity']
        self.electron_drift_time_gate = self.config['electron_drift_time_gate']
        self.default_reconstruction_algorithm = self.config['default_reconstruction_algorithm']
        self.chebyshev_coefficients = np.array(self.config['chebyshev_coefficients'])
        self.chebyshev_interval = self.config['chebyshev_interval']
        
        self.coordinate_scales = [1., 1., - self.electron_drift_velocity]
        # self.map = self.fdc_map

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

        # determine z using a chebyshev polynomial fit
        p = np.polynomial.chebyshev.Chebyshev(self.chebyshev_coefficients) 
        
        # normalize drift times such that the interval is [-1, 1]
        drift_times_normalized = 2 * (events['drift_time'] - self.chebyshev_interval[0]) / (self.config['chebyshev_interval'][1] - self.chebyshev_interval[0]) - 1

        result['z'] = p(drift_times_normalized)

        return result
