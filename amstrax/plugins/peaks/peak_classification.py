import numba
import numpy as np
import strax

@export
@strax.takes_config(
    strax.Option('s1_max_width', default=60,
                 help="Maximum (IQR) width of S1s"),
    strax.Option('s1_min_area', default=4,
                 help="Minimum area (PE) for S1s"),
    strax.Option('s2_min_area', default=4,
                 help="Minimum area (PE) for S2s"),
    strax.Option('s2_min_width', default=100,
                 help="Minimum width for S2s"))
class PeakClassification(strax.Plugin):
    rechunk_on_save = False
    __version__ = '0.0.4'
    depends_on = ('peak_basics')
    dtype = [
        ('type', np.int8, 'Classification of the peak.'),
        ('time', np.int64, 'Start time of the peak (ns since unix epoch)'),
        ('endtime', np.int64, 'End time of the peak (ns since unix epoch)')
    ]
    parallel = True

    def compute(self, peaks):
        p = peaks
        r = np.zeros(len(p), dtype=self.dtype)

        is_s1 = p['area'] >= self.config['s1_min_area']
        is_s1 &= p['range_50p_area'] < self.config['s1_max_width']
        r['type'][is_s1] = 1

        is_s2 = p['area'] > self.config['s2_min_area']
        is_s2 &= p['range_50p_area'] > self.config['s2_min_width']
        r['type'][is_s2] = 2

        for q in ['time', 'endtime']:
            r[q] = p[q]

        return r