import numba
import numpy as np
import strax
export, __all__ = strax.exporter()

@export
@strax.takes_config(
    strax.Option('s1_max_width', default=120,
                 help="Maximum (IQR) width of S1s"),
    strax.Option('s1_min_area', default=10,
                 help="Minimum area (PE) for S1s"),
    strax.Option('s2_min_area', default=10,
                 help="Minimum area (PE) for S2s"),
    strax.Option('s2_min_width', default=250,
                 help="Minimum width for S2s"))
class PeakClassification(strax.Plugin):
    rechunk_on_save = False
    __version__ = '1.0'
    depends_on = ('peaks')
    dtype = [
        ('type', np.int8, 'Classification of the peak.'),
        ('time', np.int64, 'Start time of the peak (ns since unix epoch)'),
        ('endtime', np.int64, 'End time of the peak (ns since unix epoch)')
    ]
    parallel = True

    def compute(self, peaks):

        p = peaks
        range_50p_area = p['width'][:, 5]
    
        r = np.zeros(len(p), dtype=self.dtype)

        # filter to see if the bottom PMT had a hit
        pmt1_filter = (p['area_per_channel'][:,0] != 0)
        # filter to determine whether one of the sectors in the top PMT had a hit
        pmt2_filter = np.array([p['area_per_channel'][:,i] != 0 for i in range(1, 5)]).any(axis=0)
        both_pmts_filter = pmt1_filter & pmt2_filter

        is_s1 = p['area'] >= self.config['s1_min_area']
        is_s1 &= range_50p_area < self.config['s1_max_width']
        # is_s1 &= both_pmts_filter
        r['type'][is_s1] = 1

        is_s2 = p['area'] > self.config['s2_min_area']
        is_s2 &= range_50p_area > self.config['s2_min_width']
        # is_s2 &= both_pmts_filter
        r['type'][is_s2] = 2

        print(f"We found {np.sum(is_s1)} S1s and {np.sum(is_s2)} S2s.")

        for q in ['time']:
            r[q] = p[q]

        r['endtime'] = strax.endtime(p)

        return r