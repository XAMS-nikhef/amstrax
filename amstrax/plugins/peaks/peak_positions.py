import numba
import numpy as np
import strax
export, __all__ = strax.exporter()


@export
class PeakPositions(strax.Plugin):
    depends_on = ('peaks', 'peak_classification')
    rechunk_on_save = False
    __version__ = '0.0.34'  # .33 for LNLIKE
    dtype = [
        ('x_lpf', np.float32,
         'Interaction x-position'),
        ('y_lpf', np.float32,
         'Interaction y-position'),
        ('r', np.float32,
         'radial distance'),
        ('time', np.int64, 'Start time of the peak (ns since unix epoch)'),
        ('endtime', np.int64, 'End time of the peak (ns since unix epoch)')
    ]


    def compute(self, peaks):

        result = np.empty(len(peaks), dtype=self.dtype)
        result['time'] = peaks['time']
        result['endtime'] = peaks['endtime']

        return result
