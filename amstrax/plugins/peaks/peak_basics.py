import numba
import numpy as np
import strax
export, __all__ = strax.exporter()

# For n_competing, which is temporarily added to PeakBasics	
@export
@strax.takes_config(
    strax.Option('min_area_fraction', default=0.5,
                 help='The area of competing peaks must be at least '
                      'this fraction of that of the considered peak'),
    strax.Option('nearby_window', default=int(1e6),
                 help='Peaks starting within this time window (on either side)'
                      'in ns count as nearby.'),
)
class PeakBasics(strax.Plugin):
    provides = ('peak_basics',)
    depends_on = ('peaks')
    data_kind = ('peaks')

    parallel = 'False'
    rechunk_on_save = False
    __version__ = '0.1.7'
    dtype = [
        (('Start time of the peak (ns since unix epoch)',
          'time'), np.int64),
        (('End time of the peak (ns since unix epoch)',
          'endtime'), np.int64),
        (('Peak integral in PE',
          'area'), np.float32),
        (('Number of PMTs contributing to the peak',
          'n_channels'), np.int16),
        (('PMT number which contributes the most PE',
          'max_pmt'), np.int16),
        (('Area of signal in the largest-contributing PMT (PE)',
          'max_pmt_area'), np.int32),
        (('Width (in ns) of the central 50% area of the peak',
          'range_50p_area'), np.float32),
        (('Fraction of area seen by the top array',
          'area_fraction_top'), np.float32),
        (('Length of the peak waveform in samples',
          'length'), np.int32),
        (('Time resolution of the peak waveform in ns',
          'dt'), np.int16),
        ('n_competing', np.int32,  # temporarily due to chunking issues
         'Number of nearby larger or slightly smaller peaks')
    ]

    def compute(self, peaks):
        p = peaks
        p = strax.sort_by_time(p)
        r = np.zeros(len(p), self.dtype)
        for q in 'time length dt area'.split():
            r[q] = p[q]
        r['endtime'] = p['time'] + p['dt'] * p['length']
        r['n_channels'] = (p['area_per_channel'] > 0).sum(axis=1)
        r['range_50p_area'] = p['width'][:, 5]
        r['max_pmt'] = np.argmax(p['area_per_channel'], axis=1)
        r['max_pmt_area'] = np.max(p['area_per_channel'], axis=1)

        # area_top = p['area_per_channel'][:, :8].sum(axis=1)
        area_top = p['area_per_channel'][:, 1:2].sum(axis=1)  # top pmt in ch 1
        # Negative-area peaks get 0 AFT - TODO why not NaN?
        m = p['area'] > 0
        r['area_fraction_top'][m] = area_top[m] / p['area'][m]
        # n_competing temporarily due to chunking issues
        r['n_competing'] = self.find_n_competing(
            peaks,
            window=self.config['nearby_window'],
            fraction=self.config['min_area_fraction'])
        return r

    # n_competing
    def get_window_size(self):
        return 2 * self.config['nearby_window']

    @staticmethod
    @numba.jit(nopython=True, nogil=True, cache=False)
    def find_n_competing(peaks, window, fraction):
        n = len(peaks)
        t = peaks['time']
        a = peaks['area']
        results = np.zeros(n, dtype=np.int16)
        left_i = 0
        right_i = 0
        for i, peak in enumerate(peaks):
            while t[left_i] + window < t[i] and left_i < n - 1:
                left_i += 1
            while t[right_i] - window < t[i] and right_i < n - 1:
                right_i += 1
            results[i] = np.sum(a[left_i:right_i + 1] > a[i] * fraction)

        return results
