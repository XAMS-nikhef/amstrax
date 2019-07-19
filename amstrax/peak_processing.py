import json

import numpy as np
import numba

import strax
from amstrax.common import to_pe, pax_file, get_resource, first_sr1_run,get_elife, select_channels
export, __all__ = strax.exporter()


@export
@strax.takes_config(
    strax.Option('peak_gap_threshold', default=300,
                 help="No hits for this many ns triggers a new peak"),
    strax.Option('peak_left_extension', default=30,
                 help="Include this many ns left of hits in peaks"),
    strax.Option('peak_right_extension', default=30,
                 help="Include this many ns right of hits in peaks"),
    strax.Option('peak_min_pmts', default=1,
                 help="Minimum contributing PMTs needed to define a peak"),
    strax.Option('single_channel_peaks', default=False,
                 help='Whether single-channel peaks should be reported'),
    strax.Option('min_area', default=5,
                 help="Minimum area of peak"),
    strax.Option('peak_split_min_height', default=25,
                 help="Minimum height in PE above a local sum waveform"
                      "minimum, on either side, to trigger a split"),
    strax.Option('peak_split_min_ratio', default=4,
                 help="Minimum ratio between local sum waveform"
                      "minimum and maxima on either side, to trigger a split"),
    strax.Option('diagnose_sorting', track=False, default=True,
                 help="Enable runtime checks for sorting and disjointness"),
    strax.Option('use_channels', default=[0, 1, 2, 3, 4, 5, 6, 7],
                 help='Channels that correspond to TPC channels'),
)
class Peaks(strax.Plugin):
    depends_on = ('records',)
    data_kind = 'peaks'
    parallel = False
    rechunk_on_save = True
    provides = ('peaks')
    __version__ = '0.0.1'

    def infer_dtype(self):
        return strax.peak_dtype(len(self.config['use_channels']))


    def compute(self, records):
        r = records
        r = strax.sort_by_time(r)
        r = np.sort(r, order='time', kind='mergesort')
        # Remove non-TPC channels
        r = select_channels(r, self.config['use_channels'])

        hits = strax.find_hits(r)

        # Remove hits in zero-gain channels
        # they should not affect the clustering!
        hits = hits[to_pe[hits['channel']] != 0]

        hits = strax.sort_by_time(hits)

        peaks = strax.find_peaks(
            hits, to_pe,
            gap_threshold=self.config['peak_gap_threshold'],
            left_extension=self.config['peak_left_extension'],
            right_extension=self.config['peak_right_extension'],
            min_channels=self.config['peak_min_pmts'],
            result_dtype=self.dtype)
        strax.sum_waveform(peaks, r, to_pe)

        # peaks = strax.split_peaks(
        #     peaks, r, to_pe,
        #     min_height=self.config['peak_split_min_height'],
        #     min_ratio=self.config['peak_split_min_ratio'])

        peaks = peaks[peaks['area'] >= self.config['min_area']]

        peaks = strax.sort_by_time(peaks)
        peaks = np.sort(peaks, order='time', kind='mergesort')
        strax.compute_widths(peaks)

        if self.config['diagnose_sorting']:
            assert np.diff(r['time']).min() >= 0, "Records not sorted"
            assert np.diff(hits['time']).min() >= 0, "Hits not sorted"
            assert np.diff(peaks['time']).min() >= 0, "Peaks not sorted"
            assert np.all(peaks['time'][1:]
                          >= strax.endtime(peaks)[:-1]), "Peaks not disjoint"

        return peaks


@export
@strax.takes_config(
    strax.Option('peak_gap_threshold', default=300,
                 help="No hits for this many ns triggers a new peak"),
    strax.Option('peak_left_extension', default=30,
                 help="Include this many ns left of hits in peaks"),
    strax.Option('peak_right_extension', default=30,
                 help="Include this many ns right of hits in peaks"),
    strax.Option('peak_min_pmts', default=1,
                 help="Minimum contributing PMTs needed to define a peak"),
    strax.Option('single_channel_peaks', default=False,
                 help='Whether single-channel peaks should be reported'),
    strax.Option('min_area', default=5,
                 help="Minimum area of peak"),
    strax.Option('peak_split_min_height', default=25,
                 help="Minimum height in PE above a local sum waveform"
                      "minimum, on either side, to trigger a split"),
    strax.Option('peak_split_min_ratio', default=4,
                 help="Minimum ratio between local sum waveform"
                      "minimum and maxima on either side, to trigger a split"),
    strax.Option('diagnose_sorting', track=False, default=False,
                 help="Enable runtime checks for sorting and disjointness"),
    strax.Option('use_channels', default=[9],
                 help='Channels that correspond to TPC channels'),
)
class TriggerPeaks(strax.Plugin):
    depends_on = ('records',)
    data_kind = 'peaks'
    # provides = 'TriggerPeaks'
    parallel = 'process'
    rechunk_on_save = True
    __version__ = '0.0.1'

    def infer_dtype(self):
        return strax.peak_dtype(len(self.config['use_channels']))

    def compute(self, records):
        r = records

        # Remove non-TPC channels
        r = select_channels(r, self.config['use_channels'])

        hits = strax.find_hits(r)

        # Remove hits in zero-gain channels
        # they should not affect the clustering!
        hits = hits[to_pe[hits['channel']] != 0]

        hits = strax.sort_by_time(hits)

        peaks = strax.find_peaks(
            hits, to_pe,
            gap_threshold=self.config['peak_gap_threshold'],
            left_extension=self.config['peak_left_extension'],
            right_extension=self.config['peak_right_extension'],
            min_channels=self.config['peak_min_pmts'],
            result_dtype=self.dtype)
        strax.sum_waveform(peaks, r, to_pe)

        # peaks = strax.split_peaks(
        #     peaks, r, to_pe,
        #     min_height=self.config['peak_split_min_height'],
        #     min_ratio=self.config['peak_split_min_ratio'])

        peaks = peaks[peaks['area'] >= self.config['min_area']]

        strax.compute_widths(peaks)

        if self.config['diagnose_sorting']:
            assert np.diff(r['time']).min() >= 0, "Records not sorted"
            assert np.diff(hits['time']).min() >= 0, "Hits not sorted"
            assert np.all(peaks['time'][1:]
                          >= strax.endtime(peaks)[:-1]), "Peaks not disjoint"

        return peaks

@export
class PeakBasics(strax.Plugin):
    __version__ = "0.0.1"
    parallel = False
    depends_on = ('peaks',)
    rechunk_on_save = True
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

        area_top = p['area_per_channel'][:, :8].sum(axis=1)
        # Negative-area peaks get 0 AFT - TODO why not NaN?
        m = p['area'] > 0
        r['area_fraction_top'][m] = area_top[m]/p['area'][m]
        return r


@export
@strax.takes_config(
    #     strax.Option(
    #         'nn_architecture',
    #         help='Path to JSON of neural net architecture',
    #         default_by_run=[
    #             (0, pax_file('XENON1T_tensorflow_nn_pos_20171217_sr0.json')),
    #             ('', pax_file('XENON1T_tensorflow_nn_pos_20171217_sr1.json'))]),   # noqa

    #     strax.Option(
    #         'nn_weights',
    #         help='Path to HDF5 of neural net weights',
    #         default_by_run=[
    #             (0, pax_file('XENON1T_tensorflow_nn_pos_weights_20171217_sr0.h5')),
    #             (first_sr1_run, pax_file('XENON1T_tensorflow_nn_pos_weights_20171217_sr1.h5'))]),   # noqa
    strax.Option('min_reconstruction_area',
                 help='Skip reconstruction if area (PE) is less than this',
                 default=10)
)
class PeakPositions(strax.Plugin):
    dtype = [('x', np.float32,
              'Reconstructed S2 X position (cm), uncorrected'),
             ('y', np.float32,
              'Reconstructed S2 Y position (cm), uncorrected')]
    depends_on = ('peaks',)

    def setup(self):
        import keras
        import tensorflow as tf

    def compute(self, peaks):
        x = np.zeros(len(peaks), dtype=float)
        y = np.zeros(len(peaks), dtype=float)
        return dict(x=x, y=y)


@export
@strax.takes_config(
    strax.Option('s1_max_width', default=75,
                 help="Maximum (IQR) width of S1s"),
    strax.Option('s1_min_n_channels', default=1,
                 help="Minimum number of PMTs that must contribute to a S1"),
    strax.Option('s2_min_area', default=10,
                 help="Minimum area (PE) for S2s"),
    strax.Option('s2_min_width', default=200,
                 help="Minimum width for S2s"))
class PeakClassification(strax.Plugin):
    __version__ = '0.0.1'
    depends_on = ('peak_basics',)
    dtype = [
        ('type', np.int8, 'Classification of the peak.')]
    parallel = True

    def compute(self, peaks):
        p = peaks
        r = np.zeros(len(p), dtype=self.dtype)

        is_s1 = p['n_channels'] >= self.config['s1_min_n_channels']
        is_s1 &= p['range_50p_area'] < self.config['s1_max_width']
        r['type'][is_s1] = 1

        is_s2 = p['area'] > self.config['s2_min_area']
        is_s2 &= p['range_50p_area'] > self.config['s2_min_width']
        r['type'][is_s2] = 2

        return r

@export
@strax.takes_config(
    strax.Option('min_area_fraction', default=0.5,
                 help='The area of competing peaks must be at least '
                      'this fraction of that of the considered peak'),
    strax.Option('nearby_window', default=int(1e7),
                 help='Peaks starting within this time window (on either side)'
                      'in ns count as nearby.'),
)
class NCompeting(strax.OverlapWindowPlugin):
    depends_on = ('peak_basics',)
    dtype = [
        ('n_competing', np.int32,
            'Number of nearby larger or slightly smaller peaks')]

    def get_window_size(self):
        return 2 * self.config['nearby_window']

    def compute(self, peaks):
        return dict(n_competing=self.find_n_competing(
            peaks,
            window=self.config['nearby_window'],
            fraction=self.config['min_area_fraction']))

    @staticmethod
    @numba.jit(nopython=True, nogil=True, cache=True)
    def find_n_competing(peaks, window, fraction):
        n = len(peaks)
        t = peaks['time']
        a = peaks['area']
        results = np.zeros(n, dtype=np.int32)

        left_i = 0
        right_i = 0
        for i, peak in enumerate(peaks):
            while t[left_i] + window < t[i] and left_i < n - 1:
                left_i += 1
            while t[right_i] - window < t[i] and right_i < n - 1:
                right_i += 1
            results[i] = np.sum(a[left_i:right_i + 1] > a[i] * fraction)

        return results - 1
