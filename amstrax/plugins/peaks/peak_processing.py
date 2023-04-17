import numba
import numpy as np
import strax
from amstrax.SiPMdata import *

export, __all__ = strax.exporter()

# These are also needed in peaklets, since hitfinding is repeated
HITFINDER_OPTIONS = tuple([
    strax.Option(
        'hit_min_amplitude',
        default='pmt_commissioning_initial',
        help='Minimum hit amplitude in ADC counts above baseline. '
             'See straxen.hit_min_amplitude for options.'
    )])


@strax.takes_config(
    strax.Option('peak_gap_threshold', default=300,
                 help="No hits for this many ns triggers a new peak"),
    strax.Option('peak_left_extension', default=10,
                 help="Include this many ns left of hits in peaks"),
    strax.Option('peak_right_extension', default=10,
                 help="Include this many ns right of hits in peaks"),
    strax.Option('peak_min_area', default=10,
                 help="Minimum contributing PMTs needed to define a peak"),
    strax.Option('peak_min_pmts', default=1,
                 help="Minimum contributing PMTs needed to define a peak"),
    strax.Option('single_channel_peaks', default=False,
                 help='Whether single-channel peaks should be reported'),
    strax.Option('peak_split_min_height', default=25,
                 help="Minimum height in PE above a local sum waveform"
                      "minimum, on either side, to trigger a split"),
    strax.Option('peak_split_min_ratio', default=4,
                 help="Minimum ratio between local sum waveform"
                      "minimum and maxima on either side, to trigger a split"),
    strax.Option('diagnose_sorting', track=False, default=False,
                 help="Enable runtime checks for sorting and disjointness"),
    strax.Option('n_tpc_pmts', track=False, default=False,
                 help="Number of channels"),
    strax.Option('pmt_channel', default=0,
                 help="PMT channel for splitting pmt and sipms"), )
class Peaks(strax.Plugin):
    depends_on = ('records',)
    data_kind = 'peaks'
    parallel = 'process'
    provides = ('peaks')
    rechunk_on_save = True

    __version__ = '0.1.50'

    def infer_dtype(self):
    
        return strax.peak_dtype(n_channels=self.config['n_tpc_pmts'])

    def compute(self, records, start, end):

        r = records
  
        self.to_pe = np.ones(self.config['n_tpc_pmts'])

        hits = strax.find_hits(r)
        hits = strax.sort_by_time(hits)

        rlinks = strax.record_links(r)

        # Rewrite to just peaks/hits
        peaks = strax.find_peaks(
            hits, self.to_pe,
            gap_threshold=self.config['peak_gap_threshold'],
            left_extension=self.config['peak_left_extension'],
            right_extension=self.config['peak_right_extension'],
            min_area=self.config['peak_min_area'],
            min_channels=self.config['peak_min_pmts'],
            #             min_channels=1,
            result_dtype=strax.peak_dtype(n_channels=self.config['n_tpc_pmts'])
            #             result_dtype=self.dtype
        )

        
        # self.clip_peaklet_times(peaks, start, end)

        strax.sum_waveform(peaks, hits, r, rlinks, self.to_pe)

        peaks = strax.split_peaks(
            peaks, hits, r, rlinks, self.to_pe,
            min_height=self.config['peak_split_min_height'],
            min_ratio=self.config['peak_split_min_ratio'])

        strax.compute_widths(peaks)

        return peaks

    @staticmethod
    @numba.njit(nogil=True, cache=True)
    def clip_peaklet_times(peaklets, start, end):
        for p in peaklets:
            if p['time'] < start:
                p['time'] = start
            if strax.endtime(p) > end:
                p['length'] = (end - p['time']) // p['dt']




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



@export
class PeakPositions(strax.Plugin):
    depends_on = ('peaks', 'peak_classification')
    rechunk_on_save = False
    __version__ = '0.0.34'  # .33 for LNLIKE
    dtype = [
        ('xr', np.float32,
         'Interaction x-position'),
        ('yr', np.float32,
         'Interaction y-position'),
        ('r', np.float32,
         'radial distance'),
        ('time', np.int64, 'Start time of the peak (ns since unix epoch)'),
        ('endtime', np.int64, 'End time of the peak (ns since unix epoch)')
    ]

    def setup(self):
        # z position of the in-plane SiPMs
        z_plane = 10
        # radius of the cylinder for SiPMs at the side
        r_cylinder = 22
        # radius of a SiPM - I assume circular SiPMs with a radius to make the area correspond to a 3x3mm2 square.
        r_sipm = 1.6925
        # build geometry
        geo = GeoParameters(z_plane=z_plane, r_cylinder=r_cylinder, r_sipm=r_sipm)

        sipm = SiPM(type="plane", position=[0, -15, z_plane], qeff=0.25)
        geo.add_sipm(sipm)
        sipm = SiPM(type="plane", position=[-13, -7.5, z_plane], qeff=0.25)
        geo.add_sipm(sipm)
        # sipm = SiPM(type="plane", position=[0, 15, z_plane], qeff=0.25)
        # geo.add_sipm(sipm)
        sipm = SiPM(type="plane", position=[13, -7.5, z_plane], qeff=0.25)
        geo.add_sipm(sipm)
        sipm = SiPM(type="plane", position=[-4, 0, z_plane], qeff=0.25)
        geo.add_sipm(sipm)
        sipm = SiPM(type="plane", position=[4, 0, z_plane], qeff=0.25)
        geo.add_sipm(sipm)
        sipm = SiPM(type="plane", position=[-13, 7.5, z_plane], qeff=0.25)
        geo.add_sipm(sipm)
        sipm = SiPM(type="plane", position=[13, 7.5, z_plane], qeff=0.25)
        geo.add_sipm(sipm)

        self.geo = geo

    def compute(self, peaks):

        result = np.empty(len(peaks), dtype=self.dtype)

        if not len(peaks):
            return result

        for ix, p in enumerate(peaks):

            if p['type'] != 2:
                continue

            # if [X] channel is not working
            k = np.delete(p['area_per_channel'], [2])
            for i, area in enumerate(k):
                self.geo.sipms[i].set_number_of_hits(area)

            # if all 8 channels are working
            # for i, area in enumerate(p['area_per_channel']):
            #     self.geo.sipms[i].set_number_of_hits(area)

            posrec = Reconstruction(self.geo)
            pos = posrec.reconstruct_position('CHI2')
            for key in ['xr', 'yr']:
                result[key][ix] = pos[key]

            for q in ['time', 'endtime']:
                result[q] = p[q]

        result['r'] = (result['xr'] ** 2 + result['yr'] ** 2) ** (1 / 2)
        return result
