import numba
import numpy as np
import strax
import amstrax

export, __all__ = strax.exporter()

# These are also needed in peaklets, since hitfinding is repeated
HITFINDER_OPTIONS = tuple([
    strax.Option(
        'hit_min_amplitude',
        default='pmt_commissioning_initial',
        help='Minimum hit amplitude in ADC counts above baseline. '
             'See straxen.hit_min_amplitude for options.'
    )])


@export
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
    strax.Option('peak_split_min_height', default=25,
                 help="Minimum height in PE above a local sum waveform"
                      "minimum, on either side, to trigger a split"),
    strax.Option('peak_split_min_ratio', default=4,
                 help="Minimum ratio between local sum waveform"
                      "minimum and maxima on either side, to trigger a split"),
    strax.Option('n_tpc_pmts', track=False, default=False,
                 help="Number of channels")
)
class Peaks(strax.Plugin):
    depends_on = ('records',)
    data_kind = 'peaks'
    parallel = 'process'
    provides = ('peaks')
    rechunk_on_save = True

    __version__ = '0.1.50'

    gain_to_pe_array = amstrax.XAMSConfig(
        default=None,
        help="Gain to pe array"
    )

    def infer_dtype(self):
    
        return strax.peak_dtype(n_channels=self.config['n_tpc_pmts'])


    def setup(self):

        if self.gain_to_pe_array is None:
            self.to_pe = np.ones(self.config['n_tpc_pmts'])
        else:
            self.to_pe = np.array(self.gain_to_pe_array)

    def compute(self, records, start, end):

        r = records
  
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

        strax.sum_waveform(peaks, hits, r, rlinks, self.to_pe)

        peaks = strax.split_peaks(
            peaks, hits, r, rlinks, self.to_pe,
            min_height=self.config['peak_split_min_height'],
            min_ratio=self.config['peak_split_min_ratio'])

        strax.compute_widths(peaks)

        return peaks
