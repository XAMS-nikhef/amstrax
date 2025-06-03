import numba
import numpy as np
import strax
import amstrax

export, __all__ = strax.exporter()

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
    strax.Option('gain_to_pe_array', default=None,
                 help="Gain to pe array"),
    strax.Option('n_ext_pmts', track=True, default=1,
                    help="Number of external channels"),
    strax.Option('n_sipms', track=True, default=1,
                    help="Number of external channels"),
)
class PeaksSiPM(strax.Plugin):
    depends_on = ('records_sipm',)
    data_kind = 'peaks_sipm'
    parallel = 'process'
    provides = ('peaks_sipm')
    rechunk_on_save = True

    __version__ = '0.0.2'

    gain_to_pe_array = amstrax.XAMSConfig(
        default=None,
        help="Gain to pe array"
    )

    def infer_dtype(self):
    
        # it's a bit silly, but for how strax function find_peaks is structured
        # we need to provide empty channels for the TPC pmts as well
        return strax.peak_dtype(n_channels=self.config['n_tpc_pmts']+self.config['n_ext_pmts']+self.config['n_sipms'])

    def compute(self, records_sipm, start, end):

        r = records_sipm
  
        if self.config['gain_to_pe_array'] is None:
            self.to_pe = np.ones(self.config['n_ext_pmts']+self.config['n_tpc_pmts']+self.config['n_sipms'], dtype=np.float32)
        else:
            self.to_pe = self.gain_to_pe_array

        hits = strax.find_hits(r)
        hits = strax.sort_by_time(hits)

        rlinks = strax.record_links(r)

        # Rewrite to just peaks/hits
        peaks = strax.find_peaks(
            hits, np.array(self.to_pe),
            gap_threshold=self.config['peak_gap_threshold'],
            left_extension=self.config['peak_left_extension'],
            right_extension=self.config['peak_right_extension'],
            min_area=self.config['peak_min_area'],
            min_channels=1,
            # must have more than one channel (why?)
            # see here https://github.com/AxFoundation/strax/blob/b0ca3cb245275abb84a4c3535544cce2876bd50e/strax/dtypes.py#L190
            result_dtype=strax.peak_dtype(n_channels=self.config['n_ext_pmts']+self.config['n_tpc_pmts']+self.config['n_sipms']),
        )

        strax.sum_waveform(peaks, hits, r, rlinks, self.to_pe)

        peaks = strax.split_peaks(
            peaks, hits, r, rlinks, self.to_pe,
            min_height=self.config['peak_split_min_height'],
            min_ratio=self.config['peak_split_min_ratio'])

        strax.compute_widths(peaks)

        return peaks
