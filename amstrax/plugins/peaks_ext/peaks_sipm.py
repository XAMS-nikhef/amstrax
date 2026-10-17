import numba
import numpy as np
import strax
from immutabledict import immutabledict
import amstrax

export, __all__ = strax.exporter()

@export
class PeaksSiPM(strax.Plugin):
    depends_on = ('records_sipm',)
    data_kind = 'peaks_sipm'
    parallel = 'process'
    provides = ('peaks_sipm')
    rechunk_on_save = True

    __version__ = '0.1.0'

    peak_gap_threshold = amstrax.XAMSConfig(
        default=300,
        help="No hits for this many ns triggers a new peak")
    peak_left_extension = amstrax.XAMSConfig(
        default=10,
        help="Include this many ns left of hits in peaks")
    peak_right_extension = amstrax.XAMSConfig(
        default=10,
        help="Include this many ns right of hits in peaks")
    peak_min_area = amstrax.XAMSConfig(
        default=10,
        help="Minimum contributing PMTs needed to define a peak")
    peak_split_min_height = amstrax.XAMSConfig(
        default=25,
        help="Minimum height in PE above a local sum waveform"
        "minimum, on either side, to trigger a split")
    peak_split_min_ratio = amstrax.XAMSConfig(
        default=4,
        help="Minimum ratio between local sum waveform"
        "minimum and maxima on either side, to trigger a split")
    max_sipm_channel = amstrax.XAMSConfig(
        track=True, default=7,
        help="Highest SiPM channel number; peaks store area_per_channel for channels 0..max_sipm_channel")
    channel_map = amstrax.XAMSConfig(
        default="rundoc://?path=xams_bookkeeping.channel_map,daq_config.channel_map&fallback=xams_default",
        type=immutabledict,
        track=False,
        help="Map of channel groups of this run (only checked against max_sipm_channel)",
    )
    gain_to_pe_array = amstrax.XAMSConfig(
        default=None,
        help="Gain to pe array"
    )

    def infer_dtype(self):
        # strax.find_peaks indexes area_per_channel by the absolute channel number, so the array must reach the highest
        # SiPM channel (the TPC / external channels stay empty)
        return amstrax.legacy_peak_dtype(n_channels=self.n_channels)

    @property
    def n_channels(self):
        return self.config['max_sipm_channel'] + 1

    def setup(self):
        sipm = self.channel_map.get('sipm')
        if sipm is not None and sipm[1] > self.config['max_sipm_channel']:
            raise ValueError(f"SiPM channels {tuple(sipm)} of this run exceed max_sipm_channel "
                             f"{self.config['max_sipm_channel']}")

    def compute(self, records_sipm, start, end):

        r = records_sipm
  
        if self.gain_to_pe_array is None:
            #FIXME -> numba doesn't like a NumPy array inside a NumPy array, so this will fail in strax.find_peaks()
            self.to_pe = np.ones(self.n_channels, dtype=np.float32)
        else:
            self.to_pe = np.asarray(self.gain_to_pe_array, dtype=np.float32)
            if len(self.to_pe) < self.n_channels:
                raise ValueError(f"gain_to_pe_array has {len(self.to_pe)} entries, peaks_sipm needs {self.n_channels} "
                                 f"(channels 0..{self.config['max_sipm_channel']})")

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
            result_dtype=strax.peak_dtype(n_channels=self.n_channels),
        )

        strax.sum_waveform(peaks, hits, r, rlinks, self.to_pe)

        peaks = strax.split_peaks(
            peaks, hits, r, rlinks, self.to_pe,
            min_height=self.config['peak_split_min_height'],
            min_ratio=self.config['peak_split_min_ratio'])

        amstrax.compute_peak_widths(peaks)

        # stored in the strax-1.5 peak layout (same format and lineage under strax 1 and 2; amstrax/strax_compat.py)
        return amstrax.to_dtype(peaks, self.dtype)
