import strax
import numpy as np
from immutabledict import immutabledict

import amstrax

export, __all__ = strax.exporter()

@export
@strax.takes_config(

    strax.Option('record_length', default=110, track=False, type=int,
                 help="Number of samples per raw_record"),

    strax.Option('baseline_window',
        default=(0, 50), infer_type=False,
        help="Window (samples) for baseline calculation."),

    strax.Option('n_records_per_pulse',
        default=2, type=int,
        help="how many samples per pulse"),

    amstrax.XAMSConfig(
        name="daq_registers",
        default="rundoc://?path=daq_config.registers&fallback=empty",
        track=False,
        infer_type=False,
        help="DAQ register list from rundoc, used to infer channel polarity.",
    ),

    amstrax.XAMSConfig(
        name="channel_map",
        default="rundoc://?path=xams_bookkeeping.channel_map&fallback=xams_default",
        track=False,
        type=immutabledict,
        infer_type=False,
        help="Channel map loaded from rundoc.",
    ),
)
class RecordsLED(strax.Plugin):
    """
    Carlo needs to explain
    """

    __version__ = '1.1.0'

    depends_on = ('raw_records', 'raw_records_sipm')
    data_kind = 'records_led'
    provides = 'records_led'
    compressor = 'zstd'
    parallel = 'process'
    rechunk_on_save = False

    save_when = strax.SaveWhen.TARGET
  
    def setup(self):

        self.record_length = self.config['record_length']
        self.baseline_window = self.config['baseline_window']
        self.n_records_per_pulse = self.config['n_records_per_pulse']
        self.channel_polarity = amstrax.extract_channel_polarity(self.config['daq_registers'])
        self.sipm_channels = amstrax.channels_in_map_group(self.config['channel_map'], 'sipm')

    def infer_dtype(self):

        dtype = [(('Start time since unix epoch [ns]', 'time'), '<i8'),
                    (('Length of the interval in samples', 'length'), '<i4'),
                    (('Width of one sample [ns]', 'dt'), '<i2'),
                    (('Channel/PMT number', 'channel'), '<i2'),
                    (('Length of pulse to which the record belongs (without zero-padding)', 'pulse_length'), '<i4'),
                    (('Fragment number in the pulse', 'record_i'), '<i2'),
                    (('Waveform data in raw ADC counts', 'data'), 'f4', (int(self.record_length*self.n_records_per_pulse),))]
            
        return dtype 

    def compute(self, raw_records, raw_records_sipm):
        '''
        Carlo needs to explain
        '''
        raw_records = self._combine_raw_records(raw_records, raw_records_sipm)
        if len(raw_records) == 0:
            return np.zeros(0, dtype=self.dtype)

        # Group fragments by pulse start time + channel.
        # This is robust to variable record_i multiplicity per pulse/channel.
        rec_len = int(np.shape(raw_records.dtype["data"])[0])
        dt = raw_records["dt"].astype(np.int64)
        rec_i = raw_records["record_i"].astype(np.int64)
        pulse_start = raw_records["time"].astype(np.int64) - rec_i * rec_len * dt
        group_keys = np.core.records.fromarrays(
            [pulse_start, raw_records["channel"]],
            names="pulse_start,channel",
        )
        _, group_first_idx, group_inverse = np.unique(
            group_keys, return_index=True, return_inverse=True
        )

        records = np.zeros(len(group_first_idx), dtype=self.dtype)
        out_wave_len = self.dtype["data"].shape[0]

        for out_i in range(len(group_first_idx)):
            idx = np.where(group_inverse == out_i)[0]
            fragments = raw_records[idx]
            order = np.argsort(fragments["record_i"], kind="stable")
            fragments = fragments[order]

            # Copy scalar/meta fields from first fragment
            first = fragments[0]
            records[out_i]["time"] = first["time"]
            records[out_i]["dt"] = first["dt"]
            records[out_i]["channel"] = first["channel"]
            records[out_i]["pulse_length"] = np.max(fragments["pulse_length"])
            records[out_i]["record_i"] = 0

            # Concatenate variable fragment waveforms, then truncate/pad to fixed LED size.
            parts = []
            total_length = 0
            for frag in fragments:
                frag_len = int(max(0, min(rec_len, frag["length"])))
                if frag_len == 0:
                    continue
                piece = frag["data"][:frag_len].astype(np.float32, copy=False)
                parts.append(piece)
                total_length += frag_len

            if parts:
                wf = np.concatenate(parts)
                clip_len = min(len(wf), out_wave_len)
                records[out_i]["data"][:clip_len] = wf[:clip_len]
            records[out_i]["length"] = min(total_length, out_wave_len)

        bl_lo, bl_hi = self.baseline_window
        bl_lo = max(0, int(bl_lo))
        bl_hi = min(records["data"].shape[1], int(bl_hi))
        if bl_hi <= bl_lo:
            bl_lo, bl_hi = 0, min(50, records["data"].shape[1])
        bl = records["data"][:, bl_lo:bl_hi].mean(axis=1)
        records["data"] = -1.0 * (records["data"].transpose() - bl[:]).transpose()
        self._restore_positive_polarity_channels(records)

        return records

    @staticmethod
    def _combine_raw_records(*raw_records):
        arrays = [r for r in raw_records if len(r)]
        if not arrays:
            return raw_records[0]
        if len(arrays) == 1:
            return arrays[0]
        return strax.sort_by_time(np.concatenate(arrays))

    def _restore_positive_polarity_channels(self, records):
        """
        records_led historically flips all channels after baselining.
        Keep that PMT convention, but undo it for positive-polarity channels
        and for channels explicitly marked as SiPM in the channel map.
        """
        for channel in np.unique(records["channel"]):
            ch = int(channel)
            if self.channel_polarity.get(ch) == 1 or ch in self.sipm_channels:
                records["data"][records["channel"] == channel] *= -1.0
