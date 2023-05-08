from immutabledict import immutabledict
import strax
import straxen
import numba
import numpy as np

export, __all__ = strax.exporter()

@export
@strax.takes_config(
    # All these must have track=False, so the raw_records hash never changes!
    # DAQ settings -- should match settings given to redax
    strax.Option('record_length', default=110, track=False, type=int,
                 help="Number of samples per raw_record"),
)
class RecordsLED(strax.Plugin):
    """
    Carlo needs to explain
    """

    __version__ = '1.0.0'

    depends_on = ('raw_records',)
    data_kind = 'records_led'
    provides = 'records_led'
    compressor = 'zstd'
    parallel = 'process'
    rechunk_on_save = False
  
    baseline_window = straxen.URLConfig(
        default=(0, 50), infer_type=False,
        help="Window (samples) for baseline calculation.")

    n_records_per_pulse = straxen.URLConfig(
        default=2, type=int,
        help="how many samples per pulse")


    def infer_dtype(self):

        dtype = [(('Start time since unix epoch [ns]', 'time'), '<i8'),
                    (('Length of the interval in samples', 'length'), '<i4'),
                    (('Width of one sample [ns]', 'dt'), '<i2'),
                    (('Channel/PMT number', 'channel'), '<i2'),
                    (('Length of pulse to which the record belongs (without zero-padding)', 'pulse_length'), '<i4'),
                    (('Fragment number in the pulse', 'record_i'), '<i2'),
                    (('Waveform data in raw ADC counts', 'data'), 'f4', (int(self.record_length*self.n_records_per_pulse),))]
            
        return dtype 

    def compute(self, raw_records):
        '''
        Carlo needs to explain
        '''

        record_length = np.shape(raw_records.dtype['data'])[0]
        n_record_i = np.max(raw_records['record_i'])+1
        num_channels = int(len(np.unique(raw_records['channel'])))

        records = np.zeros(int(len(raw_records)/n_record_i), dtype=self.dtype)

        datas = [raw_records[np.where(raw_records['record_i'] == i)[0]] for i in range(n_record_i)]

        for i, name in enumerate(raw_records.dtype.names):
            if name == 'data':  
                records[name] = np.hstack(list(datas[i][name] for i in range(n_record_i)))
            if name == 'length':
                records[name] = np.sum(list(datas[i][name] for i in range(n_record_i)), axis=0)
            else:
                try:
                    records[name] = datas[0][name]
                except:
                    pass
        
        bl = records['data'][:, self.baseline_window[0]:self.baseline_window[1]].mean(axis=1)
        records['data'] = -1. * (records['data'].transpose() - bl[:]).transpose()

        return records
