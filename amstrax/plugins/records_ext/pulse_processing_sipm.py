import numba
import numpy as np
import strax
from immutabledict import immutabledict

export, __all__ = strax.exporter()

@export
@strax.takes_config(
    strax.Option(
        'baseline_samples',
        default=20, infer_type=False,
        help='Number of samples to use at the start of the SiPM pulse to determine '
             'the baseline'),
)
class PulseProcessingSiPM(strax.Plugin):
    """
    Plugin which performs the pulse processing steps:
    1. Baseline subtraction
    2. Pulse splitting
    3. Pulse merging
    4. Pulse counting
    5. Pulse length and area calculation

    """
    __version__ = '0.0.1'
    
    parallel = 'process'
    rechunk_on_save = False
    compressor = 'zstd'

    depends_on = 'raw_records_sipm'

    provides = 'records_sipm'
    data_kind = 'records_sipm'
    
    # I think in amstrax we can save everything
    # default is ALWAYS
    # save_when = strax.SaveWhen.TARGET
       
    def infer_dtype(self):
        record_length = strax.record_length_from_dtype(
            self.deps["raw_records_sipm"].dtype_for("raw_records_sipm")
        )
        dtype = strax.record_dtype(record_length)
        return dtype

    def compute(self, raw_records_sipm):
        # Do not trust in DAQ + strax.baseline to leave the
        # out-of-bounds samples to zero.
        r = strax.raw_to_records(raw_records_sipm)
        del raw_records_sipm

        r = strax.sort_by_time(r)
        strax.zero_out_of_bounds(r)
        strax.baseline(r, baseline_samples=self.baseline_samples, flip=True)

        strax.integrate(r)

        return r
