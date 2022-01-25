import numpy as np
import strax

import amstrax

export, __all__ = strax.exporter()


@export
def hit_min_amplitude(model, n_tpc_pmts=16):
    """Return hitfinder height threshold to use in processing.

    :param model: Model name (str), or int to use a uniform threshold,
    or array/tuple or thresholds to use.
    :param threshold: value of the threshold to be applied in ADC counts.
    """

    if isinstance(model, (int, float)):
        return np.ones(n_tpc_pmts, dtype=np.int16) * model

    if isinstance(model, (tuple, np.ndarray)):
        return model

    if model == 'xamsl_thresholds':
        # ADC thresholds used for XAMSL PMTs
        # (January 20 2022)
        n_tpc_pmts = 4
        result = 15 * np.ones(n_tpc_pmts, dtype=np.int16)
        return result

    if model == 'xams_thresholds':
        # ADC thresholds used for XAMS PMTs
        # (January 20 2022)
        result = 15 * np.ones(n_tpc_pmts, dtype=np.int16)
        return result

    raise ValueError(f"Unknown ADC threshold model {model}")
